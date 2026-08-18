#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
L5 Echo Manager v2.0 - mxbai-embed-large 全精度升级
============================================================
src/layers/l5_evolution/lib/echo_manager.py

升级要点:
  - Embedding Model: mxbai-embed-large (1024维, 原生)
  - 彻底移除 768→1024 Padding 逻辑
  - Qdrant 集合重建 (recreate_collection)
  - 黄金记忆注入 (inject_golden_memories)

Node-103 (ZeroClaw) Qdrant 向量检索接口
  - Host:       192.0.2.30:6333
  - Collection: strategic_memory
  - Dimension:  1024 (mxbai-embed-large 原生)
  - Distance:   Cosine
  - Timeout:    3s (超时静默降级到本地 DuckDB)

Node-116 Ollama Embedding 服务
  - Host:       192.0.2.20:11434
  - Model:      mxbai-embed-large
  - Dimension:  1024 (原生输出, 零 Padding)

记忆写入/检索闭环:
  文本场景 → ollama_embed() → 1024维向量
  → qdrant_upsert() → strategic_memory
  → qdrant_search() → 最相似历史场景 → Echo 因子
============================================================
"""

import json
import time
import logging
import hashlib
import urllib.request
import urllib.error
import importlib
from pathlib import Path
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, field

logger = logging.getLogger('zhulong.l5.echo')
DBGateway = importlib.import_module('01_engine.lib.db_gateway').DBGateway

# ═══════════════════════════════════════════════════════════════
# 配置常量 (锁定, 勿随意修改)
# ═══════════════════════════════════════════════════════════════
QDRANT_HOST = '192.0.2.30'
QDRANT_PORT = 6333
QDRANT_COLLECTION = 'strategic_memory'
QDRANT_DIMENSION = 1024        # mxbai-embed-large 原生维度
QDRANT_TIMEOUT = 3             # 秒
QDRANT_BASE_URL = f'http://{QDRANT_HOST}:{QDRANT_PORT}'

# Ollama Embedding 配置 (Node-116)
OLLAMA_HOST = '192.0.2.20'
OLLAMA_PORT = 11434
OLLAMA_BASE_URL = f'http://{OLLAMA_HOST}:{OLLAMA_PORT}'
EMBEDDING_MODEL = 'mxbai-embed-large'   # 1024维原生, 零 Padding
EMBEDDING_TIMEOUT = 10                  # 秒

# DuckDB (影子账本)
DB_PATH = Path('/root/quant_project/storage/database/zhulong.duckdb')


# ═══════════════════════════════════════════════════════════════
# 数据结构
# ═══════════════════════════════════════════════════════════════
@dataclass
class EchoResult:
    """Echo 检索结果"""
    found: bool = False
    scenario_id: str = ''
    similarity: float = 0.0
    payload: Dict = field(default_factory=dict)
    source: str = 'local'      # 'qdrant' | 'local'
    latency_ms: float = 0.0


@dataclass
class UpsertResult:
    """写入结果"""
    success: bool = False
    point_id: int = 0
    error: str = ''


@dataclass
class EmbedResult:
    """Embedding 结果"""
    success: bool = False
    vector: List[float] = field(default_factory=list)
    dimension: int = 0
    model: str = ''
    latency_ms: float = 0.0
    error: str = ''


# ═══════════════════════════════════════════════════════════════
# HTTP 工具函数
# ═══════════════════════════════════════════════════════════════
def _http_request(method: str, path: str,
                  body: Optional[dict] = None,
                  timeout: int = QDRANT_TIMEOUT,
                  base_url: str = QDRANT_BASE_URL) -> Optional[dict]:
    """
    通用 HTTP 请求 (零外部依赖)

    Returns:
        解析后的 JSON dict, 失败返回 None
    """
    url = f'{base_url}{path}'
    data = json.dumps(body).encode('utf-8') if body else None

    req = urllib.request.Request(
        url,
        data=data,
        headers={'Content-Type': 'application/json'} if data else {},
        method=method
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except urllib.error.URLError as e:
        logger.warning(f'[Echo] {method} {url} 连接失败: {e}')
        return None
    except Exception as e:
        logger.warning(f'[Echo] {method} {url} 异常: {e}')
        return None


# ═══════════════════════════════════════════════════════════════
# Ollama Embedding (Node-116)
# ═══════════════════════════════════════════════════════════════
def ollama_embed(text: str, model: str = EMBEDDING_MODEL) -> EmbedResult:
    """
    调用 Node-116 Ollama 生成 Embedding 向量

    Args:
        text:   待嵌入文本
        model:  模型名 (默认 mxbai-embed-large, 1024维)

    Returns:
        EmbedResult (success, vector, dimension, latency_ms)
    """
    result = EmbedResult(model=model)
    t0 = time.time()

    resp = _http_request(
        'POST',
        '/api/embeddings',
        body={'model': model, 'prompt': text},
        timeout=EMBEDDING_TIMEOUT,
        base_url=OLLAMA_BASE_URL
    )

    result.latency_ms = (time.time() - t0) * 1000

    if resp is None:
        result.error = f'Node-116 ({OLLAMA_HOST}) 不可达'
        logger.warning(f'[Echo/Embed] {result.error}')
        return result

    embedding = resp.get('embedding', [])
    if not embedding:
        result.error = f'空向量返回 (model={model})'
        logger.warning(f'[Echo/Embed] {result.error}')
        return result

    result.vector = embedding
    result.dimension = len(embedding)
    result.success = True

    # 维度校验 (mxbai-embed-large 必须是 1024)
    if result.dimension != QDRANT_DIMENSION:
        logger.error(
            f'[Echo/Embed] 维度异常: {result.dimension} != {QDRANT_DIMENSION} '
            f'(model={model}). 严禁 Padding!'
        )
        result.success = False
        result.error = f'维度不匹配: {result.dimension} != {QDRANT_DIMENSION}'
        return result

    logger.info(
        f'[Echo/Embed] ✅ {model}: dim={result.dimension}, '
        f'latency={result.latency_ms:.1f}ms'
    )
    return result


def ollama_embed_batch(texts: List[str],
                       model: str = EMBEDDING_MODEL) -> List[EmbedResult]:
    """批量 Embedding (逐个调用, 确保内存安全)"""
    results = []
    for i, text in enumerate(texts):
        r = ollama_embed(text, model)
        results.append(r)
        if not r.success:
            logger.warning(f'[Echo/Embed] 批量 {i+1}/{len(texts)} 失败: {r.error}')
    return results


# ═══════════════════════════════════════════════════════════════
# Qdrant 核心 API
# ═══════════════════════════════════════════════════════════════
def qdrant_search(query_vector: List[float],
                  top_k: int = 5,
                  score_threshold: float = 0.6,
                  filter_conditions: Optional[dict] = None) -> List[EchoResult]:
    """
    向 Node-103 Qdrant 发起向量相似度检索

    Args:
        query_vector:      特征向量 (dim=1024, mxbai-embed-large 原生)
        top_k:             返回条数
        score_threshold:   最低相似度阈值
        filter_conditions: Qdrant filter (可选)

    Returns:
        List[EchoResult], 失败时返回空列表 (静默降级)
    """
    if len(query_vector) != QDRANT_DIMENSION:
        logger.error(
            f'[Echo/Qdrant] 维度不匹配: 输入={len(query_vector)}, '
            f'要求={QDRANT_DIMENSION}'
        )
        return []

    t0 = time.time()

    body = {
        'vector': query_vector,
        'limit': top_k,
        'score_threshold': score_threshold,
        'with_payload': True,
    }
    if filter_conditions:
        body['filter'] = filter_conditions

    resp = _http_request(
        'POST',
        f'/collections/{QDRANT_COLLECTION}/points/search',
        body=body
    )

    elapsed = (time.time() - t0) * 1000

    if resp is None:
        logger.info(f'[Echo/Qdrant] 检索失败 ({elapsed:.0f}ms) → 回退本地')
        return []

    results = []
    for point in resp.get('result', []):
        results.append(EchoResult(
            found=True,
            scenario_id=str(point.get('id', '')),
            similarity=float(point.get('score', 0)),
            payload=point.get('payload', {}),
            source='qdrant',
            latency_ms=elapsed
        ))

    if results:
        logger.info(
            f'[Echo/Qdrant] 检索成功: {len(results)} 条, '
            f'top={results[0].similarity:.3f}, '
            f'latency={elapsed:.0f}ms'
        )
    else:
        logger.info(f'[Echo/Qdrant] 检索 0 条 (threshold={score_threshold})')

    return results


def qdrant_upsert(point_id: str,
                  vector: List[float],
                  payload: Dict[str, Any]) -> UpsertResult:
    """
    向 Node-103 写入/更新向量记忆

    Args:
        point_id: 唯一 ID (如 trade_date|symbol|ssd_tag)
        vector:   特征向量 (dim=1024, mxbai-embed-large 原生)
        payload:  元数据 {ssd_tag, regime, pnl, trade_date, ...}

    Returns:
        UpsertResult
    """
    result = UpsertResult()

    if len(vector) != QDRANT_DIMENSION:
        result.error = f'维度不匹配: {len(vector)} != {QDRANT_DIMENSION}'
        logger.error(f'[Echo/Qdrant] {result.error}')
        return result

    # 生成稳定的 int64 ID
    int_id = int(hashlib.md5(point_id.encode()).hexdigest()[:15], 16)
    result.point_id = int_id

    body = {
        'points': [{
            'id': int_id,
            'vector': vector,
            'payload': {
                **payload,
                'source_id': point_id,
                '_inserted_at': time.strftime('%Y-%m-%d %H:%M:%S'),
            },
        }]
    }

    resp = _http_request(
        'PUT',
        f'/collections/{QDRANT_COLLECTION}/points',
        body=body
    )

    if resp and resp.get('status') == 'ok':
        result.success = True
        logger.info(f'[Echo/Qdrant] 写入成功: {point_id} (id={int_id})')
    else:
        result.error = str(resp) if resp else 'connection failed'
        logger.warning(f'[Echo/Qdrant] 写入失败: {result.error}')

    return result


def qdrant_health() -> Dict[str, Any]:
    """检查 Node-103 Qdrant 服务健康状态"""
    resp = _http_request('GET', f'/collections/{QDRANT_COLLECTION}')

    if resp:
        info = resp.get('result', {})
        config = info.get('config', {}).get('params', {})
        return {
            'online': True,
            'status': info.get('status', 'unknown'),
            'vectors_count': info.get('vectors_count', 0),
            'dimension': config.get('vectors', {}).get('size', 0),
            'host': f'{QDRANT_HOST}:{QDRANT_PORT}',
            'collection': QDRANT_COLLECTION,
        }

    return {
        'online': False,
        'status': 'unreachable',
        'vectors_count': 0,
        'dimension': QDRANT_DIMENSION,
        'host': f'{QDRANT_HOST}:{QDRANT_PORT}',
        'collection': QDRANT_COLLECTION,
    }


def ensure_collection() -> bool:
    """确保 strategic_memory 集合存在, 不存在则创建"""
    health = qdrant_health()
    if health['online'] and health['vectors_count'] >= 0:
        logger.info(
            f'[Echo/Qdrant] Collection {QDRANT_COLLECTION} 就绪: '
            f'{health["vectors_count"]} vectors'
        )
        return True

    body = {
        'vectors': {
            'size': QDRANT_DIMENSION,
            'distance': 'Cosine'
        }
    }

    resp = _http_request(
        'PUT',
        f'/collections/{QDRANT_COLLECTION}',
        body=body,
        timeout=10
    )

    if resp and resp.get('result'):
        logger.info(
            f'[Echo/Qdrant] Collection {QDRANT_COLLECTION} 已创建 '
            f'(dim={QDRANT_DIMENSION}, distance=Cosine)'
        )
        return True

    logger.error(f'[Echo/Qdrant] Collection 创建失败: {resp}')
    return False


def recreate_collection() -> bool:
    """
    物理重建集合 (清空旧数据)

    用途: Embedding 模型切换后, 旧向量维度/语义空间不兼容
    """
    logger.warning(
        f'[Echo/Qdrant] ⚠️ 准备重建 {QDRANT_COLLECTION} (清空所有旧向量)'
    )

    # 删除旧集合
    resp = _http_request(
        'DELETE',
        f'/collections/{QDRANT_COLLECTION}',
        timeout=10
    )
    if resp:
        logger.info(f'[Echo/Qdrant] 旧集合已删除')
    else:
        logger.info(f'[Echo/Qdrant] 旧集合不存在或删除失败 (可忽略)')

    # 重建
    time.sleep(0.5)
    body = {
        'vectors': {
            'size': QDRANT_DIMENSION,
            'distance': 'Cosine'
        }
    }
    resp = _http_request(
        'PUT',
        f'/collections/{QDRANT_COLLECTION}',
        body=body,
        timeout=10
    )

    if resp and resp.get('result'):
        logger.info(
            f'[Echo/Qdrant] ✅ 集合重建完成: '
            f'dim={QDRANT_DIMENSION}, distance=Cosine'
        )
        return True

    logger.error(f'[Echo/Qdrant] 集合重建失败: {resp}')
    return False


# ═══════════════════════════════════════════════════════════════
# 语义端到端: 文本 → Embedding → Qdrant
# ═══════════════════════════════════════════════════════════════
def semantic_search(query_text: str,
                    top_k: int = 5,
                    score_threshold: float = 0.6) -> List[EchoResult]:
    """
    语义检索: 文本 → mxbai-embed-large → Qdrant 搜索

    全链路闭环, 调用方无需关心向量维度
    """
    t0 = time.time()

    # Step 1: Embedding
    embed_result = ollama_embed(query_text)
    if not embed_result.success:
        logger.warning(f'[Echo/Semantic] Embedding 失败: {embed_result.error}')
        return []

    # Step 2: Qdrant 检索
    results = qdrant_search(
        embed_result.vector,
        top_k=top_k,
        score_threshold=score_threshold
    )

    total_ms = (time.time() - t0) * 1000
    logger.info(
        f'[Echo/Semantic] 全链路: embed={embed_result.latency_ms:.1f}ms + '
        f'search={total_ms - embed_result.latency_ms:.1f}ms = '
        f'total={total_ms:.1f}ms'
    )
    return results


def semantic_upsert(point_id: str,
                    text: str,
                    payload: Dict[str, Any]) -> UpsertResult:
    """
    语义写入: 文本 → mxbai-embed-large → Qdrant 存储

    全链路闭环, 调用方无需手动生成向量
    """
    # Step 1: Embedding
    embed_result = ollama_embed(text)
    if not embed_result.success:
        return UpsertResult(error=f'Embedding 失败: {embed_result.error}')

    # Step 2: Qdrant 写入
    return qdrant_upsert(point_id, embed_result.vector, payload)


# ═══════════════════════════════════════════════════════════════
# 黄金记忆注入
# ═══════════════════════════════════════════════════════════════
def inject_golden_memories() -> Dict[str, Any]:
    """
    从 DuckDB fact_strategic_memory 提取交易摘要, 重新 Embedding 并注入 Qdrant

    字段映射 (与 DuckDB 严格对齐):
      - narrative_text: 审计叙事文本
      - ssd_tags:       语义标签

    严禁硬编码种子, 数据必须来自 DuckDB 事实表
    """
    stats = {'total': 0, 'success': 0, 'failed': 0, 'skipped': 0, 'latency_ms': 0}
    t0 = time.time()

    # 从 DuckDB 提取记忆 (字段名与表结构严格对齐)
    memories = []
    try:
        with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
            rows = conn.execute("""
            SELECT symbol, trade_date, narrative_text, ssd_tags
            FROM fact_strategic_memory
            WHERE narrative_text IS NOT NULL
              AND LENGTH(narrative_text) > 10
            ORDER BY trade_date DESC
            LIMIT 20
        """).fetchall()

        for r in rows:
            memories.append({
                'symbol': r[0],
                'trade_date': str(r[1]),
                'narrative_text': r[2],
                'ssd_tags': r[3] or ''
            })
        logger.info(f'[Echo/Golden] DuckDB 提取: {len(memories)} 条记忆')
    except Exception as e:
        logger.error(f'[Echo/Golden] DuckDB 读取失败: {e}')

    if not memories:
        logger.warning('[Echo/Golden] fact_strategic_memory 无可用记录, 注入中止')
        stats['latency_ms'] = (time.time() - t0) * 1000
        return stats

    stats['total'] = len(memories)

    for mem in memories:
        embed_text = f"{mem['narrative_text']} {mem['ssd_tags']}"
        point_id = f"{mem['trade_date']}|{mem['symbol']}|{mem['ssd_tags']}"

        ur = semantic_upsert(point_id, embed_text, {
            'symbol': mem['symbol'],
            'trade_date': mem['trade_date'],
            'ssd_tags': mem['ssd_tags'],
            'narrative_text': mem['narrative_text'][:200],
        })

        if ur.success:
            stats['success'] += 1
        else:
            stats['failed'] += 1
            logger.warning(f'[Echo/Golden] 写入失败: {mem["symbol"]}: {ur.error}')

    stats['latency_ms'] = (time.time() - t0) * 1000
    logger.info(
        f'[Echo/Golden] 注入完成: '
        f'{stats["success"]}/{stats["total"]} 成功, '
        f'{stats["latency_ms"]:.0f}ms'
    )
    return stats


# ═══════════════════════════════════════════════════════════════
# CLI 自检
# ═══════════════════════════════════════════════════════════════
if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(message)s'
    )

    import argparse
    parser = argparse.ArgumentParser(description='L5 Echo Manager v2.0')
    parser.add_argument('--recreate', action='store_true',
                        help='重建 Qdrant 集合 (清空旧数据)')
    parser.add_argument('--inject', action='store_true',
                        help='注入黄金记忆')
    parser.add_argument('--test', action='store_true',
                        help='全链路闭环测试')
    parser.add_argument('--benchmark', action='store_true',
                        help='性能压测')
    args = parser.parse_args()

    print('=' * 60)
    print('L5 Echo Manager v2.0 - mxbai-embed-large 全精度')
    print('=' * 60)

    # 1. Embedding 健康检查
    print('\n[1/5] Embedding 模型检查...')
    test_embed = ollama_embed('健康检查测试文本')
    print(f'  Model:     {EMBEDDING_MODEL}')
    print(f'  Node:      {OLLAMA_HOST}:{OLLAMA_PORT}')
    print(f'  Success:   {test_embed.success}')
    print(f'  Dimension: {test_embed.dimension}')
    print(f'  Latency:   {test_embed.latency_ms:.1f}ms')
    if not test_embed.success:
        print(f'  Error:     {test_embed.error}')

    # 2. Qdrant 健康检查
    print('\n[2/5] Qdrant 健康检查...')
    h = qdrant_health()
    print(f'  Online:   {h["online"]}')
    print(f'  Status:   {h["status"]}')
    print(f'  Vectors:  {h["vectors_count"]}')
    print(f'  Dimension:{h["dimension"]}')
    print(f'  Host:     {h["host"]}')

    # 3. 集合管理
    if args.recreate:
        print('\n[3/5] 重建集合...')
        ok = recreate_collection()
        print(f'  Recreated: {ok}')
    else:
        print('\n[3/5] 确保集合...')
        ok = ensure_collection()
        print(f'  Collection ready: {ok}')

    # 4. 黄金记忆注入
    if args.inject:
        print('\n[4/5] 注入黄金记忆...')
        stats = inject_golden_memories()
        print(f'  Total:    {stats["total"]}')
        print(f'  Success:  {stats["success"]}')
        print(f'  Failed:   {stats["failed"]}')
        print(f'  Latency:  {stats["latency_ms"]:.0f}ms')
    else:
        print('\n[4/5] 跳过黄金记忆注入 (使用 --inject)')

    # 5. 全链路闭环测试
    if args.test or args.benchmark:
        print('\n[5/5] 全链路闭环测试...')
        if test_embed.success and ok:
            t0 = time.time()

            # 写入
            ur = semantic_upsert(
                'SELFTEST_mxbai_001',
                '科创板半导体龙头 量价突破 机构介入',
                {'ssd_tag': '#SELFTEST', 'regime': 'CAUTION', 'pnl': 0.0}
            )
            print(f'  Upsert:  success={ur.success}')

            time.sleep(0.3)  # Qdrant 索引延迟

            # 检索
            results = semantic_search(
                '半导体板块量能放大', top_k=3, score_threshold=0.3
            )
            total_ms = (time.time() - t0) * 1000
            if results:
                print(f'  Search:  found={len(results)}, '
                      f'top_sim={results[0].similarity:.3f}')
            else:
                print('  Search:  no results')
            print(f'  End2End: {total_ms:.1f}ms')

            if args.benchmark:
                # 连续 10 次检索压测
                print('\n  --- 压测 (10次检索) ---')
                latencies = []
                for i in range(10):
                    t1 = time.time()
                    _ = semantic_search('动能突破RPS转强', top_k=3,
                                        score_threshold=0.3)
                    latencies.append((time.time() - t1) * 1000)
                avg = sum(latencies) / len(latencies)
                p99 = sorted(latencies)[int(len(latencies)*0.99)]
                print(f'  Avg:  {avg:.1f}ms')
                print(f'  P99:  {p99:.1f}ms')
                print(f'  Min:  {min(latencies):.1f}ms')
                print(f'  Max:  {max(latencies):.1f}ms')
        else:
            print('  跳过 (Embedding 或 Qdrant 不可用)')

    print('\n' + '=' * 60)
    print('自检完成')
    print('=' * 60)
