#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════════╗
║   🐲 烛龙 V2.1 情报层 - RAG 记忆系统                                  ║
║   zhulong/intelligence/rag.py                                         ║
╚══════════════════════════════════════════════════════════════════════╝

Fin-R1 档案调查员 (Historical Context Specialist)
- 角色：对比当前标的异动与历史筹码分布的相似性
- 输出约束：严禁预测，严禁建议
"""

import os
import json
import time
import logging
import hashlib
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from enum import Enum
from dataclasses import dataclass, asdict

# ==================== 常量配置 ====================

BASE_DIR = Path(__file__).resolve().parents[2]
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))
from config.settings import Config
STORAGE_DIR = BASE_DIR / "storage"
SNAPSHOTS_DIR = STORAGE_DIR / "intelligence_logs" / "snapshots"
OLLAMA_URL = str(Config.OLLAMA_URL).split("/api/", 1)[0]
FIN_R1_MODEL = "fin-auditor:7b"
SMALL_MODELS_TO_CLEAR = (
    "lfm2.5-thinking:1.2b",
    "qwen2.5:1.5b",
    "deepseek-r1:1.5b",
)

# 确保目录存在
SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger('zhulong.intelligence.rag')

_CORE_DIR = BASE_DIR / "04_governance" / "lib" / "core"
if str(_CORE_DIR) not in sys.path:
    sys.path.append(str(_CORE_DIR))
from module_loader import load_attr_from_path

DBGateway = load_attr_from_path(
    "db_gateway_01",
    BASE_DIR / "01_engine" / "lib" / "db_gateway.py",
    "DBGateway",
)
ComputeGateway = load_attr_from_path(
    "compute_gateway_02",
    BASE_DIR / "02_brain" / "lib" / "compute_gateway.py",
    "ComputeGateway",
)
COMPUTE_GATEWAY = ComputeGateway(logger=logger, max_slots=3)


# ==================== 历史模式枚举 ====================

class HistoricalPattern(str, Enum):
    """历史模式枚举 (V2.1 协议约束)"""
    BREAKOUT_CONTINUATION = "BREAKOUT_CONTINUATION"  # 突破延续
    FALSE_BREAKOUT = "FALSE_BREAKOUT"                # 假突破
    CONSOLIDATION = "CONSOLIDATION"                  # 横盘整理
    REVERSAL = "REVERSAL"                            # 反转
    MOMENTUM_FADE = "MOMENTUM_FADE"                  # 动量衰减
    UNKNOWN = "UNKNOWN"                              # 未知


# ==================== 快照数据模型 ====================

@dataclass
class HistoricalSnapshot:
    """历史快照数据模型"""
    symbol: str
    trade_date: str
    created_at: str
    similarity_score: float          # 0-1
    historical_pattern: str          # 枚举值
    backtest_performance: float      # 历史胜率
    similar_cases: List[str]         # 相似案例
    key_insight: str                 # 核心洞察
    ttl_hours: int = 24              # 生存时间


# ==================== 档案调查员 Prompt ====================

PROMPT_ARCHIVIST = """你是烛龙系统首席档案调查员 (Historical Context Specialist)。

═══════════════════════════════════════════════════════════
【角色定位】
- 你是一名专注于历史数据比对的档案分析师
- 你的任务是对比当前标的异动与历史筹码分布的相似性
- 你不是交易顾问，不做任何预测或建议
═══════════════════════════════════════════════════════════

【当前标的】
- 代码: {symbol}
- 日期: {trade_date}
- 异动标签: {anomaly_flags}

【历史档案】
{historical_records}

═══════════════════════════════════════════════════════════
【输出约束】
1. 严禁预测：不得包含任何对未来走势的判断
2. 严禁建议：不得包含任何买卖建议
3. 仅作比对：只输出历史相似性分析结果
═══════════════════════════════════════════════════════════

【输出格式】必须为 JSON：
{{
    "similarity_score": 0.0-1.0,
    "historical_pattern": "BREAKOUT_CONTINUATION" | "FALSE_BREAKOUT" | "CONSOLIDATION" | "REVERSAL" | "MOMENTUM_FADE" | "UNKNOWN",
    "backtest_performance": 0.0-1.0,
    "similar_cases": ["YYYY-MM-DD: 简述案例1", "YYYY-MM-DD: 简述案例2"],
    "key_insight": "一句话核心洞察（不含预测）"
}}

禁止输出任何其他文字。"""


# ==================== RAG 管线 ====================

class RAGPipeline:
    """
    RAG 记忆系统管线

    功能：
    1. 历史记录检索
    2. Fin-R1 调用
    3. 快照生成与持久化
    """

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or (BASE_DIR / "storage" / "database" / "zhulong.duckdb")
        self.snapshots_dir = SNAPSHOTS_DIR
        self.ollama_url = OLLAMA_URL
        self.model = FIN_R1_MODEL
        logger.info(f"📚 RAG 管线初始化 | 快照目录: {self.snapshots_dir}")

    def _normalize_symbol(self, code: str) -> str:
        if not code:
            return ""
        raw = str(code).strip().upper()
        if "." in raw:
            raw = raw.split(".", 1)[0]
        digits = ''.join(ch for ch in raw if ch.isdigit())
        return digits[:6] if len(digits) >= 6 else raw

    def _resolve_daily_source(self, conn) -> Dict[str, Optional[str]]:
        """Resolve daily table/fields with symbol-first contract and fallbacks."""
        for table in ("fact_daily", "stock_daily"):
            try:
                meta = conn.execute(f"PRAGMA table_info('{table}')").fetchall()
            except Exception:
                meta = []
            if not meta:
                continue

            cols = {str(r[1]).lower() for r in meta}
            symbol_col = "symbol" if "symbol" in cols else ("ts_code" if "ts_code" in cols else None)
            trade_date_col = "trade_date" if "trade_date" in cols else None
            close_col = "close" if "close" in cols else None
            if not (symbol_col and trade_date_col and close_col):
                continue

            return {
                "table": table,
                "symbol_col": symbol_col,
                "trade_date_col": trade_date_col,
                "close_col": close_col,
                "pct_col": "pct_chg" if "pct_chg" in cols else None,
                "vol_col": "vol" if "vol" in cols else ("volume" if "volume" in cols else None),
                "amt_col": "amount" if "amount" in cols else ("amt" if "amt" in cols else None),
            }

        raise RuntimeError("No compatible daily table found (need symbol/trade_date/close)")

    def get_historical_records(self, symbol: str, limit: int = 5) -> str:
        """从数据库读取历史记录"""

        conn = None
        try:
            conn = DBGateway.get_instance(self.db_path, read_only=True, logger=logger).get_connection(read_only=True)
            source = self._resolve_daily_source(conn)

            symbol_key = self._normalize_symbol(symbol)
            if not symbol_key:
                return "暂无历史记录"

            query_symbol = symbol_key
            if source["symbol_col"] == "ts_code":
                raw = str(symbol).strip().upper()
                if "." in raw:
                    query_symbol = raw
                else:
                    suffix = "SH" if symbol_key[:1] in ("6", "9") else ("BJ" if symbol_key[:1] in ("4", "8") else "SZ")
                    query_symbol = f"{symbol_key}.{suffix}"

            pct_expr = source["pct_col"] or "0"
            vol_expr = source["vol_col"] or "0"
            amt_expr = source["amt_col"] or "0"

            cursor = conn.execute(f"""
                SELECT {source['trade_date_col']} AS trade_date,
                       {source['close_col']} AS close,
                       {pct_expr} AS pct_chg,
                       {vol_expr} AS vol,
                       {amt_expr} AS amount
                FROM {source['table']}
                WHERE {source['symbol_col']} = ?
                ORDER BY {source['trade_date_col']} DESC
                LIMIT ?
            """, (query_symbol, limit * 5))

            rows = cursor.fetchall()

            if not rows:
                return "暂无历史记录"

            records = []
            for row in rows[:limit]:
                date = row[0]
                close = row[1] or 0
                pct = row[2] or 0
                vol = row[3] or 0
                records.append(f"[{date}] 收盘:{close:.2f} 涨跌:{pct:+.2f}% 成交量:{vol:.0f}")

            return "\n".join(records)

        except Exception as e:
            logger.warning(f"历史记录读取失败: {e}")
            return "历史记录读取失败"
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception as exc:
                    logger.error("Non-fatal: RAG query row parse failed: %s", exc, exc_info=True)

    def load_existing_snapshot(self, symbol: str) -> Optional[HistoricalSnapshot]:
        """读取现有快照"""
        snapshot_path = self.snapshots_dir / f"{symbol.replace('.', '_')}.json"

        if not snapshot_path.exists():
            return None

        try:
            with open(snapshot_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # 检查 TTL
            created = datetime.fromisoformat(data.get("created_at", ""))
            ttl = data.get("ttl_hours", 24)
            if datetime.now() - created > timedelta(hours=ttl):
                logger.info(f"快照已过期: {symbol}")
                return None

            return HistoricalSnapshot(**data)

        except Exception as e:
            logger.warning(f"快照读取失败: {e}")
            return None

    def generate_snapshot(
        self,
        symbol: str,
        trade_date: str,
        anomaly_flags: List[str]
    ) -> HistoricalSnapshot:
        """调用 Fin-R1 生成快照"""
        historical_records = self.get_historical_records(symbol)

        prompt = PROMPT_ARCHIVIST.format(
            symbol=symbol,
            trade_date=trade_date,
            anomaly_flags=json.dumps(anomaly_flags),
            historical_records=historical_records
        )

        try:
            # 7b 独占前显式清场: 释放小模型驻留
            for _m in SMALL_MODELS_TO_CLEAR:
                try:
                    COMPUTE_GATEWAY.ollama_generate(
                        server=self.ollama_url,
                        payload={"model": _m, "prompt": "", "keep_alive": 0},
                        timeout=15,
                        layer="RAG",
                        decision_id=f"rag-7b-preclear:{_m}",
                    )
                except Exception as e:
                    logger.error(f'Critical Logical Gap: {str(e)}', exc_info=True)
            resp = COMPUTE_GATEWAY.ollama_generate(
                server=self.ollama_url,
                payload={
                    "model": self.model,
                    "prompt": prompt,
                    "stream": False,
                    "format": "json",
                    "keep_alive": 0,
                },
                timeout=300,
                layer="RAG",
                decision_id=f"rag_snapshot:{symbol}",
            )

            if resp.status_code == 200:
                data = resp.json()
                content = data.get("response", "{}")

                try:
                    result = json.loads(content)
                    snapshot = HistoricalSnapshot(
                        symbol=symbol,
                        trade_date=trade_date,
                        created_at=datetime.now().isoformat(),
                        similarity_score=float(result.get("similarity_score", 0)),
                        historical_pattern=result.get("historical_pattern", "UNKNOWN"),
                        backtest_performance=float(result.get("backtest_performance", 0)),
                        similar_cases=result.get("similar_cases", []),
                        key_insight=result.get("key_insight", "")
                    )

                    # 持久化
                    self.save_snapshot(snapshot)
                    return snapshot

                except json.JSONDecodeError:
                    logger.warning(f"Fin-R1 输出非法 JSON: {content[:100]}")
            else:
                logger.warning(f"Fin-R1 调用失败: HTTP {resp.status_code}")

        except Exception as e:
            logger.error(f"Fin-R1 调用异常: {e}")

        # 返回空快照
        return HistoricalSnapshot(
            symbol=symbol,
            trade_date=trade_date,
            created_at=datetime.now().isoformat(),
            similarity_score=0.0,
            historical_pattern="UNKNOWN",
            backtest_performance=0.0,
            similar_cases=[],
            key_insight="NO_HISTORICAL_RECORD"
        )

    def save_snapshot(self, snapshot: HistoricalSnapshot):
        """保存快照到磁盘"""
        filename = f"{snapshot.symbol.replace('.', '_')}.json"
        filepath = self.snapshots_dir / filename

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(asdict(snapshot), f, ensure_ascii=False, indent=2)

        logger.info(f"快照已保存: {filepath}")

    def get_context_for_l3(self, symbol: str) -> Dict[str, Any]:
        """
        获取 L3 云端审计所需的历史上下文

        实现"零延迟感"：毫秒级磁盘读取，不消耗 GPU
        """
        snapshot = self.load_existing_snapshot(symbol)

        if snapshot:
            return {
                "has_history": True,
                "similarity_score": snapshot.similarity_score,
                "historical_pattern": snapshot.historical_pattern,
                "backtest_performance": snapshot.backtest_performance,
                "similar_cases": snapshot.similar_cases,
                "key_insight": snapshot.key_insight
            }
        else:
            return {
                "has_history": False,
                "key_insight": "NO_HISTORICAL_RECORD"
            }


# ==================== 记忆清理器 ====================

class MemoryCleaner:
    """
    记忆清理机制

    - 失效策略：24小时 TTL
    - 存储限制：10GB 上限
    """

    MAX_STORAGE_GB = 10
    TTL_HOURS = 24

    def __init__(self):
        self.snapshots_dir = SNAPSHOTS_DIR

    def clean_expired(self) -> int:
        """清理过期快照"""
        cleaned = 0
        cutoff = datetime.now() - timedelta(hours=self.TTL_HOURS)

        for f in self.snapshots_dir.glob("*.json"):
            try:
                with open(f, 'r', encoding='utf-8') as fp:
                    data = json.load(fp)

                created = datetime.fromisoformat(data.get("created_at", ""))
                if created < cutoff:
                    f.unlink()
                    cleaned += 1
                    logger.info(f"清理过期快照: {f.name}")

            except Exception as e:
                logger.warning(f"清理失败: {f} - {e}")

        return cleaned

    def check_storage_limit(self) -> bool:
        """检查存储限制"""
        total_size = sum(f.stat().st_size for f in self.snapshots_dir.glob("*"))
        total_gb = total_size / (1024 ** 3)

        if total_gb > self.MAX_STORAGE_GB:
            logger.warning(f"存储超限: {total_gb:.2f}GB > {self.MAX_STORAGE_GB}GB")
            return False

        return True

    def cleanup(self) -> Dict[str, Any]:
        """执行完整清理"""
        expired_cleaned = self.clean_expired()
        storage_ok = self.check_storage_limit()

        return {
            "expired_cleaned": expired_cleaned,
            "storage_ok": storage_ok
        }


# ==================== 导出 ====================

__all__ = [
    "RAGPipeline",
    "MemoryCleaner",
    "HistoricalSnapshot",
    "HistoricalPattern",
    "PROMPT_ARCHIVIST"
]
