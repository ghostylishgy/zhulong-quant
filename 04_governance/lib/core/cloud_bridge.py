#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
04_governance/lib/core/cloud_bridge.py
======================================
Cloud Bridge v2 — 防弹云端通道

防弹机制:
- 指数退避重试 (1s, 2s, 4s) × 3 次
- 计数熔断器: 连续 5 次失败 → 自动停止云端调用
- PushPlus 降级推送: 3 次失败后向指挥官推送原始情报
- 装甲 JSON 解析器: 4 层提取策略
"""

import re
import json
import time
import logging
import os
import sys
from pathlib import Path
import threading
from dataclasses import dataclass, field
from typing import Optional, Dict, Any

logger = logging.getLogger('zhulong.cloud_bridge')


try:
    from .module_loader import load_attr_from_path, resolve_project_root
except Exception:
    _core_dir = Path(__file__).resolve().parent
    if str(_core_dir) not in sys.path:
        sys.path.append(str(_core_dir))
    from module_loader import load_attr_from_path, resolve_project_root

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
ComputeGateway = load_attr_from_path(
    "compute_gateway_02",
    _PROJECT_ROOT / "02_brain" / "lib" / "compute_gateway.py",
    "ComputeGateway",
)
COMPUTE_GATEWAY = ComputeGateway(logger=logger, max_slots=3)

# ═══════════════════════════════════════════════════════
# 全局装甲解析器 (Armored Parser v2 — Global Edition)
# ═══════════════════════════════════════════════════════

def armored_json_extract(raw: str, required_field: str = "verdict") -> Optional[dict]:
    """
    从模型原始输出中提取 JSON。4 层防护:
    1. 清洗 markdown/think 噪音
    2. 精确 {} 块匹配 (优先含 required_field)
    3. 贪婪 DOTALL 兜底
    4. 正则关键字段提取 (最后手段)
    """
    if not raw or not raw.strip():
        return None

    clean = re.sub(r'```(?:json)?\s*', '', raw)
    clean = re.sub(r'```', '', clean)
    clean = re.sub(r'</?think>', '', clean, flags=re.IGNORECASE)

    candidates = re.findall(r'\{[^{}]*\}', clean)
    for cand in candidates:
        try:
            parsed = json.loads(cand)
            if required_field in parsed:
                return parsed
        except (json.JSONDecodeError, ValueError):
            continue

    m = re.search(r'\{[\s\S]*\}', clean)
    if m:
        try:
            return json.loads(m.group())
        except (json.JSONDecodeError, ValueError):
            pass

    verdict_m = re.search(
        r'"?verdict"?\s*:\s*"?(APPROVE|HOLD|VETO|BUY|SELL|PASS|EOD_BUY|STRONG_BUY)"?',
        clean, re.IGNORECASE
    )
    score_m = re.search(r'"?(?:audit_)?score"?\s*:\s*(\d+)', clean)
    if verdict_m:
        return {
            "verdict": verdict_m.group(1).upper(),
            "audit_score": int(score_m.group(1)) if score_m else 50,
            "reasoning": "armored_regex_fallback",
            "falsifiable_conditions": []
        }

    return None


# ═══════════════════════════════════════════════════════
# 计数熔断器 (Circuit Breaker)
# ═══════════════════════════════════════════════════════

class CircuitBreaker:
    """
    简单计数熔断器。
    连续 threshold 次失败后自动熔断，拒绝后续请求。
    手动 reset 或 cool_down 秒后自动恢复。
    """

    def __init__(self, threshold: int = 5, cool_down: int = 600):
        self._fail_count = 0
        self._threshold = threshold
        self._cool_down = cool_down
        self._tripped_at: float = 0
        self._lock = threading.Lock()

    @property
    def is_open(self) -> bool:
        """熔断器是否处于熔断状态"""
        with self._lock:
            if self._fail_count >= self._threshold:
                if time.time() - self._tripped_at > self._cool_down:
                    logger.info("CircuitBreaker: cool_down 到期，自动恢复")
                    self._fail_count = 0
                    return False
                return True
            return False

    def record_success(self):
        with self._lock:
            self._fail_count = 0

    def record_failure(self):
        with self._lock:
            self._fail_count += 1
            if self._fail_count >= self._threshold:
                self._tripped_at = time.time()
                logger.critical(
                    f"CircuitBreaker TRIPPED: 连续 {self._fail_count} 次失败, "
                    f"云端中断 {self._cool_down}s"
                )

    def reset(self):
        with self._lock:
            self._fail_count = 0
            self._tripped_at = 0

    @property
    def fail_count(self) -> int:
        return self._fail_count


# 全局熔断器实例
_circuit_breaker = CircuitBreaker(threshold=5, cool_down=600)


def get_circuit_breaker() -> CircuitBreaker:
    return _circuit_breaker


# ═══════════════════════════════════════════════════════
# PushPlus 降级推送
# ═══════════════════════════════════════════════════════

def _pushplus_degrade(title: str, content: str):
    """
    PushPlus 降级通道: 云端 3 次失败后推送原始情报到指挥官手机。
    静默降级 — 推送失败本身不抛异常。
    """
    token = os.environ.get('PUSHPLUS_TOKEN', '')
    if not token:
        logger.warning("[DEGRADE] PUSHPLUS_TOKEN 未配置, 降级推送跳过")
        return

    try:
        COMPUTE_GATEWAY.http_post(
            'https://www.pushplus.plus/send',
            timeout=5,
            json_payload={
                'token': token,
                'title': f'?? {title}',
                'content': content[:2000],
                'template': 'txt',
            },
            layer='PUSH',
            decision_id='cloud_bridge:pushplus_degrade',
        )
        logger.info(f"[DEGRADE] PushPlus 降级推送已发送: {title}")
    except Exception as e:
        logger.error(f"[DEGRADE] PushPlus 推送失败: {e}")


# ═══════════════════════════════════════════════════════
# 云端 API 统一通道 (防弹版)
# ═══════════════════════════════════════════════════════

@dataclass
class CloudConfig:
    DEEPSEEK_URL: str = "https://api.deepseek.com/v1/chat/completions"
    QWEN_URL: str = "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions"
    ZHIPU_URL: str = "https://open.bigmodel.cn/api/paas/v4/chat/completions"
    DEEPSEEK_MODEL: str = "deepseek-chat"
    QWEN_MODEL: str = "qwen-plus"

    @staticmethod
    def get_key(provider: str) -> str:
        return {
            "deepseek": os.environ.get("DEEPSEEK_API_KEY", ""),
            "qwen": os.environ.get("QWEN_API_KEY", "") or os.environ.get("DASHSCOPE_API_KEY", ""),
            "zhipu": os.environ.get("ZHIPU_API_KEY", ""),
        }.get(provider, "")



def _format_fallback(data: dict) -> str:
    """将战术模块传入的 fallback_data 格式化为可读降级情报"""
    if not data:
        return ""
    lines = ["\U0001f4ca 核心降级情报:"]
    field_map = {
        "symbol": "标的", "l2_score": "L2评分",
        "vol_ratio": "量比", "pct_chg": "涨跌幅",
        "trigger_reason": "触发原因", "price": "现价",
        "eod_score": "EOD评分", "concentration": "尾盘集中度",
    }
    for k, label in field_map.items():
        if k in data:
            v = data[k]
            if isinstance(v, float):
                v = f"{v:.2f}"
            lines.append(f"  {label}: {v}")
    return "\n".join(lines) + "\n"

def cloud_fast_call(prompt: str, system: str = "",
                    provider: str = "deepseek",
                    timeout: int = 12,
                    max_tokens: int = 256,
                    temperature: float = 0.1,
                    max_retries: int = 3,
                    degrade_title: str = "",
                    fallback_data: dict = None) -> Optional[dict]:
    """
    防弹云端快速判决通道。

    防弹机制:
    1. 熔断检查: 若熔断器已打开, 直接跳过
    2. 指数退避重试: 1s → 2s → 4s (max_retries=3)
    3. 装甲 JSON 解析
    4. PushPlus 降级推送 (全部失败后)

    Args:
        degrade_title: 降级推送标题, 非空时启用 PushPlus 降级
    """
    breaker = get_circuit_breaker()

    # 熔断检查
    if breaker.is_open:
        logger.warning(f"CloudBridge: 熔断器已打开, 跳过 {provider} 调用")
        if degrade_title:
            fb = _format_fallback(fallback_data) if fallback_data else ""
            _pushplus_degrade(
                f"熔断降级 | {degrade_title}",
                f"云端连续 {breaker.fail_count} 次失败, 已自动熔断。\n"
                f"{fb}\n原始 Prompt:\n{prompt[:300]}"
            )
        return None

    cfg = CloudConfig()
    key = cfg.get_key(provider)
    if not key:
        logger.warning(f"CloudBridge: {provider} API key missing")
        return None

    url_map = {"deepseek": cfg.DEEPSEEK_URL, "qwen": cfg.QWEN_URL, "zhipu": cfg.ZHIPU_URL}
    model_map = {"deepseek": cfg.DEEPSEEK_MODEL, "qwen": cfg.QWEN_MODEL, "zhipu": "glm-4-flash"}

    url = url_map.get(provider, cfg.DEEPSEEK_URL)
    model = model_map.get(provider, cfg.DEEPSEEK_MODEL)

    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})
    payload = {"model": model, "messages": messages,
               "max_tokens": max_tokens, "temperature": temperature}

    last_error = None

    for attempt in range(1, max_retries + 1):
        backoff = 2 ** (attempt - 1)  # 1s, 2s, 4s
        t0 = time.time()

        try:
            resp = COMPUTE_GATEWAY.http_post(url, timeout=timeout, headers=headers, json_payload=payload, layer='CLOUD', decision_id=f'cloud_fast_call:{provider}:{attempt}')
            elapsed = time.time() - t0

            if resp.status_code == 200:
                content = resp.json().get("choices", [{}])[0].get("message", {}).get("content", "")
                logger.info(f"CloudBridge [{provider}] attempt {attempt}: {elapsed:.1f}s, {len(content)} chars")

                parsed = armored_json_extract(content)
                if parsed:
                    parsed["_cloud_elapsed_s"] = round(elapsed, 1)
                    parsed["_cloud_provider"] = provider
                    parsed["_cloud_attempts"] = attempt
                    breaker.record_success()
                    return parsed
                else:
                    logger.warning(f"CloudBridge: JSON extraction failed ({len(content)} chars)")
                    # JSON 解析失败也算部分成功 (API 本身可达)
                    breaker.record_success()
                    return {"_raw": content, "_cloud_elapsed_s": round(elapsed, 1),
                            "_parse_fail": True, "_cloud_attempts": attempt}
            else:
                last_error = f"HTTP {resp.status_code}"
                logger.warning(f"CloudBridge [{provider}] attempt {attempt}: {last_error}")

        except Exception as e:
            if COMPUTE_GATEWAY.is_timeout_error(e):
                last_error = f"Timeout after {timeout}s"
                logger.warning(f"CloudBridge [{provider}] attempt {attempt}: {last_error}")
            elif 'ConnectionError' in str(e):
                last_error = f"ConnectionError: {e}"
                logger.warning(f"CloudBridge [{provider}] attempt {attempt}: {last_error}")
            else:
                last_error = str(e)
                logger.error(f"CloudBridge [{provider}] attempt {attempt}: {last_error}")

        breaker.record_failure()

        if attempt < max_retries:
            logger.info(f"CloudBridge: 退避 {backoff}s 后重试...")
            time.sleep(backoff)

    # 全部重试失败 → PushPlus 降级推送 (携带结构化降级数据)
    if degrade_title:
        fb = _format_fallback(fallback_data) if fallback_data else ""
        _pushplus_degrade(
            f"云端失败 | {degrade_title}",
            f"API {provider} 连续 {max_retries} 次失败。\n"
            f"最后错误: {last_error}\n"
            f"{fb}\n原始 Prompt:\n{prompt[:400]}"
        )

    logger.error(f"CloudBridge [{provider}]: {max_retries} 次全部失败, 最后错误: {last_error}")
    return None
