#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
03_tactics/rabbit_anchor.py
============================
🐰 守株待兔协议 (Static Anchor)
基石仓位低吸 — POC 筹码密集区锚定

架构: N150 盘后全量计算 (ZPE-2 全精度模式)
时间窗口: 20:30 Phase2 后, 盘后主力计算
哲学: 不成交即风控成功, 拒绝追涨, 只接受价格主动碰撞
"""

import os
import sys
import logging
import runpy
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Any, List, Dict, Optional, Tuple
from dataclasses import dataclass

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger('zhulong.rabbit')

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    import importlib
    DBGateway = importlib.import_module('01_engine.lib.db_gateway').DBGateway
except Exception:
    loader_ns = runpy.run_path(str(PROJECT_ROOT / '04_governance' / 'lib' / 'core' / 'module_loader.py'))
    DBGateway = loader_ns['load_attr_from_path'](
        'zhulong_db_gateway_rabbit_anchor',
        PROJECT_ROOT / '01_engine' / 'lib' / 'db_gateway.py',
        'DBGateway',
    )

DB_PATH = str(PROJECT_ROOT / 'storage' / 'database' / 'zhulong.duckdb')

# ═══════════════════════════════════════════════════════
# 配置
# ═══════════════════════════════════════════════════════

POC_LOOKBACK_DAYS = 60         # 筹码分布回看天数
PRICE_BINS = 100               # 价格分布区间数
SUPPORT_ATR_FACTOR = 0.5       # 支撑位 = POC下沿 - ATR * factor


@dataclass
class POCResult:
    """筹码密集区结果"""
    symbol: str
    poc_price: float        # POC 中心价格
    poc_lower: float        # POC 下沿
    poc_upper: float        # POC 上沿
    poc_volume_pct: float   # POC 区域成交量占比
    support: float          # 支撑位 (限价单挂单价)
    atr_14: float           # 14 日 ATR
    current_price: float    # 当前收盘价
    distance_pct: float     # 当前价距 POC 距离 (%)

    @property
    def is_near_poc(self) -> bool:
        """当前价是否在 POC 区域附近 (±3%)"""
        return abs(self.distance_pct) < 3.0


@dataclass
class LimitOrder:
    """限价单建议"""
    symbol: str
    order_price: float
    current_price: float
    discount_pct: float     # 折价比例
    poc_price: float
    reason: str


class RabbitAnchor:
    """
    🐰 守株待兔协议

    N150 主战场 — 盘后全量 POC 计算
    ZPE-2 全精度模式: POC 价格和 ATR 支撑位不截断
    """

    def calc_poc(self, symbol: str, daily_data: List[dict],
                 n_bins: int = PRICE_BINS) -> Optional[POCResult]:
        """
        计算筹码密集区 (Point of Control)

        从近 N 日 K 线构建 Volume Profile:
        1. 将价格区间等分为 n_bins
        2. 将每日成交量按价格区间分配
        3. POC = 最大累积成交量的价格区间

        Args:
            symbol: 标的代码
            daily_data: [{close, high, low, volume, ...}, ...]  近60日
        """
        if not daily_data or len(daily_data) < 20:
            logger.warning(f"🐰 [Rabbit] {symbol}: 数据不足 ({len(daily_data or [])} 天)")
            return None

        # 提取价格和成交量
        highs = [float(d.get("high", d.get("close", 0))) for d in daily_data]
        lows = [float(d.get("low", d.get("close", 0))) for d in daily_data]
        closes = [float(d.get("close", 0)) for d in daily_data]
        volumes = [float(d.get("volume", 0)) for d in daily_data]

        if not any(v > 0 for v in volumes):
            return None

        # 价格范围
        price_min = min(lows)
        price_max = max(highs)
        if price_max <= price_min:
            return None

        # 构建 Volume Profile
        bin_edges = np.linspace(price_min, price_max, n_bins + 1)
        bin_volumes = np.zeros(n_bins)

        for i in range(len(daily_data)):
            h, l, v = highs[i], lows[i], volumes[i]
            if v <= 0 or h <= l:
                continue
            # 将该日成交量均匀分配到覆盖的价格区间
            for j in range(n_bins):
                bin_low = bin_edges[j]
                bin_high = bin_edges[j + 1]
                # 计算重叠比例
                overlap_low = max(l, bin_low)
                overlap_high = min(h, bin_high)
                if overlap_high > overlap_low:
                    overlap_ratio = (overlap_high - overlap_low) / (h - l)
                    bin_volumes[j] += v * overlap_ratio

        # POC: 最大成交量区间
        poc_idx = int(np.argmax(bin_volumes))
        poc_lower = float(bin_edges[poc_idx])
        poc_upper = float(bin_edges[poc_idx + 1])
        poc_price = (poc_lower + poc_upper) / 2
        poc_vol_pct = float(bin_volumes[poc_idx] / np.sum(bin_volumes)) if np.sum(bin_volumes) > 0 else 0

        # ATR-14 计算
        atr_14 = self._calc_atr(daily_data, period=14)

        # 支撑位
        support = poc_lower - atr_14 * SUPPORT_ATR_FACTOR

        # 当前价距 POC 距离
        current_price = closes[-1]
        distance_pct = ((current_price - poc_price) / poc_price * 100) if poc_price > 0 else 0

        result = POCResult(
            symbol=symbol,
            poc_price=round(poc_price, 2),
            poc_lower=round(poc_lower, 2),
            poc_upper=round(poc_upper, 2),
            poc_volume_pct=round(poc_vol_pct * 100, 1),
            support=round(support, 2),
            atr_14=round(atr_14, 3),
            current_price=round(current_price, 2),
            distance_pct=round(distance_pct, 2),
        )

        logger.info(
            f"🐰 [Rabbit] POC: {symbol} | "
            f"POC:{poc_price:.2f} [{poc_lower:.2f}-{poc_upper:.2f}] "
            f"Support:{support:.2f} ATR:{atr_14:.3f} "
            f"Dist:{distance_pct:+.1f}%"
        )
        return result

    def _calc_atr(self, daily_data: List[dict], period: int = 14) -> float:
        """计算 ATR (Average True Range)"""
        if len(daily_data) < period + 1:
            return 0.0

        trs = []
        for i in range(1, len(daily_data)):
            h = float(daily_data[i].get("high", daily_data[i].get("close", 0)))
            l = float(daily_data[i].get("low", daily_data[i].get("close", 0)))
            prev_c = float(daily_data[i-1].get("close", 0))
            tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
            trs.append(tr)

        if len(trs) < period:
            return sum(trs) / len(trs) if trs else 0.0

        return sum(trs[-period:]) / period

    def generate_limit_orders(self, audit_results: List[dict],
                               poc_map: Dict[str, POCResult]) -> List[LimitOrder]:
        """
        生成次日限价单建议

        仅对 L4 APPROVE + POC 有效标的生成挂单
        挂单价 = max(support, poc_lower)
        """
        orders = []
        for result in audit_results:
            sym = result.get("symbol", "")
            verdict = str(result.get("l4_final_verdict", "") or "").strip().upper()
            # Bridge legacy APPROVE to current PASS contract.
            if verdict == "APPROVE":
                verdict = "PASS"

            if verdict != "PASS":
                continue

            poc = poc_map.get(sym)
            if not poc:
                continue

            # 挂单价: 取支撑位和 POC 下沿的较高值 (保守策略)
            order_price = max(poc.support, poc.poc_lower)
            discount = ((poc.current_price - order_price) / poc.current_price * 100
                        if poc.current_price > 0 else 0)

            # 折价太少 (<1%) 或太多 (>8%) 则跳过
            if discount < 1.0 or discount > 8.0:
                logger.info(f"🐰 [Rabbit] {sym}: 折价 {discount:.1f}% 不在 [1%-8%] 范围, 跳过")
                continue

            order = LimitOrder(
                symbol=sym,
                order_price=round(order_price, 2),
                current_price=poc.current_price,
                discount_pct=round(discount, 1),
                poc_price=poc.poc_price,
                reason=f"POC:{poc.poc_price:.2f} Support:{poc.support:.2f} ATR:{poc.atr_14:.3f}"
            )
            orders.append(order)
            logger.info(
                f"🐰 [Rabbit] 限价单: {sym} @ {order_price:.2f} "
                f"(折价 {discount:.1f}%, POC:{poc.poc_price:.2f})"
            )
            try:
                from protocol_observer import (
                    PROTOCOL_RABBIT,
                    TRIGGER_RABBIT_ANCHOR,
                    record_protocol_event,
                )
                record_protocol_event(
                    protocol=PROTOCOL_RABBIT,
                    symbol=sym,
                    trigger_type=TRIGGER_RABBIT_ANCHOR,
                    trigger_price=float(order.order_price or 0),
                    score=float(result.get('final_score') or result.get('audit_score') or 0),
                    verdict='ANCHOR',
                    trade_date=result.get('trade_date'),
                    is_trade_candidate=True,
                    execution_enabled=False,
                    evidence={
                        'current_price': order.current_price,
                        'discount_pct': order.discount_pct,
                        'poc_price': order.poc_price,
                        'poc_lower': poc.poc_lower,
                        'poc_upper': poc.poc_upper,
                        'support': poc.support,
                        'atr_14': poc.atr_14,
                        'reason': order.reason,
                        'observer_policy': 'side_channel_no_shadow_no_rag',
                    },
                )
            except Exception as exc:
                logger.warning(f"[Rabbit] protocol observer write failed: {sym} | {exc}")

        return orders


def _normalize_trade_date(value: Any = None) -> str:
    if value is None or str(value).strip() == "":
        return datetime.now().strftime("%Y-%m-%d")
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10]
    return text[:10]


def _latest_audit_trade_date() -> str:
    with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
        row = conn.execute(
            """
            SELECT MAX(trade_date)
            FROM nexus_audits
            WHERE COALESCE(l4_final_verdict, '') != ''
            """
        ).fetchone()
    return _normalize_trade_date(row[0]) if row and row[0] else _normalize_trade_date()


def _load_l4_pass_results(trade_date: str, limit: int) -> List[Dict[str, Any]]:
    with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
        rows = conn.execute(
            """
            WITH latest AS (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY symbol, trade_date
                           ORDER BY completed_at DESC NULLS LAST,
                                    created_at DESC NULLS LAST,
                                    task_id DESC
                       ) AS rn
                FROM nexus_audits
                WHERE CAST(trade_date AS DATE) = CAST(? AS DATE)
            )
            SELECT symbol,
                   COALESCE(name, '') AS name,
                   CAST(trade_date AS VARCHAR) AS trade_date,
                   COALESCE(l4_final_verdict, '') AS l4_final_verdict,
                   COALESCE(l4_final_score, final_score, l3_audit_score, 0) AS final_score
            FROM latest
            WHERE rn = 1
              AND UPPER(COALESCE(l4_final_verdict, '')) IN ('PASS', 'APPROVE')
            ORDER BY final_score DESC, symbol
            LIMIT ?
            """,
            [trade_date, int(limit)],
        ).fetchall()
    return [
        {
            "symbol": str(r[0] or "").strip().upper(),
            "name": str(r[1] or ""),
            "trade_date": _normalize_trade_date(r[2]),
            "l4_final_verdict": str(r[3] or ""),
            "final_score": float(r[4] or 0),
        }
        for r in rows
        if str(r[0] or "").strip()
    ]


def _load_daily_window(symbol: str, trade_date: str, lookback: int = POC_LOOKBACK_DAYS) -> List[Dict[str, Any]]:
    with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
        rows = conn.execute(
            """
            SELECT CAST(trade_date AS VARCHAR) AS trade_date,
                   COALESCE(close, 0) AS close,
                   COALESCE(high, close, 0) AS high,
                   COALESCE(low, close, 0) AS low,
                   COALESCE(vol, 0) AS volume
            FROM fact_daily
            WHERE symbol = ?
              AND CAST(trade_date AS DATE) <= CAST(? AS DATE)
              AND COALESCE(close, 0) > 0
            ORDER BY trade_date DESC
            LIMIT ?
            """,
            [symbol, trade_date, int(lookback)],
        ).fetchall()
    data = [
        {
            "trade_date": str(r[0] or ""),
            "close": float(r[1] or 0),
            "high": float(r[2] or 0),
            "low": float(r[3] or 0),
            "volume": float(r[4] or 0),
        }
        for r in rows
    ]
    data.reverse()
    return data


def run_anchor_observer(trade_date: Any = None, limit: int = 20) -> Dict[str, Any]:
    """
    Side-channel Static Anchor observer.

    It records protocol events only. It never places orders, writes Shadow pending
    signals, or writes RAG memory.
    """
    td = _normalize_trade_date(trade_date) if trade_date else _latest_audit_trade_date()
    stats: Dict[str, Any] = {
        "trade_date": td,
        "l4_pass": 0,
        "poc_ready": 0,
        "orders": 0,
        "poc_missing": 0,
        "errors": 0,
    }
    observer = RabbitAnchor()
    audit_results = _load_l4_pass_results(td, limit=limit)
    stats["l4_pass"] = len(audit_results)

    poc_map: Dict[str, POCResult] = {}
    for result in audit_results:
        sym = result.get("symbol", "")
        try:
            daily_data = _load_daily_window(sym, td)
            poc = observer.calc_poc(sym, daily_data)
            if poc:
                poc_map[sym] = poc
            else:
                stats["poc_missing"] += 1
        except Exception as exc:
            stats["errors"] += 1
            logger.warning("[Rabbit] anchor POC failed %s: %s", sym, exc)

    stats["poc_ready"] = len(poc_map)
    orders = observer.generate_limit_orders(audit_results, poc_map)
    stats["orders"] = len(orders)

    try:
        from protocol_observer import (
            PROTOCOL_RABBIT,
            TRIGGER_RABBIT_ANCHOR,
            record_protocol_scan,
        )
        status = "SCAN_TRIGGERED" if orders else ("SCAN_NO_CANDIDATE" if not audit_results else "SCAN_EMPTY")
        record_protocol_scan(
            protocol=PROTOCOL_RABBIT,
            trigger_type=TRIGGER_RABBIT_ANCHOR,
            scanned_count=len(audit_results),
            triggered_count=len(orders),
            status=status,
            trade_date=td,
            event_time=datetime.now().strftime("%H:%M:%S"),
            evidence={
                "lookback_days": POC_LOOKBACK_DAYS,
                "poc_ready": stats["poc_ready"],
                "poc_missing": stats["poc_missing"],
                "errors": stats["errors"],
                "discount_window": "1%-8%",
            },
        )
    except Exception as exc:
        logger.warning("[Rabbit] protocol scan summary write failed: %s", exc)

    logger.info("[Rabbit] anchor observer stats: %s", stats)
    return stats
