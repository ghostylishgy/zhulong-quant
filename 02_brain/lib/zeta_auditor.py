#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
烛龙 Nexus v2.4.0 - Zeta Auditor (DIV_TRAP 检测)
L3 审计后 L4 决策前的筹码背离检测器
含独立日志 logs/pre_festival_ignition.log
"""

import logging
import math
from logging.handlers import RotatingFileHandler
from datetime import date, datetime
from typing import List, Dict, Optional
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from config.settings import Config
from zeta.zeta_collector_v240 import ZetaData

# ==================== 独立日志 ====================

LOG_DIR = Path(str(Config.LOG_DIR))
LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("zhulong.zeta_auditor")

veto_logger = logging.getLogger("zhulong.zeta_veto")
_veto_handler = RotatingFileHandler(
    str(LOG_DIR / "pre_festival_ignition.log"),
    maxBytes=20*1024*1024,
    backupCount=5,
    encoding="utf-8"
)
_veto_handler.setFormatter(logging.Formatter(
    "%(asctime)s | %(levelname)s | [ZETA_VETO] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
))
_veto_handler.setLevel(logging.INFO)
if not veto_logger.handlers:
    veto_logger.addHandler(_veto_handler)
    veto_logger.setLevel(logging.INFO)


# ==================== 枚举 ====================

class GameSignal(Enum):
    BUY_PRESSURE = "BUY_PRESSURE"
    SELL_PRESSURE = "SELL_PRESSURE"
    NEUTRAL = "NEUTRAL"
    DIV_TRAP = "DIV_TRAP"
    EXIT = "EXIT"


# ==================== 数据结构 ====================

@dataclass
class ZetaAuditResult:
    ts_code: str
    zeta_score: float = 5.0
    game_signal: GameSignal = GameSignal.NEUTRAL
    is_div_trap: bool = False
    force_l4_veto: bool = False
    inst_flow: str = "UNKNOWN"
    hot_money_flow: str = "UNKNOWN"
    margin_trend: str = "UNKNOWN"
    reason: str = ""
    lhb_score: float = 5.0
    margin_score: float = 5.0
    block_trade_premium: float = 0.0
    shadow_boost: float = 0.0
    shadow_score: float = 0.0
    shadow_tag: str = "#SHADOW_BLOCK_OBSERVE"


# ==================== 审计器 ====================

class ZetaAuditor:
    """DIV_TRAP 检测: divergence = logic_norm - zeta_score"""

    DIV_THRESHOLD = 3.0
    EXIT_THRESHOLD = 5.0
    LHB_STRONG_DIRECTION_YUAN = 50_000_000
    MARGIN_STRONG_DIRECTION_YUAN = 10_000_000

    def audit(self, zeta_data, logic_score, gamma_score=0.0):
        r = ZetaAuditResult(ts_code=zeta_data.ts_code)
        r.lhb_score = self._calc_lhb_score(zeta_data)
        r.margin_score = self._calc_margin_score(zeta_data)
        r.zeta_score = round(0.6 * r.lhb_score + 0.4 * r.margin_score, 2)

        r.block_trade_premium = self._safe_float(getattr(zeta_data, "block_trade_premium", 0.0))
        r.shadow_boost = self._calc_shadow_boost(r.block_trade_premium)
        r.shadow_score = r.zeta_score + r.shadow_boost
        veto_logger.info(
            f"SHADOW_BLOCK_OBSERVE | {zeta_data.ts_code} | {zeta_data.trade_date} | "
            f"vwap_premium={r.block_trade_premium:.6f} boost={r.shadow_boost:.6f} "
            f"shadow_score={r.shadow_score:.6f} tag={r.shadow_tag}"
        )

        if zeta_data.lhb_net > 0:
            r.inst_flow = "INFLOW" if zeta_data.inst_buy > 0 else "NEUTRAL"
            r.hot_money_flow = "INFLOW" if zeta_data.hot_money > 0 else "NEUTRAL"
        elif zeta_data.lhb_net < 0:
            r.inst_flow = "OUTFLOW"
            r.hot_money_flow = "OUTFLOW" if zeta_data.hot_money == 0 else "MIXED"

        if zeta_data.margin_delta > self.MARGIN_STRONG_DIRECTION_YUAN:
            r.margin_trend = "SURGE"
        elif zeta_data.margin_delta > 0:
            r.margin_trend = "INFLOW"
        elif zeta_data.margin_delta < -self.MARGIN_STRONG_DIRECTION_YUAN:
            r.margin_trend = "SQUEEZE"
        elif zeta_data.margin_delta < 0:
            r.margin_trend = "OUTFLOW"

        logic_norm = logic_score / 10.0
        divergence = logic_norm - r.zeta_score

        if divergence > self.EXIT_THRESHOLD:
            r.game_signal = GameSignal.EXIT
            r.is_div_trap = True
            r.force_l4_veto = True
            r.reason = (
                f"LOGIC_FALSIFIED: score={logic_score} vs zeta={r.zeta_score:.1f} "
                f"(div={divergence:.1f}>EXIT={self.EXIT_THRESHOLD}), "
                f"LHB_net={zeta_data.lhb_net:.0f}"
            )
            veto_logger.info(
                f"EXIT | {zeta_data.ts_code} | {zeta_data.trade_date} | "
                f"logic={logic_score} zeta={r.zeta_score:.1f} "
                f"div={divergence:.1f} lhb={zeta_data.lhb_net:.0f} "
                f"margin_d={zeta_data.margin_delta:.0f}"
            )
        elif divergence > self.DIV_THRESHOLD:
            r.game_signal = GameSignal.DIV_TRAP
            r.is_div_trap = True
            r.force_l4_veto = True
            r.reason = (
                f"DIV_TRAP: score={logic_score} vs zeta={r.zeta_score:.1f} "
                f"(div={divergence:.1f}>DIV={self.DIV_THRESHOLD}), "
                f"margin={r.margin_trend}"
            )
            veto_logger.info(
                f"DIV_TRAP | {zeta_data.ts_code} | {zeta_data.trade_date} | "
                f"logic={logic_score} zeta={r.zeta_score:.1f} "
                f"div={divergence:.1f} lhb={zeta_data.lhb_net:.0f} "
                f"margin={r.margin_trend}"
            )
        elif zeta_data.lhb_net > self.LHB_STRONG_DIRECTION_YUAN:
            r.game_signal = GameSignal.BUY_PRESSURE
        elif zeta_data.lhb_net < -self.LHB_STRONG_DIRECTION_YUAN:
            r.game_signal = GameSignal.SELL_PRESSURE

        return r

    def _calc_lhb_score(self, data):
        score = 5.0
        if data.lhb_net > self.LHB_STRONG_DIRECTION_YUAN: score += 3
        elif data.lhb_net > 0: score += 1
        elif data.lhb_net < -self.LHB_STRONG_DIRECTION_YUAN: score -= 3
        elif data.lhb_net < 0: score -= 1
        if data.inst_buy > 0: score += 1
        if data.hot_money > 2: score += 0.5
        return max(0.0, min(10.0, score))

    def _calc_margin_score(self, data):
        score = 5.0
        if data.margin_delta > self.MARGIN_STRONG_DIRECTION_YUAN: score += 2
        elif data.margin_delta > 0: score += 1
        elif data.margin_delta < -self.MARGIN_STRONG_DIRECTION_YUAN: score -= 2
        elif data.margin_delta < 0: score -= 1
        return max(0.0, min(10.0, score))

    @staticmethod
    def _safe_float(value) -> float:
        try:
            return float(value or 0.0)
        except Exception:
            return 0.0

    @staticmethod
    def _calc_shadow_boost(vwap_premium: float) -> float:
        x = 200.0 * (float(vwap_premium) - 0.03)
        try:
            return 0.1 / (1.0 + math.exp(-x))
        except OverflowError:
            return 0.0 if x < 0 else 0.1
