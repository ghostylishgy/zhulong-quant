#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
03_tactics/eagle_eye.py
========================
🦅 鹰眼破位协议 (Pulse Audit)
盘中真假突破两阶段审计 — 本地采集 + 云端快判

架构: N150 采集 (2s) → Cloud Fast Judge (10s) → 写入/推送 (1s)
时间窗口: 09:30-14:30, 每 10 分钟扫描
"""

import os
import sys
import time
import logging
import threading
from datetime import datetime
from typing import List, Dict, Optional, Callable
from dataclasses import dataclass, field

# 路径注入
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger('zhulong.eagle_eye')

# ═══════════════════════════════════════════════════════
# 配置
# ═══════════════════════════════════════════════════════

V_RATIO_THRESHOLD = 1.5       # 量比触发阈值
PCT_CHG_MIN = 2.0             # 最小涨幅 (%)
DECAY_VETO_RATIO = 0.5        # 回落超过涨幅50%即VETO
T3_DELAY_SEC = 180            # T+3min 复核延迟
T5_DELAY_SEC = 300            # T+5min 终判延迟 (T+3 后再等 120s)

EAGLE_SYSTEM_PROMPT = (
    "你是盘中破位审计员(Eagle Eye)。判断量价突破的真实性。\n"
    "MUST: Output ONLY strict JSON. No markdown.\n"
    'Fields: verdict(APPROVE/VETO), score(0-100), reason(中文30字内)'
)


@dataclass
class PulseCandidate:
    """脉冲候选标的"""
    symbol: str
    price: float
    pct_chg: float
    v_ratio: float
    amount: float
    trigger_time: str = ""
    quote_source: str = ""
    quote_date: str = ""
    quote_time: str = ""
    universe_source_date: str = ""
    historical_pct_chg: float = 0.0
    t3_status: str = "PENDING"   # PENDING → CONFIRM / SUSPECT
    t3_decay_rate: float = 0.0
    t5_verdict: str = "PENDING"  # PENDING → APPROVE / VETO
    t5_score: int = 0


class EagleEye:
    """
    🦅 鹰眼破位协议

    快慢分流机制:
    - FAST LANE: V_ratio > 1.5 的标的跳过本地 L2/L3，直达云端
    - SLOW LANE: 未触发的标的继续排队走盘后 L1→L4 全链路

    两阶段异步复核:
    - T+3min: threading.Timer 启动脉冲衰减检测
    - T+5min: 云端快判终决
    """

    def __init__(self, signal_writer: Optional[Callable[[PulseCandidate], None]] = None):
        self._active_timers: Dict[str, threading.Timer] = {}
        self._candidates: Dict[str, PulseCandidate] = {}
        self._signal_writer = signal_writer

    def scan_breakout(self, watchlist: List[dict]) -> List[PulseCandidate]:
        """
        Phase 1: 初筛突破标的 (在 phase_intraday 中调用)

        Args:
            watchlist: [{symbol, close, pct_chg, volume, avg_vol_5d, amount}, ...]

        Returns:
            触发量比阈值的候选列表
        """
        triggered = []
        now_str = datetime.now().strftime("%H:%M:%S")

        for item in watchlist:
            sym = item.get("symbol", "")
            avg_vol = item.get("avg_vol_5d", 0)
            if avg_vol <= 0:
                continue

            v_ratio = item.get("volume", 0) / avg_vol
            pct_chg = item.get("pct_chg", 0)

            if v_ratio > V_RATIO_THRESHOLD and pct_chg > PCT_CHG_MIN:
                # 去重: 同一标的 30 分钟内不重复触发
                if sym in self._candidates:
                    continue

                candidate = PulseCandidate(
                    symbol=sym,
                    price=item.get("close", 0),
                    pct_chg=pct_chg,
                    v_ratio=v_ratio,
                    amount=item.get("amount", 0),
                    trigger_time=now_str,
                    quote_source=item.get("quote_source", ""),
                    quote_date=item.get("quote_date", ""),
                    quote_time=item.get("quote_time", ""),
                    universe_source_date=item.get("universe_source_date", ""),
                    historical_pct_chg=item.get("historical_pct_chg", 0),
                )
                self._candidates[sym] = candidate
                triggered.append(candidate)
                logger.info(f"🦅 [EagleEye] 脉冲捕捉: {sym} V:{v_ratio:.2f} PCT:{pct_chg:+.1f}%")

        return triggered

    def schedule_verification(self, candidates: List[PulseCandidate]):
        """
        启动两阶段异步复核定时器

        T+3min → pulse_verify_t3()
        T+5min → cloud_fast_judge() (在 T+3 回调中调度)
        """
        for cand in candidates:
            sym = cand.symbol

            # T+3min 定时器
            timer = threading.Timer(T3_DELAY_SEC, self._t3_callback, args=(sym,))
            timer.daemon = True
            timer.name = f"Eagle_T3_{sym}"
            timer.start()
            self._active_timers[f"t3_{sym}"] = timer
            logger.info(f"🦅 [EagleEye] T+3min 定时器已启动: {sym}")

    def _t3_callback(self, symbol: str):
        """T+3min 回调: 脉冲衰减检测"""
        cand = self._candidates.get(symbol)
        if not cand:
            return

        logger.info(f"🦅 [EagleEye] T+3min 复核: {symbol}")

        try:
            # 重新抓取实时快照
            snapshot = self._fetch_realtime(symbol)
            if not snapshot:
                cand.t3_status = "SUSPECT"
                logger.warning(f"🦅 [EagleEye] T+3 快照失败: {symbol} → SUSPECT")
                return

            current_pct = snapshot.get("pct_chg", 0)
            decay_rate = 0
            if cand.pct_chg > 0:
                decay_rate = max(0, (cand.pct_chg - current_pct) / cand.pct_chg)

            cand.t3_decay_rate = decay_rate

            # 衰减检测: 回落超过涨幅 50% → VETO
            if decay_rate > DECAY_VETO_RATIO:
                cand.t3_status = "SUSPECT"
                cand.t5_verdict = "VETO"
                cand.t5_score = 10
                logger.warning(
                    f"🦅 [EagleEye] T+3 VETO: {symbol} 衰减 {decay_rate:.0%} "
                    f"(PCT: {cand.pct_chg:+.1f}% → {current_pct:+.1f}%)"
                )
                self._write_result(cand)
                return

            cand.t3_status = "CONFIRM"
            logger.info(f"🦅 [EagleEye] T+3 CONFIRM: {symbol} 衰减 {decay_rate:.0%}")

            # 启动 T+5min 终判 (T+3 后再等 120s)
            timer = threading.Timer(T5_DELAY_SEC - T3_DELAY_SEC, self._t5_callback, args=(symbol,))
            timer.daemon = True
            timer.name = f"Eagle_T5_{symbol}"
            timer.start()
            self._active_timers[f"t5_{symbol}"] = timer

        except Exception as e:
            logger.error(f"🦅 [EagleEye] T+3 异常: {symbol} - {e}")
            cand.t3_status = "SUSPECT"

    def _t5_callback(self, symbol: str):
        """T+5min 回调: 云端快判终决"""
        cand = self._candidates.get(symbol)
        if not cand or cand.t5_verdict != "PENDING":
            return

        logger.info(f"🦅 [EagleEye] T+5min 云端快判: {symbol}")

        try:
            # 构建极简 Prompt (< 200 tokens)
            prompt = (
                f"SYM:{symbol} V:{cand.v_ratio:.2f} PCT:{cand.pct_chg:+.1f}% "
                f"P:{cand.price:.2f} DECAY:{cand.t3_decay_rate:.2f} "
                f"T3:{'CONFIRM' if cand.t3_status == 'CONFIRM' else 'SUSPECT'}\n"
                f"TASK: 判断该盘中突破是真突破还是多头陷阱。"
            )

            # 调用云端
            try:
                sys.path.append(os.path.join(
                    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                    "04_governance", "lib", "core"))
                from cloud_bridge import cloud_fast_call
            except ImportError:
                logger.error("🦅 [EagleEye] cloud_bridge 导入失败")
                cand.t5_verdict = "HOLD"
                cand.t5_score = 50
                self._write_result(cand)
                return

            # 结构化降级数据 (API 失败时推送到指挥官手机)
            _fallback = {
                "symbol": symbol,
                "vol_ratio": cand.v_ratio,
                "pct_chg": cand.pct_chg,
                "price": cand.price,
                "trigger_reason": f"盘中脉冲 V:{cand.v_ratio:.2f} T3:{cand.t3_status}",
            }

            result = cloud_fast_call(
                prompt=prompt,
                system=EAGLE_SYSTEM_PROMPT,
                provider="deepseek",
                timeout=12,
                max_tokens=128,
                degrade_title=f"烛龙-实时异动 | {symbol}",
                fallback_data=_fallback,
            )

            if result and not result.get("_parse_fail"):
                cand.t5_verdict = result.get("verdict", "HOLD").upper()
                cand.t5_score = int(result.get("score", 50))
                elapsed = result.get("_cloud_elapsed_s", 0)
                logger.info(
                    f"🦅 [EagleEye] T+5 终判: {symbol} → "
                    f"{cand.t5_verdict} ({cand.t5_score}分) [{elapsed}s]"
                )
            else:
                cand.t5_verdict = "HOLD"
                cand.t5_score = 50
                logger.warning(f"🦅 [EagleEye] T+5 云端解析失败: {symbol} → 降级 HOLD")

            self._write_result(cand)

        except Exception as e:
            logger.error(f"🦅 [EagleEye] T+5 异常: {symbol} - {e}")
            cand.t5_verdict = "HOLD"
            cand.t5_score = 50

    def _fetch_realtime(self, symbol: str) -> Optional[dict]:
        """抓取实时行情快照 (Tushare realtime or DuckDB fallback)"""
        try:
            import tushare as ts
            df = ts.realtime_quote(ts_code=symbol)
            if df is not None and len(df) > 0:
                row = df.iloc[0]
                def _num(value, default=0.0):
                    try:
                        if value is None:
                            return default
                        text = str(value).strip()
                        if not text or text.lower() in ("nan", "none", "null", "--"):
                            return default
                        return float(text)
                    except Exception:
                        return default
                price = _num(row.get("PRICE", row.get("price", 0)))
                pre_close = _num(row.get("PRE_CLOSE", row.get("pre_close", 0)))
                pct_chg = _num(row.get("PCT_CHG", row.get("pct_chg", 0)))
                if pct_chg == 0 and price > 0 and pre_close > 0:
                    pct_chg = (price - pre_close) / pre_close * 100.0
                return {
                    "pct_chg": pct_chg,
                    "volume": _num(row.get("VOLUME", row.get("VOL", 0))) / 100.0,
                    "price": price,
                }
        except Exception as e:
            logger.warning(f"🦅 Tushare realtime failed: {e}")
        return None

    def _write_result(self, cand: PulseCandidate):
        """写入审计结果 (通过 safe_writer 或日志)"""
        logger.info(
            f"🦅 [EagleEye] RESULT: {cand.symbol} | "
            f"V:{cand.v_ratio:.2f} PCT:{cand.pct_chg:+.1f}% | "
            f"T3:{cand.t3_status} decay:{cand.t3_decay_rate:.0%} | "
            f"T5:{cand.t5_verdict} score:{cand.t5_score}"
        )

        if self._signal_writer is not None:
            try:
                self._signal_writer(cand)
                return
            except Exception as exc:
                logger.warning(f"[EagleEye] signal writer callback failed: {cand.symbol} | {exc}")

        try:
            from tactic_signal_bus import record_tactic_signal
            verdict = (cand.t5_verdict or 'PENDING').upper()
            signal_type = 'BREAKOUT_APPROVED' if verdict in ('APPROVE', 'BUY', 'PASS') and cand.t5_score >= 60 else 'BREAKOUT_OBSERVED'
            record_tactic_signal(
                source='EAGLE',
                symbol=cand.symbol,
                signal_type=signal_type,
                verdict=verdict,
                score=float(cand.t5_score or 0),
                price=float(cand.price or 0),
                reason=f'T3={cand.t3_status} decay={cand.t3_decay_rate:.2f} v_ratio={cand.v_ratio:.2f}',
                evidence={
                    'trigger_time': cand.trigger_time,
                    'pct_chg': cand.pct_chg,
                    'v_ratio': cand.v_ratio,
                    'amount': cand.amount,
                    'quote_source': cand.quote_source,
                    'quote_date': cand.quote_date,
                    'quote_time': cand.quote_time,
                    'universe_source_date': cand.universe_source_date,
                    'historical_pct_chg': cand.historical_pct_chg,
                    't3_status': cand.t3_status,
                    't3_decay_rate': cand.t3_decay_rate,
                },
                ttl_minutes=45,
            )
        except Exception as exc:
            logger.warning(f"[EagleEye] signal bus write failed: {cand.symbol} | {exc}")

        try:
            from protocol_observer import (
                PROTOCOL_EAGLE,
                TRIGGER_EAGLE_PULSE,
                record_protocol_event,
            )
            verdict = (cand.t5_verdict or 'PENDING').upper()
            score = float(cand.t5_score or 0)
            record_protocol_event(
                protocol=PROTOCOL_EAGLE,
                symbol=cand.symbol,
                trigger_type=TRIGGER_EAGLE_PULSE,
                trigger_price=float(cand.price or 0),
                score=score,
                verdict=verdict,
                trade_date=datetime.now().strftime('%Y-%m-%d'),
                event_time=cand.trigger_time,
                is_trade_candidate=verdict in ('APPROVE', 'BUY', 'PASS') and score >= 90,
                execution_enabled=False,
                evidence={
                    'trigger_time': cand.trigger_time,
                    'pct_chg': cand.pct_chg,
                    'v_ratio': cand.v_ratio,
                    'amount': cand.amount,
                    'quote_source': cand.quote_source,
                    'quote_date': cand.quote_date,
                    'quote_time': cand.quote_time,
                    'universe_source_date': cand.universe_source_date,
                    'historical_pct_chg': cand.historical_pct_chg,
                    't3_status': cand.t3_status,
                    't3_decay_rate': cand.t3_decay_rate,
                    't5_verdict': cand.t5_verdict,
                    'observer_policy': 'side_channel_no_shadow_no_rag',
                },
            )
        except Exception as exc:
            logger.warning(f"[EagleEye] protocol observer write failed: {cand.symbol} | {exc}")

    def scan_and_schedule(self, watchlist: List[dict]):
        """一键扫描 + 调度 (phase_intraday 入口)"""
        triggered = self.scan_breakout(watchlist)
        if triggered:
            logger.info(f"🦅 [EagleEye] {len(triggered)} 只脉冲触发，启动异步复核")
            self.schedule_verification(triggered)
        return triggered

    def cleanup(self):
        """清理所有活跃定时器"""
        for name, timer in self._active_timers.items():
            timer.cancel()
        self._active_timers.clear()
        self._candidates.clear()
