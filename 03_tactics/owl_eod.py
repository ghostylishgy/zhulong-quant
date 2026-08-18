#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
03_tactics/owl_eod.py
======================
🦉 猫头鹰抢跑协议 (EOD Momentum)
尾盘资金动能捕捉 — 14:30 启动, 绕过本地审计, 云端瞬时决策

时间窗口: 14:30-14:55 (留 5 分钟安全余量)
核心因子: 成交集中度 > 35%, 成交额强度 > 1.5x
"""

import os
import sys
import time
import logging
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import dataclass

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger('zhulong.owl')

# ═══════════════════════════════════════════════════════
# 配置
# ═══════════════════════════════════════════════════════

EOD_CONCENTRATION_THRESHOLD = 0.35  # 最后30min成交量占比 > 35%
EOD_STRENGTH_THRESHOLD = 1.5        # 当日成交额 / 5日均值 > 1.5
EOD_PCT_MIN = 1.0                   # 尾盘涨幅最低门槛 (%)

OWL_SYSTEM_PROMPT = (
    "你是尾盘动能审计员(Owl)。判断主力尾盘拿货的真实意图。\n"
    "MUST: Output ONLY strict JSON.\n"
    'Fields: verdict(EOD_BUY/PASS), score(0-100), reason(中文30字内)'
)


@dataclass
class EODCandidate:
    """尾盘候选标的"""
    symbol: str
    price: float
    pct_chg: float
    concentration: float   # 最后30min成交占比
    strength: float        # 成交额/5日均值
    eod_verdict: str = "PENDING"
    eod_score: int = 0


class OwlMonitor:
    """
    🦉 猫头鹰抢跑协议

    设计原则:
    - 14:30 触发后 **绕过本地 L2/L3** (耗时 ~8min, 窗口仅 30min)
    - 仅对今日 L1 Watchlist 已有标的生效 (安全护栏)
    - 直达云端 Qwen-Plus 瞬时决策 (< 5s)
    """

    def __init__(self):
        self._eod_results: Dict[str, EODCandidate] = {}

    def calc_eod_factors(self, symbol: str, intraday_data: dict) -> Optional[EODCandidate]:
        """
        计算尾盘动能因子

        Args:
            symbol: 标的代码
            intraday_data: {
                total_volume, total_amount,
                last_30min_volume,
                avg_amount_5d, price, pct_chg
            }
        """
        if not intraday_data.get("intraday_volume_window_available"):
            return None
        total_vol = intraday_data.get("total_volume", 0)
        last_30_vol = intraday_data.get("last_30min_volume", 0)
        total_amount = intraday_data.get("total_amount", 0)
        avg_amount_5d = intraday_data.get("avg_amount_5d", 0)
        pct_chg = intraday_data.get("pct_chg", 0)

        if total_vol <= 0 or avg_amount_5d <= 0:
            return None

        concentration = last_30_vol / total_vol
        strength = total_amount / avg_amount_5d

        if (concentration > EOD_CONCENTRATION_THRESHOLD
                and strength > EOD_STRENGTH_THRESHOLD
                and pct_chg > EOD_PCT_MIN):

            cand = EODCandidate(
                symbol=symbol,
                price=intraday_data.get("price", 0),
                pct_chg=pct_chg,
                concentration=concentration,
                strength=strength,
            )
            logger.info(
                f"🦉 [Owl] 尾盘触发: {symbol} "
                f"集中度:{concentration:.0%} 强度:{strength:.1f}x PCT:{pct_chg:+.1f}%"
            )
            return cand

        return None

    def cloud_eod_judge(self, cand: EODCandidate) -> EODCandidate:
        """
        云端尾盘判决 — 绕过本地, 直达 Qwen-Plus

        总耗时目标: < 5s
        """
        prompt = (
            f"SYM:{cand.symbol} CONC:{cand.concentration:.0%} "
            f"STR:{cand.strength:.1f}x PCT:{cand.pct_chg:+.1f}% P:{cand.price:.2f}\n"
            f"TASK: 判断主力尾盘拿货意图。是真拿货(EOD_BUY)还是尾盘诱多(PASS)?"
        )

        try:
            sys.path.append(os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "04_governance", "lib", "core"))
            from cloud_bridge import cloud_fast_call

            # 结构化降级数据 (API 失败时推送到指挥官手机)
            _fallback = {
                "symbol": cand.symbol,
                "pct_chg": cand.pct_chg,
                "concentration": cand.concentration,
                "eod_score": cand.eod_score,
                "trigger_reason": f"尾盘抢跑 集中度:{cand.concentration:.1f}%",
            }

            result = cloud_fast_call(
                prompt=prompt,
                system=OWL_SYSTEM_PROMPT,
                provider="qwen",       # Qwen-Plus 优先 (Speed tier)
                timeout=10,
                max_tokens=128,
                degrade_title=f"烛龙-盘后分析 | {cand.symbol}",
                fallback_data=_fallback,
            )

            if result and not result.get("_parse_fail"):
                raw_verdict = result.get("verdict", "PASS").upper()
                cand.eod_verdict = raw_verdict if raw_verdict in ("EOD_BUY", "PASS") else "PASS"
                cand.eod_score = int(result.get("score", 50))
                elapsed = result.get("_cloud_elapsed_s", 0)
                logger.info(
                    f"🦉 [Owl] 云端判决: {cand.symbol} → "
                    f"{cand.eod_verdict} ({cand.eod_score}分) [{elapsed}s]"
                )
            else:
                cand.eod_verdict = "PASS"
                cand.eod_score = 40
                logger.warning(f"🦉 [Owl] 云端解析失败: {cand.symbol} → 降级 PASS")

        except Exception as e:
            logger.error(f"🦉 [Owl] 云端异常: {cand.symbol} - {e}")
            cand.eod_verdict = "PASS"
            cand.eod_score = 30

        self._eod_results[cand.symbol] = cand
        try:
            from tactic_signal_bus import record_tactic_signal
            record_tactic_signal(
                source='OWL',
                symbol=cand.symbol,
                signal_type='EOD_MOMENTUM_BUY' if cand.eod_verdict == 'EOD_BUY' else 'EOD_MOMENTUM_OBSERVED',
                verdict=cand.eod_verdict,
                score=float(cand.eod_score or 0),
                price=float(cand.price or 0),
                reason=f'conc={cand.concentration:.2f} strength={cand.strength:.2f} pct={cand.pct_chg:.2f}',
                evidence={
                    'concentration': cand.concentration,
                    'strength': cand.strength,
                    'pct_chg': cand.pct_chg,
                },
                ttl_minutes=30,
            )
        except Exception as exc:
            logger.warning(f"[Owl] signal bus write failed: {cand.symbol} | {exc}")
        try:
            from protocol_observer import (
                PROTOCOL_OWL,
                TRIGGER_OWL_EOD,
                record_protocol_event,
            )
            record_protocol_event(
                protocol=PROTOCOL_OWL,
                symbol=cand.symbol,
                trigger_type=TRIGGER_OWL_EOD,
                trigger_price=float(cand.price or 0),
                score=float(cand.eod_score or 0),
                verdict=cand.eod_verdict,
                trade_date=datetime.now().strftime('%Y-%m-%d'),
                event_time=datetime.now().strftime('%H:%M:%S'),
                is_trade_candidate=cand.eod_verdict == 'EOD_BUY' and int(cand.eod_score or 0) >= 60,
                execution_enabled=False,
                evidence={
                    'concentration': cand.concentration,
                    'strength': cand.strength,
                    'pct_chg': cand.pct_chg,
                    'eod_verdict': cand.eod_verdict,
                    'observer_policy': 'side_channel_no_shadow_no_rag',
                },
            )
        except Exception as exc:
            logger.warning(f"[Owl] protocol observer write failed: {cand.symbol} | {exc}")
        return cand

    def start_eod_scan(self, watchlist: List[dict]):
        """
        尾盘扫描入口 (phase_intraday 14:30+ 调用)

        仅扫描 watchlist 中已有标的, 不接受陌生标的。
        """
        now = datetime.now()
        if now.hour != 14 or now.minute < 30 or now.minute > 55:
            return []

        logger.info(f"🦉 [Owl] 尾盘扫描启动 ({now:%H:%M})")

        has_minute_window = any(
            item.get("intraday_volume_window_available")
            and item.get("last_30min_volume") is not None
            for item in watchlist
        )
        if not has_minute_window:
            logger.warning("🦉 [Owl] 尾盘扫描禁用: 缺少真实最后30分钟成交量窗口")
            try:
                from protocol_observer import (
                    PROTOCOL_OWL,
                    TRIGGER_OWL_EOD,
                    record_protocol_scan,
                )
                record_protocol_scan(
                    protocol=PROTOCOL_OWL,
                    trigger_type=TRIGGER_OWL_EOD,
                    scanned_count=len(watchlist),
                    triggered_count=0,
                    status='SCAN_DISABLED_NO_MINUTE_BAR',
                    trade_date=now.strftime('%Y-%m-%d'),
                    event_time=now.strftime('%H:%M:%S'),
                    evidence={
                        'reason': 'missing_real_last_30min_volume',
                        'quote_source': 'tushare_realtime',
                        'observer_policy': 'side_channel_no_shadow_no_rag',
                        'eod_concentration_threshold': EOD_CONCENTRATION_THRESHOLD,
                        'eod_strength_threshold': EOD_STRENGTH_THRESHOLD,
                        'eod_pct_min': EOD_PCT_MIN,
                    },
                )
            except Exception as exc:
                logger.warning(f"[Owl] disabled scan summary write failed: {exc}")
            return []

        results = []
        for item in watchlist:
            sym = item.get("symbol", "")
            if sym in self._eod_results:
                continue  # 已判决, 不重复

            cand = self.calc_eod_factors(sym, item)
            if cand:
                cand = self.cloud_eod_judge(cand)
                results.append(cand)

        try:
            from protocol_observer import (
                PROTOCOL_OWL,
                TRIGGER_OWL_EOD,
                record_protocol_scan,
            )
            record_protocol_scan(
                protocol=PROTOCOL_OWL,
                trigger_type=TRIGGER_OWL_EOD,
                scanned_count=len(watchlist),
                triggered_count=len(results),
                status='SCAN_TRIGGERED' if results else 'SCAN_EMPTY',
                trade_date=now.strftime('%Y-%m-%d'),
                event_time=now.strftime('%H:%M:%S'),
                evidence={
                    'window': '14:30-14:55',
                    'eod_concentration_threshold': EOD_CONCENTRATION_THRESHOLD,
                    'eod_strength_threshold': EOD_STRENGTH_THRESHOLD,
                    'eod_pct_min': EOD_PCT_MIN,
                },
            )
        except Exception as exc:
            logger.warning(f"[Owl] protocol scan summary write failed: {exc}")

        if results:
            buy_count = sum(1 for r in results if r.eod_verdict == "EOD_BUY")
            logger.info(f"🦉 [Owl] 尾盘扫描完成: {len(results)} 触发, {buy_count} 买入信号")
        else:
            logger.info("🦉 [Owl] 尾盘扫描: 无触发")

        return results

    def get_results(self) -> Dict[str, EODCandidate]:
        return self._eod_results

    def reset(self):
        self._eod_results.clear()
