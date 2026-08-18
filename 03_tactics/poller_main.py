#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Zhulong Poller Tactical V3
Signal Adapter + ROE Gate + L4 Dual-Track Fuse + War Archivist
"""

import os
import runpy
import sys
import logging
from enum import Enum
from logging.handlers import RotatingFileHandler
import argparse
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

from db_contract import DB_PATH, DBGateway, detect_nexus_score_column

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BASE_DIR = PROJECT_ROOT

_loader_name = 'zhulong_core_module_loader'
if _loader_name in sys.modules:
    _module_loader = sys.modules[_loader_name]
else:
    _loader_path = BASE_DIR / '04_governance' / 'lib' / 'core' / 'module_loader.py'
    _loader_ns = runpy.run_path(str(_loader_path))

    class _ModuleLoaderShim:
        @staticmethod
        def load_module_from_path(module_name, path):
            return _loader_ns["load_module_from_path"](module_name, path)

        @staticmethod
        def load_attr_from_path(module_name, path, attr_name):
            return _loader_ns["load_attr_from_path"](module_name, path, attr_name)

    _module_loader = _ModuleLoaderShim()


def _load_internal_module(module_name: str, relative_path: str):
    return _module_loader.load_module_from_path(module_name, BASE_DIR / relative_path)


def _load_internal_attr(module_name: str, relative_path: str, attr_name: str):
    return _module_loader.load_attr_from_path(module_name, BASE_DIR / relative_path, attr_name)


# ==================== SMF Tide Sensor ====================
try:
    _tide_mod = _load_internal_module(
        'zhulong_tide_sensor',
        '04_governance/lib/tide_sensor.py',
    )
    get_tide_sensor = _tide_mod.get_sensor
    TIDE_AVAILABLE = True
except Exception:
    TIDE_AVAILABLE = False


# ==================== Path and env ====================
from config.settings import AUDIT_PASS, Config, get_audit_profile


from dotenv import load_dotenv

load_dotenv(BASE_DIR / '.env')

# Core components
def _load_core_components():
    try:
        from scripts.nexus import L4Triumvirate, Candidate, L3Result, Verdict, L2SentinelAuditor

        return L4Triumvirate, Candidate, L3Result, Verdict, L2SentinelAuditor
    except Exception as exc:
        logger.error("Non-fatal: scripts.nexus import failed, fallback to decision_engine: %s", exc, exc_info=True)

    try:
        core_mod = _load_internal_module(
            'zhulong_decision_engine',
            '02_brain/decision_engine.py',
        )

        missing = [
            name
            for name in ['L4Triumvirate', 'Candidate', 'L3Result', 'Verdict', 'L2SentinelAuditor']
            if not hasattr(core_mod, name)
        ]
        if missing:
            raise ImportError(f'Core module missing symbols: {missing}')

        return (
            core_mod.L4Triumvirate,
            core_mod.Candidate,
            core_mod.L3Result,
            core_mod.Verdict,
            core_mod.L2SentinelAuditor,
        )
    except Exception as exc:
        raise ImportError(
            'Cannot initialize tactical core components. '
            'Tried scripts.nexus and 02_brain/decision_engine.py.'
        ) from exc


L4Triumvirate, Candidate, L3Result, Verdict, L2SentinelAuditor = _load_core_components()


def _load_force_liquidator():
    try:
        fn = _load_internal_attr(
            'zhulong_shadow_engine_force',
            '05_shadow/lib/engine.py',
            'force_liquidate_positions',
        )
        if not callable(fn):
            raise RuntimeError('force_liquidate_positions not found in shadow engine')
        return fn
    except Exception as exc:
        logger.warning(f'force liquidator unavailable: {exc}')
        return None


class RecommendationTier(Enum):
    PASS = 'PASS'
    WATCH = 'WATCH'
    VETO = 'VETO'


# Logging
LOG_DIR = Path(Config.LOG_DIR)
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | [%(name)s] %(message)s',
    datefmt='%H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        RotatingFileHandler(str(LOG_DIR / 'poller_tactical.log'), maxBytes=20 * 1024 * 1024, backupCount=5, encoding='utf-8'),
    ],
)
logger = logging.getLogger('tactical_v3')


import math as _math


def safe_num(x, default=0.0):
    if x is None:
        return default
    try:
        f = float(x)
        if _math.isnan(f) or _math.isinf(f):
            return default
        return f
    except (TypeError, ValueError):
        return default


def _pass_verdict_member():
    return getattr(Verdict, AUDIT_PASS, getattr(Verdict, 'PASS', getattr(Verdict, 'HOLD')))


def _verdict_text(v):
    return str(getattr(v, 'value', v) or '').upper()


ACTIVE_AUDIT_PROFILE = get_audit_profile() if callable(get_audit_profile) else None
SIGNAL_VERDICT_PASS = AUDIT_PASS
DEFAULT_MIN_FINAL_SCORE_PASS = int(
    getattr(
        ACTIVE_AUDIT_PROFILE,
        'tactics_min_final_score_pass',
        getattr(Config, 'TACTICS_MIN_FINAL_SCORE_PASS', os.getenv('TACTICS_MIN_FINAL_SCORE_PASS', os.getenv('TACTICS_MIN_FINAL_SCORE', '70'))),
    )
)
DEFAULT_MIN_FINAL_SCORE_WATCH = int(
    getattr(
        ACTIVE_AUDIT_PROFILE,
        'tactics_min_final_score_watch',
        getattr(Config, 'TACTICS_MIN_FINAL_SCORE_WATCH', os.getenv('TACTICS_MIN_FINAL_SCORE_WATCH', '52')),
    )
)

# Hard stop loss
HARD_STOP_LOSS = -0.08


def enforce_hard_stop_loss(symbol, pct_chg, ai_verdict):
    pct = safe_num(pct_chg, 0.0)
    if pct <= HARD_STOP_LOSS:
        logger.critical(
            f'HARD_STOP_LOSS triggered: {symbol} pnl {pct*100:.1f}% <= {HARD_STOP_LOSS*100:.0f}% | '
            f'AI={ai_verdict} -> FORCE_SELL'
        )
        return 'FORCE_SELL', True
    return ai_verdict, False


class EmbeddedROEAuditor:
    def __init__(self):
        self.min_roe_avg = 0.10

    def audit_stock(self, code: str, name: str) -> Any:
        try:
            try:
                from data_engine.fetcher import Fetcher

                fetcher = Fetcher()
                df = fetcher.fetch_roe_data(code)
                if df is not None and not df.empty:
                    roe_vals = []
                    if '??' in df.columns:
                        roe_vals = df['??'].astype(float).tolist()
                    elif 'ROE' in df.columns:
                        roe_vals = df['ROE'].astype(float).tolist()

                    if roe_vals:
                        avg_roe = sum(roe_vals[:3]) / len(roe_vals[:3])
                        if avg_roe > 1.0:
                            avg_roe /= 100.0

                        class Result:
                            pass

                        result = Result()
                        result.roe_avg = avg_roe
                        result.roe_pass = avg_roe >= self.min_roe_avg
                        return result
            except Exception as e:
                logger.error(f'Critical Logical Gap: {str(e)}', exc_info=True)
            return None
        except Exception as e:
            logger.error(f'ROE audit failed: {e}')
            return None


class TacticalCommander:
    def __init__(self, replay_date: str = None):
        self.mode = 'REPLAY' if replay_date else 'LIVE'
        self.target_date = replay_date or datetime.now().strftime('%Y-%m-%d')
        self.db_path = str(DB_PATH)
        self.signal_verdict = SIGNAL_VERDICT_PASS
        self.min_final_score_pass = DEFAULT_MIN_FINAL_SCORE_PASS
        self.min_final_score_watch = min(DEFAULT_MIN_FINAL_SCORE_WATCH, self.min_final_score_pass)

        self.roe_auditor = EmbeddedROEAuditor()
        self.l4 = L4Triumvirate()
        self.l2 = L2SentinelAuditor()
        self._force_liquidate_positions = _load_force_liquidator()
        self.channel_messages: Dict[str, List[str]] = {
            RecommendationTier.PASS.value: [],
            RecommendationTier.WATCH.value: [],
        }
        self._ensure_poller_consumed_column()

        logger.info(
            f'TacticalCommander initialized | mode={self.mode} | date={self.target_date} '
            f'| gate PASS>={self.min_final_score_pass}, WATCH>={self.min_final_score_watch}'
        )

    def _ensure_poller_consumed_column(self) -> None:
        try:
            with DBGateway(self.db_path, read_only=False, logger=logger) as conn:
                cols = {
                    str(r[1]).lower()
                    for r in conn.execute("PRAGMA table_info('nexus_audits')").fetchall()
                }
                if '_poller_consumed_at' not in cols:
                    conn.execute("ALTER TABLE nexus_audits ADD COLUMN _poller_consumed_at TIMESTAMP")
                    logger.info('[SignalAdapter] added nexus_audits._poller_consumed_at')
        except Exception as exc:
            logger.warning(f'[SignalAdapter] ensure consumed column degraded: {exc}', exc_info=True)

    def _mark_poller_consumed(self, task_ids: List[str]) -> None:
        unique_ids = [tid for tid in dict.fromkeys([str(t or '').strip() for t in task_ids]) if tid]
        if not unique_ids:
            return
        placeholders = ', '.join(['?' for _ in unique_ids])
        try:
            with DBGateway(self.db_path, read_only=False, logger=logger) as conn:
                conn.execute(
                    f"""
                    UPDATE nexus_audits
                    SET _poller_consumed_at = NOW()
                    WHERE task_id IN ({placeholders})
                    """,
                    unique_ids,
                )
            logger.info(f'[SignalAdapter] marked consumed rows: {len(unique_ids)}')
        except Exception as exc:
            logger.error(f'[SignalAdapter] mark consumed failed: {exc}', exc_info=True)

    def _map_recommendation_tier(
        self,
        final_verdict: str,
        final_score: float,
        base_reason: str = '',
    ) -> Tuple[RecommendationTier, str]:
        verdict = str(final_verdict or '').strip().upper()
        if verdict == 'APPROVE':
            verdict = AUDIT_PASS
        score = safe_num(final_score, 0.0)
        reason = (base_reason or '').strip()

        if verdict == AUDIT_PASS and score >= self.min_final_score_pass:
            return RecommendationTier.PASS, reason

        if verdict in {'HOLD', 'WATCH'} and score >= self.min_final_score_watch:
            if not reason:
                reason = 'L4_HOLD'
            return RecommendationTier.WATCH, reason

        if verdict == AUDIT_PASS and score >= self.min_final_score_watch:
            reason = reason or f'SCORE_DOWNGRADE({int(score)}<{self.min_final_score_pass})'
            return RecommendationTier.WATCH, reason

        return RecommendationTier.VETO, reason

    def _build_push_template(
        self,
        tier: RecommendationTier,
        symbol: str,
        name: str,
        corrected_score: int,
        l2_tags: str,
        watch_reason: str,
    ) -> str:
        display = f'{name} {symbol}' if name and name != symbol else symbol
        tags = l2_tags or 'NORMAL'
        if tier == RecommendationTier.PASS:
            return (
                f'[\u63a8\u9001] {display} | L4\u8bc4\u5206={corrected_score} | '
                f'\u6807\u7b7e={tags} | \u4ea4\u6613\u65e5={self.target_date}'
            )

        return (
            f'[\u89c2\u5bdf] {display} | L4\u8bc4\u5206={corrected_score} | '
            f'\u964d\u7ea7\u539f\u56e0={watch_reason or "L4_HOLD"} | '
            f'\u6807\u7b7e={tags} | \u4ea4\u6613\u65e5={self.target_date}'
        )

    def radar_scan(self) -> pd.DataFrame:
        """
        Signal adapter:
        - consume nexus_audits only
        - PASS -> formal channel
        - HOLD/WATCH -> watch channel
        """
        logger.info(
            f'[SignalAdapter] scan {self.target_date} | '
            f'PASS>={self.min_final_score_pass}, WATCH>={self.min_final_score_watch}'
        )

        with DBGateway(self.db_path, read_only=False, logger=logger) as conn:
            score_col = detect_nexus_score_column(conn)
            if not score_col:
                logger.error('[SignalAdapter] missing score column in nexus_audits, execution blocked')
                return pd.DataFrame()

            query = f"""
            WITH audited AS (
                SELECT
                    task_id,
                    symbol,
                    CAST(trade_date AS DATE) AS trade_date,
                    COALESCE({score_col}, 0) AS final_score,
                    UPPER(COALESCE(l4_final_verdict, '')) AS l4_final_verdict,
                    COALESCE(NULLIF(l4_veto_reason, ''), '') AS l4_veto_reason,
                    COALESCE(l2_pattern, '') AS l2_pattern,
                    COALESCE(l2_risk_score, 0) AS l2_risk_score,
                    created_at,
                    CASE
                        WHEN UPPER(COALESCE(l4_final_verdict, '')) IN (?, 'APPROVE', 'HOLD', 'WATCH') THEN 0
                        ELSE 1
                    END AS verdict_rank,
                    ROW_NUMBER() OVER (
                        PARTITION BY symbol
                        ORDER BY
                            CASE
                                WHEN UPPER(COALESCE(l4_final_verdict, '')) IN (?, 'APPROVE', 'HOLD', 'WATCH') THEN 0
                                ELSE 1
                            END ASC,
                            created_at DESC,
                            task_id DESC
                    ) AS rn
                FROM nexus_audits
                WHERE CAST(trade_date AS DATE) = CAST(? AS DATE)
                  AND COALESCE({score_col}, 0) >= ?
                  AND _poller_consumed_at IS NULL
            )
            SELECT
                a.task_id,
                a.symbol,
                COALESCE(b.name, a.symbol) AS name,
                d.pct_chg,
                d.vol AS volume,
                d.close,
                a.final_score,
                a.l4_final_verdict,
                a.l4_veto_reason,
                a.l2_pattern,
                a.l2_risk_score
            FROM audited a
            JOIN fact_daily d
              ON a.symbol = d.symbol
             AND a.trade_date = d.trade_date
            LEFT JOIN fact_stock_basic b
              ON a.symbol = b.symbol
            WHERE a.rn = 1
              AND a.verdict_rank = 0
            ORDER BY a.final_score DESC, d.pct_chg DESC
            LIMIT 120
            """
            df = conn.execute(
                query,
                [self.signal_verdict, self.signal_verdict, self.target_date, self.min_final_score_watch],
            ).df()

            if df.empty:
                logger.warning('[SignalAdapter] no candidate above watch threshold')
                return pd.DataFrame()

            pass_items: List[Dict[str, Any]] = []
            watch_items: List[Dict[str, Any]] = []

            for _, row in df.iterrows():
                sym = row['symbol']
                avg_vol = conn.execute(
                    """
                    SELECT AVG(vol) FROM (
                        SELECT vol
                        FROM fact_daily
                        WHERE symbol = ?
                          AND trade_date < CAST(? AS DATE)
                        ORDER BY trade_date DESC
                        LIMIT 5
                    ) t
                    """,
                    [sym, self.target_date],
                ).fetchone()[0]

                v_ratio = row['volume'] / avg_vol if avg_vol and avg_vol > 0 else 1.0
                tier, watch_reason = self._map_recommendation_tier(
                    row.get('l4_final_verdict', ''),
                    row.get('final_score', 0),
                    row.get('l4_veto_reason', ''),
                )
                if tier == RecommendationTier.VETO:
                    continue

                vol_gate_ok = v_ratio > 1.2 or self.mode == 'REPLAY'
                if tier == RecommendationTier.PASS and not vol_gate_ok:
                    continue

                d = row.to_dict()
                d['vol_ratio'] = v_ratio
                d['rps_10'] = float(row.get('final_score', 0))
                d['recommendation_tier'] = tier.value
                if tier == RecommendationTier.WATCH and not vol_gate_ok:
                    vr = f'VOL_RATIO_LOW({v_ratio:.2f})'
                    watch_reason = f'{watch_reason}; {vr}' if watch_reason else vr
                d['watch_reason'] = watch_reason

                if tier == RecommendationTier.PASS:
                    pass_items.append(d)
                else:
                    watch_items.append(d)

        res_df = pd.DataFrame(pass_items + watch_items)
        logger.info(
            f"[SignalAdapter] accepted: pass={len(pass_items)} watch={len(watch_items)} total={len(res_df)}"
        )
        return res_df

    def run_financial_gate(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        logger.info('[Gate] start 10% ROE filter')

        passed = []
        for _, row in df.iterrows():
            code = row['symbol'].split('.')[0]
            tier = str(row.get('recommendation_tier', RecommendationTier.WATCH.value)).upper()
            r = self.roe_auditor.audit_stock(code, row['name'])
            if r is None or r.roe_pass:
                d = row.to_dict()
                d['roe_avg'] = getattr(r, 'roe_avg', 0.15)
                passed.append(d)
                logger.info(f"  pass {row['symbol']}")
                continue

            if tier == RecommendationTier.WATCH.value:
                d = row.to_dict()
                d['roe_avg'] = getattr(r, 'roe_avg', 0.0)
                wr = str(d.get('watch_reason') or '')
                d['watch_reason'] = f'{wr}; ROE_WEAK' if wr else 'ROE_WEAK'
                passed.append(d)
                logger.info(f"  watch-keep {row['symbol']} (roe weak)")
            else:
                logger.warning(f"  reject {row['symbol']} by ROE gate")

        return pd.DataFrame(passed)

    def run_fusion(self, df: pd.DataFrame):
        if df.empty:
            return
        logger.info('[Fuse] start L4 court')

        pass_msgs: List[str] = []
        watch_msgs: List[str] = []
        consumed_task_ids: List[str] = []

        for _, row in df.iterrows():
            symbol = row['symbol']
            task_id = str(row.get('task_id') or '').strip()
            l15_score = int(safe_num(row.get('final_score'), 0))
            l15_score = min(100, max(40, l15_score))

            candidate = Candidate(
                symbol=symbol,
                name=row['name'],
                trade_date=self.target_date,
                close=row['close'],
                pct_chg=row['pct_chg'],
                volume=row['volume'],
                rps_10=float(row.get('rps_10', l15_score)),
                vol_ratio=float(row['vol_ratio']),
            )

            l2_res = self.l2.audit(candidate)
            l2_tags = ', '.join(l2_res.fact_tags) if l2_res.fact_tags else 'NORMAL'
            header = (
                "### Header\n"
                f"- SignalAdapter FinalScore: {l15_score}\n"
                f"- L2 Tags: [{l2_tags}]\n"
                f"- ROE: {row['roe_avg']:.2%}\n"
                f"- Time: {datetime.now().strftime('%H:%M:%S')}"
            )

            l3_dummy = L3Result(symbol=symbol, verdict=_pass_verdict_member(), audit_score=l15_score)

            logger.info(f'  audit {symbol}')
            l4_res = self.l4.audit(candidate, l3_dummy, is_top3=True, rag_intel=header)

            corrected_score = int(l4_res.final_score * getattr(self, '_tide_bias', 1.0))
            tier, watch_reason = self._map_recommendation_tier(
                _verdict_text(l4_res.final_verdict),
                corrected_score,
                str(row.get('watch_reason', '') or l4_res.veto_reason or ''),
            )
            if tier == RecommendationTier.VETO:
                logger.info(
                    f"  -> VETO | raw={l4_res.final_score} | corrected={corrected_score} "
                    f"| veto={l4_res.veto_reason or 'No'}"
                )
                if task_id:
                    consumed_task_ids.append(task_id)
                continue

            message = self._build_push_template(
                tier=tier,
                symbol=symbol,
                name=row['name'],
                corrected_score=corrected_score,
                l2_tags=l2_tags,
                watch_reason=watch_reason,
            )
            if tier == RecommendationTier.PASS:
                pass_msgs.append(message)
            else:
                watch_msgs.append(message)

            logger.info(
                f"  -> {tier.value} | raw={l4_res.final_score} | corrected={corrected_score} "
                f"| reason={watch_reason or 'N/A'}"
            )
            if task_id:
                consumed_task_ids.append(task_id)

        self._mark_poller_consumed(consumed_task_ids)
        self.channel_messages[RecommendationTier.PASS.value] = pass_msgs
        self.channel_messages[RecommendationTier.WATCH.value] = watch_msgs

        for msg in pass_msgs:
            logger.info(f'[PUSH|PASS] {msg}')
        for msg in watch_msgs:
            logger.info(f'[PUSH|WATCH] {msg}')


    def _trigger_force_liquidation(self, trigger_reason: str) -> Dict[str, Any]:
        if not callable(self._force_liquidate_positions):
            return {'status': 'UNAVAILABLE'}

        run_id = f'{self.target_date}_{datetime.now().strftime("%H%M%S")}_P0'
        try:
            result = self._force_liquidate_positions(run_id=run_id, trigger_reason=trigger_reason)
            if isinstance(result, dict):
                result.setdefault('run_id', run_id)
                result.setdefault('trigger_reason', trigger_reason)
                return result
            return {'status': 'NON_DICT_RESULT', 'run_id': run_id}
        except Exception as exc:
            logger.exception(f'force liquidation execution failed: {exc}')
            return {'status': 'ERROR', 'run_id': run_id, 'error': str(exc)}

    def adjust_probe_quota(self, blind_spot_score: float = 0.0):
        if not hasattr(self, '_default_l1_top_n'):
            self._default_l1_top_n = getattr(self, 'L1_TOP_N', 30)

        if blind_spot_score > 0.7:
            expansion = 5
            logger.info(f'PROBE EXPAND: blind_spot={blind_spot_score:.2f} L1_TOP_N +{expansion}')
        elif blind_spot_score > 0.4:
            expansion = 2
            logger.info(f'PROBE ADJUST: blind_spot={blind_spot_score:.2f} L1_TOP_N +{expansion}')
        else:
            expansion = 0

        return self._default_l1_top_n + min(expansion, 10)

    def run(self):
        if TIDE_AVAILABLE:
            try:
                tide = get_tide_sensor()
                tide_state = tide.get_risk_gate(self.target_date)
                logger.info(
                    f"TIDE [{tide_state.trade_date}] gate={tide_state.risk_gate} "
                    f"bias={tide_state.style_bias:.4f} s={tide_state.ma20_ratio:.3f}"
                )
                risk_gate_text = str(getattr(tide_state, 'risk_gate', '') or '').upper()
                suspend_all_buy = risk_gate_text == 'SUSPEND_ALL_BUY'
                if tide_state.is_meltdown or suspend_all_buy:
                    if self.mode == 'REPLAY':
                        logger.warning(f'TIDE MELTDOWN in replay mode: {tide_state.status} - continue for audit validation')
                    else:
                        trigger_reason = 'SUSPEND_ALL_BUY' if suspend_all_buy else 'TIDE_MELTDOWN'
                        liquidation = self._trigger_force_liquidation(trigger_reason)
                        logger.critical(
                            f'TIDE MELTDOWN: {tide_state.status} - ABORT ALL '
                            f'| forced_liquidation={liquidation}'
                        )
                        return
                self._tide_bias = tide_state.style_bias
            except Exception as e:
                logger.warning(f'Tide check failed: {e}')
                self._tide_bias = 1.0
        else:
            self._tide_bias = 1.0

        df_radar = self.radar_scan()
        df_gate = self.run_financial_gate(df_radar)
        self.run_fusion(df_gate)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--replay', type=str)
    args = parser.parse_args()

    commander = TacticalCommander(replay_date=args.replay)
    commander.run()
