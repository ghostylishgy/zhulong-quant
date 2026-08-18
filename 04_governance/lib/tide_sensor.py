#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
04_governance/lib/tide_sensor.py
SMF Tide Sensor - L0 Macro Filter

Core:
    T_risk: MA20 breadth -> FORCE_NO_EDGE / CAUTION / AGGRESSIVE
    T_style: 1 + 0.2 * tanh(s - 0.5) -> [0.8, 1.2]
    Persistence: -> historical_factor_segments.d4_tide_state / d6_ma_bias
"""

import math
import json
import logging
import sys
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, List

logger = logging.getLogger('zhulong.tide_sensor')

# ==================== Root Detection ====================

_current = Path(__file__).resolve()
PROJECT_ROOT = next(
    (p for p in _current.parents if (p / '.git').exists() or (p / 'storage').exists()),
    _current.parents[2],
)
DB_PATH = str(PROJECT_ROOT / 'storage' / 'database' / 'zhulong.duckdb')
INDUSTRY_CONFIG = PROJECT_ROOT / '04_governance' / 'config' / 'industry_relation.json'

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))
_CORE_DIR = PROJECT_ROOT / '04_governance' / 'lib' / 'core'
if str(_CORE_DIR) not in sys.path:
    sys.path.append(str(_CORE_DIR))
from module_loader import load_attr_from_path

DBGateway = load_attr_from_path(
    'db_gateway_01',
    PROJECT_ROOT / '01_engine' / 'lib' / 'db_gateway.py',
    'DBGateway',
)
resolve_trade_date = load_attr_from_path(
    'date_util_01',
    PROJECT_ROOT / '01_engine' / 'lib' / 'date_util.py',
    'resolve_trade_date',
)


# ==================== Data Structures ====================

@dataclass
class TideState:
    """Tide sensor output"""

    trade_date: str
    risk_gate: str  # FORCE_NO_EDGE | CAUTION | AGGRESSIVE
    style_bias: float  # [0.8, 1.2]
    ma20_ratio: float  # s = count(close > ma20) / total, [0, 1]
    total_stocks: int
    above_ma20: int
    status: str  # Human-readable status
    phi: float = 0.0

    def __post_init__(self) -> None:
        self.phi = float(self.phi if self.phi is not None else 0.0)

    @property
    def is_meltdown(self) -> bool:
        return self.risk_gate == 'FORCE_NO_EDGE'

    def to_dict(self) -> dict:
        return {
            'trade_date': self.trade_date,
            'risk_gate': self.risk_gate,
            'style_bias': round(self.style_bias, 4),
            'ma20_ratio': round(self.ma20_ratio, 4),
            'total_stocks': self.total_stocks,
            'above_ma20': self.above_ma20,
            'status': self.status,
            'phi': round(self.phi, 4),
        }


# ==================== Tide Sensor ====================

class TideSensor:
    """
    L0 Macro Filter - System Macro Filter (SMF)

    Phase A: T_risk (Risk Permission Gate)
        s = count(close > MA20) / total
        s < 0.3  -> FORCE_NO_EDGE (meltdown, veto all)
        0.3-0.7  -> CAUTION
        s >= 0.7 -> AGGRESSIVE

    Phase B: T_style (Style Correction Bias)
        T_style = 1 + 0.2 * tanh(s - 0.5)
        Output clamped to [0.8, 1.2]
        Applied as: Final_Score = Raw_Score * T_style
    """

    def __init__(self):
        self._industry_blacklist: Dict[str, List[str]] = {}
        self._load_industry_config()

    def _load_industry_config(self):
        """Load industry relation blacklist (Phase 4 scaffold)."""
        if INDUSTRY_CONFIG.exists():
            try:
                with open(INDUSTRY_CONFIG, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                self._industry_blacklist = data.get('blacklist', {})
                logger.info(f"Industry config loaded: {len(self._industry_blacklist)} blacklist entries")
            except Exception as e:
                logger.warning(f'Industry config load failed: {e}')

    def get_risk_gate(self, trade_date: str = None) -> TideState:
        """
        Compute T_risk from full-market MA20 breadth.
        Uses TushareBridge -> DuckDB fail-safe path.
        """
        anchored_trade_date = resolve_trade_date(trade_date)
        try:
            _engine_lib = PROJECT_ROOT / '01_engine' / 'lib'
            if str(_engine_lib) not in sys.path:
                sys.path.append(str(_engine_lib))
            from tushare_bridge import get_bridge

            bridge = get_bridge()
            df = bridge.get_market_breadth_raw(anchored_trade_date)
        except Exception as _ie:
            logger.warning('TushareBridge import failed, direct DuckDB fallback | err=%s', _ie)
            df = self._direct_duckdb_breadth(anchored_trade_date)

        if df.empty:
            logger.error('No market data, defaulting to CAUTION')
            return TideState(
                trade_date=anchored_trade_date,
                risk_gate='CAUTION',
                style_bias=1.0,
                ma20_ratio=0.5,
                total_stocks=0,
                above_ma20=0,
                status='NO_DATA_FALLBACK',
                phi=0.0,
            )

        total = len(df)
        above = int((df['close'] > df['ma20']).sum())
        s = above / total if total > 0 else 0

        if s < 0.3:
            risk_gate = 'FORCE_NO_EDGE'
            status = f'MELTDOWN: only {s*100:.1f}% above MA20'
        elif s < 0.7:
            risk_gate = 'CAUTION'
            status = f'CAUTION: {s*100:.1f}% above MA20'
        else:
            risk_gate = 'AGGRESSIVE'
            status = f'CLEAR: {s*100:.1f}% above MA20'

        style_bias = 1.0 + 0.2 * math.tanh(3.0 * (s - 0.5))
        style_bias = max(0.8, min(1.2, style_bias))

        actual_date = anchored_trade_date

        state = TideState(
            trade_date=actual_date,
            risk_gate=risk_gate,
            style_bias=round(style_bias, 4),
            ma20_ratio=round(s, 4),
            total_stocks=total,
            above_ma20=above,
            status=status,
            phi=round(s, 4),
        )

        logger.info(f'TIDE [{actual_date}] s={s:.3f} gate={risk_gate} bias={style_bias:.4f} ({above}/{total})')
        return state

    def _direct_duckdb_breadth(self, trade_date: str = None):
        """Emergency fallback: compute directly from DuckDB without bridge."""
        import pandas as pd

        trade_date = resolve_trade_date(trade_date)
        with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
            query = """
                WITH daily_window AS (
                    SELECT symbol, trade_date, close,
                        AVG(close) OVER (
                            PARTITION BY symbol ORDER BY trade_date
                            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                        ) AS ma20,
                        COUNT(close) OVER (
                            PARTITION BY symbol ORDER BY trade_date
                            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                        ) AS window_size
                    FROM fact_daily
                    WHERE trade_date >= (
                        SELECT MIN(trade_date) FROM (
                            SELECT DISTINCT trade_date FROM fact_daily
                            WHERE trade_date <= CAST(? AS DATE)
                            ORDER BY trade_date DESC LIMIT 30
                        )
                    ) AND close > 0
                )
                SELECT symbol, trade_date, close, ma20
                FROM daily_window
                WHERE trade_date = CAST(? AS DATE) AND window_size >= 20
            """
            df = conn.execute(query, [trade_date, trade_date]).df()
        return df

    def is_sector_safe(self, symbol: str, industry: str = '') -> bool:
        """
        Industry blacklist check (scaffold for Phase 4).
        Returns True if the sector is NOT blacklisted.
        """
        if not self._industry_blacklist:
            return True

        for category, blocked_industries in self._industry_blacklist.items():
            if industry in blocked_industries:
                logger.info(f'SECTOR_BLOCK: {symbol} ({industry}) blocked by {category}')
                return False
        return True

    def persist_state(self, state: TideState):
        """
        Write tide state to historical_factor_segments.
        Revives d4_tide_state and d6_ma_bias fields.
        """
        try:
            with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
                conn.execute(
                    """
                    UPDATE historical_factor_segments
                    SET d4_tide_state = ?,
                        d6_ma_bias = ?
                    WHERE trade_date = CAST(? AS DATE)
                    """,
                    [state.risk_gate, f'{state.style_bias:.4f}', state.trade_date],
                )

                affected = conn.execute(
                    """
                    SELECT COUNT(*) FROM historical_factor_segments
                    WHERE trade_date = CAST(? AS DATE)
                      AND d4_tide_state = ?
                    """,
                    [state.trade_date, state.risk_gate],
                ).fetchone()[0]

            logger.info(f'PERSIST: {affected} rows updated with d4={state.risk_gate} d6={state.style_bias:.4f}')

        except Exception as e:
            logger.critical(f'Persistence failed: {e}', exc_info=True)
            raise


# ==================== Convenience ====================

_sensor_instance = None


def get_sensor() -> TideSensor:
    global _sensor_instance
    if _sensor_instance is None:
        _sensor_instance = TideSensor()
    return _sensor_instance


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
    sensor = get_sensor()
    state = sensor.get_risk_gate()
    print(f"\n{'='*60}")
    print(f"  TIDE STATE: {state.trade_date}")
    print(f"  Risk Gate:  {state.risk_gate}")
    print(f"  Style Bias: {state.style_bias}")
    print(f"  MA20 Ratio: {state.ma20_ratio:.4f} ({state.above_ma20}/{state.total_stocks})")
    print(f"  Status:     {state.status}")
    print(f"  Meltdown:   {state.is_meltdown}")
    print(f"{'='*60}")

    sensor.persist_state(state)
    print('Persistence complete.')
