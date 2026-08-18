#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
01_engine/lib/tushare_bridge.py
SMF Data Bridge - Tushare Pro + DuckDB Fail-safe
3x Exponential Backoff Retry
"""

import time
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional
import sys

logger = logging.getLogger('zhulong.tushare_bridge')

_current = Path(__file__).resolve()
PROJECT_ROOT = next(
    (p for p in _current.parents if (p / ".git").exists() or (p / "storage").exists()),
    _current.parents[2]
)
DB_PATH = str(PROJECT_ROOT / "storage" / "database" / "zhulong.duckdb")

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))
from config.settings import Config

_CORE_DIR = PROJECT_ROOT / "04_governance" / "lib" / "core"
if str(_CORE_DIR) not in sys.path:
    sys.path.append(str(_CORE_DIR))
from module_loader import load_attr_from_path

DBGateway = load_attr_from_path(
    "db_gateway_01",
    PROJECT_ROOT / "01_engine" / "lib" / "db_gateway.py",
    "DBGateway",
)
resolve_trade_date = load_attr_from_path(
    "date_util_01",
    PROJECT_ROOT / "01_engine" / "lib" / "date_util.py",
    "resolve_trade_date",
)

_TUSHARE_TOKEN = None

def _get_token() -> str:
    global _TUSHARE_TOKEN
    if _TUSHARE_TOKEN is None:
        _TUSHARE_TOKEN = str(getattr(Config, 'TUSHARE_TOKEN', '') or '')
    return _TUSHARE_TOKEN

def _retry_with_backoff(func, max_retries: int = 3, base_delay: float = 1.0):
    """Exponential Backoff: 1s -> 2s -> 4s"""
    last_err = None
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            last_err = e
            delay = base_delay * (2 ** attempt)
            logger.warning(f"Tushare retry {attempt+1}/{max_retries}: {e}, wait {delay:.1f}s")
            time.sleep(delay)
    raise ConnectionError(f"Tushare {max_retries}x failed: {last_err}")


class TushareBridge:
    """Tushare Pro data bridge with DuckDB fail-safe"""

    def __init__(self):
        self.api = None
        self.available = False
        self._init_api()

    def _init_api(self):
        token = _get_token()
        if not token:
            logger.warning("TUSHARE_TOKEN not set, DuckDB-only mode")
            return
        try:
            import tushare as ts
            self.api = ts.pro_api(token)
            self.available = True
            logger.info("Tushare Pro API connected")
        except ImportError:
            logger.warning("tushare not installed, DuckDB-only mode")
        except Exception as e:
            logger.warning(f"Tushare init failed: {e}, DuckDB-only mode")

    def get_market_breadth_raw(self, trade_date: str = None, lookback_days: int = 30) -> pd.DataFrame:
        """
        Get raw data for MA20 market breadth calculation.
        Returns DataFrame[symbol, trade_date, close, ma20]
        Path A: Tushare -> Path B: DuckDB fallback
        """
        anchored_trade_date = resolve_trade_date(trade_date)
        if self.available and self.api:
            try:
                return self._breadth_via_tushare(anchored_trade_date)
            except Exception as e:
                logger.warning(f"Tushare breadth failed, fallback DuckDB: {e}")
        return self._breadth_via_duckdb(anchored_trade_date, lookback_days)

    def _breadth_via_tushare(self, trade_date: str = None) -> pd.DataFrame:
        td = resolve_trade_date(trade_date)
        ts_date = td.replace("-", "")

        def _fetch():
            df = self.api.daily_basic(trade_date=ts_date, fields="ts_code,trade_date,close,turnover_rate")
            if df is None or df.empty:
                raise ValueError(f"No daily_basic data: {ts_date}")
            return df

        df = _retry_with_backoff(_fetch)
        df = df.rename(columns={"ts_code": "symbol"})
        df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d").dt.strftime("%Y-%m-%d")

        # MA20 still computed from DuckDB (Tushare has no bulk MA endpoint)
        td = resolve_trade_date(trade_date or ts_date)
        ma20_map = self._compute_ma20_duckdb(td)
        df["ma20"] = df["symbol"].map(ma20_map)
        df = df.dropna(subset=["ma20"])
        logger.info(f"Tushare+DuckDB: {len(df)} symbols with MA20")
        return df[["symbol", "trade_date", "close", "ma20"]]

    def _breadth_via_duckdb(self, trade_date: str = None, lookback_days: int = 30) -> pd.DataFrame:
        """
        DuckDB local MA20 computation.
        TRAP: MA20 needs >= 20 trading days. lookback=30 covers holidays.
        """
        trade_date = resolve_trade_date(trade_date)
        with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
            dates_df = conn.execute(f"""
                SELECT DISTINCT trade_date FROM fact_daily
                WHERE trade_date <= CAST('{trade_date}' AS DATE)
                ORDER BY trade_date DESC LIMIT {lookback_days}
            """).df()

            if len(dates_df) < 20:
                logger.error(f"Insufficient history: {len(dates_df)} days, need >= 20")
                return pd.DataFrame(columns=["symbol", "trade_date", "close", "ma20"])

            earliest_date = dates_df["trade_date"].min()

            query = f"""
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
                    WHERE trade_date >= CAST('{earliest_date}' AS DATE) AND close > 0
                )
                SELECT symbol, trade_date, close, ma20
                FROM daily_window
                WHERE trade_date = CAST('{trade_date}' AS DATE) AND window_size >= 20
            """
            df = conn.execute(query).df()

        if not df.empty and hasattr(df["trade_date"].iloc[0], "strftime"):
            df["trade_date"] = df["trade_date"].apply(
                lambda x: x.strftime("%Y-%m-%d") if hasattr(x, "strftime") else str(x)
            )
        logger.info(f"DuckDB local: {len(df)} symbols with MA20 (date: {trade_date})")
        return df

    def _compute_ma20_duckdb(self, trade_date: str) -> dict:
        df = self._breadth_via_duckdb(trade_date)
        return dict(zip(df["symbol"], df["ma20"])) if not df.empty else {}

    def get_index_daily(self, ts_code: str = "000001.SH", days: int = 60) -> pd.DataFrame:
        if self.available and self.api:
            try:
                def _fetch():
                    end_trade_date = resolve_trade_date(None)
                    end_dt = datetime.strptime(end_trade_date, "%Y-%m-%d")
                    end = end_dt.strftime("%Y%m%d")
                    start = (end_dt - timedelta(days=days * 2)).strftime("%Y%m%d")
                    return self.api.index_daily(ts_code=ts_code, start_date=start, end_date=end)
                return _retry_with_backoff(_fetch)
            except Exception as e:
                logger.warning(f"Index daily fetch failed: {e}")
        return pd.DataFrame()

    def get_latest_trade_date(self) -> str:
        with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
            result = conn.execute("SELECT MAX(trade_date) FROM fact_daily").fetchone()[0]
        return result.strftime("%Y-%m-%d") if hasattr(result, "strftime") else str(result)


_bridge_instance = None
def get_bridge() -> TushareBridge:
    global _bridge_instance
    if _bridge_instance is None:
        _bridge_instance = TushareBridge()
    return _bridge_instance


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    bridge = get_bridge()
    print(f"Tushare available: {bridge.available}")
    print(f"Latest trade_date: {bridge.get_latest_trade_date()}")
    df = bridge.get_market_breadth_raw()
    if not df.empty:
        above = (df["close"] > df["ma20"]).sum()
        total = len(df)
        s = above / total if total > 0 else 0
        print(f"Market breadth: {above}/{total} = {s:.4f} ({s*100:.1f}%)")
    else:
        print("No market data available")
