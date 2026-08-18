#!/usr/bin/env python3
"""Daily derived-feature refresh: ma20, vol_ma5."""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENGINE_ROOT = PROJECT_ROOT / '01_engine'
SCRIPTS_DIR = Path(__file__).resolve().parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))
if str(ENGINE_ROOT) not in sys.path:
    sys.path.append(str(ENGINE_ROOT))
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.append(str(SCRIPTS_DIR))

from config.settings import Config
from lib.db_gateway import DBGateway
from harvest_state import sync_harvest_state

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s')
logger = logging.getLogger('derived_features')

DB = str(getattr(Config, 'DB_PATH', PROJECT_ROOT / 'storage' / 'database' / 'zhulong.duckdb'))

TARGET_TRADE_DAYS = 10
HISTORY_TRADE_DAYS = 60


def _latest_trade_date_iso(conn) -> str:
    row = conn.execute("SELECT CAST(MAX(trade_date) AS VARCHAR) FROM fact_daily").fetchone()
    return str(row[0]) if row and row[0] else ''


def _zeta_rows_for_trade_date(conn, trade_date_iso: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM fact_zeta_signals WHERE trade_date = CAST(? AS DATE)",
        [trade_date_iso],
    ).fetchone()
    return int(row[0] or 0) if row else 0


def update() -> None:
    t0 = time.time()
    trade_date_iso = ''
    zeta_rows = 0

    try:
        with DBGateway(DB, read_only=False, logger=logger) as conn:
            conn.execute(
                """
                WITH target_dates AS (
                    SELECT trade_date
                    FROM (
                        SELECT DISTINCT trade_date
                        FROM fact_daily
                        ORDER BY trade_date DESC
                        LIMIT ?
                    )
                ),
                history_dates AS (
                    SELECT trade_date
                    FROM (
                        SELECT DISTINCT trade_date
                        FROM fact_daily
                        ORDER BY trade_date DESC
                        LIMIT ?
                    )
                ),
                calc AS (
                    SELECT symbol, trade_date,
                           AVG(close) OVER (
                               PARTITION BY symbol ORDER BY trade_date
                               ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                           ) AS ma20_val,
                           AVG(vol) OVER (
                               PARTITION BY symbol ORDER BY trade_date
                               ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
                           ) AS vol5_val
                    FROM fact_daily
                    WHERE trade_date IN (SELECT trade_date FROM history_dates)
                )
                UPDATE fact_daily
                SET ma20 = calc.ma20_val,
                    vol_ma5 = calc.vol5_val
                FROM calc
                WHERE fact_daily.symbol = calc.symbol
                  AND fact_daily.trade_date = calc.trade_date
                  AND fact_daily.trade_date IN (SELECT trade_date FROM target_dates)
                """,
                [TARGET_TRADE_DAYS, HISTORY_TRADE_DAYS],
            )

            conn.commit()
            trade_date_iso = _latest_trade_date_iso(conn)
            if trade_date_iso:
                zeta_rows = _zeta_rows_for_trade_date(conn, trade_date_iso)

        elapsed = time.time() - t0
        logger.info('ma20/vol_ma5 refresh done, elapsed %.1fs', elapsed)

        if trade_date_iso:
            if zeta_rows > 0:
                sync_harvest_state(
                    trade_date_iso,
                    'IN_PROGRESS',
                    'Derived_DONE',
                    logger=logger,
                )
                logger.info('phase_harvest step appended: %s Derived_DONE (zeta_rows=%s)', trade_date_iso, zeta_rows)
            else:
                sync_harvest_state(
                    trade_date_iso,
                    'IN_PROGRESS',
                    'Derived_DONE',
                    logger=logger,
                )
                logger.warning('phase_harvest step appended: %s Derived_DONE but zeta_rows=0', trade_date_iso)
        else:
            logger.warning('phase_harvest sync skipped: latest trade_date unresolved')

    except Exception as exc:
        if trade_date_iso:
            try:
                sync_harvest_state(
                    trade_date_iso,
                    'FAILED',
                    'Derived_FAILED',
                    logger=logger,
                )
            except Exception as sync_exc:
                logger.error('phase_harvest FAILED sync also failed: %s', sync_exc)
        raise


if __name__ == '__main__':
    update()
