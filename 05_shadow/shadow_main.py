#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
05_shadow/shadow_main.py
Shadow protocol entrypoint (contract aligned).
"""

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
import sys
from datetime import datetime
from pathlib import Path

BASE_DIR = Path('/root/quant_project')
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from config.settings import Config, ensure_syspath

ensure_syspath()
LOG_DIR = Path(Config.LOG_DIR)
LOG_DIR.mkdir(parents=True, exist_ok=True)

from lib.db_contract import (
    DBGateway,
    DB_PATH,
    DEFAULT_MIN_FINAL_SCORE,
    SIGNAL_VERDICT_PASS,
    detect_nexus_score_column,
)
from lib.engine import calculate_shadow_fill, persist_fill

def setup_logging() -> None:
    if getattr(setup_logging, '_done', False):
        return
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | [%(name)s] %(message)s',
        handlers=[
            RotatingFileHandler(str(LOG_DIR / 'shadow_main.log'), maxBytes=20*1024*1024, backupCount=5, encoding='utf-8'),
            logging.StreamHandler(),
        ],
    )
    setup_logging._done = True


logger = logging.getLogger('shadow')
setup_logging()


def signal_receiver(limit: int = 10):
    """Read latest PASS signals from nexus_audits with final_score gate."""
    with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
        score_col = detect_nexus_score_column(conn)
        if not score_col:
            logger.error('No usable score field in nexus_audits')
            return []

        rows = conn.execute(
            f"""
            WITH ranked AS (
                SELECT
                    symbol,
                    l4_final_verdict,
                    COALESCE({score_col}, 0) AS final_score,
                    trade_date,
                    created_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY symbol
                        ORDER BY created_at DESC, task_id DESC
                    ) AS rn
                FROM nexus_audits
                WHERE UPPER(COALESCE(l4_final_verdict, '')) IN (?, 'APPROVE')
                  AND COALESCE({score_col}, 0) >= ?
            )
            SELECT symbol, l4_final_verdict, final_score, trade_date
            FROM ranked
            WHERE rn = 1
            ORDER BY final_score DESC, trade_date DESC
            LIMIT ?
            """,
            [SIGNAL_VERDICT_PASS, DEFAULT_MIN_FINAL_SCORE, limit],
        ).fetchall()
        return rows


def record_trade(
    symbol: str,
    action: str,
    price: float,
    qty: int,
    trace_id: str = '',
    tide_mode: str = 'Standard',
    strategy_tag: str = '',
    trade_date: str | None = None,
) -> None:
    """Simulate and persist one shadow receipt into fact_shadow_ledger."""
    td = trade_date or datetime.now().strftime('%Y-%m-%d')
    fill = calculate_shadow_fill(symbol=symbol, action=action, price_logical=price, qty=qty, trade_date=td)
    persist_fill(
        fill,
        trace_id=trace_id,
        tide_mode=tide_mode,
        strategy_tag=strategy_tag,
        trade_date=td,
    )
    logger.info(
        f'[shadow] {action} {symbol} x{qty} @{price:.2f} -> {fill.price_shadow:.2f} '
        f'slip={fill.slippage_cost:.2f}'
    )


if __name__ == '__main__':
    setup_logging()
    print('Shadow protocol bootstrap')
    print(f'Gate: verdict={SIGNAL_VERDICT_PASS}, min_final_score={DEFAULT_MIN_FINAL_SCORE}')

    signals = signal_receiver(limit=10)
    print(f'Recent PASS signals: {len(signals)}')
    for row in signals:
        print(f'  {row[0]} | {row[1]} | score={row[2]} | trade_date={row[3]}')
