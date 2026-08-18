#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Backfill Shadow trade-cost fields atomically.

The data updates run inside one DuckDB transaction: backup snapshots, ledger
fees, fill-event fees, and position-level net PnL either all commit or all roll
back. Schema additions are idempotent and happen before the data transaction.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

BASE_DIR = Path('/root/quant_project')
SHADOW_LIB = BASE_DIR / '05_shadow' / 'lib'
if str(SHADOW_LIB) not in sys.path:
    sys.path.insert(0, str(SHADOW_LIB))

from costs import calculate_trade_cost  # noqa: E402
from db_contract import DBGateway, DB_PATH  # noqa: E402
from engine import _ensure_paper_positions, _ensure_shadow_ledger  # noqa: E402
from t1_fill_engine import ensure_t1_tables  # noqa: E402


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0)
    except Exception:
        return 0.0


def _safe_int(value: Any) -> int:
    try:
        return int(float(value or 0))
    except Exception:
        return 0


def _cost_tuple(action: str, price: float, qty: int) -> Tuple[float, float, float, float, float, float]:
    c = calculate_trade_cost(action, price=price, qty=qty)
    return c.gross_amount, c.commission, c.stamp_tax, c.transfer_fee, c.tax_total, c.net_amount


def _fetch_buy_fills(conn, symbol: str, entry_date: str, entry_price: float, initial_qty: int) -> Tuple[List[Tuple[float, int]], str]:
    rows = conn.execute(
        """
        SELECT price_shadow, qty
        FROM fact_shadow_ledger
        WHERE symbol = ?
          AND UPPER(COALESCE(action, '')) = 'BUY'
          AND CAST(trade_date AS DATE) = CAST(? AS DATE)
          AND COALESCE(price_shadow, 0) > 0
          AND COALESCE(qty, 0) > 0
        ORDER BY timestamp
        """,
        [symbol, entry_date],
    ).fetchall()
    if rows:
        return [(float(r[0]), int(r[1])) for r in rows], 'LEDGER_BUY'

    rows = conn.execute(
        """
        SELECT fill_price, qty
        FROM fact_shadow_fill_events
        WHERE symbol = ?
          AND UPPER(COALESCE(action, 'BUY')) = 'BUY'
          AND UPPER(COALESCE(status, '')) = 'FILLED'
          AND CAST(fill_date AS DATE) = CAST(? AS DATE)
          AND COALESCE(fill_price, 0) > 0
          AND COALESCE(qty, 0) > 0
        ORDER BY created_at
        """,
        [symbol, entry_date],
    ).fetchall()
    if rows:
        return [(float(r[0]), int(r[1])) for r in rows], 'FILL_EVENTS_BUY'
    return [(entry_price, initial_qty)], 'POSITION_BUY_FALLBACK'


def _fetch_sell_fills(
    conn,
    symbol: str,
    entry_date: str,
    exit_date: str,
    exit_price: float,
    realized_qty: int,
    realized_amount: float,
) -> Tuple[List[Tuple[float, int]], str]:
    rows = conn.execute(
        """
        SELECT price_shadow, qty
        FROM fact_shadow_ledger
        WHERE symbol = ?
          AND UPPER(COALESCE(action, '')) = 'SELL'
          AND CAST(trade_date AS DATE) >= CAST(? AS DATE)
          AND (? = '' OR CAST(trade_date AS DATE) <= CAST(? AS DATE))
          AND COALESCE(price_shadow, 0) > 0
          AND COALESCE(qty, 0) > 0
        ORDER BY timestamp
        """,
        [symbol, entry_date, exit_date, exit_date],
    ).fetchall()
    if rows:
        return [(float(r[0]), int(r[1])) for r in rows], 'LEDGER_SELL'
    if realized_qty > 0 and realized_amount > 0:
        return [(realized_amount / realized_qty, realized_qty)], 'POSITION_REALIZED_FALLBACK'
    if realized_qty > 0 and exit_price > 0:
        return [(exit_price, realized_qty)], 'POSITION_EXIT_FALLBACK'
    return [], 'NO_SELL'


def _sum_costs(action: str, fills: List[Tuple[float, int]]) -> Dict[str, float]:
    out = {
        'gross': 0.0,
        'commission': 0.0,
        'stamp_tax': 0.0,
        'transfer_fee': 0.0,
        'tax_total': 0.0,
        'net': 0.0,
        'qty': 0,
    }
    for price, qty in fills:
        gross, commission, stamp_tax, transfer_fee, tax_total, net = _cost_tuple(action, price, qty)
        out['gross'] += gross
        out['commission'] += commission
        out['stamp_tax'] += stamp_tax
        out['transfer_fee'] += transfer_fee
        out['tax_total'] += tax_total
        out['net'] += net
        out['qty'] += int(qty or 0)
    for key in ('gross', 'commission', 'stamp_tax', 'transfer_fee', 'tax_total', 'net'):
        out[key] = round(out[key], 2)
    return out


def run_backfill(dry_run: bool = True) -> Dict[str, Any]:
    stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    summary: Dict[str, Any] = {
        'dry_run': dry_run,
        'backup_suffix': stamp,
        'ledger_rows': 0,
        'fill_event_rows': 0,
        'position_rows': 0,
        'closed_rows': 0,
        'manual_check_920725': {},
    }
    with DBGateway(DB_PATH, read_only=False) as conn:
        _ensure_shadow_ledger(conn)
        _ensure_paper_positions(conn)
        ensure_t1_tables(conn)

        conn.execute('BEGIN TRANSACTION')
        try:
            conn.execute(f'CREATE TABLE backup_fact_shadow_ledger_cost_{stamp} AS SELECT * FROM fact_shadow_ledger')
            conn.execute(f'CREATE TABLE backup_fact_shadow_fill_events_cost_{stamp} AS SELECT * FROM fact_shadow_fill_events')
            conn.execute(f'CREATE TABLE backup_fact_paper_positions_cost_{stamp} AS SELECT * FROM fact_paper_positions')

            ledger_rows = conn.execute(
                """
                SELECT rowid, action, price_shadow, qty
                FROM fact_shadow_ledger
                WHERE COALESCE(price_shadow, 0) > 0 AND COALESCE(qty, 0) > 0
                """
            ).fetchall()
            for rowid, action, price, qty in ledger_rows:
                gross, commission, stamp_tax, transfer_fee, tax_total, net = _cost_tuple(str(action), _safe_float(price), _safe_int(qty))
                conn.execute(
                    """
                    UPDATE fact_shadow_ledger
                    SET gross_amount = ?, commission = ?, stamp_tax = ?, transfer_fee = ?,
                        tax_total = ?, net_amount = ?
                    WHERE rowid = ?
                    """,
                    [gross, commission, stamp_tax, transfer_fee, tax_total, net, rowid],
                )
            summary['ledger_rows'] = len(ledger_rows)

            event_rows = conn.execute(
                """
                SELECT rowid, action, fill_price, qty
                FROM fact_shadow_fill_events
                WHERE UPPER(COALESCE(status, '')) = 'FILLED'
                  AND COALESCE(fill_price, 0) > 0
                  AND COALESCE(qty, 0) > 0
                """
            ).fetchall()
            for rowid, action, price, qty in event_rows:
                action = str(action or 'BUY')
                gross, commission, stamp_tax, transfer_fee, tax_total, net = _cost_tuple(action, _safe_float(price), _safe_int(qty))
                conn.execute(
                    """
                    UPDATE fact_shadow_fill_events
                    SET gross_amount = ?, commission = ?, stamp_tax = ?, transfer_fee = ?,
                        tax_total = ?, net_amount = ?
                    WHERE rowid = ?
                    """,
                    [gross, commission, stamp_tax, transfer_fee, tax_total, net, rowid],
                )
            summary['fill_event_rows'] = len(event_rows)

            positions = conn.execute(
                """
                SELECT
                    symbol,
                    CAST(trade_date AS VARCHAR) AS entry_date,
                    COALESCE(entry_price, 0),
                    COALESCE(exit_price, 0),
                    COALESCE(NULLIF(initial_qty, 0), NULLIF(qty, 0), NULLIF(realized_qty, 0), 0),
                    COALESCE(realized_qty, 0),
                    COALESCE(realized_amount, 0),
                    COALESCE(status, ''),
                    CAST(exit_trade_date AS VARCHAR)
                FROM fact_paper_positions
                ORDER BY trade_date, symbol
                """
            ).fetchall()

            for row in positions:
                symbol = str(row[0] or '').strip().upper()
                entry_date = str(row[1] or '')[:10]
                entry_price = _safe_float(row[2])
                exit_price = _safe_float(row[3])
                initial_qty = _safe_int(row[4])
                realized_qty = _safe_int(row[5])
                realized_amount = _safe_float(row[6])
                status = str(row[7] or '').strip().upper()
                exit_date = str(row[8] or '')[:10]
                buy_fills, buy_source = _fetch_buy_fills(conn, symbol, entry_date, entry_price, initial_qty)
                buy_cost = _sum_costs('BUY', buy_fills)
                sell_fills, sell_source = _fetch_sell_fills(conn, symbol, entry_date, exit_date, exit_price, realized_qty, realized_amount)
                sell_cost = _sum_costs('SELL', sell_fills)
                if realized_qty <= 0:
                    realized_qty = int(sell_cost['qty'])
                if realized_qty <= 0 and status != 'HOLD':
                    realized_qty = initial_qty
                entry_cost_sold = buy_cost['net'] * (realized_qty / max(1, initial_qty)) if realized_qty else 0.0
                realized_net_pnl = round(sell_cost['net'] - entry_cost_sold, 2) if realized_qty else 0.0
                pnl_ratio_net = round(realized_net_pnl / entry_cost_sold, 6) if entry_cost_sold > 0 else None
                pnl_ratio_gross = round((sell_cost['gross'] - (buy_cost['gross'] * realized_qty / max(1, initial_qty))) / (buy_cost['gross'] * realized_qty / max(1, initial_qty)), 6) if realized_qty and buy_cost['gross'] > 0 else None
                source = f'{buy_source}+{sell_source}'
                conn.execute(
                    """
                    UPDATE fact_paper_positions
                    SET entry_gross_amount = ?,
                        entry_commission = ?,
                        entry_stamp_tax = ?,
                        entry_transfer_fee = ?,
                        entry_tax_total = ?,
                        entry_total_cost = ?,
                        realized_qty = ?,
                        realized_amount = ?,
                        realized_gross_amount = ?,
                        realized_commission = ?,
                        realized_stamp_tax = ?,
                        realized_transfer_fee = ?,
                        realized_tax_total = ?,
                        realized_net_amount = ?,
                        realized_net_pnl = ?,
                        pnl_ratio = COALESCE(?, pnl_ratio),
                        pnl_ratio_gross = COALESCE(?, pnl_ratio_gross),
                        fee_backfill_source = ?,
                        fee_backfilled_at = CAST(? AS TIMESTAMP)
                    WHERE symbol = ?
                      AND CAST(trade_date AS DATE) = CAST(? AS DATE)
                    """,
                    [
                        buy_cost['gross'],
                        buy_cost['commission'],
                        buy_cost['stamp_tax'],
                        buy_cost['transfer_fee'],
                        buy_cost['tax_total'],
                        buy_cost['net'],
                        realized_qty,
                        sell_cost['gross'],
                        sell_cost['gross'],
                        sell_cost['commission'],
                        sell_cost['stamp_tax'],
                        sell_cost['transfer_fee'],
                        sell_cost['tax_total'],
                        sell_cost['net'],
                        realized_net_pnl,
                        pnl_ratio_net,
                        pnl_ratio_gross,
                        source,
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        symbol,
                        entry_date,
                    ],
                )
                summary['position_rows'] += 1
                if status != 'HOLD':
                    summary['closed_rows'] += 1
                if symbol == '920725.BJ':
                    summary['manual_check_920725'] = {
                        'entry_fills': buy_fills,
                        'sell_fills': sell_fills,
                        'buy_source': buy_source,
                        'sell_source': sell_source,
                        'buy_cost': buy_cost,
                        'sell_cost': sell_cost,
                        'entry_cost_sold': round(entry_cost_sold, 2),
                        'realized_net_pnl': realized_net_pnl,
                        'pnl_ratio_net': pnl_ratio_net,
                    }

            if dry_run:
                conn.execute('ROLLBACK')
            else:
                conn.execute('COMMIT')
        except Exception:
            conn.execute('ROLLBACK')
            raise
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description='Backfill Shadow trade costs atomically')
    parser.add_argument('--apply', action='store_true', help='Commit updates. Default is dry-run rollback.')
    args = parser.parse_args()
    print(json.dumps(run_backfill(dry_run=not args.apply), ensure_ascii=False, indent=2, default=str))


if __name__ == '__main__':
    main()
