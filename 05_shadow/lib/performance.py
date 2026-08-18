#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Read-only Shadow performance statistics.

Closed-trade granularity is intentionally by symbol entry row: all partial exits
for one paper position are merged into one trade after per-fill fees are already
deducted in the ledger/position layer. Max drawdown is computed from the
closed-trade cumulative equity curve and excludes unrealized PnL.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

try:
    from .shadow_config import position_rules
except Exception:
    from shadow_config import position_rules


def _table_columns(conn, table_name: str) -> set[str]:
    rows = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchall()
    return {str(r[0]).lower() for r in rows}


def closed_trade_rows(conn) -> List[Dict[str, Any]]:
    if not _table_columns(conn, 'fact_paper_positions'):
        return []
    has_names = bool(_table_columns(conn, 'fact_stock_basic'))
    name_join = 'LEFT JOIN fact_stock_basic b ON b.symbol = p.symbol' if has_names else ''
    name_select = "COALESCE(b.name, '')" if has_names else "''"
    rows = conn.execute(
        f"""
        SELECT
            p.symbol,
            {name_select} AS name,
            CAST(p.trade_date AS VARCHAR) AS entry_date,
            CAST(p.exit_trade_date AS VARCHAR) AS exit_date,
            COALESCE(p.entry_price, 0) AS entry_price,
            COALESCE(p.exit_price, 0) AS exit_price,
            COALESCE(NULLIF(p.realized_qty, 0), NULLIF(p.initial_qty, 0), 0) AS realized_qty,
            COALESCE(NULLIF(p.initial_qty, 0), NULLIF(p.realized_qty, 0), 0) AS initial_qty,
            COALESCE(p.entry_score, NULL) AS entry_score,
            COALESCE(NULLIF(p.entry_total_cost, 0), p.entry_price * COALESCE(NULLIF(p.initial_qty, 0), NULLIF(p.realized_qty, 0), 0), 0) AS entry_total_cost,
            COALESCE(NULLIF(p.realized_net_amount, 0), p.realized_amount, 0) AS realized_net_amount,
            COALESCE(NULLIF(p.realized_gross_amount, 0), p.realized_amount, 0) AS realized_gross_amount,
            COALESCE(p.realized_tax_total, 0) AS realized_tax_total,
            COALESCE(p.entry_tax_total, 0) AS entry_tax_total,
            COALESCE(p.realized_net_pnl, 0) AS stored_net_pnl,
            COALESCE(p.pnl_ratio, 0) AS pnl_ratio,
            COALESCE(p.pnl_ratio_gross, 0) AS pnl_ratio_gross,
            COALESCE(p.last_sell_rule_id, '') AS sell_rule,
            COALESCE(p.last_sell_reason, '') AS sell_reason
        FROM fact_paper_positions p
        {name_join}
        WHERE UPPER(COALESCE(NULLIF(p.status, ''), 'HOLD')) != 'HOLD'
        ORDER BY COALESCE(p.exit_trade_date, p.trade_date), p.symbol
        """
    ).fetchall()

    result: List[Dict[str, Any]] = []
    for row in rows:
        realized_qty = int(row[6] or 0)
        initial_qty = int(row[7] or realized_qty or 0)
        entry_total = float(row[9] or 0)
        entry_cost_sold = entry_total * (realized_qty / max(1, initial_qty)) if entry_total else 0.0
        realized_net = float(row[10] or 0)
        net_pnl = float(row[14] or 0)
        if not net_pnl and entry_cost_sold:
            net_pnl = round(realized_net - entry_cost_sold, 2)
        pnl_ratio = float(row[15] or 0)
        if not pnl_ratio and entry_cost_sold:
            pnl_ratio = round(net_pnl / entry_cost_sold, 6)
        result.append({
            'symbol': str(row[0] or '').strip().upper(),
            'name': str(row[1] or ''),
            'entry_date': str(row[2] or ''),
            'exit_date': str(row[3] or ''),
            'entry_price': float(row[4] or 0),
            'exit_price': float(row[5] or 0),
            'qty': realized_qty,
            'entry_score': float(row[8]) if row[8] is not None else None,
            'entry_total_cost': round(entry_cost_sold, 2),
            'realized_net_amount': round(realized_net, 2),
            'realized_gross_amount': round(float(row[11] or 0), 2),
            'total_fees': round(float(row[12] or 0) + float(row[13] or 0), 2),
            'net_pnl': round(net_pnl, 2),
            'pnl_ratio': round(pnl_ratio, 6),
            'pnl_ratio_gross': round(float(row[16] or 0), 6),
            'sell_rule': str(row[17] or ''),
            'sell_reason': str(row[18] or ''),
        })
    return result


def summarize_closed_trades(conn) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    trades = closed_trade_rows(conn)
    closed_count = len(trades)
    wins = [t for t in trades if t['net_pnl'] > 0]
    losses = [t for t in trades if t['net_pnl'] < 0]
    total_net = round(sum(t['net_pnl'] for t in trades), 2)
    avg_win = round(sum(t['net_pnl'] for t in wins) / len(wins), 2) if wins else 0.0
    avg_loss = round(sum(t['net_pnl'] for t in losses) / len(losses), 2) if losses else 0.0
    profit_loss_ratio = round(avg_win / abs(avg_loss), 4) if avg_win > 0 and avg_loss < 0 else 0.0
    expectancy = round(total_net / closed_count, 2) if closed_count else 0.0
    best = max(trades, key=lambda t: t['net_pnl'], default=None)
    worst = min(trades, key=lambda t: t['net_pnl'], default=None)

    initial_capital = float(position_rules().get('initial_capital', 1_000_000.0) or 1_000_000.0)
    equity = initial_capital
    peak = initial_capital
    max_drawdown = 0.0
    for trade in trades:
        equity += float(trade['net_pnl'] or 0)
        peak = max(peak, equity)
        if peak > 0:
            max_drawdown = max(max_drawdown, (peak - equity) / peak)

    summary = {
        'closed_trades': closed_count,
        'win_trades': len(wins),
        'loss_trades': len(losses),
        'win_rate': round(len(wins) / closed_count, 4) if closed_count else 0.0,
        'realized_pnl_amount': total_net,
        'avg_closed_pnl_ratio': round(sum(t['pnl_ratio'] for t in trades) / closed_count, 6) if closed_count else 0.0,
        'profit_loss_ratio': profit_loss_ratio,
        'avg_win_amount': avg_win,
        'avg_loss_amount': avg_loss,
        'best_trade_symbol': best['symbol'] if best else '',
        'best_trade_name': best['name'] if best else '',
        'best_trade_pnl_ratio': best['pnl_ratio'] if best else 0.0,
        'best_trade_pnl_amount': best['net_pnl'] if best else 0.0,
        'worst_trade_symbol': worst['symbol'] if worst else '',
        'worst_trade_name': worst['name'] if worst else '',
        'worst_trade_pnl_ratio': worst['pnl_ratio'] if worst else 0.0,
        'worst_trade_pnl_amount': worst['net_pnl'] if worst else 0.0,
        'expectancy_amount': expectancy,
        'max_drawdown_ratio': round(max_drawdown, 6),
    }
    newest_first = sorted(trades, key=lambda t: (t.get('exit_date') or '', t.get('symbol') or ''), reverse=True)
    return summary, newest_first
