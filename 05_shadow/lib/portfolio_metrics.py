#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Canonical Shadow equity-curve reconstruction.

The curve follows the same net convention as Watchtower: realized net PnL from
closed paper positions plus net liquidation value for open positions. Historical
partial exits are recognized on the final exit date because the position table
does not retain every partial-exit date.
"""

from __future__ import annotations

from bisect import bisect_right
from datetime import date, datetime
from typing import Any

try:
    from .costs import calculate_trade_cost
    from .shadow_config import position_rules
except Exception:
    from costs import calculate_trade_cost
    from shadow_config import position_rules


def _as_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _table_exists(conn: Any, table_name: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table_name],
    ).fetchone()
    return bool(row and row[0])


def compute_shadow_equity_curve(conn: Any, through_date: str | date | None = None) -> list[dict[str, Any]]:
    if not _table_exists(conn, "fact_paper_positions"):
        return []
    raw_positions = conn.execute(
        """
        SELECT symbol, trade_date, exit_trade_date, UPPER(COALESCE(status, '')),
               COALESCE(NULLIF(initial_qty, 0), NULLIF(qty, 0), NULLIF(realized_qty, 0), 0),
               COALESCE(qty, 0), COALESCE(entry_price, 0),
               COALESCE(NULLIF(entry_total_cost, 0), entry_price * COALESCE(NULLIF(initial_qty, 0), NULLIF(qty, 0), NULLIF(realized_qty, 0), 0), 0),
               COALESCE(realized_net_pnl, 0), COALESCE(realized_net_amount, 0)
        FROM fact_paper_positions
        WHERE trade_date IS NOT NULL
        ORDER BY trade_date, symbol
        """
    ).fetchall()
    if not raw_positions:
        return []

    positions = []
    for row in raw_positions:
        initial_qty = int(row[4] or 0)
        current_qty = int(row[5] or 0)
        entry_cost = float(row[7] or 0)
        net_pnl = float(row[8] or 0)
        if not net_pnl and float(row[9] or 0) and entry_cost:
            net_pnl = float(row[9] or 0) - entry_cost
        positions.append({
            "symbol": str(row[0] or "").strip().upper(),
            "entry_date": _as_date(row[1]),
            "exit_date": _as_date(row[2]),
            "status": str(row[3] or ""),
            "initial_qty": initial_qty,
            "current_qty": current_qty,
            "entry_price": float(row[6] or 0),
            "entry_cost": entry_cost,
            "net_pnl": net_pnl,
        })

    first_date = min(item["entry_date"] for item in positions if item["entry_date"])
    requested_end = _as_date(through_date)
    if requested_end is None:
        candidates = [item["exit_date"] or item["entry_date"] for item in positions]
        if _table_exists(conn, "fact_daily"):
            row = conn.execute("SELECT MAX(trade_date) FROM fact_daily").fetchone()
            if row and row[0]:
                candidates.append(_as_date(row[0]))
        requested_end = max(item for item in candidates if item)

    curve_dates: set[date] = {first_date, requested_end}
    for item in positions:
        if item["entry_date"] and first_date <= item["entry_date"] <= requested_end:
            curve_dates.add(item["entry_date"])
        if item["exit_date"] and first_date <= item["exit_date"] <= requested_end:
            curve_dates.add(item["exit_date"])
    if _table_exists(conn, "fact_daily"):
        for row in conn.execute(
            "SELECT DISTINCT trade_date FROM fact_daily WHERE trade_date BETWEEN ? AND ? ORDER BY trade_date",
            [first_date, requested_end],
        ).fetchall():
            parsed = _as_date(row[0])
            if parsed:
                curve_dates.add(parsed)
    dates = sorted(curve_dates)

    price_dates: dict[str, list[date]] = {}
    price_values: dict[str, list[float]] = {}
    symbols = sorted({item["symbol"] for item in positions if item["symbol"]})
    if symbols and _table_exists(conn, "fact_daily"):
        marks = ",".join(["?"] * len(symbols))
        price_rows = conn.execute(
            f"SELECT symbol, trade_date, close FROM fact_daily WHERE symbol IN ({marks}) AND trade_date <= ? ORDER BY symbol, trade_date",
            symbols + [requested_end],
        ).fetchall()
        for symbol, trade_day, close in price_rows:
            if close is None:
                continue
            key = str(symbol or "").strip().upper()
            price_dates.setdefault(key, []).append(_as_date(trade_day))
            price_values.setdefault(key, []).append(float(close))

    def latest_price(item: dict[str, Any], trade_day: date) -> float:
        if trade_day == item["entry_date"]:
            return float(item["entry_price"] or 0)
        symbol_dates = price_dates.get(item["symbol"]) or []
        idx = bisect_right(symbol_dates, trade_day) - 1
        if idx >= 0:
            return float(price_values[item["symbol"]][idx])
        return float(item["entry_price"] or 0)

    initial_capital = float(position_rules().get("initial_capital", 1_000_000.0) or 1_000_000.0)
    peak = initial_capital
    curve = []
    for trade_day in dates:
        realized_pnl = sum(
            item["net_pnl"] for item in positions
            if item["exit_date"] is not None and item["exit_date"] <= trade_day
        )
        active = [
            item for item in positions
            if item["entry_date"] <= trade_day and (item["exit_date"] is None or trade_day < item["exit_date"])
        ]
        active_cost = 0.0
        liquidation_value = 0.0
        valued_active_positions = 0
        for item in active:
            if item["exit_date"] is None and item["status"] == "HOLD":
                qty = int(item["current_qty"] or item["initial_qty"] or 0)
            else:
                qty = int(item["initial_qty"] or 0)
            if qty <= 0:
                continue
            valued_active_positions += 1
            initial_qty = max(1, int(item["initial_qty"] or qty))
            cost_basis = float(item["entry_cost"] or 0) * qty / initial_qty
            price = latest_price(item, trade_day)
            active_cost += cost_basis
            if price > 0:
                liquidation_value += float(calculate_trade_cost("SELL", price=price, qty=qty).net_amount)
        cash_reserve = initial_capital + realized_pnl - active_cost
        total_equity = cash_reserve + liquidation_value
        peak = max(peak, total_equity)
        drawdown = (peak - total_equity) / peak if peak > 0 else 0.0
        curve.append({
            "trade_date": trade_day.isoformat(),
            "total_equity": round(total_equity, 2),
            "cash_reserve": round(cash_reserve, 2),
            "daily_drawdown": round(drawdown, 6),
            "active_positions": valued_active_positions,
            "realized_pnl": round(realized_pnl, 2),
            "net_liquidation_value": round(liquidation_value, 2),
        })
    return curve


def rebuild_shadow_metrics(conn: Any, through_date: str | date | None = None, replace: bool = False) -> list[dict[str, Any]]:
    curve = compute_shadow_equity_curve(conn, through_date=through_date)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS shadow_metrics (
            trade_date DATE PRIMARY KEY,
            total_equity DOUBLE,
            cash_reserve DOUBLE,
            daily_drawdown DOUBLE,
            active_positions INTEGER,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    if replace:
        conn.execute("DELETE FROM shadow_metrics")
    if curve:
        conn.executemany(
            """
            INSERT INTO shadow_metrics (trade_date,total_equity,cash_reserve,daily_drawdown,active_positions,updated_at)
            VALUES (CAST(? AS DATE),?,?,?,?,CURRENT_TIMESTAMP)
            ON CONFLICT (trade_date) DO UPDATE SET
                total_equity=EXCLUDED.total_equity,
                cash_reserve=EXCLUDED.cash_reserve,
                daily_drawdown=EXCLUDED.daily_drawdown,
                active_positions=EXCLUDED.active_positions,
                updated_at=EXCLUDED.updated_at
            """,
            [[row["trade_date"], row["total_equity"], row["cash_reserve"], row["daily_drawdown"], row["active_positions"]] for row in curve],
        )
    return curve
