#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Smoke test for P0 force liquidation idempotency + pessimistic slippage."""

from __future__ import annotations

import logging
import runpy
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "storage" / "database" / "zhulong.duckdb"

_loader_ns = runpy.run_path(str(PROJECT_ROOT / "04_governance" / "lib" / "core" / "module_loader.py"))
load_attr_from_path = _loader_ns["load_attr_from_path"]
load_module_from_path = _loader_ns["load_module_from_path"]

DBGateway = load_attr_from_path(
    "db_gateway_smoke_p0",
    PROJECT_ROOT / "01_engine" / "lib" / "db_gateway.py",
    "DBGateway",
)
shadow_engine = load_module_from_path(
    "shadow_engine_smoke_p0",
    PROJECT_ROOT / "05_shadow" / "lib" / "engine.py",
)


def _ensure_tables() -> None:
    with DBGateway(DB_PATH, read_only=False, logger=logging.getLogger("smoke_p0")) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS fact_paper_positions (
                symbol VARCHAR NOT NULL,
                trade_date DATE NOT NULL,
                entry_price DOUBLE,
                highest_price DOUBLE,
                dynamic_stop_price DOUBLE,
                status VARCHAR DEFAULT 'HOLD',
                exit_price DOUBLE,
                pnl_ratio DOUBLE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (symbol, trade_date)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS fact_paper_liquidation_guard (
                idempotency_key VARCHAR PRIMARY KEY,
                trade_date DATE NOT NULL,
                symbol VARCHAR,
                run_id VARCHAR,
                trigger_reason VARCHAR,
                sell_basis_price DOUBLE,
                sell_price DOUBLE,
                note VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )


def _seed_hold(symbol: str, trade_date: str, entry_price: float = 10.0) -> None:
    global_key = f"{trade_date}_MELTDOWN_ALL"
    symbol_key = f"{trade_date}_{symbol}_FORCE_SELL"

    with DBGateway(DB_PATH, read_only=False, logger=logging.getLogger("smoke_p0")) as conn:
        conn.execute(
            "DELETE FROM fact_paper_positions WHERE symbol = ? AND CAST(trade_date AS DATE) = CAST(? AS DATE)",
            [symbol, trade_date],
        )
        conn.execute(
            "DELETE FROM fact_paper_liquidation_guard WHERE idempotency_key IN (?, ?)",
            [global_key, symbol_key],
        )
        conn.execute(
            """
            INSERT INTO fact_paper_positions (
                symbol,
                trade_date,
                entry_price,
                highest_price,
                dynamic_stop_price,
                status,
                exit_price,
                pnl_ratio,
                created_at,
                updated_at
            )
            VALUES (
                ?, CAST(? AS DATE), ?, ?, ?, 'HOLD', NULL, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            )
            """,
            [symbol, trade_date, entry_price, entry_price, round(entry_price * 0.92, 4)],
        )


def _read_verification(symbol: str, trade_date: str):
    symbol_key = f"{trade_date}_{symbol}_FORCE_SELL"
    with DBGateway(DB_PATH, read_only=True, logger=logging.getLogger("smoke_p0")) as conn:
        sold = conn.execute(
            """
            SELECT symbol,
                   CAST(trade_date AS VARCHAR) AS trade_date,
                   status,
                   entry_price,
                   exit_price,
                   pnl_ratio
            FROM fact_paper_positions
            WHERE symbol = ?
              AND CAST(trade_date AS DATE) = CAST(? AS DATE)
            """,
            [symbol, trade_date],
        ).fetchone()

        guard = conn.execute(
            """
            SELECT idempotency_key,
                   sell_basis_price,
                   sell_price,
                   note
            FROM fact_paper_liquidation_guard
            WHERE idempotency_key = ?
            """,
            [symbol_key],
        ).fetchone()
    return sold, guard


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | [%(name)s] %(message)s")

    trade_date = datetime.now().strftime("%Y-%m-%d")
    symbol = "SMOKE_P0_999999"

    _ensure_tables()
    _seed_hold(symbol, trade_date, entry_price=10.0)

    print("===P0_SMOKE_BEGIN===")
    print(f"DB_PATH={DB_PATH}")
    print(f"TRADE_DATE={trade_date}")
    print(f"SYMBOL={symbol}")

    run1 = shadow_engine.force_liquidate_positions(run_id="SMOKE_P0_RUN1", trigger_reason="KILL_SWITCH")
    run2 = shadow_engine.force_liquidate_positions(run_id="SMOKE_P0_RUN2", trigger_reason="KILL_SWITCH")

    sold, guard = _read_verification(symbol, trade_date)

    print(f"RUN1_STATS={run1}")
    print(f"RUN2_STATS={run2}")
    print("===IDEMPOTENCY_CHECK===")
    print(f"RUN2_DUPLICATE_BLOCKED={run2.get('duplicate_blocked')}")

    print("===SOLD_RECORD===")
    print(f"SOLD_ROW={sold}")

    print("===SLIPPAGE_AUDIT===")
    print(f"GUARD_ROW={guard}")

    ok_duplicate = int(run2.get("duplicate_blocked", 0)) == 1
    ok_sold = bool(sold and str(sold[2]) == "SOLD")
    ok_penalty = bool(guard and guard[3] and "-1.5%" in str(guard[3]))

    print("===P0_SMOKE_RESULT===")
    print(f"OK_DUPLICATE={ok_duplicate}")
    print(f"OK_SOLD={ok_sold}")
    print(f"OK_PENALTY_LOG={ok_penalty}")

    if ok_duplicate and ok_sold and ok_penalty:
        print("SMOKE_RESULT=PASS")
        return 0

    print("SMOKE_RESULT=FAIL")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
