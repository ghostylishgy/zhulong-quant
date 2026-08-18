#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Preview or rebuild the canonical net Shadow equity curve."""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENGINE_LIB = ROOT / "01_engine" / "lib"
SHADOW_LIB = ROOT / "05_shadow" / "lib"
for path in (ENGINE_LIB, SHADOW_LIB):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from db_gateway import DBGateway  # noqa: E402
from portfolio_metrics import compute_shadow_equity_curve, rebuild_shadow_metrics  # noqa: E402

DB_PATH = ROOT / "storage" / "database" / "zhulong.duckdb"


def summary(curve):
    return {
        "curve_rows": len(curve),
        "first": curve[0] if curve else None,
        "latest": curve[-1] if curve else None,
        "max_drawdown": max((row["daily_drawdown"] for row in curve), default=0.0),
        "generated_tasks": 0,
        "no_trade_signal": True,
    }


def main():
    parser = argparse.ArgumentParser(description="Preview or apply the net Shadow equity-curve rebuild.")
    parser.add_argument("--db-path", type=Path, default=DB_PATH)
    parser.add_argument("--through-date")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    db_path = args.db_path.resolve()

    if not args.apply:
        with DBGateway(str(db_path), read_only=True) as conn:
            curve = compute_shadow_equity_curve(conn, through_date=args.through_date)
        print(json.dumps({"mode": "dry_run", **summary(curve)}, ensure_ascii=False, indent=2))
        return

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if not re.fullmatch(r"\d{8}_\d{6}", stamp):
        raise SystemExit("invalid backup stamp")
    backup_table = f"backup_shadow_metrics_rebuild_{stamp}"
    with DBGateway(str(db_path), read_only=False) as conn:
        conn.execute("BEGIN TRANSACTION")
        try:
            conn.execute(f"CREATE TABLE {backup_table} AS SELECT * FROM shadow_metrics")
            curve = rebuild_shadow_metrics(conn, through_date=args.through_date, replace=True)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    print(json.dumps({"mode": "apply", "backup_table": backup_table, **summary(curve)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
