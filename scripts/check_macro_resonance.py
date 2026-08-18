#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Lightweight probe for Macro Resonance V1 health.
"""

from __future__ import annotations

import argparse
import runpy
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))
if str(PROJECT_ROOT / "01_engine") not in sys.path:
    sys.path.append(str(PROJECT_ROOT / "01_engine"))
if str(PROJECT_ROOT / "01_engine" / "lib") not in sys.path:
    sys.path.append(str(PROJECT_ROOT / "01_engine" / "lib"))


def _load_module(path: Path, module_name: str):
    return load_module_from_path(module_name, path)


from config.settings import Config

_dbgw_mod = _load_module(PROJECT_ROOT / "01_engine" / "lib" / "db_gateway.py", "db_gateway_macro_probe")
DBGateway = _dbgw_mod.DBGateway


def _normalize_date(value: str) -> str:
    text = str(value or "").strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10]
    raise ValueError(f"Unsupported trade_date format: {value}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Macro Resonance V1 lightweight health probe")
    parser.add_argument("--trade_date", type=str, default=datetime.now().strftime("%Y-%m-%d"))
    parser.add_argument("--max_anti_fake_ratio", type=float, default=0.40)
    parser.add_argument("--max_top5_flagged", type=int, default=3)
    args = parser.parse_args()

    trade_date = _normalize_date(args.trade_date)

    try:
        with DBGateway(Config.DB_PATH, read_only=True, logger=None) as conn:
            top5_count = int(
                conn.execute(
                    "SELECT COUNT(*) FROM fact_macro_top5_daily WHERE trade_date = CAST(? AS DATE)",
                    [trade_date],
                ).fetchone()[0]
                or 0
            )
            top5_flagged = int(
                conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM fact_macro_top5_daily
                    WHERE trade_date = CAST(? AS DATE)
                      AND COALESCE(anti_fake_flag, 0) = 1
                    """,
                    [trade_date],
                ).fetchone()[0]
                or 0
            )
            total_topics = int(
                conn.execute(
                    "SELECT COUNT(*) FROM fact_macro_topic_daily WHERE trade_date = CAST(? AS DATE)",
                    [trade_date],
                ).fetchone()[0]
                or 0
            )
            flagged_topics = int(
                conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM fact_macro_topic_daily
                    WHERE trade_date = CAST(? AS DATE)
                      AND COALESCE(anti_fake_flag, 0) = 1
                    """,
                    [trade_date],
                ).fetchone()[0]
                or 0
            )
    except Exception as exc:
        print(f"[CRITICAL] macro probe query failed: {exc}")
        return 2

    ratio = (float(flagged_topics) / float(total_topics)) if total_topics > 0 else 0.0

    print(
        "[PROBE] trade_date={} top5_count={} top5_flagged={} total_topics={} flagged_topics={} anti_fake_ratio={:.2%}".format(
            trade_date,
            top5_count,
            top5_flagged,
            total_topics,
            flagged_topics,
            ratio,
        )
    )

    if top5_count <= 0:
        print("[CRITICAL] fact_macro_top5_daily has no rows for this trade date")
        return 2

    if ratio > float(args.max_anti_fake_ratio) or top5_flagged > int(args.max_top5_flagged):
        print("[WARN] anti-fake pressure is elevated")
        return 1

    print("[OK] macro resonance health is normal")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
