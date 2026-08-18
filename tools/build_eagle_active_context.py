#!/usr/bin/env python3
"""Build an observation-only post-close Eagle context artifact."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

import duckdb


ROOT = Path(__file__).resolve().parents[1]
TACTICS_DIR = ROOT / "03_tactics"
if str(TACTICS_DIR) not in sys.path:
    sys.path.insert(0, str(TACTICS_DIR))

from eagle_active_context import build_context_manifest, write_artifacts


DEFAULT_DB = ROOT / "storage" / "database" / "zhulong.duckdb"
DEFAULT_RAW_DIR = ROOT / "storage" / "reports" / "eagle_active_path" / "manifests"
DEFAULT_OUTPUT_DIR = ROOT / "storage" / "reports" / "eagle_active_context"


def _load_market_rows(conn, trade_date: str) -> List[Dict[str, Any]]:
    dates = [
        str(row[0])[:10]
        for row in conn.execute(
            """
            SELECT trade_date FROM (
                SELECT DISTINCT trade_date
                FROM fact_daily
                WHERE trade_date <= CAST(? AS DATE)
                ORDER BY trade_date DESC
                LIMIT 25
            ) x
            ORDER BY trade_date
            """,
            [trade_date],
        ).fetchall()
    ]
    if not dates or dates[-1] != trade_date:
        raise RuntimeError(f"EAGLE_CONTEXT_FACT_DAILY_MISSING:{trade_date}")
    rows = conn.execute(
        """
        SELECT CAST(d.trade_date AS VARCHAR), d.symbol,
               COALESCE(b.name, d.symbol), COALESCE(b.industry, ''),
               COALESCE(b.market, ''), COALESCE(b.is_st, FALSE),
               d.open, d.high, d.low, d.close, d.pre_close, d.pct_chg,
               d.vol, d.amount, d.turnover_rate, d.ma20, d.vol_ma5,
               r.rps_10
        FROM fact_daily d
        LEFT JOIN fact_stock_basic b ON b.symbol = d.symbol
        LEFT JOIN fact_rps_results r
          ON r.symbol = d.symbol AND r.trade_date = d.trade_date
        WHERE d.trade_date IN (SELECT UNNEST(?::DATE[]))
          AND d.close > 0
        ORDER BY d.symbol, d.trade_date
        """,
        [dates],
    ).fetchall()
    keys = [
        "trade_date", "symbol", "name", "industry", "market", "is_st",
        "open", "high", "low", "close", "pre_close", "pct_chg", "vol",
        "amount", "turnover_rate", "ma20", "vol_ma5", "rps_10",
    ]
    return [dict(zip(keys, row)) for row in rows]


def build_for_date(
    trade_date: str,
    *,
    db_path: Path,
    raw_manifest_path: Path,
    output_dir: Path,
) -> Dict[str, Any]:
    if not raw_manifest_path.exists():
        raise FileNotFoundError(f"EAGLE_CONTEXT_SOURCE_MISSING:{raw_manifest_path}")
    raw_manifest = json.loads(raw_manifest_path.read_text(encoding="utf-8"))
    if str(raw_manifest.get("trade_date") or "")[:10] != trade_date:
        raise ValueError("EAGLE_CONTEXT_SOURCE_DATE_MISMATCH")
    with duckdb.connect(str(db_path), read_only=True) as conn:
        market_rows = _load_market_rows(conn, trade_date)
    manifest = build_context_manifest(raw_manifest, market_rows)
    paths = write_artifacts(manifest, output_dir)
    return {
        "status": "DONE",
        "trade_date": trade_date,
        "source_candidate_count": manifest["source_candidate_count"],
        "route_counts": manifest["route_counts"],
        "content_sha256": manifest["content_sha256"],
        "observation_only": True,
        "no_trade_signal": True,
        **paths,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build post-close Eagle context without writing DuckDB or trade tasks."
    )
    parser.add_argument("--trade-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--raw-manifest", type=Path)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compatibility marker. This tool is always dry-run and observation-only.",
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    trade_date = str(args.trade_date).strip()
    if len(trade_date) != 10:
        raise SystemExit("--trade-date must be YYYY-MM-DD")
    raw_manifest_path = args.raw_manifest or (
        DEFAULT_RAW_DIR / f"eagle_candidates_{trade_date}.json"
    )
    result = build_for_date(
        trade_date,
        db_path=args.db,
        raw_manifest_path=raw_manifest_path,
        output_dir=args.output_dir,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
