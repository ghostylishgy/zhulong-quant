#!/usr/bin/env python3
"""Read-only forward outcome review for intraday side-channel signals."""

from __future__ import annotations

import argparse
import json
import statistics
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List

import duckdb

ROOT = Path("/root/quant_project")
DB_PATH = ROOT / "storage/database/zhulong.duckdb"
REPORT_DIR = ROOT / "storage/reports/intraday_signal_review"
HORIZONS = (1, 3, 5)


def _pct(entry: float, exit_price: float) -> float | None:
    if entry <= 0 or exit_price <= 0:
        return None
    return round((exit_price / entry - 1.0) * 100.0, 6)


def summarize(rows: Iterable[Dict[str, Any]], horizon: int) -> Dict[str, Any]:
    values = [float(row[f"return_t{horizon}"]) for row in rows if row.get(f"return_t{horizon}") is not None]
    if not values:
        return {"covered": 0, "mean_pct": None, "median_pct": None, "hit_rate": None}
    return {
        "covered": len(values),
        "mean_pct": round(statistics.fmean(values), 4),
        "median_pct": round(statistics.median(values), 4),
        "hit_rate": round(sum(value > 0 for value in values) / len(values), 4),
        "min_pct": round(min(values), 4),
        "max_pct": round(max(values), 4),
    }


def _load_events(conn, start: str, end: str) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        WITH ranked AS (
            SELECT CAST(trade_date AS VARCHAR) AS trade_date, source, symbol,
                   signal_type, verdict, score, price, CAST(created_at AS VARCHAR) AS created_at,
                   ROW_NUMBER() OVER (
                       PARTITION BY CAST(trade_date AS DATE), source, symbol
                       ORDER BY
                           CASE WHEN signal_type IN ('BREAKOUT_APPROVED', 'EOD_MOMENTUM_BUY', 'ECHO_AWAKENED') THEN 0 ELSE 1 END,
                           created_at ASC,
                           idempotency_key ASC
                   ) AS rn
            FROM fact_intraday_tactic_signals
            WHERE CAST(trade_date AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
              AND COALESCE(price, 0) > 0
        )
        SELECT trade_date, source, symbol, signal_type, verdict, score, price, created_at
        FROM ranked WHERE rn = 1
        ORDER BY trade_date, source, symbol, signal_type
        """,
        [start, end],
    ).fetchall()
    keys = ["trade_date", "source", "symbol", "signal_type", "verdict", "score", "price", "created_at"]
    return [dict(zip(keys, row)) for row in rows]


def _load_daily(conn, symbols: List[str], start: str) -> Dict[str, List[tuple[str, float]]]:
    if not symbols:
        return {}
    placeholders = ",".join(["?"] * len(symbols))
    rows = conn.execute(
        f"""
        SELECT symbol, CAST(trade_date AS VARCHAR), close
        FROM fact_daily
        WHERE symbol IN ({placeholders})
          AND CAST(trade_date AS DATE) > CAST(? AS DATE)
          AND COALESCE(close, 0) > 0
        ORDER BY symbol, trade_date
        """,
        [*symbols, start],
    ).fetchall()
    result: Dict[str, List[tuple[str, float]]] = defaultdict(list)
    for symbol, trade_date, close in rows:
        result[str(symbol)].append((str(trade_date), float(close)))
    return dict(result)


def evaluate(events: List[Dict[str, Any]], daily: Dict[str, List[tuple[str, float]]]) -> List[Dict[str, Any]]:
    evaluated = []
    for event in events:
        future = [row for row in daily.get(str(event["symbol"]), []) if row[0] > str(event["trade_date"])]
        item = dict(event)
        entry = float(event.get("price") or 0)
        for horizon in HORIZONS:
            item[f"return_t{horizon}"] = _pct(entry, future[horizon - 1][1]) if len(future) >= horizon else None
            item[f"date_t{horizon}"] = future[horizon - 1][0] if len(future) >= horizon else None
        first_five = [_pct(entry, close) for _, close in future[:5]]
        valid_first_five = [value for value in first_five if value is not None]
        item["max_drawdown_5d_pct"] = round(min(valid_first_five), 4) if valid_first_five else None
        evaluated.append(item)
    return evaluated


def build_report(rows: List[Dict[str, Any]], start: str, end: str) -> Dict[str, Any]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[f"{row['source']}|{row['signal_type']}"].append(row)

    groups = {}
    for key, items in sorted(grouped.items()):
        groups[key] = {
            "signals": len(items),
            "horizons": {f"t{h}": summarize(items, h) for h in HORIZONS},
            "drawdown_5d_covered": sum(row.get("max_drawdown_5d_pct") is not None for row in items),
            "mean_max_drawdown_5d_pct": round(statistics.fmean(
                [float(row["max_drawdown_5d_pct"]) for row in items if row.get("max_drawdown_5d_pct") is not None]
            ), 4) if any(row.get("max_drawdown_5d_pct") is not None for row in items) else None,
        }

    approved = groups.get("EAGLE|BREAKOUT_APPROVED", {})
    observed = groups.get("EAGLE|BREAKOUT_OBSERVED", {})
    edges = {}
    for horizon in HORIZONS:
        a = ((approved.get("horizons") or {}).get(f"t{horizon}") or {}).get("mean_pct")
        o = ((observed.get("horizons") or {}).get(f"t{horizon}") or {}).get("mean_pct")
        edges[f"t{horizon}_approved_minus_observed_pct"] = round(float(a) - float(o), 4) if a is not None and o is not None else None

    approved_t5 = ((approved.get("horizons") or {}).get("t5") or {})
    enough = int(approved.get("signals") or 0) >= 50 and int(approved_t5.get("covered") or 0) >= 30
    positive_edges = all(value is not None and value > 0 for value in edges.values())
    status = "READY_FOR_DESIGN_REVIEW" if enough and positive_edges else ("INSUFFICIENT_COVERAGE" if not enough else "KEEP_OBSERVING")
    return {
        "source": "fact_intraday_tactic_signals",
        "period": {"start": start, "end": end},
        "dedupe": "one row per trade_date/source/symbol; first approved event when present, otherwise first observation",
        "entry_price": "point-in-time intraday signal price",
        "outcome_price": "future fact_daily close",
        "read_only": True,
        "no_trade_signal": True,
        "rows": len(rows),
        "groups": groups,
        "eagle_approval_edge": edges,
        "review_status": status,
        "blocked_actions": ["write_duckdb", "consume_by_shadow", "generate_buy", "change_tactic_threshold"],
    }


def render_markdown(report: Dict[str, Any]) -> str:
    lines = [
        "# Intraday Signal Forward Outcome Review", "",
        f"- period: {report['period']['start']} to {report['period']['end']}",
        f"- review_status: `{report['review_status']}`",
        "- read_only: true",
        "- no_trade_signal: true", "",
        "| 分组 | 去重信号 | T+1均值 | T+1胜率 | T+3均值 | T+3胜率 | T+5均值 | T+5胜率 | 5日平均最差收益 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for key, group in report["groups"].items():
        values = []
        for horizon in HORIZONS:
            metric = group["horizons"][f"t{horizon}"]
            values.extend([
                "N/A" if metric["mean_pct"] is None else f"{metric['mean_pct']:.2f}%",
                "N/A" if metric["hit_rate"] is None else f"{metric['hit_rate']:.1%}",
            ])
        dd = group["mean_max_drawdown_5d_pct"]
        lines.append(f"| {key} | {group['signals']} | {' | '.join(values)} | {'N/A' if dd is None else f'{dd:.2f}%'} |")
    lines.extend(["", "## Eagle Approval Edge", ""])
    for key, value in report["eagle_approval_edge"].items():
        lines.append(f"- {key}: {'N/A' if value is None else f'{value:.4f}%'}")
    lines.extend(["", "本报告仅评估旁路信号，不授权 Shadow 买入或修改策略阈值。", ""])
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2026-06-01")
    parser.add_argument("--end", default=date.today().isoformat())
    parser.add_argument("--output-dir", default=str(REPORT_DIR))
    args = parser.parse_args()
    with duckdb.connect(str(DB_PATH), read_only=True) as conn:
        events = _load_events(conn, args.start, args.end)
        daily = _load_daily(conn, sorted({str(row["symbol"]) for row in events}), args.start)
    rows = evaluate(events, daily)
    report = build_report(rows, args.start, args.end)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = f"intraday_signal_review_{args.start.replace('-', '')}_{args.end.replace('-', '')}"
    json_path = output_dir / f"{stem}.json"
    md_path = output_dir / f"{stem}.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    md_path.write_text(render_markdown(report), encoding="utf-8")
    print(json.dumps({"json": str(json_path), "markdown": str(md_path), **report}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
