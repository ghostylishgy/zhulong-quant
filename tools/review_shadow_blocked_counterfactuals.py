#!/usr/bin/env python3
"""Read-only forward outcomes for signals blocked before Shadow entry."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import statistics
import subprocess
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "storage/database/zhulong.duckdb"
DEFAULT_OUTPUT = ROOT / "storage/reports/shadow_blocked_counterfactual"
SCHEMA_VERSION = "shadow_blocked_counterfactual_v0.1"
PRICE_MODEL = "T1_OPEN_TO_T1_T3_T5_CLOSE_GROSS_PROXY_V0.1"
HORIZONS = (1, 3, 5)
CATEGORIES = ("NEWS", "TIDE", "ACCOUNT", "ARCHETYPE")
BLOCKED_ACTIONS = [
    "write_duckdb", "write_shadow", "change_historical_skip",
    "generate_position", "generate_trade", "change_l1_l4",
    "change_news_or_tide_policy", "write_rag_memory",
    "write_nexus_audits", "trigger_daemon",
]


def _float(value: Any) -> float:
    try:
        result = float(value)
        return result if math.isfinite(result) else 0.0
    except (TypeError, ValueError):
        return 0.0


def _mean(values: Iterable[Any]) -> float | None:
    clean = [_float(value) for value in values if value is not None]
    return round(sum(clean) / len(clean), 4) if clean else None


def _median(values: Iterable[Any]) -> float | None:
    clean = [_float(value) for value in values if value is not None]
    return round(float(statistics.median(clean)), 4) if clean else None


def _canonical_sha256(value: Any) -> str:
    raw = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _git_head() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return "UNKNOWN"


def _table_exists(conn: duckdb.DuckDBPyConnection, table: str) -> bool:
    return bool(conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name=?", [table]
    ).fetchone()[0])


def classify_block(reason: Any) -> str | None:
    value = str(reason or "").strip().upper()
    if value.startswith("SKIPPED_NEWS_"):
        return "NEWS"
    if value.startswith("TIDE_SUPPRESSED_"):
        return "TIDE"
    if value.startswith("ACCOUNT_INELIGIBLE_"):
        return "ACCOUNT"
    if value.startswith("TRADE_ARCHETYPE_"):
        return "ARCHETYPE"
    return None


def _load_blocked_signals(
    conn: duckdb.DuckDBPyConnection, start: str, end: str
) -> tuple[list[dict], list[str]]:
    rows: dict[str, dict] = {}
    warnings: list[str] = []
    sources = [
        (
            "fact_shadow_skipped_signals",
            """
            SELECT COALESCE(task_id, signal_id), COALESCE(run_id, ''), symbol,
                   COALESCE(name, ''), CAST(trade_date AS VARCHAR),
                   COALESCE(reason, ''), COALESCE(final_score, 0),
                   COALESCE(l1_close, 0), COALESCE(entry_tide_gate, ''),
                   COALESCE(entry_tide_ratio, 0), COALESCE(entry_policy, ''),
                   COALESCE(evidence_json, '{}')
            FROM fact_shadow_skipped_signals
            WHERE CAST(trade_date AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
            ORDER BY trade_date, task_id
            """,
            False,
        ),
        (
            "fact_shadow_pending_signals",
            """
            SELECT COALESCE(task_id, signal_id), COALESCE(run_id, ''), symbol,
                   COALESCE(name, ''), CAST(signal_trade_date AS VARCHAR),
                   COALESCE(last_error, ''), COALESCE(final_score, 0),
                   COALESCE(l1_close, 0), COALESCE(entry_tide_gate, ''),
                   COALESCE(entry_tide_ratio, 0), COALESCE(entry_policy, ''),
                   COALESCE(trade_contract_json, '{}')
            FROM fact_shadow_pending_signals
            WHERE CAST(signal_trade_date AS DATE)
                  BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
              AND UPPER(COALESCE(status, ''))='SKIPPED'
            ORDER BY signal_trade_date, task_id
            """,
            True,
        ),
    ]
    for source_table, query, lower_priority in sources:
        if not _table_exists(conn, source_table):
            warnings.append(f"source table missing: {source_table}")
            continue
        for raw in conn.execute(query, [start, end]).fetchall():
            category = classify_block(raw[5])
            if category is None:
                continue
            task_id = str(raw[0] or "").strip()
            candidate = {
                "task_id": task_id,
                "run_id": str(raw[1] or ""),
                "symbol": str(raw[2] or "").strip().upper(),
                "name": str(raw[3] or ""),
                "signal_trade_date": str(raw[4])[:10],
                "block_reason": str(raw[5] or ""),
                "block_category": category,
                "final_score": _float(raw[6]),
                "l1_close": _float(raw[7]),
                "entry_tide_gate": str(raw[8] or ""),
                "entry_tide_ratio": _float(raw[9]),
                "entry_policy": str(raw[10] or ""),
                "source_table": source_table,
                "source_evidence_sha256": hashlib.sha256(
                    str(raw[11] or "{}").encode("utf-8")
                ).hexdigest(),
            }
            existing = rows.get(task_id)
            if existing and (
                existing["block_category"] != candidate["block_category"]
                or existing["block_reason"] != candidate["block_reason"]
            ):
                warnings.append(
                    f"conflicting blocked-signal sources for {task_id}; "
                    f"{existing['source_table']} retained"
                )
                continue
            if not existing or not lower_priority:
                rows[task_id] = candidate
    return list(rows.values()), warnings


def _next_sessions(
    conn: duckdb.DuckDBPyConnection, trade_date: str, limit: int = 5
) -> list[str]:
    return [str(row[0])[:10] for row in conn.execute(
        """
        SELECT CAST(cal_date AS VARCHAR)
        FROM fact_trade_calendar
        WHERE exchange='SSE' AND is_open=TRUE
          AND CAST(cal_date AS DATE)>CAST(? AS DATE)
        ORDER BY cal_date LIMIT ?
        """, [trade_date, limit],
    ).fetchall()]


def _daily_window(
    conn: duckdb.DuckDBPyConnection, symbol: str, sessions: list[str]
) -> dict[str, dict]:
    if not sessions:
        return {}
    placeholders = ",".join("?" for _ in sessions)
    rows = conn.execute(
        f"""
        SELECT CAST(trade_date AS VARCHAR), open, high, low, close, vol, amount
        FROM fact_daily
        WHERE symbol=? AND CAST(trade_date AS VARCHAR) IN ({placeholders})
        """, [symbol, *sessions],
    ).fetchall()
    return {
        str(row[0])[:10]: {
            "open": _float(row[1]), "high": _float(row[2]), "low": _float(row[3]),
            "close": _float(row[4]), "vol": _float(row[5]), "amount": _float(row[6]),
        }
        for row in rows
    }


def evaluate_signal(
    conn: duckdb.DuckDBPyConnection,
    signal: Mapping[str, Any],
    max_daily_date: str,
) -> dict[str, Any]:
    trade_date = str(signal["signal_trade_date"])[:10]
    sessions = _next_sessions(conn, trade_date, max(HORIZONS))
    daily = _daily_window(conn, str(signal["symbol"]), sessions)
    reasons: list[str] = []
    if len(sessions) < max(HORIZONS):
        reasons.append("TRADE_CALENDAR_HORIZON_INCOMPLETE")
    entry = daily.get(sessions[0], {}) if sessions else {}
    if entry and (entry["open"] <= 0 or entry["vol"] <= 0 or entry["amount"] <= 0):
        reasons.append("T1_NOT_TRADABLE")
    if entry and entry["high"] > 0 and abs(entry["high"] - entry["low"]) < 1e-9:
        reasons.append("T1_ONE_PRICE")
    if sessions and sessions[0] not in daily:
        reasons.append("T1_DAILY_MISSING")

    entry_price = entry.get("open", 0.0) if entry else 0.0
    entry_feasible = (
        entry_price > 0 and entry.get("vol", 0.0) > 0
        and entry.get("amount", 0.0) > 0
        and abs(entry.get("high", 0.0) - entry.get("low", 0.0)) >= 1e-9
    )
    horizon_results: dict[str, dict] = {}
    for horizon in HORIZONS:
        key = f"T{horizon}"
        if len(sessions) < horizon:
            horizon_results[key] = {
                "status": "CALENDAR_INCOMPLETE", "exit_date": "",
                "gross_return_pct": None, "mfe_pct": None, "mae_pct": None,
            }
            continue
        exit_date = sessions[horizon - 1]
        if exit_date > max_daily_date:
            status = "NOT_MATURED"
        elif any(day not in daily for day in sessions[:horizon]):
            status = "DAILY_WINDOW_INCOMPLETE"
        elif not entry_feasible:
            status = "ENTRY_INFEASIBLE"
        else:
            status = "MATURED_PROXY"
        gross_return = mfe = mae = None
        if status == "MATURED_PROXY":
            window = [daily[day] for day in sessions[:horizon]]
            gross_return = round((window[-1]["close"] / entry_price - 1) * 100, 4)
            mfe = round((max(item["high"] for item in window) / entry_price - 1) * 100, 4)
            mae = round((min(item["low"] for item in window) / entry_price - 1) * 100, 4)
        horizon_results[key] = {
            "status": status, "exit_date": exit_date,
            "gross_return_pct": gross_return, "mfe_pct": mfe, "mae_pct": mae,
        }
    return {
        **dict(signal), "price_model": PRICE_MODEL,
        "entry_date": sessions[0] if sessions else "",
        "entry_price": entry_price, "entry_feasible": entry_feasible,
        "horizons": horizon_results, "diagnostic_reasons": sorted(set(reasons)),
    }


def _horizon_stats(rows: list[dict]) -> dict[str, dict]:
    result: dict[str, dict] = {}
    for horizon in HORIZONS:
        key = f"T{horizon}"
        matured = [
            row["horizons"][key] for row in rows
            if row["horizons"][key]["status"] == "MATURED_PROXY"
        ]
        values = [item["gross_return_pct"] for item in matured]
        result[key] = {
            "samples": len(values),
            "avg_gross_return_pct": _mean(values),
            "median_gross_return_pct": _median(values),
            "win_rate": (
                round(sum(value > 0 for value in values) / len(values), 4)
                if values else None
            ),
            "avg_mfe_pct": _mean(item["mfe_pct"] for item in matured),
            "avg_mae_pct": _mean(item["mae_pct"] for item in matured),
        }
    return result


def build_report(db_path: Path, start: str, end: str) -> dict[str, Any]:
    with duckdb.connect(str(db_path), read_only=True) as conn:
        signals, warnings = _load_blocked_signals(conn, start, end)
        max_daily = str(conn.execute(
            "SELECT CAST(MAX(trade_date) AS VARCHAR) FROM fact_daily"
        ).fetchone()[0])[:10]
        rows = [evaluate_signal(conn, signal, max_daily) for signal in signals]

    groups: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        groups[str(row["block_category"])].append(row)
    category_stats = {
        category: {
            "signals": len(groups.get(category, [])),
            "reason_counts": dict(sorted(Counter(
                str(row["block_reason"]) for row in groups.get(category, [])
            ).items())),
            "horizons": _horizon_stats(groups.get(category, [])),
        }
        for category in CATEGORIES
    }
    identity = {
        "schema_version": SCHEMA_VERSION,
        "start_date": start, "end_date": end, "price_model": PRICE_MODEL,
        "rows": rows,
        "summary": {
            "blocked_signals": len(rows),
            "category_counts": dict(sorted(Counter(
                str(row["block_category"]) for row in rows
            ).items())),
            "source_counts": dict(sorted(Counter(
                str(row["source_table"]) for row in rows
            ).items())),
            "category_stats": category_stats,
            "matured_t5_signals": sum(
                row["horizons"]["T5"]["status"] == "MATURED_PROXY" for row in rows
            ),
            "entry_infeasible_signals": sum(not row["entry_feasible"] for row in rows),
            "interpretation_status": (
                "DESCRIPTIVE_ONLY_SMALL_SAMPLE"
                if len(rows) < 30 else "READY_FOR_FIRST_DESCRIPTIVE_REVIEW"
            ),
            "review_gate_signals": 30,
        },
    }
    return {
        **identity, "generated_at": datetime.now().astimezone().isoformat(),
        "mode": "read_only_counterfactual_observer",
        "observer_only": True, "no_trade_signal": True,
        "code_version": _git_head(), "warnings": warnings,
        "blocked_actions": BLOCKED_ACTIONS,
        "artifact_identity_sha256": _canonical_sha256(identity),
    }


def render_markdown(payload: Mapping[str, Any]) -> str:
    summary = payload["summary"]
    lines = [
        f"# Shadow Blocked-Signal Counterfactual - {payload['start_date']} to {payload['end_date']}",
        "", "## Safety", "",
        "- Read-only observer. It never creates a position or changes a historical gate.",
        f"- Price proxy: {PRICE_MODEL}. One-price T+1 sessions are never shifted.",
        "- Operational failures are excluded; only policy blocks are evaluated.",
        "", "## Summary", "",
        f"- blocked_signals: {summary['blocked_signals']}",
        f"- matured_t5_signals: {summary['matured_t5_signals']}",
        f"- entry_infeasible_signals: {summary['entry_infeasible_signals']}",
        f"- interpretation_status: {summary['interpretation_status']}",
        f"- artifact_identity_sha256: {payload['artifact_identity_sha256']}",
        "", "## Results By Gate", "",
        "| Gate | Signals | Horizon | Samples | Avg % | Median % | Win rate | MFE % | MAE % |",
        "|---|---:|---|---:|---:|---:|---:|---:|---:|",
    ]
    for category, item in summary["category_stats"].items():
        for horizon, stats in item["horizons"].items():
            def fmt(value):
                return "" if value is None else f"{value:.4f}"
            lines.append(
                f"| {category} | {item['signals']} | {horizon} | {stats['samples']} | "
                f"{fmt(stats['avg_gross_return_pct'])} | "
                f"{fmt(stats['median_gross_return_pct'])} | "
                f"{fmt(stats['win_rate'])} | {fmt(stats['avg_mfe_pct'])} | "
                f"{fmt(stats['avg_mae_pct'])} |"
            )
    lines.extend([
        "", "## Interpretation", "",
        "This report measures what happened after a signal was blocked. It does not prove "
        "that a gate should be relaxed or strengthened. At least 30 blocked signals permit "
        "a first descriptive review only; policy changes require an independent design review.",
    ])
    return "\n".join(lines) + "\n"


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temp.write_text(text, encoding="utf-8")
    os.replace(temp, path)


def write_artifacts(
    payload: Mapping[str, Any], output_dir: Path, batch: str
) -> tuple[Path, Path]:
    json_path = output_dir / f"shadow_blocked_counterfactual_{batch}.json"
    md_path = output_dir / f"shadow_blocked_counterfactual_{batch}.md"
    _atomic_write(json_path, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    _atomic_write(md_path, render_markdown(payload))
    return json_path, md_path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build read-only outcomes for signals blocked before Shadow entry"
    )
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--batch", default="")
    parser.add_argument("--write-artifacts", action="store_true")
    args = parser.parse_args()
    payload = build_report(Path(args.db), args.start_date, args.end_date)
    result = {
        "summary": payload["summary"],
        "artifact_identity_sha256": payload["artifact_identity_sha256"],
        "wrote_artifacts": False,
    }
    if args.write_artifacts:
        batch = args.batch.strip() or f"{args.start_date}_{args.end_date}"
        json_path, md_path = write_artifacts(payload, Path(args.output_dir), batch)
        result.update({
            "json": str(json_path), "markdown": str(md_path),
            "wrote_artifacts": True,
        })
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
