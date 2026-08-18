#!/usr/bin/env python3
"""Read-only, same-price-model L4 counterfactual scorecard for BL-021."""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import os
import re
import statistics
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "storage/database/zhulong.duckdb"
DEFAULT_BINDINGS = ROOT / "storage/reports/audit_contracts"
DEFAULT_OUTPUT = ROOT / "storage/reports/l4_counterfactual"
SCHEMA_VERSION = "bl021_l4_counterfactual_scorecard_v0.1"
PRICE_MODEL = "T1_OPEN_T3_CLOSE_GROSS_PROXY_V0.1"
REVIEW_SAMPLE_GATE = 50
SAFE_NEWS_STATUSES = {"NEWS_CLEAR", "NEWS_SIGNAL"}
RISK_NEWS_GATES = {"WOULD_CAP_HOLD", "WOULD_VETO"}
VERDICTS = {"PASS", "HOLD", "VETO"}
ST_NAME_RE = re.compile(r"^(?:\*?ST|SST|PT)", re.IGNORECASE)
BLOCKED_ACTIONS = [
    "write_duckdb",
    "write_shadow",
    "write_rag_memory",
    "write_nexus_audits",
    "change_l1_l4",
    "change_thresholds",
    "generate_trade",
    "trigger_daemon",
    "auto_promote_policy",
]


def _load_contract_module():
    path = ROOT / "02_brain/lib/audit_contract.py"
    spec = importlib.util.spec_from_file_location("zhulong_l4_scorecard_contract", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"audit contract module unavailable: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


CONTRACT = _load_contract_module()


def _canonical_sha256(value: Any) -> str:
    return CONTRACT.canonical_sha256(value)


def _git_head() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return "UNKNOWN"


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


def load_verified_bindings(directory: Path) -> tuple[dict[tuple[str, str], dict], list[str]]:
    bindings: dict[tuple[str, str], dict] = {}
    warnings: list[str] = []
    conflicted: set[tuple[str, str]] = set()
    if not directory.exists():
        return bindings, [f"binding directory missing: {directory}"]
    for path in sorted(directory.glob("audit_contract_*.json")):
        try:
            artifact = json.loads(path.read_text(encoding="utf-8"))
            if not CONTRACT.verify_bound_artifact(artifact):
                raise ValueError("artifact verification failed")
            binding = dict(artifact["binding"])
            key = (str(binding["trade_date"]), str(binding["run_id"]))
            record = {
                **binding,
                "artifact_name": path.name,
                "artifact_sha256": _canonical_sha256(artifact),
            }
            if key in bindings and bindings[key]["binding_sha256"] != record["binding_sha256"]:
                conflicted.add(key)
                bindings.pop(key, None)
                warnings.append(f"conflicting binding artifacts for {key[0]} / {key[1]}")
            elif key not in conflicted:
                bindings[key] = record
        except Exception as exc:
            warnings.append(f"invalid binding artifact {path.name}: {type(exc).__name__}:{exc}")
    return bindings, warnings


def _load_audits(conn: duckdb.DuckDBPyConnection, start: str, end: str) -> list[dict]:
    rows = conn.execute(
        """
        SELECT task_id, run_id, symbol, COALESCE(name, ''),
               CAST(trade_date AS VARCHAR),
               UPPER(COALESCE(l4_final_verdict, '')),
               COALESCE(l4_final_score, final_score, 0),
               UPPER(COALESCE(l4_news_status, '')),
               UPPER(COALESCE(l4_news_gate, '')),
               COALESCE(l4_news_as_of, '')
        FROM nexus_audits
        WHERE CAST(trade_date AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
          AND UPPER(COALESCE(status, '')) = 'L4_DONE'
        ORDER BY trade_date, run_id, task_id
        """,
        [start, end],
    ).fetchall()
    keys = [
        "task_id", "run_id", "symbol", "name", "trade_date", "verdict",
        "final_score", "news_status", "news_gate", "news_as_of",
    ]
    return [dict(zip(keys, row)) for row in rows if str(row[5]) in VERDICTS]


def _stock_basic(conn: duckdb.DuckDBPyConnection) -> dict[str, dict]:
    return {
        str(row[0]): {"name": str(row[1] or ""), "market": str(row[2] or ""), "is_st": bool(row[3])}
        for row in conn.execute(
            "SELECT symbol, name, market, COALESCE(is_st, FALSE) FROM fact_stock_basic"
        ).fetchall()
    }


def _next_sessions(conn: duckdb.DuckDBPyConnection, trade_date: str) -> list[str]:
    return [
        str(row[0])[:10]
        for row in conn.execute(
            """
            SELECT CAST(cal_date AS VARCHAR)
            FROM fact_trade_calendar
            WHERE exchange='SSE' AND is_open=TRUE
              AND CAST(cal_date AS DATE)>CAST(? AS DATE)
            ORDER BY cal_date LIMIT 3
            """,
            [trade_date],
        ).fetchall()
    ]


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
        """,
        [symbol, *sessions],
    ).fetchall()
    return {
        str(row[0])[:10]: {
            "open": _float(row[1]), "high": _float(row[2]), "low": _float(row[3]),
            "close": _float(row[4]), "vol": _float(row[5]), "amount": _float(row[6]),
        }
        for row in rows
    }


def evaluate_row(
    conn: duckdb.DuckDBPyConnection,
    row: Mapping[str, Any],
    stock_basic: Mapping[str, Mapping[str, Any]],
    bindings: Mapping[tuple[str, str], Mapping[str, Any]],
    max_daily_date: str,
) -> dict[str, Any]:
    symbol = str(row["symbol"])
    name = str(row.get("name") or "")
    trade_date = str(row["trade_date"])[:10]
    run_id = str(row["run_id"])
    verdict = str(row["verdict"]).upper()
    binding = dict(bindings.get((trade_date, run_id)) or {})
    provenance = "BOUND_VERIFIED" if binding else "UNBOUND_LEGACY_OR_DISABLED"
    reasons: list[str] = []
    basic = dict(stock_basic.get(symbol) or {})
    if not basic:
        reasons.append("STOCK_BASIC_MISSING")
    if symbol.upper().endswith(".BJ") or str(basic.get("market") or "") == "北交所":
        reasons.append("ACCOUNT_BJ_INELIGIBLE")
    if ST_NAME_RE.match(name.strip()) or bool(basic.get("is_st")):
        reasons.append("ST_INELIGIBLE")
    news_status = str(row.get("news_status") or "").upper()
    news_gate = str(row.get("news_gate") or "").upper()
    news_as_of = str(row.get("news_as_of") or "").strip()
    if news_gate in RISK_NEWS_GATES:
        reasons.append("NEWS_RISK_BLOCK")
    elif news_status not in SAFE_NEWS_STATUSES:
        reasons.append("NEWS_CHECK_INCOMPLETE")
    elif not news_as_of:
        reasons.append("NEWS_AS_OF_MISSING")
    if binding and news_as_of != str(binding.get("evidence_as_of") or ""):
        reasons.append("NEWS_AS_OF_BINDING_MISMATCH")

    sessions = _next_sessions(conn, trade_date)
    if len(sessions) < 3:
        reasons.append("TRADE_CALENDAR_HORIZON_INCOMPLETE")
    daily = _daily_window(conn, symbol, sessions)
    if len(sessions) >= 3 and sessions[2] > max_daily_date:
        reasons.append("NOT_MATURED_T3")
    elif len(sessions) >= 3 and any(session not in daily for session in sessions):
        reasons.append("DAILY_WINDOW_INCOMPLETE")

    entry = daily.get(sessions[0], {}) if sessions else {}
    if entry and (
        entry["open"] <= 0 or entry["vol"] <= 0 or entry["amount"] <= 0
    ):
        reasons.append("T1_NOT_TRADABLE")
    if entry and entry["high"] > 0 and abs(entry["high"] - entry["low"]) < 1e-9:
        reasons.append("T1_ONE_PRICE")

    exclusion_codes = {
        "STOCK_BASIC_MISSING", "ACCOUNT_BJ_INELIGIBLE", "ST_INELIGIBLE",
        "NEWS_RISK_BLOCK", "NEWS_CHECK_INCOMPLETE", "NEWS_AS_OF_MISSING",
        "NEWS_AS_OF_BINDING_MISMATCH", "T1_NOT_TRADABLE", "T1_ONE_PRICE",
    }
    maturity_codes = {
        "TRADE_CALENDAR_HORIZON_INCOMPLETE", "NOT_MATURED_T3", "DAILY_WINDOW_INCOMPLETE",
    }
    if any(reason in exclusion_codes for reason in reasons):
        evaluation_status = "EXCLUDED_SAFETY_OR_EXECUTION"
    elif any(reason in maturity_codes for reason in reasons):
        evaluation_status = "NOT_MATURED_OR_INCOMPLETE"
    elif not binding:
        evaluation_status = "DIAGNOSTIC_UNBOUND_MATURED"
    else:
        evaluation_status = "ELIGIBLE_BOUND_MATURED"

    entry_price = entry.get("open", 0.0) if entry else 0.0
    exit_price = daily.get(sessions[2], {}).get("close", 0.0) if len(sessions) >= 3 else 0.0
    gross_return = None
    mfe = None
    mae = None
    if entry_price > 0 and exit_price > 0 and len(daily) == 3:
        gross_return = round((exit_price / entry_price - 1) * 100, 4)
        mfe = round((max(daily[day]["high"] for day in sessions) / entry_price - 1) * 100, 4)
        mae = round((min(daily[day]["low"] for day in sessions) / entry_price - 1) * 100, 4)
    return {
        **dict(row),
        "trade_date": trade_date,
        "provenance_status": provenance,
        "contract_sha256": str(binding.get("contract_sha256") or ""),
        "binding_sha256": str(binding.get("binding_sha256") or ""),
        "price_model": PRICE_MODEL,
        "t1_date": sessions[0] if sessions else "",
        "t3_date": sessions[2] if len(sessions) >= 3 else "",
        "entry_price": entry_price,
        "exit_price": exit_price,
        "gross_return_pct": gross_return,
        "mfe_pct": mfe,
        "mae_pct": mae,
        "evaluation_status": evaluation_status,
        "exclusion_reasons": sorted(set(reasons)),
    }


def _group_stats(rows: list[dict]) -> dict[str, dict]:
    groups: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        groups[str(row["verdict"])].append(row)
    result: dict[str, dict] = {}
    for verdict in sorted(VERDICTS):
        items = groups.get(verdict, [])
        returns = [item["gross_return_pct"] for item in items]
        result[verdict] = {
            "samples": len(items),
            "avg_gross_return_pct": _mean(returns),
            "median_gross_return_pct": _median(returns),
            "win_rate": round(sum(value > 0 for value in returns) / len(returns), 4) if returns else None,
            "avg_mfe_pct": _mean(item["mfe_pct"] for item in items),
            "avg_mae_pct": _mean(item["mae_pct"] for item in items),
        }
    return result


def _actual_shadow_summary(conn: duckdb.DuckDBPyConnection, task_ids: list[str]) -> dict:
    if not task_ids:
        return {"positions": 0, "closed": 0, "avg_gross_pnl_ratio": None}
    placeholders = ",".join("?" for _ in task_ids)
    rows = conn.execute(
        f"""
        SELECT signal_task_id, status, pnl_ratio_gross
        FROM fact_paper_positions
        WHERE signal_task_id IN ({placeholders})
        """,
        task_ids,
    ).fetchall()
    closed = [row for row in rows if str(row[1] or "").upper() in {"CLOSED", "SOLD"}]
    return {
        "positions": len(rows),
        "closed": len(closed),
        "avg_gross_pnl_ratio": _mean(row[2] for row in closed),
        "comparison_boundary": "actual PASS execution is reported separately and never mixed into proxy groups",
    }


def build_scorecard(
    db_path: Path, binding_dir: Path, start: str, end: str
) -> dict[str, Any]:
    bindings, warnings = load_verified_bindings(binding_dir)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        audits = _load_audits(conn, start, end)
        basic = _stock_basic(conn)
        max_daily = str(conn.execute("SELECT CAST(MAX(trade_date) AS VARCHAR) FROM fact_daily").fetchone()[0])[:10]
        rows = [evaluate_row(conn, row, basic, bindings, max_daily) for row in audits]
        actual = _actual_shadow_summary(
            conn,
            [row["task_id"] for row in rows if row["verdict"] == "PASS"],
        )
    status_counts = Counter(row["evaluation_status"] for row in rows)
    reason_counts = Counter(reason for row in rows for reason in row["exclusion_reasons"])
    bound = [row for row in rows if row["evaluation_status"] == "ELIGIBLE_BOUND_MATURED"]
    unbound = [row for row in rows if row["evaluation_status"] == "DIAGNOSTIC_UNBOUND_MATURED"]
    summary = {
        "audit_rows": len(rows),
        "verified_run_bindings": len(bindings),
        "status_counts": dict(sorted(status_counts.items())),
        "exclusion_reason_counts": dict(sorted(reason_counts.items())),
        "exclusion_reason_counts_overlap": True,
        "bound_versioned_by_verdict": _group_stats(bound),
        "unbound_diagnostic_by_verdict": _group_stats(unbound),
        "actual_shadow_pass_execution": actual,
        "review_gate_samples": REVIEW_SAMPLE_GATE,
        "review_status": (
            "READY_FOR_FIRST_DESIGN_REVIEW"
            if len(bound) >= REVIEW_SAMPLE_GATE
            else "NOT_ENOUGH_BOUND_MATURED_SAMPLES"
        ),
    }
    identity = {
        "schema_version": SCHEMA_VERSION,
        "start_date": start,
        "end_date": end,
        "price_model": PRICE_MODEL,
        "binding_shas": sorted(str(item["binding_sha256"]) for item in bindings.values()),
        "rows": rows,
        "summary": summary,
    }
    return {
        **identity,
        "generated_at": datetime.now().astimezone().isoformat(),
        "mode": "read_only_observer",
        "observer_only": True,
        "no_trade_signal": True,
        "code_version": _git_head(),
        "warnings": warnings,
        "blocked_actions": BLOCKED_ACTIONS,
        "artifact_identity_sha256": _canonical_sha256(identity),
    }


def render_markdown(payload: Mapping[str, Any]) -> str:
    summary = payload["summary"]
    lines = [
        f"# L4 Counterfactual Scorecard - {payload['start_date']} to {payload['end_date']}",
        "", "## Safety", "",
        "- Read-only observer; no database, verdict, Shadow, RAG, threshold, or trading mutation.",
        f"- Common proxy: `{PRICE_MODEL}`; suspended/one-price sessions are never shifted forward.",
        "- Only BOUND_VERIFIED matured rows enter versioned comparison; legacy rows are diagnostic only.",
        "- Actual PASS execution is reported separately and is never compared against proxy HOLD/VETO returns.",
        "", "## Status", "",
        f"- audit_rows: `{summary['audit_rows']}`",
        f"- verified_run_bindings: `{summary['verified_run_bindings']}`",
        f"- review_status: `{summary['review_status']}`",
        f"- artifact_identity_sha256: `{payload['artifact_identity_sha256']}`",
        "", "## Versioned Bound Samples", "",
        "| Verdict | Samples | Avg % | Median % | Win rate | MFE % | MAE % |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for verdict, item in summary["bound_versioned_by_verdict"].items():
        def fmt(value):
            return "" if value is None else f"{value:.4f}"
        lines.append(
            f"| {verdict} | {item['samples']} | {fmt(item['avg_gross_return_pct'])} | "
            f"{fmt(item['median_gross_return_pct'])} | {fmt(item['win_rate'])} | "
            f"{fmt(item['avg_mfe_pct'])} | {fmt(item['avg_mae_pct'])} |"
        )
    lines.extend(["", "## Exclusions", "", "| Reason | Count |", "|---|---:|"])
    for reason, count in summary["exclusion_reason_counts"].items():
        lines.append(f"| {reason} | {count} |")
    lines.extend([
        "", "## Interpretation", "",
        f"The first design review requires at least {REVIEW_SAMPLE_GATE} bound matured samples. "
        "Reaching that gate permits review only; it never promotes a policy or changes production automatically.",
    ])
    return "\n".join(lines) + "\n"


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temp.write_text(text, encoding="utf-8")
    os.replace(temp, path)


def write_artifacts(payload: Mapping[str, Any], output_dir: Path, batch: str) -> tuple[Path, Path]:
    json_path = output_dir / f"l4_counterfactual_scorecard_{batch}.json"
    md_path = output_dir / f"l4_counterfactual_scorecard_{batch}.md"
    _atomic_write(json_path, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    _atomic_write(md_path, render_markdown(payload))
    return json_path, md_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the read-only BL-021 L4 scorecard")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--binding-dir", default=str(DEFAULT_BINDINGS))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--batch", default="")
    parser.add_argument("--write-artifacts", action="store_true")
    args = parser.parse_args()
    payload = build_scorecard(
        Path(args.db), Path(args.binding_dir), args.start_date, args.end_date
    )
    result = {
        "summary": payload["summary"],
        "artifact_identity_sha256": payload["artifact_identity_sha256"],
        "wrote_artifacts": False,
    }
    if args.write_artifacts:
        batch = args.batch.strip() or f"{args.start_date}_{args.end_date}"
        json_path, md_path = write_artifacts(payload, Path(args.output_dir), batch)
        result.update({"json": str(json_path), "markdown": str(md_path), "wrote_artifacts": True})
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
