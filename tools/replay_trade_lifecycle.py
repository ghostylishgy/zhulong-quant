#!/usr/bin/env python3
"""Read-only point-in-time replay for Trade Lifecycle v0.1."""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
for path in (ROOT, ROOT / "01_engine/lib", ROOT / "05_shadow/lib"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
from db_gateway import DBGateway
from account_eligibility import evaluate_account_eligibility
from trade_contract import build_trade_contract

DB_PATH = str(ROOT / "storage/database/zhulong.duckdb")


def load_observer():
    path = ROOT / "tools/observe_trade_archetypes.py"
    spec = importlib.util.spec_from_file_location("zhulong_replay_archetype_observer", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


OBSERVER = load_observer()


def audit_dates(start_date: str, end_date: str) -> List[str]:
    with DBGateway(DB_PATH, read_only=True) as conn:
        rows = conn.execute(
            """SELECT DISTINCT CAST(trade_date AS VARCHAR) FROM nexus_audits
               WHERE CAST(trade_date AS DATE)>=CAST(? AS DATE)
                 AND CAST(trade_date AS DATE)<=CAST(? AS DATE)
               ORDER BY 1""", [start_date, end_date]).fetchall()
    return [str(row[0])[:10] for row in rows]


def forward_returns(symbol: str, signal_date: str) -> Dict[str, Any]:
    with DBGateway(DB_PATH, read_only=True) as conn:
        rows = conn.execute(
            """SELECT CAST(trade_date AS VARCHAR),COALESCE(open,0),COALESCE(close,0)
               FROM fact_daily WHERE symbol=? AND CAST(trade_date AS DATE)>CAST(? AS DATE)
                 AND COALESCE(open,0)>0 AND COALESCE(close,0)>0
               ORDER BY trade_date LIMIT 10""", [symbol, signal_date]).fetchall()
    if not rows:
        return {"entry_mode": "NO_FORWARD_DATA", "entry_date": "", "entry_price": 0.0}
    entry = float(rows[0][1] or 0)
    result: Dict[str, Any] = {
        "entry_mode": "T1_OPEN_PROXY_NOT_EXECUTION_GRADE",
        "entry_date": str(rows[0][0])[:10], "entry_price": entry,
    }
    for label, index in (("t1", 0), ("t3", 2), ("t5", 4), ("t10", 9)):
        result[label + "_return"] = (
            round((float(rows[index][2]) / entry - 1) * 100, 4)
            if entry > 0 and len(rows) > index else None
        )
    return result


def enrich_row(row: Dict[str, Any]) -> Dict[str, Any]:
    classification = row["classification"]
    eligibility = evaluate_account_eligibility(row["symbol"], row["name"], None)
    contract = build_trade_contract(
        row["task_id"], row["symbol"], row["trade_date"],
        classification["primary_archetype"], classification["confidence"])
    verdict = str(row.get("l4_final_verdict") or "").upper()
    outcome = forward_returns(row["symbol"], row["trade_date"])
    return {
        **row,
        "account_eligibility": eligibility.to_dict(),
        "trade_contract": contract.to_dict(),
        "would_enter_contract_path": bool(
            verdict in {"PASS", "APPROVE"}
            and eligibility.allow_execution and contract.actionable),
        "forward_outcome": outcome,
    }


def avg(values: List[float]) -> float | None:
    clean = [float(x) for x in values if x is not None and math.isfinite(float(x))]
    return round(sum(clean) / len(clean), 4) if clean else None


def summarize(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[row["classification"]["primary_archetype"]].append(row)
    by_archetype = {}
    for name, items in sorted(groups.items()):
        by_archetype[name] = {
            "candidates": len(items),
            "l4_pass": sum(str(x.get("l4_final_verdict") or "").upper() in {"PASS", "APPROVE"} for x in items),
            "account_eligible": sum(bool(x["account_eligibility"]["allow_execution"]) for x in items),
            "contract_actionable": sum(bool(x["trade_contract"]["actionable"]) for x in items),
            "would_enter_contract_path": sum(bool(x["would_enter_contract_path"]) for x in items),
            "avg_t1_return": avg([x["forward_outcome"].get("t1_return") for x in items]),
            "avg_t3_return": avg([x["forward_outcome"].get("t3_return") for x in items]),
            "avg_t5_return": avg([x["forward_outcome"].get("t5_return") for x in items]),
            "avg_t10_return": avg([x["forward_outcome"].get("t10_return") for x in items]),
        }
    return {
        "candidates": len(rows),
        "l4_pass": sum(str(x.get("l4_final_verdict") or "").upper() in {"PASS", "APPROVE"} for x in rows),
        "blocked_account_eligibility": sum(not x["account_eligibility"]["allow_execution"] for x in rows),
        "blocked_unclassified_or_low_confidence": sum(not x["trade_contract"]["actionable"] for x in rows),
        "would_enter_contract_path": sum(x["would_enter_contract_path"] for x in rows),
        "by_archetype": by_archetype,
    }


def render_markdown(payload: Dict[str, Any]) -> str:
    summary = payload["summary"]
    lines = [
        f"# Trade Lifecycle Replay | {payload['start_date']} to {payload['end_date']}",
        "",
        "## Safety",
        "",
        "- Read-only DuckDB replay; no Shadow, RAG, nexus_audits, or daemon mutation.",
        "- Entry price is next-session open proxy, not the production 09:31-09:45 VWAP.",
        "- Results are diagnostic and not backtest-grade or a trade recommendation.",
        "",
        "## Funnel",
        "",
        "| Metric | Count |",
        "|---|---:|",
        f"| Candidates | {summary['candidates']} |",
        f"| L4 PASS | {summary['l4_pass']} |",
        f"| Account-ineligible observations | {summary['blocked_account_eligibility']} |",
        f"| Unclassified/low-confidence | {summary['blocked_unclassified_or_low_confidence']} |",
        f"| Would enter contract path | {summary['would_enter_contract_path']} |",
        "",
        "## By Archetype",
        "",
        "| Archetype | Candidates | L4 PASS | Eligible | Contract | Path | T1 % | T3 % | T5 % | T10 % |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for name, item in summary["by_archetype"].items():
        vals = [item.get("avg_t1_return"), item.get("avg_t3_return"), item.get("avg_t5_return"), item.get("avg_t10_return")]
        fmt = ["" if value is None else f"{value:.2f}" for value in vals]
        lines.append(
            f"| {name} | {item['candidates']} | {item['l4_pass']} | {item['account_eligible']} | "
            f"{item['contract_actionable']} | {item['would_enter_contract_path']} | "
            f"{fmt[0]} | {fmt[1]} | {fmt[2]} | {fmt[3]} |")
    lines.extend([
        "", "## Interpretation Boundary", "",
        "This replay measures classification separation and gate funnel behavior. "
        "It must not be used to tune thresholds until a longer point-in-time sample and realistic fill/exit replay are available.",
    ])
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-date", required=True)
    ap.add_argument("--end-date", required=True)
    ap.add_argument("--limit-per-day", type=int, default=100)
    ap.add_argument("--output-dir", default=str(ROOT / "storage/reports/trade_lifecycle"))
    ap.add_argument("--batch", default="")
    args = ap.parse_args()
    rows: List[Dict[str, Any]] = []
    for trade_date in audit_dates(args.start_date, args.end_date):
        for row in OBSERVER.load_observations(trade_date, "", max(1, args.limit_per_day)):
            rows.append(enrich_row(row))
    batch = args.batch.strip() or f"{args.start_date}_{args.end_date}"
    payload = {
        "batch": batch, "start_date": args.start_date, "end_date": args.end_date,
        "generated_at": datetime.now().astimezone().isoformat(),
        "mode": "read_only_replay", "no_trade_signal": True,
        "entry_proxy": "T1_OPEN_PROXY_NOT_EXECUTION_GRADE",
        "summary": summarize(rows), "rows": rows,
    }
    out = Path(args.output_dir); out.mkdir(parents=True, exist_ok=True)
    json_path = out / f"trade_lifecycle_replay_{batch}.json"
    md_path = out / f"trade_lifecycle_replay_{batch}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    md_path.write_text(render_markdown(payload), encoding="utf-8")
    print(json.dumps({"json": str(json_path), "markdown": str(md_path), "summary": payload["summary"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
