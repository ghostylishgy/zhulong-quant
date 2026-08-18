#!/usr/bin/env python3
"""Build read-only Strategy Episodes from Zhulong lifecycle records."""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Sequence

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "storage/database/zhulong.duckdb"
REPORT_DIR = ROOT / "storage/reports/strategy_episodes"
VERSION = "strategy_episode_v0.1"
BLOCKED_ACTIONS = ["write_duckdb", "write_rag_memory", "change_l1_l4",
                   "change_strategy", "generate_trade", "write_shadow",
                   "trigger_daemon", "call_llm"]


def pick(row: Mapping[str, Any] | None, *keys: str, default: Any = None) -> Any:
    for key in keys:
        if row and row.get(key) is not None:
            return row[key]
    return default


def json_value(value: Any, default: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value)) if value else default
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def serializable(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): serializable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [serializable(v) for v in value]
    return value


def fetch_dicts(conn, sql: str, params: Sequence[Any] = ()) -> list[Dict[str, Any]]:
    cursor = conn.execute(sql, list(params))
    columns = [item[0] for item in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def table_exists(conn, table: str) -> bool:
    return bool(conn.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name=?", [table]).fetchone()[0])


def load_table(conn, table: str) -> list[Dict[str, Any]]:
    return fetch_dicts(conn, f"SELECT * FROM {table}") if table_exists(conn, table) else []


def latest(rows: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
    values = [dict(row) for row in rows]
    time_keys = ("updated_at", "created_at", "fill_date", "review_date", "trade_date")
    return max(values, key=lambda row: tuple(str(row.get(k) or "") for k in time_keys)) if values else {}


def reason_family(reason: str) -> str:
    value = reason.upper()
    if "NEWS" in value:
        return "NEWS_GATE"
    if any(x in value for x in ("ELIGIB", "ST_", "BEIJING", "BJ_")):
        return "ACCOUNT_ELIGIBILITY"
    if any(x in value for x in ("CONTRACT", "ARCHETYPE", "UNCLASSIFIED")):
        return "CONTRACT_OR_ARCHETYPE"
    if "TIDE" in value or "MARKET" in value:
        return "MARKET_REGIME"
    if "BOARD" in value or "LIMIT" in value:
        return "EXECUTION_CONSTRAINT"
    return "OTHER_ENTRY_GATE"


def deterministic_attribution(pending, skipped, fill, position) -> Dict[str, Any]:
    """Describe the recorded path without claiming that a feature caused it."""
    if skipped:
        reason = str(pick(skipped, "reason", "skip_reason", "status", default="UNKNOWN"))
        return {"outcome_class": "NO_ENTRY", "attribution_category": f"ENTRY_BLOCKED_{reason_family(reason)}",
                "evidence": [reason], "causality_claimed": False}
    fill_status = str(pick(fill, "status", default="")).upper()
    fill_action = str(pick(fill, "action", default="")).upper()
    if not fill or (fill_status and fill_status not in {"FILLED", "SUCCESS"}) or (fill_action and fill_action != "BUY"):
        return {"outcome_class": "NO_ENTRY", "attribution_category": "ENTRY_PENDING_OR_UNFILLED",
                "evidence": [str(pick(pending, "status", default="NO_PENDING_RECORD"))], "causality_claimed": False}
    status = str(pick(position, "status", default="MISSING_POSITION_RECORD")).upper()
    if not position or status in {"ACTIVE", "OPEN", "HOLDING"}:
        return {"outcome_class": "OPEN", "attribution_category": "OPEN_POSITION_UNRESOLVED",
                "evidence": [status], "causality_claimed": False}
    pnl = pick(position, "net_realized_pnl", "realized_net_pnl", "pnl_amount", "pnl_ratio", default=0)
    outcome = "WIN" if float(pnl) > 0 else ("LOSS" if float(pnl) < 0 else "FLAT")
    reason = str(pick(position, "last_sell_reason", "exit_reason", default=""))
    rule = str(pick(position, "last_sell_rule", "sell_rule", default="UNKNOWN_EXIT")).upper()
    reason_upper = reason.upper()
    if rule == "THESIS_CRITICAL_RISK_EXIT": category = "CRITICAL_INFORMATION_RISK"
    elif rule == "WRONG_PICK_STOP": category = "WRONG_PICK_OR_NO_FOLLOW_THROUGH"
    elif rule == "TRAILING_RUNNER_STOP": category = "RUNNER_PROTECTION_EXIT"
    elif "TIME" in rule: category = "TIME_EFFICIENCY_FAILURE"
    elif any(x in rule for x in ("PROFIT", "TAKE")): category = "PROFIT_REALIZATION"
    elif "STOP" in rule: category = "PRICE_RISK_EXIT"
    elif "WRONG-PICK STOP" in reason_upper or "WRONG PICK STOP" in reason_upper: category = "WRONG_PICK_OR_NO_FOLLOW_THROUGH"
    elif "RUNNER STOP" in reason_upper: category = "RUNNER_PROTECTION_EXIT"
    elif any(x in reason_upper for x in ("PROFIT", "TAKE-PROFIT")): category = "PROFIT_REALIZATION"
    elif "TIME" in reason_upper: category = "TIME_EFFICIENCY_FAILURE"
    else: category = "CLOSED_OTHER"
    return {"outcome_class": outcome, "attribution_category": category,
            "evidence": [rule, reason],
            "causality_claimed": False}


def audit_snapshot(audit: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "l1": {"pct_change": pick(audit, "l1_pct_change", "l1_pct_chg"),
               "turnover_rate": pick(audit, "l1_turnover_rate", "turnover_rate")},
        "l2": {"pattern": pick(audit, "l2_pattern", "pattern"),
               "tags": json_value(pick(audit, "l2_tags", "tags_json"), [])},
        "l3": {"verdict": pick(audit, "l3_verdict"), "score": pick(audit, "l3_score"),
               "reason": pick(audit, "l3_reason", "l3_summary"),
               "falsifiable_conditions": json_value(pick(audit, "l3_falsifiable_conditions", "falsifiable_conditions"), [])},
        "l4": {"verdict": pick(audit, "l4_final_verdict", "final_verdict"),
               "score": pick(audit, "l4_final_score", "final_score"),
               "reason": pick(audit, "l4_reason", "l4_summary")},
        "rag": {"used": bool(pick(audit, "rag_used", "rag_context_used", default=False)),
                "similarity": pick(audit, "rag_similarity", "rag_top_similarity")},
        "news": {"status": pick(audit, "l4_news_status"), "gate": pick(audit, "l4_news_gate"),
                 "risk_level": pick(audit, "l4_news_risk_level"), "risk_score": pick(audit, "l4_news_risk_score"),
                 "as_of": pick(audit, "l4_news_as_of")},
    }


def build_episode(audit, archetype, pending, skipped, fill, position, reviews) -> Dict[str, Any]:
    task_id = str(pick(audit, "task_id", default=""))
    return serializable({
        "episode_id": f"STRATEGY_EPISODE:{task_id}", "schema_version": VERSION,
        "task_id": task_id, "run_id": pick(audit, "run_id"), "symbol": pick(audit, "symbol"),
        "name": pick(audit, "name"), "signal_trade_date": pick(audit, "trade_date"),
        "no_trade_signal": True, "read_only": True,
        "ex_ante": {
            "audit": audit_snapshot(audit),
            "archetype": {"primary": pick(archetype, "primary_archetype"),
                          "confidence": pick(archetype, "confidence"),
                          "classifier_version": pick(archetype, "classifier_version"),
                          "features": json_value(pick(archetype, "features_json"), {})},
            "trade_contract": json_value(pick(pending, "trade_contract_json"), {}),
        },
        "execution_lifecycle": {
            "pending": {"signal_id": pick(pending, "signal_id"), "status": pick(pending, "status"),
                        "earliest_fill_date": pick(pending, "earliest_fill_date"),
                        "entry_market_tide": pick(pending, "entry_market_tide")},
            "skip": {"status": pick(skipped, "status"), "reason": pick(skipped, "reason", "skip_reason")} if skipped else None,
            "fill": {"fill_id": pick(fill, "fill_id"), "status": pick(fill, "status"),
                     "action": pick(fill, "action"), "fill_date": pick(fill, "fill_date"),
                     "fill_price": pick(fill, "fill_price", "price"), "quantity": pick(fill, "quantity", "qty"),
                     "net_amount": pick(fill, "net_amount", "allocated_amount"),
                     "data_quality": pick(fill, "data_quality")} if fill else None,
        },
        "post_outcome": {
            "position": {"status": pick(position, "status"), "entry_date": pick(position, "trade_date"),
                         "exit_date": pick(position, "exit_date"), "entry_price": pick(position, "entry_price"),
                         "exit_price": pick(position, "exit_price"), "pnl_ratio": pick(position, "pnl_ratio"),
                         "net_realized_pnl": pick(position, "net_realized_pnl", "realized_net_pnl"),
                         "last_sell_rule": pick(position, "last_sell_rule"),
                         "last_sell_reason": pick(position, "last_sell_reason")} if position else None,
            "thesis_reviews": [{"review_date": pick(r, "trade_date", "review_date"),
                                "thesis_state": pick(r, "thesis_state"),
                                "management_action": pick(r, "management_action"),
                                "thesis_reason": pick(r, "thesis_reason"),
                                "observer_only": pick(r, "observer_only", default=True)} for r in reviews],
        },
        "attribution": deterministic_attribution(pending, skipped, fill, position),
    })


def index(rows, key: str) -> Dict[str, list[Dict[str, Any]]]:
    result = defaultdict(list)
    for row in rows:
        if row.get(key): result[str(row[key])].append(row)
    return result


def load_episodes(conn, start: str, end: str):
    audits = fetch_dicts(conn, "SELECT * FROM nexus_audits WHERE CAST(trade_date AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE) ORDER BY trade_date,task_id", [start, end])
    names = ("fact_trade_archetype_observations", "fact_shadow_pending_signals", "fact_shadow_skipped_signals",
             "fact_shadow_fill_events", "fact_paper_positions", "fact_shadow_thesis_reviews")
    exists = {name: table_exists(conn, name) for name in names}
    tables = {name: load_table(conn, name) for name in names}
    warnings = [f"optional_table_missing:{name}" for name in names if not exists[name]]
    indexed = {name: index(rows, "signal_task_id" if name == "fact_paper_positions" else "task_id")
               for name, rows in tables.items() if name != "fact_shadow_thesis_reviews"}
    reviews = index(tables["fact_shadow_thesis_reviews"], "signal_task_id")
    episodes = []
    for audit in audits:
        task_id = str(audit.get("task_id") or "")
        if not task_id:
            warnings.append(f"audit_missing_task_id:{audit.get('symbol')}:{audit.get('trade_date')}")
            continue
        get = lambda name: latest(indexed[name].get(task_id, []))
        episodes.append(build_episode(audit, get("fact_trade_archetype_observations"),
                                      get("fact_shadow_pending_signals"), get("fact_shadow_skipped_signals"),
                                      get("fact_shadow_fill_events"), get("fact_paper_positions"),
                                      sorted(reviews.get(task_id, []), key=lambda r: str(r.get("trade_date") or ""))))
    return episodes, sorted(set(warnings))


def build_payload(episodes, start: str, end: str, warnings):
    outcomes = Counter(x["attribution"]["outcome_class"] for x in episodes)
    categories = Counter(x["attribution"]["attribution_category"] for x in episodes)
    return {"schema_version": VERSION, "generated_at": datetime.now().astimezone().isoformat(),
            "period": {"start": start, "end": end}, "mode": "read_only_strategy_episode_build",
            "read_only": True, "no_trade_signal": True,
            "post_outcome_isolation": "post_outcome and attribution must never be used as ex_ante features",
            "blocked_actions": BLOCKED_ACTIONS,
            "summary": {"episodes": len(episodes), "outcomes": dict(sorted(outcomes.items())),
                        "attribution_categories": dict(sorted(categories.items()))},
            "warnings": warnings, "episodes": episodes}


def render_markdown(payload) -> str:
    summary = payload["summary"]
    lines = [f"# Strategy Episode Review | {payload['period']['start']} to {payload['period']['end']}", "",
             "- mode: `read_only_strategy_episode_build`", "- no_trade_signal: true", "- LLM called: false",
             "- Post-outcome data is isolated from ex-ante evidence and cannot become a historical feature.", "",
             "## Coverage", "", f"- episodes: {summary['episodes']}",
             f"- outcomes: `{json.dumps(summary['outcomes'], ensure_ascii=False, sort_keys=True)}`", "",
             "## Deterministic Attribution", "", "| Category | Count |", "|---|---:|"]
    lines += [f"| {key} | {value} |" for key, value in summary["attribution_categories"].items()]
    lines += ["", "## Episode Index", "", "| Date | Symbol | Archetype | Entry lifecycle | Outcome | Attribution |",
              "|---|---|---|---|---|---|"]
    for item in payload["episodes"]:
        lifecycle = ((item["execution_lifecycle"]["fill"] or {}).get("status") or
                     item["execution_lifecycle"]["pending"].get("status") or
                     ("SKIPPED" if item["execution_lifecycle"]["skip"] else "NO_ENTRY"))
        lines.append(f"| {item['signal_trade_date']} | {item['symbol']} | {item['ex_ante']['archetype']['primary'] or 'N/A'} | {lifecycle} | {item['attribution']['outcome_class']} | {item['attribution']['attribution_category']} |")
    if payload["warnings"]:
        lines += ["", "## Warnings", ""] + [f"- `{x}`" for x in payload["warnings"]]
    lines += ["", "## Safety Boundary", "",
              "This report describes historical lifecycle evidence. It does not recommend a trade, tune a threshold, write RAG, or promote a strategy.", ""]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", required=True); parser.add_argument("--end-date", required=True)
    parser.add_argument("--batch", default=""); parser.add_argument("--output-dir", default=str(REPORT_DIR))
    args = parser.parse_args()
    with duckdb.connect(str(DB_PATH), read_only=True) as conn:
        episodes, warnings = load_episodes(conn, args.start_date, args.end_date)
    payload = build_payload(episodes, args.start_date, args.end_date, warnings)
    batch = args.batch.strip() or f"{args.start_date}_{args.end_date}"
    out = Path(args.output_dir); out.mkdir(parents=True, exist_ok=True)
    json_path = out / f"strategy_episodes_{batch}.json"; md_path = out / f"strategy_episodes_{batch}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    md_path.write_text(render_markdown(payload), encoding="utf-8")
    print(json.dumps({"json": str(json_path), "markdown": str(md_path), **payload["summary"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__": raise SystemExit(main())
