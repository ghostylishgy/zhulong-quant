#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Resolve normalized Compass candidates into dry-run validation tasks.

Input must be normalized JSON produced by compass_report_normalizer.py. This
script performs read-only exact ticker lookup and writes dry-run files only.
"""

from __future__ import annotations

import argparse
import copy
import json
import re
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
ENGINE_LIB = ROOT / "01_engine" / "lib"
if str(ENGINE_LIB) not in sys.path:
    sys.path.insert(0, str(ENGINE_LIB))

from db_gateway import DBGateway  # noqa: E402

DB_PATH = ROOT / "storage" / "database" / "zhulong.duckdb"
OUT_DIR = ROOT / "storage" / "reports" / "compass_ingest"
BEIJING_TZ = timezone(timedelta(hours=8))
ALLOWED = ["ticker_resolve", "generate_validation_task", "render_preview"]
BLOCKED = [
    "trade", "write_shadow", "write_rag_memory", "write_nexus_audits",
    "write_duckdb", "trigger_daemon", "call_decision_engine", "call_nexus_run",
]
READONLY_MODULES = [
    "fact_stock_basic", "fact_daily", "fact_rps_results", "fact_zeta_signals",
    "fact_quantile_snapshot", "financial_snapshots_readonly",
]
HARD_NEGATIVE_OBJECT_CLASSES = {"theme_candidate", "watch_only", "excluded"}
HARD_NEGATIVE_VALIDATION_MODES = {"theme_mapping", "watch_only", "excluded"}
TASK_GATE_CHECKS = [
    "market_is_a_share",
    "ticker_status_resolved",
    "manual_review_required_false",
    "no_trade_signal_true",
    "not_theme_watch_or_excluded",
    "seed_validation_enabled_by_flag_true",
]


def now_iso() -> str:
    return datetime.now(BEIJING_TZ).isoformat(timespec="seconds")


def safe_batch(batch_id: str) -> str:
    return re.sub(r"[^0-9A-Za-zW_-]", "", str(batch_id or "batch"))


def exact_rows(conn: Any, name: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT symbol, name, industry, market
        FROM fact_stock_basic
        WHERE name = ?
        ORDER BY symbol
        """,
        [name],
    ).fetchall()
    return [{"symbol": r[0], "name": r[1], "industry": r[2], "market_board": r[3]} for r in rows]


def alias_rows(conn: Any, name: str, limit: int = 8) -> list[dict[str, Any]]:
    if not name:
        return []
    rows = conn.execute(
        """
        SELECT symbol, name, industry, market
        FROM fact_stock_basic
        WHERE name LIKE ?
        ORDER BY LENGTH(name), symbol
        LIMIT ?
        """,
        [f"%{name}%", int(limit)],
    ).fetchall()
    return [{"symbol": r[0], "name": r[1], "industry": r[2], "market_board": r[3]} for r in rows]


def hard_negative_reason(item: dict[str, Any]) -> str | None:
    object_class = str(item.get("object_class") or "").strip()
    validation_mode = str(item.get("validation_mode") or "").strip()
    if object_class in HARD_NEGATIVE_OBJECT_CLASSES:
        return f"{object_class}_never_generates_task"
    if validation_mode in HARD_NEGATIVE_VALIDATION_MODES:
        return f"{validation_mode}_never_generates_task"
    return None


def is_seed_anchor(item: dict[str, Any]) -> bool:
    if hard_negative_reason(item):
        return False
    object_class = str(item.get("object_class") or "").strip()
    validation_mode = str(item.get("validation_mode") or "").strip()
    return (
        object_class == "seed_anchor"
        or item.get("origin_role") == "seed_anchor"
        or (validation_mode == "stock_validation" and object_class in {"", "unknown", "seed_anchor"})
    )


def block_item(item: dict[str, Any], reason: str, gate_checks: list[str] | None = None) -> None:
    item["generate_task"] = False
    item["task_block_reason"] = reason
    item["task_gate_checks"] = gate_checks or []

def resolve_candidates(normalized: dict[str, Any], db_path: Path, enable_seed_validation: bool = False) -> list[dict[str, Any]]:
    candidates = copy.deepcopy(normalized.get("candidates") or [])
    with DBGateway(str(db_path), read_only=True) as conn:
        for item in candidates:
            item.setdefault("seed_validation_enabled_by_flag", False)
            item.setdefault("default_generate_validation_task", False)
            item.setdefault("task_gate_checks", [])
            if item.get("no_trade_signal") is not True:
                block_item(item, "no_trade_signal_not_true", ["no_trade_signal_true"])
                continue
            negative_reason = hard_negative_reason(item)
            if negative_reason:
                block_item(item, negative_reason, ["not_theme_watch_or_excluded"])
                continue
            if not is_seed_anchor(item):
                block_item(item, "not_seed_anchor", ["object_class_seed_anchor"])
                continue
            if item.get("market") != "A股":
                item["ticker_status"] = "unsupported_market"
                block_item(item, "market_not_a_share", ["market_is_a_share"])
                continue
            if item.get("manual_review_required") is True:
                block_item(item, "manual_review_required", ["manual_review_required_false"])
                continue
            name = str(item.get("name") or item.get("raw_name") or "").strip()
            exact = exact_rows(conn, name)
            if len(exact) == 1:
                hit = exact[0]
                should_generate = bool(enable_seed_validation)
                gates = TASK_GATE_CHECKS if should_generate else TASK_GATE_CHECKS[:-1] + ["seed_validation_enabled_by_flag_false"]
                item.update({
                    "ticker": hit["symbol"],
                    "ticker_status": "resolved",
                    "ticker_source": "fact_stock_basic.name_exact",
                    "resolved_name": hit["name"],
                    "resolved_industry": hit["industry"],
                    "manual_review_required": False,
                    "generate_task": should_generate,
                    "task_block_reason": None if should_generate else "seed_anchor_record_only",
                    "task_gate_checks": gates,
                    "seed_validation_enabled_by_flag": should_generate,
                    "default_generate_validation_task": False,
                })
            elif len(exact) > 1:
                item.update({
                    "ticker_status": "ambiguous",
                    "alias_suggestions": exact,
                    "manual_review_required": True,
                    "generate_task": False,
                    "task_block_reason": "ambiguous_exact_match",
                    "task_gate_checks": ["ticker_exact_or_manual_resolved"],
                    "seed_validation_enabled_by_flag": bool(enable_seed_validation),
                })
            else:
                item.update({
                    "ticker_status": "unresolved",
                    "alias_suggestions": alias_rows(conn, name),
                    "manual_review_required": True,
                    "generate_task": False,
                    "task_block_reason": "exact_match_not_found",
                    "task_gate_checks": ["ticker_exact_or_manual_resolved"],
                    "seed_validation_enabled_by_flag": bool(enable_seed_validation),
                })
    return candidates

def make_task(batch_id: str, item: dict[str, Any], seq: int) -> dict[str, Any]:
    questions = []
    for question in (item.get("questions_for_zhulong") or []) + (item.get("line_validation_questions") or []):
        if question and question not in questions:
            questions.append(question)
    return {
        "task_id": f"COMPASS-{safe_batch(batch_id)}-TASK-{seq:03d}",
        "task_type": "stock_validation",
        "origin": "seed_anchor_confirmed",
        "source_candidate_id": item.get("candidate_id"),
        "source_object_class": item.get("object_class"),
        "origin_role": item.get("origin_role"),
        "source_discovery_status": item.get("discovery_status"),
        "source_discovery_constraints": item.get("discovery_constraints"),
        "gate_checks": item.get("task_gate_checks") or TASK_GATE_CHECKS,
        "seed_validation_enabled_by_flag": True,
        "symbol": item.get("ticker"),
        "name": item.get("resolved_name") or item.get("name"),
        "raw_name": item.get("raw_name"),
        "compass_line": item.get("compass_line"),
        "compass_line_key": item.get("compass_line_key"),
        "priority": item.get("priority"),
        "required_readonly_modules": READONLY_MODULES,
        "questions_for_zhulong": questions,
        "thesis": item.get("thesis"),
        "benefit_mechanism": item.get("benefit_mechanism"),
        "risks": item.get("risks") or [],
        "source_section": item.get("source_section"),
        "source_row_index": item.get("source_row_index"),
        "no_trade_signal": True,
        "allowed_actions": ["read_only_validation", "render_report"],
        "blocked_actions": BLOCKED,
    }

def build_payload(
    normalized: dict[str, Any],
    resolved: list[dict[str, Any]],
    normalized_path: Path,
    enable_seed_validation: bool = False,
) -> dict[str, Any]:
    batch_id = normalized.get("batch_id") or "batch"
    ready = [item for item in resolved if item.get("generate_task") is True]
    tasks = [make_task(batch_id, item, index + 1) for index, item in enumerate(ready)]
    records = []
    for item in resolved:
        records.append({
            "candidate_id": item.get("candidate_id"),
            "name": item.get("name"),
            "market": item.get("market"),
            "object_class": item.get("object_class"),
            "origin_role": item.get("origin_role"),
            "validation_mode": item.get("validation_mode"),
            "compass_line": item.get("compass_line"),
            "compass_line_key": item.get("compass_line_key"),
            "generated_by_ima": item.get("generated_by_ima"),
            "used_as_discovery_hint": item.get("used_as_discovery_hint"),
            "discovery_status": item.get("discovery_status"),
            "discovery_constraints": item.get("discovery_constraints"),
            "default_generate_validation_task": item.get("default_generate_validation_task"),
            "seed_validation_enabled_by_flag": item.get("seed_validation_enabled_by_flag"),
            "ticker_status": item.get("ticker_status"),
            "ticker": item.get("ticker"),
            "ticker_source": item.get("ticker_source"),
            "manual_review_required": item.get("manual_review_required"),
            "generate_task": item.get("generate_task"),
            "task_block_reason": item.get("task_block_reason"),
            "task_gate_checks": item.get("task_gate_checks") or [],
            "alias_suggestions": item.get("alias_suggestions") or [],
            "downgraded_from": item.get("downgraded_from"),
            "downgrade_reason": item.get("downgrade_reason"),
            "unsupported_reason": item.get("unsupported_reason"),
        })
    status_counts = Counter(str(item.get("ticker_status") or "unknown") for item in resolved)
    mode_counts = Counter(str(item.get("validation_mode") or "unknown") for item in resolved)
    class_counts = Counter(str(item.get("object_class") or "unknown") for item in resolved)
    discovery_status_counts = Counter(str(item.get("discovery_status") or "unknown") for item in resolved)
    seed_anchors = [item for item in resolved if is_seed_anchor(item)]
    discovery_hints = [item for item in resolved if item.get("used_as_discovery_hint")]
    return {
        "batch_id": batch_id,
        "source": normalized.get("source") or "Compass/ima",
        "source_report": normalized.get("source_report"),
        "source_as_of": normalized.get("source_as_of"),
        "source_normalized": str(normalized_path),
        "generated_at": now_iso(),
        "mode": "dry_run",
        "no_trade_signal": True,
        "seed_validation_enabled": bool(enable_seed_validation),
        "discovery_protocol": normalized.get("discovery_protocol"),
        "allowed_actions": ALLOWED,
        "blocked_actions": BLOCKED,
        "stats": {
            "total_candidates": len(resolved),
            "generated_tasks": len(tasks),
            "blocked_candidates": sum(1 for item in resolved if not item.get("generate_task")),
            "manual_review_required": sum(1 for item in resolved if item.get("manual_review_required")),
            "seed_anchors": len(seed_anchors),
            "seed_anchors_resolved": sum(1 for item in seed_anchors if item.get("ticker_status") == "resolved"),
            "seed_anchors_record_only": sum(1 for item in seed_anchors if item.get("task_block_reason") == "seed_anchor_record_only"),
            "discovery_hints": len(discovery_hints),
            "theme_discovery_hints": sum(1 for item in discovery_hints if item.get("object_class") == "theme_candidate"),
            "by_object_class": dict(sorted(class_counts.items())),
            "by_discovery_status": dict(sorted(discovery_status_counts.items())),
            "by_validation_mode": dict(sorted(mode_counts.items())),
            "by_ticker_status": dict(sorted(status_counts.items())),
        },
        "tasks": tasks,
        "resolution_records": records,
    }

def md_table(headers: list[str], rows: list[list[Any]]) -> list[str]:
    out = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        out.append("| " + " | ".join(str(value if value is not None else "").replace("|", "/") for value in row) + " |")
    return out


def render_preview(payload: dict[str, Any]) -> str:
    records = payload["resolution_records"]
    protocol = payload.get("discovery_protocol") or {}
    lines = [
        f"# Compass Ingest Preview - {payload['batch_id']}", "",
        "## 1. Batch", "",
        f"- source_report: `{payload.get('source_report')}`",
        f"- source_as_of: `{payload.get('source_as_of')}`",
        f"- source_normalized: `{payload.get('source_normalized')}`",
        f"- mode: `{payload['mode']}`",
        f"- seed_validation_enabled: `{str(payload.get('seed_validation_enabled')).lower()}`",
        f"- discovery_protocol: `{protocol.get('version')}`",
        "- no_trade_signal: `true`", "",
        "## 2. Parse / Resolve Stats", "",
    ]
    st = payload["stats"]
    rows = [
        ["total_candidates", st["total_candidates"]],
        ["generated_tasks", st["generated_tasks"]],
        ["blocked_candidates", st["blocked_candidates"]],
        ["seed_anchors", st["seed_anchors"]],
        ["seed_anchors_resolved", st["seed_anchors_resolved"]],
        ["seed_anchors_record_only", st["seed_anchors_record_only"]],
        ["discovery_hints", st["discovery_hints"]],
        ["theme_discovery_hints", st["theme_discovery_hints"]],
    ]
    rows += [[f"class:{k}", v] for k, v in st["by_object_class"].items()]
    rows += [[f"discovery:{k}", v] for k, v in st["by_discovery_status"].items()]
    rows += [[f"mode:{k}", v] for k, v in st["by_validation_mode"].items()]
    rows += [[f"ticker:{k}", v] for k, v in st["by_ticker_status"].items()]
    rows.append(["manual_review_required", st["manual_review_required"]])
    lines += md_table(["type", "count"], rows)
    seed_rows = [
        [i["name"], i["ticker_status"], i.get("ticker"), i.get("generate_task"), i.get("task_block_reason"), i.get("discovery_status"), i.get("manual_review_required")]
        for i in records
        if is_seed_anchor(i)
    ]
    lines += ["", "## 3. Seed Anchor Resolve", ""] + md_table(["name", "ticker_status", "symbol", "generate_task", "block_reason", "discovery_status", "manual_review_required"], seed_rows)
    discovery_rows = []
    for item in records:
        if not item.get("used_as_discovery_hint"):
            continue
        constraints = item.get("discovery_constraints") or {}
        discovery_rows.append([
            item["name"],
            item.get("object_class"),
            item.get("compass_line_key"),
            item.get("discovery_status"),
            ", ".join(constraints.get("market_scope") or []),
            constraints.get("auto_theme_to_stock_mapping"),
            constraints.get("manual_review_required_before_task"),
        ])
    lines += ["", "## 4. Discovery Hints (No Tasks)", ""] + md_table(["name", "object_class", "Compass_line", "discovery_status", "market_scope", "auto_theme_to_stock_mapping", "manual_review_before_task"], discovery_rows)
    manual_rows = []
    for item in records:
        if not item.get("manual_review_required"):
            continue
        suggestions = ", ".join(f"{s.get('name')} {s.get('symbol')}" for s in item.get("alias_suggestions") or [])
        manual_rows.append([item["name"], item.get("ticker_status"), item.get("task_block_reason"), suggestions])
    lines += ["", "## 5. Manual Review", ""] + md_table(["name", "ticker_status", "reason", "alias_suggestions"], manual_rows)
    blocked_rows = [[i["name"], i.get("object_class"), i.get("validation_mode"), i.get("ticker_status"), i.get("discovery_status"), i.get("downgrade_reason") or i.get("unsupported_reason") or i.get("task_block_reason")] for i in records if not i.get("generate_task")]
    lines += ["", "## 6. Blocked From Stock Validation Tasks", ""] + md_table(["name", "object_class", "validation_mode", "ticker_status", "discovery_status", "reason"], blocked_rows)
    task_rows = [[t["task_id"], t["task_type"], t.get("origin"), t.get("source_discovery_status"), t["symbol"], t["name"], ", ".join(t["required_readonly_modules"])] for t in payload.get("tasks") or []]
    lines += ["", "## 7. Validation Tasks To Emit", ""] + md_table(["task_id", "task_type", "origin", "source_discovery_status", "symbol", "name", "required_readonly_modules"], task_rows)
    lines += ["", "## 8. Safety", "", "This run writes dry-run files only. Discovery hints do not map themes to stocks and do not generate stock_validation tasks. --enable-seed-validation is not a force flag: it only opens the final gate for otherwise eligible A-share seed anchors with exact/manual ticker resolution, no_trade_signal=true, manual_review_required=false, and no hard-negative object class. It does not write DuckDB, Shadow, RAG, or nexus_audits; it does not trigger daemon, decision_engine, or Nexus.run; it is not a trade signal.", ""]
    return "\n".join(lines)

def main() -> None:
    parser = argparse.ArgumentParser(description="Resolve normalized Compass candidates into dry-run validation tasks.")
    parser.add_argument("--normalized", required=True, type=Path)
    parser.add_argument("--db-path", type=Path, default=DB_PATH)
    parser.add_argument("--output-dir", type=Path, default=OUT_DIR)
    parser.add_argument("--tasks-output", type=Path)
    parser.add_argument("--preview-output", type=Path)
    parser.add_argument("--enable-seed-validation", action="store_true", help="Generate stock_validation tasks for exact-resolved A-share seed anchors. Defaults to record_only.")
    args = parser.parse_args()
    normalized_path = args.normalized.resolve()
    normalized = json.loads(normalized_path.read_text(encoding="utf-8"))
    if normalized.get("mode") != "dry_run" or normalized.get("no_trade_signal") is not True:
        raise SystemExit("normalized input must be dry_run with no_trade_signal=true")
    resolved = resolve_candidates(normalized, args.db_path.resolve(), enable_seed_validation=args.enable_seed_validation)
    payload = build_payload(normalized, resolved, normalized_path, enable_seed_validation=args.enable_seed_validation)
    output_dir = args.output_dir.resolve()
    tasks = args.tasks_output or output_dir / f"compass_validation_tasks_{safe_batch(payload['batch_id'])}.json"
    preview = args.preview_output or output_dir / f"zhulong_compass_ingest_preview_{safe_batch(payload['batch_id'])}.md"
    tasks.parent.mkdir(parents=True, exist_ok=True)
    preview.parent.mkdir(parents=True, exist_ok=True)
    tasks.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    preview.write_text(render_preview(payload), encoding="utf-8")
    print(json.dumps({"mode": "dry_run", "tasks_output": str(tasks), "preview_output": str(preview), "stats": payload["stats"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
