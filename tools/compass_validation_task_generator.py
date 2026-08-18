#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generate dry-run stock validation tasks from human-reviewed Compass candidates.

The tool consumes only the immutable output of compass_discovery_review.py. It
does not query DuckDB, call the decision engine, or execute any validation task.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "storage" / "reports" / "compass_ingest"
BEIJING_TZ = timezone(timedelta(hours=8))
INPUT_VERSION = "compass_reviewed_candidates_v0.3"
OUTPUT_VERSION = "compass_reviewed_validation_tasks_v0.3"
ELIGIBLE_EVIDENCE_LEVELS = {"medium", "strong"}
ELIGIBLE_MAPPING_EVIDENCE_LEVELS = {"L1", "L2", "L3"}
READONLY_MODULES = [
    "fact_stock_basic",
    "fact_daily",
    "fact_rps_results",
    "fact_zeta_signals",
    "fact_quantile_snapshot",
    "financial_snapshots_readonly",
]
ALLOWED_ACTIONS = ["generate_dry_run_validation_task", "render_preview"]
BLOCKED_ACTIONS = [
    "trade",
    "write_shadow",
    "write_rag_memory",
    "write_nexus_audits",
    "write_duckdb",
    "trigger_daemon",
    "call_decision_engine",
    "call_nexus_run",
    "execute_validation_task",
    "auto_promote_to_trade_candidate",
]
REQUIRED_REVIEW_BLOCKS = {
    "trade",
    "write_shadow",
    "write_rag_memory",
    "write_nexus_audits",
    "write_duckdb",
    "trigger_daemon",
    "call_decision_engine",
    "call_nexus_run",
}
TASK_GATE_CHECKS = [
    "reviewed_artifact_sha256_matches",
    "source_is_human_reviewed_theme_discovery",
    "market_is_a_share",
    "ticker_manual_resolved",
    "business_relevance_confirmed",
    "immutable_mapping_evidence_bound",
    "evidence_medium_or_strong",
    "dry_run_preview_reviewed",
    "manual_review_required_false",
    "validation_as_of_bound_to_review_manifest",
    "no_trade_signal_true",
]


def now_iso() -> str:
    return datetime.now(BEIJING_TZ).isoformat(timespec="seconds")


def clean(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def safe_batch(value: Any) -> str:
    return re.sub(r"[^0-9A-Za-zW_-]", "", clean(value) or "batch") or "batch"


def sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def canonical_sha256(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return sha256_bytes(raw)


def unique(values: list[Any]) -> list[Any]:
    output = []
    seen = set()
    for value in values:
        key = json.dumps(value, ensure_ascii=False, sort_keys=True) if isinstance(value, (dict, list)) else clean(value)
        if key and key not in seen:
            seen.add(key)
            output.append(value)
    return output


def validate_timestamp(value: Any, field: str) -> None:
    text = clean(value)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} must be timezone-aware ISO-8601") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field} must be timezone-aware ISO-8601")


def validate_date(value: Any, field: str) -> str:
    text = clean(value)
    try:
        datetime.strptime(text, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"{field} must be YYYY-MM-DD") from exc
    return text


def validate_reviewed_artifact(payload: dict[str, Any], actual_sha256: str, expected_sha256: str) -> list[dict[str, Any]]:
    if not re.fullmatch(r"[0-9a-f]{64}", clean(expected_sha256).lower()):
        raise ValueError("expected reviewed artifact SHA256 must be 64 lowercase hex characters")
    if actual_sha256 != clean(expected_sha256).lower():
        raise ValueError("reviewed artifact SHA256 mismatch")
    if payload.get("output_version") != INPUT_VERSION:
        raise ValueError(f"unsupported reviewed artifact version: {payload.get('output_version')}")
    if payload.get("mode") != "dry_run" or payload.get("dry_run") is not True or payload.get("no_trade_signal") is not True:
        raise ValueError("reviewed artifact must be dry_run with no_trade_signal=true")
    if payload.get("validation_tasks") not in (None, []):
        raise ValueError("reviewed artifact must not contain validation tasks")
    if int((payload.get("stats") or {}).get("generated_tasks") or 0) != 0:
        raise ValueError("reviewed artifact generated_tasks must be zero")
    if not REQUIRED_REVIEW_BLOCKS.issubset(set(payload.get("blocked_actions") or [])):
        raise ValueError("reviewed artifact does not preserve required blocked actions")
    if not clean(payload.get("reviewer_id")):
        raise ValueError("reviewer_id is required")
    validate_timestamp(payload.get("reviewed_at"), "reviewed_at")
    source_as_of = validate_date(
        payload.get("compass_source_as_of"), "compass_source_as_of"
    )
    validation_as_of = validate_date(
        payload.get("validation_as_of"), "validation_as_of"
    )
    if validation_as_of < source_as_of:
        raise ValueError("validation_as_of cannot precede compass_source_as_of")

    candidates = payload.get("reviewed_candidates") or []
    if int((payload.get("stats") or {}).get("reviewed_candidates") or 0) != len(candidates):
        raise ValueError("reviewed candidate count does not match stats")
    seen_ids = set()
    for item in candidates:
        candidate_id = clean(item.get("reviewed_candidate_id"))
        if not candidate_id or candidate_id in seen_ids:
            raise ValueError(f"missing or duplicate reviewed_candidate_id: {candidate_id}")
        seen_ids.add(candidate_id)
        symbol = clean(item.get("ticker"))
        checks = {
            "origin": item.get("origin") == "human_reviewed_theme_discovery",
            "source_object_class": item.get("source_object_class") == "theme_candidate",
            "market": item.get("market") == "A股",
            "ticker": bool(re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", symbol)),
            "ticker_status": item.get("ticker_status") == "manual_resolved_from_discovery_preview",
            "business_relevance": item.get("business_relevance_evidence_present") is True,
            "business_relevance_status": item.get("business_relevance_status") == "human_confirmed",
            "mapping_pool": item.get("mapping_pool") == "review_shortlist",
            "mapping_evidence": clean(item.get("mapping_evidence_level")) in ELIGIBLE_MAPPING_EVIDENCE_LEVELS,
            "business_relevance_basis": bool(clean(item.get("business_relevance_basis"))),
            "supporting_evidence": bool(item.get("supporting_evidence_items")),
            "no_market_metric_evidence": not bool(item.get("supporting_metrics")),
            "evidence_level": clean(item.get("evidence_level")).lower() in ELIGIBLE_EVIDENCE_LEVELS,
            "reviewed": item.get("dry_run_preview_reviewed") is True,
            "human_review": item.get("human_review_for_theme_mapping") is True,
            "manual_review": item.get("manual_review_required") is False,
            "eligible": item.get("eligible_for_separate_dry_run_validation_task") is True,
            "generate_task": item.get("generate_task") is False,
            "no_trade_signal": item.get("no_trade_signal") is True,
            "reviewer": clean(item.get("reviewer_id")) == clean(payload.get("reviewer_id")),
            "reviewed_at": clean(item.get("reviewed_at")) == clean(payload.get("reviewed_at")),
            "compass_source_as_of": clean(item.get("compass_source_as_of")) == source_as_of,
            "validation_as_of": clean(item.get("validation_as_of")) == validation_as_of,
        }
        failed = sorted(key for key, passed in checks.items() if not passed)
        if failed:
            raise ValueError(f"unsafe reviewed candidate {candidate_id}: {','.join(failed)}")
        evidence_items = item.get("evidence_items") or []
        evidence_by_id = {
            clean(evidence.get("evidence_item_id")): evidence
            for evidence in evidence_items
            if clean(evidence.get("evidence_item_id"))
        }
        if not evidence_by_id or len(evidence_by_id) != len(evidence_items):
            raise ValueError(f"missing or duplicate immutable evidence item id: {candidate_id}")
        supporting_items = item.get("supporting_evidence_items") or []
        if any(
            clean(evidence.get("evidence_item_id")) not in evidence_by_id
            or canonical_sha256(evidence) != canonical_sha256(
                evidence_by_id[clean(evidence.get("evidence_item_id"))]
            )
            for evidence in supporting_items
        ):
            raise ValueError(f"supporting evidence does not match immutable evidence items: {candidate_id}")
        constraints = item.get("source_discovery_constraints") or {}
        if constraints.get("auto_theme_to_stock_mapping") is not False:
            raise ValueError(f"reviewed candidate lost auto mapping constraint: {candidate_id}")
    return candidates


def make_task(batch_id: str, symbol: str, items: list[dict[str, Any]], seq: int) -> dict[str, Any]:
    questions = unique([
        question
        for item in items
        for question in (item.get("questions_for_zhulong") or []) + (item.get("line_validation_questions") or [])
        if clean(question)
    ])
    return {
        "task_id": f"COMPASS-{safe_batch(batch_id)}-REVIEWED-TASK-{seq:03d}",
        "task_type": "stock_validation",
        "origin": "human_reviewed_theme_discovery",
        "execution_status": "NOT_EXECUTED_DRY_RUN_ARTIFACT",
        "symbol": symbol,
        "name": items[0].get("name"),
        "industry": items[0].get("industry"),
        "market": "A股",
        "market_board": items[0].get("market_board"),
        "is_st_observation": any(bool(item.get("is_st")) for item in items),
        "account_eligibility_not_evaluated": True,
        "source_reviewed_candidate_ids": [item.get("reviewed_candidate_id") for item in items],
        "source_reviewed_candidate_sha256": [canonical_sha256(item) for item in items],
        "source_preview_row_ids": [item.get("source_preview_row_id") for item in items],
        "source_candidate_ids": unique([item.get("source_candidate_id") for item in items]),
        "source_theme_names": unique([item.get("source_theme_name") for item in items]),
        "compass_lines": unique([item.get("compass_line") for item in items]),
        "compass_line_keys": unique([item.get("compass_line_key") for item in items]),
        "priorities": unique([item.get("priority") for item in items]),
        "evidence_levels": unique([item.get("evidence_level") for item in items]),
        "mapping_evidence_levels": unique([item.get("mapping_evidence_level") for item in items]),
        "evidence_sources": unique([source for item in items for source in item.get("evidence_sources") or []]),
        "evidence_items": unique([evidence for item in items for evidence in item.get("evidence_items") or []]),
        "source_memberships": unique([membership for item in items for membership in item.get("source_memberships") or []]),
        "business_relevance_bases": unique([item.get("business_relevance_basis") for item in items]),
        "supporting_evidence_items": unique([
            evidence for item in items for evidence in item.get("supporting_evidence_items") or []
        ]),
        "supply_chain_nodes": unique([value for item in items for value in item.get("supply_chain_nodes") or []]),
        "bottleneck_hypotheses": unique([value for item in items for value in item.get("bottleneck_hypotheses") or []]),
        "discovery_keywords": unique([value for item in items for value in item.get("discovery_keywords") or []]),
        "questions_for_zhulong": questions,
        "validation_objective": "只读验证主题业务相关性、财务质量、估值与交易热度，并寻找能够证伪瓶颈假设的反证。",
        "required_readonly_modules": READONLY_MODULES,
        "compass_source_as_of": items[0].get("compass_source_as_of"),
        "validation_as_of": items[0].get("validation_as_of"),
        "reviewer_id": items[0].get("reviewer_id"),
        "reviewed_at": items[0].get("reviewed_at"),
        "review_notes": unique([item.get("review_notes") for item in items]),
        "review_assertions": {
            "source_is_human_reviewed_theme_discovery": True,
            "ticker_manual_resolved": True,
            "business_relevance_confirmed": True,
            "immutable_mapping_evidence_bound": True,
            "market_metrics_not_used_as_business_evidence": True,
            "dry_run_preview_reviewed": True,
            "manual_review_required": False,
        },
        "gate_checks": TASK_GATE_CHECKS,
        "no_trade_signal": True,
        "allowed_actions": ["read_only_validation", "render_validation_report"],
        "blocked_actions": BLOCKED_ACTIONS,
    }


def build_payload(
    reviewed: dict[str, Any], reviewed_path: Path, actual_sha256: str, expected_sha256: str
) -> dict[str, Any]:
    candidates = validate_reviewed_artifact(reviewed, actual_sha256, expected_sha256)
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in candidates:
        grouped[clean(item.get("ticker"))].append(item)
    tasks = [make_task(reviewed.get("batch_id") or "batch", symbol, grouped[symbol], index + 1)
             for index, symbol in enumerate(sorted(grouped))]
    return {
        "protocol_version": OUTPUT_VERSION,
        "batch_id": reviewed.get("batch_id"),
        "source": "Compass/ima + Zhulong manual review",
        "generated_at": now_iso(),
        "mode": "dry_run",
        "dry_run": True,
        "no_trade_signal": True,
        "tool": "tools/compass_validation_task_generator.py",
        "source_reviewed_artifact": str(reviewed_path),
        "source_reviewed_artifact_sha256": actual_sha256,
        "source_preview_sha256": reviewed.get("source_preview_sha256"),
        "compass_source_as_of": reviewed.get("compass_source_as_of"),
        "validation_as_of": reviewed.get("validation_as_of"),
        "review_manifest_sha256": reviewed.get("review_manifest_sha256"),
        "allowed_actions": ALLOWED_ACTIONS,
        "blocked_actions": BLOCKED_ACTIONS,
        "execution_policy": "artifact_generation_only; tasks are not executed by this tool",
        "stats": {
            "reviewed_candidates": len(candidates),
            "unique_symbols": len(grouped),
            "consolidated_duplicate_rows": len(candidates) - len(grouped),
            "generated_tasks": len(tasks),
            "executed_tasks": 0,
        },
        "validation_tasks": tasks,
    }


def md_table(headers: list[str], rows: list[list[Any]]) -> list[str]:
    output = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        output.append("| " + " | ".join(str(value if value is not None else "").replace("|", "/") for value in row) + " |")
    return output


def render_preview(payload: dict[str, Any]) -> str:
    stats = payload.get("stats") or {}
    lines = [
        f"# Compass Reviewed Validation Task Preview - {payload.get('batch_id')}",
        "",
        "## 1. Batch",
        "",
        f"- source_reviewed_artifact_sha256: `{payload.get('source_reviewed_artifact_sha256')}`",
        f"- source_preview_sha256: `{payload.get('source_preview_sha256')}`",
        f"- compass_source_as_of: `{payload.get('compass_source_as_of')}`",
        f"- validation_as_of: `{payload.get('validation_as_of')}`",
        f"- review_manifest_sha256: `{payload.get('review_manifest_sha256')}`",
        "- mode: `dry_run`",
        "- no_trade_signal: `true`",
        "",
        "## 2. Stats",
        "",
    ]
    lines += md_table(["metric", "count"], [[key, value] for key, value in stats.items()])
    rows = []
    for task in payload.get("validation_tasks") or []:
        rows.append([
            task.get("task_id"), task.get("symbol"), task.get("name"),
            ", ".join(task.get("source_theme_names") or []),
            ", ".join(task.get("evidence_levels") or []),
            len(task.get("questions_for_zhulong") or []),
            task.get("account_eligibility_not_evaluated"),
            task.get("execution_status"),
        ])
    lines += ["", "## 3. Dry-run Validation Tasks", ""] + md_table(
        ["task_id", "symbol", "name", "themes", "evidence", "questions", "eligibility_pending", "status"], rows
    )
    lines += [
        "",
        "## 4. Safety",
        "",
        "These are unexecuted dry-run task artifacts. This tool does not read or write DuckDB, does not call decision_engine or Nexus, does not affect L4, Shadow, RAG, daemon, or trading, and does not treat human-reviewed theme mapping as a buy signal.",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate dry-run validation tasks from reviewed Compass candidates.")
    parser.add_argument("--reviewed", required=True, type=Path)
    parser.add_argument("--expected-sha256", required=True, help="SHA256 printed by compass_discovery_review.py.")
    parser.add_argument("--output-dir", type=Path, default=OUT_DIR)
    parser.add_argument("--tasks-output", type=Path)
    parser.add_argument("--preview-output", type=Path)
    args = parser.parse_args()

    reviewed_path = args.reviewed.resolve()
    raw = reviewed_path.read_bytes()
    reviewed = json.loads(raw.decode("utf-8"))
    payload = build_payload(reviewed, reviewed_path, sha256_bytes(raw), args.expected_sha256)
    output_dir = args.output_dir.resolve()
    tasks_output = args.tasks_output or output_dir / f"compass_reviewed_validation_tasks_{safe_batch(payload['batch_id'])}.json"
    preview_output = args.preview_output or output_dir / f"zhulong_compass_reviewed_validation_tasks_{safe_batch(payload['batch_id'])}.md"
    tasks_output.parent.mkdir(parents=True, exist_ok=True)
    preview_output.parent.mkdir(parents=True, exist_ok=True)
    tasks_output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tasks_artifact_sha256 = sha256_bytes(tasks_output.read_bytes())
    preview_output.write_text(render_preview(payload), encoding="utf-8")
    print(json.dumps({
        "mode": "dry_run",
        "tasks_output": str(tasks_output),
        "tasks_artifact_sha256": tasks_artifact_sha256,
        "preview_output": str(preview_output),
        "stats": payload["stats"],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise SystemExit(f"compass validation task generation failed: {exc}") from None
