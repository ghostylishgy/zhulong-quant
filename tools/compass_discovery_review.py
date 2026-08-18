#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Create and apply a manual review manifest for Compass discovery previews.

This is an offline dry-run gate. It can promote an immutable discovery preview
row into a reviewed candidate artifact, but it never generates a validation
task and never writes DuckDB, Shadow, RAG, nexus_audits, or daemon state.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "storage" / "reports" / "compass_ingest"
BEIJING_TZ = timezone(timedelta(hours=8))
MANIFEST_VERSION = "compass_discovery_review_manifest_v0.3"
OUTPUT_VERSION = "compass_reviewed_candidates_v0.3"
SUPPORTED_PREVIEW_PROTOCOL = "compass_theme_discovery_preview_v0.2"
ALLOWED_DECISIONS = {"pending", "approve", "reject", "defer"}
ELIGIBLE_EVIDENCE_LEVELS = {"medium", "strong"}
ELIGIBLE_MAPPING_EVIDENCE_LEVELS = {"L1", "L2", "L3"}
ALLOWED_BUSINESS_RELEVANCE_BASES = {
    "manual_verified_industry_mapping",
    "official_business_description",
    "financial_segment_disclosure",
    "company_announcement",
}

ALLOWED_ACTIONS = ["initialize_review_manifest", "apply_manual_review", "render_review_preview"]
BLOCKED_ACTIONS = [
    "trade",
    "write_shadow",
    "write_rag_memory",
    "write_nexus_audits",
    "write_duckdb",
    "trigger_daemon",
    "call_decision_engine",
    "call_nexus_run",
    "auto_theme_to_stock_mapping",
    "auto_fuzzy_ticker_resolve",
    "generate_validation_task",
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


def md_table(headers: list[str], rows: list[list[Any]]) -> list[str]:
    out = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        out.append("| " + " | ".join(str(value if value is not None else "").replace("|", "/") for value in row) + " |")
    return out


def parse_reviewed_at(value: Any) -> str:
    text = clean(value)
    if not text:
        raise ValueError("reviewed_at is required when a decision is applied")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("reviewed_at must be ISO-8601") from exc
    if parsed.tzinfo is None:
        raise ValueError("reviewed_at must include a timezone")
    return text


def parse_date(value: Any, field: str) -> str:
    text = clean(value)
    try:
        datetime.strptime(text, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"{field} must be YYYY-MM-DD") from exc
    return text


def validate_preview(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    if payload.get("protocol_version") != SUPPORTED_PREVIEW_PROTOCOL:
        raise ValueError(f"unsupported preview protocol: {payload.get('protocol_version')}")
    if payload.get("mode") != "dry_run" or payload.get("no_trade_signal") is not True:
        raise ValueError("discovery preview must be dry_run with no_trade_signal=true")
    if payload.get("validation_tasks") not in (None, []):
        raise ValueError("discovery preview must not contain validation tasks")
    if int((payload.get("stats") or {}).get("generated_tasks") or 0) != 0:
        raise ValueError("discovery preview generated_tasks must be zero")
    parse_date(payload.get("source_as_of"), "preview.source_as_of")

    rows_by_id: dict[str, dict[str, Any]] = {}
    shortlist = payload.get("review_shortlist") or []
    if payload.get("preview_rows") != shortlist:
        raise ValueError("preview_rows must exactly equal review_shortlist")
    for row in payload.get("preview_rows") or []:
        row_id = clean(row.get("preview_row_id"))
        if not row_id or row_id in rows_by_id:
            raise ValueError(f"missing or duplicate preview_row_id: {row_id}")
        if row.get("source_object_class") != "theme_candidate":
            raise ValueError(f"review input row must originate from theme_candidate: {row_id}")
        if row.get("no_trade_signal") is not True or row.get("generate_task") is not False:
            raise ValueError(f"unsafe preview row flags: {row_id}")
        if row.get("manual_review_required") is not True:
            raise ValueError(f"preview row is not awaiting manual review: {row_id}")
        if row.get("mapping_pool") != "review_shortlist" or row.get("review_eligible") is not True:
            raise ValueError(f"preview row is not review_shortlist eligible: {row_id}")
        if row.get("shortlist_eligible") is not True or row.get("sample_only") is not False:
            raise ValueError(f"preview row has unsafe shortlist/sample flags: {row_id}")
        if clean(row.get("mapping_evidence_level")) not in ELIGIBLE_MAPPING_EVIDENCE_LEVELS:
            raise ValueError(f"preview row mapping evidence is not reviewable: {row_id}")
        if row.get("business_relevance_evidence_present") is not False or row.get("business_relevance_status") != "unverified":
            raise ValueError(f"preview row must remain business-relevance unverified: {row_id}")
        if "metrics" in row or "metric_trade_date" in row:
            raise ValueError(f"trading metrics are forbidden in discovery review input: {row_id}")
        evidence_items = row.get("evidence_items") or []
        evidence_ids = [clean(item.get("evidence_item_id")) for item in evidence_items]
        if not evidence_ids or any(not item for item in evidence_ids) or len(evidence_ids) != len(set(evidence_ids)):
            raise ValueError(f"preview row requires unique immutable evidence items: {row_id}")
        if row.get("ticker_status") != "preview_only_not_task_resolved":
            raise ValueError(f"unexpected preview ticker status: {row_id}")
        symbol = clean(row.get("symbol"))
        if not re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", symbol):
            raise ValueError(f"preview row is not an A-share symbol: {row_id}:{symbol}")
        rows_by_id[row_id] = row
    return rows_by_id


def initialize_manifest(preview: dict[str, Any], preview_sha256: str) -> dict[str, Any]:
    rows_by_id = validate_preview(preview)
    decisions = []
    for row_id, row in rows_by_id.items():
        decisions.append({
            "preview_row_id": row_id,
            "preview_row_sha256": canonical_sha256(row),
            "symbol": row.get("symbol"),
            "name": row.get("name"),
            "source_theme_name": row.get("source_theme_name"),
            "evidence_level": row.get("evidence_level"),
            "decision": "pending",
            "business_relevance_confirmed": False,
            "business_relevance_basis": "",
            "supporting_evidence_item_ids": [],
            "review_notes": "",
        })
    return {
        "manifest_version": MANIFEST_VERSION,
        "batch_id": preview.get("batch_id"),
        "source_preview_sha256": preview_sha256,
        "compass_source_as_of": parse_date(
            preview.get("source_as_of"), "preview.source_as_of"
        ),
        "validation_as_of": None,
        "mode": "manual_review",
        "dry_run": True,
        "no_trade_signal": True,
        "reviewer_id": "",
        "reviewed_at": None,
        "instructions": {
            "allowed_decisions": sorted(ALLOWED_DECISIONS),
            "approve_requires": [
                "unchanged_preview_row_hash",
                "evidence_level_medium_or_strong",
                "mapping_evidence_level_L1_or_higher",
                "business_relevance_confirmed_true",
                "allowed_business_relevance_basis",
                "at_least_one_immutable_supporting_evidence_item",
                "non_empty_review_notes",
                "validation_as_of_fixed_in_manifest",
            ],
            "allowed_business_relevance_bases": sorted(ALLOWED_BUSINESS_RELEVANCE_BASES),
            "supporting_evidence_policy": "IDs must reference immutable evidence_items from the shortlisted row",
            "weak_evidence_policy": "weak rows cannot be approved; reject or defer them",
            "output_policy": "reviewed candidates only; no validation tasks are generated",
        },
        "blocked_actions": BLOCKED_ACTIONS,
        "decisions": decisions,
    }


def validate_manifest(
    manifest: dict[str, Any], preview: dict[str, Any], preview_sha256: str, rows_by_id: dict[str, dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    if manifest.get("manifest_version") != MANIFEST_VERSION:
        raise ValueError(f"unsupported review manifest version: {manifest.get('manifest_version')}")
    if clean(manifest.get("batch_id")) != clean(preview.get("batch_id")):
        raise ValueError("review manifest batch does not match preview")
    if clean(manifest.get("source_preview_sha256")) != preview_sha256:
        raise ValueError("review manifest source_preview_sha256 does not match preview")
    source_as_of = parse_date(preview.get("source_as_of"), "preview.source_as_of")
    if clean(manifest.get("compass_source_as_of")) != source_as_of:
        raise ValueError("review manifest compass_source_as_of does not match preview")
    if manifest.get("mode") != "manual_review" or manifest.get("dry_run") is not True or manifest.get("no_trade_signal") is not True:
        raise ValueError("review manifest must be manual_review dry-run with no_trade_signal=true")
    if not set(BLOCKED_ACTIONS).issubset(set(manifest.get("blocked_actions") or [])):
        raise ValueError("review manifest does not preserve required blocked actions")

    decisions: dict[str, dict[str, Any]] = {}
    has_applied_decision = False
    has_approval = False
    for item in manifest.get("decisions") or []:
        row_id = clean(item.get("preview_row_id"))
        if row_id not in rows_by_id:
            raise ValueError(f"review manifest references unknown preview row: {row_id}")
        if row_id in decisions:
            raise ValueError(f"duplicate review decision: {row_id}")
        row = rows_by_id[row_id]
        if clean(item.get("preview_row_sha256")) != canonical_sha256(row):
            raise ValueError(f"preview row hash mismatch: {row_id}")
        decision = clean(item.get("decision")).lower()
        if decision not in ALLOWED_DECISIONS:
            raise ValueError(f"unsupported decision for {row_id}: {decision}")
        if decision != "pending":
            has_applied_decision = True
            if not clean(item.get("review_notes")):
                raise ValueError(f"review_notes required for {decision}: {row_id}")
        if decision == "approve":
            has_approval = True
            evidence_level = clean(row.get("evidence_level")).lower()
            if evidence_level not in ELIGIBLE_EVIDENCE_LEVELS:
                raise ValueError(f"weak/unsupported evidence cannot be approved: {row_id}:{evidence_level}")
            mapping_level = clean(row.get("mapping_evidence_level"))
            if mapping_level not in ELIGIBLE_MAPPING_EVIDENCE_LEVELS:
                raise ValueError(f"mapping evidence cannot be approved: {row_id}:{mapping_level}")
            if item.get("business_relevance_confirmed") is not True:
                raise ValueError(f"business relevance confirmation required: {row_id}")
            basis = clean(item.get("business_relevance_basis"))
            if basis not in ALLOWED_BUSINESS_RELEVANCE_BASES:
                raise ValueError(f"valid business_relevance_basis required: {row_id}:{basis}")
            evidence_ids = {
                clean(evidence.get("evidence_item_id")) for evidence in row.get("evidence_items") or []
            }
            supporting_ids = list(dict.fromkeys(
                clean(value) for value in item.get("supporting_evidence_item_ids") or [] if clean(value)
            ))
            invalid_ids = sorted(set(supporting_ids) - evidence_ids)
            if not supporting_ids or invalid_ids:
                raise ValueError(f"valid immutable supporting evidence required: {row_id}:{invalid_ids}")
            item = copy.deepcopy(item)
            item["business_relevance_basis"] = basis
            item["supporting_evidence_item_ids"] = supporting_ids
        decisions[row_id] = item

    if has_applied_decision:
        if not clean(manifest.get("reviewer_id")):
            raise ValueError("reviewer_id is required when decisions are applied")
        parse_reviewed_at(manifest.get("reviewed_at"))
    if has_approval:
        validation_as_of = parse_date(
            manifest.get("validation_as_of"), "validation_as_of"
        )
        if validation_as_of < source_as_of:
            raise ValueError("validation_as_of cannot precede compass_source_as_of")
    return decisions


def build_reviewed_candidate(
    row: dict[str, Any], decision: dict[str, Any], manifest: dict[str, Any], preview_sha256: str
) -> dict[str, Any]:
    evidence_by_id = {
        clean(item.get("evidence_item_id")): item for item in row.get("evidence_items") or []
    }
    supporting_ids = decision.get("supporting_evidence_item_ids") or []
    return {
        "reviewed_candidate_id": f"{row.get('preview_row_id')}-REVIEWED",
        "source_preview_row_id": row.get("preview_row_id"),
        "source_preview_row_sha256": canonical_sha256(row),
        "source_preview_sha256": preview_sha256,
        "compass_source_as_of": manifest.get("compass_source_as_of"),
        "validation_as_of": manifest.get("validation_as_of"),
        "source_candidate_id": row.get("source_candidate_id"),
        "source_theme_name": row.get("source_theme_name"),
        "source_object_class": row.get("source_object_class"),
        "source_discovery_status": row.get("source_discovery_status"),
        "source_discovery_constraints": row.get("source_discovery_constraints"),
        "origin": "human_reviewed_theme_discovery",
        "compass_line": row.get("compass_line"),
        "compass_line_key": row.get("compass_line_key"),
        "priority": row.get("priority"),
        "supply_chain_nodes": row.get("supply_chain_nodes") or [],
        "bottleneck_hypotheses": row.get("bottleneck_hypotheses") or [],
        "discovery_keywords": row.get("discovery_keywords") or [],
        "questions_for_zhulong": row.get("questions_for_zhulong") or [],
        "line_validation_questions": row.get("line_validation_questions") or [],
        "market": "A股",
        "ticker": row.get("symbol"),
        "ticker_status": "manual_resolved_from_discovery_preview",
        "ticker_source": "discovery_preview.fact_stock_basic.symbol",
        "name": row.get("name"),
        "industry": row.get("industry"),
        "market_board": row.get("market_board"),
        "list_date": row.get("list_date"),
        "is_st": bool(row.get("is_st")),
        "mapping_pool": row.get("mapping_pool"),
        "mapping_evidence_level": row.get("mapping_evidence_level"),
        "evidence_level": row.get("evidence_level"),
        "evidence_scope": row.get("evidence_scope"),
        "evidence_items": row.get("evidence_items") or [],
        "evidence_sources": row.get("evidence_sources") or [],
        "source_memberships": row.get("source_memberships") or [],
        "matched_name_terms": row.get("matched_name_terms") or [],
        "matched_industry_terms": row.get("matched_industry_terms") or [],
        "matched_bridge_terms": row.get("matched_bridge_terms") or [],
        "matched_snapshot_terms": row.get("matched_snapshot_terms") or [],
        "business_relevance_evidence_present": True,
        "business_relevance_status": "human_confirmed",
        "business_relevance_confirmation_source": "manual_review_with_immutable_mapping_evidence",
        "business_relevance_basis": decision.get("business_relevance_basis"),
        "supporting_evidence_item_ids": supporting_ids,
        "supporting_evidence_items": [evidence_by_id[item_id] for item_id in supporting_ids],
        "supporting_metrics": {},
        "readonly_metrics_available": False,
        "dry_run_preview_reviewed": True,
        "human_review_for_theme_mapping": True,
        "reviewer_id": manifest.get("reviewer_id"),
        "reviewed_at": manifest.get("reviewed_at"),
        "review_notes": decision.get("review_notes"),
        "manual_review_required": False,
        "eligible_for_separate_dry_run_validation_task": True,
        "generate_task": False,
        "task_block_reason": "separate_validation_task_generator_not_invoked",
        "no_trade_signal": True,
        "blocked_actions": BLOCKED_ACTIONS,
    }


def apply_manifest(
    preview: dict[str, Any], preview_sha256: str, manifest: dict[str, Any], manifest_sha256: str
) -> dict[str, Any]:
    rows_by_id = validate_preview(preview)
    decisions = validate_manifest(manifest, preview, preview_sha256, rows_by_id)
    records = []
    reviewed_candidates = []
    for row_id, row in rows_by_id.items():
        decision = decisions.get(row_id) or {
            "preview_row_id": row_id,
            "decision": "pending",
            "review_notes": "not included in manifest",
        }
        status = clean(decision.get("decision")).lower() or "pending"
        records.append({
            "preview_row_id": row_id,
            "symbol": row.get("symbol"),
            "name": row.get("name"),
            "source_theme_name": row.get("source_theme_name"),
            "evidence_level": row.get("evidence_level"),
            "decision": status,
            "review_notes": decision.get("review_notes"),
        })
        if status == "approve":
            reviewed_candidates.append(build_reviewed_candidate(row, decision, manifest, preview_sha256))

    warnings = []
    symbol_counts = Counter(item.get("ticker") for item in reviewed_candidates)
    for symbol, count in sorted(symbol_counts.items()):
        if symbol and count > 1:
            warnings.append(f"duplicate_approved_symbol_across_themes:{symbol}:{count}")
    decision_counts = Counter(item.get("decision") or "pending" for item in records)
    return {
        "output_version": OUTPUT_VERSION,
        "batch_id": preview.get("batch_id"),
        "source": "Compass/ima + Zhulong manual review",
        "generated_at": now_iso(),
        "mode": "dry_run",
        "dry_run": True,
        "no_trade_signal": True,
        "tool": "tools/compass_discovery_review.py",
        "source_preview_sha256": preview_sha256,
        "review_manifest_sha256": manifest_sha256,
        "compass_source_as_of": manifest.get("compass_source_as_of"),
        "validation_as_of": manifest.get("validation_as_of"),
        "reviewer_id": manifest.get("reviewer_id"),
        "reviewed_at": manifest.get("reviewed_at"),
        "allowed_actions": ALLOWED_ACTIONS,
        "blocked_actions": BLOCKED_ACTIONS,
        "required_next_path": ["reviewed_candidate", "separate_dry_run_validation_task_generator"],
        "forbidden_path": "review_manifest -> live audit or trade",
        "stats": {
            "preview_rows": len(rows_by_id),
            "approved": decision_counts.get("approve", 0),
            "rejected": decision_counts.get("reject", 0),
            "deferred": decision_counts.get("defer", 0),
            "pending": decision_counts.get("pending", 0),
            "reviewed_candidates": len(reviewed_candidates),
            "generated_tasks": 0,
        },
        "warnings": warnings,
        "review_records": records,
        "reviewed_candidates": reviewed_candidates,
        "validation_tasks": [],
    }


def render_review(payload: dict[str, Any]) -> str:
    lines = [
        f"# Compass Discovery Review - {payload.get('batch_id')}",
        "",
        "## 1. Review Batch",
        "",
        f"- mode: `{payload.get('mode')}`",
        f"- source_preview_sha256: `{payload.get('source_preview_sha256')}`",
        f"- compass_source_as_of: `{payload.get('compass_source_as_of')}`",
        f"- validation_as_of: `{payload.get('validation_as_of')}`",
        f"- review_manifest_sha256: `{payload.get('review_manifest_sha256')}`",
        f"- reviewer_id: `{payload.get('reviewer_id')}`",
        f"- reviewed_at: `{payload.get('reviewed_at')}`",
        "- no_trade_signal: `true`",
        "",
        "## 2. Stats",
        "",
    ]
    lines += md_table(["metric", "count"], [[key, value] for key, value in (payload.get("stats") or {}).items()])
    for decision, heading in [
        ("approve", "Approved Reviewed Candidates"),
        ("reject", "Rejected Rows"),
        ("defer", "Deferred Rows"),
        ("pending", "Pending Rows"),
    ]:
        rows = [
            [item.get("preview_row_id"), item.get("source_theme_name"), item.get("symbol"), item.get("name"), item.get("evidence_level"), item.get("review_notes")]
            for item in payload.get("review_records") or []
            if item.get("decision") == decision
        ]
        lines += ["", f"## {heading}", ""] + md_table(
            ["preview_row_id", "theme", "symbol", "name", "evidence", "review_notes"], rows
        )
    lines += ["", "## Warnings", ""] + md_table(["warning"], [[item] for item in payload.get("warnings") or []])
    lines += [
        "",
        "## Safety",
        "",
        "This artifact records manual review only. It generates no validation task, writes no database, does not call decision_engine or Nexus, and cannot affect L4, Shadow, RAG, daemon, or trading.",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Initialize or apply a Compass discovery manual-review manifest.")
    parser.add_argument("--preview", required=True, type=Path, help="Compass discovery preview JSON.")
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--init-manifest-output", type=Path, help="Write an editable review manifest template.")
    action.add_argument("--review-manifest", type=Path, help="Apply a completed review manifest.")
    parser.add_argument("--output-dir", type=Path, default=OUT_DIR)
    parser.add_argument("--json-output", type=Path)
    parser.add_argument("--preview-output", type=Path)
    args = parser.parse_args()

    preview_raw = args.preview.resolve().read_bytes()
    preview = json.loads(preview_raw.decode("utf-8"))
    preview_sha256 = sha256_bytes(preview_raw)

    if args.init_manifest_output:
        manifest = initialize_manifest(preview, preview_sha256)
        output = args.init_manifest_output.resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({"mode": "manual_review_template", "manifest_output": str(output), "rows": len(manifest["decisions"])}, ensure_ascii=False, indent=2))
        return

    manifest_raw = args.review_manifest.resolve().read_bytes()
    manifest = json.loads(manifest_raw.decode("utf-8"))
    payload = apply_manifest(preview, preview_sha256, manifest, sha256_bytes(manifest_raw))
    output_dir = args.output_dir.resolve()
    json_output = args.json_output or output_dir / f"compass_reviewed_candidates_{safe_batch(payload['batch_id'])}.json"
    preview_output = args.preview_output or output_dir / f"zhulong_compass_discovery_review_{safe_batch(payload['batch_id'])}.md"
    json_output.parent.mkdir(parents=True, exist_ok=True)
    preview_output.parent.mkdir(parents=True, exist_ok=True)
    json_output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    preview_output.write_text(render_review(payload), encoding="utf-8")
    reviewed_artifact_sha256 = sha256_bytes(json_output.read_bytes())
    print(json.dumps({
        "mode": "dry_run",
        "json_output": str(json_output),
        "reviewed_artifact_sha256": reviewed_artifact_sha256,
        "preview_output": str(preview_output),
        "stats": payload["stats"],
        "warnings": payload["warnings"],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise SystemExit(f"compass discovery review failed: {exc}") from None
