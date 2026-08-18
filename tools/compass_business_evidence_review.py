#!/usr/bin/env python3
"""Review specific Compass business evidence without creating validation tasks."""

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
DEFAULT_OUTPUT_DIR = ROOT / "storage" / "reports" / "compass_ingest"
BEIJING_TZ = timezone(timedelta(hours=8))
INPUT_PROTOCOL_VERSION = "compass_business_evidence_snapshot_v0.2"
MANIFEST_VERSION = "compass_business_evidence_review_manifest_v0.1"
OUTPUT_VERSION = "compass_business_evidence_review_v0.1"
ALLOWED_DECISIONS = {"pending", "promote", "keep_broad", "reject_mapping"}
ALLOWED_ACTIONS = [
    "initialize_business_evidence_review_manifest",
    "apply_business_evidence_review",
    "render_business_evidence_review",
]
BLOCKED_ACTIONS = [
    "trade",
    "write_duckdb",
    "write_shadow",
    "write_rag_memory",
    "write_nexus_audits",
    "trigger_daemon",
    "call_decision_engine",
    "call_nexus_run",
    "change_discovery_pool",
    "initialize_discovery_review_manifest",
    "generate_validation_task",
    "auto_approve_business_relevance",
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
    raw = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return sha256_bytes(raw)


def verify_payload_sha(snapshot: dict[str, Any]) -> str:
    claimed = clean(snapshot.get("payload_sha256"))
    if not re.fullmatch(r"[0-9a-f]{64}", claimed):
        raise ValueError("business snapshot payload_sha256 is missing or invalid")
    unsigned = copy.deepcopy(snapshot)
    unsigned.pop("payload_sha256", None)
    actual = canonical_sha256(unsigned)
    if actual != claimed:
        raise ValueError("business snapshot payload_sha256 mismatch")
    return claimed


def business_row_id(row: dict[str, Any]) -> str:
    identity = {
        "source_candidate_id": clean(row.get("source_candidate_id")),
        "candidate_scope": clean(row.get("candidate_scope")),
        "symbol": clean(row.get("symbol")),
    }
    return f"BIZROW-{canonical_sha256(identity)[:16].upper()}"


def validate_reviewable_rows(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    if snapshot.get("protocol_version") != INPUT_PROTOCOL_VERSION:
        raise ValueError("unsupported business-evidence snapshot protocol")
    if snapshot.get("mode") != "dry_run" or snapshot.get("dry_run") is not True:
        raise ValueError("business snapshot must be dry_run")
    if snapshot.get("no_trade_signal") is not True:
        raise ValueError("business snapshot must preserve no_trade_signal=true")
    if snapshot.get("validation_tasks") not in (None, []):
        raise ValueError("business snapshot must not contain validation tasks")
    if int((snapshot.get("stats") or {}).get("generated_tasks") or 0) != 0:
        raise ValueError("business snapshot generated_tasks must be zero")
    if not clean(snapshot.get("source_discovery_preview_sha256")):
        raise ValueError("source discovery preview SHA is required")
    if not clean(snapshot.get("node_dictionary_sha256")):
        raise ValueError("node dictionary SHA is required")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", clean(snapshot.get("evidence_as_of"))):
        raise ValueError("evidence_as_of must be YYYY-MM-DD")
    verify_payload_sha(snapshot)

    all_rows: dict[str, dict[str, Any]] = {}
    expected_reviewable: list[dict[str, Any]] = []
    expected_context_only: list[dict[str, Any]] = []
    for row in snapshot.get("business_evidence") or []:
        row_id = business_row_id(row)
        if row_id in all_rows:
            raise ValueError(f"duplicate business evidence row: {row_id}")
        symbol = clean(row.get("symbol"))
        if not re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", symbol):
            raise ValueError(f"unsupported A-share symbol: {row_id}:{symbol}")
        if row.get("generate_task") is not False or row.get("no_trade_signal") is not True:
            raise ValueError(f"unsafe business evidence row: {row_id}")
        if row.get("existing_review_manifest_eligible") is not False:
            raise ValueError(f"business evidence row bypasses existing review gate: {row_id}")
        if "metrics" in row or "metric_trade_date" in row:
            raise ValueError(f"trading metrics are forbidden in business review: {row_id}")
        evidence_items = (
            (row.get("descriptor_evidence") or [])
            + (row.get("reported_business_evidence") or [])
        )
        evidence_by_id = {
            clean(item.get("evidence_item_id")): item for item in evidence_items
        }
        if len(evidence_by_id) != len(evidence_items) or "" in evidence_by_id:
            raise ValueError(f"business evidence IDs must be unique: {row_id}")
        qualifying_ids = [
            clean(value) for value in row.get("review_qualifying_evidence_item_ids") or []
        ]
        expected_qualifying_ids = {
            item_id for item_id, item in evidence_by_id.items()
            if item.get("review_qualifying") is True
        }
        if set(qualifying_ids) != expected_qualifying_ids:
            raise ValueError(f"review qualifying evidence IDs mismatch: {row_id}")
        if row.get("business_review_eligible") is True:
            if row.get("business_review_eligibility") != "eligible_specific_node":
                raise ValueError(f"review eligibility status mismatch: {row_id}")
            if row.get("business_relevance_evidence_present") is not True:
                raise ValueError(f"reviewable row lacks business evidence: {row_id}")
            if not qualifying_ids:
                raise ValueError(f"reviewable row lacks qualifying evidence: {row_id}")
            if row.get("candidate_scope") != "broad_universe_recheck":
                continue
            if row.get("mapping_pool_unchanged") != "broad_universe":
                raise ValueError(f"broad review row has changed source pool: {row_id}")
            if row.get("source_review_eligible") is not False:
                raise ValueError(f"broad review row already eligible upstream: {row_id}")
            expected_reviewable.append(row)
        elif row.get("business_review_eligibility") == "context_only_generic_node":
            expected_context_only.append(row)
        all_rows[row_id] = row

    if snapshot.get("business_evidence_matches") != [
        row for row in snapshot.get("business_evidence") or []
        if row.get("business_relevance_evidence_present")
    ]:
        raise ValueError("business_evidence_matches does not match evidence rows")
    if snapshot.get("business_context_only_matches") != expected_context_only:
        raise ValueError("business_context_only_matches does not match evidence rows")
    return {business_row_id(row): row for row in expected_reviewable}


def initialize_manifest(
    snapshot: dict[str, Any], snapshot_file_sha256: str
) -> dict[str, Any]:
    rows_by_id = validate_reviewable_rows(snapshot)
    decisions = []
    for row_id, row in rows_by_id.items():
        decisions.append({
            "business_evidence_row_id": row_id,
            "business_evidence_row_sha256": canonical_sha256(row),
            "symbol": row.get("symbol"),
            "name": row.get("name"),
            "source_theme_name": row.get("source_theme_name"),
            "business_evidence_level": row.get("business_evidence_level"),
            "decision": "pending",
            "selected_evidence_item_ids": [],
            "review_notes": "",
        })
    return {
        "manifest_version": MANIFEST_VERSION,
        "batch_id": snapshot.get("batch_id"),
        "source_business_snapshot_file_sha256": snapshot_file_sha256,
        "source_business_payload_sha256": snapshot.get("payload_sha256"),
        "source_discovery_preview_sha256": snapshot.get(
            "source_discovery_preview_sha256"
        ),
        "node_dictionary_sha256": snapshot.get("node_dictionary_sha256"),
        "evidence_as_of": snapshot.get("evidence_as_of"),
        "mode": "manual_review",
        "dry_run": True,
        "no_trade_signal": True,
        "reviewer_id": "",
        "reviewed_at": None,
        "instructions": {
            "allowed_decisions": sorted(ALLOWED_DECISIONS),
            "promote_requires": [
                "unchanged_business_snapshot_and_row_hashes",
                "eligible_specific_node",
                "at_least_one_selected_qualifying_evidence_item",
                "reviewer_id_and_timezone_aware_reviewed_at",
                "non_empty_review_notes",
            ],
            "promotion_semantics": (
                "reviewed for a future discovery adapter only; no pool change or task"
            ),
        },
        "blocked_actions": BLOCKED_ACTIONS,
        "decisions": decisions,
    }


def parse_reviewed_at(value: Any) -> str:
    text = clean(value)
    if not text:
        raise ValueError("reviewed_at is required when decisions are applied")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("reviewed_at must be ISO-8601") from exc
    if parsed.tzinfo is None:
        raise ValueError("reviewed_at must include a timezone")
    return text


def validate_manifest(
    manifest: dict[str, Any], snapshot: dict[str, Any],
    snapshot_file_sha256: str, rows_by_id: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    if manifest.get("manifest_version") != MANIFEST_VERSION:
        raise ValueError("unsupported business-evidence review manifest version")
    bindings = {
        "batch_id": snapshot.get("batch_id"),
        "source_business_snapshot_file_sha256": snapshot_file_sha256,
        "source_business_payload_sha256": snapshot.get("payload_sha256"),
        "source_discovery_preview_sha256": snapshot.get(
            "source_discovery_preview_sha256"
        ),
        "node_dictionary_sha256": snapshot.get("node_dictionary_sha256"),
        "evidence_as_of": snapshot.get("evidence_as_of"),
    }
    for field, expected in bindings.items():
        if clean(manifest.get(field)) != clean(expected):
            raise ValueError(f"review manifest binding mismatch: {field}")
    if manifest.get("mode") != "manual_review" or manifest.get("dry_run") is not True:
        raise ValueError("review manifest must remain manual_review dry-run")
    if manifest.get("no_trade_signal") is not True:
        raise ValueError("review manifest must preserve no_trade_signal=true")
    if not set(BLOCKED_ACTIONS).issubset(set(manifest.get("blocked_actions") or [])):
        raise ValueError("review manifest does not preserve blocked actions")

    decisions: dict[str, dict[str, Any]] = {}
    has_applied_decision = False
    for item in manifest.get("decisions") or []:
        row_id = clean(item.get("business_evidence_row_id"))
        if row_id not in rows_by_id:
            raise ValueError(f"review manifest references non-reviewable row: {row_id}")
        if row_id in decisions:
            raise ValueError(f"duplicate business review decision: {row_id}")
        row = rows_by_id[row_id]
        if clean(item.get("business_evidence_row_sha256")) != canonical_sha256(row):
            raise ValueError(f"business evidence row hash mismatch: {row_id}")
        decision = clean(item.get("decision")).lower()
        if decision not in ALLOWED_DECISIONS:
            raise ValueError(f"unsupported business review decision: {row_id}:{decision}")
        normalized = copy.deepcopy(item)
        normalized["decision"] = decision
        selected_ids = list(dict.fromkeys(
            clean(value) for value in item.get("selected_evidence_item_ids") or []
            if clean(value)
        ))
        if decision != "pending":
            has_applied_decision = True
            if not clean(item.get("review_notes")):
                raise ValueError(f"review_notes required for {decision}: {row_id}")
        if decision == "promote":
            allowed_ids = set(row.get("review_qualifying_evidence_item_ids") or [])
            if not selected_ids or not set(selected_ids).issubset(allowed_ids):
                raise ValueError(f"valid qualifying evidence selection required: {row_id}")
        elif selected_ids:
            raise ValueError(f"only promote may select evidence items: {row_id}")
        normalized["selected_evidence_item_ids"] = selected_ids
        decisions[row_id] = normalized

    if set(decisions) != set(rows_by_id):
        missing = sorted(set(rows_by_id) - set(decisions))
        raise ValueError(f"review manifest must retain every reviewable row: {missing}")
    if has_applied_decision:
        if not clean(manifest.get("reviewer_id")):
            raise ValueError("reviewer_id is required when decisions are applied")
        parse_reviewed_at(manifest.get("reviewed_at"))
    return decisions


def build_promoted_candidate(
    row_id: str, row: dict[str, Any], decision: dict[str, Any],
    manifest: dict[str, Any], snapshot_file_sha256: str,
) -> dict[str, Any]:
    evidence_by_id = {
        clean(item.get("evidence_item_id")): item
        for item in (row.get("descriptor_evidence") or [])
        + (row.get("reported_business_evidence") or [])
    }
    selected_ids = decision.get("selected_evidence_item_ids") or []
    return {
        "reviewed_promotion_id": f"{row_id}-PROMOTED",
        "source_business_evidence_row_id": row_id,
        "source_business_evidence_row_sha256": canonical_sha256(row),
        "source_business_snapshot_file_sha256": snapshot_file_sha256,
        "source_business_payload_sha256": manifest.get(
            "source_business_payload_sha256"
        ),
        "source_discovery_preview_sha256": manifest.get(
            "source_discovery_preview_sha256"
        ),
        "node_dictionary_sha256": manifest.get("node_dictionary_sha256"),
        "source_candidate_id": row.get("source_candidate_id"),
        "source_theme_name": row.get("source_theme_name"),
        "compass_line_key": row.get("compass_line_key"),
        "symbol": row.get("symbol"),
        "name": row.get("name"),
        "source_mapping_pool": row.get("mapping_pool_unchanged"),
        "source_candidate_scope": row.get("candidate_scope"),
        "promotion_review_status": "human_approved_for_future_discovery_adapter",
        "recommended_next_pool": "review_shortlist",
        "pool_change_applied": False,
        "existing_review_manifest_eligible": False,
        "eligible_for_future_discovery_adapter": True,
        "business_evidence_level": row.get("business_evidence_level"),
        "business_review_eligibility": row.get("business_review_eligibility"),
        "selected_evidence_item_ids": selected_ids,
        "selected_evidence_items": [evidence_by_id[item_id] for item_id in selected_ids],
        "evidence_as_of": row.get("evidence_as_of"),
        "report_period_stale": row.get("report_period_stale"),
        "reviewer_id": manifest.get("reviewer_id"),
        "reviewed_at": manifest.get("reviewed_at"),
        "review_notes": decision.get("review_notes"),
        "manual_review_required": False,
        "generate_task": False,
        "no_trade_signal": True,
        "blocked_actions": BLOCKED_ACTIONS,
    }


def apply_manifest(
    snapshot: dict[str, Any], snapshot_file_sha256: str,
    manifest: dict[str, Any], manifest_file_sha256: str,
) -> dict[str, Any]:
    rows_by_id = validate_reviewable_rows(snapshot)
    decisions = validate_manifest(
        manifest, snapshot, snapshot_file_sha256, rows_by_id
    )
    records = []
    promoted = []
    for row_id, row in rows_by_id.items():
        decision = decisions[row_id]
        status = decision["decision"]
        records.append({
            "business_evidence_row_id": row_id,
            "symbol": row.get("symbol"),
            "name": row.get("name"),
            "source_theme_name": row.get("source_theme_name"),
            "business_evidence_level": row.get("business_evidence_level"),
            "decision": status,
            "review_notes": decision.get("review_notes"),
        })
        if status == "promote":
            promoted.append(build_promoted_candidate(
                row_id, row, decision, manifest, snapshot_file_sha256
            ))
    counts = Counter(record["decision"] for record in records)
    output = {
        "output_version": OUTPUT_VERSION,
        "batch_id": snapshot.get("batch_id"),
        "source": "Compass business evidence + Zhulong manual review",
        "generated_at": now_iso(),
        "mode": "dry_run",
        "dry_run": True,
        "no_trade_signal": True,
        "tool": "tools/compass_business_evidence_review.py",
        "source_business_snapshot_file_sha256": snapshot_file_sha256,
        "source_business_payload_sha256": snapshot.get("payload_sha256"),
        "source_discovery_preview_sha256": snapshot.get(
            "source_discovery_preview_sha256"
        ),
        "node_dictionary_sha256": snapshot.get("node_dictionary_sha256"),
        "review_manifest_file_sha256": manifest_file_sha256,
        "evidence_as_of": snapshot.get("evidence_as_of"),
        "reviewer_id": manifest.get("reviewer_id"),
        "reviewed_at": manifest.get("reviewed_at"),
        "allowed_actions": ALLOWED_ACTIONS,
        "blocked_actions": BLOCKED_ACTIONS,
        "required_next_path": [
            "reviewed_promotion_candidate",
            "future_separately_reviewed_discovery_adapter",
        ],
        "forbidden_path": "business review manifest -> validation task or trade",
        "stats": {
            "reviewable_rows": len(rows_by_id),
            "promoted": counts.get("promote", 0),
            "kept_broad": counts.get("keep_broad", 0),
            "rejected_mapping": counts.get("reject_mapping", 0),
            "pending": counts.get("pending", 0),
            "promoted_candidates": len(promoted),
            "generated_tasks": 0,
        },
        "review_records": records,
        "promoted_candidates": promoted,
        "validation_tasks": [],
    }
    output["payload_sha256"] = canonical_sha256(output)
    return output


def md_table(headers: list[str], rows: list[list[Any]]) -> list[str]:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(
            clean(value).replace("|", "/") for value in row
        ) + " |")
    return lines


def render_review(payload: dict[str, Any]) -> str:
    lines = [
        f"# Compass Business Evidence Review - {payload.get('batch_id')}",
        "",
        f"- mode: `{payload.get('mode')}`",
        f"- evidence_as_of: `{payload.get('evidence_as_of')}`",
        f"- source_business_snapshot_file_sha256: `{payload.get('source_business_snapshot_file_sha256')}`",
        f"- source_business_payload_sha256: `{payload.get('source_business_payload_sha256')}`",
        f"- source_discovery_preview_sha256: `{payload.get('source_discovery_preview_sha256')}`",
        f"- node_dictionary_sha256: `{payload.get('node_dictionary_sha256')}`",
        f"- review_manifest_file_sha256: `{payload.get('review_manifest_file_sha256')}`",
        f"- reviewer_id: `{payload.get('reviewer_id')}`",
        f"- reviewed_at: `{payload.get('reviewed_at')}`",
        "- no_trade_signal: `true`",
        "",
        "## Stats",
        "",
    ]
    lines += md_table(
        ["metric", "count"],
        [[key, value] for key, value in (payload.get("stats") or {}).items()],
    )
    for decision, title in [
        ("promote", "Promoted For Future Discovery Adapter"),
        ("keep_broad", "Kept In Broad Universe"),
        ("reject_mapping", "Rejected Mappings"),
        ("pending", "Pending Review"),
    ]:
        rows = [[
            item.get("business_evidence_row_id"), item.get("source_theme_name"),
            item.get("symbol"), item.get("name"),
            item.get("business_evidence_level"), item.get("review_notes"),
        ] for item in payload.get("review_records") or []
            if item.get("decision") == decision]
        lines += ["", f"## {title}", ""] + md_table(
            ["row_id", "theme", "symbol", "name", "level", "review_notes"], rows
        )
    lines += [
        "",
        "## Safety",
        "",
        "This review never changes the source discovery pool and generates no validation task. It writes no DuckDB, Shadow, RAG, or nexus_audits state; it cannot affect decision_engine, Nexus, daemon, L4, or trading.",
        "",
    ]
    return "\n".join(lines)


def atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(content, encoding="utf-8")
    temporary.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", required=True, type=Path)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--init-manifest-output", type=Path)
    action.add_argument("--review-manifest", type=Path)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--json-output", type=Path)
    parser.add_argument("--preview-output", type=Path)
    args = parser.parse_args()

    snapshot_raw = args.snapshot.resolve().read_bytes()
    snapshot = json.loads(snapshot_raw.decode("utf-8"))
    snapshot_file_sha256 = sha256_bytes(snapshot_raw)
    if args.init_manifest_output:
        manifest = initialize_manifest(snapshot, snapshot_file_sha256)
        output = args.init_manifest_output.resolve()
        atomic_write(output, json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
        print(json.dumps({
            "mode": "manual_review_template",
            "manifest_output": str(output),
            "reviewable_rows": len(manifest["decisions"]),
            "generated_tasks": 0,
        }, ensure_ascii=False, indent=2))
        return 0

    manifest_raw = args.review_manifest.resolve().read_bytes()
    manifest = json.loads(manifest_raw.decode("utf-8"))
    payload = apply_manifest(
        snapshot, snapshot_file_sha256, manifest, sha256_bytes(manifest_raw)
    )
    output_dir = args.output_dir.resolve()
    batch = safe_batch(payload.get("batch_id"))
    json_output = args.json_output or output_dir / f"compass_business_evidence_review_{batch}.json"
    preview_output = args.preview_output or output_dir / f"zhulong_compass_business_evidence_review_{batch}.md"
    atomic_write(json_output, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    atomic_write(preview_output, render_review(payload))
    print(json.dumps({
        "mode": "dry_run",
        "json_output": str(json_output),
        "preview_output": str(preview_output),
        "reviewed_artifact_sha256": sha256_bytes(json_output.read_bytes()),
        "stats": payload["stats"],
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise SystemExit(f"compass business evidence review failed: {exc}") from None
