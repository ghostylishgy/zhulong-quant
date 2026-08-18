#!/usr/bin/env python3
"""Export and apply hash-bound manual reviews for L4 news risk samples."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = str(ROOT / "storage" / "database" / "zhulong.duckdb")
ENGINE_LIB = ROOT / "01_engine" / "lib"
for path in (ROOT, ENGINE_LIB):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from db_gateway import DBGateway

logger = logging.getLogger("zhulong.news_manual_review")
BEIJING_TZ = timezone(timedelta(hours=8))
MANIFEST_VERSION = "l4_news_manual_review_manifest_v0.1"
RISK_GATES = {"WOULD_CAP_HOLD", "WOULD_VETO"}
REVIEW_LABELS = {
    "PENDING",
    "TRUE_RISK",
    "FALSE_POSITIVE",
    "PARTIAL_MATCH",
    "INSUFFICIENT_EVIDENCE",
}


def clean(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def now_iso() -> str:
    return datetime.now(BEIJING_TZ).isoformat(timespec="seconds")


def canonical_sha256(value: Any) -> str:
    raw = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def require_sha256(value: Any, field: str) -> str:
    digest = clean(value).lower()
    if not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ValueError(f"{field} must be 64 lowercase hex")
    return digest


def parse_timestamp(value: Any, field: str) -> str:
    text = clean(value)
    if not text:
        raise ValueError(f"{field} is required")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} must be ISO-8601") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field} must include a timezone")
    return text


def parse_evidence(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    try:
        parsed = json.loads(str(raw or "{}"))
    except (TypeError, json.JSONDecodeError):
        return {"parse_error": True, "raw": clean(raw)[:1000]}
    return parsed if isinstance(parsed, dict) else {"parse_error": True}


def compact_evidence(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for item in payload.get("evidence") or []:
        rows.append(
            {
                "provider": clean(item.get("provider")),
                "source_grade": clean(item.get("source_grade")),
                "published_at": clean(item.get("published_at")),
                "title": clean(item.get("title")),
                "url": clean(item.get("url")),
                "critical_tags": list(item.get("critical_tags") or []),
                "caution_tags": list(item.get("caution_tags") or []),
                "positive_tags": list(item.get("positive_tags") or []),
                "entity_match_type": clean(item.get("entity_match_type")),
                "entity_match_value": clean(item.get("entity_match_value")),
            }
        )
    return rows


def load_risk_rows(
    start_date: str,
    end_date: str,
    db_path: str | None = None,
) -> list[dict[str, Any]]:
    path = db_path or DB_PATH
    with DBGateway(path, read_only=True, logger=logger) as conn:
        rows = conn.execute(
            """
            SELECT task_id, CAST(trade_date AS VARCHAR), symbol, COALESCE(name, ''),
                   COALESCE(l4_final_verdict, 'UNKNOWN'),
                   COALESCE(l4_news_status, ''),
                   COALESCE(l4_news_gate, 'NONE'),
                   COALESCE(l4_news_risk_level, ''),
                   COALESCE(l4_news_risk_score, 0),
                   COALESCE(l4_news_summary, ''),
                   COALESCE(l4_news_evidence, '{}'),
                   COALESCE(l4_news_as_of, '')
            FROM nexus_audits
            WHERE COALESCE(l4_news_policy, '') = 'OBSERVE_ONLY'
              AND COALESCE(status, '') = 'L4_DONE'
              AND COALESCE(l4_news_gate, 'NONE') IN ('WOULD_CAP_HOLD', 'WOULD_VETO')
              AND CAST(trade_date AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
            ORDER BY trade_date, task_id
            """,
            [start_date, end_date],
        ).fetchall()
    result = []
    for row in rows:
        payload = parse_evidence(row[10])
        record = {
            "task_id": clean(row[0]),
            "trade_date": clean(row[1]),
            "symbol": clean(row[2]),
            "name": clean(row[3]),
            "l4_final_verdict": clean(row[4]).upper(),
            "news_status": clean(row[5]).upper(),
            "news_gate": clean(row[6]).upper(),
            "risk_level": clean(row[7]).upper(),
            "risk_score": int(row[8] or 0),
            "summary": clean(row[9]),
            "news_as_of": clean(row[11]),
            "negative_tags": list(payload.get("negative_tags") or []),
            "sources_ok": list(payload.get("sources_ok") or []),
            "sources_failed": payload.get("sources_failed") or {},
            "evidence": compact_evidence(payload),
        }
        record["source_row_sha256"] = canonical_sha256(record)
        result.append(record)
    return result


def initialize_manifest(
    start_date: str,
    end_date: str,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    decisions = []
    for row in rows:
        decisions.append(
            {
                "task_id": row["task_id"],
                "source_row_sha256": row["source_row_sha256"],
                "trade_date": row["trade_date"],
                "symbol": row["symbol"],
                "name": row["name"],
                "news_gate": row["news_gate"],
                "review_label": "PENDING",
                "entity_correct": None,
                "negation_correct": None,
                "material_false_veto": False,
                "notes": "",
            }
        )
    source_rows = [
        {key: value for key, value in row.items() if key != "source_row_sha256"}
        for row in rows
    ]
    return {
        "manifest_version": MANIFEST_VERSION,
        "created_at": now_iso(),
        "start_date": start_date,
        "end_date": end_date,
        "source_rows_sha256": canonical_sha256(source_rows),
        "mode": "manual_review",
        "reviewer": "",
        "reviewed_at": None,
        "instructions": {
            "review_labels": sorted(REVIEW_LABELS),
            "pending_rows_are_skipped": True,
            "non_pending_requires": [
                "unchanged_source_row_sha256",
                "reviewer",
                "timezone_aware_reviewed_at",
                "entity_correct_boolean",
                "negation_correct_boolean",
            ],
            "notes_required_for": [
                "FALSE_POSITIVE",
                "PARTIAL_MATCH",
                "INSUFFICIENT_EVIDENCE",
                "entity_correct=false",
                "negation_correct=false",
            ],
            "write_policy": "default dry-run; --write-reviews is required",
        },
        "blocked_actions": [
            "change_l4_verdict",
            "change_l4_score",
            "change_news_policy",
            "write_shadow",
            "write_rag_memory",
            "generate_trade",
            "trigger_daemon",
        ],
        "source_rows": rows,
        "decisions": decisions,
    }


def validate_manifest(
    manifest: dict[str, Any],
    current_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if manifest.get("manifest_version") != MANIFEST_VERSION:
        raise ValueError(
            f"unsupported manifest version: {manifest.get('manifest_version')}"
        )
    if manifest.get("mode") != "manual_review":
        raise ValueError("manifest mode must be manual_review")
    current_by_id = {row["task_id"]: row for row in current_rows}
    if len(current_by_id) != len(current_rows):
        raise ValueError("duplicate task_id in current risk rows")
    source_rows = [
        {key: value for key, value in row.items() if key != "source_row_sha256"}
        for row in current_rows
    ]
    if clean(manifest.get("source_rows_sha256")) != canonical_sha256(source_rows):
        raise ValueError("source risk rows changed; initialize a fresh manifest")

    decisions = []
    seen = set()
    for item in manifest.get("decisions") or []:
        task_id = clean(item.get("task_id"))
        if task_id not in current_by_id:
            raise ValueError(f"unknown or stale task_id: {task_id}")
        if task_id in seen:
            raise ValueError(f"duplicate decision: {task_id}")
        seen.add(task_id)
        row = current_by_id[task_id]
        if clean(item.get("source_row_sha256")) != row["source_row_sha256"]:
            raise ValueError(f"source row hash mismatch: {task_id}")
        label = clean(item.get("review_label")).upper()
        if label not in REVIEW_LABELS:
            raise ValueError(f"unsupported review_label for {task_id}: {label}")
        if label == "PENDING":
            continue
        if not isinstance(item.get("entity_correct"), bool):
            raise ValueError(f"entity_correct must be boolean: {task_id}")
        if not isinstance(item.get("negation_correct"), bool):
            raise ValueError(f"negation_correct must be boolean: {task_id}")
        notes = clean(item.get("notes"))
        needs_notes = (
            label in {"FALSE_POSITIVE", "PARTIAL_MATCH", "INSUFFICIENT_EVIDENCE"}
            or item.get("entity_correct") is False
            or item.get("negation_correct") is False
        )
        if needs_notes and not notes:
            raise ValueError(f"review notes required: {task_id}")
        if bool(item.get("material_false_veto")) and row["news_gate"] != "WOULD_VETO":
            raise ValueError(
                f"material_false_veto is only valid for WOULD_VETO: {task_id}"
            )
        decisions.append(
            {
                "task_id": task_id,
                "source_row_sha256": row["source_row_sha256"],
                "review_label": label,
                "entity_correct": item["entity_correct"],
                "negation_correct": item["negation_correct"],
                "material_false_veto": bool(item.get("material_false_veto")),
                "notes": notes,
            }
        )
    if decisions:
        reviewer = clean(manifest.get("reviewer"))
        if not reviewer:
            raise ValueError("reviewer is required for applied decisions")
        parse_timestamp(manifest.get("reviewed_at"), "reviewed_at")
    return decisions


def ensure_review_table(db_path: str) -> None:
    provenance_columns = {
        "source_row_sha256": "VARCHAR DEFAULT ''",
        "source_rows_sha256": "VARCHAR DEFAULT ''",
        "manifest_version": "VARCHAR DEFAULT ''",
        "manifest_sha256": "VARCHAR DEFAULT ''",
    }
    with DBGateway(db_path, read_only=False, logger=logger) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ops_l4_news_manual_reviews (
                task_id VARCHAR PRIMARY KEY,
                source_row_sha256 VARCHAR DEFAULT '',
                source_rows_sha256 VARCHAR DEFAULT '',
                manifest_version VARCHAR DEFAULT '',
                manifest_sha256 VARCHAR DEFAULT '',
                review_label VARCHAR NOT NULL,
                entity_correct BOOLEAN,
                negation_correct BOOLEAN,
                material_false_veto BOOLEAN DEFAULT FALSE,
                reviewer VARCHAR DEFAULT '',
                notes VARCHAR DEFAULT '',
                reviewed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        existing = {
            str(row[0]).lower()
            for row in conn.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'ops_l4_news_manual_reviews'
                """
            ).fetchall()
        }
        for column, ddl in provenance_columns.items():
            if column not in existing:
                conn.execute(
                    f"ALTER TABLE ops_l4_news_manual_reviews "
                    f"ADD COLUMN {column} {ddl}"
                )


def apply_reviews(
    manifest: dict[str, Any],
    decisions: list[dict[str, Any]],
    db_path: str,
    write_reviews: bool,
    manifest_sha256: str = "",
) -> dict[str, Any]:
    reviewer = clean(manifest.get("reviewer"))
    reviewed_at = (
        parse_timestamp(manifest.get("reviewed_at"), "reviewed_at")
        if decisions
        else ""
    )
    manifest_digest = require_sha256(
        manifest_sha256, "manifest_sha256"
    ) if decisions else ""
    source_rows_digest = require_sha256(
        manifest.get("source_rows_sha256"), "source_rows_sha256"
    ) if decisions else ""
    manifest_version = clean(manifest.get("manifest_version"))
    if decisions and manifest_version != MANIFEST_VERSION:
        raise ValueError("manifest_version changed before persistence")
    if not write_reviews:
        return {
            "mode": "dry_run",
            "validated_reviews": len(decisions),
            "written_reviews": 0,
            "reviewer": reviewer,
            "manifest_sha256": manifest_digest,
        }
    ensure_review_table(db_path)
    with DBGateway(db_path, read_only=False, logger=logger) as conn:
        task_ids = [item["task_id"] for item in decisions]
        existing = set()
        if task_ids:
            placeholders = ",".join("?" for _ in task_ids)
            existing = {
                str(row[0])
                for row in conn.execute(
                    f"SELECT task_id FROM ops_l4_news_manual_reviews "
                    f"WHERE task_id IN ({placeholders})",
                    task_ids,
                ).fetchall()
            }
        if existing:
            raise ValueError(
                "manual reviews are immutable; existing task_id: "
                + ",".join(sorted(existing))
            )
        for item in decisions:
            source_row_digest = require_sha256(
                item.get("source_row_sha256"),
                f"{item['task_id']}.source_row_sha256",
            )
            conn.execute(
                """
                INSERT INTO ops_l4_news_manual_reviews
                    (task_id, source_row_sha256, source_rows_sha256,
                     manifest_version, manifest_sha256, review_label,
                     entity_correct, negation_correct, material_false_veto,
                     reviewer, notes, reviewed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS TIMESTAMP))
                """,
                [
                    item["task_id"],
                    source_row_digest,
                    source_rows_digest,
                    manifest_version,
                    manifest_digest,
                    item["review_label"],
                    item["entity_correct"],
                    item["negation_correct"],
                    item["material_false_veto"],
                    reviewer,
                    item["notes"],
                    reviewed_at,
                ],
            )
    return {
        "mode": "write_reviews",
        "validated_reviews": len(decisions),
        "written_reviews": len(decisions),
        "reviewer": reviewer,
        "manifest_sha256": manifest_digest,
    }


def md_table(headers: list[str], rows: list[list[Any]]) -> list[str]:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        values = [
            clean(value).replace("|", "/") if value is not None else ""
            for value in row
        ]
        lines.append("| " + " | ".join(values) + " |")
    return lines


def render_preview(manifest: dict[str, Any]) -> str:
    lines = [
        "# L4 News Manual Review Samples",
        "",
        f"- Window: {manifest.get('start_date')} to {manifest.get('end_date')}",
        f"- Source rows SHA256: {manifest.get('source_rows_sha256')}",
        f"- Risk samples: {len(manifest.get('source_rows') or [])}",
        "- Default action: dry_run",
        "- Policy change: NO",
        "",
        "## Risk Samples",
        "",
    ]
    rows = []
    for item in manifest.get("source_rows") or []:
        titles = "; ".join(
            f"{e.get('provider')}:{e.get('title')}"
            for e in (item.get("evidence") or [])
        )
        rows.append(
            [
                item.get("task_id"),
                item.get("trade_date"),
                item.get("symbol"),
                item.get("name"),
                item.get("news_gate"),
                ",".join(item.get("negative_tags") or []),
                titles,
            ]
        )
    lines += md_table(
        ["task_id", "date", "symbol", "name", "gate", "tags", "evidence"],
        rows,
    )
    lines += [
        "",
        "## Safety",
        "",
        "Manual labels are evaluation evidence only. They do not change historical "
        "L4 verdicts or scores, news policy, Shadow, RAG, daemon state, or trades.",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export or apply hash-bound manual reviews for L4 news samples."
    )
    parser.add_argument("--start-date", default="2026-06-22")
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument("--db-path", default=DB_PATH)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--init-manifest-output", type=Path)
    action.add_argument("--review-manifest", type=Path)
    parser.add_argument("--preview-output", type=Path)
    parser.add_argument("--write-reviews", action="store_true")
    parser.add_argument(
        "--replace-existing", action="store_true", help=argparse.SUPPRESS
    )
    args = parser.parse_args()

    if args.replace_existing:
        raise ValueError(
            "--replace-existing is forbidden; manual reviews are immutable"
        )
    if args.init_manifest_output and args.write_reviews:
        raise ValueError("manifest initialization is read-only")

    rows = load_risk_rows(args.start_date, args.end_date, args.db_path)
    if args.init_manifest_output:
        manifest = initialize_manifest(args.start_date, args.end_date, rows)
        output = args.init_manifest_output.resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        if args.preview_output:
            preview = args.preview_output.resolve()
            preview.parent.mkdir(parents=True, exist_ok=True)
            preview.write_text(render_preview(manifest), encoding="utf-8")
        print(
            json.dumps(
                {
                    "mode": "manual_review_template",
                    "manifest_output": str(output),
                    "preview_output": str(args.preview_output or ""),
                    "risk_samples": len(rows),
                    "source_rows_sha256": manifest["source_rows_sha256"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    manifest_path = args.review_manifest.resolve()
    manifest_raw = manifest_path.read_bytes()
    manifest = json.loads(manifest_raw.decode("utf-8"))
    manifest_sha256 = sha256_bytes(manifest_raw)
    start_date = clean(manifest.get("start_date"))
    end_date = clean(manifest.get("end_date"))
    current_rows = load_risk_rows(start_date, end_date, args.db_path)
    decisions = validate_manifest(manifest, current_rows)
    result = apply_reviews(
        manifest,
        decisions,
        args.db_path,
        args.write_reviews,
        manifest_sha256,
    )
    result.update(
        {
            "start_date": start_date,
            "end_date": end_date,
            "pending_reviews": len(current_rows) - len(decisions),
            "policy_changed": False,
        }
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise SystemExit(f"news manual review failed: {exc}") from None
