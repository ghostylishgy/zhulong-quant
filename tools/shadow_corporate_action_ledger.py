#!/usr/bin/env python3
"""Unwired append-only ledger contract for reviewed Shadow corporate actions."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from typing import Any


AUDIT_SCHEMA = "shadow_corporate_action_audit_v0.3"
REVIEW_SCHEMA = "shadow_corporate_action_review_v0.1"
LEDGER_SCHEMA = "shadow_corporate_action_ledger_v0.1"
TABLE_NAME = "fact_shadow_corporate_action_applications"
ALLOWED_STAGES = {
    "RIGHTS_FROZEN",
    "RECEIVABLE_CREATED",
    "CASH_PAYABLE",
    "STOCK_AVAILABLE",
}
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
RECORD_FIELDS = (
    "ledger_schema_version",
    "application_id",
    "event_identity",
    "event_sha256",
    "symbol",
    "position_trade_date",
    "signal_task_id",
    "stage",
    "effective_date",
    "rights_frozen_qty",
    "cash_receivable_delta_gross",
    "cash_balance_delta_gross",
    "pending_share_delta",
    "available_share_delta",
    "tax_delta",
    "tax_status",
    "source_as_of",
    "source_audit_sha256",
    "review_manifest_sha256",
    "reviewed_by",
    "reviewed_at",
)


class LedgerContractError(ValueError):
    pass


class ApplicationConflict(LedgerContractError):
    pass


class EventRevisionConflict(LedgerContractError):
    pass


def canonical_sha(payload: dict[str, Any]) -> str:
    body = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def require_sha256(value: Any, field: str) -> str:
    text = str(value or "").strip().lower()
    if not SHA256_RE.fullmatch(text):
        raise LedgerContractError(f"{field} must be a lowercase sha256")
    return text


def require_iso_timestamp(value: Any, field: str) -> str:
    text = str(value or "").strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise LedgerContractError(f"{field} must be ISO-8601") from exc
    if parsed.tzinfo is None:
        raise LedgerContractError(f"{field} must be timezone-aware")
    return parsed.isoformat()


def verify_audit_artifact(audit: dict[str, Any]) -> str:
    if audit.get("schema_version") != AUDIT_SCHEMA:
        raise LedgerContractError("unsupported audit schema")
    if audit.get("observer_only") is not True or audit.get("no_trade_signal") is not True:
        raise LedgerContractError("audit safety boundary missing")
    if audit.get("quality_status") != "DATA_OK":
        raise LedgerContractError("audit is not DATA_OK")
    if audit.get("warnings"):
        raise LedgerContractError("audit warnings require manual resolution")
    actual = require_sha256(audit.get("payload_sha256"), "audit.payload_sha256")
    expected = canonical_sha(
        {
            key: value
            for key, value in audit.items()
            if key not in {"generated_at", "payload_sha256"}
        }
    )
    if actual != expected:
        raise LedgerContractError("audit payload sha mismatch")
    return actual


def verify_review_manifest(review: dict[str, Any], audit_sha256: str) -> str:
    if review.get("schema_version") != REVIEW_SCHEMA:
        raise LedgerContractError("unsupported review schema")
    if review.get("decision") != "APPROVE_LEDGER_DRY_RUN":
        raise LedgerContractError("review decision does not approve ledger dry-run")
    if require_sha256(review.get("source_audit_sha256"), "source_audit_sha256") != audit_sha256:
        raise LedgerContractError("review does not bind the audit artifact")
    if not str(review.get("reviewed_by") or "").strip():
        raise LedgerContractError("reviewed_by is required")
    require_iso_timestamp(review.get("reviewed_at"), "reviewed_at")
    actual = require_sha256(review.get("review_manifest_sha256"), "review_manifest_sha256")
    expected = canonical_sha(
        {key: value for key, value in review.items() if key != "review_manifest_sha256"}
    )
    if actual != expected:
        raise LedgerContractError("review manifest sha mismatch")
    return actual


def application_record_sha(record: dict[str, Any]) -> str:
    missing = [field for field in RECORD_FIELDS if field not in record]
    if missing:
        raise LedgerContractError(f"application record fields missing: {missing}")
    return canonical_sha({field: record[field] for field in RECORD_FIELDS})


def validate_application_record(record: dict[str, Any]) -> None:
    if record.get("ledger_schema_version") != LEDGER_SCHEMA:
        raise LedgerContractError("unsupported ledger schema")
    require_sha256(record.get("application_id"), "application_id")
    event_sha256 = require_sha256(record.get("event_sha256"), "event_sha256")
    require_sha256(record.get("source_audit_sha256"), "source_audit_sha256")
    require_sha256(record.get("review_manifest_sha256"), "review_manifest_sha256")
    require_sha256(record.get("record_sha256"), "record_sha256")
    if application_record_sha(record) != record["record_sha256"]:
        raise LedgerContractError("application record sha mismatch")
    stage = str(record.get("stage") or "").upper()
    if stage not in ALLOWED_STAGES:
        raise LedgerContractError(f"unsupported stage: {stage}")
    for field in ("event_identity", "symbol", "position_trade_date", "effective_date"):
        if not str(record.get(field) or "").strip():
            raise LedgerContractError(f"{field} is required")
    expected_application_id = canonical_sha(
        {
            "event_sha256": event_sha256,
            "position_trade_date": record["position_trade_date"],
            "stage": stage,
        }
    )
    if record["application_id"] != expected_application_id:
        raise LedgerContractError("application id does not bind event, lot, and stage")
    rights = int(record.get("rights_frozen_qty") or 0)
    receivable = float(record.get("cash_receivable_delta_gross") or 0)
    cash = float(record.get("cash_balance_delta_gross") or 0)
    pending = float(record.get("pending_share_delta") or 0)
    available = float(record.get("available_share_delta") or 0)
    tax_delta = record.get("tax_delta")
    if stage == "RIGHTS_FROZEN" and not (
        rights > 0 and receivable == cash == pending == available == 0
    ):
        raise LedgerContractError("invalid RIGHTS_FROZEN deltas")
    if stage == "RECEIVABLE_CREATED" and not (
        rights == 0 and cash == available == 0 and (receivable > 0 or pending > 0)
    ):
        raise LedgerContractError("invalid RECEIVABLE_CREATED deltas")
    if stage == "CASH_PAYABLE" and not (
        rights == 0 and receivable == pending == available == 0 and cash > 0
    ):
        raise LedgerContractError("invalid CASH_PAYABLE deltas")
    if stage == "STOCK_AVAILABLE" and not (
        rights == 0 and receivable == cash == 0 and pending < 0 and available == -pending
    ):
        raise LedgerContractError("invalid STOCK_AVAILABLE deltas")
    if record.get("tax_status") == "PENDING_FIFO_SALE_LEDGER" and tax_delta is not None:
        raise LedgerContractError("pending FIFO tax cannot carry a tax delta")


def build_application_records(
    audit: dict[str, Any], review: dict[str, Any]
) -> list[dict[str, Any]]:
    audit_sha = verify_audit_artifact(audit)
    review_sha = verify_review_manifest(review, audit_sha)
    records = []
    for impact in audit.get("impacts") or []:
        if impact.get("history_complete") is not True:
            raise LedgerContractError("incomplete position history")
        if (impact.get("price_anchor_evidence") or {}).get("status") != "EVIDENCE_AVAILABLE":
            raise LedgerContractError("price anchor evidence is unavailable")
        for preview in impact.get("stage_application_preview") or []:
            stage = str(preview.get("stage") or "").upper()
            application_id = require_sha256(
                preview.get("application_key"), "stage.application_key"
            )
            expected_id = canonical_sha(
                {
                    "event_sha256": impact["event_sha256"],
                    "position_trade_date": impact["position_trade_date"],
                    "stage": stage,
                }
            )
            if application_id != expected_id:
                raise LedgerContractError("stage application key mismatch")
            share_delta = float(preview.get("share_delta_preview") or 0)
            cash_delta = float(preview.get("cash_delta_gross_preview") or 0)
            record = {
                "ledger_schema_version": LEDGER_SCHEMA,
                "application_id": application_id,
                "event_identity": impact["event_identity"],
                "event_sha256": impact["event_sha256"],
                "symbol": impact["symbol"],
                "position_trade_date": impact["position_trade_date"],
                "signal_task_id": impact.get("signal_task_id") or "",
                "stage": stage,
                "effective_date": preview["effective_date"],
                "rights_frozen_qty": int(preview.get("rights_qty") or 0),
                "cash_receivable_delta_gross": cash_delta if stage == "RECEIVABLE_CREATED" else 0.0,
                "cash_balance_delta_gross": cash_delta if stage == "CASH_PAYABLE" else 0.0,
                "pending_share_delta": (
                    share_delta
                    if stage == "RECEIVABLE_CREATED"
                    else -share_delta
                    if stage == "STOCK_AVAILABLE"
                    else 0.0
                ),
                "available_share_delta": share_delta if stage == "STOCK_AVAILABLE" else 0.0,
                "tax_delta": None,
                "tax_status": impact.get("dividend_tax_status") or "NOT_APPLICABLE",
                "source_as_of": audit["source_as_of"],
                "source_audit_sha256": audit_sha,
                "review_manifest_sha256": review_sha,
                "reviewed_by": str(review["reviewed_by"]).strip(),
                "reviewed_at": require_iso_timestamp(review["reviewed_at"], "reviewed_at"),
            }
            record["record_sha256"] = application_record_sha(record)
            validate_application_record(record)
            records.append(record)
    ids = [record["application_id"] for record in records]
    if not records:
        raise LedgerContractError("audit produced no application records")
    if len(ids) != len(set(ids)):
        raise LedgerContractError("duplicate application ids in reviewed batch")
    return sorted(records, key=lambda item: (item["effective_date"], item["stage"]))


def ensure_ledger_schema(conn: Any) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            application_id VARCHAR PRIMARY KEY,
            record_sha256 VARCHAR NOT NULL,
            ledger_schema_version VARCHAR NOT NULL,
            event_identity VARCHAR NOT NULL,
            event_sha256 VARCHAR NOT NULL,
            symbol VARCHAR NOT NULL,
            position_trade_date DATE NOT NULL,
            signal_task_id VARCHAR DEFAULT '',
            stage VARCHAR NOT NULL,
            effective_date DATE NOT NULL,
            rights_frozen_qty INTEGER DEFAULT 0,
            cash_receivable_delta_gross DOUBLE DEFAULT 0,
            cash_balance_delta_gross DOUBLE DEFAULT 0,
            pending_share_delta DOUBLE DEFAULT 0,
            available_share_delta DOUBLE DEFAULT 0,
            tax_delta DOUBLE,
            tax_status VARCHAR NOT NULL,
            source_as_of DATE NOT NULL,
            source_audit_sha256 VARCHAR NOT NULL,
            review_manifest_sha256 VARCHAR NOT NULL,
            reviewed_by VARCHAR NOT NULL,
            reviewed_at TIMESTAMPTZ NOT NULL,
            application_status VARCHAR NOT NULL DEFAULT 'APPLIED',
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_shadow_corp_action_identity
        ON {TABLE_NAME}(event_identity, position_trade_date, stage)
        """
    )


def append_application(conn: Any, record: dict[str, Any]) -> str:
    validate_application_record(record)
    existing = conn.execute(
        f"SELECT record_sha256 FROM {TABLE_NAME} WHERE application_id = ?",
        [record["application_id"]],
    ).fetchone()
    if existing:
        if existing[0] == record["record_sha256"]:
            return "ALREADY_APPLIED"
        raise ApplicationConflict("application id exists with different content")
    revision = conn.execute(
        f"""
        SELECT event_sha256
        FROM {TABLE_NAME}
        WHERE event_identity = ?
          AND position_trade_date = CAST(? AS DATE)
          AND stage = ?
        LIMIT 1
        """,
        [record["event_identity"], record["position_trade_date"], record["stage"]],
    ).fetchone()
    if revision and revision[0] != record["event_sha256"]:
        raise EventRevisionConflict("reviewed correction path required for event revision")
    columns = ["application_id", "record_sha256"] + [
        field for field in RECORD_FIELDS if field != "application_id"
    ]
    placeholders = ",".join("?" for _ in columns)
    conn.execute(
        f"INSERT INTO {TABLE_NAME} ({','.join(columns)}) VALUES ({placeholders})",
        [record[column] for column in columns],
    )
    return "APPLIED"


def append_reviewed_batch(
    conn: Any, audit: dict[str, Any], review: dict[str, Any]
) -> list[dict[str, str]]:
    records = build_application_records(audit, review)
    outcomes = []
    conn.execute("BEGIN TRANSACTION")
    try:
        for record in records:
            outcomes.append(
                {
                    "application_id": record["application_id"],
                    "stage": record["stage"],
                    "status": append_application(conn, record),
                }
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return outcomes
