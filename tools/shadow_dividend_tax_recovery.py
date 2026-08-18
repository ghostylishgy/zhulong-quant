#!/usr/bin/env python3
"""Unwired account-level FIFO dividend-tax recovery contract for BL-018."""

from __future__ import annotations

import calendar
import hashlib
import json
import re
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tools import shadow_corporate_action_ledger as action_ledger


PREVIEW_SCHEMA = "shadow_dividend_tax_recovery_preview_v0.1"
REVIEW_SCHEMA = "shadow_dividend_tax_recovery_review_v0.1"
LEDGER_SCHEMA = "shadow_dividend_tax_recovery_ledger_v0.1"
TABLE_NAME = "fact_shadow_dividend_tax_recoveries"
APPROVE_DECISION = "APPROVE_TAX_LEDGER_DRY_RUN"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
CENT = Decimal("0.01")
SIX_DP = Decimal("0.000001")
RECORD_FIELDS = (
    "ledger_schema_version",
    "tax_application_id",
    "event_identity",
    "event_sha256",
    "symbol",
    "lot_id",
    "acquisition_trade_date",
    "signal_task_id",
    "sale_idempotency_key",
    "sale_trade_date",
    "declared_sale_position_trade_date",
    "effective_date",
    "allocated_qty",
    "holding_period_bucket",
    "tax_rate",
    "cash_div_per_share",
    "taxable_cash_amount",
    "tax_delta",
    "source_as_of",
    "source_audit_sha256",
    "source_position_evidence_sha256",
    "source_sales_evidence_sha256",
    "source_tax_preview_sha256",
    "review_manifest_sha256",
    "reviewed_by",
    "reviewed_at",
)


class TaxContractError(ValueError):
    pass


class TaxApplicationConflict(TaxContractError):
    pass


class TaxEventRevisionConflict(TaxContractError):
    pass


@dataclass(frozen=True)
class Sale:
    symbol: str
    position_trade_date: str
    trade_date: str
    idempotency_key: str
    qty_sold: int


def canonical_sha(payload: dict[str, Any]) -> str:
    body = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def clean(value: Any) -> str:
    return str(value or "").strip()


def parse_date(value: Any, field: str) -> date:
    text = clean(value)[:10]
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError as exc:
        raise TaxContractError(f"{field} must be YYYY-MM-DD") from exc


def require_sha256(value: Any, field: str) -> str:
    text = clean(value).lower()
    if not SHA256_RE.fullmatch(text):
        raise TaxContractError(f"{field} must be a lowercase sha256")
    return text


def require_iso_timestamp(value: Any, field: str) -> str:
    text = clean(value)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise TaxContractError(f"{field} must be ISO-8601") from exc
    if parsed.tzinfo is None:
        raise TaxContractError(f"{field} must be timezone-aware")
    return parsed.isoformat()


def decimal_value(value: Any) -> Decimal:
    return Decimal(str(value or 0))


def six_dp(value: Decimal) -> float:
    return float(value.quantize(SIX_DP, rounding=ROUND_HALF_UP))


def money(value: Decimal) -> float:
    return float(value.quantize(CENT, rounding=ROUND_HALF_UP))


def add_calendar_months(value: date, months: int) -> date:
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def holding_period_terms(acquisition_date: Any, sale_date: Any) -> dict[str, Any]:
    acquired = parse_date(acquisition_date, "acquisition_trade_date")
    sold = parse_date(sale_date, "sale_trade_date")
    if sold < acquired:
        raise TaxContractError("sale date precedes acquisition date")
    one_month = add_calendar_months(acquired, 1)
    one_year = add_calendar_months(acquired, 12)
    if sold <= one_month:
        bucket, rate = "LE_1_MONTH", Decimal("0.20")
    elif sold <= one_year:
        bucket, rate = "GT_1_MONTH_LE_1_YEAR", Decimal("0.10")
    else:
        bucket, rate = "GT_1_YEAR", Decimal("0.00")
    return {
        "holding_period_bucket": bucket,
        "tax_rate": float(rate),
        "holding_days": (sold - acquired).days,
        "one_month_anniversary": one_month.isoformat(),
        "one_year_anniversary": one_year.isoformat(),
    }


def lot_id(impact: dict[str, Any]) -> str:
    return canonical_sha(
        {
            "symbol": clean(impact.get("symbol")).upper(),
            "acquisition_trade_date": clean(impact.get("position_trade_date")),
            "signal_task_id": clean(impact.get("signal_task_id")),
        }
    )


def tax_application_id(
    event_sha256: str, lot_key: str, sale_idempotency_key: str
) -> str:
    return canonical_sha(
        {
            "event_sha256": event_sha256,
            "lot_id": lot_key,
            "sale_idempotency_key": sale_idempotency_key,
        }
    )


def normalize_sales(sales: Iterable[Sale | dict[str, Any]]) -> list[dict[str, Any]]:
    by_key: dict[str, dict[str, Any]] = {}
    for raw in sales:
        item = asdict(raw) if isinstance(raw, Sale) else dict(raw)
        normalized = {
            "symbol": clean(item.get("symbol")).upper(),
            "position_trade_date": parse_date(
                item.get("position_trade_date"), "position_trade_date"
            ).isoformat(),
            "trade_date": parse_date(item.get("trade_date"), "trade_date").isoformat(),
            "idempotency_key": clean(item.get("idempotency_key")),
            "qty_sold": int(item.get("qty_sold") or 0),
        }
        if not normalized["symbol"] or not normalized["idempotency_key"]:
            raise TaxContractError("sale symbol and idempotency_key are required")
        if normalized["qty_sold"] <= 0:
            raise TaxContractError("sale qty_sold must be positive")
        existing = by_key.get(normalized["idempotency_key"])
        if existing and existing != normalized:
            raise TaxContractError("sale idempotency key has conflicting content")
        by_key[normalized["idempotency_key"]] = normalized
    return sorted(
        by_key.values(),
        key=lambda item: (
            item["trade_date"],
            item["symbol"],
            item["idempotency_key"],
        ),
    )


def _event_map(audit: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for event in audit.get("events") or []:
        event_sha = require_sha256(event.get("event_sha256"), "event.event_sha256")
        result[event_sha] = event
    return result


def _validate_supported_event(event: dict[str, Any]) -> None:
    if clean(event.get("event_type")).upper() != "CASH_DIVIDEND":
        raise TaxContractError("only pure cash-dividend events are supported")
    if any(
        decimal_value(event.get(field)) != 0
        for field in (
            "stock_div_per_share",
            "stock_bonus_rate",
            "stock_conversion_rate",
        )
    ):
        raise TaxContractError("stock or conversion events require a separate tax basis")
    if decimal_value(event.get("cash_div_per_share")) <= 0:
        raise TaxContractError("cash_div_per_share must be positive")
    if not clean(event.get("pay_date")):
        raise TaxContractError("cash dividend pay_date is required")


def _allocation(
    *,
    event: dict[str, Any],
    impact: dict[str, Any],
    sale: dict[str, Any],
    qty: int,
) -> dict[str, Any]:
    terms = holding_period_terms(impact["position_trade_date"], sale["trade_date"])
    cash_per_share = decimal_value(event["cash_div_per_share"])
    taxable_cash = cash_per_share * Decimal(qty)
    tax_amount = taxable_cash * decimal_value(terms["tax_rate"])
    tax_delta = -money(tax_amount) if tax_amount else 0.0
    lot_key = lot_id(impact)
    effective_date = max(
        parse_date(sale["trade_date"], "sale_trade_date"),
        parse_date(event["pay_date"], "pay_date"),
    ).isoformat()
    result = {
        "tax_application_id": tax_application_id(
            event["event_sha256"], lot_key, sale["idempotency_key"]
        ),
        "event_identity": event["event_identity"],
        "event_sha256": event["event_sha256"],
        "symbol": event["symbol"],
        "lot_id": lot_key,
        "acquisition_trade_date": impact["position_trade_date"],
        "signal_task_id": clean(impact.get("signal_task_id")),
        "sale_idempotency_key": sale["idempotency_key"],
        "sale_trade_date": sale["trade_date"],
        "declared_sale_position_trade_date": sale["position_trade_date"],
        "effective_date": effective_date,
        "allocated_qty": qty,
        "cash_div_per_share": six_dp(cash_per_share),
        "taxable_cash_amount": six_dp(taxable_cash),
        "tax_delta": tax_delta,
    }
    result.update(terms)
    return result


def build_tax_preview(audit: dict[str, Any]) -> dict[str, Any]:
    try:
        audit_sha = action_ledger.verify_audit_artifact(audit)
    except action_ledger.LedgerContractError as exc:
        raise TaxContractError(str(exc)) from exc
    source_as_of = parse_date(audit.get("source_as_of"), "source_as_of")
    sale_rows = normalize_sales(audit.get("sale_evidence") or [])
    sales_sha = require_sha256(
        audit.get("sale_evidence_sha256"), "sale_evidence_sha256"
    )
    if sales_sha != canonical_sha({"sales": sale_rows}):
        raise TaxContractError("sale evidence sha mismatch")
    position_rows = [dict(item) for item in audit.get("position_evidence") or []]
    positions_sha = require_sha256(
        audit.get("position_evidence_sha256"), "position_evidence_sha256"
    )
    if positions_sha != canonical_sha({"positions": position_rows}):
        raise TaxContractError("position evidence sha mismatch")
    acquisition_days = {
        (
            clean(item.get("symbol")).upper(),
            parse_date(item.get("position_trade_date"), "position_trade_date").isoformat(),
        )
        for item in position_rows
        if int(item.get("initial_qty") or 0) > 0
    }
    event_by_sha = _event_map(audit)
    impacts_by_event: dict[str, list[dict[str, Any]]] = {}
    for impact in audit.get("impacts") or []:
        if impact.get("history_complete") is not True:
            raise TaxContractError("incomplete position history")
        if clean(impact.get("dividend_tax_status")) != "PENDING_FIFO_SALE_LEDGER":
            continue
        event_sha = require_sha256(impact.get("event_sha256"), "impact.event_sha256")
        event = event_by_sha.get(event_sha)
        if event is None:
            raise TaxContractError("impact references a missing event")
        _validate_supported_event(event)
        if int(impact.get("rights_frozen_qty") or 0) <= 0:
            raise TaxContractError("taxable impact must have positive rights quantity")
        realized_qty = int(impact.get("realized_qty_total") or 0)
        evidence_qty = sum(
            int(sale["qty_sold"])
            for sale in sale_rows
            if sale["symbol"] == clean(impact.get("symbol")).upper()
            and sale["position_trade_date"] == clean(impact.get("position_trade_date"))
        )
        if evidence_qty != realized_qty:
            raise TaxContractError("sale evidence does not match realized lot quantity")
        expected_gross = six_dp(
            decimal_value(event["cash_div_per_share"])
            * Decimal(int(impact["rights_frozen_qty"]))
        )
        if decimal_value(impact.get("cash_receivable_gross")) != decimal_value(expected_gross):
            raise TaxContractError("cash receivable does not match event and rights quantity")
        impacts_by_event.setdefault(event_sha, []).append(impact)

    allocations: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []
    for event_sha, impacts in sorted(impacts_by_event.items()):
        event = event_by_sha[event_sha]
        record_date = parse_date(event["record_date"], "record_date")
        lots = []
        seen_lots: set[str] = set()
        for impact in sorted(
            impacts,
            key=lambda item: (
                clean(item.get("position_trade_date")),
                clean(item.get("signal_task_id")),
            ),
        ):
            key = lot_id(impact)
            if key in seen_lots:
                raise TaxContractError("duplicate entitlement lot in audit")
            seen_lots.add(key)
            lots.append(
                {
                    "impact": impact,
                    "remaining_qty": int(impact["rights_frozen_qty"]),
                }
            )

        event_sales = []
        for sale in sale_rows:
            if sale["symbol"] != event["symbol"]:
                continue
            sold = parse_date(sale["trade_date"], "sale_trade_date")
            if sold <= record_date:
                continue
            if sold > source_as_of:
                raise TaxContractError("sale evidence exceeds source_as_of")
            if (sale["symbol"], sale["trade_date"]) in acquisition_days:
                raise TaxContractError(
                    "same-day account netting requires a separate daily-net contract"
                )
            event_sales.append(sale)

        lot_index = 0
        for sale in event_sales:
            sale_qty = int(sale["qty_sold"])
            while sale_qty > 0 and lot_index < len(lots):
                lot = lots[lot_index]
                if lot["remaining_qty"] <= 0:
                    lot_index += 1
                    continue
                allocated = min(sale_qty, lot["remaining_qty"])
                allocations.append(
                    _allocation(
                        event=event,
                        impact=lot["impact"],
                        sale=sale,
                        qty=allocated,
                    )
                )
                lot["remaining_qty"] -= allocated
                sale_qty -= allocated
                if lot["remaining_qty"] == 0:
                    lot_index += 1

        event_allocations = [
            item for item in allocations if item["event_sha256"] == event_sha
        ]
        entitled_qty = sum(int(item["rights_frozen_qty"]) for item in impacts)
        allocated_qty = sum(int(item["allocated_qty"]) for item in event_allocations)
        remaining_qty = entitled_qty - allocated_qty
        recovered_tax = round(sum(float(item["tax_delta"]) for item in event_allocations), 2)
        gross_cash = money(decimal_value(event["cash_div_per_share"]) * Decimal(entitled_qty))
        summaries.append(
            {
                "event_identity": event["event_identity"],
                "event_sha256": event_sha,
                "symbol": event["symbol"],
                "record_date": event["record_date"],
                "pay_date": event["pay_date"],
                "entitled_qty": entitled_qty,
                "allocated_sold_qty": allocated_qty,
                "remaining_entitled_qty": remaining_qty,
                "cash_receivable_gross": gross_cash,
                "tax_recovered_to_date": recovered_tax,
                "cash_receivable_net_final": (
                    round(gross_cash + recovered_tax, 2) if remaining_qty == 0 else None
                ),
                "tax_settlement_status": (
                    "SETTLED"
                    if remaining_qty == 0
                    else "PARTIALLY_SETTLED"
                    if allocated_qty > 0
                    else "PENDING_SALE"
                ),
            }
        )

    ids = [item["tax_application_id"] for item in allocations]
    if len(ids) != len(set(ids)):
        raise TaxContractError("duplicate tax application ids")
    report = {
        "schema_version": PREVIEW_SCHEMA,
        "source_as_of": source_as_of.isoformat(),
        "generated_at": datetime.now().astimezone().isoformat(),
        "mode": "isolated_fifo_tax_preview",
        "observer_only": True,
        "no_trade_signal": True,
        "quality_status": "DATA_OK" if summaries else "VALID_EMPTY",
        "source_audit_sha256": audit_sha,
        "source_position_evidence_sha256": positions_sha,
        "source_sales_evidence_sha256": sales_sha,
        "account_scope": "SHADOW_ACCOUNT_SYMBOL_FIFO",
        "supported_event_scope": "PURE_CASH_DIVIDEND_ONLY",
        "tax_policy": {
            "holding_period_basis": "CALENDAR_MONTH_YEAR",
            "sale_allocation": "ACCOUNT_LEVEL_FIFO",
            "rates": {
                "LE_1_MONTH": 0.20,
                "GT_1_MONTH_LE_1_YEAR": 0.10,
                "GT_1_YEAR": 0.00,
            },
        },
        "events_evaluated": len(summaries),
        "allocation_count": len(allocations),
        "event_summaries": summaries,
        "allocations": sorted(
            allocations,
            key=lambda item: (
                item["sale_trade_date"],
                item["symbol"],
                item["sale_idempotency_key"],
                item["acquisition_trade_date"],
            ),
        ),
        "warnings": [],
        "blocked_actions": [
            "write_production_duckdb",
            "mutate_shadow_cash",
            "mutate_shadow_position",
            "rewrite_historical_fill",
            "trigger_daemon",
        ],
    }
    report["payload_sha256"] = canonical_sha(
        {
            key: value
            for key, value in report.items()
            if key not in {"generated_at", "payload_sha256"}
        }
    )
    return report


def verify_tax_preview(preview: dict[str, Any]) -> str:
    if preview.get("schema_version") != PREVIEW_SCHEMA:
        raise TaxContractError("unsupported tax preview schema")
    if preview.get("observer_only") is not True or preview.get("no_trade_signal") is not True:
        raise TaxContractError("tax preview safety boundary missing")
    if preview.get("quality_status") != "DATA_OK":
        raise TaxContractError("tax preview is not DATA_OK")
    actual = require_sha256(preview.get("payload_sha256"), "preview.payload_sha256")
    expected = canonical_sha(
        {
            key: value
            for key, value in preview.items()
            if key not in {"generated_at", "payload_sha256"}
        }
    )
    if actual != expected:
        raise TaxContractError("tax preview payload sha mismatch")
    require_sha256(preview.get("source_audit_sha256"), "source_audit_sha256")
    require_sha256(
        preview.get("source_position_evidence_sha256"),
        "source_position_evidence_sha256",
    )
    require_sha256(
        preview.get("source_sales_evidence_sha256"),
        "source_sales_evidence_sha256",
    )
    return actual


def verify_review_manifest(review: dict[str, Any], preview_sha256: str) -> str:
    if review.get("schema_version") != REVIEW_SCHEMA:
        raise TaxContractError("unsupported tax review schema")
    if review.get("decision") != APPROVE_DECISION:
        raise TaxContractError("review does not approve tax ledger dry-run")
    if require_sha256(
        review.get("source_tax_preview_sha256"), "source_tax_preview_sha256"
    ) != preview_sha256:
        raise TaxContractError("review does not bind the tax preview")
    if not clean(review.get("reviewed_by")):
        raise TaxContractError("reviewed_by is required")
    require_iso_timestamp(review.get("reviewed_at"), "reviewed_at")
    actual = require_sha256(review.get("review_manifest_sha256"), "review_manifest_sha256")
    expected = canonical_sha(
        {key: value for key, value in review.items() if key != "review_manifest_sha256"}
    )
    if actual != expected:
        raise TaxContractError("tax review manifest sha mismatch")
    return actual


def tax_record_sha(record: dict[str, Any]) -> str:
    missing = [field for field in RECORD_FIELDS if field not in record]
    if missing:
        raise TaxContractError(f"tax record fields missing: {missing}")
    return canonical_sha({field: record[field] for field in RECORD_FIELDS})


def validate_tax_record(record: dict[str, Any]) -> None:
    if record.get("ledger_schema_version") != LEDGER_SCHEMA:
        raise TaxContractError("unsupported tax ledger schema")
    application_id = require_sha256(record.get("tax_application_id"), "tax_application_id")
    event_sha = require_sha256(record.get("event_sha256"), "event_sha256")
    lot_key = require_sha256(record.get("lot_id"), "lot_id")
    require_sha256(record.get("source_audit_sha256"), "source_audit_sha256")
    require_sha256(
        record.get("source_position_evidence_sha256"),
        "source_position_evidence_sha256",
    )
    require_sha256(
        record.get("source_sales_evidence_sha256"),
        "source_sales_evidence_sha256",
    )
    require_sha256(record.get("source_tax_preview_sha256"), "source_tax_preview_sha256")
    require_sha256(record.get("review_manifest_sha256"), "review_manifest_sha256")
    require_sha256(record.get("record_sha256"), "record_sha256")
    if tax_record_sha(record) != record["record_sha256"]:
        raise TaxContractError("tax record sha mismatch")
    expected_id = tax_application_id(
        event_sha, lot_key, clean(record.get("sale_idempotency_key"))
    )
    if application_id != expected_id:
        raise TaxContractError("tax application id does not bind event, lot, and sale")
    qty = int(record.get("allocated_qty") or 0)
    if qty <= 0:
        raise TaxContractError("allocated_qty must be positive")
    terms = holding_period_terms(
        record.get("acquisition_trade_date"), record.get("sale_trade_date")
    )
    if record.get("holding_period_bucket") != terms["holding_period_bucket"]:
        raise TaxContractError("holding period bucket mismatch")
    if decimal_value(record.get("tax_rate")) != decimal_value(terms["tax_rate"]):
        raise TaxContractError("tax rate mismatch")
    cash_per_share = decimal_value(record.get("cash_div_per_share"))
    expected_base = six_dp(cash_per_share * Decimal(qty))
    if decimal_value(record.get("taxable_cash_amount")) != decimal_value(expected_base):
        raise TaxContractError("taxable cash amount mismatch")
    expected_tax = money(decimal_value(expected_base) * decimal_value(terms["tax_rate"]))
    expected_delta = -expected_tax if expected_tax else 0.0
    if decimal_value(record.get("tax_delta")) != decimal_value(expected_delta):
        raise TaxContractError("tax delta mismatch")
    sale_date = parse_date(record.get("sale_trade_date"), "sale_trade_date")
    effective_date = parse_date(record.get("effective_date"), "effective_date")
    if effective_date < sale_date:
        raise TaxContractError("tax effective date precedes sale")


def build_tax_records(
    preview: dict[str, Any], review: dict[str, Any]
) -> list[dict[str, Any]]:
    preview_sha = verify_tax_preview(preview)
    review_sha = verify_review_manifest(review, preview_sha)
    source_audit_sha = require_sha256(
        preview.get("source_audit_sha256"), "source_audit_sha256"
    )
    source_sales_sha = require_sha256(
        preview.get("source_sales_evidence_sha256"),
        "source_sales_evidence_sha256",
    )
    source_positions_sha = require_sha256(
        preview.get("source_position_evidence_sha256"),
        "source_position_evidence_sha256",
    )
    records = []
    for allocation in preview.get("allocations") or []:
        record = {
            "ledger_schema_version": LEDGER_SCHEMA,
            "tax_application_id": allocation["tax_application_id"],
            "event_identity": allocation["event_identity"],
            "event_sha256": allocation["event_sha256"],
            "symbol": allocation["symbol"],
            "lot_id": allocation["lot_id"],
            "acquisition_trade_date": allocation["acquisition_trade_date"],
            "signal_task_id": allocation.get("signal_task_id") or "",
            "sale_idempotency_key": allocation["sale_idempotency_key"],
            "sale_trade_date": allocation["sale_trade_date"],
            "declared_sale_position_trade_date": allocation[
                "declared_sale_position_trade_date"
            ],
            "effective_date": allocation["effective_date"],
            "allocated_qty": int(allocation["allocated_qty"]),
            "holding_period_bucket": allocation["holding_period_bucket"],
            "tax_rate": float(allocation["tax_rate"]),
            "cash_div_per_share": float(allocation["cash_div_per_share"]),
            "taxable_cash_amount": float(allocation["taxable_cash_amount"]),
            "tax_delta": float(allocation["tax_delta"]),
            "source_as_of": preview["source_as_of"],
            "source_audit_sha256": source_audit_sha,
            "source_position_evidence_sha256": source_positions_sha,
            "source_sales_evidence_sha256": source_sales_sha,
            "source_tax_preview_sha256": preview_sha,
            "review_manifest_sha256": review_sha,
            "reviewed_by": clean(review["reviewed_by"]),
            "reviewed_at": require_iso_timestamp(review["reviewed_at"], "reviewed_at"),
        }
        record["record_sha256"] = tax_record_sha(record)
        validate_tax_record(record)
        records.append(record)
    if not records:
        raise TaxContractError("tax preview produced no recovery records")
    ids = [record["tax_application_id"] for record in records]
    if len(ids) != len(set(ids)):
        raise TaxContractError("duplicate tax application ids in reviewed batch")
    return sorted(
        records,
        key=lambda item: (
            item["effective_date"],
            item["symbol"],
            item["sale_idempotency_key"],
            item["acquisition_trade_date"],
        ),
    )


def ensure_tax_ledger_schema(conn: Any) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            tax_application_id VARCHAR PRIMARY KEY,
            record_sha256 VARCHAR NOT NULL,
            ledger_schema_version VARCHAR NOT NULL,
            event_identity VARCHAR NOT NULL,
            event_sha256 VARCHAR NOT NULL,
            symbol VARCHAR NOT NULL,
            lot_id VARCHAR NOT NULL,
            acquisition_trade_date DATE NOT NULL,
            signal_task_id VARCHAR DEFAULT '',
            sale_idempotency_key VARCHAR NOT NULL,
            sale_trade_date DATE NOT NULL,
            declared_sale_position_trade_date DATE NOT NULL,
            effective_date DATE NOT NULL,
            allocated_qty INTEGER NOT NULL,
            holding_period_bucket VARCHAR NOT NULL,
            tax_rate DOUBLE NOT NULL,
            cash_div_per_share DOUBLE NOT NULL,
            taxable_cash_amount DOUBLE NOT NULL,
            tax_delta DOUBLE NOT NULL,
            source_as_of DATE NOT NULL,
            source_audit_sha256 VARCHAR NOT NULL,
            source_position_evidence_sha256 VARCHAR NOT NULL,
            source_sales_evidence_sha256 VARCHAR NOT NULL,
            source_tax_preview_sha256 VARCHAR NOT NULL,
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
        CREATE INDEX IF NOT EXISTS idx_shadow_dividend_tax_revision
        ON {TABLE_NAME}(
            event_identity, lot_id, sale_idempotency_key
        )
        """
    )


def append_tax_record(conn: Any, record: dict[str, Any]) -> str:
    validate_tax_record(record)
    existing = conn.execute(
        f"SELECT record_sha256 FROM {TABLE_NAME} WHERE tax_application_id = ?",
        [record["tax_application_id"]],
    ).fetchone()
    if existing:
        if existing[0] == record["record_sha256"]:
            return "ALREADY_APPLIED"
        raise TaxApplicationConflict("tax application id exists with different content")
    revision = conn.execute(
        f"""
        SELECT event_sha256
        FROM {TABLE_NAME}
        WHERE event_identity = ?
          AND lot_id = ?
          AND sale_idempotency_key = ?
        LIMIT 1
        """,
        [
            record["event_identity"],
            record["lot_id"],
            record["sale_idempotency_key"],
        ],
    ).fetchone()
    if revision and revision[0] != record["event_sha256"]:
        raise TaxEventRevisionConflict("reviewed tax correction path required")
    columns = ["tax_application_id", "record_sha256"] + [
        field for field in RECORD_FIELDS if field != "tax_application_id"
    ]
    placeholders = ",".join("?" for _ in columns)
    conn.execute(
        f"INSERT INTO {TABLE_NAME} ({','.join(columns)}) VALUES ({placeholders})",
        [record[column] for column in columns],
    )
    return "APPLIED"


def append_reviewed_tax_batch(
    conn: Any, preview: dict[str, Any], review: dict[str, Any]
) -> list[dict[str, str]]:
    records = build_tax_records(preview, review)
    outcomes = []
    conn.execute("BEGIN TRANSACTION")
    try:
        for record in records:
            outcomes.append(
                {
                    "tax_application_id": record["tax_application_id"],
                    "status": append_tax_record(conn, record),
                }
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return outcomes
