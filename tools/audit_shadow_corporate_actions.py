#!/usr/bin/env python3
"""Read-only Shadow corporate-action impact audit for BL-018."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = Path("/root/quant_project/storage/database/zhulong.duckdb")
DEFAULT_REPORT_DIR = PROJECT_ROOT / "storage" / "reports" / "corporate_action_audit"

IMPLEMENTED = "实施"
SCHEMA_VERSION = "shadow_corporate_action_audit_v0.3"


def clean(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"none", "nan", "nat"} else text


def clean_date(value: Any) -> str:
    text = clean(value)
    if not text:
        return ""
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10]
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 8:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
    raise ValueError(f"invalid date: {value}")


def parse_date(value: Any) -> date | None:
    text = clean_date(value)
    return datetime.strptime(text, "%Y-%m-%d").date() if text else None


def safe_float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def canonical_sha(payload: dict[str, Any]) -> str:
    body = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Position:
    symbol: str
    position_trade_date: str
    signal_task_id: str
    initial_qty: int
    current_qty: int
    realized_qty: int
    exit_trade_date: str


@dataclass(frozen=True)
class Sale:
    symbol: str
    position_trade_date: str
    trade_date: str
    idempotency_key: str
    qty_sold: int


def normalize_position_evidence(positions: Iterable[Position]) -> list[dict[str, Any]]:
    return sorted(
        (
            {
                "symbol": clean(position.symbol).upper(),
                "position_trade_date": clean_date(position.position_trade_date),
                "signal_task_id": clean(position.signal_task_id),
                "initial_qty": int(position.initial_qty or 0),
                "current_qty": int(position.current_qty or 0),
                "realized_qty": int(position.realized_qty or 0),
                "exit_trade_date": clean_date(position.exit_trade_date),
            }
            for position in positions
        ),
        key=lambda item: (
            item["position_trade_date"],
            item["symbol"],
            item["signal_task_id"],
        ),
    )


def normalize_sale_evidence(
    sales: Iterable[Sale],
) -> tuple[list[dict[str, Any]], list[str]]:
    by_key: dict[str, dict[str, Any]] = {}
    warnings: list[str] = []
    for sale in sales:
        item = {
            "symbol": clean(sale.symbol).upper(),
            "position_trade_date": clean_date(sale.position_trade_date),
            "trade_date": clean_date(sale.trade_date),
            "idempotency_key": clean(sale.idempotency_key),
            "qty_sold": int(sale.qty_sold or 0),
        }
        key = item["idempotency_key"]
        if not key or not item["symbol"] or item["qty_sold"] <= 0:
            warnings.append(f"INVALID_SALE_EVIDENCE:{key or 'MISSING_KEY'}")
            continue
        existing = by_key.get(key)
        if existing and existing != item:
            warnings.append(f"SALE_EVIDENCE_CONFLICT:{key}")
            continue
        by_key[key] = item
    return (
        sorted(
            by_key.values(),
            key=lambda item: (
                item["trade_date"],
                item["symbol"],
                item["idempotency_key"],
            ),
        ),
        warnings,
    )


def event_type(raw: dict[str, Any]) -> str:
    has_cash = safe_float(raw.get("cash_div")) != 0 or safe_float(raw.get("cash_div_tax")) != 0
    has_stock = any(
        safe_float(raw.get(key)) != 0
        for key in ("stk_div", "stk_bo_rate", "stk_co_rate")
    )
    if has_cash and has_stock:
        return "CASH_AND_STOCK"
    if has_stock:
        return "STOCK_DIVIDEND"
    if has_cash:
        return "CASH_DIVIDEND"
    return "NO_DISTRIBUTION"


def normalize_event(raw: dict[str, Any], source_as_of: str) -> dict[str, Any] | None:
    if clean(raw.get("div_proc")) != IMPLEMENTED:
        return None
    symbol = clean(raw.get("ts_code") or raw.get("symbol")).upper()
    record_date = clean_date(raw.get("record_date"))
    ex_date = clean_date(raw.get("ex_date"))
    announcement_date = clean_date(raw.get("ann_date"))
    if not symbol or not record_date or not ex_date:
        return None
    if announcement_date and parse_date(announcement_date) > parse_date(source_as_of):
        return None
    stock_div = safe_float(raw.get("stk_div"))
    if stock_div == 0:
        stock_div = safe_float(raw.get("stk_bo_rate")) + safe_float(raw.get("stk_co_rate"))
    normalized = {
        "symbol": symbol,
        "report_period": clean_date(raw.get("end_date")),
        "announcement_date": announcement_date,
        "record_date": record_date,
        "ex_date": ex_date,
        "pay_date": clean_date(raw.get("pay_date")),
        "div_listdate": clean_date(raw.get("div_listdate")),
        "event_type": event_type(raw),
        "cash_div_per_share": safe_float(raw.get("cash_div")),
        "cash_div_tax_reference": safe_float(raw.get("cash_div_tax")),
        "stock_div_per_share": stock_div,
        "stock_bonus_rate": safe_float(raw.get("stk_bo_rate")),
        "stock_conversion_rate": safe_float(raw.get("stk_co_rate")),
        "source": clean(raw.get("source")) or "TUSHARE_DIVIDEND",
        "source_as_of": clean_date(source_as_of),
        "source_semantics": "RETROSPECTIVE_CURRENT_SOURCE",
    }
    normalized["event_identity"] = "|".join(
        [
            normalized["symbol"],
            normalized["report_period"],
            "DIVIDEND_IMPLEMENTATION",
        ]
    )
    normalized["event_sha256"] = canonical_sha(
        {
            key: value for key, value in normalized.items()
            if key not in {"source", "source_as_of", "source_semantics"}
        }
    )
    return normalized


def dedupe_events(rows: Iterable[dict[str, Any]], source_as_of: str) -> tuple[list[dict[str, Any]], list[str]]:
    by_sha: dict[str, dict[str, Any]] = {}
    identity_hashes: dict[str, set[str]] = {}
    for raw in rows:
        event = normalize_event(dict(raw), source_as_of)
        if event is None:
            continue
        by_sha[event["event_sha256"]] = event
        identity_hashes.setdefault(event["event_identity"], set()).add(event["event_sha256"])
    warnings = [
        f"EVENT_REVISION_DETECTED:{identity}"
        for identity, hashes in sorted(identity_hashes.items())
        if len(hashes) > 1
    ]
    return sorted(by_sha.values(), key=lambda item: (item["record_date"], item["symbol"])), warnings


def quantity_at_record_date(position: Position, sales: Iterable[Sale], record_date: str) -> tuple[int, int]:
    record = parse_date(record_date)
    entry = parse_date(position.position_trade_date)
    if record is None or entry is None or entry > record:
        return 0, 0
    exit_date = parse_date(position.exit_trade_date)
    if exit_date is not None and exit_date < record:
        return 0, 0
    sold = sum(
        max(0, int(item.qty_sold))
        for item in sales
        if item.symbol == position.symbol
        and item.position_trade_date == position.position_trade_date
        and parse_date(item.trade_date) is not None
        and parse_date(item.trade_date) <= record
    )
    return max(0, int(position.initial_qty) - sold), sold


def stage_application_key(event_sha256: str, position_trade_date: str, stage: str) -> str:
    return canonical_sha(
        {
            "event_sha256": clean(event_sha256),
            "position_trade_date": clean_date(position_trade_date),
            "stage": clean(stage).upper(),
        }
    )


def build_stage_preview(
    event: dict[str, Any], position: Position, rights_qty: int
) -> list[dict[str, Any]]:
    cash_gross = round(rights_qty * event["cash_div_per_share"], 6)
    pending_stock = round(rights_qty * event["stock_div_per_share"], 6)
    stages = [
        ("RIGHTS_FROZEN", event["record_date"], rights_qty, 0.0, 0.0),
        ("RECEIVABLE_CREATED", event["ex_date"], 0, cash_gross, pending_stock),
    ]
    if cash_gross and event["pay_date"]:
        stages.append(("CASH_PAYABLE", event["pay_date"], 0, cash_gross, 0.0))
    if pending_stock and event["div_listdate"]:
        stages.append(("STOCK_AVAILABLE", event["div_listdate"], 0, 0.0, pending_stock))
    return [
        {
            "application_key": stage_application_key(
                event["event_sha256"], position.position_trade_date, stage
            ),
            "stage": stage,
            "effective_date": effective_date,
            "rights_qty": stage_rights_qty,
            "cash_delta_gross_preview": cash_delta,
            "share_delta_preview": share_delta,
        }
        for stage, effective_date, stage_rights_qty, cash_delta, share_delta in stages
    ]


def summarize_impacts(impacts: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    totals: dict[tuple[str, str], dict[str, Any]] = {}
    for impact in impacts:
        key = (impact["event_sha256"], impact["symbol"])
        item = totals.setdefault(
            key,
            {
                "event_sha256": impact["event_sha256"],
                "event_identity": impact["event_identity"],
                "symbol": impact["symbol"],
                "position_lot_count": 0,
                "rights_frozen_qty_total": 0,
                "cash_receivable_gross_total": 0.0,
                "pending_stock_qty_total": 0.0,
            },
        )
        item["position_lot_count"] += 1
        item["rights_frozen_qty_total"] += int(impact["rights_frozen_qty"])
        item["cash_receivable_gross_total"] += float(impact["cash_receivable_gross"])
        item["pending_stock_qty_total"] += float(impact["pending_stock_qty_raw"])
    for item in totals.values():
        item["cash_receivable_gross_total"] = round(
            item["cash_receivable_gross_total"], 6
        )
        item["pending_stock_qty_total"] = round(item["pending_stock_qty_total"], 6)
    return sorted(totals.values(), key=lambda item: (item["symbol"], item["event_sha256"]))


def build_price_anchor_evidence(
    event: dict[str, Any], price_rows: Iterable[dict[str, Any]] | None
) -> dict[str, Any]:
    if price_rows is None:
        return {"status": "NOT_REQUESTED"}
    symbol_rows = sorted(
        (
            {
                "trade_date": clean_date(row.get("trade_date")),
                "close": safe_float(row.get("close")),
                "pre_close": safe_float(row.get("pre_close")),
            }
            for row in price_rows
            if clean(row.get("symbol")).upper() == event["symbol"]
        ),
        key=lambda item: item["trade_date"],
    )
    ex_row = next(
        (item for item in symbol_rows if item["trade_date"] == event["ex_date"]), None
    )
    prior_rows = [item for item in symbol_rows if item["trade_date"] < event["ex_date"]]
    if ex_row is None or not prior_rows:
        return {"status": "UNAVAILABLE"}
    prior = prior_rows[-1]
    denominator = 1.0 + float(event["stock_div_per_share"])
    if prior["close"] <= 0 or ex_row["pre_close"] <= 0 or denominator <= 0:
        return {"status": "INVALID_PRICE_EVIDENCE"}
    theoretical = (
        prior["close"] - float(event["cash_div_per_share"])
    ) / denominator
    delta = ex_row["pre_close"] - theoretical
    return {
        "status": "EVIDENCE_AVAILABLE",
        "prior_trade_date": prior["trade_date"],
        "prior_raw_close": round(prior["close"], 6),
        "ex_date": event["ex_date"],
        "ex_date_pre_close": round(ex_row["pre_close"], 6),
        "theoretical_reference": round(theoretical, 6),
        "absolute_delta": round(delta, 6),
        "relative_delta": round(delta / theoretical, 8) if theoretical else None,
        "authority": "CROSS_CHECK_ONLY",
    }


def load_positions(conn) -> list[Position]:
    rows = conn.execute(
        """
        SELECT symbol, CAST(trade_date AS VARCHAR), COALESCE(signal_task_id, ''),
               COALESCE(initial_qty, 0), COALESCE(qty, 0),
               COALESCE(realized_qty, 0),
               COALESCE(CAST(exit_trade_date AS VARCHAR), '')
        FROM fact_paper_positions
        ORDER BY trade_date, symbol
        """
    ).fetchall()
    return [Position(*row) for row in rows]


def load_sales(conn) -> list[Sale]:
    rows = conn.execute(
        """
        SELECT symbol, CAST(position_trade_date AS VARCHAR), CAST(trade_date AS VARCHAR),
               idempotency_key, COALESCE(qty_sold, 0)
        FROM fact_shadow_intraday_decisions
        WHERE UPPER(COALESCE(action_intent, '')) = 'SELL'
          AND COALESCE(qty_sold, 0) > 0
        ORDER BY trade_date, check_time
        """
    ).fetchall()
    return [Sale(*row) for row in rows]


def load_price_rows(conn, symbols: Iterable[str]) -> list[dict[str, Any]]:
    unique = sorted({clean(symbol).upper() for symbol in symbols if clean(symbol)})
    if not unique:
        return []
    placeholders = ",".join("?" for _ in unique)
    rows = conn.execute(
        f"""
        SELECT symbol, CAST(trade_date AS VARCHAR), close, pre_close
        FROM fact_daily
        WHERE symbol IN ({placeholders})
        ORDER BY symbol, trade_date
        """,
        unique,
    ).fetchall()
    return [
        {"symbol": row[0], "trade_date": row[1], "close": row[2], "pre_close": row[3]}
        for row in rows
    ]


def load_events_json(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return [dict(item) for item in payload]
    return [dict(item) for item in payload.get("events", [])]


def fetch_tushare_events(symbols: Iterable[str]) -> list[dict[str, Any]]:
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))
    from config.settings import Config
    import tushare as ts

    token = clean(getattr(Config, "TUSHARE_TOKEN", ""))
    if not token:
        raise RuntimeError("TUSHARE_TOKEN is not configured")
    api = ts.pro_api(token)
    fields = (
        "ts_code,end_date,ann_date,div_proc,stk_div,stk_bo_rate,stk_co_rate,"
        "cash_div,cash_div_tax,record_date,ex_date,pay_date,div_listdate"
    )
    rows: list[dict[str, Any]] = []
    for symbol in sorted(set(symbols)):
        frame = api.dividend(ts_code=symbol, fields=fields)
        if frame is not None and not frame.empty:
            rows.extend(frame.to_dict("records"))
    return rows


def build_audit(
    positions: list[Position],
    sales: list[Sale],
    raw_events: list[dict[str, Any]],
    source_as_of: str,
    price_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    events, warnings = dedupe_events(raw_events, source_as_of)
    position_evidence = normalize_position_evidence(positions)
    sale_evidence, sale_warnings = normalize_sale_evidence(sales)
    warnings.extend(sale_warnings)
    revised_identities = {
        item.split(":", 1)[1]
        for item in warnings
        if item.startswith("EVENT_REVISION_DETECTED:")
    }
    impacts: list[dict[str, Any]] = []
    for position in positions:
        position_events = [
            event for event in events
            if event["symbol"] == position.symbol
            and event["event_identity"] not in revised_identities
            and parse_date(position.position_trade_date) <= parse_date(event["record_date"])
            and (
                parse_date(position.exit_trade_date) is None
                or parse_date(position.exit_trade_date) >= parse_date(event["record_date"])
            )
        ]
        if not position_events:
            continue
        position_sales = [
            item for item in sales
            if item.symbol == position.symbol
            and item.position_trade_date == position.position_trade_date
        ]
        reconstructed_realized = sum(max(0, item.qty_sold) for item in position_sales)
        history_complete = reconstructed_realized == int(position.realized_qty)
        if not history_complete:
            warnings.append(
                "PARTIAL_HISTORY:"
                f"{position.symbol}:{position.position_trade_date}:"
                f"sales={reconstructed_realized}:position={position.realized_qty}"
            )
        for event in position_events:
            rights_qty, sold_through_record = quantity_at_record_date(
                position, position_sales, event["record_date"]
            )
            if rights_qty <= 0:
                continue
            cash_receivable_gross = round(
                rights_qty * event["cash_div_per_share"], 6
            )
            pending_stock_qty = round(
                rights_qty * event["stock_div_per_share"], 6
            )
            stage_preview = build_stage_preview(event, position, rights_qty)
            impacts.append(
                {
                    "symbol": position.symbol,
                    "position_trade_date": position.position_trade_date,
                    "signal_task_id": position.signal_task_id,
                    "event_identity": event["event_identity"],
                    "event_sha256": event["event_sha256"],
                    "record_date": event["record_date"],
                    "ex_date": event["ex_date"],
                    "pay_date": event["pay_date"],
                    "div_listdate": event["div_listdate"],
                    "initial_qty": position.initial_qty,
                    "realized_qty_total": position.realized_qty,
                    "sold_through_record_date": sold_through_record,
                    "rights_frozen_qty": rights_qty,
                    "cash_receivable_gross": cash_receivable_gross,
                    "cash_receivable_net": None,
                    "dividend_tax_status": (
                        "PENDING_FIFO_SALE_LEDGER"
                        if cash_receivable_gross
                        else "NOT_APPLICABLE"
                    ),
                    "pending_stock_qty_raw": pending_stock_qty,
                    "history_complete": history_complete,
                    "price_anchor_evidence": build_price_anchor_evidence(
                        event, price_rows
                    ),
                    "stage_application_preview": stage_preview,
                    "stages": {
                        "record_date": "RIGHTS_FROZEN",
                        "ex_date": "RECEIVABLE_CREATED",
                        "pay_date": "CASH_PAYABLE" if event["pay_date"] else "NOT_APPLICABLE",
                        "div_listdate": (
                            "STOCK_AVAILABLE" if event["div_listdate"] else "NOT_APPLICABLE"
                        ),
                    },
                }
            )
    for identity in sorted(revised_identities):
        warnings.append(f"AMBIGUOUS_EVENT_SKIPPED:{identity}")
    quality = "VALID_EMPTY" if not impacts else "DATA_OK"
    if any(item.startswith("PARTIAL_HISTORY:") for item in warnings):
        quality = "PARTIAL_HISTORY"
    elif any(item.startswith("EVENT_REVISION_DETECTED:") for item in warnings):
        quality = "EVENT_REVISION_DETECTED"
    elif any(
        item.startswith(("INVALID_SALE_EVIDENCE:", "SALE_EVIDENCE_CONFLICT:"))
        for item in warnings
    ):
        quality = "SCHEMA_DRIFT"
    event_totals = summarize_impacts(impacts)
    application_keys = [
        stage["application_key"]
        for impact in impacts
        for stage in impact["stage_application_preview"]
    ]
    if len(application_keys) != len(set(application_keys)):
        warnings.append("DUPLICATE_APPLICATION_PREVIEW_KEY")
        quality = "SCHEMA_DRIFT"
    price_anchor_status_counts: dict[str, int] = {}
    for impact in impacts:
        status = impact["price_anchor_evidence"]["status"]
        price_anchor_status_counts[status] = price_anchor_status_counts.get(status, 0) + 1
    report = {
        "schema_version": SCHEMA_VERSION,
        "source_as_of": clean_date(source_as_of),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": "read_only_retrospective_audit",
        "observer_only": True,
        "no_trade_signal": True,
        "quality_status": quality,
        "source_semantics": "RETROSPECTIVE_CURRENT_SOURCE",
        "positions_scanned": len(positions),
        "events_normalized": len(events),
        "impacts_found": len(impacts),
        "events": events,
        "position_evidence": position_evidence,
        "position_evidence_sha256": canonical_sha({"positions": position_evidence}),
        "sale_evidence": sale_evidence,
        "sale_evidence_sha256": canonical_sha({"sales": sale_evidence}),
        "impacts": impacts,
        "event_impact_totals": event_totals,
        "application_preview_count": len(application_keys),
        "price_anchor_status_counts": dict(sorted(price_anchor_status_counts.items())),
        "warnings": sorted(set(warnings)),
        "blocked_actions": [
            "write_duckdb",
            "mutate_shadow_cash",
            "mutate_shadow_position",
            "change_rps_or_trendhunter",
            "trigger_daemon",
        ],
    }
    report["payload_sha256"] = canonical_sha(
        {key: value for key, value in report.items() if key not in {"generated_at", "payload_sha256"}}
    )
    return report


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Shadow Corporate Action Audit",
        "",
        f"- schema_version: `{report['schema_version']}`",
        f"- source_as_of: `{report['source_as_of']}`",
        f"- quality_status: `{report['quality_status']}`",
        "- observer_only: `true`",
        "- no_trade_signal: `true`",
        f"- payload_sha256: `{report['payload_sha256']}`",
        "",
        "## Impact Summary",
        "",
        "| Symbol | Position Date | Record Date | Rights Qty | Gross Cash | Pending Shares | Tax | Price Anchor | Complete |",
        "|---|---|---|---:|---:|---:|---|---|---|",
    ]
    for item in report["impacts"]:
        lines.append(
            f"| {item['symbol']} | {item['position_trade_date']} | {item['record_date']} | "
            f"{item['rights_frozen_qty']} | {item['cash_receivable_gross']:.2f} | "
            f"{item['pending_stock_qty_raw']:.2f} | {item['dividend_tax_status']} | "
            f"{item['price_anchor_evidence']['status']} | "
            f"{str(item['history_complete']).lower()} |"
        )
    if not report["impacts"]:
        lines.append("| - | - | - | 0 | 0.00 | 0.00 | - | - | - |")
    lines.extend(["", "## Warnings", ""])
    lines.extend(f"- `{warning}`" for warning in report["warnings"])
    if not report["warnings"]:
        lines.append("- none")
    lines.extend(
        [
            "",
            "## Safety Boundary",
            "",
            "This report is retrospective and read-only. It does not write DuckDB, "
            "change Shadow state, or produce a trade signal.",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--source-as-of", required=True)
    parser.add_argument("--events-json", type=Path)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--no-write-report", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    import duckdb

    with duckdb.connect(str(args.db), read_only=True) as conn:
        positions = load_positions(conn)
        sales = load_sales(conn)
        price_rows = load_price_rows(conn, (position.symbol for position in positions))
    raw_events = (
        load_events_json(args.events_json)
        if args.events_json
        else fetch_tushare_events(position.symbol for position in positions)
    )
    report = build_audit(
        positions, sales, raw_events, args.source_as_of, price_rows=price_rows
    )
    if args.no_write_report:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0
    args.output_dir.mkdir(parents=True, exist_ok=True)
    stem = f"shadow_corporate_action_audit_{clean_date(args.source_as_of).replace('-', '')}"
    json_path = args.output_dir / f"{stem}.json"
    md_path = args.output_dir / f"{stem}.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    md_path.write_text(render_markdown(report), encoding="utf-8")
    print(json_path)
    print(md_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
