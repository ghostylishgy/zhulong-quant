#!/usr/bin/env python3
"""Build a bounded, read-only Tushare business-evidence snapshot for Compass."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
ENGINE_LIB = ROOT / "01_engine" / "lib"
if str(ENGINE_LIB) not in sys.path:
    sys.path.insert(0, str(ENGINE_LIB))

from tushare_bridge import get_bridge  # noqa: E402


PROTOCOL_VERSION = "compass_business_evidence_snapshot_v0.2"
INPUT_PROTOCOL_VERSION = "compass_theme_discovery_preview_v0.2"
NODE_DICTIONARY_SCHEMA = "compass_business_node_dictionary_v0.2"
DEFAULT_NODE_DICTIONARY = ROOT / "config" / "compass_business_nodes_v0.2.json"
DEFAULT_OUTPUT_DIR = ROOT / "storage" / "reports" / "compass_ingest"
BEIJING_TZ = timezone(timedelta(hours=8))
ALLOWED_ACTIONS = ["fetch_business_evidence", "render_preview", "manual_review"]
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
    "generate_validation_task",
    "auto_approve_business_relevance",
]


def now_iso() -> str:
    return datetime.now(BEIJING_TZ).isoformat(timespec="seconds")


def clean(value: Any) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    return re.sub(r"\s+", " ", str(value).strip())


def normalized_text(value: Any) -> str:
    return clean(value).casefold()


def sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def canonical_sha(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return sha256_bytes(raw.encode("utf-8"))


def load_json_with_sha(path: Path) -> tuple[dict[str, Any], str]:
    raw = path.read_bytes()
    payload = json.loads(raw.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON object required: {path}")
    return payload, sha256_bytes(raw)


def validate_input(preview: dict[str, Any]) -> None:
    if preview.get("protocol_version") != INPUT_PROTOCOL_VERSION:
        raise ValueError("unsupported Compass discovery preview protocol")
    if preview.get("mode") != "dry_run" or preview.get("no_trade_signal") is not True:
        raise ValueError("Compass discovery preview safety boundary missing")
    if preview.get("validation_tasks"):
        raise ValueError("discovery preview must not contain validation tasks")


def validate_dictionary(dictionary: dict[str, Any]) -> None:
    if dictionary.get("schema_version") != NODE_DICTIONARY_SCHEMA:
        raise ValueError("unsupported node dictionary schema")
    if dictionary.get("no_trade_signal") is not True:
        raise ValueError("node dictionary safety boundary missing")
    theme_keys: set[str] = set()
    node_ids: set[str] = set()
    for theme in dictionary.get("themes") or []:
        theme_key = clean(theme.get("theme_key"))
        if not theme_key or theme_key in theme_keys:
            raise ValueError("node dictionary theme_key is missing or duplicated")
        theme_keys.add(theme_key)
        if not theme.get("compass_line_keys") or not theme.get("theme_match_terms"):
            raise ValueError(f"node dictionary theme is incomplete: {theme_key}")
        for node in theme.get("nodes") or []:
            node_id = clean(node.get("node_id"))
            if not node_id or node_id in node_ids or not node.get("terms"):
                raise ValueError(f"node dictionary node is invalid: {node_id}")
            terms = {normalized_text(value) for value in node.get("terms") or []}
            qualifying_terms = {
                normalized_text(value)
                for value in node.get("review_qualifying_terms") or []
            }
            if not qualifying_terms or not qualifying_terms.issubset(terms):
                raise ValueError(
                    f"node dictionary review terms are invalid: {node_id}"
                )
            node_ids.add(node_id)


def validate_evidence_as_of(value: str) -> date:
    parsed = datetime.strptime(value, "%Y-%m-%d").date()
    today_beijing = datetime.now(BEIJING_TZ).date()
    if parsed > today_beijing:
        raise ValueError("evidence_as_of cannot be in the future")
    return parsed


def theme_context(row: dict[str, Any]) -> str:
    values = [
        row.get("source_theme_name"),
        " ".join(row.get("supply_chain_nodes") or []),
        " ".join(row.get("discovery_keywords") or []),
        " ".join(row.get("bottleneck_hypotheses") or []),
    ]
    return normalized_text(" ".join(clean(value) for value in values))


def match_theme_definition(
    row: dict[str, Any], dictionary: dict[str, Any]
) -> dict[str, Any] | None:
    line = clean(row.get("compass_line_key"))
    context = theme_context(row)
    for theme in dictionary.get("themes") or []:
        lines = {clean(value) for value in theme.get("compass_line_keys") or []}
        terms = [normalized_text(value) for value in theme.get("theme_match_terms") or []]
        if line in lines and any(term and term in context for term in terms):
            return theme
    return None


def eligible_rows(
    preview: dict[str, Any], include_anchors: bool, include_broad: bool
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in preview.get("review_shortlist") or []:
        if row.get("mapping_pool") != "review_shortlist":
            continue
        if row.get("review_eligible") is not True or row.get("generate_task") is not False:
            raise ValueError("unsafe review_shortlist row")
        item = dict(row)
        item["candidate_scope"] = "review_shortlist"
        rows.append(item)
    if include_broad:
        for row in preview.get("broad_universe") or []:
            if row.get("mapping_pool") != "broad_universe":
                continue
            if row.get("review_eligible") is not False or row.get("generate_task") is not False:
                raise ValueError("unsafe broad_universe row")
            item = dict(row)
            item["candidate_scope"] = "broad_universe_recheck"
            rows.append(item)
    if include_anchors:
        for row in preview.get("anchor_reference") or []:
            if row.get("mapping_pool") != "anchor_reference" or row.get("generate_task") is not False:
                raise ValueError("unsafe anchor_reference row")
            if row.get("exact_resolved") is not True or not clean(row.get("symbol")):
                continue
            item = dict(row)
            item["candidate_scope"] = "anchor_reference"
            rows.append(item)
    unique: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (
            clean(row.get("source_candidate_id")),
            clean(row.get("symbol")),
            clean(row.get("candidate_scope")),
        )
        unique.setdefault(key, row)
    return sorted(unique.values(), key=lambda row: (
        clean(row.get("source_candidate_id")),
        clean(row.get("candidate_scope")),
        clean(row.get("symbol")),
    ))


def call_api(api: Any, endpoint: str, **params: Any) -> pd.DataFrame:
    if not hasattr(api, endpoint):
        raise RuntimeError(f"Tushare SDK missing API: {endpoint}")
    frame = getattr(api, endpoint)(**params)
    if frame is None:
        return pd.DataFrame()
    if not isinstance(frame, pd.DataFrame):
        raise RuntimeError(f"Tushare API returned non-DataFrame: {endpoint}")
    return frame


def scalar(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        value = value.item()
    return value


def frame_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return [
        {clean(key): scalar(value) for key, value in row.items()}
        for row in frame.to_dict("records")
    ]


def node_matches(fields: dict[str, Any], nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for node in nodes:
        for field, raw in fields.items():
            text = normalized_text(raw)
            terms = [clean(term) for term in node.get("terms") or []]
            hits = sorted({term for term in terms if normalized_text(term) in text})
            if hits:
                qualifying = {
                    normalized_text(term)
                    for term in node.get("review_qualifying_terms") or []
                }
                qualifying_hits = sorted({
                    term for term in hits if normalized_text(term) in qualifying
                })
                matches.append({
                    "node_id": node["node_id"],
                    "node_label": node.get("label"),
                    "matched_field": field,
                    "matched_terms": hits,
                    "review_qualifying_terms": qualifying_hits,
                    "review_qualifying": bool(qualifying_hits),
                    "evidence_specificity": (
                        "specific_node" if qualifying_hits else "generic_node_context"
                    ),
                    "matched_text": clean(raw),
                })
    return matches


def evidence_item(kind: str, source: str, payload: dict[str, Any]) -> dict[str, Any]:
    item = {"type": kind, "source": source, **payload}
    item["evidence_item_id"] = f"BIZ-{canonical_sha(item)[:16].upper()}"
    return item


def report_period_age_days(period: Any, evidence_as_of: str) -> int | None:
    text = clean(period)
    if not re.fullmatch(r"\d{8}", text):
        return None
    return (
        datetime.strptime(evidence_as_of, "%Y-%m-%d").date()
        - datetime.strptime(text, "%Y%m%d").date()
    ).days


def symbol_exchange(symbol: str) -> str:
    suffix = clean(symbol).upper().rsplit(".", 1)[-1]
    mapping = {"SH": "SSE", "SZ": "SZSE", "BJ": "BSE"}
    if suffix not in mapping:
        raise ValueError(f"unsupported A-share symbol: {symbol}")
    return mapping[suffix]


def fetch_company_sources(
    api: Any, symbols: set[str]
) -> tuple[dict[str, dict[str, Any]], int]:
    sources = {
        symbol: {"company_fields": {}, "api_errors": [], "mainbz_query_status": "NOT_EVALUATED"}
        for symbol in symbols
    }
    by_exchange: dict[str, set[str]] = {}
    for symbol in symbols:
        by_exchange.setdefault(symbol_exchange(symbol), set()).add(symbol)
    calls = 0
    for exchange, exchange_symbols in sorted(by_exchange.items()):
        calls += 1
        try:
            company = call_api(
                api,
                "stock_company",
                exchange=exchange,
                fields="ts_code,com_name,introduction,main_business,business_scope",
            )
            for record in frame_records(company):
                symbol = clean(record.get("ts_code"))
                if symbol not in exchange_symbols:
                    continue
                sources[symbol]["company_fields"] = {
                    "main_business": record.get("main_business"),
                    "business_scope": record.get("business_scope"),
                    "introduction": record.get("introduction"),
                }
        except Exception as exc:
            error = f"stock_company:{exchange}:{clean(exc)[:220]}"
            for symbol in exchange_symbols:
                sources[symbol]["api_errors"].append(error)
    return sources, calls


def fetch_mainbz_source(api: Any, symbol: str, evidence_as_of: str) -> dict[str, Any]:
    errors: list[str] = []
    latest_period = None
    latest_product_rows: list[dict[str, Any]] = []
    try:
        mainbz = call_api(
            api,
            "fina_mainbz",
            ts_code=symbol,
            type="P",
            end_date=evidence_as_of.replace("-", ""),
            fields="ts_code,end_date,bz_item,bz_code,bz_sales,bz_profit,bz_cost,curr_type,update_flag",
        )
        records = [
            item for item in frame_records(mainbz)
            if clean(item.get("end_date")) and clean(item.get("end_date")) <= evidence_as_of.replace("-", "")
        ]
        if records:
            latest_period = max(clean(item.get("end_date")) for item in records)
            latest_product_rows = [
                item for item in records if clean(item.get("end_date")) == latest_period
            ]
    except Exception as exc:
        errors.append(f"fina_mainbz:{clean(exc)[:240]}")

    return {
        "latest_report_period_seen": latest_period,
        "latest_product_rows": latest_product_rows,
        "api_errors": errors,
        "mainbz_query_status": "FETCHED" if not errors else "FETCH_FAILED",
    }


def evaluate_symbol_evidence(
    source: dict[str, Any], row: dict[str, Any], theme: dict[str, Any],
    evidence_as_of: str,
) -> dict[str, Any]:
    symbol = clean(row.get("symbol"))
    nodes = theme.get("nodes") or []
    descriptor_matches = node_matches(source.get("company_fields") or {}, nodes)
    descriptor_items = [
        evidence_item("company_main_business", "tushare.stock_company", match)
        for match in descriptor_matches if match.get("matched_field") == "main_business"
    ]
    supporting_descriptor_items = [
        evidence_item("company_supporting_descriptor", "tushare.stock_company", match)
        for match in descriptor_matches if match.get("matched_field") != "main_business"
    ]
    reported_items: list[dict[str, Any]] = []
    latest_period = source.get("latest_report_period_seen")
    period_age = report_period_age_days(latest_period, evidence_as_of)
    for item in source.get("latest_product_rows") or []:
        matches = node_matches({"bz_item": item.get("bz_item")}, nodes)
        for match in matches:
            reported_items.append(evidence_item(
                "reported_business_item",
                "tushare.fina_mainbz",
                {
                    **match,
                    "report_period": latest_period,
                    "bz_code": item.get("bz_code"),
                    "bz_sales": item.get("bz_sales"),
                    "bz_profit": item.get("bz_profit"),
                    "bz_cost": item.get("bz_cost"),
                    "curr_type": item.get("curr_type"),
                    "revenue_share": None,
                    "revenue_share_status": "NOT_COMPUTED_UNRELIABLE_DENOMINATOR",
                },
            ))

    qualifying_descriptor_items = [
        item for item in descriptor_items if item.get("review_qualifying") is True
    ]
    qualifying_reported_items = [
        item for item in reported_items if item.get("review_qualifying") is True
    ]
    qualifying_items = qualifying_descriptor_items + qualifying_reported_items
    context_only_items = [
        item for item in descriptor_items + reported_items
        if item.get("review_qualifying") is not True
    ]
    if qualifying_reported_items:
        level = "B2_REPORTED_BUSINESS_ITEM"
        status = "direct_reported_item_match"
    elif qualifying_descriptor_items:
        level = "B1_COMPANY_DESCRIPTOR"
        status = "descriptor_match_only"
    elif context_only_items:
        level = "B0_CONTEXT_ONLY_GENERIC"
        status = "context_only_generic_node"
    else:
        level = "B0_NO_DIRECT_MATCH"
        status = "unverified"
    return {
        "source_candidate_id": row.get("source_candidate_id"),
        "source_theme_name": row.get("source_theme_name"),
        "compass_line_key": row.get("compass_line_key"),
        "candidate_scope": row.get("candidate_scope"),
        "symbol": symbol,
        "name": row.get("name"),
        "mapping_pool_unchanged": row.get("mapping_pool"),
        "source_review_eligible": bool(row.get("review_eligible")),
        "existing_review_manifest_eligible": False,
        "node_dictionary_theme_key": theme.get("theme_key"),
        "business_evidence_level": level,
        "business_relevance_status": status,
        "business_relevance_evidence_present": bool(qualifying_items),
        "business_context_evidence_present": bool(descriptor_items or reported_items),
        "business_review_eligible": bool(qualifying_items),
        "business_review_eligibility": (
            "eligible_specific_node"
            if qualifying_items
            else "context_only_generic_node"
            if context_only_items
            else "no_direct_match"
        ),
        "review_qualifying_evidence_item_ids": [
            item["evidence_item_id"] for item in qualifying_items
        ],
        "descriptor_evidence": descriptor_items,
        "supporting_descriptor_evidence": supporting_descriptor_items,
        "reported_business_evidence": reported_items,
        "latest_report_period_seen": latest_period,
        "report_period_age_days": period_age,
        "report_period_stale": period_age is not None and period_age > 550,
        "mainbz_query_status": source.get("mainbz_query_status"),
        "evidence_as_of": evidence_as_of,
        "retrieval_time_semantics": "current_api_snapshot_not_historical_reconstruction",
        "historical_point_in_time_reconstructable": False,
        "manual_review_required": True,
        "auto_approved": False,
        "generate_task": False,
        "no_trade_signal": True,
        "api_errors": source.get("api_errors") or [],
    }


def build_snapshot(
    preview: dict[str, Any], preview_sha: str, dictionary: dict[str, Any],
    dictionary_sha: str, api: Any, evidence_as_of: str, max_input_symbols: int,
    max_mainbz_symbols: int, include_anchors: bool, include_broad: bool,
) -> dict[str, Any]:
    validate_input(preview)
    validate_dictionary(dictionary)
    as_of_date = validate_evidence_as_of(evidence_as_of)
    rows = eligible_rows(preview, include_anchors, include_broad)
    matched: list[tuple[dict[str, Any], dict[str, Any]]] = []
    unmatched: set[str] = set()
    for row in rows:
        theme = match_theme_definition(row, dictionary)
        if theme:
            matched.append((row, theme))
        else:
            unmatched.add(clean(row.get("source_candidate_id")) or clean(row.get("symbol")))
    unique_symbols = {clean(row.get("symbol")) for row, _theme in matched}
    warnings = [f"no_node_dictionary_match:{value}" for value in sorted(unmatched)]
    if as_of_date < datetime.now(BEIJING_TZ).date():
        warnings.append("historical_evidence_as_of_not_point_in_time_reconstructable")
    quality_status = "DATA_OK"
    evidence_rows: list[dict[str, Any]] = []
    stock_company_calls = 0
    mainbz_calls = 0
    if len(unique_symbols) > max_input_symbols:
        quality_status = "INPUT_LIMIT_EXCEEDED"
        warnings.append(
            f"input_limit_exceeded:{len(unique_symbols)}>{max_input_symbols}:no_api_calls"
        )
    else:
        company_sources, stock_company_calls = fetch_company_sources(api, unique_symbols)
        preliminary: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]] = []
        mainbz_symbols: set[str] = set()
        for row, theme in matched:
            symbol = clean(row.get("symbol"))
            source = company_sources[symbol]
            item = evaluate_symbol_evidence(source, row, theme, evidence_as_of)
            preliminary.append((row, theme, item))
            if item.get("descriptor_evidence"):
                mainbz_symbols.add(symbol)

        mainbz_sources: dict[str, dict[str, Any]] = {}
        if len(mainbz_symbols) > max_mainbz_symbols:
            quality_status = "PARTIAL_MAINBZ_LIMIT"
            warnings.append(
                f"mainbz_limit_exceeded:{len(mainbz_symbols)}>{max_mainbz_symbols}:no_mainbz_calls"
            )
            for symbol in mainbz_symbols:
                company_sources[symbol]["mainbz_query_status"] = "NOT_REQUESTED_LIMIT_EXCEEDED"
        else:
            for symbol in sorted(mainbz_symbols):
                mainbz_sources[symbol] = fetch_mainbz_source(api, symbol, evidence_as_of)
                mainbz_calls += 1

        for row, theme, preliminary_item in preliminary:
            symbol = clean(row.get("symbol"))
            source = dict(company_sources[symbol])
            if symbol in mainbz_sources:
                source["latest_report_period_seen"] = mainbz_sources[symbol]["latest_report_period_seen"]
                source["latest_product_rows"] = mainbz_sources[symbol]["latest_product_rows"]
                source["mainbz_query_status"] = mainbz_sources[symbol]["mainbz_query_status"]
                source["api_errors"] = (
                    source.get("api_errors") or []
                ) + (mainbz_sources[symbol].get("api_errors") or [])
            elif not preliminary_item.get("descriptor_evidence"):
                source["mainbz_query_status"] = "NOT_REQUESTED_NO_DESCRIPTOR_MATCH"
            evidence_rows.append(evaluate_symbol_evidence(source, row, theme, evidence_as_of))
        if any(row.get("api_errors") for row in evidence_rows):
            quality_status = "PARTIAL_API_ERRORS"
        elif not evidence_rows:
            quality_status = "NO_ELIGIBLE_ROWS"

    return {
        "batch_id": preview.get("batch_id") or "batch",
        "source": "Compass/ima + Tushare",
        "snapshot_type": "business_evidence",
        "protocol_version": PROTOCOL_VERSION,
        "generated_at": now_iso(),
        "evidence_as_of": evidence_as_of,
        "mode": "dry_run",
        "dry_run": True,
        "no_trade_signal": True,
        "quality_status": quality_status,
        "source_discovery_preview_sha256": preview_sha,
        "node_dictionary_id": dictionary.get("dictionary_id"),
        "node_dictionary_sha256": dictionary_sha,
        "node_dictionary_schema": dictionary.get("schema_version"),
        "parameters": {
            "max_input_symbols": max_input_symbols,
            "max_mainbz_symbols": max_mainbz_symbols,
            "include_anchors": include_anchors,
            "include_broad_universe_recheck": include_broad,
        },
        "time_semantics": {
            "api_snapshot_retrieved_now": True,
            "historical_point_in_time_reconstruction": False,
            "report_period_is_not_publication_timestamp": True,
        },
        "evidence_policy": {
            "B0_NO_DIRECT_MATCH": "no business relevance evidence",
            "B0_CONTEXT_ONLY_GENERIC": "generic node term only; retained as context and not review eligible",
            "B1_COMPANY_DESCRIPTOR": "direct company descriptor match; manual review only",
            "B2_REPORTED_BUSINESS_ITEM": "reported product item match; revenue share not inferred",
            "business_scope_or_introduction_alone": "supporting only; remains B0",
            "pool_changes_allowed": False,
            "task_generation_allowed": False,
            "manual_review_required": True,
        },
        "stats": {
            "input_rows": len(rows),
            "dictionary_matched_rows": len(matched),
            "unique_symbols": len(unique_symbols),
            "stock_company_api_calls": stock_company_calls,
            "fina_mainbz_api_calls": mainbz_calls,
            "descriptor_matched_symbols": len({
                row.get("symbol") for row in evidence_rows if row.get("descriptor_evidence")
            }),
            "evidence_rows": len(evidence_rows),
            "business_evidence_present_rows": sum(
                1 for row in evidence_rows if row.get("business_relevance_evidence_present")
            ),
            "business_review_eligible_rows": sum(
                1 for row in evidence_rows if row.get("business_review_eligible")
            ),
            "context_only_generic_rows": sum(
                1 for row in evidence_rows
                if row.get("business_review_eligibility") == "context_only_generic_node"
            ),
            "supporting_only_rows": sum(
                1 for row in evidence_rows
                if not row.get("business_context_evidence_present")
                and row.get("supporting_descriptor_evidence")
            ),
            "stale_report_period_rows": sum(
                1 for row in evidence_rows if row.get("report_period_stale")
            ),
            "api_error_rows": sum(1 for row in evidence_rows if row.get("api_errors")),
            "generated_tasks": 0,
        },
        "business_evidence": evidence_rows,
        "business_evidence_matches": [
            row for row in evidence_rows if row.get("business_relevance_evidence_present")
        ],
        "business_context_only_matches": [
            row for row in evidence_rows
            if row.get("business_review_eligibility") == "context_only_generic_node"
        ],
        "warnings": warnings,
        "validation_tasks": [],
        "allowed_actions": ALLOWED_ACTIONS,
        "blocked_actions": BLOCKED_ACTIONS,
    }


def md_table(headers: list[str], rows: list[list[Any]]) -> list[str]:
    out = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        out.append("| " + " | ".join(clean(value).replace("|", "/") for value in row) + " |")
    return out


def render_preview(payload: dict[str, Any]) -> str:
    lines = [
        f"# Compass Business Evidence Snapshot - {payload['batch_id']}",
        "",
        f"- quality_status: `{payload['quality_status']}`",
        f"- evidence_as_of: `{payload['evidence_as_of']}`",
        f"- node_dictionary_sha256: `{payload['node_dictionary_sha256']}`",
        "- mode: `dry_run`",
        "- no_trade_signal: `true`",
        "",
        "## Stats",
        "",
    ]
    lines += md_table(["metric", "value"], [[key, value] for key, value in payload["stats"].items()])
    rows = []
    for item in payload.get("business_evidence_matches") or []:
        nodes = sorted({
            evidence.get("node_label") or evidence.get("node_id")
            for evidence in (item.get("descriptor_evidence") or []) + (item.get("reported_business_evidence") or [])
        })
        rows.append([
            item.get("symbol"), item.get("name"), item.get("candidate_scope"),
            item.get("business_evidence_level"), ", ".join(nodes),
            item.get("business_review_eligibility"), item.get("latest_report_period_seen"),
            item.get("report_period_stale"), True, False,
        ])
    lines += ["", "## Business Evidence", ""] + md_table(
        ["symbol", "name", "scope", "level", "nodes", "review_eligibility", "report_period", "stale", "manual_review", "generate_task"],
        rows,
    )
    context_rows = []
    for item in payload.get("business_context_only_matches") or []:
        nodes = sorted({
            evidence.get("node_label") or evidence.get("node_id")
            for evidence in (item.get("descriptor_evidence") or [])
            + (item.get("reported_business_evidence") or [])
        })
        context_rows.append([
            item.get("symbol"), item.get("name"), item.get("candidate_scope"),
            item.get("business_evidence_level"), ", ".join(nodes),
            item.get("business_review_eligibility"), False,
        ])
    lines += ["", "## Context Only Generic Matches", ""] + md_table(
        ["symbol", "name", "scope", "level", "nodes", "review_eligibility", "generate_task"],
        context_rows,
    )
    lines += ["", "## Warnings", ""]
    lines += [f"- `{warning}`" for warning in payload.get("warnings") or []] or ["- none"]
    lines += [
        "",
        "## Safety Boundary",
        "",
        "This snapshot does not change discovery pools, approve business relevance, generate validation tasks, write DuckDB/Shadow/RAG/nexus_audits, call decision_engine/Nexus, or trigger daemon. Report periods are not publication timestamps; the snapshot cannot reconstruct historical point-in-time availability.",
        "",
    ]
    return "\n".join(lines)


def atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(content, encoding="utf-8")
    temp.replace(path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True, help="Compass theme-discovery preview JSON")
    parser.add_argument("--node-dictionary", type=Path, default=DEFAULT_NODE_DICTIONARY)
    parser.add_argument("--evidence-as-of", default=datetime.now(BEIJING_TZ).date().isoformat())
    parser.add_argument("--max-input-symbols", type=int, default=500)
    parser.add_argument("--max-mainbz-symbols", type=int, default=30)
    parser.add_argument("--exclude-anchors", action="store_true")
    parser.add_argument("--exclude-broad-universe", action="store_true")
    parser.add_argument("--write-artifacts", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.max_input_symbols < 1 or args.max_input_symbols > 1000:
        raise SystemExit("--max-input-symbols must be between 1 and 1000")
    if args.max_mainbz_symbols < 1 or args.max_mainbz_symbols > 50:
        raise SystemExit("--max-mainbz-symbols must be between 1 and 50")
    preview, preview_sha = load_json_with_sha(args.input.resolve())
    dictionary, dictionary_sha = load_json_with_sha(args.node_dictionary.resolve())
    bridge = get_bridge()
    api = getattr(bridge, "api", None)
    if not getattr(bridge, "available", False) or api is None:
        raise SystemExit("Tushare bridge API unavailable; token or SDK not ready")
    payload = build_snapshot(
        preview, preview_sha, dictionary, dictionary_sha, api,
        args.evidence_as_of, args.max_input_symbols, args.max_mainbz_symbols,
        not args.exclude_anchors, not args.exclude_broad_universe,
    )
    payload["payload_sha256"] = canonical_sha(payload)
    markdown = render_preview(payload)
    if args.write_artifacts:
        batch = re.sub(r"[^0-9A-Za-zW_-]", "", clean(payload.get("batch_id"))) or "batch"
        json_path = args.output_dir / f"compass_business_evidence_snapshot_{batch}.json"
        md_path = args.output_dir / f"zhulong_compass_business_evidence_snapshot_{batch}.md"
        atomic_write(json_path, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
        atomic_write(md_path, markdown)
        print(json.dumps({"json": str(json_path), "preview": str(md_path), "quality_status": payload["quality_status"]}, ensure_ascii=False))
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
