#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Build a read-only Compass theme discovery preview.

This tool is the third dry-run step after:
1. compass_report_normalizer.py: Markdown -> normalized candidates
2. compass_ingest.py: seed-anchor exact ticker resolve / optional dry-run tasks

It reads theme_candidate rows and produces a manual-review discovery preview only.
It never maps themes directly to validation tasks, never writes DuckDB, and never
calls Nexus, decision_engine, Shadow, RAG, daemon, or any trading path.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter, defaultdict
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
PROTOCOL_VERSION = "compass_theme_discovery_preview_v0.2"
DISCOVERY_CONSTRAINTS_VERSION = "compass_discovery_constraints_v0.1"
SOURCE_SNAPSHOT_VERSION = "compass_discovery_source_snapshot_v0.2"
DEFAULT_NARROW_INDEX_MEMBER_THRESHOLD = 50
DEFAULT_DIAGNOSTIC_SAMPLE_LIMIT = 20
DEFAULT_MAX_SHORTLIST_ROWS = 100

ALLOWED_ACTIONS = ["read_only_discovery_preview", "render_preview", "manual_review"]
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
    "generate_stock_validation_from_theme",
    "write_validation_tasks_from_theme",
    "unbounded_tushare_discovery",
]
READONLY_MODULES = [
    "fact_stock_basic",
    "tushare_discovery_source_snapshot",
]

# Curated industry bridges are deliberately conservative. They only create
# preview rows with manual review required; they do not resolve a theme to a task.
DOMAIN_KEYWORD_EXPANSIONS: list[tuple[tuple[str, ...], tuple[str, ...]]] = [
    (("\u53d8\u538b\u5668", "\u7279\u9ad8\u538b", "\u5206\u63a5\u5f00\u5173", "\u7535\u529b\u8bbe\u5907", "\u897f\u90e8\u7535\u529b", "\u7b97\u7535\u5c9b", "\u56fa\u6001\u53d8\u538b\u5668"), ("\u7535\u6c14\u8bbe\u5907", "\u7535\u5668\u4eea\u8868", "\u4e13\u7528\u673a\u68b0", "\u673a\u68b0\u57fa\u4ef6", "\u65b0\u578b\u7535\u529b")),
    (("\u53d6\u5411\u7845\u94a2", "\u7845\u94a2", "\u94a2\u6750", "\u5b9d\u94a2", "\u9996\u94a2"), ("\u666e\u94a2", "\u94a2\u52a0\u5de5", "\u7279\u79cd\u94a2", "\u7535\u6c14\u8bbe\u5907")),
    (("AI Coding", "Coding", "\u4ee3\u7801", "\u767d\u540d\u5355", "\u5185\u63a7", "\u79c1\u6709\u5316\u90e8\u7f72", "\u6570\u636e\u5b89\u5168", "\u5185\u5bb9\u5b89\u5168", "\u5b89\u5168\u6d4b\u8bc4", "\u5907\u6848", "\u8ba4\u8bc1", "\u5408\u89c4"), ("\u8f6f\u4ef6\u670d\u52a1", "\u4e92\u8054\u7f51", "IT\u8bbe\u5907", "\u901a\u4fe1\u8bbe\u5907", "\u7535\u5668\u4eea\u8868")),
    (("\u653f\u4f01AI", "\u7cfb\u7edf\u96c6\u6210", "AI\u843d\u5730", "\u4f01\u4e1aAI", "\u4e2d\u95f4\u5c42", "\u5b9e\u65bd", "\u54a8\u8be2"), ("\u8f6f\u4ef6\u670d\u52a1", "IT\u8bbe\u5907", "\u4e92\u8054\u7f51", "\u901a\u4fe1\u8bbe\u5907")),
    (("\u7b97\u529b", "\u63a8\u7406", "Token", "\u670d\u52a1\u5668", "\u6570\u636e\u4e2d\u5fc3", "\u7b97\u529b\u79df\u8d41", "\u91cd\u8d44\u4ea7\u7b97\u529b"), ("IT\u8bbe\u5907", "\u901a\u4fe1\u8bbe\u5907", "\u4e92\u8054\u7f51", "\u8f6f\u4ef6\u670d\u52a1", "\u7535\u6c14\u8bbe\u5907")),
    (("ABF", "\u5c01\u88c5", "\u57fa\u677f", "HBM", "\u5b58\u50a8", "\u6750\u6599", "\u56fd\u4ea7\u66ff\u4ee3", "\u534a\u5bfc\u4f53\u6750\u6599"), ("\u534a\u5bfc\u4f53", "\u5143\u5668\u4ef6", "\u5316\u5de5\u539f\u6599", "\u5851\u6599", "\u5c0f\u91d1\u5c5e")),
]

STOP_TERMS = {
    "AI", "\u0041\u80a1", "\u884c\u4e1a", "\u73af\u8282", "\u65b9\u5411", "\u751f\u6001", "\u516c\u53f8", "\u5019\u9009", "\u9a8c\u8bc1", "\u91cd\u70b9", "\u6570\u636e",
    "\u98ce\u9669", "\u4e1a\u52a1", "\u5546\u4e1a", "\u5e02\u573a", "\u8fdb\u5165", "\u89c2\u5bdf", "\u5de5\u5177", "\u7cfb\u7edf", "\u76f8\u5173", "\u4e3b\u9898", "\u6620\u5c04",
    "high", "medium", "low", "true", "false", "PASS", "HOLD", "VETO",
}


def now_iso() -> str:
    return datetime.now(BEIJING_TZ).isoformat(timespec="seconds")


def safe_batch(batch_id: str) -> str:
    return re.sub(r"[^0-9A-Za-zW_-]", "", str(batch_id or "batch"))


def clean(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def md_table(headers: list[str], rows: list[list[Any]]) -> list[str]:
    out = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        out.append("| " + " | ".join(str(value if value is not None else "").replace("|", "/") for value in row) + " |")
    return out


def split_text_terms(value: Any) -> list[str]:
    text = clean(value)
    if not text:
        return []
    parts = re.split(r"[\s,;:/()\[\]+*]+|[\u3001\uff0c\uff1b\uff08\uff09\u3010\u3011\uff1a\u00b7\u00d7]+", text)
    terms: list[str] = []
    for part in parts:
        item = clean(part).strip("-_.\u3002\uff01\uff1f!?\u2018\u2019'\"")
        if not item:
            continue
        if item in STOP_TERMS or item.upper() in STOP_TERMS:
            continue
        if re.fullmatch(r"[A-Za-z0-9_-]+", item):
            if len(item) < 2:
                continue
        else:
            if len(item) < 2 or len(item) > 12:
                continue
        terms.append(item)
    return terms


def unique(values: list[Any], limit: int | None = None) -> list[str]:
    out: list[str] = []
    for value in values:
        item = clean(value)
        if item and item not in out:
            out.append(item)
    return out[:limit] if limit else out


def theme_candidates(normalized: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    for item in normalized.get("candidates") or []:
        constraints = item.get("discovery_constraints") or {}
        if item.get("object_class") != "theme_candidate":
            continue
        if item.get("used_as_discovery_hint") is not True:
            continue
        if constraints.get("auto_theme_to_stock_mapping") is not False:
            continue
        allowed = set(constraints.get("allowed_actions") or [])
        if "read_only_discovery_preview" not in allowed:
            continue
        out.append(item)
    return out


def expanded_keywords(item: dict[str, Any], max_terms: int) -> tuple[list[str], list[str]]:
    explicit_sources = (item.get("discovery_keywords") or []) + (item.get("supply_chain_nodes") or [])
    if explicit_sources:
        explicit_terms = unique([term for source in explicit_sources for term in split_text_terms(source)], limit=max_terms)
        return explicit_terms, []
    constraints = item.get("discovery_constraints") or {}
    raw_sources: list[Any] = []
    raw_sources += constraints.get("query_terms") or []
    raw_sources += [
        item.get("name"),
        item.get("thesis"),
        item.get("benefit_mechanism"),
        item.get("watch_focus"),
        " ".join(item.get("questions_for_zhulong") or []),
        " ".join(item.get("line_validation_questions") or []),
    ]
    base_terms = unique([term for source in raw_sources for term in split_text_terms(source)], limit=40)
    joined = " ".join(str(source or "") for source in raw_sources)
    bridge_terms: list[str] = []
    for triggers, expansions in DOMAIN_KEYWORD_EXPANSIONS:
        if any(trigger.lower() in joined.lower() for trigger in triggers):
            bridge_terms.extend(expansions)
    keywords = unique(bridge_terms + base_terms, limit=max_terms)
    return keywords, unique(bridge_terms)


def fetch_stock_rows(conn: Any, keywords: list[str], scan_limit: int, include_st: bool) -> list[dict[str, Any]]:
    if not keywords:
        return []
    clauses = []
    params: list[Any] = []
    for kw in keywords:
        like = f"%{kw}%"
        clauses.append("(b.name LIKE ? OR b.industry LIKE ?)")
        params.extend([like, like])
    st_clause = "" if include_st else "AND COALESCE(b.is_st, false) = false"
    rows = conn.execute(
        f"""
        SELECT
            b.symbol,
            b.name,
            COALESCE(b.industry, '') AS industry,
            COALESCE(b.market, '') AS market_board,
            CAST(b.list_date AS VARCHAR) AS list_date,
            COALESCE(b.is_st, false) AS is_st
        FROM fact_stock_basic b
        WHERE ({' OR '.join(clauses)})
          AND regexp_matches(b.symbol, '\\.(SH|SZ|BJ)$')
          {st_clause}
        ORDER BY b.symbol
        LIMIT ?
        """,
        params + [int(scan_limit)],
    ).fetchall()
    keys = [
        "symbol", "name", "industry", "market_board", "list_date", "is_st",
    ]
    return [{keys[i]: row[i] for i in range(len(keys))} for row in rows]


def score_row(row: dict[str, Any], keywords: list[str], bridge_terms: list[str]) -> dict[str, Any]:
    name = str(row.get("name") or "")
    industry = str(row.get("industry") or "")
    name_hits = [kw for kw in keywords if kw and kw in name]
    industry_hits = [kw for kw in keywords if kw and kw in industry]
    bridge_hits = [kw for kw in bridge_terms if kw and kw in industry]
    sources = []
    if industry_hits:
        sources.append("fact_stock_basic.industry")
    if name_hits:
        sources.append("fact_stock_basic.name")
    return {
        "name_hits": name_hits,
        "industry_hits": industry_hits,
        "bridge_hits": bridge_hits,
        "evidence_sources": sources,
        "evidence_level": "weak" if industry_hits or name_hits else "none",
    }


def validate_source_snapshot(payload: dict[str, Any], batch_id: str, normalized_sha256: str) -> list[str]:
    if payload.get("source") != "tushare":
        raise ValueError("source snapshot must have source=tushare")
    if payload.get("snapshot_type") != "discovery_source":
        raise ValueError("source snapshot must have snapshot_type=discovery_source")
    if payload.get("snapshot_version") != SOURCE_SNAPSHOT_VERSION:
        raise ValueError(f"unsupported source snapshot version: {payload.get('snapshot_version')}")
    if payload.get("mode") != "dry_run" or payload.get("dry_run") is not True or payload.get("no_trade_signal") is not True:
        raise ValueError("source snapshot must be dry_run with no_trade_signal=true")
    if clean(payload.get("batch_id")) != clean(batch_id):
        raise ValueError(f"source snapshot batch mismatch: {payload.get('batch_id')} != {batch_id}")

    warnings: list[str] = []
    source_sha = clean(payload.get("source_normalized_sha256"))
    if source_sha and source_sha != normalized_sha256:
        raise ValueError("source snapshot normalized SHA256 does not match current normalized input")
    if not source_sha:
        warnings.append("source_snapshot_missing_normalized_sha256")

    required_blocked = {
        "write_duckdb", "generate_validation_task", "write_shadow", "write_rag_memory",
        "write_nexus_audits", "trigger_daemon", "call_decision_engine", "call_nexus_run", "trade",
    }
    if not required_blocked.issubset(set(payload.get("blocked_actions") or [])):
        raise ValueError("source snapshot does not preserve required blocked actions")
    if payload.get("validation_tasks") not in (None, []):
        raise ValueError("source snapshot must not contain validation tasks")
    if int((payload.get("stats") or {}).get("generated_tasks") or 0) != 0:
        raise ValueError("source snapshot generated_tasks must be zero")

    for section in ("sw_index_matches", "sw_index_members", "ths_indices_matched", "ths_members"):
        for row in payload.get(section) or []:
            if row.get("generate_task") is not False or row.get("manual_review_required") is not True:
                raise ValueError(f"unsafe source snapshot row in {section}: task/review flags invalid")
            if row.get("business_relevance_evidence_present") is not False:
                raise ValueError(f"unsafe source snapshot row in {section}: business relevance must remain false")
    return warnings


def load_source_snapshot(path: Path, batch_id: str, normalized_sha256: str) -> tuple[dict[str, Any], str, list[str]]:
    raw = path.read_bytes()
    payload = json.loads(raw.decode("utf-8"))
    warnings = validate_source_snapshot(payload, batch_id=batch_id, normalized_sha256=normalized_sha256)
    return payload, hashlib.sha256(raw).hexdigest(), warnings


def snapshot_keywords_for_theme(theme: dict[str, Any], snapshot: dict[str, Any], theme_keywords: list[str]) -> tuple[list[str], str]:
    candidate_id = clean(theme.get("candidate_id"))
    for source in snapshot.get("theme_sources") or []:
        if clean(source.get("candidate_id")) == candidate_id:
            return unique(source.get("keywords") or []), "candidate_id"
    snapshot_keywords = unique(snapshot.get("keywords") or [])
    theme_terms = {clean(term).lower() for term in theme_keywords if clean(term)}
    matched = [kw for kw in snapshot_keywords if clean(kw).lower() in theme_terms]
    return matched, "exact_keyword_fallback" if matched else "unmatched"


def snapshot_evidence_for_theme(
    theme: dict[str, Any], snapshot: dict[str, Any], theme_keywords: list[str]
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    source_keywords, mapping_mode = snapshot_keywords_for_theme(theme, snapshot, theme_keywords)
    allowed_keywords = set(source_keywords)
    evidence: dict[str, dict[str, Any]] = {}

    def relevant_keywords(row: dict[str, Any]) -> list[str]:
        row_keywords = unique((row.get("source_keywords") or []) + [row.get("source_keyword")])
        return [keyword for keyword in row_keywords if keyword in allowed_keywords]

    def add(symbol: Any, name: Any, membership: dict[str, Any]) -> None:
        code = clean(symbol)
        if not code or not re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", code):
            return
        item = evidence.setdefault(code, {"symbol": code, "name": clean(name), "memberships": [], "keywords": []})
        identity = (clean(membership.get("source")), clean(membership.get("index_code")))
        existing = {
            (clean(current.get("source")), clean(current.get("index_code")))
            for current in item["memberships"]
        }
        if identity not in existing:
            item["memberships"].append(membership)
        for keyword in membership.get("matched_keywords") or []:
            if keyword and keyword not in item["keywords"]:
                item["keywords"].append(keyword)

    for row in snapshot.get("sw_index_members") or []:
        matched_keywords = relevant_keywords(row)
        if not matched_keywords:
            continue
        # index_member_all can contain historical membership. Only current rows
        # are eligible for a discovery preview.
        if clean(row.get("is_new")).upper() != "Y" or clean(row.get("out_date")):
            continue
        add(row.get("ts_code"), row.get("name"), {
            "source": "tushare.sw_index_member_all",
            "matched_keywords": matched_keywords,
            "index_code": row.get("source_index_code"),
            "index_name": row.get("source_index_name"),
            "index_level": row.get("source_index_level"),
            "index_member_count": row.get("source_index_member_count"),
        })

    for row in snapshot.get("ths_members") or []:
        matched_keywords = relevant_keywords(row)
        if not matched_keywords:
            continue
        add(row.get("con_code"), row.get("con_name"), {
            "source": "tushare.ths_member",
            "matched_keywords": matched_keywords,
            "index_code": row.get("source_index_code"),
            "index_name": row.get("source_index_name"),
            "index_type": row.get("source_index_type"),
            "index_member_count": row.get("source_index_member_count"),
        })

    matched_indices = []
    for section in ("sw_index_matches", "ths_indices_matched"):
        for row in snapshot.get(section) or []:
            matched_keywords = [
                keyword for keyword in unique((row.get("matched_keywords") or []) + [row.get("keyword")])
                if keyword in allowed_keywords
            ]
            if matched_keywords:
                matched_indices.append({
                    "source": section,
                    "matched_keywords": matched_keywords,
                    "index_code": row.get("index_code") or row.get("ts_code"),
                    "index_name": row.get("industry_name") or row.get("name"),
                    "member_count": row.get("member_count") or row.get("count"),
                    "broad_theme": bool(row.get("broad_theme")),
                    "expand_members": row.get("expand_members"),
                    "skip_member_reason": row.get("skip_member_reason"),
                })
    return evidence, {
        "mapping_mode": mapping_mode,
        "source_keywords": source_keywords,
        "matched_indices": matched_indices,
        "member_symbols": len(evidence),
    }


def fetch_stock_rows_by_symbols(conn: Any, symbols: list[str], include_st: bool) -> list[dict[str, Any]]:
    if not symbols:
        return []
    keys = [
        "symbol", "name", "industry", "market_board", "list_date", "is_st",
    ]
    out: list[dict[str, Any]] = []
    for start in range(0, len(symbols), 400):
        chunk = symbols[start:start + 400]
        placeholders = ",".join(["?"] * len(chunk))
        st_clause = "" if include_st else "AND COALESCE(b.is_st, false) = false"
        rows = conn.execute(
            f"""
            SELECT b.symbol, b.name, COALESCE(b.industry, ''), COALESCE(b.market, ''),
                   CAST(b.list_date AS VARCHAR), COALESCE(b.is_st, false)
            FROM fact_stock_basic b
            WHERE b.symbol IN ({placeholders})
              AND regexp_matches(b.symbol, '\\.(SH|SZ|BJ)$')
              {st_clause}
            ORDER BY b.symbol
            """,
            chunk,
        ).fetchall()
        out.extend({keys[i]: row[i] for i in range(len(keys))} for row in rows)
    return out


def fetch_anchor_rows(conn: Any, anchors: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    names = unique([anchor.get("name") for anchor in anchors])
    if not names:
        return {}
    placeholders = ",".join(["?"] * len(names))
    rows = conn.execute(
        f"""
        SELECT symbol, name, COALESCE(industry, ''), COALESCE(market, ''),
               CAST(list_date AS VARCHAR), COALESCE(is_st, false)
        FROM fact_stock_basic
        WHERE name IN ({placeholders})
          AND regexp_matches(symbol, '\\.(SH|SZ|BJ)$')
        ORDER BY symbol
        """,
        names,
    ).fetchall()
    keys = ["symbol", "name", "industry", "market_board", "list_date", "is_st"]
    return {clean(row[1]): {keys[i]: row[i] for i in range(len(keys))} for row in rows}


def evidence_items(match: dict[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for membership in match.get("source_memberships") or []:
        items.append({
            "type": "classification_membership",
            "source": membership.get("source"),
            "index_code": membership.get("index_code"),
            "index_name": membership.get("index_name"),
            "index_member_count": membership.get("index_member_count"),
            "matched_keywords": membership.get("matched_keywords") or [],
            "business_relevance_proven": False,
        })
    for field, hits in (("name", match.get("name_hits") or []), ("industry", match.get("industry_hits") or [])):
        for hit in hits:
            items.append({
                "type": "local_keyword_match",
                "source": f"fact_stock_basic.{field}",
                "matched_field": field,
                "matched_text": hit,
                "business_relevance_proven": False,
            })
    items.sort(key=lambda item: (
        clean(item.get("type")), clean(item.get("source")), clean(item.get("index_code")),
        clean(item.get("matched_field")), clean(item.get("matched_text")),
    ))
    for item in items:
        canonical = json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        item["evidence_item_id"] = f"EVID-{hashlib.sha256(canonical.encode('utf-8')).hexdigest()[:16].upper()}"
    return items


def classify_mapping(match: dict[str, Any], narrow_index_member_threshold: int) -> tuple[str, bool, list[str]]:
    memberships = match.get("source_memberships") or []
    distinct_indices = {
        (clean(item.get("source")), clean(item.get("index_code")))
        for item in memberships if clean(item.get("index_code"))
    }
    bridge_hits = set(match.get("bridge_hits") or [])
    local_node_match = bool(
        (match.get("name_hits") or [])
        + [value for value in match.get("industry_hits") or [] if value not in bridge_hits]
    )
    narrow_index_match = any(
        isinstance(item.get("index_member_count"), (int, float))
        and 0 < float(item.get("index_member_count")) <= narrow_index_member_threshold
        for item in memberships
    )
    if memberships and local_node_match:
        return "L2", True, []
    if len(distinct_indices) >= 2:
        return "L1", True, []
    if memberships and narrow_index_match:
        return "L1", True, []
    reasons = []
    if memberships:
        reasons.append("single_broad_board_membership_only")
    elif local_node_match:
        reasons.append("local_keyword_match_only")
    else:
        reasons.append("no_mapping_evidence")
    return "L0", False, reasons


def build_discovery_row(
    theme: dict[str, Any], row: dict[str, Any], match: dict[str, Any], index: int,
    mapping_pool: str, mapping_evidence_level: str, shortlist_eligible: bool,
    shortlist_block_reasons: list[str],
) -> dict[str, Any]:
    review_eligible = mapping_pool == "review_shortlist" and shortlist_eligible
    prefix = "DISC" if review_eligible else "BROAD"
    items = evidence_items(match)
    return {
        "preview_row_id": f"{theme.get('candidate_id')}-{prefix}-{index:03d}",
        "source_candidate_id": theme.get("candidate_id"),
        "source_theme_name": theme.get("name"),
        "source_object_class": theme.get("object_class"),
        "source_discovery_status": theme.get("discovery_status"),
        "source_discovery_constraints": theme.get("discovery_constraints"),
        "compass_line": theme.get("compass_line"),
        "compass_line_key": theme.get("compass_line_key"),
        "priority": theme.get("priority"),
        "supply_chain_nodes": theme.get("supply_chain_nodes") or [],
        "bottleneck_hypotheses": theme.get("bottleneck_hypotheses") or [],
        "discovery_keywords": theme.get("discovery_keywords") or [],
        "questions_for_zhulong": theme.get("questions_for_zhulong") or [],
        "line_validation_questions": theme.get("line_validation_questions") or [],
        "symbol": row.get("symbol"),
        "name": row.get("name"),
        "industry": row.get("industry"),
        "market_board": row.get("market_board"),
        "list_date": row.get("list_date"),
        "is_st": bool(row.get("is_st")),
        "mapping_pool": mapping_pool,
        "mapping_evidence_level": mapping_evidence_level,
        "evidence_level": "medium" if match.get("source_memberships") else "weak",
        "evidence_scope": "mapping_only_not_business_relevance",
        "evidence_items": items,
        "business_relevance_status": "unverified",
        "business_relevance_evidence_present": False,
        "weak_evidence_can_generate_task": False,
        "evidence_sources": match.get("evidence_sources") or [],
        "matched_name_terms": match.get("name_hits") or [],
        "matched_industry_terms": match.get("industry_hits") or [],
        "matched_bridge_terms": match.get("bridge_hits") or [],
        "matched_snapshot_terms": match.get("snapshot_keywords") or [],
        "source_memberships": match.get("source_memberships") or [],
        "shortlist_eligible": shortlist_eligible,
        "shortlist_block_reasons": shortlist_block_reasons,
        "review_eligible": review_eligible,
        "sample_only": False,
        "not_a_ranked_shortlist": mapping_pool != "review_shortlist",
        "review_status": "unreviewed" if review_eligible else "not_review_eligible",
        "manual_review_required": review_eligible,
        "ticker_status": "preview_only_not_task_resolved" if review_eligible else "mapping_only_not_reviewable",
        "generate_task": False,
        "task_block_reason": "theme_discovery_preview_requires_human_review" if review_eligible else "mapping_not_shortlisted",
        "allowed_actions": ["manual_review", "render_preview"] if review_eligible else ["render_preview"],
        "blocked_actions": BLOCKED_ACTIONS,
        "no_trade_signal": True,
    }


def stratified_diagnostic_sample(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    if limit <= 0:
        return []
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in sorted(rows, key=lambda item: (clean(item.get("industry")), clean(item.get("symbol")))):
        groups[clean(row.get("industry")) or "UNKNOWN"].append(row)
    sample: list[dict[str, Any]] = []
    industries = sorted(groups)
    while industries and len(sample) < limit:
        remaining = []
        for industry in industries:
            if groups[industry] and len(sample) < limit:
                source = groups[industry].pop(0)
                item = dict(source)
                item["preview_row_id"] = source["preview_row_id"].replace("-BROAD-", "-SAMPLE-")
                item["mapping_pool"] = "diagnostic_sample"
                item["sample_only"] = True
                item["not_a_ranked_shortlist"] = True
                item["review_eligible"] = False
                item["manual_review_required"] = False
                item["review_status"] = "diagnostic_only"
                item["ticker_status"] = "diagnostic_sample_not_reviewable"
                item["task_block_reason"] = "diagnostic_sample_never_reviewable"
                sample.append(item)
            if groups[industry]:
                remaining.append(industry)
        industries = remaining
    return sample


def discover(
    normalized: dict[str, Any], db_path: Path, max_keywords: int, diagnostic_sample_limit: int,
    max_shortlist_rows: int, narrow_index_member_threshold: int, scan_limit: int,
    include_st: bool, source_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    batch_id = normalized.get("batch_id") or "batch"
    themes = theme_candidates(normalized)
    anchors = [item for item in normalized.get("candidates") or [] if item.get("object_class") == "seed_anchor"]
    review_shortlist: list[dict[str, Any]] = []
    broad_universe: list[dict[str, Any]] = []
    diagnostic_sample: list[dict[str, Any]] = []
    anchor_reference: list[dict[str, Any]] = []
    theme_summaries: list[dict[str, Any]] = []
    warnings: list[str] = []
    with DBGateway(str(db_path), read_only=True) as conn:
        anchor_rows_by_name = fetch_anchor_rows(conn, anchors)
        for theme in themes:
            keywords, bridge_terms = expanded_keywords(theme, max_terms=max_keywords)
            if not keywords:
                warnings.append(f"no_discovery_keywords:{theme.get('candidate_id')}")
                theme_summaries.append({
                    "source_candidate_id": theme.get("candidate_id"),
                    "theme_name": theme.get("name"),
                    "keywords": [],
                    "bridge_terms": [],
                    "preview_rows": 0,
                    "review_shortlist_rows": 0,
                    "broad_universe_rows": 0,
                    "warning": "no_discovery_keywords",
                })
                continue
            raw_rows = fetch_stock_rows(conn, keywords, scan_limit=scan_limit, include_st=include_st)
            rows_by_symbol = {clean(row.get("symbol")): row for row in raw_rows}
            snapshot_evidence: dict[str, dict[str, Any]] = {}
            snapshot_summary: dict[str, Any] = {"mapping_mode": "disabled", "source_keywords": [], "matched_indices": [], "member_symbols": 0}
            if source_snapshot:
                snapshot_evidence, snapshot_summary = snapshot_evidence_for_theme(theme, source_snapshot, keywords)
                snapshot_rows_all = fetch_stock_rows_by_symbols(conn, sorted(snapshot_evidence), include_st=True)
                snapshot_rows = [
                    row for row in snapshot_rows_all if include_st or not bool(row.get("is_st"))
                ]
                for row in snapshot_rows:
                    rows_by_symbol.setdefault(clean(row.get("symbol")), row)
                all_stock_basic_symbols = {clean(row.get("symbol")) for row in snapshot_rows_all}
                eligible_stock_basic_symbols = {clean(row.get("symbol")) for row in snapshot_rows}
                missing_symbols = sorted(set(snapshot_evidence) - all_stock_basic_symbols)
                if missing_symbols:
                    warnings.append(f"snapshot_symbols_missing_stock_basic:{theme.get('candidate_id')}:{len(missing_symbols)}")
                excluded_st_symbols = sorted(all_stock_basic_symbols - eligible_stock_basic_symbols)
                if excluded_st_symbols:
                    warnings.append(f"snapshot_symbols_excluded_st:{theme.get('candidate_id')}:{len(excluded_st_symbols)}")
            theme_shortlist: list[dict[str, Any]] = []
            theme_broad: list[dict[str, Any]] = []
            for symbol, row in rows_by_symbol.items():
                match = score_row(row, keywords, bridge_terms)
                source_evidence = snapshot_evidence.get(symbol)
                if source_evidence:
                    match["evidence_level"] = "medium"
                    match["evidence_sources"] = unique((match.get("evidence_sources") or []) + [
                        membership.get("source") for membership in source_evidence.get("memberships") or []
                    ])
                    match["snapshot_keywords"] = source_evidence.get("keywords") or []
                    match["source_memberships"] = source_evidence.get("memberships") or []
                if match["evidence_level"] == "none":
                    continue
                level, eligible, block_reasons = classify_mapping(match, narrow_index_member_threshold)
                target = theme_shortlist if eligible else theme_broad
                target.append(build_discovery_row(
                    theme, row, match, len(target) + 1,
                    "review_shortlist" if eligible else "broad_universe",
                    level, eligible, block_reasons,
                ))
            theme_shortlist.sort(key=lambda item: (clean(item.get("symbol")), clean(item.get("name"))))
            theme_broad.sort(key=lambda item: (clean(item.get("symbol")), clean(item.get("name"))))
            shortlist_overflow = len(theme_shortlist) > max_shortlist_rows
            if shortlist_overflow:
                warnings.append(
                    f"shortlist_overflow:{theme.get('candidate_id')}:{len(theme_shortlist)}>{max_shortlist_rows}"
                )
                for row in theme_shortlist:
                    row["preview_row_id"] = row["preview_row_id"].replace("-DISC-", "-BROAD-")
                    row["mapping_pool"] = "broad_universe"
                    row["shortlist_eligible"] = False
                    row["shortlist_block_reasons"] = ["shortlist_overflow_requires_theme_refinement"]
                    row["review_eligible"] = False
                    row["sample_only"] = False
                    row["not_a_ranked_shortlist"] = True
                    row["manual_review_required"] = False
                    row["review_status"] = "not_review_eligible"
                    row["ticker_status"] = "mapping_only_not_reviewable"
                    row["task_block_reason"] = "shortlist_overflow"
                    row["allowed_actions"] = ["render_preview"]
                theme_broad.extend(theme_shortlist)
                theme_shortlist = []
            review_shortlist.extend(theme_shortlist)
            broad_universe.extend(theme_broad)
            theme_sample = stratified_diagnostic_sample(theme_broad, diagnostic_sample_limit)
            diagnostic_sample.extend(theme_sample)

            theme_line = clean(theme.get("compass_line_key"))
            theme_anchor_rows = []
            shortlist_symbols = {clean(row.get("symbol")) for row in theme_shortlist}
            for anchor in anchors:
                if clean(anchor.get("compass_line_key")) != theme_line:
                    continue
                resolved = anchor_rows_by_name.get(clean(anchor.get("name")))
                symbol = clean((resolved or {}).get("symbol"))
                source_recalled = bool(symbol and symbol in snapshot_evidence)
                shortlist_recalled = bool(symbol and symbol in shortlist_symbols)
                record = {
                    "source_theme_name": theme.get("name"),
                    "source_candidate_id": theme.get("candidate_id"),
                    "anchor_candidate_id": anchor.get("candidate_id"),
                    "compass_line_key": theme_line,
                    "name": anchor.get("name"),
                    "symbol": symbol or None,
                    "exact_resolved": bool(resolved),
                    "source_universe_recalled": source_recalled,
                    "review_shortlist_recalled": shortlist_recalled,
                    "is_anchor": True,
                    "mapping_pool": "anchor_reference",
                    "generate_task": False,
                    "no_trade_signal": True,
                }
                theme_anchor_rows.append(record)
                anchor_reference.append(record)
            if theme_anchor_rows and snapshot_evidence and not any(row["source_universe_recalled"] for row in theme_anchor_rows):
                warnings.append(f"anchor_coverage_warning:{theme.get('candidate_id')}:zero_source_recall")
            theme_summaries.append({
                "source_candidate_id": theme.get("candidate_id"),
                "theme_name": theme.get("name"),
                "compass_line_key": theme.get("compass_line_key"),
                "priority": theme.get("priority"),
                "keywords": keywords,
                "bridge_terms": bridge_terms,
                "raw_matches_scanned": len(raw_rows),
                "snapshot_mapping_mode": snapshot_summary.get("mapping_mode"),
                "snapshot_source_keywords": snapshot_summary.get("source_keywords") or [],
                "snapshot_matched_indices": snapshot_summary.get("matched_indices") or [],
                "snapshot_member_symbols": snapshot_summary.get("member_symbols", 0),
                "preview_rows": len(theme_shortlist),
                "review_shortlist_rows": len(theme_shortlist),
                "broad_universe_rows": len(theme_broad),
                "diagnostic_sample_rows": len(theme_sample),
                "shortlist_overflow": shortlist_overflow,
                "anchor_reference_rows": len(theme_anchor_rows),
                "anchor_source_recalled": sum(1 for row in theme_anchor_rows if row["source_universe_recalled"]),
                "anchor_shortlist_recalled": sum(1 for row in theme_anchor_rows if row["review_shortlist_recalled"]),
                "discovery_constraints": theme.get("discovery_constraints"),
                "generate_task": False,
                "manual_review_required": bool(theme_shortlist),
            })
    mapping_counts = Counter(row.get("mapping_evidence_level") or "unknown" for row in review_shortlist + broad_universe)
    line_counts = Counter(row.get("compass_line_key") or "unknown" for row in review_shortlist)
    return {
        "batch_id": batch_id,
        "source": normalized.get("source") or "Compass/ima",
        "source_report": normalized.get("source_report"),
        "source_as_of": normalized.get("source_as_of"),
        "source_normalized_report_sha256": normalized.get("report_sha256"),
        "generated_at": now_iso(),
        "mode": "dry_run",
        "tool": "tools/compass_theme_discovery.py",
        "protocol_version": PROTOCOL_VERSION,
        "discovery_constraints_protocol": DISCOVERY_CONSTRAINTS_VERSION,
        "no_trade_signal": True,
        "llm_used": False,
        "llm_policy": "disabled_in_mvp; deterministic read-only discovery preview only",
        "source_snapshot_used": bool(source_snapshot),
        "allowed_actions": ALLOWED_ACTIONS,
        "blocked_actions": BLOCKED_ACTIONS,
        "required_review_path": ["discovery_preview", "human_review", "reviewed_seed_or_candidate", "separate_dry_run_validation_task"],
        "forbidden_path": "theme_candidate -> validation_task",
        "readonly_modules": READONLY_MODULES,
        "parameters": {
            "max_keywords": max_keywords,
            "diagnostic_sample_limit": diagnostic_sample_limit,
            "max_shortlist_rows": max_shortlist_rows,
            "narrow_index_member_threshold": narrow_index_member_threshold,
            "scan_limit": scan_limit,
            "include_st": include_st,
        },
        "stats": {
            "theme_candidates_scanned": len(themes),
            "themes_with_preview_rows": sum(1 for item in theme_summaries if item.get("review_shortlist_rows", 0) > 0),
            "preview_rows": len(review_shortlist),
            "review_shortlist_rows": len(review_shortlist),
            "broad_universe_rows": len(broad_universe),
            "diagnostic_sample_rows": len(diagnostic_sample),
            "anchor_reference_rows": len(anchor_reference),
            "anchor_source_recalled": sum(1 for row in anchor_reference if row.get("source_universe_recalled")),
            "anchor_shortlist_recalled": sum(1 for row in anchor_reference if row.get("review_shortlist_recalled")),
            "shortlist_overflow_themes": sum(1 for item in theme_summaries if item.get("shortlist_overflow")),
            "manual_review_required_rows": len(review_shortlist),
            "generated_tasks": 0,
            "source_snapshot_medium_rows": sum(1 for row in review_shortlist + broad_universe if row.get("source_memberships")),
            "by_mapping_evidence_level": dict(sorted(mapping_counts.items())),
            "by_compass_line": dict(sorted(line_counts.items())),
        },
        "warnings": warnings,
        "theme_summaries": theme_summaries,
        "anchor_reference": anchor_reference,
        "review_shortlist": review_shortlist,
        "broad_universe": broad_universe,
        "diagnostic_sample": diagnostic_sample,
        "preview_rows": review_shortlist,
        "validation_tasks": [],
    }


def render_preview(payload: dict[str, Any]) -> str:
    lines = [
        f"# Compass Theme Discovery Preview - {payload['batch_id']}",
        "",
        "## 1. Batch",
        "",
        f"- source_report: `{payload.get('source_report')}`",
        f"- source_as_of: `{payload.get('source_as_of')}`",
        f"- mode: `{payload.get('mode')}`",
        f"- protocol_version: `{payload.get('protocol_version')}`",
        f"- llm_used: `{str(payload.get('llm_used')).lower()}`",
        f"- source_snapshot_used: `{str(payload.get('source_snapshot_used')).lower()}`",
        f"- source_snapshot: `{payload.get('source_snapshot')}`",
        f"- source_snapshot_version: `{payload.get('source_snapshot_version')}`",
        f"- source_snapshot_sha256: `{payload.get('source_snapshot_sha256')}`",
        "- no_trade_signal: `true`",
        "",
        "## 2. Discovery Stats",
        "",
    ]
    st = payload.get("stats") or {}
    rows = [[key, value] for key, value in st.items() if key not in {"by_mapping_evidence_level", "by_compass_line"}]
    rows += [[f"mapping:{k}", v] for k, v in (st.get("by_mapping_evidence_level") or {}).items()]
    rows += [[f"line:{k}", v] for k, v in (st.get("by_compass_line") or {}).items()]
    lines += md_table(["type", "count"], rows)

    theme_rows = []
    for item in payload.get("theme_summaries") or []:
        theme_rows.append([
            item.get("theme_name"),
            item.get("compass_line_key"),
            item.get("priority"),
            item.get("review_shortlist_rows"),
            item.get("broad_universe_rows"),
            item.get("diagnostic_sample_rows"),
            item.get("anchor_source_recalled"),
            item.get("anchor_reference_rows"),
            item.get("snapshot_member_symbols"),
            item.get("shortlist_overflow"),
        ])
    lines += ["", "## 3. Theme Search Constraints", ""] + md_table(
        ["theme", "Compass_line", "priority", "shortlist", "broad", "sample", "anchor_hit", "anchors", "source_members", "overflow"],
        theme_rows,
    )

    anchor_rows = [[
        row.get("source_theme_name"), row.get("name"), row.get("symbol"), row.get("exact_resolved"),
        row.get("source_universe_recalled"), row.get("review_shortlist_recalled"),
    ] for row in payload.get("anchor_reference") or []]
    lines += ["", "## 4. Anchor Reference", ""] + md_table(
        ["theme", "name", "symbol", "exact_resolved", "source_recalled", "shortlist_recalled"], anchor_rows
    )

    shortlist_rows = []
    for row in payload.get("review_shortlist") or []:
        shortlist_rows.append([
            row.get("source_theme_name"),
            row.get("symbol"),
            row.get("name"),
            row.get("industry"),
            row.get("mapping_evidence_level"),
            ", ".join(row.get("matched_industry_terms") or row.get("matched_name_terms") or []),
            ", ".join(row.get("matched_snapshot_terms") or []),
            row.get("review_status"),
            row.get("task_block_reason"),
        ])
    lines += ["", "## 5. Review Shortlist", ""] + md_table(
        ["theme", "symbol", "name", "industry", "mapping_level", "local_terms", "snapshot_terms", "review_status", "task_block_reason"],
        shortlist_rows,
    )

    broad_summary = [[
        item.get("theme_name"), item.get("compass_line_key"), item.get("broad_universe_rows"),
        item.get("review_shortlist_rows"), item.get("shortlist_overflow"),
    ] for item in payload.get("theme_summaries") or []]
    lines += ["", "## 6. Broad Universe Summary", ""] + md_table(
        ["theme", "Compass_line", "broad_rows", "shortlist_rows", "overflow"], broad_summary
    )

    sample_rows = [[
        row.get("source_theme_name"), row.get("symbol"), row.get("name"), row.get("industry"),
        row.get("mapping_evidence_level"), ", ".join(row.get("shortlist_block_reasons") or []),
        row.get("sample_only"), row.get("review_eligible"),
    ] for row in payload.get("diagnostic_sample") or []]
    lines += ["", "## 7. Diagnostic Sample (Not Ranked / Not Reviewable)", ""] + md_table(
        ["theme", "symbol", "name", "industry", "mapping_level", "block_reason", "sample_only", "review_eligible"],
        sample_rows,
    )

    warn_rows = [[w] for w in payload.get("warnings") or []]
    lines += ["", "## 8. Warnings", ""] + md_table(["warning"], warn_rows)
    lines += [
        "",
        "## 9. Safety",
        "",
        "This run is a read-only dry-run discovery preview. Only review_shortlist rows may initialize a review manifest. Broad-universe and diagnostic-sample rows are not ranked, are not reviewable, and never generate validation tasks. Price, RPS, turnover, order-flow, and other trading metrics are absent from this discovery contract.",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a dry-run, read-only discovery preview from Compass theme candidates.")
    parser.add_argument("--normalized", required=True, type=Path)
    parser.add_argument("--db-path", type=Path, default=DB_PATH)
    parser.add_argument("--source-snapshot", type=Path, help="Optional dry-run Tushare SW/THS discovery source snapshot.")
    parser.add_argument("--output-dir", type=Path, default=OUT_DIR)
    parser.add_argument("--json-output", type=Path)
    parser.add_argument("--preview-output", type=Path)
    parser.add_argument("--max-keywords", type=int, default=18)
    parser.add_argument("--diagnostic-sample-limit", type=int, default=DEFAULT_DIAGNOSTIC_SAMPLE_LIMIT)
    parser.add_argument("--max-shortlist-rows", type=int, default=DEFAULT_MAX_SHORTLIST_ROWS)
    parser.add_argument("--narrow-index-member-threshold", type=int, default=DEFAULT_NARROW_INDEX_MEMBER_THRESHOLD)
    parser.add_argument("--per-theme-limit", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--scan-limit", type=int, default=800)
    parser.add_argument("--include-st", action="store_true", help="Include ST rows in preview. Default excludes ST rows.")
    args = parser.parse_args()

    normalized_path = args.normalized.resolve()
    raw = normalized_path.read_bytes()
    normalized = json.loads(raw.decode("utf-8"))
    if normalized.get("mode") != "dry_run" or normalized.get("no_trade_signal") is not True:
        raise SystemExit("normalized input must be dry_run with no_trade_signal=true")

    normalized_sha256 = hashlib.sha256(raw).hexdigest()
    source_snapshot = None
    source_snapshot_sha256 = None
    source_snapshot_warnings: list[str] = []
    if args.source_snapshot:
        try:
            source_snapshot, source_snapshot_sha256, source_snapshot_warnings = load_source_snapshot(
                args.source_snapshot.resolve(), batch_id=normalized.get("batch_id") or "batch", normalized_sha256=normalized_sha256
            )
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            raise SystemExit(f"invalid source snapshot: {exc}") from exc

    payload = discover(
        normalized,
        args.db_path.resolve(),
        max_keywords=max(1, int(args.max_keywords)),
        diagnostic_sample_limit=max(0, int(args.per_theme_limit if args.per_theme_limit is not None else args.diagnostic_sample_limit)),
        max_shortlist_rows=max(1, int(args.max_shortlist_rows)),
        narrow_index_member_threshold=max(1, int(args.narrow_index_member_threshold)),
        scan_limit=max(10, int(args.scan_limit)),
        include_st=bool(args.include_st),
        source_snapshot=source_snapshot,
    )
    payload["source_normalized"] = str(normalized_path)
    payload["source_normalized_sha256"] = normalized_sha256
    payload["source_snapshot"] = str(args.source_snapshot.resolve()) if args.source_snapshot else None
    payload["source_snapshot_sha256"] = source_snapshot_sha256
    payload["source_snapshot_version"] = source_snapshot.get("snapshot_version") if source_snapshot else None
    payload["warnings"] = source_snapshot_warnings + payload.get("warnings", [])

    output_dir = args.output_dir.resolve()
    json_output = args.json_output or output_dir / f"compass_discovery_preview_{safe_batch(payload['batch_id'])}.json"
    preview_output = args.preview_output or output_dir / f"zhulong_compass_discovery_preview_{safe_batch(payload['batch_id'])}.md"
    json_output.parent.mkdir(parents=True, exist_ok=True)
    preview_output.parent.mkdir(parents=True, exist_ok=True)
    json_output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    preview_output.write_text(render_preview(payload), encoding="utf-8")
    print(json.dumps({
        "mode": "dry_run",
        "json_output": str(json_output),
        "preview_output": str(preview_output),
        "stats": payload["stats"],
        "warnings": payload["warnings"],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
