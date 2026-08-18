#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Normalize Compass/ima Markdown into dry-run Zhulong handoff files.

Only sections 4.1-4.4 and 5 are machine parsed. This script never touches
DuckDB, Shadow, RAG, Nexus, decision_engine, APScheduler, or the daemon.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "storage" / "reports" / "compass_ingest"
BEIJING_TZ = timezone(timedelta(hours=8))
SOURCE = "Compass/ima"
ALLOWED = ["normalize", "render_preview"]
BLOCKED = [
    "trade", "write_shadow", "write_rag_memory", "write_nexus_audits",
    "write_duckdb", "trigger_daemon", "call_decision_engine", "call_nexus_run",
]

DISCOVERY_PROTOCOL_VERSION = "compass_discovery_constraints_v0.1"
DISCOVERY_ALLOWED_ACTIONS = [
    "preserve_source",
    "render_preview",
    "manual_review",
    "exact_ticker_resolve",
    "read_only_discovery_preview",
]
DISCOVERY_BLOCKED_ACTIONS = BLOCKED + [
    "auto_theme_to_stock_mapping",
    "auto_fuzzy_ticker_resolve",
    "generate_stock_validation_from_theme",
    "write_validation_tasks_from_theme",
    "unbounded_tushare_discovery",
]
DISCOVERY_READONLY_MODULES = [
    "fact_stock_basic",
    "fact_daily",
    "fact_rps_results",
    "fact_zeta_signals",
    "fact_quantile_snapshot",
    "financial_snapshots_readonly",
]

SECTIONS = {
    "stock_validation_candidates": ("4.1", "stock_validation_candidates"),
    "theme_mapping_candidates": ("4.2", "theme_mapping_candidates"),
    "watch_only_candidates": ("4.3", "watch_only_candidates"),
    "excluded_candidates": ("4.4", "excluded_candidates"),
    "validation_questions": ("5.", "烛龙验证问题总表"),
}
LABELS = {
    "stock_validation_candidates": "4.1 stock_validation_candidates",
    "theme_mapping_candidates": "4.2 theme_mapping_candidates",
    "watch_only_candidates": "4.3 watch_only_candidates",
    "excluded_candidates": "4.4 excluded_candidates",
    "validation_questions": "5 validation_questions",
}
FIELDS = {
    "raw_name": ("raw_name", "\u5019\u9009\u540d\u79f0", "\u5bf9\u8c61 / \u65b9\u5411", "\u5bf9\u8c61", "\u540d\u79f0", "\u516c\u53f8\u7b80\u79f0", "candidate_name"),
    "market": ("市场", "market"),
    "candidate_type": ("候选类型", "类型", "candidate_type"),
    "compass_line": ("Compass线条", "所属线条", "线条", "compass_line"),
    "pool_type": ("池类型", "所属池", "池", "pool_type"),
    "priority": ("优先级", "priority"),
    "thesis": ("进入烛龙验证的理由", "为什么是公认现金牛", "潜在信息差在哪里", "处理建议", "理由", "thesis"),
    "benefit_mechanism": ("受益机制", "benefit_mechanism"),
    "questions_for_zhulong": ("questions_for_zhulong", "mapping_questions", "questions", "\u70db\u9f99\u9700\u8981\u91cd\u70b9\u9a8c\u8bc1\u7684\u6570\u636e", "\u9700\u8981\u70db\u9f99\u9a8c\u8bc1\u4ec0\u4e48", "\u9700\u8981\u9a8c\u8bc1\u54ea\u4e9b\u6570\u636e", "\u91cd\u70b9\u9a8c\u8bc1\u6570\u636e", "\u9a8c\u8bc1\u6570\u636e"),
    "risks": ("反证风险", "可能的反证", "风险", "risks"),
    "excluded_reason": ("暂不验证原因", "排除原因", "excluded_reason"),
    "watch_focus": ("watch_focus", "观察重点", "需要烛龙验证什么"),
    "supply_chain_nodes": ("supply_chain_nodes", "产业链节点", "关键产业链节点"),
    "bottleneck_hypotheses": ("bottleneck_hypotheses", "瓶颈假设", "待验证瓶颈假设"),
    "discovery_keywords": ("discovery_keywords", "discovery keywords", "发现关键词"),
}


def now_iso() -> str:
    return datetime.now(BEIJING_TZ).isoformat(timespec="seconds")


def nk(value: Any) -> str:
    return re.sub(r"[\s`*_:/\\|\-·.]+", "", str(value or "").strip().lower())


def clean(value: Any) -> str:
    text = str(value or "").strip()
    text = text.replace("<br>", "; ").replace("<br/>", "; ").replace("<br />", "; ")
    text = text.replace("\\_", "_")
    return re.sub(r"\s+", " ", text).strip()


def heading_spans(text: str) -> list[dict[str, Any]]:
    matches = list(re.finditer(r"^(#{1,6})\s*(.*?)\s*$", text, flags=re.MULTILINE))
    out = []
    for index, match in enumerate(matches):
        level = len(match.group(1))
        end = len(text)
        for nxt in matches[index + 1:]:
            if len(nxt.group(1)) <= level:
                end = nxt.start()
                break
        out.append({
            "title": match.group(2).strip(),
            "start": match.start(),
            "content_start": match.end(),
            "end": end,
            "line": text.count("\n", 0, match.start()) + 1,
        })
    return out


def find_section(text: str, key: str) -> tuple[str, int] | None:
    aliases = SECTIONS[key]
    found = None
    for item in heading_spans(text):
        title_key = nk(item["title"])
        title_raw = item["title"].lower()
        if any(nk(alias) in title_key or alias.lower() in title_raw for alias in aliases):
            found = (text[item["content_start"]: item["end"]], int(item["line"]))
    return found


def split_row(line: str) -> list[str]:
    raw = line.strip()
    if "|" not in raw:
        return []
    if raw.startswith("|"):
        raw = raw[1:]
    if raw.endswith("|"):
        raw = raw[:-1]
    return [clean(cell) for cell in raw.split("|")]


def is_delim(cells: list[str]) -> bool:
    if not cells:
        return False
    saw_dash = False
    for cell in cells:
        marker = cell.strip().replace(":", "")
        if marker and set(marker) != {"-"}:
            return False
        saw_dash = saw_dash or "-" in marker
    return saw_dash


def tables(section_text: str, base_line: int) -> list[dict[str, Any]]:
    lines = section_text.splitlines()
    out = []
    i = 0
    while i < len(lines) - 1:
        header = split_row(lines[i])
        delim = split_row(lines[i + 1])
        if not header or not is_delim(delim):
            i += 1
            continue
        rows = []
        cursor = i + 2
        row_index = 1
        while cursor < len(lines) and lines[cursor].strip().startswith("|"):
            cells = split_row(lines[cursor])
            if any(cells):
                cells = cells + [""] * max(0, len(header) - len(cells))
                rows.append({
                    "fields": {header[pos]: cells[pos] for pos in range(len(header))},
                    "source_line": base_line + cursor,
                    "source_row_index": row_index,
                })
                row_index += 1
            cursor += 1
        out.append({"headers": header, "rows": rows})
        i = cursor
    return out


def get_field(fields: dict[str, str], field: str) -> str:
    normalized = {nk(key): value for key, value in fields.items()}
    for alias in FIELDS[field]:
        value = normalized.get(nk(alias))
        if value:
            return clean(value)
    for alias in FIELDS[field]:
        alias_key = nk(alias)
        for key, value in normalized.items():
            if alias_key and alias_key in key and value:
                return clean(value)
    return ""


def first_value(fields: dict[str, str]) -> str:
    for value in fields.values():
        if clean(value):
            return clean(value)
    return ""


def market(raw: str) -> str:
    text = str(raw or "").replace(" ", "")
    if not text:
        return ""
    has_a = "A股" in text or "a股" in text.lower() or "A-share" in text
    has_hk = "港股" in text or "H股" in text or "HK" in text.upper()
    has_us = "美股" in text or "美国" in text or "NASDAQ" in text.upper() or "NYSE" in text.upper()
    has_pre = "待上市" in text or "未上市" in text or "IPO" in text.upper()
    if has_a and not (has_hk or has_us or has_pre):
        return "A股"
    if has_a and (has_hk or has_us or has_pre):
        return "多市场"
    if has_hk:
        return "港股"
    if has_us:
        return "美股"
    if has_pre:
        return "待上市"
    if any(token in text for token in ("行业", "环节", "机构", "方向", "生态")):
        return "不适用"
    return raw.strip()


def candidate_type(raw_type: str, name: str, market_value: str) -> str:
    text = f"{raw_type or ''} {name or ''}".replace(" ", "")
    normalized_type = str(raw_type or "").strip().lower()
    if normalized_type in {"theme_candidate", "industry_segment", "policy_signal", "macro_signal", "market_rule"}:
        return "theme_candidate"
    if normalized_type in {"company_candidate", "company_signal"}:
        return "company_candidate"
    if "公司" in text or market_value == "A股":
        return "company_candidate"
    if any(token in text for token in ("行业", "方向", "机构", "生态", "材料", "环节", "业务", "工具")):
        return "theme_candidate"
    if "排除" in text:
        return "excluded_candidate"
    return "unknown_candidate"


def priority(raw: str) -> str:
    text = str(raw or "").strip().lower()
    if not text:
        return "unknown"
    if text in {"高", "high", "h"} or "高" in text:
        return "high"
    if text in {"中", "medium", "mid", "m"} or "中" in text:
        return "medium"
    if text in {"低", "low", "l"} or "低" in text:
        return "low"
    return text

def split_items(value: str) -> list[str]:
    text = clean(value)
    if not text:
        return []
    return [part.strip(" -•\t") for part in re.split(r"[；;]\s*|\n+", text) if part.strip(" -•\t")]


def line_key(raw: str) -> str:
    match = re.search(r"([A-Za-z])\s*线", str(raw or ""))
    if match:
        return f"{match.group(1).upper()}线"
    return "OTHER"


def unique_nonempty(values: list[Any], limit: int | None = None) -> list[str]:
    out = []
    for value in values:
        item = clean(value)
        if item and item not in out:
            out.append(item)
    return out[:limit] if limit else out


def discovery_protocol() -> dict[str, Any]:
    return {
        "version": DISCOVERY_PROTOCOL_VERSION,
        "status": "dry_run_contract",
        "purpose": "Convert Compass/ima direction-level inputs into auditable constraints before any stock discovery.",
        "object_classes": {
            "seed_anchor": "Explicit historical stock seed; record_only by default; not an ima-generated buy list.",
            "theme_candidate": "Direction-level hint; may constrain a future read-only discovery preview; never maps to stocks automatically.",
            "watch_only": "Observation-only object; no validation task generation.",
            "excluded": "Recorded exclusion; no discovery or validation task generation.",
        },
        "hard_gates_before_stock_validation_task": [
            "market_is_a_share",
            "ticker_exact_or_manual_resolved",
            "business_relevance_evidence_present",
            "human_review_for_theme_mapping",
            "dry_run_preview_reviewed",
            "no_trade_signal_true",
            "not_watch_only_or_excluded",
        ],
        "business_relevance_evidence_levels": {
            "strong": "main business disclosure, annual report, exchange announcement, official filing, or explicit company statement tied to the theme",
            "medium": "concept board membership, industry component table, broker/industry report, or curated theme membership table; requires human review and supporting metric",
            "weak": "company name keyword, industry keyword, fuzzy/LIKE match, or broad semantic similarity; cannot independently unlock validation task",
        },
        "review_gate": {
            "field": "dry_run_preview_reviewed",
            "status": "implemented_as_separate_offline_review_tool",
            "accepted_input": "review_manifest.json bound to preview and row SHA256",
            "tool": "tools/compass_discovery_review.py",
            "output_policy": "reviewed candidates only; validation task generation remains separate",
        },
        "theme_candidate_pipeline": ["theme_candidate", "discovery_preview", "human_review", "reviewed_seed_or_candidate", "dry_run_validation_task"],
        "forbidden_pipelines": ["theme_candidate -> validation_task"],
        "seed_validation_flag_scope": "--enable-seed-validation is not a force flag; it only opens the final gate for otherwise eligible seed_anchor rows.",
        "allowed_actions": DISCOVERY_ALLOWED_ACTIONS,
        "blocked_actions": DISCOVERY_BLOCKED_ACTIONS,
        "readonly_modules": DISCOVERY_READONLY_MODULES,
        "fail_closed_policy": "If any gate is missing, keep the object as record_only or hint_only and generate no validation task.",
        "no_trade_signal": True,
    }


def make_discovery_constraints(item: dict[str, Any], role: str, status: str) -> dict[str, Any]:
    query_terms = unique_nonempty([
        *(item.get("discovery_keywords") or []),
        *(item.get("supply_chain_nodes") or []),
        *(item.get("bottleneck_hypotheses") or []),
        item.get("name"),
        item.get("thesis"),
        item.get("benefit_mechanism"),
        item.get("watch_focus"),
    ], limit=8)
    constraints = {
        "protocol_version": DISCOVERY_PROTOCOL_VERSION,
        "role": role,
        "status": status,
        "source_candidate_id": item.get("candidate_id"),
        "source_section": item.get("source_section"),
        "source_row_index": item.get("source_row_index"),
        "market_scope": ["A\u80a1"] if role in {"seed_anchor_record_only", "theme_discovery_hint"} else [],
        "query_terms": query_terms,
        "allowed_actions": ["preserve_source", "render_preview", "manual_review"],
        "blocked_actions": DISCOVERY_BLOCKED_ACTIONS,
        "readonly_modules": DISCOVERY_READONLY_MODULES,
        "auto_generate_stock_validation_task": False,
        "auto_theme_to_stock_mapping": False,
        "auto_fuzzy_ticker_resolve": False,
        "manual_review_required_before_task": True,
        "business_relevance_evidence_required": True,
        "weak_evidence_can_generate_task": False,
        "review_manifest_required_for_theme_task": True,
        "fail_closed_on": [
            "missing_business_relevance_evidence",
            "missing_exact_or_manual_ticker",
            "unsupported_market",
            "ambiguous_mapping",
            "unreviewed_theme_mapping",
        ],
        "no_trade_signal": True,
    }
    if role == "seed_anchor_record_only":
        constraints["allowed_actions"].append("exact_ticker_resolve")
        constraints["required_before_stock_validation_task"] = [
            "market_is_a_share",
            "ticker_status_resolved",
            "manual_review_required_false",
            "no_trade_signal_true",
            "not_theme_watch_or_excluded",
            "seed_validation_enabled_by_flag_true",
        ]
    elif role == "theme_discovery_hint":
        constraints["allowed_actions"].append("read_only_discovery_preview")
        constraints["required_before_stock_validation_task"] = [
            "discovery_preview_row_reviewed",
            "human_approved_theme_to_stock_mapping",
            "business_relevance_evidence_present_medium_or_strong",
            "market_is_a_share",
            "ticker_exact_or_manual_resolved",
            "readonly_metrics_available",
        ]
    else:
        constraints["required_before_stock_validation_task"] = ["not_applicable"]
    return constraints


def make_id(batch_id: str, raw_line: str, seq: int) -> str:
    safe_batch = re.sub(r"[^0-9A-Za-zW_-]", "", batch_id)
    line = line_key(raw_line).replace("线", "") or "X"
    return f"COMPASS-{safe_batch}-{line}-{seq:03d}"


def parse_validation_question_records(section_text: str) -> list[dict[str, Any]]:
    records = []
    for table in tables(section_text, 0):
        for row in table["rows"]:
            fields = {nk(key): clean(value) for key, value in row["fields"].items()}
            raw_line = fields.get("compassline") or fields.get("线条") or ""
            question = fields.get("verificationquestion") or fields.get("questions") or fields.get("问题") or fields.get("questionsforzhulong") or ""
            if not raw_line or not question:
                continue
            records.append({
                "compass_line": raw_line,
                "compass_line_key": line_key(raw_line),
                "theme_name": fields.get("themename") or "",
                "bottleneck_node": fields.get("bottlenecknode") or "",
                "verification_question": question,
                "required_evidence": split_items(fields.get("requiredevidence") or ""),
                "counter_evidence": split_items(fields.get("counterevidence") or ""),
                "source_row_index": row["source_row_index"],
            })
    return records


def parse_questions(section_text: str, records: list[dict[str, Any]] | None = None) -> dict[str, list[str]]:
    questions: dict[str, list[str]] = defaultdict(list)
    for record in records if records is not None else parse_validation_question_records(section_text):
        questions[record["compass_line_key"]].append(record["verification_question"])
    current = "general"
    for raw in section_text.splitlines():
        line = raw.strip()
        if not line or line.startswith("|") or re.fullmatch(r"-{3,}", line):
            continue
        heading = re.sub(r"^#{1,6}\s*", "", line).strip()
        if "\u901a\u7528" in heading and "\u95ee\u9898" in heading:
            current = "general"
            questions.setdefault(current, [])
            continue
        match = re.search("([A-Za-z])\\s*\u7ebf", heading)
        if match and ("\u95ee\u9898" in heading or heading.endswith(("\uff1a", ":"))):
            current = f"{match.group(1).upper()}\u7ebf"
            questions.setdefault(current, [])
            continue
        bullet = re.sub(r"^[-*\u2022]\s*", "", line)
        bullet = re.sub(r"^\d+[.)\u3001]\s*", "", bullet).strip()
        if bullet != line or line.startswith(("-", "*", "\u2022")):
            questions[current].append(bullet)
    return {key: value for key, value in questions.items() if value}


def build_candidate(batch_id: str, section_key: str, row: dict[str, Any], seq: int,
                    questions: dict[str, list[str]], question_records: list[dict[str, Any]]) -> dict[str, Any]:
    fields = {key: clean(value) for key, value in row["fields"].items()}
    raw_name = get_field(fields, "raw_name") or first_value(fields)
    raw_market = get_field(fields, "market")
    mkt = market(raw_market)
    compass_line = get_field(fields, "compass_line")
    lk = line_key(compass_line)
    candidate_question_records = [
        record for record in question_records
        if record["compass_line_key"] == lk
        and (not record.get("theme_name") or clean(record["theme_name"]) == clean(raw_name))
    ]
    ctype = candidate_type(get_field(fields, "candidate_type"), raw_name, mkt)
    warnings = []
    if not raw_name:
        warnings.append("missing_raw_name")
    if section_key == "stock_validation_candidates" and not raw_market:
        warnings.append("missing_market")
    item: dict[str, Any] = {
        "candidate_id": make_id(batch_id, compass_line, seq),
        "raw_name": raw_name,
        "name": raw_name,
        "market": mkt,
        "raw_market": raw_market,
        "candidate_type": ctype,
        "object_class": "unknown",
        "origin_role": "unknown",
        "generated_by_ima": False,
        "used_as_discovery_hint": False,
        "default_generate_validation_task": False,
        "do_not_treat_as_recommendation": True,
        "seed_validation_enabled_by_flag": False,
        "discovery_status": "not_applicable",
        "discovery_constraints": None,
        "validation_mode": "record_only",
        "compass_line": compass_line,
        "compass_line_key": lk,
        "pool_type": get_field(fields, "pool_type"),
        "priority": priority(get_field(fields, "priority")),
        "ticker": None,
        "ticker_status": "unresolved",
        "ticker_source": None,
        "manual_review_required": False,
        "thesis": get_field(fields, "thesis"),
        "benefit_mechanism": get_field(fields, "benefit_mechanism"),
        "supply_chain_nodes": split_items(get_field(fields, "supply_chain_nodes")),
        "bottleneck_hypotheses": split_items(get_field(fields, "bottleneck_hypotheses")),
        "discovery_keywords": split_items(get_field(fields, "discovery_keywords")),
        "questions_for_zhulong": split_items(get_field(fields, "questions_for_zhulong")),
        "line_validation_questions": questions.get("general", []) + questions.get(lk, []),
        "validation_question_records": candidate_question_records,
        "risks": split_items(get_field(fields, "risks")),
        "watch_focus": get_field(fields, "watch_focus"),
        "excluded_reason": get_field(fields, "excluded_reason"),
        "source_section": LABELS[section_key],
        "source_row_index": row["source_row_index"],
        "source_line": row["source_line"],
        "raw_fields": fields,
        "warnings": warnings,
        "downgraded_from": None,
        "downgrade_reason": None,
        "unsupported_reason": None,
        "alias_suggestions": [],
        "no_trade_signal": True,
    }
    if section_key == "stock_validation_candidates":
        item.update({
            "object_class": "seed_anchor",
            "origin_role": "seed_anchor",
            "validation_mode": "seed_anchor",
            "generated_by_ima": False,
            "used_as_discovery_hint": True,
            "default_generate_validation_task": False,
            "ticker_status": "unresolved",
            "discovery_status": "record_only_seed_anchor",
        })
        item["discovery_constraints"] = make_discovery_constraints(item, "seed_anchor_record_only", "record_only")
        if mkt != "A股":
            item.update({
                "object_class": "watch_only",
                "origin_role": "watch_only",
                "downgraded_from": "seed_anchor",
                "validation_mode": "watch_only",
                "ticker_status": "unsupported_market",
                "downgrade_reason": "market_not_a_share",
                "unsupported_reason": f"market={mkt or raw_market or 'unknown'} is not A股",
                "used_as_discovery_hint": False,
                "discovery_status": "watch_only_no_task",
            })
            item["discovery_constraints"] = make_discovery_constraints(item, "watch_only", "observation_only")
            item["warnings"].append("ima_classification_not_strict_auto_downgraded")
        elif ctype != "company_candidate":
            item.update({
                "object_class": "theme_candidate",
                "origin_role": "theme_candidate",
                "downgraded_from": "seed_anchor",
                "validation_mode": "theme_mapping",
                "ticker_status": "not_applicable",
                "manual_review_required": True,
                "downgrade_reason": "non_company_candidate",
                "discovery_status": "hint_only_theme",
            })
            item["discovery_constraints"] = make_discovery_constraints(item, "theme_discovery_hint", "hint_only")
            item["warnings"].append("stock_seed_requires_company_candidate")
    elif section_key == "theme_mapping_candidates":
        item.update({
            "object_class": "theme_candidate",
            "origin_role": "theme_candidate",
            "validation_mode": "theme_mapping",
            "ticker_status": "not_applicable",
            "generated_by_ima": True,
            "used_as_discovery_hint": True,
            "discovery_status": "hint_only_theme",
        })
        item["discovery_constraints"] = make_discovery_constraints(item, "theme_discovery_hint", "hint_only")
    elif section_key == "watch_only_candidates":
        item.update({
            "object_class": "watch_only",
            "origin_role": "watch_only",
            "validation_mode": "watch_only",
            "ticker_status": "unsupported_market" if mkt not in {"", "A股", "不适用"} else "not_applicable",
            "generated_by_ima": True,
            "discovery_status": "watch_only_no_task",
        })
        item["discovery_constraints"] = make_discovery_constraints(item, "watch_only", "observation_only")
    elif section_key == "excluded_candidates":
        item.update({
            "object_class": "excluded",
            "origin_role": "excluded",
            "validation_mode": "excluded",
            "ticker_status": "not_applicable",
            "generated_by_ima": True,
            "discovery_status": "excluded_no_task",
        })
        item["discovery_constraints"] = make_discovery_constraints(item, "excluded", "excluded")
    return item


def collect(text: str, batch_id: str, questions: dict[str, list[str]],
            question_records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
    candidates = []
    warnings = []
    seq = 1
    for key in ("stock_validation_candidates", "theme_mapping_candidates", "watch_only_candidates", "excluded_candidates"):
        section = find_section(text, key)
        if section is None:
            warnings.append(f"missing_section:{LABELS[key]}")
            continue
        section_text, base_line = section
        found_tables = tables(section_text, base_line)
        if not found_tables:
            warnings.append(f"no_markdown_table:{LABELS[key]}")
            continue
        for table in found_tables:
            for row in table["rows"]:
                candidates.append(build_candidate(batch_id, key, row, seq, questions, question_records))
                seq += 1
    return candidates, warnings


def stats(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    by_mode = Counter(str(item.get("validation_mode") or "unknown") for item in candidates)
    by_status = Counter(str(item.get("ticker_status") or "unknown") for item in candidates)
    by_class = Counter(str(item.get("object_class") or "unknown") for item in candidates)
    by_discovery_status = Counter(str(item.get("discovery_status") or "unknown") for item in candidates)
    seed_ready = sum(
        1 for item in candidates
        if item.get("object_class") == "seed_anchor"
        and item.get("validation_mode") == "seed_anchor"
        and item.get("market") == "A股"
        and item.get("ticker_status") == "unresolved"
    )
    return {
        "total_candidates": len(candidates),
        "by_validation_mode": dict(sorted(by_mode.items())),
        "by_ticker_status": dict(sorted(by_status.items())),
        "by_object_class": dict(sorted(by_class.items())),
        "by_discovery_status": dict(sorted(by_discovery_status.items())),
        "downgraded_candidates": sum(1 for item in candidates if item.get("downgraded_from")),
        "manual_review_required": sum(1 for item in candidates if item.get("manual_review_required")),
        "seed_anchors": sum(1 for item in candidates if item.get("object_class") == "seed_anchor"),
        "explicit_stock_seeds": sum(1 for item in candidates if item.get("origin_role") == "seed_anchor"),
        "discovery_hints": sum(1 for item in candidates if item.get("used_as_discovery_hint")),
        "theme_discovery_hints": sum(1 for item in candidates if item.get("object_class") == "theme_candidate" and item.get("used_as_discovery_hint")),
        "seed_anchors_ready_for_exact_resolve": seed_ready,
        "stock_candidates_ready_for_exact_resolve": seed_ready,
    }

def md_table(headers: list[str], rows: list[list[Any]]) -> list[str]:
    out = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        out.append("| " + " | ".join(str(value if value is not None else "").replace("|", "/") for value in row) + " |")
    return out


def render_preview(payload: dict[str, Any]) -> str:
    cands = payload["candidates"]
    lines = [
        f"# Compass Report Normalizer Preview - {payload['batch_id']}", "",
        "## 1. Batch", "",
        f"- source_report: `{payload['source_report']}`",
        f"- source_as_of: `{payload['source_as_of']}`",
        f"- report_sha256: `{payload['report_sha256']}`",
        f"- mode: `{payload['mode']}`",
        f"- discovery_protocol: `{payload['discovery_protocol']['version']}`",
        "- no_trade_signal: `true`", "",
        "## 2. Parse Stats", "",
    ]
    st = payload["stats"]
    rows = [["total_candidates", st["total_candidates"]]]
    rows += [[f"class:{k}", v] for k, v in st["by_object_class"].items()]
    rows += [[f"discovery:{k}", v] for k, v in st["by_discovery_status"].items()]
    rows += [[f"mode:{k}", v] for k, v in st["by_validation_mode"].items()]
    rows += [[f"ticker:{k}", v] for k, v in st["by_ticker_status"].items()]
    rows += [
        ["downgraded_candidates", st["downgraded_candidates"]],
        ["manual_review_required", st["manual_review_required"]],
        ["seed_anchors", st["seed_anchors"]],
        ["discovery_hints", st["discovery_hints"]],
        ["theme_discovery_hints", st["theme_discovery_hints"]],
        ["seed_anchors_ready_for_exact_resolve", st["seed_anchors_ready_for_exact_resolve"]],
    ]
    lines += md_table(["type", "count"], rows)
    seed_rows = [[i["name"], i["ticker_status"], i["market"], i.get("object_class"), i.get("validation_mode"), i.get("discovery_status"), i["compass_line_key"], i["priority"], i["manual_review_required"]] for i in cands if i.get("object_class") == "seed_anchor"]
    lines += ["", "## 3. record_only seed_anchor objects", ""] + md_table(["name", "ticker_status", "market", "object_class", "validation_mode", "discovery_status", "Compass_line", "priority", "manual_review_required"], seed_rows)
    down_rows = [[i["name"], i["source_section"], i.get("raw_market") or i.get("market"), i["validation_mode"], i.get("downgrade_reason") or i.get("unsupported_reason")] for i in cands if i.get("downgraded_from")]
    lines += ["", "## 4. Downgraded Objects", ""] + md_table(["name", "source_section", "raw_market", "validation_mode", "reason"], down_rows)
    theme_rows = [[i["name"], i["compass_line_key"], i.get("discovery_status"), "manual mapping required"] for i in cands if i.get("validation_mode") == "theme_mapping"]
    lines += ["", "## 5. theme_mapping Objects", ""] + md_table(["name", "Compass_line", "discovery_status", "handling"], theme_rows)
    watch_rows = [[i["name"], i.get("unsupported_reason") or i.get("downgrade_reason") or i.get("thesis"), i.get("watch_focus")] for i in cands if i.get("validation_mode") == "watch_only"]
    lines += ["", "## 6. watch_only Objects", ""] + md_table(["name", "reason", "watch_focus"], watch_rows)
    discovery_rows = [[i["name"], i.get("object_class"), i.get("discovery_status"), ", ".join((i.get("discovery_constraints") or {}).get("market_scope") or []), (i.get("discovery_constraints") or {}).get("auto_theme_to_stock_mapping"), (i.get("discovery_constraints") or {}).get("manual_review_required_before_task")] for i in cands if i.get("used_as_discovery_hint")]
    lines += ["", "## 7. Discovery Constraints", ""] + md_table(["name", "object_class", "discovery_status", "market_scope", "auto_theme_to_stock_mapping", "manual_review_before_task"], discovery_rows)
    excluded_rows = [[i["name"], i.get("excluded_reason") or i.get("thesis")] for i in cands if i.get("validation_mode") == "excluded"]
    lines += ["", "## 8. excluded_candidates", ""] + md_table(["name", "excluded_reason"], excluded_rows)
    lines += ["", "## 9. Parse Warnings", ""] + md_table(["warning"], [[w] for w in payload.get("warnings", [])])
    lines += ["", "## 10. Safety", "", "This run writes dry-run files only. It does not write DuckDB, Shadow, RAG, or nexus_audits; it does not trigger daemon, decision_engine, or Nexus.run; it is not a trade signal.", ""]
    return "\n".join(lines)

def safe_batch(batch_id: str) -> str:
    return re.sub(r"[^0-9A-Za-zW_-]", "", batch_id)


def source_document_warnings(text: str) -> list[str]:
    warnings = []
    spans = heading_spans(text)
    for key in ("stock_validation_candidates", "theme_mapping_candidates", "watch_only_candidates", "excluded_candidates", "validation_questions"):
        named_alias = SECTIONS[key][1]
        matches = [item for item in spans if nk(named_alias) in nk(item["title"])]
        if len(matches) > 1:
            warnings.append(f"multiple_sections_last_used:{LABELS[key]}:{len(matches)}")
    if "<details" in text.lower() and "思考过程" in text:
        warnings.append("embedded_reasoning_detected")
    if "内容由AI生成" in text or "内容由 AI 生成" in text:
        warnings.append("content_after_structured_report_detected")
    return warnings


def parsed_report_text(text: str) -> str:
    starts = [item["start"] for item in heading_spans(text)
              if nk(item["title"]).startswith(nk("Compass 当前全景与瓶颈发现输入报告"))]
    return text[starts[-1]:].strip() if starts else text.strip()


def normalize(args: argparse.Namespace) -> dict[str, Any]:
    path = args.input.resolve()
    raw = path.read_bytes()
    text = raw.decode(args.encoding)
    q_section = find_section(text, "validation_questions")
    question_records = parse_validation_question_records(q_section[0]) if q_section else []
    questions = parse_questions(q_section[0], question_records) if q_section else {}
    candidates, warnings = collect(text, args.batch_id, questions, question_records)
    warnings.extend(source_document_warnings(text))
    if not q_section:
        warnings.append("missing_section:5 validation_questions")
    result_stats = stats(candidates)
    result_stats["validation_question_records"] = len(question_records)
    return {
        "batch_id": args.batch_id,
        "source": SOURCE,
        "source_report": args.source_report or path.name,
        "source_as_of": args.source_as_of,
        "report_sha256": hashlib.sha256(raw).hexdigest(),
        "parsed_report_sha256": hashlib.sha256(parsed_report_text(text).encode(args.encoding)).hexdigest(),
        "generated_at": now_iso(),
        "mode": "dry_run",
        "no_trade_signal": True,
        "allowed_actions": ALLOWED,
        "blocked_actions": BLOCKED,
        "parser": {"tool": "tools/compass_report_normalizer.py", "version": "v0.2", "strategy": "markdown_section_table_extraction", "parsed_sections": list(LABELS.values())},
        "discovery_protocol": discovery_protocol(),
        "validation_questions": question_records,
        "validation_questions_by_line": questions,
        "stats": result_stats,
        "warnings": warnings,
        "candidates": candidates,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize Compass/ima Markdown into dry-run candidate JSON.")
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--source-as-of", default="")
    parser.add_argument("--source-report", default="")
    parser.add_argument("--output-dir", type=Path, default=OUT_DIR)
    parser.add_argument("--normalized-output", type=Path)
    parser.add_argument("--preview-output", type=Path)
    parser.add_argument("--encoding", default="utf-8")
    args = parser.parse_args()
    payload = normalize(args)
    output_dir = args.output_dir.resolve()
    normalized = args.normalized_output or output_dir / f"compass_candidates_{safe_batch(args.batch_id)}.normalized.json"
    preview = args.preview_output or output_dir / f"zhulong_compass_normalizer_preview_{safe_batch(args.batch_id)}.md"
    normalized.parent.mkdir(parents=True, exist_ok=True)
    preview.parent.mkdir(parents=True, exist_ok=True)
    normalized.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    preview.write_text(render_preview(payload), encoding="utf-8")
    print(json.dumps({"mode": "dry_run", "normalized_output": str(normalized), "preview_output": str(preview), "stats": payload["stats"], "warnings": payload["warnings"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
