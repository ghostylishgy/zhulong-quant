#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Create a dry-run Tushare discovery-source snapshot for Compass themes.

The snapshot is a read-only source artifact for later Compass theme discovery.
It does not write DuckDB, does not generate validation tasks, and does not call
Nexus, decision_engine, Shadow, RAG, or daemon paths.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
ENGINE_LIB = ROOT / "01_engine" / "lib"
if str(ENGINE_LIB) not in sys.path:
    sys.path.insert(0, str(ENGINE_LIB))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tushare_bridge import get_bridge  # noqa: E402

OUT_DIR = ROOT / "storage" / "reports" / "compass_ingest"
BEIJING_TZ = timezone(timedelta(hours=8))
TOOL_NAME = "tools/compass_discovery_source_snapshot.py"
SNAPSHOT_VERSION = "compass_discovery_source_snapshot_v0.2"

ALLOWED_ACTIONS = ["fetch_tushare_sample", "render_snapshot", "manual_review"]
BLOCKED_ACTIONS = [
    "write_duckdb",
    "generate_validation_task",
    "write_shadow",
    "write_rag_memory",
    "write_nexus_audits",
    "trigger_daemon",
    "call_decision_engine",
    "call_nexus_run",
    "trade",
]

SW_LEVELS = ("L1", "L2", "L3")
SW_MEMBER_PARAM_BY_LEVEL = {"L1": "l1_code", "L2": "l2_code", "L3": "l3_code"}
DEFAULT_BROAD_THEME_THRESHOLD = 300
DEFAULT_PREVIEW_ROWS = 60

STOP_TERMS = {
    "AI", "A", "\u0041\u80a1", "theme", "candidate", "Compass", "ima", "stock", "validation",
    "\u884c\u4e1a", "\u73af\u8282", "\u65b9\u5411", "\u751f\u6001", "\u516c\u53f8", "\u5019\u9009", "\u9a8c\u8bc1", "\u91cd\u70b9", "\u6570\u636e", "\u98ce\u9669", "\u4e1a\u52a1", "\u5e02\u573a", "\u89c2\u5bdf",
}


def now_iso() -> str:
    return datetime.now(BEIJING_TZ).isoformat(timespec="seconds")


def safe_batch(batch_id: str) -> str:
    return re.sub(r"[^0-9A-Za-zW_-]", "", str(batch_id or "batch")) or "batch"


def clean(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        val = float(value)
        if math.isnan(val):
            return None
        return val
    except Exception:
        return None


def json_value(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def df_records(df: pd.DataFrame, limit: int | None = None) -> list[dict[str, Any]]:
    if df is None or df.empty:
        return []
    data = df.head(limit) if limit is not None else df
    out: list[dict[str, Any]] = []
    for rec in data.to_dict(orient="records"):
        out.append({str(k): json_value(v) for k, v in rec.items()})
    return out


def split_keywords(value: str) -> list[str]:
    text = clean(value)
    if not text:
        return []
    parts = re.split(r"[\s,;:/()\[\]+*]+|[\u3001\uff0c\uff1b\uff08\uff09\u3010\u3011\uff1a\u00b7\u00d7]+", text)
    out: list[str] = []
    for part in parts:
        item = clean(part).strip("-_.\u3002\uff01\uff1f!?\u2018\u2019'\"")
        if not item:
            continue
        if item in STOP_TERMS or item.upper() in STOP_TERMS:
            continue
        if re.fullmatch(r"[A-Za-z0-9_-]+", item) and len(item) < 2:
            continue
        if not re.fullmatch(r"[A-Za-z0-9_-]+", item) and len(item) < 2:
            continue
        if item not in out:
            out.append(item)
    return out


def load_keywords_from_file(path: Path) -> list[str]:
    if not path:
        return []
    raw = path.read_text(encoding="utf-8")
    keywords: list[str] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        for item in split_keywords(line):
            if item not in keywords:
                keywords.append(item)
    return keywords


def load_keywords_from_normalized(path: Path) -> tuple[str, list[str], list[dict[str, Any]], str]:
    if not path:
        return "", [], [], ""
    raw = path.read_bytes()
    payload = json.loads(raw.decode("utf-8"))
    batch_id = str(payload.get("batch_id") or "")
    keywords: list[str] = []
    sources: list[dict[str, Any]] = []
    for item in payload.get("candidates") or []:
        if item.get("object_class") != "theme_candidate" or item.get("used_as_discovery_hint") is not True:
            continue
        constraints = item.get("discovery_constraints") or {}
        explicit_terms = (item.get("discovery_keywords") or []) + (item.get("supply_chain_nodes") or [])
        raw_terms: list[Any] = explicit_terms or (
            (constraints.get("query_terms") or []) + [
                item.get("name"), item.get("thesis"), item.get("benefit_mechanism"), item.get("watch_focus")
            ]
        )
        local_terms: list[str] = []
        for raw_term in raw_terms:
            for kw in split_keywords(str(raw_term or "")):
                if kw not in local_terms:
                    local_terms.append(kw)
                if kw not in keywords:
                    keywords.append(kw)
        sources.append({
            "candidate_id": item.get("candidate_id"),
            "name": item.get("name"),
            "compass_line_key": item.get("compass_line_key"),
            "keywords": local_terms,
            "keyword_source": "explicit_discovery_fields" if explicit_terms else "legacy_fallback",
            "discovery_status": item.get("discovery_status"),
        })
    return batch_id, keywords, sources, hashlib.sha256(raw).hexdigest()


def normalize_keywords(values: list[str], limit: int) -> list[str]:
    out: list[str] = []
    for value in values:
        for item in split_keywords(str(value or "")):
            if item not in out:
                out.append(item)
    return out[:limit]


def call_api(api: Any, api_name: str, **params) -> pd.DataFrame:
    if not hasattr(api, api_name):
        raise RuntimeError(f"Tushare SDK missing API: {api_name}")
    fn = getattr(api, api_name)
    df = fn(**params)
    if df is None:
        return pd.DataFrame()
    if not isinstance(df, pd.DataFrame):
        raise RuntimeError(f"Tushare API returned non-DataFrame api={api_name}: {type(df)}")
    return df


def fetch_sw_index_classify(api: Any, warnings: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for level in SW_LEVELS:
        try:
            df = call_api(api, "index_classify", src="SW2021", level=level)
            for rec in df_records(df):
                rec["fetch_level"] = level
                rows.append(rec)
        except Exception as exc:
            warnings.append(f"sw_index_classify_failed:{level}:{str(exc)[:240]}")
    return rows


def text_contains(text: Any, keyword: str) -> bool:
    return str(keyword or "").lower() in str(text or "").lower()


def match_sw_indices(sw_rows: list[dict[str, Any]], keywords: list[str], max_matches: int, warnings: list[str]) -> list[dict[str, Any]]:
    matched_by_code: dict[str, dict[str, Any]] = {}
    for kw in keywords:
        for row in sw_rows:
            name = str(row.get("industry_name") or "")
            code = str(row.get("index_code") or "")
            if not code or not name:
                continue
            if text_contains(name, kw) or text_contains(kw, name):
                match = matched_by_code.get(code)
                if match:
                    if kw not in match["matched_keywords"]:
                        match["matched_keywords"].append(kw)
                    continue
                matched_by_code[code] = {
                    "keyword": kw,
                    "matched_keywords": [kw],
                    "index_code": code,
                    "industry_name": name,
                    "level": row.get("level") or row.get("fetch_level"),
                    "industry_code": row.get("industry_code"),
                    "parent_code": row.get("parent_code"),
                    "src": row.get("src"),
                    "evidence_level": "medium",
                    "business_relevance_evidence_present": False,
                    "manual_review_required": True,
                    "generate_task": False,
                    "match_source": "sw_index_classify.name_keyword",
                }
    matched = list(matched_by_code.values())
    if len(matched) > max_matches:
        warnings.append(f"sw_index_matches_truncated:{len(matched)}>{max_matches}")
        matched = matched[:max_matches]
    return matched


def fetch_sw_members(api: Any, sw_matches: list[dict[str, Any]], threshold: int,
                     max_store: int, warnings: list[str]) -> list[dict[str, Any]]:
    members: list[dict[str, Any]] = []
    for match in sw_matches:
        level = str(match.get("level") or "")
        code = str(match.get("index_code") or "")
        param = SW_MEMBER_PARAM_BY_LEVEL.get(level)
        if not param or not code:
            warnings.append(f"sw_member_skip_missing_param:{code}:{level}")
            continue
        try:
            df = call_api(api, "index_member_all", **{param: code})
            total = len(df)
            match["member_count"] = total
            if total > threshold:
                match["broad_theme"] = True
                match["expand_members"] = False
                match["member_count"] = total
                match["skip_member_reason"] = f"actual_member_count>{threshold}"
                warnings.append(f"sw_members_broad_after_fetch:{code}:{total}>{threshold}")
                continue
            records = df_records(df, max_store)
            if len(df) > max_store:
                warnings.append(f"sw_members_truncated:{code}:{len(df)}>{max_store}")
            for rec in records:
                rec.update({
                    "source_keyword": match.get("keyword"),
                    "source_keywords": match.get("matched_keywords") or [match.get("keyword")],
                    "source_index_code": code,
                    "source_index_name": match.get("industry_name"),
                    "source_index_level": level,
                    "source_index_member_count": total,
                    "evidence_level": "medium",
                    "business_relevance_evidence_present": False,
                    "manual_review_required": True,
                    "generate_task": False,
                })
                members.append(rec)
        except Exception as exc:
            warnings.append(f"sw_member_fetch_failed:{code}:{level}:{str(exc)[:240]}")
    return members


def fetch_ths_indices(api: Any, warnings: list[str]) -> list[dict[str, Any]]:
    try:
        df = call_api(api, "ths_index", exchange="A")
        return df_records(df)
    except Exception as exc:
        warnings.append(f"ths_index_failed:{str(exc)[:240]}")
        return []


def match_ths_indices(ths_rows: list[dict[str, Any]], keywords: list[str], max_matches: int, threshold: int, warnings: list[str]) -> list[dict[str, Any]]:
    matched_by_code: dict[str, dict[str, Any]] = {}
    for kw in keywords:
        for row in ths_rows:
            name = str(row.get("name") or "")
            code = str(row.get("ts_code") or "")
            if not code or not name:
                continue
            if not text_contains(name, kw) and not text_contains(kw, name):
                continue
            match = matched_by_code.get(code)
            if match:
                if kw not in match["matched_keywords"]:
                    match["matched_keywords"].append(kw)
                continue
            count = safe_float(row.get("count"))
            broad = bool(count is not None and count > threshold)
            unknown_count = count is None
            matched_by_code[code] = {
                "keyword": kw,
                "matched_keywords": [kw],
                "ts_code": code,
                "name": name,
                "count": count,
                "exchange": row.get("exchange"),
                "list_date": row.get("list_date"),
                "type": row.get("type"),
                "broad_theme": broad,
                "member_count_unknown": unknown_count,
                "expand_members": (not broad and not unknown_count),
                "skip_member_reason": "broad_theme" if broad else ("member_count_unknown" if unknown_count else ""),
                "evidence_level": "medium",
                "business_relevance_evidence_present": False,
                "manual_review_required": True,
                "generate_task": False,
                "match_source": "ths_index.name_keyword",
            }
    matched = list(matched_by_code.values())
    if len(matched) > max_matches:
        warnings.append(f"ths_index_matches_truncated:{len(matched)}>{max_matches}")
        matched = matched[:max_matches]
    return matched


def fetch_ths_members(api: Any, ths_matches: list[dict[str, Any]], threshold: int, max_store: int, warnings: list[str]) -> list[dict[str, Any]]:
    members: list[dict[str, Any]] = []
    for match in ths_matches:
        if not match.get("expand_members"):
            continue
        code = str(match.get("ts_code") or "")
        try:
            df = call_api(api, "ths_member", ts_code=code)
            total = len(df)
            match["member_count"] = total
            if total > threshold:
                match["broad_theme"] = True
                match["expand_members"] = False
                match["skip_member_reason"] = f"actual_member_count>{threshold}"
                warnings.append(f"ths_members_broad_after_fetch:{code}:{total}>{threshold}")
                continue
            records = df_records(df, max_store)
            if total > max_store:
                warnings.append(f"ths_members_truncated:{code}:{total}>{max_store}")
            for rec in records:
                rec.update({
                    "source_keyword": match.get("keyword"),
                    "source_keywords": match.get("matched_keywords") or [match.get("keyword")],
                    "source_index_code": code,
                    "source_index_name": match.get("name"),
                    "source_index_type": match.get("type"),
                    "source_index_member_count": total,
                    "evidence_level": "medium",
                    "business_relevance_evidence_present": False,
                    "manual_review_required": True,
                    "generate_task": False,
                })
                members.append(rec)
        except Exception as exc:
            warnings.append(f"ths_member_fetch_failed:{code}:{str(exc)[:240]}")
    return members


def md_table(headers: list[str], rows: list[list[Any]]) -> list[str]:
    out = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        out.append("| " + " | ".join(str(value if value is not None else "").replace("|", "/") for value in row) + " |")
    return out


def build_snapshot(args: argparse.Namespace) -> dict[str, Any]:
    warnings: list[str] = []
    normalized_batch = ""
    normalized_keywords: list[str] = []
    normalized_sources: list[dict[str, Any]] = []
    normalized_sha = ""
    if args.normalized:
        normalized_batch, normalized_keywords, normalized_sources, normalized_sha = load_keywords_from_normalized(args.normalized.resolve())

    file_keywords = load_keywords_from_file(args.keywords_file.resolve()) if args.keywords_file else []
    cli_keywords = normalize_keywords(args.keywords or [], limit=args.max_keywords)
    keywords = normalize_keywords(normalized_keywords + file_keywords + cli_keywords, limit=args.max_keywords)
    batch_id = args.batch_id or normalized_batch or "batch"
    if not keywords:
        raise SystemExit("no keywords supplied; use --keywords, --keywords-file, or --normalized")

    bridge = get_bridge()
    api = getattr(bridge, "api", None)
    if not getattr(bridge, "available", False) or api is None:
        raise SystemExit("Tushare bridge API unavailable; token or SDK not ready")

    sw_rows = fetch_sw_index_classify(api, warnings)
    sw_matches = match_sw_indices(sw_rows, keywords, args.max_sw_index_matches, warnings)
    sw_members = fetch_sw_members(api, sw_matches, args.broad_theme_threshold, args.max_sw_members_store, warnings)
    ths_rows = fetch_ths_indices(api, warnings)
    ths_matches = match_ths_indices(ths_rows, keywords, args.max_ths_index_matches, args.broad_theme_threshold, warnings)
    ths_members = fetch_ths_members(api, ths_matches, args.broad_theme_threshold, args.max_ths_members_store, warnings)

    broad = [row for row in sw_matches + ths_matches if row.get("broad_theme") or row.get("member_count_unknown")]
    by_keyword = Counter(
        keyword
        for row in sw_matches + ths_matches
        for keyword in (row.get("matched_keywords") or [row.get("keyword")])
        if keyword
    )
    return {
        "batch_id": batch_id,
        "source": "tushare",
        "snapshot_type": "discovery_source",
        "snapshot_version": SNAPSHOT_VERSION,
        "generated_at": now_iso(),
        "dry_run": True,
        "mode": "dry_run",
        "no_trade_signal": True,
        "tool": TOOL_NAME,
        "source_normalized": str(args.normalized.resolve()) if args.normalized else None,
        "source_normalized_sha256": normalized_sha or None,
        "theme_sources": normalized_sources,
        "keywords": keywords,
        "parameters": {
            "broad_theme_threshold": args.broad_theme_threshold,
            "max_keywords": args.max_keywords,
            "max_sw_index_matches": args.max_sw_index_matches,
            "max_ths_index_matches": args.max_ths_index_matches,
            "max_sw_members_store": args.max_sw_members_store,
            "max_ths_members_store": args.max_ths_members_store,
        },
        "allowed_actions": ALLOWED_ACTIONS,
        "blocked_actions": BLOCKED_ACTIONS,
        "evidence_policy": {
            "sw_index_classify_plus_members": "medium_when_not_broad",
            "ths_index_plus_members": "medium",
            "stock_basic_industry_or_name_keyword": "weak",
            "sw_ths_hit_is_not_strong_business_relevance": True,
            "business_relevance_evidence_present": False,
            "manual_review_required": True,
            "generate_task": False,
        },
        "stats": {
            "keywords": len(keywords),
            "sw_index_classify_rows": len(sw_rows),
            "sw_index_matches": len(sw_matches),
            "sw_unique_indices_matched": len(sw_matches),
            "sw_index_members": len(sw_members),
            "ths_indices_scanned": len(ths_rows),
            "ths_indices_matched": len(ths_matches),
            "ths_unique_indices_matched": len(ths_matches),
            "ths_members": len(ths_members),
            "broad_theme_indices": len(broad),
            "generated_tasks": 0,
            "by_keyword_matches": dict(sorted(by_keyword.items())),
        },
        "sw_index_classify": sw_rows,
        "sw_index_matches": sw_matches,
        "sw_index_members": sw_members,
        "ths_indices_matched": ths_matches,
        "ths_members": ths_members,
        "warnings": warnings,
        "validation_tasks": [],
    }


def render_preview(payload: dict[str, Any], preview_rows: int) -> str:
    lines = [
        f"# Compass Discovery Source Snapshot - {payload['batch_id']}",
        "",
        "## 1. Batch",
        "",
        f"- source: `{payload.get('source')}`",
        f"- snapshot_type: `{payload.get('snapshot_type')}`",
        f"- snapshot_version: `{payload.get('snapshot_version')}`",
        f"- mode: `{payload.get('mode')}`",
        "- dry_run: `true`",
        "- no_trade_signal: `true`",
        f"- keywords: `{', '.join(payload.get('keywords') or [])}`",
        "",
        "## 2. Stats",
        "",
    ]
    st = payload.get("stats") or {}
    stat_rows = [[key, value] for key, value in st.items() if key != "by_keyword_matches"]
    stat_rows += [[f"keyword:{k}", v] for k, v in (st.get("by_keyword_matches") or {}).items()]
    lines += md_table(["metric", "value"], stat_rows)

    sw_rows = [
        [", ".join(row.get("matched_keywords") or [row.get("keyword")]), row.get("index_code"), row.get("industry_name"), row.get("level"), row.get("broad_theme"), row.get("skip_member_reason"), row.get("evidence_level"), row.get("manual_review_required"), row.get("generate_task")]
        for row in payload.get("sw_index_matches") or []
    ]
    lines += ["", "## 3. SW Index Matches", ""] + md_table(
        ["keyword", "index_code", "industry_name", "level", "broad_theme", "skip_reason", "evidence", "manual_review_required", "generate_task"],
        sw_rows,
    )

    sw_member_rows = [
        [", ".join(row.get("source_keywords") or [row.get("source_keyword")]), row.get("source_index_name"), row.get("ts_code"), row.get("name"), row.get("l1_name"), row.get("l2_name"), row.get("l3_name")]
        for row in (payload.get("sw_index_members") or [])[:preview_rows]
    ]
    lines += ["", f"## 4. SW Member Sample (First {preview_rows})", ""] + md_table(
        ["keyword", "source_index", "ts_code", "name", "l1", "l2", "l3"],
        sw_member_rows,
    )

    ths_rows = [
        [", ".join(row.get("matched_keywords") or [row.get("keyword")]), row.get("ts_code"), row.get("name"), row.get("type"), row.get("count"), row.get("broad_theme"), row.get("expand_members"), row.get("skip_member_reason")]
        for row in payload.get("ths_indices_matched") or []
    ]
    lines += ["", "## 5. THS Index Matches", ""] + md_table(
        ["keyword", "ts_code", "name", "type", "count", "broad_theme", "expand_members", "skip_reason"],
        ths_rows,
    )

    ths_member_rows = [
        [", ".join(row.get("source_keywords") or [row.get("source_keyword")]), row.get("source_index_name"), row.get("source_index_code"), row.get("con_code"), row.get("con_name")]
        for row in (payload.get("ths_members") or [])[:preview_rows]
    ]
    lines += ["", f"## 6. THS Member Sample (First {preview_rows})", ""] + md_table(
        ["keyword", "source_index", "source_code", "con_code", "con_name"],
        ths_member_rows,
    )

    broad_rows = [
        [row.get("keyword"), row.get("index_code") or row.get("ts_code"), row.get("industry_name") or row.get("name"), row.get("member_count") or row.get("count"), row.get("skip_member_reason")]
        for row in (payload.get("sw_index_matches") or []) + (payload.get("ths_indices_matched") or [])
        if row.get("broad_theme") or row.get("member_count_unknown")
    ]
    lines += ["", "## 7. Broad / Not Expanded Themes", ""] + md_table(
        ["keyword", "index_code", "name", "count", "reason"],
        broad_rows,
    )

    warn_rows = [[warning] for warning in payload.get("warnings") or []]
    lines += ["", "## 8. Warnings", ""] + md_table(["warning"], warn_rows)
    lines += [
        "",
        "## 9. Safety",
        "",
        "This snapshot is a dry-run discovery source. SW/THS hits are medium evidence only and are not strong business relevance evidence. Every downstream row must remain manual_review_required=true, business_relevance_evidence_present=false, and generate_task=false until a separate human-reviewed process promotes it.",
        "",
        "Blocked actions: `" + ", ".join(payload.get("blocked_actions") or []) + "`.",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Create a dry-run Tushare discovery-source snapshot for Compass themes.")
    parser.add_argument("--batch-id", default="")
    parser.add_argument("--normalized", type=Path, help="Optional normalized Compass JSON; theme keywords will be extracted from theme_candidate rows.")
    parser.add_argument("--keywords", nargs="*", default=[], help="Theme keywords. Repeat or separate with spaces/punctuation.")
    parser.add_argument("--keywords-file", type=Path, help="UTF-8 text file with one or more keywords per line.")
    parser.add_argument("--output-dir", type=Path, default=OUT_DIR)
    parser.add_argument("--json-output", type=Path)
    parser.add_argument("--preview-output", type=Path)
    parser.add_argument("--max-keywords", type=int, default=40)
    parser.add_argument("--max-sw-index-matches", type=int, default=40)
    parser.add_argument("--max-ths-index-matches", type=int, default=40)
    parser.add_argument("--max-sw-members-store", type=int, default=500)
    parser.add_argument("--max-ths-members-store", type=int, default=300)
    parser.add_argument("--broad-theme-threshold", type=int, default=DEFAULT_BROAD_THEME_THRESHOLD)
    parser.add_argument("--preview-rows", type=int, default=DEFAULT_PREVIEW_ROWS)
    args = parser.parse_args()

    payload = build_snapshot(args)
    output_dir = args.output_dir.resolve()
    json_output = args.json_output or output_dir / f"compass_discovery_source_snapshot_{safe_batch(payload['batch_id'])}.json"
    preview_output = args.preview_output or output_dir / f"zhulong_compass_discovery_source_snapshot_{safe_batch(payload['batch_id'])}.md"
    json_output.parent.mkdir(parents=True, exist_ok=True)
    preview_output.parent.mkdir(parents=True, exist_ok=True)
    json_output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    preview_output.write_text(render_preview(payload, max(1, int(args.preview_rows))), encoding="utf-8")
    print(json.dumps({
        "mode": "dry_run",
        "json_output": str(json_output),
        "preview_output": str(preview_output),
        "stats": payload.get("stats"),
        "warnings": payload.get("warnings"),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
