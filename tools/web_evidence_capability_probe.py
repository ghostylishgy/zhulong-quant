#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generate a bounded, read-only Web evidence capability snapshot.

Default execution is offline planning.  Network calls require ``--network``;
AnySearch anonymous access additionally requires an explicit opt-in.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
LIB = ROOT / "02_brain" / "lib"
if str(LIB) not in sys.path:
    sys.path.insert(0, str(LIB))

from web_evidence_snapshot import (  # noqa: E402
    DISCOVERED_UNVERIFIED,
    FETCHED_UNVERIFIED,
    ProviderConfig,
    ProviderError,
    load_provider_configs,
    normalize_public_url,
    safe_error,
    scrape_firecrawl,
    search_anysearch,
    search_firecrawl,
)

BEIJING_TZ = timezone(timedelta(hours=8))
SCHEMA_VERSION = "web_evidence_snapshot_v0.1"
DEFAULT_OUTPUT_DIR = ROOT / "storage" / "reports" / "web_evidence_snapshot"
BLOCKED_ACTIONS = [
    "write_duckdb",
    "generate_validation_task",
    "write_shadow",
    "write_rag_memory",
    "write_nexus_audits",
    "change_l4_verdict",
    "trigger_daemon",
    "call_decision_engine",
    "call_nexus_run",
    "trade",
]


def now_iso() -> str:
    return datetime.now(BEIJING_TZ).isoformat(timespec="seconds")


def implementation_sha256() -> str:
    digest = hashlib.sha256()
    for path in (Path(__file__).resolve(), LIB / "web_evidence_snapshot.py"):
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def safe_batch(value: str) -> str:
    return re.sub(r"[^0-9A-Za-zW_-]", "", str(value or "probe")) or "probe"


def parse_as_of(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("--as-of must be YYYY-MM-DD") from exc


def atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    finally:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read-only Web evidence capability probe")
    parser.add_argument("--batch", required=True)
    parser.add_argument("--as-of", required=True, type=parse_as_of)
    parser.add_argument("--provider", action="append", choices=["anysearch", "firecrawl"])
    parser.add_argument("--query", action="append", default=[])
    parser.add_argument("--fetch-url", action="append", default=[])
    parser.add_argument("--limit", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=8.0)
    parser.add_argument("--network", action="store_true")
    parser.add_argument("--allow-anonymous-anysearch", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser


def validate_args(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    if not args.query and not args.fetch_url:
        parser.error("at least one --query or --fetch-url is required")
    providers = list(dict.fromkeys(args.provider or ["anysearch", "firecrawl"]))
    if any(not str(item).strip() or len(str(item).strip()) > 500 for item in args.query):
        parser.error("each --query must contain 1 to 500 characters")
    if args.fetch_url and "firecrawl" not in providers:
        parser.error("--fetch-url requires --provider firecrawl")
    for raw_url in args.fetch_url:
        try:
            normalize_public_url(raw_url)
        except ValueError as exc:
            parser.error(f"unsafe --fetch-url: {exc}")
    planned_jobs = len(args.query) * len(providers) + (len(args.fetch_url) if "firecrawl" in providers else 0)
    if planned_jobs > 5:
        parser.error("expanded provider jobs must not exceed 5")
    if not 1 <= args.limit <= 5:
        parser.error("--limit must be between 1 and 5")
    if not 1.0 <= args.timeout <= 12.0:
        parser.error("--timeout must be between 1 and 12 seconds")
    if args.as_of > datetime.now(BEIJING_TZ).date():
        parser.error("--as-of cannot be in the future")


def execute(args: argparse.Namespace) -> dict[str, Any]:
    providers = list(dict.fromkeys(args.provider or ["anysearch", "firecrawl"]))
    configs = load_provider_configs()
    configs["anysearch"] = ProviderConfig(
        name="anysearch",
        api_key=configs["anysearch"].api_key,
        allow_anonymous=bool(args.allow_anonymous_anysearch),
    )
    jobs: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    warnings: list[str] = []

    def record_job(provider: str, operation: str, target: str, index: int) -> None:
        query_id = f"{provider.upper()}-{operation.upper()}-{index:03d}"
        job = {
            "query_id": query_id,
            "provider": provider,
            "operation": operation,
            "query_text": target if operation == "search" else None,
            "target_sha256": hashlib.sha256(target.encode("utf-8")).hexdigest(),
            "status": "NETWORK_DISABLED" if not args.network else "PENDING",
            "result_count": 0,
            "provider_meta": {},
        }
        if not args.network:
            jobs.append(job)
            return
        try:
            if provider == "anysearch" and operation == "search":
                rows, meta = search_anysearch(
                    target, query_id=query_id, as_of=args.as_of, limit=args.limit,
                    config=configs[provider], timeout=args.timeout,
                )
            elif provider == "firecrawl" and operation == "search":
                rows, meta = search_firecrawl(
                    target, query_id=query_id, as_of=args.as_of, limit=args.limit,
                    config=configs[provider], timeout=args.timeout,
                )
            elif provider == "firecrawl" and operation == "scrape":
                rows, meta = scrape_firecrawl(
                    target, query_id=query_id, as_of=args.as_of,
                    config=configs[provider], timeout=args.timeout,
                )
            else:
                job["status"] = "UNSUPPORTED_OPERATION"
                jobs.append(job)
                return
            results.extend(rows)
            job["status"] = "DONE"
            job["result_count"] = len(rows)
            job["provider_meta"] = meta
        except Exception as exc:
            job["status"] = safe_error(exc)
            warnings.append(f"{query_id}:{job['status']}")
        jobs.append(job)

    counter = 0
    for provider in providers:
        for query in args.query:
            counter += 1
            record_job(provider, "search", str(query), counter)
        if provider == "firecrawl":
            for url in args.fetch_url:
                counter += 1
                record_job(provider, "scrape", str(url), counter)

    accepted_statuses = {DISCOVERED_UNVERIFIED, FETCHED_UNVERIFIED}
    return {
        "schema_version": SCHEMA_VERSION,
        "implementation_sha256": implementation_sha256(),
        "batch_id": safe_batch(args.batch),
        "source": "AnySearch/Firecrawl",
        "snapshot_type": "web_evidence_capability_probe",
        "created_at": now_iso(),
        "as_of": args.as_of.isoformat(),
        "dry_run": True,
        "read_only": True,
        "no_trade_signal": True,
        "network_requested": bool(args.network),
        "execution_mode": "BOUNDED_NETWORK_PROBE" if args.network else "OFFLINE_PLAN",
        "provider_credentials_configured": {
            name: bool(configs[name].api_key) for name in providers
        },
        "jobs": jobs,
        "results": results,
        "summary": {
            "job_count": len(jobs),
            "done_jobs": sum(1 for item in jobs if item["status"] == "DONE"),
            "unverified_results": sum(1 for item in results if item["evidence_status"] in accepted_statuses),
            "rejected_results": sum(1 for item in results if item["evidence_status"] not in accepted_statuses),
            "verified_evidence_count": 0,
        },
        "warnings": warnings,
        "allowed_actions": ["bounded_search", "bounded_fetch", "normalize", "render_preview", "manual_review"],
        "blocked_actions": BLOCKED_ACTIONS,
    }


def render_preview(snapshot: dict[str, Any]) -> str:
    lines = [
        f"# Web Evidence Capability Probe · {snapshot['batch_id']}",
        "",
        "## 1. 边界",
        "",
        f"- as_of: `{snapshot['as_of']}`",
        f"- mode: `{snapshot['execution_mode']}`",
        "- dry_run / read_only / no_trade_signal: `true`",
        "- 所有搜索命中和抓取页面均为未验证材料，不能直接进入 L4、RAG、Shadow 或交易判断。",
        "- 外部文本统一视为 tainted content，`prompt_use_allowed=false`。",
        "",
        "## 2. Provider 能力",
        "",
        "| provider | operation | status | result_count |",
        "|---|---|---|---:|",
    ]
    for job in snapshot["jobs"]:
        lines.append(f"| {job['provider']} | {job['operation']} | {job['status']} | {job['result_count']} |")
    lines += [
        "",
        "## 3. 未验证结果",
        "",
        "| provider | title | domain | reported_date | status |",
        "|---|---|---|---|---|",
    ]
    for row in snapshot["results"]:
        title = str(row.get("title") or "").replace("|", "\\|")[:100]
        lines.append(
            f"| {row['provider']} | {title} | {row.get('source_domain') or ''} | "
            f"{row.get('published_date_parsed') or ''} | {row['evidence_status']} |"
        )
    if not snapshot["results"]:
        lines.append("| - | - | - | - | 无结果或未执行联网探测 |")
    lines += [
        "",
        "## 4. 统计",
        "",
        f"- jobs: `{snapshot['summary']['job_count']}`",
        f"- unverified_results: `{snapshot['summary']['unverified_results']}`",
        f"- verified_evidence_count: `{snapshot['summary']['verified_evidence_count']}`",
        "",
        "## 5. 警告",
        "",
    ]
    lines.extend([f"- `{item}`" for item in snapshot["warnings"]] or ["- 无"])
    lines += [
        "",
        "## 6. 安全声明",
        "",
        "本工具不写 DuckDB，不生成 validation task，不调用 decision_engine/Nexus，不写 RAG/Shadow/nexus_audits，不触发 daemon。",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    validate_args(args, parser)
    snapshot = execute(args)
    batch = snapshot["batch_id"]
    json_path = args.output_dir / f"web_evidence_capability_probe_{batch}.json"
    md_path = args.output_dir / f"web_evidence_capability_probe_{batch}.md"
    atomic_write(json_path, json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n")
    atomic_write(md_path, render_preview(snapshot))
    print(json.dumps({
        "json": str(json_path),
        "preview": str(md_path),
        "summary": snapshot["summary"],
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
