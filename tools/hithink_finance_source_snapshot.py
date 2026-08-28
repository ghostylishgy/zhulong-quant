#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Create a bounded, read-only HiThink discovery/evidence source snapshot.

Network access requires --network. Output remains an unverified observation
artifact and is never consumed by L4, RAG, Shadow, Nexus, or daemon code.
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
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
ENGINE_LIB = ROOT / "01_engine" / "lib"
if str(ENGINE_LIB) not in sys.path:
    sys.path.insert(0, str(ENGINE_LIB))

from hithink_finance_bridge import (  # noqa: E402
    HithinkBridgeError,
    HithinkFinanceBridge,
    HithinkResult,
)

BEIJING_TZ = timezone(timedelta(hours=8))
SCHEMA_VERSION = "hithink_finance_source_snapshot_v0.1"
DEFAULT_OUTPUT_DIR = ROOT / "storage" / "reports" / "hithink_finance"
PRIVATE_ENV_FILE = ROOT / "config" / ".env"
MAX_SYMBOLS = 5
MAX_BOARDS = 3
MAX_QUERIES = 3
MAX_BOARD_TAGS = 2
MAX_CATALOG_MATCHES = 30
MAX_CONSTITUENTS = 300
BLOCKED_ACTIONS = [
    "write_duckdb",
    "generate_validation_task",
    "change_l4_verdict",
    "write_shadow",
    "write_rag_memory",
    "write_nexus_audits",
    "trigger_daemon",
    "call_decision_engine",
    "call_nexus_run",
    "trade",
]


def now_iso() -> str:
    return datetime.now(BEIJING_TZ).isoformat(timespec="seconds")


def safe_batch(value: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-zW_-]", "", str(value or "probe"))[:80]
    return cleaned or "probe"


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


def implementation_sha256() -> str:
    digest = hashlib.sha256()
    for path in (Path(__file__).resolve(), ENGINE_LIB / "hithink_finance_bridge.py"):
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def private_api_key() -> str:
    """Read one private value without executing shell syntax from .env."""
    configured = os.getenv("HITHINK_FINANCE_API_KEY", "").strip()
    if configured or not PRIVATE_ENV_FILE.is_file():
        return configured
    for raw_line in PRIVATE_ENV_FILE.read_text(
        encoding="utf-8", errors="ignore"
    ).splitlines():
        line = raw_line.strip()
        if line.startswith("export "):
            line = line[7:].lstrip()
        if not line.startswith("HITHINK_FINANCE_API_KEY="):
            continue
        value = line.split("=", 1)[1].strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        return value.strip()
    return ""


def normalise_unique(values: list[str], maximum: int, label: str) -> list[str]:
    output: list[str] = []
    for raw in values:
        value = str(raw or "").strip()
        if not value:
            raise ValueError(f"{label} contains an empty value")
        if any(char in value for char in "\r\n\t"):
            raise ValueError(f"{label} contains a control character")
        if value not in output:
            output.append(value)
    if len(output) > maximum:
        raise ValueError(f"{label} count must not exceed {maximum}")
    return output


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Read-only HiThink capability and evidence source snapshot"
    )
    parser.add_argument("--batch", required=True)
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--symbol", action="append", default=[])
    parser.add_argument("--board-code", action="append", default=[])
    parser.add_argument("--query", action="append", default=[])
    parser.add_argument(
        "--board-tag",
        action="append",
        choices=["cn_concept", "region", "tszs", "industry"],
        default=[],
    )
    parser.add_argument("--board-keyword", action="append", default=[])
    parser.add_argument("--pool-size", type=int, default=20)
    parser.add_argument("--network", action="store_true")
    parser.add_argument("--timeout", type=float, default=8.0)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser


def validate_args(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    try:
        date.fromisoformat(str(args.trade_date))
        args.symbol = normalise_unique(args.symbol, MAX_SYMBOLS, "symbol")
        args.board_code = normalise_unique(args.board_code, MAX_BOARDS, "board-code")
        args.query = normalise_unique(args.query, MAX_QUERIES, "query")
        args.board_tag = normalise_unique(args.board_tag, MAX_BOARD_TAGS, "board-tag")
        args.board_keyword = normalise_unique(
            args.board_keyword, 10, "board-keyword"
        )
    except ValueError as exc:
        parser.error(str(exc))
    if not 1 <= args.pool_size <= 20:
        parser.error("--pool-size must be between 1 and 20")
    if not 1.0 <= args.timeout <= 12.0:
        parser.error("--timeout must be between 1 and 12 seconds")


def _list(data: Any, key: str = "item") -> list[dict[str, Any]]:
    if not isinstance(data, dict):
        return []
    rows = data.get(key)
    return [dict(row) for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def _filter_catalog(data: Any, keywords: list[str]) -> dict[str, Any]:
    rows = _list(data)
    if keywords:
        lowered = [item.lower() for item in keywords]
        rows = [
            row for row in rows
            if any(term in str(row.get("name") or "").lower() for term in lowered)
        ]
    total = len(rows)
    return {
        "matched_count": total,
        "truncated": total > MAX_CATALOG_MATCHES,
        "item": rows[:MAX_CATALOG_MATCHES],
    }


def _bounded_constituents(data: Any) -> dict[str, Any]:
    rows = _list(data)
    total = len(rows)
    return {
        "timestamp": data.get("timestamp") if isinstance(data, dict) else None,
        "total": total,
        "broad_board": total > MAX_CONSTITUENTS,
        "truncated": total > MAX_CONSTITUENTS,
        "item": rows[:MAX_CONSTITUENTS],
        "historical_membership_supported": False,
    }


def _filter_dragon_tiger(data: Any, symbols: list[str]) -> dict[str, Any]:
    source = dict(data) if isinstance(data, dict) else {}
    wanted = {item.upper() for item in symbols}
    stocks = _list(source, "stock_items")
    if wanted:
        stocks = [row for row in stocks if str(row.get("thscode") or "").upper() in wanted]
    else:
        stocks = stocks[:20]

    hot_money: list[dict[str, Any]] = []
    for raw in _list(source, "hot_money_items"):
        item = dict(raw)
        rows = [dict(row) for row in item.get("rows") or [] if isinstance(row, dict)]
        if wanted:
            rows = [row for row in rows if str(row.get("thscode") or "").upper() in wanted]
        else:
            rows = rows[:20]
        if rows:
            item["rows"] = rows
            hot_money.append(item)
    return {
        "timestamp": source.get("timestamp"),
        "trade_date": source.get("trade_date"),
        "board_type": source.get("board_type"),
        "source_stock_count": source.get("stock_count"),
        "matched_stock_count": len(stocks),
        "stock_items": stocks,
        "hot_money_items": hot_money,
    }


def _bounded_pool(data: Any) -> dict[str, Any]:
    source = dict(data) if isinstance(data, dict) else {}
    rows = _list(source)
    return {
        "timestamp": source.get("timestamp"),
        "pagination": source.get("pagination"),
        "item": rows,
    }


def _job_record(name: str, target: str, time_scope: str) -> dict[str, Any]:
    return {
        "job_name": name,
        "target": target,
        "time_scope": time_scope,
        "status": "PLANNED",
        "request": {},
        "result_meta": {},
        "result_count": 0,
        "error_code": None,
        "error_request_id": "",
    }


def _result_count(data: Any) -> int:
    if isinstance(data, list):
        return len(data)
    if not isinstance(data, dict):
        return 0
    for key in ("item", "stock_items"):
        if isinstance(data.get(key), list):
            return len(data[key])
    return 1 if data else 0


def execute(
    args: argparse.Namespace,
    *,
    client: HithinkFinanceBridge | None = None,
) -> dict[str, Any]:
    bridge = client or HithinkFinanceBridge(
        api_key=private_api_key(), timeout=args.timeout
    )
    jobs: list[dict[str, Any]] = []
    data: dict[str, Any] = {
        "ticker_search": [],
        "board_catalog_matches": [],
        "board_constituents": [],
        "board_snapshots": None,
        "auction_snapshot": None,
        "auction_benchmark": None,
        "dragon_tiger": None,
        "limit_up_pool": None,
        "limit_break_pool": None,
        "limit_up_ladder": None,
    }
    warnings: list[str] = []

    calls: list[
        tuple[str, str, str, Callable[[], HithinkResult], Callable[[Any], Any]]
    ] = []
    for query in args.query:
        calls.append((
            "ticker_search", query, "CURRENT_METADATA",
            lambda q=query: bridge.ticker_search(q), lambda value: value,
        ))
    for tag in args.board_tag:
        calls.append((
            "board_catalog",
            tag,
            "CURRENT_CATALOG",
            lambda t=tag: bridge.ths_index_list(tag=t),
            lambda value, terms=args.board_keyword: _filter_catalog(value, terms),
        ))
    for code in args.board_code:
        calls.append((
            "board_constituents",
            code,
            "CURRENT_MEMBERSHIP_ONLY",
            lambda c=code: bridge.ths_constituents(c),
            _bounded_constituents,
        ))
    if args.board_code:
        calls.append((
            "board_snapshots",
            ",".join(args.board_code),
            "LATEST_MARKET_SNAPSHOT",
            lambda: bridge.index_snapshot(args.board_code),
            lambda value: value,
        ))
    if args.symbol:
        calls.append((
            "auction_snapshot",
            ",".join(args.symbol),
            "TODAY_ONLY",
            lambda: bridge.auction_snapshot(args.symbol, stage="final"),
            lambda value: value,
        ))
    calls.extend([
        (
            "auction_benchmark",
            args.trade_date,
            "REQUESTED_TRADE_DATE",
            lambda: bridge.auction_benchmark(args.trade_date),
            lambda value: value,
        ),
        (
            "dragon_tiger",
            args.trade_date,
            "REQUESTED_TRADE_DATE",
            lambda: bridge.dragon_tiger(args.trade_date, board_type="all"),
            lambda value: _filter_dragon_tiger(value, args.symbol),
        ),
        (
            "limit_up_pool",
            args.trade_date,
            "REQUESTED_TRADE_DATE",
            lambda: bridge.limit_pool(
                args.trade_date, pool="limit_up", size=args.pool_size
            ),
            _bounded_pool,
        ),
        (
            "limit_break_pool",
            args.trade_date,
            "REQUESTED_TRADE_DATE",
            lambda: bridge.limit_pool(
                args.trade_date, pool="limit_break", size=args.pool_size
            ),
            _bounded_pool,
        ),
        (
            "limit_up_ladder",
            "latest_30_trade_days",
            "LATEST_30_TRADE_DAYS",
            bridge.limit_up_ladder,
            lambda value: value,
        ),
    ])

    if len(calls) > 15:
        raise ValueError("expanded HiThink jobs must not exceed 15")

    for name, target, time_scope, call, transform in calls:
        job = _job_record(name, target, time_scope)
        if not args.network:
            job["status"] = "NETWORK_DISABLED"
            jobs.append(job)
            continue
        if not bridge.credential_configured:
            job["status"] = "KEY_MISSING"
            jobs.append(job)
            continue
        try:
            result = call()
            transformed = transform(result.data)
            job["status"] = "DONE"
            job["result_meta"] = result.metadata()
            job["result_count"] = _result_count(transformed)
            if name == "ticker_search":
                data["ticker_search"].append({"target": target, "data": transformed})
            elif name == "board_catalog":
                data["board_catalog_matches"].append({"tag": target, **transformed})
            elif name == "board_constituents":
                data["board_constituents"].append({"board_code": target, **transformed})
            else:
                data[name] = transformed
        except HithinkBridgeError as exc:
            job["status"] = "API_ERROR"
            job["error_code"] = exc.code
            job["error_request_id"] = exc.request_id
            warnings.append(f"{name}:API_ERROR:{exc.code}")
        except Exception as exc:
            job["status"] = "VALIDATION_ERROR"
            warnings.append(f"{name}:VALIDATION_ERROR:{type(exc).__name__}")
        jobs.append(job)

    completed = sum(item["status"] == "DONE" for item in jobs)
    return {
        "schema_version": SCHEMA_VERSION,
        "implementation_sha256": implementation_sha256(),
        "batch_id": safe_batch(args.batch),
        "source": "HiThink Financial API",
        "snapshot_type": "external_market_evidence_source",
        "created_at": now_iso(),
        "trade_date": args.trade_date,
        "dry_run": True,
        "read_only": True,
        "observer_only": True,
        "no_trade_signal": True,
        "network_requested": bool(args.network),
        "credential_configured": bridge.credential_configured,
        "evidence_status": "OBSERVED_UNVERIFIED" if completed else "NOT_FETCHED",
        "manual_review_required": True,
        "decision_authority": False,
        "prompt_use_allowed": False,
        "jobs": jobs,
        "data": data,
        "summary": {
            "job_count": len(jobs),
            "completed_jobs": completed,
            "failed_jobs": sum(
                item["status"] in {"API_ERROR", "VALIDATION_ERROR"} for item in jobs
            ),
            "network_disabled_jobs": sum(
                item["status"] == "NETWORK_DISABLED" for item in jobs
            ),
            "key_missing_jobs": sum(item["status"] == "KEY_MISSING" for item in jobs),
        },
        "warnings": warnings,
        "allowed_actions": [
            "bounded_fetch",
            "render_snapshot",
            "render_preview",
            "manual_review",
            "cross_source_compare",
        ],
        "blocked_actions": BLOCKED_ACTIONS,
    }


def render_preview(snapshot: dict[str, Any]) -> str:
    lines = [
        f"# HiThink Financial Evidence Snapshot · {snapshot['batch_id']}",
        "",
        "## 1. Boundary",
        "",
        f"- trade_date: `{snapshot['trade_date']}`",
        f"- evidence_status: `{snapshot['evidence_status']}`",
        "- dry_run / read_only / observer_only / no_trade_signal: `true`",
        "- decision_authority / prompt_use_allowed: `false`",
        "- Data is vendor-derived and unverified until cross-source/manual review.",
        "",
        "## 2. Jobs",
        "",
        "| job | target | time_scope | status | rows | latency_ms | request_id |",
        "|---|---|---|---|---:|---:|---|",
    ]
    for job in snapshot["jobs"]:
        meta = job.get("result_meta") or {}
        target = str(job.get("target") or "").replace("|", "\\|")
        target = " ".join(target.splitlines())
        lines.append(
            f"| {job['job_name']} | {target} | {job['time_scope']} | {job['status']} | "
            f"{job['result_count']} | {meta.get('elapsed_ms', '')} | "
            f"{meta.get('request_id', job.get('error_request_id', ''))} |"
        )

    sections = [
        ("Dragon-Tiger matched stocks", snapshot["data"].get("dragon_tiger"), "stock_items"),
        ("Limit-up pool", snapshot["data"].get("limit_up_pool"), "item"),
        ("Limit-break pool", snapshot["data"].get("limit_break_pool"), "item"),
    ]
    for title, payload, key in sections:
        lines.extend(["", f"## {title}", "", "| symbol | name | change | note |", "|---|---|---:|---|"])
        rows = payload.get(key, []) if isinstance(payload, dict) else []
        for row in rows[:20]:
            symbol = str(row.get("thscode") or "")
            name = str(row.get("name") or "").replace("|", "\\|")
            change = row.get("change", row.get("price_change_ratio_pct", ""))
            note = str(
                row.get("limit_reason") or row.get("limit_up_reason") or ""
            ).replace("|", "\\|")[:100]
            lines.append(f"| {symbol} | {name} | {change} | {note} |")
        if not rows:
            lines.append("| - | - | - | no fetched rows |")

    lines.extend([
        "",
        "## Safety",
        "",
        "This snapshot does not write DuckDB, generate tasks, call decision_engine/Nexus,",
        "write RAG/Shadow/nexus_audits, trigger daemon, or produce a trade signal.",
        "",
    ])
    return "\n".join(lines)


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    validate_args(args, parser)
    snapshot = execute(args)
    batch = snapshot["batch_id"]
    json_path = args.output_dir / f"hithink_finance_source_snapshot_{batch}.json"
    md_path = args.output_dir / f"hithink_finance_source_snapshot_{batch}.md"
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
