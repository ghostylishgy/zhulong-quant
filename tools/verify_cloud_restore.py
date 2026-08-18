#!/usr/bin/env python3
"""Verify a Zhulong restic restore without activating production behavior."""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping

import duckdb


SCHEMA_VERSION = "zhulong_cloud_restore_verification_v0.1"
KEY_TABLES: Mapping[str, str] = {
    "fact_daily": "trade_date",
    "fact_stock_basic": "updated_at",
    "nexus_audits": "trade_date",
    "fact_strategic_memory": "trade_date",
    "fact_paper_positions": "trade_date",
    "fact_trade_calendar": "cal_date",
}
REQUIRED_ASSETS = (
    ".env",
    "05_shadow/config/rules.yaml",
    "config/config.yaml",
    "config/settings.py",
    "README.md",
    "README.zh-CN.md",
    "devlog.md",
)
BLOCKED_ACTIONS = (
    "start_daemon",
    "write_production_duckdb",
    "write_shadow",
    "write_rag_memory",
    "write_nexus_audits",
    "call_tushare",
    "send_pushplus",
    "execute_trade",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return str(value)


def _safe_identifier(value: str) -> str:
    if not value.replace("_", "").isalnum():
        raise ValueError(f"unsafe SQL identifier: {value}")
    return value


def validate_isolated_root(restore_root: Path, production_root: Path) -> Path:
    root = restore_root.expanduser().resolve()
    production = production_root.expanduser().resolve()
    if not root.is_dir():
        raise ValueError(f"restore root does not exist: {root}")
    if root == production:
        raise ValueError("restore root must not be the production project root")

    restored_db = root / "storage/database/zhulong_cloud_backup.duckdb"
    production_db = production / "storage/database/zhulong.duckdb"
    if restored_db.exists() and production_db.exists():
        try:
            if restored_db.samefile(production_db):
                raise ValueError("restored database resolves to production database")
        except OSError:
            pass
    return root


def inspect_assets(root: Path) -> Dict[str, Any]:
    rows = []
    for relative in REQUIRED_ASSETS:
        path = root / relative
        row: Dict[str, Any] = {
            "path": relative,
            "exists": path.is_file(),
            "sensitive": relative == ".env",
        }
        if path.is_file():
            row["size_bytes"] = path.stat().st_size
            if relative != ".env":
                row["sha256"] = _sha256(path)
        rows.append(row)
    return {
        "required_count": len(rows),
        "present_count": sum(1 for row in rows if row["exists"]),
        "all_present": all(row["exists"] for row in rows),
        "files": rows,
        "secret_contents_read": False,
        "secret_contents_exposed": False,
    }


def inspect_duckdb(db_path: Path) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "relative_path": "storage/database/zhulong_cloud_backup.duckdb",
        "exists": db_path.is_file(),
        "opened_read_only": False,
        "size_bytes": db_path.stat().st_size if db_path.is_file() else 0,
        "tables": {},
    }
    if not db_path.is_file():
        result["error"] = "DATABASE_MISSING"
        return result

    try:
        with duckdb.connect(str(db_path), read_only=True) as conn:
            result["opened_read_only"] = True
            result["database_table_count"] = int(
                conn.execute(
                    "SELECT COUNT(*) FROM information_schema.tables "
                    "WHERE table_schema = 'main'"
                ).fetchone()[0]
            )
            for table, time_column in KEY_TABLES.items():
                safe_table = _safe_identifier(table)
                safe_time = _safe_identifier(time_column)
                exists = bool(
                    conn.execute(
                        "SELECT COUNT(*) FROM information_schema.tables "
                        "WHERE table_schema = 'main' AND table_name = ?",
                        [table],
                    ).fetchone()[0]
                )
                row: Dict[str, Any] = {"exists": exists}
                if exists:
                    count, min_value, max_value = conn.execute(
                        f"SELECT COUNT(*), MIN({safe_time}), MAX({safe_time}) "
                        f"FROM {safe_table}"
                    ).fetchone()
                    row.update(
                        {
                            "row_count": int(count),
                            "min_time": _json_value(min_value),
                            "max_time": _json_value(max_value),
                        }
                    )
                result["tables"][table] = row
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    result["metadata_queries_passed"] = "error" not in result
    result["all_key_tables_present"] = bool(
        result["metadata_queries_passed"]
        and set(result["tables"]) == set(KEY_TABLES)
        and all(row.get("exists") for row in result["tables"].values())
    )
    return result


def _walk_files(root: Path) -> Iterable[Path]:
    if not root.is_dir():
        return ()
    return (path for path in root.rglob("*") if path.is_file())


def inspect_chroma(chroma_root: Path) -> Dict[str, Any]:
    files = list(_walk_files(chroma_root))
    sqlite_path = chroma_root / "chroma.sqlite3"
    result: Dict[str, Any] = {
        "relative_path": "storage/chromadb",
        "exists": chroma_root.is_dir(),
        "file_count": len(files),
        "size_bytes": sum(path.stat().st_size for path in files),
        "sqlite_exists": sqlite_path.is_file(),
        "sqlite_opened_read_only": False,
    }
    if sqlite_path.is_file():
        try:
            uri = f"file:{sqlite_path.as_posix()}?mode=ro"
            with contextlib.closing(sqlite3.connect(uri, uri=True)) as conn:
                result["sqlite_opened_read_only"] = True
                tables = {
                    row[0]
                    for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    ).fetchall()
                }
                for table in ("collections", "segments", "embeddings"):
                    if table in tables:
                        result[f"{table}_count"] = int(
                            conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                        )
        except Exception as exc:
            result["sqlite_error"] = f"{type(exc).__name__}: {exc}"
    return result


def compare_table_baseline(
    database: Mapping[str, Any], baseline: Mapping[str, Any]
) -> Dict[str, Any]:
    expected_tables = baseline.get("tables", {})
    actual_tables = database.get("tables", {})
    rows: Dict[str, Any] = {}
    for table in KEY_TABLES:
        expected = expected_tables.get(table, {})
        actual = actual_tables.get(table, {})
        fields = ("row_count", "min_time", "max_time")
        mismatches = {
            field: {"expected": expected.get(field), "actual": actual.get(field)}
            for field in fields
            if expected.get(field) != actual.get(field)
        }
        rows[table] = {
            "match": bool(expected) and not mismatches,
            "mismatches": mismatches,
        }
    snapshot_id = str(baseline.get("snapshot_id") or "")
    return {
        "snapshot_id": snapshot_id,
        "all_tables_match": bool(snapshot_id) and all(row["match"] for row in rows.values()),
        "tables": rows,
    }


def build_report(
    restore_root: Path,
    *,
    production_root: Path,
    snapshot_id: str,
    snapshot_time: str,
    repository_check: str,
    cloud_retrieved: bool,
    rto_seconds: float,
    active_recovery_seconds: float,
    password_source: str,
    offline_password_custody_verified: bool,
    baseline: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    if not snapshot_id.strip():
        raise ValueError("snapshot_id is required")
    if password_source not in {"offline_custody", "production_node_export"}:
        raise ValueError(f"unsupported password_source: {password_source}")
    if offline_password_custody_verified and password_source != "offline_custody":
        raise ValueError("offline custody cannot be verified from a production-node export")
    if rto_seconds < 0 or active_recovery_seconds < 0:
        raise ValueError("recovery durations must be non-negative")
    root = validate_isolated_root(restore_root, production_root)
    assets = inspect_assets(root)
    database = inspect_duckdb(root / "storage/database/zhulong_cloud_backup.duckdb")
    chroma = inspect_chroma(root / "storage/chromadb")

    technical_checks = {
        "cloud_repository_retrieved": bool(cloud_retrieved),
        "repository_integrity_check_passed": repository_check == "passed",
        "database_opened_read_only": bool(database.get("opened_read_only")),
        "database_metadata_queries_passed": bool(database.get("metadata_queries_passed")),
        "key_tables_present": bool(database.get("all_key_tables_present")),
        "chroma_present": bool(
            chroma.get("exists")
            and chroma.get("file_count", 0) > 0
            and chroma.get("sqlite_opened_read_only")
            and "sqlite_error" not in chroma
            and "embeddings_count" in chroma
        ),
        "required_assets_present": bool(assets.get("all_present")),
        "same_snapshot_baseline_supplied": baseline is not None,
    }
    baseline_comparison = None
    if baseline is not None:
        baseline_comparison = compare_table_baseline(database, baseline)
        technical_checks["baseline_key_tables_match"] = bool(
            baseline_comparison["all_tables_match"]
            and baseline_comparison["snapshot_id"] == snapshot_id
        )
    technical_passed = all(technical_checks.values())
    readiness_checks = {
        "offline_password_custody_verified": bool(offline_password_custody_verified),
    }
    if not technical_passed:
        overall_status = "FAIL"
    elif all(readiness_checks.values()):
        overall_status = "PASS"
    else:
        overall_status = "PARTIAL_KEY_CUSTODY_OPEN"
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": "isolated_restore_verification",
        "read_only": True,
        "observer_only": True,
        "no_trade_signal": True,
        "production_runtime_activated": False,
        "source": {
            "cloud_retrieved": bool(cloud_retrieved),
            "snapshot_id": snapshot_id,
            "snapshot_time": snapshot_time,
            "repository_check": repository_check,
            "password_source": password_source,
            "offline_password_custody_verified": bool(offline_password_custody_verified),
        },
        "recovery": {
            "restore_root_label": "isolated_non_production_restore",
            "wall_clock_rto_seconds": round(float(rto_seconds), 3),
            "active_recovery_seconds": round(float(active_recovery_seconds), 3),
        },
        "technical_restore_status": "PASS" if technical_passed else "FAIL",
        "dr_readiness_status": "PASS" if all(readiness_checks.values()) else "PARTIAL",
        "technical_checks": technical_checks,
        "readiness_checks": readiness_checks,
        "overall_status": overall_status,
        "database": database,
        "baseline_comparison": baseline_comparison,
        "chroma": chroma,
        "assets": assets,
        "blocked_actions": list(BLOCKED_ACTIONS),
    }


def render_markdown(report: Mapping[str, Any]) -> str:
    source = report["source"]
    recovery = report["recovery"]
    lines = [
        "# BL-022 云备份异机隔离恢复验证",
        "",
        "## 结论",
        "",
        f"- overall_status: `{report['overall_status']}`",
        f"- snapshot_id: `{source['snapshot_id']}`",
        f"- snapshot_time: `{source['snapshot_time']}`",
        f"- repository_check: `{source['repository_check']}`",
        f"- password_source: `{source['password_source']}`",
        f"- technical_restore_status: `{report['technical_restore_status']}`",
        f"- dr_readiness_status: `{report['dr_readiness_status']}`",
        f"- wall_clock_RTO: `{recovery['wall_clock_rto_seconds']}` seconds",
        f"- active_recovery: `{recovery['active_recovery_seconds']}` seconds",
        "- 模式：异机、隔离、只读；未启动生产 runtime。",
        "",
        "## 安全边界",
        "",
        "- 不启动 daemon，不调用 Tushare，不发送 PushPlus。",
        "- 不写 Shadow、RAG、nexus_audits 或生产 DuckDB。",
        "- `.env` 只校验存在性和大小，不读取、解析、哈希或输出内容。",
        "",
        "## 核心检查",
        "",
        "| 检查 | 结果 |",
        "|---|---|",
    ]
    for name, passed in report["technical_checks"].items():
        lines.append(f"| {name} | {'PASS' if passed else 'FAIL'} |")
    for name, passed in report["readiness_checks"].items():
        lines.append(f"| {name} | {'PASS' if passed else 'FAIL'} |")

    lines.extend(
        [
            "",
            "## DuckDB 对账",
            "",
            "| 表 | 行数 | 最早时间 | 最晚时间 |",
            "|---|---:|---|---|",
        ]
    )
    for table, row in report["database"].get("tables", {}).items():
        lines.append(
            f"| {table} | {row.get('row_count', '-')} | "
            f"{row.get('min_time', '-')} | {row.get('max_time', '-')} |"
        )

    chroma = report["chroma"]
    assets = report["assets"]
    baseline = report.get("baseline_comparison")
    if baseline is not None:
        lines.extend(
            [
                "",
                "## 同快照基线对账",
                "",
                f"- baseline_snapshot_id: `{baseline.get('snapshot_id', '')}`",
                f"- all_key_tables_match: `{str(bool(baseline.get('all_tables_match'))).lower()}`",
            ]
        )
    lines.extend(
        [
            "",
            "## Chroma 与配置资产",
            "",
            f"- Chroma files: `{chroma.get('file_count', 0)}`",
            f"- Chroma bytes: `{chroma.get('size_bytes', 0)}`",
            f"- Chroma embeddings: `{chroma.get('embeddings_count', 'N/A')}`",
            f"- Required assets: `{assets.get('present_count', 0)}/{assets.get('required_count', 0)}`",
            "",
            "本报告仅证明备份可恢复与关键资产可读，不授权任何生产接入。",
        ]
    )
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--restore-root", required=True, type=Path)
    parser.add_argument("--production-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--snapshot-time", required=True)
    parser.add_argument("--repository-check", required=True, choices=("passed", "failed", "not_run"))
    parser.add_argument("--cloud-retrieved", action="store_true")
    parser.add_argument(
        "--password-source",
        required=True,
        choices=("offline_custody", "production_node_export"),
    )
    parser.add_argument("--offline-password-custody-verified", action="store_true")
    parser.add_argument("--rto-seconds", required=True, type=float)
    parser.add_argument("--active-recovery-seconds", required=True, type=float)
    parser.add_argument("--output-json", required=True, type=Path)
    parser.add_argument("--output-md", required=True, type=Path)
    parser.add_argument("--baseline-json", required=True, type=Path)
    parser.add_argument(
        "--ack-isolated-restore",
        action="store_true",
        help="Confirm that restore-root is isolated from the production runtime.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.ack_isolated_restore:
        raise SystemExit("--ack-isolated-restore is required")
    baseline = None
    if args.baseline_json:
        baseline = json.loads(args.baseline_json.read_text(encoding="utf-8"))
    report = build_report(
        args.restore_root,
        production_root=args.production_root,
        snapshot_id=args.snapshot_id,
        snapshot_time=args.snapshot_time,
        repository_check=args.repository_check,
        cloud_retrieved=args.cloud_retrieved,
        rto_seconds=args.rto_seconds,
        active_recovery_seconds=args.active_recovery_seconds,
        password_source=args.password_source,
        offline_password_custody_verified=args.offline_password_custody_verified,
        baseline=baseline,
    )
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_md.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    args.output_md.write_text(render_markdown(report), encoding="utf-8")
    print(json.dumps({"status": report["overall_status"], "json": str(args.output_json), "markdown": str(args.output_md)}))
    if report["overall_status"] == "PASS":
        return 0
    return 3 if report["technical_restore_status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
