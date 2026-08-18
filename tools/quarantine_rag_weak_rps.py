#!/usr/bin/env python3
"""SHA-bound quarantine for confirmed weak-RPS semantic-memory defects."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from config.rag_contract import COLLECTION_NAME

DEFAULT_DB = PROJECT_ROOT / "storage" / "database" / "zhulong.duckdb"
DEFAULT_CHROMA = PROJECT_ROOT / "storage" / "chromadb"
DEFAULT_REPORT_DIR = PROJECT_ROOT / "storage" / "reports" / "rag_quarantine"
QUARANTINE_STATUS = "QUARANTINED_SEMANTIC"
TOOL_VERSION = "rag_weak_rps_quarantine_v1"


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        + "\n"
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _row_payload(row: Iterable[Any]) -> Dict[str, Any]:
    values = list(row)
    keys = (
        "symbol",
        "trade_date",
        "source",
        "facts_json",
        "narrative_text",
        "ssd_tags",
        "l4_verdict",
        "l4_score",
        "embedding_status",
        "created_at",
        "enrichment_status",
        "enrichment_attempts",
        "enrichment_error",
        "enrichment_model",
        "enriched_at",
    )
    payload = dict(zip(keys, values))
    for key in ("trade_date", "created_at", "enriched_at"):
        if payload.get(key) is not None:
            payload[key] = str(payload[key])
    return payload


def _row_sha256(payload: Dict[str, Any]) -> str:
    return _sha256_bytes(_json_bytes(payload))


def _decision_tags(facts_json: str) -> List[str]:
    try:
        payload = json.loads(str(facts_json or "{}"))
    except (TypeError, ValueError):
        return []
    tags = payload.get("decision_tags") or []
    if isinstance(tags, str):
        tags = [item.strip() for item in tags.replace(";", ",").split(",")]
    if not isinstance(tags, list):
        return []
    return [str(item or "").strip() for item in tags if str(item or "").strip()]


def _signal_trade_date(source: str, memory_date: str, facts_json: str) -> str:
    if str(source or "") == "audit":
        return memory_date
    try:
        payload = json.loads(str(facts_json or "{}"))
    except (TypeError, ValueError):
        return memory_date
    return str(payload.get("signal_trade_date") or memory_date)


def _memory_rows(conn: duckdb.DuckDBPyConnection, from_date: str, to_date: str):
    return conn.execute(
        """
        SELECT symbol,
               CAST(trade_date AS VARCHAR),
               source,
               facts_json,
               narrative_text,
               ssd_tags,
               l4_verdict,
               l4_score,
               embedding_status,
               CAST(created_at AS VARCHAR),
               enrichment_status,
               enrichment_attempts,
               enrichment_error,
               enrichment_model,
               CAST(enriched_at AS VARCHAR)
        FROM fact_strategic_memory
        WHERE trade_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
        ORDER BY trade_date, symbol, source
        """,
        [from_date, to_date],
    ).fetchall()


def _vector_ids_by_key(chroma_dir: Path) -> Tuple[Dict[Tuple[str, str, str], List[str]], List[str]]:
    warnings: List[str] = []
    try:
        import chromadb
        from chromadb.config import Settings

        client = chromadb.PersistentClient(
            path=str(chroma_dir),
            settings=Settings(anonymized_telemetry=False),
        )
        collection = client.get_collection(COLLECTION_NAME)
        payload = collection.get(include=["metadatas"])
        grouped: Dict[Tuple[str, str, str], List[str]] = {}
        ids = payload.get("ids") or []
        metadatas = payload.get("metadatas") or []
        for vector_id, metadata in zip(ids, metadatas):
            metadata = metadata or {}
            key = (
                str(metadata.get("symbol") or ""),
                str(metadata.get("trade_date") or ""),
                str(metadata.get("source") or ""),
            )
            grouped.setdefault(key, []).append(str(vector_id))
        return grouped, warnings
    except Exception as exc:
        warnings.append(f"chroma_inventory_unavailable:{type(exc).__name__}:{exc}")
        return {}, warnings


def build_manifest(
    db_path: Path,
    chroma_dir: Path,
    from_date: str,
    to_date: str,
) -> Dict[str, Any]:
    vectors_by_key, warnings = _vector_ids_by_key(chroma_dir)
    records: List[Dict[str, Any]] = []
    with duckdb.connect(str(db_path), read_only=True) as conn:
        for raw_row in _memory_rows(conn, from_date, to_date):
            payload = _row_payload(raw_row)
            tags = _decision_tags(payload["facts_json"])
            if not any(tag.lower() == "#weak_rps" for tag in tags):
                continue
            rps_date = _signal_trade_date(
                payload["source"],
                payload["trade_date"],
                payload["facts_json"],
            )
            rps_row = conn.execute(
                """
                SELECT rps_10
                FROM fact_rps_results
                WHERE symbol = ? AND trade_date = CAST(? AS DATE)
                """,
                [payload["symbol"], rps_date],
            ).fetchone()
            if not rps_row or rps_row[0] is None:
                warnings.append(
                    f"missing_rps:{payload['symbol']}:{payload['trade_date']}:{payload['source']}"
                )
                continue
            actual_rps = float(rps_row[0])
            if actual_rps < 50.0:
                continue
            key = (
                payload["symbol"],
                payload["trade_date"],
                payload["source"],
            )
            records.append(
                {
                    "memory_key": "|".join(key),
                    "row_sha256": _row_sha256(payload),
                    "rps_trade_date": rps_date,
                    "actual_rps_10": actual_rps,
                    "decision_tags": tags,
                    "narrative_contains_weak_rps": (
                        "weak_rps" in str(payload["narrative_text"] or "").lower()
                    ),
                    "vector_ids": sorted(vectors_by_key.get(key, [])),
                    "original": payload,
                }
            )

    source_counts: Dict[str, int] = {}
    for record in records:
        source = record["original"]["source"]
        source_counts[source] = source_counts.get(source, 0) + 1
    return {
        "tool_version": TOOL_VERSION,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "mode": "dry_run",
        "no_trade_signal": True,
        "database": str(db_path),
        "chroma_dir": str(chroma_dir),
        "criteria": {
            "from_date": from_date,
            "to_date": to_date,
            "decision_tag": "#weak_rps",
            "confirmed_actual_rps_10_min": 50.0,
            "strategy_rps_date_field": "facts_json.signal_trade_date",
        },
        "blocked_actions": [
            "delete_memory_row",
            "rewrite_l4_verdict",
            "write_shadow",
            "write_nexus_audits",
            "trigger_daemon",
        ],
        "candidate_count": len(records),
        "narrative_defect_count": sum(
            bool(record["narrative_contains_weak_rps"]) for record in records
        ),
        "vector_count": sum(len(record["vector_ids"]) for record in records),
        "source_counts": source_counts,
        "warnings": sorted(set(warnings)),
        "records": records,
    }


def write_manifest(manifest: Dict[str, Any], output_path: Path) -> str:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = _json_bytes(manifest)
    output_path.write_bytes(payload)
    return _sha256_bytes(payload)


def _daemon_is_running() -> bool:
    pid_path = Path("/tmp/zhulong.pid")
    try:
        pid = int(pid_path.read_text(encoding="utf-8").strip())
        os.kill(pid, 0)
        return True
    except (FileNotFoundError, ProcessLookupError, ValueError, PermissionError):
        return False


def _read_current_payload(
    conn: duckdb.DuckDBPyConnection,
    symbol: str,
    trade_date: str,
    source: str,
) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """
        SELECT symbol,
               CAST(trade_date AS VARCHAR),
               source,
               facts_json,
               narrative_text,
               ssd_tags,
               l4_verdict,
               l4_score,
               embedding_status,
               CAST(created_at AS VARCHAR),
               enrichment_status,
               enrichment_attempts,
               enrichment_error,
               enrichment_model,
               CAST(enriched_at AS VARCHAR)
        FROM fact_strategic_memory
        WHERE symbol = ? AND trade_date = CAST(? AS DATE) AND source = ?
        """,
        [symbol, trade_date, source],
    ).fetchone()
    return _row_payload(row) if row else None


def apply_manifest(
    manifest_path: Path,
    expected_sha256: str,
    db_path: Path,
    chroma_dir: Path,
    require_daemon_stopped: bool = True,
) -> Dict[str, Any]:
    if require_daemon_stopped and _daemon_is_running():
        raise RuntimeError("daemon_is_running: stop zhulong-daemon.service before apply")
    payload_bytes = manifest_path.read_bytes()
    actual_manifest_sha = _sha256_bytes(payload_bytes)
    if actual_manifest_sha != str(expected_sha256 or "").lower():
        raise ValueError(
            f"manifest_sha256_mismatch expected={expected_sha256} actual={actual_manifest_sha}"
        )
    manifest = json.loads(payload_bytes.decode("utf-8"))
    records = manifest.get("records") or []
    if manifest.get("tool_version") != TOOL_VERSION:
        raise ValueError("unsupported_manifest_version")
    if int(manifest.get("candidate_count", -1)) != len(records):
        raise ValueError("manifest_candidate_count_mismatch")

    mismatches: List[str] = []
    with duckdb.connect(str(db_path), read_only=False) as conn:
        for record in records:
            original = record["original"]
            current = _read_current_payload(
                conn,
                original["symbol"],
                original["trade_date"],
                original["source"],
            )
            if current is None or _row_sha256(current) != record["row_sha256"]:
                mismatches.append(record["memory_key"])
                continue
            rps_row = conn.execute(
                """
                SELECT rps_10
                FROM fact_rps_results
                WHERE symbol = ? AND trade_date = CAST(? AS DATE)
                """,
                [original["symbol"], record["rps_trade_date"]],
            ).fetchone()
            current_rps = float(rps_row[0]) if rps_row and rps_row[0] is not None else None
            if current_rps is None or current_rps < 50.0:
                mismatches.append(record["memory_key"] + ":rps")
                continue
            if abs(current_rps - float(record["actual_rps_10"])) > 1e-9:
                mismatches.append(record["memory_key"] + ":rps_changed")
        if mismatches:
            raise RuntimeError("manifest_precondition_failed:" + ",".join(mismatches[:10]))

        conn.execute("BEGIN TRANSACTION")
        try:
            for record in records:
                original = record["original"]
                conn.execute(
                    """
                    UPDATE fact_strategic_memory
                    SET enrichment_status = ?,
                        embedding_status = 'quarantined',
                        enrichment_error = ?
                    WHERE symbol = ?
                      AND trade_date = CAST(? AS DATE)
                      AND source = ?
                    """,
                    [
                        QUARANTINE_STATUS,
                        f"confirmed_wrong_weak_rps:{actual_manifest_sha}",
                        original["symbol"],
                        original["trade_date"],
                        original["source"],
                    ],
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

        verified = 0
        for record in records:
            original = record["original"]
            row = conn.execute(
                """
                SELECT enrichment_status, embedding_status
                FROM fact_strategic_memory
                WHERE symbol = ? AND trade_date = CAST(? AS DATE) AND source = ?
                """,
                [original["symbol"], original["trade_date"], original["source"]],
            ).fetchone()
            if row and row[0] == QUARANTINE_STATUS and row[1] == "quarantined":
                verified += 1
        if verified != len(records):
            raise RuntimeError(f"database_postcondition_failed:{verified}/{len(records)}")

    vector_ids = sorted(
        {
            str(vector_id)
            for record in records
            for vector_id in (record.get("vector_ids") or [])
            if str(vector_id)
        }
    )
    vector_delete_error = ""
    vectors_remaining = 0
    if vector_ids:
        try:
            import chromadb
            from chromadb.config import Settings

            client = chromadb.PersistentClient(
                path=str(chroma_dir),
                settings=Settings(anonymized_telemetry=False),
            )
            collection = client.get_collection(COLLECTION_NAME)
            collection.delete(ids=vector_ids)
            remaining = collection.get(ids=vector_ids, include=["metadatas"])
            vectors_remaining = len(remaining.get("ids") or [])
        except Exception as exc:
            vector_delete_error = f"{type(exc).__name__}:{exc}"
            vectors_remaining = len(vector_ids)

    result = {
        "tool_version": TOOL_VERSION,
        "applied_at": datetime.now().isoformat(timespec="seconds"),
        "manifest": str(manifest_path),
        "manifest_sha256": actual_manifest_sha,
        "database_rows_quarantined": len(records),
        "database_rows_verified": len(records),
        "vector_ids_requested": len(vector_ids),
        "vectors_remaining": vectors_remaining,
        "vector_delete_error": vector_delete_error,
        "reader_fail_closed": True,
    }
    report_path = manifest_path.with_suffix(".applied.json")
    write_manifest(result, report_path)
    if vectors_remaining:
        raise RuntimeError(
            f"vector_cleanup_incomplete:{vectors_remaining}; database quarantine is active"
        )
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--chroma-dir", type=Path, default=DEFAULT_CHROMA)
    parser.add_argument("--from-date", default="2026-07-01")
    parser.add_argument("--to-date", default="2026-07-31")
    parser.add_argument("--manifest-out", type=Path)
    parser.add_argument("--apply-manifest", type=Path)
    parser.add_argument("--manifest-sha256", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.apply_manifest:
        if not args.manifest_sha256:
            raise SystemExit("--manifest-sha256 is required with --apply-manifest")
        result = apply_manifest(
            args.apply_manifest,
            args.manifest_sha256,
            args.db,
            args.chroma_dir,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    output = args.manifest_out
    if output is None:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output = DEFAULT_REPORT_DIR / f"rag_weak_rps_quarantine_{stamp}.json"
    manifest = build_manifest(args.db, args.chroma_dir, args.from_date, args.to_date)
    manifest_sha = write_manifest(manifest, output)
    print(
        json.dumps(
            {
                "manifest": str(output),
                "manifest_sha256": manifest_sha,
                "candidate_count": manifest["candidate_count"],
                "narrative_defect_count": manifest["narrative_defect_count"],
                "vector_count": manifest["vector_count"],
                "warnings": manifest["warnings"],
                "mode": "dry_run",
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
