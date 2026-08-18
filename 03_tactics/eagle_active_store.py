#!/usr/bin/env python3
"""Observation-only persistence for Eagle Active Path daily candidates."""

from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "storage" / "database" / "zhulong.duckdb"
TABLE_NAME = "fact_eagle_active_path_observations"

ENGINE_LIB = PROJECT_ROOT / "01_engine" / "lib"
if str(ENGINE_LIB) not in sys.path:
    sys.path.insert(0, str(ENGINE_LIB))
from db_gateway import DBGateway


def _json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _manifest_sha256(manifest: Dict[str, Any]) -> str:
    return hashlib.sha256(_json_text(manifest).encode("utf-8")).hexdigest()


def _observation_id(trade_date: str, symbol: str) -> str:
    raw = f"EAGLE_ACTIVE|{trade_date}|{symbol.upper()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def ensure_eagle_active_observation_table(conn) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            observation_id VARCHAR PRIMARY KEY,
            trade_date DATE NOT NULL,
            symbol VARCHAR NOT NULL,
            candidate_id VARCHAR NOT NULL,
            rule_version VARCHAR NOT NULL,
            schema_version VARCHAR NOT NULL,
            origin_type VARCHAR DEFAULT '',
            primary_morphology VARCHAR DEFAULT '',
            candidate_status VARCHAR DEFAULT '',
            eligibility VARCHAR DEFAULT '',
            data_quality VARCHAR DEFAULT '',
            active_window_count INTEGER DEFAULT 0,
            active_span_minutes INTEGER DEFAULT 0,
            last_observed_price DOUBLE DEFAULT 0,
            manifest_sha256 VARCHAR NOT NULL,
            source_window_file VARCHAR DEFAULT '',
            payload_json VARCHAR NOT NULL,
            observation_only BOOLEAN DEFAULT TRUE,
            no_trade_signal BOOLEAN DEFAULT TRUE,
            execution_enabled BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_eagle_active_observation_date
        ON {TABLE_NAME}(trade_date, rule_version)
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_eagle_active_observation_symbol
        ON {TABLE_NAME}(symbol, trade_date)
        """
    )


def _eligible_candidates(manifest: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    for candidate in manifest.get("candidates") or []:
        if not isinstance(candidate, dict):
            continue
        if not candidate.get("observation_eligible"):
            continue
        if str(candidate.get("data_quality") or "") != "COMPLETE":
            continue
        if not candidate.get("observation_only") or not candidate.get("no_trade_signal"):
            continue
        yield candidate


def persist_manifest(
    manifest: Dict[str, Any],
    *,
    db_path: Path | str = DB_PATH,
) -> Dict[str, Any]:
    """Idempotently upsert a COMPLETE observation-only manifest."""
    if not manifest.get("observation_only") or not manifest.get("no_trade_signal"):
        raise ValueError("EAGLE_ACTIVE_UNSAFE_MANIFEST")
    if str(manifest.get("data_quality") or "") != "COMPLETE":
        return {
            "status": "SKIPPED_MANIFEST_QUALITY",
            "data_quality": str(manifest.get("data_quality") or ""),
            "upserted": 0,
        }

    trade_date = str(manifest.get("trade_date") or "").strip()
    rule_version = str(manifest.get("rule_version") or "").strip()
    schema_version = str(manifest.get("schema_version") or "").strip()
    if len(trade_date) != 10 or not rule_version or not schema_version:
        raise ValueError("EAGLE_ACTIVE_INVALID_MANIFEST_IDENTITY")

    manifest_sha = _manifest_sha256(manifest)
    source_window_file = str(manifest.get("window_file") or "")
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for candidate in _eligible_candidates(manifest):
        symbol = str(candidate.get("symbol") or "").strip().upper()
        candidate_id = str(candidate.get("candidate_id") or "").strip()
        if not symbol or not candidate_id:
            continue
        rows.append(
            [
                _observation_id(trade_date, symbol),
                trade_date,
                symbol,
                candidate_id,
                rule_version,
                schema_version,
                str(candidate.get("origin_type") or ""),
                str(candidate.get("primary_morphology") or ""),
                str(candidate.get("candidate_status") or ""),
                str(candidate.get("eligibility") or ""),
                str(candidate.get("data_quality") or ""),
                int(candidate.get("active_window_count") or 0),
                int(candidate.get("active_span_minutes") or 0),
                float(candidate.get("last_observed_price") or 0),
                manifest_sha,
                source_window_file,
                _json_text(candidate),
                True,
                True,
                False,
                now_text,
                now_text,
            ]
        )

    db_target = Path(db_path)
    existing = set()
    stale_ids = set()
    with DBGateway(db_target, read_only=False, expected_hold_seconds=30) as conn:
        conn.execute("BEGIN TRANSACTION")
        try:
            ensure_eagle_active_observation_table(conn)
            existing = {
                str(row[0])
                for row in conn.execute(
                    f"SELECT observation_id FROM {TABLE_NAME} WHERE trade_date = CAST(? AS DATE)",
                    [trade_date],
                ).fetchall()
            }
            current_ids = {str(row[0]) for row in rows}
            stale_ids = existing - current_ids
            if stale_ids:
                conn.executemany(
                    f"DELETE FROM {TABLE_NAME} WHERE observation_id = ?",
                    [[observation_id] for observation_id in sorted(stale_ids)],
                )
            if rows:
                conn.executemany(
                    f"""
                    INSERT INTO {TABLE_NAME} (
                        observation_id, trade_date, symbol, candidate_id,
                        rule_version, schema_version, origin_type,
                        primary_morphology, candidate_status, eligibility,
                        data_quality, active_window_count, active_span_minutes,
                        last_observed_price, manifest_sha256, source_window_file,
                        payload_json, observation_only, no_trade_signal,
                        execution_enabled, created_at, updated_at
                    )
                    VALUES (?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP))
                    ON CONFLICT (observation_id) DO UPDATE SET
                        candidate_id = EXCLUDED.candidate_id,
                        rule_version = EXCLUDED.rule_version,
                        schema_version = EXCLUDED.schema_version,
                        origin_type = EXCLUDED.origin_type,
                        primary_morphology = EXCLUDED.primary_morphology,
                        candidate_status = EXCLUDED.candidate_status,
                        eligibility = EXCLUDED.eligibility,
                        data_quality = EXCLUDED.data_quality,
                        active_window_count = EXCLUDED.active_window_count,
                        active_span_minutes = EXCLUDED.active_span_minutes,
                        last_observed_price = EXCLUDED.last_observed_price,
                        manifest_sha256 = EXCLUDED.manifest_sha256,
                        source_window_file = EXCLUDED.source_window_file,
                        payload_json = EXCLUDED.payload_json,
                        observation_only = TRUE,
                        no_trade_signal = TRUE,
                        execution_enabled = FALSE,
                        updated_at = EXCLUDED.updated_at
                    """,
                    rows,
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    inserted = sum(row[0] not in existing for row in rows)
    return {
        "status": "UPSERTED",
        "trade_date": trade_date,
        "rule_version": rule_version,
        "manifest_content_sha256": manifest_sha,
        "upserted": len(rows),
        "inserted": inserted,
        "updated": len(rows) - inserted,
        "deleted_stale": len(stale_ids),
        "execution_enabled": False,
    }


__all__ = [
    "DB_PATH",
    "TABLE_NAME",
    "ensure_eagle_active_observation_table",
    "persist_manifest",
]
