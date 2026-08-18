#!/usr/bin/env python3

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from tests import _test_log_isolation  # noqa: F401

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "tools" / "quarantine_rag_weak_rps.py"
SPEC = importlib.util.spec_from_file_location("zhulong_test_rag_quarantine", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class RagWeakRpsQuarantineTest(unittest.TestCase):
    @staticmethod
    def create_db(path):
        conn = duckdb.connect(str(path))
        conn.execute(
            """
            CREATE TABLE fact_strategic_memory (
                symbol VARCHAR, trade_date DATE, source VARCHAR,
                facts_json VARCHAR, narrative_text VARCHAR, ssd_tags VARCHAR,
                l4_verdict VARCHAR, l4_score INTEGER, embedding_status VARCHAR,
                created_at TIMESTAMP, enrichment_status VARCHAR,
                enrichment_attempts INTEGER, enrichment_error VARCHAR,
                enrichment_model VARCHAR, enriched_at TIMESTAMP,
                UNIQUE(symbol, trade_date, source)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE fact_rps_results (
                symbol VARCHAR, trade_date DATE, rps_10 DOUBLE
            )
            """
        )
        rows = [
            (
                "000001.SZ", "2026-07-01", "audit",
                json.dumps({"decision_tags": ["#weak_rps"]}),
                "wrong weak_rps narrative", "", "HOLD", 50, "embedded",
                "2026-07-01 19:00:00", "ENRICHED", 0, "", "legacy",
                "2026-07-01 19:00:00",
            ),
            (
                "000002.SZ", "2026-07-01", "audit",
                json.dumps({"decision_tags": ["#weak_rps"]}),
                "valid weak narrative", "", "HOLD", 50, "embedded",
                "2026-07-01 19:00:00", "ENRICHED", 0, "", "legacy",
                "2026-07-01 19:00:00",
            ),
        ]
        conn.executemany(
            "INSERT INTO fact_strategic_memory VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.executemany(
            "INSERT INTO fact_rps_results VALUES (?, ?, ?)",
            [
                ("000001.SZ", "2026-07-01", 88.0),
                ("000002.SZ", "2026-07-01", 35.0),
            ],
        )
        conn.close()

    def test_manifest_selects_only_confirmed_wrong_weak_rps_and_applies(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            db_path = root / "test.duckdb"
            manifest_path = root / "manifest.json"
            self.create_db(db_path)
            with patch.object(MODULE, "_vector_ids_by_key", return_value=({}, [])):
                manifest = MODULE.build_manifest(
                    db_path,
                    root / "chroma",
                    "2026-07-01",
                    "2026-07-31",
                )
            self.assertEqual(manifest["candidate_count"], 1)
            self.assertEqual(manifest["records"][0]["memory_key"], "000001.SZ|2026-07-01|audit")
            manifest_sha = MODULE.write_manifest(manifest, manifest_path)
            result = MODULE.apply_manifest(
                manifest_path,
                manifest_sha,
                db_path,
                root / "chroma",
                require_daemon_stopped=False,
            )
            self.assertEqual(result["database_rows_quarantined"], 1)
            conn = duckdb.connect(str(db_path), read_only=True)
            rows = conn.execute(
                "SELECT symbol, enrichment_status, embedding_status FROM fact_strategic_memory ORDER BY symbol"
            ).fetchall()
            conn.close()
            self.assertEqual(rows[0], ("000001.SZ", "QUARANTINED_SEMANTIC", "quarantined"))
            self.assertEqual(rows[1], ("000002.SZ", "ENRICHED", "embedded"))

    def test_apply_rejects_manifest_after_source_row_changes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            db_path = root / "test.duckdb"
            manifest_path = root / "manifest.json"
            self.create_db(db_path)
            with patch.object(MODULE, "_vector_ids_by_key", return_value=({}, [])):
                manifest = MODULE.build_manifest(
                    db_path,
                    root / "chroma",
                    "2026-07-01",
                    "2026-07-31",
                )
            manifest_sha = MODULE.write_manifest(manifest, manifest_path)
            conn = duckdb.connect(str(db_path))
            conn.execute(
                "UPDATE fact_strategic_memory SET narrative_text='changed' WHERE symbol='000001.SZ'"
            )
            conn.close()
            with self.assertRaisesRegex(RuntimeError, "manifest_precondition_failed"):
                MODULE.apply_manifest(
                    manifest_path,
                    manifest_sha,
                    db_path,
                    root / "chroma",
                    require_daemon_stopped=False,
                )


if __name__ == "__main__":
    unittest.main()
