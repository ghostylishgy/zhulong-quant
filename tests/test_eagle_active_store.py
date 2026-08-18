import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "03_tactics" / "eagle_active_store.py"
spec = importlib.util.spec_from_file_location("zhulong_test_eagle_active_store", MODULE_PATH)
MOD = importlib.util.module_from_spec(spec)
spec.loader.exec_module(MOD)


def manifest():
    return {
        "schema_version": "eagle_active_path_v0.4",
        "rule_version": "eagle_active_rules_v0.4",
        "trade_date": "2026-08-03",
        "data_quality": "COMPLETE",
        "observation_only": True,
        "no_trade_signal": True,
        "window_file": "/tmp/eagle_windows_2026-08-03.jsonl",
        "candidates": [
            {
                "candidate_id": "EAGLE-20260803-000001.SZ",
                "symbol": "000001.SZ",
                "origin_type": "SECTOR_RESONANCE",
                "primary_morphology": "TREND_CONTINUATION",
                "candidate_status": "PERSISTENT_ACTIVITY",
                "eligibility": "OBSERVE_ONLY_A_SHARE",
                "data_quality": "COMPLETE",
                "observation_eligible": True,
                "observation_only": True,
                "no_trade_signal": True,
                "active_window_count": 3,
                "active_span_minutes": 40,
                "last_observed_price": 10.5,
            }
        ],
    }


class EagleActiveStoreTest(unittest.TestCase):
    def test_complete_manifest_is_idempotent_and_execution_disabled(self):
        with tempfile.TemporaryDirectory() as temp:
            db_path = Path(temp) / "observations.duckdb"
            payload = manifest()

            first = MOD.persist_manifest(payload, db_path=db_path)
            payload["candidates"][0]["active_window_count"] = 4
            second = MOD.persist_manifest(payload, db_path=db_path)

            self.assertEqual(first["inserted"], 1)
            self.assertEqual(second["updated"], 1)
            con = duckdb.connect(str(db_path), read_only=True)
            row = con.execute(
                f"""
                SELECT COUNT(*), MAX(active_window_count),
                       BOOL_AND(observation_only), BOOL_AND(no_trade_signal),
                       BOOL_OR(execution_enabled), MAX(payload_json)
                FROM {MOD.TABLE_NAME}
                """
            ).fetchone()
            con.close()
            self.assertEqual(row[0], 1)
            self.assertEqual(row[1], 4)
            self.assertTrue(row[2])
            self.assertTrue(row[3])
            self.assertFalse(row[4])
            self.assertEqual(json.loads(row[5])["symbol"], "000001.SZ")

    def test_reprocessing_deletes_rows_no_longer_in_manifest(self):
        with tempfile.TemporaryDirectory() as temp:
            db_path = Path(temp) / "observations.duckdb"
            payload = manifest()
            MOD.persist_manifest(payload, db_path=db_path)
            payload["candidates"] = []

            result = MOD.persist_manifest(payload, db_path=db_path)

            self.assertEqual(result["deleted_stale"], 1)
            con = duckdb.connect(str(db_path), read_only=True)
            count = con.execute(f"SELECT COUNT(*) FROM {MOD.TABLE_NAME}").fetchone()[0]
            con.close()
            self.assertEqual(count, 0)

    def test_partial_manifest_is_not_written(self):
        with tempfile.TemporaryDirectory() as temp:
            payload = manifest()
            payload["data_quality"] = "PARTIAL_LOW_COVERAGE"
            result = MOD.persist_manifest(
                payload,
                db_path=Path(temp) / "observations.duckdb",
            )
            self.assertEqual(result["status"], "SKIPPED_MANIFEST_QUALITY")
            self.assertEqual(result["upserted"], 0)

    def test_unsafe_manifest_is_rejected(self):
        payload = manifest()
        payload["no_trade_signal"] = False
        with self.assertRaisesRegex(ValueError, "EAGLE_ACTIVE_UNSAFE_MANIFEST"):
            MOD.persist_manifest(payload)


if __name__ == "__main__":
    unittest.main()
