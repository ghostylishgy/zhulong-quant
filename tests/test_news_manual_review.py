#!/usr/bin/env python3

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "tools" / "review_l4_news_samples.py"
SPEC = importlib.util.spec_from_file_location(
    "zhulong_test_news_manual_review", MODULE_PATH
)
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def create_database(db_path: str) -> None:
    conn = duckdb.connect(db_path)
    conn.execute(
        """
        CREATE TABLE nexus_audits (
            task_id VARCHAR PRIMARY KEY,
            trade_date VARCHAR,
            symbol VARCHAR,
            name VARCHAR,
            status VARCHAR,
            l4_final_verdict VARCHAR,
            l4_news_policy VARCHAR,
            l4_news_status VARCHAR,
            l4_news_gate VARCHAR,
            l4_news_risk_level VARCHAR,
            l4_news_risk_score INTEGER,
            l4_news_summary VARCHAR,
            l4_news_evidence VARCHAR,
            l4_news_as_of VARCHAR
        )
        """
    )
    payload = json.dumps(
        {
            "negative_tags": ["ST_DESIGNATION"],
            "sources_ok": ["CNINFO"],
            "sources_failed": {},
            "evidence": [
                {
                    "provider": "CNINFO",
                    "source_grade": "A",
                    "published_at": "2026-07-06 10:00:00",
                    "title": "sample company will receive ST designation",
                    "critical_tags": ["ST_DESIGNATION"],
                    "caution_tags": [],
                    "positive_tags": [],
                }
            ],
        }
    )
    conn.execute(
        """
        INSERT INTO nexus_audits VALUES
            ('task-1', '2026-07-06', '300716.SZ', 'sample company',
             'L4_DONE', 'PASS', 'OBSERVE_ONLY', 'NEWS_CRITICAL_CANDIDATE',
             'WOULD_VETO', 'CRITICAL_CANDIDATE', 90, 'manual confirmation',
             ?, '2026-07-06T21:00:00+08:00')
        """,
        [payload],
    )
    conn.close()


class NewsManualReviewTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tempdir.name) / "review.duckdb")
        create_database(self.db_path)
        self.rows = MODULE.load_risk_rows(
            "2026-07-01", "2026-07-10", self.db_path
        )
        self.manifest = MODULE.initialize_manifest(
            "2026-07-01", "2026-07-10", self.rows
        )

    def tearDown(self):
        self.tempdir.cleanup()

    def complete_manifest(self):
        manifest = json.loads(json.dumps(self.manifest))
        manifest["reviewer"] = "tester"
        manifest["reviewed_at"] = "2026-07-18T12:00:00+08:00"
        manifest["decisions"][0].update(
            {
                "review_label": "TRUE_RISK",
                "entity_correct": True,
                "negation_correct": True,
            }
        )
        return manifest

    def test_manifest_contains_hash_bound_risk_sample(self):
        self.assertEqual(len(self.rows), 1)
        self.assertEqual(len(self.manifest["decisions"]), 1)
        self.assertEqual(
            self.manifest["decisions"][0]["source_row_sha256"],
            self.rows[0]["source_row_sha256"],
        )
        self.assertEqual(self.rows[0]["news_gate"], "WOULD_VETO")

    def test_pending_manifest_can_be_previewed_without_reviewer(self):
        decisions = MODULE.validate_manifest(self.manifest, self.rows)
        result = MODULE.apply_reviews(
            self.manifest, decisions, self.db_path, write_reviews=False
        )
        self.assertEqual(result["validated_reviews"], 0)
        self.assertEqual(result["written_reviews"], 0)

    def test_dry_run_validates_without_writing(self):
        manifest = self.complete_manifest()
        decisions = MODULE.validate_manifest(manifest, self.rows)
        result = MODULE.apply_reviews(
            manifest, decisions, self.db_path, write_reviews=False,
            manifest_sha256="a" * 64,
        )
        self.assertEqual(result["validated_reviews"], 1)
        conn = duckdb.connect(self.db_path, read_only=True)
        table_count = conn.execute(
            """
            SELECT count(*) FROM information_schema.tables
            WHERE table_name = 'ops_l4_news_manual_reviews'
            """
        ).fetchone()[0]
        conn.close()
        self.assertEqual(table_count, 0)

    def test_explicit_write_persists_review_only(self):
        manifest = self.complete_manifest()
        decisions = MODULE.validate_manifest(manifest, self.rows)
        result = MODULE.apply_reviews(
            manifest, decisions, self.db_path, write_reviews=True,
            manifest_sha256="b" * 64,
        )
        self.assertEqual(result["written_reviews"], 1)
        conn = duckdb.connect(self.db_path, read_only=True)
        review = conn.execute(
            """
            SELECT review_label, entity_correct, negation_correct, reviewer,
                   source_row_sha256, source_rows_sha256,
                   manifest_version, manifest_sha256
            FROM ops_l4_news_manual_reviews
            """
        ).fetchone()
        verdict = conn.execute(
            """
            SELECT l4_final_verdict, l4_news_policy
            FROM nexus_audits WHERE task_id = 'task-1'
            """
        ).fetchone()
        conn.close()
        self.assertEqual(review[:4], ("TRUE_RISK", True, True, "tester"))
        self.assertEqual(review[4], self.rows[0]["source_row_sha256"])
        self.assertEqual(
            review[5], manifest["source_rows_sha256"]
        )
        self.assertEqual(review[6], MODULE.MANIFEST_VERSION)
        self.assertEqual(review[7], "b" * 64)
        self.assertEqual(verdict, ("PASS", "OBSERVE_ONLY"))

        with self.assertRaisesRegex(ValueError, "immutable"):
            MODULE.apply_reviews(
                manifest, decisions, self.db_path, write_reviews=True,
                manifest_sha256="c" * 64,
            )

    def test_source_change_fails_closed(self):
        manifest = self.complete_manifest()
        conn = duckdb.connect(self.db_path)
        conn.execute(
            """
            UPDATE nexus_audits SET l4_news_summary = 'changed'
            WHERE task_id = 'task-1'
            """
        )
        conn.close()
        changed = MODULE.load_risk_rows(
            "2026-07-01", "2026-07-10", self.db_path
        )
        with self.assertRaisesRegex(ValueError, "source risk rows changed"):
            MODULE.validate_manifest(manifest, changed)

    def test_false_positive_requires_notes(self):
        manifest = self.complete_manifest()
        manifest["decisions"][0].update(
            {
                "review_label": "FALSE_POSITIVE",
                "entity_correct": False,
                "negation_correct": True,
                "notes": "",
            }
        )
        with self.assertRaisesRegex(ValueError, "review notes required"):
            MODULE.validate_manifest(manifest, self.rows)

    def test_material_false_veto_rejected_for_caution_gate(self):
        rows = json.loads(json.dumps(self.rows))
        rows[0]["news_gate"] = "WOULD_CAP_HOLD"
        rows[0]["source_row_sha256"] = MODULE.canonical_sha256(
            {
                key: value
                for key, value in rows[0].items()
                if key != "source_row_sha256"
            }
        )
        manifest = MODULE.initialize_manifest(
            "2026-07-01", "2026-07-10", rows
        )
        manifest["reviewer"] = "tester"
        manifest["reviewed_at"] = "2026-07-18T12:00:00+08:00"
        manifest["decisions"][0].update(
            {
                "review_label": "FALSE_POSITIVE",
                "entity_correct": False,
                "negation_correct": True,
                "material_false_veto": True,
                "notes": "not a veto sample",
            }
        )
        with self.assertRaisesRegex(ValueError, "only valid for WOULD_VETO"):
            MODULE.validate_manifest(manifest, rows)


if __name__ == "__main__":
    unittest.main()
