#!/usr/bin/env python3

import importlib.util
import json
import os
import sys
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

import duckdb

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "tools" / "review_l4_news_observation.py"
SPEC = importlib.util.spec_from_file_location("zhulong_test_news_observation_review", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def create_nexus_table(db_path: str) -> None:
    conn = duckdb.connect(db_path)
    conn.execute(
        """
        CREATE TABLE nexus_audits (
            task_id VARCHAR PRIMARY KEY,
            trade_date VARCHAR,
            status VARCHAR,
            l4_news_policy VARCHAR,
            l4_news_status VARCHAR,
            l4_news_gate VARCHAR,
            l4_news_evidence VARCHAR,
            l4_news_prompt_injected BOOLEAN,
            l4_news_gate_applied BOOLEAN
        )
        """
    )
    conn.close()


class NewsObservationReviewTest(unittest.TestCase):
    def test_gate_reports_ready_but_never_changes_policy(self):
        old_db_path = MODULE.DB_PATH
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                db_path = str(Path(tmpdir) / "review.duckdb")
                create_nexus_table(db_path)
                MODULE.DB_PATH = db_path
                MODULE.ensure_review_schema()
                payload = json.dumps({
                    "sources_ok": ["CNINFO", "CLS", "EASTMONEY"],
                    "sources_failed": {},
                })
                rows = []
                start = date(2026, 6, 22)
                risk_tasks = []
                for day_index in range(15):
                    trade_date = (start + timedelta(days=day_index)).isoformat()
                    for item_index in range(7):
                        task_id = f"task_{day_index}_{item_index}"
                        is_risk = len(risk_tasks) < 20
                        gate = "WOULD_CAP_HOLD" if is_risk else "NONE"
                        if is_risk:
                            risk_tasks.append(task_id)
                        rows.append([
                            task_id, trade_date, "L4_DONE", "OBSERVE_ONLY",
                            "NEWS_CLEAR", gate, payload, False, False,
                        ])
                conn = duckdb.connect(db_path)
                conn.executemany(
                    """
                    INSERT INTO nexus_audits
                        (task_id, trade_date, status, l4_news_policy,
                         l4_news_status, l4_news_gate, l4_news_evidence,
                         l4_news_prompt_injected, l4_news_gate_applied)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
                conn.executemany(
                    """
                    INSERT INTO ops_l4_news_manual_reviews
                        (task_id, source_row_sha256, source_rows_sha256,
                         manifest_version, manifest_sha256, review_label,
                         entity_correct, negation_correct,
                         material_false_veto, reviewer, notes)
                    VALUES (?, ?, ?, ?, ?, 'TRUE_RISK',
                            TRUE, TRUE, FALSE, 'tester', '')
                    """,
                    [
                        [
                            task_id, "a" * 64, "b" * 64,
                            "l4_news_manual_review_manifest_v0.1", "c" * 64,
                        ]
                        for task_id in risk_tasks
                    ],
                )
                conn.execute(
                    """
                    UPDATE ops_l4_news_control_state
                    SET control_value = '1', updated_at = CURRENT_TIMESTAMP
                    WHERE control_key = 'sentence_entity_hardening_complete'
                    """
                )
                conn.close()
                with patch.dict(os.environ, {"L4_NEWS_POLICY": "OBSERVE_ONLY"}):
                    report = MODULE.run("2026-06-22", "2026-07-06")
                self.assertEqual(report["decision"], "READY_FOR_DESIGN_REVIEW")
                self.assertTrue(report["policy_unchanged"])
                self.assertEqual(report["metrics"]["eligible_candidates"], 105)
                self.assertEqual(report["metrics"]["risk_events"], 20)
                conn = duckdb.connect(db_path, read_only=True)
                persisted = conn.execute(
                    "SELECT decision FROM ops_l4_news_graduation_reviews"
                ).fetchone()[0]
                conn.close()
                self.assertEqual(persisted, "READY_FOR_DESIGN_REVIEW")
        finally:
            MODULE.DB_PATH = old_db_path

    def test_empty_window_is_not_ready(self):
        old_db_path = MODULE.DB_PATH
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                db_path = str(Path(tmpdir) / "empty.duckdb")
                create_nexus_table(db_path)
                MODULE.DB_PATH = db_path
                with patch.dict(os.environ, {"L4_NEWS_POLICY": "OBSERVE_ONLY"}):
                    report = MODULE.run("2026-06-22", "2026-06-22")
                self.assertEqual(report["decision"], "NOT_READY")
                self.assertFalse(report["conditions"]["eligible_candidates"]["passed"])
        finally:
            MODULE.DB_PATH = old_db_path


if __name__ == "__main__":
    unittest.main()
