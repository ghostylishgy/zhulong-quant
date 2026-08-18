#!/usr/bin/env python3

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "tools" / "process_l4_news_observations.py"
SPEC = importlib.util.spec_from_file_location("zhulong_test_news_observation_worker", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class _Result:
    @staticmethod
    def to_dict():
        return {
            "status": "NEWS_CLEAR",
            "risk_level": "LOW",
            "risk_score": 0,
            "hypothetical_gate": "NONE",
            "summary": "No material risk found.",
            "checked_at": "2026-06-22 23:35:00",
            "evidence": [],
            "sources_ok": ["CNINFO", "CLS", "EASTMONEY"],
            "sources_failed": {},
        }


class _Verifier:
    seen_cutoff = None

    def __init__(self, **_kwargs):
        pass

    def verify(self, symbol, stock_name, cutoff_at):
        self.__class__.seen_cutoff = cutoff_at
        return _Result()


class _NewsModule:
    NewsVerifier = _Verifier


class NewsObservationWorkerTest(unittest.TestCase):
    def test_persistence_retries_without_refetching(self):
        as_of = MODULE.parse_as_of("2026-07-13T21:00:00+08:00", "2026-07-13")
        payload = _Result.to_dict()
        with patch.object(
            MODULE,
            "persist_result",
            side_effect=[RuntimeError("temporary lock"), None],
        ) as persist, patch.object(MODULE.time, "sleep") as sleep:
            MODULE.persist_result_with_retry(
                "task", as_of, payload, attempts=2, delay_seconds=0.25
            )
        self.assertEqual(persist.call_count, 2)
        sleep.assert_called_once_with(0.25)

    def test_exhausted_persistence_keeps_task_queued_for_next_window(self):
        row = (
            "task", "600519.SH", "sample", "2026-07-13",
            "2026-07-13T21:00:00+08:00", "PASS", False, False,
        )
        with patch.object(MODULE, "ensure_schema"), patch.object(
            MODULE, "pending_count", side_effect=[1, 1]
        ), patch.object(MODULE, "fetch_queued", return_value=[row]), patch.object(
            MODULE, "load_news_module", return_value=_NewsModule
        ), patch.object(
            MODULE,
            "persist_result_with_retry",
            side_effect=MODULE.NewsPersistenceDeferred("temporary lock"),
        ), patch.object(MODULE, "persist_failure") as persist_failure, patch.object(
            MODULE, "persist_run"
        ), patch.object(MODULE, "evaluate_alerts", return_value=[]):
            stats = MODULE.run(10, scheduled_window="22:35")
        self.assertEqual(stats["completed"], 0)
        self.assertEqual(stats["failed"], 1)
        self.assertEqual(stats["pending_after"], 1)
        self.assertEqual(stats["status"], "FAILED")
        persist_failure.assert_not_called()

    def test_worker_uses_persisted_cutoff_without_touching_verdict(self):
        old_db_path = MODULE.DB_PATH
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                db_path = str(Path(tmpdir) / "news.duckdb")
                conn = duckdb.connect(db_path)
                conn.execute(
                    """
                    CREATE TABLE nexus_audits (
                        task_id VARCHAR PRIMARY KEY, symbol VARCHAR, name VARCHAR,
                        trade_date VARCHAR, status VARCHAR, l4_final_verdict VARCHAR,
                        l4_news_policy VARCHAR, l4_news_status VARCHAR,
                        l4_news_risk_level VARCHAR, l4_news_risk_score INTEGER,
                        l4_news_gate VARCHAR, l4_news_summary VARCHAR,
                        l4_news_evidence VARCHAR, l4_news_as_of VARCHAR,
                        l4_news_checked_at VARCHAR, l4_news_prompt_injected BOOLEAN,
                        l4_news_gate_applied BOOLEAN, l4_news_gate_reason VARCHAR,
                        created_at TIMESTAMP
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO nexus_audits VALUES (
                        'run_symbol', '600519.SH', 'sample', '2026-06-22',
                        'L4_DONE', 'PASS', 'OBSERVE_ONLY', 'NEWS_QUEUED',
                        'PENDING', 0, 'NONE', '', '{}',
                        '2026-06-22T21:00:00+08:00', '', FALSE, FALSE, '',
                        CURRENT_TIMESTAMP
                    )
                    """
                )
                conn.close()
                MODULE.DB_PATH = db_path
                with patch.object(MODULE, "load_news_module", return_value=_NewsModule):
                    stats = MODULE.run(10, scheduled_window="22:35")
                conn = duckdb.connect(db_path, read_only=True)
                row = conn.execute(
                    """
                    SELECT l4_final_verdict, l4_news_status, l4_news_as_of,
                           l4_news_prompt_injected, l4_news_evidence
                    FROM nexus_audits
                    """
                ).fetchone()
                run_row = conn.execute(
                    """
                    SELECT queued_before, attempted, completed, pending_after,
                           cninfo_ok, cls_ok, eastmoney_ok, gate_none,
                           counterfactual_changes, safety_violations, status
                    FROM ops_l4_news_observation_runs
                    """
                ).fetchone()
                conn.close()
                self.assertEqual(stats["queued_before"], 1)
                self.assertEqual(stats["completed"], 1)
                self.assertEqual(stats["failed"], 0)
                self.assertEqual(stats["status"], "DONE")
                self.assertEqual(row[0], "PASS")
                self.assertEqual(row[1], "NEWS_CLEAR")
                self.assertEqual(row[2], "2026-06-22T21:00:00+08:00")
                self.assertFalse(row[3])
                self.assertEqual(json.loads(row[4])["news_as_of"], row[2])
                self.assertEqual(json.loads(row[4])["counterfactual_verdict"], "PASS")
                self.assertEqual(_Verifier.seen_cutoff.isoformat(), row[2])
                self.assertEqual(run_row, (1, 1, 1, 0, 1, 1, 1, 1, 0, 0, "DONE"))
        finally:
            MODULE.DB_PATH = old_db_path

    def test_counterfactual_mapping_is_monotonic(self):
        self.assertEqual(MODULE.counterfactual_verdict("PASS", "WOULD_CAP_HOLD"), "HOLD")
        self.assertEqual(MODULE.counterfactual_verdict("HOLD", "WOULD_CAP_HOLD"), "HOLD")
        self.assertEqual(MODULE.counterfactual_verdict("PASS", "WOULD_VETO"), "VETO")
        self.assertEqual(MODULE.counterfactual_verdict("VETO", "NONE"), "VETO")

    def test_empty_window_does_not_repeat_rolling_provider_alert(self):
        stats = {
            "scheduled_window": "23:35",
            "queued_before": 0,
            "attempted": 0,
            "completed": 0,
            "failed": 0,
            "pending_after": 0,
            "cninfo_ok": 0,
            "cls_ok": 0,
            "eastmoney_ok": 0,
            "safety_violations": 0,
        }
        with patch.object(MODULE, "rolling_cninfo_availability", return_value=(5, 0.0)):
            self.assertEqual(MODULE.evaluate_alerts(stats), [])

    def test_news_alert_text_is_operator_facing_chinese(self):
        stats = {
            "scheduled_window": "22:35",
            "queued_before": 3,
            "attempted": 3,
            "completed": 0,
            "failed": 3,
            "pending_after": 0,
            "cninfo_ok": 0,
            "cls_ok": 0,
            "eastmoney_ok": 0,
            "safety_violations": 0,
        }
        with patch.object(MODULE, "rolling_cninfo_availability", return_value=(5, 0.0)):
            alerts = MODULE.evaluate_alerts(stats)
        content = MODULE.render_alert_content(stats, alerts)
        self.assertIn("三路新闻源", content)
        self.assertIn("观察模式", content)
        self.assertNotIn("worker=", content)
        self.assertNotIn("cninfo_5d_availability", content)

    def test_worker_failed_alert_does_not_include_raw_exception_text(self):
        exc = RuntimeError("low-level English traceback fragment should stay in logs")
        content = MODULE.render_worker_failed_content(exc)
        self.assertIn("新闻观察后台任务未能正常完成", content)
        self.assertIn("RuntimeError", content)
        self.assertIn("完整错误详情已写入 daemon 日志", content)
        self.assertNotIn("low-level English", content)

    def test_fatal_worker_initialization_updates_durable_run_to_failed(self):
        old_db_path = MODULE.DB_PATH
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                db_path = str(Path(tmpdir) / "news-fatal.duckdb")
                with duckdb.connect(db_path) as conn:
                    conn.execute("CREATE TABLE nexus_audits (task_id VARCHAR PRIMARY KEY)")
                MODULE.DB_PATH = db_path
                with patch.object(MODULE, "pending_count", return_value=0), patch.object(
                    MODULE, "fetch_queued", return_value=[]
                ), patch.object(
                    MODULE, "load_news_module", side_effect=RuntimeError("module init failed")
                ):
                    with self.assertRaisesRegex(RuntimeError, "module init failed"):
                        MODULE.run(10, scheduled_window="22:35")
                with duckdb.connect(db_path, read_only=True) as conn:
                    row = conn.execute(
                        "SELECT status, failed, ended_at, error_summary "
                        "FROM ops_l4_news_observation_runs"
                    ).fetchone()
                self.assertEqual(row[0], "FAILED")
                self.assertEqual(row[1], 1)
                self.assertIsNotNone(row[2])
                self.assertIn("FATAL:RuntimeError", row[3])
        finally:
            MODULE.DB_PATH = old_db_path


if __name__ == "__main__":
    unittest.main()
