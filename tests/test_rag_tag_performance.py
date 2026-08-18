#!/usr/bin/env python3

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "04_governance" / "lib" / "weight_engine.py"
spec = importlib.util.spec_from_file_location("zhulong_test_weight_engine", MODULE_PATH)
MOD = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = MOD
spec.loader.exec_module(MOD)


def _facts(task_id, horizon, action, outcome, tags):
    return json.dumps({
        "action": action,
        "horizon_days": horizon,
        "outcome_label": outcome,
        "decision_tags": tags,
        "strategy_evidence": {"signal_task_id": task_id},
    })


class RagTagPerformanceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        MOD.DB_PATH = str(Path(self.tmp.name) / "tag_perf.duckdb")
        with duckdb.connect(MOD.DB_PATH) as conn:
            conn.execute(
                "CREATE TABLE fact_strategic_memory "
                "(source VARCHAR, tide_status VARCHAR, facts_json VARCHAR)"
            )

    def tearDown(self):
        self.tmp.cleanup()

    def _insert(self, *facts_rows):
        with duckdb.connect(MOD.DB_PATH) as conn:
            conn.executemany(
                "INSERT INTO fact_strategic_memory VALUES ('strategy:test', 'SIDE', ?)",
                [(row,) for row in facts_rows],
            )

    def test_counts_one_fixed_horizon_buy_per_task(self):
        self._insert(
            _facts("task-1", 1, "L4_PASS_T1_BUY", "WIN", "#a,#b"),
            _facts("task-1", 3, "L4_PASS_T1_BUY", "WIN", "#a,#b"),
            _facts("task-1", 5, "L4_PASS_T1_BUY", "LOSS", "#a,#b"),
            _facts("task-1", 3, "L4_PASS_T1_BUY", "WIN", "#a,#b"),
            _facts("task-1", 3, "WRONG_PICK_STOP", "LOSS", "#a,#b"),
            _facts("task-2", 3, "L4_PASS_T1_BUY", "LOSS", "#a"),
        )

        result = MOD.calc_tag_success_rate(
            min_sample=1,
            prior_strength=0,
            dry_run=True,
        )

        self.assertEqual(result["fixed_horizon_days"], 3)
        self.assertEqual(result["eligible_events"], 3)
        self.assertEqual(result["independent_decisions"], 2)
        self.assertEqual(result["duplicate_events"], 1)
        self.assertEqual(result["metric_version"], "v3")
        all_rows = {
            row["tag"]: row
            for row in result["rows"]
            if row["regime"] == "ALL"
        }
        self.assertEqual(all_rows["#a"]["total_count"], 2)
        self.assertEqual(all_rows["#a"]["success_count"], 1)
        self.assertEqual(all_rows["#b"]["total_count"], 1)

    def test_conflicting_duplicate_task_is_excluded(self):
        self._insert(
            _facts("task-1", 3, "L4_PASS_BUY", "WIN", "#a"),
            _facts("task-1", 3, "L4_PASS_BUY", "LOSS", "#a"),
        )

        result = MOD.calc_tag_success_rate(
            min_sample=1,
            prior_strength=0,
            dry_run=True,
        )

        self.assertEqual(result["conflicted_tasks"], 1)
        self.assertEqual(result["independent_decisions"], 0)
        self.assertEqual(result["generated_rows"], 0)


if __name__ == "__main__":
    unittest.main()
