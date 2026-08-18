import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = ROOT / "tools/review_l1_simple_scorecard.py"
spec = importlib.util.spec_from_file_location("zhulong_test_l1_scorecard", TOOL_PATH)
TOOL = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = TOOL
spec.loader.exec_module(TOOL)


class L1SimpleScorecardTest(unittest.TestCase):
    @staticmethod
    def source_payload(trade_date):
        return {
            "schema_version": "l1_path_observer_report_v0.1",
            "observer_only": True,
            "no_trade_signal": True,
            "production_l1_changed": False,
            "blocked_actions": list(TOOL.REQUIRED_SOURCE_BLOCKS),
            "daily_snapshots": [{
                "trade_date": trade_date,
                "scorecard_sets": {
                    "schema_version": "l1_scorecard_teams_v0.1",
                    "legacy_l1_top50": ["000001.SZ"],
                    "path_l1_top50": ["000002.SZ"],
                },
            }],
        }

    def test_directory_discovery_filters_pre_baseline_reports(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            old = directory / "l1_path_observer_20260710.json"
            current = directory / "l1_path_observer_20260713.json"
            old.write_text(json.dumps(self.source_payload("2026-07-10")), encoding="utf-8")
            current.write_text(json.dumps(self.source_payload("2026-07-13")), encoding="utf-8")
            paths = TOOL.discover_input_paths([str(current)], str(directory))
            sources, snapshots = TOOL.load_snapshots(paths, "2026-07-13")
        self.assertEqual(len(paths), 2)
        self.assertEqual([row["trade_date"] for row in snapshots], ["2026-07-13"])
        self.assertEqual(len(sources), 1)

    def test_positive_followthrough_scores_nine(self):
        result = TOOL.score_pick({
            "return_t1_pct": 1.0,
            "return_t3_pct": 2.0,
            "return_t5_pct": 4.0,
            "market_excess_t5_pct": 3.0,
            "mfe3_pct": 5.0,
            "mae5_pct": -2.0,
        })
        self.assertEqual(result["points"], 9)
        self.assertTrue(result["followthrough_success"])

    def test_tail_loss_receives_full_penalty(self):
        result = TOOL.score_pick({
            "return_t1_pct": -1.0,
            "return_t3_pct": -4.0,
            "return_t5_pct": -11.0,
            "market_excess_t5_pct": -8.0,
            "mfe3_pct": 1.0,
            "mae5_pct": -12.0,
        })
        self.assertEqual(result["points"], -12)
        self.assertFalse(result["followthrough_success"])

    def test_failed_spike_is_separate_from_opportunity_point(self):
        result = TOOL.score_pick({
            "return_t1_pct": 2.0,
            "return_t3_pct": -1.0,
            "return_t5_pct": -2.0,
            "market_excess_t5_pct": -1.0,
            "mfe3_pct": 4.0,
            "mae5_pct": -3.0,
        })
        self.assertTrue(result["failed_spike"])
        self.assertEqual(result["breakdown"]["mfe3_at_least_3pct"], 1)
        self.assertEqual(result["breakdown"]["failed_spike"], -1)

    def test_day_winner_requires_coverage_and_margin(self):
        legacy = {"coverage": 1.0, "mean_points": 1.0}
        path = {"coverage": 1.0, "mean_points": 1.6}
        self.assertEqual(TOOL.day_winner(legacy, path), ("PATH_L1", 0.6))
        path["mean_points"] = 1.2
        self.assertEqual(TOOL.day_winner(legacy, path), ("DRAW", 0.2))
        path["coverage"] = 0.5
        self.assertEqual(TOOL.day_winner(legacy, path), ("PENDING_COVERAGE", None))


if __name__ == "__main__":
    unittest.main()
