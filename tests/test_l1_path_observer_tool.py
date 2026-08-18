import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = ROOT / "tools/observe_l1_paths.py"
spec = importlib.util.spec_from_file_location("zhulong_test_l1_path_tool", TOOL_PATH)
TOOL = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = TOOL
spec.loader.exec_module(TOOL)


def candidate(symbol, path):
    return {
        "symbol": symbol,
        "name": symbol,
        "industry": "test",
        "selected_by_new_l1": True,
        "selected_by_old_l1_production_proxy": False,
        "account_context": {"allow_execution": True},
        "features": {"rps_10": 80.0},
        "classification": {"primary_path": path},
    }


class L1PathObserverToolTest(unittest.TestCase):
    def test_frozen_team_signature_rejects_changed_team(self):
        existing = {
            "schema_version": "l1_path_observer_report_v0.1",
            "daily_snapshots": [{
                "trade_date": "2026-07-13",
                "scorecard_sets": {"legacy_l1_top50": ["000001.SZ"]},
            }],
        }
        same = {
            **existing,
            "generated_at": "later",
        }
        TOOL.verify_frozen_teams(existing, same)
        changed = {
            **existing,
            "daily_snapshots": [{
                "trade_date": "2026-07-13",
                "scorecard_sets": {"legacy_l1_top50": ["000002.SZ"]},
            }],
        }
        with self.assertRaisesRegex(ValueError, "refusing point-in-time overwrite"):
            TOOL.verify_frozen_teams(existing, changed)

    def test_scorecard_sets_are_equal_budget_and_eligibility_filtered(self):
        rows = []
        channels = {"A1": [], "A2": [], "B": [], "C": []}
        for index in range(60):
            symbol = f"{index:06d}.SZ"
            row = candidate(symbol, "PERSISTENT_LEADER")
            row["account_context"] = {"allow_execution": index != 0}
            row["features"]["ret_1d"] = float(60 - index)
            row["classification"]["primary_confidence"] = 0.8
            rows.append(row)
            channels["B"].append(symbol)
        result = TOOL.build_scorecard_sets(rows, channels)
        self.assertEqual(len(result["legacy_l1_top50"]), 50)
        self.assertEqual(len(result["path_l1_top50"]), 50)
        self.assertNotIn("000000.SZ", result["path_l1_top50"])
        self.assertTrue(result["ready"])

    def test_episode_keeps_path_transition_without_duplicate(self):
        snapshots = [
            {
                "trade_date": "2026-07-01",
                "candidates": [candidate("000001.SZ", "FRESH_IGNITION")],
            },
            {
                "trade_date": "2026-07-02",
                "candidates": [candidate("000001.SZ", "PERSISTENT_LEADER")],
            },
        ]
        episodes = TOOL.assign_episodes(snapshots)
        self.assertEqual(len(episodes), 1)
        self.assertEqual(episodes[0]["appearance_count"], 2)
        self.assertEqual(len(episodes[0]["path_transitions"]), 1)
        self.assertEqual(episodes[0]["episode_scope"], "report_range_local")

    def test_five_absent_trading_days_start_new_episode(self):
        snapshots = []
        for day in range(1, 8):
            rows = []
            if day in (1, 7):
                rows = [candidate("000001.SZ", "PERSISTENT_LEADER")]
            snapshots.append({
                "trade_date": f"2026-07-{day:02d}",
                "candidates": rows,
            })
        episodes = TOOL.assign_episodes(snapshots)
        self.assertEqual(len(episodes), 2)

    def test_percentile_threshold(self):
        self.assertEqual(TOOL.percentile_threshold(list(range(1, 11)), 0.9), 9)
        self.assertIsNone(TOOL.percentile_threshold([], 0.9))


if __name__ == "__main__":
    unittest.main()
