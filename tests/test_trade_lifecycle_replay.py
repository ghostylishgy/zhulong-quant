#!/usr/bin/env python3

import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "zhulong_test_trade_lifecycle_replay", ROOT / "tools/replay_trade_lifecycle.py")
MOD = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = MOD
spec.loader.exec_module(MOD)


class TradeLifecycleReplayTest(unittest.TestCase):
    def row(self, archetype, actionable=True, eligible=True, enter=True, value=1.0):
        return {
            "classification": {"primary_archetype": archetype},
            "l4_final_verdict": "PASS",
            "account_eligibility": {"allow_execution": eligible},
            "trade_contract": {"actionable": actionable},
            "would_enter_contract_path": enter,
            "forward_outcome": {
                "t1_return": value, "t3_return": value + 1,
                "t5_return": None, "t10_return": None,
            },
        }

    def test_summary_keeps_unclassified_separate(self):
        result = MOD.summarize([
            self.row("TREND_INITIATION", True, True, True, 2),
            self.row("UNCLASSIFIED", False, True, False, -3),
        ])
        self.assertEqual(result["candidates"], 2)
        self.assertEqual(result["blocked_unclassified_or_low_confidence"], 1)
        self.assertEqual(result["would_enter_contract_path"], 1)
        self.assertEqual(result["by_archetype"]["TREND_INITIATION"]["avg_t1_return"], 2)

    def test_account_block_is_counterfactual_not_deleted(self):
        result = MOD.summarize([self.row("EMOTION_RELAY", True, False, False, 5)])
        self.assertEqual(result["blocked_account_eligibility"], 1)
        self.assertEqual(result["by_archetype"]["EMOTION_RELAY"]["candidates"], 1)


if __name__ == "__main__":
    unittest.main()
