#!/usr/bin/env python3

import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("zhulong_test_trade_contract", ROOT / "05_shadow/lib/trade_contract.py")
MOD = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = MOD
spec.loader.exec_module(MOD)


class TradeContractTest(unittest.TestCase):
    def test_emotion_contract_has_short_holding_window(self):
        c = MOD.build_trade_contract("t1", "000001.SZ", "2026-07-10", "EMOTION_RELAY", 0.8)
        self.assertTrue(c.actionable)
        self.assertEqual(c.expected_holding_days, [1, 3])
        self.assertIn("do_not_convert_loss_to_long_term", c.exit_playbook)
        self.assertTrue(MOD.validate_trade_contract(c.to_dict()))

    def test_trend_contract_allows_longer_validation(self):
        c = MOD.build_trade_contract("t2", "600000.SH", "2026-07-10", "TREND_INITIATION", 0.75)
        self.assertEqual(c.expected_holding_days, [5, 15])
        self.assertTrue(c.actionable)

    def test_unclassified_is_not_actionable(self):
        c = MOD.build_trade_contract("t3", "000001.SZ", "2026-07-10", "UNCLASSIFIED", 0)
        self.assertFalse(c.actionable)
        self.assertEqual(c.block_reason, "TRADE_ARCHETYPE_UNCLASSIFIED")
        self.assertFalse(MOD.validate_trade_contract(c.to_dict()))

    def test_low_confidence_is_not_actionable(self):
        c = MOD.build_trade_contract("t4", "000001.SZ", "2026-07-10", "TREND_CONTINUATION", 0.4)
        self.assertFalse(c.actionable)
        self.assertEqual(c.block_reason, "TRADE_ARCHETYPE_LOW_CONFIDENCE")


if __name__ == "__main__":
    unittest.main()
