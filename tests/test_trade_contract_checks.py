#!/usr/bin/env python3

import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LIB = ROOT / "05_shadow" / "lib"
spec = importlib.util.spec_from_file_location("zhulong_test_trade_contract_checks", LIB / "trade_contract_checks.py")
MOD = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = MOD
spec.loader.exec_module(MOD)


class TradeContractChecksTest(unittest.TestCase):
    def test_dynamic_unknown_is_explicit(self):
        result = MOD.evaluate_contract_conditions({"entry_confirmation": ["next_session_relay_confirmed"]})
        self.assertEqual(result["checks"][0]["status"], "UNVERIFIED_RUNTIME")
        self.assertFalse(result["runtime_block"])

    def test_nonpositive_gap_can_be_verified_without_threshold(self):
        result = MOD.evaluate_contract_conditions(
            {"entry_confirmation": ["no_excessive_open_gap"]}, {"open_gap_pct": -1.2}
        )
        self.assertEqual(result["checks"][0]["status"], "VERIFIED_RUNTIME")

    def test_positive_gap_is_observed_not_arbitrarily_blocked(self):
        result = MOD.evaluate_contract_conditions(
            {"entry_confirmation": ["no_excessive_open_gap"]}, {"open_gap_pct": 3.5}
        )
        self.assertEqual(result["checks"][0]["status"], "OBSERVED_NO_THRESHOLD")
        self.assertFalse(result["runtime_block"])


if __name__ == "__main__":
    unittest.main()
