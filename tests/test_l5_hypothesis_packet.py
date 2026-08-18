#!/usr/bin/env python3
import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("zhulong_test_l5_packet", ROOT / "tools/build_l5_hypothesis_packet.py")
MOD = importlib.util.module_from_spec(spec); sys.modules[spec.name] = MOD; spec.loader.exec_module(MOD)


class L5HypothesisPacketTest(unittest.TestCase):
    def test_aggregate_contains_no_symbol_level_data(self):
        payload = {"episodes": [{"symbol": "600001.SH", "attribution": {"outcome_class": "LOSS", "attribution_category": "PRICE_RISK_EXIT"}, "ex_ante": {"archetype": {"primary": "TREND_INITIATION"}}}]}
        stats = MOD.aggregate(payload)
        self.assertNotIn("600001.SH", str(stats))
        self.assertEqual(stats["closed_outcomes"], 1)
        self.assertEqual(stats["per_archetype_closed"]["TREND_INITIATION"], 1)

    def test_source_contract_fails_closed(self):
        with self.assertRaises(ValueError):
            MOD.validate_source({"schema_version": MOD.INPUT_VERSION, "read_only": False, "no_trade_signal": True})

    def test_prompt_blocks_trade_and_parameter_advice(self):
        prompt = MOD.build_prompt({"closed_outcomes": 20})
        self.assertIn("Do not name or recommend any stock", prompt)
        self.assertIn("Do not provide", prompt)
        self.assertIn("HUMAN_REVIEW_REQUIRED", prompt)


if __name__ == "__main__": unittest.main()
