#!/usr/bin/env python3

import importlib.util
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "zhulong_test_shadow_thesis_review", ROOT / "tools/build_shadow_position_context.py")
CTX = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = CTX
spec.loader.exec_module(CTX)


class ShadowThesisReviewTest(unittest.TestCase):
    def setUp(self):
        self.original_hold_days = CTX._hold_trading_days
        CTX._hold_trading_days = lambda position, trade_date: 1

    def tearDown(self):
        CTX._hold_trading_days = self.original_hold_days

    def position(self, archetype):
        contract = {
            "contract_id": "c1",
            "expected_holding_days": [1, 3] if archetype == "EMOTION_RELAY" else [5, 15],
        }
        return CTX.PositionRow(
            symbol="000001.SZ", name="test", position_trade_date="2026-07-10",
            strength_tier="STRONG", entry_tide_gate="AGGRESSIVE",
            entry_policy="STANDARD_ENTRY", signal_task_id="t1", qty=100,
            entry_price=10, trade_archetype=archetype,
            trade_contract_json=json.dumps(contract),
        )

    def review(self, archetype, market="AGGRESSIVE", news="CLEAR", reg="CLEAR", volume=True, above=True):
        return CTX.evaluate_position_thesis(
            self.position(archetype), "2026-07-11",
            {"market_gate": market}, {"news_risk_level": news},
            {"regulatory_risk_level": reg},
            {"volume_expansion_ok": volume, "close_above_ma20": above},
        )

    def test_emotion_relay_requires_environment_and_volume(self):
        good = self.review("EMOTION_RELAY")
        weak = self.review("EMOTION_RELAY", volume=False)
        self.assertEqual(good["thesis_state"], "THESIS_CONFIRMED")
        self.assertEqual(weak["management_action"], "TAKE_PROFIT_OR_EXIT")

    def test_trend_initiation_does_not_require_daily_volume_expansion(self):
        result = self.review("TREND_INITIATION", volume=False, above=True)
        self.assertEqual(result["management_action"], "HOLD_WITH_TRAILING_STOP")

    def test_trend_break_is_archetype_specific(self):
        start = self.review("TREND_INITIATION", above=False)
        cont = self.review("TREND_CONTINUATION", above=False)
        self.assertEqual(start["thesis_state"], "THESIS_WEAKENED")
        self.assertEqual(cont["thesis_state"], "THESIS_INVALIDATED")

    def test_critical_news_invalidates_every_archetype(self):
        result = self.review("TREND_INITIATION", news="CRITICAL")
        self.assertEqual(result["management_action"], "EXIT_NEXT_SESSION")

    def test_legacy_position_requires_manual_review(self):
        p = self.position("UNCLASSIFIED")
        p.trade_contract_json = "{}"
        result = CTX.evaluate_position_thesis(
            p, "2026-07-11", {"market_gate": "AGGRESSIVE"},
            {"news_risk_level": "CLEAR"}, {"regulatory_risk_level": "CLEAR"},
            {"volume_expansion_ok": True, "close_above_ma20": True})
        self.assertEqual(result["thesis_state"], "MANUAL_REVIEW")


if __name__ == "__main__":
    unittest.main()
