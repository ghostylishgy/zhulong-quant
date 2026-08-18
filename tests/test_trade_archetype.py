#!/usr/bin/env python3

import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("zhulong_test_trade_archetype", ROOT / "02_brain/lib/trade_archetype.py")
MOD = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = MOD
spec.loader.exec_module(MOD)


class TradeArchetypeTest(unittest.TestCase):
    def test_emotion_relay(self):
        r = MOD.classify_trade_archetype(MOD.ArchetypeFeatures(
            pct_chg=10, near_limit_up=True, turnover=21, vol_ratio=2.4, rps_10=98,
            limit_up_streak=2, lhb_present=True, data_as_of="2026-07-10"))
        self.assertEqual(r.primary_archetype, MOD.EMOTION_RELAY)
        self.assertTrue(r.observer_only)
        self.assertFalse(r.generate_trade)

    def test_event_catalyst(self):
        r = MOD.classify_trade_archetype(MOD.ArchetypeFeatures(
            pct_chg=4, positive_news_tags=["CONTRACT"],
            official_event_evidence=True, data_as_of="2026-07-10"))
        self.assertEqual(r.primary_archetype, MOD.EVENT_CATALYST)

    def test_trend_initiation(self):
        r = MOD.classify_trade_archetype(MOD.ArchetypeFeatures(
            pct_chg=4.5, vol_ratio=1.6, rps_10=93, close_above_ma20=True,
            breakout_above_ma20=True, ma20_slope_positive=True,
            above_ma20_days=1, data_as_of="2026-07-10"))
        self.assertEqual(r.primary_archetype, MOD.TREND_INITIATION)

    def test_trend_continuation(self):
        r = MOD.classify_trade_archetype(MOD.ArchetypeFeatures(
            pct_chg=1.2, vol_ratio=1.1, rps_10=90, close_above_ma20=True,
            ma20_slope_positive=True, above_ma20_days=7, data_as_of="2026-07-10"))
        self.assertEqual(r.primary_archetype, MOD.TREND_CONTINUATION)

    def test_conflict_fails_closed(self):
        r = MOD.classify_trade_archetype(MOD.ArchetypeFeatures(
            pct_chg=9.8, near_limit_up=True, turnover=18, vol_ratio=2, rps_10=96,
            close_above_ma20=True, breakout_above_ma20=True,
            ma20_slope_positive=True, above_ma20_days=1,
            positive_news_tags=["BID_WIN"], official_event_evidence=True,
            limit_up_streak=1, data_as_of="2026-07-10"))
        self.assertEqual(r.primary_archetype, MOD.UNCLASSIFIED)
        self.assertIn("mixed_or_conflicting_archetype_evidence", r.warnings)

    def test_weak_features_do_not_force_label(self):
        r = MOD.classify_trade_archetype(MOD.ArchetypeFeatures(data_as_of="2026-07-10"))
        self.assertEqual(r.primary_archetype, MOD.UNCLASSIFIED)


if __name__ == "__main__":
    unittest.main()
