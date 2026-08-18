#!/usr/bin/env python3
import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("zhulong_test_strategy_episodes", ROOT / "tools/build_strategy_episodes.py")
MOD = importlib.util.module_from_spec(spec); sys.modules[spec.name] = MOD; spec.loader.exec_module(MOD)


class StrategyEpisodeTest(unittest.TestCase):
    def test_news_skip_is_no_entry(self):
        episode = MOD.build_episode({"task_id": "t1", "symbol": "600001.SH", "trade_date": "2026-07-10"},
                                    {}, {}, {"reason": "SKIPPED_NEWS_RISK"}, {}, {}, [])
        self.assertTrue(episode["no_trade_signal"])
        self.assertEqual(episode["attribution"]["attribution_category"], "ENTRY_BLOCKED_NEWS_GATE")

    def test_closed_loss_uses_recorded_rule_without_causality(self):
        result = MOD.deterministic_attribution({"status": "FILLED"}, None,
            {"status": "FILLED", "action": "BUY"},
            {"status": "SOLD", "net_realized_pnl": -120, "last_sell_rule": "WRONG_PICK_STOP"})
        self.assertEqual(result["outcome_class"], "LOSS")
        self.assertEqual(result["attribution_category"], "WRONG_PICK_OR_NO_FOLLOW_THROUGH")
        self.assertFalse(result["causality_claimed"])

    def test_post_outcome_is_separate_from_ex_ante(self):
        episode = MOD.build_episode({"task_id": "t2", "symbol": "600002.SH", "trade_date": "2026-07-10"},
            {"primary_archetype": "TREND_INITIATION", "confidence": 0.8},
            {"status": "FILLED", "trade_contract_json": '{"entry_thesis":"x"}'}, None,
            {"status": "FILLED", "action": "BUY"},
            {"status": "SOLD", "net_realized_pnl": 100, "last_sell_rule": "PROFIT_LOCK"}, [])
        self.assertNotIn("position", episode["ex_ante"])
        self.assertNotIn("attribution", episode["ex_ante"])
        self.assertEqual(episode["post_outcome"]["position"]["net_realized_pnl"], 100)


if __name__ == "__main__": unittest.main()
