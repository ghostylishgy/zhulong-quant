#!/usr/bin/env python3

import importlib.util
import sys
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from tests import _test_log_isolation  # noqa: F401

ROOT = Path(__file__).resolve().parents[1]
LIB = ROOT / "05_shadow" / "lib"
sys.path.insert(0, str(LIB))
spec = importlib.util.spec_from_file_location("zhulong_test_thesis_exit", LIB / "intraday_manager.py")
MOD = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = MOD
spec.loader.exec_module(MOD)


class ThesisExitGateTest(unittest.TestCase):
    def test_critical_review_preempts_price_rules(self):
        manager = MOD.ShadowIntradayManager.__new__(MOD.ShadowIntradayManager)
        critical = {
            "review_date": "2026-07-13",
            "thesis_state": "THESIS_INVALIDATED",
            "management_action": "EXIT_NEXT_SESSION",
            "thesis_reason": "critical_news_or_regulatory_risk",
        }
        with patch.object(manager, "_latest_critical_thesis_exit", return_value=critical):
            plan = manager._select_sell_plan(
                conn=object(), symbol="600001.SH", trade_date="2026-07-14",
                position_trade_date="2026-07-10", now=datetime(2026, 7, 14, 10, 0),
                price=11, entry_price=10, highest_after=11, stop_after=9.5,
                qty=1000, initial_qty=1000, sell_stage="HOLD_FULL", strength_tier="STRONG",
                pnl_now=0.10, max_gain_ratio=0.10, drawdown_from_high=0, days_held=2,
            )
        self.assertEqual(plan["rule_id"], "THESIS_CRITICAL_RISK_EXIT")
        self.assertEqual(plan["qty_to_sell"], 1000)
        self.assertTrue(manager._is_liquidation_rule(plan["rule_id"]))

    def test_noncritical_review_does_not_match_helper_contract(self):
        class Conn:
            calls = 0
            def execute(self, _sql, _params=None):
                self.calls += 1
                return self
            def fetchone(self):
                if self.calls == 1:
                    return (1,)
                return ("2026-07-13", "THESIS_INVALIDATED", "EXIT_NEXT_SESSION", "trend_structure_broken_below_ma20", True)

        result = MOD.ShadowIntradayManager._latest_critical_thesis_exit(
            Conn(), "600001.SH", "2026-07-10", "2026-07-14"
        )
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
