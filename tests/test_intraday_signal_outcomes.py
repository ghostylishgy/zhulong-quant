#!/usr/bin/env python3

import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("zhulong_test_intraday_outcomes", ROOT / "tools/review_intraday_signal_outcomes.py")
MOD = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = MOD
spec.loader.exec_module(MOD)


class IntradaySignalOutcomesTest(unittest.TestCase):
    def test_evaluate_uses_strict_future_dates(self):
        rows = MOD.evaluate(
            [{"trade_date": "2026-07-01", "source": "EAGLE", "symbol": "600001.SH", "signal_type": "BREAKOUT_APPROVED", "price": 10}],
            {"600001.SH": [("2026-07-01", 99), ("2026-07-02", 11), ("2026-07-03", 12), ("2026-07-06", 9)]},
        )
        self.assertEqual(rows[0]["return_t1"], 10)
        self.assertEqual(rows[0]["return_t3"], -10)

    def test_report_never_authorizes_trading(self):
        rows = []
        for index in range(50):
            rows.append({
                "source": "EAGLE", "signal_type": "BREAKOUT_APPROVED",
                "return_t1": 2, "return_t3": 3, "return_t5": 4, "max_drawdown_5d_pct": -1,
            })
            rows.append({
                "source": "EAGLE", "signal_type": "BREAKOUT_OBSERVED",
                "return_t1": 0, "return_t3": 0, "return_t5": 0, "max_drawdown_5d_pct": -3,
            })
        report = MOD.build_report(rows, "2026-06-01", "2026-07-10")
        self.assertEqual(report["review_status"], "READY_FOR_DESIGN_REVIEW")
        self.assertTrue(report["no_trade_signal"])
        self.assertIn("consume_by_shadow", report["blocked_actions"])


if __name__ == "__main__":
    unittest.main()
