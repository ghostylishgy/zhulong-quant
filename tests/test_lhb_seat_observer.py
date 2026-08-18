#!/usr/bin/env python3

import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("zhulong_test_lhb_seats", ROOT / "tools/observe_lhb_seat_risk.py")
MOD = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = MOD
spec.loader.exec_module(MOD)


class LhbSeatObserverTest(unittest.TestCase):
    def test_empty_detail_is_not_a_risk_signal(self):
        result = MOD.analyze_seats("600001.SH", [])
        self.assertEqual(result["status"], "NO_LHB_DETAIL")
        self.assertFalse(result["manual_review_required"])
        self.assertFalse(result["generate_trade"])

    def test_proxy_and_explicit_patterns_remain_review_only(self):
        rows = [
            {"exalter": "东方财富证券股份有限公司拉萨团结路营业部", "buy": 10, "net_buy": 10, "buy_rate": 8},
            {"exalter": "东方财富证券股份有限公司拉萨东环路营业部", "sell": 20, "net_buy": -20, "sell_rate": 16},
            {"exalter": "机构专用", "sell": 30, "net_buy": -30, "sell_rate": 10},
        ]
        result = MOD.analyze_seats("600001.SH", rows, ["拉萨团结路"])
        self.assertEqual(result["status"], "KNOWN_RISK_PATTERN_REVIEW")
        self.assertIn("RETAIL_HOT_SEAT_CLUSTER_PROXY", result["warnings"])
        self.assertIn("INSTITUTION_NET_SELL", result["warnings"])
        self.assertIn("SINGLE_SEAT_SELL_CONCENTRATION", result["warnings"])
        self.assertTrue(result["manual_review_required"])
        self.assertFalse(result["generate_trade"])


if __name__ == "__main__":
    unittest.main()
