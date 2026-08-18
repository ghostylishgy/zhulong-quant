#!/usr/bin/env python3

import importlib.util
import sys
import unittest
from pathlib import Path

from tests import _test_log_isolation  # noqa: F401

ROOT = Path(__file__).resolve().parents[1]
LIB = ROOT / "05_shadow" / "lib"
sys.path.insert(0, str(LIB))
spec = importlib.util.spec_from_file_location("zhulong_test_action_plan", LIB / "action_plan.py")
MOD = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = MOD
spec.loader.exec_module(MOD)


class ActionPlanTest(unittest.TestCase):
    audit = {
        "task_id": "task-1", "symbol": "600001.SH", "name": "样例",
        "l4_final_verdict": "PASS", "final_score": 78, "l1_close": 10,
        "l4_news_status": "NEWS_CLEAR", "l4_news_gate": "NONE",
        "trade_archetype": "TREND_INITIATION",
    }
    contract = {
        "primary_archetype": "TREND_INITIATION",
        "entry_confirmation": ["breakout_confirmed"],
        "invalidation_conditions": ["breakout_failed"],
    }

    def test_pending_plan_exposes_cap_without_calling_it_allocation(self):
        pending = {
            "status": "PENDING", "earliest_fill_date": "2026-07-14",
            "entry_tide_gate": "CAUTION", "trade_contract_json": self.contract,
        }
        item = MOD.compose_plan_item(
            self.audit, pending, None, cash_reserve=100000, max_single_pct=0.35,
            sector_tide={"sector": "电气设备", "status": "SECTOR_SUPPORTIVE", "observer_only": True, "block_entry": False},
        )
        self.assertEqual(item["plan_status"], "WAIT_T1_ENTRY")
        self.assertEqual(item["suggested_max_amount"], 35000)
        self.assertIn("not committed allocation", item["amount_semantics"])
        self.assertTrue(item["shadow_actionable"])
        self.assertEqual(item["sector_tide_status"], "SECTOR_SUPPORTIVE")
        self.assertFalse(item["sector_tide_evidence"]["block_entry"])
        self.assertIn("UNVERIFIED_RUNTIME", item["contract_condition_evaluation"]["counts"])

    def test_st_skip_remains_observation_only(self):
        skipped = {"reason": "ACCOUNT_INELIGIBLE_ST", "evidence_json": "{}"}
        item = MOD.compose_plan_item(self.audit, None, skipped, cash_reserve=100000, max_single_pct=0.35)
        self.assertEqual(item["plan_status"], "OBSERVE_ONLY_ST")
        self.assertEqual(item["suggested_max_amount"], 0)
        self.assertFalse(item["shadow_actionable"])

    def test_news_wait_is_not_actionable(self):
        pending = {"status": "WAIT_NEWS_CHECK", "trade_contract_json": self.contract}
        item = MOD.compose_plan_item(self.audit, pending, None, cash_reserve=100000, max_single_pct=0.35)
        self.assertEqual(item["plan_status"], "WAIT_NEWS_CHECK")
        self.assertFalse(item["shadow_actionable"])

    def test_push_states_pass_boundary(self):
        item = MOD.compose_plan_item(
            self.audit,
            {"status": "PENDING", "earliest_fill_date": "2026-07-14", "trade_contract_json": self.contract},
            None,
            cash_reserve=100000,
            max_single_pct=0.35,
        )
        _, content = MOD.render_push({"trade_date": "2026-07-13", "items": [item]})
        self.assertIn("PASS 不是买入指令", content)
        self.assertIn("等待 T+1 进入条件", content)
        self.assertNotIn("TREND_INITIATION", content)
        self.assertNotIn("NEWS_CLEAR", content)
        self.assertIn("契约条件：突破确认", content)
        self.assertIn("失效条件：突破失败", content)
        self.assertIn("09:31-09:45 VWAP", content)


if __name__ == "__main__":
    unittest.main()
