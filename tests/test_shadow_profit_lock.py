#!/usr/bin/env python3

import importlib.util
import sys
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from tests import _test_log_isolation  # noqa: F401

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "05_shadow" / "lib"))


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


INTRADAY = load_module(
    "zhulong_test_shadow_intraday_profit_lock",
    ROOT / "05_shadow" / "lib" / "intraday_manager.py",
)
RECEIVER = load_module(
    "zhulong_test_shadow_signal_receiver_profit_lock",
    ROOT / "05_shadow" / "lib" / "signal_receiver.py",
)


class ShadowProfitLockTest(unittest.TestCase):
    def setUp(self):
        self.manager = INTRADAY.ShadowIntradayManager(db_path=":memory:")
        self.now = datetime(2026, 6, 26, 10, 0)

    def sell_plan(self, **overrides):
        payload = {
            "conn": None,
            "symbol": "000001.SZ",
            "trade_date": "2026-06-26",
            "position_trade_date": "2026-06-20",
            "now": self.now,
            "price": 112.0,
            "entry_price": 100.0,
            "highest_after": 112.0,
            "stop_after": 100.0,
            "qty": 600,
            "initial_qty": 1000,
            "sell_stage": "TAKE_1_DONE",
            "strength_tier": "NORMAL",
            "pnl_now": 0.12,
            "max_gain_ratio": 0.12,
            "drawdown_from_high": 0.0,
            "days_held": 2,
        }
        payload.update(overrides)
        return self.manager._select_sell_plan(**payload)

    def test_normal_exits_remaining_at_second_take_profit(self):
        plan = self.sell_plan(strength_tier="NORMAL")
        self.assertEqual(plan["rule_id"], "TAKE_PROFIT_12_FULL")
        self.assertEqual(plan["qty_to_sell"], 600)
        self.assertEqual(plan["new_stage"], "SOLD")
        self.assertAlmostEqual(plan["stop_after"], 108.0)

    def test_strong_runner_is_capped_at_ten_percent(self):
        with patch.object(self.manager, "_current_limit_up_streak", return_value=0), \
             patch.object(self.manager, "_load_runner_context", return_value={"runner_gate": "ALLOW"}):
            plan = self.sell_plan(strength_tier="STRONG", conn=object())
        self.assertEqual(plan["rule_id"], "TAKE_PROFIT_12_LOCK")
        self.assertEqual(plan["qty_to_sell"], 500)
        self.assertEqual(plan["new_stage"], "PROFIT_LOCK")
        self.assertAlmostEqual(plan["stop_after"], 108.0)

    def test_small_strong_position_exits_when_runner_would_exceed_cap(self):
        with patch.object(self.manager, "_current_limit_up_streak", return_value=0), \
             patch.object(self.manager, "_load_runner_context", return_value={"runner_gate": "ALLOW"}):
            plan = self.sell_plan(
                strength_tier="STRONG",
                conn=object(),
                qty=300,
                initial_qty=500,
            )
        self.assertEqual(plan["rule_id"], "TAKE_PROFIT_12_FULL")
        self.assertEqual(plan["qty_to_sell"], 300)
        self.assertEqual(plan["new_stage"], "SOLD")
        self.assertIn("runner target rounds to zero", plan["reason"])

    def test_strong_runner_is_denied_when_context_is_missing(self):
        with patch.object(self.manager, "_current_limit_up_streak", return_value=0), \
             patch.object(self.manager, "_load_runner_context", return_value={"runner_gate": "DENY", "runner_gate_reason": "context_missing"}):
            plan = self.sell_plan(strength_tier="STRONG", conn=object())
        self.assertEqual(plan["rule_id"], "TAKE_PROFIT_12_FULL")
        self.assertEqual(plan["qty_to_sell"], 600)
        self.assertIn("context_missing", plan["reason"])

    def test_second_limit_up_exits_immediately(self):
        with patch.object(self.manager, "_current_limit_up_streak", return_value=2):
            plan = self.sell_plan(conn=object(), pnl_now=0.20, max_gain_ratio=0.20)
        self.assertEqual(plan["rule_id"], "TWO_LIMIT_UP_FULL_EXIT")
        self.assertEqual(plan["qty_to_sell"], 600)
        self.assertEqual(plan["new_stage"], "SOLD")

    def test_profit_lock_trims_once_on_pullback(self):
        plan = self.sell_plan(
            sell_stage="PROFIT_LOCK",
            strength_tier="STRONG",
            price=116.0,
            highest_after=121.0,
            stop_after=108.0,
            qty=300,
            initial_qty=3000,
            pnl_now=0.16,
            max_gain_ratio=0.21,
            drawdown_from_high=(116.0 / 121.0) - 1,
        )
        self.assertEqual(plan["rule_id"], "PROFIT_LOCK_TRIM")
        self.assertEqual(plan["qty_to_sell"], 100)
        self.assertEqual(plan["new_stage"], "PROFIT_LOCK_TRIMMED")

    def test_profit_lock_clears_on_deeper_pullback(self):
        plan = self.sell_plan(
            sell_stage="PROFIT_LOCK",
            strength_tier="STRONG",
            price=114.0,
            highest_after=120.0,
            stop_after=108.0,
            qty=300,
            pnl_now=0.14,
            max_gain_ratio=0.20,
            drawdown_from_high=-0.05,
        )
        self.assertEqual(plan["rule_id"], "PROFIT_LOCK_CLEAR")
        self.assertEqual(plan["qty_to_sell"], 300)
        self.assertEqual(plan["new_stage"], "SOLD")

    def test_profit_lock_floor_exits_before_profit_is_lost(self):
        plan = self.sell_plan(
            sell_stage="PROFIT_LOCK_TRIMMED",
            strength_tier="STRONG",
            price=107.9,
            highest_after=120.0,
            stop_after=108.0,
            qty=100,
            pnl_now=0.079,
            max_gain_ratio=0.20,
            drawdown_from_high=-0.10,
        )
        self.assertEqual(plan["rule_id"], "PROFIT_LOCK_FLOOR_EXIT")
        self.assertEqual(plan["qty_to_sell"], 100)


class ShadowSignalGateTest(unittest.TestCase):
    def test_force_no_edge_signal_is_skipped_not_queued(self):
        signal = {
            "task_id": "task-force-no-edge",
            "run_id": "run-1",
            "symbol": "000001.SZ",
            "name": "test",
            "trade_date": "2026-06-26",
            "final_score": 90,
            "close_price": 10.0,
            "entry_tide_gate": "FORCE_NO_EDGE",
            "entry_tide_ratio": 0.1,
            "entry_policy": "TIDE_SUPPRESSED_EXPERIMENT",
        }
        with patch.object(RECEIVER, "_record_reinforcement_if_holding", return_value="NO_POSITION"), \
             patch.object(RECEIVER, "_record_shadow_skip", return_value=True) as record_skip, \
             patch.object(RECEIVER, "_mark_shadow_processed", return_value=True) as mark_processed, \
             patch.object(RECEIVER, "_remember_task_id") as remember, \
             patch.object(RECEIVER, "enqueue_pending_signal") as enqueue:
            self.assertTrue(RECEIVER.process_signal(signal))

        record_skip.assert_called_once()
        mark_processed.assert_called_once_with("task-force-no-edge")
        remember.assert_called_once_with("task-force-no-edge")
        enqueue.assert_not_called()


if __name__ == "__main__":
    unittest.main()
