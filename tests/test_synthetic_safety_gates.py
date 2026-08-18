#!/usr/bin/env python3

import hashlib
import importlib.util
import os
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import duckdb

from tests import _test_log_isolation  # noqa: F401


ROOT = Path(__file__).resolve().parents[1]
SHADOW_LIB = ROOT / "05_shadow" / "lib"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SHADOW_LIB) not in sys.path:
    sys.path.insert(0, str(SHADOW_LIB))


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


ACCOUNT = load_module("zhulong_test_safety_account", SHADOW_LIB / "account_eligibility.py")
NEWS = load_module("zhulong_test_safety_news", SHADOW_LIB / "news_entry_gate.py")
TRADE = load_module("zhulong_test_safety_trade_contract", SHADOW_LIB / "trade_contract.py")
RECEIVER = load_module("zhulong_test_safety_receiver", SHADOW_LIB / "signal_receiver.py")
T1 = load_module("zhulong_test_safety_t1", SHADOW_LIB / "t1_fill_engine.py")

from config.market_session import validate_realtime_quote_time  # noqa: E402


class SyntheticSafetyGateTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "safety_gates.duckdb"
        conn = duckdb.connect(str(self.db_path))
        conn.execute(
            """
            CREATE TABLE fact_stock_basic (
                symbol VARCHAR PRIMARY KEY,
                name VARCHAR,
                market VARCHAR,
                is_st BOOLEAN
            )
            """
        )
        conn.executemany(
            "INSERT INTO fact_stock_basic VALUES (?, ?, ?, ?)",
            [
                ("600000.SH", "浦发银行", "主板", False),
                ("300716.SZ", "ST泉为", "创业板", True),
                ("920725.BJ", "惠丰钻石", "北交所", False),
            ],
        )
        conn.close()
        self.fixture_sha = self._sha(self.db_path)

    def tearDown(self):
        self.tmp.cleanup()

    @staticmethod
    def _sha(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def assert_fixture_unchanged(self):
        self.assertEqual(self.fixture_sha, self._sha(self.db_path))

    @staticmethod
    def signal(symbol="600000.SH", name="浦发银行", **overrides):
        payload = {
            "signal_id": "signal-safety",
            "task_id": "task-safety",
            "run_id": "a1b2c3d4",
            "symbol": symbol,
            "name": name,
            "trade_date": "2026-07-29",
            "final_score": 82,
            "close_price": 10.0,
            "trade_archetype": "TREND_INITIATION",
            "archetype_confidence": 0.8,
        }
        payload.update(overrides)
        return payload

    @staticmethod
    def news_decision(status="NEWS_CLEAR", gate="NONE", risk_level="LOW"):
        return NEWS.evaluate_news_fields(
            {
                "task_id": "task-safety",
                "run_id": "a1b2c3d4",
                "symbol": "600000.SH",
                "trade_date": "2026-07-29",
                "l4_final_verdict": "PASS",
                "final_score": 82,
                "l4_news_status": status,
                "l4_news_gate": gate,
                "l4_news_risk_level": risk_level,
                "l4_news_as_of": "2026-07-29T20:55:00+08:00",
                "l4_news_summary": "合成安全门样本",
            },
            symbol="600000.SH",
        )

    def _receiver_base_patches(self):
        return (
            patch.object(RECEIVER, "DB_PATH", str(self.db_path)),
            patch.object(RECEIVER, "_record_reinforcement_if_holding", return_value="NO_POSITION"),
            patch.object(RECEIVER, "_mark_shadow_processed", return_value=True),
            patch.object(RECEIVER, "_remember_task_id"),
        )

    def test_st_and_bj_are_terminal_observation_only_before_queue(self):
        cases = [
            (self.signal("300716.SZ", "ST泉为"), "ACCOUNT_INELIGIBLE_ST"),
            (self.signal("920725.BJ", "惠丰钻石"), "ACCOUNT_INELIGIBLE_BJ"),
        ]
        for signal, expected_reason in cases:
            with self.subTest(reason=expected_reason), patch.dict(
                os.environ, {"ZHULONG_ACCOUNT_PROFILE": "PERSONAL_MAINLAND"}
            ):
                db_patch, reinforce_patch, mark_patch, remember_patch = self._receiver_base_patches()
                with db_patch, reinforce_patch, mark_patch, remember_patch, \
                     patch.object(RECEIVER, "_record_shadow_skip", return_value=True) as skipped, \
                     patch.object(RECEIVER, "enqueue_pending_signal") as enqueue, \
                     patch.object(RECEIVER, "evaluate_shadow_news_entry_gate") as news_gate:
                    self.assertTrue(RECEIVER.process_signal(signal))
                self.assertEqual(skipped.call_args.args[1], expected_reason)
                enqueue.assert_not_called()
                news_gate.assert_not_called()
                self.assert_fixture_unchanged()

    def test_news_risk_and_caution_stop_before_tide_and_queue(self):
        cases = [
            ("NEWS_CRITICAL_CANDIDATE", "WOULD_VETO", "CRITICAL", "SKIPPED_NEWS_RISK"),
            ("NEWS_CAUTION", "WOULD_CAP_HOLD", "CAUTION", "SKIPPED_NEWS_CAUTION"),
        ]
        for status, gate, level, expected_reason in cases:
            with self.subTest(reason=expected_reason):
                decision = self.news_decision(status, gate, level)
                db_patch, reinforce_patch, mark_patch, remember_patch = self._receiver_base_patches()
                with db_patch, reinforce_patch, mark_patch, remember_patch, \
                     patch.object(RECEIVER, "_record_shadow_skip", return_value=True) as skipped, \
                     patch.object(RECEIVER, "enqueue_pending_signal") as enqueue, \
                     patch.object(RECEIVER, "evaluate_shadow_news_entry_gate", return_value=decision), \
                     patch.object(RECEIVER, "push_shadow_news_gate_skip") as push:
                    self.assertTrue(RECEIVER.process_signal(self.signal()))
                self.assertEqual(skipped.call_args.args[1], expected_reason)
                enqueue.assert_not_called()
                push.assert_called_once()
                self.assert_fixture_unchanged()

    def test_incomplete_news_enters_wait_state_not_buy_queue(self):
        decision = self.news_decision("NEWS_QUEUED", "NONE", "")
        db_patch, reinforce_patch, mark_patch, remember_patch = self._receiver_base_patches()
        with db_patch, reinforce_patch, mark_patch, remember_patch, \
             patch.object(RECEIVER, "_record_shadow_skip") as skipped, \
             patch.object(RECEIVER, "enqueue_pending_signal", return_value=True) as enqueue, \
             patch.object(RECEIVER, "evaluate_shadow_news_entry_gate", return_value=decision):
            self.assertTrue(RECEIVER.process_signal(self.signal()))
        queued = enqueue.call_args.args[0]
        self.assertEqual(queued["status"], "WAIT_NEWS_CHECK")
        self.assertIn("WAIT_NEWS_CHECK", queued["last_error"])
        skipped.assert_not_called()
        self.assert_fixture_unchanged()

    def test_force_no_edge_reaches_exact_tide_gate_after_prior_gates_pass(self):
        decision = self.news_decision()
        signal = self.signal(
            entry_tide_gate="FORCE_NO_EDGE",
            entry_tide_ratio=0.12,
            entry_policy="TIDE_SUPPRESSED_EXPERIMENT",
        )
        db_patch, reinforce_patch, mark_patch, remember_patch = self._receiver_base_patches()
        with db_patch, reinforce_patch, mark_patch, remember_patch, \
             patch.object(RECEIVER, "_record_shadow_skip", return_value=True) as skipped, \
             patch.object(RECEIVER, "enqueue_pending_signal") as enqueue, \
             patch.object(RECEIVER, "evaluate_shadow_news_entry_gate", return_value=decision):
            self.assertTrue(RECEIVER.process_signal(signal))
        reason = skipped.call_args.args[1]
        self.assertEqual(
            reason,
            "TIDE_SUPPRESSED_NO_SHADOW_ENTRY gate=FORCE_NO_EDGE policy=TIDE_SUPPRESSED_EXPERIMENT",
        )
        enqueue.assert_not_called()
        self.assert_fixture_unchanged()

    def _t1_patches(self, *, daily_result, limit_info=None, minutes=None):
        return (
            patch.object(T1, "DB_PATH", str(self.db_path)),
            patch.object(T1, "evaluate_shadow_news_entry_gate", return_value=self.news_decision()),
            patch.object(T1, "has_open_paper_position", return_value=False),
            patch.object(T1, "_fetch_daily_bar", return_value=daily_result),
            patch.object(T1, "_fetch_limit_info", return_value=limit_info or T1.LimitInfo()),
            patch.object(T1, "_fetch_minutes", return_value=minutes or T1.MinutePack()),
            patch.object(T1, "push_shadow_news_gate_skip"),
        )

    def _t1_signal(self):
        signal = self.signal()
        signal["trade_contract"] = TRADE.build_trade_contract(
            signal["task_id"], signal["symbol"], signal["trade_date"],
            signal["trade_archetype"], signal["archetype_confidence"],
        ).to_dict()
        return signal

    def test_missing_daily_bar_waits_without_shifting_session(self):
        patches = self._t1_patches(daily_result=(None, "DAILY_MISSING"))
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], \
             patch.object(T1, "_mark_terminal") as terminal:
            status = T1.process_due_signal(
                self._t1_signal(), fill_date="2026-07-30", mode=T1.MAIN_MODE, api=None
            )
        self.assertEqual(status, "WAIT_DATA")
        self.assertEqual(terminal.call_args.args[2:4], ("WAIT_DATA", "DAILY_MISSING"))
        self.assert_fixture_unchanged()

    def test_one_price_limit_up_is_unfilled_not_shifted(self):
        daily = T1.DailyBar(
            symbol="600000.SH", trade_date="2026-07-30",
            open=11.0, high=11.0, low=11.0, close=11.0,
            pre_close=10.0, vol=5000, amount=5500, source="SYNTHETIC",
        )
        limit_info = T1.LimitInfo(
            up_limit=11.0, down_limit=9.0, source="SYNTHETIC_LIMIT", estimated=False
        )
        patches = self._t1_patches(
            daily_result=(daily, ""), limit_info=limit_info,
            minutes=T1.MinutePack(source="SYNTHETIC", unavailable_reason="MINUTE_EMPTY"),
        )
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], \
             patch.object(T1, "_mark_terminal") as terminal:
            status = T1.process_due_signal(
                self._t1_signal(), fill_date="2026-07-30", mode=T1.MAIN_MODE, api=None
            )
        self.assertEqual(status, "UNFILLED")
        self.assertEqual(terminal.call_args.args[2:4], ("UNFILLED", "LIMIT_UP_ONE_PRICE"))
        self.assert_fixture_unchanged()

    def test_stale_quote_reason_is_exact_and_fail_closed(self):
        valid, reason, _, _ = validate_realtime_quote_time(
            "20260729", "14:59:58", now=datetime(2026, 7, 30, 9, 30)
        )
        self.assertFalse(valid)
        self.assertEqual(reason, "QUOTE_STALE_DATE")

    def test_news_push_is_chinese_and_uses_network_stub(self):
        decision = self.news_decision(
            "NEWS_CRITICAL_CANDIDATE", "WOULD_VETO", "CRITICAL"
        )
        with patch.object(NEWS.Config, "PUSHPLUS_TOKEN", "test-token"), \
             patch.object(NEWS.requests, "post") as post:
            NEWS.push_shadow_news_gate_skip(
                self.signal(), decision, stage="queue"
            )
        payload = post.call_args.kwargs["json"]
        self.assertIn("新闻风险拦截", payload["title"])
        self.assertIn("已跳过影子盘买入计划", payload["content"])
        self.assertIn("不修改 L4 verdict", payload["content"])


if __name__ == "__main__":
    unittest.main()
