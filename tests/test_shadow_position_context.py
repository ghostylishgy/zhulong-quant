#!/usr/bin/env python3

import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


CTX = load_module(
    "zhulong_test_shadow_position_context",
    ROOT / "tools" / "build_shadow_position_context.py",
)


class ShadowPositionContextTest(unittest.TestCase):
    def strong_position(self):
        return CTX.PositionRow(
            symbol="000001.SZ",
            name="test",
            position_trade_date="2026-06-20",
            strength_tier="STRONG",
            entry_tide_gate="AGGRESSIVE",
            entry_policy="STANDARD_ENTRY",
            signal_task_id="task-1",
            qty=1000,
            entry_price=10.0,
        )

    def clean_inputs(self):
        return {
            "tide": {"market_gate": "AGGRESSIVE"},
            "news": {"news_risk_level": "CLEAR"},
            "regulatory": {"regulatory_risk_level": "CLEAR"},
            "liquidity": {"liquidity_ok": True, "volume_expansion_ok": True},
            "limit_up_streak": 0,
            "missing": [],
        }

    def test_runner_gate_allows_only_when_every_condition_is_clean(self):
        payload = self.clean_inputs()
        gate, reason, abnormal, quality = CTX.evaluate_runner_gate(self.strong_position(), **payload)
        self.assertEqual(gate, "ALLOW")
        self.assertEqual(reason, "all_runner_conditions_met")
        self.assertFalse(abnormal)
        self.assertEqual(quality, "DATA_OK")

    def test_runner_gate_fails_closed_on_missing_news(self):
        payload = self.clean_inputs()
        payload["news"] = {"news_risk_level": "UNAVAILABLE"}
        payload["missing"] = ["news_unavailable"]
        gate, reason, abnormal, quality = CTX.evaluate_runner_gate(self.strong_position(), **payload)
        self.assertEqual(gate, "DENY")
        self.assertIn("news_UNAVAILABLE", reason)
        self.assertIn("news_unavailable", reason)
        self.assertTrue(abnormal)
        self.assertEqual(quality, "PARTIAL")

    def test_runner_gate_denies_two_limit_up_context(self):
        payload = self.clean_inputs()
        payload["limit_up_streak"] = 2
        gate, reason, abnormal, _ = CTX.evaluate_runner_gate(self.strong_position(), **payload)
        self.assertEqual(gate, "DENY")
        self.assertIn("two_limit_up_full_exit", reason)
        self.assertTrue(abnormal)

    def test_regulatory_classifier_flags_critical_titles(self):
        status, level, score, hits = CTX.classify_regulatory_hits([
            {"公告标题": "关于收到行政处罚决定书的公告"},
        ])
        self.assertEqual(status, "REG_CRITICAL")
        self.assertEqual(level, "CRITICAL")
        self.assertEqual(score, 90)
        self.assertTrue(hits)

    @staticmethod
    def cninfo_response(payload):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = payload
        return response

    def test_cninfo_empty_disclosure_result_is_clean(self):
        responses = [
            self.cninfo_response([{"code": "000001", "orgId": "gssz0000001"}]),
            self.cninfo_response({"totalAnnouncement": 0, "announcements": None}),
            self.cninfo_response({"totalAnnouncement": 0, "announcements": []}),
        ]
        with patch.object(CTX.requests, "post", side_effect=responses):
            rows = CTX.fetch_cninfo_disclosures(
                "000001.SZ", "2026-08-11", "2026-08-18"
            )
        self.assertEqual(rows, [])

    def test_cninfo_disclosure_rows_are_normalized_and_deduplicated(self):
        announcement = {
            "secCode": "000001",
            "secName": "平安银行",
            "announcementTitle": "关于收到监管函的公告",
            "announcementTime": 1786982400000,
            "announcementId": "notice-1",
            "orgId": "gssz0000001",
        }
        responses = [
            self.cninfo_response([{"code": "000001", "orgId": "gssz0000001"}]),
            self.cninfo_response({"announcements": [announcement]}),
            self.cninfo_response({"announcements": [announcement]}),
        ]
        with patch.object(CTX.requests, "post", side_effect=responses):
            rows = CTX.fetch_cninfo_disclosures(
                "000001.SZ", "2026-08-11", "2026-08-18"
            )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["title"], "关于收到监管函的公告")
        status, level, score, hits = CTX.classify_regulatory_hits(rows)
        self.assertEqual((status, level, score), ("REG_CAUTION", "CAUTION", 65))
        self.assertTrue(hits)

    def test_cninfo_schema_drift_fails_closed(self):
        responses = [
            self.cninfo_response([{"code": "000001", "orgId": "gssz0000001"}]),
            self.cninfo_response({"announcements": {"unexpected": True}}),
        ]
        with patch.object(CTX.requests, "post", side_effect=responses):
            with self.assertRaisesRegex(ValueError, "announcements schema"):
                CTX.fetch_cninfo_disclosures(
                    "000001.SZ", "2026-08-11", "2026-08-18"
                )

    def test_context_push_text_is_operator_facing_chinese(self):
        title, content = CTX.render_context_push(
            {"trade_date": "2026-06-26", "runner_allowed": 0, "runner_denied": 1, "market_gate": "FORCE_NO_EDGE"},
            [{
                "symbol": "000001.SZ",
                "name": "平安银行",
                "runner_gate": "DENY",
                "runner_gate_reason": "market_gate_FORCE_NO_EDGE,news_unavailable",
                "news_risk_level": "UNAVAILABLE",
                "regulatory_risk_level": "CLEAR",
                "liquidity_ok": False,
                "avg_amount_5d_yuan": 123456789,
                "volume_expansion_ok": False,
                "volume_ratio_3d": 0.8,
            }],
        )
        self.assertIn("烛龙模拟盘持仓复核", title)
        self.assertIn("不保留观察仓", content)
        self.assertIn("市场处于潮汐压制", content)
        self.assertNotIn("FORCE_NO_EDGE", content)
        self.assertNotIn("DENY", content)
        self.assertNotIn("news_unavailable", content)


if __name__ == "__main__":
    unittest.main()
