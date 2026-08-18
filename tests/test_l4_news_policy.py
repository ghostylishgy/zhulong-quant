#!/usr/bin/env python3

import importlib.util
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
TEST_LOG_DIR = Path(tempfile.gettempdir()) / f"zhulong_unittest_logs_{os.getpid()}"
TEST_LOG_DIR.mkdir(parents=True, exist_ok=True)
os.environ["ZHULONG_NEXUS_LOG_PATH"] = str(TEST_LOG_DIR / "nexus.log")
os.environ["ZHULONG_GOVERNANCE_LOG_PATH"] = str(TEST_LOG_DIR / "governance.log")
MODULE_PATH = ROOT / "02_brain" / "decision_engine.py"
SPEC = importlib.util.spec_from_file_location("zhulong_test_decision_engine", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class FakeResponse:
    def __init__(self, content):
        self.status_code = 200
        self.content = b"ok"
        self._content = content

    def json(self):
        return {"choices": [{"message": {"content": self._content}}]}


class L4NewsPolicyTest(unittest.TestCase):
    def court(self, policy="ENFORCED"):
        court = MODULE.L4SupremeCourt.__new__(MODULE.L4SupremeCourt)
        court.news_policy = policy
        court._news_gate_decider = MODULE._load_internal_module(
            "zhulong_test_news_policy_core",
            "02_brain/lib/news_verifier.py",
        ).decide_news_gate
        court.kimi_key = "test"
        court.deepseek_key = "test"
        court.qwen_key = "test"
        court.notary_strict = False
        court.notary_veto_hard = False
        return court

    @staticmethod
    def official_critical_payload():
        return {
            "evidence": [{
                "provider": "CNINFO",
                "source_grade": "A",
                "critical_tags": ["INVESTIGATION"],
                "caution_tags": [],
            }]
        }

    def test_engine_gate_records_original_and_only_downgrades(self):
        court = self.court("ENFORCED")
        result = MODULE.L4Result(
            symbol="600519",
            final_verdict=MODULE.Verdict.PASS,
            final_score=82,
        )
        applied = court._apply_news_decision_gate(result, self.official_critical_payload())
        self.assertTrue(applied)
        self.assertEqual(result.news_pre_gate_verdict, "PASS")
        self.assertEqual(result.news_pre_gate_score, 82)
        self.assertEqual(result.final_verdict, MODULE.Verdict.VETO)
        self.assertTrue(result.veto_applied)

    def test_observe_policy_preserves_verdict(self):
        court = self.court("OBSERVE_ONLY")
        result = MODULE.L4Result(
            symbol="600519",
            final_verdict=MODULE.Verdict.PASS,
            final_score=82,
        )
        applied = court._apply_news_decision_gate(result, self.official_critical_payload())
        self.assertFalse(applied)
        self.assertEqual(result.final_verdict, MODULE.Verdict.PASS)

    def test_news_context_reaches_all_four_court_prompts(self):
        court = self.court("COURT_CONTEXT")
        captured = {}

        def fake_call(url, headers, payload, timeout, label, **kwargs):
            captured[label] = payload["messages"][-1]["content"]
            if label == "L4.2-Judge":
                return FakeResponse(
                    '{"verdict":"HOLD","eligibility_score":60,"confidence":60,'
                    '"ruling":"test","decisive_evidence_refs":["L1.PCT_CHG"],'
                    '"unresolved_gaps":[]}'
                )
            if label == "L4.3-Notary":
                return FakeResponse(json.dumps({
                    "stock_code": "600519",
                    "final_verdict": "HOLD",
                    "eligibility_score": 60,
                    "confidence": 60,
                    "dominant_logic": "test",
                    "bull_summary": "test",
                    "bear_summary": "test",
                    "fatal_risk_flag": False,
                }))
            if label == "L4.2-Bull":
                return FakeResponse(
                    '{"case_strength":"WEAK","thesis":"test",'
                    '"evidence_refs":["L1.PCT_CHG"],"condition_refs":["L3.COND.01"],'
                    '"unknowns":[]}'
                )
            return FakeResponse(
                '{"risk_strength":"MINOR","risk_thesis":"test",'
                '"evidence_refs":["L1.PCT_CHG"],"condition_refs":["L3.COND.01"],'
                '"unknowns":[]}'
            )

        marker = "<verified_news_evidence>test</verified_news_evidence>"
        candidate = MODULE.Candidate(symbol="600519")
        packet = court._legacy_call_evidence_packet(
            "600519", 1.0, "tag", 70, "reason", "fc", rag_present=True
        )
        with patch.object(MODULE, "api_call_with_retry", side_effect=fake_call):
            bull = court._call_bull(
                "600519", 1.0, "tag", 70, "reason", "rag", "fc", marker, packet
            )
            bear = court._call_bear(
                "600519", 1.0, "tag", 70, "reason", "rag", "fc", marker, packet
            )
            judge = court._call_judge(
                "600519", bull, bear, 0.5, "NEUTRAL", "rag", marker, packet
            )
            court.post_audit_notary(candidate, {}, judge, "bull", "bear", marker)

        self.assertEqual(
            set(captured),
            {"L4.2-Bull", "L4.2-Bear", "L4.2-Judge", "L4.3-Notary"},
        )
        for prompt in captured.values():
            self.assertIn(marker, prompt)


if __name__ == "__main__":
    unittest.main()
