import importlib.util
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("l2_model_bakeoff_test_module", ROOT / "tools" / "l2_model_bakeoff.py")
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class L2ModelBakeoffTest(unittest.TestCase):
    def test_semantic_expectations_accept_bound_evidence(self):
        payload = {
            "structure_state": "MIXED",
            "supporting_evidence_ids": ["T.PATTERN_SCORE"],
            "risk_evidence_ids": ["Z.INST_FLOW"],
            "missing_evidence": [],
            "evidence_conflicts": ["价格与资金方向冲突"],
            "summary": "技术结构偏强，但机构资金方向形成冲突，需要进一步复核。",
        }
        expected = {
            "allowed_states": ["MIXED"],
            "required_any_support": ["T.PATTERN_SCORE"],
            "required_any_risk": ["Z.INST_FLOW"],
        }
        result = MODULE.evaluate_semantics(payload, expected)
        self.assertTrue(result["passed"])
        self.assertTrue(all(result["checks"].values()))

    def test_semantic_expectations_reject_negative_proof(self):
        payload = {
            "structure_state": "HEALTHY",
            "supporting_evidence_ids": ["T.MA_ALIGNMENT"],
            "risk_evidence_ids": [],
            "missing_evidence": [],
            "evidence_conflicts": [],
            "summary": "当前没有机构参与，因此结构健康。",
        }
        expected = {
            "allowed_states": ["HEALTHY", "MIXED"],
            "required_any_support": ["T.MA_ALIGNMENT"],
            "forbidden_summary_patterns": ["没有机构"],
        }
        result = MODULE.evaluate_semantics(payload, expected)
        self.assertFalse(result["passed"])
        self.assertFalse(result["checks"]["forbidden_summary"])

    def test_aggregate_disqualifies_state_collapse(self):
        runs = []
        for index in range(8):
            runs.append(
                {
                    "case_id": f"case-{index}",
                    "contract_accepted": True,
                    "semantic": {"passed": index < 6},
                    "payload": {"structure_state": "EXHAUSTED"},
                    "elapsed_ms": 1000,
                    "thinking_chars": 0,
                    "eval_tokens_per_second": 10,
                    "error": "",
                }
            )
        summary = MODULE.aggregate_model("example", runs)
        self.assertEqual(summary["status"], "DISQUALIFIED")
        self.assertEqual(summary["max_state_share"], 1.0)
        self.assertFalse(summary["gates"]["max_state_share_le_0_75"])

    def test_aggregate_disqualifies_hard_contract_issue(self):
        runs = [
            {
                "case_id": "case-1",
                "contract_accepted": False,
                "semantic": {"passed": False},
                "payload": {},
                "elapsed_ms": 500,
                "thinking_chars": 0,
                "eval_tokens_per_second": 0,
                "error": "ValueError:L2_OBSERVER_AUTHORITY_FIELD:risk_score",
            }
        ]
        summary = MODULE.aggregate_model("example", runs)
        self.assertEqual(summary["hard_contract_violations"], 1)
        self.assertEqual(summary["status"], "DISQUALIFIED")


if __name__ == "__main__":
    unittest.main()
