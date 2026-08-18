import importlib.util
import json
import sys
import unittest
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "l2_l3_model_bakeoff_test_module",
    ROOT / "tools" / "l2_l3_model_bakeoff.py",
)
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)
FIXTURE = ROOT / "tests" / "fixtures" / "l2_l3_evidence_review_bakeoff_v2.json"


class L2L3ModelBakeoffTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.suite = MODULE.load_suite(FIXTURE)

    def test_suite_is_balanced_and_has_twenty_four_cases(self):
        cases = self.suite["cases"]
        self.assertEqual(len(cases), 24)
        l2_counts = Counter(
            case["l2"]["expected"]["allowed_interpretation_status"][0]
            for case in cases
        )
        thesis_counts = Counter(
            case["l3"]["expected"]["allowed_thesis_states"][0] for case in cases
        )
        lifecycle_counts = Counter(
            case["l3"]["expected"]["allowed_lifecycle"][0] for case in cases
        )
        self.assertEqual(set(l2_counts.values()), {8})
        self.assertEqual(set(thesis_counts.values()), {6})
        self.assertEqual(set(lifecycle_counts.values()), {6})

    def test_l2_contract_and_semantics_accept_valid_payload(self):
        case = self.suite["cases"][1]
        payload = {
            "contract_version": MODULE.L2_CONTRACT,
            "interpretation_status": "REFUTED",
            "conflict_present": True,
            "evidence_ids": ["T.TREND"],
            "conflict_evidence_ids": ["Z.INST_FLOW"],
            "missing_evidence_ids": [],
            "confidence": "HIGH",
            "summary": "技术结构偏强但机构方向转弱，候选解释没有处理这组冲突。",
        }
        parsed = MODULE.parse_and_validate_contract(
            json.dumps(payload, ensure_ascii=False), case, "l2"
        )
        result = MODULE.evaluate_payload(parsed, case, "l2")
        self.assertTrue(result["strict_pass"])
        self.assertTrue(result["safe_pass"])

    def test_l2_direction_reversal_is_not_relaxed(self):
        case = self.suite["cases"][1]
        payload = {
            "contract_version": MODULE.L2_CONTRACT,
            "interpretation_status": "SUPPORTED",
            "conflict_present": False,
            "evidence_ids": ["T.TREND"],
            "conflict_evidence_ids": [],
            "missing_evidence_ids": [],
            "confidence": "HIGH",
            "summary": "技术结构偏强且机构资金流入，因此解释完全一致。",
        }
        result = MODULE.evaluate_payload(payload, case, "l2")
        self.assertFalse(result["safe_pass"])
        self.assertIn(
            "FORBIDDEN_INTERPRETATION_STATUS:SUPPORTED",
            result["direction_errors"],
        )
        self.assertTrue(any(item.startswith("FORBIDDEN_CLAIM") for item in result["direction_errors"]))

    def test_summary_with_trade_instruction_or_new_number_is_fatal(self):
        case = self.suite["cases"][0]
        payload = {
            "contract_version": MODULE.L2_CONTRACT,
            "interpretation_status": "SUPPORTED",
            "conflict_present": False,
            "evidence_ids": ["T.TREND"],
            "conflict_evidence_ids": [],
            "missing_evidence_ids": [],
            "confidence": "HIGH",
            "summary": "趋势证据有效，建议买入并把目标价格设置为二十点五。价格编号为5。",
        }
        result = MODULE.evaluate_payload(payload, case, "l2")
        self.assertIn("TRADE_INSTRUCTION", result["fatal_errors"])
        self.assertIn("INVENTED_NUMBER", result["fatal_errors"])

    def test_l3_contract_accepts_evidence_bound_argument(self):
        case = self.suite["cases"][1]
        payload = {
            "contract_version": MODULE.L3_CONTRACT,
            "lifecycle_stage": "CONTINUATION",
            "thesis_state": "MIXED",
            "bull_evidence_ids": ["T.TREND"],
            "bear_evidence_ids": ["Z.INST_FLOW"],
            "contradiction_evidence_ids": ["Z.INST_FLOW"],
            "missing_evidence_ids": [],
            "selected_invalidation_ids": ["I.PRICE_WEAKEN"],
            "confidence": "MEDIUM",
            "abstain": False,
            "summary": "技术趋势仍然偏强，但机构方向和融资方向同步转弱，正反证据并存，论点只能保持混合。",
        }
        parsed = MODULE.parse_and_validate_contract(
            json.dumps(payload, ensure_ascii=False), case, "l3"
        )
        result = MODULE.evaluate_payload(parsed, case, "l3")
        self.assertTrue(result["strict_pass"])

    def test_l3_unknown_requires_abstention_and_missing_evidence(self):
        case = self.suite["cases"][3]
        payload = {
            "contract_version": MODULE.L3_CONTRACT,
            "lifecycle_stage": "UNCLEAR",
            "thesis_state": "UNKNOWN",
            "bull_evidence_ids": [],
            "bear_evidence_ids": [],
            "contradiction_evidence_ids": [],
            "missing_evidence_ids": ["M.TREND", "M.RPS", "M.FLOW"],
            "selected_invalidation_ids": [],
            "confidence": "LOW",
            "abstain": True,
            "summary": "趋势、相对强度和资金方向均缺少有效证据，当前无法确认生命周期或论点状态，应等待证据补齐。",
        }
        result = MODULE.evaluate_payload(payload, case, "l3")
        self.assertTrue(result["strict_pass"])

    def test_aggregate_rejects_constant_output_even_above_dummy(self):
        cases = self.suite["cases"]
        runs = []
        for case in cases:
            expected = case["l2"]["expected"]
            evaluation = {
                "strict_pass": "SUPPORTED" in expected["allowed_interpretation_status"],
                "safe_pass": True,
                "fatal_errors": [],
                "direction_errors": [],
                "judgment_errors": [],
                "checks": {
                    "primary_label": "SUPPORTED" in expected["allowed_interpretation_status"],
                    "conflict": not expected["conflict_expected"],
                    "evidence": True,
                    "conflict_evidence": not expected["required_any_conflict_ids"],
                    "missing_evidence": not expected["required_missing_ids"],
                },
            }
            runs.append(
                {
                    "case_id": case["case_id"],
                    "contract_accepted": True,
                    "evaluation": evaluation,
                    "payload": {
                        "interpretation_status": "SUPPORTED",
                        "conflict_present": False,
                    },
                    "elapsed_ms": 1000,
                    "eval_tokens_per_second": 10,
                    "thinking_chars": 0,
                }
            )
        summary = MODULE.aggregate_model("constant", runs, cases, "l2")
        self.assertAlmostEqual(summary["dummy_accuracy"], 1 / 3, places=4)
        self.assertEqual(summary["max_primary_share"], 1.0)
        self.assertEqual(summary["status"], "DISQUALIFIED")

    def test_forbidden_pattern_ignores_explicit_refutation(self):
        self.assertFalse(
            MODULE._asserts_forbidden_pattern(
                "现有输入不足以确认结构健康，关键证据仍然缺失。",
                "确认.*健康",
            )
        )
        self.assertFalse(
            MODULE._asserts_forbidden_pattern(
                "解释声称财务质量改善，但这与现金流证据矛盾。",
                "财务.*改善",
            )
        )
        self.assertFalse(
            MODULE._asserts_forbidden_pattern(
                "现金流弱于利润，构成冲突，但现有证据支持这句平衡解释。",
                "现金流.*支持",
            )
        )
        self.assertFalse(
            MODULE._asserts_forbidden_pattern(
                "相对强度升高并不支持筹码稳定。",
                "筹码.*稳定",
            )
        )
        self.assertFalse(
            MODULE._asserts_forbidden_pattern(
                "候选论点认为风险不会影响健康延续，但证据显示相反。",
                "不会影响",
            )
        )
        self.assertFalse(
            MODULE._asserts_forbidden_pattern(
                "候选论点认为风险不会影响健康延续，极端换手仍属正常状态。"
                "但证据显示公告包含明确风险提示。",
                "正常状态",
            )
        )
        self.assertFalse(
            MODULE._asserts_forbidden_pattern(
                "缺乏席位连续参与及趋势结构的稳定延续。",
                "稳定.*延续",
            )
        )
        self.assertFalse(
            MODULE._asserts_forbidden_pattern(
                "缺少后续确认，导致新趋势启动的论点存在不确定性。",
                "确认.*启动",
            )
        )
        self.assertFalse(
            MODULE._asserts_forbidden_pattern(
                "个股表现优于板块且刚出现突破，但板块广度偏弱使论点无法完全确认。",
                "板块.*确认",
            )
        )
        self.assertFalse(
            MODULE._asserts_forbidden_pattern(
                "资金面虽为零但无机构行为证据。",
                "无机构",
            )
        )
        self.assertFalse(
            MODULE._asserts_forbidden_pattern(
                "成交活跃度没有同步改善，且板块广度轻微改善。",
                "成交.*改善",
            )
        )
        self.assertFalse(
            MODULE._asserts_forbidden_pattern(
                "趋势快照已经陈旧，且无其他有效证据。",
                "快照.*有效",
            )
        )
        self.assertTrue(
            MODULE._asserts_forbidden_pattern(
                "陈旧快照能够证明当前趋势健康。",
                "当前.*健康",
            )
        )
        self.assertTrue(
            MODULE._asserts_forbidden_pattern(
                "资金面为零，说明没有机构参与本次上涨。",
                "没有机构",
            )
        )
        self.assertTrue(
            MODULE._asserts_forbidden_pattern(
                "风险不会影响健康延续。",
                "不会影响",
            )
        )
        self.assertTrue(
            MODULE._asserts_forbidden_pattern(
                "板块已经形成共振确认。",
                "板块.*确认",
            )
        )
        self.assertTrue(
            MODULE._asserts_forbidden_pattern(
                "成交活跃度已经改善。",
                "成交.*改善",
            )
        )
        self.assertTrue(
            MODULE._asserts_forbidden_pattern(
                "当前快照依然有效。",
                "快照.*有效",
            )
        )


if __name__ == "__main__":
    unittest.main()
