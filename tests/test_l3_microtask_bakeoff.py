import unittest

from tools.l2_l3_model_bakeoff import DEFAULT_CASES, load_suite
from tools.l3_microtask_bakeoff import evaluate, task_payload


def _case(required, allowed, forbidden=(), empty_selection_allowed=False):
    return {
        "case_id": "evaluation_probe",
        "evidence": {},
        "condition_options": {},
        "l3": {
            "expected": {
                "required_any_invalidation_ids": required,
                "allowed_invalidation_ids": allowed,
                "forbidden_invalidation_ids": list(forbidden),
                "empty_selection_allowed": empty_selection_allowed,
                "forbidden_summary_patterns": [],
            }
        },
    }


class InvalidationScoringTest(unittest.TestCase):
    def test_fixture_separates_condition_kinds_and_expected_selection(self):
        suite = load_suite(DEFAULT_CASES)
        self.assertEqual(suite["suite_version"], "L2_L3_EVIDENCE_REVIEW_BAKEOFF_V2_2")
        for case in suite["cases"]:
            expected = case["l3"]["expected"]
            options = case["condition_options"]
            expected_allowed = {
                condition_id
                for condition_id, item in options.items()
                if item["condition_kind"] == "THESIS_INVALIDATION"
            }
            self.assertEqual(set(expected["allowed_invalidation_ids"]), expected_allowed)
            self.assertEqual(
                set(expected["forbidden_invalidation_ids"]),
                set(options).difference(expected_allowed),
            )
            self.assertEqual(expected["empty_selection_allowed"], not expected_allowed)

    def test_model_payload_uses_neutral_condition_options(self):
        suite = load_suite(DEFAULT_CASES)
        case = next(item for item in suite["cases"] if item["case_id"] == "price_flow_conflict")
        payload = task_payload(case, "invalidation")
        options = {item["condition_id"]: item for item in payload["condition_options"]}
        self.assertEqual(set(options), {"I.PRICE_WEAKEN", "I.FLOW_RECOVER"})
        self.assertNotIn("condition_kind", options["I.FLOW_RECOVER"])
        self.assertNotIn("invalidation_id", options["I.FLOW_RECOVER"])

    def test_valid_subset_and_multiple_selection_pass(self):
        case = _case(["I.A", "I.B"], ["I.A", "I.B"])
        for selected in (["I.A"], ["I.A", "I.B"]):
            result = evaluate(
                {"selected_invalidation_ids": selected, "summary": ""},
                case,
                "invalidation",
            )
            self.assertTrue(result["strict_pass"])
            self.assertTrue(result["checks"]["invalidation_precision"])
            self.assertTrue(result["checks"]["invalidation_recall"])

    def test_unexpected_selection_fails_precision(self):
        case = _case(["I.A"], ["I.A"], ["C.RECHECK"])
        result = evaluate(
            {"selected_invalidation_ids": ["I.A", "C.RECHECK"], "summary": ""},
            case,
            "invalidation",
        )
        self.assertFalse(result["strict_pass"])
        self.assertFalse(result["checks"]["invalidation_precision"])
        self.assertFalse(result["checks"]["forbidden_selection"])
        self.assertIn("UNEXPECTED_INVALIDATION_SELECTION:C.RECHECK", result["judgment_errors"])
        self.assertIn("FORBIDDEN_INVALIDATION_SELECTION:C.RECHECK", result["judgment_errors"])

    def test_missing_selection_fails_recall(self):
        case = _case(["I.A"], ["I.A"])
        result = evaluate(
            {"selected_invalidation_ids": [], "summary": ""},
            case,
            "invalidation",
        )
        self.assertFalse(result["strict_pass"])
        self.assertFalse(result["checks"]["invalidation_recall"])

    def test_empty_selection_is_valid_when_no_invalidation_is_expected(self):
        case = _case([], [], ["C.RECHECK"], empty_selection_allowed=True)
        result = evaluate(
            {"selected_invalidation_ids": [], "summary": ""},
            case,
            "invalidation",
        )
        self.assertTrue(result["strict_pass"])
        self.assertTrue(result["checks"]["empty_selection"])


if __name__ == "__main__":
    unittest.main()
