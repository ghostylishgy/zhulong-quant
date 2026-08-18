import importlib.util
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "02_brain" / "lib" / "local_model_microtasks.py"
SPEC = importlib.util.spec_from_file_location("zhulong_test_local_model_microtasks", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class LocalModelMicrotasksTest(unittest.TestCase):
    def test_l2_missing_task_exposes_only_deterministic_gaps(self):
        task = MODULE.build_l2_missing_task(
            symbol="600000.SH",
            evidence_ledger=(
                "[P.PCT_CHG] +3.20%\n[P.CLOSE] 10.00\n[P.AMOUNT] 80000000 yuan\n"
                "[P.TURNOVER] 4.00%\n[P.VOL_RATIO] 1.50\n"
                "[T.PATTERN_SCORE] 65.0\n[T.MA_ALIGNMENT] true\n[T.PATTERN_NAME] 均线多头"
            ),
            deterministic_interpretation="确定性 L2 认为当前结构可进入下一层复核。",
        )
        self.assertEqual(
            set(task["required_missing_ids"]),
            {"M.MULTIDAY_PATH", "M.SECTOR_CONTEXT", "M.CURRENT_NEWS", "M.FUNDAMENTAL_QUALITY"},
        )
        self.assertEqual(set(task["allowed_ids"]), set(task["required_missing_ids"]))
        self.assertNotIn("M.PRICE_STATUS", task["allowed_ids"])
        self.assertNotIn("required_missing_ids", str(task["payload"]))

    def test_l2_output_rejects_unbound_id(self):
        task = MODULE.build_l2_missing_task(
            symbol="600000.SH",
            evidence_ledger="[P.PCT_CHG] +3.20%\n[P.CLOSE] 10.00",
            deterministic_interpretation="确定性解释",
        )
        payload = {
            "contract_version": MODULE.L2_CONTRACT_VERSION,
            "missing_evidence_ids": task["required_missing_ids"] + ["M.PRICE_STATUS"],
        }
        with self.assertRaisesRegex(MODULE.MicrotaskContractError, "UNBOUND_ID"):
            MODULE.validate_l2_missing_output(payload, task)

    def test_l2_output_accepts_exact_required_gaps(self):
        task = MODULE.build_l2_missing_task(
            symbol="600000.SH",
            evidence_ledger="[P.PCT_CHG] +3.20%\n[P.CLOSE] 10.00",
            deterministic_interpretation="确定性解释",
        )
        result = MODULE.validate_l2_missing_output(
            {
                "contract_version": MODULE.L2_CONTRACT_VERSION,
                "missing_evidence_ids": task["required_missing_ids"],
            },
            task,
        )
        self.assertEqual(result["missing_evidence_ids"], task["required_missing_ids"])

    def test_l3_task_hides_condition_kinds_and_rejects_completion(self):
        task = MODULE.build_l3_invalidation_task(
            symbol="600000.SH",
            deterministic_reasoning="当前量价结构具备建设性，但仍需后续验证。",
            falsifiable_conditions=["收盘价跌破审计日参考且未快速收回", "相对强度继续下降"],
        )
        self.assertNotIn("invalidation_ids", str(task["payload"]))
        self.assertEqual(set(task["invalidation_ids"]), {"K.001", "K.002"})
        with self.assertRaisesRegex(MODULE.MicrotaskContractError, "NON_INVALIDATION_SELECTED"):
            MODULE.validate_l3_invalidation_output(
                {
                    "contract_version": MODULE.L3_CONTRACT_VERSION,
                    "selected_invalidation_ids": ["K.001", "K.901"],
                },
                task,
            )

    def test_l3_output_accepts_bound_invalidation_subset(self):
        task = MODULE.build_l3_invalidation_task(
            symbol="600000.SH",
            deterministic_reasoning="当前量价结构具备建设性。",
            falsifiable_conditions=["收盘价跌破审计日参考且未快速收回", "相对强度继续下降"],
        )
        result = MODULE.validate_l3_invalidation_output(
            {
                "contract_version": MODULE.L3_CONTRACT_VERSION,
                "selected_invalidation_ids": ["K.002"],
            },
            task,
        )
        self.assertEqual(result["selected_conditions"], ["相对强度继续下降"])


if __name__ == "__main__":
    unittest.main()
