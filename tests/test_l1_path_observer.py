import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PATH = ROOT / "02_brain/lib/l1_path_observer.py"
spec = importlib.util.spec_from_file_location("zhulong_test_l1_path_observer", PATH)
MOD = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = MOD
spec.loader.exec_module(MOD)


class L1PathObserverTest(unittest.TestCase):
    def test_board_limit_and_account_context(self):
        self.assertEqual(MOD.board_limit_pct("600000.SH"), 10.0)
        self.assertEqual(MOD.board_limit_pct("300001.SZ"), 20.0)
        self.assertEqual(MOD.board_limit_pct("920001.BJ"), 30.0)
        self.assertEqual(MOD.board_limit_pct("600000.SH", True), 5.0)
        self.assertEqual(
            MOD.account_context("920001.BJ", False)["status"], "OBSERVE_ONLY_BJ"
        )
        self.assertFalse(
            MOD.account_context("300716.SZ", True)["allow_execution"]
        )

    def test_scores_are_independent_not_normalized_probabilities(self):
        features = MOD.PathFeatures(
            ret_1d=4.0,
            ret_2d=2.0,
            ret_3d=8.0,
            positive_days_3d=3,
            today_gain_share_of_3d=0.5,
            rps_10=96,
            rps_10_change_3d=10,
            volume_ratio_prev5=1.5,
            continuous_volume_expansion=True,
            close_above_ma20=True,
            breakout_above_ma20=True,
            close_location=0.9,
        )
        result = MOD.classify_path(features)
        self.assertEqual(
            result["path_scores_type"], "independent_evidence_scores"
        )
        self.assertGreater(sum(result["path_scores"].values()), 1.0)
        self.assertFalse(result["generate_trade"])
        self.assertTrue(result["observer_only"])

    def test_mixed_evidence_can_fail_to_unclassified(self):
        features = MOD.PathFeatures(
            ret_1d=5.0,
            ret_2d=1.0,
            ret_3d=7.0,
            positive_days_3d=2,
            today_gain_share_of_3d=0.8,
            rps_10=90,
            rps_10_change_3d=7,
            turnover_zscore_20=3.0,
            volume_ratio_prev5=2.7,
            continuous_volume_expansion=True,
            single_day_volume_explosion=True,
            close_above_ma20=True,
            close_location=0.8,
            upper_shadow_ratio=0.3,
        )
        result = MOD.classify_path(features, min_score=0.0, min_margin=0.5)
        self.assertEqual(result["primary_path"], MOD.UNCLASSIFIED)
        self.assertIn("mixed_path_evidence", result["warnings"])

    def test_fresh_breakout_classifies_without_llm(self):
        features = MOD.PathFeatures(
            ret_1d=3.0,
            ret_3d=4.0,
            positive_days_3d=2,
            rps_10=82,
            rps_10_change_3d=12,
            volume_ratio_prev5=1.5,
            close_above_ma20=True,
            breakout_above_ma20=True,
            breakout_above_20d_high=True,
            close_location=0.85,
        )
        result = MOD.classify_path(features)
        self.assertEqual(result["primary_path"], "FRESH_IGNITION")
        self.assertGreater(result["primary_confidence"], 0)


if __name__ == "__main__":
    unittest.main()
