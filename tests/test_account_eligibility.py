#!/usr/bin/env python3

import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "zhulong_test_account_eligibility", ROOT / "05_shadow/lib/account_eligibility.py")
MOD = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = MOD
spec.loader.exec_module(MOD)


class AccountEligibilityTest(unittest.TestCase):
    def test_regular_a_share_is_eligible(self):
        r = MOD.evaluate_account_eligibility("600000.SH", "浦发银行", False)
        self.assertTrue(r.allow_execution)
        self.assertEqual(r.qualification_status, "ELIGIBLE")

    def test_st_is_observation_only_even_on_shenzhen(self):
        r = MOD.evaluate_account_eligibility("300716.SZ", "ST泉为", True)
        self.assertFalse(r.allow_execution)
        self.assertEqual(r.reason_code, "ACCOUNT_INELIGIBLE_ST")
        self.assertTrue(r.observation_retained)

    def test_bj_is_observation_only_for_default_profile(self):
        r = MOD.evaluate_account_eligibility("920725.BJ", "惠丰钻石", False)
        self.assertFalse(r.allow_execution)
        self.assertEqual(r.qualification_status, "OBSERVE_ONLY_BJ")

    def test_bj_enabled_profile_must_be_explicit(self):
        r = MOD.evaluate_account_eligibility(
            "920725.BJ", "惠丰钻石", False, account_profile="BJ_ENABLED")
        self.assertTrue(r.allow_execution)

    def test_unknown_profile_fails_to_personal_default(self):
        r = MOD.evaluate_account_eligibility(
            "920725.BJ", "惠丰钻石", False, account_profile="UNKNOWN")
        self.assertFalse(r.allow_execution)


if __name__ == "__main__":
    unittest.main()
