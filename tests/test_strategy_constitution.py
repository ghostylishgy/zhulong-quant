#!/usr/bin/env python3

import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "config" / "constitution.py"
SPEC = importlib.util.spec_from_file_location(
    "zhulong_test_strategy_constitution",
    MODULE_PATH,
)
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class StrategyConstitutionTest(unittest.TestCase):
    def test_primary_competence_boundary_is_orderly_tradeable_market(self):
        self.assertEqual(
            MODULE.MetaRules.PRIMARY_OPERATING_REGIME,
            "ORDERLY_TRADEABLE_MARKET",
        )

    def test_single_extreme_event_cannot_change_strategy(self):
        self.assertFalse(
            MODULE.MetaRules.SINGLE_EXTREME_EVENT_MAY_CHANGE_STRATEGY
        )
        self.assertEqual(
            MODULE.MetaRules.EXTREME_REGIME_POLICY,
            "OBSERVE_RECORD_ANALYZE",
        )

    def test_extreme_event_actions_are_observational_only(self):
        self.assertEqual(
            MODULE.MetaRules.EXTREME_EVENT_ALLOWED_ACTIONS,
            ("RECORD", "LABEL", "POST_ANALYZE"),
        )
        blocked = set(MODULE.MetaRules.EXTREME_EVENT_BLOCKED_ACTIONS)
        self.assertIn("AUTO_TUNE", blocked)
        self.assertIn("AUTO_RELAX_GATE", blocked)
        self.assertIn("AUTO_ADD_STRATEGY", blocked)
        self.assertIn("AUTO_PROMOTE_TO_PRODUCTION", blocked)

    def test_strategy_change_requires_repeatable_forward_evidence_and_review(self):
        requirements = set(MODULE.MetaRules.STRATEGY_CHANGE_REQUIRES)
        self.assertIn("RECURRENT_PATTERN_OR_PERSISTENT_INERTIA", requirements)
        self.assertIn("MULTIPLE_INDEPENDENT_EVENTS", requirements)
        self.assertIn("FORWARD_OUTCOME_EVIDENCE", requirements)
        self.assertIn("HUMAN_DESIGN_REVIEW", requirements)


if __name__ == "__main__":
    unittest.main()
