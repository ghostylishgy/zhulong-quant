import unittest

from tools.l2_l3_v03_contract import (
    CONTRACT_VERSION,
    ContractError,
    build_evidence_packet,
    build_l3_model_payload,
    reduce_relation,
    validate_evidence_card,
    validate_l3_model_output,
)


class L2L3V03ContractTest(unittest.TestCase):
    def _claim(self, **overrides):
        value = {
            "claim_id": "C.FLOW_INST",
            "claim_text": "机构资金持续净流出",
            "dimension": "FLOW",
            "variable": "INST_FLOW",
            "claim_role": "RISK",
            "claim_state": "ACTIVE",
            "as_of": "2026-08-03",
        }
        value.update(overrides)
        return value

    def _condition(self, **overrides):
        value = {
            "condition_id": "K.FLOW_INST_RECOVER",
            "hypothesis": "若机构资金由净流出转为净流入",
            "dimension": "FLOW",
            "variable": "INST_FLOW",
            "evidence_ids": ["E1"],
            "as_of": "2026-08-03",
        }
        value.update(overrides)
        return value

    def test_evidence_packet_is_neutral_and_derives_missing(self):
        packet = build_evidence_packet(
            symbol="000001.SZ",
            as_of="2026-08-03",
            evidence_cards=[{
                "evidence_id": "E1",
                "dimension": "FLOW",
                "normalized_fact": "机构资金净流出",
                "quality": "VALID",
                "as_of": "2026-08-03",
            }],
            expected_evidence_ids=["E1", "E2"],
        )
        self.assertEqual(packet["missing_evidence_ids"], ["E2"])
        self.assertNotIn("market_role", packet["evidence_cards"][0])
        self.assertEqual(packet["contract_version"], CONTRACT_VERSION)

    def test_evidence_card_rejects_market_role_and_stance(self):
        card = {
            "evidence_id": "E1",
            "dimension": "FLOW",
            "normalized_fact": "机构资金净流出",
            "quality": "VALID",
            "as_of": "2026-08-03",
            "market_role": "RISK",
        }
        with self.assertRaisesRegex(ContractError, "UNEXPECTED_KEYS"):
            validate_evidence_card(card)

    def test_signal_and_data_conflict_are_separate(self):
        packet = build_evidence_packet(
            symbol="000001.SZ",
            as_of="2026-08-03",
            evidence_cards=[
                {"evidence_id": "E1", "dimension": "TREND", "normalized_fact": "趋势偏强", "quality": "VALID", "as_of": "2026-08-03"},
                {"evidence_id": "E2", "dimension": "FLOW", "normalized_fact": "机构资金净流出", "quality": "VALID", "as_of": "2026-08-03"},
            ],
            signal_divergences=[{"left_id": "E1", "right_id": "E2", "type": "CROSS_DIMENSION_DIVERGENCE"}],
            data_conflicts=[{"evidence_ids": ["E1", "E2"], "type": "SAME_SOURCE_CONTRADICTION"}],
        )
        self.assertEqual(packet["signal_divergences"][0]["type"], "CROSS_DIMENSION_DIVERGENCE")
        self.assertEqual(packet["data_conflicts"][0]["type"], "SAME_SOURCE_CONTRADICTION")

    def test_atomic_claim_and_condition_hide_semantic_axes(self):
        payload = build_l3_model_payload(self._claim(), self._condition())
        self.assertEqual(set(payload), {"claim", "condition_card"})
        self.assertEqual(set(payload["claim"]), {"claim_id", "claim_text"})
        self.assertEqual(set(payload["condition_card"]), {"condition_id", "hypothesis", "evidence_ids"})
        self.assertNotIn("claim_role", str(payload))
        self.assertNotIn("claim_state", str(payload))
        self.assertNotIn("dimension", str(payload))
        self.assertNotIn("as_of", str(payload))

    def test_atomicity_and_same_variable_gate(self):
        with self.assertRaisesRegex(ContractError, "NOT_ATOMIC"):
            build_l3_model_payload(
                self._claim(claim_text="机构资金持续净流出且趋势结构偏弱"),
                self._condition(),
            )
        with self.assertRaisesRegex(ContractError, "VARIABLE_MISMATCH"):
            build_l3_model_payload(self._claim(), self._condition(variable="MARGIN_FLOW"))

    def test_l3_output_is_closed_and_evidence_bound(self):
        result = validate_l3_model_output(
            {"relation": "CONTRADICTS", "bound_evidence_ids": ["E1"]},
            ["E1", "E2"],
        )
        self.assertEqual(result["relation"], "CONTRADICTS")
        with self.assertRaisesRegex(ContractError, "UNBOUND_EVIDENCE"):
            validate_l3_model_output(
                {"relation": "SUPPORTS", "bound_evidence_ids": ["E9"]},
                ["E1"],
            )

    def test_reducer_is_claim_level_and_handles_lifecycle_hint(self):
        self.assertEqual(
            reduce_relation(claim_role="RISK", claim_state="ACTIVE", relation="CONTRADICTS"),
            {"packet_type": "CLAIM_RISK_RELIEVED", "lifecycle_hint": None},
        )
        self.assertEqual(
            reduce_relation(claim_role="SUPPORTING", claim_state="ACTIVE", relation="CONTRADICTS"),
            {"packet_type": "CLAIM_SUPPORT_CHALLENGED", "lifecycle_hint": "INVALIDATION_CANDIDATE"},
        )
        self.assertEqual(
            reduce_relation(claim_role="SUPPORTING", claim_state="INVALIDATED", relation="SUPPORTS"),
            {"packet_type": "CLAIM_SUPPORT_CONFIRMED", "lifecycle_hint": "REVALIDATION_CANDIDATE"},
        )


if __name__ == "__main__":
    unittest.main()
