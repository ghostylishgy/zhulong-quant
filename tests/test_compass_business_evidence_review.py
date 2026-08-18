import copy
import importlib.util
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "tools" / "compass_business_evidence_review.py"


def load_module():
    spec = importlib.util.spec_from_file_location(
        "compass_business_evidence_review", MODULE_PATH
    )
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


MOD = load_module()


def evidence_row(
    symbol="600001.SH", eligible=True, context_only=False, scope="broad_universe_recheck"
):
    item = {
        "type": "company_main_business",
        "source": "tushare.stock_company",
        "node_id": "A_TRANSFORMER",
        "node_label": "变压器",
        "matched_field": "main_business",
        "matched_terms": ["变压器"] if eligible else ["电力设备"],
        "review_qualifying_terms": ["变压器"] if eligible else [],
        "review_qualifying": eligible,
        "evidence_specificity": "specific_node" if eligible else "generic_node_context",
        "matched_text": "变压器研发与销售" if eligible else "电力设备业务",
        "evidence_item_id": "BIZ-QUALIFIED" if eligible else "BIZ-CONTEXT",
    }
    row = {
        "source_candidate_id": "COMPASS-2026W30-A-010",
        "source_theme_name": "算电协同与电网接入",
        "compass_line_key": "A线",
        "candidate_scope": scope,
        "symbol": symbol,
        "name": "样例电气",
        "mapping_pool_unchanged": (
            "broad_universe" if scope == "broad_universe_recheck" else "review_shortlist"
        ),
        "source_review_eligible": scope != "broad_universe_recheck",
        "existing_review_manifest_eligible": False,
        "node_dictionary_theme_key": "power_grid_bottlenecks",
        "business_evidence_level": (
            "B1_COMPANY_DESCRIPTOR" if eligible else "B0_CONTEXT_ONLY_GENERIC"
        ),
        "business_relevance_status": (
            "descriptor_match_only" if eligible else "context_only_generic_node"
        ),
        "business_relevance_evidence_present": eligible,
        "business_context_evidence_present": True,
        "business_review_eligible": eligible,
        "business_review_eligibility": (
            "eligible_specific_node" if eligible else "context_only_generic_node"
        ),
        "review_qualifying_evidence_item_ids": ["BIZ-QUALIFIED"] if eligible else [],
        "descriptor_evidence": [item],
        "supporting_descriptor_evidence": [],
        "reported_business_evidence": [],
        "latest_report_period_seen": "20251231",
        "report_period_age_days": 211,
        "report_period_stale": False,
        "mainbz_query_status": "FETCHED",
        "evidence_as_of": "2026-07-30",
        "retrieval_time_semantics": "current_api_snapshot_not_historical_reconstruction",
        "historical_point_in_time_reconstructable": False,
        "manual_review_required": True,
        "auto_approved": False,
        "generate_task": False,
        "no_trade_signal": True,
        "api_errors": [],
    }
    if context_only:
        row["business_review_eligible"] = False
    return row


def signed_snapshot(rows=None):
    rows = rows or [evidence_row(), evidence_row("600002.SH", eligible=False, context_only=True)]
    payload = {
        "batch_id": "2026W30",
        "source": "Compass/ima + Tushare",
        "snapshot_type": "business_evidence",
        "protocol_version": MOD.INPUT_PROTOCOL_VERSION,
        "generated_at": "2026-07-30T22:00:00+08:00",
        "evidence_as_of": "2026-07-30",
        "mode": "dry_run",
        "dry_run": True,
        "no_trade_signal": True,
        "quality_status": "DATA_OK",
        "source_discovery_preview_sha256": "a" * 64,
        "node_dictionary_sha256": "b" * 64,
        "stats": {"generated_tasks": 0},
        "business_evidence": rows,
        "business_evidence_matches": [
            row for row in rows if row["business_relevance_evidence_present"]
        ],
        "business_context_only_matches": [
            row for row in rows
            if row["business_review_eligibility"] == "context_only_generic_node"
        ],
        "validation_tasks": [],
        "blocked_actions": MOD.BLOCKED_ACTIONS,
    }
    payload["payload_sha256"] = MOD.canonical_sha256(payload)
    return payload


def promoted_manifest(snapshot, file_sha="c" * 64):
    manifest = MOD.initialize_manifest(snapshot, file_sha)
    manifest["reviewer_id"] = "steve"
    manifest["reviewed_at"] = "2026-07-30T22:30:00+08:00"
    manifest["decisions"][0].update({
        "decision": "promote",
        "selected_evidence_item_ids": ["BIZ-QUALIFIED"],
        "review_notes": "主营描述明确命中变压器节点，允许进入后续独立适配评审。",
    })
    return manifest


class CompassBusinessEvidenceReviewTest(unittest.TestCase):
    def test_initialize_manifest_only_includes_specific_broad_rows(self):
        snapshot = signed_snapshot()
        manifest = MOD.initialize_manifest(snapshot, "c" * 64)
        self.assertEqual(len(manifest["decisions"]), 1)
        self.assertEqual(manifest["decisions"][0]["symbol"], "600001.SH")
        self.assertEqual(manifest["source_business_payload_sha256"], snapshot["payload_sha256"])
        self.assertEqual(manifest["source_discovery_preview_sha256"], "a" * 64)
        self.assertEqual(manifest["node_dictionary_sha256"], "b" * 64)

    def test_review_shortlist_row_is_not_duplicated_into_promotion_manifest(self):
        row = evidence_row(scope="review_shortlist")
        snapshot = signed_snapshot([row])
        manifest = MOD.initialize_manifest(snapshot, "c" * 64)
        self.assertEqual(manifest["decisions"], [])

    def test_payload_tamper_fails_closed(self):
        snapshot = signed_snapshot()
        snapshot["business_evidence"][0]["name"] = "被篡改"
        with self.assertRaisesRegex(ValueError, "payload_sha256 mismatch"):
            MOD.initialize_manifest(snapshot, "c" * 64)

    def test_manifest_binding_tamper_fails_closed(self):
        snapshot = signed_snapshot()
        manifest = promoted_manifest(snapshot)
        manifest["node_dictionary_sha256"] = "d" * 64
        with self.assertRaisesRegex(ValueError, "node_dictionary_sha256"):
            MOD.apply_manifest(snapshot, "c" * 64, manifest, "e" * 64)

    def test_row_hash_tamper_fails_closed(self):
        snapshot = signed_snapshot()
        manifest = promoted_manifest(snapshot)
        manifest["decisions"][0]["business_evidence_row_sha256"] = "stale"
        with self.assertRaisesRegex(ValueError, "row hash mismatch"):
            MOD.apply_manifest(snapshot, "c" * 64, manifest, "e" * 64)

    def test_promote_outputs_reviewed_artifact_but_no_task_or_pool_change(self):
        snapshot = signed_snapshot()
        manifest = promoted_manifest(snapshot)
        output = MOD.apply_manifest(snapshot, "c" * 64, manifest, "e" * 64)
        self.assertEqual(output["stats"]["promoted"], 1)
        self.assertEqual(output["stats"]["generated_tasks"], 0)
        self.assertEqual(output["validation_tasks"], [])
        candidate = output["promoted_candidates"][0]
        self.assertEqual(candidate["source_mapping_pool"], "broad_universe")
        self.assertEqual(candidate["recommended_next_pool"], "review_shortlist")
        self.assertFalse(candidate["pool_change_applied"])
        self.assertFalse(candidate["existing_review_manifest_eligible"])
        self.assertTrue(candidate["eligible_for_future_discovery_adapter"])
        self.assertFalse(candidate["generate_task"])

    def test_promote_requires_qualifying_evidence_selection(self):
        snapshot = signed_snapshot()
        manifest = promoted_manifest(snapshot)
        manifest["decisions"][0]["selected_evidence_item_ids"] = ["UNKNOWN"]
        with self.assertRaisesRegex(ValueError, "qualifying evidence selection"):
            MOD.apply_manifest(snapshot, "c" * 64, manifest, "e" * 64)

    def test_non_promote_decision_cannot_select_evidence(self):
        snapshot = signed_snapshot()
        manifest = promoted_manifest(snapshot)
        manifest["decisions"][0]["decision"] = "keep_broad"
        with self.assertRaisesRegex(ValueError, "only promote"):
            MOD.apply_manifest(snapshot, "c" * 64, manifest, "e" * 64)

    def test_applied_decision_requires_reviewer_and_timezone(self):
        snapshot = signed_snapshot()
        manifest = promoted_manifest(snapshot)
        manifest["reviewer_id"] = ""
        with self.assertRaisesRegex(ValueError, "reviewer_id"):
            MOD.apply_manifest(snapshot, "c" * 64, manifest, "e" * 64)
        manifest = promoted_manifest(snapshot)
        manifest["reviewed_at"] = "2026-07-30T22:30:00"
        with self.assertRaisesRegex(ValueError, "timezone"):
            MOD.apply_manifest(snapshot, "c" * 64, manifest, "e" * 64)

    def test_manifest_must_retain_every_reviewable_row(self):
        rows = [evidence_row(), evidence_row("600003.SH")]
        snapshot = signed_snapshot(rows)
        manifest = MOD.initialize_manifest(snapshot, "c" * 64)
        manifest["decisions"].pop()
        with self.assertRaisesRegex(ValueError, "retain every reviewable row"):
            MOD.apply_manifest(snapshot, "c" * 64, manifest, "e" * 64)

    def test_bj_row_can_be_reviewed_but_never_becomes_task(self):
        snapshot = signed_snapshot([evidence_row("920001.BJ")])
        manifest = promoted_manifest(snapshot)
        output = MOD.apply_manifest(snapshot, "c" * 64, manifest, "e" * 64)
        candidate = output["promoted_candidates"][0]
        self.assertEqual(candidate["symbol"], "920001.BJ")
        self.assertFalse(candidate["generate_task"])
        self.assertEqual(output["validation_tasks"], [])


if __name__ == "__main__":
    unittest.main()
