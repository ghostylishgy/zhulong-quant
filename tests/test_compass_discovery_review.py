import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "tools" / "compass_discovery_review.py"
SPEC = importlib.util.spec_from_file_location("compass_discovery_review", MODULE_PATH)
assert SPEC and SPEC.loader
review = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(review)


def preview_payload(evidence_level="medium"):
    row = {
        "preview_row_id": "COMPASS-2026W28-A-001-DISC-001",
        "source_candidate_id": "COMPASS-2026W28-A-001",
        "source_theme_name": "特高压",
        "source_object_class": "theme_candidate",
        "compass_line": "A线",
        "compass_line_key": "A线",
        "priority": "high",
        "supply_chain_nodes": ["分接开关"],
        "bottleneck_hypotheses": ["订单传导"],
        "discovery_keywords": ["特高压设备"],
        "questions_for_zhulong": ["订单是否增长？"],
        "line_validation_questions": ["毛利率是否改善？"],
        "symbol": "000001.SZ",
        "name": "Sample",
        "industry": "电气设备",
        "mapping_pool": "review_shortlist",
        "mapping_evidence_level": "L1",
        "evidence_level": evidence_level,
        "evidence_scope": "mapping_only_not_business_relevance",
        "evidence_items": [{
            "evidence_item_id": "EVID-001",
            "type": "classification_membership",
            "source": "tushare.ths_member",
            "index_code": "885425.TI",
            "business_relevance_proven": False,
        }],
        "evidence_sources": ["tushare.ths_member"],
        "source_memberships": [{"source": "tushare.ths_member"}],
        "business_relevance_status": "unverified",
        "business_relevance_evidence_present": False,
        "shortlist_eligible": True,
        "review_eligible": True,
        "sample_only": False,
        "manual_review_required": True,
        "ticker_status": "preview_only_not_task_resolved",
        "generate_task": False,
        "no_trade_signal": True,
    }
    return {
        "batch_id": "2026W28",
        "source_as_of": "2026-07-10",
        "protocol_version": review.SUPPORTED_PREVIEW_PROTOCOL,
        "mode": "dry_run",
        "no_trade_signal": True,
        "stats": {"generated_tasks": 0},
        "review_shortlist": [row],
        "preview_rows": [row],
        "validation_tasks": [],
    }


def approved_manifest(preview, preview_sha="preview-sha"):
    manifest = review.initialize_manifest(preview, preview_sha)
    manifest["reviewer_id"] = "steve"
    manifest["reviewed_at"] = "2026-07-11T20:00:00+08:00"
    manifest["validation_as_of"] = "2026-07-11"
    manifest["decisions"][0].update({
        "decision": "approve",
        "business_relevance_confirmed": True,
        "business_relevance_basis": "manual_verified_industry_mapping",
        "supporting_evidence_item_ids": ["EVID-001"],
        "review_notes": "人工确认该公司与主题存在业务映射，RPS 数据可用于后续只读验证。",
    })
    return manifest


class CompassDiscoveryReviewTest(unittest.TestCase):
    def test_initialize_manifest_binds_every_row_hash(self):
        preview = preview_payload()
        manifest = review.initialize_manifest(preview, "preview-sha")
        self.assertEqual(manifest["source_preview_sha256"], "preview-sha")
        self.assertEqual(manifest["compass_source_as_of"], "2026-07-10")
        self.assertIsNone(manifest["validation_as_of"])
        self.assertEqual(manifest["decisions"][0]["preview_row_sha256"], review.canonical_sha256(preview["preview_rows"][0]))
        self.assertEqual(manifest["decisions"][0]["decision"], "pending")

    def test_stale_row_hash_fails_closed(self):
        preview = preview_payload()
        manifest = approved_manifest(preview)
        manifest["decisions"][0]["preview_row_sha256"] = "stale"
        with self.assertRaisesRegex(ValueError, "row hash mismatch"):
            review.apply_manifest(preview, "preview-sha", manifest, "manifest-sha")

    def test_medium_approval_creates_reviewed_candidate_but_no_task(self):
        preview = preview_payload()
        manifest = approved_manifest(preview)
        payload = review.apply_manifest(preview, "preview-sha", manifest, "manifest-sha")
        self.assertEqual(payload["stats"]["reviewed_candidates"], 1)
        self.assertEqual(payload["stats"]["generated_tasks"], 0)
        self.assertEqual(payload["validation_tasks"], [])
        candidate = payload["reviewed_candidates"][0]
        self.assertIs(candidate["business_relevance_evidence_present"], True)
        self.assertEqual(candidate["business_relevance_status"], "human_confirmed")
        self.assertEqual(candidate["mapping_evidence_level"], "L1")
        self.assertEqual(candidate["supporting_evidence_item_ids"], ["EVID-001"])
        self.assertEqual(candidate["supporting_metrics"], {})
        self.assertIs(candidate["eligible_for_separate_dry_run_validation_task"], True)
        self.assertIs(candidate["generate_task"], False)
        self.assertEqual(candidate["bottleneck_hypotheses"], ["订单传导"])
        self.assertEqual(candidate["line_validation_questions"], ["毛利率是否改善？"])
        self.assertEqual(candidate["compass_source_as_of"], "2026-07-10")
        self.assertEqual(candidate["validation_as_of"], "2026-07-11")

    def test_approval_requires_validation_as_of(self):
        preview = preview_payload()
        manifest = approved_manifest(preview)
        manifest["validation_as_of"] = None
        with self.assertRaisesRegex(ValueError, "validation_as_of"):
            review.apply_manifest(preview, "preview-sha", manifest, "manifest-sha")

    def test_validation_as_of_cannot_precede_source(self):
        preview = preview_payload()
        manifest = approved_manifest(preview)
        manifest["validation_as_of"] = "2026-07-09"
        with self.assertRaisesRegex(ValueError, "cannot precede"):
            review.apply_manifest(preview, "preview-sha", manifest, "manifest-sha")

    def test_weak_evidence_cannot_be_approved(self):
        preview = preview_payload(evidence_level="weak")
        manifest = approved_manifest(preview)
        with self.assertRaisesRegex(ValueError, "cannot be approved"):
            review.apply_manifest(preview, "preview-sha", manifest, "manifest-sha")

    def test_approval_requires_immutable_supporting_evidence(self):
        preview = preview_payload()
        manifest = approved_manifest(preview)
        manifest["decisions"][0]["supporting_evidence_item_ids"] = ["UNKNOWN"]
        with self.assertRaisesRegex(ValueError, "supporting evidence required"):
            review.apply_manifest(preview, "preview-sha", manifest, "manifest-sha")

    def test_trading_metrics_are_rejected_from_review_input(self):
        preview = preview_payload()
        preview["preview_rows"][0]["metrics"] = {"rps_20": 88.0}
        preview["review_shortlist"] = preview["preview_rows"]
        with self.assertRaisesRegex(ValueError, "trading metrics are forbidden"):
            review.initialize_manifest(preview, "preview-sha")

    def test_broad_universe_row_cannot_initialize_review_manifest(self):
        preview = preview_payload()
        row = preview["preview_rows"][0]
        row["mapping_pool"] = "broad_universe"
        row["review_eligible"] = False
        row["manual_review_required"] = False
        preview["review_shortlist"] = [row]
        with self.assertRaisesRegex(ValueError, "not awaiting manual review|not review_shortlist"):
            review.initialize_manifest(preview, "preview-sha")


if __name__ == "__main__":
    unittest.main()
