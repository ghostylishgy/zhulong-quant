import importlib.util
import json
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "tools" / "compass_validation_task_generator.py"
SPEC = importlib.util.spec_from_file_location("compass_validation_task_generator", MODULE_PATH)
assert SPEC and SPEC.loader
generator = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(generator)


def candidate(candidate_id="ROW-1-REVIEWED", theme="特高压") -> dict:
    return {
        "reviewed_candidate_id": candidate_id,
        "source_preview_row_id": candidate_id.removesuffix("-REVIEWED"),
        "source_preview_row_sha256": "1" * 64,
        "compass_source_as_of": "2026-07-10",
        "validation_as_of": "2026-07-17",
        "source_candidate_id": "COMPASS-2026W28-A-001",
        "source_theme_name": theme,
        "source_object_class": "theme_candidate",
        "source_discovery_constraints": {"auto_theme_to_stock_mapping": False},
        "origin": "human_reviewed_theme_discovery",
        "compass_line": "A线",
        "compass_line_key": "A线",
        "priority": "high",
        "market": "A股",
        "ticker": "000001.SZ",
        "ticker_status": "manual_resolved_from_discovery_preview",
        "name": "Sample",
        "industry": "电气设备",
        "market_board": "主板",
        "is_st": False,
        "mapping_pool": "review_shortlist",
        "mapping_evidence_level": "L1",
        "evidence_level": "medium",
        "evidence_items": [{
            "evidence_item_id": "EVID-001", "type": "classification_membership",
            "source": "tushare.ths_member", "index_code": "885425.TI",
        }],
        "evidence_sources": ["tushare.ths_member"],
        "source_memberships": [{"source": "tushare.ths_member", "index": "特高压"}],
        "business_relevance_status": "human_confirmed",
        "business_relevance_basis": "manual_verified_industry_mapping",
        "supporting_evidence_items": [{
            "evidence_item_id": "EVID-001", "type": "classification_membership",
            "source": "tushare.ths_member", "index_code": "885425.TI",
        }],
        "supporting_metrics": {},
        "supply_chain_nodes": ["分接开关"],
        "bottleneck_hypotheses": ["订单传导"],
        "discovery_keywords": ["特高压设备"],
        "questions_for_zhulong": ["订单是否增长？"],
        "line_validation_questions": ["毛利率是否改善？"],
        "business_relevance_evidence_present": True,
        "dry_run_preview_reviewed": True,
        "human_review_for_theme_mapping": True,
        "reviewer_id": "steve",
        "reviewed_at": "2026-07-13T20:00:00+08:00",
        "review_notes": "人工确认业务映射。",
        "manual_review_required": False,
        "eligible_for_separate_dry_run_validation_task": True,
        "generate_task": False,
        "no_trade_signal": True,
    }


def reviewed_payload(candidates=None) -> dict:
    candidates = candidates if candidates is not None else [candidate()]
    return {
        "output_version": generator.INPUT_VERSION,
        "batch_id": "2026W28",
        "mode": "dry_run",
        "dry_run": True,
        "no_trade_signal": True,
        "source_preview_sha256": "2" * 64,
        "review_manifest_sha256": "3" * 64,
        "compass_source_as_of": "2026-07-10",
        "validation_as_of": "2026-07-17",
        "reviewer_id": "steve",
        "reviewed_at": "2026-07-13T20:00:00+08:00",
        "blocked_actions": sorted(generator.REQUIRED_REVIEW_BLOCKS | {"generate_validation_task"}),
        "stats": {"reviewed_candidates": len(candidates), "generated_tasks": 0},
        "reviewed_candidates": candidates,
        "validation_tasks": [],
    }


def payload_and_sha(payload: dict) -> tuple[bytes, str]:
    raw = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    return raw, generator.sha256_bytes(raw)


class CompassValidationTaskGeneratorTest(unittest.TestCase):
    def test_approved_candidate_generates_unexecuted_dry_run_task(self):
        reviewed = reviewed_payload()
        _, sha = payload_and_sha(reviewed)
        output = generator.build_payload(reviewed, Path("reviewed.json"), sha, sha)

        self.assertEqual(output["stats"]["generated_tasks"], 1)
        self.assertEqual(output["stats"]["executed_tasks"], 0)
        task = output["validation_tasks"][0]
        self.assertEqual(task["execution_status"], "NOT_EXECUTED_DRY_RUN_ARTIFACT")
        self.assertEqual(task["bottleneck_hypotheses"], ["订单传导"])
        self.assertEqual(task["questions_for_zhulong"], ["订单是否增长？", "毛利率是否改善？"])
        self.assertEqual(task["compass_source_as_of"], "2026-07-10")
        self.assertEqual(task["validation_as_of"], "2026-07-17")
        self.assertIs(task["no_trade_signal"], True)
        self.assertIs(
            task["review_assertions"]["business_relevance_confirmed"], True
        )
        self.assertIs(
            task["review_assertions"]["market_metrics_not_used_as_business_evidence"], True
        )
        self.assertEqual(task["mapping_evidence_levels"], ["L1"])
        self.assertTrue(task["supporting_evidence_items"])
        self.assertIs(
            task["review_assertions"]["manual_review_required"], False
        )
        self.assertIn("call_decision_engine", task["blocked_actions"])

    def test_duplicate_symbol_is_consolidated_without_losing_themes(self):
        second = candidate("ROW-2-REVIEWED", "变压器")
        reviewed = reviewed_payload([candidate(), second])
        _, sha = payload_and_sha(reviewed)
        output = generator.build_payload(reviewed, Path("reviewed.json"), sha, sha)

        self.assertEqual(output["stats"]["reviewed_candidates"], 2)
        self.assertEqual(output["stats"]["generated_tasks"], 1)
        self.assertEqual(output["stats"]["consolidated_duplicate_rows"], 1)
        self.assertEqual(output["validation_tasks"][0]["source_theme_names"], ["特高压", "变压器"])

    def test_expected_sha256_is_mandatory_and_fail_closed(self):
        reviewed = reviewed_payload()
        _, sha = payload_and_sha(reviewed)
        with self.assertRaisesRegex(ValueError, "SHA256 mismatch"):
            generator.build_payload(reviewed, Path("reviewed.json"), sha, "0" * 64)

    def test_weak_evidence_cannot_generate_task(self):
        item = candidate()
        item["evidence_level"] = "weak"
        reviewed = reviewed_payload([item])
        _, sha = payload_and_sha(reviewed)
        with self.assertRaisesRegex(ValueError, "evidence_level"):
            generator.build_payload(reviewed, Path("reviewed.json"), sha, sha)

    def test_tampered_review_flags_fail_closed(self):
        item = candidate()
        item["manual_review_required"] = True
        reviewed = reviewed_payload([item])
        _, sha = payload_and_sha(reviewed)
        with self.assertRaisesRegex(ValueError, "manual_review"):
            generator.build_payload(reviewed, Path("reviewed.json"), sha, sha)

    def test_candidate_as_of_must_match_reviewed_artifact(self):
        item = candidate()
        item["validation_as_of"] = "2026-07-18"
        reviewed = reviewed_payload([item])
        _, sha = payload_and_sha(reviewed)
        with self.assertRaisesRegex(ValueError, "validation_as_of"):
            generator.build_payload(reviewed, Path("reviewed.json"), sha, sha)

    def test_market_metrics_cannot_substitute_for_mapping_evidence(self):
        item = candidate()
        item["supporting_evidence_items"] = []
        item["supporting_metrics"] = {"rps_20": 88.0}
        reviewed = reviewed_payload([item])
        _, sha = payload_and_sha(reviewed)
        with self.assertRaisesRegex(ValueError, "supporting_evidence|no_market_metric_evidence"):
            generator.build_payload(reviewed, Path("reviewed.json"), sha, sha)

    def test_module_has_no_database_or_live_engine_dependency(self):
        source = MODULE_PATH.read_text(encoding="utf-8")
        self.assertNotIn("DBGateway", source)
        self.assertNotIn("import decision_engine", source)
        self.assertNotIn("from decision_engine", source)
        self.assertNotIn("from db_gateway", source)
        self.assertNotIn("Nexus.run", source)


if __name__ == "__main__":
    unittest.main()
