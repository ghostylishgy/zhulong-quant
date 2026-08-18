import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "tools" / "compass_theme_discovery.py"
SPEC = importlib.util.spec_from_file_location("compass_theme_discovery", MODULE_PATH)
assert SPEC and SPEC.loader
discovery = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(discovery)


REQUIRED_BLOCKED = [
    "write_duckdb",
    "generate_validation_task",
    "write_shadow",
    "write_rag_memory",
    "write_nexus_audits",
    "trigger_daemon",
    "call_decision_engine",
    "call_nexus_run",
    "trade",
]


def snapshot_payload() -> dict:
    return {
        "batch_id": "2026W28",
        "source": "tushare",
        "snapshot_type": "discovery_source",
        "snapshot_version": discovery.SOURCE_SNAPSHOT_VERSION,
        "mode": "dry_run",
        "dry_run": True,
        "no_trade_signal": True,
        "source_normalized_sha256": "abc123",
        "blocked_actions": REQUIRED_BLOCKED,
        "stats": {"generated_tasks": 0},
        "validation_tasks": [],
        "keywords": ["特高压"],
        "theme_sources": [
            {"candidate_id": "COMPASS-2026W28-A-001", "keywords": ["特高压"]},
        ],
        "sw_index_matches": [],
        "sw_index_members": [
            {
                "source_keyword": "特高压",
                "source_index_code": "801010.SI",
                "source_index_name": "电力设备",
                "source_index_level": "L1",
                "ts_code": "000001.SZ",
                "name": "Current",
                "is_new": "Y",
                "out_date": None,
                "evidence_level": "medium",
                "business_relevance_evidence_present": False,
                "manual_review_required": True,
                "generate_task": False,
            },
            {
                "source_keyword": "特高压",
                "source_index_code": "801010.SI",
                "source_index_name": "电力设备",
                "source_index_level": "L1",
                "ts_code": "000002.SZ",
                "name": "Historical",
                "is_new": "N",
                "out_date": "20200101",
                "evidence_level": "medium",
                "business_relevance_evidence_present": False,
                "manual_review_required": True,
                "generate_task": False,
            },
        ],
        "ths_indices_matched": [
            {
                "keyword": "特高压",
                "ts_code": "885425.TI",
                "name": "特高压",
                "broad_theme": False,
                "expand_members": True,
                "evidence_level": "medium",
                "business_relevance_evidence_present": False,
                "manual_review_required": True,
                "generate_task": False,
            },
        ],
        "ths_members": [
            {
                "source_keyword": "特高压",
                "source_index_code": "885425.TI",
                "source_index_name": "特高压",
                "source_index_type": "N",
                "con_code": "600001.SH",
                "con_name": "THS Member",
                "evidence_level": "medium",
                "business_relevance_evidence_present": False,
                "manual_review_required": True,
                "generate_task": False,
            },
        ],
    }


class CompassThemeDiscoveryTest(unittest.TestCase):
    def test_explicit_discovery_keywords_disable_broad_legacy_bridges(self):
        keywords, bridges = discovery.expanded_keywords({
            "name": "AI材料国产替代",
            "discovery_keywords": ["ABF封装基板", "HBM材料"],
            "supply_chain_nodes": ["先进封装耗材"],
            "discovery_constraints": {"query_terms": ["半导体"]},
        }, 10)
        self.assertEqual(keywords, ["ABF封装基板", "HBM材料", "先进封装耗材"])
        self.assertEqual(bridges, [])

    def test_source_snapshot_validation_fails_closed_on_hash_mismatch(self):
        payload = snapshot_payload()
        with self.assertRaisesRegex(ValueError, "SHA256"):
            discovery.validate_source_snapshot(payload, "2026W28", "different")

    def test_source_snapshot_membership_is_mapping_evidence_only(self):
        payload = snapshot_payload()
        self.assertEqual(discovery.validate_source_snapshot(payload, "2026W28", "abc123"), [])
        theme = {"candidate_id": "COMPASS-2026W28-A-001"}
        evidence, summary = discovery.snapshot_evidence_for_theme(theme, payload, ["特高压"])

        self.assertEqual(summary["mapping_mode"], "candidate_id")
        self.assertEqual(set(evidence), {"000001.SZ", "600001.SH"})
        self.assertNotIn("000002.SZ", evidence)
        self.assertEqual(
            {item["source"] for item in evidence["000001.SZ"]["memberships"]},
            {"tushare.sw_index_member_all"},
        )

    def test_source_snapshot_rejects_embedded_validation_tasks(self):
        payload = snapshot_payload()
        payload["validation_tasks"] = [{"task_id": "forbidden"}]
        with self.assertRaisesRegex(ValueError, "must not contain validation tasks"):
            discovery.validate_source_snapshot(payload, "2026W28", "abc123")

    def test_local_stock_basic_match_remains_weak_evidence(self):
        match = discovery.score_row(
            {"name": "Sample", "industry": "电气设备"},
            keywords=["电气设备"],
            bridge_terms=["电气设备"],
        )
        self.assertEqual(match["evidence_level"], "weak")

    def test_single_broad_membership_stays_out_of_review_shortlist(self):
        match = {
            "source_memberships": [{
                "source": "tushare.ths_member", "index_code": "885425.TI",
                "index_member_count": 130, "matched_keywords": ["特高压"],
            }],
            "name_hits": [], "industry_hits": [],
        }
        level, eligible, reasons = discovery.classify_mapping(match, narrow_index_member_threshold=50)
        self.assertEqual(level, "L0")
        self.assertIs(eligible, False)
        self.assertEqual(reasons, ["single_broad_board_membership_only"])

    def test_narrow_membership_can_enter_review_shortlist_without_market_metrics(self):
        row = {"symbol": "000001.SZ", "name": "Sample", "industry": "电气设备", "is_st": False}
        match = {
            "evidence_level": "medium",
            "evidence_sources": ["tushare.ths_member"],
            "snapshot_keywords": ["特高压"],
            "source_memberships": [{
                "source": "tushare.ths_member", "index_code": "885425.TI",
                "index_name": "特高压设备", "index_member_count": 20,
                "matched_keywords": ["特高压"],
            }],
            "name_hits": [], "industry_hits": [], "bridge_hits": [],
        }
        level, eligible, reasons = discovery.classify_mapping(match, narrow_index_member_threshold=50)
        preview = discovery.build_discovery_row({
            "candidate_id": "T-1",
            "name": "特高压",
            "object_class": "theme_candidate",
            "supply_chain_nodes": ["分接开关"],
            "bottleneck_hypotheses": ["订单传导"],
            "discovery_keywords": ["特高压设备"],
            "questions_for_zhulong": ["订单是否增长？"],
            "line_validation_questions": ["毛利率是否改善？"],
        }, row, match, 1, "review_shortlist", level, eligible, reasons)

        self.assertIs(preview["business_relevance_evidence_present"], False)
        self.assertEqual(preview["business_relevance_status"], "unverified")
        self.assertEqual(preview["mapping_evidence_level"], "L1")
        self.assertEqual(preview["mapping_pool"], "review_shortlist")
        self.assertIs(preview["manual_review_required"], True)
        self.assertIs(preview["generate_task"], False)
        self.assertEqual(preview["ticker_status"], "preview_only_not_task_resolved")
        self.assertEqual(preview["bottleneck_hypotheses"], ["订单传导"])
        self.assertEqual(preview["questions_for_zhulong"], ["订单是否增长？"])
        self.assertNotIn("metrics", preview)
        self.assertNotIn("metric_trade_date", preview)

    def test_diagnostic_sample_is_not_reviewable_or_ranked(self):
        row = {
            "preview_row_id": "T-1-BROAD-001", "industry": "电气设备", "symbol": "000001.SZ",
            "mapping_pool": "broad_universe", "manual_review_required": False,
        }
        sample = discovery.stratified_diagnostic_sample([row], 1)[0]
        self.assertEqual(sample["mapping_pool"], "diagnostic_sample")
        self.assertIs(sample["sample_only"], True)
        self.assertIs(sample["not_a_ranked_shortlist"], True)
        self.assertIs(sample["review_eligible"], False)


if __name__ == "__main__":
    unittest.main()
