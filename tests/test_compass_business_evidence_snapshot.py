import importlib.util
import json
import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "tools" / "compass_business_evidence_snapshot.py"
DICTIONARY_PATH = ROOT / "config" / "compass_business_nodes_v0.2.json"


def load_module():
    spec = importlib.util.spec_from_file_location("compass_business_evidence_snapshot", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


MOD = load_module()


class FakeApi:
    def __init__(self):
        self.calls = []

    def stock_company(self, **kwargs):
        exchange = kwargs["exchange"]
        self.calls.append(("stock_company", exchange))
        if exchange != "SSE":
            return pd.DataFrame()
        return pd.DataFrame([
            {
                "ts_code": "600001.SH",
                "com_name": "样例电气股份有限公司",
                "introduction": "提供电网设备",
                "main_business": "有载分接开关研发与销售",
                "business_scope": "变压器部件",
            },
            {
                "ts_code": "600002.SH",
                "com_name": "样例软件股份有限公司",
                "introduction": "软件服务",
                "main_business": "企业软件",
                "business_scope": "软件开发",
            },
            {
                "ts_code": "600003.SH",
                "com_name": "样例电力股份有限公司",
                "introduction": "提供输变电设备",
                "main_business": "电力变压器研发与销售",
                "business_scope": "电网设备",
            },
            {
                "ts_code": "600004.SH",
                "com_name": "样例范围股份有限公司",
                "introduction": "提供变压器咨询",
                "main_business": "企业软件",
                "business_scope": "许可经营电网设备",
            },
            {
                "ts_code": "600005.SH",
                "com_name": "样例宽泛股份有限公司",
                "introduction": "综合设备供应商",
                "main_business": "电力设备业务",
                "business_scope": "电力设备研发",
            },
        ])

    def fina_mainbz(self, **kwargs):
        self.calls.append(("fina_mainbz", kwargs["ts_code"]))
        if kwargs["ts_code"] != "600001.SH":
            return pd.DataFrame()
        return pd.DataFrame([
            {
                "ts_code": "600001.SH", "end_date": "20251231",
                "bz_item": "有载分接开关", "bz_code": "P", "bz_sales": 100.0,
                "bz_profit": 30.0, "bz_cost": 70.0, "curr_type": "CNY", "update_flag": "0",
            },
            {
                "ts_code": "600001.SH", "end_date": "20261231",
                "bz_item": "有载分接开关", "bz_code": "P", "bz_sales": 200.0,
                "bz_profit": 60.0, "bz_cost": 140.0, "curr_type": "CNY", "update_flag": "0",
            },
        ])


def preview(rows, anchors=None, broad=None):
    return {
        "protocol_version": MOD.INPUT_PROTOCOL_VERSION,
        "batch_id": "2026W30",
        "mode": "dry_run",
        "no_trade_signal": True,
        "review_shortlist": rows,
        "broad_universe": broad or [],
        "anchor_reference": anchors or [],
        "validation_tasks": [],
    }


def shortlist_row(symbol="600001.SH", name="样例电气"):
    return {
        "source_candidate_id": "COMPASS-2026W30-A-001",
        "source_theme_name": "特高压与变压器瓶颈",
        "compass_line_key": "A",
        "supply_chain_nodes": ["分接开关"],
        "discovery_keywords": ["特高压"],
        "bottleneck_hypotheses": [],
        "symbol": symbol,
        "name": name,
        "mapping_pool": "review_shortlist",
        "review_eligible": True,
        "generate_task": False,
    }


class CompassBusinessEvidenceSnapshotTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dictionary = json.loads(DICTIONARY_PATH.read_text(encoding="utf-8"))

    def build(
        self, payload, api=None, max_input_symbols=500, max_mainbz_symbols=30,
        include_anchors=True, include_broad=True,
    ):
        return MOD.build_snapshot(
            payload,
            "a" * 64,
            self.dictionary,
            "b" * 64,
            api or FakeApi(),
            "2026-07-30",
            max_input_symbols,
            max_mainbz_symbols,
            include_anchors,
            include_broad,
        )

    def test_reported_business_item_is_b2_but_never_generates_task(self):
        result = self.build(preview([shortlist_row()]))
        row = result["business_evidence"][0]
        self.assertEqual(result["quality_status"], "DATA_OK")
        self.assertEqual(row["business_evidence_level"], "B2_REPORTED_BUSINESS_ITEM")
        self.assertEqual(row["latest_report_period_seen"], "20251231")
        self.assertTrue(row["business_relevance_evidence_present"])
        self.assertIsNone(row["reported_business_evidence"][0]["revenue_share"])
        self.assertFalse(row["generate_task"])

    def test_unrelated_company_stays_b0(self):
        result = self.build(preview([shortlist_row("600002.SH", "样例软件")]))
        row = result["business_evidence"][0]
        self.assertEqual(row["business_evidence_level"], "B0_NO_DIRECT_MATCH")
        self.assertFalse(row["business_relevance_evidence_present"])

    def test_scope_or_introduction_only_is_supporting_b0(self):
        result = self.build(preview([shortlist_row("600004.SH", "样例范围")]))
        row = result["business_evidence"][0]
        self.assertEqual(row["business_evidence_level"], "B0_NO_DIRECT_MATCH")
        self.assertFalse(row["business_relevance_evidence_present"])
        self.assertTrue(row["supporting_descriptor_evidence"])
        self.assertEqual(row["mainbz_query_status"], "NOT_REQUESTED_NO_DESCRIPTOR_MATCH")

    def test_generic_main_business_term_is_context_only_not_review_eligible(self):
        result = self.build(preview([shortlist_row("600005.SH", "样例宽泛")]))
        row = result["business_evidence"][0]
        self.assertEqual(row["business_evidence_level"], "B0_CONTEXT_ONLY_GENERIC")
        self.assertEqual(row["business_review_eligibility"], "context_only_generic_node")
        self.assertFalse(row["business_relevance_evidence_present"])
        self.assertTrue(row["business_context_evidence_present"])
        self.assertFalse(row["business_review_eligible"])
        self.assertEqual(result["business_evidence_matches"], [])
        self.assertEqual(len(result["business_context_only_matches"]), 1)

    def test_specific_term_is_review_eligible_and_ids_are_explicit(self):
        result = self.build(preview([shortlist_row()]))
        row = result["business_evidence"][0]
        self.assertTrue(row["business_review_eligible"])
        self.assertEqual(row["business_review_eligibility"], "eligible_specific_node")
        self.assertTrue(row["review_qualifying_evidence_item_ids"])
        qualifying_ids = {
            item["evidence_item_id"]
            for item in row["descriptor_evidence"] + row["reported_business_evidence"]
            if item["review_qualifying"]
        }
        self.assertEqual(set(row["review_qualifying_evidence_item_ids"]), qualifying_ids)

    def test_broad_universe_is_rechecked_without_pool_change(self):
        broad = shortlist_row()
        broad["mapping_pool"] = "broad_universe"
        broad["review_eligible"] = False
        payload = preview([], broad=[broad])
        payload["diagnostic_sample"] = [shortlist_row("600002.SH")]
        api = FakeApi()
        result = self.build(payload, api=api)
        row = result["business_evidence_matches"][0]
        self.assertEqual(result["quality_status"], "DATA_OK")
        self.assertEqual(row["candidate_scope"], "broad_universe_recheck")
        self.assertEqual(row["mapping_pool_unchanged"], "broad_universe")
        self.assertFalse(row["source_review_eligible"])
        self.assertFalse(row["existing_review_manifest_eligible"])
        self.assertFalse(row["generate_task"])

    def test_diagnostic_rows_are_never_queried(self):
        payload = preview([])
        payload["diagnostic_sample"] = [shortlist_row()]
        api = FakeApi()
        result = self.build(payload, api=api)
        self.assertEqual(result["quality_status"], "NO_ELIGIBLE_ROWS")
        self.assertEqual(api.calls, [])

    def test_limit_exceeded_makes_no_api_calls(self):
        rows = [shortlist_row(f"60000{i}.SH", f"样例{i}") for i in range(1, 4)]
        api = FakeApi()
        result = self.build(preview(rows), api=api, max_input_symbols=2)
        self.assertEqual(result["quality_status"], "INPUT_LIMIT_EXCEEDED")
        self.assertEqual(result["business_evidence"], [])
        self.assertEqual(api.calls, [])

    def test_anchor_is_reference_only(self):
        anchor = {
            "source_candidate_id": "COMPASS-2026W30-A-001",
            "source_theme_name": "特高压与变压器瓶颈",
            "compass_line_key": "A",
            "symbol": "600001.SH",
            "name": "样例电气",
            "mapping_pool": "anchor_reference",
            "exact_resolved": True,
            "generate_task": False,
        }
        result = self.build(preview([], [anchor]))
        row = result["business_evidence"][0]
        self.assertEqual(row["candidate_scope"], "anchor_reference")
        self.assertFalse(row["generate_task"])
        self.assertTrue(row["manual_review_required"])

    def test_same_symbol_is_fetched_once_across_duplicate_theme_rows(self):
        first = shortlist_row()
        second = dict(first)
        second["source_candidate_id"] = "COMPASS-2026W30-A-002"
        api = FakeApi()
        result = self.build(preview([first, second]), api=api)
        self.assertEqual(len(result["business_evidence"]), 2)
        self.assertEqual(api.calls.count(("stock_company", "SSE")), 1)
        self.assertEqual(api.calls.count(("fina_mainbz", "600001.SH")), 1)

    def test_mainbz_limit_does_not_truncate_descriptor_matches(self):
        rows = [shortlist_row(), shortlist_row("600003.SH", "样例电力")]
        api = FakeApi()
        result = self.build(preview(rows), api=api, max_mainbz_symbols=1)
        self.assertEqual(result["quality_status"], "PARTIAL_MAINBZ_LIMIT")
        self.assertEqual(len(result["business_evidence_matches"]), 2)
        self.assertTrue(all(
            row["business_evidence_level"] == "B1_COMPANY_DESCRIPTOR"
            for row in result["business_evidence_matches"]
        ))
        self.assertFalse(any(call[0] == "fina_mainbz" for call in api.calls))

    def test_unsafe_preview_is_rejected(self):
        payload = preview([shortlist_row()])
        payload["validation_tasks"] = [{"task_id": "unsafe"}]
        with self.assertRaisesRegex(ValueError, "must not contain"):
            self.build(payload)

    def test_future_evidence_as_of_is_rejected(self):
        future = (
            datetime.now(MOD.BEIJING_TZ).date() + timedelta(days=1)
        ).isoformat()
        with self.assertRaisesRegex(ValueError, "cannot be in the future"):
            MOD.build_snapshot(
                preview([shortlist_row()]), "a" * 64, self.dictionary, "b" * 64,
                FakeApi(), future, 500, 30, True, True,
            )

    def test_report_period_staleness_is_explicit(self):
        self.assertTrue(MOD.report_period_age_days("20220101", "2026-07-30") > 550)
        self.assertFalse(MOD.report_period_age_days("20251231", "2026-07-30") > 550)
        self.assertIsNone(MOD.report_period_age_days(None, "2026-07-30"))

    def test_dictionary_sha_changes_with_vocabulary(self):
        original = MOD.canonical_sha(self.dictionary)
        changed = json.loads(json.dumps(self.dictionary, ensure_ascii=False))
        changed["themes"][0]["nodes"][0]["terms"].append("新词")
        self.assertNotEqual(original, MOD.canonical_sha(changed))

    def test_dictionary_rejects_review_terms_outside_node_terms(self):
        changed = json.loads(json.dumps(self.dictionary, ensure_ascii=False))
        changed["themes"][0]["nodes"][0]["review_qualifying_terms"].append("越界词")
        with self.assertRaisesRegex(ValueError, "review terms are invalid"):
            MOD.validate_dictionary(changed)


if __name__ == "__main__":
    unittest.main()
