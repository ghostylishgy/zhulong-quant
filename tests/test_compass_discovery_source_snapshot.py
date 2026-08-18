#!/usr/bin/env python3
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from tests import _test_log_isolation  # noqa: F401

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("zhulong_test_compass_snapshot", ROOT / "tools/compass_discovery_source_snapshot.py")
MOD = importlib.util.module_from_spec(spec); sys.modules[spec.name] = MOD; spec.loader.exec_module(MOD)


class CompassDiscoverySourceSnapshotTest(unittest.TestCase):
    def test_explicit_keywords_replace_legacy_narrative_terms(self):
        payload = {"batch_id": "2026W28", "candidates": [{
            "candidate_id": "T1", "name": "宽泛主题", "object_class": "theme_candidate",
            "used_as_discovery_hint": True, "discovery_keywords": ["取向硅钢", "有载分接开关"],
            "supply_chain_nodes": ["变压器"], "thesis": "不应进入关键词",
            "discovery_constraints": {"query_terms": ["专用机械"]},
        }]}
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "normalized.json"; path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            _, keywords, sources, _ = MOD.load_keywords_from_normalized(path)
        self.assertEqual(keywords, ["取向硅钢", "有载分接开关", "变压器"])
        self.assertEqual(sources[0]["keyword_source"], "explicit_discovery_fields")

    def test_broad_sw_index_is_not_expanded_to_medium_member_rows(self):
        class API:
            @staticmethod
            def index_member_all(**_kwargs):
                return pd.DataFrame([{"ts_code": f"{index:06d}.SZ", "is_new": "Y"} for index in range(4)])
        matches = [{"level": "L1", "index_code": "801000.SI", "industry_name": "宽行业", "keyword": "测试"}]
        warnings = []
        rows = MOD.fetch_sw_members(API(), matches, threshold=3, max_store=20, warnings=warnings)
        self.assertEqual(rows, [])
        self.assertTrue(matches[0]["broad_theme"])
        self.assertIn("sw_members_broad_after_fetch:801000.SI:4>3", warnings)

    def test_ths_synonym_matches_fetch_unique_index_once(self):
        rows = [{"ts_code": "885425.TI", "name": "特高压", "count": 2, "type": "N"}]
        warnings = []
        matches = MOD.match_ths_indices(rows, ["特高压", "特高压设备"], 10, 300, warnings)
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["matched_keywords"], ["特高压", "特高压设备"])

        class API:
            calls = 0

            @classmethod
            def ths_member(cls, **_kwargs):
                cls.calls += 1
                return pd.DataFrame([
                    {"con_code": "600001.SH", "con_name": "A"},
                    {"con_code": "600002.SH", "con_name": "B"},
                ])

        members = MOD.fetch_ths_members(API(), matches, threshold=300, max_store=300, warnings=warnings)
        self.assertEqual(API.calls, 1)
        self.assertEqual(len(members), 2)
        self.assertEqual(members[0]["source_keywords"], ["特高压", "特高压设备"])


if __name__ == "__main__": unittest.main()
