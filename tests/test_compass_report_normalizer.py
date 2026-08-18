#!/usr/bin/env python3
import argparse
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("zhulong_test_compass_normalizer", ROOT / "tools/compass_report_normalizer.py")
MOD = importlib.util.module_from_spec(spec); sys.modules[spec.name] = MOD; spec.loader.exec_module(MOD)


REPORT = r"""# User prompt
### 4.1 stock_validation_candidates
### 4.2 theme_mapping_candidates
### 4.3 watch_only_candidates
### 4.4 excluded_candidates
## 5. 烛龙验证问题总表
<details><summary>思考过程</summary>hidden</details>
# Compass 当前全景与瓶颈发现输入报告 · 2026-W28
## 4. 烛龙结构化输入区
### 4.1 stock\_validation\_candidates
| name | market | compass\_line | pool\_type | priority | thesis | benefit\_mechanism | questions\_for\_zhulong | risks |
|---|---|---|---|---|---|---|---|---|
| 华明装备 | A股 | A线 | 历史锚点 | high | 分接开关 | 变压器需求 | 订单 | 需求不足 |
### 4.2 theme\_mapping\_candidates
| theme\_name | compass\_line | candidate\_type | priority | thesis | supply\_chain\_nodes | bottleneck\_hypotheses | discovery\_keywords | questions\_for\_zhulong | risks |
|---|---|---|---|---|---|---|---|---|---|
| 数据中心电力设备 | A线 | industry\_segment | high | 电力瓶颈 | 取向硅钢；分接开关 | 客户认证；扩产周期 | 取向硅钢；有载分接开关 | 订单占比 | 宽行业误召回 |
### 4.3 watch\_only\_candidates
| name | market | compass\_line | candidate\_type | watch\_focus | risks |
|---|---|---|---|---|---|
| Physical AI | 未知 | Physical AI | theme\_candidate | 订单 | 证据不足 |
### 4.4 excluded\_candidates
| name | compass\_line | candidate\_type | excluded\_reason | risks |
|---|---|---|---|---|
| JPMorgan AI Agent回测 | AI金融应用 | theme\_candidate | 回测不可执行 | 暂无 |
## 5. 烛龙验证问题总表
| compass\_line | theme\_name | bottleneck\_node | verification\_question | required\_evidence | counter\_evidence |
|---|---|---|---|---|---|
| A线 | 数据中心电力设备 | 取向硅钢 | 产能利用率是否提升 | 产能利用率；价格 | 扩产后价格下降 |

（内容由AI生成，仅供参考）
"""


class CompassReportNormalizerTest(unittest.TestCase):
    def payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "report.md"; path.write_text(REPORT, encoding="utf-8")
            return MOD.normalize(argparse.Namespace(input=path, encoding="utf-8", batch_id="2026W28",
                                                     source_as_of="2026-07-12", source_report="report.md"))

    def test_preserves_bottleneck_discovery_fields_and_questions(self):
        payload = self.payload()
        theme = next(item for item in payload["candidates"] if item["name"] == "数据中心电力设备")
        self.assertEqual(theme["candidate_type"], "theme_candidate")
        self.assertEqual(theme["supply_chain_nodes"], ["取向硅钢", "分接开关"])
        self.assertEqual(theme["discovery_keywords"], ["取向硅钢", "有载分接开关"])
        self.assertEqual(theme["validation_question_records"][0]["bottleneck_node"], "取向硅钢")
        self.assertEqual(payload["validation_questions"][0]["required_evidence"], ["产能利用率", "价格"])

    def test_nonstandard_line_is_not_guessed_from_first_letter(self):
        payload = self.payload()
        excluded = next(item for item in payload["candidates"] if item["name"] == "JPMorgan AI Agent回测")
        self.assertEqual(excluded["compass_line_key"], "OTHER")
        self.assertIn("-OTHER-", excluded["candidate_id"])

    def test_contaminated_export_is_auditable_but_last_sections_are_used(self):
        payload = self.payload()
        self.assertEqual(len(payload["candidates"]), 4)
        self.assertIn("embedded_reasoning_detected", payload["warnings"])
        self.assertIn("content_after_structured_report_detected", payload["warnings"])
        self.assertNotEqual(payload["report_sha256"], payload["parsed_report_sha256"])


if __name__ == "__main__": unittest.main()
