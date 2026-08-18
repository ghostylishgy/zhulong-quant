#!/usr/bin/env python3

import importlib.util
import inspect
import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
TEST_LOG_DIR = Path(tempfile.gettempdir()) / f"zhulong_unittest_logs_{os.getpid()}"
TEST_LOG_DIR.mkdir(parents=True, exist_ok=True)
os.environ["ZHULONG_NEXUS_LOG_PATH"] = str(TEST_LOG_DIR / "nexus.log")
os.environ["ZHULONG_GOVERNANCE_LOG_PATH"] = str(TEST_LOG_DIR / "governance.log")
MODULE_PATH = ROOT / "02_brain" / "decision_engine.py"
SPEC = importlib.util.spec_from_file_location("zhulong_test_prompt_contract_v4", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class FakeResponse:
    status_code = 200
    content = b"ok"

    def __init__(self, content):
        self._content = content

    def json(self):
        return {"choices": [{"message": {"content": self._content}}]}


class FakeProviderResponse:
    content = b""

    def __init__(self, status_code, *, text="", payload=None, headers=None):
        self.status_code = status_code
        self.text = text
        self._payload = payload or {}
        self.headers = headers or {}
        self.closed = False

    def json(self):
        return self._payload

    def close(self):
        self.closed = True


class FakeOllamaResponse:
    status_code = 200
    content = b"ok"

    def __init__(self, response):
        self._response = response

    def json(self):
        return {"response": self._response}

    def close(self):
        return None


class PromptContractV4Test(unittest.TestCase):
    def tearDown(self):
        with MODULE._L4_PROVIDER_INCIDENT_LOCK:
            MODULE._L4_PROVIDER_INCIDENTS.clear()

    def test_l4_recovery_poll_defaults_to_30_minutes(self):
        with patch.dict(
            os.environ,
            {"L4_PROVIDER_RECOVERY_POLL_SECONDS": ""},
        ):
            self.assertEqual(MODULE._l4_recovery_poll_seconds(), 1800.0)

    def test_l4_provider_push_is_concise_and_hides_request_content(self):
        accepted = FakeProviderResponse(200, payload={"code": 200})
        with patch.object(
            MODULE.Config,
            "PUSHPLUS_TOKEN",
            "test-token",
        ), patch.object(
            MODULE.COMPUTE_GATEWAY,
            "http_post",
            return_value=accepted,
        ) as http_post:
            ok = MODULE._send_l4_provider_notification(
                phase="PAUSED",
                label="L4.2-Judge",
                provider="deepseek",
                model="deepseek-v4-flash",
                failure_class="BALANCE_OR_QUOTA_EXHAUSTED",
                status_code=402,
            )

        self.assertTrue(ok)
        payload = http_post.call_args.kwargs["json_payload"]
        self.assertEqual(payload["token"], "test-token")
        self.assertIn("审计已暂停", payload["title"])
        self.assertIn("请检查账户余额并充值", payload["content"])
        self.assertIn("L4.2-Judge", payload["content"])
        self.assertNotIn("messages", payload["content"])
        self.assertNotIn("Authorization", payload["content"])

    def test_l4_balance_failure_pauses_alerts_and_resumes_same_request(self):
        unavailable = FakeProviderResponse(
            402,
            text='{"error":"insufficient balance"}',
        )
        recovered = FakeProviderResponse(200, payload={"choices": []})
        with patch.object(
            MODULE.COMPUTE_GATEWAY,
            "http_post",
            side_effect=[unavailable, recovered],
        ) as http_post, patch.object(
            MODULE,
            "_send_l4_provider_notification",
            return_value=True,
        ) as notify, patch.object(
            MODULE.time,
            "sleep",
        ) as sleep:
            response = MODULE.api_call_with_retry(
                "https://unused.invalid",
                {"Authorization": "Bearer test"},
                {"model": "deepseek-v4-flash", "messages": []},
                timeout=1,
                max_retries=1,
                label="L4.2-Judge",
                model_hint="deepseek-v4-flash",
            )

        self.assertIs(response, recovered)
        self.assertTrue(unavailable.closed)
        self.assertEqual(http_post.call_count, 2)
        self.assertEqual(
            [call.kwargs["phase"] for call in notify.call_args_list],
            ["PAUSED", "RECOVERED"],
        )
        self.assertEqual(
            notify.call_args_list[0].kwargs["failure_class"],
            "BALANCE_OR_QUOTA_EXHAUSTED",
        )
        self.assertGreaterEqual(sleep.call_count, 1)
        self.assertEqual(MODULE._L4_PROVIDER_INCIDENTS, {})

    def test_l4_timeout_exhaustion_pauses_instead_of_returning_none(self):
        recovered = FakeProviderResponse(200, payload={"choices": []})
        with patch.object(
            MODULE.COMPUTE_GATEWAY,
            "http_post",
            side_effect=[TimeoutError("provider timeout"), recovered],
        ), patch.object(
            MODULE.COMPUTE_GATEWAY,
            "is_timeout_error",
            return_value=True,
        ), patch.object(
            MODULE,
            "_send_l4_provider_notification",
            return_value=True,
        ) as notify, patch.object(MODULE.time, "sleep"):
            response = MODULE.api_call_with_retry(
                "https://unused.invalid",
                {},
                {"model": "moonshot-v1-128k", "messages": []},
                timeout=1,
                max_retries=1,
                label="L4.2-Bull",
                model_hint="moonshot-v1-128k",
            )

        self.assertIs(response, recovered)
        self.assertEqual(
            notify.call_args_list[0].kwargs["failure_class"],
            "TIMEOUT",
        )
        self.assertEqual(
            [call.kwargs["phase"] for call in notify.call_args_list],
            ["PAUSED", "RECOVERED"],
        )

    def test_non_l4_nonretryable_response_keeps_legacy_return_behavior(self):
        rejected = FakeProviderResponse(402, text="insufficient balance")
        with patch.object(
            MODULE.COMPUTE_GATEWAY,
            "http_post",
            return_value=rejected,
        ), patch.object(
            MODULE,
            "_send_l4_provider_notification",
        ) as notify:
            response = MODULE.api_call_with_retry(
                "https://unused.invalid",
                {},
                {"model": "deepseek-chat", "messages": []},
                timeout=1,
                max_retries=1,
                label="OTHER-CLOUD-CALL",
                model_hint="deepseek-chat",
            )
        self.assertIs(response, rejected)
        notify.assert_not_called()

    def test_l4_missing_provider_key_stops_before_simplified_scoring(self):
        court = self.court()
        court.qwen_key = ""
        with patch.object(
            MODULE,
            "_send_l4_provider_notification",
            return_value=True,
        ) as notify:
            with self.assertRaisesRegex(
                MODULE.L4ProviderUnavailableError,
                "Qwen/Notary",
            ):
                court._require_mandatory_provider_credentials()
        notify.assert_called_once()
        self.assertEqual(
            notify.call_args.kwargs["failure_class"],
            "CREDENTIAL_OR_PERMISSION_ERROR",
        )

    def test_l4_parallel_court_has_no_timeout_bypass(self):
        source = inspect.getsource(MODULE.L4SupremeCourt.audit)
        self.assertIn("t_bull.join()", source)
        self.assertIn("t_bear.join()", source)
        self.assertNotIn("t_bull.join(timeout=", source)
        self.assertNotIn("t_bear.join(timeout=", source)
        self.assertNotIn(
            'bull_box[0] or {"verdict": "HOLD"',
            source,
        )

    def test_model_enable_flags_use_strict_allowlist(self):
        for value in ("1", "true", "YES", "on"):
            with patch.dict(os.environ, {"L2_MODEL_ENABLED": value}):
                self.assertTrue(MODULE._env_flag_enabled("L2_MODEL_ENABLED"))
        for value in ("", "0", "false", "off", "unexpected", "enabled"):
            with patch.dict(os.environ, {"L2_MODEL_ENABLED": value}):
                self.assertFalse(MODULE._env_flag_enabled("L2_MODEL_ENABLED"))

    @staticmethod
    def court():
        court = MODULE.L4SupremeCourt.__new__(MODULE.L4SupremeCourt)
        court.kimi_key = "test"
        court.deepseek_key = "test"
        court.qwen_key = "test"
        court.notary_strict = False
        court.notary_veto_hard = False
        court.TIMEOUT = 5
        return court

    def test_l3_prompt_is_audit_not_trade_instruction(self):
        prompt = MODULE.L3StrategicAuditor._build_fin_auditor_prompt("TASK: legacy\nDATA")
        self.assertIn("[L3_OBSERVATION]", prompt)
        self.assertIn("LIFECYCLE_STAGE", prompt)
        self.assertIn("SUPPORTING_EVIDENCE_IDS", prompt)
        self.assertIn("EVIDENCE_CONTRACT", prompt)
        self.assertNotIn("[FINAL_DECISION]", prompt)
        self.assertNotIn("买入建议", prompt)

    @staticmethod
    def l3_observer_raw(*, support="P.RPS10", risk="P.TURNOVER", summary=None):
        summary = summary or (
            "当前相对强度与量价结构提供有限支持，但换手与阶段位置仍需继续确认；"
            "结论只引用给定证据，缺失的新闻和基本面保持未知。"
        )
        return (
            "[L3_OBSERVATION]\n"
            "CONTRACT_VERSION: L3_OBSERVER_V5\n"
            "LIFECYCLE_STAGE: CONTINUATION\n"
            "THESIS_STATE: MIXED\n"
            "SUGGESTED_GATE: HOLD\n"
            "QUALITY_SCORE: 50\n"
            "CONFIDENCE: MEDIUM\n"
            f"SUPPORTING_EVIDENCE_IDS: {support}\n"
            f"RISK_EVIDENCE_IDS: {risk}\n"
            "MISSING_EVIDENCE: 新闻, 基本面\n"
            "INVALIDATION_CONDITIONS: [P.CLOSE] 收盘价低于审计日收盘参考且未收回；[P.RPS10] 相对强度明显回落\n"
            f"SUMMARY: {summary}\n"
            "[/L3_OBSERVATION]"
        )

    def test_l3_observer_parser_binds_evidence_ids(self):
        evidence = "[P.RPS10] 88.0\n[P.TURNOVER] 5.00%\n[P.CLOSE] 10.00"
        parsed = MODULE.L3StrategicAuditor._parse_fin_auditor_response(
            self.l3_observer_raw(),
            evidence_text=evidence,
        )
        self.assertEqual(parsed["verdict"], "HOLD")
        self.assertEqual(parsed["lifecycle_stage"], "CONTINUATION")
        self.assertEqual(parsed["supporting_evidence_ids"], ["P.RPS10"])

    def test_l3_observer_parser_rejects_unbound_evidence_id(self):
        evidence = "[P.RPS10] 88.0\n[P.TURNOVER] 5.00%\n[P.CLOSE] 10.00"
        with self.assertRaisesRegex(ValueError, "L3_OBSERVER_UNBOUND_EVIDENCE_ID"):
            MODULE.L3StrategicAuditor._parse_fin_auditor_response(
                self.l3_observer_raw(support="NEWS.UNBOUND"),
                evidence_text=evidence,
            )

    def test_l3_observer_parser_accepts_versioned_model_probe_shape(self):
        candidate = MODULE.Candidate(
            symbol="300996.SZ",
            close=26.42,
            pct_chg=4.18,
            amount=336_000_000,
            turnover=5.82,
            vol_ratio=1.46,
            rps_10=82.0,
            pattern_score=62.0,
            ma_alignment=True,
            pattern_name="均线多头+放量",
        )
        l2 = MODULE.L2Result(
            symbol=candidate.symbol,
            pattern="volume_breakout",
            risk_score=27,
            fact_tags=["#trend_ok"],
        )
        evidence = MODULE.L3StrategicAuditor._build_l3_evidence_input(candidate, l2, "")
        raw = (
            "[L3_OBSERVATION]\n"
            "CONTRACT_VERSION: L3_OBSERVER_V5\n"
            "LIFECYCLE_STAGE: CONTINUATION\n"
            "THESIS_STATE: SUPPORTED\n"
            "SUGGESTED_GATE: PASS\n"
            "QUALITY_SCORE: 80\n"
            "CONFIDENCE: HIGH\n"
            "SUPPORTING_EVIDENCE_IDS: [P.PCT_CHG], [P.VOL_RATIO], [T.MA_ALIGNMENT], [L2.RISK_SCORE]\n"
            "RISK_EVIDENCE_IDS: NONE\n"
            "MISSING_EVIDENCE: NONE\n"
            "INVALIDATION_CONDITIONS: [Z.INST_FLOW] != 0; [Z.HOT_MONEY] != 0\n"
            "SUMMARY: 当前量价与均线结构支持延续判断，涨幅、量比和确定性L2风险分均来自已给证据；"
            "当前没有引用新闻或基本面，结论仅作为旁路观察。\n"
            "[/L3_OBSERVATION]"
        )
        parsed = MODULE.L3StrategicAuditor._parse_fin_auditor_response(raw, evidence)
        self.assertEqual(parsed["verdict"], "PASS")
        self.assertEqual(parsed["audit_score"], 80)

    def test_l3_parser_accepts_new_and_legacy_section_names(self):
        for section in ("Entry_Preconditions", "Suggested_Entry"):
            raw = (
                "输入量价信号有一定延续性，但确认信息仍然有限。当前只根据已提供的涨幅、换手率、量比与RPS判断，"
                "没有把缺失的基本面或新闻当作正负证据，因此维持等待确认的中性判断。\n"
                "[FINAL_DECISION]\nVERDICT: HOLD\nRISK_SCORE: 55\n"
                "[Invalidate_Condition]\n- 收盘价跌破当前参考位\n"
                f"[{section}]\n- 等待确认\n[/FINAL_DECISION]"
            )
            parsed = MODULE.L3StrategicAuditor._parse_fin_auditor_response(raw)
            self.assertEqual(parsed["falsifiable_conditions"], ["收盘价跌破当前参考位"])

    def test_l3_parser_rejects_template_residue_and_unsupported_claims(self):
        residue = (
            "这里给出一段足够长的分析，但它仍然照抄了根据上述模板填写的提示词，因此不能作为可靠的审计结论。"
            "这段文字继续补足长度，确保失败原因来自语义质量而不是简单的文本长度检查。\n"
            "[FINAL_DECISION]\nVERDICT: PASS\nRISK_SCORE: 35\n"
            "[Invalidate_Condition]\n- 条件1\n"
            "[Entry_Preconditions]\n- 等待确认\n[/FINAL_DECISION]"
        )
        with self.assertRaisesRegex(ValueError, "L3_TEMPLATE_RESIDUE"):
            MODULE.L3StrategicAuditor._parse_fin_auditor_response(residue)

        unsupported = (
            "当前量价信号仍需确认，同时MACD已经形成金叉，且二季度营收显著增长，因此风险较低。"
            "这里继续说明输入证据的局限，但前述两项事实并未在输入中出现，不能被系统接受。\n"
            "[FINAL_DECISION]\nVERDICT: PASS\nRISK_SCORE: 35\n"
            "[Invalidate_Condition]\n- 收盘价跌破已提供的收盘参考\n"
            "[Entry_Preconditions]\n- 等待T+1确认\n[/FINAL_DECISION]"
        )
        with self.assertRaisesRegex(ValueError, "L3_UNSUPPORTED_EVIDENCE"):
            MODULE.L3StrategicAuditor._parse_fin_auditor_response(
                unsupported,
                evidence_text="SYM:000001.SZ PCT:+3.0% RPS:88 FIN:N/A",
            )

    def test_l3_parser_rejects_repeated_blocks_and_trade_instructions(self):
        reasoning = (
            "输入显示量价和相对强度偏强，但这些字段只支持候选审计，仍需结合下一交易日表现确认。"
            "缺失项保持未知，不据此增加或降低风险，当前结论只代表继续观察资格。\n"
        )
        block = (
            "[FINAL_DECISION]\nVERDICT: HOLD\nRISK_SCORE: 55\n"
            "[Invalidate_Condition]\n- RPS跌破当前相对强度区间\n"
            "[Entry_Preconditions]\n- 等待T+1量价确认\n[/FINAL_DECISION]"
        )
        with self.assertRaisesRegex(ValueError, "L3_FINAL_BLOCK_NOT_UNIQUE"):
            MODULE.L3StrategicAuditor._parse_fin_auditor_response(reasoning + block + "\n" + block)

        actionable = (
            "输入显示RPS较强并伴随放量，因此这是强买入信号，后续可以继续持有。"
            "其余缺失字段保持未知，但这里仍然给出了候选审计阶段不应出现的交易动作。\n"
            + block
        )
        with self.assertRaisesRegex(ValueError, "L3_TRADE_INSTRUCTION"):
            MODULE.L3StrategicAuditor._parse_fin_auditor_response(
                actionable,
                evidence_text="SYM:000001.SZ PCT:+3.0% RPS:88 VR:1.5 FIN:N/A",
            )

    def test_l3_parser_allows_explicit_unknown_evidence(self):
        raw = (
            "量价与RPS显示出一定相对强度，但输入未提供MACD数据，也没有提供季度营收信息，无法据此判断。"
            "本次仅把这些缺失项列为待验证，不将其作为通过或否决证据，整体仍需要下一交易日确认。\n"
            "[FINAL_DECISION]\nVERDICT: HOLD\nRISK_SCORE: 55\n"
            "[Invalidate_Condition]\n- RPS跌破当前相对强度区间\n"
            "[Entry_Preconditions]\n- 等待T+1量价确认\n[/FINAL_DECISION]"
        )
        parsed = MODULE.L3StrategicAuditor._parse_fin_auditor_response(
            raw,
            evidence_text="SYM:000001.SZ PCT:+3.0% RPS:88 FIN:N/A",
        )
        self.assertEqual(parsed["verdict"], "HOLD")

    def test_common_l3_guard_covers_json_schema_models(self):
        payload = {
            "verdict": "VETO",
            "audit_score": 30,
            "reasoning": (
                "量价存在短期过热风险，同时机构增仓和游资介入表明投机资金集中。"
                "这些额外资金事实并未出现在实际输入中，因此该JSON结论不能被接受。"
            ),
            "falsifiable_conditions": ["机构停止增仓"],
        }
        with self.assertRaisesRegex(ValueError, "L3_UNSUPPORTED_EVIDENCE"):
            MODULE.L3StrategicAuditor._validate_l3_semantic_payload(
                payload,
                evidence_text="SYM:000001.SZ PCT:+3.0% RPS:88 VR:1.5 FIN:N/A",
                raw_response='{"verdict":"VETO"}',
            )

    def test_common_l3_guard_rejects_metric_semantic_misuse(self):
        payload = {
            "verdict": "HOLD",
            "audit_score": 50,
            "reasoning": (
                "RPS\u9ad8\u8bf4\u660e\u80a1\u4ef7\u5904\u4e8e\u8fd1\u4e00\u5e74\u6700\u9ad8\u4f4d\uff0c\u56e0\u6b64\u540e\u7eed\u4e0a\u6da8\u7a7a\u95f4\u5df2\u7ecf\u53d7\u9650\u3002"
                "\u8be5\u7ed3\u8bba\u628a\u6a2a\u622a\u9762\u76f8\u5bf9\u5f3a\u5ea6\u9519\u8bef\u89e3\u91ca\u6210\u4e2a\u80a1\u5386\u53f2\u4ef7\u683c\u4f4d\u7f6e\uff0c\u4e0d\u80fd\u8fdb\u5165\u5ba1\u8ba1\u8bc1\u636e\u94fe\u3002"
            ),
            "falsifiable_conditions": ["RPS\u76f8\u5bf9\u5f3a\u5ea6\u56de\u843d"],
        }
        with self.assertRaisesRegex(ValueError, "metric_semantics:RPS_AS_PRICE_POSITION"):
            MODULE.L3StrategicAuditor._validate_l3_semantic_payload(
                payload,
                evidence_text="SYM:000001.SZ PCT:+3.0% RPS:88 VR:1.5 FIN:N/A",
                raw_response='{"verdict":"HOLD"}',
            )

    def test_directional_guard_accepts_matching_structured_flow_tag(self):
        issues = MODULE._find_evidence_contract_issues(
            "\u673a\u6784\u51c0\u4e70\u5165\u4e3a\u5019\u9009\u63d0\u4f9b\u8d44\u91d1\u4fa7\u652f\u6301\u3002",
            "SYM:000001.SZ #inst_buy",
        )
        self.assertEqual(issues, [])

    def test_directional_guard_rejects_claim_reversing_structured_flow_tag(self):
        positive_claim = MODULE._find_evidence_contract_issues(
            "\u673a\u6784\u6301\u7eed\u51c0\u4e70\u5165\u4e3a\u5019\u9009\u63d0\u4f9b\u8d44\u91d1\u4fa7\u652f\u6301\u3002",
            "SYM:000001.SZ #inst_sell",
        )
        negative_claim = MODULE._find_evidence_contract_issues(
            "\u673a\u6784\u6301\u7eed\u51c0\u5356\u51fa\u6784\u6210\u8d44\u91d1\u4fa7\u98ce\u9669\u3002",
            "SYM:000001.SZ #inst_buy",
        )
        self.assertIn(
            "direction:institution:positive_claim_vs_negative_evidence",
            positive_claim,
        )
        self.assertIn(
            "direction:institution:negative_claim_vs_positive_evidence",
            negative_claim,
        )

    def test_directional_guard_rejects_generic_flow_reversals(self):
        main_flow = MODULE._find_evidence_contract_issues(
            "\u4e3b\u529b\u8d44\u91d1\u51c0\u6d41\u5165\u660e\u663e\u3002",
            "SYM:000001.SZ #inst_sell",
        )
        margin_flow = MODULE._find_evidence_contract_issues(
            "\u8d44\u91d1\u51c0\u6d41\u5165\u8f83\u5f3a\u3002",
            "SYM:000001.SZ #margin_outflow",
        )
        negative_flow = MODULE._find_evidence_contract_issues(
            "\u4e3b\u529b\u8d44\u91d1\u51c0\u6d41\u51fa\u660e\u663e\u3002",
            "SYM:000001.SZ #inst_buy",
        )
        self.assertIn(
            "direction:generic_flow:positive_claim_vs_negative_evidence",
            main_flow,
        )
        self.assertIn(
            "direction:generic_flow:positive_claim_vs_negative_evidence",
            margin_flow,
        )
        self.assertIn(
            "direction:generic_flow:negative_claim_vs_positive_evidence",
            negative_flow,
        )

    def test_directional_guard_understands_local_negation(self):
        issues = MODULE._find_evidence_contract_issues(
            "\u673a\u6784\u672a\u51cf\u4ed3\u3002",
            "SYM:000001.SZ #inst_buy",
        )
        self.assertEqual(issues, [])

    def test_directional_guard_understands_clause_level_uncertainty(self):
        uncertain = MODULE._find_evidence_contract_issues(
            "未见明确的资金净流入证据。",
            "SYM:000001.SZ",
        )
        asserted = MODULE._find_evidence_contract_issues(
            "未见资金流出证据，但资金净流入明显。",
            "SYM:000001.SZ",
        )
        self.assertEqual(uncertain, [])
        self.assertIn("flow_generic:资金净流入明显", asserted)

    def test_directional_guard_accepts_matching_generic_flow(self):
        issues = MODULE._find_evidence_contract_issues(
            "\u4e3b\u529b\u8d44\u91d1\u51c0\u6d41\u5165\u8f83\u5f3a\u3002",
            "SYM:000001.SZ #margin_inflow",
        )
        self.assertEqual(issues, [])

    def test_metric_semantic_misuse_is_detected(self):
        issues = MODULE._find_metric_semantic_misuse(
            "RPS为99.7，说明价格处于近一年最高区间；VR为1.5，说明资金流入较强。"
        )
        self.assertIn("RPS_AS_PRICE_POSITION", issues)
        self.assertIn("VR_AS_CAPITAL_FLOW", issues)
        self.assertEqual(
            MODULE._find_metric_semantic_misuse("RPS不代表价格处于近一年最高区间。"),
            [],
        )

    def test_uncertainty_in_previous_clause_does_not_whitelist_new_claim(self):
        issues = MODULE._find_unsupported_evidence_claims(
            "输入未提供MACD数据，但MACD已经形成金叉。",
            "SYM:000001.SZ RPS:88 FIN:N/A",
        )
        self.assertIn("technical_macd:MACD", issues)

    def test_deterministic_l2_tags_follow_actual_rps(self):
        auditor = MODULE.L2SentinelAuditor()
        strong = auditor._deterministic_audit(
            MODULE.Candidate(symbol="000001.SZ", pct_chg=4.0, turnover=4.0, vol_ratio=1.4, rps_10=92),
            time.time(),
        )
        weak = auditor._deterministic_audit(
            MODULE.Candidate(symbol="000002.SZ", pct_chg=4.0, turnover=4.0, vol_ratio=1.4, rps_10=35),
            time.time(),
        )
        self.assertIn("#strong_rps", strong.fact_tags)
        self.assertNotIn("#weak_rps", strong.fact_tags)
        self.assertIn("#weak_rps", weak.fact_tags)

    def test_deterministic_l3_separates_overheat_continuation_and_hard_risk(self):
        auditor = MODULE.L3StrategicAuditor.__new__(MODULE.L3StrategicAuditor)
        hot_candidate = MODULE.Candidate(
            symbol="300765.SZ", close=42.0, pct_chg=12.33,
            turnover=2.42, vol_ratio=1.54, rps_10=99.7,
        )
        hot_l2 = MODULE.L2Result(
            symbol=hot_candidate.symbol,
            risk_score=48,
            fact_tags=["#volume_expansion", "#strong_rps"],
        )
        hot = auditor._deterministic_l3_audit(
            hot_candidate,
            hot_l2,
            "[STRATEGIC_AMNESIA] No case file",
        )
        self.assertEqual(hot.verdict, MODULE.Verdict.HOLD)
        self.assertEqual(hot.audit_score, 48)
        self.assertIn("#L3_DETERMINISTIC", hot.fact_tags)
        self.assertTrue(hot.falsifiable_conditions)

        normal_candidate = MODULE.Candidate(
            symbol="600000.SH", close=10.0, pct_chg=4.0,
            turnover=4.0, vol_ratio=1.5, rps_10=82.0,
        )
        normal = auditor._deterministic_l3_audit(
            normal_candidate,
            MODULE.L2Result(symbol=normal_candidate.symbol, risk_score=35, fact_tags=["#strong_rps"]),
            "",
        )
        self.assertEqual(normal.verdict, MODULE.Verdict.PASS)

        high_rps_only = auditor._deterministic_l3_audit(
            MODULE.Candidate(
                symbol="600001.SH", close=12.0, pct_chg=4.0,
                turnover=4.0, vol_ratio=1.5, rps_10=97.0,
            ),
            MODULE.L2Result(symbol="600001.SH", risk_score=35, fact_tags=["#strong_rps"]),
            "",
        )
        self.assertEqual(high_rps_only.verdict, MODULE.Verdict.PASS)

        hard = auditor._deterministic_l3_audit(
            normal_candidate,
            MODULE.L2Result(symbol=normal_candidate.symbol, risk_score=75, fact_tags=["#L2_HARD_VETO_DISTRIBUTION"]),
            "",
        )
        self.assertEqual(hard.verdict, MODULE.Verdict.VETO)

    def test_default_l3_path_does_not_call_local_model(self):
        auditor = MODULE.L3StrategicAuditor.__new__(MODULE.L3StrategicAuditor)
        auditor.model = "fin-auditor:latest"
        candidate = MODULE.Candidate(
            symbol="600000.SH", close=10.0, pct_chg=4.0,
            turnover=4.0, vol_ratio=1.5, rps_10=82.0,
        )
        l2 = MODULE.L2Result(
            symbol=candidate.symbol,
            risk_score=35,
            fact_tags=["#strong_rps"],
        )
        with patch.object(MODULE, "L3_MODEL_ENABLED", False), \
             patch.object(MODULE, "ZETA_AVAILABLE", False), \
             patch.object(MODULE.HardwareMonitor, "is_overheating", return_value=False), \
             patch.object(MODULE.COMPUTE_GATEWAY, "ollama_generate", side_effect=AssertionError("model called")):
            result = auditor.audit(candidate, l2)
        self.assertEqual(result.verdict, MODULE.Verdict.PASS)
        self.assertEqual(result.raw_response, "[deterministic_l3]")

    def test_l2_model_observer_cannot_change_deterministic_authority(self):
        auditor = MODULE.L2SentinelAuditor.__new__(MODULE.L2SentinelAuditor)
        auditor.server = "http://observer.invalid"
        auditor.model = "lfm2.5-thinking:1.2b"
        candidate = MODULE.Candidate(
            symbol="600000.SH",
            close=10.0,
            pct_chg=4.0,
            amount=80_000_000,
            turnover=4.0,
            vol_ratio=1.5,
            rps_10=82.0,
            pattern_score=65.0,
            ma_alignment=True,
            pattern_name="均线多头",
        )
        payload = {
            "contract_version": "L2_MISSING_EVIDENCE_OBSERVER_V1",
            "missing_evidence_ids": [
                "M.CURRENT_NEWS",
                "M.FUNDAMENTAL_QUALITY",
                "M.MULTIDAY_PATH",
                "M.SECTOR_CONTEXT",
            ],
        }
        expected = auditor._deterministic_audit(candidate, time.time())
        with patch.object(MODULE, "L2_MODEL_ENABLED", True), \
             patch.object(MODULE, "_resolve_ollama_format", return_value=("json", "TEST")), \
             patch.object(
                 MODULE.COMPUTE_GATEWAY,
                 "ollama_generate",
                 return_value=FakeOllamaResponse(__import__("json").dumps(payload, ensure_ascii=False)),
             ) as mocked_generate:
            result = auditor.audit(candidate)
        self.assertEqual(result.risk_score, expected.risk_score)
        self.assertEqual(result.fact_tags, expected.fact_tags)
        self.assertEqual(result.pattern, expected.pattern)
        self.assertIn("L2_MISSING_OBSERVER_V1:VALID", result.extraction_mode)
        self.assertIn('"M.CURRENT_NEWS"', result.thinking_trace)
        self.assertIn('"observer_trace_sha256"', result.thinking_trace)
        self.assertNotIn("structure_state", result.thinking_trace)
        self.assertEqual(
            mocked_generate.call_args.kwargs["timeout"],
            MODULE.L2_MICROTASK_TIMEOUT_SECONDS,
        )

    def test_l2_observer_rejects_authority_and_unbound_fields(self):
        evidence = "[P.RPS10] 88.0\n[P.TURNOVER] 5.00%"
        payload = {
            "contract_version": "L2_OBSERVER_V5",
            "structure_state": "HEALTHY",
            "supporting_evidence_ids": ["NEWS.UNBOUND"],
            "risk_evidence_ids": [],
            "evidence_conflicts": [],
            "missing_evidence": [],
            "questions_for_l3": [],
            "confidence": "HIGH",
            "summary": "现有相对强度可以支持继续观察，但仍然只能依据输入证据判断结构状态。",
            "risk_score": 10,
        }
        with self.assertRaisesRegex(ValueError, "L2_OBSERVER_AUTHORITY_FIELD"):
            MODULE.L2SentinelAuditor._validate_observer_payload(payload, evidence)
        payload.pop("risk_score")
        with self.assertRaisesRegex(ValueError, "L2_OBSERVER_UNBOUND_EVIDENCE_ID"):
            MODULE.L2SentinelAuditor._validate_observer_payload(payload, evidence)

    def test_l2_observer_rejects_cross_field_contradictions(self):
        evidence = "[P.RPS10] 88.0\n[P.TURNOVER] 5.00%"
        payload = {
            "contract_version": "L2_OBSERVER_V5",
            "structure_state": "EXHAUSTED",
            "supporting_evidence_ids": ["P.RPS10"],
            "risk_evidence_ids": [],
            "evidence_conflicts": [],
            "missing_evidence": [],
            "questions_for_l3": [],
            "confidence": "LOW",
            "summary": "当前证据不足，需要更多信息才能确认结构状态，因此暂时不能形成可靠的阶段判断。",
        }
        with self.assertRaisesRegex(ValueError, "MISSING_EVIDENCE_CONTRADICTION"):
            MODULE.L2SentinelAuditor._validate_observer_payload(payload, evidence)

    def test_l2_observer_state_contract_is_symmetric(self):
        candidate = MODULE.Candidate(symbol="600000.SH")
        prompt, _ = MODULE.L2SentinelAuditor._build_observer_prompt(candidate)
        self.assertIn("No structure state is the default", prompt)
        self.assertIn("HEALTHY requires at least one supporting_evidence_id", prompt)
        self.assertIn("MIXED requires both supporting and risk evidence", prompt)
        self.assertIn("EXHAUSTED requires at least one risk_evidence_id", prompt)
        self.assertIn("UNKNOWN requires a non-empty missing_evidence list", prompt)
        self.assertIn("P.AMOUNT is traded amount in yuan", prompt)

    def test_l2_observer_rejects_unsubstantiated_non_exhausted_states(self):
        evidence = "[P.RPS10] 88.0\n[P.TURNOVER] 5.00%"
        base = {
            "contract_version": "L2_OBSERVER_V5",
            "supporting_evidence_ids": [],
            "risk_evidence_ids": [],
            "evidence_conflicts": [],
            "missing_evidence": [],
            "questions_for_l3": [],
            "confidence": "MEDIUM",
            "summary": "当前结构状态需要严格依据已经提供的证据进行判断，不能用未经绑定的信息补足结论。",
        }

        healthy = dict(base, structure_state="HEALTHY")
        with self.assertRaisesRegex(ValueError, "HEALTHY_WITHOUT_SUPPORT_EVIDENCE"):
            MODULE.L2SentinelAuditor._validate_observer_payload(healthy, evidence)

        mixed = dict(base, structure_state="MIXED")
        with self.assertRaisesRegex(ValueError, "MIXED_WITHOUT_TWO_SIDED_EVIDENCE"):
            MODULE.L2SentinelAuditor._validate_observer_payload(mixed, evidence)

        unknown = dict(base, structure_state="UNKNOWN")
        with self.assertRaisesRegex(ValueError, "UNKNOWN_WITHOUT_MISSING_EVIDENCE"):
            MODULE.L2SentinelAuditor._validate_observer_payload(unknown, evidence)

    def test_l2_observer_accepts_each_state_with_required_evidence(self):
        evidence = "[P.RPS10] 88.0\n[P.TURNOVER] 5.00%"
        base = {
            "contract_version": "L2_OBSERVER_V5",
            "supporting_evidence_ids": [],
            "risk_evidence_ids": [],
            "evidence_conflicts": [],
            "missing_evidence": [],
            "questions_for_l3": [],
            "confidence": "MEDIUM",
            "summary": "当前结构状态严格依据已经提供的相对强度与换手证据进行判断，并保留后续复核空间。",
        }
        payloads = [
            dict(base, structure_state="HEALTHY", supporting_evidence_ids=["P.RPS10"]),
            dict(
                base,
                structure_state="MIXED",
                supporting_evidence_ids=["P.RPS10"],
                risk_evidence_ids=["P.TURNOVER"],
            ),
            dict(base, structure_state="EXHAUSTED", risk_evidence_ids=["P.TURNOVER"]),
            dict(base, structure_state="UNKNOWN", missing_evidence=["需要更长周期结构"]),
        ]
        for payload in payloads:
            with self.subTest(state=payload["structure_state"]):
                normalized = MODULE.L2SentinelAuditor._validate_observer_payload(payload, evidence)
                self.assertEqual(normalized["structure_state"], payload["structure_state"])

    def test_l3_metric_guard_rejects_volume_ratio_as_buy_activity(self):
        issues = MODULE._find_metric_semantic_misuse(
            "量比的升高可能预示着买盘活跃度增强。"
        )
        self.assertIn("VR_AS_BUY_SIDE_ACTIVITY", issues)

    def test_l3_model_observer_preserves_deterministic_l4_handoff(self):
        auditor = MODULE.L3StrategicAuditor.__new__(MODULE.L3StrategicAuditor)
        auditor.model = "fin-auditor:latest"
        auditor._zeta_online_outage_code = ""
        candidate = MODULE.Candidate(
            symbol="600000.SH",
            close=10.0,
            pct_chg=4.0,
            turnover=4.0,
            vol_ratio=1.5,
            rps_10=82.0,
        )
        l2 = MODULE.L2Result(
            symbol=candidate.symbol,
            risk_score=35,
            fact_tags=["#strong_rps"],
        )
        observer_raw = (
            '{"contract_version":"L3_INVALIDATION_SELECTOR_OBSERVER_V1",'
            '"selected_invalidation_ids":["K.001"]}'
        )
        observer_payload = {
            "contract_version": "L3_INVALIDATION_SELECTOR_OBSERVER_V1",
            "selected_invalidation_ids": ["K.001"],
            "selected_conditions": ["收盘价低于审计日参考"],
            "status": "VALID",
        }
        expected = auditor._deterministic_l3_audit(candidate, l2, "")
        with patch.object(MODULE, "L3_MODEL_ENABLED", True), \
             patch.object(MODULE, "L3_MODEL_AUTHORITY_ENABLED", False), \
             patch.object(
                 auditor,
                 "_run_invalidation_observer",
                 return_value=(observer_raw, observer_payload, 1234.0),
             ), \
             patch.object(auditor, "_run_zeta_post_audit"):
            result = auditor.audit(candidate, l2)
        self.assertEqual(result.verdict, MODULE.Verdict.PASS)
        self.assertEqual(result.audit_score, 65)
        self.assertEqual(result.reasoning.startswith("600000.SH 的确定性输入"), True)
        self.assertIn('"selected_invalidation_ids": ["K.001"]', result.thinking_trace)
        self.assertIn('"observer_trace_sha256"', result.thinking_trace)
        self.assertNotIn("suggested_gate", result.thinking_trace)
        self.assertEqual(result.raw_response, observer_raw)
        self.assertEqual(result.logic_hash, expected.logic_hash)
        self.assertEqual(result.audit_trace_text, expected.audit_trace_text)

    def test_l3_invalidation_observer_uses_microtask_timeout(self):
        auditor = MODULE.L3StrategicAuditor.__new__(MODULE.L3StrategicAuditor)
        auditor.server = "http://observer.invalid"
        auditor.model = "fin-auditor-observer:v0.1"
        candidate = MODULE.Candidate(symbol="600000.SH")
        authoritative = MODULE.L3Result(
            symbol=candidate.symbol,
            verdict=MODULE.Verdict.PASS,
            audit_score=65,
            reasoning="当前量价结构具备建设性。",
            falsifiable_conditions=["收盘价跌破审计日参考且未快速收回"],
        )
        raw = (
            '{"contract_version":"L3_INVALIDATION_SELECTOR_OBSERVER_V1",'
            '"selected_invalidation_ids":["K.001"]}'
        )
        with patch.object(MODULE, "_resolve_ollama_format", return_value=("json", "TEST")), \
             patch.object(
                 MODULE.COMPUTE_GATEWAY,
                 "ollama_generate",
                 return_value=FakeOllamaResponse(raw),
             ) as mocked_generate:
            _, payload, _ = auditor._run_invalidation_observer(candidate, authoritative)
        self.assertEqual(payload["status"], "VALID")
        self.assertEqual(
            mocked_generate.call_args.kwargs["timeout"],
            MODULE.L3_MICROTASK_TIMEOUT_SECONDS,
        )

    def test_l3_model_authority_switch_is_fail_closed(self):
        auditor = MODULE.L3StrategicAuditor.__new__(MODULE.L3StrategicAuditor)
        candidate = MODULE.Candidate(symbol="600000.SH")
        l2 = MODULE.L2Result(symbol=candidate.symbol)
        with patch.object(MODULE, "L3_MODEL_ENABLED", True), \
             patch.object(MODULE, "L3_MODEL_AUTHORITY_ENABLED", True):
            with self.assertRaisesRegex(RuntimeError, "L3_MODEL_AUTHORITY_NOT_AUTHORIZED"):
                auditor.audit(candidate, l2)

    def test_prompts_do_not_force_buy_sell_or_invented_bear_cases(self):
        self.assertIn("evidence_packet", MODULE.BULL_USER_PROMPT)
        self.assertNotIn("STRONG_BUY", MODULE.BULL_USER_PROMPT)
        self.assertIn("FATAL/MATERIAL/MINOR", MODULE.BEAR_USER_PROMPT)
        self.assertNotIn("STRONG_SELL", MODULE.BEAR_USER_PROMPT)
        self.assertNotIn("至少3条", MODULE.BULL_USER_PROMPT)
        self.assertIn("不得凑数", MODULE.L4_1_PRESCREEN_PROMPT)
        self.assertIn("不代表买入", MODULE.L41_PREAUDIT_PROMPT)
        self.assertIn("PASS only grants eligibility", MODULE.JUDGE_USER_PROMPT)
        self.assertIn("eligibility_score", MODULE.JUDGE_USER_PROMPT)
        self.assertIn("authoritative_verdict", MODULE.NOTARY_USER_PROMPT)

    def test_l4_evidence_packet_carries_authoritative_upstream_fields(self):
        court = self.court()
        candidate = MODULE.Candidate(
            symbol="600000.SH",
            trade_date="2026-08-18",
            close=12.3,
            pct_chg=4.2,
            turnover=6.1,
            rps_10=83.0,
            vol_ratio=1.6,
            pattern_score=60,
            ma_alignment=True,
            pattern_name="均线多头+放量",
            zeta_inst_buy=-1,
        )
        l3 = MODULE.L3Result(
            symbol=candidate.symbol,
            verdict=MODULE.Verdict.HOLD,
            audit_score=50,
            reasoning="确定性证据混合。",
            falsifiable_conditions=["后续收盘价跌破审计日收盘价12.30且未快速收回"],
            l2_risk_score=44,
            l2_pattern="volume_breakout",
            fact_tags=["#strong_rps", "#inst_sell"],
        )
        packet = court._build_evidence_packet(candidate, l3, rag_present=True)
        facts = {item["evidence_id"]: item["value"] for item in packet["facts"]}
        self.assertEqual(packet["contract_version"], "L4_EVIDENCE_PACKET_V1")
        self.assertEqual(facts["L1.RPS_10"], 83.0)
        self.assertEqual(facts["L2.RISK_SCORE"], 44)
        self.assertEqual(facts["L2.PATTERN"], "volume_breakout")
        self.assertEqual(facts["L3.VERDICT"], "HOLD")
        self.assertEqual(facts["ZETA.INST_DIRECTION"], -1)
        self.assertIn("L3.COND.01", packet["allowed_condition_ids"])
        self.assertIn("RAG.CONTEXT", packet["allowed_evidence_ids"])

    def test_structured_advocate_labels_map_to_internal_strength(self):
        court = self.court()
        packet = court._legacy_call_evidence_packet(
            "600519",
            1.0,
            "#strong_rps",
            70,
            "reason",
            "condition",
        )

        def call_with(content, method):
            with patch.object(MODULE, "api_call_with_retry", return_value=FakeResponse(content)):
                return method(
                    "600519",
                    1.0,
                    "#strong_rps",
                    70,
                    "reason",
                    "",
                    "condition",
                    evidence_packet=packet,
                )

        strong = call_with(
            '{"case_strength":"STRONG","thesis":"涨幅证据支持延续审计",'
            '"evidence_refs":["L1.PCT_CHG"],"condition_refs":["L3.COND.01"],'
            '"unknowns":[]}',
            court._call_bull,
        )
        fatal = call_with(
            '{"risk_strength":"FATAL","risk_thesis":"涨幅证据构成不可接受风险",'
            '"evidence_refs":["L1.PCT_CHG"],"condition_refs":["L3.COND.01"],'
            '"unknowns":[]}',
            court._call_bear,
        )

        self.assertEqual((strong["verdict"], strong["score"]), ("SUPPORT_STRONG", 82))
        self.assertEqual((fatal["verdict"], fatal["score"]), ("RISK_FATAL", 82))
        self.assertEqual(strong["semantic_quality"], "VALID")
        self.assertEqual(fatal["semantic_quality"], "VALID")

    def test_advocate_reference_ids_are_not_treated_as_narrative_claims(self):
        court = self.court()
        candidate = MODULE.Candidate(symbol="600000.SH", zeta_hot_money=0)
        l3 = MODULE.L3Result(
            symbol="600000.SH",
            verdict=MODULE.Verdict.PASS,
            audit_score=65,
            l2_risk_score=30,
        )
        packet = court._build_evidence_packet(candidate, l3)
        payload = (
            '{"risk_strength":"MINOR","risk_thesis":"未识别独立硬风险",'
            '"evidence_refs":["ZETA.HOT_MONEY_DIRECTION"],'
            '"condition_refs":[],"unknowns":[]}'
        )
        with patch.object(MODULE, "api_call_with_retry", return_value=FakeResponse(payload)):
            result = court._call_bear(
                "600000.SH", 0.0, "", 65, "趋势延续", "", "",
                evidence_packet=packet,
            )
        self.assertEqual(result["semantic_quality"], "VALID")
        self.assertEqual(result["verdict"], "RISK_MINOR")

    def test_zero_flow_value_allows_neutral_description_but_rejects_direction(self):
        court = self.court()
        packet = court._build_evidence_packet(
            MODULE.Candidate(symbol="600000.SH", zeta_lhb_net=0),
            MODULE.L3Result(symbol="600000.SH", verdict=MODULE.Verdict.PASS),
        )
        evidence = court._render_evidence_packet(packet)
        neutral = MODULE._find_evidence_contract_issues("龙虎榜净额为0", evidence)
        directional = MODULE._find_evidence_contract_issues("龙虎榜出现净买入", evidence)
        self.assertEqual(neutral, [])
        self.assertIn("direction:lhb:claim_vs_neutral_evidence", directional)

    def test_advocate_rejects_reference_dump(self):
        court = self.court()
        packet = court._build_evidence_packet(
            MODULE.Candidate(symbol="600000.SH", pct_chg=3.0),
            MODULE.L3Result(
                symbol="600000.SH",
                verdict=MODULE.Verdict.PASS,
                audit_score=70,
                fact_tags=["#strong_rps", "#trend_ok"],
            ),
        )
        refs = packet["allowed_evidence_ids"][:9]
        payload = {
            "case_strength": "STRONG",
            "thesis": "关键证据支持继续审计",
            "evidence_refs": refs,
            "condition_refs": [],
            "unknowns": [],
        }
        with patch.object(
            MODULE,
            "api_call_with_retry",
            return_value=FakeResponse(json.dumps(payload, ensure_ascii=False)),
        ):
            result = court._call_bull(
                "600000.SH", 3.0, "#strong_rps", 70, "量价较强", "", "",
                evidence_packet=packet,
            )
        self.assertEqual(result["semantic_quality"], "UNSUPPORTED_EVIDENCE")

    def test_plain_text_advocate_output_is_rejected(self):
        court = self.court()
        with patch.object(
            MODULE,
            "api_call_with_retry",
            return_value=FakeResponse("FINAL_SUPPORT: SUPPORT_STRONG"),
        ):
            result = court._call_bull(
                "600519", 1.0, "#strong_rps", 70, "reason", "", "condition"
            )
        self.assertEqual(result["semantic_quality"], "UNSUPPORTED_EVIDENCE")
        self.assertIn("CONTRACT_GUARD", result["report"])

    def test_local_judge_penalizes_stronger_bear_risk(self):
        court = self.court()
        minor = court._local_ruling({"score": 72}, {"score": 48}, 0.5)
        fatal = court._local_ruling({"score": 72}, {"score": 82}, 0.5)
        self.assertGreater(minor["S_v3"], fatal["S_v3"])

    def test_local_ruling_neutralizes_guarded_model_scores(self):
        court = self.court()
        ruling = court._local_ruling(
            {"score": 90, "semantic_quality": "UNSUPPORTED_EVIDENCE"},
            {"score": 10, "semantic_quality": "UNSUPPORTED_EVIDENCE"},
            0.5,
        )
        self.assertEqual(ruling["S_v3"], 50.0)
        self.assertEqual(ruling["final_verdict"], "HOLD")
        self.assertIn("ignored_guarded=Bull,Bear", ruling["ruling"])

    def test_l4_evidence_guard_downgrades_unsupported_bull_and_bear(self):
        court = self.court()
        with patch.object(
            MODULE,
            "api_call_with_retry",
            return_value=FakeResponse(
                '{"case_strength":"STRONG","thesis":"Q2营收同比增长形成强支撑",'
                '"evidence_refs":["L1.PCT_CHG"],"condition_refs":["L3.COND.01"],'
                '"unknowns":[]}'
            ),
        ):
            bull = court._call_bull("600000.SH", 3.0, "#strong_rps", 70, "量价较强", "", "跌破收盘价")
        with patch.object(
            MODULE,
            "api_call_with_retry",
            return_value=FakeResponse(
                '{"risk_strength":"FATAL","risk_thesis":"MACD死叉构成致命风险",'
                '"evidence_refs":["L1.PCT_CHG"],"condition_refs":["L3.COND.01"],'
                '"unknowns":[]}'
            ),
        ):
            bear = court._call_bear("600000.SH", 3.0, "#strong_rps", 70, "量价较强", "", "跌破收盘价")

        self.assertEqual((bull["verdict"], bull["score"]), ("SUPPORT_WEAK", 58))
        self.assertEqual((bear["verdict"], bear["score"]), ("RISK_MINOR", 48))
        self.assertEqual(bull["semantic_quality"], "UNSUPPORTED_EVIDENCE")
        self.assertEqual(bear["semantic_quality"], "UNSUPPORTED_EVIDENCE")

    def test_judge_and_notary_cannot_add_new_evidence(self):
        court = self.court()
        judge_payload = (
            '{"verdict":"PASS","eligibility_score":90,"confidence":90,'
            '"ruling":"Q2营收增长支持通过","decisive_evidence_refs":["L1.PCT_CHG"],'
            '"unresolved_gaps":[]}'
        )
        packet = court._legacy_call_evidence_packet(
            "600000.SH", 3.0, "#strong_rps", 70, "量价较强", ""
        )
        with patch.object(MODULE, "api_call_with_retry", return_value=FakeResponse(judge_payload)):
            judge = court._call_judge(
                "600000.SH",
                {"score": 82, "report": "量价支持"},
                {"score": 48, "report": "无独立硬风险"},
                0.5,
                "NEUTRAL",
                "",
                evidence_packet=packet,
            )
        self.assertEqual(judge["judge_parse_mode"], "evidence_guard")
        self.assertNotEqual(judge["final_verdict"], "PASS")

        notary_payload = (
            '{"stock_code":"600000.SH","final_verdict":"HOLD","eligibility_score":60,"confidence":88,'
            '"dominant_logic":"MACD金叉支持通过","bull_summary":"量价支持",'
            '"bear_summary":"无硬风险","fatal_risk_flag":false}'
        )
        with patch.object(MODULE, "api_call_with_retry", return_value=FakeResponse(notary_payload)):
            notary = court.post_audit_notary(
                MODULE.Candidate(symbol="600000.SH"),
                {},
                {"ruling": "量价支持", "final_verdict": "HOLD", "S_v3": 60},
                "量价支持",
                "无独立硬风险",
            )
        self.assertEqual(notary["notary_verdict"], "NOTARY_UNSUPPORTED_EVIDENCE")
        self.assertFalse(notary["hard_veto"])

    def test_judge_uses_eligibility_score_not_confidence(self):
        court = self.court()
        packet = court._legacy_call_evidence_packet(
            "600000.SH", 3.0, "#strong_rps", 70, "量价较强", ""
        )
        payload = (
            '{"verdict":"PASS","eligibility_score":72,"confidence":41,'
            '"ruling":"当前证据达到后续安全门资格",'
            '"decisive_evidence_refs":["L1.PCT_CHG"],"unresolved_gaps":["基本面"]}'
        )
        with patch.object(MODULE, "api_call_with_retry", return_value=FakeResponse(payload)):
            result = court._call_judge(
                "600000.SH",
                {"score": 82, "report": "量价支持"},
                {"score": 48, "report": "无独立硬风险"},
                0.5,
                "BULL",
                "",
                evidence_packet=packet,
            )
        self.assertEqual(result["final_verdict"], "PASS")
        self.assertEqual(result["S_v3"], 72)
        self.assertEqual(result["confidence"], 0.41)
        self.assertEqual(result["judge_parse_mode"], "json_contract_v3_2")

    def test_both_guarded_advocates_force_insufficient_evidence_hold(self):
        court = self.court()
        guarded = {
            "score": 90,
            "report": "guarded",
            "semantic_quality": "UNSUPPORTED_EVIDENCE",
        }
        result = court._call_judge(
            "600000.SH", guarded, guarded, 0.8, "BULL", ""
        )
        self.assertEqual(result["final_verdict"], "HOLD")
        self.assertGreaterEqual(result["S_v3"], MODULE.L4_WATCH_THRESHOLD)
        self.assertLess(result["S_v3"], MODULE.L4_PASS_THRESHOLD)

    def test_notary_must_echo_authoritative_verdict_and_score(self):
        court = self.court()
        payload = (
            '{"stock_code":"600000.SH","final_verdict":"PASS",'
            '"eligibility_score":72,"confidence":80,"dominant_logic":"量价支持",'
            '"bull_summary":"量价支持","bear_summary":"无硬风险",'
            '"fatal_risk_flag":false}'
        )
        with patch.object(MODULE, "api_call_with_retry", return_value=FakeResponse(payload)):
            result = court.post_audit_notary(
                MODULE.Candidate(symbol="600000.SH"),
                {},
                {"final_verdict": "HOLD", "S_v3": 60, "ruling": "证据不足"},
                "量价支持",
                "无硬风险",
            )
        self.assertEqual(result["notary_verdict"], "NOTARY_INCONSISTENT")
        self.assertFalse(result["hard_veto"])

    def test_notary_recorder_cannot_cap_judge_pass(self):
        court = self.court()
        result = MODULE.L4Result(symbol="600000.SH")
        score, terminal = court._apply_court_verdict_cap(
            result,
            76,
            {"final_verdict": "PASS", "S_v3": 76},
            "HOLD",
            False,
        )
        self.assertEqual(score, 76)
        self.assertFalse(terminal)

    def test_notary_strict_flags_do_not_restore_verdict_authority(self):
        court = self.court()
        court.notary_strict = True
        court.notary_veto_hard = True
        payload = (
            '{"stock_code":"600000.SH","final_verdict":"VETO",'
            '"eligibility_score":40,"confidence":90,"dominant_logic":"风险占优",'
            '"bull_summary":"支持不足","bear_summary":"风险明确",'
            '"fatal_risk_flag":true}'
        )
        with patch.object(MODULE, "api_call_with_retry", return_value=FakeResponse(payload)):
            result = court.post_audit_notary(
                MODULE.Candidate(symbol="600000.SH"),
                {},
                {"final_verdict": "VETO", "S_v3": 40, "ruling": "风险占优"},
                "支持不足",
                "风险明确",
            )
        self.assertEqual(result["notary_verdict"], "VETO")
        self.assertFalse(result["hard_veto"])

    def test_hold_attribution_rejects_open_ended_factor(self):
        court = self.court()
        payload = (
            '{"risk_aversion_factor":"流动性陷阱",'
            '"attribution_detail":"输入没有提供该事实"}'
        )
        with patch.object(MODULE, "api_call_with_retry", return_value=FakeResponse(payload)):
            result = court.analyze_hold_logic(
                "600000.SH",
                {"report": '{"case_strength":"WEAK"}'},
                {"report": '{"risk_strength":"MINOR"}'},
            )
        self.assertEqual(result["error_type"], "CONTRACT_REJECTED")


if __name__ == "__main__":
    unittest.main()
