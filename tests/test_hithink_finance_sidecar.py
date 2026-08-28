#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
LIB = ROOT / "01_engine" / "lib"
TOOLS = ROOT / "tools"
for item in (str(LIB), str(TOOLS)):
    if item not in sys.path:
        sys.path.insert(0, item)

import hithink_finance_bridge as bridge_module  # noqa: E402
from hithink_finance_bridge import (  # noqa: E402
    HithinkBridgeError,
    HithinkCredentialError,
    HithinkFinanceBridge,
    HithinkResult,
    trade_date_ms,
)


def load_snapshot_module():
    path = TOOLS / "hithink_finance_source_snapshot.py"
    spec = importlib.util.spec_from_file_location("hithink_finance_source_snapshot", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise bridge_module.requests.HTTPError("synthetic http failure")

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, *, params, headers, timeout, allow_redirects):
        self.calls.append({
            "url": url, "params": params, "headers": headers,
            "timeout": timeout, "allow_redirects": allow_redirects,
        })
        return self.responses.pop(0)


def result(endpoint: str, data):
    return HithinkResult(
        endpoint=endpoint,
        request_id="req-test",
        retrieved_at="2026-08-22T12:00:00+08:00",
        elapsed_ms=5,
        source_timestamp=data.get("timestamp") if isinstance(data, dict) else None,
        content_sha256="a" * 64,
        data=data,
    )


class FakeSnapshotClient:
    credential_configured = True

    def ticker_search(self, query):
        return result("/ticker", {"item": [{"thscode": "300016.SZ", "name": query}]})

    def ths_index_list(self, *, tag):
        return result("/catalog", {"timestamp": 1, "item": [
            {"thscode": "885338.TI", "name": "特高压"},
            {"thscode": "885999.TI", "name": "无关板块"},
        ]})

    def ths_constituents(self, code):
        return result("/members", {"timestamp": 2, "item": [
            {"thscode": "600089.SH", "name": "特变电工"},
            {"thscode": "601179.SH", "name": "中国西电"},
        ]})

    def index_snapshot(self, codes):
        return result("/index", {"timestamp": 3, "item": [{"thscode": codes[0]}]})

    def auction_snapshot(self, symbols, *, stage):
        return result("/auction", {"timestamp": 4, "item": [{"thscode": symbols[0], "stage": stage}]})

    def auction_benchmark(self, trade_date):
        return result("/benchmark", {"timestamp": 5, "date": trade_date, "item": []})

    def dragon_tiger(self, trade_date, *, board_type):
        return result("/lhb", {
            "timestamp": 6,
            "trade_date": trade_date,
            "board_type": board_type,
            "stock_count": 2,
            "stock_items": [
                {"thscode": "300016.SZ", "name": "北陆药业", "net_value": 1},
                {"thscode": "600000.SH", "name": "other", "net_value": 2},
            ],
            "hot_money_items": [],
        })

    def limit_pool(self, trade_date, *, pool, size):
        return result(f"/{pool}", {"timestamp": 7, "pagination": {"size": size}, "item": []})

    def limit_up_ladder(self):
        return result("/ladder", {"timestamp": 8, "item": []})


class HithinkFinanceSidecarTest(unittest.TestCase):
    def test_bridge_requires_key_and_rejects_custom_base_url(self):
        client = HithinkFinanceBridge(api_key="", session=FakeSession([]))
        with self.assertRaises(HithinkCredentialError):
            client.auction_benchmark("2026-08-21")
        with self.assertRaises(ValueError):
            HithinkFinanceBridge(api_key="fake", base_url="http://127.0.0.1")

    def test_success_envelope_is_hashed_and_key_never_enters_result(self):
        secret = "unit-test-secret"
        session = FakeSession([FakeResponse({
            "code": 0, "message": "ok", "request_id": "req-1",
            "data": {"timestamp": 123, "item": [{"name": "A"}]},
        })])
        response = HithinkFinanceBridge(api_key=secret, session=session).ths_index_list()
        self.assertEqual(response.request_id, "req-1")
        self.assertEqual(response.source_timestamp, 123)
        self.assertEqual(len(response.content_sha256), 64)
        self.assertEqual(session.calls[0]["headers"]["X-api-key"], secret)
        self.assertFalse(session.calls[0]["allow_redirects"])
        self.assertNotIn(secret, json.dumps(response.metadata()))
        self.assertNotIn(secret, json.dumps(response.data))

    def test_retryable_business_code_retries_without_exposing_message(self):
        session = FakeSession([
            FakeResponse({"code": 4001, "message": "secret body", "request_id": "r1", "data": None}),
            FakeResponse({"code": 0, "message": "ok", "request_id": "r2", "data": {"item": []}}),
        ])
        with patch.object(bridge_module.time, "sleep"):
            response = HithinkFinanceBridge(api_key="fake", session=session).limit_up_ladder()
        self.assertEqual(response.request_id, "r2")
        self.assertEqual(len(session.calls), 2)

    def test_business_error_preserves_code_and_request_id_but_not_body(self):
        session = FakeSession([FakeResponse({
            "code": 2002, "message": "leak-this-body", "request_id": "req-denied", "data": None,
        })])
        with self.assertRaises(HithinkBridgeError) as caught:
            HithinkFinanceBridge(api_key="fake", session=session).limit_up_ladder()
        self.assertEqual(caught.exception.code, 2002)
        self.assertEqual(caught.exception.request_id, "req-denied")
        self.assertNotIn("leak-this-body", str(caught.exception))
        self.assertNotIn("fake", str(caught.exception))

    def test_redirect_and_malformed_envelopes_fail_closed(self):
        redirect = HithinkFinanceBridge(
            api_key="fake", session=FakeSession([FakeResponse({}, status_code=302)])
        )
        with self.assertRaisesRegex(HithinkBridgeError, "redirect response rejected"):
            redirect.limit_up_ladder()

        malformed = HithinkFinanceBridge(
            api_key="fake",
            session=FakeSession([FakeResponse({"code": None, "data": []})]),
        )
        with self.assertRaisesRegex(HithinkBridgeError, "response validation failed"):
            malformed.limit_up_ladder()

    def test_adapter_bounds_apply_without_cli(self):
        client = HithinkFinanceBridge(api_key="fake", session=FakeSession([]))
        with self.assertRaises(ValueError):
            client.auction_snapshot([f"00000{i}.SZ" for i in range(6)])
        with self.assertRaises(ValueError):
            client.limit_pool("2026-08-21", pool="limit_up", size=21)
        with self.assertRaises(ValueError):
            client.ths_constituents("not-an-index")

    def test_trade_date_milliseconds_use_asia_shanghai_midnight(self):
        self.assertEqual(trade_date_ms("1970-01-01"), -28_800_000)

    def test_offline_snapshot_plans_calls_without_fetching(self):
        snapshot = load_snapshot_module()
        parser = snapshot.build_parser()
        args = parser.parse_args(["--batch", "b", "--trade-date", "2026-08-21"])
        snapshot.validate_args(args, parser)
        output = snapshot.execute(args, client=HithinkFinanceBridge(api_key=""))
        self.assertEqual(output["evidence_status"], "NOT_FETCHED")
        self.assertTrue(all(job["status"] == "NETWORK_DISABLED" for job in output["jobs"]))
        self.assertTrue(output["manual_review_required"])
        self.assertFalse(output["decision_authority"])
        self.assertFalse(output["prompt_use_allowed"])

    def test_private_env_reader_does_not_execute_shell_syntax(self):
        snapshot = load_snapshot_module()
        with tempfile.TemporaryDirectory() as tmp:
            env_file = Path(tmp) / ".env"
            marker = Path(tmp) / "must-not-exist"
            env_file.write_text(
                f"OTHER=$(touch {marker})\n"
                "export HITHINK_FINANCE_API_KEY='private-test-value'\n",
                encoding="utf-8",
            )
            with patch.object(snapshot, "PRIVATE_ENV_FILE", env_file), patch.dict(
                os.environ, {"HITHINK_FINANCE_API_KEY": ""}, clear=False
            ):
                self.assertEqual(snapshot.private_api_key(), "private-test-value")
            self.assertFalse(marker.exists())

    def test_network_snapshot_filters_lhb_and_keeps_external_evidence_unverified(self):
        snapshot = load_snapshot_module()
        parser = snapshot.build_parser()
        args = parser.parse_args([
            "--batch", "2026W34", "--trade-date", "2026-08-21", "--network",
            "--symbol", "300016.SZ", "--board-code", "885338.TI",
            "--query", "北陆药业", "--board-tag", "cn_concept",
            "--board-keyword", "特高压",
        ])
        snapshot.validate_args(args, parser)
        output = snapshot.execute(args, client=FakeSnapshotClient())
        self.assertEqual(output["evidence_status"], "OBSERVED_UNVERIFIED")
        self.assertEqual(output["summary"]["failed_jobs"], 0)
        lhb = output["data"]["dragon_tiger"]
        self.assertEqual([row["thscode"] for row in lhb["stock_items"]], ["300016.SZ"])
        self.assertEqual(output["data"]["board_catalog_matches"][0]["matched_count"], 1)
        scopes = {job["job_name"]: job["time_scope"] for job in output["jobs"]}
        self.assertEqual(scopes["board_constituents"], "CURRENT_MEMBERSHIP_ONLY")
        self.assertEqual(scopes["limit_up_ladder"], "LATEST_30_TRADE_DAYS")
        self.assertIn("trade", output["blocked_actions"])

    def test_broad_board_is_flagged_and_bounded(self):
        snapshot = load_snapshot_module()
        payload = {"timestamp": 1, "item": [{"thscode": f"{i:06d}.SZ"} for i in range(301)]}
        output = snapshot._bounded_constituents(payload)
        self.assertTrue(output["broad_board"])
        self.assertTrue(output["truncated"])
        self.assertEqual(len(output["item"]), 300)
        self.assertFalse(output["historical_membership_supported"])

    def test_tool_has_no_core_runtime_imports(self):
        source = (TOOLS / "hithink_finance_source_snapshot.py").read_text(encoding="utf-8")
        for forbidden in (
            "import duckdb", "from duckdb", "import decision_engine",
            "from decision_engine", "import zhulong_daemon", "from nexus",
        ):
            self.assertNotIn(forbidden, source)


if __name__ == "__main__":
    unittest.main()
