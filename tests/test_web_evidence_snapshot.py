#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import hashlib
import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LIB = ROOT / "02_brain" / "lib"
TOOLS = ROOT / "tools"
for path in (str(LIB), str(TOOLS)):
    if path not in sys.path:
        sys.path.insert(0, path)

from web_evidence_snapshot import (  # noqa: E402
    ProviderConfig,
    ProviderError,
    normalize_public_url,
    scrape_firecrawl,
    search_anysearch,
    search_firecrawl,
)


def load_probe_module():
    path = TOOLS / "web_evidence_capability_probe.py"
    spec = importlib.util.spec_from_file_location("web_evidence_capability_probe", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


class WebEvidenceSnapshotTest(unittest.TestCase):
    def test_url_guard_rejects_private_credentials_and_fragments_public_url(self):
        for value in ("http://127.0.0.1/a", "http://10.1.2.3/a", "https://u:p@example.com/a"):
            with self.assertRaises(ValueError):
                normalize_public_url(value)
        self.assertEqual(normalize_public_url("HTTPS://Example.COM/a?q=1#frag"), "https://example.com/a?q=1")

    def test_anysearch_requires_explicit_key_or_anonymous_opt_in(self):
        with self.assertRaisesRegex(ProviderError, "KEY_MISSING"):
            search_anysearch(
                "test", query_id="Q1", as_of=date(2026, 8, 9), limit=2,
                config=ProviderConfig("anysearch"), timeout=2,
            )

    def test_anysearch_normalizes_results_without_promoting_evidence(self):
        def fake_transport(url, payload, headers, timeout):
            self.assertEqual(payload["max_results"], 2)
            return {"id": "req-a", "results": [
                {"title": "A", "url": "https://example.com/a", "snippet": "one", "date": "2026-08-08"},
                {"title": "future", "url": "https://example.com/f", "date": "2026-08-10"},
            ]}

        rows, meta = search_anysearch(
            "test", query_id="Q1", as_of=date(2026, 8, 9), limit=2,
            config=ProviderConfig("anysearch", allow_anonymous=True), timeout=2,
            transport=fake_transport,
        )
        self.assertEqual(rows[0]["evidence_status"], "DISCOVERED_UNVERIFIED")
        self.assertFalse(rows[0]["published_at_verified"])
        self.assertEqual(rows[0]["entity_binding_status"], "UNVERIFIED")
        self.assertFalse(rows[0]["generate_task"])
        self.assertEqual(rows[1]["evidence_status"], "REJECTED_AFTER_AS_OF")
        self.assertEqual(meta["provider_request_id"], "req-a")

    def test_firecrawl_search_and_scrape_keep_unverified_boundary(self):
        def fake_search(url, payload, headers, timeout):
            self.assertIn("Bearer secret", headers["Authorization"])
            return {"success": True, "id": "req-f", "data": {"web": [{
                "title": "Official notice",
                "url": "https://www.gov.cn/notice",
                "description": "notice",
                "metadata": {"date": "2026-08-01"},
            }]}}

        rows, _ = search_firecrawl(
            "policy", query_id="F1", as_of=date(2026, 8, 9), limit=1,
            config=ProviderConfig("firecrawl", api_key="secret"), timeout=2,
            transport=fake_search,
        )
        self.assertEqual(rows[0]["evidence_status"], "DISCOVERED_UNVERIFIED")

        full_content = "A" * 4_000 + "TAIL"

        def fake_scrape(url, payload, headers, timeout):
            return {"success": True, "data": {
                "markdown": full_content,
                "metadata": {"title": "Page", "sourceURL": payload["url"]},
            }}

        rows, _ = scrape_firecrawl(
            "https://example.com/page", query_id="F2", as_of=date(2026, 8, 9),
            config=ProviderConfig("firecrawl", api_key="secret"), timeout=2,
            transport=fake_scrape,
        )
        self.assertEqual(rows[0]["evidence_status"], "FETCHED_UNVERIFIED")
        self.assertEqual(rows[0]["content_sha256"], hashlib.sha256(full_content.encode()).hexdigest())
        self.assertEqual(len(rows[0]["content_excerpt"]), 4_000)
        self.assertEqual(rows[0]["content_length_chars"], len(full_content))
        self.assertEqual(rows[0]["content_trust"], "TAINTED_EXTERNAL_UNVERIFIED")
        self.assertFalse(rows[0]["prompt_use_allowed"])

    def test_provider_error_never_records_secret_or_body(self):
        def failing_transport(url, payload, headers, timeout):
            raise RuntimeError(f"leak secret-token {headers}")

        with self.assertRaises(ProviderError) as caught:
            search_firecrawl(
                "policy", query_id="F1", as_of=date(2026, 8, 9), limit=1,
                config=ProviderConfig("firecrawl", api_key="secret-token"), timeout=2,
                transport=failing_transport,
            )
        self.assertNotIn("secret-token", str(caught.exception))

    def test_offline_probe_writes_no_verified_evidence(self):
        probe = load_probe_module()
        parser = probe.build_parser()
        with tempfile.TemporaryDirectory() as tmp:
            args = parser.parse_args([
                "--batch", "2026W32", "--as-of", "2026-08-09",
                "--query", "特高压 政策", "--output-dir", tmp,
            ])
            probe.validate_args(args, parser)
            snapshot = probe.execute(args)
            self.assertEqual(snapshot["execution_mode"], "OFFLINE_PLAN")
            self.assertTrue(all(job["status"] == "NETWORK_DISABLED" for job in snapshot["jobs"]))
            self.assertEqual(snapshot["summary"]["verified_evidence_count"], 0)
            self.assertEqual(len(snapshot["implementation_sha256"]), 64)
            rendered = probe.render_preview(snapshot)
            self.assertIn("不写 DuckDB", rendered)
            self.assertNotIn("secret-token", json.dumps(snapshot))

    def test_cli_rejects_expanded_job_overflow_and_unsafe_fetch(self):
        probe = load_probe_module()
        parser = probe.build_parser()
        too_many = parser.parse_args([
            "--batch", "b", "--as-of", "2026-08-09",
            "--query", "q1", "--query", "q2", "--query", "q3",
        ])
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            probe.validate_args(too_many, parser)

        unsafe = parser.parse_args([
            "--batch", "b", "--as-of", "2026-08-09",
            "--provider", "firecrawl", "--fetch-url", "http://127.0.0.1/admin",
        ])
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            probe.validate_args(unsafe, parser)

    def test_adapter_enforces_result_and_timeout_bounds_without_cli(self):
        with self.assertRaisesRegex(ProviderError, "RESULT_LIMIT_OUT_OF_BOUNDS"):
            search_anysearch(
                "test", query_id="Q1", as_of=date(2026, 8, 9), limit=6,
                config=ProviderConfig("anysearch", allow_anonymous=True), timeout=2,
            )
        with self.assertRaisesRegex(ProviderError, "TIMEOUT_OUT_OF_BOUNDS"):
            search_firecrawl(
                "test", query_id="Q2", as_of=date(2026, 8, 9), limit=1,
                config=ProviderConfig("firecrawl", api_key="fake"), timeout=13,
            )


if __name__ == "__main__":
    unittest.main()
