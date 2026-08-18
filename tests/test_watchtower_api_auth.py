import importlib
import os
from pathlib import Path
import sys
import unittest
from unittest import mock

from fastapi import HTTPException


ROOT = Path(__file__).resolve().parents[1]
WATCHTOWER_DIR = ROOT / "06_watchtower"
if str(WATCHTOWER_DIR) not in sys.path:
    sys.path.insert(0, str(WATCHTOWER_DIR))


class WatchtowerApiAuthTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.api = importlib.import_module("06_watchtower.api_server")

    def test_key_comparison_is_fail_closed(self):
        with mock.patch.dict(os.environ, {"WATCHTOWER_TOUCHSTONE_KEY": "expected-key"}):
            self.assertFalse(self.api._watchtower_key_ok(""))
            self.assertFalse(self.api._watchtower_key_ok("wrong-key"))
            self.assertTrue(self.api._watchtower_key_ok("expected-key"))

    def test_cors_origins_are_explicit(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(
                self.api._watchtower_allowed_origins(),
                ["http://127.0.0.1:5173", "http://localhost:5173"],
            )
        with mock.patch.dict(
            os.environ,
            {"WATCHTOWER_ALLOWED_ORIGINS": "https://example.com, https://www.example.com"},
        ):
            origins = self.api._watchtower_allowed_origins()
            self.assertEqual(origins, ["https://example.com", "https://www.example.com"])
            self.assertNotIn("*", origins)

    def test_sensitive_routes_reject_before_data_access(self):
        calls = [
            lambda: self.api.get_latest_audits(x_watchtower_key="wrong"),
            lambda: self.api.get_audit_disagreements(x_watchtower_key="wrong"),
            lambda: self.api.daily_alpha(x_watchtower_key="wrong"),
            lambda: self.api.shadow_portfolio(x_watchtower_key="wrong"),
            lambda: self.api.token_telemetry(x_watchtower_key="wrong"),
            lambda: self.api.system_status(x_watchtower_key="wrong"),
            lambda: self.api.system_liveness(x_watchtower_key="wrong"),
            lambda: self.api.get_funnel_stats(x_watchtower_key="wrong"),
            lambda: self.api.get_approved_results(x_watchtower_key="wrong"),
            lambda: self.api.get_ollama_node_116_status(x_watchtower_key="wrong"),
        ]
        with mock.patch.dict(os.environ, {"WATCHTOWER_TOUCHSTONE_KEY": "expected-key"}):
            for call in calls:
                with self.subTest(call=call):
                    with self.assertRaises(HTTPException) as raised:
                        call()
                    self.assertEqual(raised.exception.status_code, 401)


if __name__ == "__main__":
    unittest.main()
