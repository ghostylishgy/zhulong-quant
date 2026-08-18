#!/usr/bin/env python3

import importlib.util
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from tests import _test_log_isolation  # noqa: F401

ROOT = Path(__file__).resolve().parents[1]
LIB = ROOT / "05_shadow" / "lib"
sys.path.insert(0, str(LIB))
spec = importlib.util.spec_from_file_location("zhulong_test_t1_fill_push", LIB / "t1_fill_engine.py")
MOD = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = MOD
spec.loader.exec_module(MOD)


class Response:
    def __init__(self, status, code, text=""):
        self.status_code = status
        self._code = code
        self.text = text

    def json(self):
        return {"code": self._code}


def fill():
    return SimpleNamespace(
        symbol="600001.SH", qty=100, price_logical=10.0, price_shadow=10.1,
        gross_amount=1010.0, slippage_cost=10.0, commission=5.0,
        stamp_tax=0.0, transfer_fee=0.1, tax_total=5.1,
        pricing_mode="T1_VWAP_15M",
    )


class T1FillPushTest(unittest.TestCase):
    def setUp(self):
        self.token = patch.object(MOD.Config, "PUSHPLUS_TOKEN", "test-token")
        self.token.start()
        self.addCleanup(self.token.stop)

    def test_retries_then_confirms_success(self):
        responses = [Response(503, 500, "busy"), Response(200, 200)]
        with patch.object(MOD.requests, "post", side_effect=responses) as post, patch.object(MOD.time, "sleep") as sleep:
            ok = MOD._push_shadow_buy(fill(), {"name": "sample", "signal_trade_date": "2026-07-13", "final_score": 78}, 1015.1, "2026-07-14", "VWAP_0931_0945")
        self.assertTrue(ok)
        self.assertEqual(post.call_count, 2)
        sleep.assert_called_once_with(1)

    def test_all_failures_are_observable_without_raising(self):
        with patch.object(MOD.requests, "post", side_effect=RuntimeError("network down")) as post, patch.object(MOD.time, "sleep"):
            ok = MOD._push_shadow_buy(fill(), {"name": "sample"}, 1015.1, "2026-07-14", "VWAP_0931_0945")
        self.assertFalse(ok)
        self.assertEqual(post.call_count, 3)

    def test_missing_token_skips_network(self):
        with patch.object(MOD.Config, "PUSHPLUS_TOKEN", ""), patch.object(MOD.requests, "post") as post:
            ok = MOD._push_shadow_buy(fill(), {}, 1015.1, "2026-07-14", "VWAP_0931_0945")
        self.assertFalse(ok)
        post.assert_not_called()


if __name__ == "__main__":
    unittest.main()
