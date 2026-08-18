#!/usr/bin/env python3

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

from tests import _test_log_isolation  # noqa: F401


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "03_tactics" / "tactics_bridge.py"
spec = importlib.util.spec_from_file_location(
    "zhulong_test_eagle_active_bridge", MODULE_PATH
)
MOD = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = MOD
spec.loader.exec_module(MOD)


class _FakeObserver:
    def __init__(self):
        self.calls = []

    def ingest_scan(self, watchlist, *, scan_time, quote_stats):
        self.calls.append((watchlist, scan_time, quote_stats))
        return {
            "status": "RECORDED",
            "recorded": True,
            "quote_count": len(watchlist),
            "scan_id": "test-scan-1",
        }


class _FinalizingObserver:
    def __init__(self, trade_date=None):
        self.trade_date = trade_date
        self.finalize_calls = 0

    def finalize(self):
        self.finalize_calls += 1
        return {
            "trade_date": self.trade_date or "2026-08-03",
            "scan_count": 2,
            "candidate_count": 0,
            "data_quality": "PARTIAL_INSUFFICIENT_SAMPLE",
        }


class EagleActiveBridgeTest(unittest.TestCase):
    def test_disabled_path_has_no_observer_side_effect(self):
        old_enabled = MOD.EAGLE_ACTIVE_PATH_ENABLED
        old_loader = MOD._get_eagle_active
        try:
            MOD.EAGLE_ACTIVE_PATH_ENABLED = False

            def should_not_load(_trade_date):
                raise AssertionError("observer must not load while disabled")

            MOD._get_eagle_active = should_not_load
            result = MOD._record_eagle_active_scan([], {}, "2026-08-03")
            self.assertIsNone(result)
        finally:
            MOD.EAGLE_ACTIVE_PATH_ENABLED = old_enabled
            MOD._get_eagle_active = old_loader

    def test_enabled_path_forwards_real_window_without_mutation(self):
        old_enabled = MOD.EAGLE_ACTIVE_PATH_ENABLED
        old_loader = MOD._get_eagle_active
        observer = _FakeObserver()
        watchlist = [
            {
                "symbol": "000001.SZ",
                "price": 10.2,
                "volume": 1200,
                "amount": 12000,
                "quote_source": "tushare_realtime",
                "quote_date": "2026-08-03",
                "quote_time": "10:20:00",
            }
        ]
        quote_stats = {
            "realtime_count": 1,
            "stale_quote_count": 0,
            "quote_source": "tushare_realtime",
        }
        try:
            MOD.EAGLE_ACTIVE_PATH_ENABLED = True
            MOD._get_eagle_active = lambda _trade_date: observer
            result = MOD._record_eagle_active_scan(
                watchlist, quote_stats, "2026-08-03"
            )
            self.assertEqual(result["status"], "RECORDED")
            self.assertEqual(len(observer.calls), 1)
            forwarded_watchlist, _scan_time, forwarded_stats = observer.calls[0]
            self.assertIs(forwarded_watchlist, watchlist)
            self.assertIs(forwarded_stats, quote_stats)
        finally:
            MOD.EAGLE_ACTIVE_PATH_ENABLED = old_enabled
            MOD._get_eagle_active = old_loader

    def test_disabled_path_does_not_change_existing_eagle_inputs(self):
        old_enabled = MOD.EAGLE_ACTIVE_PATH_ENABLED
        old_loader = MOD._get_eagle_active
        watchlist = [{"symbol": "600000.SH", "price": 8.5}]
        quote_stats = {"realtime_count": 1}
        try:
            MOD.EAGLE_ACTIVE_PATH_ENABLED = False
            MOD._get_eagle_active = lambda _trade_date: self.fail(
                "disabled path unexpectedly loaded"
            )
            self.assertIsNone(
                MOD._record_eagle_active_scan(
                    watchlist, quote_stats, "2026-08-03"
                )
            )
            self.assertEqual(watchlist, [{"symbol": "600000.SH", "price": 8.5}])
            self.assertEqual(quote_stats, {"realtime_count": 1})
        finally:
            MOD.EAGLE_ACTIVE_PATH_ENABLED = old_enabled
            MOD._get_eagle_active = old_loader

    def test_daily_flush_recovers_pending_window_file_after_restart(self):
        old_enabled = MOD.EAGLE_ACTIVE_PATH_ENABLED
        old_root = MOD._ROOT
        old_observer = MOD._eagle_active
        old_loader = MOD._load_internal_attr
        old_persist = MOD._persist_eagle_active_manifest
        with tempfile.TemporaryDirectory() as temp:
            try:
                MOD.EAGLE_ACTIVE_PATH_ENABLED = True
                MOD._ROOT = Path(temp)
                MOD._eagle_active = None
                window_path = (
                    MOD._ROOT
                    / "storage"
                    / "reports"
                    / "eagle_active_path"
                    / "windows"
                    / "eagle_windows_2026-08-03.jsonl"
                )
                window_path.parent.mkdir(parents=True)
                window_path.write_text("pending\n", encoding="utf-8")
                fake = _FinalizingObserver("2026-08-03")
                MOD._load_internal_attr = lambda *_args: lambda _date: fake
                persisted = []
                MOD._persist_eagle_active_manifest = lambda manifest: persisted.append(manifest)
                result = MOD._finalize_eagle_active_if_pending("2026-08-03")
                self.assertEqual(result["data_quality"], "PARTIAL_INSUFFICIENT_SAMPLE")
                self.assertEqual(fake.finalize_calls, 1)
                self.assertEqual(persisted, [result])
                self.assertIsNone(MOD._eagle_active)
            finally:
                MOD.EAGLE_ACTIVE_PATH_ENABLED = old_enabled
                MOD._ROOT = old_root
                MOD._eagle_active = old_observer
                MOD._load_internal_attr = old_loader
                MOD._persist_eagle_active_manifest = old_persist

    def test_trade_date_rollover_persists_previous_manifest(self):
        old_observer = MOD._eagle_active
        old_loader = MOD._load_internal_attr
        old_persist = MOD._persist_eagle_active_manifest
        previous = _FinalizingObserver("2026-08-03")
        persisted = []
        try:
            MOD._eagle_active = previous
            MOD._load_internal_attr = lambda *_args: _FinalizingObserver
            MOD._persist_eagle_active_manifest = lambda manifest: persisted.append(manifest)

            current = MOD._get_eagle_active("2026-08-04")

            self.assertEqual(previous.finalize_calls, 1)
            self.assertEqual(len(persisted), 1)
            self.assertEqual(persisted[0]["trade_date"], "2026-08-03")
            self.assertEqual(current.trade_date, "2026-08-04")
        finally:
            MOD._eagle_active = old_observer
            MOD._load_internal_attr = old_loader
            MOD._persist_eagle_active_manifest = old_persist


if __name__ == "__main__":
    unittest.main()
