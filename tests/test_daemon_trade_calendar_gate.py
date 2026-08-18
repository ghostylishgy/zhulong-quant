#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import os
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
os.environ["ZHULONG_SKIP_SINGLETON_LOCK"] = "1"
os.environ["ZHULONG_BASE_DIR"] = str(ROOT)
os.environ["ZHULONG_DAEMON_LOG_PATH"] = str(
    Path(tempfile.gettempdir()) / f"zhulong_calendar_daemon_test_{os.getpid()}.log"
)


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


DAEMON = load_module("zhulong_test_trade_calendar_daemon", ROOT / "zhulong_daemon.py")


class DaemonTradeCalendarGateTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "calendar.duckdb"
        rows = [
            {
                "exchange": "SSE",
                "cal_date": f"202607{day:02d}",
                "is_open": 0 if day == 29 else 1,
                "pretrade_date": "20260724" if day == 27 else f"202607{day - 1:02d}",
            }
            for day in range(27, 32)
        ]
        DAEMON.TRADE_CALENDAR.replace_calendar_rows(
            self.db_path,
            rows,
            fetched_at=datetime(2026, 7, 28, 18, 0),
            requested_start="2026-07-27",
        )
        self.db_patch = patch.object(DAEMON, "DB_PATH", self.db_path)
        self.db_patch.start()
        DAEMON._CALENDAR_PUSH_GUARD.clear()

    def tearDown(self):
        self.db_patch.stop()
        self.tmp.cleanup()

    def test_trade_date_environment_does_not_bypass_unknown_date(self):
        with patch.dict(os.environ, {"TRADE_DATE": "2027-01-04"}, clear=False):
            self.assertFalse(DAEMON._is_trading_day_for_current())

    def test_audit_override_cannot_open_shadow_entry_scope(self):
        DAEMON.TRADE_CALENDAR.add_calendar_override(
            self.db_path,
            cal_date="2026-07-29",
            scope="AUDIT",
            is_open=True,
            reason="verified audit-only exception",
            created_by="unit-test",
            created_at=datetime(2026, 7, 28, 19, 0),
        )
        self.assertTrue(
            DAEMON._calendar_allows(
                "2026-07-29",
                scope=DAEMON.CALENDAR_SCOPE_AUDIT,
            )
        )
        self.assertFalse(
            DAEMON._calendar_allows(
                "2026-07-29",
                scope=DAEMON.CALENDAR_SCOPE_ENTRY,
            )
        )

    def test_unknown_date_push_is_deduplicated_per_scope_and_status(self):
        with patch.object(DAEMON, "_push_wechat", return_value=True) as push_mock:
            for context in ("phase_harvest", "phase_audit"):
                self.assertFalse(
                    DAEMON._calendar_allows(
                        "2027-01-04",
                        scope=DAEMON.CALENDAR_SCOPE_AUDIT,
                        notify=True,
                        context=context,
                    )
                )
        self.assertEqual(1, push_mock.call_count)
        title, content = push_mock.call_args.args
        self.assertIn("交易日历安全关闭", title)
        self.assertIn("UNKNOWN_DATE", content)

    def test_news_observer_holds_heartbeat_writer_interlock(self):
        observed = {}

        def fake_safe_run(*args, **kwargs):
            observed["news_active"] = DAEMON._NEWS_OBSERVATION_ACTIVE.is_set()
            observed["lock_held"] = DAEMON._HEARTBEAT_PERSIST_LOCK.locked()
            return True

        with patch.object(DAEMON, "safe_run", side_effect=fake_safe_run):
            DAEMON.phase_news_observation()
        self.assertEqual({"news_active": True, "lock_held": True}, observed)
        self.assertFalse(DAEMON._NEWS_OBSERVATION_ACTIVE.is_set())

    def test_heartbeat_skips_database_write_while_news_is_active(self):
        DAEMON._NEWS_OBSERVATION_ACTIVE.set()
        try:
            with (
                patch.object(DAEMON, "_record_daemon_heartbeat") as persist_mock,
                patch.object(DAEMON, "is_trading_day", return_value=True),
            ):
                DAEMON.heartbeat()
        finally:
            DAEMON._NEWS_OBSERVATION_ACTIVE.clear()
        persist_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
