#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "01_engine" / "lib" / "trade_calendar.py"
SPEC = importlib.util.spec_from_file_location("trade_calendar_under_test", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class FakeAPI:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def trade_cal(self, **kwargs):
        self.calls.append(kwargs)
        return self.rows


class TradeCalendarTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "calendar.duckdb"
        self.now = datetime(2026, 7, 28, 18, 30)

    def tearDown(self):
        self.tmp.cleanup()

    @staticmethod
    def rows(start_day=27, end_day=31, closed_days=()):
        rows = []
        for day in range(start_day, end_day + 1):
            rows.append(
                {
                    "exchange": "SSE",
                    "cal_date": f"202607{day:02d}",
                    "is_open": 0 if day in closed_days else 1,
                    "pretrade_date": "20260724" if day == 27 else f"202607{day - 1:02d}",
                }
            )
        return rows

    def seed(self, rows=None, fetched_at=None):
        return MODULE.replace_calendar_rows(
            self.db_path,
            rows or self.rows(),
            fetched_at=fetched_at or self.now,
            requested_start="2026-07-27",
        )

    def test_cached_open_date_allows_audit_and_entry(self):
        self.seed()
        for scope in (MODULE.SCOPE_AUDIT, MODULE.SCOPE_ENTRY):
            decision = MODULE.decide_trade_day(
                self.db_path,
                "2026-07-28",
                scope=scope,
                now=self.now,
                public_holiday_checker=lambda _: False,
            )
            self.assertTrue(decision.is_open)
            self.assertEqual("OPEN", decision.status)

    def test_cached_closed_date_stays_closed(self):
        self.seed(self.rows(closed_days=(29,)))
        decision = MODULE.decide_trade_day(
            self.db_path,
            "2026-07-29",
            now=self.now,
            public_holiday_checker=lambda _: True,
        )
        self.assertFalse(decision.is_open)
        self.assertEqual("CLOSED", decision.status)

    def test_unknown_date_fails_closed(self):
        self.seed()
        decision = MODULE.decide_trade_day(
            self.db_path,
            "2026-08-03",
            now=self.now,
            public_holiday_checker=lambda _: False,
        )
        self.assertFalse(decision.is_open)
        self.assertEqual("UNKNOWN_DATE", decision.status)
        self.assertTrue(decision.should_alert)

    def test_stale_cache_fails_closed(self):
        self.seed(fetched_at=datetime(2026, 7, 1, 12, 0))
        decision = MODULE.decide_trade_day(
            self.db_path,
            "2026-07-28",
            now=self.now,
            public_holiday_checker=lambda _: False,
        )
        self.assertEqual("STALE_CACHE", decision.status)
        self.assertFalse(decision.is_open)

    def test_source_disagreement_fails_closed(self):
        self.seed()
        decision = MODULE.decide_trade_day(
            self.db_path,
            "2026-07-28",
            now=self.now,
            public_holiday_checker=lambda _: True,
        )
        self.assertEqual("SOURCE_DISAGREEMENT", decision.status)
        self.assertFalse(decision.is_open)

    def test_unavailable_secondary_calendar_does_not_veto_fresh_tushare_row(self):
        self.seed()
        decision = MODULE.decide_trade_day(
            self.db_path,
            "2026-07-28",
            now=self.now,
            public_holiday_checker=lambda _: None,
        )
        self.assertEqual("OPEN", decision.status)
        self.assertTrue(decision.is_open)

    def test_audit_override_does_not_open_entry(self):
        self.seed(self.rows(closed_days=(28,)))
        override_id = MODULE.add_calendar_override(
            self.db_path,
            cal_date="2026-07-28",
            scope="AUDIT",
            is_open=True,
            reason="verified exchange notice",
            created_by="unit-test",
            created_at=self.now,
        )
        audit = MODULE.decide_trade_day(
            self.db_path,
            "2026-07-28",
            scope="AUDIT",
            now=self.now,
            public_holiday_checker=lambda _: True,
        )
        entry = MODULE.decide_trade_day(
            self.db_path,
            "2026-07-28",
            scope="ENTRY",
            now=self.now,
            public_holiday_checker=lambda _: True,
        )
        self.assertTrue(audit.is_open)
        self.assertEqual(override_id, audit.override_id)
        self.assertFalse(entry.is_open)
        self.assertEqual("CLOSED", entry.status)

    def test_entry_requires_its_own_override(self):
        self.seed(self.rows(closed_days=(28,)))
        MODULE.add_calendar_override(
            self.db_path,
            cal_date="2026-07-28",
            scope="ENTRY",
            is_open=True,
            reason="verified exchange notice and entry approval",
            created_by="unit-test",
            created_at=self.now,
        )
        entry = MODULE.decide_trade_day(
            self.db_path,
            "2026-07-28",
            scope="ENTRY",
            now=self.now,
            public_holiday_checker=lambda _: True,
        )
        audit = MODULE.decide_trade_day(
            self.db_path,
            "2026-07-28",
            scope="AUDIT",
            now=self.now,
            public_holiday_checker=lambda _: True,
        )
        self.assertTrue(entry.is_open)
        self.assertFalse(audit.is_open)

    def test_unknown_date_requires_scope_specific_manual_override(self):
        self.seed()
        MODULE.add_calendar_override(
            self.db_path,
            cal_date="2026-08-03",
            scope="AUDIT",
            is_open=True,
            reason="verified exchange notice before source refresh",
            created_by="unit-test",
            created_at=self.now,
        )
        audit = MODULE.decide_trade_day(
            self.db_path,
            "2026-08-03",
            scope="AUDIT",
            now=self.now,
            public_holiday_checker=lambda _: False,
        )
        entry = MODULE.decide_trade_day(
            self.db_path,
            "2026-08-03",
            scope="ENTRY",
            now=self.now,
            public_holiday_checker=lambda _: False,
        )
        self.assertEqual("MANUAL_OVERRIDE_OPEN", audit.status)
        self.assertTrue(audit.is_open)
        self.assertEqual("UNKNOWN_DATE", entry.status)
        self.assertFalse(entry.is_open)

    def test_weekend_cannot_be_opened_by_override(self):
        MODULE.ensure_calendar_schema(self.db_path)
        with self.assertRaisesRegex(ValueError, "weekend cannot be opened"):
            MODULE.add_calendar_override(
                self.db_path,
                cal_date="2026-08-01",
                scope="ENTRY",
                is_open=True,
                reason="bad weekend override",
                created_by="unit-test",
                created_at=self.now,
            )
        decision = MODULE.decide_trade_day(self.db_path, "2026-08-01", scope="ENTRY")
        self.assertFalse(decision.is_open)
        self.assertEqual("WEEKEND_CLOSED", decision.status)

    def test_refresh_is_atomic_and_records_source_horizon_warning(self):
        api = FakeAPI(self.rows())
        result = MODULE.refresh_trade_calendar(
            api,
            self.db_path,
            start_date="2026-07-27",
            end_date="2026-08-31",
            fetched_at=self.now,
        )
        self.assertEqual(5, result["rows_written"])
        self.assertEqual(1, len(result["warnings"]))
        self.assertEqual("20260727", api.calls[0]["start_date"])
        with duckdb.connect(str(self.db_path), read_only=True) as conn:
            self.assertEqual(5, conn.execute("SELECT COUNT(*) FROM fact_trade_calendar").fetchone()[0])

    def test_gap_payload_does_not_replace_existing_rows(self):
        self.seed()
        bad_rows = [self.rows()[0], self.rows()[2]]
        with self.assertRaisesRegex(ValueError, "coverage gap"):
            MODULE.replace_calendar_rows(
                self.db_path,
                bad_rows,
                fetched_at=self.now,
                requested_start="2026-07-27",
            )
        with duckdb.connect(str(self.db_path), read_only=True) as conn:
            self.assertEqual(5, conn.execute("SELECT COUNT(*) FROM fact_trade_calendar").fetchone()[0])

    def test_coverage_reports_horizon_short(self):
        self.seed()
        coverage = MODULE.inspect_calendar_coverage(
            self.db_path,
            as_of="2026-07-28",
            minimum_horizon_days=90,
        )
        self.assertEqual("HORIZON_SHORT", coverage["status"])
        self.assertEqual(3, coverage["horizon_days"])

    def test_override_rows_are_append_only(self):
        self.seed(self.rows(closed_days=(28,)))
        for hour, state in ((9, True), (10, False)):
            MODULE.add_calendar_override(
                self.db_path,
                cal_date="2026-07-28",
                scope="AUDIT",
                is_open=state,
                reason=f"review at {hour}",
                created_by="unit-test",
                created_at=datetime(2026, 7, 28, hour, 0),
            )
        with duckdb.connect(str(self.db_path), read_only=True) as conn:
            self.assertEqual(
                2,
                conn.execute("SELECT COUNT(*) FROM ops_trade_calendar_override").fetchone()[0],
            )
        decision = MODULE.decide_trade_day(
            self.db_path,
            "2026-07-28",
            scope="AUDIT",
            now=self.now,
            public_holiday_checker=lambda _: True,
        )
        self.assertFalse(decision.is_open)
        self.assertEqual("MANUAL_OVERRIDE_CLOSED", decision.status)


if __name__ == "__main__":
    unittest.main()
