#!/usr/bin/env python3

import sys
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

import duckdb

from tests import _test_log_isolation  # noqa: F401


ROOT = Path(__file__).resolve().parents[1]
SHADOW_LIB = ROOT / "05_shadow" / "lib"
for path in (ROOT, SHADOW_LIB):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import t1_fill_engine as T1  # noqa: E402


class T1TradeCalendarAgeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "calendar.duckdb"
        with duckdb.connect(str(self.db_path)) as conn:
            conn.execute(
                "CREATE TABLE fact_trade_calendar "
                "(exchange VARCHAR, cal_date DATE, is_open BOOLEAN)"
            )
            conn.execute(
                "CREATE TABLE ops_trade_calendar_override "
                "(override_id VARCHAR, cal_date DATE, scope VARCHAR, is_open BOOLEAN, created_at TIMESTAMP)"
            )

    def tearDown(self):
        self.tmp.cleanup()

    def insert_range(self, start, end, open_dates):
        rows = []
        current = start
        while current <= end:
            rows.append(("SSE", current, current in open_dates))
            current += timedelta(days=1)
        with duckdb.connect(str(self.db_path)) as conn:
            conn.executemany("INSERT INTO fact_trade_calendar VALUES (?, ?, ?)", rows)

    def test_holiday_span_counts_only_authoritative_open_sessions(self):
        start = date(2026, 9, 30)
        end = date(2026, 10, 9)
        self.insert_range(start, end, {start, end})
        with patch.object(T1, "DB_PATH", str(self.db_path)):
            self.assertEqual(T1._trading_day_age(start, end), 1)

    def test_calendar_coverage_gap_holds_pending_age(self):
        with duckdb.connect(str(self.db_path)) as conn:
            conn.execute("INSERT INTO fact_trade_calendar VALUES ('SSE', '2026-10-08', FALSE)")
            conn.execute("INSERT INTO fact_trade_calendar VALUES ('SSE', '2026-10-09', TRUE)")
        with patch.object(T1, "DB_PATH", str(self.db_path)):
            self.assertEqual(T1._trading_day_age("2026-10-01", "2026-10-09"), 0)


if __name__ == "__main__":
    unittest.main()
