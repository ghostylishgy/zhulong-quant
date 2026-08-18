#!/usr/bin/env python3

import unittest
from datetime import datetime
from pathlib import Path
import sys

from tests import _test_log_isolation  # noqa: F401

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.market_session import is_intraday_scan_time, validate_realtime_quote_time


class MarketSessionTest(unittest.TestCase):
    def test_intraday_scan_excludes_lunch(self):
        self.assertTrue(is_intraday_scan_time(datetime(2026, 6, 18, 11, 30)))
        self.assertFalse(is_intraday_scan_time(datetime(2026, 6, 18, 11, 31)))
        self.assertFalse(is_intraday_scan_time(datetime(2026, 6, 18, 12, 50)))
        self.assertTrue(is_intraday_scan_time(datetime(2026, 6, 18, 13, 0)))

    def test_fresh_quote_passes(self):
        result = validate_realtime_quote_time(
            '20260618', '09:29:57', now=datetime(2026, 6, 18, 9, 30)
        )
        self.assertEqual((result[0], result[1]), (True, 'QUOTE_FRESH'))

    def test_lunch_snapshot_fails_closed(self):
        result = validate_realtime_quote_time(
            '20260618', '11:30:00', now=datetime(2026, 6, 18, 11, 40)
        )
        self.assertEqual((result[0], result[1]), (False, 'QUOTE_STALE_TIME'))

    def test_wrong_date_and_missing_time_fail_closed(self):
        stale = validate_realtime_quote_time(
            '20260617', '14:49:57', now=datetime(2026, 6, 18, 14, 50)
        )
        missing = validate_realtime_quote_time(
            '20260618', '', now=datetime(2026, 6, 18, 14, 50)
        )
        self.assertEqual(stale[1], 'QUOTE_STALE_DATE')
        self.assertEqual(missing[1], 'QUOTE_MISSING_TIMESTAMP')


if __name__ == '__main__':
    unittest.main()
