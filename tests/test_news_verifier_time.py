import importlib.util
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "02_brain" / "lib" / "news_verifier.py"
SPEC = importlib.util.spec_from_file_location("zhulong_test_news_verifier", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class NewsVerifierTimeTest(unittest.TestCase):
    def test_parse_datetime_normalizes_to_beijing_aware(self):
        parsed = MODULE._parse_datetime("2026-06-22 20:59:00")
        self.assertIsNotNone(parsed)
        self.assertIsNotNone(parsed.tzinfo)
        self.assertEqual(parsed.utcoffset(), timedelta(hours=8))

        utc_parsed = MODULE._parse_datetime("2026-06-22T12:59:00Z")
        self.assertEqual(utc_parsed.isoformat(), "2026-06-22T20:59:00+08:00")

    def test_evaluate_handles_aware_cutoff_and_naive_news_time(self):
        verifier = MODULE.NewsVerifier()
        cutoff = datetime(2026, 6, 22, 21, 0, tzinfo=timezone(timedelta(hours=8)))
        items = [
            MODULE.NewsItem(
                provider="TEST",
                source_grade="B",
                title="600519 neutral company update",
                content="sample",
                published_at="2026-06-22 20:59:00",
            ),
            MODULE.NewsItem(
                provider="TEST",
                source_grade="B",
                title="600519 future update",
                content="sample",
                published_at="2026-06-22 21:01:00",
            ),
        ]

        relevant = verifier._relevant_items("600519", "", cutoff, items)
        self.assertEqual(len(relevant), 1)
        self.assertEqual(relevant[0].published_at, "2026-06-22 20:59:00")

        result = verifier.evaluate("600519", "", cutoff, items, ["TEST"], {})
        self.assertEqual(result.status, "NEWS_CLEAR")


if __name__ == "__main__":
    unittest.main()
