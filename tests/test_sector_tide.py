#!/usr/bin/env python3

import importlib.util
import sys
import unittest
from pathlib import Path

from tests import _test_log_isolation  # noqa: F401

ROOT = Path(__file__).resolve().parents[1]
LIB = ROOT / "05_shadow" / "lib"
sys.path.insert(0, str(LIB))
spec = importlib.util.spec_from_file_location("zhulong_test_sector_tide", LIB / "sector_tide.py")
MOD = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = MOD
spec.loader.exec_module(MOD)


class SectorTideTest(unittest.TestCase):
    def test_supportive(self):
        self.assertEqual(MOD.classify_sector_tide(stock_count=20, advancer_ratio=0.7, above_ma20_ratio=0.6, avg_pct_chg=1.2), "SECTOR_SUPPORTIVE")

    def test_adverse(self):
        self.assertEqual(MOD.classify_sector_tide(stock_count=20, advancer_ratio=0.2, above_ma20_ratio=0.3, avg_pct_chg=-1.2), "SECTOR_ADVERSE")

    def test_missing_ma20_is_not_forced_adverse(self):
        self.assertEqual(MOD.classify_sector_tide(stock_count=20, advancer_ratio=0.2, above_ma20_ratio=None, avg_pct_chg=-1.2), "SECTOR_MIXED")


if __name__ == "__main__":
    unittest.main()
