#!/usr/bin/env python3

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

ROOT = Path(__file__).resolve().parents[1]


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


ZETA = load_module(
    "zhulong_test_zeta_persist", ROOT / "scripts" / "run_zeta_collect.py"
)


class ZetaPersistTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tempdir.name) / "zeta.duckdb")
        self.db_patch = patch.object(ZETA.Config, "DB_PATH", self.db_path)
        self.db_patch.start()
        ZETA._ensure_fact_zeta_signals_table()

    def tearDown(self):
        self.db_patch.stop()
        self.tempdir.cleanup()

    @staticmethod
    def row(symbol, trade_date, lhb_net):
        return (
            symbol, trade_date, lhb_net, 2.0, 1.0, 1, 1, 0,
            100.0, 10.0, 5.0, 0.0, 0.0, "test", f"{trade_date} 20:30:00",
        )

    def test_batch_insert_replaces_same_trade_date_atomically(self):
        first = [
            self.row("000001.SZ", "2026-08-18", 1.0),
            self.row("600000.SH", "2026-08-18", 2.0),
        ]
        self.assertEqual(ZETA._persist_rows("2026-08-18", first), 2)
        replacement = [self.row("000001.SZ", "2026-08-18", 9.0)]
        self.assertEqual(ZETA._persist_rows("2026-08-18", replacement), 1)

        con = duckdb.connect(self.db_path, read_only=True)
        try:
            rows = con.execute(
                "SELECT ts_code, lhb_net FROM fact_zeta_signals ORDER BY ts_code"
            ).fetchall()
        finally:
            con.close()
        self.assertEqual(rows, [("000001.SZ", 9.0)])

    def test_duplicate_symbol_is_rejected_before_write(self):
        rows = [
            self.row("000001.SZ", "2026-08-18", 1.0),
            self.row("000001.SZ", "2026-08-18", 2.0),
        ]
        with self.assertRaisesRegex(ValueError, "duplicate Zeta symbols"):
            ZETA._persist_rows("2026-08-18", rows)

        con = duckdb.connect(self.db_path, read_only=True)
        try:
            count = con.execute("SELECT COUNT(*) FROM fact_zeta_signals").fetchone()[0]
        finally:
            con.close()
        self.assertEqual(count, 0)

    def test_failed_replacement_rolls_back_previous_batch(self):
        original = [self.row("000001.SZ", "2026-08-18", 3.0)]
        ZETA._persist_rows("2026-08-18", original)
        invalid = list(self.row("600000.SH", "2026-08-18", 4.0))
        invalid[-1] = "not-a-timestamp"
        with self.assertRaises(Exception):
            ZETA._persist_rows("2026-08-18", [tuple(invalid)])

        con = duckdb.connect(self.db_path, read_only=True)
        try:
            rows = con.execute(
                "SELECT ts_code, lhb_net FROM fact_zeta_signals ORDER BY ts_code"
            ).fetchall()
        finally:
            con.close()
        self.assertEqual(rows, [("000001.SZ", 3.0)])


if __name__ == "__main__":
    unittest.main()
