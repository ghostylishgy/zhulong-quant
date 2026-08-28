import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
PATH = ROOT / "tools/review_shadow_blocked_counterfactuals.py"
SPEC = importlib.util.spec_from_file_location("zhulong_test_blocked_cf", PATH)
MOD = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MOD
SPEC.loader.exec_module(MOD)


class ShadowBlockedCounterfactualTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "blocked.duckdb"
        conn = duckdb.connect(str(self.db_path))
        conn.execute(
            "CREATE TABLE fact_trade_calendar(exchange VARCHAR,cal_date DATE,is_open BOOLEAN)"
        )
        conn.executemany(
            "INSERT INTO fact_trade_calendar VALUES ('SSE',?,TRUE)",
            [
                ("2026-07-02",), ("2026-07-03",), ("2026-07-06",),
                ("2026-07-07",), ("2026-07-08",), ("2026-07-09",),
            ],
        )
        conn.execute(
            """
            CREATE TABLE fact_daily(
                symbol VARCHAR,trade_date DATE,open DOUBLE,high DOUBLE,low DOUBLE,
                close DOUBLE,vol DOUBLE,amount DOUBLE
            )
            """
        )
        for symbol in ("000001.SZ", "000002.SZ", "000003.SZ"):
            conn.executemany(
                "INSERT INTO fact_daily VALUES (?,?,?,?,?,?,?,?)",
                [
                    (symbol, "2026-07-02", 10.0, 10.5, 9.8, 10.2, 1000, 10000),
                    (symbol, "2026-07-03", 10.2, 10.8, 10.0, 10.5, 1000, 10000),
                    (symbol, "2026-07-06", 10.5, 11.2, 10.4, 11.0, 1000, 10000),
                    (symbol, "2026-07-07", 11.0, 11.4, 10.8, 11.2, 1000, 10000),
                    (symbol, "2026-07-08", 11.2, 11.7, 11.0, 11.5, 1000, 10000),
                ],
            )
        conn.execute(
            """
            CREATE TABLE fact_shadow_skipped_signals(
                signal_id VARCHAR,task_id VARCHAR,run_id VARCHAR,trade_date DATE,
                symbol VARCHAR,name VARCHAR,reason VARCHAR,final_score DOUBLE,
                l1_close DOUBLE,entry_tide_gate VARCHAR,entry_tide_ratio DOUBLE,
                entry_policy VARCHAR,evidence_json VARCHAR
            )
            """
        )
        conn.executemany(
            "INSERT INTO fact_shadow_skipped_signals VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                ("task-account","task-account","run","2026-07-01","000001.SZ",
                 "A","ACCOUNT_INELIGIBLE_BJ",78,10,"AGGRESSIVE",0.8,
                 "STANDARD_ENTRY","{}"),
                ("task-archetype","task-archetype","run","2026-07-01","000002.SZ",
                 "B","TRADE_ARCHETYPE_UNCLASSIFIED",72,10,"AGGRESSIVE",0.8,
                 "STANDARD_ENTRY","{}"),
                ("task-operational","task-operational","run","2026-07-01","000003.SZ",
                 "C","LEDGER_DUPLICATE_OR_FAILED",72,10,"AGGRESSIVE",0.8,
                 "STANDARD_ENTRY","{}"),
            ],
        )
        conn.execute(
            """
            CREATE TABLE fact_shadow_pending_signals(
                signal_id VARCHAR,task_id VARCHAR,run_id VARCHAR,symbol VARCHAR,
                name VARCHAR,signal_trade_date DATE,final_score DOUBLE,l1_close DOUBLE,
                status VARCHAR,last_error VARCHAR,entry_tide_gate VARCHAR,
                entry_tide_ratio DOUBLE,entry_policy VARCHAR,trade_contract_json VARCHAR
            )
            """
        )
        conn.execute(
            """
            INSERT INTO fact_shadow_pending_signals VALUES
            ('task-news','task-news','run','000003.SZ','C','2026-07-01',
             74,10,'SKIPPED','SKIPPED_NEWS_CAUTION','CAUTION',0.5,
             'STANDARD_ENTRY','{}')
            """
        )
        conn.close()

    def tearDown(self):
        self.tempdir.cleanup()

    def test_classify_block_rejects_operational_failures(self):
        self.assertEqual(MOD.classify_block("SKIPPED_NEWS_CAUTION"), "NEWS")
        self.assertEqual(MOD.classify_block("TIDE_SUPPRESSED_NO_SHADOW_ENTRY"), "TIDE")
        self.assertEqual(MOD.classify_block("ACCOUNT_INELIGIBLE_ST"), "ACCOUNT")
        self.assertEqual(MOD.classify_block("TRADE_ARCHETYPE_UNCLASSIFIED"), "ARCHETYPE")
        self.assertIsNone(MOD.classify_block("LEDGER_DUPLICATE_OR_FAILED"))

    def test_loads_both_skip_sources_without_operational_rows(self):
        with duckdb.connect(str(self.db_path), read_only=True) as conn:
            rows, warnings = MOD._load_blocked_signals(
                conn, "2026-07-01", "2026-07-01"
            )
        self.assertEqual(len(rows), 3)
        self.assertEqual(
            {row["block_category"] for row in rows},
            {"NEWS", "ACCOUNT", "ARCHETYPE"},
        )
        self.assertEqual(warnings, [])

    def test_fixed_t1_open_to_t1_t3_t5_returns(self):
        with duckdb.connect(str(self.db_path), read_only=True) as conn:
            result = MOD.evaluate_signal(
                conn, {"task_id": "x", "symbol": "000001.SZ",
                       "signal_trade_date": "2026-07-01"}, "2026-07-08"
            )
        self.assertEqual(result["entry_price"], 10.0)
        self.assertEqual(result["horizons"]["T1"]["gross_return_pct"], 2.0)
        self.assertEqual(result["horizons"]["T3"]["gross_return_pct"], 10.0)
        self.assertEqual(result["horizons"]["T5"]["gross_return_pct"], 15.0)

    def test_one_price_entry_is_never_shifted(self):
        conn = duckdb.connect(str(self.db_path))
        conn.execute(
            "UPDATE fact_daily SET high=10,low=10 "
            "WHERE symbol='000002.SZ' AND trade_date='2026-07-02'"
        )
        conn.close()
        with duckdb.connect(str(self.db_path), read_only=True) as conn:
            result = MOD.evaluate_signal(
                conn, {"task_id": "x", "symbol": "000002.SZ",
                       "signal_trade_date": "2026-07-01"}, "2026-07-08"
            )
        self.assertFalse(result["entry_feasible"])
        self.assertEqual(result["entry_date"], "2026-07-02")
        self.assertEqual(result["horizons"]["T5"]["status"], "ENTRY_INFEASIBLE")

    def test_build_report_separates_gate_categories(self):
        payload = MOD.build_report(self.db_path, "2026-07-01", "2026-07-01")
        summary = payload["summary"]
        self.assertEqual(summary["blocked_signals"], 3)
        self.assertEqual(summary["category_counts"]["NEWS"], 1)
        self.assertEqual(summary["category_counts"]["ACCOUNT"], 1)
        self.assertEqual(summary["category_counts"]["ARCHETYPE"], 1)
        self.assertEqual(
            summary["category_stats"]["NEWS"]["horizons"]["T5"]["samples"], 1
        )
        self.assertTrue(payload["observer_only"])
        self.assertTrue(payload["no_trade_signal"])


if __name__ == "__main__":
    unittest.main()
