import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb


ROOT = Path(__file__).resolve().parents[1]
PATH = ROOT / "tools/review_l4_counterfactual_scorecard.py"
SPEC = importlib.util.spec_from_file_location("zhulong_test_l4_scorecard", PATH)
MOD = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MOD
SPEC.loader.exec_module(MOD)


class L4CounterfactualScorecardTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "scorecard.duckdb"
        conn = duckdb.connect(str(self.db_path))
        conn.execute(
            "CREATE TABLE fact_trade_calendar(exchange VARCHAR,cal_date DATE,is_open BOOLEAN)"
        )
        conn.executemany(
            "INSERT INTO fact_trade_calendar VALUES ('SSE',?,TRUE)",
            [("2026-07-02",), ("2026-07-03",), ("2026-07-06",), ("2026-07-07",)],
        )
        conn.execute(
            """
            CREATE TABLE fact_daily(
                symbol VARCHAR,trade_date DATE,open DOUBLE,high DOUBLE,low DOUBLE,
                close DOUBLE,vol DOUBLE,amount DOUBLE
            )
            """
        )
        for symbol in ("000001.SZ", "000002.SZ", "000003.SZ", "920001.BJ"):
            conn.executemany(
                "INSERT INTO fact_daily VALUES (?,?,?,?,?,?,?,?)",
                [
                    (symbol, "2026-07-02", 10.0, 10.5, 9.8, 10.2, 1000, 10000),
                    (symbol, "2026-07-03", 10.2, 10.8, 10.0, 10.5, 1000, 10000),
                    (symbol, "2026-07-06", 10.5, 11.2, 10.4, 11.0, 1000, 10000),
                ],
            )
        conn.execute(
            "CREATE TABLE fact_stock_basic(symbol VARCHAR,name VARCHAR,market VARCHAR,is_st BOOLEAN)"
        )
        conn.executemany(
            "INSERT INTO fact_stock_basic VALUES (?,?,?,?)",
            [
                ("000001.SZ", "平安银行", "主板", False),
                ("000002.SZ", "ST测试", "主板", True),
                ("000003.SZ", "测试股份", "主板", False),
                ("920001.BJ", "北交测试", "北交所", False),
            ],
        )
        conn.execute(
            """
            CREATE TABLE nexus_audits(
                task_id VARCHAR,run_id VARCHAR,symbol VARCHAR,name VARCHAR,trade_date DATE,
                l4_final_verdict VARCHAR,l4_final_score DOUBLE,final_score DOUBLE,
                l4_news_status VARCHAR,l4_news_gate VARCHAR,l4_news_as_of VARCHAR,status VARCHAR
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE fact_paper_positions(
                signal_task_id VARCHAR,status VARCHAR,pnl_ratio_gross DOUBLE
            )
            """
        )
        conn.close()
        self.binding = {
            ("2026-07-01", "a1b2c3d4"): {
                "contract_sha256": "a" * 64,
                "binding_sha256": "b" * 64,
                "evidence_as_of": "2026-07-01T21:00:00+08:00",
            }
        }

    def tearDown(self):
        self.tempdir.cleanup()

    def row(self, **overrides):
        value = {
            "task_id": "a1b2c3d4_000001.SZ",
            "run_id": "a1b2c3d4",
            "symbol": "000001.SZ",
            "name": "平安银行",
            "trade_date": "2026-07-01",
            "verdict": "PASS",
            "final_score": 80,
            "news_status": "NEWS_CLEAR",
            "news_gate": "NONE",
            "news_as_of": "2026-07-01T21:00:00+08:00",
        }
        value.update(overrides)
        return value

    def evaluate(self, row=None, bindings=None):
        conn = duckdb.connect(str(self.db_path), read_only=True)
        try:
            return MOD.evaluate_row(
                conn,
                row or self.row(),
                MOD._stock_basic(conn),
                self.binding if bindings is None else bindings,
                "2026-07-06",
            )
        finally:
            conn.close()

    def test_bound_matured_row_uses_fixed_t1_open_t3_close(self):
        result = self.evaluate()
        self.assertEqual(result["evaluation_status"], "ELIGIBLE_BOUND_MATURED")
        self.assertEqual(result["t1_date"], "2026-07-02")
        self.assertEqual(result["t3_date"], "2026-07-06")
        self.assertEqual(result["gross_return_pct"], 10.0)
        self.assertEqual(result["mfe_pct"], 12.0)
        self.assertEqual(result["mae_pct"], -2.0)

    def test_unbound_row_is_diagnostic_only(self):
        result = self.evaluate(bindings={})
        self.assertEqual(result["evaluation_status"], "DIAGNOSTIC_UNBOUND_MATURED")
        self.assertEqual(result["provenance_status"], "UNBOUND_LEGACY_OR_DISABLED")

    def test_st_bj_and_news_risk_are_excluded(self):
        st = self.evaluate(self.row(symbol="000002.SZ", name="ST测试"))
        bj = self.evaluate(self.row(symbol="920001.BJ", name="北交测试"))
        news = self.evaluate(self.row(news_status="NEWS_CAUTION", news_gate="WOULD_CAP_HOLD"))
        self.assertIn("ST_INELIGIBLE", st["exclusion_reasons"])
        self.assertIn("ACCOUNT_BJ_INELIGIBLE", bj["exclusion_reasons"])
        self.assertIn("NEWS_RISK_BLOCK", news["exclusion_reasons"])
        self.assertEqual(news["evaluation_status"], "EXCLUDED_SAFETY_OR_EXECUTION")

    def test_news_cutoff_must_match_bound_run(self):
        result = self.evaluate(self.row(news_as_of="2026-07-01T20:59:00+08:00"))
        self.assertEqual(result["evaluation_status"], "EXCLUDED_SAFETY_OR_EXECUTION")
        self.assertIn("NEWS_AS_OF_BINDING_MISMATCH", result["exclusion_reasons"])

    def test_one_price_t1_is_not_shifted_to_later_session(self):
        conn = duckdb.connect(str(self.db_path))
        conn.execute(
            "UPDATE fact_daily SET high=10,low=10 WHERE symbol='000003.SZ' AND trade_date='2026-07-02'"
        )
        conn.close()
        result = self.evaluate(self.row(symbol="000003.SZ", name="测试股份"))
        self.assertEqual(result["t1_date"], "2026-07-02")
        self.assertIn("T1_ONE_PRICE", result["exclusion_reasons"])

    def test_missing_fixed_session_is_incomplete_not_shifted(self):
        conn = duckdb.connect(str(self.db_path))
        conn.execute(
            "DELETE FROM fact_daily WHERE symbol='000003.SZ' AND trade_date='2026-07-02'"
        )
        conn.execute(
            "INSERT INTO fact_daily VALUES ('000003.SZ','2026-07-07',12,12.2,11.8,12,1000,10000)"
        )
        conn.close()
        result = self.evaluate(self.row(symbol="000003.SZ", name="测试股份"))
        self.assertEqual(result["t1_date"], "2026-07-02")
        self.assertEqual(result["evaluation_status"], "NOT_MATURED_OR_INCOMPLETE")
        self.assertIn("DAILY_WINDOW_INCOMPLETE", result["exclusion_reasons"])

    def test_build_scorecard_separates_proxy_and_actual_pass(self):
        conn = duckdb.connect(str(self.db_path))
        conn.executemany(
            "INSERT INTO nexus_audits VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                ("a1b2c3d4_000001.SZ", "a1b2c3d4", "000001.SZ", "平安银行",
                 "2026-07-01", "PASS", 80, 80, "NEWS_CLEAR", "NONE",
                 "2026-07-01T21:00:00+08:00", "L4_DONE"),
                ("a1b2c3d4_000003.SZ", "a1b2c3d4", "000003.SZ", "测试股份",
                 "2026-07-01", "HOLD", 60, 60, "NEWS_CLEAR", "NONE",
                 "2026-07-01T21:00:00+08:00", "L4_DONE"),
            ],
        )
        conn.execute(
            "INSERT INTO fact_paper_positions VALUES ('a1b2c3d4_000001.SZ','SOLD',0.08)"
        )
        conn.close()
        with patch.object(MOD, "load_verified_bindings", return_value=(self.binding, [])):
            payload = MOD.build_scorecard(
                self.db_path, Path(self.tempdir.name) / "bindings", "2026-07-01", "2026-07-01"
            )
        self.assertEqual(payload["summary"]["bound_versioned_by_verdict"]["PASS"]["samples"], 1)
        self.assertEqual(payload["summary"]["bound_versioned_by_verdict"]["HOLD"]["samples"], 1)
        self.assertEqual(payload["summary"]["actual_shadow_pass_execution"]["positions"], 1)
        self.assertEqual(payload["summary"]["review_status"], "NOT_ENOUGH_BOUND_MATURED_SAMPLES")


if __name__ == "__main__":
    unittest.main()
