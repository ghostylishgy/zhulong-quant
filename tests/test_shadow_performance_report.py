import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "tools" / "review_shadow_performance.py"
SPEC = importlib.util.spec_from_file_location("shadow_performance_report", MODULE_PATH)
MOD = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules[SPEC.name] = MOD
SPEC.loader.exec_module(MOD)


def trade(pnl, exit_date, rule="RULE"):
    return {
        "symbol": "000001.SZ",
        "exit_date": exit_date,
        "net_pnl": pnl,
        "sell_rule": rule,
        "entry_total_cost": 1000.0,
        "realized_gross_amount": 1000.0 + pnl,
    }


class ShadowPerformanceReportTest(unittest.TestCase):
    def test_drawdown_duration_uses_underwater_sessions(self):
        curve = [
            {"trade_date": "2026-07-01", "daily_drawdown": 0.0},
            {"trade_date": "2026-07-02", "daily_drawdown": 0.1},
            {"trade_date": "2026-07-03", "daily_drawdown": 0.2},
            {"trade_date": "2026-07-06", "daily_drawdown": 0.0},
            {"trade_date": "2026-07-07", "daily_drawdown": 0.05},
        ]
        result = MOD.drawdown_profile(curve)
        self.assertEqual(result["max_drawdown_ratio"], 0.2)
        self.assertEqual(result["max_drawdown_duration_sessions"], 2)
        self.assertEqual(result["max_drawdown_period"]["start_date"], "2026-07-02")
        self.assertEqual(result["max_drawdown_period"]["recovered_on"], "2026-07-06")
        self.assertTrue(result["current_underwater"])

    def test_streak_and_exit_rule_breakdown(self):
        trades = [
            trade(100, "2026-07-01", "TAKE"),
            trade(-50, "2026-07-02", "STOP"),
            trade(-25, "2026-07-03", "STOP"),
            trade(200, "2026-07-04", "TAKE"),
        ]
        self.assertEqual(MOD.max_streak(trades, lambda value: value < 0), 2)
        rows = {row["sell_rule"]: row for row in MOD.exit_rule_breakdown(trades)}
        self.assertEqual(rows["STOP"]["closed_trades"], 2)
        self.assertEqual(rows["STOP"]["win_rate"], 0.0)
        self.assertEqual(rows["TAKE"]["net_pnl_amount"], 300.0)

    def test_build_report_is_read_only_and_omits_annualized_metrics(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "shadow.duckdb"
            conn = duckdb.connect(str(db))
            conn.execute(
                """
                CREATE TABLE fact_paper_positions (
                    symbol VARCHAR, trade_date DATE, exit_trade_date DATE, status VARCHAR,
                    initial_qty INTEGER, qty INTEGER, realized_qty INTEGER,
                    entry_price DOUBLE, exit_price DOUBLE, entry_score DOUBLE,
                    entry_total_cost DOUBLE, realized_amount DOUBLE, realized_net_amount DOUBLE,
                    realized_gross_amount DOUBLE, realized_tax_total DOUBLE,
                    entry_tax_total DOUBLE, realized_net_pnl DOUBLE,
                    pnl_ratio DOUBLE, pnl_ratio_gross DOUBLE,
                    last_sell_rule_id VARCHAR, last_sell_reason VARCHAR
                )
                """
            )
            conn.execute("CREATE TABLE fact_stock_basic(symbol VARCHAR, name VARCHAR)")
            conn.execute("CREATE TABLE fact_daily(symbol VARCHAR, trade_date DATE, close DOUBLE)")
            rows = [
                ("000001.SZ", "2026-07-01", "2026-07-02", "SOLD", 100, 0, 100, 10, 11, 70, 1000, 1100, 1100, 1100, 0, 0, 100, 0.1, 0.1, "TAKE", ""),
                ("000002.SZ", "2026-07-02", "2026-07-03", "SOLD", 100, 0, 100, 10, 9.5, 60, 1000, 950, 950, 950, 0, 0, -50, -0.05, -0.05, "STOP", ""),
                ("000003.SZ", "2026-07-03", "2026-07-04", "SOLD", 100, 0, 100, 10, 9.75, 55, 1000, 975, 975, 975, 0, 0, -25, -0.025, -0.025, "STOP", ""),
                ("000004.SZ", "2026-07-04", "2026-07-07", "SOLD", 100, 0, 100, 10, 12, 80, 1000, 1200, 1200, 1200, 0, 0, 200, 0.2, 0.2, "TAKE", ""),
            ]
            conn.executemany("INSERT INTO fact_paper_positions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
            for symbol, entry, exit_date, *_ in rows:
                conn.execute(
                    "INSERT INTO fact_daily VALUES (?,?,10),(?,?,10)",
                    [symbol, entry, symbol, exit_date],
                )
            conn.close()
            before = db.read_bytes()
            with duckdb.connect(str(db), read_only=True) as read_conn:
                report = MOD.build_report(read_conn, "2026-07-07")
            self.assertEqual(before, db.read_bytes())
            metrics = report["trade_metrics"]
            self.assertEqual(metrics["profit_factor"], 4.0)
            self.assertEqual(metrics["payoff_ratio"], 4.0)
            self.assertEqual(metrics["max_consecutive_losses"], 2)
            self.assertEqual(metrics["largest_winner_profit_share"], 0.6667)
            self.assertEqual(report["sample_status"], "INSUFFICIENT_SAMPLE")
            self.assertNotIn("sharpe", metrics)
            self.assertIn("sharpe", report["annualized_metrics_omitted"])
            self.assertIn("read_only", MOD.render_markdown(report))


if __name__ == "__main__":
    unittest.main()
