import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
SHADOW_LIB = ROOT / "05_shadow" / "lib"
sys.path.insert(0, str(SHADOW_LIB))
MODULE_PATH = SHADOW_LIB / "portfolio_metrics.py"
SPEC = importlib.util.spec_from_file_location("shadow_portfolio_metrics", MODULE_PATH)
assert SPEC and SPEC.loader
metrics = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(metrics)


class ShadowPortfolioMetricsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.conn = duckdb.connect(str(Path(self.tmp.name) / "test.duckdb"))
        self.conn.execute("""
            CREATE TABLE fact_paper_positions (
                symbol VARCHAR, trade_date DATE, exit_trade_date DATE, status VARCHAR,
                initial_qty INTEGER, qty INTEGER, realized_qty INTEGER, entry_price DOUBLE,
                entry_total_cost DOUBLE, realized_net_pnl DOUBLE, realized_net_amount DOUBLE
            )
        """)
        self.conn.execute("CREATE TABLE fact_daily (symbol VARCHAR, trade_date DATE, close DOUBLE)")

    def tearDown(self):
        self.conn.close()
        self.tmp.cleanup()

    def test_closed_trade_changes_final_equity_and_cash(self):
        self.conn.execute("INSERT INTO fact_paper_positions VALUES ('000001.SZ','2026-07-01','2026-07-02','SOLD',100,0,100,10,1005,-55,950)")
        self.conn.execute("INSERT INTO fact_daily VALUES ('000001.SZ','2026-07-01',10),('000001.SZ','2026-07-02',9.5)")
        curve = metrics.compute_shadow_equity_curve(self.conn, "2026-07-02")
        self.assertEqual(curve[-1]["total_equity"], 999945.0)
        self.assertEqual(curve[-1]["cash_reserve"], 999945.0)
        self.assertEqual(curve[-1]["active_positions"], 0)

    def test_open_position_uses_net_liquidation_value(self):
        self.conn.execute("INSERT INTO fact_paper_positions VALUES ('000001.SZ','2026-07-01',NULL,'HOLD',100,100,0,10,1005,0,0)")
        self.conn.execute("INSERT INTO fact_daily VALUES ('000001.SZ','2026-07-01',10),('000001.SZ','2026-07-02',11)")
        curve = metrics.compute_shadow_equity_curve(self.conn, "2026-07-02")
        expected_liquidation = metrics.calculate_trade_cost("SELL", price=11, qty=100).net_amount
        self.assertEqual(curve[-1]["cash_reserve"], 998995.0)
        self.assertEqual(curve[-1]["total_equity"], round(998995.0 + expected_liquidation, 2))
        self.assertEqual(curve[-1]["active_positions"], 1)

    def test_rebuild_replaces_flat_legacy_metrics(self):
        self.conn.execute("INSERT INTO fact_paper_positions VALUES ('000001.SZ','2026-07-01','2026-07-02','SOLD',100,0,100,10,1005,100,1105)")
        self.conn.execute("INSERT INTO fact_daily VALUES ('000001.SZ','2026-07-01',10),('000001.SZ','2026-07-02',11)")
        self.conn.execute("CREATE TABLE shadow_metrics (trade_date DATE PRIMARY KEY,total_equity DOUBLE,cash_reserve DOUBLE,daily_drawdown DOUBLE,active_positions INTEGER,updated_at TIMESTAMP)")
        self.conn.execute("INSERT INTO shadow_metrics VALUES ('2026-07-02',1000000,1000000,0,0,CURRENT_TIMESTAMP)")
        metrics.rebuild_shadow_metrics(self.conn, "2026-07-02", replace=True)
        row = self.conn.execute("SELECT total_equity,cash_reserve FROM shadow_metrics ORDER BY trade_date DESC LIMIT 1").fetchone()
        self.assertEqual(row, (1000100.0, 1000100.0))


if __name__ == "__main__":
    unittest.main()
