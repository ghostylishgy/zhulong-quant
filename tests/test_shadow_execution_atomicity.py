#!/usr/bin/env python3

import sys
import tempfile
import unittest
import ast
from pathlib import Path
from unittest.mock import patch

import duckdb

from tests import _test_log_isolation  # noqa: F401


ROOT = Path(__file__).resolve().parents[1]
SHADOW_LIB = ROOT / "05_shadow" / "lib"
for path in (ROOT, SHADOW_LIB):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import engine as ENGINE  # noqa: E402
import intraday_manager as INTRADAY  # noqa: E402
import t1_fill_engine as T1  # noqa: E402


def make_fill(action="BUY"):
    price = 10.1 if action == "BUY" else 9.7
    return ENGINE.ShadowFill(
        symbol="600000.SH",
        action=action,
        price_logical=price,
        price_shadow=price,
        qty=100,
        slippage_cost=0.0,
        breakdown=ENGINE.SlippageBreakdown(original_qty=100, adjusted_qty=100),
        pricing_mode="ATOMIC_TEST",
        timestamp="2026-07-31 09:35:00",
    )


class ShadowExecutionAtomicityTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "shadow_atomic.duckdb"

    def tearDown(self):
        self.tmp.cleanup()

    @staticmethod
    def table_count(conn, table):
        exists = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name=?",
            [table],
        ).fetchone()[0]
        return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] if exists else 0

    @staticmethod
    def signal():
        return {
            "signal_id": "atomic-signal",
            "task_id": "atomic-task",
            "run_id": "atomic-run",
            "symbol": "600000.SH",
            "name": "浦发银行",
            "signal_trade_date": "2026-07-30",
            "final_score": 75.0,
        }

    @staticmethod
    def event():
        return {
            "fill_id": "atomic-signal|2026-07-31|FILLED",
            "signal_id": "atomic-signal",
            "task_id": "atomic-task",
            "run_id": "atomic-run",
            "symbol": "600000.SH",
            "name": "浦发银行",
            "signal_trade_date": "2026-07-30",
            "fill_date": "2026-07-31",
            "status": T1.FILLED_STATUS,
            "reason": "ATOMIC_TEST",
            "base_price": 10.1,
            "fill_price": 10.1,
            "qty": 100,
            "allocated_cash": 2000.0,
            "gross_amount": 1010.0,
            "data_quality": "TEST_OK",
            "evidence": {"test": True},
        }

    def prepare_pending(self):
        conn = duckdb.connect(str(self.db_path))
        T1.ensure_t1_tables(conn)
        conn.execute(
            """
            INSERT INTO fact_shadow_pending_signals (
                signal_id, task_id, run_id, symbol, name, signal_trade_date,
                earliest_fill_date, final_score, l1_close, status, attempts,
                last_error, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, CAST(? AS DATE), CAST(? AS DATE), ?, ?, ?, 0, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            [
                "atomic-signal", "atomic-task", "atomic-run", "600000.SH", "浦发银行",
                "2026-07-30", "2026-07-31", 75.0, 10.0, T1.PENDING_STATUS,
            ],
        )
        conn.close()

    def test_persist_fill_rolls_back_dedup_when_ledger_insert_fails(self):
        conn = duckdb.connect(str(self.db_path))
        conn.execute(
            """
            CREATE TABLE fact_shadow_ledger (
                timestamp TIMESTAMP, trade_date DATE, trace_id VARCHAR, symbol VARCHAR,
                action VARCHAR, price_logical DOUBLE, price_shadow DOUBLE,
                qty INTEGER CHECK(qty < 0), tide_mode VARCHAR, strategy_tag VARCHAR,
                slippage_cost DOUBLE, pricing_mode VARCHAR, gross_amount DOUBLE,
                commission DOUBLE, stamp_tax DOUBLE, transfer_fee DOUBLE,
                tax_total DOUBLE, net_amount DOUBLE
            )
            """
        )
        conn.close()
        with patch.object(ENGINE, "DB_PATH", str(self.db_path)):
            self.assertFalse(ENGINE.persist_fill(make_fill(), trace_id="rollback-dedup"))
        conn = duckdb.connect(str(self.db_path), read_only=True)
        self.assertEqual(self.table_count(conn, "shadow_dedup_guard"), 0)
        self.assertEqual(self.table_count(conn, "fact_shadow_ledger"), 0)
        conn.close()

    def test_t1_failure_rolls_back_ledger_position_event_and_pending(self):
        self.prepare_pending()
        with patch.object(T1, "DB_PATH", str(self.db_path)), patch.object(
            T1, "_record_fill_event", side_effect=RuntimeError("event write failed")
        ):
            committed, reason = T1._commit_filled_buy(
                self.signal(), make_fill(), "2026-07-31",
                tide_mode="STANDARD", strategy_tag="ATOMIC_TEST",
                entry_tide_gate="STANDARD", entry_tide_ratio=1.0,
                entry_policy="STANDARD_ENTRY", event=self.event(),
            )
        self.assertFalse(committed)
        self.assertEqual(reason, "ATOMIC_BUY_TRANSACTION_FAILED")
        conn = duckdb.connect(str(self.db_path), read_only=True)
        self.assertEqual(self.table_count(conn, "shadow_dedup_guard"), 0)
        self.assertEqual(self.table_count(conn, "fact_shadow_ledger"), 0)
        self.assertEqual(self.table_count(conn, "fact_paper_positions"), 0)
        self.assertEqual(self.table_count(conn, "fact_shadow_fill_events"), 0)
        self.assertEqual(
            conn.execute("SELECT status FROM fact_shadow_pending_signals WHERE signal_id='atomic-signal'").fetchone()[0],
            T1.PENDING_STATUS,
        )
        conn.close()

    def test_t1_success_commits_complete_buy_chain(self):
        self.prepare_pending()
        with patch.object(T1, "DB_PATH", str(self.db_path)):
            committed, reason = T1._commit_filled_buy(
                self.signal(), make_fill(), "2026-07-31",
                tide_mode="STANDARD", strategy_tag="ATOMIC_TEST",
                entry_tide_gate="STANDARD", entry_tide_ratio=1.0,
                entry_policy="STANDARD_ENTRY", event=self.event(),
            )
        self.assertTrue(committed)
        self.assertEqual(reason, "")
        conn = duckdb.connect(str(self.db_path), read_only=True)
        for table in ("shadow_dedup_guard", "fact_shadow_ledger", "fact_paper_positions", "fact_shadow_fill_events"):
            self.assertEqual(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0], 1)
        self.assertEqual(
            conn.execute("SELECT status FROM fact_shadow_pending_signals WHERE signal_id='atomic-signal'").fetchone()[0],
            T1.FILLED_STATUS,
        )
        conn.close()

    def test_intraday_sell_failure_rolls_back_position_and_dedup(self):
        conn = duckdb.connect(str(self.db_path))
        ENGINE._ensure_paper_positions(conn)
        ENGINE._ensure_shadow_ledger(conn)
        buy = make_fill()
        self.assertTrue(
            ENGINE.open_paper_position_from_fill(
                buy, signal_task_id="atomic-task", trade_date="2026-07-31", conn=conn
            )
        )
        conn.execute("DROP TABLE fact_shadow_ledger")
        conn.execute(
            """
            CREATE TABLE fact_shadow_ledger (
                timestamp TIMESTAMP, trade_date DATE, trace_id VARCHAR, symbol VARCHAR,
                action VARCHAR, price_logical DOUBLE, price_shadow DOUBLE,
                qty INTEGER CHECK(qty < 0), tide_mode VARCHAR, strategy_tag VARCHAR,
                slippage_cost DOUBLE, pricing_mode VARCHAR, gross_amount DOUBLE,
                commission DOUBLE, stamp_tax DOUBLE, transfer_fee DOUBLE,
                tax_total DOUBLE, net_amount DOUBLE
            )
            """
        )
        manager = object.__new__(INTRADAY.ShadowIntradayManager)
        sell = make_fill("SELL")
        now_ts = "2026-08-03 10:00:00"
        values = [
            10.1, 9.5, 0, 100, sell.gross_amount, sell.gross_amount,
            sell.commission, sell.stamp_tax, sell.transfer_fee, sell.tax_total,
            sell.net_amount, -50.0, "SOLD", "NORMAL", "WRONG_PICK_STOP",
            "test", sell.price_shadow, -0.04, -0.04, "SOLD", True,
            "2026-08-03", now_ts, "600000.SH", "2026-07-31", 100,
        ]
        committed, _ = manager._commit_sell_execution(
            conn, fill=sell, trace_id="atomic-sell", strategy_tag="ATOMIC_TEST",
            trade_date="2026-08-03", position_values=values,
            liquidation={"enabled": False},
        )
        self.assertFalse(committed)
        self.assertEqual(
            conn.execute("SELECT status FROM fact_paper_positions WHERE signal_task_id='atomic-task'").fetchone()[0],
            "HOLD",
        )
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM shadow_dedup_guard").fetchone()[0], 0)
        conn.close()

    def test_authoritative_ledger_is_not_daily_pruned(self):
        tree = ast.parse((ROOT / "zhulong_daemon.py").read_text(encoding="utf-8"))
        prune = next(
            node for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "_run_daily_prune"
        )
        source = ast.unparse(prune)
        self.assertIn("ops_daemon_heartbeat", source)
        self.assertNotIn("fact_shadow_ledger", source)


if __name__ == "__main__":
    unittest.main()
