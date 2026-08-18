import contextlib
import importlib.util
import io
import json
import sys
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

import duckdb


ROOT = Path(__file__).resolve().parents[1]
PATH = ROOT / "tools/audit_funnel_observer.py"
SPEC = importlib.util.spec_from_file_location("zhulong_audit_funnel_observer", PATH)
MOD = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MOD
SPEC.loader.exec_module(MOD)


class AuditFunnelObserverTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.base = Path(self.tempdir.name)
        self.db_path = self.base / "fixture.duckdb"
        self.state_path = self.base / "nexus_state.json"
        self.trade_date = self._build_database()

    def tearDown(self):
        self.tempdir.cleanup()

    def _build_database(self):
        conn = duckdb.connect(str(self.db_path))
        conn.execute(
            """
            CREATE TABLE fact_daily (
                symbol VARCHAR, trade_date DATE, open DOUBLE, high DOUBLE,
                low DOUBLE, close DOUBLE, pre_close DOUBLE, pct_chg DOUBLE,
                vol DOUBLE, amount DOUBLE
            );
            CREATE TABLE fact_rps_results (
                symbol VARCHAR, trade_date DATE, rps_10 DOUBLE
            );
            CREATE TABLE fact_stock_basic (
                symbol VARCHAR, name VARCHAR, is_st BOOLEAN
            );
            CREATE TABLE nexus_audits (
                task_id VARCHAR, run_id VARCHAR, symbol VARCHAR,
                trade_date DATE, status VARCHAR, l2_passed INTEGER,
                l3_verdict VARCHAR, l4_final_verdict VARCHAR,
                l4_veto_reason VARCHAR
            );
            """
        )
        symbols = ["000001.SZ", "000002.SZ", "600000.SH"]
        conn.executemany(
            "INSERT INTO fact_stock_basic VALUES (?, ?, ?)",
            [(symbol, f"Name-{index}", False) for index, symbol in enumerate(symbols)],
        )
        start = date(2026, 1, 1)
        rows = []
        rps_rows = []
        for offset in range(65):
            current = start + timedelta(days=offset)
            for index, symbol in enumerate(symbols):
                close = 10.0 + offset * 0.1 + index
                volume = 150.0 if offset == 64 else 100.0
                rows.append(
                    (symbol, current, close - 0.1, close + 0.2, close - 0.2,
                     close, close - 0.1, 1.0 + index * 0.1, volume, 20000.0)
                )
                rps_rows.append((symbol, current, 80.0 + index))
        conn.executemany(
            "INSERT INTO fact_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows
        )
        conn.executemany(
            "INSERT INTO fact_rps_results VALUES (?, ?, ?)", rps_rows
        )
        conn.close()
        return str(start + timedelta(days=64))

    def _write_state(self, passed=0, l2=0, l3=None):
        l3 = l3 or []
        state = {
            "run_id": "a1b2c3d4",
            "trade_date": self.trade_date,
            "current_phase": "COMPLETED",
            "candidates": [{"symbol": f"S{index}"} for index in range(passed)],
            "l2_passed": [[f"S{index}", {}] for index in range(l2)],
            "l3_results": l3,
            "l1_gate_stats": {
                "raw_count": 3,
                "passed_count": passed,
                "ma_alignment_false": 3 - passed,
                "score_not_above_threshold": 0,
                "invalid_score": 0,
                "candidate_errors": 0,
                "fatal_error": "",
                "score_threshold": 40.0,
            },
            "audit_contract_binding": {
                "binding_valid": True,
                "provenance_status": "BOUND_VERIFIED",
                "binding_status": "BOUND_AT_AUDIT_START",
                "schema_version": "zhulong_audit_run_contract_binding_v0.1",
                "contract_schema_version": "zhulong_audit_contract_v0.1",
                "contract_sha256": "a" * 64,
                "binding_sha256": "b" * 64,
                "trade_date": self.trade_date,
                "run_id": "a1b2c3d4",
                "evidence_as_of": f"{self.trade_date}T21:00:00+08:00",
                "artifact_name": "audit_contract_test.json",
            },
            "last_checkpoint": f"{self.trade_date} 21:00:20",
        }
        self.state_path.write_text(json.dumps(state), encoding="utf-8")
        return state

    def test_valid_zero_candidate_run_is_warning_not_failure(self):
        self._write_state()
        payload = MOD.build_payload(
            self.db_path, self.state_path, code_version="test-sha"
        )
        self.assertEqual(payload["batch_quality"], "DATA_OK")
        self.assertEqual(payload["observation_status"], "OBSERVE_WARN")
        self.assertEqual(payload["hard_failure_count"], 0)
        self.assertEqual(payload["receipt_metrics"]["l15_count"], 0)
        self.assertTrue(payload["observer_only"])
        self.assertTrue(payload["no_trade_signal"])
        self.assertIn(
            "L1_5_ZERO_PASS", [item["code"] for item in payload["canaries"]]
        )
        self.assertEqual(payload["market_context"]["first_l15_proxy_rank"], 1)
        self.assertEqual(
            payload["audit_contract_provenance"]["provenance_status"],
            "BOUND_VERIFIED",
        )

    def test_unbound_contract_is_warning_only(self):
        state = self._write_state(passed=1, l2=1, l3=[["S0", {"verdict": "PASS"}]])
        state.pop("audit_contract_binding")
        self.state_path.write_text(json.dumps(state), encoding="utf-8")
        conn = duckdb.connect(str(self.db_path))
        conn.execute(
            """
            INSERT INTO nexus_audits VALUES
            ('a1b2c3d4_000001.SZ','a1b2c3d4','000001.SZ',?,'L4_DONE',1,'PASS','PASS','')
            """,
            [self.trade_date],
        )
        conn.close()
        payload = MOD.build_payload(self.db_path, self.state_path, code_version="test-sha")
        self.assertEqual(payload["batch_quality"], "DATA_OK")
        self.assertEqual(payload["observation_status"], "OBSERVE_WARN")
        self.assertEqual(
            payload["audit_contract_provenance"]["provenance_status"],
            "UNBOUND_LEGACY_OR_DISABLED",
        )

    def test_terminal_rows_produce_complete_nonzero_funnel(self):
        self._write_state(
            passed=1,
            l2=1,
            l3=[["S0", {"verdict": "PASS"}]],
        )
        conn = duckdb.connect(str(self.db_path))
        conn.execute(
            """
            INSERT INTO nexus_audits VALUES
            ('a1b2c3d4_000001.SZ','a1b2c3d4','000001.SZ',?,'L4_DONE',1,'PASS','PASS','')
            """,
            [self.trade_date],
        )
        conn.close()
        payload = MOD.build_payload(
            self.db_path, self.state_path, code_version="test-sha"
        )
        self.assertEqual(payload["batch_quality"], "DATA_OK")
        self.assertEqual(payload["observation_status"], "OBSERVE_CLEAR")
        self.assertEqual(payload["audit_rows"]["terminal_rows"], 1)
        self.assertEqual(payload["audit_rows"]["pass_rows"], 1)
        self.assertEqual(payload["receipt_metrics"]["l3_non_veto_count"], 1)

    def test_stale_scope_fails_closed(self):
        state = self._write_state()
        state["trade_date"] = "2026-12-31"
        self.state_path.write_text(json.dumps(state), encoding="utf-8")
        payload = MOD.build_payload(
            self.db_path,
            self.state_path,
            trade_date="2026-12-31",
            code_version="test-sha",
        )
        self.assertEqual(payload["batch_quality"], "DATA_FAIL_CLOSED")
        codes = {
            item["code"] for item in payload["canaries"]
            if item["status"] == "FAIL_CLOSED"
        }
        self.assertIn("SOURCE_DATE_MATCH", codes)
        self.assertIn("SOURCE_PRIMARY_KEY_UNIQUE", codes)

    def test_missing_rps_pair_fails_closed(self):
        self._write_state()
        conn = duckdb.connect(str(self.db_path))
        conn.execute(
            "DELETE FROM fact_rps_results WHERE symbol='000001.SZ' AND trade_date=?",
            [self.trade_date],
        )
        conn.close()
        payload = MOD.build_payload(
            self.db_path, self.state_path, code_version="test-sha"
        )
        self.assertEqual(payload["batch_quality"], "DATA_FAIL_CLOSED")
        rps = next(item for item in payload["canaries"] if item["code"] == "RPS_EXACT_COVERAGE")
        self.assertEqual(rps["metrics"]["missing_pairs"], 1)

    def test_cli_returns_nonzero_for_fail_closed_batch(self):
        state = self._write_state()
        state["trade_date"] = "2026-12-31"
        self.state_path.write_text(json.dumps(state), encoding="utf-8")
        argv = [
            str(PATH),
            "--db", str(self.db_path),
            "--state", str(self.state_path),
            "--trade-date", "2026-12-31",
            "--run-id", "a1b2c3d4",
        ]
        stdout = io.StringIO()
        with patch.object(sys, "argv", argv), contextlib.redirect_stdout(stdout):
            exit_code = MOD.main()

        self.assertEqual(exit_code, 2)
        result = json.loads(stdout.getvalue())
        self.assertEqual(result["batch_quality"], "DATA_FAIL_CLOSED")
        self.assertFalse(result["wrote_artifacts"])

    def test_frozen_artifact_is_reused_and_cannot_change_identity(self):
        self._write_state()
        payload = MOD.build_payload(
            self.db_path, self.state_path, code_version="test-sha"
        )
        output_dir = self.base / "reports"
        json_path, md_path, action = MOD.write_artifacts(payload, output_dir)
        self.assertEqual(action, "CREATED_FROZEN")
        self.assertTrue(json_path.exists())
        self.assertTrue(md_path.exists())
        _, _, action = MOD.write_artifacts(payload, output_dir)
        self.assertEqual(action, "REUSED_FROZEN")
        md_path.unlink()
        MOD.write_artifacts(payload, output_dir)
        self.assertTrue(md_path.exists())
        changed = dict(payload)
        changed["artifact_identity_sha256"] = "different"
        with self.assertRaises(ValueError):
            MOD.write_artifacts(changed, output_dir)


if __name__ == "__main__":
    unittest.main()
