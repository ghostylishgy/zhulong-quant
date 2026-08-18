#!/usr/bin/env python3

import importlib.util
import json
import logging
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
os.environ["ZHULONG_SKIP_SINGLETON_LOCK"] = "1"
TEST_DAEMON_LOG = (
    Path(tempfile.gettempdir()) / f"zhulong_daemon_unittest_{os.getpid()}.log"
)
os.environ["ZHULONG_DAEMON_LOG_PATH"] = str(TEST_DAEMON_LOG)
TEST_LOG_DIR = Path(tempfile.gettempdir()) / f"zhulong_unittest_logs_{os.getpid()}"
TEST_LOG_DIR.mkdir(parents=True, exist_ok=True)
TEST_NEXUS_LOG = TEST_LOG_DIR / "nexus.log"
TEST_GOVERNANCE_LOG = TEST_LOG_DIR / "governance.log"
os.environ["ZHULONG_NEXUS_LOG_PATH"] = str(TEST_NEXUS_LOG)
os.environ["ZHULONG_GOVERNANCE_LOG_PATH"] = str(TEST_GOVERNANCE_LOG)


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


DECISION = load_module(
    "zhulong_test_run_scope_decision",
    ROOT / "02_brain" / "decision_engine.py",
)
DAEMON = load_module(
    "zhulong_test_run_scope_daemon",
    ROOT / "zhulong_daemon.py",
)
sys.path.insert(0, str(ROOT / "05_shadow" / "lib"))
RECEIVER = load_module(
    "zhulong_test_run_scope_receiver",
    ROOT / "05_shadow" / "lib" / "signal_receiver.py",
)


class _FakeGateway:
    row = (0, 0, 0, 0)

    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        return self

    def fetchone(self):
        return self.row


class _L1Gateway:
    rows = []

    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        return self

    def fetchall(self):
        return list(self.rows)


class AuditRunScopeTest(unittest.TestCase):
    def test_daemon_file_logging_isolated_from_production_log(self):
        file_paths = [
            Path(handler.baseFilename).resolve()
            for handler in DAEMON.logger.handlers
            if hasattr(handler, "baseFilename")
        ]
        self.assertEqual(file_paths, [TEST_DAEMON_LOG.resolve()])
        self.assertNotIn((ROOT / "logs" / "daemon.log").resolve(), file_paths)

    def test_nexus_and_governance_file_logging_isolated_from_production_logs(self):
        DECISION.setup_logging()
        nexus_paths = [
            Path(handler.baseFilename).resolve()
            for handler in DECISION.logger.handlers
            if hasattr(handler, "baseFilename")
        ]
        governance_paths = [
            Path(handler.baseFilename).resolve()
            for handler in logging.getLogger("governance.settings").handlers
            if hasattr(handler, "baseFilename")
        ]
        self.assertEqual(nexus_paths, [TEST_NEXUS_LOG.resolve()])
        self.assertEqual(governance_paths, [TEST_GOVERNANCE_LOG.resolve()])
        self.assertNotIn((ROOT / "logs" / "governance.log").resolve(), governance_paths)
        self.assertTrue(all((ROOT / "logs").resolve() not in path.parents for path in nexus_paths))
        self.assertEqual(
            sum(
                1 for handler in DECISION.logger.handlers
                if getattr(handler, "_zhulong_nexus_managed", False)
            ),
            2,
        )

    def test_audit_push_describes_pass_as_downstream_eligibility(self):
        self.assertIn("后续仍需通过", DAEMON._AUDIT_PASS_NEXT_GATES_CN)
        self.assertIn("PASS 仅表示", DAEMON._AUDIT_PASS_BOUNDARY_CN)
        self.assertIn("不代表买入建议", DAEMON._AUDIT_PASS_BOUNDARY_CN)
        self.assertIn("不会直接创建影子盘成交", DAEMON._AUDIT_PASS_BOUNDARY_CN)

    def test_rag_query_uses_natural_language_representation(self):
        nexus = DECISION.Nexus.__new__(DECISION.Nexus)
        candidate = DECISION.Candidate(
            symbol="600519.SH",
            pct_chg=5.2,
            turnover=3.1,
            rps_10=88,
            vol_ratio=1.8,
            pattern_name="均线多头",
            ma_alignment=True,
        )
        l3 = DECISION.L3Result(
            symbol="600519.SH",
            verdict=DECISION.Verdict.PASS,
            audit_score=72,
            reasoning="动量确认但需要警惕高位放量",
            fact_tags=["#MOMENTUM_CONFIRM"],
            falsifiable_conditions=["跌破MA20"],
        )
        query = nexus._build_l4_rag_query_text(candidate, l3)
        self.assertIn("当前上涨5.20%", query)
        self.assertIn("审计理由：动量确认", query)
        self.assertIn("请检索历史上", query)
        self.assertNotIn("symbol=", query)
        self.assertNotIn("pct_chg=", query)

    def test_l1_gate_records_rejections_and_fails_closed_per_symbol(self):
        def row(symbol):
            return (
                symbol, "2026-06-22", 10.0, 5.0, 1000.0, 1000000.0,
                3.0, 500.0, 0.0, 0, 0, 0.0, 0.0, 0.0, 80.0,
            )

        _L1Gateway.rows = [row("A"), row("B"), row("C"), row("D")]

        class Hunter:
            def _identify_pattern(self, symbol, trade_date):
                if symbol == "A":
                    return {"score": 70, "ma_alignment": False}
                if symbol == "B":
                    return {"score": 40, "ma_alignment": True}
                if symbol == "C":
                    return {"score": 41, "ma_alignment": True}
                raise RuntimeError("broken candidate")

        gate = DECISION.L1PhysicalFilter(Path("/tmp/unused.duckdb"))
        with patch.object(DECISION, "DBGateway", _L1Gateway), \
             patch.object(DECISION, "TREND_HUNTER_AVAILABLE", True), \
             patch.object(DECISION, "TrendHunter", Hunter, create=True), \
             patch.object(gate, "_save_watchlist"):
            candidates = gate.run("2026-06-22", limit=4)
        self.assertEqual([item.symbol for item in candidates], ["C"])
        self.assertEqual(gate.last_gate_stats["raw_count"], 4)
        self.assertEqual(gate.last_gate_stats["passed_count"], 1)
        self.assertEqual(gate.last_gate_stats["ma_alignment_false"], 1)
        self.assertEqual(gate.last_gate_stats["score_not_above_threshold"], 1)
        self.assertEqual(gate.last_gate_stats["candidate_errors"], 1)

    def test_l1_gate_init_failure_stops_the_run(self):
        _L1Gateway.rows = [
            (
                "A", "2026-06-22", 10.0, 5.0, 1000.0, 1000000.0,
                3.0, 500.0, 0.0, 0, 0, 0.0, 0.0, 0.0, 80.0,
            )
        ]

        class BrokenHunter:
            def __init__(self):
                raise RuntimeError("init failed")

        gate = DECISION.L1PhysicalFilter(Path("/tmp/unused.duckdb"))
        with patch.object(DECISION, "DBGateway", _L1Gateway), \
             patch.object(DECISION, "TREND_HUNTER_AVAILABLE", True), \
             patch.object(DECISION, "TrendHunter", BrokenHunter, create=True), \
             patch.object(gate, "_save_watchlist"):
            with self.assertRaisesRegex(RuntimeError, "L1_TREND_HUNTER_INIT_FAILED"):
                gate.run("2026-06-22", limit=1)
        self.assertIn("RuntimeError", gate.last_gate_stats["fatal_error"])

    def test_l1_gate_import_failure_stops_the_run(self):
        _L1Gateway.rows = [
            (
                "A", "2026-06-22", 10.0, 5.0, 1000.0, 1000000.0,
                3.0, 500.0, 0.0, 0, 0, 0.0, 0.0, 0.0, 80.0,
            )
        ]
        gate = DECISION.L1PhysicalFilter(Path("/tmp/unused.duckdb"))
        with patch.object(DECISION, "DBGateway", _L1Gateway), \
             patch.object(DECISION, "TREND_HUNTER_AVAILABLE", False), \
             patch.object(
                 DECISION,
                 "TREND_HUNTER_IMPORT_ERROR",
                 "ImportError:test missing module",
             ), \
             patch.object(gate, "_save_watchlist"):
            with self.assertRaisesRegex(RuntimeError, "L1_TREND_HUNTER_IMPORT_FAILED"):
                gate.run("2026-06-22", limit=1)
        self.assertIn("ImportError", gate.last_gate_stats["fatal_error"])

    def test_state_manager_uses_daemon_supplied_run_id(self):
        with tempfile.TemporaryDirectory() as tempdir:
            state_path = Path(tempdir) / "state.json"
            with patch.dict(os.environ, {"AUDIT_RUN_ID": "a1b2c3d4"}):
                manager = DECISION.StateManager(state_path)
                manager.reset("2026-06-22")
            self.assertEqual(manager.state["run_id"], "a1b2c3d4")
            self.assertEqual(
                manager.state["audit_contract_binding"]["provenance_status"],
                "UNBOUND_LEGACY_OR_DISABLED",
            )

    def test_state_manager_records_verified_contract_binding(self):
        manifest = DECISION._AUDIT_CONTRACT_MODULE.build_manifest(ROOT, {})
        binding = DECISION._AUDIT_CONTRACT_MODULE.bind_manifest(
            manifest,
            "2026-06-22",
            "a1b2c3d4",
            "2026-06-22T21:00:00+08:00",
        )
        env = {
            "AUDIT_RUN_ID": "a1b2c3d4",
            "AUDIT_CONTRACT_SCHEMA_VERSION": binding["contract_schema_version"],
            "AUDIT_CONTRACT_SHA256": binding["contract_sha256"],
            "AUDIT_CONTRACT_BINDING_SCHEMA_VERSION": binding["schema_version"],
            "AUDIT_CONTRACT_BINDING_SHA256": binding["binding_sha256"],
            "AUDIT_CONTRACT_BINDING_STATUS": binding["binding_status"],
            "AUDIT_CONTRACT_EVIDENCE_AS_OF": binding["evidence_as_of"],
            "AUDIT_CONTRACT_ARTIFACT_NAME": "audit_contract_test.json",
        }
        with tempfile.TemporaryDirectory() as tempdir:
            state_path = Path(tempdir) / "state.json"
            with patch.dict(os.environ, env):
                manager = DECISION.StateManager(state_path)
                manager.reset("2026-06-22")
        self.assertTrue(manager.state["audit_contract_binding"]["binding_valid"])
        self.assertEqual(
            manager.state["audit_contract_binding"]["binding_sha256"],
            binding["binding_sha256"],
        )

    def test_state_manager_rejects_invalid_run_id(self):
        with tempfile.TemporaryDirectory() as tempdir:
            state_path = Path(tempdir) / "state.json"
            with patch.dict(os.environ, {"AUDIT_RUN_ID": "not-valid"}):
                manager = DECISION.StateManager(state_path)
                with self.assertRaises(ValueError):
                    manager.reset("2026-06-22")

    def test_daemon_freezes_contract_artifact_and_child_environment(self):
        captured = {}

        class FakeContract:
            @staticmethod
            def build_bound_artifact(root, trade_date, run_id, evidence_as_of, environment):
                captured["root"] = root
                captured["budget"] = environment["AUDIT_CYCLE_BUDGET_SEC"]
                return {
                    "artifact_schema_version": "zhulong_audit_contract_artifact_v0.1",
                    "manifest": {
                        "schema_version": "zhulong_audit_contract_v0.1",
                        "contract_sha256": "a" * 64,
                    },
                    "binding": {
                        "schema_version": "zhulong_audit_run_contract_binding_v0.1",
                        "binding_status": "BOUND_AT_AUDIT_START",
                        "contract_schema_version": "zhulong_audit_contract_v0.1",
                        "contract_sha256": "a" * 64,
                        "trade_date": trade_date,
                        "run_id": run_id,
                        "evidence_as_of": evidence_as_of,
                        "binding_sha256": "b" * 64,
                    },
                }

            @staticmethod
            def verify_bound_artifact(_artifact):
                return True

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            reports = root / "reports"
            with patch.object(DAEMON, "BASE_DIR", root), \
                 patch.object(DAEMON, "AUDIT_CONTRACT_REPORT_DIR", reports), \
                 patch.object(DAEMON, "_load_audit_contract_module", return_value=FakeContract):
                child_env, artifact_path = DAEMON._prepare_audit_contract_binding(
                    "2026-06-22",
                    "a1b2c3d4",
                    "2026-06-22T21:00:00+08:00",
                    {"AUDIT_CYCLE_BUDGET_SEC": 705},
                )
                reused_env, reused_path = DAEMON._prepare_audit_contract_binding(
                    "2026-06-22",
                    "a1b2c3d4",
                    "2026-06-22T21:00:00+08:00",
                    {"AUDIT_CYCLE_BUDGET_SEC": 705},
                )
            self.assertTrue(artifact_path.exists())
            self.assertEqual(artifact_path, reused_path)
            self.assertEqual(child_env, reused_env)
            self.assertEqual(child_env["AUDIT_CONTRACT_SHA256"], "a" * 64)
            self.assertEqual(child_env["AUDIT_CONTRACT_BINDING_SHA256"], "b" * 64)
            self.assertEqual(captured["budget"], "705")
            self.assertNotIn("TOKEN", artifact_path.read_text(encoding="utf-8"))

    def test_daemon_contract_failure_does_not_block_audit(self):
        with patch.object(
            DAEMON,
            "_load_audit_contract_module",
            side_effect=RuntimeError("test unavailable"),
        ):
            child_env, artifact_path = DAEMON._prepare_audit_contract_binding(
                "2026-06-22",
                "a1b2c3d4",
                "2026-06-22T21:00:00+08:00",
                {},
            )
        self.assertEqual(child_env, {})
        self.assertIsNone(artifact_path)

    def test_run_validation_requires_all_rows_terminal(self):
        _FakeGateway.row = (3, 3, 2, 0)
        with patch.object(DAEMON, "DBGateway", _FakeGateway):
            ready, stats = DAEMON._validate_audit_run_completion(
                "2026-06-22", "a1b2c3d4"
            )
        self.assertTrue(ready)
        self.assertEqual(stats["terminal_rows"], 3)

        _FakeGateway.row = (3, 2, 1, 0)
        with patch.object(DAEMON, "DBGateway", _FakeGateway):
            ready, _ = DAEMON._validate_audit_run_completion(
                "2026-06-22", "a1b2c3d4"
            )
        self.assertFalse(ready)

    def test_run_validation_rejects_task_scope_mismatch(self):
        _FakeGateway.row = (2, 2, 1, 1)
        with patch.object(DAEMON, "DBGateway", _FakeGateway):
            ready, _ = DAEMON._validate_audit_run_completion(
                "2026-06-22", "a1b2c3d4"
            )
        self.assertFalse(ready)

    def test_run_validation_accepts_exact_completed_zero_candidate_receipt(self):
        _FakeGateway.row = (0, 0, 0, 0)
        receipt = {
            "run_id": "a1b2c3d4",
            "trade_date": "2026-06-22",
            "current_phase": "COMPLETED",
            "candidates": [],
            "l2_passed": [],
            "l3_results": [],
            "l1_gate_stats": {
                "raw_count": 50,
                "passed_count": 0,
                "ma_alignment_false": 50,
                "score_not_above_threshold": 38,
                "candidate_errors": 0,
                "fatal_error": "",
                "score_threshold": 40.0,
            },
            "last_checkpoint": "2026-06-22 21:00:20",
        }
        with tempfile.TemporaryDirectory() as tempdir:
            state_path = Path(tempdir) / "nexus_state.json"
            state_path.write_text(json.dumps(receipt), encoding="utf-8")
            with patch.object(DAEMON, "DBGateway", _FakeGateway), patch.object(
                DAEMON, "NEXUS_STATE_PATH", state_path
            ):
                ready, stats = DAEMON._validate_audit_run_completion(
                    "2026-06-22", "a1b2c3d4", source_row_count=5000
                )
        self.assertTrue(ready)
        self.assertEqual(stats["completion_mode"], "zero_candidates")
        self.assertEqual(stats["receipt_candidate_count"], 0)
        funnel, reason = DAEMON._format_zero_candidate_funnel(stats)
        self.assertEqual(funnel, "L1\u539f\u59cb 50 \u2192 L1.5 0 \u2192 L2 0 \u2192 L3 0 \u2192 L4 0")
        self.assertIn("\u5747\u7ebf\u672a\u5bf9\u9f50 50", reason)
        self.assertIn("\u5f62\u6001\u5206\u4e0d\u9ad8\u4e8e40 38", reason)

    def test_zero_candidate_receipt_rejects_l1_gate_errors(self):
        _FakeGateway.row = (0, 0, 0, 0)
        receipt = {
            "run_id": "a1b2c3d4",
            "trade_date": "2026-06-22",
            "current_phase": "COMPLETED",
            "candidates": [],
            "l2_passed": [],
            "l3_results": [],
            "l1_gate_stats": {
                "raw_count": 50,
                "passed_count": 0,
                "candidate_errors": 1,
                "fatal_error": "",
            },
            "last_checkpoint": "2026-06-22 21:00:20",
        }
        with tempfile.TemporaryDirectory() as tempdir:
            state_path = Path(tempdir) / "nexus_state.json"
            state_path.write_text(json.dumps(receipt), encoding="utf-8")
            with patch.object(DAEMON, "DBGateway", _FakeGateway), patch.object(
                DAEMON, "NEXUS_STATE_PATH", state_path
            ):
                ready, stats = DAEMON._validate_audit_run_completion(
                    "2026-06-22", "a1b2c3d4", source_row_count=5000
                )
        self.assertFalse(ready)
        self.assertEqual(stats["completion_mode"], "incomplete")


    def test_zero_candidate_receipt_requires_l1_gate_stats(self):
        _FakeGateway.row = (0, 0, 0, 0)
        receipt = {
            "run_id": "a1b2c3d4",
            "trade_date": "2026-06-22",
            "current_phase": "COMPLETED",
            "candidates": [],
            "l2_passed": [],
            "l3_results": [],
            "last_checkpoint": "2026-06-22 21:00:20",
        }
        with tempfile.TemporaryDirectory() as tempdir:
            state_path = Path(tempdir) / "nexus_state.json"
            state_path.write_text(json.dumps(receipt), encoding="utf-8")
            with patch.object(DAEMON, "DBGateway", _FakeGateway), patch.object(
                DAEMON, "NEXUS_STATE_PATH", state_path
            ):
                ready, stats = DAEMON._validate_audit_run_completion(
                    "2026-06-22", "a1b2c3d4", source_row_count=5000
                )
        self.assertFalse(ready)
        self.assertEqual(stats["completion_mode"], "incomplete")
        self.assertFalse(stats["receipt_l1_gate_stats_present"])

    def test_run_validation_rejects_stale_zero_candidate_receipt(self):
        _FakeGateway.row = (0, 0, 0, 0)
        receipt = {
            "run_id": "deadbeef",
            "trade_date": "2026-06-22",
            "current_phase": "COMPLETED",
            "candidates": [],
            "l2_passed": [],
            "l3_results": [],
            "last_checkpoint": "2026-06-22 21:00:20",
        }
        with tempfile.TemporaryDirectory() as tempdir:
            state_path = Path(tempdir) / "nexus_state.json"
            state_path.write_text(json.dumps(receipt), encoding="utf-8")
            with patch.object(DAEMON, "DBGateway", _FakeGateway), patch.object(
                DAEMON, "NEXUS_STATE_PATH", state_path
            ):
                ready, stats = DAEMON._validate_audit_run_completion(
                    "2026-06-22", "a1b2c3d4", source_row_count=5000
                )
        self.assertFalse(ready)
        self.assertEqual(stats["completion_mode"], "incomplete")
        self.assertEqual(stats["receipt_reason"], "receipt_scope_mismatch")

    def test_run_validation_rejects_zero_candidate_receipt_without_source_rows(self):
        _FakeGateway.row = (0, 0, 0, 0)
        receipt = {
            "run_id": "a1b2c3d4",
            "trade_date": "2026-06-22",
            "current_phase": "COMPLETED",
            "candidates": [],
            "l2_passed": [],
            "l3_results": [],
            "last_checkpoint": "2026-06-22 21:00:20",
        }
        with tempfile.TemporaryDirectory() as tempdir:
            state_path = Path(tempdir) / "nexus_state.json"
            state_path.write_text(json.dumps(receipt), encoding="utf-8")
            with patch.object(DAEMON, "DBGateway", _FakeGateway), patch.object(
                DAEMON, "NEXUS_STATE_PATH", state_path
            ):
                ready, stats = DAEMON._validate_audit_run_completion(
                    "2026-06-22", "a1b2c3d4", source_row_count=0
                )
        self.assertFalse(ready)
        self.assertEqual(stats["completion_mode"], "incomplete")

    def test_audit_funnel_observer_uses_exact_scope_and_readonly_snapshot(self):
        with patch.dict(
            os.environ,
            {"AUDIT_FUNNEL_OBSERVER_ENABLED": "1"},
        ), patch.object(DAEMON, "safe_run", return_value=True) as run:
            ok = DAEMON._run_audit_funnel_observer(
                "2026-06-22", "a1b2c3d4", snapshot_ready=True
            )

        self.assertTrue(ok)
        command = run.call_args.args[0]
        self.assertEqual(command[0], DAEMON.PYTHON)
        self.assertEqual(command[1], str(DAEMON.COMP["audit_funnel_observer"]))
        self.assertEqual(
            command[command.index("--db") + 1],
            str(DAEMON.API_SNAPSHOT_DB_PATH),
        )
        self.assertEqual(
            command[command.index("--state") + 1],
            str(DAEMON.NEXUS_STATE_PATH),
        )
        self.assertEqual(command[command.index("--trade-date") + 1], "2026-06-22")
        self.assertEqual(command[command.index("--run-id") + 1], "a1b2c3d4")
        self.assertIn("--write-artifacts", command)
        self.assertEqual(run.call_args.kwargs["label"], "AuditFunnelObserver")
        self.assertFalse(run.call_args.kwargs["use_sem"])

    def test_audit_funnel_observer_skips_stale_snapshot(self):
        with patch.dict(
            os.environ,
            {"AUDIT_FUNNEL_OBSERVER_ENABLED": "1"},
        ), patch.object(DAEMON, "safe_run") as run:
            ok = DAEMON._run_audit_funnel_observer(
                "2026-06-22", "a1b2c3d4", snapshot_ready=False
            )

        self.assertFalse(ok)
        run.assert_not_called()

    def test_audit_funnel_observer_can_be_disabled_without_side_effects(self):
        with patch.dict(
            os.environ,
            {"AUDIT_FUNNEL_OBSERVER_ENABLED": "0"},
        ), patch.object(DAEMON, "safe_run") as run:
            ok = DAEMON._run_audit_funnel_observer(
                "2026-06-22", "a1b2c3d4", snapshot_ready=True
            )

        self.assertTrue(ok)
        run.assert_not_called()

    def test_receiver_blocks_unscoped_fetch_before_db_access(self):
        with patch.object(RECEIVER, "_ensure_nexus_shadow_columns") as ensure:
            self.assertEqual(RECEIVER.fetch_pending_signals(), [])
            self.assertEqual(
                RECEIVER.fetch_pending_signals(
                    trade_date="2026-06-22", run_id=""
                ),
                [],
            )
            ensure.assert_not_called()


if __name__ == "__main__":
    unittest.main()
