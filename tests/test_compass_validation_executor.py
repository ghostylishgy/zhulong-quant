#!/usr/bin/env python3

import importlib.util
import json
import sys
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd

from tests import _test_log_isolation  # noqa: F401

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "tools" / "compass_validation_executor.py"
SPEC = importlib.util.spec_from_file_location(
    "compass_validation_executor", MODULE_PATH
)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class FakeTushare:
    def daily_basic(self, **kwargs):
        return pd.DataFrame([
            {
                "ts_code": "000001.SZ", "trade_date": "20260718",
                "close": 13.0, "turnover_rate": 3.0, "pe_ttm": 30.0,
                "pb": 3.0, "ps_ttm": 4.0, "dv_ttm": 0.0,
                "total_mv": 3000.0, "circ_mv": 2500.0,
            },
            {
                "ts_code": "000001.SZ", "trade_date": "20260717",
                "close": 12.0, "turnover_rate": 2.0, "pe_ttm": 20.0,
                "pb": 2.0, "ps_ttm": 3.0, "dv_ttm": 1.0,
                "total_mv": 2000.0, "circ_mv": 1800.0,
            },
            {
                "ts_code": "000001.SZ", "trade_date": "20250717",
                "close": 8.0, "turnover_rate": 1.0, "pe_ttm": 10.0,
                "pb": 1.0, "ps_ttm": 2.0, "dv_ttm": 2.0,
                "total_mv": 1000.0, "circ_mv": 900.0,
            },
        ])

    def fina_indicator(self, **kwargs):
        return pd.DataFrame([
            {
                "ts_code": "000001.SZ", "ann_date": "20260720",
                "end_date": "20260630", "roe": 99.0,
                "grossprofit_margin": 99.0, "debt_to_assets": 1.0,
                "current_ratio": 9.0, "q_sales_yoy": 99.0,
                "q_profit_yoy": 99.0, "ocf_to_or": 9.0,
                "ar_turn": 9.0, "inv_turn": 9.0,
            },
            {
                "ts_code": "000001.SZ", "ann_date": "20260425",
                "end_date": "20260331", "roe": 12.0,
                "grossprofit_margin": 35.0, "debt_to_assets": 45.0,
                "current_ratio": 1.5, "q_sales_yoy": 8.0,
                "q_profit_yoy": 6.0, "ocf_to_or": 0.8,
                "ar_turn": 2.0, "inv_turn": 3.0,
            },
        ])

    def income(self, **kwargs):
        return pd.DataFrame([
            {
                "ts_code": "000001.SZ", "ann_date": "20260425",
                "f_ann_date": "20260425", "end_date": "20260331",
                "report_type": "1", "revenue": 1000.0,
                "n_income_attr_p": 100.0,
            }
        ])

    def balancesheet(self, **kwargs):
        return pd.DataFrame([
            {
                "ts_code": "000001.SZ", "ann_date": "20260425",
                "f_ann_date": "20260425", "end_date": "20260331",
                "total_assets": 2000.0, "total_liab": 900.0,
                "total_cur_assets": 1000.0, "total_cur_liab": 500.0,
                "accounts_receiv": 100.0, "inventories": 200.0,
                "contract_liab": 50.0,
            }
        ])

    def cashflow(self, **kwargs):
        return pd.DataFrame([
            {
                "ts_code": "000001.SZ", "ann_date": "20260425",
                "f_ann_date": "20260425", "end_date": "20260331",
                "n_cashflow_act": 80.0,
            }
        ])


def create_database(db_path: str) -> None:
    conn = duckdb.connect(db_path)
    conn.execute(
        """
        CREATE TABLE fact_stock_basic (
            symbol VARCHAR, name VARCHAR, industry VARCHAR, market VARCHAR,
            list_date DATE, is_st BOOLEAN, updated_at TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        INSERT INTO fact_stock_basic VALUES
        ('000001.SZ', '平安银行', '银行', '主板', '1991-04-03', FALSE, NOW())
        """
    )
    conn.execute(
        """
        CREATE TABLE fact_daily (
            symbol VARCHAR, trade_date DATE, open DOUBLE, high DOUBLE,
            low DOUBLE, close DOUBLE, pre_close DOUBLE, pct_chg DOUBLE,
            vol DOUBLE, amount DOUBLE, turnover_rate DOUBLE,
            ma20 DOUBLE, vol_ma5 DOUBLE
        )
        """
    )
    start = date(2026, 3, 19)
    rows = []
    for index in range(121):
        trade_date = start + timedelta(days=index)
        close = 8.0 + index * 0.03
        rows.append([
            "000001.SZ", trade_date, close - 0.1, close + 0.2,
            close - 0.2, close, close - 0.05, 0.3, 1000 + index,
            10000 + index, 1.0 + index / 100, close - 0.3, 900.0,
        ])
    conn.executemany(
        "INSERT INTO fact_daily VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", rows
    )
    conn.execute(
        """
        CREATE TABLE fact_rps_results (
            symbol VARCHAR, trade_date DATE, rps_10 DOUBLE, rps_20 DOUBLE,
            rps_50 DOUBLE, rps_120 DOUBLE, rps_250 DOUBLE
        )
        """
    )
    conn.execute(
        """
        INSERT INTO fact_rps_results VALUES
        ('000001.SZ', '2026-07-17', 90, 88, 80, 70, 60)
        """
    )
    conn.execute(
        """
        CREATE TABLE fact_zeta_signals (
            ts_code VARCHAR, trade_date DATE, lhb_net DOUBLE, lhb_buy DOUBLE,
            lhb_sell DOUBLE, seat_count INTEGER, inst_buy INTEGER,
            hot_money INTEGER, rzye DOUBLE, rzmre DOUBLE,
            margin_delta DOUBLE, block_trade_vol DOUBLE,
            block_trade_premium DOUBLE, data_source VARCHAR,
            collected_at TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        INSERT INTO fact_zeta_signals VALUES
        ('000001.SZ', '2026-07-17', 10, 20, 10, 2, 1, 0,
         100, 5, 3, 0, 0, 'test', NOW())
        """
    )
    conn.execute(
        """
        CREATE TABLE fact_quantile_snapshot (
            trade_date DATE, rps_p85 DOUBLE, rps_p90 DOUBLE,
            rps_p95 DOUBLE, vol_median DOUBLE, vol_p90 DOUBLE
        )
        """
    )
    conn.execute(
        """
        INSERT INTO fact_quantile_snapshot VALUES
        ('2026-07-17', 85, 90, 95, 1000, 2000)
        """
    )
    conn.close()


def task_payload() -> dict:
    blocked = sorted(MODULE.REQUIRED_BLOCKS | {"execute_validation_task"})
    task = {
        "task_id": "COMPASS-2026W28-REVIEWED-TASK-001",
        "task_type": "stock_validation",
        "origin": "human_reviewed_theme_discovery",
        "execution_status": "NOT_EXECUTED_DRY_RUN_ARTIFACT",
        "symbol": "000001.SZ",
        "name": "平安银行",
        "market": "A股",
        "source_theme_names": ["银行数字化"],
        "compass_lines": ["D线"],
        "source_reviewed_candidate_ids": ["ROW-1-REVIEWED"],
        "source_reviewed_candidate_sha256": ["4" * 64],
        "source_preview_row_ids": ["ROW-1"],
        "reviewer_id": "steve",
        "reviewed_at": "2026-07-13T20:00:00+08:00",
        "compass_source_as_of": "2026-07-10",
        "validation_as_of": "2026-07-17",
        "review_assertions": dict(MODULE.REQUIRED_REVIEW_ASSERTIONS),
        "evidence_levels": ["medium"],
        "mapping_evidence_levels": ["L1"],
        "evidence_items": [
            {
                "evidence_item_id": "EVID-ROW-1-001",
                "type": "independent_board_membership",
                "source": "tushare_discovery_source_snapshot",
                "source_id": "885001.TI",
                "matched_keywords": ["digital banking"],
            }
        ],
        "supporting_evidence_items": [
            {
                "evidence_item_id": "EVID-ROW-1-001",
                "type": "independent_board_membership",
                "source": "tushare_discovery_source_snapshot",
                "source_id": "885001.TI",
                "matched_keywords": ["digital banking"],
            }
        ],
        "gate_checks": sorted(MODULE.REQUIRED_TASK_GATES),
        "required_readonly_modules": sorted(MODULE.REQUIRED_READONLY_MODULES),
        "questions_for_zhulong": [
            "近20日股价是否过热？",
            "财务质量和客户认证是否支持逻辑？",
            "真实订单是否增长？",
        ],
        "no_trade_signal": True,
        "blocked_actions": blocked,
    }
    return {
        "protocol_version": MODULE.INPUT_VERSION,
        "batch_id": "2026W28",
        "mode": "dry_run",
        "dry_run": True,
        "no_trade_signal": True,
        "source_reviewed_artifact_sha256": "1" * 64,
        "source_preview_sha256": "2" * 64,
        "review_manifest_sha256": "3" * 64,
        "compass_source_as_of": "2026-07-10",
        "validation_as_of": "2026-07-17",
        "blocked_actions": blocked,
        "stats": {"generated_tasks": 1},
        "validation_tasks": [task],
    }


def payload_bytes(payload: dict) -> tuple[bytes, str]:
    raw = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode()
    return raw, MODULE.sha256_bytes(raw)


class CompassValidationExecutorTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tempdir.name) / "test.duckdb")
        create_database(self.db_path)

    def tearDown(self):
        self.tempdir.cleanup()

    def test_contract_requires_exact_hash_and_safety_flags(self):
        payload = task_payload()
        _, digest = payload_bytes(payload)
        tasks = MODULE.validate_task_artifact(payload, digest, digest)
        self.assertEqual(len(tasks), 1)
        with self.assertRaisesRegex(ValueError, "SHA256 mismatch"):
            MODULE.validate_task_artifact(payload, digest, "0" * 64)

        unsafe = task_payload()
        unsafe["validation_tasks"][0]["no_trade_signal"] = False
        raw, digest = payload_bytes(unsafe)
        with self.assertRaisesRegex(ValueError, "no_trade_signal"):
            MODULE.validate_task_artifact(unsafe, digest, digest)

    def test_review_assertions_and_task_limit_fail_closed(self):
        payload = task_payload()
        payload["validation_tasks"][0]["review_assertions"][
            "manual_review_required"
        ] = True
        raw, digest = payload_bytes(payload)
        with self.assertRaisesRegex(ValueError, "review_assertions"):
            MODULE.validate_task_artifact(payload, digest, digest)

        oversized = task_payload()
        template = oversized["validation_tasks"][0]
        oversized["validation_tasks"] = []
        for index in range(MODULE.MAX_TASKS_PER_RUN + 1):
            clone = json.loads(json.dumps(template))
            clone["task_id"] = f"TASK-{index:03d}"
            oversized["validation_tasks"].append(clone)
        oversized["stats"]["generated_tasks"] = len(
            oversized["validation_tasks"]
        )
        raw, digest = payload_bytes(oversized)
        with self.assertRaisesRegex(ValueError, "safety limit"):
            MODULE.validate_task_artifact(oversized, digest, digest)

    def test_missing_immutable_mapping_evidence_fails_closed(self):
        payload = task_payload()
        payload["validation_tasks"][0]["supporting_evidence_items"] = []
        raw, digest = payload_bytes(payload)
        with self.assertRaisesRegex(ValueError, "supporting_evidence_items"):
            MODULE.validate_task_artifact(payload, digest, digest)

    def test_tampered_mapping_evidence_fails_closed(self):
        payload = task_payload()
        payload["validation_tasks"][0]["evidence_items"] = [
            dict(payload["validation_tasks"][0]["supporting_evidence_items"][0])
        ]
        payload["validation_tasks"][0]["supporting_evidence_items"][0]["source_id"] = "tampered"
        raw, digest = payload_bytes(payload)
        with self.assertRaisesRegex(ValueError, "supporting evidence does not match"):
            MODULE.validate_task_artifact(payload, digest, digest)

    def test_missing_candidate_lineage_fails_closed(self):
        payload = task_payload()
        payload["validation_tasks"][0][
            "source_reviewed_candidate_sha256"
        ] = []
        raw, digest = payload_bytes(payload)
        with self.assertRaisesRegex(ValueError, "candidate_hashes"):
            MODULE.validate_task_artifact(payload, digest, digest)

    def test_validation_as_of_is_bound_and_cannot_be_overridden(self):
        payload = task_payload()
        _, digest = payload_bytes(payload)
        MODULE.validate_task_artifact(payload, digest, digest)
        self.assertEqual(
            MODULE.resolve_validation_as_of(payload), "2026-07-17"
        )
        self.assertEqual(
            MODULE.resolve_validation_as_of(payload, "2026-07-17"),
            "2026-07-17",
        )
        with self.assertRaisesRegex(ValueError, "must match"):
            MODULE.resolve_validation_as_of(payload, "2026-07-18")

        payload["validation_tasks"][0]["validation_as_of"] = "2026-07-18"
        raw, digest = payload_bytes(payload)
        with self.assertRaisesRegex(ValueError, "validation_as_of"):
            MODULE.validate_task_artifact(payload, digest, digest)

    def test_local_snapshot_uses_only_as_of_data(self):
        conn = duckdb.connect(self.db_path, read_only=True)
        snapshot, warnings = MODULE.collect_local_snapshot(
            conn, "000001.SZ", "2026-07-17"
        )
        conn.close()
        self.assertEqual(warnings, [])
        self.assertEqual(
            snapshot["latest_daily"]["trade_date"], "2026-07-17"
        )
        self.assertEqual(snapshot["latest_rps"]["rps_10"], 90)
        self.assertEqual(snapshot["latest_zeta"]["inst_buy"], 1)
        self.assertIsNotNone(
            snapshot["path_metrics"]["return_20d_pct"]
        )
        self.assertIsNotNone(
            snapshot["path_metrics"]["turnover_120d_percentile"]
        )

    def test_tushare_snapshot_filters_future_disclosures(self):
        api = FakeTushare()
        api.income = lambda **kwargs: pd.DataFrame([
            {
                "ts_code": "000001.SZ", "ann_date": "20260420",
                "f_ann_date": "20260720", "end_date": "20260630",
                "report_type": "1", "revenue": 9999.0,
                "n_income_attr_p": 999.0,
            },
            {
                "ts_code": "000001.SZ", "ann_date": "20260425",
                "f_ann_date": "20260425", "end_date": "20260331",
                "report_type": "1", "revenue": 1000.0,
                "n_income_attr_p": 100.0,
            },
        ])
        snapshot, warnings = MODULE.fetch_tushare_snapshot(
            api, "000001.SZ", "2026-07-17"
        )
        self.assertEqual(warnings, [])
        self.assertEqual(snapshot["valuation"]["trade_date"], "20260717")
        self.assertEqual(snapshot["valuation"]["pe_ttm"], 20)
        self.assertEqual(
            snapshot["latest_financial_indicator"]["ann_date"], "20260425"
        )
        self.assertEqual(snapshot["latest_income"]["end_date"], "20260331")
        self.assertEqual(
            snapshot["latest_income"]["availability_date"], "20260425"
        )
        self.assertEqual(
            snapshot["latest_financial_indicator"]["roe"], 12
        )
        self.assertEqual(
            snapshot["derived_financial_metrics"][
                "operating_cash_to_net_profit"
            ],
            0.8,
        )
        self.assertEqual(
            snapshot["derived_financial_metrics"]["inventory_to_assets"],
            0.1,
        )

    def test_question_coverage_does_not_invent_answers(self):
        self.assertEqual(
            MODULE.question_coverage("近20日股价是否过热？")[
                "coverage_status"
            ],
            "STRUCTURED_EVIDENCE_AVAILABLE",
        )
        self.assertEqual(
            MODULE.question_coverage("真实订单是否增长？")[
                "coverage_status"
            ],
            "EXTERNAL_EVIDENCE_REQUIRED",
        )
        self.assertEqual(
            MODULE.question_coverage("财务质量和客户认证是否支持逻辑？")[
                "coverage_status"
            ],
            "PARTIAL_STRUCTURED_SUPPORT",
        )

    def test_payload_is_read_only_and_has_no_trade_decision(self):
        payload = task_payload()
        raw, digest = payload_bytes(payload)
        tasks = MODULE.validate_task_artifact(payload, digest, digest)
        before = Path(self.db_path).stat()
        output = MODULE.build_payload(
            payload,
            Path("tasks.json"),
            digest,
            tasks,
            self.db_path,
            "2026-07-17",
            FakeTushare(),
        )
        after = Path(self.db_path).stat()
        self.assertEqual(output["stats"]["completed_snapshots"], 1)
        self.assertEqual(output["stats"]["database_writes"], 0)
        self.assertEqual(output["stats"]["trade_signals"], 0)
        result = output["results"][0]
        self.assertEqual(
            result["validation_status"], "STRUCTURED_SNAPSHOT_READY"
        )
        self.assertIsNone(result["decision_verdict"])
        self.assertIsNone(result["decision_score"])
        self.assertEqual(
            result["lineage"]["source_reviewed_candidate_ids"],
            ["ROW-1-REVIEWED"],
        )
        self.assertFalse(result["generate_trade"])
        self.assertTrue(result["no_trade_signal"])
        self.assertEqual(before.st_size, after.st_size)
        self.assertEqual(before.st_mtime_ns, after.st_mtime_ns)

    def test_markdown_preserves_zero_counts(self):
        payload = {
            "batch_id": "test",
            "as_of": "2026-07-17",
            "compass_source_as_of": "2026-07-10",
            "validation_as_of": "2026-07-17",
            "source_task_artifact_sha256": "1" * 64,
            "stats": {"trade_signals": 0, "database_writes": 0},
            "results": [],
            "warnings": [],
        }
        rendered = MODULE.render_markdown(payload)
        self.assertIn("| trade_signals | 0 |", rendered)
        self.assertIn("| database_writes | 0 |", rendered)

    def test_offline_mode_remains_usable_with_explicit_warning(self):
        payload = task_payload()
        raw, digest = payload_bytes(payload)
        tasks = MODULE.validate_task_artifact(payload, digest, digest)
        output = MODULE.build_payload(
            payload,
            Path("tasks.json"),
            digest,
            tasks,
            self.db_path,
            "2026-07-17",
            None,
        )
        result = output["results"][0]
        self.assertIsNone(result["tushare_snapshot"])
        self.assertEqual(
            result["validation_status"], "PARTIAL_STRUCTURED_SNAPSHOT"
        )
        self.assertIn(
            "tushare_disabled_or_unavailable", result["warnings"]
        )


if __name__ == "__main__":
    unittest.main()
