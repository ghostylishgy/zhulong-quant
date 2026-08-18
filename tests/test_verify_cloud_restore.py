import contextlib
import copy
import importlib.util
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "tools" / "verify_cloud_restore.py"
SPEC = importlib.util.spec_from_file_location("verify_cloud_restore", MODULE_PATH)
MOD = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules[SPEC.name] = MOD
SPEC.loader.exec_module(MOD)


class CloudRestoreVerificationTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.base = Path(self.tempdir.name)
        self.restore = self.base / "restored/root/quant_project"
        self.restore.mkdir(parents=True)
        self._build_assets()
        self._build_database()
        self._build_chroma()
        self.baseline = {
            "snapshot_id": "abc123",
            "tables": MOD.inspect_duckdb(
                self.restore / "storage/database/zhulong_cloud_backup.duckdb"
            )["tables"],
        }

    def tearDown(self):
        self.tempdir.cleanup()

    def _build_assets(self):
        values = {
            ".env": "SECRET_TOKEN=must-not-leak\n",
            "05_shadow/config/rules.yaml": "enabled: false\n",
            "config/config.yaml": "mode: test\n",
            "config/settings.py": "TEST = True\n",
            "README.md": "readme\n",
            "README.zh-CN.md": "readme zh\n",
            "devlog.md": "devlog\n",
        }
        for relative, content in values.items():
            path = self.restore / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")

    def _build_database(self):
        path = self.restore / "storage/database/zhulong_cloud_backup.duckdb"
        path.parent.mkdir(parents=True)
        with duckdb.connect(str(path)) as conn:
            conn.execute("CREATE TABLE fact_daily(symbol VARCHAR, trade_date DATE)")
            conn.execute("INSERT INTO fact_daily VALUES ('000001.SZ', '2026-07-30')")
            conn.execute("CREATE TABLE fact_stock_basic(symbol VARCHAR, updated_at TIMESTAMP)")
            conn.execute("INSERT INTO fact_stock_basic VALUES ('000001.SZ', '2026-07-30 08:00:00')")
            conn.execute("CREATE TABLE nexus_audits(task_id VARCHAR, trade_date VARCHAR)")
            conn.execute("INSERT INTO nexus_audits VALUES ('task', '2026-07-30')")
            conn.execute("CREATE TABLE fact_strategic_memory(id INTEGER, trade_date DATE)")
            conn.execute("INSERT INTO fact_strategic_memory VALUES (1, '2026-07-30')")
            conn.execute("CREATE TABLE fact_paper_positions(symbol VARCHAR, trade_date DATE)")
            conn.execute("INSERT INTO fact_paper_positions VALUES ('000001.SZ', '2026-07-30')")
            conn.execute("CREATE TABLE fact_trade_calendar(exchange VARCHAR, cal_date DATE)")
            conn.execute("INSERT INTO fact_trade_calendar VALUES ('SSE', '2026-07-30')")

    def _build_chroma(self):
        path = self.restore / "storage/chromadb/chroma.sqlite3"
        path.parent.mkdir(parents=True)
        with contextlib.closing(sqlite3.connect(path)) as conn:
            conn.execute("CREATE TABLE collections(id TEXT)")
            conn.execute("CREATE TABLE segments(id TEXT)")
            conn.execute("CREATE TABLE embeddings(id TEXT)")
            conn.execute("INSERT INTO collections VALUES ('c')")
            conn.execute("INSERT INTO segments VALUES ('s')")
            conn.executemany("INSERT INTO embeddings VALUES (?)", [("e1",), ("e2",)])
            conn.commit()

    def test_complete_restore_passes_without_exposing_secret(self):
        report = MOD.build_report(
            self.restore,
            production_root=self.base / "production",
            snapshot_id="abc123",
            snapshot_time="2026-07-31T06:30:11+08:00",
            repository_check="passed",
            cloud_retrieved=True,
            rto_seconds=123.4,
            active_recovery_seconds=45.6,
            password_source="offline_custody",
            offline_password_custody_verified=True,
            baseline=self.baseline,
        )
        self.assertEqual(report["overall_status"], "PASS")
        self.assertEqual(report["database"]["tables"]["fact_daily"]["row_count"], 1)
        self.assertEqual(report["chroma"]["embeddings_count"], 2)
        serialized = json.dumps(report, ensure_ascii=False)
        self.assertNotIn("must-not-leak", serialized)
        self.assertFalse(report["assets"]["secret_contents_read"])
        env_row = next(row for row in report["assets"]["files"] if row["path"] == ".env")
        self.assertNotIn("sha256", env_row)

    def test_production_root_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "must not be the production"):
            MOD.validate_isolated_root(self.restore, self.restore)

    def test_missing_key_table_fails_closed(self):
        db = self.restore / "storage/database/zhulong_cloud_backup.duckdb"
        with duckdb.connect(str(db)) as conn:
            conn.execute("DROP TABLE fact_trade_calendar")
        report = MOD.build_report(
            self.restore,
            production_root=self.base / "production",
            snapshot_id="abc123",
            snapshot_time="2026-07-31T06:30:11+08:00",
            repository_check="passed",
            cloud_retrieved=True,
            rto_seconds=123.4,
            active_recovery_seconds=45.6,
            password_source="offline_custody",
            offline_password_custody_verified=True,
            baseline=self.baseline,
        )
        self.assertEqual(report["overall_status"], "FAIL")
        self.assertFalse(report["technical_checks"]["key_tables_present"])

    def test_same_snapshot_baseline_must_match_exactly(self):
        baseline = copy.deepcopy(self.baseline)
        matching = MOD.build_report(
            self.restore,
            production_root=self.base / "production",
            snapshot_id="abc123",
            snapshot_time="2026-07-31T06:30:11+08:00",
            repository_check="passed",
            cloud_retrieved=True,
            rto_seconds=123.4,
            active_recovery_seconds=45.6,
            password_source="offline_custody",
            offline_password_custody_verified=True,
            baseline=baseline,
        )
        self.assertEqual(matching["overall_status"], "PASS")
        baseline["tables"]["fact_daily"]["row_count"] = 99
        mismatch = MOD.build_report(
            self.restore,
            production_root=self.base / "production",
            snapshot_id="abc123",
            snapshot_time="2026-07-31T06:30:11+08:00",
            repository_check="passed",
            cloud_retrieved=True,
            rto_seconds=123.4,
            active_recovery_seconds=45.6,
            password_source="offline_custody",
            offline_password_custody_verified=True,
            baseline=baseline,
        )
        self.assertEqual(mismatch["overall_status"], "FAIL")
        self.assertFalse(mismatch["technical_checks"]["baseline_key_tables_match"])

    def test_node_exported_password_keeps_dr_readiness_partial(self):
        report = MOD.build_report(
            self.restore,
            production_root=self.base / "production",
            snapshot_id="abc123",
            snapshot_time="2026-07-31T06:30:11+08:00",
            repository_check="passed",
            cloud_retrieved=True,
            rto_seconds=123.4,
            active_recovery_seconds=45.6,
            password_source="production_node_export",
            offline_password_custody_verified=False,
            baseline=self.baseline,
        )
        self.assertEqual(report["technical_restore_status"], "PASS")
        self.assertEqual(report["overall_status"], "PARTIAL_KEY_CUSTODY_OPEN")

    def test_missing_baseline_fails_closed(self):
        report = MOD.build_report(
            self.restore,
            production_root=self.base / "production",
            snapshot_id="abc123",
            snapshot_time="2026-07-31T06:30:11+08:00",
            repository_check="passed",
            cloud_retrieved=True,
            rto_seconds=123.4,
            active_recovery_seconds=45.6,
            password_source="offline_custody",
            offline_password_custody_verified=True,
        )
        self.assertEqual(report["overall_status"], "FAIL")
        self.assertFalse(report["technical_checks"]["same_snapshot_baseline_supplied"])


if __name__ == "__main__":
    unittest.main()
