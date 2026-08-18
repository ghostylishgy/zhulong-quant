#!/usr/bin/env python3

import importlib.util
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from tests import _test_log_isolation  # noqa: F401

ROOT = Path(__file__).resolve().parents[1]
os.environ["ZHULONG_SKIP_SINGLETON_LOCK"] = "1"
TEST_DAEMON_LOG = (
    Path(tempfile.gettempdir()) / f"zhulong_daemon_unittest_{os.getpid()}.log"
)
os.environ["ZHULONG_DAEMON_LOG_PATH"] = str(TEST_DAEMON_LOG)


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


DAEMON = load_module(
    "zhulong_test_snapshot_daemon",
    ROOT / "zhulong_daemon.py",
)
CLOUD = load_module(
    "zhulong_test_snapshot_cloud",
    ROOT / "tools" / "cloud_backup.py",
)


class DuckDBSnapshotLockTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.source = self.root / "source.duckdb"
        with duckdb.connect(str(self.source)) as conn:
            conn.execute("CREATE TABLE sample AS SELECT * FROM range(100)")
        self.real_copy2 = DAEMON.shutil.copy2

    def tearDown(self):
        self.tempdir.cleanup()

    def guarded_copy(self, source, destination):
        probe = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "import duckdb,sys;"
                    "c=duckdb.connect(sys.argv[1],read_only=False);"
                    "c.close()"
                ),
                str(source),
            ],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=10,
            check=False,
        )
        self.assertNotEqual(
            probe.returncode,
            0,
            "external writer unexpectedly opened source during snapshot copy",
        )
        return self.real_copy2(source, destination)

    @staticmethod
    def assert_snapshot_rows(path):
        with duckdb.connect(str(path), read_only=True) as conn:
            rows = conn.execute("SELECT COUNT(*) FROM sample").fetchone()[0]
        if rows != 100:
            raise AssertionError(f"snapshot row mismatch: {rows}")

    def test_daemon_snapshot_holds_cross_process_writer_lock(self):
        destination = self.root / "api.duckdb"
        with (
            patch.object(DAEMON, "DB_PATH", self.source),
            patch.object(DAEMON, "API_SNAPSHOT_DB_PATH", destination),
            patch.object(DAEMON.shutil, "copy2", side_effect=self.guarded_copy),
        ):
            self.assertTrue(DAEMON._refresh_api_readonly_snapshot("TEST"))
        self.assert_snapshot_rows(destination)

    def test_cloud_snapshot_holds_cross_process_writer_lock(self):
        destination = self.root / "cloud.duckdb"
        with (
            patch.object(CLOUD, "DB_PATH", self.source),
            patch.object(CLOUD, "DB_SNAPSHOT_PATH", destination),
            patch.object(CLOUD, "LOG_PATH", self.root / "cloud.log"),
            patch.object(CLOUD.shutil, "copy2", side_effect=self.guarded_copy),
        ):
            CLOUD._refresh_db_snapshot()
        self.assert_snapshot_rows(destination)

    def test_us_radar_snapshot_uses_sqlite_backup_and_integrity_check(self):
        source = self.root / "us_radar.sqlite3"
        destination = self.root / "us_radar_cloud.sqlite3"
        with sqlite3.connect(source) as conn:
            conn.execute("CREATE TABLE events (event_id TEXT PRIMARY KEY, title TEXT)")
            conn.execute("INSERT INTO events VALUES ('evt-1', 'sample')")
        with (
            patch.object(CLOUD, "US_RADAR_DB_PATH", source),
            patch.object(CLOUD, "US_RADAR_DB_SNAPSHOT_PATH", destination),
            patch.object(CLOUD, "LOG_PATH", self.root / "cloud.log"),
        ):
            self.assertEqual(CLOUD._refresh_us_radar_snapshot(), "READY")
        with sqlite3.connect(f"file:{destination}?mode=ro", uri=True) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0], 1)
            self.assertEqual(conn.execute("PRAGMA integrity_check").fetchone()[0], "ok")

    def _run_cloud_main(self, *, backup_error=None, upload_error=None):
        snapshot = self.root / "cloud.duckdb"
        snapshot.write_bytes(b"snapshot")
        repo = self.root / "restic"
        repo.mkdir()
        password = self.root / "restic-password"
        env = {
            "RESTIC_REPOSITORY": str(repo),
            "RESTIC_PASSWORD_FILE": str(password),
        }

        def run_backup(*_args, **_kwargs):
            if backup_error:
                raise RuntimeError(backup_error)
            return {
                "sources": [str(snapshot)],
                "snapshots": 1,
                "latest_snapshot_id": "snapshot-id",
            }

        def upload(*_args, **_kwargs):
            if upload_error:
                raise RuntimeError(upload_error)
            return "UPLOADED_VERIFIED"

        with (
            patch.object(CLOUD, "DB_SNAPSHOT_PATH", snapshot),
            patch.object(CLOUD, "LOG_PATH", self.root / "cloud-main.log"),
            patch.object(CLOUD, "_load_env", return_value=env),
            patch.object(CLOUD, "_ensure_password", return_value=False),
            patch.object(CLOUD, "_refresh_db_snapshot"),
            patch.object(CLOUD, "_refresh_us_radar_snapshot", return_value="READY"),
            patch.object(CLOUD, "_run_restic_backup", side_effect=run_backup),
            patch.object(CLOUD, "_upload_to_baidu", side_effect=upload),
            patch.object(CLOUD, "_cleanup_us_radar_snapshot", return_value="REMOVED"),
            patch.object(CLOUD, "_pushplus"),
        ):
            rc = CLOUD.main()
        return rc, snapshot

    def test_cloud_main_removes_snapshot_after_verified_backup(self):
        rc, snapshot = self._run_cloud_main()
        self.assertEqual(rc, 0)
        self.assertFalse(snapshot.exists())

    def test_cloud_main_preserves_snapshot_when_restic_backup_fails(self):
        rc, snapshot = self._run_cloud_main(backup_error="restic failed")
        self.assertEqual(rc, 2)
        self.assertTrue(snapshot.exists())

    def test_cloud_main_preserves_snapshot_when_upload_fails(self):
        rc, snapshot = self._run_cloud_main(upload_error="upload failed")
        self.assertEqual(rc, 2)
        self.assertTrue(snapshot.exists())

    def test_baidu_reported_path_verification_fails_closed_on_empty_parse(self):
        repo = self.root / "restic"
        repo.mkdir()
        with patch.object(CLOUD, "_baidu_assert_remote_exists") as remote_exists:
            missing = CLOUD._baidu_verify_reported_repo_paths(
                "/usr/local/bin/BaiduPCS-Go",
                {},
                repo,
                "/zhulong-quant/restic_repo",
                "upload completed without parseable repository paths",
            )
        self.assertEqual(len(missing), 1)
        self.assertIn("UPLOAD_OUTPUT_UNVERIFIABLE", missing[0])
        remote_exists.assert_not_called()

    def test_backup_sources_include_required_us_radar_runtime_assets(self):
        db_snapshot = self.root / "us_radar_cloud.sqlite3"
        env_local = self.root / ".env.local"
        reports = self.root / "reports"
        project_reports = self.root / "project-reports"
        db_snapshot.write_bytes(b"sqlite")
        env_local.write_text("TOKEN=test\n", encoding="utf-8")
        reports.mkdir()
        project_reports.mkdir()
        (reports / "daily.md").write_text("report\n", encoding="utf-8")
        with (
            patch.object(CLOUD, "US_RADAR_DB_SNAPSHOT_PATH", db_snapshot),
            patch.object(CLOUD, "US_RADAR_ENV_PATH", env_local),
            patch.object(CLOUD, "US_RADAR_REPORTS_DIR", reports),
            patch.object(CLOUD, "PROJECT_REPORTS_DIR", project_reports),
        ):
            sources = CLOUD._backup_sources({})
        self.assertIn(str(db_snapshot), sources)
        self.assertIn(str(env_local), sources)
        self.assertIn(str(reports), sources)
        self.assertIn(str(project_reports), sources)


if __name__ == "__main__":
    unittest.main()
