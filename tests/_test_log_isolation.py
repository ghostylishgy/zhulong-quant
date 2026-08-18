"""Redirect import-time runtime logging for unit tests."""

import os
import tempfile
from pathlib import Path


TEST_LOG_DIR = Path(tempfile.gettempdir()) / f"zhulong_unittest_logs_{os.getpid()}"
TEST_LOG_DIR.mkdir(parents=True, exist_ok=True)

os.environ.setdefault("ZHULONG_DAEMON_LOG_PATH", str(TEST_LOG_DIR / "daemon.log"))
os.environ.setdefault("ZHULONG_NEXUS_LOG_PATH", str(TEST_LOG_DIR / "nexus.log"))
os.environ.setdefault(
    "ZHULONG_GOVERNANCE_LOG_PATH",
    str(TEST_LOG_DIR / "governance.log"),
)
