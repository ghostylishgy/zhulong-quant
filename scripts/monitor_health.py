#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Lightweight daemon health probe.
Checks freshness of ops_daemon_heartbeat.ts in DuckDB.
"""

import runpy
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

_current = Path(__file__).resolve()
PROJECT_ROOT = next(
    (p for p in _current.parents if (p / ".git").exists() or (p / "storage").exists()),
    _current.parents[1],
)
DB_PATH = str(PROJECT_ROOT / "storage" / "database" / "zhulong.duckdb")
STALE_THRESHOLD_MINUTES = 15

RED = "\033[1;31m"
RESET = "\033[0m"

_loader_ns = runpy.run_path(str(PROJECT_ROOT / "04_governance" / "lib" / "core" / "module_loader.py"))
load_attr_from_path = _loader_ns["load_attr_from_path"]
DBGateway = load_attr_from_path(
    "db_gateway_01_monitor_health",
    PROJECT_ROOT / "01_engine" / "lib" / "db_gateway.py",
    "DBGateway",
)


def _is_retryable_duckdb_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return ("locked" in msg) or ("busy" in msg) or ("conflict" in msg)


def with_duckdb_retry(fn, retries: int = 5, base_delay: float = 0.5):
    last_err = None
    for attempt in range(retries):
        try:
            return fn()
        except Exception as exc:
            if _is_retryable_duckdb_error(exc):
                last_err = exc
                delay = base_delay * (2 ** attempt) + random.uniform(0, 0.5)
                time.sleep(delay)
                continue
            raise
    if last_err is not None:
        raise last_err


def _normalize_ts(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value

    raw = str(value).strip()
    if not raw:
        return None

    raw = raw.replace("T", " ").replace("Z", "").strip()
    if "+" in raw:
        raw = raw.split("+", 1)[0].strip()

    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def fetch_last_heartbeat_ts():
    def _read():
        with DBGateway(DB_PATH, read_only=True) as conn:
            exists = conn.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = 'ops_daemon_heartbeat'
                LIMIT 1
                """
            ).fetchone()
            if not exists:
                return None
            row = conn.execute("SELECT MAX(ts) FROM ops_daemon_heartbeat").fetchone()
            return row[0] if row else None

    return with_duckdb_retry(_read, retries=6, base_delay=0.5)


def main() -> int:
    try:
        last_raw = fetch_last_heartbeat_ts()
    except Exception as exc:
        print(f"{RED}[CRITICAL] Daemon is DEAD or HANGING!{RESET} db_read_error={exc}")
        return 2

    last_ts = _normalize_ts(last_raw)
    if last_ts is None:
        print(f"{RED}[CRITICAL] Daemon is DEAD or HANGING!{RESET} heartbeat_missing")
        return 2

    now = datetime.now()
    lag = now - last_ts
    stale = lag > timedelta(minutes=STALE_THRESHOLD_MINUTES)

    if stale:
        print(
            f"{RED}[CRITICAL] Daemon is DEAD or HANGING!{RESET} "
            f"last_heartbeat={last_ts:%Y-%m-%d %H:%M:%S} "
            f"lag_minutes={lag.total_seconds() / 60:.1f}"
        )
        return 2

    print(
        f"[OK] Daemon heartbeat is healthy. "
        f"last_heartbeat={last_ts:%Y-%m-%d %H:%M:%S} "
        f"lag_seconds={lag.total_seconds():.1f}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
