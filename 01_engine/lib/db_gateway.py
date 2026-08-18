#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
01_engine/lib/db_gateway.py
DuckDB gateway with explicit read/write mode and strict error propagation.
"""

from __future__ import annotations

import importlib
import logging
import os
import random
import re
import sys
import threading
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class DBGatewayError(RuntimeError):
    """Base class for all DB gateway errors."""


class DBConnectionOpenError(DBGatewayError):
    """Raised when opening a DuckDB connection fails."""


class DBConnectionCloseError(DBGatewayError):
    """Raised when closing a DuckDB connection fails."""


class _RetryingConnection:
    """Proxy connection that retries lock-conflicted write statements."""

    def __init__(self, raw_conn, gateway: "DBGateway"):
        self._raw_conn = raw_conn
        self._gateway = gateway

    def execute(self, query, parameters=None):
        return self._gateway._execute_with_retry(self._raw_conn, query, parameters)

    def insert_dataframe(self, table_name: str, df, temp_view_name: str = "temp_df"):
        return self._gateway.insert_dataframe(
            self._raw_conn,
            table_name=table_name,
            df=df,
            temp_view_name=temp_view_name,
        )

    def __getattr__(self, item):
        return getattr(self._raw_conn, item)


class _LockedConnection:
    """Proxy connection that releases the process DB lock on close()."""

    def __init__(self, raw_conn, lock: threading.RLock):
        self._raw_conn = raw_conn
        self._lock = lock
        self._closed = False

    def close(self):
        try:
            return self._raw_conn.close()
        finally:
            if not self._closed:
                self._closed = True
                self._lock.release()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def __getattr__(self, item):
        return getattr(self._raw_conn, item)


class DBGateway:
    """
    Context-managed DuckDB gateway.

    Usage:
        with DBGateway("/path/to/db.duckdb", read_only=True) as conn:
            rows = conn.execute("SELECT 1").fetchall()
    """

    _instance_lock = threading.Lock()
    _instances: dict[tuple[str, bool], "DBGateway"] = {}
    _connection_lock = threading.RLock()

    _WAL_SET_STATEMENTS = (
        "SET wal_autocheckpoint='512MB';",
        "SET checkpoint_threshold='512MB';",
    )

    _WRITE_SQL_PREFIXES = {
        'INSERT',
        'UPDATE',
        'DELETE',
        'MERGE',
        'CREATE',
        'ALTER',
        'DROP',
        'TRUNCATE',
        'REPLACE',
        'COPY',
        'BEGIN',
        'COMMIT',
        'ROLLBACK',
        'CHECKPOINT',
        'VACUUM',
    }
    _LOCK_ERROR_HINTS = (
        'database is locked',
        'database is busy',
        'could not set lock',
        'write-lock',
        'write lock',
        'could not acquire',
        'different configuration',
        'existing connections',
        'conflicting lock',
        'transaction conflict',
        'conflict',
        'busy',
    )
    _MAX_WRITE_RETRIES = 5
    _BACKOFF_BASE_SECONDS = 0.20
    _BACKOFF_MAX_SECONDS = 2.00

    def __init__(
        self,
        db_path: str | Path,
        read_only: bool = True,
        logger=None,
        expected_hold_seconds: float | None = None,
    ):
        self.db_path = Path(db_path)
        self.read_only = read_only
        self.logger = logger
        self.expected_hold_seconds = self._resolve_expected_hold_seconds(expected_hold_seconds)
        self._conn: Optional[object] = None
        self._conn_lock_acquired = False
        self._opened_at = 0.0
        self._write_count = 0
        self._last_sql_head = ""

    @staticmethod
    def _connect(path: Path, read_only: bool):
        duckdb_mod = importlib.import_module("duckdb")
        connector = getattr(duckdb_mod, "connect")
        return connector(str(path), read_only=read_only)

    @staticmethod
    def _resolve_expected_hold_seconds(value: float | None) -> float:
        if value is not None:
            try:
                return max(0.1, float(value))
            except Exception:
                return 5.0
        raw = os.getenv('ZHULONG_DBGATEWAY_EXPECTED_HOLD_SECONDS')
        if raw:
            try:
                return max(0.1, float(raw))
            except Exception:
                pass
        argv_text = ' '.join(str(x).lower() for x in sys.argv)
        if any(token in argv_text for token in ('decision_engine', 'data_sync', 'evolver', 'evolution')):
            return 600.0
        return 5.0

    @staticmethod
    def _sql_head(query: object, limit: int = 96) -> str:
        if not isinstance(query, str):
            return type(query).__name__
        lines = [ln.strip() for ln in query.strip().splitlines() if ln.strip()]
        head = ' '.join(lines)[:limit]
        return head or 'EMPTY_SQL'

    def _log_warning(self, message: str, *args) -> None:
        target = self.logger or logger
        target.warning(message, *args)

    def _mark_sql(self, query: object, is_write: bool) -> None:
        self._last_sql_head = self._sql_head(query)
        if is_write:
            self._write_count += 1

    def _connect_with_retry(self, read_only: bool):
        last_exc: Exception | None = None
        for attempt in range(1, self._MAX_WRITE_RETRIES + 1):
            try:
                return self._connect(self.db_path, read_only)
            except Exception as exc:
                last_exc = exc
                if not self._is_lock_conflict_error(exc) or attempt >= self._MAX_WRITE_RETRIES:
                    raise
                sleep_seconds = min(
                    self._BACKOFF_MAX_SECONDS,
                    self._BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)) + random.uniform(0.0, 0.15),
                )
                if self.logger:
                    mode_tag = 'RO' if read_only else 'RW'
                    self.logger.warning(
                        '[DBGateway] connect conflict (%s), retry %s/%s after %.3fs: %s',
                        mode_tag,
                        attempt,
                        self._MAX_WRITE_RETRIES,
                        sleep_seconds,
                        exc,
                    )
                time.sleep(sleep_seconds)
        if last_exc is not None:
            raise last_exc
        raise RuntimeError('DBGateway connect retry exhausted without exception details')

    @classmethod
    def get_instance(
        cls,
        db_path: str | Path,
        read_only: bool = True,
        logger=None,
    ) -> "DBGateway":
        """Compatibility bridge for legacy callers expecting singleton access."""
        path = str(Path(db_path))
        key = (path, bool(read_only))
        with cls._instance_lock:
            inst = cls._instances.get(key)
            if inst is None:
                inst = cls(db_path=path, read_only=read_only, logger=logger)
                cls._instances[key] = inst
            elif logger is not None:
                inst.logger = logger
        return inst

    def get_connection(self, read_only: Optional[bool] = None):
        """
        Return a raw duckdb connection for legacy callsites.
        Caller is responsible for closing the returned connection.
        """
        mode = self.read_only if read_only is None else bool(read_only)
        if mode and not self.db_path.exists():
            raise DBConnectionOpenError(
                f"DuckDB read-only open blocked: database file not found ({self.db_path})"
            )

        wait_start = time.perf_counter()
        self._connection_lock.acquire()
        wait_elapsed = time.perf_counter() - wait_start
        if wait_elapsed > 1.0:
            self._log_warning(
                '[DBGateway] raw connection lock wait %.3fs mode=%s path=%s',
                wait_elapsed, 'RO' if mode else 'RW', self.db_path,
            )
        try:
            conn = self._connect_with_retry(mode)
            if not mode:
                journal_mode_supported = False
                for statement in self._WAL_SET_STATEMENTS:
                    conn.execute(statement)
                if self.logger:
                    self.logger.info(
                        "[DBGateway] raw connection WAL policy tuned "
                        "(journal_mode_pragma_supported=%s)",
                        journal_mode_supported,
                    )
        except Exception:
            self._connection_lock.release()
            raise

        if self.logger:
            mode_tag = "RO" if mode else "RW"
            self.logger.info("[DBGateway] raw connected (%s) -> %s", mode_tag, self.db_path)
        wrapped = conn if mode else _RetryingConnection(conn, self)
        return _LockedConnection(wrapped, self._connection_lock)

    def _enforce_wal_policy(self) -> None:
        if self._conn is None:
            return
        # DuckDB path: skip SQLite-only journal_mode probe to avoid noisy warnings.
        journal_mode_supported = False

        for statement in self._WAL_SET_STATEMENTS:
            self._conn.execute(statement)

        if self.logger:
            self.logger.info(
                "[DBGateway] DuckDB WAL policy tuned "
                "(wal_autocheckpoint=512MB, checkpoint_threshold=512MB, "
                "journal_mode_pragma_supported=%s)",
                journal_mode_supported,
            )

    @classmethod
    def _is_lock_conflict_error(cls, exc: Exception) -> bool:
        msg = str(exc or '').lower()
        return any(token in msg for token in cls._LOCK_ERROR_HINTS)

    @classmethod
    def _looks_like_write_sql(cls, query: object) -> bool:
        if not isinstance(query, str):
            return False

        statement = query.strip()
        if not statement:
            return False

        lines = [ln.strip() for ln in statement.splitlines() if ln.strip() and not ln.strip().startswith('--')]
        if not lines:
            return False

        head = lines[0].upper()
        if head.startswith('WITH '):
            normalized = re.sub(r'\s+', ' ', statement).upper()
            return bool(re.search(
                r'\)\s*(INSERT|UPDATE|DELETE|MERGE)\b',
                normalized,
            ))

        first_token = head.split(None, 1)[0].rstrip(';')
        return first_token in cls._WRITE_SQL_PREFIXES

    def _execute_with_retry(self, conn, query, parameters=None):
        is_write = self._looks_like_write_sql(query)
        self._mark_sql(query, is_write)
        if not is_write:
            if parameters is None:
                return conn.execute(query)
            return conn.execute(query, parameters)

        last_exc: Exception | None = None
        for attempt in range(1, self._MAX_WRITE_RETRIES + 1):
            try:
                if parameters is None:
                    return conn.execute(query)
                return conn.execute(query, parameters)
            except Exception as exc:
                last_exc = exc
                if not self._is_lock_conflict_error(exc) or attempt >= self._MAX_WRITE_RETRIES:
                    raise
                sleep_seconds = min(
                    self._BACKOFF_MAX_SECONDS,
                    self._BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)) + random.uniform(0.0, 0.15),
                )
                if self.logger:
                    self.logger.warning(
                        '[DBGateway] write lock conflict, retry %s/%s after %.3fs: %s',
                        attempt,
                        self._MAX_WRITE_RETRIES,
                        sleep_seconds,
                        exc,
                    )
                time.sleep(sleep_seconds)

        if last_exc is not None:
            raise last_exc
        raise RuntimeError('DBGateway write retry exhausted without exception details')

    @staticmethod
    def _normalize_view_name(temp_view_name: str) -> str:
        raw = str(temp_view_name or 'temp_df').strip()
        if not raw:
            raw = 'temp_df'
        normalized = ''.join(ch if (ch.isalnum() or ch == '_') else '_' for ch in raw)
        if not normalized:
            normalized = 'temp_df'
        if normalized[0].isdigit():
            normalized = f'v_{normalized}'
        return normalized

    @staticmethod
    def _normalize_table_name(table_name: str) -> str:
        raw = str(table_name or '').strip()
        if not raw:
            raise ValueError('table_name is required')
        parts = raw.split('.')
        if any(not part for part in parts):
            raise ValueError(f'invalid table_name: {table_name}')
        for part in parts:
            if not (part[0].isalpha() or part[0] == '_'):
                raise ValueError(f'invalid table_name: {table_name}')
            if not all(ch.isalnum() or ch == '_' for ch in part):
                raise ValueError(f'invalid table_name: {table_name}')
        return '.'.join(parts)

    def insert_dataframe(self, conn, table_name: str, df, temp_view_name: str = "temp_df") -> int:
        """
        Register a dataframe as a temporary DuckDB view and insert into table.

        This method centralizes register/unregister lifecycle and reuses
        DBGateway write-lock retry logic through _execute_with_retry.
        """
        if conn is None:
            raise ValueError('connection is required')
        if df is None:
            raise ValueError('df is required')

        target_table = self._normalize_table_name(table_name)
        base_view = self._normalize_view_name(temp_view_name)
        unique_view = f'{base_view}_{int(time.time() * 1000)}_{random.randint(1000, 9999)}'
        quoted_view = f'"{unique_view}"'
        insert_sql = f'INSERT INTO {target_table} SELECT * FROM {quoted_view}'

        row_count = -1
        try:
            row_count = int(len(df))
        except Exception:
            row_count = -1

        registered = False
        try:
            conn.register(unique_view, df)
            registered = True
            self._execute_with_retry(conn, insert_sql)
            return row_count
        finally:
            if registered:
                try:
                    conn.unregister(unique_view)
                except Exception as exc:
                    if self.logger:
                        self.logger.warning(
                            '[DBGateway] unregister temp view failed: %s (%s)',
                            unique_view,
                            exc,
                        )

    def __enter__(self):
        if self.read_only and not self.db_path.exists():
            raise DBConnectionOpenError(
                f"DuckDB read-only open blocked: database file not found ({self.db_path})"
            )

        wait_start = time.perf_counter()
        self._connection_lock.acquire()
        wait_elapsed = time.perf_counter() - wait_start
        if wait_elapsed > 1.0:
            self._log_warning(
                '[DBGateway] connection lock wait %.3fs mode=%s path=%s',
                wait_elapsed, 'RO' if self.read_only else 'RW', self.db_path,
            )
        self._conn_lock_acquired = True
        self._opened_at = time.perf_counter()
        self._write_count = 0
        self._last_sql_head = ''
        try:
            self._conn = self._connect_with_retry(self.read_only)
            if not self.read_only:
                self._enforce_wal_policy()
            if self.logger:
                mode = "RO" if self.read_only else "RW"
                self.logger.info(f"[DBGateway] connected ({mode}) -> {self.db_path}")
            return self._conn if self.read_only else _RetryingConnection(self._conn, self)
        except Exception as exc:
            if self._conn_lock_acquired:
                self._connection_lock.release()
                self._conn_lock_acquired = False
            raise DBConnectionOpenError(
                f"DuckDB connect failed (read_only={self.read_only}, path={self.db_path}): {exc}"
            ) from exc

    def __exit__(self, exc_type, exc, tb):
        if self._conn is None:
            return False

        hold_elapsed = time.perf_counter() - self._opened_at if self._opened_at else 0.0
        try:
            self._conn.close()
            if self.logger:
                self.logger.info(f"[DBGateway] closed -> {self.db_path}")
            if hold_elapsed > self.expected_hold_seconds:
                self._log_warning(
                    '[DBGateway] long hold %.3fs > %.3fs mode=%s writes=%s last_sql=%s path=%s',
                    hold_elapsed, self.expected_hold_seconds,
                    'RO' if self.read_only else 'RW', self._write_count, self._last_sql_head or 'NONE', self.db_path,
                )
            if (not self.read_only) and self._write_count == 0:
                self._log_warning(
                    '[DBGateway] pseudo-write connection: RW opened but no write SQL executed; last_sql=%s path=%s',
                    self._last_sql_head or 'NONE', self.db_path,
                )
        except Exception as close_exc:
            raise DBConnectionCloseError(
                f"DuckDB close failed (path={self.db_path}): {close_exc}"
            ) from close_exc
        finally:
            self._conn = None
            if self._conn_lock_acquired:
                self._connection_lock.release()
                self._conn_lock_acquired = False
        return False
