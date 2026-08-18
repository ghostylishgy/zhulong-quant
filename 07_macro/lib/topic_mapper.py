#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
07_macro/lib/topic_mapper.py
Phase 3 concept mapping layer with low-frequency cache.
"""

from __future__ import annotations

import runpy
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Generator, Iterable, Optional

from config.settings import Config

logger = logging.getLogger("zhulong.macro.topic_mapper")

_current = Path(__file__).resolve()
PROJECT_ROOT = next(
    (p for p in _current.parents if (p / ".git").exists() or (p / "storage").exists()),
    _current.parents[2],
)
DEFAULT_DB_PATH = str(PROJECT_ROOT / "storage" / "database" / "zhulong.duckdb")


_loader_ns = runpy.run_path(str(PROJECT_ROOT / "04_governance" / "lib" / "core" / "module_loader.py"))
load_module_from_path = _loader_ns["load_module_from_path"]


def _load_module(path: Path, module_name: str):
    return load_module_from_path(module_name, path)


_dbgw_mod = _load_module(PROJECT_ROOT / "01_engine" / "lib" / "db_gateway.py", "db_gateway_macro_topic")
DBGateway = _dbgw_mod.DBGateway

_safe_mod = _load_module(
    PROJECT_ROOT / "04_governance" / "lib" / "core" / "safe_writer.py",
    "safe_writer_macro_topic",
)
duckdb_safe = _safe_mod.duckdb_safe
sanitize_row = _safe_mod.sanitize_row
normalize_date = _safe_mod.normalize_date

_tu_mod = _load_module(_current.parent / "tushare_client.py", "tushare_client_macro_topic")
MacroTushareClient = _tu_mod.MacroTushareClient


@dataclass
class MappingBuildResult:
    used_cache: bool = False
    refreshed: bool = False
    used_fallback: bool = False
    fallback_reason: str = ""
    topic_count: int = 0
    total_rows: int = 0
    industry_rows: int = 0
    concept_rows: int = 0
    stale_days: int = 0


class TopicMapper:
    """Build and serve topic-member mapping with 7-day low-frequency cache."""

    def __init__(
        self,
        db_path: Optional[str] = None,
        cache_ttl_days: int = 7,
        concept_delay_sec: float = 0.08,
    ):
        self.db_path = str(db_path or os.getenv("DB_PATH", "") or Config.DB_PATH or DEFAULT_DB_PATH)
        self.cache_ttl_days = max(1, int(cache_ttl_days))
        self.concept_delay_sec = max(0.0, float(concept_delay_sec))
        self.client = MacroTushareClient(timeout_sec=30, max_retries=4, base_delay=1.0, jitter_max=0.5)

    def ensure_mapping(self) -> MappingBuildResult:
        is_empty, stale_days = self._mapping_cache_status()
        need_refresh = is_empty or stale_days > self.cache_ttl_days

        if not need_refresh:
            topic_count, total_rows = self._current_topic_stats()
            return MappingBuildResult(
                used_cache=True,
                refreshed=False,
                used_fallback=False,
                topic_count=topic_count,
                total_rows=total_rows,
                stale_days=stale_days,
            )

        return self._refresh_mapping(stale_days=stale_days)

    def iter_active_mapping_rows(self, batch_size: int = 4000) -> Generator[Dict[str, str], None, None]:
        with DBGateway(self.db_path, read_only=True, logger=logger) as conn:
            cursor = conn.execute(
                """
                SELECT topic_type, topic_id, topic_name, symbol, ts_code, updated_at
                FROM dim_macro_topic_member
                WHERE is_active = 1
                """
            )
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                for row in rows:
                    updated_at = row[5]
                    yield {
                        "topic_type": str(row[0] or ""),
                        "topic_id": str(row[1] or ""),
                        "topic_name": str(row[2] or ""),
                        "symbol": str(row[3] or ""),
                        "ts_code": str(row[4] or ""),
                        "updated_at": self._to_ts_text(updated_at),
                    }

    def _refresh_mapping(self, stale_days: int) -> MappingBuildResult:
        now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        result = MappingBuildResult(refreshed=True, stale_days=stale_days)

        try:
            with duckdb_safe(self.db_path, read_only=False, retries=6, base_delay=0.5) as conn:
                conn.execute("DELETE FROM dim_macro_topic_member")

                result.industry_rows = self._upsert_industry_mapping(conn, now_text)
                logger.info("Industry fallback mapping loaded rows=%s", result.industry_rows)

                try:
                    result.concept_rows = self._upsert_concept_mapping(conn, now_text)
                    logger.info("Concept mapping loaded rows=%s", result.concept_rows)
                except Exception as concept_exc:
                    result.used_fallback = True
                    result.fallback_reason = str(concept_exc)
                    logger.error(
                        "Concept mapping failed, keep industry fallback only: %s",
                        concept_exc,
                        exc_info=True,
                    )

                conn.commit()

            result.total_rows = result.industry_rows + result.concept_rows
            result.topic_count, _ = self._current_topic_stats()

            if result.total_rows <= 0:
                raise RuntimeError("topic mapper produced zero rows")
            return result
        except Exception as exc:
            logger.error("Topic mapping refresh failed", exc_info=True)
            raise RuntimeError(f"Topic mapping refresh failed: {exc}") from exc

    def _upsert_industry_mapping(self, conn, updated_at: str) -> int:
        count = 0
        rows = conn.execute(
            """
            SELECT symbol, industry
            FROM fact_stock_basic
            WHERE COALESCE(TRIM(industry), '') <> ''
            """
        ).fetchall()

        for symbol, industry in rows:
            symbol_text = str(symbol or "").strip()
            industry_text = str(industry or "").strip()
            if not symbol_text or not industry_text:
                continue

            payload = {
                "topic_type": "industry",
                "topic_id": f"IND::{industry_text}",
                "topic_name": industry_text,
                "symbol": symbol_text,
                "ts_code": symbol_text,
                "source": "fact_stock_basic.industry",
                "is_active": 1,
                "updated_at": updated_at,
            }
            self._upsert_topic_member(conn, payload)
            count += 1

        return count

    def _upsert_concept_mapping(self, conn, updated_at: str) -> int:
        concepts_df = self.client.get_concept_list()
        if concepts_df is None or concepts_df.empty:
            raise RuntimeError("Tushare concept list is empty")

        total = 0
        for idx, concept in enumerate(concepts_df.itertuples(index=False), start=1):
            concept_id = str(getattr(concept, "code", "") or "").strip()
            concept_name = str(getattr(concept, "name", "") or "").strip()
            if not concept_id:
                continue

            detail_df = self.client.get_concept_detail(concept_id)
            if detail_df is None or detail_df.empty:
                if self.concept_delay_sec > 0:
                    time.sleep(self.concept_delay_sec)
                continue

            for detail in detail_df.itertuples(index=False):
                ts_code = str(getattr(detail, "ts_code", "") or "").strip()
                topic_name = str(getattr(detail, "concept_name", "") or concept_name or concept_id).strip()
                if not ts_code:
                    continue

                payload = {
                    "topic_type": "concept",
                    "topic_id": concept_id,
                    "topic_name": topic_name,
                    "symbol": ts_code,
                    "ts_code": ts_code,
                    "source": "tushare.concept_detail",
                    "is_active": 1,
                    "updated_at": updated_at,
                }
                self._upsert_topic_member(conn, payload)
                total += 1

            if idx % 50 == 0:
                logger.info("Concept detail progress: %s/%s", idx, len(concepts_df))
            if self.concept_delay_sec > 0:
                time.sleep(self.concept_delay_sec)

        if total <= 0:
            raise RuntimeError("Tushare concept detail returned zero rows")
        return total

    def _upsert_topic_member(self, conn, payload: Dict[str, object]) -> None:
        clean = sanitize_row(payload)
        conn.execute(
            """
            INSERT INTO dim_macro_topic_member (
                topic_type,
                topic_id,
                topic_name,
                symbol,
                ts_code,
                source,
                is_active,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, CAST(? AS TIMESTAMP))
            ON CONFLICT (topic_type, topic_id, symbol) DO UPDATE SET
                topic_name = excluded.topic_name,
                ts_code = excluded.ts_code,
                source = excluded.source,
                is_active = excluded.is_active,
                updated_at = excluded.updated_at
            """,
            [
                clean.get("topic_type"),
                clean.get("topic_id"),
                clean.get("topic_name"),
                clean.get("symbol"),
                clean.get("ts_code"),
                clean.get("source"),
                clean.get("is_active"),
                clean.get("updated_at"),
            ],
        )

    def _mapping_cache_status(self) -> tuple[bool, int]:
        try:
            with DBGateway(self.db_path, read_only=True, logger=logger) as conn:
                row = conn.execute(
                    "SELECT COUNT(*), MAX(updated_at) FROM dim_macro_topic_member"
                ).fetchone()
        except Exception:
            logger.warning("dim_macro_topic_member missing, force refresh")
            return True, 9999

        cnt = int((row or [0, None])[0] or 0)
        latest = (row or [0, None])[1]
        if cnt <= 0 or latest is None:
            return True, 9999

        if hasattr(latest, "replace"):
            latest_dt = latest
        else:
            latest_dt = datetime.fromisoformat(str(latest).replace("Z", ""))

        if getattr(latest_dt, "tzinfo", None) is not None:
            latest_dt = latest_dt.astimezone(timezone.utc).replace(tzinfo=None)
        now_local = datetime.utcnow()
        stale_days = max(0, (now_local - latest_dt).days)
        return False, stale_days

    def _current_topic_stats(self) -> tuple[int, int]:
        with DBGateway(self.db_path, read_only=True, logger=logger) as conn:
            topic_count = conn.execute(
                "SELECT COUNT(DISTINCT topic_type || '|' || topic_id) FROM dim_macro_topic_member WHERE is_active=1"
            ).fetchone()[0]
            total_rows = conn.execute(
                "SELECT COUNT(*) FROM dim_macro_topic_member WHERE is_active=1"
            ).fetchone()[0]
        return int(topic_count or 0), int(total_rows or 0)

    @staticmethod
    def _to_ts_text(value) -> str:
        if value is None:
            return ""
        if hasattr(value, "strftime"):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        normalized = normalize_date(value)
        if normalized:
            return f"{normalized} 00:00:00"
        return str(value)
