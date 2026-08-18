#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Shared harvest state writer for manual/scheduled pipelines."""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENGINE_ROOT = PROJECT_ROOT / '01_engine'

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))
if str(ENGINE_ROOT) not in sys.path:
    sys.path.append(str(ENGINE_ROOT))

from config.settings import Config
from lib.db_gateway import DBGateway

DETAIL_MAX_LEN = 512


def ensure_ops_pipeline_state_table(logger: logging.Logger | None = None) -> None:
    with DBGateway(Config.DB_PATH, read_only=False, logger=logger) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ops_pipeline_state (
                trade_date DATE NOT NULL,
                phase TEXT NOT NULL,
                status TEXT,
                detail TEXT,
                updated_at TIMESTAMP DEFAULT now(),
                PRIMARY KEY (trade_date, phase)
            )
            """
        )
        conn.commit()


def _normalize_status(status: str) -> str:
    st = str(status or '').strip().upper()
    return st or 'IN_PROGRESS'


def _normalize_detail(detail: str) -> str:
    return str(detail or '').strip()[:DETAIL_MAX_LEN]


def sync_harvest_state(
    trade_date_iso: str,
    status: str,
    detail: str,
    logger: logging.Logger | None = None,
    retries: int = 3,
    base_delay: float = 0.4,
) -> None:
    ensure_ops_pipeline_state_table(logger=logger)

    status_norm = _normalize_status(status)
    detail_norm = _normalize_detail(detail)
    retry_total = max(1, int(retries))

    last_err = None
    for attempt in range(retry_total):
        try:
            with DBGateway(Config.DB_PATH, read_only=False, logger=logger) as conn:
                conn.execute(
                    """
                    INSERT INTO ops_pipeline_state (trade_date, phase, status, detail, updated_at)
                    VALUES (CAST(? AS DATE), 'phase_harvest', ?, ?, now())
                    ON CONFLICT (trade_date, phase) DO UPDATE SET
                        status = CASE
                            WHEN UPPER(COALESCE(excluded.status, '')) IN ('DONE', 'FAILED')
                                THEN excluded.status
                            WHEN UPPER(COALESCE(ops_pipeline_state.status, '')) IN ('DONE', 'FAILED')
                                THEN ops_pipeline_state.status
                            ELSE excluded.status
                        END,
                        detail = CASE
                            WHEN COALESCE(excluded.detail, '') = ''
                                THEN ops_pipeline_state.detail
                            WHEN COALESCE(ops_pipeline_state.detail, '') = ''
                                THEN SUBSTR(excluded.detail, 1, ?)
                            WHEN POSITION(excluded.detail IN COALESCE(ops_pipeline_state.detail, '')) > 0
                                THEN ops_pipeline_state.detail
                            ELSE SUBSTR(ops_pipeline_state.detail || ',' || excluded.detail, 1, ?)
                        END,
                        updated_at = now()
                    """,
                    [trade_date_iso, status_norm, detail_norm, DETAIL_MAX_LEN, DETAIL_MAX_LEN],
                )
                conn.commit()
            return
        except Exception as exc:
            last_err = exc
            if logger:
                logger.warning(
                    '[HARVEST_STATE] write retry %s/%s failed: %s',
                    attempt + 1,
                    retry_total,
                    exc,
                )
            if attempt < retry_total - 1:
                time.sleep(base_delay * (2 ** attempt))

    if last_err is not None:
        raise last_err
