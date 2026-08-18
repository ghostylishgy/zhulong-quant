#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
03_tactics/tactic_signal_bus.py
Unified intraday tactic signal sink for Eagle/Owl/Echo.

Contract:
- Discovery modules write signals only.
- No paper BUY/SELL is executed here.
- Shadow can consume approved signals in a later, explicit receiver.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    DBGateway = importlib.import_module('01_engine.lib.db_gateway').DBGateway
except Exception:
    import runpy
    loader_ns = runpy.run_path(str(PROJECT_ROOT / '04_governance' / 'lib' / 'core' / 'module_loader.py'))
    DBGateway = loader_ns['load_attr_from_path'](
        'zhulong_db_gateway_tactic_signal_bus',
        PROJECT_ROOT / '01_engine' / 'lib' / 'db_gateway.py',
        'DBGateway',
    )

DB_PATH = str(PROJECT_ROOT / 'storage' / 'database' / 'zhulong.duckdb')
logger = logging.getLogger('zhulong.tactics.signal_bus')


_SIGNAL_COLUMNS = {
    'idempotency_key': 'VARCHAR PRIMARY KEY',
    'trade_date': 'DATE',
    'source': 'VARCHAR',
    'symbol': 'VARCHAR',
    'signal_type': 'VARCHAR',
    'verdict': 'VARCHAR',
    'score': 'DOUBLE DEFAULT 0',
    'price': 'DOUBLE DEFAULT 0',
    'reason': 'VARCHAR DEFAULT \'\'',
    'evidence_json': 'VARCHAR DEFAULT \'{}\'',
    'status': "VARCHAR DEFAULT 'NEW'",
    'expires_at': 'TIMESTAMP',
    'consumed_at': 'TIMESTAMP',
    'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
    'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
}


def _today() -> str:
    return datetime.now().strftime('%Y-%m-%d')


def _safe_json(payload: Optional[Dict[str, Any]]) -> str:
    try:
        return json.dumps(payload or {}, ensure_ascii=False, sort_keys=True)[:4000]
    except Exception:
        return '{}'


def _make_key(source: str, symbol: str, signal_type: str, trade_date: str, evidence: Dict[str, Any]) -> str:
    raw = '|'.join([
        str(source or '').upper(),
        str(symbol or '').upper(),
        str(signal_type or '').upper(),
        str(trade_date or ''),
        str(evidence.get('fingerprint') or evidence.get('trigger_time') or evidence.get('reason') or ''),
    ])
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()[:24]


def ensure_signal_table(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_intraday_tactic_signals (
            idempotency_key VARCHAR PRIMARY KEY,
            trade_date DATE,
            source VARCHAR,
            symbol VARCHAR,
            signal_type VARCHAR,
            verdict VARCHAR,
            score DOUBLE DEFAULT 0,
            price DOUBLE DEFAULT 0,
            reason VARCHAR DEFAULT '',
            evidence_json VARCHAR DEFAULT '{}',
            status VARCHAR DEFAULT 'NEW',
            expires_at TIMESTAMP,
            consumed_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    rows = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'fact_intraday_tactic_signals'
        """
    ).fetchall()
    existing = {str(r[0]).lower() for r in rows}
    for col, ddl in _SIGNAL_COLUMNS.items():
        if col not in existing:
            conn.execute(f'ALTER TABLE fact_intraday_tactic_signals ADD COLUMN {col} {ddl}')
            logger.info('[signal-bus] patch schema add column: %s', col)
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_intraday_tactic_source_symbol
        ON fact_intraday_tactic_signals(source, symbol, trade_date)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_intraday_tactic_status
        ON fact_intraday_tactic_signals(status, expires_at)
        """
    )


def record_tactic_signal(
    source: str,
    symbol: str,
    signal_type: str,
    verdict: str,
    score: float = 0.0,
    price: float = 0.0,
    reason: str = '',
    evidence: Optional[Dict[str, Any]] = None,
    trade_date: Optional[str] = None,
    ttl_minutes: int = 60,
    status: str = 'NEW',
    db_path: str = DB_PATH,
) -> bool:
    source_norm = str(source or '').strip().upper()
    symbol_norm = str(symbol or '').strip().upper()
    signal_norm = str(signal_type or '').strip().upper()
    verdict_norm = str(verdict or '').strip().upper()
    td = str(trade_date or _today())[:10]
    if not source_norm or not symbol_norm or not signal_norm:
        logger.warning('[signal-bus] skip invalid signal: source=%s symbol=%s type=%s', source, symbol, signal_type)
        return False

    ev = dict(evidence or {})
    ev.setdefault('source', source_norm)
    ev.setdefault('signal_type', signal_norm)
    key = _make_key(source_norm, symbol_norm, signal_norm, td, ev)
    expires_at = datetime.now() + timedelta(minutes=max(1, int(ttl_minutes or 60)))
    now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    with DBGateway(db_path, read_only=False, logger=logger) as conn:
        ensure_signal_table(conn)
        row = conn.execute(
            """
            INSERT INTO fact_intraday_tactic_signals (
                idempotency_key, trade_date, source, symbol, signal_type, verdict,
                score, price, reason, evidence_json, status, expires_at,
                created_at, updated_at
            )
            VALUES (?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS TIMESTAMP),
                    CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP))
            ON CONFLICT (idempotency_key) DO UPDATE SET
                verdict = EXCLUDED.verdict,
                score = EXCLUDED.score,
                price = EXCLUDED.price,
                reason = EXCLUDED.reason,
                evidence_json = EXCLUDED.evidence_json,
                status = CASE
                    WHEN fact_intraday_tactic_signals.status = 'CONSUMED' THEN fact_intraday_tactic_signals.status
                    ELSE EXCLUDED.status
                END,
                expires_at = EXCLUDED.expires_at,
                updated_at = EXCLUDED.updated_at
            RETURNING idempotency_key
            """,
            [
                key,
                td,
                source_norm,
                symbol_norm,
                signal_norm,
                verdict_norm,
                float(score or 0),
                float(price or 0),
                str(reason or '')[:500],
                _safe_json(ev),
                str(status or 'NEW')[:32],
                expires_at.strftime('%Y-%m-%d %H:%M:%S'),
                now_ts,
                now_ts,
            ],
        ).fetchone()
    ok = bool(row)
    if ok:
        logger.info('[signal-bus] %s %s %s verdict=%s score=%s', source_norm, symbol_norm, signal_norm, verdict_norm, score)
    return ok


__all__ = ['DB_PATH', 'ensure_signal_table', 'record_tactic_signal']
