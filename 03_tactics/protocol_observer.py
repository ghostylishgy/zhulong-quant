#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unified side-channel observer for intraday protocols.

This module is intentionally non-trading:
- it does not write Shadow pending signals;
- it does not update paper positions or cash;
- it does not write RAG memory.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import logging
import statistics
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    DBGateway = importlib.import_module('01_engine.lib.db_gateway').DBGateway
except Exception:
    import runpy
    loader_ns = runpy.run_path(str(PROJECT_ROOT / '04_governance' / 'lib' / 'core' / 'module_loader.py'))
    DBGateway = loader_ns['load_attr_from_path'](
        'zhulong_db_gateway_protocol_observer',
        PROJECT_ROOT / '01_engine' / 'lib' / 'db_gateway.py',
        'DBGateway',
    )

from config.settings import Config, ensure_syspath  # noqa: E402

ensure_syspath()

DB_PATH = str(PROJECT_ROOT / 'storage' / 'database' / 'zhulong.duckdb')
logger = logging.getLogger('zhulong.tactics.protocol_observer')

PROTOCOL_EAGLE = 'EAGLE'
PROTOCOL_OWL = 'OWL'
PROTOCOL_RABBIT = 'RABBIT'

TRIGGER_EAGLE_PULSE = 'EAGLE_PULSE_AUDIT'
TRIGGER_OWL_EOD = 'OWL_EOD_MOMENTUM'
TRIGGER_RABBIT_ANCHOR = 'RABBIT_ANCHOR_LIMIT'

SCAN_STATUSES = {
    'SCAN_EMPTY',
    'SCAN_ERROR',
    'SCAN_DISABLED_NO_MINUTE_BAR',
    'SCAN_NO_CANDIDATE',
    'SCAN_NO_REALTIME',
    'SCAN_SKIPPED',
    'SCAN_TRIGGERED',
}


def _now_ts() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def _trade_date(value: Any = None) -> str:
    if value is None or str(value).strip() == '':
        return datetime.now().strftime('%Y-%m-%d')
    text = str(value).strip()
    if len(text) >= 10 and text[4] == '-' and text[7] == '-':
        return text[:10]
    if len(text) == 8 and text.isdigit():
        return f'{text[:4]}-{text[4:6]}-{text[6:8]}'
    return text[:10]


def _event_time(trade_date: str, event_time: Any = None) -> str:
    if event_time is None or str(event_time).strip() == '':
        return _now_ts()
    text = str(event_time).strip()
    if len(text) >= 19 and text[4] == '-' and text[7] == '-':
        return text[:19]
    if len(text) >= 8 and text[2] == ':' and text[5] == ':':
        return f'{trade_date} {text[:8]}'
    if len(text) >= 5 and text[2] == ':':
        return f'{trade_date} {text[:5]}:00'
    return _now_ts()


def _safe_json(payload: Optional[Dict[str, Any]]) -> str:
    try:
        return json.dumps(payload or {}, ensure_ascii=False, sort_keys=True, default=str)[:8000]
    except Exception:
        return '{}'


def _event_id(protocol: str, symbol: str, trigger_type: str, event_time: str, evidence: Dict[str, Any]) -> str:
    fp = str(evidence.get('fingerprint') or evidence.get('trigger_time') or evidence.get('reason') or '')
    raw = '|'.join([protocol.upper(), symbol.upper(), trigger_type.upper(), event_time[:16], fp])
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()[:28]


def ensure_protocol_event_table(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_intraday_protocol_events (
            event_id VARCHAR PRIMARY KEY,
            trade_date DATE NOT NULL,
            protocol VARCHAR NOT NULL,
            symbol VARCHAR NOT NULL,
            name VARCHAR DEFAULT '',
            event_time TIMESTAMP,
            trigger_price DOUBLE DEFAULT 0,
            trigger_type VARCHAR DEFAULT '',
            score DOUBLE DEFAULT 0,
            verdict VARCHAR DEFAULT '',
            evidence_json VARCHAR DEFAULT '{}',
            is_trade_candidate INTEGER DEFAULT 0,
            execution_enabled INTEGER DEFAULT 0,
            consumed_by_shadow INTEGER DEFAULT 0,
            status VARCHAR DEFAULT 'OBSERVED',
            review_status VARCHAR DEFAULT 'PENDING',
            next_trade_date DATE,
            next_close DOUBLE,
            next_return DOUBLE,
            return_3d DOUBLE,
            return_5d DOUBLE,
            max_drawdown_5d DOUBLE,
            reviewed_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    rows = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'fact_intraday_protocol_events'
        """
    ).fetchall()
    existing = {str(r[0]).lower() for r in rows}
    required = {
        'execution_enabled': 'INTEGER DEFAULT 0',
        'consumed_by_shadow': 'INTEGER DEFAULT 0',
        'review_status': "VARCHAR DEFAULT 'PENDING'",
        'next_trade_date': 'DATE',
        'next_close': 'DOUBLE',
        'next_return': 'DOUBLE',
        'return_3d': 'DOUBLE',
        'return_5d': 'DOUBLE',
        'max_drawdown_5d': 'DOUBLE',
        'reviewed_at': 'TIMESTAMP',
    }
    for col, ddl in required.items():
        if col not in existing:
            conn.execute(f'ALTER TABLE fact_intraday_protocol_events ADD COLUMN {col} {ddl}')
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_intraday_protocol_date
        ON fact_intraday_protocol_events(trade_date, protocol)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_intraday_protocol_review
        ON fact_intraday_protocol_events(review_status, trade_date)
        """
    )


def lookup_stock_name(symbol: str) -> str:
    sym = str(symbol or '').strip().upper()
    if not sym:
        return ''
    try:
        with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
            row = conn.execute(
                "SELECT name FROM fact_stock_basic WHERE symbol = ? LIMIT 1",
                [sym],
            ).fetchone()
        if row and row[0]:
            return str(row[0]).strip()
    except Exception as exc:
        logger.debug('[protocol-observer] stock name lookup failed %s: %s', sym, exc)
    return ''


def record_protocol_event(
    *,
    protocol: str,
    symbol: str,
    trigger_type: str,
    trigger_price: float,
    score: float = 0.0,
    verdict: str = '',
    evidence: Optional[Dict[str, Any]] = None,
    name: str = '',
    trade_date: Any = None,
    event_time: Any = None,
    is_trade_candidate: bool = False,
    status: str = 'OBSERVED',
    execution_enabled: bool = False,
) -> bool:
    protocol_norm = str(protocol or '').strip().upper()
    symbol_norm = str(symbol or '').strip().upper()
    trigger_norm = str(trigger_type or '').strip().upper()
    if not protocol_norm or not symbol_norm or not trigger_norm:
        logger.warning('[protocol-observer] invalid event: protocol=%s symbol=%s trigger=%s', protocol, symbol, trigger_type)
        return False

    td = _trade_date(trade_date)
    ts = _event_time(td, event_time)
    ev = dict(evidence or {})
    ev.setdefault('protocol', protocol_norm)
    ev.setdefault('trigger_type', trigger_norm)
    event_id = _event_id(protocol_norm, symbol_norm, trigger_norm, ts, ev)
    display_name = str(name or '').strip() or lookup_stock_name(symbol_norm)
    now_ts = _now_ts()

    with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
        ensure_protocol_event_table(conn)
        row = conn.execute(
            """
            INSERT INTO fact_intraday_protocol_events (
                event_id, trade_date, protocol, symbol, name, event_time,
                trigger_price, trigger_type, score, verdict, evidence_json,
                is_trade_candidate, execution_enabled, consumed_by_shadow,
                status, review_status, created_at, updated_at
            )
            VALUES (?, CAST(? AS DATE), ?, ?, ?, CAST(? AS TIMESTAMP),
                    ?, ?, ?, ?, ?, ?, ?, 0, ?, 'PENDING',
                    CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP))
            ON CONFLICT (event_id) DO UPDATE SET
                name = EXCLUDED.name,
                trigger_price = EXCLUDED.trigger_price,
                score = EXCLUDED.score,
                verdict = EXCLUDED.verdict,
                evidence_json = EXCLUDED.evidence_json,
                is_trade_candidate = EXCLUDED.is_trade_candidate,
                execution_enabled = EXCLUDED.execution_enabled,
                status = EXCLUDED.status,
                updated_at = EXCLUDED.updated_at
            RETURNING event_id
            """,
            [
                event_id,
                td,
                protocol_norm,
                symbol_norm,
                display_name,
                ts,
                float(trigger_price or 0),
                trigger_norm,
                float(score or 0),
                str(verdict or '').strip().upper(),
                _safe_json(ev),
                1 if is_trade_candidate else 0,
                1 if execution_enabled else 0,
                str(status or 'OBSERVED')[:32],
                now_ts,
                now_ts,
            ],
        ).fetchone()
    ok = bool(row)
    if ok:
        logger.info('[protocol-observer] %s %s %s verdict=%s score=%s', protocol_norm, symbol_norm, trigger_norm, verdict, score)
    return ok


def record_protocol_scan(
    *,
    protocol: str,
    trigger_type: str,
    scanned_count: int = 0,
    triggered_count: int = 0,
    status: str = 'SCAN_EMPTY',
    evidence: Optional[Dict[str, Any]] = None,
    trade_date: Any = None,
    event_time: Any = None,
) -> bool:
    """Persist a non-trading scan summary so empty Owl/Anchor runs are auditable."""
    protocol_norm = str(protocol or '').strip().upper()
    trigger_norm = str(trigger_type or '').strip().upper()
    status_norm = str(status or 'SCAN_EMPTY').strip().upper()
    if status_norm not in SCAN_STATUSES:
        status_norm = 'SCAN_EMPTY'
    ev = dict(evidence or {})
    ev.update(
        {
            'scan_summary': True,
            'scanned_count': int(scanned_count or 0),
            'triggered_count': int(triggered_count or 0),
            'observer_policy': 'side_channel_no_shadow_no_rag',
            'fingerprint': (
                f"scan|{protocol_norm}|{trigger_norm}|{status_norm}|"
                f"{int(scanned_count or 0)}|{int(triggered_count or 0)}"
            ),
        }
    )
    return record_protocol_event(
        protocol=protocol_norm,
        symbol=f'__{protocol_norm}_SCAN__',
        trigger_type=trigger_norm,
        trigger_price=0.0,
        score=0.0,
        verdict=status_norm,
        evidence=ev,
        name=f'{protocol_norm}扫描摘要',
        trade_date=trade_date,
        event_time=event_time,
        is_trade_candidate=False,
        status=status_norm,
        execution_enabled=False,
    )


def _previous_week(today: Optional[date] = None) -> Tuple[str, str]:
    current = today or datetime.now().date()
    this_monday = current - timedelta(days=current.weekday())
    start = this_monday - timedelta(days=7)
    end = this_monday - timedelta(days=1)
    return start.isoformat(), end.isoformat()


def _future_bars(conn, symbol: str, trade_date: str, limit: int = 5) -> list:
    return conn.execute(
        """
        SELECT CAST(trade_date AS VARCHAR) AS trade_date,
               COALESCE(close, 0) AS close,
               COALESCE(low, close, 0) AS low
        FROM fact_daily
        WHERE symbol = ?
          AND CAST(trade_date AS DATE) > CAST(? AS DATE)
          AND COALESCE(close, 0) > 0
        ORDER BY trade_date
        LIMIT ?
        """,
        [symbol, trade_date, limit],
    ).fetchall()


def refresh_event_outcomes(start_date: str, end_date: str) -> Dict[str, int]:
    stats = {'events': 0, 'reviewed': 0, 'wait_close_data': 0, 'no_trigger_price': 0}
    now_ts = _now_ts()
    with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
        ensure_protocol_event_table(conn)
        rows = conn.execute(
            """
            SELECT event_id, symbol, CAST(trade_date AS VARCHAR), COALESCE(trigger_price, 0)
            FROM fact_intraday_protocol_events
            WHERE CAST(trade_date AS DATE) >= CAST(? AS DATE)
              AND CAST(trade_date AS DATE) <= CAST(? AS DATE)
              AND symbol NOT LIKE '__%_SCAN__'
              AND UPPER(COALESCE(status, '')) NOT IN (
                  'SCAN_EMPTY', 'SCAN_ERROR', 'SCAN_NO_CANDIDATE',
                  'SCAN_SKIPPED', 'SCAN_TRIGGERED'
              )
            ORDER BY trade_date, protocol, symbol
            """,
            [start_date, end_date],
        ).fetchall()
        stats['events'] = len(rows)
        for event_id, symbol, td, trigger_price_raw in rows:
            trigger_price = float(trigger_price_raw or 0)
            if trigger_price <= 0:
                conn.execute(
                    """
                    UPDATE fact_intraday_protocol_events
                    SET review_status = 'NO_TRIGGER_PRICE',
                        reviewed_at = CAST(? AS TIMESTAMP),
                        updated_at = CAST(? AS TIMESTAMP)
                    WHERE event_id = ?
                    """,
                    [now_ts, now_ts, event_id],
                )
                stats['no_trigger_price'] += 1
                continue

            bars = _future_bars(conn, str(symbol), str(td), 5)
            if not bars:
                conn.execute(
                    """
                    UPDATE fact_intraday_protocol_events
                    SET review_status = 'WAIT_CLOSE_DATA',
                        reviewed_at = CAST(? AS TIMESTAMP),
                        updated_at = CAST(? AS TIMESTAMP)
                    WHERE event_id = ?
                    """,
                    [now_ts, now_ts, event_id],
                )
                stats['wait_close_data'] += 1
                continue

            closes = [float(r[1] or 0) for r in bars]
            lows = [float(r[2] or 0) for r in bars]
            next_return = round((closes[0] - trigger_price) / trigger_price, 6) if len(closes) >= 1 else None
            return_3d = round((closes[2] - trigger_price) / trigger_price, 6) if len(closes) >= 3 else None
            return_5d = round((closes[4] - trigger_price) / trigger_price, 6) if len(closes) >= 5 else None
            max_drawdown = round((min(lows) - trigger_price) / trigger_price, 6) if lows else None
            conn.execute(
                """
                UPDATE fact_intraday_protocol_events
                SET review_status = 'REVIEWED',
                    next_trade_date = CAST(? AS DATE),
                    next_close = ?,
                    next_return = ?,
                    return_3d = ?,
                    return_5d = ?,
                    max_drawdown_5d = ?,
                    reviewed_at = CAST(? AS TIMESTAMP),
                    updated_at = CAST(? AS TIMESTAMP)
                WHERE event_id = ?
                """,
                [
                    str(bars[0][0])[:10],
                    closes[0],
                    next_return,
                    return_3d,
                    return_5d,
                    max_drawdown,
                    now_ts,
                    now_ts,
                    event_id,
                ],
            )
            stats['reviewed'] += 1
    return stats


def _fmt_pct(value: Any) -> str:
    try:
        if value is None:
            return '--'
        return f'{float(value) * 100:+.2f}%'
    except Exception:
        return '--'


def _avg(values: Iterable[float]) -> Optional[float]:
    vals = [float(v) for v in values if v is not None]
    return statistics.mean(vals) if vals else None


def _is_scan_summary_row(row: tuple) -> bool:
    try:
        symbol = str(row[1] or '').upper()
        status = str(row[12] or '').upper() if len(row) > 12 else ''
        return symbol.startswith('__') or status in SCAN_STATUSES
    except Exception:
        return False


def build_weekly_review(start_date: str, end_date: str) -> Tuple[str, Dict[str, Any]]:
    refresh_stats = refresh_event_outcomes(start_date, end_date)
    with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
        rows = conn.execute(
            """
            SELECT protocol, symbol, COALESCE(name, ''), trigger_type, verdict,
                   COALESCE(score, 0), trigger_price, next_return, return_3d,
                   return_5d, max_drawdown_5d, review_status,
                   COALESCE(status, '') AS status
            FROM fact_intraday_protocol_events
            WHERE CAST(trade_date AS DATE) >= CAST(? AS DATE)
              AND CAST(trade_date AS DATE) <= CAST(? AS DATE)
            ORDER BY protocol, next_return DESC NULLS LAST, symbol
            """,
            [start_date, end_date],
        ).fetchall()

    signal_rows = [r for r in rows if not _is_scan_summary_row(r)]
    scan_rows = [r for r in rows if _is_scan_summary_row(r)]
    reviewed = [r for r in signal_rows if str(r[11] or '') == 'REVIEWED' and r[7] is not None]
    wait_count = sum(1 for r in signal_rows if str(r[11] or '') == 'WAIT_CLOSE_DATA')
    hit_count = sum(1 for r in reviewed if float(r[7] or 0) > 0)
    hit_rate = hit_count / len(reviewed) if reviewed else None
    avg_t1 = _avg([r[7] for r in reviewed])
    avg_3d = _avg([r[8] for r in reviewed])
    avg_5d = _avg([r[9] for r in reviewed])
    worst_dd = min([float(r[10]) for r in reviewed if r[10] is not None], default=None)

    lines = [
        '[烛龙盘中观察周报]',
        '',
        f'观察周期：{start_date} 至 {end_date}',
        f'复盘时间：{datetime.now():%Y-%m-%d %H:%M}',
        '',
        '总体：',
        f'扫描摘要：{len(scan_rows)} 条',
        f'触发信号：{len(signal_rows)} 条',
        f'T+1 已验证：{len(reviewed)} 条',
        f'等待收盘数据：{wait_count} 条',
        f'命中率：{hit_rate * 100:.1f}%' if hit_rate is not None else '命中率：--',
        f'平均 T+1 收益：{_fmt_pct(avg_t1)}',
        f'平均 3日收益：{_fmt_pct(avg_3d)}',
        f'平均 5日收益：{_fmt_pct(avg_5d)}',
        f'5日最大不利回撤：{_fmt_pct(worst_dd)}',
        '',
        '分协议：',
    ]

    for protocol in [PROTOCOL_EAGLE, PROTOCOL_OWL, PROTOCOL_RABBIT]:
        subset = [r for r in signal_rows if str(r[0] or '').upper() == protocol]
        subset_scans = [r for r in scan_rows if str(r[0] or '').upper() == protocol]
        sub_reviewed = [r for r in subset if str(r[11] or '') == 'REVIEWED' and r[7] is not None]
        sub_hit = sum(1 for r in sub_reviewed if float(r[7] or 0) > 0)
        sub_rate = sub_hit / len(sub_reviewed) if sub_reviewed else None
        lines.append(
            f'{protocol}：扫描 {len(subset_scans)}，触发 {len(subset)}，验证 {len(sub_reviewed)}，'
            f'命中率 {sub_rate * 100:.1f}%，均值 {_fmt_pct(_avg([r[7] for r in sub_reviewed]))}'
            if sub_rate is not None else
            f'{protocol}：扫描 {len(subset_scans)}，触发 {len(subset)}，验证 {len(sub_reviewed)}，命中率 --，均值 --'
        )

    if reviewed:
        best = max(reviewed, key=lambda r: float(r[7] or -999))
        worst = min(reviewed, key=lambda r: float(r[7] or 999))
        def display(row) -> str:
            name = str(row[2] or '').strip()
            sym = str(row[1] or '').strip()
            return f'{name} {sym}' if name and name != sym else sym
        lines.extend([
            '',
            '最佳样本：',
            f'{display(best)} | {best[0]} | T+1 {_fmt_pct(best[7])}',
            '',
            '失败样本：',
            f'{display(worst)} | {worst[0]} | T+1 {_fmt_pct(worst[7])}',
        ])

    lines.extend([
        '',
        '说明：该报告仅评估盘中观察信号，不进入模拟盘，不影响买卖，不写入 RAG。',
    ])
    return '\n'.join(lines), {
        'start_date': start_date,
        'end_date': end_date,
        'events': len(rows),
        'reviewed': len(reviewed),
        'wait_close_data': wait_count,
        'hit_count': hit_count,
        'hit_rate': hit_rate,
        'scan_summaries': len(scan_rows),
        'refresh_stats': refresh_stats,
    }


def push_weekly_review(content: str) -> bool:
    token = str(getattr(Config, 'PUSHPLUS_TOKEN', '') or '').strip()
    if not token:
        logger.warning('[protocol-observer] weekly push skipped: PUSHPLUS_TOKEN missing')
        return False
    try:
        resp = requests.post(
            str(getattr(Config, 'PUSHPLUS_URL', 'https://www.pushplus.plus/send')),
            json={
                'token': token,
                'title': '烛龙盘中观察周报',
                'content': content[:3000],
                'template': 'txt',
            },
            timeout=int(getattr(Config, 'PUSHPLUS_TIMEOUT', 10) or 10),
        )
        ok = resp.status_code == 200 and resp.json().get('code') == 200
        if ok:
            logger.info('[protocol-observer] weekly review push sent')
            return True
        logger.warning('[protocol-observer] weekly review push failed: %s', resp.text[:300])
    except Exception as exc:
        logger.warning('[protocol-observer] weekly review push exception: %s', exc)
    return False


def run_weekly_review(push: bool = True, today: Optional[date] = None) -> Dict[str, Any]:
    start_date, end_date = _previous_week(today)
    content, stats = build_weekly_review(start_date, end_date)
    stats['pushed'] = push_weekly_review(content) if push else False
    logger.info('[protocol-observer] weekly review stats: %s', stats)
    return stats


__all__ = [
    'PROTOCOL_EAGLE',
    'PROTOCOL_OWL',
    'PROTOCOL_RABBIT',
    'TRIGGER_EAGLE_PULSE',
    'TRIGGER_OWL_EOD',
    'TRIGGER_RABBIT_ANCHOR',
    'ensure_protocol_event_table',
    'record_protocol_event',
    'record_protocol_scan',
    'refresh_event_outcomes',
    'build_weekly_review',
    'run_weekly_review',
]
