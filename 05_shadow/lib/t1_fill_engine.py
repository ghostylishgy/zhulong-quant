#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
05_shadow/lib/t1_fill_engine.py
T+1 paper-fill engine for L4 PASS signals.
"""

from __future__ import annotations

import json
import logging
import math
import os
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

try:
    from .costs import max_affordable_buy_qty
    from .db_contract import DBGateway, DB_PATH
    from .engine import (
        ShadowFill,
        SlippageBreakdown,
        _load_rules,
        has_open_paper_position,
        open_paper_position_from_fill,
        persist_fill,
    )
    from .news_entry_gate import evaluate_shadow_news_entry_gate, push_shadow_news_gate_skip
    from .account_eligibility import evaluate_account_eligibility
    from .trade_contract import validate_trade_contract
    from .trade_contract_checks import evaluate_contract_conditions
    from .portfolio_metrics import rebuild_shadow_metrics
except Exception:
    from costs import max_affordable_buy_qty
    from db_contract import DBGateway, DB_PATH
    from engine import (
        ShadowFill,
        SlippageBreakdown,
        _load_rules,
        has_open_paper_position,
        open_paper_position_from_fill,
        persist_fill,
    )
    from news_entry_gate import evaluate_shadow_news_entry_gate, push_shadow_news_gate_skip
    from account_eligibility import evaluate_account_eligibility
    from trade_contract import validate_trade_contract
    from trade_contract_checks import evaluate_contract_conditions
    from portfolio_metrics import rebuild_shadow_metrics


BASE_DIR = Path('/root/quant_project')
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from config.settings import Config, ensure_syspath  # noqa: E402

ensure_syspath()

logger = logging.getLogger('shadow.t1_fill')

PENDING_STATUS = 'PENDING'
WAIT_DATA_STATUS = 'WAIT_DATA'
WAIT_NEWS_STATUS = 'WAIT_NEWS_CHECK'
FILLED_STATUS = 'FILLED'
UNFILLED_STATUS = 'UNFILLED'
SKIPPED_STATUS = 'SKIPPED'
EXPIRED_STATUS = 'EXPIRED'

try:
    T1_PENDING_MAX_TRADING_DAYS = int(os.getenv('T1_PENDING_MAX_TRADING_DAYS', '2'))
except Exception:
    T1_PENDING_MAX_TRADING_DAYS = 2
try:
    T1_EXPIRY_SWEEP_LIMIT = int(os.getenv('T1_EXPIRY_SWEEP_LIMIT', '200'))
except Exception:
    T1_EXPIRY_SWEEP_LIMIT = 200

PROBE_MODE = 'probe'
MAIN_MODE = 'main'
FINAL_MODE = 'final'

_MINUTE_FETCH_LOCK = threading.Lock()
_MINUTE_LAST_CALL_TS = 0.0
_MINUTE_MIN_INTERVAL_SEC = float(os.getenv('T1_STK_MINS_MIN_INTERVAL_SEC', '65'))


def _throttle_minute_fetch(symbol: str, fill_date: str) -> None:
    """Serialize TuShare minute calls to avoid stk_mins per-minute throttling."""
    global _MINUTE_LAST_CALL_TS
    if _MINUTE_MIN_INTERVAL_SEC <= 0:
        return
    with _MINUTE_FETCH_LOCK:
        now = time.monotonic()
        wait = _MINUTE_MIN_INTERVAL_SEC - (now - _MINUTE_LAST_CALL_TS)
        if wait > 0:
            logger.info('[T1] stk_mins throttle %.1fs before %s %s', wait, symbol, fill_date)
            time.sleep(wait)
        _MINUTE_LAST_CALL_TS = time.monotonic()


@dataclass
class DailyBar:
    symbol: str
    trade_date: str
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    pre_close: float = 0.0
    vol: float = 0.0
    amount: float = 0.0
    source: str = ''


@dataclass
class LimitInfo:
    up_limit: float = 0.0
    down_limit: float = 0.0
    source: str = ''
    estimated: bool = False


@dataclass
class MinutePack:
    rows: List[Dict[str, float]] = field(default_factory=list)
    source: str = ''
    unavailable_reason: str = ''


@dataclass
class PriceDecision:
    status: str
    reason: str
    base_price: float = 0.0
    v_ref: float = 0.0
    pricing_mode: str = ''
    data_quality: str = ''
    evidence: Dict[str, Any] = field(default_factory=dict)


def _normalize_trade_date(value: Any = None) -> str:
    if value is None or str(value).strip() == '':
        return datetime.now().strftime('%Y-%m-%d')
    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%d')
    if isinstance(value, date):
        return value.strftime('%Y-%m-%d')
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        return f'{text[:4]}-{text[4:6]}-{text[6:8]}'
    if len(text) >= 10 and text[4] == '-' and text[7] == '-':
        return text[:10]
    raise ValueError(f'Unsupported trade_date: {value}')


def _next_calendar_date(value: Any) -> str:
    dt = datetime.strptime(_normalize_trade_date(value), '%Y-%m-%d').date()
    return (dt + timedelta(days=1)).strftime('%Y-%m-%d')


def _trade_dates_between(start_value: Any, end_value: Any) -> List[date]:
    start = datetime.strptime(_normalize_trade_date(start_value), '%Y-%m-%d').date()
    end = datetime.strptime(_normalize_trade_date(end_value), '%Y-%m-%d').date()
    if end < start:
        return []

    try:
        with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
            rows = conn.execute(
                """
                SELECT CAST(cal_date AS VARCHAR), is_open
                FROM fact_trade_calendar
                WHERE exchange = 'SSE'
                  AND cal_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
                ORDER BY cal_date
                """,
                [start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')],
            ).fetchall()
            try:
                override_rows = conn.execute(
                    """
                    SELECT cal_date, is_open
                    FROM (
                        SELECT cal_date, is_open,
                               ROW_NUMBER() OVER (
                                   PARTITION BY cal_date
                                   ORDER BY created_at DESC, override_id DESC
                               ) AS row_num
                        FROM ops_trade_calendar_override
                        WHERE scope = 'ENTRY'
                          AND cal_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
                    )
                    WHERE row_num = 1
                    """,
                    [start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')],
                ).fetchall()
            except Exception:
                override_rows = []
    except Exception as exc:
        logger.warning('[T1] authoritative trade calendar unavailable; pending age held: %s', exc)
        return []

    expected_days = (end - start).days + 1
    if len(rows) != expected_days:
        logger.warning(
            '[T1] authoritative trade calendar coverage gap %s..%s rows=%s expected=%s; '
            'pending age held',
            start, end, len(rows), expected_days,
        )
        return []

    overrides = {row[0]: bool(row[1]) for row in override_rows}
    dates: List[date] = []
    for raw_date, raw_open in rows:
        cal_date = datetime.strptime(_normalize_trade_date(raw_date), '%Y-%m-%d').date()
        is_open = bool(raw_open)
        if cal_date.weekday() < 5 and cal_date in overrides:
            is_open = overrides[cal_date]
        if is_open:
            dates.append(cal_date)
    return dates


def _trading_day_age(start_value: Any, end_value: Any) -> int:
    dates = _trade_dates_between(start_value, end_value)
    if not dates:
        return 0
    return max(0, len(dates) - 1)


def _to_ts_date(value: Any) -> str:
    return _normalize_trade_date(value).replace('-', '')


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(float(value))
    except Exception:
        return default


def _round_lot(qty: float) -> int:
    return max(0, int(qty) // 100 * 100)


def _is_buyable_board(symbol: str) -> bool:
    sym = str(symbol or '').strip().upper()
    return bool(sym.endswith(('.SH', '.SZ', '.BJ')))


def _limit_pct_for_symbol(symbol: str) -> float:
    sym = str(symbol or '').strip().upper()
    code = sym.split('.')[0]
    if sym.endswith('.BJ') or code.startswith(('4', '8', '9')):
        return 0.30
    if sym.endswith('.SH') and code.startswith('688'):
        return 0.20
    if sym.endswith('.SZ') and code.startswith(('300', '301')):
        return 0.20
    return 0.10


def _estimate_limit(symbol: str, pre_close: float) -> LimitInfo:
    if pre_close <= 0:
        return LimitInfo(source='LIMIT_MISSING', estimated=True)
    pct = _limit_pct_for_symbol(symbol)
    return LimitInfo(
        up_limit=round(pre_close * (1 + pct), 2),
        down_limit=round(pre_close * (1 - pct), 2),
        source='LIMIT_ESTIMATED',
        estimated=True,
    )


def _ensure_columns(conn, table_name: str, required: Dict[str, str]) -> None:
    rows = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchall()
    existing = {str(r[0]).lower() for r in rows}
    for col, ddl in required.items():
        if col.lower() not in existing:
            conn.execute(f'ALTER TABLE {table_name} ADD COLUMN {col} {ddl}')
            logger.info('[T1] patch schema add column: %s.%s', table_name, col)


def ensure_t1_tables(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_shadow_pending_signals (
            signal_id VARCHAR PRIMARY KEY,
            task_id VARCHAR DEFAULT '',
            run_id VARCHAR DEFAULT '',
            symbol VARCHAR NOT NULL,
            name VARCHAR DEFAULT '',
            signal_trade_date DATE NOT NULL,
            earliest_fill_date DATE,
            final_score DOUBLE DEFAULT 0,
            l1_close DOUBLE DEFAULT 0,
            entry_tide_gate VARCHAR DEFAULT '',
            entry_tide_ratio DOUBLE DEFAULT 0,
            entry_policy VARCHAR DEFAULT '',
            status VARCHAR DEFAULT 'PENDING',
            attempts INTEGER DEFAULT 0,
            last_error VARCHAR DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_shadow_pending_status_date
        ON fact_shadow_pending_signals(status, earliest_fill_date)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_shadow_fill_events (
            fill_id VARCHAR PRIMARY KEY,
            signal_id VARCHAR NOT NULL,
            task_id VARCHAR DEFAULT '',
            run_id VARCHAR DEFAULT '',
            symbol VARCHAR NOT NULL,
            name VARCHAR DEFAULT '',
            signal_trade_date DATE NOT NULL,
            fill_date DATE NOT NULL,
            action VARCHAR DEFAULT 'BUY',
            status VARCHAR NOT NULL,
            reason VARCHAR DEFAULT '',
            base_price DOUBLE DEFAULT 0,
            fill_price DOUBLE DEFAULT 0,
            qty INTEGER DEFAULT 0,
            allocated_cash DOUBLE DEFAULT 0,
            gross_amount DOUBLE DEFAULT 0,
            commission DOUBLE DEFAULT 0,
            stamp_tax DOUBLE DEFAULT 0,
            transfer_fee DOUBLE DEFAULT 0,
            tax_total DOUBLE DEFAULT 0,
            net_amount DOUBLE DEFAULT 0,
            slippage_cost DOUBLE DEFAULT 0,
            pricing_mode VARCHAR DEFAULT '',
            data_quality VARCHAR DEFAULT '',
            entry_tide_gate VARCHAR DEFAULT '',
            entry_tide_ratio DOUBLE DEFAULT 0,
            entry_policy VARCHAR DEFAULT '',
            evidence_json VARCHAR DEFAULT '{}',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_shadow_fill_events_signal
        ON fact_shadow_fill_events(signal_id, fill_date)
        """
    )
    _ensure_columns(
        conn,
        'fact_shadow_pending_signals',
        {
            'entry_tide_gate': "VARCHAR DEFAULT ''",
            'entry_tide_ratio': 'DOUBLE DEFAULT 0',
            'entry_policy': "VARCHAR DEFAULT ''",
            'trade_archetype': "VARCHAR DEFAULT ''",
            'trade_contract_json': "VARCHAR DEFAULT '{}'",
            'contract_version': "VARCHAR DEFAULT ''",
        },
    )
    _ensure_columns(
        conn,
        'fact_shadow_fill_events',
        {
            'entry_tide_gate': "VARCHAR DEFAULT ''",
            'entry_tide_ratio': 'DOUBLE DEFAULT 0',
            'entry_policy': "VARCHAR DEFAULT ''",
            'commission': 'DOUBLE DEFAULT 0',
            'stamp_tax': 'DOUBLE DEFAULT 0',
            'transfer_fee': 'DOUBLE DEFAULT 0',
            'tax_total': 'DOUBLE DEFAULT 0',
            'net_amount': 'DOUBLE DEFAULT 0',
            'trade_archetype': "VARCHAR DEFAULT ''",
            'contract_version': "VARCHAR DEFAULT ''",
        },
    )


def enqueue_pending_signal(signal: Dict[str, Any]) -> bool:
    task_id = str(signal.get('task_id') or '').strip()
    symbol = str(signal.get('symbol') or '').strip().upper()
    signal_td = _normalize_trade_date(signal.get('trade_date'))
    if not task_id or not symbol:
        logger.warning('[T1] enqueue skipped invalid signal: task_id=%s symbol=%s', task_id, symbol)
        return False

    name = str(signal.get('name') or symbol).strip()
    entry_tide_gate = str(signal.get('entry_tide_gate') or '').strip().upper()
    entry_tide_ratio = _safe_float(signal.get('entry_tide_ratio'))
    entry_policy = str(signal.get('entry_policy') or '').strip().upper()
    contract = signal.get('trade_contract') if isinstance(signal.get('trade_contract'), dict) else {}
    trade_archetype = str(contract.get('primary_archetype') or signal.get('trade_archetype') or '').strip().upper()
    contract_version = str(contract.get('contract_version') or '').strip()
    contract_json = json.dumps(contract, ensure_ascii=False, default=str)
    status = str(signal.get('status') or PENDING_STATUS).strip().upper()
    if status not in {PENDING_STATUS, WAIT_NEWS_STATUS}:
        status = PENDING_STATUS
    last_error = str(signal.get('last_error') or '').strip()[:500]
    now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
        ensure_t1_tables(conn)
        conn.execute(
            """
            INSERT INTO fact_shadow_pending_signals (
                signal_id, task_id, run_id, symbol, name, signal_trade_date,
                earliest_fill_date, final_score, l1_close, entry_tide_gate,
                entry_tide_ratio, entry_policy, trade_archetype, trade_contract_json,
                contract_version, status, attempts, last_error, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, CAST(? AS DATE), CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?,
                    CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP))
            ON CONFLICT (signal_id) DO UPDATE SET
                run_id = EXCLUDED.run_id,
                name = EXCLUDED.name,
                final_score = EXCLUDED.final_score,
                l1_close = EXCLUDED.l1_close,
                entry_tide_gate = EXCLUDED.entry_tide_gate,
                entry_tide_ratio = EXCLUDED.entry_tide_ratio,
                entry_policy = EXCLUDED.entry_policy,
                trade_archetype = EXCLUDED.trade_archetype,
                trade_contract_json = EXCLUDED.trade_contract_json,
                contract_version = EXCLUDED.contract_version,
                updated_at = EXCLUDED.updated_at
            """,
            [
                task_id,
                task_id,
                str(signal.get('run_id') or ''),
                symbol,
                name,
                signal_td,
                _next_calendar_date(signal_td),
                _safe_float(signal.get('final_score')),
                _safe_float(signal.get('close_price')),
                entry_tide_gate,
                entry_tide_ratio,
                entry_policy,
                trade_archetype,
                contract_json,
                contract_version,
                status,
                last_error,
                now_ts,
                now_ts,
            ],
        )
    logger.info('[T1] pending signal queued: %s %s %s', symbol, signal_td, task_id)
    return True


def _get_current_metrics() -> Dict[str, float | int]:
    with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS shadow_metrics (
                trade_date DATE PRIMARY KEY,
                total_equity DOUBLE,
                cash_reserve DOUBLE,
                daily_drawdown DOUBLE,
                active_positions INTEGER,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        row = conn.execute(
            """
            SELECT total_equity, cash_reserve, active_positions
            FROM shadow_metrics
            ORDER BY trade_date DESC
            LIMIT 1
            """
        ).fetchone()
    if row:
        return {
            'total_equity': float(row[0] or 0),
            'cash_reserve': float(row[1] or 0),
            'active_positions': int(row[2] or 0),
        }
    init_cap = float(_load_rules().get('position', {}).get('initial_capital', 1_000_000.0))
    return {'total_equity': init_cap, 'cash_reserve': init_cap, 'active_positions': 0}


def _update_metrics(trade_date: str, cost: float, direction: int = 1) -> None:
    with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
        rebuild_shadow_metrics(conn, through_date=trade_date)


def _get_tushare_api():
    token = str(getattr(Config, 'TUSHARE_TOKEN', '') or '').strip()
    if not token:
        return None, 'TUSHARE_TOKEN_MISSING'
    try:
        import tushare as ts
        return ts.pro_api(token), ''
    except Exception as exc:
        return None, f'TUSHARE_INIT_FAILED:{exc}'


def _fetch_daily_bar(api, symbol: str, fill_date: str) -> Tuple[Optional[DailyBar], str]:
    ts_date = _to_ts_date(fill_date)
    if api is not None:
        try:
            df = api.daily(
                ts_code=symbol,
                trade_date=ts_date,
                fields='ts_code,trade_date,open,high,low,close,pre_close,vol,amount',
            )
            if df is not None and not df.empty:
                r = df.iloc[0].to_dict()
                return DailyBar(
                    symbol=symbol,
                    trade_date=fill_date,
                    open=_safe_float(r.get('open')),
                    high=_safe_float(r.get('high')),
                    low=_safe_float(r.get('low')),
                    close=_safe_float(r.get('close')),
                    pre_close=_safe_float(r.get('pre_close')),
                    vol=_safe_float(r.get('vol')),
                    amount=_safe_float(r.get('amount')),
                    source='TUSHARE_DAILY',
                ), ''
        except Exception as exc:
            logger.warning('[T1] daily fetch failed %s %s: %s', symbol, fill_date, exc)

    try:
        with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
            row = conn.execute(
                """
                SELECT open, high, low, close, pre_close, vol, amount
                FROM fact_daily
                WHERE symbol = ?
                  AND CAST(trade_date AS DATE) = CAST(? AS DATE)
                LIMIT 1
                """,
                [symbol, fill_date],
            ).fetchone()
        if row:
            return DailyBar(
                symbol=symbol,
                trade_date=fill_date,
                open=_safe_float(row[0]),
                high=_safe_float(row[1]),
                low=_safe_float(row[2]),
                close=_safe_float(row[3]),
                pre_close=_safe_float(row[4]),
                vol=_safe_float(row[5]),
                amount=_safe_float(row[6]),
                source='DUCKDB_DAILY',
            ), ''
    except Exception as exc:
        logger.warning('[T1] DuckDB daily fallback failed %s %s: %s', symbol, fill_date, exc)
    return None, 'DAILY_MISSING'


def _fetch_limit_info(api, symbol: str, fill_date: str, daily: DailyBar) -> LimitInfo:
    ts_date = _to_ts_date(fill_date)
    if api is not None:
        try:
            df = api.stk_limit(
                ts_code=symbol,
                trade_date=ts_date,
                fields='ts_code,trade_date,up_limit,down_limit',
            )
            if df is not None and not df.empty:
                r = df.iloc[0].to_dict()
                up = _safe_float(r.get('up_limit'))
                down = _safe_float(r.get('down_limit'))
                if up > 0 or down > 0:
                    return LimitInfo(up_limit=up, down_limit=down, source='TUSHARE_STK_LIMIT')
        except Exception as exc:
            logger.warning('[T1] stk_limit fetch failed %s %s: %s', symbol, fill_date, exc)
    return _estimate_limit(symbol, daily.pre_close)


def _fetch_minutes(api, symbol: str, fill_date: str) -> MinutePack:
    if api is None:
        return MinutePack(source='NONE', unavailable_reason='TUSHARE_UNAVAILABLE')
    try:
        start = f'{fill_date} 09:30:00'
        end = f'{fill_date} 15:00:00'
        df = None
        if hasattr(api, 'stk_mins'):
            _throttle_minute_fetch(symbol, fill_date)
            df = api.stk_mins(ts_code=symbol, freq='1min', start_date=start, end_date=end)
        if (df is None or df.empty):
            try:
                import tushare as ts
                df = ts.pro_bar(
                    ts_code=symbol,
                    start_date=_to_ts_date(fill_date),
                    end_date=_to_ts_date(fill_date),
                    freq='1min',
                    asset='E',
                    adj=None,
                )
            except Exception as exc:
                logger.debug('[T1] pro_bar minute fallback failed %s %s: %s', symbol, fill_date, exc)
        if df is None or df.empty:
            return MinutePack(source='TUSHARE_STK_MINS', unavailable_reason='MINUTE_EMPTY')

        rows: List[Dict[str, float]] = []
        for _, r in df.iterrows():
            raw_time = r.get('trade_time') or r.get('datetime') or r.get('time') or r.get('trade_date') or ''
            text_time = str(raw_time)
            hhmm = text_time[11:16] if len(text_time) >= 16 else text_time[-8:-3]
            rows.append(
                {
                    'time': hhmm,
                    'open': _safe_float(r.get('open')),
                    'high': _safe_float(r.get('high')),
                    'low': _safe_float(r.get('low')),
                    'close': _safe_float(r.get('close')),
                    'vol': _safe_float(r.get('vol') if 'vol' in r else r.get('volume')),
                    'amount': _safe_float(r.get('amount')),
                }
            )
        rows.sort(key=lambda x: str(x.get('time') or ''))
        return MinutePack(rows=rows, source='TUSHARE_STK_MINS')
    except Exception as exc:
        logger.warning('[T1] minute fetch failed %s %s: %s', symbol, fill_date, exc)
        return MinutePack(source='TUSHARE_STK_MINS', unavailable_reason=f'MINUTE_ERROR:{exc}')


def _choose_amount_volume_units(rows: List[Dict[str, float]]) -> Tuple[float, float]:
    total_vol = sum(max(0.0, _safe_float(r.get('vol'))) for r in rows)
    total_amount = sum(max(0.0, _safe_float(r.get('amount'))) for r in rows)
    close_vals = [_safe_float(r.get('close')) for r in rows if _safe_float(r.get('close')) > 0]
    ref_price = sorted(close_vals)[len(close_vals) // 2] if close_vals else 0.0
    if total_vol <= 0:
        return 0.0, 0.0
    if total_amount <= 0:
        return ref_price, total_vol * 100.0

    candidates = [
        (total_amount / total_vol, total_vol, 'amount_yuan_vol_shares'),
        (total_amount / (total_vol * 100.0), total_vol * 100.0, 'amount_yuan_vol_hands'),
        ((total_amount * 1000.0) / total_vol, total_vol, 'amount_k_yuan_vol_shares'),
        ((total_amount * 1000.0) / (total_vol * 100.0), total_vol * 100.0, 'amount_k_yuan_vol_hands'),
    ]
    plausible = [(p, v, label) for p, v, label in candidates if 0.2 <= p <= 1000 and v > 0]
    if not plausible:
        return ref_price, total_vol * 100.0
    if ref_price > 0:
        price, vol_shares, _ = min(plausible, key=lambda item: abs(item[0] - ref_price))
    else:
        price, vol_shares, _ = plausible[0]
    return float(price), float(vol_shares)


def _daily_vwap(daily: DailyBar) -> Tuple[float, float]:
    if daily.vol > 0 and daily.amount > 0:
        # Tushare/zhulong daily convention: amount is thousand yuan, vol is hands.
        return (daily.amount * 1000.0) / (daily.vol * 100.0), daily.vol * 100.0
    return daily.open or daily.close, max(daily.vol * 100.0, 0.0)


def _minute_window(rows: List[Dict[str, float]], start_hhmm: str, end_hhmm: str) -> List[Dict[str, float]]:
    return [r for r in rows if start_hhmm <= str(r.get('time') or '') <= end_hhmm]


def _is_one_price_limit_up(daily: DailyBar, limit_info: LimitInfo) -> bool:
    up = limit_info.up_limit
    if up <= 0:
        return False
    eps = max(0.01, up * 0.001)
    vals = [daily.open, daily.high, daily.low, daily.close]
    return all(v > 0 and abs(v - up) <= eps for v in vals)


def _price_decision(
    *,
    symbol: str,
    fill_date: str,
    daily: DailyBar,
    limit_info: LimitInfo,
    minutes: MinutePack,
    mode: str,
) -> PriceDecision:
    evidence: Dict[str, Any] = {
        'daily_source': daily.source,
        'limit_source': limit_info.source,
        'limit_estimated': limit_info.estimated,
        'minute_source': minutes.source,
        'minute_rows': len(minutes.rows),
    }
    if daily.open <= 0 or daily.high <= 0 or daily.low <= 0 or daily.close <= 0:
        return PriceDecision(WAIT_DATA_STATUS, 'DAILY_INCOMPLETE', data_quality='DAILY_INCOMPLETE', evidence=evidence)
    if _is_one_price_limit_up(daily, limit_info):
        return PriceDecision(
            UNFILLED_STATUS,
            'LIMIT_UP_ONE_PRICE',
            data_quality='LIMIT_CONFIRMED' if not limit_info.estimated else 'LIMIT_ESTIMATED',
            evidence=evidence,
        )

    has_minute = bool(minutes.rows)
    if not has_minute and mode != FINAL_MODE:
        return PriceDecision(
            WAIT_DATA_STATUS,
            minutes.unavailable_reason or 'MINUTE_WAIT',
            data_quality='MINUTE_WAIT',
            evidence=evidence,
        )

    if has_minute:
        up = limit_info.up_limit
        if up > 0 and daily.high >= up * 0.999 and daily.low < up * 0.999:
            open_rows = [r for r in minutes.rows if _safe_float(r.get('low')) < up - 0.0001]
            if open_rows:
                price, vol_shares = _choose_amount_volume_units(open_rows)
                if price > 0 and vol_shares >= 100:
                    evidence['open_window_rows'] = len(open_rows)
                    return PriceDecision(
                        FILLED_STATUS,
                        'LIMIT_OPEN_WINDOW_VWAP',
                        base_price=price,
                        v_ref=vol_shares,
                        pricing_mode='T1_LIMIT_OPEN_VWAP',
                        data_quality='MINUTE_OK',
                        evidence=evidence,
                    )
        early = _minute_window(minutes.rows, '09:31', '09:45')
        if early:
            price, vol_shares = _choose_amount_volume_units(early)
            if price > 0 and vol_shares >= 100:
                return PriceDecision(
                    FILLED_STATUS,
                    'VWAP_0931_0945',
                    base_price=price,
                    v_ref=vol_shares,
                    pricing_mode='T1_VWAP_15M',
                    data_quality='MINUTE_OK',
                    evidence=evidence,
                )

    price, vol_shares = _daily_vwap(daily)
    if price <= 0:
        price = daily.open or daily.close
    if price <= 0:
        return PriceDecision(WAIT_DATA_STATUS, 'PRICE_MISSING', data_quality='PRICE_MISSING', evidence=evidence)
    return PriceDecision(
        FILLED_STATUS,
        'DAILY_VWAP_FALLBACK' if daily.amount > 0 and daily.vol > 0 else 'OPEN_FALLBACK',
        base_price=price,
        v_ref=max(vol_shares, 100.0),
        pricing_mode='T1_DAILY_VWAP' if daily.amount > 0 and daily.vol > 0 else 'T1_OPEN',
        data_quality='DAILY_FALLBACK' if not has_minute else 'MINUTE_WEAK',
        evidence=evidence,
    )


def _fetch_sigma(symbol: str, fill_date: str) -> float:
    try:
        with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
            row = conn.execute(
                """
                WITH recent AS (
                    SELECT pct_chg
                    FROM fact_daily
                    WHERE symbol = ?
                      AND CAST(trade_date AS DATE) <= CAST(? AS DATE)
                      AND pct_chg IS NOT NULL
                    ORDER BY trade_date DESC
                    LIMIT 5
                )
                SELECT STDDEV_SAMP(pct_chg), COUNT(*) FROM recent
                """,
                [symbol, fill_date],
            ).fetchone()
        if row and int(row[1] or 0) >= 3:
            return max(0.005, float(row[0] or 0) / 100.0)
    except Exception as exc:
        logger.debug('[T1] sigma fallback %s: %s', symbol, exc)
    return 0.025


def _fetch_total_mv(api, symbol: str, fill_date: str) -> float:
    if api is None:
        return 0.0
    try:
        df = api.daily_basic(ts_code=symbol, trade_date=_to_ts_date(fill_date), fields='ts_code,total_mv')
        if df is not None and not df.empty:
            return _safe_float(df.iloc[0].to_dict().get('total_mv'))
    except Exception as exc:
        logger.debug('[T1] daily_basic total_mv failed %s %s: %s', symbol, fill_date, exc)
    return 0.0


def _impact_k(symbol: str, total_mv_wan: float) -> float:
    if total_mv_wan >= 5_000_000:
        return 0.15
    if total_mv_wan >= 1_000_000:
        return 0.50
    if symbol.startswith(('600', '601', '603', '605', '000', '001', '002')):
        return 0.50
    return 0.80


def _build_fill(
    *,
    symbol: str,
    base_price: float,
    qty: int,
    v_ref: float,
    sigma: float,
    k: float,
    pricing_mode: str,
    evidence: Dict[str, Any],
) -> Tuple[ShadowFill, Dict[str, Any]]:
    v_ref = max(float(v_ref or 0), 100.0)
    qty = _round_lot(qty)
    notes: List[str] = []
    if qty <= 0:
        raise ValueError('qty <= 0')

    participation = max(0.0, qty / v_ref)
    impact_pct = k * sigma * math.sqrt(max(participation, 0.0))
    if impact_pct > 0.02:
        notes.append('HIGH_IMPACT')
        max_participation = (0.02 / max(k * sigma, 0.000001)) ** 2
        capped_qty = _round_lot(v_ref * max_participation)
        if 0 < capped_qty < qty:
            qty = capped_qty
            participation = max(0.0, qty / v_ref)
            impact_pct = k * sigma * math.sqrt(max(participation, 0.0))
            notes.append('QTY_CUT_FOR_IMPACT')
    if impact_pct > 0.03:
        impact_pct = 0.03
        notes.append('IMPACT_CAPPED_3PCT')

    price_shadow = round(base_price * (1 + impact_pct), 4)
    slippage_cost = round(abs(price_shadow - base_price) * qty, 2)
    breakdown = SlippageBreakdown(
        vol_part_pct=impact_pct * 100,
        size_part_pct=0.0,
        total_pct=impact_pct * 100,
        is_fallback=False,
        is_capacity_capped='QTY_CUT_FOR_IMPACT' in notes,
        original_qty=int(evidence.get('original_qty') or qty),
        adjusted_qty=qty,
        sigma=sigma,
        adv20=v_ref,
        avg_vol_5d=v_ref,
    )
    fill = ShadowFill(
        symbol=symbol,
        action='BUY',
        price_logical=round(base_price, 4),
        price_shadow=price_shadow,
        qty=qty,
        slippage_cost=slippage_cost,
        breakdown=breakdown,
        pricing_mode=pricing_mode,
    )
    detail = {
        'impact_pct': round(impact_pct, 6),
        'participation': round(participation, 6),
        'v_ref': round(v_ref, 2),
        'sigma': round(sigma, 6),
        'impact_k': k,
        'notes': notes,
    }
    return fill, detail


def _lookup_stock_name(symbol: str) -> str:
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
        logger.debug('[T1] stock name lookup failed %s: %s', sym, exc)
    return ''


def _pricing_mode_text(value: str) -> str:
    mapping = {
        'T1_LIMIT_OPEN_VWAP': '涨停打开后按早盘均价模拟',
        'T1_VWAP_15M': '按开盘后 15 分钟均价模拟',
        'T1_DAILY_VWAP': '按全天均价模拟',
        'T1_OPEN': '按开盘价模拟',
    }
    raw = str(value or '').strip().upper()
    return mapping.get(raw, '按可用行情模拟')


def _pending_status_text(value: str) -> str:
    mapping = {
        PENDING_STATUS: '等待成交窗口',
        WAIT_DATA_STATUS: '等待行情数据',
        FILLED_STATUS: '已模拟成交',
        UNFILLED_STATUS: '未能成交',
        SKIPPED_STATUS: '已跳过',
        EXPIRED_STATUS: '已过期',
    }
    raw = str(value or '').strip().upper()
    return mapping.get(raw, raw or '未知')


def _fill_reason_text(value: str) -> str:
    raw = str(value or '').strip()
    upper = raw.upper()
    mapping = {
        'POSITION_FULL': '模拟盘持仓数量已满',
        'INSUFFICIENT_CASH_OR_CAPACITY': '可用现金或容量不足',
        'INSUFFICIENT_CASH_AFTER_IMPACT': '计入冲击成本后现金不足',
        'NO_DAILY_DATA': '缺少日线行情，暂不成交',
        'NO_MINUTE_DATA': '缺少分钟行情，暂不成交',
        'TUSHARE_UNAVAILABLE': '行情接口暂不可用',
        'MINUTE_EMPTY': '分钟行情为空',
    }
    if upper.startswith('T1_PENDING_EXPIRED'):
        return '等待成交超过纪律窗口，已过期'
    if upper.startswith('MINUTE_ERROR'):
        return '分钟行情读取异常'
    return mapping.get(upper, raw or '按模拟盘成交纪律执行')


def _push_shadow_buy(fill: ShadowFill, signal: Dict[str, Any], total_cost: float, fill_date: str, reason: str) -> bool:
    token = str(getattr(Config, 'PUSHPLUS_TOKEN', '') or '').strip()
    if not token:
        logger.warning('[T1] fill push skipped %s: PUSHPLUS_TOKEN missing', fill.symbol)
        return False
    name = str(signal.get('name') or '').strip() or _lookup_stock_name(fill.symbol)
    display = f'{name} {fill.symbol}' if name and name != fill.symbol else fill.symbol
    title = f'烛龙模拟盘成交 | {display}'
    content = "\n".join([
        '[模拟盘 T+1 成交]',
        f'标的：{display}',
        f'信号日：{signal.get("signal_trade_date", "")}',
        f'成交日：{fill_date}',
        f'系统评分：{_safe_float(signal.get("final_score")):.0f}',
        f'成交数量：{fill.qty:,} 股',
        f'基准价格：{fill.price_logical:.4f}',
        f'模拟成交价：{fill.price_shadow:.4f}',
        f'成交额：{fill.gross_amount:,.2f}',
        f'冲击/滑点：{fill.slippage_cost:,.2f}',
        f'佣金：{fill.commission:,.2f}',
        f'印花税：{fill.stamp_tax:,.2f}',
        f'过户费：{fill.transfer_fee:,.2f}',
        f'税费合计：{fill.tax_total:,.2f}',
        f'模拟入账成本：{total_cost:,.2f}',
        f'成交依据：{_fill_reason_text(reason)}',
        f'价格口径：{_pricing_mode_text(fill.pricing_mode)}',
        '',
        '说明：仅为影子模拟盘记录，不代表真实交易指令。',
    ])
    url = str(getattr(Config, 'PUSHPLUS_URL', 'https://www.pushplus.plus/send'))
    timeout = int(getattr(Config, 'PUSHPLUS_TIMEOUT', 10) or 10)
    payload = {'token': token, 'title': title, 'content': content[:2000], 'template': 'txt'}
    for attempt in range(1, 4):
        try:
            response = requests.post(url, json=payload, timeout=timeout)
            try:
                response_payload = response.json()
            except Exception:
                response_payload = {}
            if response.status_code == 200 and response_payload.get('code') == 200:
                logger.info('[T1] fill push sent %s attempt=%s/3', fill.symbol, attempt)
                return True
            logger.warning(
                '[T1] fill push rejected %s attempt=%s/3 http=%s code=%s body=%s',
                fill.symbol,
                attempt,
                response.status_code,
                response_payload.get('code'),
                str(getattr(response, 'text', '') or '')[:240],
            )
        except Exception as exc:
            logger.warning('[T1] fill push exception %s attempt=%s/3: %s', fill.symbol, attempt, exc)
        if attempt < 3:
            time.sleep(1)
    logger.error('[T1] fill push failed after retries %s; simulated fill remains committed', fill.symbol)
    return False


def _push_t1_watchdog(rows: List[Dict[str, Any]], fill_date: str, stats: Dict[str, Any]) -> None:
    token = str(getattr(Config, 'PUSHPLUS_TOKEN', '') or '').strip()
    if not token or not rows:
        return
    lines = [
        '[模拟盘 T+1 待成交提醒]',
        f'检查日期：{fill_date}',
        f'到期未终态：{stats.get("pending_due", 0)} 条',
        f'等待成交窗口：{stats.get("pending", 0)} 条',
        f'等待行情数据：{stats.get("wait_data", 0)} 条',
        '',
    ]
    for i, row in enumerate(rows[:10], 1):
        symbol = str(row.get('symbol') or '').strip().upper()
        name = str(row.get('name') or '').strip() or _lookup_stock_name(symbol)
        display = f'{name} {symbol}' if name and name != symbol else symbol
        lines.append(
            f"{i}. {display} | 状态：{_pending_status_text(row.get('status', ''))} "
            f"尝试:{row.get('attempts', 0)} "
            f"信号日:{row.get('signal_trade_date', '')} "
            f"最早成交:{row.get('earliest_fill_date', '')}"
        )
        err = str(row.get('last_error') or '').strip()
        if err:
            lines.append(f'   原因：{_fill_reason_text(err)[:80]}')
    if len(rows) > 10:
        lines.append(f'... 另有 {len(rows) - 10} 条未展示')
    lines.extend([
        '',
        '说明：该提醒只表示模拟盘成交链路需要关注，不代表真实交易指令。',
    ])
    try:
        requests.post(
            str(getattr(Config, 'PUSHPLUS_URL', 'https://www.pushplus.plus/send')),
            json={
                'token': token,
                'title': f'烛龙模拟盘待成交 | {stats.get("pending_due", 0)}条',
                'content': '\n'.join(lines)[:2200],
                'template': 'txt',
            },
            timeout=int(getattr(Config, 'PUSHPLUS_TIMEOUT', 10) or 10),
        )
    except Exception as exc:
        logger.warning('[T1] watchdog push exception: %s', exc)


def _record_fill_event(conn, event: Dict[str, Any]) -> None:
    ensure_t1_tables(conn)
    conn.execute(
        """
        INSERT INTO fact_shadow_fill_events (
            fill_id, signal_id, task_id, run_id, symbol, name, signal_trade_date, fill_date,
            action, status, reason, base_price, fill_price, qty, allocated_cash, gross_amount,
            commission, stamp_tax, transfer_fee, tax_total, net_amount,
            slippage_cost, pricing_mode, data_quality, entry_tide_gate, entry_tide_ratio,
            entry_policy, evidence_json, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, CAST(? AS DATE), CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS TIMESTAMP))
        ON CONFLICT (fill_id) DO NOTHING
        """,
        [
            event['fill_id'],
            event['signal_id'],
            event.get('task_id', ''),
            event.get('run_id', ''),
            event['symbol'],
            event.get('name', ''),
            event['signal_trade_date'],
            event['fill_date'],
            event.get('action', 'BUY'),
            event['status'],
            event.get('reason', ''),
            float(event.get('base_price') or 0),
            float(event.get('fill_price') or 0),
            int(event.get('qty') or 0),
            float(event.get('allocated_cash') or 0),
            float(event.get('gross_amount') or 0),
            float(event.get('commission') or 0),
            float(event.get('stamp_tax') or 0),
            float(event.get('transfer_fee') or 0),
            float(event.get('tax_total') or 0),
            float(event.get('net_amount') or 0),
            float(event.get('slippage_cost') or 0),
            event.get('pricing_mode', ''),
            event.get('data_quality', ''),
            event.get('entry_tide_gate', ''),
            _safe_float(event.get('entry_tide_ratio')),
            event.get('entry_policy', ''),
            json.dumps(event.get('evidence', {}), ensure_ascii=False, default=str)[:4000],
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        ],
    )


def _update_pending_status(conn, signal_id: str, status: str, last_error: str = '') -> None:
    conn.execute(
        """
        UPDATE fact_shadow_pending_signals
        SET status = ?, attempts = COALESCE(attempts, 0) + 1,
            last_error = ?, updated_at = CAST(? AS TIMESTAMP)
        WHERE signal_id = ?
        """,
        [status, str(last_error or '')[:500], datetime.now().strftime('%Y-%m-%d %H:%M:%S'), signal_id],
    )


def _fetch_due_signals(fill_date: str, limit: int, *, expiry_order: bool = False) -> List[Dict[str, Any]]:
    order_by = (
        'COALESCE(p.earliest_fill_date, p.signal_trade_date) ASC, p.created_at ASC'
        if expiry_order else
        'p.final_score DESC, p.created_at ASC'
    )
    with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
        rows = conn.execute(
            f"""
            SELECT
                p.signal_id, p.task_id, p.run_id, p.symbol, p.name,
                CAST(p.signal_trade_date AS VARCHAR) AS signal_trade_date,
                CAST(p.earliest_fill_date AS VARCHAR) AS earliest_fill_date,
                p.final_score, p.l1_close, p.status, p.attempts,
                COALESCE(p.entry_tide_gate, '') AS entry_tide_gate,
                COALESCE(p.entry_tide_ratio, 0) AS entry_tide_ratio,
                COALESCE(p.entry_policy, '') AS entry_policy,
                COALESCE(p.trade_archetype, '') AS trade_archetype,
                COALESCE(p.trade_contract_json, '{{}}') AS trade_contract_json,
                COALESCE(p.contract_version, '') AS contract_version,
                COALESCE(n.l2_pattern, '') AS l2_pattern,
                COALESCE(n.l3_reasoning, '') AS l3_reasoning,
                COALESCE(n.l4_veto_reason, '') AS l4_veto_reason,
                COALESCE(n.l4_notary_payload, '') AS l4_notary_payload,
                COALESCE(n.rag_intel, '') AS rag_intel
            FROM fact_shadow_pending_signals p
            LEFT JOIN nexus_audits n
              ON n.task_id = p.task_id
            WHERE UPPER(COALESCE(p.status, '')) IN (?, ?, ?)
              AND CAST(p.signal_trade_date AS DATE) < CAST(? AS DATE)
              AND (
                  p.earliest_fill_date IS NULL
                  OR CAST(p.earliest_fill_date AS DATE) <= CAST(? AS DATE)
              )
            ORDER BY {order_by}
            LIMIT ?
            """,
            [PENDING_STATUS, WAIT_DATA_STATUS, WAIT_NEWS_STATUS, fill_date, fill_date, limit],
        ).fetchall()
    return [
        {
            'signal_id': str(r[0] or ''),
            'task_id': str(r[1] or ''),
            'run_id': str(r[2] or ''),
            'symbol': str(r[3] or '').strip().upper(),
            'name': str(r[4] or r[3] or ''),
            'signal_trade_date': _normalize_trade_date(r[5]),
            'earliest_fill_date': str(r[6] or ''),
            'final_score': _safe_float(r[7]),
            'l1_close': _safe_float(r[8]),
            'status': str(r[9] or ''),
            'attempts': _safe_int(r[10]),
            'entry_tide_gate': str(r[11] or '').strip().upper(),
            'entry_tide_ratio': _safe_float(r[12]),
            'entry_policy': str(r[13] or '').strip().upper(),
            'trade_archetype': str(r[14] or '').strip().upper(),
            'trade_contract': json.loads(str(r[15] or '{}')),
            'contract_version': str(r[16] or ''),
            'l2_pattern': str(r[17] or ''),
            'l3_reasoning': str(r[18] or ''),
            'l4_veto_reason': str(r[19] or ''),
            'l4_notary_payload': str(r[20] or ''),
            'rag_intel': str(r[21] or ''),
        }
        for r in rows
    ]


def _pending_expiry_decision(signal: Dict[str, Any], fill_date: str) -> Tuple[bool, str, Dict[str, Any]]:
    if T1_PENDING_MAX_TRADING_DAYS < 0:
        return False, '', {}
    fill_td = _normalize_trade_date(fill_date)
    anchor = str(signal.get('earliest_fill_date') or '').strip()
    if not anchor:
        anchor = _next_calendar_date(signal.get('signal_trade_date'))
    anchor = _normalize_trade_date(anchor)
    if datetime.strptime(fill_td, '%Y-%m-%d').date() < datetime.strptime(anchor, '%Y-%m-%d').date():
        return False, '', {}

    age = _trading_day_age(anchor, fill_td)
    if age <= T1_PENDING_MAX_TRADING_DAYS:
        return False, '', {
            'expiry_anchor_date': anchor,
            'pending_trading_days': age,
            'max_pending_trading_days': T1_PENDING_MAX_TRADING_DAYS,
        }

    reason = (
        f'T1_PENDING_EXPIRED:age={age}>max={T1_PENDING_MAX_TRADING_DAYS}'
        f' anchor={anchor}'
    )
    evidence = {
        'data_quality': 'EXPIRED',
        'expiry_anchor_date': anchor,
        'fill_date': fill_td,
        'pending_trading_days': age,
        'max_pending_trading_days': T1_PENDING_MAX_TRADING_DAYS,
        'previous_status': signal.get('status', ''),
        'attempts_before_expiry': signal.get('attempts', 0),
    }
    return True, reason, evidence


def _expire_due_signals(fill_date: str, limit: int = T1_EXPIRY_SWEEP_LIMIT) -> List[Dict[str, Any]]:
    candidates = _fetch_due_signals(fill_date, max(1, int(limit or 1)), expiry_order=True)
    expired: List[Dict[str, Any]] = []
    for signal in candidates:
        should_expire, reason, evidence = _pending_expiry_decision(signal, fill_date)
        if not should_expire:
            continue
        _mark_terminal(signal, fill_date, EXPIRED_STATUS, reason, evidence)
        expired.append({**signal, 'expiry_reason': reason})
    if expired:
        logger.warning('[T1] expired stale pending signals: %s fill_date=%s', len(expired), fill_date)
    return expired


def _mark_terminal(signal: Dict[str, Any], fill_date: str, status: str, reason: str, evidence: Dict[str, Any]) -> None:
    fill_id = f"{signal['signal_id']}|{fill_date}|{status}"
    with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
        _record_fill_event(
            conn,
            {
                'fill_id': fill_id,
                'signal_id': signal['signal_id'],
                'task_id': signal.get('task_id', ''),
                'run_id': signal.get('run_id', ''),
                'symbol': signal['symbol'],
                'name': signal.get('name', ''),
                'signal_trade_date': signal['signal_trade_date'],
                'fill_date': fill_date,
                'status': status,
                'reason': reason,
                'data_quality': str(evidence.get('data_quality') or ''),
                'entry_tide_gate': signal.get('entry_tide_gate', ''),
                'entry_tide_ratio': signal.get('entry_tide_ratio', 0),
                'entry_policy': signal.get('entry_policy', ''),
                'evidence': evidence,
            },
        )
        _update_pending_status(conn, signal['signal_id'], status, reason)


def _fill_account_eligibility(signal: Dict[str, Any]):
    symbol = str(signal.get("symbol") or "").strip().upper()
    name = str(signal.get("name") or symbol).strip()
    is_st = None
    try:
        with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
            row = conn.execute(
                "SELECT COALESCE(name, ''), COALESCE(is_st, FALSE) FROM fact_stock_basic WHERE symbol=? LIMIT 1",
                [symbol],
            ).fetchone()
        if row:
            name = str(row[0] or name).strip()
            is_st = bool(row[1])
    except Exception as exc:
        logger.warning("[T1] account eligibility lookup failed closed %s: %s", symbol, exc)
        return evaluate_account_eligibility("", name, False)
    return evaluate_account_eligibility(symbol, name, is_st)


def _allocation_context_text(signal: Dict[str, Any]) -> str:
    parts = [
        signal.get('l2_pattern', ''),
        signal.get('l3_reasoning', ''),
        signal.get('l4_veto_reason', ''),
        signal.get('l4_notary_payload', ''),
        signal.get('rag_intel', ''),
    ]
    return ' '.join(str(p or '') for p in parts).upper()


def _commit_filled_buy(
    signal: Dict[str, Any],
    fill: ShadowFill,
    fill_date: str,
    *,
    tide_mode: str,
    strategy_tag: str,
    entry_tide_gate: str,
    entry_tide_ratio: float,
    entry_policy: str,
    event: Dict[str, Any],
) -> Tuple[bool, str]:
    """Commit one T+1 BUY as a single evidence-chain transaction."""
    with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
        conn.execute('BEGIN TRANSACTION')
        try:
            written = persist_fill(
                fill,
                trace_id=signal['signal_id'],
                tide_mode=tide_mode,
                strategy_tag=strategy_tag,
                trade_date=fill_date,
                conn=conn,
            )
            if not written:
                conn.execute('ROLLBACK')
                return False, 'LEDGER_DUPLICATE_OR_FAILED'

            opened = open_paper_position_from_fill(
                fill,
                signal_task_id=signal.get('task_id') or signal['signal_id'],
                entry_score=float(signal.get('final_score') or 0),
                source='L4_PASS_T1',
                strategy_tag=strategy_tag,
                entry_tide_gate=entry_tide_gate,
                entry_tide_ratio=entry_tide_ratio,
                entry_policy=entry_policy,
                trade_date=fill_date,
                conn=conn,
            )
            if not opened:
                conn.execute('ROLLBACK')
                return False, 'PAPER_OPEN_REJECTED'

            _record_fill_event(conn, event)
            _update_pending_status(conn, signal['signal_id'], FILLED_STATUS, '')
            conn.execute('COMMIT')
            return True, ''
        except Exception as exc:
            try:
                conn.execute('ROLLBACK')
            except Exception:
                pass
            logger.exception('[T1] atomic BUY rollback %s %s: %s', signal.get('symbol'), fill_date, exc)
            return False, 'ATOMIC_BUY_TRANSACTION_FAILED'


def _allocation_weight(signal: Dict[str, Any]) -> Tuple[float, List[str]]:
    score = float(signal.get('final_score') or 0)
    weight = 1.0
    reasons: List[str] = []
    if score >= 80:
        weight += 0.6
        reasons.append('L4>=80')
    elif score >= 70:
        weight += 0.3
        reasons.append('L4>=70')
    elif score >= 60:
        reasons.append('L4>=60')

    ctx = _allocation_context_text(signal)
    strong_terms = (
        'CORE_ATTACK', 'MOMENTUM_CONFIRM', 'MOMENTUM', 'VOLUME_BREAKOUT',
        'POCKET', 'BREAKOUT', 'TREND_FOLLOWING', '\u6838\u5fc3\u51fa\u51fb',
        '\u4e3b\u5347', '\u8fde\u6da8', '\u5f3a\u52bf', '\u7a81\u7834',
        '\u653e\u91cf', '\u53e3\u888b\u652f\u70b9', '\u8d8b\u52bf'
    )
    short_terms = (
        'PULLBACK', 'ARBITRAGE', 'REBOUND', '\u77ed\u7ebf', '\u53cd\u5f39',
        '\u56de\u62bd', '\u8865\u6da8', '\u5957\u5229'
    )
    risk_terms = (
        'DISTRIBUTION', 'EXHAUST', 'VETO', 'RETREAT', 'DIVERGENCE',
        'RISK_NEEDS_WATCH', 'DANGER', '\u51fa\u8d27', '\u9000\u6f6e',
        '\u900f\u652f', '\u5206\u6b67', '\u9ad8\u98ce\u9669'
    )
    if any(term in ctx for term in strong_terms):
        weight += 0.4
        reasons.append('strong_stage')
    if any(term in ctx for term in short_terms):
        weight -= 0.3
        reasons.append('short_stage')
    if any(term in ctx for term in risk_terms):
        weight -= 0.4
        reasons.append('risk_discount')

    return max(0.5, min(1.8, weight)), reasons


def _build_allocation_plan(signals: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    rules = _load_rules()
    position_rules = rules.get('position', {})
    max_positions = int(position_rules.get('max_positions', 4))
    max_single_pct = float(position_rules.get('max_single_pct', 0.35))
    min_single_pct = float(position_rules.get('min_single_pct', 0.10))
    metrics = _get_current_metrics()
    cash = float(metrics.get('cash_reserve') or 0)
    active_positions = int(metrics.get('active_positions') or 0)
    slots = max(0, max_positions - active_positions)
    if cash <= 0 or slots <= 0:
        return {}

    selected = list(signals[:slots])
    weighted = []
    for signal in selected:
        weight, reasons = _allocation_weight(signal)
        weighted.append((signal, weight, reasons))
    total_weight = sum(item[1] for item in weighted) or 1.0
    max_cash = max(0.0, cash * max_single_pct)
    min_cash = max(0.0, cash * min_single_pct)

    plan: Dict[str, Dict[str, Any]] = {}
    for signal, weight, reasons in weighted:
        alloc = cash * weight / total_weight
        alloc = min(max_cash, max(min_cash, alloc))
        plan[signal['signal_id']] = {
            'allocated_cash': round(alloc, 2),
            'weight': round(weight, 4),
            'reasons': reasons,
        }

    total_alloc = sum(float(v['allocated_cash']) for v in plan.values())
    if total_alloc > cash and total_alloc > 0:
        scale = cash / total_alloc
        for item in plan.values():
            item['allocated_cash'] = round(float(item['allocated_cash']) * scale, 2)
            item.setdefault('reasons', []).append('scaled_to_cash')

    return plan


def process_due_signal(
    signal: Dict[str, Any],
    *,
    fill_date: str,
    mode: str,
    api=None,
    allocated_cash_override: float | None = None,
) -> str:
    symbol = signal['symbol']
    eligibility = _fill_account_eligibility(signal)
    if not eligibility.allow_execution:
        evidence = eligibility.to_dict()
        evidence["data_quality"] = "ACCOUNT_ELIGIBILITY"
        evidence["fill_gate_stage"] = "t1_fill"
        _mark_terminal(signal, fill_date, SKIPPED_STATUS, eligibility.reason_code, evidence)
        logger.warning(
            "[T1] account eligibility skipped fill | %s signal=%s status=%s reason=%s",
            symbol, signal.get("signal_id", ""), eligibility.qualification_status, eligibility.reason_code,
        )
        return SKIPPED_STATUS

    contract = signal.get('trade_contract') if isinstance(signal.get('trade_contract'), dict) else {}
    if not validate_trade_contract(contract):
        evidence = {'data_quality': 'TRADE_CONTRACT', 'fill_gate_stage': 't1_fill', 'trade_contract': contract}
        _mark_terminal(signal, fill_date, SKIPPED_STATUS, 'TRADE_CONTRACT_INVALID', evidence)
        logger.warning('[T1] invalid trade contract skipped fill | %s signal=%s', symbol, signal.get('signal_id', ''))
        return SKIPPED_STATUS

    news_decision = evaluate_shadow_news_entry_gate(signal)
    if not news_decision.get('allow'):
        reason = str(news_decision.get('reason') or 'SKIPPED_NEWS_UNAVAILABLE')
        evidence = dict(news_decision.get('evidence') or {})
        evidence['data_quality'] = 'NEWS_GATE'
        evidence['fill_gate_stage'] = 't1_fill'
        _mark_terminal(signal, fill_date, SKIPPED_STATUS, reason, evidence)
        push_shadow_news_gate_skip(signal, news_decision, stage='t1_fill')
        logger.warning(
            '[T1] news gate skipped fill | %s signal=%s task=%s reason=%s status=%s gate=%s',
            symbol,
            signal.get('signal_id', ''),
            signal.get('task_id', ''),
            reason,
            evidence.get('l4_news_status', ''),
            evidence.get('l4_news_gate', ''),
        )
        return SKIPPED_STATUS

    if not _is_buyable_board(symbol):
        _mark_terminal(signal, fill_date, SKIPPED_STATUS, 'UNSUPPORTED_BOARD', {'data_quality': 'SKIPPED'})
        return SKIPPED_STATUS
    if has_open_paper_position(symbol):
        _mark_terminal(signal, fill_date, SKIPPED_STATUS, 'EXISTING_HOLD_POSITION', {'data_quality': 'SKIPPED'})
        return SKIPPED_STATUS

    daily, daily_reason = _fetch_daily_bar(api, symbol, fill_date)
    if daily is None:
        status = WAIT_DATA_STATUS if mode != FINAL_MODE else UNFILLED_STATUS
        _mark_terminal(signal, fill_date, status, daily_reason, {'data_quality': daily_reason})
        return status

    limit_info = _fetch_limit_info(api, symbol, fill_date, daily)
    minutes = _fetch_minutes(api, symbol, fill_date)
    decision = _price_decision(
        symbol=symbol,
        fill_date=fill_date,
        daily=daily,
        limit_info=limit_info,
        minutes=minutes,
        mode=mode,
    )
    if decision.status != FILLED_STATUS:
        status = WAIT_DATA_STATUS if decision.status == WAIT_DATA_STATUS and mode != FINAL_MODE else UNFILLED_STATUS
        evidence = dict(decision.evidence)
        evidence['data_quality'] = decision.data_quality
        _mark_terminal(signal, fill_date, status, decision.reason, evidence)
        return status

    open_gap_pct = (
        (float(daily.open) / float(daily.pre_close) - 1.0) * 100.0
        if float(daily.open or 0) > 0 and float(daily.pre_close or 0) > 0 else None
    )
    contract_condition_evaluation = evaluate_contract_conditions(
        contract,
        {"open_gap_pct": open_gap_pct},
    )

    rules = _load_rules()
    metrics = _get_current_metrics()
    max_positions = int(rules.get('position', {}).get('max_positions', 4))
    if int(metrics.get('active_positions') or 0) >= max_positions:
        _mark_terminal(signal, fill_date, SKIPPED_STATUS, 'POSITION_FULL', {'data_quality': 'SKIPPED'})
        return SKIPPED_STATUS

    cash = float(metrics.get('cash_reserve') or 0)
    max_single_pct = float(rules.get('position', {}).get('max_single_pct', 0.35))
    if allocated_cash_override is None:
        allocated_cash = cash * max_single_pct
        allocation_source = 'single_signal_default'
    else:
        allocated_cash = float(allocated_cash_override or 0)
        allocation_source = 'batch_weighted'
    allocated_cash = max(0.0, min(cash, allocated_cash))
    qty = _round_lot(allocated_cash / max(decision.base_price, 0.0001))
    max_participation = float(rules.get('t1_fill', {}).get('max_participation', 0.10))
    if decision.v_ref > 0:
        qty = min(qty, _round_lot(decision.v_ref * max_participation))
    if qty < 100:
        _mark_terminal(
            signal,
            fill_date,
            UNFILLED_STATUS,
            'INSUFFICIENT_CASH_OR_CAPACITY',
            {'data_quality': decision.data_quality, **decision.evidence},
        )
        return UNFILLED_STATUS

    sigma = _fetch_sigma(symbol, fill_date)
    total_mv = _fetch_total_mv(api, symbol, fill_date)
    k = _impact_k(symbol, total_mv)
    evidence = dict(decision.evidence)
    evidence['original_qty'] = qty
    evidence['allocated_cash'] = allocated_cash
    evidence['allocation_source'] = allocation_source
    if signal.get('allocation_weight') is not None:
        evidence['allocation_weight'] = signal.get('allocation_weight')
        evidence['allocation_reasons'] = signal.get('allocation_reasons', [])
    fill, impact_detail = _build_fill(
        symbol=symbol,
        base_price=decision.base_price,
        qty=qty,
        v_ref=decision.v_ref,
        sigma=sigma,
        k=k,
        pricing_mode=decision.pricing_mode,
        evidence=evidence,
    )
    gross_amount = fill.gross_amount
    if fill.net_amount > allocated_cash:
        capped_qty = max_affordable_buy_qty(fill.price_shadow, allocated_cash)
        if capped_qty < 100:
            _mark_terminal(
                signal,
                fill_date,
                UNFILLED_STATUS,
                'INSUFFICIENT_CASH_AFTER_IMPACT',
                {'data_quality': decision.data_quality, **decision.evidence, **impact_detail},
            )
            return UNFILLED_STATUS
        fill, impact_detail = _build_fill(
            symbol=symbol,
            base_price=decision.base_price,
            qty=capped_qty,
            v_ref=decision.v_ref,
            sigma=sigma,
            k=k,
            pricing_mode=decision.pricing_mode,
            evidence={**evidence, 'original_qty': qty},
        )
        gross_amount = fill.gross_amount
        impact_detail.setdefault('notes', []).append('QTY_CUT_FOR_CASH')

    entry_tide_gate = str(signal.get('entry_tide_gate') or '').strip().upper()
    entry_tide_ratio = _safe_float(signal.get('entry_tide_ratio'))
    entry_policy = str(signal.get('entry_policy') or '').strip().upper()
    tide_mode = entry_tide_gate or 'Golden'
    strategy_tag = (
        f'T1_FILL|signal_date={signal["signal_trade_date"]}|'
        f'final_score={signal.get("final_score", 0)}|run_id={signal.get("run_id", "")}'
        f'|tide={entry_tide_gate or "UNKNOWN"}'
        f'|archetype={signal.get("trade_archetype") or "UNKNOWN"}'
    )
    fill_id = f"{signal['signal_id']}|{fill_date}|FILLED"
    event_evidence = {
        **decision.evidence,
        **impact_detail,
        'entry_tide_gate': entry_tide_gate,
        'entry_tide_ratio': entry_tide_ratio,
        'entry_policy': entry_policy,
        'trade_archetype': signal.get('trade_archetype', ''),
        'trade_contract': signal.get('trade_contract', {}),
        'trade_contract_condition_evaluation': contract_condition_evaluation,
        'daily': daily.__dict__,
        'limit': limit_info.__dict__,
        'data_quality': decision.data_quality,
        'reason': decision.reason,
    }
    committed, failure_reason = _commit_filled_buy(
        signal,
        fill,
        fill_date,
        tide_mode=tide_mode,
        strategy_tag=strategy_tag,
        entry_tide_gate=entry_tide_gate,
        entry_tide_ratio=entry_tide_ratio,
        entry_policy=entry_policy,
        event={
            'fill_id': fill_id,
            'signal_id': signal['signal_id'],
            'task_id': signal.get('task_id', ''),
            'run_id': signal.get('run_id', ''),
            'symbol': symbol,
            'name': signal.get('name', ''),
            'signal_trade_date': signal['signal_trade_date'],
            'fill_date': fill_date,
            'status': FILLED_STATUS,
            'reason': decision.reason,
            'base_price': fill.price_logical,
            'fill_price': fill.price_shadow,
            'qty': fill.qty,
            'allocated_cash': allocated_cash,
            'gross_amount': gross_amount,
            'commission': fill.commission,
            'stamp_tax': fill.stamp_tax,
            'transfer_fee': fill.transfer_fee,
            'tax_total': fill.tax_total,
            'net_amount': fill.net_amount,
            'slippage_cost': fill.slippage_cost,
            'pricing_mode': fill.pricing_mode,
            'data_quality': decision.data_quality,
            'entry_tide_gate': entry_tide_gate,
            'entry_tide_ratio': entry_tide_ratio,
            'entry_policy': entry_policy,
            'evidence': event_evidence,
        },
    )
    if not committed:
        _mark_terminal(signal, fill_date, SKIPPED_STATUS, failure_reason, impact_detail)
        return SKIPPED_STATUS

    try:
        _update_metrics(fill_date, fill.net_amount, direction=1)
    except Exception as exc:
        logger.warning('[T1] metrics rebuild deferred after committed BUY %s: %s', symbol, exc)
    _push_shadow_buy(fill, signal, fill.net_amount, fill_date, decision.reason)
    logger.info('[T1] filled %s qty=%s price=%.4f date=%s', symbol, fill.qty, fill.price_shadow, fill_date)
    return FILLED_STATUS


def run_t1_fill_cycle(
    fill_date: str | None = None,
    *,
    mode: str = MAIN_MODE,
    limit: int = 20,
    dry_run: bool = False,
) -> Dict[str, Any]:
    fill_td = _normalize_trade_date(fill_date)
    mode = str(mode or MAIN_MODE).lower()
    if mode not in {PROBE_MODE, MAIN_MODE, FINAL_MODE}:
        mode = MAIN_MODE
    stats: Dict[str, Any] = {
        'fill_date': fill_td,
        'mode': mode,
        'due': 0,
        'filled': 0,
        'wait_data': 0,
        'unfilled': 0,
        'skipped': 0,
        'expired': 0,
        'errors': 0,
        'dry_run': dry_run,
        'allocation_plan': {},
        'expiry_window_trading_days': T1_PENDING_MAX_TRADING_DAYS,
    }
    with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
        ensure_t1_tables(conn)
    if not dry_run and mode != PROBE_MODE:
        expired = _expire_due_signals(fill_td)
        stats['expired'] = len(expired)
    signals = _fetch_due_signals(fill_td, limit)
    stats['due'] = len(signals)
    api, api_reason = _get_tushare_api()
    stats['api'] = 'OK' if api is not None else api_reason
    allocation_plan = _build_allocation_plan(signals)
    stats['allocation_plan'] = {
        sid: {
            'allocated_cash': info.get('allocated_cash', 0),
            'weight': info.get('weight', 0),
            'reasons': info.get('reasons', []),
        }
        for sid, info in allocation_plan.items()
    }
    if dry_run or mode == PROBE_MODE:
        logger.info(
            '[T1] probe due=%s fill_date=%s api=%s allocation=%s',
            len(signals), fill_td, stats['api'], stats['allocation_plan']
        )
        return stats

    for signal in signals:
        alloc = allocation_plan.get(signal.get('signal_id', ''))
        if alloc:
            signal['allocation_weight'] = alloc.get('weight')
            signal['allocation_reasons'] = alloc.get('reasons', [])
        try:
            result = process_due_signal(
                signal,
                fill_date=fill_td,
                mode=mode,
                api=api,
                allocated_cash_override=float(alloc.get('allocated_cash')) if alloc else None,
            )
            if result == FILLED_STATUS:
                stats['filled'] += 1
            elif result == WAIT_DATA_STATUS:
                stats['wait_data'] += 1
            elif result == UNFILLED_STATUS:
                stats['unfilled'] += 1
            else:
                stats['skipped'] += 1
        except Exception as exc:
            stats['errors'] += 1
            logger.error('[T1] signal processing failed %s: %s', signal.get('symbol'), exc, exc_info=True)
            try:
                with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
                    _update_pending_status(conn, signal['signal_id'], WAIT_DATA_STATUS, f'ERROR:{exc}')
            except Exception:
                logger.error('[T1] failed to mark error status', exc_info=True)
    logger.info('[T1] cycle stats: %s', stats)
    return stats


def run_t1_pending_watchdog(fill_date: str | None = None, *, push: bool = True) -> Dict[str, Any]:
    """Report due T+1 paper signals that are still non-terminal after the final window."""
    fill_td = _normalize_trade_date(fill_date)
    with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
        ensure_t1_tables(conn)

    with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
        rows_raw = conn.execute(
            """
            SELECT signal_id, task_id, run_id, symbol, name,
                   CAST(signal_trade_date AS VARCHAR) AS signal_trade_date,
                   CAST(earliest_fill_date AS VARCHAR) AS earliest_fill_date,
                   status, attempts, last_error, final_score,
                   COALESCE(entry_tide_gate, '') AS entry_tide_gate,
                   COALESCE(entry_tide_ratio, 0) AS entry_tide_ratio,
                   COALESCE(entry_policy, '') AS entry_policy,
                   CAST(updated_at AS VARCHAR) AS updated_at
            FROM fact_shadow_pending_signals
            WHERE UPPER(COALESCE(status, '')) IN (?, ?)
              AND CAST(signal_trade_date AS DATE) < CAST(? AS DATE)
              AND (
                  earliest_fill_date IS NULL
                  OR CAST(earliest_fill_date AS DATE) <= CAST(? AS DATE)
              )
            ORDER BY earliest_fill_date ASC NULLS FIRST, final_score DESC, created_at ASC
            LIMIT 50
            """,
            [PENDING_STATUS, WAIT_DATA_STATUS, fill_td, fill_td],
        ).fetchall()

    rows = [
        {
            'signal_id': str(r[0] or ''),
            'task_id': str(r[1] or ''),
            'run_id': str(r[2] or ''),
            'symbol': str(r[3] or '').strip().upper(),
            'name': str(r[4] or ''),
            'signal_trade_date': str(r[5] or ''),
            'earliest_fill_date': str(r[6] or ''),
            'status': str(r[7] or '').strip().upper(),
            'attempts': _safe_int(r[8]),
            'last_error': str(r[9] or ''),
            'final_score': _safe_float(r[10]),
            'entry_tide_gate': str(r[11] or '').strip().upper(),
            'entry_tide_ratio': _safe_float(r[12]),
            'entry_policy': str(r[13] or '').strip().upper(),
            'updated_at': str(r[14] or ''),
        }
        for r in rows_raw
    ]
    stats = {
        'fill_date': fill_td,
        'pending_due': len(rows),
        'pending': sum(1 for r in rows if r['status'] == PENDING_STATUS),
        'wait_data': sum(1 for r in rows if r['status'] == WAIT_DATA_STATUS),
        'pushed': False,
    }
    if push and rows:
        _push_t1_watchdog(rows, fill_td, stats)
        stats['pushed'] = True
    logger.info('[T1] pending watchdog stats: %s', stats)
    return stats


if __name__ == '__main__':
    import argparse

    logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
    parser = argparse.ArgumentParser(description='Shadow T+1 fill engine')
    parser.add_argument('--date', type=str, default=None)
    parser.add_argument('--mode', type=str, default=MAIN_MODE, choices=[PROBE_MODE, MAIN_MODE, FINAL_MODE])
    parser.add_argument('--limit', type=int, default=20)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    print(run_t1_fill_cycle(args.date, mode=args.mode, limit=args.limit, dry_run=args.dry_run))
