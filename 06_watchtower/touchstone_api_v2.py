#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
06_watchtower/touchstone_api_v2.py
Touchstone v2 single-symbol audit API.

Route:
- POST /api/v2/touchstone/audit

Contracts:
- All historical queries read from zhulong_api_readonly.duckdb snapshot only.
- No writes to DuckDB in this request chain.
- SaaS auth/billing are stubbed for future multi-tenant evolution.
"""

from __future__ import annotations

import hmac
import importlib
import importlib.util
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field

try:
    from saas_stub import TokenLedgerMock, get_current_user
except Exception:  # pragma: no cover
    from .saas_stub import TokenLedgerMock, get_current_user


BASE_DIR = Path('/root/quant_project')


def _load_dbgateway():
    try:
        mod = importlib.import_module('01_engine.lib.db_gateway')
        return mod.DBGateway
    except Exception:
        mod_path = BASE_DIR / '01_engine' / 'lib' / 'db_gateway.py'
        spec = importlib.util.spec_from_file_location('db_gateway_touchstone_v2', str(mod_path))
        if spec is None or spec.loader is None:
            raise RuntimeError(f'DBGateway loader unavailable: {mod_path}')
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod.DBGateway


DBGateway = _load_dbgateway()


class TouchstoneAuditRequest(BaseModel):
    symbol: str = Field(..., description='Stock code or Chinese name, e.g. 600519 or Guizhou Maotai')
    insight_query: str = Field(default='', description='Optional user question/insight for this symbol')


_CORE_LOCK = threading.Lock()
_CORE_CACHE = None


def _env_flag(name: str, default: str = '0') -> bool:
    return str(os.getenv(name, default)).strip().lower() in {'1', 'true', 'yes', 'on'}


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


TOUCHSTONE_USE_MODEL_L2 = _env_flag('TOUCHSTONE_USE_MODEL_L2', '0')
TOUCHSTONE_L2_TIMEOUT_SEC = max(3.0, _env_float('TOUCHSTONE_L2_TIMEOUT_SEC', 25.0))
TOUCHSTONE_AUTH_KEY_ENV = 'WATCHTOWER_TOUCHSTONE_KEY'
TOUCHSTONE_RATE_WINDOW_SEC = int(max(60, _env_float('WATCHTOWER_TOUCHSTONE_RATE_WINDOW_SEC', 600)))
TOUCHSTONE_RATE_MAX = int(max(1, _env_float('WATCHTOWER_TOUCHSTONE_RATE_MAX', 10)))
_TOUCHSTONE_RATE_LOCK = threading.Lock()
_TOUCHSTONE_RATE_BUCKETS: Dict[str, List[float]] = {}


def _client_ip(request: Request) -> str:
    forwarded = str(request.headers.get('cf-connecting-ip') or request.headers.get('x-forwarded-for') or '').split(',', 1)[0].strip()
    if forwarded:
        return forwarded
    return request.client.host if request.client else 'unknown'


def _require_touchstone_access(request: Request, provided_key: str) -> None:
    expected_key = str(os.getenv(TOUCHSTONE_AUTH_KEY_ENV, '') or '').strip()
    if not expected_key:
        raise HTTPException(status_code=503, detail='验金石访问口令尚未配置。')
    if not hmac.compare_digest(str(provided_key or '').strip(), expected_key):
        raise HTTPException(status_code=401, detail='验金石访问口令不正确。')

    now = time.time()
    client = _client_ip(request)
    with _TOUCHSTONE_RATE_LOCK:
        recent = [ts for ts in _TOUCHSTONE_RATE_BUCKETS.get(client, []) if now - ts < TOUCHSTONE_RATE_WINDOW_SEC]
        if len(recent) >= TOUCHSTONE_RATE_MAX:
            raise HTTPException(status_code=429, detail='验金石访问过于频繁，请稍后再试。')
        recent.append(now)
        _TOUCHSTONE_RATE_BUCKETS[client] = recent


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return default
    if out != out:  # NaN
        return default
    return out


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _enum_text(value: Any) -> str:
    if value is None:
        return ''
    if hasattr(value, 'value'):
        return str(getattr(value, 'value') or '').upper()
    return str(value).upper()


def _normalize_symbol(raw: str) -> str:
    text = str(raw or '').strip().upper()
    text = text.replace(' ', '').replace('?', '')
    if not text:
        return text
    if len(text) == 8 and text[:2] in {'SH', 'SZ'} and text[2:].isdigit():
        return f'{text[2:]}.{text[:2]}'
    if '.' in text:
        code, suffix = text.split('.', 1)
        if code.isdigit() and len(code) == 6 and suffix in {'SH', 'SZ'}:
            return f'{code}.{suffix}'
        return text
    if text.isdigit() and len(text) == 6:
        if text.startswith(('6', '9')):
            return f'{text}.SH'
        return f'{text}.SZ'
    return text


def _lookup_symbol_name(db_path: str, symbol: str, logger: logging.Logger) -> str:
    sym = str(symbol or '').strip().upper()
    if not sym:
        return ''
    try:
        with DBGateway(db_path, read_only=True, logger=logger) as conn:
            if _table_exists(conn, 'fact_stock_basic'):
                row = conn.execute(
                    "SELECT COALESCE(name, '') FROM fact_stock_basic WHERE symbol = ? LIMIT 1",
                    [sym],
                ).fetchone()
                if row and str(row[0] or '').strip():
                    return str(row[0] or '').strip()
            if _table_exists(conn, 'nexus_audits'):
                row = conn.execute(
                    """
                    SELECT COALESCE(name, '')
                    FROM nexus_audits
                    WHERE symbol = ? AND COALESCE(name, '') <> ''
                    ORDER BY trade_date DESC
                    LIMIT 1
                    """,
                    [sym],
                ).fetchone()
                if row and str(row[0] or '').strip():
                    return str(row[0] or '').strip()
    except Exception as exc:
        logger.warning('touchstone symbol name lookup failed symbol=%s err=%s', sym, exc)
    return ''


def _resolve_touchstone_symbol(db_path: str, raw: str, logger: logging.Logger) -> Dict[str, Any]:
    query = str(raw or '').strip()
    normalized = _normalize_symbol(query)
    if not query:
        return {'symbol': '', 'query': query, 'resolved_by': 'empty'}

    if normalized and normalized.upper().endswith(('.SH', '.SZ')):
        return {'symbol': normalized, 'query': query, 'resolved_by': 'code', 'name': _lookup_symbol_name(db_path, normalized, logger)}

    try:
        with DBGateway(db_path, read_only=True, logger=logger) as conn:
            lookup_sqls = []
            if _table_exists(conn, 'fact_stock_basic'):
                lookup_sqls.append(
                    (
                        """
                        SELECT symbol, COALESCE(name, '') AS name, 'fact_stock_basic' AS source,
                               CAST(updated_at AS VARCHAR) AS latest_trade_date, 1 AS hits
                        FROM fact_stock_basic
                        WHERE name = ? OR name LIKE ?
                        ORDER BY CASE WHEN name = ? THEN 0 ELSE 1 END, symbol
                        LIMIT 5
                        """,
                        [query, f'%{query}%', query],
                    )
                )
            if _table_exists(conn, 'nexus_audits'):
                lookup_sqls.append(
                    (
                        """
                        SELECT symbol, COALESCE(name, '') AS name, 'nexus_audits' AS source,
                               CAST(MAX(trade_date) AS VARCHAR) AS latest_trade_date, COUNT(*) AS hits
                        FROM nexus_audits
                        WHERE name = ? OR name LIKE ?
                        GROUP BY symbol, name
                        ORDER BY CASE WHEN name = ? THEN 0 ELSE 1 END, latest_trade_date DESC, hits DESC
                        LIMIT 5
                        """,
                        [query, f'%{query}%', query],
                    )
                )

            for sql, params in lookup_sqls:
                rows = conn.execute(sql, params).fetchall()
                if rows:
                    return {
                        'symbol': str(rows[0][0] or '').strip().upper(),
                        'query': query,
                        'resolved_by': 'name',
                        'name': str(rows[0][1] or ''),
                        'source': str(rows[0][2] or ''),
                        'candidates': [
                            {
                                'symbol': str(r[0] or ''),
                                'name': str(r[1] or ''),
                                'source': str(r[2] or ''),
                                'latest_trade_date': str(r[3] or ''),
                            }
                            for r in rows
                        ],
                    }
    except Exception as exc:
        logger.warning('touchstone symbol resolve failed query=%s err=%s', query, exc)

    return {'symbol': normalized, 'query': query, 'resolved_by': 'unresolved_name'}


def _table_exists(conn: Any, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE lower(table_name) = lower(?)
        LIMIT 1
        """,
        [table_name],
    ).fetchone()
    return bool(row)


def _read_symbol_snapshot(db_path: str, symbol: str, logger: logging.Logger) -> Optional[Dict[str, Any]]:
    try:
        with DBGateway(db_path, read_only=True, logger=logger) as conn:
            row = conn.execute(
                """
                WITH latest AS (
                    SELECT *
                    FROM fact_daily
                    WHERE symbol = ?
                    ORDER BY CAST(trade_date AS DATE) DESC
                    LIMIT 1
                )
                SELECT
                    CAST(d.trade_date AS VARCHAR) AS trade_date,
                    d.close,
                    d.pct_chg,
                    d.vol,
                    d.amount * 1000.0 AS amount,
                    d.turnover_rate,
                    COALESCE(d.vol_ma5, 0) AS vol_ma5,
                    COALESCE(r.rps_10, 0) AS rps_10,
                    COALESCE(z.lhb_net, d.lhb_net, 0) AS zeta_lhb_net,
                    COALESCE(z.inst_buy, 0) AS zeta_inst_buy,
                    COALESCE(z.hot_money, 0) AS zeta_hot_money,
                    COALESCE(z.margin_delta, d.margin_delta, 0) AS zeta_margin_delta,
                    COALESCE(z.block_trade_vol, 0) AS zeta_block_vol,
                    COALESCE(z.block_trade_premium, 0) AS zeta_block_premium
                FROM latest d
                LEFT JOIN fact_rps_results r
                  ON d.symbol = r.symbol
                 AND CAST(d.trade_date AS DATE) = CAST(r.trade_date AS DATE)
                LEFT JOIN fact_zeta_signals z
                  ON d.symbol = z.ts_code
                 AND CAST(d.trade_date AS DATE) = CAST(z.trade_date AS DATE)
                """,
                [symbol],
            ).fetchone()
            if not row:
                return None

            trade_date = str(row[0] or '')
            has_top = 0
            if _table_exists(conn, 'fact_top_list'):
                top_row = conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM fact_top_list
                    WHERE symbol = ?
                      AND CAST(trade_date AS DATE) = CAST(? AS DATE)
                    """,
                    [symbol, trade_date],
                ).fetchone()
                has_top = _safe_int(top_row[0] if top_row else 0)

            volume = _safe_float(row[3])
            vol_ma5 = _safe_float(row[6])
            return {
                'symbol': symbol,
                'trade_date': trade_date,
                'close': _safe_float(row[1]),
                'pct_chg': _safe_float(row[2]),
                'volume': volume,
                'amount': _safe_float(row[4]),
                'turnover': _safe_float(row[5]),
                'vol_ma5': vol_ma5,
                'vol_ratio': volume / vol_ma5 if vol_ma5 > 0 else 1.0,
                'rps_10': _safe_float(row[7]),
                'zeta_lhb_net': _safe_float(row[8]),
                'zeta_inst_buy': _safe_int(row[9]),
                'zeta_hot_money': _safe_int(row[10]),
                'zeta_margin_delta': _safe_float(row[11]),
                'zeta_block_vol': _safe_float(row[12]),
                'zeta_block_premium': _safe_float(row[13]),
                'has_top': has_top,
            }
    except Exception as exc:
        logger.warning('touchstone snapshot read failed symbol=%s err=%s', symbol, exc)
        return None


def _build_minimal_snapshot(symbol: str, name: str, realtime_slice: Dict[str, Any]) -> Dict[str, Any]:
    price = _safe_float(realtime_slice.get('price')) if isinstance(realtime_slice, dict) else 0.0
    volume = _safe_float(realtime_slice.get('volume')) if isinstance(realtime_slice, dict) else 0.0
    amount = _safe_float(realtime_slice.get('amount')) if isinstance(realtime_slice, dict) else 0.0
    return {
        'symbol': symbol,
        'name': name,
        'trade_date': time.strftime('%Y-%m-%d'),
        'close': price,
        'pct_chg': 0.0,
        'volume': volume,
        'amount': amount,
        'turnover': 0.0,
        'vol_ma5': 0.0,
        'vol_ratio': 1.0,
        'rps_10': 0.0,
        'zeta_lhb_net': 0.0,
        'zeta_inst_buy': 0,
        'zeta_hot_money': 0,
        'zeta_margin_delta': 0.0,
        'zeta_block_vol': 0.0,
        'zeta_block_premium': 0.0,
        'has_top': 0,
        'data_sparse': True,
        'snapshot_source': 'touchstone_minimal',
    }


def _first_number(payload: Dict[str, Any], keys: List[str]) -> Optional[float]:
    for key in keys:
        if key not in payload:
            continue
        val = payload.get(key)
        try:
            num = float(val)
            if num == num:
                return num
        except Exception:
            continue
    return None


def _fetch_tushare_realtime_slice(symbol: str, logger: logging.Logger) -> Dict[str, Any]:
    token = str(os.getenv('TUSHARE_TOKEN', '')).strip()
    if not token:
        return {
            'status': 'skipped',
            'source': 'tushare.realtime_quote',
            'reason': 'TUSHARE_TOKEN missing',
        }

    try:
        import tushare as ts

        ts.set_token(token)
        df = ts.realtime_quote(ts_code=symbol)
        if df is None or getattr(df, 'empty', True):
            return {
                'status': 'empty',
                'source': 'tushare.realtime_quote',
                'symbol': symbol,
            }

        row = df.iloc[0].to_dict() if hasattr(df, 'iloc') else {}
        price = _first_number(row, ['PRICE', 'price', 'current', 'last', 'LAST', 'close'])
        vol = _first_number(row, ['VOLUME', 'volume', 'vol'])
        amt = _first_number(row, ['AMOUNT', 'amount'])
        rt = str(row.get('TIME') or row.get('time') or row.get('DATE') or row.get('date') or '')
        return {
            'status': 'ok',
            'source': 'tushare.realtime_quote',
            'symbol': symbol,
            'price': price,
            'volume': vol,
            'amount': amt,
            'slice_time': rt,
        }
    except Exception as exc:
        logger.warning('touchstone realtime quote failed symbol=%s err=%s', symbol, exc)
        return {
            'status': 'error',
            'source': 'tushare.realtime_quote',
            'symbol': symbol,
            'error': f'{type(exc).__name__}:{exc}',
        }


def _load_touchstone_core(db_path: str, logger: logging.Logger):
    global _CORE_CACHE
    with _CORE_LOCK:
        if _CORE_CACHE is None:
            _CORE_CACHE = importlib.import_module('02_brain.decision_engine')

        # Force read-only snapshot path for historical reads in this API chain.
        try:
            _CORE_CACHE.DB_PATH = Path(db_path)
            _CORE_CACHE.TABLE_STOCK_DAILY = 'fact_daily'
        except Exception as exc:
            logger.warning('touchstone core path override warning: %s', exc)

        return _CORE_CACHE



def _run_touchstone_l2(core: Any, candidate: Any, logger: logging.Logger):
    l2 = core.L2SentinelAuditor()
    det = getattr(l2, '_deterministic_audit', None)

    if not TOUCHSTONE_USE_MODEL_L2 and callable(det):
        result = det(candidate, time.time())
        try:
            result.extraction_mode = 'TOUCHSTONE_RULES'
        except Exception:
            pass
        return result

    if not callable(det):
        return l2.audit(candidate)

    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='touchstone-l2')
    future = executor.submit(l2.audit, candidate)
    try:
        return future.result(timeout=TOUCHSTONE_L2_TIMEOUT_SEC)
    except FuturesTimeoutError:
        logger.warning(
            'touchstone L2 model timeout symbol=%s timeout=%.1fs; fallback to deterministic rules',
            getattr(candidate, 'symbol', ''),
            TOUCHSTONE_L2_TIMEOUT_SEC,
        )
        future.cancel()
        result = det(candidate, time.time())
        try:
            result.extraction_mode = 'TOUCHSTONE_RULES_TIMEOUT_FALLBACK'
        except Exception:
            pass
        return result
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

def _is_l2_garbage(l2_result: Any) -> bool:
    passed = bool(getattr(l2_result, 'passed', False))
    pattern = str(getattr(l2_result, 'pattern', '') or '').upper()
    risk = _safe_int(getattr(l2_result, 'risk_score', 100), 100)

    if not passed:
        return True
    if 'UNKNOWN' in pattern or 'PARSE' in pattern:
        return True
    if risk > 75:
        return True
    return False


def _build_light_reasons(snapshot: Optional[Dict[str, Any]], l2_result: Any, error_hint: str = '') -> List[Dict[str, str]]:
    reasons: List[Dict[str, str]] = []

    if error_hint:
        reasons.append(
            {
                'code': 'ENGINE_DEGRADED',
                'title': '审计引擎降级',
                'detail': error_hint,
            }
        )

    if snapshot is None:
        reasons.append(
            {
                'code': 'SNAPSHOT_MISSING',
                'title': '历史切片缺失',
                'detail': '只读快照中未找到该标的历史行情，无法进入深度审计。',
            }
        )

    if l2_result is not None:
        pattern = str(getattr(l2_result, 'pattern', 'UNKNOWN') or 'UNKNOWN')
        risk_score = _safe_int(getattr(l2_result, 'risk_score', 100), 100)
        reasons.append(
            {
                'code': 'L2_PATTERN_RISK',
                'title': 'L2 形态高风险',
                'detail': f'pattern={pattern}, risk_score={risk_score}，不满足进阶审计门槛。',
            }
        )

    if snapshot is not None:
        amount = _safe_float(snapshot.get('amount', 0.0))
        turnover = _safe_float(snapshot.get('turnover', 0.0))
        pct_chg = _safe_float(snapshot.get('pct_chg', 0.0))

        if amount < 20_000_000:
            reasons.append(
                {
                    'code': 'LIQUIDITY_DRY',
                    'title': '流动性枯竭',
                    'detail': f'amount={amount:.0f}，低于验金石默认流动性阈值。',
                }
            )
        if turnover < 1.0:
            reasons.append(
                {
                    'code': 'TURNOVER_WEAK',
                    'title': '换手不足',
                    'detail': f'turnover={turnover:.2f}，博弈强度偏弱。',
                }
            )
        if pct_chg < -2.0:
            reasons.append(
                {
                    'code': 'MOMENTUM_BEAR',
                    'title': '短动量偏空',
                    'detail': f'pct_chg={pct_chg:.2f}%，价格动量处于弱势。',
                }
            )

    while len(reasons) < 3:
        reasons.append(
            {
                'code': f'GENERIC_{len(reasons)+1}',
                'title': '结构化轻诊断',
                'detail': '当前票未通过验金石深度审计门槛，建议仅做观察。',
            }
        )

    # Keep only 3 structured reasons by contract.
    return reasons[:3]


def _read_macro_v2_assessment(db_path: str, symbol: str, logger: logging.Logger) -> Dict[str, Any]:
    try:
        with DBGateway(db_path, read_only=True, logger=logger) as conn:
            if not _table_exists(conn, 'fact_macro_micro_overlay'):
                return {'status': 'unavailable', 'reason': 'fact_macro_micro_overlay missing'}

            row = conn.execute(
                """
                SELECT
                    CAST(o.trade_date AS VARCHAR) AS trade_date,
                    o.signal_label,
                    o.topic_type,
                    o.topic_id,
                    o.macro_score,
                    o.anti_fake_flag,
                    d.llm_resonance_score,
                    d.is_event_driven_trap,
                    COALESCE(d.topic_name, o.topic_id) AS topic_name
                FROM fact_macro_micro_overlay o
                LEFT JOIN fact_macro_topic_daily d
                  ON d.trade_date = o.trade_date
                 AND d.topic_type = o.topic_type
                 AND d.topic_id = o.topic_id
                WHERE o.symbol = ?
                ORDER BY o.trade_date DESC, o.composite_score DESC
                LIMIT 1
                """,
                [symbol],
            ).fetchone()

            if not row:
                return {'status': 'no_data', 'symbol': symbol}

            return {
                'status': 'ok',
                'symbol': symbol,
                'trade_date': str(row[0] or ''),
                'signal_label': str(row[1] or ''),
                'topic_type': str(row[2] or ''),
                'topic_id': str(row[3] or ''),
                'macro_score': _safe_float(row[4], 0.0),
                'anti_fake_flag': _safe_int(row[5], 0),
                'llm_resonance_score': None if row[6] is None else _safe_int(row[6], 0),
                'is_event_driven_trap': bool(row[7]) if row[7] is not None else False,
                'topic_name': str(row[8] or ''),
            }
    except Exception as exc:
        logger.warning('touchstone macro read failed symbol=%s err=%s', symbol, exc)
        return {'status': 'error', 'symbol': symbol, 'reason': f'{type(exc).__name__}:{exc}'}


def register_touchstone_routes(app: FastAPI, db_path: str, logger: logging.Logger) -> None:
    @app.post('/api/v2/touchstone/audit')
    def touchstone_audit(
        payload: TouchstoneAuditRequest,
        request: Request,
        x_watchtower_key: str = Header(default='', alias='X-Watchtower-Key'),
        current_user: Dict[str, str] = Depends(get_current_user),
    ):
        _require_touchstone_access(request=request, provided_key=x_watchtower_key)
        resolved_symbol = _resolve_touchstone_symbol(db_path=db_path, raw=payload.symbol, logger=logger)
        symbol = str(resolved_symbol.get('symbol') or '')
        insight_query = str(payload.insight_query or '').strip()
        user_id = str(current_user.get('user_id', 'local_admin'))

        if not symbol:
            raise HTTPException(status_code=400, detail='symbol is required')

        billing = TokenLedgerMock()
        if not billing.reserve(user_id=user_id, token_amount=1):
            raise HTTPException(status_code=402, detail='base token reserve failed')

        display_name = str(resolved_symbol.get('name') or '').strip() or _lookup_symbol_name(db_path, symbol, logger)
        if display_name and not resolved_symbol.get('name'):
            resolved_symbol['name'] = display_name

        snapshot = _read_symbol_snapshot(db_path=db_path, symbol=symbol, logger=logger)
        realtime_slice = _fetch_tushare_realtime_slice(symbol=symbol, logger=logger)
        if snapshot is not None:
            snapshot['name'] = display_name
            snapshot.setdefault('snapshot_source', 'fact_daily')
        else:
            snapshot = _build_minimal_snapshot(symbol=symbol, name=display_name, realtime_slice=realtime_slice)
            logger.warning('touchstone snapshot missing symbol=%s; continuing with minimal L2/L4 context', symbol)

        core = _load_touchstone_core(db_path=db_path, logger=logger)
        candidate = core.Candidate(
            symbol=symbol,
            name=display_name,
            trade_date=str(snapshot.get('trade_date', '')),
            close=_safe_float(snapshot.get('close')),
            pct_chg=_safe_float(snapshot.get('pct_chg')),
            volume=_safe_float(snapshot.get('volume')),
            amount=_safe_float(snapshot.get('amount')),
            turnover=_safe_float(snapshot.get('turnover')),
            rps_10=_safe_float(snapshot.get('rps_10')),
            vol_ratio=_safe_float(snapshot.get('vol_ratio'), 1.0),
            zeta_lhb_net=_safe_float(snapshot.get('zeta_lhb_net')),
            zeta_inst_buy=_safe_int(snapshot.get('zeta_inst_buy')),
            zeta_hot_money=_safe_int(snapshot.get('zeta_hot_money')),
            zeta_margin_delta=_safe_float(snapshot.get('zeta_margin_delta')),
            zeta_block_vol=_safe_float(snapshot.get('zeta_block_vol')),
            zeta_block_premium=_safe_float(snapshot.get('zeta_block_premium')),
        )

        try:
            l2_result = _run_touchstone_l2(core, candidate, logger)
        except Exception as exc:
            reasons = _build_light_reasons(snapshot=snapshot, l2_result=None, error_hint=f'L2_EXCEPTION:{type(exc).__name__}')
            billing.consume(user_id=user_id, token_amount=1)
            return {
                'success': True,
                'mode': 'L2_GATE_BLOCKED',
                'symbol': symbol,
                'name': display_name,
                'user': current_user,
                'resolved_symbol': resolved_symbol,
                'billing': {
                    'base_audit_reserved': True,
                    'advanced_ai_reserved': False,
                    'consumed_tokens': 1,
                },
                'realtime_slice': realtime_slice,
                'l2': {
                    'pattern': 'L2_EXCEPTION',
                    'risk_score': 100,
                    'passed': False,
                    'fact_tags': ['#L2_EXCEPTION'],
                },
                'diagnosis': reasons,
                'message': 'L2 体检异常，已降级为轻量诊断。',
            }

        fact_tags = [str(tag) for tag in (getattr(l2_result, 'fact_tags', []) or [])]
        if snapshot.get('data_sparse') and '#NO_SNAPSHOT' not in fact_tags:
            fact_tags.insert(0, '#NO_SNAPSHOT')
        l2_payload = {
            'pattern': str(getattr(l2_result, 'pattern', 'UNKNOWN') or 'UNKNOWN'),
            'risk_score': _safe_int(getattr(l2_result, 'risk_score', 100), 100),
            'passed': bool(getattr(l2_result, 'passed', False)),
            'fact_tags': fact_tags[:8],
            'elapsed_ms': _safe_float(getattr(l2_result, 'elapsed_ms', 0.0), 0.0),
            'extraction_mode': str(getattr(l2_result, 'extraction_mode', '') or ''),
        }

        if _is_l2_garbage(l2_result):
            reasons = _build_light_reasons(snapshot=snapshot, l2_result=l2_result)
            billing.consume(user_id=user_id, token_amount=1)
            return {
                'success': True,
                'mode': 'L2_GATE_BLOCKED',
                'symbol': symbol,
                'name': display_name,
                'user': current_user,
                'resolved_symbol': resolved_symbol,
                'billing': {
                    'base_audit_reserved': True,
                    'advanced_ai_reserved': False,
                    'consumed_tokens': 1,
                },
                'realtime_slice': realtime_slice,
                'l2': l2_payload,
                'diagnosis': reasons,
                'message': 'L2 判定为垃圾形态，已停止深入 AI 审计。',
            }

        if not billing.reserve(user_id=user_id, token_amount=10):
            raise HTTPException(status_code=402, detail='advanced ai token reserve failed')

        try:
            # Touchstone is an on-demand reference tool: keep cheap L1/L2 gates,
            # skip the batch-oriented L3 auditor, and bridge clean L2 context into L4.
            l2_risk = _safe_int(getattr(l2_result, 'risk_score', 50), 50)
            bridge_score = max(50, min(88, 100 - l2_risk))
            bridge_reasoning = (
                f'Touchstone direct-reference mode: snapshot_source={snapshot.get("snapshot_source", "unknown")}, data_sparse={bool(snapshot.get("data_sparse"))}, and L2 fast gate passed. '
                f'pattern={l2_payload["pattern"]}, risk_score={l2_risk}. '
                'L3 batch audit intentionally skipped; L4 should provide decision reference, not an execution order.'
            )
            l3_context = core.L3Result(
                symbol=symbol,
                verdict=core.Verdict.PASS,
                audit_score=bridge_score,
                reasoning=bridge_reasoning,
                parse_failed=False,
                l2_risk_score=l2_risk,
                fact_tags=list(l2_payload.get('fact_tags') or []),
            )

            l4 = core.L4CloudArbiter()
            l4_result = l4.audit(candidate, l3_context, is_top3=True, rag_intel=insight_query)

            macro_v2 = _read_macro_v2_assessment(db_path=db_path, symbol=symbol, logger=logger)

            l3_payload = {
                'skipped': True,
                'verdict': 'SKIPPED',
                'audit_score': bridge_score,
                'reasoning': '验金石模式已略过 L3 批量深审，将可用行情切片与 L2 Python 快检结果送入 L4 作为参考上下文。',
                'parse_failed': False,
            }
            l4_payload = {
                'final_verdict': _enum_text(getattr(l4_result, 'final_verdict', 'UNKNOWN')),
                'final_score': _safe_int(getattr(l4_result, 'final_score', 0), 0),
                'veto_applied': bool(getattr(l4_result, 'veto_applied', False)),
                'veto_reason': str(getattr(l4_result, 'veto_reason', '') or '')[:240],
                'recommendation': str(getattr(l4_result, 'recommendation', '') or '')[:400],
                'notary_verdict': str(getattr(l4_result, 'notary_verdict', '') or ''),
                'notary_fatal_flag': bool(getattr(l4_result, 'notary_fatal_flag', False)),
                'notary_advisory': str(getattr(l4_result, 'notary_advisory', '') or '')[:240],
            }

            billing.consume(user_id=user_id, token_amount=11)
            return {
                'success': True,
                'mode': 'L1_L2_L4_REFERENCE',
                'symbol': symbol,
                'name': display_name,
                'insight_query': insight_query,
                'user': current_user,
                'resolved_symbol': resolved_symbol,
                'billing': {
                    'base_audit_reserved': True,
                    'advanced_ai_reserved': True,
                    'consumed_tokens': 11,
                },
                'snapshot': snapshot,
                'realtime_slice': realtime_slice,
                'l2': l2_payload,
                'l3': l3_payload,
                'l4': l4_payload,
                'macro_v2': macro_v2,
                'message': '验金石参考完成：已用可用行情切片执行 L2 Python 快检，并进入 L4 参考裁决。',
            }
        except Exception as exc:
            billing.consume(user_id=user_id, token_amount=11)
            raise HTTPException(status_code=500, detail=f'touchstone l4 reference failed: {type(exc).__name__}: {exc}')
