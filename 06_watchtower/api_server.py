#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
06_watchtower/api_server.py
FastAPI Watchtower - Read-Only Observatory
"""

from __future__ import annotations

import hmac
import logging
from logging.handlers import RotatingFileHandler
import os
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import importlib
import importlib.util
import requests
import uvicorn
from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field


BASE_DIR = Path('/root/quant_project')
GOVERNANCE_DIR = BASE_DIR / '04_governance'
if str(GOVERNANCE_DIR) not in sys.path:
    sys.path.insert(0, str(GOVERNANCE_DIR))

from config.settings import AUDIT_PASS, Config, ensure_syspath, get_node_config

ensure_syspath()


def _load_dbgateway():
    try:
        mod = importlib.import_module('01_engine.lib.db_gateway')
        return mod.DBGateway
    except Exception:
        mod_path = BASE_DIR / '01_engine' / 'lib' / 'db_gateway.py'
        spec = importlib.util.spec_from_file_location('db_gateway_01_watchtower', str(mod_path))
        if spec is None or spec.loader is None:
            raise RuntimeError(f'DBGateway loader unavailable: {mod_path}')
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod.DBGateway


DBGateway = _load_dbgateway()


def _load_computegateway():
    try:
        mod = importlib.import_module('02_brain.lib.compute_gateway')
        return mod.ComputeGateway
    except Exception:
        mod_path = BASE_DIR / '02_brain' / 'lib' / 'compute_gateway.py'
        spec = importlib.util.spec_from_file_location('compute_gateway_02_watchtower', str(mod_path))
        if spec is None or spec.loader is None:
            raise RuntimeError(f'ComputeGateway loader unavailable: {mod_path}')
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod.ComputeGateway


ComputeGateway = _load_computegateway()

try:
    from lib.core.push_service import get_notifier
except Exception:
    get_notifier = None

PROJECT_ROOT = Path(Config.PROJECT_ROOT)
SHADOW_LIB_DIR = PROJECT_ROOT / '05_shadow' / 'lib'
if str(SHADOW_LIB_DIR) not in sys.path:
    sys.path.insert(0, str(SHADOW_LIB_DIR))

try:
    from costs import calculate_trade_cost
    from performance import summarize_closed_trades
except Exception:
    calculate_trade_cost = None  # type: ignore
    summarize_closed_trades = None  # type: ignore
LOG_DIR = Path(Config.LOG_DIR)
LOG_DIR.mkdir(parents=True, exist_ok=True)
DAEMON_LOG_FILE = LOG_DIR / 'daemon.log'
DB_PATH = str(PROJECT_ROOT / 'storage' / 'database' / 'zhulong_api_readonly.duckdb')
NODE_CFG = get_node_config()
OLLAMA_NODE_116_BASE = str(Config.OLLAMA_URL).split('/api/', 1)[0]
OLLAMA_TAGS_URL = f'{OLLAMA_NODE_116_BASE}/api/tags'
OLLAMA_GENERATE_URL = f'{OLLAMA_NODE_116_BASE}/api/generate'
OLLAMA_MODEL = str(getattr(Config, 'OLLAMA_MODEL', 'deepseek-r1:7b'))
OLLAMA_ALERT_THRESHOLD_SECONDS = 30.0
OLLAMA_PROBE_INTERVAL_SECONDS = 60
OLLAMA_PROBE_TIMEOUT_SECONDS = 40
ALERT_COOLDOWN_SECONDS = 600

logger = logging.getLogger('watchtower')
PROBE_GATEWAY = ComputeGateway(logger=logger, max_slots=3)

app = FastAPI(
    title='Zhulong Watchtower',
    version='1.0-rc',
    description='Read-only observatory for Zhulong quant system',
)


def _watchtower_allowed_origins() -> List[str]:
    raw = str(os.getenv('WATCHTOWER_ALLOWED_ORIGINS', '') or '').strip()
    if raw:
        return [item.strip() for item in raw.split(',') if item.strip()]
    return ['http://127.0.0.1:5173', 'http://localhost:5173']


app.add_middleware(
    CORSMiddleware,
    allow_origins=_watchtower_allowed_origins(),
    allow_methods=['GET', 'POST'],
    allow_headers=['Content-Type', 'X-Watchtower-Key'],
)


WATCHTOWER_AUTH_ENV = 'WATCHTOWER_TOUCHSTONE_KEY'


def _watchtower_key_ok(value: str) -> bool:
    expected = str(os.getenv(WATCHTOWER_AUTH_ENV, '') or '').strip()
    supplied = str(value or '').strip()
    return bool(expected) and bool(supplied) and hmac.compare_digest(supplied, expected)


def _require_watchtower_key(value: str) -> None:
    if not _watchtower_key_ok(value):
        raise HTTPException(status_code=401, detail='访问口令不正确。')


@app.exception_handler(HTTPException)
async def _http_exc_handler(_: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            'ok': False,
            'error': 'HTTP_ERROR',
            'detail': str(exc.detail),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        },
    )


@app.exception_handler(Exception)
async def _unhandled_exc_handler(_: Request, exc: Exception):
    logger.error(f'watchtower unhandled exception: {exc}', exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            'ok': False,
            'error': 'INTERNAL_ERROR',
            'detail': str(exc),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        },
    )


@contextmanager
def get_db():
    if not os.path.exists(DB_PATH):
        raise HTTPException(
            status_code=503,
            detail=f'API readonly snapshot missing: {DB_PATH}. Wait for daemon snapshot refresh.',
        )
    with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
        yield conn


def _get_table_columns(conn, table: str) -> set[str]:
    try:
        return {str(r[1]).lower() for r in conn.execute(f"PRAGMA table_info('{table}')").fetchall()}
    except Exception:
        return set()


def _pick_col(columns: set[str], *candidates: str) -> str:
    for c in candidates:
        if c.lower() in columns:
            return c
    return ''


def _expr_or_null(col: str, cast_type: str = 'DOUBLE') -> str:
    if col:
        return col
    return f'CAST(NULL AS {cast_type})'


def _normalize_empty_bucket(final_verdict: str, veto_reason: str, status: str) -> str:
    fv = str(final_verdict or '').strip().upper()
    vr = str(veto_reason or '').strip().upper()
    st = str(status or '').strip().upper()
    if fv != 'VETO':
        return ''
    if vr == 'L3_VETO_GATE' or st.startswith('L3_VETO'):
        return 'L3_INTERCEPT'
    if vr.startswith('NOTARY HARD VETO'):
        return 'L4_NOTARY_HARD_VETO'
    if st == 'L4_DONE':
        return 'L4_JUDGE_VETO'
    if st in {'L2_REJECTED_TERMINAL', 'L2_FAILED_TERMINAL'}:
        return 'L2_TERMINAL'
    return 'OTHER_VETO'

def _safe_parse_dt(raw: Any) -> Optional[datetime]:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw
    val = str(raw).strip()
    if not val:
        return None
    for fmt in (
        '%Y-%m-%d %H:%M:%S.%f',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%dT%H:%M:%S.%f',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%d',
    ):
        try:
            return datetime.strptime(val, fmt)
        except Exception:
            continue
    try:
        return datetime.fromisoformat(val.replace('Z', '+00:00')).replace(tzinfo=None)
    except Exception:
        return None


def _clean_veto_semantics(final_verdict: str, veto_reason: str) -> tuple[str, str]:
    fv = str(final_verdict or '').strip().upper()
    vr = str(veto_reason or '').strip()
    if fv != 'VETO' and vr:
        return '', 'veto_reason_cleared_non_veto'
    return vr, ''




def _resolve_latest_run_id(conn, trade_date: str) -> str:
    td = str(trade_date or '').strip()
    if not td:
        return ''
    cols = _get_table_columns(conn, 'nexus_audits')
    if 'run_id' not in cols:
        return ''

    last_touch_col = _pick_col(cols, 'completed_at', 'created_at')
    if last_touch_col:
        last_touch_expr = f'MAX(COALESCE({last_touch_col}, created_at))'
    else:
        last_touch_expr = 'MAX(created_at)'

    row = conn.execute(
        f"""
        SELECT
            run_id,
            COUNT(*) AS total_rows,
            SUM(CASE WHEN COALESCE(l4_final_verdict, '') <> '' THEN 1 ELSE 0 END) AS l4_filled_rows,
            {last_touch_expr} AS last_touch
        FROM nexus_audits
        WHERE trade_date = ?
          AND run_id IS NOT NULL
          AND TRIM(run_id) <> ''
        GROUP BY run_id
        ORDER BY
            l4_filled_rows DESC,
            total_rows DESC,
            last_touch DESC NULLS LAST,
            run_id DESC
        LIMIT 1
        """,
        [td],
    ).fetchone()
    return str(row[0] or '').strip() if row else ''


def _build_empty_reason_snapshot(conn, trade_date: str, run_id: str) -> Dict[str, Any]:
    td = str(trade_date or '').strip()
    rid = str(run_id or '').strip()
    empty = {
        'run_rows': 0,
        'selected_count': 0,
        'l2_passed': 0,
        'l3_veto_gate': 0,
        'l4_judge_veto': 0,
        'notary_hard_veto': 0,
        'l2_terminal': 0,
        'veto_top': [],
    }
    if not td or not rid:
        return empty

    stats_row = conn.execute(
        f"""
        SELECT
            COUNT(1) AS run_rows,
            SUM(CASE WHEN UPPER(COALESCE(l4_final_verdict, '')) = ? THEN 1 ELSE 0 END) AS selected_count,
            SUM(CASE WHEN COALESCE(l2_passed, FALSE) THEN 1 ELSE 0 END) AS l2_passed,
            SUM(CASE WHEN UPPER(COALESCE(l4_veto_reason, '')) = 'L3_VETO_GATE' THEN 1 ELSE 0 END) AS l3_veto_gate,
            SUM(CASE WHEN UPPER(COALESCE(status, '')) = 'L4_DONE'
                      AND UPPER(COALESCE(l4_final_verdict, '')) = 'VETO' THEN 1 ELSE 0 END) AS l4_judge_veto,
            SUM(CASE WHEN UPPER(COALESCE(l4_veto_reason, '')) LIKE 'NOTARY HARD VETO%' THEN 1 ELSE 0 END) AS notary_hard_veto,
            SUM(CASE WHEN UPPER(COALESCE(status, '')) IN ('L2_REJECTED_TERMINAL', 'L2_FAILED_TERMINAL') THEN 1 ELSE 0 END) AS l2_terminal
        FROM nexus_audits
        WHERE trade_date = ? AND run_id = ?
        """,
        [AUDIT_PASS, td, rid],
    ).fetchone()

    if stats_row:
        empty.update(
            {
                'run_rows': int(stats_row[0] or 0),
                'selected_count': int(stats_row[1] or 0),
                'l2_passed': int(stats_row[2] or 0),
                'l3_veto_gate': int(stats_row[3] or 0),
                'l4_judge_veto': int(stats_row[4] or 0),
                'notary_hard_veto': int(stats_row[5] or 0),
                'l2_terminal': int(stats_row[6] or 0),
            }
        )

    veto_rows = conn.execute(
        """
        SELECT COALESCE(NULLIF(TRIM(l4_veto_reason), ''), 'UNKNOWN') AS veto_reason, COUNT(1) AS cnt
        FROM nexus_audits
        WHERE trade_date = ? AND run_id = ? AND UPPER(COALESCE(l4_final_verdict, '')) = 'VETO'
        GROUP BY 1
        ORDER BY cnt DESC
        LIMIT 3
        """,
        [td, rid],
    ).fetchall()
    empty['veto_top'] = [{'reason': str(r[0]), 'count': int(r[1] or 0)} for r in veto_rows]
    return empty


class HealthResponse(BaseModel):
    status: str
    timestamp: str
    daemon_pid: Optional[int] = None
    db_size_mb: float
    latest_trade_date: str
    stock_count: int


class AuditRecord(BaseModel):
    run_id: str = ''
    task_id: str = ''
    symbol: str = ''
    name: str = ''
    trade_date: str = ''
    l2_passed: Optional[bool] = None
    l2_risk_score: Optional[int] = None
    l3_verdict: str = ''
    l3_audit_score: Optional[int] = None
    l4_final_verdict: str = ''
    l4_veto_reason: str = ''
    l4_notary_verdict: str = ''
    l4_notary_fatal_flag: Optional[bool] = None
    empty_reason_bucket: str = ''
    l4_red_score: Optional[float] = None
    l4_blue_score: Optional[float] = None
    status: str = ''
    data_quality_warning: str = ''
    created_at: str = ''


class ShadowRequest(BaseModel):

    stock_code: str = Field(..., description='Stock code e.g. 600519.SH')
    risk_profile: float = Field(0.5, ge=0.0, le=1.0, description='Risk slider 0~1')


class ShadowResponse(BaseModel):
    stock_code: str
    risk_profile: float
    mode: str = 'Golden'
    shadow_fills: List[Dict[str, Any]] = []
    slippage_config: Dict[str, float] = {}
    message: str = ''


class ShadowPosition(BaseModel):
    symbol: str
    name: str = ''
    trade_date: str
    status: str
    qty: int = 0
    entry_price: float = 0.0
    current_price: float = 0.0
    market_value: float = 0.0
    cost_basis: float = 0.0
    pnl_amount: float = 0.0
    pnl_ratio: float = 0.0
    hold_days: Optional[int] = None
    dynamic_stop_price: Optional[float] = None
    decision_state: str = ''
    quality_label: str = ''
    entry_score: Optional[float] = None
    source: str = ''
    strategy_tag: str = ''
    updated_at: str = ''


class ShadowHistoryTrade(BaseModel):
    symbol: str
    name: str = ''
    entry_date: str = ''
    exit_date: str = ''
    entry_price: float = 0.0
    exit_price: float = 0.0
    qty: int = 0
    entry_score: Optional[float] = None
    pnl_amount: float = 0.0
    pnl_ratio: float = 0.0
    sell_rule: str = ''
    sell_reason: str = ''


class ShadowPerformanceSummary(BaseModel):
    total_positions: int = 0
    open_positions: int = 0
    closed_trades: int = 0
    win_trades: int = 0
    loss_trades: int = 0
    win_rate: float = 0.0
    realized_pnl_amount: float = 0.0
    unrealized_pnl_amount: float = 0.0
    total_pnl_amount: float = 0.0
    avg_closed_pnl_ratio: float = 0.0
    profit_loss_ratio: float = 0.0
    avg_win_amount: float = 0.0
    avg_loss_amount: float = 0.0
    best_trade_pnl_amount: float = 0.0
    worst_trade_pnl_amount: float = 0.0
    expectancy_amount: float = 0.0
    max_drawdown_ratio: float = 0.0
    best_trade_symbol: str = ''
    best_trade_name: str = ''
    best_trade_pnl_ratio: float = 0.0
    worst_trade_symbol: str = ''
    worst_trade_name: str = ''
    worst_trade_pnl_ratio: float = 0.0
    buy_count: int = 0
    sell_count: int = 0


class ShadowPortfolioResponse(BaseModel):
    trade_date: str = ''
    total_equity: float = 0.0
    cash_reserve: float = 0.0
    invested_amount: float = 0.0
    market_value: float = 0.0
    pnl_amount: float = 0.0
    pnl_ratio: float = 0.0
    active_positions: int = 0
    max_positions: int = 4
    positions: List[ShadowPosition] = []
    performance: ShadowPerformanceSummary = Field(default_factory=ShadowPerformanceSummary)
    history_trades: List[ShadowHistoryTrade] = []
    message: str = ''


class AlphaCard(BaseModel):
    stock_code: str
    stock_name: str
    l4_verdict: Dict[str, Any]
    shadow_metrics: Dict[str, Any]


class DailyAlphaResponse(BaseModel):
    trade_date: str
    total_audited: int
    approved_count: int
    cards: List[AlphaCard]


class TokenBucket(BaseModel):
    provider: str
    model: str
    call_count: int
    est_prompt_tokens: int
    est_completion_tokens: int
    est_cost_cny: float


class TokenResponse(BaseModel):
    trade_date: str
    total_calls: int
    total_est_cost_cny: float
    buckets: List[TokenBucket]
    note: str


class PipelineStatus(BaseModel):
    state: str
    daemon_alive: bool
    daemon_pid: Optional[int] = None
    phase3_running: bool
    latest_audit_date: str
    today_audit_count: int
    today_l4_count: int
    last_heartbeat: str
    message: str


class LivenessResponse(BaseModel):
    timestamp: str
    daemon_last_phase_age_s: Optional[float] = None
    l4_filled_ratio_last_1h: Optional[float] = None
    poller_last_run_age_s: Optional[float] = None
    notary_parse_fail_rate_last_1h: Optional[float] = None
    ollama_api_latency_p95_ms: Optional[float] = None


class OllamaNode116Status(BaseModel):
    node: str
    profile: str
    model: str
    ok: bool
    tags_ok: bool
    generate_ok: bool
    tags_latency_s: float
    generate_latency_s: float
    max_latency_s: float
    last_checked: str
    last_error: str
    consecutive_failures: int


_probe_lock = threading.Lock()
_probe_stop_event = threading.Event()
_probe_thread: Optional[threading.Thread] = None
_probe_state: Dict[str, Any] = {
    'ok': False,
    'tags_ok': False,
    'generate_ok': False,
    'tags_latency_s': 0.0,
    'generate_latency_s': 0.0,
    'max_latency_s': 0.0,
    'last_checked': '',
    'last_error': 'not probed yet',
    'consecutive_failures': 0,
    'last_alert_ts': 0.0,
}



def _safe_close_response_obj(resp: Any) -> None:
    try:
        if resp is not None and hasattr(resp, 'close'):
            resp.close()
    except Exception:
        pass

def _safe_push_alert(title: str, content: str) -> None:
    if get_notifier is None:
        logger.warning('push_service unavailable, skip alert')
        return
    try:
        notifier = get_notifier()
        notifier.error(title, content)
    except Exception as exc:
        logger.warning(f'push alert failed: {exc}')


def _probe_tags() -> tuple[bool, float, str]:
    start = time.perf_counter()
    try:
        with requests.get(OLLAMA_TAGS_URL, timeout=OLLAMA_PROBE_TIMEOUT_SECONDS) as r:
            elapsed = time.perf_counter() - start
            if r.status_code != 200:
                return False, elapsed, f'/api/tags status={r.status_code}'
            return True, elapsed, ''
    except Exception as exc:
        elapsed = time.perf_counter() - start
        return False, elapsed, f'/api/tags error={exc}'


def _probe_generate() -> tuple[bool, float, str]:
    start = time.perf_counter()
    payload = {
        'model': OLLAMA_MODEL,
        'prompt': 'ping',
        'stream': False,
        'keep_alive': '2m',
        'options': {'num_thread': int(NODE_CFG.get('num_thread', 1))},
    }
    r = None
    try:
        r = PROBE_GATEWAY.ollama_generate(
            server=OLLAMA_NODE_116_BASE,
            payload=payload,
            timeout=OLLAMA_PROBE_TIMEOUT_SECONDS,
            layer='WATCH',
            decision_id='watchtower:probe-generate',
        )
        elapsed = time.perf_counter() - start
        if r.status_code != 200:
            return False, elapsed, f'/api/generate status={r.status_code}'
        return True, elapsed, ''
    except Exception as exc:
        elapsed = time.perf_counter() - start
        return False, elapsed, f'/api/generate error={exc}'
    finally:
        _safe_close_response_obj(r)


def check_ollama_node_116(force_alert: bool = False) -> Dict[str, Any]:
    tags_ok, tags_latency, tags_err = _probe_tags()
    gen_ok, gen_latency, gen_err = _probe_generate()

    ok = tags_ok and gen_ok
    max_latency = max(tags_latency, gen_latency)
    err = '; '.join([e for e in (tags_err, gen_err) if e]).strip()

    now_ts = time.time()
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    with _probe_lock:
        if ok:
            _probe_state['consecutive_failures'] = 0
        else:
            _probe_state['consecutive_failures'] = int(_probe_state.get('consecutive_failures', 0)) + 1

        _probe_state.update(
            {
                'ok': ok,
                'tags_ok': tags_ok,
                'generate_ok': gen_ok,
                'tags_latency_s': round(tags_latency, 3),
                'generate_latency_s': round(gen_latency, 3),
                'max_latency_s': round(max_latency, 3),
                'last_checked': now_str,
                'last_error': err,
            }
        )

        should_alert = force_alert or (max_latency > OLLAMA_ALERT_THRESHOLD_SECONDS)
        cooldown_passed = (now_ts - float(_probe_state.get('last_alert_ts', 0))) >= ALERT_COOLDOWN_SECONDS

        if should_alert and cooldown_passed:
            _probe_state['last_alert_ts'] = now_ts
            _safe_push_alert(
                title='Ollama 116 latency alert',
                content=(
                    f'<b>Node:</b> 192.0.2.20<br>'
                    f'<b>Max latency:</b> {max_latency:.3f}s (threshold {OLLAMA_ALERT_THRESHOLD_SECONDS:.0f}s)<br>'
                    f'<b>/api/tags:</b> ok={tags_ok}, latency={tags_latency:.3f}s<br>'
                    f'<b>/api/generate:</b> ok={gen_ok}, latency={gen_latency:.3f}s<br>'
                    f'<b>Model:</b> {OLLAMA_MODEL}<br>'
                    f'<b>Node profile:</b> {NODE_CFG.get("profile", "unknown")}<br>'
                    f'<b>Error:</b> {err or "none"}<br>'
                    f'<b>Time:</b> {now_str}'
                ),
            )

        return dict(_probe_state)


def _probe_loop() -> None:
    logger.info('start check_ollama_node_116 loop (interval=60s)')
    while not _probe_stop_event.is_set():
        try:
            check_ollama_node_116()
        except Exception as exc:
            logger.warning(f'check_ollama_node_116 loop error: {exc}')
        _probe_stop_event.wait(OLLAMA_PROBE_INTERVAL_SECONDS)


@app.on_event('startup')
def _on_startup() -> None:
    global _probe_thread
    if _probe_thread is None or not _probe_thread.is_alive():
        _probe_stop_event.clear()
        _probe_thread = threading.Thread(target=_probe_loop, name='watchtower-116-probe', daemon=True)
        _probe_thread.start()
    if not os.path.exists(DB_PATH):
        logger.warning(f'readonly snapshot not ready: {DB_PATH}')


@app.on_event('shutdown')
def _on_shutdown() -> None:
    _probe_stop_event.set()


@app.get('/health', response_model=HealthResponse)
def health_check():
    pid = None
    try:
        r = subprocess.run(['pgrep', '-f', 'zhulong_daemon.py'], capture_output=True, text=True, timeout=5)
        pids = [int(p) for p in r.stdout.strip().split('\n') if p.strip()]
        pid = pids[0] if pids else None
    except Exception as e:
        logger.error(f'Critical Logical Gap: {str(e)}', exc_info=True)
    with get_db() as conn:
        latest = conn.execute('SELECT MAX(trade_date) FROM fact_daily').fetchone()[0]
        cnt = conn.execute(
            'SELECT COUNT(*) FROM fact_daily WHERE trade_date=(SELECT MAX(trade_date) FROM fact_daily)'
        ).fetchone()[0]

    db_size = os.path.getsize(DB_PATH) / (1024 * 1024) if os.path.exists(DB_PATH) else 0
    return HealthResponse(
        status='alive',
        timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        daemon_pid=pid,
        db_size_mb=round(db_size, 1),
        latest_trade_date=str(latest or ''),
        stock_count=int(cnt or 0),
    )


@app.get('/api/v1/audits/latest', response_model=List[AuditRecord])
def get_latest_audits(
    limit: int = Query(20, ge=1, le=100),
    verdict: Optional[str] = Query(None, description=f'{AUDIT_PASS}/HOLD/VETO'),
    trade_date: Optional[str] = Query(None, description='YYYY-MM-DD'),
    x_watchtower_key: str = Header(default='', alias='X-Watchtower-Key'),
):
    _require_watchtower_key(x_watchtower_key)
    with get_db() as conn:
        cols = _get_table_columns(conn, 'nexus_audits')
        red_col = _pick_col(cols, 'l4_red_score', 'l4_market_sentiment')
        blue_col = _pick_col(cols, 'l4_final_score', 'l4_blue_score', 'l3_audit_score')
        run_id_col = _pick_col(cols, 'run_id')
        veto_col = _pick_col(cols, 'l4_veto_reason')
        notary_col = _pick_col(cols, 'l4_notary_verdict')
        notary_fatal_col = _pick_col(cols, 'l4_notary_fatal_flag')
        red_expr = _expr_or_null(red_col, 'DOUBLE')
        blue_expr = _expr_or_null(blue_col, 'DOUBLE')
        run_id_expr = _expr_or_null(run_id_col, 'VARCHAR')
        veto_expr = _expr_or_null(veto_col, 'VARCHAR')
        notary_expr = _expr_or_null(notary_col, 'VARCHAR')
        notary_fatal_expr = _expr_or_null(notary_fatal_col, 'BOOLEAN')

        where_parts: List[str] = []
        params: List[Any] = []
        if verdict:
            where_parts.append('l4_final_verdict = ?')
            params.append(verdict.upper())
        if trade_date:
            where_parts.append('trade_date = ?')
            params.append(trade_date)

        wc = f"WHERE {' AND '.join(where_parts)}" if where_parts else ''
        rows = conn.execute(
            f"""
            SELECT {run_id_expr} AS run_id, task_id, symbol, name, trade_date,
                   l2_passed, l2_risk_score, l3_verdict, l3_audit_score,
                   l4_final_verdict, {veto_expr} AS l4_veto_reason,
                   {notary_expr} AS l4_notary_verdict, {notary_fatal_expr} AS l4_notary_fatal_flag,
                   {red_expr} AS l4_red_score, {blue_expr} AS l4_blue_score,
                   status, created_at
            FROM nexus_audits {wc}
            ORDER BY created_at DESC
            LIMIT ?
            """,
            params + [limit],
        ).fetchall()

        name_map: Dict[str, str] = {}
        symbols = [str(row[2] or '').strip().upper() for row in rows if str(row[2] or '').strip()]
        if symbols and _get_table_columns(conn, 'fact_stock_basic'):
            placeholders = ','.join(['?'] * len(symbols))
            query = (
                "SELECT symbol, COALESCE(name, '') "
                "FROM fact_stock_basic "
                f"WHERE symbol IN ({placeholders})"
            )
            for name_row in conn.execute(query, symbols).fetchall():
                name_map[str(name_row[0] or '').strip().upper()] = str(name_row[1] or '').strip()

    records: List[AuditRecord] = []
    for r in rows:
        final_verdict = str(r[9] or '')
        veto_reason_raw = str(r[10] or '')
        veto_reason, warning = _clean_veto_semantics(final_verdict, veto_reason_raw)
        records.append(
            AuditRecord(
                run_id=str(r[0] or ''),
                task_id=str(r[1] or ''),
                symbol=str(r[2] or ''),
                name=str(r[3] or '') or name_map.get(str(r[2] or '').strip().upper(), ''),
                trade_date=str(r[4] or ''),
                l2_passed=r[5],
                l2_risk_score=r[6],
                l3_verdict=str(r[7] or ''),
                l3_audit_score=r[8],
                l4_final_verdict=final_verdict,
                l4_veto_reason=veto_reason,
                l4_notary_verdict=str(r[11] or ''),
                l4_notary_fatal_flag=r[12],
                empty_reason_bucket=_normalize_empty_bucket(
                    final_verdict=final_verdict,
                    veto_reason=veto_reason,
                    status=str(r[15] or ''),
                ),
                l4_red_score=float(r[13]) if r[13] is not None else None,
                l4_blue_score=float(r[14]) if r[14] is not None else None,
                status=str(r[15] or ''),
                data_quality_warning=warning,
                created_at=str(r[16] or ''),
            )
        )
    return records



@app.get('/api/v1/audits/disagreements')
def get_audit_disagreements(
    trade_date: Optional[str] = Query(None, description='YYYY-MM-DD, default latest audit date'),
    run_id: Optional[str] = Query(None, description='Specific audit run_id'),
    limit: int = Query(12, ge=1, le=50),
    x_watchtower_key: str = Header(default='', alias='X-Watchtower-Key'),
):
    """Surface L2/L3/L4 disagreement cases for calibration, not execution."""
    _require_watchtower_key(x_watchtower_key)
    with get_db() as conn:
        cols = _get_table_columns(conn, 'nexus_audits')
        if not cols:
            return {'trade_date': '', 'run_id': '', 'summary': {}, 'items': []}

        red_col = _pick_col(cols, 'l4_red_score', 'l4_market_sentiment')
        blue_col = _pick_col(cols, 'l4_final_score', 'l4_blue_score', 'l3_audit_score')
        veto_col = _pick_col(cols, 'l4_veto_reason')
        run_id_col = _pick_col(cols, 'run_id')
        red_expr = _expr_or_null(red_col, 'DOUBLE')
        blue_expr = _expr_or_null(blue_col, 'DOUBLE')
        veto_expr = _expr_or_null(veto_col, 'VARCHAR')
        run_id_expr = _expr_or_null(run_id_col, 'VARCHAR')

        if trade_date:
            td = str(trade_date)
        else:
            row = conn.execute('SELECT MAX(trade_date) FROM nexus_audits').fetchone()
            td = str(row[0]) if row and row[0] else ''
        if not td:
            return {'trade_date': '', 'run_id': '', 'summary': {}, 'items': []}

        resolved_run_id = str(run_id or '').strip() or _resolve_latest_run_id(conn, td)
        where = ['trade_date = ?']
        params: List[Any] = [td]
        if resolved_run_id and run_id_col:
            where.append(f'{run_id_col} = ?')
            params.append(resolved_run_id)
        wc = ' AND '.join(where)

        rows = conn.execute(
            f"""
            SELECT
                {run_id_expr} AS run_id,
                task_id,
                symbol,
                COALESCE(name, '') AS name,
                trade_date,
                COALESCE(l2_passed, FALSE) AS l2_passed,
                COALESCE(l2_risk_score, 0) AS l2_risk_score,
                UPPER(COALESCE(l3_verdict, '')) AS l3_verdict,
                COALESCE(l3_audit_score, 0) AS l3_audit_score,
                UPPER(COALESCE(l4_final_verdict, '')) AS l4_final_verdict,
                {veto_expr} AS l4_veto_reason,
                {red_expr} AS l4_red_score,
                {blue_expr} AS l4_blue_score,
                COALESCE(status, '') AS status,
                COALESCE(l3_reasoning, '') AS l3_reasoning,
                COALESCE(created_at, CURRENT_TIMESTAMP) AS created_at
            FROM nexus_audits
            WHERE {wc}
              AND (
                   (UPPER(COALESCE(l3_verdict, '')) = 'PASS' AND UPPER(COALESCE(l4_final_verdict, '')) = 'VETO')
                OR (UPPER(COALESCE(l3_verdict, '')) = 'HOLD' AND UPPER(COALESCE(l4_final_verdict, '')) = 'PASS')
                OR (UPPER(COALESCE(l3_verdict, '')) = 'VETO' AND UPPER(COALESCE(l4_final_verdict, '')) = 'PASS')
                OR (COALESCE(l2_risk_score, 0) >= 55 AND UPPER(COALESCE(l3_verdict, '')) = 'PASS')
                OR (COALESCE(l2_passed, FALSE) = TRUE AND UPPER(COALESCE(l3_verdict, '')) = 'VETO')
              )
            ORDER BY created_at DESC
            LIMIT ?
            """,
            params + [limit],
        ).fetchall()

        name_map: Dict[str, str] = {}
        symbols = [str(row[2] or '').strip().upper() for row in rows if str(row[2] or '').strip()]
        if symbols and _get_table_columns(conn, 'fact_stock_basic'):
            placeholders = ','.join(['?'] * len(symbols))
            query = (
                "SELECT symbol, COALESCE(name, '') "
                "FROM fact_stock_basic "
                f"WHERE symbol IN ({placeholders})"
            )
            for name_row in conn.execute(query, symbols).fetchall():
                name_map[str(name_row[0] or '').strip().upper()] = str(name_row[1] or '').strip()

    summary = {
        'l3_pass_l4_veto': 0,
        'l3_hold_l4_pass': 0,
        'l3_veto_l4_pass': 0,
        'l2_high_risk_l3_pass': 0,
        'l2_pass_l3_veto': 0,
    }
    items = []
    for r in rows:
        l2_risk = int(r[6] or 0)
        l3_v = str(r[7] or '')
        l4_v = str(r[9] or '')
        reasons = []
        if l3_v == 'PASS' and l4_v == 'VETO':
            reasons.append('L3_PASS_L4_VETO')
            summary['l3_pass_l4_veto'] += 1
        if l3_v == 'HOLD' and l4_v == AUDIT_PASS:
            reasons.append('L3_HOLD_L4_PASS')
            summary['l3_hold_l4_pass'] += 1
        if l3_v == 'VETO' and l4_v == AUDIT_PASS:
            reasons.append('L3_VETO_L4_PASS')
            summary['l3_veto_l4_pass'] += 1
        if l2_risk >= 55 and l3_v == 'PASS':
            reasons.append('L2_HIGH_RISK_L3_PASS')
            summary['l2_high_risk_l3_pass'] += 1
        if bool(r[5]) and l3_v == 'VETO':
            reasons.append('L2_PASS_L3_VETO')
            summary['l2_pass_l3_veto'] += 1
        items.append({
            'run_id': str(r[0] or ''),
            'task_id': str(r[1] or ''),
            'symbol': str(r[2] or ''),
            'name': str(r[3] or '') or name_map.get(str(r[2] or '').strip().upper(), ''),
            'trade_date': str(r[4] or ''),
            'reasons': reasons,
            'l2_risk_score': l2_risk,
            'l3_verdict': l3_v,
            'l3_audit_score': int(r[8] or 0),
            'l4_final_verdict': l4_v,
            'l4_veto_reason': str(r[10] or ''),
            'l4_red_score': float(r[11]) if r[11] is not None else None,
            'l4_blue_score': float(r[12]) if r[12] is not None else None,
            'status': str(r[13] or ''),
            'l3_reasoning': str(r[14] or '')[:180],
            'created_at': str(r[15] or ''),
        })

    return {
        'trade_date': td,
        'run_id': resolved_run_id,
        'summary': summary,
        'items': items,
    }



@app.post('/api/v1/shadow/simulate', response_model=ShadowResponse)

def shadow_simulate(req: ShadowRequest, x_watchtower_key: str = Header(default='', alias='X-Watchtower-Key')):
    _require_watchtower_key(x_watchtower_key)
    with get_db() as conn:
        row = conn.execute(
            'SELECT symbol, close FROM fact_daily WHERE symbol=? ORDER BY trade_date DESC LIMIT 1',
            [req.stock_code],
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f'Stock {req.stock_code} not found')

        close_price = float(row[1])
        fills = conn.execute(
            """
            SELECT timestamp, symbol, action, price_logical, price_shadow,
                   qty, tide_mode, slippage_cost
            FROM fact_shadow_ledger
            WHERE symbol = ?
            ORDER BY timestamp DESC
            LIMIT 5
            """,
            [req.stock_code],
        ).fetchall()

    fill_list = [
        {
            'timestamp': str(f[0]),
            'symbol': f[1],
            'action': f[2],
            'price_logical': f[3],
            'price_shadow': f[4],
            'qty': f[5],
            'tide_mode': f[6],
            'slippage_cost': f[7],
        }
        for f in fills
    ]

    rm = 0.5 + req.risk_profile
    scfg = {
        'alpha': round(0.1 * rm, 4),
        'beta': round(0.05 * rm, 4),
        'risk_multiplier': round(rm, 2),
        'latest_close': close_price,
    }
    msg = f'Golden Mode: {len(fill_list)} historical fills' if fill_list else 'Golden Mode: no shadow fills yet'

    return ShadowResponse(
        stock_code=req.stock_code,
        risk_profile=req.risk_profile,
        mode='Golden',
        shadow_fills=fill_list,
        slippage_config=scfg,
        message=msg,
    )

@app.get('/api/v1/presentation/daily_alpha', response_model=DailyAlphaResponse)
def daily_alpha(x_watchtower_key: str = Header(default='', alias='X-Watchtower-Key')):
    _require_watchtower_key(x_watchtower_key)
    with get_db() as conn:
        cols = _get_table_columns(conn, 'nexus_audits')
        blue_col = _pick_col(cols, 'l4_final_score', 'l4_blue_score', 'l3_audit_score')
        red_col = _pick_col(cols, 'l4_red_score')
        sentiment_col = _pick_col(cols, 'l4_market_sentiment', 'l4_red_score')
        blue_expr = _expr_or_null(blue_col, 'DOUBLE')
        red_expr = _expr_or_null(red_col, 'DOUBLE')
        sentiment_expr = _expr_or_null(sentiment_col, 'DOUBLE')

        td_row = conn.execute('SELECT MAX(trade_date) FROM nexus_audits').fetchone()
        latest_td = str(td_row[0]) if td_row and td_row[0] else ''
        total = conn.execute('SELECT COUNT(*) FROM nexus_audits WHERE trade_date = ?', [latest_td]).fetchone()[0]

        rows = conn.execute(
            f"""
            SELECT symbol, name, l4_final_verdict, {blue_expr} AS l4_blue_score, {red_expr} AS l4_red_score,
                   l3_verdict, l3_audit_score, l3_reasoning, l1_close,
                   l4_veto_reason, {sentiment_expr} AS l4_market_sentiment
            FROM nexus_audits
            WHERE trade_date = ? AND l4_final_verdict = ?
            ORDER BY l4_blue_score DESC NULLS LAST
            """,
            [latest_td, AUDIT_PASS],
        ).fetchall()

        pass_symbols = [r[0] for r in rows]
        shadow_map: Dict[str, Dict[str, Any]] = {}
        if pass_symbols:
            placeholders = ','.join(['?' for _ in pass_symbols])
            sfills = conn.execute(
                f"""
                SELECT symbol, price_logical, price_shadow, qty, slippage_cost, tide_mode
                FROM fact_shadow_ledger
                WHERE symbol IN ({placeholders})
                ORDER BY timestamp DESC
                """,
                pass_symbols,
            ).fetchall()
            for sf in sfills:
                if sf[0] not in shadow_map:
                    shadow_map[sf[0]] = {
                        'price_logical': sf[1],
                        'price_shadow': sf[2],
                        'qty': sf[3],
                        'slippage_cost': sf[4],
                        'tide_mode': sf[5],
                    }

    cards = []
    for r in rows:
        sym = r[0]
        cards.append(
            AlphaCard(
                stock_code=sym,
                stock_name=str(r[1] or ''),
                l4_verdict={
                    'final': AUDIT_PASS,
                    'blue_score': r[3],
                    'red_score': r[4],
                    'l3_verdict': str(r[5] or ''),
                    'l3_score': r[6],
                    'l3_reasoning': str(r[7] or '')[:200],
                    'close': r[8],
                    'veto_reason': str(r[9] or ''),
                    'sentiment': str(r[10] or ''),
                },
                shadow_metrics=shadow_map.get(sym, {'status': 'no_shadow_fill'}),
            )
        )

    return DailyAlphaResponse(
        trade_date=latest_td,
        total_audited=int(total or 0),
        approved_count=len(cards),
        cards=cards,
    )


@app.get('/api/v1/shadow/portfolio', response_model=ShadowPortfolioResponse)
def shadow_portfolio(x_watchtower_key: str = Header(default='', alias='X-Watchtower-Key')):
    _require_watchtower_key(x_watchtower_key)
    max_positions = 4
    rules_path = PROJECT_ROOT / '05_shadow' / 'config' / 'rules.yaml'
    try:
        if rules_path.exists():
            for line in rules_path.read_text(encoding='utf-8').splitlines():
                stripped = line.strip()
                if stripped.startswith('max_positions:'):
                    max_positions = int(stripped.split(':', 1)[1].strip())
                    break
    except Exception as exc:
        logger.warning(f'shadow rules read failed: {exc}')

    with get_db() as conn:
        latest_td_row = conn.execute('SELECT MAX(trade_date) FROM fact_daily').fetchone()
        latest_td = str(latest_td_row[0] or '') if latest_td_row else ''

        metrics = None
        if _get_table_columns(conn, 'shadow_metrics'):
            metrics = conn.execute(
                """
                SELECT CAST(trade_date AS VARCHAR), total_equity, cash_reserve,
                       active_positions, CAST(updated_at AS VARCHAR)
                FROM shadow_metrics
                ORDER BY trade_date DESC
                LIMIT 1
                """
            ).fetchone()

        position_rows = []
        if _get_table_columns(conn, 'fact_paper_positions'):
            position_rows = conn.execute(
                """
                SELECT symbol, CAST(trade_date AS VARCHAR), COALESCE(status, 'HOLD') AS status,
                       COALESCE(qty, 0), COALESCE(entry_price, 0),
                       COALESCE(dynamic_stop_price, NULL), COALESCE(entry_score, NULL),
                       COALESCE(source, ''), COALESCE(strategy_tag, ''), CAST(updated_at AS VARCHAR),
                       COALESCE(NULLIF(entry_total_cost, 0), entry_price * COALESCE(NULLIF(initial_qty, 0), qty, 0), 0),
                       COALESCE(NULLIF(initial_qty, 0), qty, 0)
                FROM fact_paper_positions
                WHERE UPPER(COALESCE(NULLIF(status, ''), 'HOLD')) = 'HOLD'
                ORDER BY trade_date DESC, symbol
                """
            ).fetchall()

        review_cols = _get_table_columns(conn, 'fact_shadow_position_reviews')

        name_map: Dict[str, str] = {}
        symbols = [str(row[0] or '').strip().upper() for row in position_rows if str(row[0] or '').strip()]
        if symbols and _get_table_columns(conn, 'fact_stock_basic'):
            placeholders = ','.join(['?'] * len(symbols))
            query = (
                'SELECT symbol, COALESCE(name, '') '
                'FROM fact_stock_basic '
                f'WHERE symbol IN ({placeholders})'
            )
            for name_row in conn.execute(query, symbols).fetchall():
                name_map[str(name_row[0] or '').strip().upper()] = str(name_row[1] or '').strip()

        positions: List[ShadowPosition] = []
        total_cost = 0.0
        total_value = 0.0
        for row in position_rows:
            symbol = str(row[0] or '').strip().upper()
            trade_date = str(row[1] or '')
            qty = int(row[3] or 0)
            entry_price = float(row[4] or 0.0)
            current_row = conn.execute(
                """
                SELECT close
                FROM fact_daily
                WHERE symbol = ?
                ORDER BY trade_date DESC
                LIMIT 1
                """,
                [symbol],
            ).fetchone()
            current_price = float(current_row[0] or entry_price or 0.0) if current_row else entry_price

            review = None
            if review_cols:
                review = conn.execute(
                    """
                    SELECT hold_days, decision_state, quality_label, unrealized_pnl,
                           CAST(updated_at AS VARCHAR)
                    FROM fact_shadow_position_reviews
                    WHERE symbol = ? AND CAST(position_trade_date AS DATE) = CAST(? AS DATE)
                    ORDER BY trade_date DESC
                    LIMIT 1
                    """,
                    [symbol, trade_date],
                ).fetchone()

            initial_qty = int(row[11] or qty or 0)
            entry_total_cost = float(row[10] or 0.0)
            cost_basis = round(entry_total_cost * qty / max(1, initial_qty), 2) if entry_total_cost and qty else round(entry_price * qty, 2)
            market_value = round(current_price * qty, 2) if qty else 0.0
            net_liquidation_value = market_value
            if calculate_trade_cost is not None and qty > 0:
                net_liquidation_value = calculate_trade_cost('SELL', price=current_price, qty=qty).net_amount
            pnl_ratio = round((net_liquidation_value - cost_basis) / cost_basis, 6) if cost_basis > 0 else 0.0
            if review and review[3] is not None:
                pnl_ratio = round(float(review[3]), 6)
            pnl_amount = round(net_liquidation_value - cost_basis, 2) if cost_basis else 0.0
            total_cost += cost_basis
            total_value += net_liquidation_value

            positions.append(
                ShadowPosition(
                    symbol=symbol,
                    name=name_map.get(symbol, ''),
                    trade_date=trade_date,
                    status=str(row[2] or 'HOLD'),
                    qty=qty,
                    entry_price=round(entry_price, 4),
                    current_price=round(current_price, 4),
                    market_value=market_value,
                    cost_basis=cost_basis,
                    pnl_amount=pnl_amount,
                    pnl_ratio=pnl_ratio,
                    hold_days=int(review[0]) if review and review[0] is not None else None,
                    dynamic_stop_price=round(float(row[5]), 4) if row[5] is not None else None,
                    decision_state=str(review[1] or '') if review else '',
                    quality_label=str(review[2] or '') if review else '',
                    entry_score=float(row[6]) if row[6] is not None else None,
                    source=str(row[7] or ''),
                    strategy_tag=str(row[8] or ''),
                    updated_at=str(review[4] or row[9] or '') if review else str(row[9] or ''),
                )
            )

        history_trades: List[ShadowHistoryTrade] = []
        performance = ShadowPerformanceSummary()
        if _get_table_columns(conn, 'fact_paper_positions') and summarize_closed_trades is not None:
            perf_summary, closed_history = summarize_closed_trades(conn)
            for item in closed_history:
                history_trades.append(
                    ShadowHistoryTrade(
                        symbol=item['symbol'],
                        name=item.get('name', ''),
                        entry_date=item.get('entry_date', ''),
                        exit_date=item.get('exit_date', ''),
                        entry_price=round(float(item.get('entry_price') or 0), 4),
                        exit_price=round(float(item.get('exit_price') or 0), 4),
                        qty=int(item.get('qty') or 0),
                        entry_score=item.get('entry_score'),
                        pnl_amount=round(float(item.get('net_pnl') or 0), 2),
                        pnl_ratio=round(float(item.get('pnl_ratio') or 0), 6),
                        sell_rule=item.get('sell_rule', ''),
                        sell_reason=item.get('sell_reason', ''),
                    )
                )

            total_positions_row = conn.execute('SELECT COUNT(*) FROM fact_paper_positions').fetchone()
            total_positions_count = int((total_positions_row[0] if total_positions_row else 0) or 0)
            buy_count = 0
            sell_count = 0
            if _get_table_columns(conn, 'fact_shadow_ledger'):
                for action, cnt in conn.execute(
                    """
                    SELECT UPPER(COALESCE(action, '')) AS action, COUNT(*)
                    FROM fact_shadow_ledger
                    GROUP BY 1
                    """
                ).fetchall():
                    if action == 'BUY':
                        buy_count = int(cnt or 0)
                    elif action == 'SELL':
                        sell_count = int(cnt or 0)

            performance = ShadowPerformanceSummary(
                total_positions=total_positions_count,
                open_positions=len(positions),
                closed_trades=int(perf_summary.get('closed_trades') or 0),
                win_trades=int(perf_summary.get('win_trades') or 0),
                loss_trades=int(perf_summary.get('loss_trades') or 0),
                win_rate=float(perf_summary.get('win_rate') or 0),
                realized_pnl_amount=float(perf_summary.get('realized_pnl_amount') or 0),
                unrealized_pnl_amount=0.0,  # filled after active positions are valued
                total_pnl_amount=float(perf_summary.get('realized_pnl_amount') or 0),
                avg_closed_pnl_ratio=float(perf_summary.get('avg_closed_pnl_ratio') or 0),
                profit_loss_ratio=float(perf_summary.get('profit_loss_ratio') or 0),
                avg_win_amount=float(perf_summary.get('avg_win_amount') or 0),
                avg_loss_amount=float(perf_summary.get('avg_loss_amount') or 0),
                best_trade_symbol=str(perf_summary.get('best_trade_symbol') or ''),
                best_trade_name=str(perf_summary.get('best_trade_name') or ''),
                best_trade_pnl_ratio=float(perf_summary.get('best_trade_pnl_ratio') or 0),
                best_trade_pnl_amount=float(perf_summary.get('best_trade_pnl_amount') or 0),
                worst_trade_symbol=str(perf_summary.get('worst_trade_symbol') or ''),
                worst_trade_name=str(perf_summary.get('worst_trade_name') or ''),
                worst_trade_pnl_ratio=float(perf_summary.get('worst_trade_pnl_ratio') or 0),
                worst_trade_pnl_amount=float(perf_summary.get('worst_trade_pnl_amount') or 0),
                expectancy_amount=float(perf_summary.get('expectancy_amount') or 0),
                max_drawdown_ratio=float(perf_summary.get('max_drawdown_ratio') or 0),
                buy_count=buy_count,
                sell_count=sell_count,
            )

    pnl_amount_total = round(sum(p.pnl_amount for p in positions), 2)
    pnl_ratio_total = round(pnl_amount_total / total_cost, 6) if total_cost > 0 else 0.0
    performance.unrealized_pnl_amount = pnl_amount_total
    performance.total_pnl_amount = round(performance.realized_pnl_amount + pnl_amount_total, 2)
    cash = float(metrics[2] or 0.0) if metrics else 0.0
    base_equity = float(metrics[1] or 0.0) if metrics else 0.0
    total_equity = round((cash + total_value) if cash or total_value else base_equity, 2)
    active_positions = len(positions)
    msg = f'{active_positions}/{max_positions} active paper positions' if positions else 'No active shadow positions.'

    return ShadowPortfolioResponse(
        trade_date=latest_td,
        total_equity=total_equity,
        cash_reserve=round(cash, 2),
        invested_amount=round(total_cost, 2),
        market_value=round(total_value, 2),
        pnl_amount=pnl_amount_total,
        pnl_ratio=pnl_ratio_total,
        active_positions=active_positions,
        max_positions=max_positions,
        positions=positions,
        performance=performance,
        history_trades=history_trades[:10],
        message=msg,
    )


@app.get('/api/v1/telemetry/tokens', response_model=TokenResponse)
def token_telemetry(x_watchtower_key: str = Header(default='', alias='X-Watchtower-Key')):
    _require_watchtower_key(x_watchtower_key)
    with get_db() as conn:
        td_row = conn.execute('SELECT MAX(trade_date) FROM nexus_audits').fetchone()
        latest_td = str(td_row[0]) if td_row and td_row[0] else ''
        stats = conn.execute(
            """
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN l2_passed IS NOT NULL THEN 1 ELSE 0 END) AS l2_count,
                   SUM(CASE WHEN l3_verdict IS NOT NULL AND l3_verdict != '' THEN 1 ELSE 0 END) AS l3_count,
                   SUM(CASE WHEN l4_final_verdict IS NOT NULL AND l4_final_verdict != '' THEN 1 ELSE 0 END) AS l4_count
            FROM nexus_audits
            WHERE trade_date = ?
            """,
            [latest_td],
        ).fetchone()

    total, l2_cnt, l3_cnt, l4_cnt = int(stats[0]), int(stats[1] or 0), int(stats[2] or 0), int(stats[3] or 0)

    buckets: List[TokenBucket] = []
    l2_tokens = l2_cnt * 2 * 800
    l3_tokens = l3_cnt * 2000
    buckets.append(
        TokenBucket(
            provider='Ollama (Local)',
            model='lfm2.5-thinking:1.2b + qwen2.5:1.5b + deepseek-r1:1.5b',
            call_count=l2_cnt * 2 + l3_cnt,
            est_prompt_tokens=l2_tokens + l3_tokens,
            est_completion_tokens=(l2_cnt * 2 + l3_cnt) * 500,
            est_cost_cny=0.0,
        )
    )

    ds_calls = l4_cnt * 2
    ds_prompt = l4_cnt * (1500 + 2000)
    ds_completion = l4_cnt * (800 + 500)
    ds_cost = (ds_prompt * 2 + ds_completion * 8) / 1_000_000
    buckets.append(
        TokenBucket(
            provider='DeepSeek',
            model='deepseek-reasoner + deepseek-chat',
            call_count=ds_calls,
            est_prompt_tokens=ds_prompt,
            est_completion_tokens=ds_completion,
            est_cost_cny=round(ds_cost, 4),
        )
    )

    zp_calls = l4_cnt * 2
    zp_prompt = l4_cnt * (1000 + 1500)
    zp_completion = l4_cnt * (300 + 4096)
    zp_cost = (zp_prompt * 1 + zp_completion * 5) / 1_000_000
    buckets.append(
        TokenBucket(
            provider='Zhipu (GLM-4.7)',
            model='glm-4.7',
            call_count=zp_calls,
            est_prompt_tokens=zp_prompt,
            est_completion_tokens=zp_completion,
            est_cost_cny=round(zp_cost, 4),
        )
    )

    qw_calls = l4_cnt
    qw_prompt = l4_cnt * 1500
    qw_completion = l4_cnt * 500
    qw_cost = (qw_prompt * 2 + qw_completion * 6) / 1_000_000
    buckets.append(
        TokenBucket(
            provider='Alibaba (Qwen)',
            model='qwen-plus',
            call_count=qw_calls,
            est_prompt_tokens=qw_prompt,
            est_completion_tokens=qw_completion,
            est_cost_cny=round(qw_cost, 4),
        )
    )

    kimi_calls = l4_cnt
    kimi_prompt = l4_cnt * 2200
    kimi_completion = l4_cnt * 1200
    # Kimi/Moonshot Bull side estimate. Unit prices are approximate CNY per 1M tokens.
    kimi_cost = (kimi_prompt * 12 + kimi_completion * 12) / 1_000_000
    buckets.append(
        TokenBucket(
            provider='Kimi (Moonshot)',
            model='moonshot-v1-128k',
            call_count=kimi_calls,
            est_prompt_tokens=kimi_prompt,
            est_completion_tokens=kimi_completion,
            est_cost_cny=round(kimi_cost, 4),
        )
    )

    total_cost = sum(b.est_cost_cny for b in buckets)
    total_calls_all = sum(b.call_count for b in buckets)

    return TokenResponse(
        trade_date=latest_td,
        total_calls=total_calls_all,
        total_est_cost_cny=round(total_cost, 4),
        buckets=buckets,
        note='Estimates based on prompt template sizes. Local Ollama models are free. Actual cloud costs may vary.',
    )


@app.get('/api/v1/system/status', response_model=PipelineStatus)
def system_status(x_watchtower_key: str = Header(default='', alias='X-Watchtower-Key')):
    _require_watchtower_key(x_watchtower_key)
    today = datetime.now().strftime('%Y-%m-%d')

    pid = None
    daemon_alive = False
    try:
        r = subprocess.run(['pgrep', '-f', 'zhulong_daemon.py'], capture_output=True, text=True, timeout=5)
        pids = [int(p) for p in r.stdout.strip().split('\n') if p.strip()]
        if pids:
            pid = pids[0]
            daemon_alive = True
    except Exception as e:
        logger.error(f'Critical Logical Gap: {str(e)}', exc_info=True)
    phase3_running = False
    try:
        r2 = subprocess.run(['pgrep', '-f', 'decision_engine'], capture_output=True, text=True, timeout=5)
        if r2.stdout.strip():
            phase3_running = True
    except Exception as e:
        logger.error(f'Critical Logical Gap: {str(e)}', exc_info=True)
    with get_db() as conn:
        latest_td = str(conn.execute('SELECT MAX(trade_date) FROM nexus_audits').fetchone()[0] or '')
        today_total = conn.execute('SELECT COUNT(*) FROM nexus_audits WHERE trade_date = ?', [today]).fetchone()[0]
        today_l4 = conn.execute(
            """
            SELECT COUNT(*)
            FROM nexus_audits
            WHERE trade_date = ?
              AND l4_final_verdict IS NOT NULL
              AND l4_final_verdict != ''
            """,
            [today],
        ).fetchone()[0]

    if phase3_running:
        state = 'AUDITING'
        msg = f'第三阶段审计进行中：已处理 {today_total} 只标的，{today_l4} 只已进入 L4。'
    elif today_total > 0 and today_l4 > 0:
        state = 'IDLE'
        msg = f'今日审计已完成：共处理 {today_total} 只标的，{today_l4} 只完成 L4 终审。'
    elif today_total > 0:
        state = 'AUDITING'
        msg = f'审计进行中：{today_total} 只标的已进入流水线，等待 L4 终审。'
    else:
        state = 'IDLE'
        msg = '今天暂无审计记录，下一次跑批预计 21:00 开始。'

    hb = ''
    try:
        r3 = subprocess.run(['grep', 'heartbeat', str(DAEMON_LOG_FILE)], capture_output=True, text=True, timeout=5)
        lines = r3.stdout.strip().split('\n')
        if lines and lines[-1].strip():
            hb = lines[-1][:19]
    except Exception as e:
        logger.error(f'Critical Logical Gap: {str(e)}', exc_info=True)
    return PipelineStatus(
        state=state,
        daemon_alive=daemon_alive,
        daemon_pid=pid,
        phase3_running=phase3_running,
        latest_audit_date=latest_td,
        today_audit_count=int(today_total or 0),
        today_l4_count=int(today_l4 or 0),
        last_heartbeat=hb,
        message=msg,
    )


@app.get('/api/v1/system/liveness', response_model=LivenessResponse)
def system_liveness(x_watchtower_key: str = Header(default='', alias='X-Watchtower-Key')):
    _require_watchtower_key(x_watchtower_key)
    now = datetime.now()
    one_hour_ago = (now - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')

    daemon_age_s: Optional[float] = None
    l4_ratio: Optional[float] = None
    poller_age_s: Optional[float] = None
    notary_fail_rate: Optional[float] = None

    with get_db() as conn:
        ops_cols = _get_table_columns(conn, 'ops_pipeline_state')
        if ops_cols:
            row_daemon = conn.execute('SELECT MAX(updated_at) FROM ops_pipeline_state').fetchone()
            daemon_dt = _safe_parse_dt(row_daemon[0] if row_daemon else None)
            if daemon_dt:
                daemon_age_s = max(0.0, (now - daemon_dt).total_seconds())

            row_poller = conn.execute(
                """
                SELECT MAX(updated_at)
                FROM ops_pipeline_state
                WHERE LOWER(COALESCE(phase, '')) LIKE '%poller%'
                """
            ).fetchone()
            poller_dt = _safe_parse_dt(row_poller[0] if row_poller else None)
            if poller_dt:
                poller_age_s = max(0.0, (now - poller_dt).total_seconds())

        row_ratio = conn.execute(
            """
            SELECT
                COUNT(*) AS total_rows,
                SUM(CASE WHEN COALESCE(l4_final_verdict, '') <> '' THEN 1 ELSE 0 END) AS filled_rows
            FROM nexus_audits
            WHERE created_at >= CAST(? AS TIMESTAMP)
            """,
            [one_hour_ago],
        ).fetchone()
        total_rows = int((row_ratio[0] if row_ratio else 0) or 0)
        filled_rows = int((row_ratio[1] if row_ratio else 0) or 0)
        if total_rows > 0:
            l4_ratio = round(filled_rows / total_rows, 4)

        row_notary = conn.execute(
            """
            SELECT
                COUNT(*) AS total_rows,
                SUM(
                    CASE
                        WHEN UPPER(COALESCE(l4_notary_verdict, '')) IN ('PARSE_FAIL', 'NOTARY_INVALID', 'NOTARY_UNAVAILABLE')
                        THEN 1 ELSE 0
                    END
                ) AS fail_rows
            FROM nexus_audits
            WHERE created_at >= CAST(? AS TIMESTAMP)
              AND COALESCE(l4_notary_verdict, '') <> ''
            """,
            [one_hour_ago],
        ).fetchone()
        notary_total = int((row_notary[0] if row_notary else 0) or 0)
        notary_fail = int((row_notary[1] if row_notary else 0) or 0)
        if notary_total > 0:
            notary_fail_rate = round(notary_fail / notary_total, 4)

    with _probe_lock:
        last_checked = str(_probe_state.get('last_checked') or '').strip()
        latency_ms = None
        if last_checked:
            latency_ms = round(float(_probe_state.get('max_latency_s', 0.0) or 0.0) * 1000.0, 1)

    return LivenessResponse(
        timestamp=now.strftime('%Y-%m-%d %H:%M:%S'),
        daemon_last_phase_age_s=daemon_age_s,
        l4_filled_ratio_last_1h=l4_ratio,
        poller_last_run_age_s=poller_age_s,
        notary_parse_fail_rate_last_1h=notary_fail_rate,
        ollama_api_latency_p95_ms=latency_ms,
    )


@app.get('/api/stats/funnel')
def get_funnel_stats(
    trade_date: Optional[str] = Query(None, description='YYYY-MM-DD, default today'),
    run_id: Optional[str] = Query(None, description='Specific audit run_id'),
    x_watchtower_key: str = Header(default='', alias='X-Watchtower-Key'),
):
    _require_watchtower_key(x_watchtower_key)
    today = str(trade_date or date.today().strftime('%Y-%m-%d'))
    with get_db() as conn:
        l1_count = conn.execute('SELECT COUNT(*) FROM fact_daily WHERE trade_date = ?', [today]).fetchone()[0]
        l2_count = conn.execute(
            'SELECT COUNT(*) FROM nexus_audits WHERE trade_date = ? AND l2_risk_score IS NOT NULL', [today]
        ).fetchone()[0]
        l3_count = conn.execute(
            "SELECT COUNT(*) FROM nexus_audits WHERE trade_date = ? AND l3_verdict IS NOT NULL AND l3_verdict != ''",
            [today],
        ).fetchone()[0]
        l4_count = conn.execute(
            "SELECT COUNT(*) FROM nexus_audits WHERE trade_date = ? AND l4_final_verdict = ?",
            [today, AUDIT_PASS],
        ).fetchone()[0]

        resolved_run_id = str(run_id or '').strip() or _resolve_latest_run_id(conn, today)
        empty_snapshot = _build_empty_reason_snapshot(conn, today, resolved_run_id)

    return {
        'trade_date': today,
        'run_id': resolved_run_id,
        'l1': int(l1_count or 0),
        'l2': int(l2_count or 0),
        'l3': int(l3_count or 0),
        'l4': int(l4_count or 0),
        'selected_count': int(empty_snapshot.get('selected_count', 0) or 0),
        'is_empty_position': int(empty_snapshot.get('selected_count', 0) or 0) == 0,
        'empty_reason': {
            'run_rows': int(empty_snapshot.get('run_rows', 0) or 0),
            'l2_passed': int(empty_snapshot.get('l2_passed', 0) or 0),
            'l3_intercept': int(empty_snapshot.get('l3_veto_gate', 0) or 0),
            'l4_judge_veto': int(empty_snapshot.get('l4_judge_veto', 0) or 0),
            'notary_hard_veto': int(empty_snapshot.get('notary_hard_veto', 0) or 0),
            'l2_terminal': int(empty_snapshot.get('l2_terminal', 0) or 0),
            'veto_top': list(empty_snapshot.get('veto_top', [])),
        },
    }

@app.get('/api/results/approved')

def get_approved_results(x_watchtower_key: str = Header(default='', alias='X-Watchtower-Key')):
    _require_watchtower_key(x_watchtower_key)
    today = date.today().strftime('%Y-%m-%d')
    with get_db() as conn:
        cols = _get_table_columns(conn, 'nexus_audits')
        score_col = _pick_col(cols, 'l4_final_score', 'l4_blue_score', 'l3_audit_score', 'l4_market_sentiment', 'l4_red_score')
        score_expr = _expr_or_null(score_col, 'DOUBLE')
        results = conn.execute(
            f"""
            SELECT symbol, name, l4_final_verdict, l4_veto_reason, trade_date, {score_expr} AS score
            FROM nexus_audits
            WHERE trade_date = ? AND l4_final_verdict = ?
            ORDER BY symbol
            """,
            [today, AUDIT_PASS],
        ).fetchall()

    approved = []
    for row in results:
        approved.append(
            {
                'symbol': row[0],
                'name': row[1] or row[0],
                'verdict': row[2],
                'reasoning': row[3] or 'Strategic alignment confirmed.',
                'trade_date': row[4],
                'score': int(row[5]) if row[5] is not None else 85,
            }
        )
    return approved


@app.get('/api/v1/system/ollama116', response_model=OllamaNode116Status)
def get_ollama_node_116_status(force: bool = Query(False, description='Force an immediate probe'), x_watchtower_key: str = Header(default='', alias='X-Watchtower-Key')):
    _require_watchtower_key(x_watchtower_key)
    state = check_ollama_node_116(force_alert=False) if force else dict(_probe_state)
    return OllamaNode116Status(
        node=OLLAMA_NODE_116_BASE.replace('http://', '').replace('https://', ''),
        profile=str(NODE_CFG.get('profile', 'unknown')),
        model=OLLAMA_MODEL,
        ok=bool(state.get('ok', False)),
        tags_ok=bool(state.get('tags_ok', False)),
        generate_ok=bool(state.get('generate_ok', False)),
        tags_latency_s=float(state.get('tags_latency_s', 0.0) or 0.0),
        generate_latency_s=float(state.get('generate_latency_s', 0.0) or 0.0),
        max_latency_s=float(state.get('max_latency_s', 0.0) or 0.0),
        last_checked=str(state.get('last_checked', '')),
        last_error=str(state.get('last_error', '')),
        consecutive_failures=int(state.get('consecutive_failures', 0) or 0),
    )


try:
    from touchstone_api_v2 import register_touchstone_routes
    register_touchstone_routes(app=app, db_path=DB_PATH, logger=logger)
except Exception as exc:
    logger.warning('touchstone v2 router load failed: %s', exc)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | [%(name)s] %(message)s',
        handlers=[
            RotatingFileHandler(str(LOG_DIR / 'watchtower.log'), maxBytes=20*1024*1024, backupCount=5, encoding='utf-8'),
            logging.StreamHandler(),
        ],
    )
    uvicorn.run(app, host='127.0.0.1', port=8000, log_level='info')
