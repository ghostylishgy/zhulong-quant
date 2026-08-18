#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
03_tactics/tactics_bridge.py
=============================
战术桥接层 v2 — 逻辑隔离舱 (Safe Runner)

防弹机制:
- safe_execute_tactic 装饰器: 异常全部内部消化
- concurrent.futures + daemon 线程: 发射即不管
- PushPlus 告警: 任何异常向指挥官推送告警
- 主循环零污染: 子模块崩溃不影响盘后审计
"""

import os
import sys
import logging
import functools
import importlib
import threading
import runpy
import time
import traceback
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, Future
from typing import List, Dict, Callable, Any, Optional

_ROOT = Path(__file__).resolve().parents[1]

_loader_name = 'zhulong_core_module_loader'
if _loader_name in sys.modules:
    _module_loader = sys.modules[_loader_name]
else:
    _loader_path = _ROOT / '04_governance' / 'lib' / 'core' / 'module_loader.py'
    _loader_ns = runpy.run_path(str(_loader_path))

    class _ModuleLoaderShim:
        @staticmethod
        def load_module_from_path(module_name, path):
            return _loader_ns["load_module_from_path"](module_name, path)

        @staticmethod
        def load_attr_from_path(module_name, path, attr_name):
            return _loader_ns["load_attr_from_path"](module_name, path, attr_name)

    _module_loader = _ModuleLoaderShim()


def _load_internal_module(module_name: str, relative_path: str):
    return _module_loader.load_module_from_path(module_name, _ROOT / relative_path)


def _load_internal_attr(module_name: str, relative_path: str, attr_name: str):
    return _module_loader.load_attr_from_path(module_name, _ROOT / relative_path, attr_name)


logger = logging.getLogger('zhulong.tactics')

if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
from config.market_session import validate_realtime_quote_time

try:
    import importlib
    DBGateway = importlib.import_module('01_engine.lib.db_gateway').DBGateway
except Exception:
    DBGateway = _load_internal_attr(
        'zhulong_db_gateway',
        '01_engine/lib/db_gateway.py',
        'DBGateway',
    )
ComputeGateway = _load_internal_attr(
    'zhulong_compute_gateway_tactics_bridge',
    '02_brain/lib/compute_gateway.py',
    'ComputeGateway',
)
COMPUTE_GATEWAY = ComputeGateway(logger=logger, max_slots=2)

DB_PATH = str(_ROOT / 'storage' / 'database' / 'zhulong.duckdb')


def _env_int(name: str, default: int) -> int:
    try:
        return max(1, int(os.environ.get(name, str(default)) or default))
    except Exception:
        return default


# 后台线程池 (daemon=True, 主进程退出时自动清理)
# 并发背压控制: 限制盘中同时运行的策略线程数
_BACKPRESSURE_CAPACITY = 5
_backpressure = threading.BoundedSemaphore(value=_BACKPRESSURE_CAPACITY)
_backpressure_lock = threading.Lock()
_backpressure_stats = {
    'submitted': 0,
    'completed': 0,
    'rejected': 0,
    'submit_failed': 0,
    'inflight': 0,
    'active': 0,
    'high_watermark': 0,
}

_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="tactic")

# Eagle persistence stays isolated from the main tactic queue, but it is
# explicitly bounded so a prolonged DuckDB lock cannot grow memory forever.
_EAGLE_PERSISTENCE_CAPACITY = _env_int('EAGLE_PERSISTENCE_CAPACITY', 100)
_EAGLE_PERSISTENCE_SUBMIT_TIMEOUT_MS = _env_int(
    'EAGLE_PERSISTENCE_SUBMIT_TIMEOUT_MS', 250
)
_EAGLE_PERSISTENCE_RETRY_ATTEMPTS = _env_int(
    'EAGLE_PERSISTENCE_RETRY_ATTEMPTS', 3
)
_EAGLE_PERSISTENCE_RETRY_DELAY_MS = _env_int(
    'EAGLE_PERSISTENCE_RETRY_DELAY_MS', 200
)
_eagle_persistence_executor = ThreadPoolExecutor(
    max_workers=1,
    thread_name_prefix="eagle-persist",
)
_eagle_persistence_slots = threading.BoundedSemaphore(
    value=_EAGLE_PERSISTENCE_CAPACITY
)
_eagle_persistence_lifecycle_lock = threading.Lock()
_eagle_persistence_lock = threading.Lock()
_eagle_persistence_stats = {
    'submitted': 0,
    'completed': 0,
    'failed': 0,
    'rejected': 0,
    'submit_failed': 0,
    'inflight': 0,
    'active': 0,
    'high_watermark': 0,
    'signal_bus_retries': 0,
    'protocol_observer_retries': 0,
    'signal_bus_failed': 0,
    'protocol_observer_failed': 0,
    'partial_failures': 0,
}


def backpressure_snapshot() -> Dict[str, int]:
    """Return process-local queue telemetry without changing tactic behavior."""
    with _backpressure_lock:
        snapshot = dict(_backpressure_stats)
    snapshot['capacity'] = _BACKPRESSURE_CAPACITY
    snapshot['queued'] = max(0, int(snapshot['inflight']) - int(snapshot['active']))
    return snapshot


def eagle_persistence_snapshot() -> Dict[str, int]:
    """Return telemetry for the dedicated bounded Eagle persistence queue."""
    with _eagle_persistence_lock:
        snapshot = dict(_eagle_persistence_stats)
    snapshot['queued'] = max(0, int(snapshot['inflight']) - int(snapshot['active']))
    snapshot['capacity'] = _EAGLE_PERSISTENCE_CAPACITY
    return snapshot


# ═══════════════════════════════════════════════════════
# PushPlus 告警 (独立于 cloud_bridge, 最小依赖)
# ═══════════════════════════════════════════════════════

def _pushplus_alert(title: str, content: str):
    """????????, ?????????"""
    token = os.environ.get('PUSHPLUS_TOKEN', '')
    if not token:
        return
    try:
        COMPUTE_GATEWAY.http_post(
            'https://www.pushplus.plus/send',
            timeout=5,
            json_payload={
                'token': token,
                'title': f'\u70db\u9f99\u76d8\u4e2d\u6218\u672f\u5f02\u5e38 | {title}',
                'content': content[:2000],
                'template': 'txt',
            },
            layer='TACTICS',
            decision_id='pushplus_alert',
        )
    except Exception as exc:
        logger.error('Non-fatal: pushplus alert send failed: %s', exc, exc_info=True)


# ???????????????????????????????????????????????????????
# safe_execute_tactic ???
# ???????????????????????????????????????????????????????

def safe_execute_tactic(tactic_name: str, structured_errors: bool = False):
    """
    战术安全隔离装饰器。

    包裹所有盘中策略行为:
    1. 捕获一切异常 → 记录日志 + PushPlus 告警
    2. 严禁向上抛出 → 主进程零污染
    3. 超时保护: 线程池内置超时
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except (SystemExit, KeyboardInterrupt):
                raise  # 这两个必须向上传播
            except BaseException as e:
                tb = traceback.format_exc()
                logger.error(f"[SafeRunner] {tactic_name} 崩溃: {e}\n{tb}")
                _pushplus_alert(
                    f"{tactic_name} \u5f02\u5e38",
                    "\n".join([
                        f"\u6a21\u5757\uff1a{tactic_name}",
                        "\u72b6\u6001\uff1a\u6267\u884c\u5d29\u6e83\uff0c\u5df2\u9694\u79bb\u5904\u7406",
                        f"\u9519\u8bef\uff1a{str(e)[:300]}",
                        "",
                        "\u8bf4\u660e\uff1a\u5b8c\u6574\u9519\u8bef\u6808\u5df2\u5199\u5165\u65e5\u5fd7\uff0c\u672c\u63a8\u9001\u53ea\u4fdd\u7559\u624b\u673a\u7aef\u9700\u8981\u77e5\u9053\u7684\u6458\u8981\u3002",
                    ])
                )
                if structured_errors:
                    return {
                        'ok': False,
                        'tactic': tactic_name,
                        'error': str(e),
                        'traceback': tb[:4000],
                    }
                return None
        return wrapper
    return decorator


# ═══════════════════════════════════════════════════════
# 异步发射器 (Fire-and-Forget)
# ═══════════════════════════════════════════════════════

def fire_and_forget(tactic_name: str, func: Callable, *args, **kwargs) -> Optional:
    """
    异步非阻塞执行。主循环发射后立即返回。

    子任务在后台 daemon 线程中执行, 即使崩溃也不影响主循环。
    """
    @safe_execute_tactic(tactic_name, structured_errors=True)
    def _wrapped():
        return func(*args, **kwargs)

    if not _backpressure.acquire(blocking=False):
        with _backpressure_lock:
            _backpressure_stats['rejected'] += 1
        logger.warning(f"[FireForget] {tactic_name} 被背压拒绝 (5 slots 已满)")
        return None

    with _backpressure_lock:
        _backpressure_stats['submitted'] += 1
        _backpressure_stats['inflight'] += 1
        _backpressure_stats['high_watermark'] = max(
            _backpressure_stats['high_watermark'],
            _backpressure_stats['inflight'],
        )

    def _wrapped_with_release():
        with _backpressure_lock:
            _backpressure_stats['active'] += 1
        try:
            return _wrapped()
        finally:
            with _backpressure_lock:
                _backpressure_stats['active'] = max(0, _backpressure_stats['active'] - 1)
                _backpressure_stats['inflight'] = max(0, _backpressure_stats['inflight'] - 1)
                _backpressure_stats['completed'] += 1
            _backpressure.release()

    try:
        future: Future = _executor.submit(_wrapped_with_release)
        logger.info(f"[FireForget] {tactic_name} 已发射 (thread pool, backpressure OK)")
        return future
    except Exception as e:
        with _backpressure_lock:
            _backpressure_stats['inflight'] = max(0, _backpressure_stats['inflight'] - 1)
            _backpressure_stats['submit_failed'] += 1
        _backpressure.release()
        logger.error(f"[FireForget] {tactic_name} 提交失败: {e}")
        return None



# ═══════════════════════════════════════════════════════
# DB Watchlist
# ═══════════════════════════════════════════════════════

def _submit_eagle_persistence(func: Callable, *args, **kwargs) -> Optional[Future]:
    """Submit one Eagle observer write to its bounded, isolated executor."""
    @safe_execute_tactic('EagleSignal', structured_errors=True)
    def _safe_write():
        return func(*args, **kwargs)

    timeout = _EAGLE_PERSISTENCE_SUBMIT_TIMEOUT_MS / 1000.0
    with _eagle_persistence_lifecycle_lock:
        slot = _eagle_persistence_slots
        if not slot.acquire(timeout=timeout):
            with _eagle_persistence_lock:
                _eagle_persistence_stats['rejected'] += 1
            logger.error(
                '[EaglePersist] bounded queue full: capacity=%s timeout_ms=%s',
                _EAGLE_PERSISTENCE_CAPACITY,
                _EAGLE_PERSISTENCE_SUBMIT_TIMEOUT_MS,
            )
            return None

        with _eagle_persistence_lock:
            _eagle_persistence_stats['submitted'] += 1
            _eagle_persistence_stats['inflight'] += 1
            _eagle_persistence_stats['high_watermark'] = max(
                _eagle_persistence_stats['high_watermark'],
                _eagle_persistence_stats['inflight'],
            )
            inflight = _eagle_persistence_stats['inflight']

        if inflight in (25, 50, 75, _EAGLE_PERSISTENCE_CAPACITY):
            logger.warning('[EaglePersist] queue high watermark reached: %s', inflight)

        def _wrapped():
            result = None
            with _eagle_persistence_lock:
                _eagle_persistence_stats['active'] += 1
            try:
                result = _safe_write()
                if isinstance(result, dict) and result.get('ok') is False:
                    with _eagle_persistence_lock:
                        _eagle_persistence_stats['failed'] += 1
                return result
            finally:
                with _eagle_persistence_lock:
                    _eagle_persistence_stats['active'] = max(
                        0,
                        _eagle_persistence_stats['active'] - 1,
                    )
                    _eagle_persistence_stats['inflight'] = max(
                        0,
                        _eagle_persistence_stats['inflight'] - 1,
                    )
                    _eagle_persistence_stats['completed'] += 1
                slot.release()

        try:
            return _eagle_persistence_executor.submit(_wrapped)
        except Exception as exc:
            with _eagle_persistence_lock:
                _eagle_persistence_stats['inflight'] = max(
                    0,
                    _eagle_persistence_stats['inflight'] - 1,
                )
                _eagle_persistence_stats['submit_failed'] += 1
            slot.release()
            logger.error('[EaglePersist] submit failed: %s', exc, exc_info=True)
            return None


def _retry_eagle_write(label: str, writer: Callable[[], Any]) -> tuple[bool, int]:
    """Retry one idempotent observer sink without rerunning the other sink."""
    retries = 0
    for attempt in range(1, _EAGLE_PERSISTENCE_RETRY_ATTEMPTS + 1):
        try:
            if bool(writer()):
                return True, retries
            error = 'returned_false'
        except Exception as exc:
            error = str(exc)
        if attempt < _EAGLE_PERSISTENCE_RETRY_ATTEMPTS:
            retries += 1
            logger.warning(
                '[EaglePersist] %s write retry %s/%s: %s',
                label,
                attempt,
                _EAGLE_PERSISTENCE_RETRY_ATTEMPTS - 1,
                error,
            )
            time.sleep(_EAGLE_PERSISTENCE_RETRY_DELAY_MS / 1000.0)
    logger.error(
        '[EaglePersist] %s write exhausted after %s attempts: %s',
        label,
        _EAGLE_PERSISTENCE_RETRY_ATTEMPTS,
        error,
    )
    return False, retries


def _recycle_eagle_persistence_executor() -> None:
    """Drain the Eagle queue and replace its executor and capacity semaphore."""
    global _eagle_persistence_executor, _eagle_persistence_slots
    with _eagle_persistence_lifecycle_lock:
        old_executor = _eagle_persistence_executor
        old_executor.shutdown(wait=True, cancel_futures=False)
        _eagle_persistence_executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="eagle-persist",
        )
        _eagle_persistence_slots = threading.BoundedSemaphore(
            value=_EAGLE_PERSISTENCE_CAPACITY
        )


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {'1', 'true', 'yes', 'on'}


REALTIME_UNIVERSE_LIMIT = _env_int('TACTICS_REALTIME_UNIVERSE_LIMIT', 1200)
REALTIME_BATCH_SIZE = _env_int('TACTICS_REALTIME_BATCH_SIZE', 50)
OWL_MINUTE_WINDOW_AVAILABLE = _env_bool('TACTICS_OWL_MINUTE_WINDOW_AVAILABLE', False)
EAGLE_ACTIVE_PATH_ENABLED = _env_bool('EAGLE_ACTIVE_PATH_ENABLED', False)


def _to_float(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        text = str(value).strip()
        if not text or text.lower() in ('nan', 'none', 'null', '--'):
            return default
        return float(text)
    except Exception:
        return default


def _quote_date(value) -> str:
    text = str(value or '').strip()
    if len(text) == 8 and text.isdigit():
        return f'{text[:4]}-{text[4:6]}-{text[6:8]}'
    if len(text) >= 10 and text[4] == '-' and text[7] == '-':
        return text[:10]
    return ''


def _record_tactic_scan(protocol: str, trigger_type: str, scanned_count: int, triggered_count: int, status: str, evidence: Optional[Dict[str, Any]] = None) -> None:
    try:
        from protocol_observer import record_protocol_scan
        record_protocol_scan(
            protocol=protocol,
            trigger_type=trigger_type,
            scanned_count=scanned_count,
            triggered_count=triggered_count,
            status=status,
            evidence=evidence or {},
        )
    except Exception as exc:
        logger.warning('[%s] protocol scan summary write failed: %s', protocol, exc)


@safe_execute_tactic("Watchlist")
def _get_watchlist() -> list:
    """Load a liquid static universe; realtime quote data must supply intraday fields."""
    today = datetime.now().strftime("%Y-%m-%d")
    rows = []
    metadata_by_symbol = {}
    with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
        latest = conn.execute(
            """
            SELECT MAX(trade_date)
            FROM fact_daily
            WHERE trade_date <= CAST(? AS DATE)
            """,
            [today],
        ).fetchone()[0]
        if latest:
            source_date = str(latest)
            if source_date != today:
                logger.info("[Watchlist] realtime universe uses latest fact_daily source_date=%s", source_date)
            rows = conn.execute(
                """
                SELECT symbol, close, pct_chg, vol AS volume, turnover_rate as turnover,
                       COALESCE(vol_ma5, 0) AS avg_vol_5d, amount
                FROM fact_daily
                WHERE trade_date = CAST(? AS DATE)
                  AND COALESCE(close, 0) > 0
                  AND COALESCE(amount, 0) > 0
                ORDER BY amount DESC
                LIMIT ?
                """,
                [source_date, REALTIME_UNIVERSE_LIMIT],
            ).fetchall()
            if EAGLE_ACTIVE_PATH_ENABLED and rows:
                symbols = [str(row[0] or '').strip().upper() for row in rows]
                placeholders = ','.join('?' for _ in symbols)
                try:
                    metadata_rows = conn.execute(
                        f"""
                        SELECT symbol, COALESCE(industry, ''), COALESCE(market, ''),
                               COALESCE(is_st, FALSE), CAST(list_date AS VARCHAR)
                        FROM fact_stock_basic
                        WHERE symbol IN ({placeholders})
                        """,
                        symbols,
                    ).fetchall()
                    metadata_by_symbol = {
                        str(row[0] or '').strip().upper(): {
                            'industry': row[1] or '',
                            'market': row[2] or '',
                            'is_st': bool(row[3]),
                            'list_date': _quote_date(row[4]),
                        }
                        for row in metadata_rows
                    }
                except Exception as exc:
                    logger.warning(
                        '[Watchlist] Eagle Active metadata unavailable; using base universe: %s',
                        exc,
                    )
        else:
            logger.warning("[Watchlist] no fact_daily rows available for realtime universe")

    watchlist = []
    for r in rows:
        avg_vol_5d = r[5] or r[3] or 0
        avg_amount_5d = r[6] or 0
        item = {
            "symbol": r[0],
            "historical_close": r[1] or 0,
            "historical_pct_chg": r[2] or 0,
            "historical_volume": r[3] or 0,
            "turnover": r[4] or 0,
            "avg_vol_5d": avg_vol_5d,
            "historical_amount": r[6] or 0,
            "avg_amount_5d": avg_amount_5d,
            "universe_source_date": source_date,
            "quote_source": "historical_universe_only",
            "intraday_volume_window_available": False,
        }
        if EAGLE_ACTIVE_PATH_ENABLED:
            item.update(metadata_by_symbol.get(str(r[0] or '').strip().upper(), {}))
        watchlist.append(item)
    return watchlist


def _fetch_realtime_quotes(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    """Fetch batch realtime quotes. Volumes are normalized to fact_daily units."""
    quotes: Dict[str, Dict[str, Any]] = {}
    clean_symbols = [str(s or '').strip().upper() for s in symbols if str(s or '').strip()]
    if not clean_symbols:
        return quotes

    try:
        import tushare as ts
    except Exception as exc:
        logger.warning("[RealtimeQuote] tushare import failed: %s", exc)
        return quotes

    batch_size = max(1, REALTIME_BATCH_SIZE)
    for start in range(0, len(clean_symbols), batch_size):
        chunk = clean_symbols[start:start + batch_size]
        try:
            df = ts.realtime_quote(ts_code=','.join(chunk))
        except Exception as exc:
            logger.warning("[RealtimeQuote] batch failed start=%s size=%s: %s", start, len(chunk), exc)
            continue
        if df is None or len(df) == 0:
            logger.warning("[RealtimeQuote] empty response start=%s requested=%s", start, len(chunk))
            continue
        returned_symbols = set()
        for _, row in df.iterrows():
            sym = str(row.get('TS_CODE') or row.get('ts_code') or '').strip().upper()
            if not sym:
                continue
            returned_symbols.add(sym)
            price = _to_float(row.get('PRICE') if 'PRICE' in row else row.get('price'))
            pre_close = _to_float(row.get('PRE_CLOSE') if 'PRE_CLOSE' in row else row.get('pre_close'))
            pct_chg = _to_float(row.get('PCT_CHG') if 'PCT_CHG' in row else row.get('pct_chg'))
            if pct_chg == 0 and price > 0 and pre_close > 0:
                pct_chg = (price - pre_close) / pre_close * 100.0
            # Tushare realtime VOLUME is shares; fact_daily.vol is hands.
            volume = _to_float(row.get('VOLUME') if 'VOLUME' in row else row.get('VOL')) / 100.0
            # Tushare realtime AMOUNT is yuan; fact_daily.amount is thousand yuan.
            amount = _to_float(row.get('AMOUNT') if 'AMOUNT' in row else row.get('amount')) / 1000.0
            quotes[sym] = {
                'price': price,
                'pct_chg': pct_chg,
                'volume': volume,
                'amount': amount,
                'pre_close': pre_close,
                'quote_date': _quote_date(row.get('DATE') if 'DATE' in row else row.get('date')),
                'quote_time': str(row.get('TIME') if 'TIME' in row else row.get('time') or '').strip(),
                'quote_source': 'tushare_realtime',
            }
        requested_symbols = set(chunk)
        if len(returned_symbols) < len(requested_symbols):
            logger.warning(
                "[RealtimeQuote] partial response start=%s requested=%s returned=%s missing=%s batch_size=%s",
                start,
                len(requested_symbols),
                len(returned_symbols),
                len(requested_symbols - returned_symbols),
                batch_size,
            )
    return quotes


def _build_realtime_watchlist(universe: List[dict]) -> tuple:
    now = datetime.now()
    symbols = [item.get('symbol') for item in universe]
    quotes = _fetch_realtime_quotes(symbols)
    stats = {
        'observer_policy': 'side_channel_no_shadow_no_rag',
        'universe_count': len(universe),
        'quote_count': len(quotes),
        'stale_quote_count': 0,
        'realtime_count': 0,
        'quote_source': 'tushare_realtime',
        'universe_source_date': universe[0].get('universe_source_date') if universe else '',
    }
    realtime_watchlist = []
    for base in universe:
        sym = str(base.get('symbol') or '').strip().upper()
        quote = quotes.get(sym)
        if not quote:
            continue
        q_date = quote.get('quote_date') or ''
        q_time = quote.get('quote_time') or ''
        fresh, _status, _reason, _age = validate_realtime_quote_time(q_date, q_time, now=now)
        if not fresh:
            stats['stale_quote_count'] += 1
            continue
        price = float(quote.get('price') or 0)
        volume = float(quote.get('volume') or 0)
        if price <= 0 or volume <= 0:
            continue
        avg_vol = float(base.get('avg_vol_5d') or 0)
        avg_amount = float(base.get('avg_amount_5d') or 0)
        item = dict(base)
        item.update({
            'close': price,
            'price': price,
            'pct_chg': float(quote.get('pct_chg') or 0),
            'volume': volume,
            'amount': float(quote.get('amount') or 0),
            'total_volume': volume,
            'total_amount': float(quote.get('amount') or 0),
            'avg_vol_5d': avg_vol,
            'avg_amount_5d': avg_amount,
            'vol_ratio': (volume / avg_vol) if avg_vol > 0 else 0,
            'quote_date': q_date,
            'quote_time': q_time,
            'quote_source': quote.get('quote_source') or 'tushare_realtime',
            'intraday_volume_window_available': False,
        })
        realtime_watchlist.append(item)
    realtime_watchlist.sort(key=lambda x: float(x.get('pct_chg') or 0), reverse=True)
    stats['realtime_count'] = len(realtime_watchlist)
    return realtime_watchlist, stats


# ═══════════════════════════════════════════════════════
# 全局单例 (线程安全)
# ═══════════════════════════════════════════════════════

_eagle = None
_eagle_active = None
_owl = None
_singleton_lock = threading.Lock()


def _get_eagle_active(trade_date: str):
    """Load the observation-only Eagle path accumulator lazily."""
    global _eagle_active
    with _singleton_lock:
        if _eagle_active is not None and getattr(_eagle_active, 'trade_date', '') != trade_date:
            try:
                manifest = _eagle_active.finalize()
                _persist_eagle_active_manifest(manifest)
            except Exception as exc:
                logger.error('[EagleActive] previous-day finalize failed: %s', exc, exc_info=True)
            _eagle_active = None
        if _eagle_active is None:
            observer_cls = _load_internal_attr(
                'zhulong_tactics_eagle_active_path',
                '03_tactics/eagle_active_path.py',
                'EagleActivePathObserver',
            )
            _eagle_active = observer_cls(trade_date)
        return _eagle_active


def _record_eagle_active_scan(watchlist: List[dict], quote_stats: Dict[str, Any], trade_date: str):
    """Record a real quote window without touching the trade or audit chain."""
    if not EAGLE_ACTIVE_PATH_ENABLED:
        return None
    observer = _get_eagle_active(trade_date)
    result = observer.ingest_scan(
        watchlist,
        scan_time=datetime.now(),
        quote_stats=quote_stats,
    )
    logger.info(
        '[EagleActive] status=%s recorded=%s quotes=%s scan_id=%s',
        result.get('status'), result.get('recorded'), result.get('quote_count', 0),
        result.get('scan_id', ''),
    )
    return result


def _persist_eagle_active_manifest(manifest: Dict[str, Any]):
    """Persist only observation-safe candidates; failures stay off the trade path."""
    try:
        persist = _load_internal_attr(
            'zhulong_tactics_eagle_active_store',
            '03_tactics/eagle_active_store.py',
            'persist_manifest',
        )
        result = persist(manifest)
        logger.info(
            '[EagleActive] observation persistence status=%s date=%s upserted=%s',
            result.get('status'), result.get('trade_date', manifest.get('trade_date')),
            result.get('upserted', 0),
        )
        return result
    except Exception as exc:
        logger.error('[EagleActive] observation persistence failed: %s', exc, exc_info=True)
        return {'status': 'FAILED', 'upserted': 0, 'error': str(exc)}


def _finalize_eagle_active_if_pending(trade_date: Optional[str] = None):
    """Finalize today's JSONL even when the daemon restarted before daily_flush."""
    global _eagle_active
    if not EAGLE_ACTIVE_PATH_ENABLED:
        return None
    active_date = trade_date or datetime.now().strftime('%Y-%m-%d')
    window_path = (
        _ROOT / 'storage' / 'reports' / 'eagle_active_path' / 'windows'
        / f'eagle_windows_{active_date}.jsonl'
    )
    with _singleton_lock:
        observer = _eagle_active
        if observer is None:
            if not window_path.exists():
                return None
            observer_cls = _load_internal_attr(
                'zhulong_tactics_eagle_active_path',
                '03_tactics/eagle_active_path.py',
                'EagleActivePathObserver',
            )
            observer = observer_cls(active_date)
            _eagle_active = observer
        try:
            manifest = observer.finalize()
            _persist_eagle_active_manifest(manifest)
            logger.info(
                '[EagleActive] recovered/finalized pending day: date=%s scans=%s candidates=%s quality=%s',
                manifest.get('trade_date'), manifest.get('scan_count'),
                manifest.get('candidate_count'), manifest.get('data_quality'),
            )
            return manifest
        except Exception as exc:
            logger.error('[EagleActive] pending finalize failed: %s', exc, exc_info=True)
            return None
        finally:
            _eagle_active = None


def _queue_eagle_signal(cand) -> Optional:
    """Queue Eagle timer results on the dedicated observer persistence worker."""
    def _do_write():
        verdict = str(getattr(cand, 't5_verdict', '') or 'PENDING').upper()
        score = float(getattr(cand, 't5_score', 0) or 0)
        trade_date = datetime.now().strftime('%Y-%m-%d')
        signal_type = 'BREAKOUT_APPROVED' if verdict in ('APPROVE', 'BUY', 'PASS') and score >= 60 else 'BREAKOUT_OBSERVED'
        evidence = {
            'trigger_time': getattr(cand, 'trigger_time', ''),
            'pct_chg': getattr(cand, 'pct_chg', 0),
            'v_ratio': getattr(cand, 'v_ratio', 0),
            'amount': getattr(cand, 'amount', 0),
            'quote_source': getattr(cand, 'quote_source', ''),
            'quote_date': getattr(cand, 'quote_date', ''),
            'quote_time': getattr(cand, 'quote_time', ''),
            'universe_source_date': getattr(cand, 'universe_source_date', ''),
            'historical_pct_chg': getattr(cand, 'historical_pct_chg', 0),
            't3_status': getattr(cand, 't3_status', ''),
            't3_decay_rate': getattr(cand, 't3_decay_rate', 0),
            't5_verdict': verdict,
            'observer_policy': 'side_channel_no_shadow_no_rag',
        }

        def _record_signal_bus():
            from tactic_signal_bus import record_tactic_signal
            return record_tactic_signal(
                source='EAGLE',
                symbol=str(getattr(cand, 'symbol', '') or ''),
                signal_type=signal_type,
                verdict=verdict,
                score=score,
                price=float(getattr(cand, 'price', 0) or 0),
                reason=(
                    f"T3={getattr(cand, 't3_status', '')} "
                    f"decay={float(getattr(cand, 't3_decay_rate', 0) or 0):.2f} "
                    f"v_ratio={float(getattr(cand, 'v_ratio', 0) or 0):.2f}"
                ),
                evidence=evidence,
                trade_date=trade_date,
                ttl_minutes=45,
            )

        def _record_protocol_observer():
            from protocol_observer import (
                PROTOCOL_EAGLE,
                TRIGGER_EAGLE_PULSE,
                record_protocol_event,
            )
            return record_protocol_event(
                protocol=PROTOCOL_EAGLE,
                symbol=str(getattr(cand, 'symbol', '') or ''),
                trigger_type=TRIGGER_EAGLE_PULSE,
                trigger_price=float(getattr(cand, 'price', 0) or 0),
                score=score,
                verdict=verdict,
                trade_date=trade_date,
                event_time=getattr(cand, 'trigger_time', ''),
                is_trade_candidate=verdict in ('APPROVE', 'BUY', 'PASS') and score >= 90,
                execution_enabled=False,
                evidence=evidence,
            )

        bus_ok, bus_retries = _retry_eagle_write(
            'signal_bus', _record_signal_bus
        )
        protocol_ok, protocol_retries = _retry_eagle_write(
            'protocol_observer', _record_protocol_observer
        )
        with _eagle_persistence_lock:
            _eagle_persistence_stats['signal_bus_retries'] += bus_retries
            _eagle_persistence_stats['protocol_observer_retries'] += protocol_retries
            if not bus_ok:
                _eagle_persistence_stats['signal_bus_failed'] += 1
            if not protocol_ok:
                _eagle_persistence_stats['protocol_observer_failed'] += 1
            if bool(bus_ok) != bool(protocol_ok):
                _eagle_persistence_stats['partial_failures'] += 1
        if bool(bus_ok) != bool(protocol_ok):
            logger.error(
                '[EaglePersist] partial observer write: symbol=%s bus_ok=%s protocol_ok=%s',
                getattr(cand, 'symbol', ''),
                bus_ok,
                protocol_ok,
            )
        return {
            'ok': bool(bus_ok and protocol_ok),
            'signal_bus_ok': bool(bus_ok),
            'protocol_observer_ok': bool(protocol_ok),
            'signal_bus_retries': bus_retries,
            'protocol_observer_retries': protocol_retries,
        }

    return _submit_eagle_persistence(_do_write)

def _get_eagle():
    global _eagle
    with _singleton_lock:
        if _eagle is None:
            eagle_cls = _load_internal_attr(
                'zhulong_tactics_eagle_eye',
                '03_tactics/eagle_eye.py',
                'EagleEye',
            )
            _eagle = eagle_cls(signal_writer=_queue_eagle_signal)
        return _eagle


def _get_owl():
    global _owl
    with _singleton_lock:
        if _owl is None:
            owl_cls = _load_internal_attr(
                'zhulong_tactics_owl_eod',
                '03_tactics/owl_eod.py',
                'OwlMonitor',
            )
            _owl = owl_cls()
        return _owl


# ═══════════════════════════════════════════════════════
# 公共入口 (daemon 调用点)
# ═══════════════════════════════════════════════════════

def eagle_scan():
    """Eagle scan fire-and-forget entry."""
    def _do_eagle():
        eagle = _get_eagle()
        universe = _get_watchlist() or []
        watchlist, quote_stats = _build_realtime_watchlist(universe)
        active_trade_date = datetime.now().strftime('%Y-%m-%d')
        if EAGLE_ACTIVE_PATH_ENABLED:
            try:
                _record_eagle_active_scan(watchlist, quote_stats, active_trade_date)
            except Exception as exc:
                # Active Path is strictly observation-only; its failure must
                # not suppress the existing Eagle observer or position guard.
                logger.error('[EagleActive] scan record failed: %s', exc, exc_info=True)
        if not watchlist:
            _record_tactic_scan(
                'EAGLE',
                'EAGLE_PULSE_AUDIT',
                len(universe),
                0,
                'SCAN_NO_REALTIME',
                quote_stats,
            )
            return []
        triggered = eagle.scan_and_schedule(watchlist) or []
        _record_tactic_scan(
            'EAGLE',
            'EAGLE_PULSE_AUDIT',
            len(watchlist),
            len(triggered),
            'SCAN_TRIGGERED' if triggered else 'SCAN_EMPTY',
            quote_stats,
        )
        return triggered

    return fire_and_forget("EagleEye", _do_eagle)


def owl_scan():
    """Owl scan fire-and-forget entry."""
    def _do_owl():
        now = datetime.now()
        if now.hour != 14 or now.minute < 30 or now.minute > 55:
            return []
        if not OWL_MINUTE_WINDOW_AVAILABLE:
            _record_tactic_scan(
                'OWL',
                'OWL_EOD_MOMENTUM',
                0,
                0,
                'SCAN_DISABLED_NO_MINUTE_BAR',
                {
                    'reason': 'missing_real_last_30min_volume',
                    'quote_fetch_skipped': True,
                    'minute_window_source': 'unavailable',
                    'observer_policy': 'side_channel_no_shadow_no_rag',
                },
            )
            logger.warning("[Owl] scan skipped before realtime quote fetch: missing real last-30min volume window")
            return []
        universe = _get_watchlist() or []
        watchlist, quote_stats = _build_realtime_watchlist(universe)
        if not watchlist:
            _record_tactic_scan(
                'OWL',
                'OWL_EOD_MOMENTUM',
                len(universe),
                0,
                'SCAN_NO_REALTIME',
                quote_stats,
            )
            return []
        owl = _get_owl()
        return owl.start_eod_scan(watchlist) or []

    return fire_and_forget("Owl", _do_owl)


def shadow_position_scan():
    """Shadow paper-position intraday management entry."""
    def _do_shadow_position():
        manager_cls = _load_internal_attr(
            'zhulong_shadow_intraday_manager',
            '05_shadow/lib/intraday_manager.py',
            'ShadowIntradayManager',
        )
        return manager_cls().run_once()

    return fire_and_forget("ShadowPosition", _do_shadow_position)


def daily_flush():
    """
    每日 15:05 强制资源回收:
    - 清空 Eagle/Owl 临时状态
    - 重置熔断器
    - 清理残留 Timer 线程
    """
    global _eagle, _eagle_active, _owl

    logger.info("[DailyFlush] 15:05 系统资源回收开始")

    _finalize_eagle_active_if_pending()

    with _singleton_lock:
        if _eagle is not None:
            try:
                _eagle.cleanup()
            except Exception as exc:
                logger.error("Non-fatal: tactic resource cleanup failed: %s", exc, exc_info=True)
            _eagle = None

        if _owl is not None:
            try:
                _owl.reset()
            except Exception as exc:
                logger.error("Non-fatal: tactic resource cleanup failed: %s", exc, exc_info=True)
            _owl = None

    # 重置熔断器
    try:
        core_dir = str(_ROOT / '04_governance' / 'lib' / 'core')
        if core_dir not in sys.path:
            sys.path.insert(0, core_dir)
        get_circuit_breaker = importlib.import_module('cloud_bridge').get_circuit_breaker
        get_circuit_breaker().reset()
        logger.info("[DailyFlush] circuit breaker reset")
    except Exception as exc:
        logger.error("Non-fatal: circuit breaker reset failed: %s", exc, exc_info=True)

    # Clear residual Timer threads
    killed = 0
    for t in threading.enumerate():
        if t.name.startswith("Eagle_") or t.name.startswith("Owl_"):
            try:
                t.cancel()
                killed += 1
            except Exception as exc:
                logger.error("Non-fatal: tactic resource cleanup failed: %s", exc, exc_info=True)

    # 线程池深度回收
    global _executor
    try:
        _executor.shutdown(wait=True, cancel_futures=True)
        logger.info("[DailyFlush] 旧线程池已关闭")
    except Exception as e:
        logger.warning(f"[DailyFlush] 线程池关闭异常: {e}")
    _executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="tactic")
    logger.info("[DailyFlush] 新线程池已创建")

    try:
        _recycle_eagle_persistence_executor()
        logger.info("[DailyFlush] Eagle 持久化队列已排空并重建")
    except Exception as exc:
        logger.error(
            "Non-fatal: Eagle persistence executor recycle failed: %s",
            exc,
            exc_info=True,
        )

    # 显式内存碎片回收
    import gc
    gc.collect()
    logger.info(f"[DailyFlush] 完成: Eagle/Owl 清空, {killed} Timer 清理, gc.collect 完毕")
