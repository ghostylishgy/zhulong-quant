#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════╗
║   🐲 烛龙 Daemon v1.1 — Chronos 错峰编排器                    ║
║                                                              ║
║   Phase 1  09:15-15:00  盘中快轨 (鹰眼/猫头鹰 + optional Echo)║
║   Phase 2  20:30        统一全量数据收割                       ║
║   Phase 3  21:00        盘后全量审计 (L1→L4)                   ║
║   Phase 4  05:00        记忆合成 (RAG Refresh)                 ║
║   Phase 5  06:00        进化引擎 (L5 Evolver)                  ║
║   Backup   07:00        DuckDB 每日备份 (保留7天)               ║
║   Dawn     09:10        晨曦预检 (系统自检+推送)                ║
║                                                              ║
║   核心原则: 绝对错峰 + 强制 GC + 留白缓冲                      ║
║   基础设施: APScheduler + subprocess 隔离 + Semaphore(3)       ║
║                                                              ║
║   部署:                                                       ║
║     scp zhulong_daemon.py root@192.0.2.10:/root/quant_project/  ║
║     ssh root@192.0.2.10                                    ║
║     pip install apscheduler                                  ║
║     python3 zhulong_daemon.py --bootstrap-only  # 先修复      ║
║     python3 zhulong_daemon.py                   # 正式启动    ║
╚══════════════════════════════════════════════════════════════╝
"""

import os
import sys
import gc
import signal
import logging
import shutil
import tarfile
import threading
import subprocess
import argparse
import json
import time
import functools
import hashlib
import traceback
import atexit
import re
from collections import deque, OrderedDict
import importlib
import importlib.util
from pathlib import Path
import requests
from datetime import datetime, date, timedelta

from config.market_session import is_intraday_scan_time

from logging.handlers import RotatingFileHandler

#  Singleton Lock (fcntl)
import fcntl
_LOCK_FILE = "/tmp/zhulong.pid"
_lock_fd = None
_boot_log = logging.getLogger('zhulong.bootstrap')


def _release_singleton_lock():
    global _lock_fd
    try:
        if _lock_fd:
            try:
                fcntl.flock(_lock_fd, fcntl.LOCK_UN)
            except Exception:
                pass
            _lock_fd.close()
            _lock_fd = None
            _boot_log.info('[SINGLETON] lock released')
    except Exception:
        pass


def _read_lock_pid() -> int:
    try:
        raw = Path(_LOCK_FILE).read_text(encoding='utf-8').strip()
        return int(raw or '0')
    except Exception:
        return 0


atexit.register(_release_singleton_lock)

if os.getenv('ZHULONG_SKIP_SINGLETON_LOCK', '0') != '1':
    _lock_fd = open(_LOCK_FILE, "a+")
    try:
        fcntl.flock(_lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _lock_fd.seek(0)
        _lock_fd.truncate()
        _lock_fd.write(str(os.getpid()))
        _lock_fd.flush()
    except IOError:
        holder_pid = _read_lock_pid()
        alive = 'unknown'
        if holder_pid > 0:
            try:
                os.kill(holder_pid, 0)
                alive = 'alive'
            except ProcessLookupError:
                alive = 'stale'
            except PermissionError:
                alive = 'permission-denied'
            except Exception:
                alive = 'unknown'
        _boot_log.error(f'[SINGLETON] lock busy, holder pid={holder_pid}, state={alive}, exiting')
        sys.exit(3)


def _check_fact_daily_schema() -> bool:
    """Fail-fast probe: fact_daily must expose symbol/trade_date/vol/amount."""
    try:
        with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
            conn.execute(
                "SELECT symbol, trade_date, vol, amount FROM fact_daily LIMIT 1"
            ).fetchone()
        return True
    except Exception as exc:
        logger.error(f'[SCHEMA] fact_daily probe failed: {exc}', exc_info=True)
        return False


def _ensure_nexus_runtime_columns() -> bool:
    """Ensure runtime columns used by downstream consumers exist before jobs run."""
    required = {
        '_poller_consumed_at': 'TIMESTAMP',
        'shadow_processed': 'INTEGER DEFAULT 0',
        'l2_fact_tags': 'VARCHAR',
    }
    try:
        with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
            exists = conn.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = 'nexus_audits'
                LIMIT 1
                """
            ).fetchone()
            if not exists:
                logger.info('  [SCHEMA] nexus_audits absent; runtime column preflight deferred')
                return True

            cols = {
                str(r[1]).lower()
                for r in conn.execute("PRAGMA table_info('nexus_audits')").fetchall()
            }
            missing = [col for col in required if col not in cols]
            if not missing:
                logger.info('  [SCHEMA] nexus_audits runtime columns ready')
                return True

        with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
            cols = {
                str(r[1]).lower()
                for r in conn.execute("PRAGMA table_info('nexus_audits')").fetchall()
            }
            added = []
            for col, ddl in required.items():
                if col not in cols:
                    conn.execute(f"ALTER TABLE nexus_audits ADD COLUMN {col} {ddl}")
                    added.append(col)
            if added:
                logger.info('  [SCHEMA] added nexus_audits runtime column(s): %s', ', '.join(added))
            else:
                logger.info('  [SCHEMA] nexus_audits runtime columns ready after recheck')
        return True
    except Exception as exc:
        logger.error(f'[SCHEMA] nexus_audits runtime column preflight failed: {exc}', exc_info=True)
        return False


# ╔══════════════════════════════════════════════════════════════╗
# ║                    单一真相源 (Truth Source)                   ║
# ╚══════════════════════════════════════════════════════════════╝

BASE_DIR   = Path(
    os.getenv('ZHULONG_BASE_DIR', str(Path(__file__).resolve().parent))
).resolve()
DB_PATH    = BASE_DIR / 'storage' / 'database' / 'zhulong.duckdb'
API_SNAPSHOT_DB_PATH = Path('/root/quant_project/storage/database/zhulong_api_readonly.duckdb')
NEXUS_STATE_PATH = BASE_DIR / 'data' / 'nexus_state.json'
AUDIT_CONTRACT_REPORT_DIR = BASE_DIR / 'storage' / 'reports' / 'audit_contracts'
LOG_DIR    = BASE_DIR / 'logs'
CONFIG_ENV = BASE_DIR / '.env'
PYTHON     = sys.executable
RAG_PYTHON = os.getenv('RAG_PYTHON') or str(BASE_DIR / 'venv-rag' / 'bin' / 'python')


def _resolve_rag_python() -> str:
    candidate = Path(RAG_PYTHON)
    if candidate.exists() and os.access(candidate, os.X_OK):
        return str(candidate)
    logger.warning(f'[RAG] dedicated python unavailable: {candidate}; fallback to {PYTHON}')
    return PYTHON

OLLAMA_HOST = 'http://192.0.2.20:11434'

PRUNE_RETENTION_DAYS = int(os.getenv('PRUNE_RETENTION_DAYS', '7'))


def _load_dbgateway():
    try:
        mod = importlib.import_module('01_engine.lib.db_gateway')
        return mod.DBGateway
    except Exception:
        mod_path = BASE_DIR / '01_engine' / 'lib' / 'db_gateway.py'
        spec = importlib.util.spec_from_file_location('db_gateway_01_daemon', str(mod_path))
        if spec is None or spec.loader is None:
            raise RuntimeError(f'DBGateway loader unavailable: {mod_path}')
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod.DBGateway


def _load_governance_contract():
    mod = None
    for mod_name in ('04_governance.config.settings', 'config.settings'):
        try:
            candidate = importlib.import_module(mod_name)
            if hasattr(candidate, 'Config'):
                mod = candidate
                break
        except Exception:
            continue
    if mod is None:
        mod_path = BASE_DIR / '04_governance' / 'config' / 'settings.py'
        spec = importlib.util.spec_from_file_location('settings_bridge_daemon', str(mod_path))
        if spec is None or spec.loader is None:
            raise RuntimeError(f'Governance settings loader unavailable: {mod_path}')
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        if not hasattr(mod, 'Config'):
            raise RuntimeError(f'Governance settings missing Config class: {mod_path}')
    return mod.Config, str(getattr(mod, 'AUDIT_PASS', 'PASS')).upper()


DBGateway = _load_dbgateway()
Config, AUDIT_PASS = _load_governance_contract()

# 组件绝对路径
COMP = {
    'decision_engine': BASE_DIR / '02_brain' / 'decision_engine.py',
    'news_observer':   BASE_DIR / 'tools' / 'process_l4_news_observations.py',
    'news_review':     BASE_DIR / 'tools' / 'review_l4_news_observation.py',
    'shadow_context':  BASE_DIR / 'tools' / 'build_shadow_position_context.py',
    'trade_archetype_observer': BASE_DIR / 'tools' / 'observe_trade_archetypes.py',
    'l1_path_observer': BASE_DIR / 'tools' / 'observe_l1_paths.py',
    'l1_scorecard': BASE_DIR / 'tools' / 'review_l1_simple_scorecard.py',
    'eagle_active_context': BASE_DIR / 'tools' / 'build_eagle_active_context.py',
    'audit_funnel_observer': BASE_DIR / 'tools' / 'audit_funnel_observer.py',
    'data_sync':       BASE_DIR / '01_engine' / 'data_sync.py',
    'rag_refresher':   BASE_DIR / '01_engine' / 'lib' / 'rag_refresher.py',
    'evolver':         BASE_DIR / 'src' / 'layers' / 'l5_evolution' / 'evolver_main.py',
    'derived_features': BASE_DIR / 'scripts' / 'update_derived_features.py',
    'zeta_collect':    BASE_DIR / 'scripts' / 'run_zeta_collect.py',
    'rps_etl':         BASE_DIR / '02_brain' / 'lib' / 'rps_etl.py',
    'trade_calendar':  BASE_DIR / 'tools' / 'manage_trade_calendar.py',
}

# ╔══════════════════════════════════════════════════════════════╗
# ║                          日志系统                             ║
# ╚══════════════════════════════════════════════════════════════╝

LOG_DIR.mkdir(parents=True, exist_ok=True)
_daemon_log_path = Path(
    os.getenv('ZHULONG_DAEMON_LOG_PATH', str(LOG_DIR / 'daemon.log'))
).expanduser()
if not _daemon_log_path.is_absolute():
    _daemon_log_path = (BASE_DIR / _daemon_log_path).resolve()
_daemon_log_path.parent.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger('zhulong.daemon')
logger.setLevel(logging.INFO)
for _existing_handler in list(logger.handlers):
    if not getattr(_existing_handler, '_zhulong_daemon_managed', False):
        continue
    logger.removeHandler(_existing_handler)
    try:
        _existing_handler.close()
    except Exception:
        pass

_fh = RotatingFileHandler(
    str(_daemon_log_path), maxBytes=10*1024*1024,
    backupCount=5, encoding='utf-8')
_fh._zhulong_daemon_managed = True
_fh.setFormatter(logging.Formatter(
    '%(asctime)s | %(levelname)-5s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
_ch = logging.StreamHandler()
_ch._zhulong_daemon_managed = True
_ch.setFormatter(logging.Formatter(
    '%(asctime)s | %(levelname)-5s | %(message)s', datefmt='%H:%M:%S'))
logger.addHandler(_fh)
logger.addHandler(_ch)

def _get_proxies():
    p = os.environ.get('HTTP_PROXY') or os.environ.get('http_proxy')
    return {'http': p, 'https': p} if p else None


def _env_flag(name: str, default: str = '0') -> bool:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == '':
        raw = default
    return str(raw).strip().lower() in {'1', 'true', 'yes', 'on'}


def _load_audit_contract_module():
    mod_path = BASE_DIR / '02_brain' / 'lib' / 'audit_contract.py'
    spec = importlib.util.spec_from_file_location('zhulong_audit_contract_daemon', str(mod_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f'audit contract loader unavailable: {mod_path}')
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def _atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + '\n'
    temp_path = path.with_name(f'.{path.name}.tmp-{os.getpid()}')
    temp_path.write_text(text, encoding='utf-8')
    os.replace(temp_path, path)


def _prepare_audit_contract_binding(
    trade_date: str,
    run_id: str,
    evidence_as_of: str,
    runtime_overrides: dict,
) -> tuple[dict, Path | None]:
    """Freeze a run-scoped provenance artifact without gating the audit."""
    if not _env_flag('AUDIT_CONTRACT_BINDING_ENABLED', '1'):
        logger.info('[AUDIT-CONTRACT] binding disabled by configuration')
        return {}, None
    try:
        contract = _load_audit_contract_module()
        environment = dict(os.environ)
        environment.update({
            str(key): str(value)
            for key, value in (runtime_overrides or {}).items()
            if value is not None
        })
        artifact = contract.build_bound_artifact(
            BASE_DIR,
            trade_date,
            run_id,
            evidence_as_of,
            environment,
        )
        if not contract.verify_bound_artifact(artifact):
            raise RuntimeError('generated audit contract artifact failed verification')
        output_path = AUDIT_CONTRACT_REPORT_DIR / (
            f'audit_contract_{str(trade_date).replace("-", "")}_{run_id}.json'
        )
        if output_path.exists():
            existing = json.loads(output_path.read_text(encoding='utf-8'))
            if existing != artifact:
                raise RuntimeError(f'frozen audit contract artifact conflict: {output_path.name}')
        else:
            _atomic_write_json(output_path, artifact)
        manifest = artifact['manifest']
        binding = artifact['binding']
        child_env = {
            'AUDIT_CONTRACT_SCHEMA_VERSION': manifest['schema_version'],
            'AUDIT_CONTRACT_SHA256': manifest['contract_sha256'],
            'AUDIT_CONTRACT_BINDING_SCHEMA_VERSION': binding['schema_version'],
            'AUDIT_CONTRACT_BINDING_SHA256': binding['binding_sha256'],
            'AUDIT_CONTRACT_BINDING_STATUS': binding['binding_status'],
            'AUDIT_CONTRACT_EVIDENCE_AS_OF': binding['evidence_as_of'],
            'AUDIT_CONTRACT_ARTIFACT_NAME': output_path.name,
        }
        logger.info(
            '[AUDIT-CONTRACT] bound | trade_date=%s run_id=%s contract=%s binding=%s artifact=%s',
            trade_date,
            run_id,
            str(manifest['contract_sha256'])[:12],
            str(binding['binding_sha256'])[:12],
            output_path.name,
        )
        return child_env, output_path
    except Exception as exc:
        logger.warning(
            '[AUDIT-CONTRACT] unavailable; audit continues without versioned provenance: %s',
            exc,
            exc_info=True,
        )
        return {}, None


_ECHO_DISABLED_LOGGED_DAYS: set[str] = set()


def _echo_enabled() -> bool:
    return _env_flag('ZHULONG_ECHO_ENABLED', '0')


def _log_echo_disabled_once(trade_date: str | None = None) -> None:
    day_key = str(trade_date or datetime.now().strftime('%Y-%m-%d'))[:10]
    if day_key in _ECHO_DISABLED_LOGGED_DAYS:
        return
    _ECHO_DISABLED_LOGGED_DAYS.add(day_key)
    logger.info(
        '[ECHO] dormant: ZHULONG_ECHO_ENABLED=0; '
        'intraday scan/awaken skipped, incubation remains unconnected'
    )


def _current_ollama_host():
    try:
        base = str(getattr(Config, 'OLLAMA_BASE_URL', '') or '').strip()
        if base:
            return base.rstrip('/')
        url = str(getattr(Config, 'OLLAMA_URL', '') or '').strip()
        if '/api/' in url:
            return url.split('/api/', 1)[0].rstrip('/')
    except Exception:
        logger.error('[Config] failed to resolve OLLAMA host from Config', exc_info=True)
    return OLLAMA_HOST


def _node_runtime_env():
    env = {
        'ZL_AUDIT_PASS': AUDIT_PASS,
        'OLLAMA_HOST': _current_ollama_host(),
        'OLLAMA_BASE_URL': _current_ollama_host(),
    }
    getter = getattr(Config, 'get_node_config', None)
    if not callable(getter):
        return env
    try:
        node_cfg = getter() or {}
        env.update({
            'ZL_NODE_ID': str(node_cfg.get('node_id', 'unknown')),
            'ZL_NODE_PROFILE': str(node_cfg.get('profile', 'unknown')),
            'ZL_NUM_THREAD': str(node_cfg.get('num_thread', 1)),
            'ZL_KEEP_ALIVE': str(node_cfg.get('keep_alive', '0')),
            'OLLAMA_NUM_THREAD': str(node_cfg.get('num_thread', 1)),
            'OLLAMA_KEEP_ALIVE': str(node_cfg.get('keep_alive', '0')),
        })
    except Exception:
        logger.error('[Config] get_node_config injection failed', exc_info=True)
    return env


# ╔══════════════════════════════════════════════════════════════╗
# ║                  权威交易日历 / fail-closed                  ║
# ╚══════════════════════════════════════════════════════════════╝


def _load_trade_calendar_contract():
    mod_path = BASE_DIR / '01_engine' / 'lib' / 'trade_calendar.py'
    spec = importlib.util.spec_from_file_location('trade_calendar_01_daemon', str(mod_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f'trade calendar loader unavailable: {mod_path}')
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


TRADE_CALENDAR = _load_trade_calendar_contract()
CALENDAR_SCOPE_AUDIT = TRADE_CALENDAR.SCOPE_AUDIT
CALENDAR_SCOPE_ENTRY = TRADE_CALENDAR.SCOPE_ENTRY
_CALENDAR_PUSH_GUARD: set[str] = set()
_CALENDAR_PUSH_LOCK = threading.Lock()


def _calendar_decision(d=None, scope: str = CALENDAR_SCOPE_AUDIT):
    return TRADE_CALENDAR.decide_trade_day(
        DB_PATH,
        d or date.today(),
        scope=scope,
    )


def _push_calendar_block_once(decision, context: str = '') -> bool:
    if not getattr(decision, 'should_alert', False):
        return False
    key = '|'.join([
        str(decision.cal_date),
        str(decision.scope),
        str(decision.status),
    ])
    with _CALENDAR_PUSH_LOCK:
        if key in _CALENDAR_PUSH_GUARD:
            return False
        _CALENDAR_PUSH_GUARD.add(key)
    lines = [
        f'日期：{decision.cal_date}',
        f'权限域：{decision.scope}',
        f'受影响环节：{context or "未标注环节"}',
        f'日历状态：{decision.status}',
        '处理：交易日状态无法被权威确认，相关流程已安全关闭。',
        f'原因：{decision.reason}',
        '',
        '说明：审计权限与影子盘买入权限彼此独立；不会因审计开放而自动允许买入。',
    ]
    return _push_wechat('烛龙交易日历安全关闭', '\n'.join(lines))


def _calendar_allows(
    d=None,
    *,
    scope: str = CALENDAR_SCOPE_AUDIT,
    notify: bool = False,
    context: str = '',
) -> bool:
    decision = _calendar_decision(d, scope=scope)
    if context and (not decision.is_open or str(decision.status).startswith('MANUAL_OVERRIDE')):
        level = logger.warning if decision.should_alert else logger.info
        level(
            '[CALENDAR] context=%s date=%s scope=%s status=%s open=%s reason=%s',
            context,
            decision.cal_date,
            decision.scope,
            decision.status,
            decision.is_open,
            decision.reason,
        )
    if notify:
        _push_calendar_block_once(decision, context=context)
    return bool(decision.is_open)


def is_trading_day(d=None, scope: str = CALENDAR_SCOPE_AUDIT):
    """Compatibility boolean backed only by the authoritative cached contract."""
    return _calendar_allows(d, scope=scope)

def _resolve_trade_date_override() -> str | None:
    raw = (os.getenv('TRADE_DATE', '') or os.getenv('ZHULONG_TRADE_DATE', '')).strip()
    if not raw:
        return None
    if len(raw) == 8 and raw.isdigit():
        raw = f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
    try:
        datetime.strptime(raw, '%Y-%m-%d')
    except Exception:
        logger.error('[DATE_OVERRIDE] invalid TRADE_DATE=%s (expected YYYYMMDD or YYYY-MM-DD)', raw)
        return None
    return raw


def _current_trade_date_str() -> str:
    return _resolve_trade_date_override() or datetime.now().strftime('%Y-%m-%d')


def _latest_fact_daily_trade_date_str(max_date_str: str) -> str:
    """Return the latest completed fact_daily date at or before max_date_str."""
    try:
        with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
            row = conn.execute(
                """
                SELECT CAST(MAX(trade_date) AS VARCHAR)
                FROM fact_daily
                WHERE trade_date <= CAST(? AS DATE)
                """,
                [max_date_str],
            ).fetchone()
        resolved = str((row[0] if row else '') or '').strip()
        return resolved[:10] if resolved else max_date_str
    except Exception as exc:
        logger.warning('[INTRADAY] fact_daily date fallback failed: %s', exc, exc_info=True)
        return max_date_str


def _is_trading_day_for_current(
    *,
    scope: str = CALENDAR_SCOPE_AUDIT,
    notify: bool = False,
    context: str = '',
) -> bool:
    td = _current_trade_date_str()
    try:
        d = datetime.strptime(td, '%Y-%m-%d').date()
        return _calendar_allows(d, scope=scope, notify=notify, context=context)
    except Exception:
        logger.error('[DATE_OVERRIDE] parse failed for td=%s', td, exc_info=True)
        return _calendar_allows(
            td,
            scope=scope,
            notify=notify,
            context=context or 'invalid_trade_date_override',
        )


# ╔══════════════════════════════════════════════════════════════╗
# ║                  Node-102 并发锁 Semaphore(3)                ║
# ╚══════════════════════════════════════════════════════════════╝



PIPELINE_STATE_DETAIL_MAX = 512
PIPELINE_STATE_RETRIES = 3
PIPELINE_STATE_RETRY_BASE_DELAY = 0.4


def _normalize_pipeline_status(status: str) -> str:
    st = str(status or '').strip().upper()
    return st or 'IN_PROGRESS'


def _normalize_pipeline_detail(detail: str) -> str:
    return str(detail or '').strip()[:PIPELINE_STATE_DETAIL_MAX]


def _ensure_pipeline_state_table() -> bool:
    """Ensure ops_pipeline_state uses canonical DuckDB schema; rebuild on drift."""
    expected_cols = ['trade_date', 'phase', 'status', 'detail', 'updated_at']

    def _read_state_schema(conn):
        exists = bool(conn.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = 'ops_pipeline_state'
            LIMIT 1
            """
        ).fetchone())
        current_meta = conn.execute("PRAGMA table_info('ops_pipeline_state')").fetchall() if exists else []
        current_cols = [str(r[1]).lower() for r in current_meta]
        current_types = {str(r[1]).lower(): str(r[2]).upper() for r in current_meta}
        pk_cols = [str(r[1]).lower() for r in current_meta if int(r[5]) == 1]
        schema_ok = (
            current_cols == expected_cols
            and pk_cols == ['trade_date', 'phase']
            and current_types.get('trade_date', '').startswith('DATE')
            and 'VARCHAR' in current_types.get('phase', '')
            and 'VARCHAR' in current_types.get('status', '')
            and 'VARCHAR' in current_types.get('detail', '')
            and 'TIMESTAMP' in current_types.get('updated_at', '')
        )
        return exists, current_cols, current_types, schema_ok

    try:
        with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
            exists, current_cols, current_types, schema_ok = _read_state_schema(conn)
            if schema_ok:
                return True

        with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
            exists, current_cols, current_types, schema_ok = _read_state_schema(conn)
            if schema_ok:
                return True

            if exists:
                logger.warning('[OPS] ops_pipeline_state schema drift detected, rebuilding table')

            conn.execute('DROP TABLE IF EXISTS ops_pipeline_state__rebuild')
            conn.execute(
                """
                CREATE TABLE ops_pipeline_state__rebuild (
                    trade_date DATE,
                    phase VARCHAR,
                    status VARCHAR,
                    detail VARCHAR,
                    updated_at TIMESTAMP DEFAULT now(),
                    PRIMARY KEY (trade_date, phase)
                )
                """
            )

            if exists:
                colset = set(current_cols)
                trade_expr = "TRY_CAST(trade_date AS DATE)" if 'trade_date' in colset else 'NULL'
                phase_expr = "CAST(phase AS VARCHAR)" if 'phase' in colset else 'NULL'
                status_expr = "CAST(status AS VARCHAR)" if 'status' in colset else "'UNKNOWN'"
                detail_expr = "CAST(detail AS VARCHAR)" if 'detail' in colset else "''"
                updated_expr = (
                    'COALESCE(TRY_CAST(updated_at AS TIMESTAMP), now())'
                    if 'updated_at' in colset else 'now()'
                )

                conn.execute(f"""
                    INSERT INTO ops_pipeline_state__rebuild (trade_date, phase, status, detail, updated_at)
                    SELECT trade_date_v, phase_v, status_v, detail_v, updated_v
                    FROM (
                        SELECT
                            {trade_expr} AS trade_date_v,
                            {phase_expr} AS phase_v,
                            {status_expr} AS status_v,
                            {detail_expr} AS detail_v,
                            {updated_expr} AS updated_v
                        FROM ops_pipeline_state
                    ) t
                    WHERE trade_date_v IS NOT NULL AND phase_v IS NOT NULL
                """)
                conn.execute('DROP TABLE ops_pipeline_state')

            conn.execute('ALTER TABLE ops_pipeline_state__rebuild RENAME TO ops_pipeline_state')
        return True
    except Exception as exc:
        logger.critical('[OPS] ensure ops_pipeline_state failed: %s', exc, exc_info=True)
        raise RuntimeError('ops_pipeline_state schema/persist failed') from exc


def _set_pipeline_state(
    trade_date_str: str,
    phase: str,
    status: str,
    detail: str = '',
    retries: int = PIPELINE_STATE_RETRIES,
    base_delay: float = PIPELINE_STATE_RETRY_BASE_DELAY,
) -> None:
    """Atomic state upsert with terminal-state protection and lock-retry policy."""
    status_norm = _normalize_pipeline_status(status)
    detail_norm = _normalize_pipeline_detail(detail)
    retry_total = max(1, int(retries))

    last_err = None
    for attempt in range(retry_total):
        try:
            _ensure_pipeline_state_table()
            with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
                conn.execute(
                    """
                    INSERT INTO ops_pipeline_state (trade_date, phase, status, detail, updated_at)
                    VALUES (CAST(? AS DATE), ?, ?, ?, now())
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
                    [trade_date_str, phase, status_norm, detail_norm, PIPELINE_STATE_DETAIL_MAX, PIPELINE_STATE_DETAIL_MAX],
                )
            return
        except Exception as exc:
            last_err = exc
            logger.warning('[OPS] write ops_pipeline_state retry %s/%s failed: %s', attempt + 1, retry_total, exc)
            if attempt < retry_total - 1:
                time.sleep(base_delay * (2 ** attempt))

    if last_err is not None:
        logger.critical('[OPS] write ops_pipeline_state failed: %s', last_err, exc_info=True)
        try:
            _push_exception_once('Zhulong PipelineState CRITICAL', last_err, context='ops_pipeline_state_write')
        except Exception:
            logger.error('[OPS] critical alert dispatch failed', exc_info=True)
        raise RuntimeError('ops_pipeline_state write failed') from last_err

def _can_enter_audit(trade_date_str: str | None = None):
    """Hard gate for Phase 3. Returns (ok: bool, reason: str)."""
    td = trade_date_str or datetime.now().strftime('%Y-%m-%d')
    _ensure_pipeline_state_table()

    try:
        with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
            row = conn.execute(
                """
                SELECT status, COALESCE(detail, '')
                FROM ops_pipeline_state
                WHERE trade_date = ? AND phase = 'phase_harvest'
                LIMIT 1
                """,
                [td],
            ).fetchone()
            if not row:
                return False, f'{td} phase_harvest state missing'

            st = str(row[0] or '').upper()
            detail = str(row[1] or '')
            if st != 'DONE':
                return False, f'{td} phase_harvest={st or "UNKNOWN"} {detail}'.strip()

            daily_cnt = conn.execute(
                "SELECT COUNT(*) FROM fact_daily WHERE trade_date = ?",
                [td],
            ).fetchone()
            if not daily_cnt or int(daily_cnt[0] or 0) <= 0:
                return False, f'{td} fact_daily empty, audit blocked'

            zeta_cnt = conn.execute(
                "SELECT COUNT(*) FROM fact_zeta_signals WHERE trade_date = ?",
                [td],
            ).fetchone()
            if not zeta_cnt or int(zeta_cnt[0] or 0) <= 0:
                return False, f'{td} fact_zeta_signals empty, audit blocked'

            rps_missing = conn.execute(
                """
                SELECT COUNT(*)
                FROM fact_daily d
                LEFT JOIN fact_rps_results r
                  ON d.symbol = r.symbol AND d.trade_date = r.trade_date
                WHERE d.trade_date = ?
                  AND r.symbol IS NULL
                """,
                [td],
            ).fetchone()
            rps_missing_cnt = int((rps_missing[0] if rps_missing else 0) or 0)
            if rps_missing_cnt > 0:
                return False, f'{td} fact_rps_results missing {rps_missing_cnt} symbol rows, audit blocked'

        return True, 'harvest complete with rps coverage'
    except Exception as exc:
        return False, f'audit gate check exception: {exc}'

def _estimate_audit_target_count(trade_date_str: str) -> tuple[int, int, int, int]:
    """Estimate audit candidate count using L1 physical filters and L2 cap."""
    l1_limit = int(getattr(Config, 'L1_CANDIDATE_LIMIT', 50) or 50)
    l2_cap = int(getattr(Config, 'L2_TOP_N_CLOSE', 12) or 12)
    l1_limit = max(1, l1_limit)
    l2_cap = max(1, min(l2_cap, 15))

    raw_count = 0
    try:
        with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
            row = conn.execute(
                """
                SELECT COUNT(*)
                FROM fact_daily
                WHERE trade_date = ?
                  AND close > 0
                  AND amount > 10000
                """,
                [trade_date_str],
            ).fetchone()
        raw_count = int((row[0] if row else 0) or 0)
    except Exception as exc:
        logger.warning(f'[AUDIT] target-count probe failed: {exc}', exc_info=True)

    target_count = max(1, min(raw_count, l1_limit, l2_cap))
    return target_count, raw_count, l1_limit, l2_cap


def _calc_dynamic_audit_window_seconds(target_count: int) -> int:
    """AIOps dynamic window: max(1, target_count) * 705 * 1.15"""
    return int(max(1, int(target_count or 1)) * 705 * 1.15)


def _refresh_api_readonly_snapshot(stage: str) -> bool:
    """Checkpoint and copy while the DuckDB writer lock remains held."""
    src = DB_PATH
    dst = API_SNAPSHOT_DB_PATH
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    try:
        if not src.exists():
            logger.warning(f'[{stage}] snapshot skipped, source missing: {src}')
            return False
        dst.parent.mkdir(parents=True, exist_ok=True)
        checkpoint_t0 = time.perf_counter()
        with DBGateway(
            src,
            read_only=False,
            logger=logger,
            expected_hold_seconds=120,
        ) as conn:
            conn.execute("CHECKPOINT;")
            checkpoint_ms = (time.perf_counter() - checkpoint_t0) * 1000
            logger.info(f'[{stage}] CHECKPOINT done | {checkpoint_ms:.1f}ms')
            # Keep this connection open: its DuckDB file lock blocks external
            # writers until the byte-for-byte copy has completed.
            shutil.copy2(str(src), str(tmp))

        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                os.replace(str(tmp), str(dst))
                break
            except (PermissionError, OSError) as lock_exc:
                if attempt >= max_retries:
                    raise
                logger.warning(
                    f'[{stage}] snapshot replace retry {attempt}/{max_retries} due to lock: {lock_exc}'
                )
                time.sleep(1)

        size_mb = dst.stat().st_size / 1024 / 1024
        logger.info(f'[{stage}] API readonly snapshot refreshed: {dst.name} ({size_mb:.1f}MB)')
        return True
    except Exception as exc:
        logger.error(f'[{stage}] API readonly snapshot failed: {exc}', exc_info=True)
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass
        return False


def _table_exists(conn, table_name: str) -> bool:
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


def _table_has_column(conn, table_name: str, column_name: str) -> bool:
    cols = [str(r[1]).lower() for r in conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()]
    return column_name.lower() in cols


def _run_daily_prune(retention_days: int = PRUNE_RETENTION_DAYS) -> dict:
    cutoff_dt = datetime.now() - timedelta(days=max(1, int(retention_days)))
    cutoff_ts = cutoff_dt.strftime('%Y-%m-%d %H:%M:%S')
    table_targets = [
        ('ops_daemon_heartbeat', 'ts'),
        ('nexus_thinking_traces', 'created_at'),
        ('nexus_l2_reviews', 'created_at'),
        ('fact_echo_logs', 'updated_at'),
    ]
    pruned_rows = 0

    with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
        for table, col in table_targets:
            try:
                if (not _table_exists(conn, table)) or (not _table_has_column(conn, table, col)):
                    continue
                before = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] or 0)
                conn.execute(
                    f"DELETE FROM {table} WHERE TRY_CAST({col} AS TIMESTAMP) < CAST(? AS TIMESTAMP)",
                    [cutoff_ts],
                )
                after = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] or 0)
                delta = max(0, before - after)
                if delta > 0:
                    logger.info(f'[PRUNE] {table} -{delta} rows (< {cutoff_ts})')
                pruned_rows += delta
            except Exception as exc:
                logger.warning(f'[PRUNE] skip table {table}: {exc}')

    log_pruned = 0
    try:
        for f in LOG_DIR.glob('*'):
            if not f.is_file():
                continue
            if datetime.fromtimestamp(f.stat().st_mtime) < cutoff_dt:
                f.unlink(missing_ok=True)
                log_pruned += 1
    except Exception as exc:
        logger.warning(f'[PRUNE] log cleanup degraded: {exc}')

    return {
        'cutoff': cutoff_ts,
        'rows': pruned_rows,
        'log_files': log_pruned,
    }


def phase_prune():
    """Daily low-peak prune to prevent unbounded DuckDB growth."""
    logger.info('=' * 50)
    logger.info('[PRUNE] low-peak prune start')
    logger.info('=' * 50)
    stats = _run_daily_prune(PRUNE_RETENTION_DAYS)
    logger.info(
        f"[PRUNE] done | cutoff={stats['cutoff']} | rows={stats['rows']} | logs={stats['log_files']}"
    )
    _refresh_api_readonly_snapshot('PRUNE')
    gc.collect()
    logger.info('[PRUNE] gc.collect() done')

OLLAMA_SEM = threading.BoundedSemaphore(3)

# ╔══════════════════════════════════════════════════════════════╗
# ║                       优雅关闭信号                            ║
# ╚══════════════════════════════════════════════════════════════╝

_shutdown = threading.Event()
_AUDIT_ACTIVE = threading.Event()
_HEARTBEAT_DB_PAUSED = threading.Event()
_NEWS_OBSERVATION_ACTIVE = threading.Event()
_HEARTBEAT_PERSIST_LOCK = threading.Lock()


def _on_signal(signum, _):
    logger.warning(f'收到信号 {signum}，准备关闭...')
    _shutdown.set()


signal.signal(signal.SIGTERM, _on_signal)
signal.signal(signal.SIGINT, _on_signal)


def _rss_mb():
    try:
        import psutil
        return psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
    except Exception:
        logger.error('[Metrics] RSS probe failed', exc_info=True)
        return None


def monitor_phase(phase_name):
    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            t0 = time.perf_counter()
            m0 = _rss_mb()
            if m0 is None:
                logger.info(f'⏱️ [{phase_name}] START')
            else:
                logger.info(f'⏱️ [{phase_name}] START | rss={m0:.1f}MB')
            try:
                return fn(*args, **kwargs)
            finally:
                dt = time.perf_counter() - t0
                m1 = _rss_mb()
                if m0 is None or m1 is None:
                    logger.info(f'⏱️ [{phase_name}] END | {dt:.2f}s')
                else:
                    logger.info(
                        f'⏱️ [{phase_name}] END | {dt:.2f}s | '
                        f'rss={m1:.1f}MB | Δ={m1-m0:+.1f}MB'
                    )
        return wrapper
    return deco


# ╔══════════════════════════════════════════════════════════════╗
# ║                   .env 环境变量加载                            ║
# ╚══════════════════════════════════════════════════════════════╝


def load_env():
    if not CONFIG_ENV.exists():
        logger.warning(f'.env 不存在: {CONFIG_ENV}')
        return
    for line in CONFIG_ENV.read_text(encoding='utf-8').splitlines():
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            k, v = line.split('=', 1)
            os.environ.setdefault(k.strip(), v.strip())
    logger.info(f'  ✅ .env 已加载')


# ╔══════════════════════════════════════════════════════════════╗
# ║                   安全子进程执行器                              ║
# ╚══════════════════════════════════════════════════════════════╝


def safe_run(cmd, label, timeout=3600, use_sem=False, extra_env=None):
    """
    安全执行子进程 — 进程组级击杀，杜绝僵尸孙进程。
    use_sem=True 时获取 Node-102 并发锁 (最多3个推理并发)。

    Memory safety:
    - 流式读取 stdout/stderr 到日志。
    - 主进程仅保留最后 10 行缓冲，避免全量输出占用内存。
    """
    if use_sem:
        if not OLLAMA_SEM.acquire(timeout=60):
            logger.warning(f'[{label}] Node-102 slots 已满, 跳过')
            return False

    env = {**os.environ, 'PYTHONPATH': str(BASE_DIR)}
    env.update(_node_runtime_env())
    if extra_env:
        env.update({str(k): str(v) for k, v in extra_env.items() if v is not None})
    logger.info(f'[{label}] ▶ 启动')
    t0 = time.time()

    proc = None
    stdout_tail = deque(maxlen=10)
    stderr_tail = deque(maxlen=10)
    stdout_thread = None
    stderr_thread = None

    def _derive_line_level(line: str, default: str = 'info') -> str:
        cleaned = re.sub(r'\x1b\[[0-9;]*m', '', str(line or ''))
        upper = cleaned.upper()
        if '❌' in cleaned or '💥' in cleaned or 'TRACEBACK' in upper:
            return 'error'
        if '⚠' in cleaned:
            return 'warning'
        structured = re.search(
            r'(?:^|\|)\s*(CRITICAL|ERROR|WARNING|WARN|INFO|DEBUG)\s*(?:\||$)',
            upper,
        )
        if structured:
            level_name = structured.group(1)
            if level_name in {'CRITICAL', 'ERROR'}:
                return 'error'
            if level_name in {'WARNING', 'WARN'}:
                return 'warning'
            return 'info'
        if '📋' in cleaned or '✅' in cleaned:
            return 'info'
        return default

    def _pump(pipe, tail):
        if pipe is None:
            return
        try:
            for raw in iter(pipe.readline, ''):
                line = raw.rstrip('\n')
                if not line:
                    continue
                line_short = line[:240]
                tail.append(line_short)
                line_level = _derive_line_level(
                    line_short,
                    default='info',
                )
                if line_level == 'error':
                    logger.error(f'[{label}] | {line_short}')
                elif line_level == 'warning':
                    logger.warning(f'[{label}] | {line_short}')
                else:
                    logger.info(f'[{label}] | {line_short}')
        except Exception:
            logger.error(f'[{label}] stream pump failed', exc_info=True)
        finally:
            try:
                pipe.close()
            except Exception:
                logger.error(f'[{label}] pipe close failed', exc_info=True)

    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(BASE_DIR),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8',
            errors='replace',
            bufsize=1,
            preexec_fn=os.setsid,
        )

        stdout_thread = threading.Thread(
            target=_pump, args=(proc.stdout, stdout_tail), daemon=True
        )
        stderr_thread = threading.Thread(
            target=_pump, args=(proc.stderr, stderr_tail), daemon=True
        )
        stdout_thread.start()
        stderr_thread.start()

        proc.wait(timeout=timeout)
        dt = time.time() - t0

        if stdout_thread.is_alive():
            stdout_thread.join(timeout=2)
        if stderr_thread.is_alive():
            stderr_thread.join(timeout=2)

        if proc.returncode == 0:
            logger.info(f'[{label}] ✅ ({dt:.0f}s)')
            return True

        logger.error(f'[{label}] ❌ rc={proc.returncode} ({dt:.0f}s)')
        if stderr_tail:
            logger.error(f'[{label}] stderr tail (last {len(stderr_tail)} lines):')
            for ln in stderr_tail:
                logger.error(f'  | {ln}')
        elif stdout_tail:
            logger.error(f'[{label}] stdout tail (last {len(stdout_tail)} lines):')
            for ln in stdout_tail:
                logger.error(f'  | {ln}')
        return False

    except subprocess.TimeoutExpired:
        logger.error(f'[{label}] ⏰ 超时 ({timeout}s) — 进程组击杀', exc_info=True)
        if proc:
            try:
                pgid = os.getpgid(proc.pid)
                os.killpg(pgid, signal.SIGTERM)
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    logger.error(f'[{label}] graceful wait timeout, force SIGKILL', exc_info=True)
                    os.killpg(pgid, signal.SIGKILL)
                    proc.wait(timeout=3)
                logger.warning(f'[{label}] 进程组 {pgid} 已清除')
            except (ProcessLookupError, PermissionError):
                logger.error(f'[{label}] process-group cleanup failed', exc_info=True)
        return False
    except Exception as e:
        logger.error(f'[{label}] 💥 {e}', exc_info=True)
        return False
    finally:
        if proc and proc.poll() is None:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except Exception:
                logger.error(f'[{label}] final SIGKILL cleanup failed', exc_info=True)

        if stdout_thread and stdout_thread.is_alive():
            stdout_thread.join(timeout=1)
        if stderr_thread and stderr_thread.is_alive():
            stderr_thread.join(timeout=1)

        if use_sem:
            OLLAMA_SEM.release()


# ╔══════════════════════════════════════════════════════════════╗
# ║           Bootstrap: 基础设施修复 (启动时一次性)               ║
# ╚══════════════════════════════════════════════════════════════╝


def bootstrap():
    logger.info('=' * 55)
    logger.info('\U0001f527 Bootstrap: \u57fa\u7840\u8bbe\u65bd\u68c0\u67e5 (v2 - no source patching)')
    logger.info('=' * 55)

    de = COMP['decision_engine']
    if not de.exists():
        logger.error(f'\u274c {de} \u4e0d\u5b58\u5728'); return False

    # --- Runtime config overrides (NO source file modification) ---
    # These values are injected into the module namespace at import time
    # instead of rewriting .py files on disk.
    import importlib, importlib.util

    # 1. Prepare runtime overrides for decision_engine
    RUNTIME_OVERRIDES = {
        'DB_PATH': Path("/root/quant_project/storage/database/zhulong.duckdb"),
        'TABLE_STOCK_DAILY': "fact_daily",
    }

    # 2. TIDE sensor: safe import with fallback
    try:
        spec = importlib.util.spec_from_file_location(
            "tide_sensor",
            str(Path("/root/quant_project/04_governance/lib/tide_sensor.py"))
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        RUNTIME_OVERRIDES['get_tide_sensor'] = mod.get_sensor
        RUNTIME_OVERRIDES['TIDE_AVAILABLE'] = True
        logger.info('  \u2705 TIDE sensor loaded')
    except Exception as e:
        RUNTIME_OVERRIDES['TIDE_AVAILABLE'] = False
        RUNTIME_OVERRIDES['get_tide_sensor'] = lambda: None
        logger.error(f'  \u26a0\ufe0f TIDE sensor unavailable: {e}', exc_info=True)

    # 3. Apply overrides to decision_engine module after import
    try:
        de_mod = importlib.import_module('02_brain.decision_engine')
        for key, val in RUNTIME_OVERRIDES.items():
            if hasattr(de_mod, key) or key in ('DB_PATH', 'TABLE_STOCK_DAILY', 'TIDE_AVAILABLE', 'get_tide_sensor'):
                setattr(de_mod, key, val)
                logger.info(f'  \u2705 {key} \u2192 override applied')
    except ImportError as e:
        logger.error(f'  \u26a0\ufe0f decision_engine import skipped: {e}', exc_info=True)

    # 4. zeta/ import shim (safe, idempotent)
    zeta_dir = BASE_DIR / 'zeta'
    zeta_dir.mkdir(exist_ok=True)
    init = zeta_dir / '__init__.py'
    if not init.exists():
        init.write_text('# Auto-generated by zhulong_daemon\n')

    shim_map = {
        'zeta_collector_v240.py': BASE_DIR / '02_brain' / 'lib' / 'zeta_collector.py',
        'zeta_auditor_v240.py':  BASE_DIR / '02_brain' / 'lib' / 'zeta_auditor.py',
    }
    for name, target in shim_map.items():
        link = zeta_dir / name
        if link.exists():
            continue
        if not target.exists():
            logger.warning(f'  \u26a0\ufe0f \u6e90\u4e0d\u5b58\u5728: {target}'); continue
        try:
            os.symlink(str(target), str(link))
            logger.info(f'  \U0001f517 zeta/{name} \u2192 {target.name}')
        except OSError:
            logger.error(f'  symlink failed, fallback to copy: {target}', exc_info=True)
            shutil.copy2(str(target), str(link))
            logger.info(f'  \U0001f4cb zeta/{name} \u2190 copy')
    try:
        _ensure_pipeline_state_table()
    except Exception as exc:
        logger.critical('  [OPS] ops_pipeline_state init failed: %s', exc, exc_info=True)
        return False
    logger.info('  [OPS] ops_pipeline_state ready')

    if not _check_fact_daily_schema():
        logger.error('  [SCHEMA] fact_daily preflight failed')
        return False
    logger.info('  [SCHEMA] fact_daily preflight ready')

    if not _ensure_nexus_runtime_columns():
        logger.error('  [SCHEMA] nexus_audits runtime preflight failed')
        return False

    calendar_coverage = TRADE_CALENDAR.inspect_calendar_coverage(DB_PATH)
    if calendar_coverage.get('status') == 'OK':
        logger.info('  [CALENDAR] authoritative cache ready: %s', calendar_coverage)
    else:
        logger.warning(
            '  [CALENDAR] cache not ready; dated phases remain fail-closed: %s',
            calendar_coverage,
        )

    logger.info('\U0001f527 Bootstrap \u5b8c\u6210 (v2)')
    logger.info('=' * 55)
    return True


# ╔══════════════════════════════════════════════════════════════╗
# ║       Phase 1: 盘中快轨 09:15-15:00 (每 10 分钟)            ║
# ╚══════════════════════════════════════════════════════════════╝


@monitor_phase('phase_intraday')
def phase_intraday():
    """Echo awaken + Eagle/Owl tactical hooks."""
    if not _is_trading_day_for_current(notify=True, context='phase_intraday'):
        return

    now = datetime.now()
    if not is_intraday_scan_time(now):
        return

    tactics_dir = str(BASE_DIR / '03_tactics')
    base_dir = str(BASE_DIR)
    if tactics_dir not in sys.path:
        sys.path.insert(0, tactics_dir)
    if base_dir not in sys.path:
        sys.path.insert(0, base_dir)

    intraday_td = _current_trade_date_str()
    if _echo_enabled():
        tide_td = _latest_fact_daily_trade_date_str(intraday_td)
        logger.info(f'[INTRADAY] Echo scan ({now:%H:%M}) trade_date={intraday_td} tide_date={tide_td}')

        echo_ok = safe_run(
            [PYTHON, '-c',
             'import sys; '
             'sys.path.insert(0,"/root/quant_project/03_tactics"); '
             'sys.path.insert(0,"/root/quant_project"); '
             'from echo_manager import get_echo_manager; '
             'em = get_echo_manager(); '
             f'ready = em.scan_ready({tide_td!r}); r = em.attempt_awaken({tide_td!r}); '
             'print(f"Echo: {len(ready)} ready, {len(r)} awakened")'],
            label='Echo-Scan', timeout=300, use_sem=True)

        if echo_ok:
            echo_hits = _collect_recent_echo_awakened(window_minutes=15)
            if echo_hits:
                _push_intraday_hits('ECHO', echo_hits)
    else:
        _log_echo_disabled_once(intraday_td)

    tactics_bridge = None
    try:
        tactics_bridge = importlib.import_module('tactics_bridge')
    except Exception as _tb_e:
        logger.error(f'  [INTRADAY] tactics_bridge load failed: {_tb_e}', exc_info=True)
        _push_exception_once('Zhulong Intraday Bridge Error', _tb_e, context='phase_intraday:tactics_bridge_load')

    if tactics_bridge is not None:
        # ---- shadow paper position management ----
        try:
            shadow_future = tactics_bridge.shadow_position_scan()
            if shadow_future is not None:
                shadow_future.add_done_callback(lambda fut, _ch='SHADOW_POSITION': _on_tactic_done(_ch, fut))
        except Exception as _sp_e:
            logger.error(f'  [INTRADAY] Shadow position skip: {_sp_e}', exc_info=True)
            _push_exception_once('Zhulong Shadow Position Exception', _sp_e, context='phase_intraday:shadow_position_scan')

        # ---- tactical slots ----
        # Eagle Pulse Audit
        try:
            eagle_future = tactics_bridge.eagle_scan()
            if eagle_future is not None:
                eagle_future.add_done_callback(lambda fut, _ch='EAGLE': _on_tactic_done(_ch, fut))
        except Exception as _ee_e:
            logger.error(f'  [INTRADAY] EagleEye skip: {_ee_e}', exc_info=True)
            _push_exception_once('Zhulong EagleEye Exception', _ee_e, context='phase_intraday:eagle_scan')

        if now.hour == 14 and now.minute >= 30:
            # Owl EOD Momentum
            try:
                owl_future = tactics_bridge.owl_scan()
                if owl_future is not None:
                    owl_future.add_done_callback(lambda fut, _ch='OWL': _on_tactic_done(_ch, fut))
            except Exception as _owl_e:
                logger.error(f'  [INTRADAY] Owl skip: {_owl_e}', exc_info=True)
                _push_exception_once('Zhulong Owl Exception', _owl_e, context='phase_intraday:owl_scan')

        try:
            if hasattr(tactics_bridge, 'backpressure_snapshot'):
                bp = tactics_bridge.backpressure_snapshot()
                logger.info(
                    '[INTRADAY-BP] inflight=%s active=%s queued=%s capacity=%s '
                    'high_watermark=%s submitted=%s completed=%s rejected=%s submit_failed=%s',
                    bp.get('inflight', 0), bp.get('active', 0), bp.get('queued', 0),
                    bp.get('capacity', 0), bp.get('high_watermark', 0),
                    bp.get('submitted', 0), bp.get('completed', 0),
                    bp.get('rejected', 0), bp.get('submit_failed', 0),
                )
        except Exception as _bp_e:
            logger.warning('[INTRADAY-BP] snapshot unavailable: %s', _bp_e)

        try:
            if hasattr(tactics_bridge, 'eagle_persistence_snapshot'):
                eagle_persist = tactics_bridge.eagle_persistence_snapshot()
                logger.info(
                    '[EAGLE-PERSIST] inflight=%s active=%s queued=%s '
                    'high_watermark=%s submitted=%s completed=%s failed=%s submit_failed=%s',
                    eagle_persist.get('inflight', 0),
                    eagle_persist.get('active', 0),
                    eagle_persist.get('queued', 0),
                    eagle_persist.get('high_watermark', 0),
                    eagle_persist.get('submitted', 0),
                    eagle_persist.get('completed', 0),
                    eagle_persist.get('failed', 0),
                    eagle_persist.get('submit_failed', 0),
                )
        except Exception as _eagle_persist_e:
            logger.warning(
                '[EAGLE-PERSIST] snapshot unavailable: %s',
                _eagle_persist_e,
            )

    gc.collect()


@monitor_phase('phase_intraday_protocol_review')
def phase_intraday_protocol_review():
    """Weekly side-channel review for Eagle/Owl/Rabbit observer events."""
    try:
        import sys as _sys
        tactics_dir = str(BASE_DIR / '03_tactics')
        base_dir = str(BASE_DIR)
        if tactics_dir not in _sys.path:
            _sys.path.insert(0, tactics_dir)
        if base_dir not in _sys.path:
            _sys.path.insert(0, base_dir)
        from protocol_observer import run_weekly_review

        stats = run_weekly_review(push=True)
        logger.info('[INTRADAY_PROTOCOL] weekly review done: %s', stats)
    except Exception as exc:
        logger.error('[INTRADAY_PROTOCOL] weekly review exception: %s', exc, exc_info=True)
        _push_exception_once('Zhulong Intraday Protocol Review Exception', exc, context='phase_intraday_protocol_review')


@monitor_phase('phase_anchor_observer')
def phase_anchor_observer():
    """Daily side-channel Static Anchor observer; records protocol events only."""
    td = _resolve_trade_date_override() or _current_trade_date_str()
    if not _is_trading_day_for_current(notify=True, context='phase_anchor_observer'):
        logger.info('[ANCHOR_OBSERVER] non-trading day, skip')
        return
    if _pipeline_phase_in_progress(td, ('phase_harvest', 'phase_audit')):
        logger.warning('[ANCHOR_OBSERVER] skipped because harvest/audit is in progress: %s', td)
        return

    try:
        import sys as _sys
        tactics_dir = str(BASE_DIR / '03_tactics')
        base_dir = str(BASE_DIR)
        if tactics_dir not in _sys.path:
            _sys.path.insert(0, tactics_dir)
        if base_dir not in _sys.path:
            _sys.path.insert(0, base_dir)
        from rabbit_anchor import run_anchor_observer

        stats = run_anchor_observer(td, limit=20)
        logger.info('[ANCHOR_OBSERVER] done: %s', stats)
    except Exception as exc:
        logger.error('[ANCHOR_OBSERVER] exception: %s', exc, exc_info=True)
        _push_exception_once('Zhulong Anchor Observer Exception', exc, context='phase_anchor_observer')


# ????????????????????????????????????????????????????????????????
# ?          Phase 2: ???????? 20:30                       ?
# ????????????????????????????????????????????????????????????????


@monitor_phase('phase_harvest')
def phase_harvest():
    """DataSync + Zeta + derived features, then persist harvest state."""
    td = _current_trade_date_str()

    _HEARTBEAT_DB_PAUSED.set()
    try:
        forced_td = _resolve_trade_date_override()
        if not _is_trading_day_for_current(notify=True, context='phase_harvest'):
            logger.info('[HARVEST] non-trading day, skip')
            _set_pipeline_state(td, 'phase_harvest', 'SKIPPED', 'non-trading day')
            return
        if forced_td:
            logger.warning('[HARVEST] forced trade date mode active: %s', forced_td)

        logger.info('=' * 50)
        logger.info('[HARVEST] Phase 2 @ 20:30 start')
        logger.info('=' * 50)

        _set_pipeline_state(td, 'phase_harvest', 'IN_PROGRESS', 'Harvest_STARTED')

        data_sync_ok = safe_run(
            [PYTHON, str(COMP['data_sync']), '--sync', '--verify'],
            label='DataSync',
            timeout=1800,
        )
        if not data_sync_ok:
            _set_pipeline_state(td, 'phase_harvest', 'FAILED', 'DataSync_FAILED')
            logger.error('[HARVEST] failed: DataSync_FAILED')
            return
        _set_pipeline_state(td, 'phase_harvest', 'IN_PROGRESS', 'DataSync_DONE')

        zeta_ok = safe_run(
            [PYTHON, str(COMP['zeta_collect'])],
            label='ZetaCollect',
            timeout=600,
        )
        if not zeta_ok:
            _set_pipeline_state(td, 'phase_harvest', 'FAILED', 'Zeta_FAILED')
            logger.error('[HARVEST] failed: Zeta_FAILED')
            return
        _set_pipeline_state(td, 'phase_harvest', 'IN_PROGRESS', 'Zeta_DONE')

        derived_ok = safe_run(
            [PYTHON, str(COMP['derived_features'])],
            label='DerivedFeatures',
            timeout=600,
        )
        if not derived_ok:
            _set_pipeline_state(td, 'phase_harvest', 'FAILED', 'Derived_FAILED')
            logger.error('[HARVEST] failed: Derived_FAILED')
            return
        _set_pipeline_state(td, 'phase_harvest', 'IN_PROGRESS', 'Derived_DONE')

        rps_ok = safe_run(
            [PYTHON, str(COMP['rps_etl'])],
            label='RpsETL',
            timeout=900,
        )
        if not rps_ok:
            _set_pipeline_state(td, 'phase_harvest', 'FAILED', 'RPS_FAILED')
            logger.error('[HARVEST] failed: RPS_FAILED')
            return
        _set_pipeline_state(td, 'phase_harvest', 'IN_PROGRESS', 'RPS_DONE')

        try:
            with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
                daily_cnt = conn.execute(
                    'SELECT COUNT(*) FROM fact_daily WHERE trade_date = ?',
                    [td],
                ).fetchone()
                zeta_cnt = conn.execute(
                    'SELECT COUNT(*) FROM fact_zeta_signals WHERE trade_date = ?',
                    [td],
                ).fetchone()
                rps_cnt = conn.execute(
                    'SELECT COUNT(*) FROM fact_rps_results WHERE trade_date = ?',
                    [td],
                ).fetchone()
                # anti-join 检查 (symbol, trade_date) 覆盖完整
                rps_missing_cnt = conn.execute('''
                    SELECT COUNT(*) FROM fact_daily d
                    LEFT JOIN fact_rps_results r
                      ON d.symbol = r.symbol AND d.trade_date = r.trade_date
                    WHERE d.trade_date = ? AND r.symbol IS NULL
                ''', [td]).fetchone()
            daily_rows = int((daily_cnt[0] if daily_cnt else 0) or 0)
            zeta_rows  = int((zeta_cnt[0]  if zeta_cnt  else 0) or 0)
            rps_rows   = int((rps_cnt[0]   if rps_cnt   else 0) or 0)
            rps_miss   = int((rps_missing_cnt[0] if rps_missing_cnt else 0) or 0)

            if daily_rows <= 0 or zeta_rows <= 0 or rps_rows <= 0 or rps_miss > 0:
                _set_pipeline_state(td, 'phase_harvest', 'FAILED', 'Integrity_FAILED')
                logger.error(
                    '[HARVEST] failed integrity gate: daily=%s zeta=%s rps=%s rps_missing=%s',
                    daily_rows,
                    zeta_rows,
                    rps_rows,
                    rps_miss,
                )
            else:
                _set_pipeline_state(td, 'phase_harvest', 'DONE', 'Harvest_DONE')
                logger.info(
                    '[HARVEST] completed, state=DONE (daily=%s, zeta=%s, rps=%s, rps_missing=0)',
                    daily_rows,
                    zeta_rows,
                    rps_rows,
                )
        except Exception as exc:
            _set_pipeline_state(td, 'phase_harvest', 'FAILED', 'Integrity_FAILED')
            logger.error('[HARVEST] integrity check exception: %s', exc, exc_info=True)
            raise
    finally:
        _refresh_api_readonly_snapshot('HARVEST')
        gc.collect()
        logger.info('[HARVEST] end | gc.collect() done')
        _HEARTBEAT_DB_PAUSED.clear()


@monitor_phase('phase_eagle_active_context')
def phase_eagle_active_context():
    """Rebuild post-close Eagle context without entering the audit chain."""
    td = _resolve_trade_date_override() or _current_trade_date_str()
    if not _env_flag('EAGLE_ACTIVE_PATH_ENABLED', '0'):
        logger.info('[EAGLE-CONTEXT] observer disabled, skip')
        return
    if not _is_trading_day_for_current(notify=False, context='phase_eagle_active_context'):
        logger.info('[EAGLE-CONTEXT] non-trading day, skip')
        return
    if _pipeline_phase_in_progress(td, ('phase_harvest', 'phase_audit')):
        logger.warning('[EAGLE-CONTEXT] skipped because harvest/audit is in progress: %s', td)
        return

    ready, reason = _can_enter_audit(td)
    if not ready:
        logger.warning('[EAGLE-CONTEXT] post-close facts unavailable, skip: %s', reason)
        return
    source_manifest = (
        BASE_DIR / 'storage' / 'reports' / 'eagle_active_path' / 'manifests'
        / f'eagle_candidates_{td}.json'
    )
    if not source_manifest.exists():
        logger.warning('[EAGLE-CONTEXT] source manifest missing, skip: %s', source_manifest)
        return

    ok = safe_run(
        [
            PYTHON,
            str(COMP['eagle_active_context']),
            '--trade-date',
            td,
            '--raw-manifest',
            str(source_manifest),
            '--dry-run',
        ],
        label='EagleActiveContext',
        timeout=600,
        use_sem=False,
    )
    if not ok:
        logger.error('[EAGLE-CONTEXT] observer artifact build failed: %s', td)
        return
    logger.info('[EAGLE-CONTEXT] observation-only artifact complete: %s', td)


def _push_wechat(title: str, content: str) -> bool:
    """PushPlus 底层发送 (带重试)"""
    token = Config.PUSHPLUS_TOKEN
    if not token:
        logger.warning('[PUSH] PUSHPLUS_TOKEN 未配置')
        return False
    push_url = os.getenv('PUSHPLUS_URL', 'https://www.pushplus.plus/send').strip()
    if push_url.startswith('http://'):
        logger.warning('[PUSH] insecure PUSHPLUS_URL configured; prefer https://')

    for attempt in range(3):
        try:
            resp = requests.post(
                push_url,
                json={'token': token, 'title': title,
                      'content': content, 'template': 'txt'},
                timeout=15, proxies=_get_proxies())
            if resp.status_code == 200 and resp.json().get('code') == 200:
                logger.info(f'[PUSH] ✅ 推送成功: {title}')
                return True
            else:
                logger.warning(f'[PUSH] ⚠️ 尝试 {attempt+1}/3 失败: {resp.text}')
        except Exception as e:
            logger.error(f'[PUSH] ⚠️ 尝试 {attempt+1}/3 异常: {e}', exc_info=True)
        time.sleep(2)

    logger.error(f'[PUSH] ❌ 3次重试全部失败: {title}')
    return False


_EXCEPTION_TITLE_CN = {
    'Zhulong PipelineState CRITICAL': '流程状态写入异常',
    'Zhulong Intraday Bridge Error': '盘中桥接异常',
    'Zhulong Shadow Position Exception': '模拟盘持仓扫描异常',
    'Zhulong EagleEye Exception': '鹰眼盘中扫描异常',
    'Zhulong Owl Exception': '盘后观察扫描异常',
    'Zhulong Intraday Protocol Review Exception': '盘中观察周报异常',
    'Zhulong Anchor Observer Exception': '锚点观察异常',
    'Zhulong Intraday Future Error': '盘中异动任务异常',
    'Zhulong Intraday SafeRunner Error': '盘中安全执行器异常',
    'Zhulong Phase Flush Exception': '早间模型热身异常',
    'Zhulong Tactics Daily Flush Exception': '盘后战术资源清理异常',
    'Zhulong Audit Probe Exception': '审计前模型探测异常',
    'Zhulong Audit State Precheck Failed': '审计状态预检异常',
    'Zhulong Audit Report Exception': '审计战报推送异常',
    'Zhulong Shadow T1 Fill Exception': '模拟盘 T+1 成交异常',
    'Zhulong Trade Calendar Refresh Exception': '交易日历刷新异常',
}

_EXCEPTION_CONTEXT_CN = {
    'ops_pipeline_state_write': '流程状态写入',
    'phase_intraday:tactics_bridge_load': '盘中战术桥接加载',
    'phase_intraday:shadow_position_scan': '盘中模拟盘持仓扫描',
    'phase_intraday:eagle_scan': '鹰眼盘中扫描',
    'phase_intraday:owl_scan': '盘后观察扫描',
    'phase_intraday_protocol_review': '盘中观察周报',
    'phase_anchor_observer': '锚点观察',
    'phase_flush:restart_ollama': '早间模型热身',
    'daily_flush:tactics_bridge': '盘后战术资源清理',
    'phase_audit:probe': '审计前模型探测',
    'phase_audit:state_precheck': '审计状态预检',
    'phase_audit:send_audit_report': '审计战报推送',
    'phase_trade_calendar_refresh': '交易日历周度刷新',
}


def _exception_title_text(title: str) -> str:
    raw = str(title or '').strip()
    return _EXCEPTION_TITLE_CN.get(raw, raw.replace('Zhulong', '烛龙') or '系统异常')


def _exception_context_text(context: str) -> str:
    raw = str(context or '').strip()
    if not raw:
        return '未标注环节'
    if raw.startswith('phase_shadow_t1_fill:'):
        mode = raw.split(':', 1)[1]
        mode_map = {'probe': '探测轮', 'main': '主轮', 'final': '最终复核轮'}
        return f'模拟盘 T+1 成交{mode_map.get(mode, mode)}'
    if raw.startswith('phase_intraday:') and raw.endswith('_future'):
        channel = raw.split(':', 1)[1].replace('_future', '').upper()
        return f'盘中{_INTRADAY_CHANNEL_NAMES.get(channel, channel)}异动任务'
    if raw.startswith('phase_intraday:') and raw.endswith('_saferunner'):
        channel = raw.split(':', 1)[1].replace('_saferunner', '').upper()
        return f'盘中{_INTRADAY_CHANNEL_NAMES.get(channel, channel)}安全执行器'
    return _EXCEPTION_CONTEXT_CN.get(raw, raw)


_ERROR_PUSH_GUARD = set()
_ERROR_PUSH_LOCK = threading.Lock()


def _error_fingerprint(exc: Exception, context: str = "") -> str:
    day = datetime.now().strftime('%Y-%m-%d')
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    tail = "|".join([ln.strip() for ln in tb.splitlines() if ln.strip()][-10:])
    seed = f"{context}|{type(exc).__name__}|{str(exc)}|{tail}"
    digest = hashlib.sha256(seed.encode("utf-8", errors="ignore")).hexdigest()[:16]
    return f"{day}|{digest}"


def _push_exception_once(title: str, exc: Exception, context: str = "") -> bool:
    fp = _error_fingerprint(exc, context=context)
    with _ERROR_PUSH_LOCK:
        if len(_ERROR_PUSH_GUARD) > 8192:
            _ERROR_PUSH_GUARD.clear()
        if fp in _ERROR_PUSH_GUARD:
            logger.warning(f'[PUSH-GUARD] suppress duplicated exception push fp={fp} title={title}')
            return False
        _ERROR_PUSH_GUARD.add(fp)

    lines = [
        f'时间：{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
        f'环节：{_exception_context_text(context)}',
        '状态：该环节已被隔离或安全停下，daemon 会继续保护主流程。',
        f'错误摘要：{type(exc).__name__}: {str(exc)[:300]}',
        f'技术指纹：{fp}',
        '',
        '说明：完整错误栈已写入 daemon 日志，手机推送只保留排障所需摘要。',
    ]
    return _push_wechat(f'烛龙系统异常 | {_exception_title_text(title)}', '\n'.join(lines))


def _push_calendar_coverage_once(coverage: dict) -> bool:
    if not coverage.get('should_alert'):
        return False
    key = '|'.join([
        'COVERAGE',
        str(coverage.get('status', 'UNKNOWN')),
        str(coverage.get('fresh_max_date', 'NONE')),
    ])
    with _CALENDAR_PUSH_LOCK:
        if key in _CALENDAR_PUSH_GUARD:
            return False
        _CALENDAR_PUSH_GUARD.add(key)
    lines = [
        f"检查日期：{coverage.get('as_of', 'unknown')}",
        f"覆盖状态：{coverage.get('status', 'UNKNOWN')}",
        f"最新有效日历：{coverage.get('fresh_max_date', coverage.get('max_date', '无'))}",
        f"剩余覆盖天数：{coverage.get('horizon_days', 'unknown')}",
        '处理：现有已知日期仍按缓存执行；未知日期一律安全关闭。',
        '',
        '说明：这是交易所日历覆盖预警，不会自动开放审计或影子盘买入。',
    ]
    return _push_wechat('烛龙交易日历覆盖预警', '\n'.join(lines))


@monitor_phase('phase_trade_calendar_refresh')
def phase_trade_calendar_refresh():
    """Weekly Tushare trade_cal refresh; calendar metadata only."""
    try:
        ok = safe_run(
            [
                PYTHON,
                str(COMP['trade_calendar']),
                '--db',
                str(DB_PATH),
                'refresh',
                '--env-file',
                str(CONFIG_ENV),
            ],
            label='TradeCalendarRefresh',
            timeout=300,
            use_sem=False,
        )
        if not ok:
            raise RuntimeError('Tushare trade_cal refresh command failed')
        coverage = TRADE_CALENDAR.inspect_calendar_coverage(DB_PATH)
        logger.info('[CALENDAR] weekly refresh coverage=%s', coverage)
        _push_calendar_coverage_once(coverage)
    except Exception as exc:
        logger.error('[CALENDAR] weekly refresh failed: %s', exc, exc_info=True)
        _push_exception_once(
            'Zhulong Trade Calendar Refresh Exception',
            exc,
            context='phase_trade_calendar_refresh',
        )


_INTRADAY_PUSH_GUARD = OrderedDict()
_INTRADAY_PUSH_LOCK = threading.Lock()
_INTRADAY_PUSH_GUARD_MAX = max(512, int(os.getenv('INTRADAY_PUSH_GUARD_MAX', '4096') or 4096))
_INTRADAY_CHANNEL_NAMES = {
    'EAGLE': '\u9e70\u773c',
    'OWL': '\u732b\u5934\u9e70',
    'ECHO': '\u56de\u58f0',
}
_VERDICT_CN = {
    'PENDING': '\u5f85\u786e\u8ba4',
    'PASS': '\u901a\u8fc7',
    'APPROVE': '\u901a\u8fc7',
    'BUY': '\u4e70\u5165',
    'HOLD': '\u89c2\u5bdf',
    'WATCH': '\u89c2\u5bdf',
    'VETO': '\u5426\u51b3',
    'REJECT': '\u62d2\u7edd',
    'SELL': '\u5356\u51fa',
}


def _cn_verdict(value) -> str:
    raw = str(value or '').strip().upper()
    if not raw:
        return '\u5f85\u786e\u8ba4'
    return _VERDICT_CN.get(raw, raw)


def _lookup_stock_names(symbols) -> dict:
    clean = []
    for sym in symbols or []:
        s = str(sym or '').strip().upper()
        if s and s not in clean:
            clean.append(s)
    if not clean:
        return {}
    try:
        placeholders = ','.join(['?'] * len(clean))
        with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
            rows = conn.execute(
                f"SELECT symbol, name FROM fact_stock_basic WHERE symbol IN ({placeholders})",
                clean,
            ).fetchall()
        return {str(sym).strip().upper(): str(name).strip() for sym, name in rows if name}
    except Exception as exc:
        logger.debug(f'[INTRADAY-PUSH] stock name lookup failed: {exc}')
        return {}


def _format_stock_display(symbol: str, name_map: dict) -> str:
    sym = str(symbol or '').strip().upper()
    name = str((name_map or {}).get(sym) or '').strip()
    return f'{name} {sym}' if name and name != sym else sym


def _normalize_intraday_hits(channel: str, payload):
    channel_u = str(channel or '').upper()
    if payload is None:
        return []

    if isinstance(payload, dict):
        items = [payload]
    elif isinstance(payload, (list, tuple, set)):
        items = list(payload)
    else:
        items = [payload]

    normalized = []
    for item in items:
        if isinstance(item, dict):
            data = dict(item)
        else:
            data = {}
            for attr in (
                'symbol', 'fingerprint', 'v_ratio', 'pct_chg',
                't3_status', 't5_verdict', 't5_score',
                'concentration', 'strength', 'eod_verdict', 'eod_score',
                'note', 'dedup_key',
            ):
                if hasattr(item, attr):
                    data[attr] = getattr(item, attr)

        symbol = str(data.get('symbol') or '').strip().upper()
        if not symbol:
            continue

        hit = {'symbol': symbol}
        if data.get('fingerprint'):
            hit['fingerprint'] = str(data.get('fingerprint'))
        if data.get('dedup_key'):
            hit['dedup_key'] = str(data.get('dedup_key'))

        if data.get('note'):
            hit['note'] = str(data.get('note'))
        elif channel_u == 'EAGLE':
            v_ratio = float(data.get('v_ratio') or 0.0)
            pct_chg = float(data.get('pct_chg') or 0.0)
            t3_status = str(data.get('t3_status') or 'PENDING')
            t5_verdict = str(data.get('t5_verdict') or 'PENDING')
            t5_score = int(float(data.get('t5_score') or 0))
            hit['note'] = (
                f'\u91cf\u6bd4:{v_ratio:.2f} \u6da8\u5e45:{pct_chg:+.1f}% '
                f'T3:{_cn_verdict(t3_status)} T5:{_cn_verdict(t5_verdict)}/{t5_score}'
            )
            hit['verdict'] = t5_verdict
        elif channel_u == 'OWL':
            concentration = float(data.get('concentration') or 0.0)
            strength = float(data.get('strength') or 0.0)
            pct_chg = float(data.get('pct_chg') or 0.0)
            eod_verdict = str(data.get('eod_verdict') or 'PASS')
            eod_score = int(float(data.get('eod_score') or 0))
            hit['note'] = (
                f'\u96c6\u4e2d\u5ea6:{concentration:.1%} \u5f3a\u5ea6:{strength:.1f}x '
                f'\u6da8\u5e45:{pct_chg:+.1f}% \u88c1\u51b3:{_cn_verdict(eod_verdict)}/{eod_score}'
            )
            hit['verdict'] = eod_verdict
        else:
            hit['note'] = str(data.get('note') or '')

        normalized.append(hit)

    return normalized


def _push_intraday_hits(channel: str, payload, is_mock: bool = False) -> bool:
    channel_u = str(channel or '').upper()
    hits = _normalize_intraday_hits(channel_u, payload)
    if not hits:
        return False

    today = datetime.now().strftime('%Y-%m-%d')
    fresh = []
    with _INTRADAY_PUSH_LOCK:
        while len(_INTRADAY_PUSH_GUARD) > _INTRADAY_PUSH_GUARD_MAX:
            _INTRADAY_PUSH_GUARD.popitem(last=False)

        for hit in hits:
            dedup_key = str(
                hit.get('dedup_key')
                or f"{today}|{channel_u}|{hit.get('symbol','')}|{hit.get('fingerprint','') or hit.get('verdict','')}"
            )
            if dedup_key in _INTRADAY_PUSH_GUARD:
                _INTRADAY_PUSH_GUARD.move_to_end(dedup_key)
                continue
            _INTRADAY_PUSH_GUARD[dedup_key] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if len(_INTRADAY_PUSH_GUARD) > _INTRADAY_PUSH_GUARD_MAX:
                _INTRADAY_PUSH_GUARD.popitem(last=False)
            fresh.append(hit)

    if not fresh:
        logger.info(f'[INTRADAY-PUSH] {channel_u} no fresh hits after dedup')
        return False

    prefix = '\u6a21\u62df ' if is_mock else ''
    channel_name = _INTRADAY_CHANNEL_NAMES.get(channel_u, channel_u)
    title = f'{prefix}\u70db\u9f99\u76d8\u4e2d\u63d0\u9192 | {channel_name} {len(fresh)}\u6761'
    name_map = _lookup_stock_names([hit.get('symbol') for hit in fresh])
    lines = [
        f'\u65f6\u95f4\uff1a{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
        f'\u901a\u9053\uff1a{channel_name}',
        '',
    ]
    for i, hit in enumerate(fresh, 1):
        note = str(hit.get('note') or '').strip()
        display = _format_stock_display(hit.get('symbol'), name_map)
        if note:
            lines.append(f'{i}. {display} | {note}')
        else:
            lines.append(f'{i}. {display}')

    body = '\n'.join(lines)
    ok = _push_wechat(title, body)
    if ok:
        logger.info(f'[INTRADAY-PUSH] {channel_u} pushed {len(fresh)} hit(s)')
    return ok


def _collect_recent_echo_awakened(window_minutes: int = 15):
    threshold = (datetime.now() - timedelta(minutes=window_minutes)).strftime('%Y-%m-%d %H:%M:%S')
    sql = (
        "SELECT symbol, fingerprint, COALESCE(veto_reason, ''), COALESCE(original_score, 0), updated_at "
        "FROM fact_echo_logs "
        "WHERE current_state = 'AWAKENED' "
        "  AND updated_at >= CAST(? AS TIMESTAMP) "
        "ORDER BY updated_at DESC LIMIT 20"
    )

    try:
        with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
            rows = conn.execute(sql, [threshold]).fetchall()
    except Exception as exc:
        logger.error(f'[ECHO] collect awakened failed: {exc}', exc_info=True)
        return []

    hits = []
    for sym, fp, reason, score, updated_at in rows:
        if not sym:
            continue
        note = f'fp={str(fp)[:10]} score={int(float(score or 0))}'
        if reason:
            note += f' reason={str(reason)[:24]}'
        hits.append({
            'symbol': str(sym),
            'fingerprint': str(fp or ''),
            'note': note,
            'dedup_key': f"ECHO|{sym}|{fp or ''}|{updated_at}",
        })

    return hits


def _on_tactic_done(channel: str, future):
    channel_u = str(channel or '').upper()
    try:
        payload = future.result()
    except Exception as exc:
        logger.error(f'[INTRADAY] {channel_u} future failed: {exc}', exc_info=True)
        _push_exception_once('Zhulong Intraday Future Error', exc, context=f'phase_intraday:{channel_u}_future')
        return

    if payload is None:
        logger.info(f'[INTRADAY] {channel_u} completed with empty payload')
        return

    if isinstance(payload, dict) and payload.get('ok') is False:
        err_text = str(payload.get('error') or 'unknown SafeRunner failure')
        tactic = str(payload.get('tactic') or channel_u)
        tb = str(payload.get('traceback') or '').strip()
        logger.error(f'[INTRADAY] {channel_u} failed inside SafeRunner | tactic={tactic} | error={err_text}\n{tb}')
        _push_exception_once(
            'Zhulong Intraday SafeRunner Error', RuntimeError(err_text), context=f'phase_intraday:{channel_u}_saferunner'
        )
        return

    if isinstance(payload, dict):
        summary_keys = [
            'trade_date', 'hold_scanned', 'raised_stop', 'sold', 'held',
            'no_realtime', 'skip_invalid',
        ]
        summary = ' '.join(
            f'{key}={payload.get(key)}'
            for key in summary_keys
            if key in payload
        )
        logger.info(f'[INTRADAY] {channel_u} completed | {summary or payload}')
        return

    pushed = _push_intraday_hits(channel_u, payload)
    if not pushed:
        try:
            count = len(payload)
        except Exception:
            count = 'unknown'
        logger.info(f'[INTRADAY] {channel_u} completed | hits={count} pushed=0')


def _read_audit_completion_receipt(trade_date: str, run_id: str) -> dict:
    """Read the exact decision-engine receipt used to prove a valid zero-candidate run."""
    td = str(trade_date or '').strip()
    rid = str(run_id or '').strip()
    result = {
        'receipt_valid': False,
        'receipt_phase': '',
        'receipt_candidate_count': -1,
        'receipt_l2_count': -1,
        'receipt_l3_count': -1,
        'receipt_l1_gate_stats': {},
        'receipt_audit_contract_binding': {},
        'receipt_reason': 'missing_receipt',
    }
    try:
        payload = json.loads(NEXUS_STATE_PATH.read_text(encoding='utf-8'))
    except FileNotFoundError:
        return result
    except Exception as exc:
        result['receipt_reason'] = f'receipt_read_error:{type(exc).__name__}'
        return result

    if not isinstance(payload, dict):
        result['receipt_reason'] = 'receipt_not_object'
        return result

    phase = str(payload.get('current_phase') or '').strip().upper()
    candidates = payload.get('candidates')
    l2_passed = payload.get('l2_passed')
    l3_results = payload.get('l3_results')
    l1_gate_stats = payload.get('l1_gate_stats')
    if not isinstance(l1_gate_stats, dict):
        l1_gate_stats = {}
    audit_contract_binding = payload.get('audit_contract_binding')
    if not isinstance(audit_contract_binding, dict):
        audit_contract_binding = {}
    lists_valid = all(isinstance(value, list) for value in (candidates, l2_passed, l3_results))
    exact_scope = (
        str(payload.get('trade_date') or '').strip() == td
        and str(payload.get('run_id') or '').strip() == rid
    )
    checkpoint_present = bool(str(payload.get('last_checkpoint') or '').strip())

    result.update({
        'receipt_phase': phase,
        'receipt_candidate_count': len(candidates) if isinstance(candidates, list) else -1,
        'receipt_l2_count': len(l2_passed) if isinstance(l2_passed, list) else -1,
        'receipt_l3_count': len(l3_results) if isinstance(l3_results, list) else -1,
        'receipt_l1_gate_stats': l1_gate_stats,
        'receipt_audit_contract_binding': audit_contract_binding,
    })
    if not exact_scope:
        result['receipt_reason'] = 'receipt_scope_mismatch'
    elif phase != 'COMPLETED':
        result['receipt_reason'] = 'receipt_not_completed'
    elif not checkpoint_present:
        result['receipt_reason'] = 'receipt_missing_checkpoint'
    elif not lists_valid:
        result['receipt_reason'] = 'receipt_invalid_stage_lists'
    else:
        result['receipt_valid'] = True
        result['receipt_reason'] = 'ok'
    return result


def _validate_audit_run_completion(
    trade_date: str,
    run_id: str,
    source_row_count: int | None = None,
) -> tuple[bool, dict]:
    td = str(trade_date or '').strip()
    rid = str(run_id or '').strip()
    if not td or not rid:
        return False, {'reason': 'missing_scope'}
    try:
        with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
            row = conn.execute(
                """
                SELECT
                    COUNT(*) AS total_rows,
                    SUM(
                        CASE
                            WHEN UPPER(COALESCE(status, '')) = 'L4_DONE'
                              OR RIGHT(UPPER(COALESCE(status, '')), 9) = '_TERMINAL'
                            THEN 1 ELSE 0
                        END
                    ) AS terminal_rows,
                    SUM(CASE WHEN COALESCE(l4_final_verdict, '') <> '' THEN 1 ELSE 0 END) AS l4_rows,
                    SUM(
                        CASE WHEN starts_with(COALESCE(task_id, ''), ?) THEN 0 ELSE 1 END
                    ) AS task_scope_mismatches
                FROM nexus_audits
                WHERE trade_date = ?
                  AND run_id = ?
                """,
                [f'{rid}_', td, rid],
            ).fetchone()
        stats = {
            'total_rows': int((row[0] if row else 0) or 0),
            'terminal_rows': int((row[1] if row else 0) or 0),
            'l4_rows': int((row[2] if row else 0) or 0),
            'task_scope_mismatches': int((row[3] if row else 0) or 0),
            'source_row_count': int(source_row_count) if source_row_count is not None else -1,
        }
        row_completion_ready = (
            stats['total_rows'] > 0
            and stats['terminal_rows'] == stats['total_rows']
            and stats['task_scope_mismatches'] == 0
        )

        zero_candidate_ready = False
        if stats['total_rows'] == 0:
            receipt = _read_audit_completion_receipt(td, rid)
            stats.update(receipt)
            gate_stats_raw = receipt.get('receipt_l1_gate_stats')
            required_gate_keys = {
                'raw_count', 'passed_count', 'candidate_errors', 'fatal_error'
            }
            gate_stats_present = (
                isinstance(gate_stats_raw, dict)
                and required_gate_keys.issubset(gate_stats_raw)
            )
            gate_stats = gate_stats_raw if gate_stats_present else {}
            stats['receipt_l1_gate_stats_present'] = gate_stats_present
            zero_candidate_ready = (
                stats['source_row_count'] > 0
                and receipt['receipt_valid']
                and receipt['receipt_candidate_count'] == 0
                and receipt['receipt_l2_count'] == 0
                and receipt['receipt_l3_count'] == 0
                and gate_stats_present
                and int(gate_stats.get('raw_count', 0) or 0) > 0
                and int(gate_stats.get('passed_count', -1)) == 0
                and int(gate_stats.get('candidate_errors', -1)) == 0
                and not str(gate_stats.get('fatal_error') or '').strip()
            )

        ready = row_completion_ready or zero_candidate_ready
        if row_completion_ready:
            stats['completion_mode'] = 'rows'
        elif zero_candidate_ready:
            stats['completion_mode'] = 'zero_candidates'
        else:
            stats['completion_mode'] = 'incomplete'

        logger.info(
            '[AUDIT] exact run validation | trade_date=%s run_id=%s ready=%s stats=%s',
            td, rid, ready, stats,
        )
        return ready, stats
    except Exception as exc:
        logger.error('[AUDIT] exact run validation failed: %s', exc, exc_info=True)
        return False, {'reason': f'validation_error:{type(exc).__name__}'}


def _format_zero_candidate_funnel(stats: dict) -> tuple[str, str]:
    gate = stats.get("receipt_l1_gate_stats") or {}
    raw_count = int(gate.get("raw_count", 0) or 0)
    l15_count = int(stats.get("receipt_candidate_count", 0) or 0)
    l2_count = int(stats.get("receipt_l2_count", 0) or 0)
    l3_count = int(stats.get("receipt_l3_count", 0) or 0)
    l4_count = int(stats.get("l4_rows", 0) or 0)
    funnel = (
        f"L1\u539f\u59cb {raw_count} \u2192 L1.5 {l15_count} \u2192 "
        f"L2 {l2_count} \u2192 L3 {l3_count} \u2192 L4 {l4_count}"
    )
    reasons = []
    ma_reject = int(gate.get("ma_alignment_false", 0) or 0)
    score_reject = int(gate.get("score_not_above_threshold", 0) or 0)
    invalid_score = int(gate.get("invalid_score", 0) or 0)
    if ma_reject:
        reasons.append(f"\u5747\u7ebf\u672a\u5bf9\u9f50 {ma_reject}")
    if score_reject:
        threshold = float(gate.get("score_threshold", 40.0) or 40.0)
        reasons.append(f"\u5f62\u6001\u5206\u4e0d\u9ad8\u4e8e{threshold:g} {score_reject}")
    if invalid_score:
        reasons.append(f"\u5f62\u6001\u5206\u5f02\u5e38 {invalid_score}")
    reason = "\uff1b".join(reasons) if reasons else "\u65e0\u540e\u7eed\u5ba1\u8ba1\u5019\u9009"
    return funnel, reason


def _run_audit_funnel_observer(
    trade_date: str,
    run_id: str,
    snapshot_ready: bool,
) -> bool:
    """Persist the BL-020 sidecar only after the authoritative audit snapshot exists."""
    td = str(trade_date or '').strip()
    rid = str(run_id or '').strip()
    if not _env_flag('AUDIT_FUNNEL_OBSERVER_ENABLED', '1'):
        logger.info('[AUDIT-FUNNEL] disabled by AUDIT_FUNNEL_OBSERVER_ENABLED=0')
        return True
    if not td or not rid:
        logger.warning('[AUDIT-FUNNEL] skipped: exact trade_date/run_id scope is missing')
        return False
    if not snapshot_ready:
        logger.warning(
            '[AUDIT-FUNNEL] skipped: API read-only snapshot refresh failed | '
            'trade_date=%s run_id=%s',
            td,
            rid,
        )
        return False

    output_dir = BASE_DIR / 'storage' / 'reports' / 'audit_funnel_observer'
    ok = safe_run(
        [
            PYTHON,
            str(COMP['audit_funnel_observer']),
            '--db',
            str(API_SNAPSHOT_DB_PATH),
            '--state',
            str(NEXUS_STATE_PATH),
            '--trade-date',
            td,
            '--run-id',
            rid,
            '--output-dir',
            str(output_dir),
            '--write-artifacts',
        ],
        label='AuditFunnelObserver',
        timeout=300,
        use_sem=False,
    )
    if ok:
        logger.info('[AUDIT-FUNNEL] frozen observer artifact ready | trade_date=%s run_id=%s', td, rid)
    else:
        logger.warning('[AUDIT-FUNNEL] observer failed; authoritative audit remains unchanged | trade_date=%s run_id=%s', td, rid)
    return bool(ok)


def _audit_reason_text(reason: str) -> str:
    raw = str(reason or '').strip()
    upper = raw.upper()
    mapping = {
        'L3_VETO_GATE': '前置复核否决',
        'NOTARY HARD VETO': '书记官硬否决',
        'UNKNOWN': '未标注原因',
        'AUDIT_FAILED': '审计子进程失败',
        'RUN_INCOMPLETE': '审计结果未完整落库',
    }
    if upper.startswith('NOTARY HARD VETO'):
        return '书记官硬否决'
    if upper.startswith('VALIDATION_ERROR'):
        return '结果完整性校验异常'
    return mapping.get(upper, raw or '未标注原因')


def _audit_gate_text(reason: str) -> str:
    raw = str(reason or '').strip()
    lower = raw.lower()
    if 'harvest' in lower:
        return '晚间数据收割尚未完成，审计已按纪律暂停'
    if 'already' in lower and 'done' in lower:
        return '今日审计已经完成，避免重复运行'
    if 'missing' in lower or 'unavailable' in lower:
        return '前置数据不足，审计已安全拒绝启动'
    return raw or '前置条件不满足，审计已安全拒绝启动'


_AUDIT_PASS_NEXT_GATES_CN = '后续仍需通过：账户资格、交易类型与契约、新闻风险、市场潮汐和 T+1 进入条件。'
_AUDIT_PASS_BOUNDARY_CN = '说明：PASS 仅表示取得后续安全门评估资格，不代表买入建议，也不会直接创建影子盘成交。'


def send_audit_report(trade_date: str, run_id: str):
    """Audit report push (strictly keyed by trade_date + run_id)."""
    td = str(trade_date or '').strip()
    rid = str(run_id or '').strip()
    if not td or not rid:
        logger.warning(f'[PUSH] send_audit_report skipped: trade_date={td or "<empty>"}, run_id={rid or "<empty>"}')
        return

    today_short = td[5:] if len(td) >= 10 else td
    now_str = time.strftime('%Y-%m-%d %H:%M:%S')
    trade_tag = f'[\u4ea4\u6613\u65e5: {td}（定位编号：{rid[:4]}）]'

    charged = []
    audit_stats = {
        'run_rows': 0,
        'l2_passed': 0,
        'l3_veto_gate': 0,
        'l4_judge_veto': 0,
        'notary_hard_veto': 0,
        'l2_terminal': 0,
    }
    veto_top = []
    try:
        with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
            rows = conn.execute(
                """
                SELECT
                    n.symbol,
                    COALESCE(NULLIF(TRIM(n.name), ''), NULLIF(TRIM(b.name), ''), n.symbol) AS display_name,
                    n.l4_final_verdict,
                    COALESCE(n.l4_final_score, n.final_score, n.l3_audit_score, 0) AS display_score
                FROM nexus_audits n
                LEFT JOIN fact_stock_basic b
                  ON b.symbol = n.symbol
                WHERE n.trade_date = ?
                  AND n.run_id = ?
                  AND UPPER(n.l4_final_verdict) = ?
                ORDER BY display_score DESC, n.symbol
                """,
                [td, rid, AUDIT_PASS],
            ).fetchall()
            charged = [(r[0], r[1] or r[0], r[2] or '', float(r[3] or 0)) for r in rows]

            stats_row = conn.execute(
                """
                SELECT
                    COUNT(1) AS run_rows,
                    SUM(CASE WHEN COALESCE(l2_passed, FALSE) THEN 1 ELSE 0 END) AS l2_passed,
                    SUM(CASE WHEN UPPER(COALESCE(l4_veto_reason, '')) = 'L3_VETO_GATE' THEN 1 ELSE 0 END) AS l3_veto_gate,
                    SUM(CASE WHEN UPPER(COALESCE(status, '')) = 'L4_DONE'
                              AND UPPER(COALESCE(l4_final_verdict, '')) = 'VETO' THEN 1 ELSE 0 END) AS l4_judge_veto,
                    SUM(CASE WHEN UPPER(COALESCE(l4_veto_reason, '')) LIKE 'NOTARY HARD VETO%' THEN 1 ELSE 0 END) AS notary_hard_veto,
                    SUM(CASE WHEN UPPER(COALESCE(status, '')) IN ('L2_REJECTED_TERMINAL', 'L2_FAILED_TERMINAL') THEN 1 ELSE 0 END) AS l2_terminal
                FROM nexus_audits
                WHERE trade_date = ? AND run_id = ?
                """,
                [td, rid],
            ).fetchone()
            if stats_row:
                audit_stats = {
                    'run_rows': int(stats_row[0] or 0),
                    'l2_passed': int(stats_row[1] or 0),
                    'l3_veto_gate': int(stats_row[2] or 0),
                    'l4_judge_veto': int(stats_row[3] or 0),
                    'notary_hard_veto': int(stats_row[4] or 0),
                    'l2_terminal': int(stats_row[5] or 0),
                }

            veto_rows = conn.execute(
                """
                SELECT COALESCE(NULLIF(TRIM(l4_veto_reason), ''), 'UNKNOWN') AS veto_reason,
                       COUNT(1) AS cnt
                FROM nexus_audits
                WHERE trade_date = ? AND run_id = ?
                  AND UPPER(COALESCE(l4_final_verdict, '')) = 'VETO'
                GROUP BY 1
                ORDER BY cnt DESC
                LIMIT 3
                """,
                [td, rid],
            ).fetchall()
            veto_top = [(str(r[0]), int(r[1])) for r in veto_rows]
    except Exception as e:
        logger.error(f'[PUSH] query nexus_audits failed: {e}', exc_info=True)

    tide_alert = None
    try:
        import importlib.util as ilu
        ts_path = BASE_DIR / '04_governance' / 'lib' / 'tide_sensor.py'
        if ts_path.exists():
            spec = ilu.spec_from_file_location('tide_sensor', str(ts_path))
            ts_mod = ilu.module_from_spec(spec)
            spec.loader.exec_module(ts_mod)
            if hasattr(ts_mod, 'get_sensor'):
                sensor = ts_mod.get_sensor()
                if hasattr(sensor, 'get_risk_gate'):
                    tide_state = sensor.get_risk_gate(trade_date=td)
                    if tide_state is not None:
                        ratio = getattr(tide_state, 'ma20_ratio', 0.5)
                        gate = getattr(tide_state, 'risk_gate', 'CAUTION')
                        if ratio < 0.3 or gate == 'FORCE_NO_EDGE':
                            gate_text = '强制无优势' if gate == 'FORCE_NO_EDGE' else str(gate)
                            tide_alert = f'市场健康比例 {ratio:.2f}，当前风控状态：{gate_text}'
    except Exception as e:
        logger.error(f'[PUSH] tide sensor query failed: {e}', exc_info=True)

    if tide_alert:
        _push_wechat(
            '\u98ce\u63a7\u63d0\u793a | \u6f6e\u6c50\u538b\u5236',
            chr(10).join([
                trade_tag,
                '\u68c0\u6d4b\u5230\u5e02\u573a\u6f6e\u6c50\u98ce\u63a7\u538b\u5236\uff0c\u5efa\u8bae\u4fdd\u6301\u9632\u5b88\u3002',
                '',
                tide_alert,
                '',
                f'\u65f6\u95f4: {now_str}',
            ]),
        )

    if not charged:
        funnel_line = (
            f"审计漏斗：总数 {audit_stats['run_rows']} | "
            f"初筛通过 {audit_stats['l2_passed']} | "
            f"复核拦截 {audit_stats['l3_veto_gate']} | "
            f"终审否决 {audit_stats['l4_judge_veto']}"
        )
        veto_reason_line = ' / '.join([f'{_audit_reason_text(reason)}:{cnt}' for reason, cnt in veto_top]) if veto_top else '无'
        _push_wechat(
            f'\u5ba1\u8ba1\u6218\u62a5 | {today_short} \u7a7a\u4ed3\u9632\u5b88',
            chr(10).join([
                trade_tag,
                f'{td} \u5168\u57df\u5ba1\u8ba1\u5df2\u5b8c\u6210',
                '',
                '\u7cfb\u7edf\u8fd0\u884c\u6b63\u5e38\u3002\u4eca\u65e5\u672a\u4ea7\u751f\u53ef\u8fdb\u5165\u540e\u7eed\u5b89\u5168\u95e8\u7684 PASS \u5019\u9009\u3002',
                funnel_line,
                f"分布：书记官硬否决 {audit_stats['notary_hard_veto']} | 初筛终止 {audit_stats['l2_terminal']}",
                f'主要否决原因：{veto_reason_line}',
                '',
                f'\u65f6\u95f4: {now_str}',
            ]),
        )
    else:
        lines = [
            f'{i}. {name} {sym} | 审计安全分 {score:.0f}'
            for i, (sym, name, _verdict, score) in enumerate(charged, 1)
        ]
        body = chr(10).join(lines)
        _push_wechat(
            f'\u5ba1\u8ba1\u6218\u62a5 | {today_short} \u540e\u7eed\u5b89\u5168\u95e8\u5019\u9009 ({len(charged)}\u53ea)',
            chr(10).join([
                trade_tag,
                f'{td} \u5168\u57df\u5ba1\u8ba1\u5b8c\u6210\uff0c\u4ee5\u4e0b\u6807\u7684\u53d6\u5f97\u8fdb\u5165\u540e\u7eed\u5b89\u5168\u95e8\u7684\u8d44\u683c\uff1a',
                '',
                body,
                '',
                _AUDIT_PASS_NEXT_GATES_CN,
                _AUDIT_PASS_BOUNDARY_CN,
                '',
                f'\u65f6\u95f4: {now_str}',
            ]),
        )

    logger.info(f'[PUSH] audit report sent | trade_date={td} run_id={rid} selected={len(charged)}')




@monitor_phase('phase_flush')
def phase_flush():
    logger.info('=' * 50)
    logger.info('[Hot Flush] Phase Flush @ 08:00')
    logger.info('=' * 50)
    try:
        import sys; sys.path.insert(0, '/root/quant_project/02_brain')
        from ollama_probe import restart_ollama_if_needed
        restart_ollama_if_needed(force=True)
        logger.info('Ollama hot flush completed')
    except Exception as e:
        logger.error(f'Hot flush failed: {e}', exc_info=True)
        _push_exception_once('Zhulong Phase Flush Exception', e, context='phase_flush:restart_ollama')


def _run_echo_daily_decay(trade_date: str | None = None) -> bool:
    td = str(trade_date or _current_trade_date_str())
    if not _echo_enabled():
        _log_echo_disabled_once(td)
        return False

    logger.info(f'[ECHO] daily TTL decay start | trade_date={td}')
    return safe_run(
        [PYTHON, '-c',
         'import sys; '
         'sys.path.insert(0,"/root/quant_project/03_tactics"); '
         'sys.path.insert(0,"/root/quant_project"); '
         'from echo_manager import get_echo_manager; '
         'em = get_echo_manager(); '
         'stats = em.decay_ttl(); '
         'print(f"Echo daily decay: expired={stats.get(\'expired\', 0)} active={stats.get(\'active_remaining\', 0)}")'],
        label='Echo-Daily-Decay', timeout=180, use_sem=True)


@monitor_phase('daily_flush')
def phase_tactics_daily_flush():
    logger.info('=' * 50)
    logger.info('[DailyFlush] Tactics resource flush @ 15:05')
    logger.info('=' * 50)
    try:
        tactics_dir = str(BASE_DIR / '03_tactics')
        base_dir = str(BASE_DIR)
        if tactics_dir not in sys.path:
            sys.path.insert(0, tactics_dir)
        if base_dir not in sys.path:
            sys.path.insert(0, base_dir)
        importlib.import_module('tactics_bridge').daily_flush()
    except Exception as exc:
        logger.error('[DailyFlush] tactics daily flush failed: %s', exc, exc_info=True)
        _push_exception_once('Zhulong Tactics Daily Flush Exception', exc, context='daily_flush:tactics_bridge')
    _run_echo_daily_decay(_current_trade_date_str())


@monitor_phase('phase_audit')
def phase_audit():
    """L1 -> L4 full audit chain."""
    audit_observer_scope = None
    try:
        try:
            import sys; sys.path.insert(0, '/root/quant_project/02_brain')
            from ollama_probe import restart_ollama_if_needed
            restart_ollama_if_needed(force=False)
        except Exception as e:
            logger.error(f'[AUDIT] probe exception: {e}', exc_info=True)
            _push_exception_once('Zhulong Audit Probe Exception', e, context='phase_audit:probe')

        forced_td = _resolve_trade_date_override()
        if not _is_trading_day_for_current(notify=True, context='phase_audit'):
            logger.info('[AUDIT] non-trading day, skip')
            return

        td = _current_trade_date_str()
        force_rerun_date = str(os.getenv('FORCE_RERUN_DATE', '') or '').strip()
        force_rerun = bool(force_rerun_date) and force_rerun_date == td
        try:
            _ensure_pipeline_state_table()
            with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
                row = conn.execute(
                    """
                    SELECT status, COALESCE(detail, '')
                    FROM ops_pipeline_state
                    WHERE trade_date = ? AND phase = 'phase_audit'
                    LIMIT 1
                    """,
                    [td],
                ).fetchone()
            if row and str(row[0] or '').upper() == 'DONE' and not force_rerun:
                logger.info('[AUDIT] skip: already done for %s | detail=%s', td, str(row[1] or ''))
                return
            if force_rerun:
                logger.warning('[AUDIT] FORCE_RERUN_DATE active: %s', force_rerun_date)
        except Exception as state_e:
            logger.warning('[AUDIT] pre-check phase_audit state failed: %s', state_e, exc_info=True)
            _push_exception_once('Zhulong Audit State Precheck Failed', state_e, context='phase_audit:state_precheck')
            return
        if forced_td:
            logger.warning('[AUDIT] forced trade date mode active: %s', forced_td)
        gate_ok, gate_reason = _can_enter_audit(td)
        if not gate_ok:
            if forced_td:
                logger.warning('[AUDIT-GATE] forced bypass for %s: %s', td, gate_reason)
            else:
                logger.warning(f'[AUDIT-GATE] blocked: {gate_reason}')
                _push_wechat(
                    '烛龙审计未启动 | 前置条件不满足',
                    chr(10).join([
                        f'交易日：{td}',
                        f'原因：{_audit_gate_text(gate_reason)}',
                        '处理：本轮审计已安全跳过，避免在数据不完整时产生错误结果。',
                    ]),
                )
                return

        target_count, raw_count, l1_limit, l2_cap = _estimate_audit_target_count(td)
        dynamic_seconds = _calc_dynamic_audit_window_seconds(target_count)
        safe_timeout = max(1800, dynamic_seconds + 900)
        timeout_at = (datetime.now() + timedelta(seconds=dynamic_seconds)).strftime('%Y-%m-%d %H:%M:%S')

        logger.info('=' * 50)
        logger.info('[AUDIT] Phase 3 @ 21:00 start, L1->L4')
        logger.info(
            f'  dynamic window: target={target_count} raw={raw_count} '
            f'(l1_limit={l1_limit}, l2_cap={l2_cap}) -> budget={dynamic_seconds}s timeout={safe_timeout}s '
            f'formula=max(1,target_count)*705*1.15'
        )
        logger.info('=' * 50)

        audit_run_id = hashlib.md5(
            f'{td}:{time.time_ns()}'.encode()
        ).hexdigest()[:8]
        news_as_of = ''
        if td == datetime.now().strftime('%Y-%m-%d'):
            news_as_of = datetime.now().astimezone().isoformat()
        else:
            try:
                with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
                    prior = conn.execute(
                        """
                        SELECT MIN(created_at)
                        FROM nexus_audits
                        WHERE trade_date = ? AND COALESCE(status, '') = 'L4_DONE'
                        """,
                        [td],
                    ).fetchone()
                if prior and prior[0]:
                    news_as_of = prior[0].replace(tzinfo=datetime.now().astimezone().tzinfo).isoformat()
            except Exception as exc:
                logger.warning('[AUDIT] historical news_as_of lookup failed: %s', exc)
            if not news_as_of:
                news_as_of = f'{td}T21:00:00+08:00'
        logger.info('[AUDIT] fixed news_as_of=%s', news_as_of)
        audit_child_env = {
            'AUDIT_CYCLE_BUDGET_SEC': dynamic_seconds,
            'AUDIT_L2_TOP_N_CLOSE': min(l2_cap, 15),
            'AUDIT_RUN_ID': audit_run_id,
            'AUDIT_NEWS_AS_OF': news_as_of,
        }
        contract_env, _contract_artifact_path = _prepare_audit_contract_binding(
            td,
            audit_run_id,
            news_as_of,
            audit_child_env,
        )
        audit_child_env.update(contract_env)
        audit_ready = False
        run_stats = {}
        _AUDIT_ACTIVE.set()
        _set_pipeline_state(
            td,
            'phase_audit',
            'IN_PROGRESS',
            f'run_id={audit_run_id}|target={target_count}|raw={raw_count}|'
            f'budget={dynamic_seconds}|timeout_at={timeout_at}',
        )

        ok = safe_run(
            [PYTHON, str(COMP['decision_engine']), '--mode', 'normal', '--date', td, '--no-resume'],
            label='L1->L4',
            timeout=safe_timeout,
            use_sem=True,
            extra_env=audit_child_env,
        )

        if ok:
            audit_ready, run_stats = _validate_audit_run_completion(
                td,
                audit_run_id,
                source_row_count=raw_count,
            )

        if ok and audit_ready:
            completion_mode = str(run_stats.get('completion_mode') or 'rows')
            _set_pipeline_state(
                td,
                'phase_audit',
                'DONE',
                f'run_id={audit_run_id}|budget={dynamic_seconds}|target={target_count}|'
                f'rows={run_stats.get("total_rows", 0)}|outcome={completion_mode}|audit_completed',
            )
            audit_observer_scope = (td, audit_run_id)
            if completion_mode == 'zero_candidates':
                logger.info(
                    '[AUDIT] completed with zero candidates | run_id=%s source_rows=%s',
                    audit_run_id,
                    run_stats.get('source_row_count', 0),
                )
                funnel_text, rejection_text = _format_zero_candidate_funnel(run_stats)
                _push_wechat(
                    '烛龙审计完成 | 今日零入选',
                    chr(10).join([
                        f'交易日：{td}',
                        f'审计漏斗：{funnel_text}',
                        f'主要淘汰：{rejection_text}',
                        '结果：审计流程正常完成，筛选与形态门控后没有标的进入后续审计。',
                        '处理：今日不生成新的影子盘买入候选，系统继续保持观察。',
                        f'定位编号：{audit_run_id[:4]}（供排障定位）',
                        '',
                        '说明：这是正常的零入选结果，不是系统运行失败。',
                    ]),
                )
            else:
                logger.info('[AUDIT] nexus_audits write complete')
                try:
                    send_audit_report(td, audit_run_id)
                except Exception as e:
                    logger.error(f'[AUDIT] report push exception: {e}', exc_info=True)
                    _push_exception_once('Zhulong Audit Report Exception', e, context='phase_audit:send_audit_report')
        else:
            failure_reason = 'audit_failed' if not ok else 'run_incomplete'
            _set_pipeline_state(
                td,
                'phase_audit',
                'FAILED',
                f'run_id={audit_run_id}|budget={dynamic_seconds}|target={target_count}|'
                f'reason={failure_reason}|stats={run_stats}',
            )
            logger.error(
                '[AUDIT] failed closed | reason=%s run_id=%s stats=%s',
                failure_reason, audit_run_id, run_stats,
            )
            _push_wechat(
                '烛龙审计失败 | 已安全停下',
                chr(10).join([
                    f'交易日：{td}',
                    f'原因：{_audit_reason_text(failure_reason)}',
                    f'定位编号：{audit_run_id[:4]}（供排障定位）',
                    f'已落库记录：{run_stats.get("total_rows", 0) if isinstance(run_stats, dict) else 0}',
                    f'时间：{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
                    '',
                    '说明：失败时系统按 fail-closed 处理，不会放行模拟盘新买入。',
                ]),
            )

        gc.collect()
        logger.info('[AUDIT] gc.collect() done')

        # ---- Shadow Protocol: only a validated exact run with rows may enqueue signals ----
        if audit_ready and str(run_stats.get('completion_mode') or '') == 'rows':
            try:
                archetype_ok = safe_run(
                    [
                        PYTHON, str(COMP['trade_archetype_observer']),
                        '--trade-date', td, '--run-id', audit_run_id, '--apply',
                        '--output', str(LOG_DIR / 'trade_archetype_latest.json'),
                    ],
                    label='TradeArchetypeObserver', timeout=180, use_sem=False,
                )
                if not archetype_ok:
                    raise RuntimeError('trade archetype observer failed closed before Shadow dispatch')
                import sys as _sys
                _sys.path.insert(0, str(BASE_DIR / "05_shadow" / "lib"))
                from signal_receiver import fetch_pending_signals, process_signal
                _sigs = fetch_pending_signals(trade_date=td, run_id=audit_run_id)
                logger.info(f'[SHADOW] process pending signals: {len(_sigs)}')
                _ok_cnt = 0
                for _sig in _sigs:
                    try:
                        if process_signal(_sig):
                            _ok_cnt += 1
                    except Exception as _se:
                        logger.error(f'[SHADOW] single-signal error: {_sig.get("symbol", "?")} -> {_se}', exc_info=True)
                logger.info(f'[SHADOW] completed: {_ok_cnt}/{len(_sigs)}')
                try:
                    from action_plan import build_action_plan, render_push
                    _plan = build_action_plan(td, audit_run_id)
                    logger.info(
                        '[ACTION_PLAN] rendered | trade_date=%s run_id=%s items=%s json=%s markdown=%s',
                        td, audit_run_id, len(_plan.get('items') or []),
                        _plan.get('json_path', ''), _plan.get('markdown_path', ''),
                    )
                    if _plan.get('items'):
                        _plan_title, _plan_content = render_push(_plan)
                        _push_wechat(_plan_title, _plan_content)
                except Exception as _plan_err:
                    logger.error('[ACTION_PLAN] render/push failed: %s', _plan_err, exc_info=True)
            except Exception as _shadow_err:
                logger.error(f'[SHADOW] signal dispatch exception: {_shadow_err}', exc_info=True)
        elif audit_ready:
            logger.info(
                '[SHADOW] signal dispatch not required: audit completed with zero candidates | run_id=%s',
                audit_run_id,
            )
        else:
            logger.warning(
                '[SHADOW] signal dispatch skipped: audit run not validated | run_id=%s',
                audit_run_id,
            )

        # Existing paper-position review is observational and independent.
        try:
            import sys as _sys
            _sys.path.insert(0, str(BASE_DIR / "05_shadow" / "lib"))
            from engine import review_paper_positions
            _review_stats = review_paper_positions(td)
            logger.info(
                '[SHADOW] paper review done | '
                f'reviews={_review_stats.get("reviews_upserted", 0)} '
                '| sell_execution=intraday_only'
            )
        except Exception as _paper_err:
            logger.error(f'[SHADOW] paper review exception: {_paper_err}', exc_info=True)
    finally:
        try:
            snapshot_ready = _refresh_api_readonly_snapshot('AUDIT')
            if audit_observer_scope:
                _run_audit_funnel_observer(
                    audit_observer_scope[0],
                    audit_observer_scope[1],
                    snapshot_ready,
                )
        except Exception as observer_exc:
            logger.error('[AUDIT-FUNNEL] post-audit sidecar exception: %s', observer_exc, exc_info=True)
        finally:
            _AUDIT_ACTIVE.clear()


def _pipeline_phase_in_progress(trade_date_str: str, phases: tuple[str, ...]) -> bool:
    try:
        with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
            placeholders = ','.join(['?'] * len(phases))
            rows = conn.execute(
                f"""
                SELECT phase, status
                FROM ops_pipeline_state
                WHERE trade_date = ?
                  AND phase IN ({placeholders})
                """,
                [trade_date_str, *phases],
            ).fetchall()
        return any(str(row[1] or '').upper() == 'IN_PROGRESS' for row in rows)
    except Exception as exc:
        logger.warning(
            '[PIPELINE-GATE] phase state probe failed; fail-closed for trade_date=%s phases=%s: %s',
            trade_date_str,
            ','.join(phases),
            exc,
        )
        return True


@monitor_phase('phase_shadow_t1_fill')
def phase_shadow_t1_fill(mode: str = 'main'):
    """T+1 paper fill: L4 PASS opens the gate, this phase decides fill/unfilled."""
    td = _resolve_trade_date_override() or datetime.now().strftime('%Y-%m-%d')
    mode_norm = str(mode or 'main').lower()
    if not _calendar_allows(
        datetime.strptime(td, '%Y-%m-%d').date(),
        scope=CALENDAR_SCOPE_ENTRY,
        notify=True,
        context=f'phase_shadow_t1_fill:{mode_norm}',
    ):
        logger.info('[SHADOW_T1] skipped non-trading day: %s', td)
        return
    if _pipeline_phase_in_progress(td, ('phase_harvest', 'phase_audit')):
        logger.warning('[SHADOW_T1] skipped because harvest/audit is in progress: %s mode=%s', td, mode_norm)
        return

    try:
        import sys as _sys
        _sys.path.insert(0, str(BASE_DIR / '05_shadow' / 'lib'))
        from t1_fill_engine import run_t1_fill_cycle, run_t1_pending_watchdog

        stats = run_t1_fill_cycle(td, mode=mode_norm, limit=20)
        logger.info('[SHADOW_T1] mode=%s stats=%s', mode_norm, stats)
        if mode_norm == 'final':
            watchdog_stats = run_t1_pending_watchdog(td, push=True)
            logger.info('[SHADOW_T1] watchdog stats=%s', watchdog_stats)
    except Exception as exc:
        logger.error('[SHADOW_T1] exception: %s', exc, exc_info=True)
        _push_exception_once('Zhulong Shadow T1 Fill Exception', exc, context=f'phase_shadow_t1_fill:{mode_norm}')


@monitor_phase('phase_l1_path_observation')
def phase_l1_path_observation():
    """Freeze the daily read-only L1 comparison and rebuild its forward scorecard."""
    td = _resolve_trade_date_override() or _current_trade_date_str()
    if not _is_trading_day_for_current(notify=True, context='phase_l1_path_observation'):
        logger.info('[L1-PATH] non-trading day, skip')
        return
    if _pipeline_phase_in_progress(td, ('phase_harvest', 'phase_audit')):
        logger.warning('[L1-PATH] skipped because harvest/audit is in progress: %s', td)
        return

    observer_ok = safe_run(
        [PYTHON, str(COMP['l1_path_observer']), '--trade-date', td],
        label='L1PathObserver',
        timeout=1200,
        use_sem=False,
    )
    if not observer_ok:
        logger.error('[L1-PATH] daily team freeze failed: %s', td)
        return

    scorecard_ok = safe_run(
        [
            PYTHON,
            str(COMP['l1_scorecard']),
            '--input-dir',
            str(BASE_DIR / 'storage' / 'reports' / 'l1_path_observer'),
            '--forward-start',
            '2026-07-13',
        ],
        label='L1SimpleScorecard',
        timeout=1200,
        use_sem=False,
    )
    if not scorecard_ok:
        logger.error('[L1-PATH] cumulative scorecard rebuild failed: %s', td)
        return
    logger.info('[L1-PATH] daily freeze and cumulative scorecard complete: %s', td)


# ================================================================
# Phase 4: Memory Synthesis 01:00 (RAG Refresh)
# ================================================================


@monitor_phase('phase_synthesis')
def phase_synthesis():
    """RAG \u8bb0\u5fc6\u5408\u6210 (\u4e0e\u5ba1\u8ba1\u5f7b\u5e95\u9694\u5f00, \u786e\u4fdd\u63a8\u7406\u5185\u5b58\u5df2\u56de\u6536)"""
    now = datetime.now()
    if not is_trading_day() and now.weekday() != 5:
        logger.info('\U0001f9e0 [\u5408\u6210] \u975e\u4ea4\u6613\u65e5, \u8df3\u8fc7'); return

    phase_started = datetime.now()
    logger.info('=' * 50)
    logger.info('\U0001f9e0 [\u8bb0\u5fc6\u5408\u6210] Phase 4 @ 01:00 \u542f\u52a8')
    logger.info('=' * 50)

    rag_python = _resolve_rag_python()
    logger.info(f'[RAG] using python: {rag_python}')
    stats_suffix = phase_started.strftime('%Y%m%d_%H%M%S')
    refresh_stats_path = LOG_DIR / f'rag_refresh_stats_{stats_suffix}.json'
    backfill_stats_path = LOG_DIR / f'rag_backfill_stats_{stats_suffix}.json'

    refresh_script = (
        'import sys, json; '
        'from pathlib import Path; '
        'sys.path.insert(0,"/root/quant_project"); '
        'sys.path.insert(0,"/root/quant_project/01_engine/lib"); '
        'from rag_refresher import get_refresher; '
        'r = get_refresher(); stats = r.refresh(); '
        f'Path({str(refresh_stats_path)!r}).write_text(json.dumps(stats, ensure_ascii=False, default=str), encoding="utf-8"); '
        'print(f"RAG refresh done stats={stats}")'
    )
    refresh_t0 = time.time()
    refresh_ok = safe_run(
        [rag_python, '-c', refresh_script],
        label='RAG-Refresh', timeout=19000, use_sem=True)
    refresh_seconds = time.time() - refresh_t0
    refresh_stats = _read_json_dict(refresh_stats_path)

    backfill_ok = None
    backfill_seconds = 0.0
    backfill_stats = {}
    backfill_enabled = str(os.getenv('RAG_BACKFILL_ENABLED', '1')).strip().lower() in {'1', 'true', 'yes', 'on'}
    if refresh_ok and backfill_enabled:
        backfill_timeout = max(600, int(os.getenv('RAG_BACKFILL_PROCESS_TIMEOUT_SEC', '17000') or 17000))
        backfill_script = (
            'import sys, json; '
            'from pathlib import Path; '
            'sys.path.insert(0,"/root/quant_project"); '
            'sys.path.insert(0,"/root/quant_project/01_engine/lib"); '
            'from rag_refresher import get_refresher; '
            'r = get_refresher(); '
            'stats = r.backfill_raw_sync_enrichment(mode="nightly"); '
            f'Path({str(backfill_stats_path)!r}).write_text(json.dumps(stats, ensure_ascii=False, default=str), encoding="utf-8"); '
            'print(f"RAG backfill done stats={stats}")'
        )
        backfill_t0 = time.time()
        backfill_ok = safe_run(
            [rag_python, '-c', backfill_script],
            label='RAG-Backfill', timeout=backfill_timeout, use_sem=True)
        backfill_seconds = time.time() - backfill_t0
        backfill_stats = _read_json_dict(backfill_stats_path) or _latest_rag_backfill_run(mode='nightly')
    elif not backfill_enabled:
        logger.info('[RAG-Backfill] disabled by RAG_BACKFILL_ENABLED=0')

    tag_perf_ok = None
    tag_perf_seconds = 0.0
    tag_perf_enabled = str(os.getenv('RAG_TAG_PERF_ENABLED', '1')).strip().lower() in {'1', 'true', 'yes', 'on'}
    if tag_perf_enabled:
        tag_perf_timeout = max(120, int(os.getenv('RAG_TAG_PERF_TIMEOUT_SEC', '600') or 600))
        tag_perf_t0 = time.time()
        tag_perf_ok = safe_run(
            [PYTHON, '-c',
             'import sys; '
             'sys.path.insert(0,"/root/quant_project"); '
             'sys.path.insert(0,"/root/quant_project/04_governance/lib"); '
             'from weight_engine import calc_tag_success_rate; '
             'stats = calc_tag_success_rate(); '
             'print(f"RAG tag performance done stats={stats}")'],
            label='RAG-TagPerf', timeout=tag_perf_timeout, use_sem=True)
        tag_perf_seconds = time.time() - tag_perf_t0
    else:
        logger.info('[RAG-TagPerf] disabled by RAG_TAG_PERF_ENABLED=0')

    _push_rag_synthesis_summary(
        phase_started=phase_started,
        refresh_ok=refresh_ok,
        refresh_seconds=refresh_seconds,
        refresh_stats=refresh_stats,
        backfill_enabled=backfill_enabled,
        backfill_ok=backfill_ok,
        backfill_seconds=backfill_seconds,
        backfill_stats=backfill_stats,
        tag_perf_enabled=tag_perf_enabled,
        tag_perf_ok=tag_perf_ok,
        tag_perf_seconds=tag_perf_seconds,
    )
    _push_rag_health_if_needed()
    _refresh_api_readonly_snapshot('SYNTHESIS')
    gc.collect()
    logger.info('\U0001f9e0 Phase 4 \u5b8c\u6210 | gc.collect() \u2705 \u5185\u5b58\u5df2\u91ca\u653e')


def _rag_health_snapshot(days: int = 7, stale_days: int = 3) -> dict:
    recent_start = (datetime.now() - timedelta(days=max(1, int(days)))).strftime('%Y-%m-%d %H:%M:%S')
    stale_before = (datetime.now() - timedelta(days=max(1, int(stale_days)))).strftime('%Y-%m-%d %H:%M:%S')
    with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
        summary = conn.execute(
            """
            SELECT
                COUNT(*) AS total_recent,
                SUM(CASE WHEN UPPER(COALESCE(enrichment_status, 'RAW_SYNC')) = 'RAW_SYNC' THEN 1 ELSE 0 END) AS raw_sync,
                SUM(CASE WHEN UPPER(COALESCE(enrichment_status, '')) = 'TEMPLATE_READY' THEN 1 ELSE 0 END) AS template_ready,
                SUM(CASE WHEN UPPER(COALESCE(enrichment_status, '')) = 'ENRICHED' THEN 1 ELSE 0 END) AS enriched,
                SUM(CASE WHEN UPPER(COALESCE(enrichment_status, '')) = 'ENRICH_FAILED' THEN 1 ELSE 0 END) AS enrich_failed,
                SUM(CASE WHEN UPPER(COALESCE(enrichment_status, '')) = 'ENRICH_DEFERRED' THEN 1 ELSE 0 END) AS enrich_deferred
            FROM fact_strategic_memory
            WHERE created_at >= CAST(? AS TIMESTAMP)
            """,
            [recent_start],
        ).fetchone()
        stale_rows = conn.execute(
            """
            SELECT symbol,
                   CAST(trade_date AS VARCHAR) AS trade_date,
                   source,
                   COALESCE(enrichment_attempts, 0) AS attempts,
                   COALESCE(enrichment_error, '') AS error_text,
                   CAST(created_at AS VARCHAR) AS created_at
            FROM fact_strategic_memory
            WHERE UPPER(COALESCE(enrichment_status, 'RAW_SYNC')) = 'RAW_SYNC'
              AND created_at >= CAST(? AS TIMESTAMP)
              AND created_at < CAST(? AS TIMESTAMP)
            ORDER BY created_at ASC
            LIMIT 10
            """,
            [recent_start, stale_before],
        ).fetchall()
    total_recent = int(summary[0] or 0) if summary else 0
    raw_sync = int(summary[1] or 0) if summary else 0
    return {
        'total_recent': total_recent,
        'raw_sync': raw_sync,
        'template_ready': int(summary[2] or 0) if summary else 0,
        'enriched': int(summary[3] or 0) if summary else 0,
        'enrich_failed': int(summary[4] or 0) if summary else 0,
        'enrich_deferred': int(summary[5] or 0) if summary else 0,
        'raw_ratio': (raw_sync / total_recent) if total_recent else 0.0,
        'stale_raw': len(stale_rows),
        'stale_samples': [
            {
                'symbol': str(r[0] or ''),
                'trade_date': str(r[1] or ''),
                'source': str(r[2] or ''),
                'attempts': int(r[3] or 0),
                'error': str(r[4] or ''),
                'created_at': str(r[5] or ''),
            }
            for r in stale_rows
        ],
    }


def _latest_rag_backfill_run(mode: str = '') -> dict:
    try:
        with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
            row = conn.execute(
                """
                SELECT run_id, mode,
                       CAST(started_at AS VARCHAR), CAST(ended_at AS VARCHAR),
                       raw_before, raw_after, deferred_before, deferred_after,
                       daily_new_avg, attempted, enriched, failed, deferred,
                       skipped, embedded, circuit_breaker_reason, trend_status, notes
                FROM ops_rag_enrichment_runs
                WHERE (? = '' OR mode = ?)
                ORDER BY started_at DESC
                LIMIT 1
                """,
                [mode or '', mode or ''],
            ).fetchone()
        if not row:
            return {}
        return {
            'run_id': str(row[0] or ''),
            'mode': str(row[1] or ''),
            'started_at': str(row[2] or ''),
            'ended_at': str(row[3] or ''),
            'raw_before': int(row[4] or 0),
            'raw_after': int(row[5] or 0),
            'deferred_before': int(row[6] or 0),
            'deferred_after': int(row[7] or 0),
            'daily_new_avg': float(row[8] or 0),
            'attempted': int(row[9] or 0),
            'enriched': int(row[10] or 0),
            'failed': int(row[11] or 0),
            'deferred': int(row[12] or 0),
            'skipped': int(row[13] or 0),
            'embedded': int(row[14] or 0),
            'circuit_breaker_reason': str(row[15] or ''),
            'trend_status': str(row[16] or ''),
            'notes': str(row[17] or ''),
        }
    except Exception as exc:
        logger.warning('[RAG-BACKFILL] latest run read failed: %s', exc, exc_info=True)
        return {}




def _read_json_dict(path: Path) -> dict:
    try:
        if not path or not Path(path).exists():
            return {}
        data = json.loads(Path(path).read_text(encoding='utf-8'))
        return data if isinstance(data, dict) else {}
    except Exception as exc:
        logger.warning('[RAG] stats file read failed path=%s error=%s', path, exc, exc_info=True)
        return {}


def _fmt_duration(seconds: float) -> str:
    try:
        value = float(seconds or 0.0)
    except Exception:
        value = 0.0
    if value <= 0:
        return '\u672a\u8bb0\u5f55'
    if value < 1.0:
        return '<1\u79d2'
    return f'{value:.0f}\u79d2'


def _fmt_bool_status(ok) -> str:
    if ok is True:
        return '\u6210\u529f'
    if ok is False:
        return '\u5931\u8d25'
    return '\u672a\u8fd0\u884c'


def _push_rag_synthesis_summary(
    *,
    phase_started: datetime,
    refresh_ok: bool,
    refresh_seconds: float,
    refresh_stats: dict,
    backfill_enabled: bool,
    backfill_ok,
    backfill_seconds: float,
    backfill_stats: dict,
    tag_perf_enabled: bool,
    tag_perf_ok,
    tag_perf_seconds: float,
) -> bool:
    enabled = str(os.getenv('RAG_SYNTHESIS_PUSH_ENABLED', '1')).strip().lower() in {'1', 'true', 'yes', 'on'}
    if not enabled:
        return False
    try:
        health = _rag_health_snapshot(days=7, stale_days=int(os.getenv('RAG_HEALTH_STALE_DAYS', '3') or 3))
    except Exception as exc:
        logger.warning('[RAG] synthesis summary health snapshot failed: %s', exc, exc_info=True)
        health = {}

    issue = refresh_ok is not True or backfill_ok is False or tag_perf_ok is False
    summary_status = '\u9700\u5173\u6ce8' if issue else '\u6b63\u5e38'
    title = '\u70db\u9f99 RAG \u5408\u6210\u7ed3\u679c | ' + summary_status
    tag_status = _fmt_bool_status(tag_perf_ok) if tag_perf_enabled else '\u672a\u5f00\u542f'
    tag_duration = _fmt_duration(tag_perf_seconds) if tag_perf_enabled else '\u672a\u8fd0\u884c'
    refresh_stats = refresh_stats or {}
    backfill_stats = backfill_stats or {}
    lines = [
        '[\u70db\u9f99 RAG \u5408\u6210\u7ed3\u679c]',
        f"\u603b\u4f53\u72b6\u6001\uff1a{summary_status}",
        f"\u5f00\u59cb\u65f6\u95f4\uff1a{phase_started.strftime('%Y-%m-%d %H:%M:%S')}",
        '',
        f"\u8bb0\u5fc6\u5237\u65b0\uff1a{_fmt_bool_status(refresh_ok)}\uff0c\u8017\u65f6\uff1a{_fmt_duration(refresh_seconds)}",
        f"\u63d0\u53d6\u8bb0\u5f55\uff1a{int(refresh_stats.get('extracted') or 0)} \u6761",
        f"\u4e8b\u5b9e\u5165\u5e93\uff1a{int(refresh_stats.get('raw_synced') or 0)} \u6761",
        f"\u6a21\u677f\u8bb0\u5fc6\uff1a{int(refresh_stats.get('template_ready') or 0)} \u6761",
        f"\u5411\u91cf\u5199\u5165\uff1a{int(refresh_stats.get('embedded') or 0)} \u6761",
        f"\u5f85\u8865\u5411\u91cf\u56de\u586b\uff1a{int(refresh_stats.get('embedded_backfill') or 0)} \u6761",
        f"LLM\u589e\u5f3a\uff1a{int(refresh_stats.get('enriched') or 0)} / {int(refresh_stats.get('enrich_attempted') or 0)} \u6761",
        f"\u589e\u5f3a\u5931\u8d25\uff1a{int(refresh_stats.get('enrich_failed') or 0)} \u6761",
        '',
    ]
    if backfill_enabled:
        attempted = int(backfill_stats.get('attempted') or 0)
        lines.extend([
            f"\u591c\u95f4\u8865\u5f3a\uff1a{_fmt_bool_status(backfill_ok)}\uff0c\u8017\u65f6\uff1a{_fmt_duration(backfill_seconds)}",
            f"\u672c\u8f6e\u5c1d\u8bd5\uff1a{attempted} \u6761",
            f"\u8865\u5f3a\u5b8c\u6210\uff1a{int(backfill_stats.get('enriched') or 0)} \u6761",
            f"\u8865\u5f3a\u5931\u8d25\uff1a{int(backfill_stats.get('failed') or 0)} \u6761",
            f"\u4f4e\u9891\u6c60\uff1a{int(backfill_stats.get('deferred_before') or 0)} -> {int(backfill_stats.get('deferred_after') or 0)} \u6761",
            f"\u4e8b\u5b9e\u5c42\u5f85\u589e\u5f3a\uff1a{int(backfill_stats.get('raw_before') or 0)} -> {int(backfill_stats.get('raw_after') or 0)} \u6761",
        ])
        reason = str(backfill_stats.get('circuit_breaker_reason') or '').strip()
        if reason:
            if reason == 'no_candidates':
                reason_text = '\u6ca1\u6709\u5f85\u8865\u5f3a\u5019\u9009\uff0c\u5feb\u901f\u7ed3\u675f\u662f\u6b63\u5e38\u7ed3\u679c'
            else:
                reason_text = reason
            lines.append(f"\u505c\u6b62\u539f\u56e0\uff1a{reason_text}")
    else:
        lines.append('\u591c\u95f4\u8865\u5f3a\uff1a\u672a\u5f00\u542f')
    lines.extend([
        '',
        f"\u6807\u7b7e\u80dc\u7387\u66f4\u65b0\uff1a{tag_status}\uff0c\u8017\u65f6\uff1a{tag_duration}",
        '',
        f"\u6700\u8fd17\u65e5\u8bb0\u5fc6\uff1a{int(health.get('total_recent') or 0)} \u6761",
        f"\u6a21\u677f\u53ef\u68c0\u7d22\uff1a{int(health.get('template_ready') or 0)} \u6761",
        f"LLM\u589e\u5f3a\u5b8c\u6210\uff1a{int(health.get('enriched') or 0)} \u6761",
        f"\u4ec5\u4e8b\u5b9e\u5c42\uff1a{int(health.get('raw_sync') or 0)} \u6761",
        f"\u8fc7\u671f\u672a\u589e\u5f3a\uff1a{int(health.get('stale_raw') or 0)} \u6761",
        '',
        '\u8bf4\u660e\uff1aRAG \u662f\u5426\u5de5\u4f5c\u4ee5\u201c\u8bb0\u5fc6\u5237\u65b0\u201d\u7684\u5165\u5e93\u3001\u6a21\u677f\u548c\u5411\u91cf\u7ed3\u679c\u4e3a\u51c6\uff1b\u591c\u95f4\u8865\u5f3a\u662f\u7a7a\u95f2\u7b97\u529b\u4efb\u52a1\uff0c\u6ca1\u6709\u5019\u9009\u65f6\u4f1a\u5f88\u5feb\u7ed3\u675f\u3002',
    ])
    return _push_wechat(title, '\n'.join(lines))

def _push_rag_backfill_summary(mode: str = 'nightly') -> bool:
    enabled = str(os.getenv('RAG_BACKFILL_PUSH_ENABLED', '1')).strip().lower() in {'1', 'true', 'yes', 'on'}
    if not enabled:
        return False
    run = _latest_rag_backfill_run(mode=mode)
    if not run:
        return False
    trend_map = {
        'HEALTHY_CONVERGING': '\u5065\u5eb7\u6536\u655b',
        'HEALTHY_LOW_STABLE': '\u4f4e\u4f4d\u7a33\u5b9a',
        'CAPACITY_WARNING': '\u4ea7\u80fd\u544a\u6025',
        'WATCH_STABLE': '\u89c2\u5bdf\u6301\u5e73',
        'ALL_FAILED': '\u672c\u8f6e\u5168\u5931\u8d25',
        'ALL_UNRESOLVED': '\u672c\u8f6e\u672a\u89e3\u51b3',
        'HIGH_FAILURE_RATE': '\u5931\u8d25\u7387\u504f\u9ad8',
    }
    trend = trend_map.get(run.get('trend_status', ''), run.get('trend_status', '') or '\u672a\u77e5')
    title = '\u70db\u9f99 RAG \u591c\u95f4\u8865\u5f3a' if mode == 'nightly' else '\u70db\u9f99 RAG \u4f4e\u9891\u8865\u5f3a'
    attempted = max(0, int(run.get('attempted') or 0))
    unresolved = int(run.get('failed') or 0) + int(run.get('deferred') or 0)
    failure_rate = (unresolved / attempted) if attempted else 0.0
    lines = [
        f'[{title}]',
        f"模式：{'夜间补强' if mode == 'nightly' else '低频补强'}",
        f"\u5f00\u59cb\uff1a{run.get('started_at')}",
        f"\u7ed3\u675f\uff1a{run.get('ended_at')}",
        '',
        f"\u672c\u8f6e\u5c1d\u8bd5\uff1a{run.get('attempted')} \u6761",
        f"转为可检索：{run.get('enriched')} 条",
        f"失败保留事实层：{run.get('failed')} 条",
        f"\u8f6c\u5165\u4f4e\u9891\u6c60\uff1a{run.get('deferred')} \u6761",
        f"\u8df3\u8fc7\uff1a{run.get('skipped')} \u6761",
        f"\u5411\u91cf\u66f4\u65b0\uff1a{run.get('embedded')} \u6761",
        f"\u672a\u89e3\u51b3\u7387\uff1a{failure_rate * 100:.1f}%",
        '',
        f"事实层待增强：{run.get('raw_before')} -> {run.get('raw_after')}",
        f"低频补强池：{run.get('deferred_before')} -> {run.get('deferred_after')}",
        f"\u8fd15\u4e2a\u5165\u5e93\u65e5\u5747\u65b0\u589e\uff1a{run.get('daily_new_avg'):.1f} \u6761",
        f"\u8d8b\u52bf\u5224\u65ad\uff1a{trend}",
    ]
    if run.get('circuit_breaker_reason'):
        lines.append(f"\u7194\u65ad/\u505c\u6b62\u539f\u56e0\uff1a{run.get('circuit_breaker_reason')}")
    lines.extend([
        '',
        '\u8bf4\u660e\uff1a\u4e8b\u5b9e\u5c42\u4ecd\u7136\u5148\u884c\u53ef\u68c0\u7d22\uff1b\u672c\u4efb\u52a1\u53ea\u5229\u7528\u591c\u95f4\u7a7a\u95f2\u7b97\u529b\u8865\u5f3a\u8bed\u4e49\u5c42\u3002',
    ])
    return _push_wechat(title, '\n'.join(lines))


def _push_rag_health_if_needed() -> bool:
    try:
        stats = _rag_health_snapshot(days=7, stale_days=int(os.getenv('RAG_HEALTH_STALE_DAYS', '3') or 3))
        logger.info('[RAG-HEALTH] stats=%s', stats)
        issue = (
            stats.get('stale_raw', 0) > 0
            or stats.get('enrich_failed', 0) > 0
            or stats.get('enrich_deferred', 0) > 0
            or (stats.get('total_recent', 0) >= 5 and stats.get('raw_ratio', 0.0) >= 0.60)
        )
        if not issue:
            return False
        lines = [
            '[烛龙 RAG 健康提醒]',
            f"最近7日记忆：{stats.get('total_recent', 0)} 条",
            f"仅事实层可检索：{stats.get('raw_sync', 0)} 条 ({stats.get('raw_ratio', 0.0) * 100:.1f}%)",
            f"模板记忆可检索：{stats.get('template_ready', 0)} 条",
            f"LLM增强完成：{stats.get('enriched', 0)} 条",
            f"LLM增强失败：{stats.get('enrich_failed', 0)} 条",
            f"进入低频补强池：{stats.get('enrich_deferred', 0)} 条",
            f"超过阈值仍未增强：{stats.get('stale_raw', 0)} 条",
            '',
        ]
        for i, row in enumerate(stats.get('stale_samples', [])[:5], 1):
            lines.append(
                f"{i}. {row.get('symbol')} {row.get('trade_date')} "
                f"来源：{row.get('source')} 尝试：{row.get('attempts')}"
            )
            if row.get('error'):
                lines.append(f"   错误摘要：{str(row.get('error'))[:80]}")
        lines.extend([
            '',
            '说明：事实层仍可供检索；该提醒表示语义增强链路存在积压或失败，需优先看错误类型，再判断是链路、模型还是算力。',
        ])
        return _push_wechat('烛龙 RAG 健康提醒', '\n'.join(lines))
    except Exception as exc:
        logger.warning('[RAG-HEALTH] check failed: %s', exc, exc_info=True)
        return False


# ╔══════════════════════════════════════════════════════════════╗
# ║          Phase 5: 进化引擎 06:00 (L5 Evolver)                ║
# ╚══════════════════════════════════════════════════════════════╝


@monitor_phase('phase_rag_deferred_backfill')
def phase_rag_deferred_backfill():
    """Weekly low-frequency retry for ENRICH_DEFERRED records."""
    if str(os.getenv('RAG_BACKFILL_ENABLED', '1')).strip().lower() not in {'1', 'true', 'yes', 'on'}:
        logger.info('[RAG-DEFERRED] disabled')
        return
    rag_python = _resolve_rag_python()
    timeout_s = max(1800, int(os.getenv('RAG_DEFERRED_BACKFILL_PROCESS_TIMEOUT_SEC', '7200') or 7200))
    safe_run(
        [rag_python, '-c',
         'import sys; '
         'sys.path.insert(0,"/root/quant_project"); '
         'sys.path.insert(0,"/root/quant_project/01_engine/lib"); '
         'from rag_refresher import get_refresher; '
         'r = get_refresher(); '
         'stats = r.backfill_raw_sync_enrichment(mode="weekly_deferred", include_deferred=True, deferred_only=True); '
         'print(f"RAG deferred backfill done stats={stats}")'],
        label='RAG-Deferred-Backfill', timeout=timeout_s, use_sem=True)
    _push_rag_backfill_summary(mode='weekly_deferred')


@monitor_phase('phase_news_observation')
def phase_news_observation():
    """Drain persisted OBSERVE_ONLY news work without entering the verdict path."""
    command = [
        PYTHON,
        str(COMP['news_observer']),
        '--limit',
        '30',
        '--scheduled-window',
        'auto',
    ]
    if _AUDIT_ACTIVE.is_set():
        logger.info('[L4-NEWS] observation worker skipped while audit is active')
        command.extend(['--skip-reason', 'AUDIT_ACTIVE'])
    _NEWS_OBSERVATION_ACTIVE.set()
    try:
        # The observer is a separate process and needs DuckDB's process-level
        # writer lock. Serialize its launch against daemon heartbeat writes.
        with _HEARTBEAT_PERSIST_LOCK:
            ok = safe_run(
                command,
                label='L4NewsObservation',
                timeout=900,
                use_sem=False,
            )
    finally:
        _NEWS_OBSERVATION_ACTIVE.clear()
    if not ok:
        logger.error('[L4-NEWS] observation worker failed')


@monitor_phase('phase_shadow_position_context')
def phase_shadow_position_context():
    """Build EOD position context used by next-day Shadow runner gates."""
    ok = safe_run(
        [
            PYTHON,
            str(COMP['shadow_context']),
            '--limit',
            '50',
        ],
        label='ShadowPositionContext',
        timeout=900,
        use_sem=False,
    )
    if not ok:
        logger.error('[ShadowContext] position context worker failed')


@monitor_phase('phase_news_observation_review')
def phase_news_observation_review():
    """Evaluate evidence sufficiency without changing the deployed news policy."""
    ok = safe_run(
        [
            PYTHON,
            str(COMP['news_review']),
            '--start-date',
            '2026-06-22',
            '--output',
            str(LOG_DIR / 'l4_news_observation_review_latest.md'),
        ],
        label='L4NewsObservationReview',
        timeout=300,
        use_sem=False,
    )
    if not ok:
        logger.error('[L4-NEWS-REVIEW] graduation review failed')


@monitor_phase('phase_evolution')
def phase_evolution():
    """L5 进化引擎 (独立进程, 与 RAG 隔开)"""
    if not is_trading_day():
        logger.info('🧬 [进化] 非交易日, 跳过'); return

    logger.info('=' * 50)
    logger.info('🧬 [进化引擎] Phase 5 @ 06:00 启动')
    logger.info('=' * 50)

    safe_run(
        [PYTHON, str(COMP['evolver']), '--run'],
        label='L5-Evolution', timeout=3600)

    gc.collect()
    logger.info('🧬 Phase 5 完成 | gc.collect() ✅ 内存已释放')


# ╔══════════════════════════════════════════════════════════════╗
# ║          Backup: DuckDB 每日备份 07:00 (保留7天)              ║
# ╚══════════════════════════════════════════════════════════════╝


@monitor_phase('phase_backup')
def phase_backup():
    """Hard pre-batch cold backup before nightly harvest."""
    logger.info('=' * 50)
    logger.info('[PRE-HARVEST BACKUP] phase_backup @ 19:45 start')
    logger.info('=' * 50)

    backup_dir = BASE_DIR / 'storage' / 'backups'
    backup_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    archive_path = backup_dir / f'backup_{ts}.tar.gz'

    snapshot_ok = _refresh_api_readonly_snapshot('BACKUP')
    if not snapshot_ok or not API_SNAPSHOT_DB_PATH.exists():
        raise RuntimeError('backup blocked: consistent DuckDB snapshot unavailable')
    db_backup_src = API_SNAPSHOT_DB_PATH
    logger.info('  [backup] using checkpointed API snapshot for database archive')

    snapshot_files = [
        (db_backup_src, 'storage/database/zhulong.duckdb'),
        (BASE_DIR / '05_shadow' / 'config' / 'rules.yaml', '05_shadow/config/rules.yaml'),
    ]
    logger.info('  [backup] sensitive runtime config excluded: .env')

    added_files = 0
    try:
        with tarfile.open(archive_path, 'w:gz') as tar:
            for src, arcname in snapshot_files:
                if src.exists():
                    tar.add(str(src), arcname=arcname)
                    added_files += 1
                    logger.info(f'  [backup] add {arcname}')
                else:
                    logger.warning(f'  [backup] missing source, skip: {src}')

        if added_files == 0:
            logger.warning('[backup] no source file added, removing empty archive')
            archive_path.unlink(missing_ok=True)
        else:
            size_mb = archive_path.stat().st_size / 1024 / 1024
            logger.info(f'[backup] archive ready: {archive_path.name} ({size_mb:.2f}MB)')
    except Exception as exc:
        logger.error(f'[backup] archive build failed: {exc}', exc_info=True)

    # Rolling retention: keep only backups from last 7 days.
    try:
        cutoff = datetime.now() - timedelta(days=7)
        for old in sorted(backup_dir.glob('backup_*.tar.gz')):
            mtime = datetime.fromtimestamp(old.stat().st_mtime)
            if mtime < cutoff:
                old.unlink(missing_ok=True)
                logger.info(f'  [backup] removed expired archive: {old.name}')
    except Exception as exc:
        logger.error(f'[backup] retention cleanup failed: {exc}', exc_info=True)

    gc.collect()
    logger.info('[backup] phase_backup done | gc.collect() done')


@monitor_phase('phase_dawn_check')
def phase_dawn_check():
    """晨曦预检 — 系统健康确认推送"""
    if not _calendar_allows(notify=True, context='phase_dawn_check'):
        logger.info('🌅 [晨检] 非交易日, 跳过'); return

    logger.info('=' * 50)
    logger.info('🌅 [晨曦预检] Dawn @ 09:10 启动')
    logger.info('=' * 50)

    checks = []

    # 1. DB 文件存在
    db_ok = DB_PATH.exists()
    db_size = DB_PATH.stat().st_size / 1024 / 1024 if db_ok else 0
    checks.append(f"{'✅' if db_ok else '❌'} DuckDB: {db_size:.0f}MB")

    # 2. Node-102 连通性
    n102_ok = False
    try:
        resp = requests.get(f'{_current_ollama_host()}/api/tags', timeout=5)
        n102_ok = resp.status_code == 200
    except Exception:
        logger.error('[Dawn] Node-102 connectivity probe failed', exc_info=True)
    checks.append(f"{'✅' if n102_ok else '❌'} 大模型节点连通")

    # 3. 备份状态
    backup_dir = BASE_DIR / 'storage' / 'backups'
    bak_count = len(list(backup_dir.glob('backup_*.tar.gz'))) if backup_dir.exists() else 0
    checks.append(f"✅ 备份: {bak_count} 份")

    # 4. Echo 状态
    checks.append(f"{'✅' if _echo_enabled() else 'ℹ️'} 回声系统：{'已启用' if _echo_enabled() else '已休眠'}")

    # 5. 推送
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M')
    status = '🟢 全系统就绪' if (db_ok and n102_ok) else '🔴 存在异常'
    body = f'⏰ {now_str}\n\n{status}\n\n' + '\n'.join(checks)

    _push_wechat(f'🌅 烛龙晨检 | {status}', body)
    logger.info(f'🌅 晨曦预检完成 | {status}')


# ╔══════════════════════════════════════════════════════════════╗

def heartbeat():
    sv = getattr(OLLAMA_SEM, '_value', '?')
    mem_tag = 'NA'
    try:
        import psutil
        mem = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
        with _HEARTBEAT_PERSIST_LOCK:
            pause_reason = _heartbeat_db_pause_reason()
            if pause_reason:
                logger.info(f'[Heartbeat] {pause_reason} active, skip DB heartbeat persistence')
            else:
                _record_daemon_heartbeat(mem)
        mem_tag = f"{mem:.0f}MB"
    except Exception as e:
        logger.warning(f'[Heartbeat] memory/persist tick degraded: {e}')
    logger.info(f'?? ?? | trading={is_trading_day()} | sem={sv}/3 | PID={os.getpid()} | rss={mem_tag}')


def _record_daemon_heartbeat(mem_mb: float) -> None:
    try:
        with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ops_daemon_heartbeat (
                    ts TIMESTAMP,
                    pid BIGINT,
                    rss_mb DOUBLE,
                    node_id TEXT,
                    status TEXT
                )
                """
            )
            conn.execute(
                "INSERT INTO ops_daemon_heartbeat (ts, pid, rss_mb, node_id, status) VALUES (now(), ?, ?, ?, ?)",
                [os.getpid(), float(mem_mb), str(Config.get_node_config().get('node_id', 'unknown')), 'ALIVE'],
            )
    except Exception:
        logger.error('[Heartbeat] failed to persist daemon heartbeat', exc_info=True)


def _heartbeat_report():
    import psutil
    try:
        proc = psutil.Process(os.getpid())
        mem = proc.memory_info().rss / 1024 / 1024
        with _HEARTBEAT_PERSIST_LOCK:
            pause_reason = _heartbeat_db_pause_reason()
            if pause_reason:
                logger.info(f'[HeartbeatReport] {pause_reason} active, skip DB heartbeat persistence')
            else:
                _record_daemon_heartbeat(mem)
        msg = "\n".join([
            f"进程号：{os.getpid()}",
            f"内存占用：{mem:.0f}MB",
            f"时间：{datetime.now():%Y-%m-%d %H:%M:%S}",
            "",
            "说明：这是 daemon 存活心跳，表示主进程仍在运行。",
        ])
        _push_wechat("💓 烛龙生存报告", msg)
    except Exception as e:
        logger.error(f"心跳报告异常: {e}", exc_info=True)


def _heartbeat_db_pause_reason() -> str:
    if _AUDIT_ACTIVE.is_set():
        return 'audit'
    if _NEWS_OBSERVATION_ACTIVE.is_set():
        return 'news_observation'
    if _HEARTBEAT_DB_PAUSED.is_set():
        return 'harvest'
    return ''


def main():
    load_env()
    if not bootstrap():
        logger.error('❌ Bootstrap 失败'); sys.exit(1)

    forced_td = _resolve_trade_date_override()
    if forced_td:
        logger.warning('[ONE_SHOT] TRADE_DATE override active: %s', forced_td)
        phase_harvest()
        phase_audit()
        logger.info('[ONE_SHOT] completed for trade_date=%s', forced_td)
        return

    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger
        from apscheduler.triggers.interval import IntervalTrigger
    except ImportError:
        logger.error('❌ 请执行: pip install apscheduler', exc_info=True); sys.exit(1)

    s = BackgroundScheduler(timezone='Asia/Shanghai', job_defaults={
        'coalesce': True, 'max_instances': 1, 'misfire_grace_time': 300})

    s.add_job(_heartbeat_report, CronTrigger(hour='8,12,20', minute=0), id='hb_biz_report')
    s.add_job(heartbeat, IntervalTrigger(minutes=5), id='hb_sys_tick')

    # 注册盘中调度
    s.add_job(phase_intraday, CronTrigger(minute='*/10', hour='9-14', day_of_week='mon-fri'), id='intraday')
    s.add_job(phase_tactics_daily_flush, CronTrigger(hour=15, minute=5, day_of_week='mon-fri'), id='daily_flush')
    s.add_job(phase_intraday_protocol_review, CronTrigger(hour=20, minute=45, day_of_week='mon'), id='intraday_protocol_review', misfire_grace_time=1800)
    s.add_job(lambda: phase_shadow_t1_fill('probe'), CronTrigger(hour=17, minute=30, day_of_week='mon-fri'), id='shadow_t1_probe')
    s.add_job(lambda: phase_shadow_t1_fill('main'), CronTrigger(hour=20, minute=15, day_of_week='mon-fri'), id='shadow_t1_main', misfire_grace_time=1200)
    s.add_job(phase_harvest, CronTrigger(hour=20, minute=30, day_of_week='mon-fri'), id='harvest', misfire_grace_time=1800)
    s.add_job(phase_eagle_active_context, CronTrigger(hour=20, minute=50, day_of_week='mon-fri'), id='eagle_active_context', misfire_grace_time=900)
    s.add_job(phase_audit, CronTrigger(hour=21, minute=0, day_of_week='mon-fri'), id='audit', misfire_grace_time=1800)
    s.add_job(lambda: phase_shadow_t1_fill('final'), CronTrigger(hour=22, minute=20, day_of_week='mon-fri'), id='shadow_t1_final', misfire_grace_time=1800)
    s.add_job(phase_l1_path_observation, CronTrigger(hour=23, minute=12, day_of_week='mon-fri'), id='l1_path_observation', misfire_grace_time=1800)
    s.add_job(phase_anchor_observer, CronTrigger(hour=23, minute=45, day_of_week='mon-fri'), id='anchor_observer', misfire_grace_time=1800)
    s.add_job(phase_prune, CronTrigger(hour=4, minute=20), id='prune')
    s.add_job(phase_synthesis, CronTrigger(hour=1, minute=0, day_of_week='tue-sat'), id='synthesis')
    s.add_job(phase_rag_deferred_backfill, CronTrigger(hour=2, minute=0, day_of_week='sun'), id='rag_deferred_backfill', misfire_grace_time=1800)
    s.add_job(
        phase_news_observation,
        CronTrigger(hour='0,1,22,23', minute=35, day_of_week='mon-sat'),
        id='l4_news_observation',
        misfire_grace_time=1800,
    )
    s.add_job(
        phase_shadow_position_context,
        CronTrigger(hour=23, minute=50, day_of_week='mon-fri'),
        id='shadow_position_context',
        misfire_grace_time=1800,
    )
    news_graduation_review_enabled = _env_flag(
        'L4_NEWS_GRADUATION_REVIEW_ENABLED', '1'
    )
    if news_graduation_review_enabled:
        s.add_job(
            phase_news_observation_review,
            CronTrigger(hour=20, minute=30, day_of_week='sun'),
            id='l4_news_observation_review',
            misfire_grace_time=1800,
        )
    else:
        logger.info(
            '[L4-News] graduation review schedule disabled '
            '(L4_NEWS_GRADUATION_REVIEW_ENABLED=0)'
        )
    s.add_job(
        phase_trade_calendar_refresh,
        CronTrigger(hour=18, minute=30, day_of_week='sun'),
        id='trade_calendar_refresh',
        misfire_grace_time=1800,
    )
    l5_enabled = str(os.getenv('L5_EVOLUTION_ENABLED', '0')).strip().lower() in {'1', 'true', 'yes', 'on'}
    if l5_enabled:
        s.add_job(phase_evolution, CronTrigger(hour=6, minute=0, day_of_week='mon-fri'), id='evolution')
    else:
        logger.info('[L5-Evolution] schedule disabled (L5_EVOLUTION_ENABLED=0)')
    if _echo_enabled():
        logger.info('[Echo] optional subsystem enabled (ZHULONG_ECHO_ENABLED=1)')
    else:
        logger.info('[Echo] dormant (ZHULONG_ECHO_ENABLED=0); intraday scan/awaken disabled')
    s.add_job(phase_backup, CronTrigger(hour=19, minute=45, day_of_week='mon-fri'), id='backup')
    s.add_job(phase_flush, CronTrigger(hour=8, minute=0, day_of_week='mon-fri'), id='flush')
    s.add_job(phase_dawn_check, CronTrigger(hour=9, minute=10, day_of_week='mon-fri'), id='dawn_check')

    s.start()
    logger.info('🚀 烛龙 Daemon v1.1.2 启动 (PVE 控制台注入版)')

    try:
        while not _shutdown.wait(timeout=1.0):
            pass
    except KeyboardInterrupt:
        _shutdown.set()
    finally:
        try:
            s.shutdown(wait=True)
            logger.info('[Graceful Shutdown] 调度器已关闭')
        except Exception as e:
            logger.error(f'[Graceful Shutdown] 调度器关闭异常: {e}', exc_info=True)
        finally:
            _release_singleton_lock()
            logger.info('[Graceful Shutdown Complete]')

if __name__ == '__main__':
    main()
