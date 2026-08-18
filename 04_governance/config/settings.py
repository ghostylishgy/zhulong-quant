#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
04_governance/config/settings.py
Governance truth-source settings with lazy singleton loading.
"""

from __future__ import annotations

import logging
import os
import sys
import threading
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict

from dotenv import load_dotenv


logger = logging.getLogger('governance.settings')


_current = Path(__file__).resolve()
PROJECT_ROOT = next(
    (p for p in _current.parents if (p / '.git').exists() or (p / 'storage').exists()),
    _current.parents[2],
)


AUDIT_PASS = 'PASS'


def _resolve_governance_log_path() -> Path:
    default = PROJECT_ROOT / 'logs' / 'governance.log'
    path = Path(os.getenv('ZHULONG_GOVERNANCE_LOG_PATH', str(default))).expanduser()
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    return path


GOVERNANCE_LOG_FILE = _resolve_governance_log_path()

VALID_AUDIT_PROFILE_NAMES = ('conservative', 'balanced', 'aggressive')


@dataclass(frozen=True)
class AuditProfile:
    """Audit profile for runtime gating and latency control."""

    name: str
    l1_candidate_limit: int
    l2_top_n_intraday: int
    l2_top_n_close: int
    l2_pass_threshold: int
    l2_watch_threshold: int
    l4_pass_threshold: int
    l4_watch_threshold: int
    zeta_div_soft: float
    zeta_div_hard: float
    tactics_min_final_score_pass: int
    tactics_min_final_score_watch: int
    cycle_budget_sec: int
    next_stage_reserve_sec: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'l1_candidate_limit': self.l1_candidate_limit,
            'l2_top_n_intraday': self.l2_top_n_intraday,
            'l2_top_n_close': self.l2_top_n_close,
            'l2_pass_threshold': self.l2_pass_threshold,
            'l2_watch_threshold': self.l2_watch_threshold,
            'l4_pass_threshold': self.l4_pass_threshold,
            'l4_watch_threshold': self.l4_watch_threshold,
            'zeta_div_soft': self.zeta_div_soft,
            'zeta_div_hard': self.zeta_div_hard,
            'tactics_min_final_score_pass': self.tactics_min_final_score_pass,
            'tactics_min_final_score_watch': self.tactics_min_final_score_watch,
            'cycle_budget_sec': self.cycle_budget_sec,
            'next_stage_reserve_sec': self.next_stage_reserve_sec,
        }


AUDIT_PROFILE_PRESETS: Dict[str, AuditProfile] = {
    'conservative': AuditProfile(
        name='conservative',
        l1_candidate_limit=50,
        l2_top_n_intraday=6,
        l2_top_n_close=10,
        l2_pass_threshold=62,
        l2_watch_threshold=74,
        l4_pass_threshold=72,
        l4_watch_threshold=58,
        zeta_div_soft=3.2,
        zeta_div_hard=5.0,
        tactics_min_final_score_pass=68,
        tactics_min_final_score_watch=58,
        cycle_budget_sec=540,
        next_stage_reserve_sec=150,
    ),
    'balanced': AuditProfile(
        name='balanced',
        l1_candidate_limit=50,
        l2_top_n_intraday=8,
        l2_top_n_close=12,
        l2_pass_threshold=65,
        l2_watch_threshold=80,
        l4_pass_threshold=68,
        l4_watch_threshold=52,
        zeta_div_soft=2.8,
        zeta_div_hard=5.5,
        tactics_min_final_score_pass=65,
        tactics_min_final_score_watch=52,
        cycle_budget_sec=8400,
        next_stage_reserve_sec=300,
    ),
    'aggressive': AuditProfile(
        name='aggressive',
        l1_candidate_limit=50,
        l2_top_n_intraday=10,
        l2_top_n_close=15,
        l2_pass_threshold=68,
        l2_watch_threshold=85,
        l4_pass_threshold=64,
        l4_watch_threshold=46,
        zeta_div_soft=2.3,
        zeta_div_hard=6.2,
        tactics_min_final_score_pass=62,
        tactics_min_final_score_watch=46,
        cycle_budget_sec=660,
        next_stage_reserve_sec=90,
    ),
}


def normalize_audit_profile_name(name: str) -> str:
    raw = str(name or '').strip().lower()
    if raw in AUDIT_PROFILE_PRESETS:
        return raw
    return 'balanced'



def ensure_syspath() -> None:
    """Centralized sys.path management."""
    root_str = str(PROJECT_ROOT)
    if root_str not in sys.path:
        sys.path.append(root_str)


def _read_mem_gb() -> float:
    """Best-effort memory detection in GB."""
    try:
        with open('/proc/meminfo', 'r', encoding='utf-8') as f:
            for line in f:
                if line.startswith('MemTotal:'):
                    kb = int(line.split()[1])
                    return kb / (1024 * 1024)
    except Exception as e:
        logger.error(f'Critical Logical Gap: {str(e)}', exc_info=True)
    return 0.0


class _SettingsData:
    """Internal lazy-loaded settings store."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._loaded = False
        self._logger_ready = False
        self._values: Dict[str, Any] = {}

    def _env(self, key: str, default: Any = None) -> Any:
        return os.getenv(key, default)

    def _env_int(self, key: str, default: int) -> int:
        try:
            return int(self._env(key, str(default)))
        except Exception:
            return default

    def _env_float(self, key: str, default: float) -> float:
        try:
            return float(self._env(key, str(default)))
        except Exception:
            return default

    def _env_bool(self, key: str, default: bool) -> bool:
        raw = str(self._env(key, str(default))).strip().lower()
        return raw in {'1', 'true', 'yes', 'on'}


    def _build_audit_profile(self, profile_name: str = '') -> AuditProfile:
        selected = normalize_audit_profile_name(
            profile_name or self._env('AUDIT_PROFILE', self._env('ZHULONG_AUDIT_PROFILE', 'balanced'))
        )
        base = AUDIT_PROFILE_PRESETS.get(selected, AUDIT_PROFILE_PRESETS['balanced'])

        def _clamp(value: int, low: int, high: int) -> int:
            return max(low, min(high, int(value)))

        reserve = _clamp(
            self._env_int('AUDIT_NEXT_STAGE_RESERVE_SEC', base.next_stage_reserve_sec),
            30,
            3600,
        )
        budget_default = max(base.cycle_budget_sec, reserve + 30)
        budget = _clamp(
            self._env_int('AUDIT_CYCLE_BUDGET_SEC', budget_default),
            reserve + 30,
            21600,
        )

        return AuditProfile(
            name=selected,
            l1_candidate_limit=_clamp(
                self._env_int('AUDIT_L1_CANDIDATE_LIMIT', base.l1_candidate_limit),
                1,
                500,
            ),
            l2_top_n_intraday=_clamp(
                self._env_int('AUDIT_L2_TOP_N_INTRADAY', base.l2_top_n_intraday),
                1,
                15,
            ),
            l2_top_n_close=_clamp(
                self._env_int('AUDIT_L2_TOP_N_CLOSE', base.l2_top_n_close),
                1,
                15,
            ),
            l2_pass_threshold=_clamp(
                self._env_int('AUDIT_L2_PASS_THRESHOLD', base.l2_pass_threshold),
                0,
                100,
            ),
            l2_watch_threshold=_clamp(
                self._env_int('AUDIT_L2_WATCH_THRESHOLD', base.l2_watch_threshold),
                0,
                100,
            ),
            l4_pass_threshold=_clamp(
                self._env_int('AUDIT_L4_PASS_THRESHOLD', base.l4_pass_threshold),
                0,
                100,
            ),
            l4_watch_threshold=_clamp(
                self._env_int('AUDIT_L4_WATCH_THRESHOLD', base.l4_watch_threshold),
                0,
                100,
            ),
            zeta_div_soft=float(self._env_float('AUDIT_ZETA_DIV_SOFT', base.zeta_div_soft)),
            zeta_div_hard=float(self._env_float('AUDIT_ZETA_DIV_HARD', base.zeta_div_hard)),
            tactics_min_final_score_pass=_clamp(
                self._env_int('TACTICS_MIN_FINAL_SCORE_PASS', base.tactics_min_final_score_pass),
                0,
                100,
            ),
            tactics_min_final_score_watch=_clamp(
                self._env_int('TACTICS_MIN_FINAL_SCORE_WATCH', base.tactics_min_final_score_watch),
                0,
                100,
            ),
            cycle_budget_sec=budget,
            next_stage_reserve_sec=reserve,
        )

    def get_audit_profile(self) -> AuditProfile:
        self._load()
        return self._values['AUDIT_PROFILE_OBJECT']

    def get_audit_profiles(self) -> Dict[str, Dict[str, Any]]:
        return {name: profile.to_dict() for name, profile in AUDIT_PROFILE_PRESETS.items()}

    def _setup_governance_logger(self) -> None:
        if self._logger_ready:
            return

        GOVERNANCE_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        target = str(GOVERNANCE_LOG_FILE)

        for h in list(logger.handlers):
            if not getattr(h, '_zhulong_governance_managed', False):
                continue
            logger.removeHandler(h)
            try:
                h.close()
            except Exception:
                pass

        handler = RotatingFileHandler(
            filename=target,
            maxBytes=5 * 1024 * 1024,
            backupCount=3,
            encoding='utf-8',
        )
        handler._zhulong_governance_managed = True
        handler.setFormatter(
            logging.Formatter('%(asctime)s | %(levelname)s | [%(name)s] %(message)s')
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
        self._logger_ready = True

    def _derive_node_runtime(self, cpu: int, ram: float) -> Dict[str, Any]:
        if cpu >= 4 and ram >= 9.0:
            profile = 'node_116_4c10g'
            num_thread = 4
            keep_alive = '15m'
            node_id = '116'
        elif cpu >= 3 and ram >= 5.0:
            profile = 'node_121_3c6g'
            num_thread = 3
            keep_alive = '10m'
            node_id = '121'
        else:
            profile = 'fallback_lowmem'
            num_thread = max(1, min(cpu, 2))
            keep_alive = '0'
            node_id = 'unknown'

        num_thread = self._env_int('ZL_NUM_THREAD', self._env_int('OLLAMA_NUM_THREAD', num_thread))
        keep_alive = str(self._env('ZL_KEEP_ALIVE', self._env('OLLAMA_KEEP_ALIVE', keep_alive)))

        return {
            'node_id': node_id,
            'profile': profile,
            'cpu_cores': cpu,
            'ram_gb': round(ram, 2),
            'num_thread': num_thread,
            'keep_alive': keep_alive,
        }

    def _load(self) -> None:
        if self._loaded:
            return
        with self._lock:
            if self._loaded:
                return

            env_file = PROJECT_ROOT / '.env'
            if not env_file.exists():
                raise RuntimeError(f'Critical Logical Gap: .env missing at {env_file}')
            if not load_dotenv(env_file, override=False):
                raise RuntimeError(f'Critical Logical Gap: .env load failed at {env_file}')

            v: Dict[str, Any] = {}
            v['PROJECT_ROOT'] = PROJECT_ROOT
            v['AUDIT_PASS'] = AUDIT_PASS
            v['DB_PATH'] = PROJECT_ROOT / 'storage' / 'database' / 'zhulong.duckdb'
            v['LOG_DIR'] = PROJECT_ROOT / 'storage' / 'logs'
            v['CACHE_DIR'] = PROJECT_ROOT / '.cache'

            v['OLLAMA_BASE_URL'] = self._env('OLLAMA_BASE_URL', 'http://192.0.2.20:11434')
            v['OLLAMA_URL'] = self._env('OLLAMA_URL', f"{v['OLLAMA_BASE_URL']}/api/generate")
            v['OLLAMA_MODEL'] = self._env('OLLAMA_MODEL', 'deepseek-r1:7b')
            v['OLLAMA_TIMEOUT'] = self._env_int('OLLAMA_TIMEOUT', 30)
            v['AUDIT_STRATEGY'] = str(
                self._env('AUDIT_STRATEGY', self._env('ZHULONG_AUDIT_STRATEGY', 'prod_only'))
            ).strip().lower()
            v['OPENVINO_SHADOW_HOST'] = self._env('OPENVINO_SHADOW_HOST', 'root@192.0.2.20')
            v['OPENVINO_SHADOW_PYTHON'] = self._env('OPENVINO_SHADOW_PYTHON', '/opt/openvino_venv102/bin/python')
            v['OPENVINO_SHADOW_MODEL_DIR'] = self._env('OPENVINO_SHADOW_MODEL_DIR', '/root/intel/models/deepseek-r1-1.5b-ir')
            v['OPENVINO_SHADOW_DEVICE'] = self._env('OPENVINO_SHADOW_DEVICE', 'GPU')
            v['OPENVINO_SHADOW_TIMEOUT'] = self._env_int('OPENVINO_SHADOW_TIMEOUT', 900)
            v['audit_strategy'] = v['AUDIT_STRATEGY']


            audit_profile = self._build_audit_profile()
            v['AUDIT_PROFILE'] = audit_profile.name
            v['AUDIT_PROFILE_OBJECT'] = audit_profile
            v['AUDIT_PROFILE_DICT'] = audit_profile.to_dict()
            v['AUDIT_PROFILES'] = {name: profile.to_dict() for name, profile in AUDIT_PROFILE_PRESETS.items()}

            v['L1_CANDIDATE_LIMIT'] = audit_profile.l1_candidate_limit
            v['L2_TOP_N_INTRADAY'] = audit_profile.l2_top_n_intraday
            v['L2_TOP_N_CLOSE'] = audit_profile.l2_top_n_close
            v['L2_TOP_N'] = audit_profile.l2_top_n_close
            v['L2_PASS_THRESHOLD'] = audit_profile.l2_pass_threshold
            v['L2_WATCH_THRESHOLD'] = audit_profile.l2_watch_threshold
            v['L4_PASS_THRESHOLD'] = audit_profile.l4_pass_threshold
            v['L4_WATCH_THRESHOLD'] = audit_profile.l4_watch_threshold
            v['ZETA_DIV_SOFT'] = audit_profile.zeta_div_soft
            v['ZETA_DIV_HARD'] = audit_profile.zeta_div_hard
            v['TACTICS_MIN_FINAL_SCORE_PASS'] = audit_profile.tactics_min_final_score_pass
            v['TACTICS_MIN_FINAL_SCORE_WATCH'] = audit_profile.tactics_min_final_score_watch
            v['TACTICS_MIN_FINAL_SCORE'] = audit_profile.tactics_min_final_score_pass
            v['AUDIT_CYCLE_BUDGET_SEC'] = audit_profile.cycle_budget_sec
            v['AUDIT_NEXT_STAGE_RESERVE_SEC'] = audit_profile.next_stage_reserve_sec

            v['DEEPSEEK_API_KEY'] = self._env('DEEPSEEK_API_KEY', '')
            v['DEEPSEEK_BASE_URL'] = self._env('DEEPSEEK_BASE_URL', 'https://api.deepseek.com')
            v['DEEPSEEK_MODEL_V3'] = self._env('DEEPSEEK_MODEL_V3', 'deepseek-v4-flash')
            v['DEEPSEEK_MODEL_R1'] = self._env('DEEPSEEK_MODEL_R1', 'deepseek-v4-pro')

            v['ZHIPU_API_KEY'] = self._env('ZHIPU_API_KEY', '')
            v['ZHIPU_BASE_URL'] = self._env('ZHIPU_BASE_URL', 'https://open.bigmodel.cn/api/paas/v4/')
            v['ZHIPU_MODEL'] = self._env('ZHIPU_MODEL', 'glm-4-flash')

            v['QWEN_API_KEY'] = self._env('QWEN_API_KEY', self._env('DASHSCOPE_API_KEY', ''))
            v['PUSHPLUS_TOKEN'] = self._env('PUSHPLUS_TOKEN', '')
            v['PUSHPLUS_URL'] = self._env('PUSHPLUS_URL', 'https://www.pushplus.plus/send')
            v['PUSHPLUS_TIMEOUT'] = self._env_int('PUSHPLUS_TIMEOUT', 10)
            v['WEBHOOK_URL'] = self._env('WEBHOOK_URL', '')

            v['TUSHARE_TOKEN'] = self._env('TUSHARE_TOKEN', '')
            v['TUSHARE_RETRY_TIMES'] = self._env_int('TUSHARE_RETRY_TIMES', 3)
            v['TUSHARE_BATCH_SIZE'] = self._env_int('TUSHARE_BATCH_SIZE', 500)

            v['MAX_WORKERS'] = self._env_int('MAX_WORKERS', 8)
            v['MIN_WORKERS'] = self._env_int('MIN_WORKERS', 4)
            v['REQUEST_QUEUE_SIZE'] = self._env_int('REQUEST_QUEUE_SIZE', 50)
            v['AUTO_SCALE_ENABLED'] = self._env_bool('AUTO_SCALE_ENABLED', True)

            v['DB_POOL_SIZE'] = self._env_int('DB_POOL_SIZE', 10)
            v['DB_MAX_OVERFLOW'] = self._env_int('DB_MAX_OVERFLOW', 5)
            v['DB_POOL_TIMEOUT'] = self._env_int('DB_POOL_TIMEOUT', 30)
            v['DB_ECHO'] = self._env_bool('DB_ECHO', False)

            v['MIN_ROE_AVG'] = self._env_float('MIN_ROE_AVG', 0.10)
            v['HARD_STOP_LOSS'] = self._env_float('HARD_STOP_LOSS', -0.07)
            v['TAKE_PROFIT_START'] = self._env_float('TAKE_PROFIT_START', 0.05)
            v['TRAILING_STOP'] = self._env_float('TRAILING_STOP', 0.03)

            v['RETRY_TIMES'] = self._env_int('RETRY_TIMES', 3)
            v['RETRY_DELAY'] = self._env_float('RETRY_DELAY', 2.0)
            v['PUSH_LEVEL_REALTIME'] = self._env_int('PUSH_LEVEL_REALTIME', 1)
            v['PUSH_LEVEL_DAILY'] = self._env_int('PUSH_LEVEL_DAILY', 2)
            v['PUSH_LEVEL_SILENT'] = self._env_int('PUSH_LEVEL_SILENT', 3)

            v['SYNC_STOCK_BATCH_SIZE'] = self._env_int('SYNC_STOCK_BATCH_SIZE', 500)
            v['SYNC_PRICE_BATCH_SIZE'] = self._env_int('SYNC_PRICE_BATCH_SIZE', 1000)
            v['SYNC_REQUEST_DELAY'] = self._env_float('SYNC_REQUEST_DELAY', 0.1)
            v['SYNC_ENABLE_INCREMENTAL'] = self._env_bool('SYNC_ENABLE_INCREMENTAL', True)

            v['TRADE_START_TIME'] = self._env('TRADE_START_TIME', '09:30')
            v['TRADE_END_TIME'] = self._env('TRADE_END_TIME', '15:00')

            v['ADMIN_TOKEN'] = self._env('ADMIN_TOKEN', '')
            v['BACKUP_PATH'] = self._env('BACKUP_PATH', str(PROJECT_ROOT / 'backup'))
            v['BACKUP_RETENTION_DAYS'] = self._env_int('BACKUP_RETENTION_DAYS', 30)

            v['DASHBOARD_THEME_COLOR'] = self._env('DASHBOARD_THEME_COLOR', '#0B1222')
            v['DASHBOARD_SECONDARY_COLOR'] = self._env('DASHBOARD_SECONDARY_COLOR', '#1E222D')

            v['NODE_CPU_CORES'] = self._env_int('NODE_CPU_CORES', os.cpu_count() or 1)
            mem_gb = self._env_float('NODE_RAM_GB', 0.0)
            v['NODE_RAM_GB'] = mem_gb if mem_gb > 0 else _read_mem_gb()

            Path(v['LOG_DIR']).mkdir(parents=True, exist_ok=True)
            Path(v['CACHE_DIR']).mkdir(parents=True, exist_ok=True)

            runtime = self._derive_node_runtime(int(v['NODE_CPU_CORES']), float(v['NODE_RAM_GB']))
            v['NODE_ID'] = runtime['node_id']
            v['NODE_PROFILE'] = runtime['profile']

            self._values = v
            self._setup_governance_logger()

            logger.info(
                'Config loaded | node=%s | snapshot=%s',
                runtime['node_id'],
                {
                    'profile': runtime['profile'],
                    'cpu_cores': runtime['cpu_cores'],
                    'ram_gb': runtime['ram_gb'],
                    'num_thread': runtime['num_thread'],
                    'keep_alive': runtime['keep_alive'],
                    'db_path': str(v['DB_PATH']),
                    'log_dir': str(v['LOG_DIR']),
                    'ollama_url': v['OLLAMA_URL'],
                    'ollama_model': v['OLLAMA_MODEL'],
                },
            )

            self._loaded = True

    def get(self, key: str) -> Any:
        self._load()
        return self._values[key]

    def to_dict(self) -> Dict[str, Any]:
        self._load()
        return dict(self._values)

    def get_node_config(self) -> Dict[str, Any]:
        """
        Node-aware presets for num_thread and keep_alive.

        Profiles:
        - node_116_4c10g: >=4C and >=9GB
        - node_121_3c6g:  >=3C and >=5GB
        - fallback_lowmem
        """
        self._load()
        cpu = int(self._values.get('NODE_CPU_CORES', 1) or 1)
        ram = float(self._values.get('NODE_RAM_GB', 0.0) or 0.0)
        runtime = self._derive_node_runtime(cpu, ram)
        return {
            'node_id': runtime['node_id'],
            'profile': runtime['profile'],
            'cpu_cores': runtime['cpu_cores'],
            'ram_gb': runtime['ram_gb'],
            'num_thread': runtime['num_thread'],
            'keep_alive': runtime['keep_alive'],
        }


_STORE = _SettingsData()


class _LazyConfigMeta(type):
    def __getattr__(cls, item: str) -> Any:
        if item == 'get_node_config':
            return _STORE.get_node_config
        if item == 'get_audit_profile':
            return _STORE.get_audit_profile
        if item == 'get_audit_profiles':
            return _STORE.get_audit_profiles
        if item == 'to_dict':
            return _STORE.to_dict
        try:
            return _STORE.get(item)
        except KeyError as exc:
            # 让内置 getattr(Config, 'KEY', default) 正常回落到 default
            raise AttributeError(item) from exc


class Config(metaclass=_LazyConfigMeta):
    """Singleton facade for lazy configuration access."""


def get_node_config() -> Dict[str, Any]:
    return _STORE.get_node_config()


def get_audit_profile() -> AuditProfile:
    return _STORE.get_audit_profile()


def get_audit_profiles() -> Dict[str, Dict[str, Any]]:
    return _STORE.get_audit_profiles()


def __getattr__(name: str):
    """Module-level lazy access for legacy callers."""
    if name in {'DB_PATH', 'LOG_DIR', 'OLLAMA_URL', 'AUDIT_PASS', 'AUDIT_PROFILE'}:
        return getattr(Config, name)
    raise AttributeError(name)


if __name__ == '__main__':
    ensure_syspath()
    node_cfg = get_node_config()
    print(f'PROJECT_ROOT: {Config.PROJECT_ROOT}')
    print(f'DB_PATH:      {Config.DB_PATH}')
    print(f'LOG_DIR:      {Config.LOG_DIR}')
    print(f'OLLAMA_URL:   {Config.OLLAMA_URL}')
    print(f'NodeConfig:   {node_cfg}')
    print('Settings OK')
