# ============================================================
# ?? ???? v2 - ???????????????
# ============================================================

from __future__ import annotations

import os
import sys
import importlib.util
from pathlib import Path
from typing import Dict, Any

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_GOV_SETTINGS = _PROJECT_ROOT / '04_governance' / 'config' / 'settings.py'

_spec = importlib.util.spec_from_file_location('zhulong_gov_settings', str(_GOV_SETTINGS))
if _spec is None or _spec.loader is None:
    raise RuntimeError(f'Cannot load governance settings: {_GOV_SETTINGS}')

_mod = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = _mod
_spec.loader.exec_module(_mod)

Config = _mod.Config
ensure_syspath = _mod.ensure_syspath
get_node_config = _mod.get_node_config
AuditProfile = _mod.AuditProfile
get_audit_profile = _mod.get_audit_profile
get_audit_profiles = _mod.get_audit_profiles
normalize_audit_profile_name = _mod.normalize_audit_profile_name

PROJECT_ROOT = _mod.PROJECT_ROOT
DB_PATH = Config.DB_PATH
LOG_DIR = Config.LOG_DIR
CACHE_DIR = Config.CACHE_DIR
OLLAMA_URL = Config.OLLAMA_URL
AUDIT_PASS = _mod.AUDIT_PASS
AUDIT_PROFILE = getattr(_mod.Config, 'AUDIT_PROFILE', 'balanced')
AUDIT_PROFILE_OBJECT = get_audit_profile()
AUDIT_PROFILES = get_audit_profiles()


class Config:
    """烛龙 v2 系统全局配置"""
    AUDIT_PROFILE = AUDIT_PROFILE
    AUDIT_PROFILE_OBJECT = AUDIT_PROFILE_OBJECT
    AUDIT_PROFILES = AUDIT_PROFILES
    L1_CANDIDATE_LIMIT = AUDIT_PROFILE_OBJECT.l1_candidate_limit
    L2_TOP_N_INTRADAY = AUDIT_PROFILE_OBJECT.l2_top_n_intraday
    L2_TOP_N_CLOSE = AUDIT_PROFILE_OBJECT.l2_top_n_close
    L2_PASS_THRESHOLD = AUDIT_PROFILE_OBJECT.l2_pass_threshold
    L2_WATCH_THRESHOLD = AUDIT_PROFILE_OBJECT.l2_watch_threshold
    L4_PASS_THRESHOLD = AUDIT_PROFILE_OBJECT.l4_pass_threshold
    L4_WATCH_THRESHOLD = AUDIT_PROFILE_OBJECT.l4_watch_threshold
    ZETA_DIV_SOFT = AUDIT_PROFILE_OBJECT.zeta_div_soft
    ZETA_DIV_HARD = AUDIT_PROFILE_OBJECT.zeta_div_hard
    AUDIT_CYCLE_BUDGET_SEC = AUDIT_PROFILE_OBJECT.cycle_budget_sec
    AUDIT_NEXT_STAGE_RESERVE_SEC = AUDIT_PROFILE_OBJECT.next_stage_reserve_sec
    TACTICS_MIN_FINAL_SCORE_PASS = AUDIT_PROFILE_OBJECT.tactics_min_final_score_pass
    TACTICS_MIN_FINAL_SCORE_WATCH = AUDIT_PROFILE_OBJECT.tactics_min_final_score_watch


    # ==================== 路径配置 ====================
    PROJECT_ROOT = PROJECT_ROOT
    DB_PATH = PROJECT_ROOT / 'storage' / 'database' / 'zhulong.duckdb'
    LOG_DIR = PROJECT_ROOT / 'logs'
    CACHE_DIR = PROJECT_ROOT / '.cache'

    # ==================== 本地 Ollama 配置 ====================
    OLLAMA_BASE_URL = os.getenv('OLLAMA_BASE_URL', 'http://192.0.2.20:11434')
    OLLAMA_URL = os.getenv('OLLAMA_URL', OLLAMA_BASE_URL + '/api/generate')
    OLLAMA_MODEL = os.getenv('OLLAMA_MODEL', 'deepseek-r1:7b')
    OLLAMA_TIMEOUT = int(os.getenv('OLLAMA_TIMEOUT', '30'))  # 请求超时（秒）
    AUDIT_STRATEGY = os.getenv('AUDIT_STRATEGY', os.getenv('ZHULONG_AUDIT_STRATEGY', 'prod_only')).strip().lower()
    OPENVINO_SHADOW_HOST = os.getenv('OPENVINO_SHADOW_HOST', 'root@192.0.2.20')
    OPENVINO_SHADOW_PYTHON = os.getenv('OPENVINO_SHADOW_PYTHON', '/opt/openvino_venv102/bin/python')
    OPENVINO_SHADOW_MODEL_DIR = os.getenv('OPENVINO_SHADOW_MODEL_DIR', '/root/intel/models/deepseek-r1-1.5b-ir')
    OPENVINO_SHADOW_DEVICE = os.getenv('OPENVINO_SHADOW_DEVICE', 'GPU')
    OPENVINO_SHADOW_TIMEOUT = int(os.getenv('OPENVINO_SHADOW_TIMEOUT', '900'))
    audit_strategy = AUDIT_STRATEGY

    # ==================== 云端 AI 配置 (仅用于最终裁决) ====================
    DEEPSEEK_API_KEY = os.getenv('DEEPSEEK_API_KEY', '')
    DEEPSEEK_BASE_URL = os.getenv('DEEPSEEK_BASE_URL', 'https://api.deepseek.com')
    DEEPSEEK_MODEL_V3 = 'deepseek-chat'
    DEEPSEEK_MODEL_R1 = 'deepseek-reasoner'
    DEEPSEEK_MAX_CALLS_PER_DAY = int(os.getenv('DEEPSEEK_MAX_CALLS_PER_DAY', '10'))  # 每日调用次数限制

    # ==================== 智谱 GLM-4 配置 (备用) ====================
    # ==================== Kimi (Moonshot) ?? ====================
    KIMI_API_KEY = os.getenv('KIMI_API_KEY', os.getenv('MOONSHOT_API_KEY', ''))
    KIMI_BASE_URL = os.getenv('KIMI_BASE_URL', 'https://api.moonshot.cn')
    KIMI_MODEL = os.getenv('KIMI_MODEL', 'moonshot-v1-128k')

    ZHIPU_API_KEY = os.getenv('ZHIPU_API_KEY', '')
    ZHIPU_BASE_URL = os.getenv('ZHIPU_BASE_URL', 'https://open.bigmodel.cn/api/paas/v4/')
    ZHIPU_MODEL = 'glm-4-flash'

    # ==================== ?? Qwen ?? (DashScope) ====================
    QWEN_API_KEY = os.getenv('QWEN_API_KEY', os.getenv('DASHSCOPE_API_KEY', ''))

    # ==================== 数据源配置 ====================
    DATA_SOURCE = 'akshare'
    AKSHARE_TIMEOUT = int(os.getenv('AKSHARE_TIMEOUT', '30'))  # 数据请求超时（秒）
    # ==================== Tushare 配置 (申万行业分类) ====================
    TUSHARE_TOKEN = os.getenv('TUSHARE_TOKEN', '')
    TUSHARE_RETRY_TIMES = int(os.getenv('TUSHARE_RETRY_TIMES', '3'))
    TUSHARE_BATCH_SIZE = int(os.getenv('TUSHARE_BATCH_SIZE', '500'))


    # ==================== 并发控制 ====================
    MAX_WORKERS = int(os.getenv('MAX_WORKERS', '8'))  # 最大并发数（适配4核CPU）
    REQUEST_QUEUE_SIZE = int(os.getenv('REQUEST_QUEUE_SIZE', '50'))  # 请求队列大小
    MIN_WORKERS = int(os.getenv('MIN_WORKERS', '4'))  # 最小并发数
    AUTO_SCALE_ENABLED = os.getenv('AUTO_SCALE_ENABLED', 'true').lower() == 'true'  # 自动扩缩容

    # ==================== 数据库配置 ====================
    DB_POOL_SIZE = int(os.getenv('DB_POOL_SIZE', '10'))  # 连接池大小
    DB_MAX_OVERFLOW = int(os.getenv('DB_MAX_OVERFLOW', '5'))  # 最大溢出连接数
    DB_POOL_TIMEOUT = int(os.getenv('DB_POOL_TIMEOUT', '30'))  # 连接超时（秒）
    DB_ECHO = os.getenv('DB_ECHO', 'false').lower() == 'true'  # SQL日志开关（生产环境关闭）

    # ==================== 缓存配置 ====================
    CACHE_TTL_DAYS = int(os.getenv('CACHE_TTL_DAYS', '7'))  # 缓存有效期（天）
    CACHE_MAX_SIZE = int(os.getenv('CACHE_MAX_SIZE', '1000'))  # LRU缓存最大条目数
    ENABLE_REDIS_CACHE = os.getenv('ENABLE_REDIS_CACHE', 'false').lower() == 'true'  # 是否启用Redis
    REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
    REDIS_DB = int(os.getenv('REDIS_DB', '0'))

    # ==================== ROE 审计配置 ====================
    MIN_ROE_YEARS = int(os.getenv('MIN_ROE_YEARS', '3'))  # 最少年份
    MIN_ROE_AVG = float(os.getenv('MIN_ROE_AVG', '0.10'))  # 最低平均ROE (10%)
    ROE_DATA_SOURCE = os.getenv('ROE_DATA_SOURCE', 'akshare')  # roe数据源

    # ==================== 现金流背离检查配置 ====================
    CASHFLOW_DIV_THRESHOLD = float(os.getenv('CASHFLOW_DIV_THRESHOLD', '0.30'))  # 背离度阈值 (30%)
    ENABLE_CASHFLOW_CHECK = os.getenv('ENABLE_CASHFLOW_CHECK', 'true').lower() == 'true'  # 是否启用

    # ==================== 风控参数 ====================
    HARD_STOP_LOSS = float(os.getenv('HARD_STOP_LOSS', '-0.07'))  # 硬止损：亏损 7%
    TAKE_PROFIT_START = float(os.getenv('TAKE_PROFIT_START', '0.05'))  # 移动止盈起点：盈利 5%
    TRAILING_STOP = float(os.getenv('TRAILING_STOP', '0.03'))  # 移动止盈回撤：3%
    MAX_SINGLE_POSITION = float(os.getenv('MAX_SINGLE_POSITION', '0.25'))  # 单股最大仓位 25%
    MAX_TOTAL_POSITIONS = int(os.getenv('MAX_TOTAL_POSITIONS', '4'))  # 最多同时持有 4 只

    # ==================== RPS 计算配置 ====================
    RPS_PERIODS = [50, 120, 250]  # RPS 计算周期（交易日）
    RPS_MIN_DATA_DAYS = int(os.getenv('RPS_MIN_DATA_DAYS', '260'))  # 最少需要多少天数据
    RPS_UPDATE_INTERVAL_HOURS = int(os.getenv('RPS_UPDATE_INTERVAL_HOURS', '4'))  # RPS更新间隔（小时）

    # ==================== 重试配置 ====================
    RETRY_TIMES = int(os.getenv('RETRY_TIMES', '3'))  # 重试次数
    RETRY_DELAY = float(os.getenv('RETRY_DELAY', '2'))  # 重试间隔（秒）
    RETRY_EXPONENTIAL = os.getenv('RETRY_EXPONENTIAL', 'true').lower() == 'true'  # 指数退避

    # ==================== PushPlus 推送配置 ====================
    PUSHPLUS_TOKEN = os.getenv('PUSHPLUS_TOKEN', '')
    PUSHPLUS_URL = 'https://www.pushplus.plus/send'
    PUSHPLUS_TIMEOUT = int(os.getenv('PUSHPLUS_TIMEOUT', '10'))

    # 推送级别
    PUSH_LEVEL_REALTIME = 1  # L1: 买卖信号/止损 (实时推送)
    PUSH_LEVEL_DAILY = 2  # L2: 每日简报 (定时推送)
    PUSH_LEVEL_SILENT = 3  # L3: 静默记录

    # ==================== 日志配置 ====================
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
    LOG_FORMAT = '%(asctime)s | %(levelname)s | %(name)s | %(message)s'
    LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
    LOG_MAX_BYTES = int(os.getenv('LOG_MAX_BYTES', '10485760'))  # 10MB
    LOG_BACKUP_COUNT = int(os.getenv('LOG_BACKUP_COUNT', '5'))  # 保留5个备份

    # ==================== 数据同步配置 ====================
    SYNC_STOCK_BATCH_SIZE = int(os.getenv('SYNC_STOCK_BATCH_SIZE', '500'))  # 批量写入大小
    SYNC_PRICE_BATCH_SIZE = int(os.getenv('SYNC_PRICE_BATCH_SIZE', '1000'))  # K线批量写入大小
    SYNC_REQUEST_DELAY = float(os.getenv('SYNC_REQUEST_DELAY', '0.1'))  # 请求间隔（秒）
    SYNC_ENABLE_INCREMENTAL = os.getenv('SYNC_ENABLE_INCREMENTAL', 'true').lower() == 'true'  # 增量同步

    # ==================== 监控配置 ====================
    ENABLE_MONITORING = os.getenv('ENABLE_MONITORING', 'true').lower() == 'true'
    HEALTH_CHECK_INTERVAL = int(os.getenv('HEALTH_CHECK_INTERVAL', '60'))  # 健康检查间隔（秒）
    MEMORY_WARNING_THRESHOLD = float(os.getenv('MEMORY_WARNING_THRESHOLD', '0.80'))  # 内存警告阈值（80%）

    # ==================== 交易时间 ====================
    TRADE_START_TIME = '09:30'
    TRADE_END_TIME = '15:00'
    PRE_MARKET_PUSH_TIME = '09:00'  # 盘前推送时间
    POST_MARKET_PUSH_TIME = '15:30'  # 盘后推送时间

    # ==================== v1.8 新增配置 ====================
    ADMIN_TOKEN = os.getenv('ADMIN_TOKEN', '')  # Admin Key，用于敏感操作验证
    BACKUP_PATH = os.getenv('BACKUP_PATH', '/root/quant_project/backup')  # 本地备份路径
    BACKUP_RETENTION_DAYS = int(os.getenv('BACKUP_RETENTION_DAYS', '30'))  # 备份保留天数

    # Dashboard 主题配置
    DASHBOARD_THEME_COLOR = os.getenv('DASHBOARD_THEME_COLOR', '#0B1222')  # 午夜蓝
    DASHBOARD_SECONDARY_COLOR = os.getenv('DASHBOARD_SECONDARY_COLOR', '#1E222D')  # 钛金灰


    @classmethod
    def ensure_dirs(cls):
        """确保必要的目录存在"""
        cls.LOG_DIR.mkdir(parents=True, exist_ok=True)
        cls.CACHE_DIR.mkdir(parents=True, exist_ok=True)

    @classmethod
    def validate(cls):
        """验证配置完整性"""
        errors = []
        warnings = []

        # 必需配置
        if not cls.PUSHPLUS_TOKEN:
            warnings.append("⚠️ PUSHPLUS_TOKEN 未配置，推送功能将不可用")

        if not cls.OLLAMA_BASE_URL:
            errors.append("❌ OLLAMA_BASE_URL 未配置")

        # 可选配置
        if not cls.DEEPSEEK_API_KEY:
            warnings.append("⚠️ DEEPSEEK_API_KEY 未配置，云端AI功能将受限")

        if not cls.ZHIPU_API_KEY:
            warnings.append("⚠️ ZHIPU_API_KEY 未配置，智谱AI功能将不可用")

        # 验证并发数
        if cls.MAX_WORKERS < cls.MIN_WORKERS:
            errors.append(f"❌ MAX_WORKERS ({cls.MAX_WORKERS}) 不能小于 MIN_WORKERS ({cls.MIN_WORKERS})")

        # 输出结果
        if errors:
            for err in errors:
                print(err)
            return False

        if warnings:
            for warn in warnings:
                print(warn)

        return True

    @classmethod
    def show(cls):
        """显示当前配置（隐藏敏感信息）"""
        print("=" * 60)
        print("🐲 烛龙计划 v2 - 当前配置")
        print("=" * 60)
        print(f"📁 项目根目录: {cls.PROJECT_ROOT}")
        print(f"💾 数据库路径: {cls.DB_PATH}")
        print(f"📝 日志目录: {cls.LOG_DIR}")
        print(f"🔄 缓存目录: {cls.CACHE_DIR}")
        print()
        print("🤖 AI 配置:")
        print(f"   本地 Ollama: {cls.OLLAMA_BASE_URL} ({cls.OLLAMA_MODEL})")
        print(f"   云端 DeepSeek: {'已配置' if cls.DEEPSEEK_API_KEY else '未配置'}")
        print(f"   智谱 GLM-4: {'已配置' if cls.ZHIPU_API_KEY else '未配置'}")
        print()
        print("🚀 并发配置:")
        print(f"   最大并发数: {cls.MAX_WORKERS}")
        print(f"   最小并发数: {cls.MIN_WORKERS}")
        print(f"   自动扩缩容: {'启用' if cls.AUTO_SCALE_ENABLED else '禁用'}")
        print()
        print("📊 审计配置:")
        print(f"   ROE 最低均值: {cls.MIN_ROE_AVG*100:.0f}% (过去{cls.MIN_ROE_YEARS}年)")
        print(f"   现金流背离阈值: {cls.CASHFLOW_DIV_THRESHOLD*100:.0f}%")
        print(f"   现金流检查: {'启用' if cls.ENABLE_CASHFLOW_CHECK else '禁用'}")
        print()
        print("🛡️ 风控配置:")
        print(f"   硬止损: {cls.HARD_STOP_LOSS*100:.0f}%")
        print(f"   移动止盈: {cls.TAKE_PROFIT_START*100:.0f}%")
        print(f"   单股最大仓位: {cls.MAX_SINGLE_POSITION*100:.0f}%")
        print()
        print("📡 数据源:")
        print(f"   主要来源: {cls.DATA_SOURCE}")
        print(f"   增量同步: {'启用' if cls.SYNC_ENABLE_INCREMENTAL else '禁用'}")
        print()
        print("🔔 推送服务:")
        print(f"   PushPlus: {'已配置' if cls.PUSHPLUS_TOKEN else '未配置'}")
        print("🔧 V1.8 配置:")
        print(f"   Admin Token: {'已配置' if cls.ADMIN_TOKEN else '未配置'}")
        print(f"   备份路径: {cls.BACKUP_PATH}")
        print(f"   备份保留: {cls.BACKUP_RETENTION_DAYS} 天")
        print(f"   主题色: {cls.DASHBOARD_THEME_COLOR}")
        print()

        print("=" * 60)

    @classmethod
    def get_redis_url(cls):
        """获取Redis连接URL（如果启用）"""
        if not cls.ENABLE_REDIS_CACHE:
            return None
        return f"redis://{cls.REDIS_HOST}:{cls.REDIS_PORT}/{cls.REDIS_DB}"


def _build_v18_config():
    return {}


V1_8_CONFIG = _build_v18_config()
