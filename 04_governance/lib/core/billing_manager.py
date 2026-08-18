#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
 烛龙 V1.7 Platinum - 财务管理器
============================================================
core/billing_manager.py

核心功能:
1. 官方余额同步 (DeepSeek API)
2. 财务熔断自愈 (< 5 CNY 自动 WATCH_ONLY)
3. 402 异常降级处理
4. 微信推送恢复公告
============================================================
"""

import sys
import json
import logging
import time
import threading
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, Callable
from enum import Enum

from config.settings import Config

logger = logging.getLogger('zhulong.billing_manager')


try:
    from .module_loader import load_attr_from_path, resolve_project_root
except Exception:
    _core_dir = Path(__file__).resolve().parent
    if str(_core_dir) not in sys.path:
        sys.path.append(str(_core_dir))
    from module_loader import load_attr_from_path, resolve_project_root

_PROJECT_ROOT = resolve_project_root(Path(__file__))
ComputeGateway = load_attr_from_path(
    "compute_gateway_02",
    _PROJECT_ROOT / "02_brain" / "lib" / "compute_gateway.py",
    "ComputeGateway",
)
COMPUTE_GATEWAY = ComputeGateway(logger=logger, max_slots=3)

BASE_DIR = Path(__file__).parent.parent
CONFIG_DIR = BASE_DIR / "config"

# ==================== 配置 ====================

DEEPSEEK_API_BASE = "https://api.deepseek.com"
BALANCE_ENDPOINT = "/user/balance"

# 熔断阈值
MELTDOWN_THRESHOLD = 5.0      # < 5 CNY 触发熔断
WARNING_THRESHOLD = 10.0       # < 10 CNY 进入高频监控

# 同步频率
NORMAL_SYNC_INTERVAL = 3600    # 正常: 1 小时
HIGH_FREQ_SYNC_INTERVAL = 300  # 高频: 5 分钟


class SystemMode(Enum):
    """系统运行模式"""
    ACTIVE = "ACTIVE"           # 正常交易
    WATCH_ONLY = "WATCH_ONLY"   # 静默观察
    MELTDOWN = "MELTDOWN"       # 熔断状态


@dataclass
class BalanceSnapshot:
    """余额快照"""
    # DeepSeek
    deepseek_balance: float = 0.0
    deepseek_currency: str = "CNY"

    # 状态
    is_sufficient: bool = True
    sync_time: datetime = field(default_factory=datetime.now)
    sync_success: bool = True
    error: str = ""

    # 系统模式
    system_mode: SystemMode = SystemMode.ACTIVE

    def to_dict(self) -> dict:
        return {
            "deepseek_balance": self.deepseek_balance,
            "deepseek_currency": self.deepseek_currency,
            "is_sufficient": self.is_sufficient,
            "system_mode": self.system_mode.value,
            "sync_time": self.sync_time.isoformat(),
            "sync_success": self.sync_success,
            "error": self.error
        }


class BillingManager:
    """
    财务管理器

    职责:
    1. 定期同步 API 余额
    2. 财务熔断自愈
    3. 异常降级处理
    """

    def __init__(self, on_mode_change: Callable[[SystemMode], None] = None):
        CONFIG_DIR.mkdir(exist_ok=True)

        # API 密钥
        self.deepseek_api_key = str(getattr(Config, "DEEPSEEK_API_KEY", "") or "")

        # 状态
        self._current_snapshot = BalanceSnapshot()
        self._system_mode = SystemMode.ACTIVE
        self._last_sync = datetime.min

        # 回调
        self._on_mode_change = on_mode_change

        # 后台同步
        self._sync_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

        # 持久化文件
        self._cache_file = CONFIG_DIR / "billing_cache.json"
        self._load_cache()

        logger.info(f" BillingManager 初始化, 当前模式: {self._system_mode.value}")

    # ==================== 持久化 ====================

    def _load_cache(self):
        """加载缓存"""
        if self._cache_file.exists():
            try:
                with open(self._cache_file, 'r') as f:
                    data = json.load(f)
                    self._current_snapshot.deepseek_balance = data.get("deepseek_balance", 0)
                    mode_str = data.get("system_mode", "ACTIVE")
                    self._system_mode = SystemMode(mode_str)
                    self._current_snapshot.system_mode = self._system_mode
            except Exception as e:
                logger.error(f"加载缓存失败: {e}")

    def _save_cache(self):
        """保存缓存"""
        try:
            with open(self._cache_file, 'w') as f:
                json.dump(self._current_snapshot.to_dict(), f, indent=2)
        except Exception as e:
            logger.error(f"保存缓存失败: {e}")

    # ==================== 余额同步 ====================

    def sync_deepseek_balance(self) -> float:
        """
        同步 DeepSeek 余额

        API: GET https://api.deepseek.com/user/balance
        """
        if not self.deepseek_api_key:
            logger.warning("DeepSeek API Key 未配置")
            self._current_snapshot.error = "API Key 未配置"
            self._current_snapshot.sync_success = False
            return 0.0

        try:
            response = COMPUTE_GATEWAY.http_get(
                f"{DEEPSEEK_API_BASE}{BALANCE_ENDPOINT}",
                timeout=10,
                headers={"Authorization": f"Bearer {self.deepseek_api_key}"},
                layer='BILLING',
                decision_id='billing_balance_sync',
            )

            if response.status_code == 200:
                data = response.json()
                # API 返回格式: {"is_available": true, "balance_infos": [{"currency": "CNY", "total_balance": "100.00", ...}]}
                balance_infos = data.get("balance_infos", [])

                for info in balance_infos:
                    if info.get("currency") == "CNY":
                        balance = float(info.get("total_balance", 0))
                        self._current_snapshot.deepseek_balance = balance
                        self._current_snapshot.deepseek_currency = "CNY"
                        break

                self._current_snapshot.sync_success = True
                self._current_snapshot.sync_time = datetime.now()
                self._current_snapshot.error = ""

                logger.info(f" DeepSeek 余额: {self._current_snapshot.deepseek_balance:.2f}")

            elif response.status_code == 401:
                self._current_snapshot.error = "API Key 无效"
                self._current_snapshot.sync_success = False

            else:
                self._current_snapshot.error = f"HTTP {response.status_code}"
                self._current_snapshot.sync_success = False

        except Exception as e:
            if COMPUTE_GATEWAY.is_timeout_error(e):
                self._current_snapshot.error = "????"
                self._current_snapshot.sync_success = False
            else:
                self._current_snapshot.error = str(e)[:50]
                self._current_snapshot.sync_success = False
                logger.error(f"??????: {e}")

        self._last_sync = datetime.now()

        # 检查熔断
        self._check_meltdown()

        # 保存
        self._save_cache()

        return self._current_snapshot.deepseek_balance

    # ==================== 熔断逻辑 ====================

    def _check_meltdown(self):
        """检查财务熔断"""
        balance = self._current_snapshot.deepseek_balance
        old_mode = self._system_mode

        if balance < MELTDOWN_THRESHOLD:
            # 触发熔断
            if self._system_mode != SystemMode.WATCH_ONLY:
                self._system_mode = SystemMode.WATCH_ONLY
                self._current_snapshot.is_sufficient = False
                logger.warning(f" 财务熔断! 余额 {balance:.2f} < {MELTDOWN_THRESHOLD}")
                self._notify_mode_change(old_mode, self._system_mode, balance)
        else:
            # 恢复
            if self._system_mode == SystemMode.WATCH_ONLY:
                self._system_mode = SystemMode.ACTIVE
                self._current_snapshot.is_sufficient = True
                logger.info(f" 财务恢复! 余额 {balance:.2f}")
                self._notify_mode_change(old_mode, self._system_mode, balance)

        self._current_snapshot.system_mode = self._system_mode

    def _notify_mode_change(self, old_mode: SystemMode, new_mode: SystemMode, balance: float):
        """通知模式变更"""
        if self._on_mode_change:
            self._on_mode_change(new_mode)

        # 尝试微信推送
        try:
            from core.notification import send_wechat_message

            if new_mode == SystemMode.WATCH_ONLY:
                msg = f" 烛龙财务熔断\n余额: {balance:.2f}\n状态: WATCH_ONLY\n时间: {datetime.now().strftime('%H:%M:%S')}"
            else:
                msg = f" 烛龙财务恢复\n余额: {balance:.2f}\n状态: ACTIVE\n时间: {datetime.now().strftime('%H:%M:%S')}"

            send_wechat_message(msg)
        except Exception as e:
            logger.warning(f"微信推送失败: {e}")

    # ==================== 402 降级处理 ====================

    def handle_api_error(self, error_code: int, api_name: str = "Unknown") -> bool:
        """
        处理 API 错误

        返回: True = 已处理, False = 未处理
        """
        if error_code == 402:
            logger.warning(f" {api_name} 余额不足 (402)")

            # 强制同步
            self.sync_deepseek_balance()

            # 触发熔断
            if self._system_mode != SystemMode.WATCH_ONLY:
                old_mode = self._system_mode
                self._system_mode = SystemMode.WATCH_ONLY
                self._current_snapshot.is_sufficient = False
                self._current_snapshot.system_mode = self._system_mode
                self._notify_mode_change(old_mode, self._system_mode, self._current_snapshot.deepseek_balance)

            return True

        return False

    # ==================== 后台同步 ====================

    def _sync_loop(self):
        """后台同步循环"""
        while not self._stop_event.is_set():
            try:
                self.sync_deepseek_balance()
            except Exception as e:
                logger.error(f"后台同步异常: {e}")

            # 根据余额决定同步频率
            if self._current_snapshot.deepseek_balance < WARNING_THRESHOLD:
                interval = HIGH_FREQ_SYNC_INTERVAL
            else:
                interval = NORMAL_SYNC_INTERVAL

            self._stop_event.wait(interval)

    def start_background_sync(self):
        """启动后台同步"""
        if self._sync_thread and self._sync_thread.is_alive():
            return

        self._stop_event.clear()
        self._sync_thread = threading.Thread(target=self._sync_loop, daemon=True)
        self._sync_thread.start()

        logger.info(" 后台余额同步已启动")

    def stop_background_sync(self):
        """停止后台同步"""
        self._stop_event.set()
        if self._sync_thread:
            self._sync_thread.join(timeout=5)
        logger.info(" 后台余额同步已停止")

    # ==================== 公开接口 ====================

    def get_snapshot(self) -> BalanceSnapshot:
        """获取当前快照"""
        return self._current_snapshot

    def get_system_mode(self) -> SystemMode:
        """获取系统模式"""
        return self._system_mode

    def is_active(self) -> bool:
        """是否可交易"""
        return self._system_mode == SystemMode.ACTIVE

    def force_sync(self) -> BalanceSnapshot:
        """强制同步"""
        self.sync_deepseek_balance()
        return self._current_snapshot


# ==================== 单例 ====================

_manager: Optional[BillingManager] = None


def get_billing_manager() -> BillingManager:
    global _manager
    if _manager is None:
        _manager = BillingManager()
    return _manager


def is_trading_allowed() -> bool:
    """是否允许交易"""
    return get_billing_manager().is_active()


def handle_402_error(api_name: str = "API") -> bool:
    """处理 402 错误"""
    return get_billing_manager().handle_api_error(402, api_name)


# ==================== 测试 ====================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s')

    print("=" * 60)
    print(" BillingManager 测试")
    print("=" * 60)

    manager = BillingManager()
    snapshot = manager.force_sync()

    print(f"\n 余额快照:")
    print(f"   DeepSeek: {snapshot.deepseek_balance:.2f}")
    print(f"   系统模式: {snapshot.system_mode.value}")
    print(f"   同步成功: {snapshot.sync_success}")
    print(f"   错误: {snapshot.error or '无'}")
