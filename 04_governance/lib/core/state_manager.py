#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
 烛龙 V1.7 Platinum - 状态管理器
============================================================
core/state_manager.py

核心功能:
1. SQLite WAL 异步持久化
2. Watchlist / Logic_DNA / Token 余额 快速恢复
3. NTP 时间偏移检测
4. 5 秒内恢复战斗状态
============================================================
"""

import os
import sys
import json
import logging
import time
import socket
import struct
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Any
from threading import Lock

logger = logging.getLogger('zhulong.state_manager')

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
STATE_DB = DATA_DIR / "state.db"
PROJECT_ROOT = Path(__file__).resolve().parents[3]

# DBGateway contract import (01_engine)
try:
    from .module_loader import load_attr_from_path
except Exception:
    _core_dir = Path(__file__).resolve().parent
    if str(_core_dir) not in sys.path:
        sys.path.append(str(_core_dir))
    from module_loader import load_attr_from_path

DBGateway = load_attr_from_path(
    "db_gateway_01",
    PROJECT_ROOT / "01_engine" / "lib" / "db_gateway.py",
    "DBGateway",
)

# NTP 配置
NTP_SERVERS = ["ntp.aliyun.com", "time.windows.com", "pool.ntp.org"]
NTP_TIMEOUT = 2
NTP_MAX_OFFSET = 1.0  # 最大允许偏移 (秒)


@dataclass
class SystemState:
    """系统状态快照"""
    # 监控池
    watchlist: List[str] = None

    # 财务
    deepseek_balance: float = 0.0
    system_mode: str = "ACTIVE"

    # 进化
    blacklist_count: int = 0

    # 时间
    last_save: str = ""
    ntp_offset: float = 0.0

    def __post_init__(self):
        if self.watchlist is None:
            self.watchlist = []


class StateManager:
    """
    状态管理器

    特性:
    - SQLite WAL 模式 (高并发写入)
    - 原子性保存
    - 5 秒恢复时间
    """

    def __init__(self):
        DATA_DIR.mkdir(exist_ok=True)
        self._lock = Lock()
        self._state = SystemState()

        # 初始化数据库
        self._init_db()

        # 恢复状态
        start = time.time()
        self._load_state()
        load_time = time.time() - start

        logger.info(f" StateManager 初始化 ({load_time*1000:.0f}ms)")

    def _init_db(self):
        """??????"""
        with DBGateway(str(STATE_DB), read_only=False, logger=logger) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS system_state (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS watchlist (
                    ts_code TEXT PRIMARY KEY,
                    name TEXT,
                    added_at TEXT
                )
            """)
    def _load_state(self):
        """????"""
        try:
            with DBGateway(str(STATE_DB), read_only=False, logger=logger) as conn:
                for key, value in conn.execute("SELECT key, value FROM system_state").fetchall():
                    if key == "deepseek_balance":
                        self._state.deepseek_balance = float(value)
                    elif key == "system_mode":
                        self._state.system_mode = value
                    elif key == "blacklist_count":
                        self._state.blacklist_count = int(value)
                    elif key == "ntp_offset":
                        self._state.ntp_offset = float(value)

                self._state.watchlist = [row[0] for row in conn.execute("SELECT ts_code FROM watchlist").fetchall()]

        except Exception as e:
            logger.error(f"??????: {e}")
    def save_state(self, state: Dict[str, Any] = None):
        """????"""
        with self._lock:
            try:
                now = datetime.now().isoformat()
                with DBGateway(str(STATE_DB), read_only=False, logger=logger) as conn:
                    if state:
                        for key, value in state.items():
                            conn.execute("""
                                INSERT OR REPLACE INTO system_state (key, value, updated_at)
                                VALUES (?, ?, ?)
                            """, [key, str(value), now])

                            if hasattr(self._state, key):
                                setattr(self._state, key, value)

                    self._state.last_save = now
                    conn.execute("""
                        INSERT OR REPLACE INTO system_state (key, value, updated_at)
                        VALUES ('last_save', ?, ?)
                    """, [now, now])

            except Exception as e:
                logger.error(f"??????: {e}")
    def save_watchlist(self, watchlist: List[Dict[str, str]]):
        """?? Watchlist"""
        with self._lock:
            try:
                now = datetime.now().isoformat()
                with DBGateway(str(STATE_DB), read_only=False, logger=logger) as conn:
                    conn.execute("DELETE FROM watchlist")

                    for item in watchlist:
                        conn.execute("""
                            INSERT INTO watchlist (ts_code, name, added_at)
                            VALUES (?, ?, ?)
                        """, [item.get("ts_code"), item.get("name", ""), now])

                self._state.watchlist = [item.get("ts_code") for item in watchlist]

            except Exception as e:
                logger.error(f"?? Watchlist ??: {e}")
    def get_watchlist(self) -> List[str]:
        """获取 Watchlist"""
        return self._state.watchlist.copy()

    def get_state(self) -> SystemState:
        """获取当前状态"""
        return self._state

    # ==================== NTP 时间校准 ====================

    def check_ntp_offset(self) -> float:
        """
        检测 NTP 时间偏移

        返回: 偏移秒数 (正=本地快, 负=本地慢)
        """
        for server in NTP_SERVERS:
            try:
                offset = self._get_ntp_offset(server)
                if offset is not None:
                    self._state.ntp_offset = offset

                    if abs(offset) > NTP_MAX_OFFSET:
                        logger.warning(f" NTP 偏移 {offset:.3f}s > {NTP_MAX_OFFSET}s, 建议校准")
                    else:
                        logger.info(f" NTP 偏移: {offset*1000:.0f}ms")

                    # 保存
                    self.save_state({"ntp_offset": offset})

                    return offset

            except Exception as e:
                logger.debug(f"NTP {server} 失败: {e}")
                continue

        logger.warning("所有 NTP 服务器不可用")
        return 0.0

    def _get_ntp_offset(self, server: str) -> Optional[float]:
        """获取 NTP 偏移"""
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            client.settimeout(NTP_TIMEOUT)

            # NTP 请求包
            data = b'\x1b' + 47 * b'\0'
            send_time = time.time()

            client.sendto(data, (server, 123))
            data, _ = client.recvfrom(1024)
            recv_time = time.time()

            client.close()

            if data:
                # 解析 NTP 响应
                unpacked = struct.unpack('!12I', data)
                t = unpacked[10] + float(unpacked[11]) / 2**32
                t -= 2208988800  # NTP -> Unix epoch

                # 计算偏移
                offset = t - (send_time + recv_time) / 2
                return offset

        except Exception:
            return None

        return None


# ==================== 单例 ====================

_manager: Optional[StateManager] = None


def get_state_manager() -> StateManager:
    global _manager
    if _manager is None:
        _manager = StateManager()
    return _manager


def quick_save(key: str, value: Any):
    """快速保存单个状态"""
    get_state_manager().save_state({key: value})


def check_time_sync() -> float:
    """检查时间同步"""
    return get_state_manager().check_ntp_offset()


# ==================== 测试 ====================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s')

    print("=" * 60)
    print(" StateManager 测试")
    print("=" * 60)

    manager = StateManager()

    # 保存状态
    manager.save_state({
        "deepseek_balance": 88.50,
        "system_mode": "ACTIVE"
    })

    # NTP 检测
    offset = manager.check_ntp_offset()

    # 显示状态
    state = manager.get_state()
    print(f"\n 系统状态:")
    print(f"   余额: {state.deepseek_balance:.2f}")
    print(f"   模式: {state.system_mode}")
    print(f"   NTP偏移: {state.ntp_offset*1000:.0f}ms")
    print(f"   Watchlist: {len(state.watchlist)} 只")
