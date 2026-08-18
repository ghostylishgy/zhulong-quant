#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
烛龙 v2.2.2 - 全局常量与归一化适配器
============================================================
core/constants.py

【核心职责】
1. 全局表名常量 - 消除硬编码
2. normalize_code() - 统一识别码转换
3. 字段映射常量

作者: Opus (CTO)
版本: v2.2.2 - 大归一版
============================================================
"""

import re
from typing import Optional

# ==================== 网络配置常量 ====================

# 102 智库 AI 服务器地址
AI_SERVER_URL = "http://192.0.2.20:11434"
AI_SERVER_IP = "192.0.2.20"
AI_SERVER_PORT = 11434

# 101 指挥部本地地址
LOCAL_API_PORT = 8000
LOCAL_DASHBOARD_PORT = 8501


# ==================== 全局表名常量 ====================

# 日线行情表 (物理表名)
TABLE_STOCK_DAILY = "stock_daily"

# 基础信息表
TABLE_STOCK_BASIC = "stock_basic"

# 行业分类表
TABLE_SECTOR_MAP = "sector_map"

# 审计日志表
TABLE_AI_AUDIT_LOGS = "ai_audit_logs"

# 漏斗快照表
TABLE_FUNNEL_SNAPSHOTS = "funnel_snapshots"

# 审计记录表
TABLE_AUDIT_RECORDS = "audit_records"


# ==================== 识别码归一化适配器 ====================

def normalize_code(code: str, output_format: str = 'symbol') -> str:
    """
    【大归一】统一识别码转换

    无论输入什么格式，都能标准化输出。

    Args:
        code: 任意格式的股票代码
            - 600519
            - 600519.SH
            - SH600519
            - sh600519
            - 600519.sh
        output_format: 输出格式
            - 'symbol': 纯6位数字 (默认)
            - 'ts_code': 带后缀格式 (600519.SH)

    Returns:
        标准化后的股票代码

    Examples:
        >>> normalize_code("600519.SH")
        '600519'
        >>> normalize_code("SH600519")
        '600519'
        >>> normalize_code("600519", output_format='ts_code')
        '600519.SH'
    """
    if not code or not isinstance(code, str):
        return ""

    # 转大写并清理空格
    code = code.upper().strip()

    # 提取纯数字部分 (保留前6位)
    digits = re.sub(r'[^0-9]', '', code)
    if len(digits) < 6:
        return ""
    symbol = digits[:6]

    if output_format == 'symbol':
        return symbol

    elif output_format == 'ts_code':
        # 根据首位判断市场
        # 6开头 = 上海, 0/3开头 = 深圳, 4/8开头 = 北交所
        first_digit = symbol[0]
        if first_digit in ('6', '9'):
            return f"{symbol}.SH"
        elif first_digit in ('0', '2', '3'):
            return f"{symbol}.SZ"
        elif first_digit in ('4', '8'):
            return f"{symbol}.BJ"
        else:
            return f"{symbol}.SH"  # 默认上海

    return symbol


def get_market_suffix(symbol: str) -> str:
    """
    根据股票代码获取市场后缀

    Args:
        symbol: 6位纯数字代码

    Returns:
        '.SH' | '.SZ' | '.BJ'
    """
    if not symbol:
        return ".SH"

    first_digit = symbol[0]
    if first_digit in ('6', '9'):
        return ".SH"
    elif first_digit in ('0', '2', '3'):
        return ".SZ"
    elif first_digit in ('4', '8'):
        return ".BJ"
    return ".SH"


def symbol_to_ts_code(symbol: str) -> str:
    """symbol 转 ts_code 的便捷函数"""
    return normalize_code(symbol, output_format='ts_code')


def ts_code_to_symbol(ts_code: str) -> str:
    """ts_code 转 symbol 的便捷函数"""
    return normalize_code(ts_code, output_format='symbol')


# ==================== 字段映射常量 ====================

# stock_daily 表字段
FIELD_SYMBOL = "symbol"
FIELD_TRADE_DATE = "trade_date"
FIELD_CLOSE = "close"
FIELD_OPEN = "open"
FIELD_HIGH = "high"
FIELD_LOW = "low"
FIELD_VOLUME = "volume"
FIELD_AMOUNT = "amount"
FIELD_PCT_CHANGE = "pct_chg"
FIELD_TURNOVER = "turnover"


# ==================== 默认值常量 ====================

# 填充 None 值的默认值
DEFAULT_FILLNA = {
    'pct_chg': 0.0,
    'turnover': 0.0,
    'volume': 0,
    'amount': 0.0,
    'pe': 0.0,
    'pb': 0.0,
    'rps_10': 50,
    'rps_20': 50,
    'rps_50': 50,
}


# ==================== SQL 模板 ====================

SQL_SELECT_DAILY = f"""
    SELECT {FIELD_SYMBOL}, {FIELD_TRADE_DATE}, {FIELD_OPEN}, {FIELD_HIGH},
           {FIELD_LOW}, {FIELD_CLOSE}, {FIELD_VOLUME}, {FIELD_AMOUNT},
           {FIELD_PCT_CHANGE}, {FIELD_TURNOVER}
    FROM {TABLE_STOCK_DAILY}
    WHERE {FIELD_SYMBOL} = ?
    ORDER BY {FIELD_TRADE_DATE} DESC
    LIMIT ?
"""

SQL_SELECT_ALL_DAILY = f"""
    SELECT {FIELD_SYMBOL}, {FIELD_TRADE_DATE}, {FIELD_CLOSE}
    FROM {TABLE_STOCK_DAILY}
    WHERE {FIELD_TRADE_DATE} >= ?
    ORDER BY {FIELD_TRADE_DATE}
"""
