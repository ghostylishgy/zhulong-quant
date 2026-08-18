#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Shared A-share limit-price helpers for Shadow buy/sell simulation."""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from pathlib import Path

BASE_DIR = Path('/root/quant_project')
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

logger = logging.getLogger('shadow.market_limits')


@dataclass(frozen=True)
class LimitInfo:
    up_limit: float = 0.0
    down_limit: float = 0.0
    source: str = ''
    estimated: bool = False


def limit_pct_for_symbol(symbol: str) -> float:
    sym = str(symbol or '').strip().upper()
    code = sym.split('.')[0]
    if sym.endswith('.BJ') or code.startswith(('4', '8', '9')):
        return 0.30
    if sym.endswith('.SH') and code.startswith('688'):
        return 0.20
    if sym.endswith('.SZ') and code.startswith(('300', '301')):
        return 0.20
    return 0.10


def estimate_limit(symbol: str, pre_close: float) -> LimitInfo:
    if pre_close <= 0:
        return LimitInfo(source='LIMIT_MISSING', estimated=True)
    pct = limit_pct_for_symbol(symbol)
    return LimitInfo(
        up_limit=round(pre_close * (1 + pct), 2),
        down_limit=round(pre_close * (1 - pct), 2),
        source='LIMIT_ESTIMATED',
        estimated=True,
    )


def previous_close(conn, symbol: str, trade_date: str) -> float:
    row = conn.execute(
        """
        SELECT close
        FROM fact_daily
        WHERE symbol = ?
          AND CAST(trade_date AS DATE) < CAST(? AS DATE)
          AND COALESCE(close, 0) > 0
        ORDER BY trade_date DESC
        LIMIT 1
        """,
        [symbol, trade_date],
    ).fetchone()
    return float(row[0] or 0) if row else 0.0


def fetch_limit_info(symbol: str, trade_date: str, pre_close: float = 0.0) -> LimitInfo:
    try:
        from config.settings import Config
        token = str(getattr(Config, 'TUSHARE_TOKEN', '') or '').strip()
        if token:
            import tushare as ts
            api = ts.pro_api(token)
            df = api.stk_limit(ts_code=symbol, trade_date=str(trade_date).replace('-', '')[:8])
            if df is not None and len(df) > 0:
                row = df.iloc[0]
                up = float(row.get('up_limit') or row.get('UP_LIMIT') or 0)
                down = float(row.get('down_limit') or row.get('DOWN_LIMIT') or 0)
                if up > 0 and down > 0:
                    return LimitInfo(up_limit=round(up, 2), down_limit=round(down, 2), source='TUSHARE_STK_LIMIT')
    except Exception as exc:
        logger.debug('stk_limit unavailable %s %s: %s', symbol, trade_date, exc)
    return estimate_limit(symbol, pre_close)


def is_price_near_down_limit(price: float, down_limit: float) -> bool:
    if price <= 0 or down_limit <= 0:
        return False
    return price <= down_limit + max(0.01, down_limit * 0.001)
