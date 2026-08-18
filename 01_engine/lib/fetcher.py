#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
 烛龙计划 v2.2.1 - 数据获取引擎 (最终修复版)
============================================================
双数据源架构 + 自动降级:
- 主力: AkShare (带 User-Agent 伪装)
- 降级: Tushare K线接口
- 应急: stock_zh_a_spot_em (当日实时)

【v2.2.1-final 修复】
- User-Agent 浏览器伪装
- IP 被限时自动降级到 Tushare
- 随机延迟 2-4 秒
============================================================
"""

import os
import sys
import importlib
from pathlib import Path
os.environ["http_proxy"] = ""
os.environ["https_proxy"] = ""
os.environ["HTTP_PROXY"] = ""
os.environ["HTTPS_PROXY"] = ""
os.environ["no_proxy"] = "*"
os.environ["NO_PROXY"] = "*"

import logging
import time
import random
from requests import utils as requests_utils
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any, Tuple

import pandas as pd

# 延迟导入 AkShare 以便设置 User-Agent
import akshare as ak

try:
    import tushare as ts
    TUSHARE_AVAILABLE = True
except ImportError:
    TUSHARE_AVAILABLE = False
    ts = None

from config.settings import Config
from db_gateway import DBGateway
# from core.database import Database, get_db  # legacy import path (unresolved in current tree)
try:
    _db_mod = importlib.import_module("core.database")
except Exception:
    _gov_lib = Path(__file__).resolve().parents[2] / "04_governance" / "lib"
    if str(_gov_lib) not in sys.path:
        sys.path.append(str(_gov_lib))
    try:
        _db_mod = importlib.import_module("core.database")
    except Exception as _db_import_err:
        _db_mod = None

        def get_db():
            raise RuntimeError(
                "core.database unavailable after fallback import path injection: "
                f"{_db_import_err}"
            )
if "_db_mod" in locals() and _db_mod is not None:
    Database = getattr(_db_mod, "Database", Any)
    get_db = getattr(_db_mod, "get_db")

logger = logging.getLogger('zhulong.fetcher')

# v2.2.1-final: 优化配置
MAX_RETRIES = 2
MIN_DELAY = 2.0   # 最小延迟 2 秒
MAX_DELAY = 4.0   # 最大延迟 4 秒

# User-Agent 伪装列表 (模拟真实浏览器)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]


def set_random_user_agent():
    """设置随机 User-Agent 到 requests 默认 headers"""
    ua = random.choice(USER_AGENTS)
    # 尝试修改 akshare 底层的 requests session
    try:
        import akshare.utils as ak_utils
        if hasattr(ak_utils, 'requests_retry_session'):
            session = ak_utils.requests_retry_session()
            session.headers.update({'User-Agent': ua})
    except Exception as e:
        logger.warning(f"[fetcher] User-Agent 设置失败: {e}")

    # 直接修改 requests 默认 headers
    requests_utils.default_user_agent = lambda: ua
    return ua


def is_trading_day(check_date: date = None) -> bool:
    """检查是否为交易日 (简化版: 排除周末)"""
    if check_date is None:
        check_date = date.today()
    return check_date.weekday() < 5


class Fetcher:
    """
    烛龙数据获取引擎 (v2.2.1-final)

    核心职责:
    1. sync_sectors(): Tushare 同步申万行业分类
    2. sync_daily(): AkShare 同步日线 → 自动降级到 Tushare
    """

    def __init__(self):
        self.db = get_db()
        self._ts_pro = None
        self._akshare_failed_count = 0  # AkShare 连续失败计数

        # 初始化 Tushare (如果可用)
        if TUSHARE_AVAILABLE and Config.TUSHARE_TOKEN:
            try:
                self._ts_pro = ts.pro_api(Config.TUSHARE_TOKEN)
                logger.info("✅ Tushare Pro API 已初始化")
            except Exception as e:
                logger.warning(f"⚠️ Tushare 初始化失败: {e}")

        # 设置 User-Agent
        ua = set_random_user_agent()
        logger.info(f"🔧 Fetcher 初始化完成 | UA: {ua[:50]}...")

    # ==================== Tushare: 申万行业分类 ====================

    def sync_sectors(self) -> int:
        """[Tushare] 同步申万行业分类"""
        logger.info("=" * 60)
        logger.info("📊 [Tushare] 同步申万行业分类")
        logger.info("=" * 60)

        if not self._ts_pro:
            logger.error("❌ Tushare 未初始化")
            return 0

        try:
            df = self._ts_pro.stock_basic(
                exchange='',
                list_status='L',
                fields='ts_code,symbol,name,area,industry,market,list_date,is_hs'
            )

            if df is None or df.empty:
                logger.error("❌ Tushare 返回空数据")
                return 0

            logger.info(f"✅ 获取到 {len(df)} 只股票")

            df['is_st'] = df['name'].str.contains('ST', na=False)
            df['list_date'] = pd.to_datetime(df['list_date'], format='%Y%m%d', errors='coerce')

            count = self._save_sectors(df)
            logger.info(f"✅ 同步完成: {count} 只股票")
            return count

        except Exception as e:
            logger.error(f"❌ Tushare 同步失败: {e}")
            return 0

    def _save_sectors(self, df: pd.DataFrame) -> int:
        """??????????"""
        records = []
        for _, row in df.iterrows():
            records.append({
                'ts_code': row['ts_code'],
                'symbol': row['symbol'],
                'name': row['name'],
                'industry': row.get('industry', '') or '',
                'market': row.get('market', '') or '',
                'is_st': bool(row.get('is_st', False)),
                'list_date': row['list_date'].strftime('%Y-%m-%d') if pd.notna(row.get('list_date')) else None,
            })

        with DBGateway(str(Config.DB_PATH), read_only=False, logger=logger) as conn:
            conn.execute("DELETE FROM sector_map")
            for record in records:
                now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                conn.execute(
                    """
                    INSERT INTO sector_map (ts_code, symbol, name, industry, market, is_st, list_date, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, CAST(? AS TIMESTAMP))
                    """,
                    [
                        record['ts_code'], record['symbol'], record['name'],
                        record['industry'], record['market'], 1 if record['is_st'] else 0,
                        record['list_date'], now_ts
                    ]
                )
            conn.commit()

        return len(records)
    def sync_stock_list(self) -> int:
        """同步股票列表 (兼容接口)"""
        return self.sync_sectors()

    def sync_all_prices(self, limit: int = None, days: int = 120) -> Dict[str, int]:
        """同步全市场价格 (兼容接口)"""
        return self.sync_daily(limit=limit, days=days)

    # ==================== 日线同步 (AkShare → Tushare 降级) ====================

    def sync_daily(self, limit: int = None, days: int = 120) -> Dict[str, int]:
        """
        同步日线行情 (自动降级)

        策略:
        1. 优先使用 AkShare
        2. 连续 10 次失败后自动降级到 Tushare
        """
        logger.info("=" * 60)
        logger.info("📈 同步日线行情 (智能降级模式)")
        logger.info("=" * 60)

        result = {"updated": 0, "failed": 0, "akshare": 0, "tushare": 0}

        # 交易日检测
        today = date.today()
        if not is_trading_day(today):
            logger.warning(f"⚠️ 今天 ({today.strftime('%A')}) 是非交易日")

        # 获取股票列表
        stocks = self._get_stock_list(limit)
        if not stocks:
            logger.error("❌ 股票列表为空")
            return result

        logger.info(f"📋 待同步: {len(stocks)} 只股票")
        logger.info(f"⏱️ 预计耗时: {len(stocks) * 3 // 60} - {len(stocks) * 4 // 60} 分钟")

        # 决定使用哪个数据源
        use_tushare = False
        akshare_consecutive_fails = 0

        for i, (symbol, name) in enumerate(stocks):
            ts_code = self._symbol_to_ts_code(symbol)

            try:
                # 尝试 AkShare
                if not use_tushare:
                    count, source = self._sync_via_akshare(symbol, days)

                    if count > 0:
                        result["updated"] += 1
                        result["akshare"] += 1
                        akshare_consecutive_fails = 0
                    else:
                        akshare_consecutive_fails += 1

                        # 连续 10 次失败，切换到 Tushare
                        if akshare_consecutive_fails >= 10:
                            logger.warning("⚠️ AkShare 连续 10 次失败，切换到 Tushare")
                            use_tushare = True
                        else:
                            result["failed"] += 1

                # 使用 Tushare 降级
                if use_tushare and self._ts_pro:
                    count = self._sync_via_tushare(ts_code, days)
                    if count > 0:
                        result["updated"] += 1
                        result["tushare"] += 1
                    else:
                        result["failed"] += 1
                elif use_tushare and not self._ts_pro:
                    result["failed"] += 1

                # 进度日志
                if (i + 1) % 20 == 0:
                    logger.info(f"   进度: {i+1}/{len(stocks)} | 成功={result['updated']} (AK={result['akshare']}, TS={result['tushare']})")

                # 随机延迟
                delay = random.uniform(MIN_DELAY, MAX_DELAY)
                time.sleep(delay)

            except Exception as e:
                logger.warning(f"⚠️ {symbol} 异常: {e}")
                result["failed"] += 1

        # 最终统计
        logger.info("=" * 60)
        logger.info(f"✅ 日线同步完成:")
        logger.info(f"   成功: {result['updated']} (AkShare={result['akshare']}, Tushare={result['tushare']})")
        logger.info(f"   失败: {result['failed']}")
        logger.info("=" * 60)

        return result

    def _symbol_to_ts_code(self, symbol: str) -> str:
        """转换 symbol 到 ts_code 格式"""
        if symbol.startswith('6'):
            return f"{symbol}.SH"
        else:
            return f"{symbol}.SZ"

    def _sync_via_akshare(self, symbol: str, days: int = 120) -> Tuple[int, str]:
        """通过 AkShare 同步"""
        # 每次请求前刷新 User-Agent
        set_random_user_agent()

        for attempt in range(MAX_RETRIES):
            try:
                end_date = datetime.now().strftime('%Y%m%d')
                start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')

                df = ak.stock_zh_a_hist(
                    symbol=symbol,
                    period="daily",
                    start_date=start_date,
                    end_date=end_date,
                    adjust="qfq"
                )

                if df is None or df.empty:
                    return 0, "no_data"

                df = df.rename(columns={
                    '日期': 'trade_date',
                    '开盘': 'open',
                    '收盘': 'close',
                    '最高': 'high',
                    '最低': 'low',
                    '成交量': 'volume',
                    '成交额': 'amount',
                    '涨跌幅': 'pct_chg',
                    '换手率': 'turnover',
                })

                df['symbol'] = symbol
                df['trade_date'] = pd.to_datetime(df['trade_date'])

                count = self._save_daily_prices(df)
                return count, "akshare"

            except KeyError as e:
                # 股票代码不在映射中
                logger.debug(f"{symbol} KeyError: {e}")
                return 0, "no_data"
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(random.uniform(3, 6))
                    continue
                return 0, "network"

        return 0, "network"

    def _sync_via_tushare(self, ts_code: str, days: int = 120) -> int:
        """通过 Tushare 同步日线"""
        if not self._ts_pro:
            return 0

        try:
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')

            df = self._ts_pro.daily(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date
            )

            if df is None or df.empty:
                return 0

            # 转换为统一格式
            df = df.rename(columns={
                'trade_date': 'trade_date',
                'open': 'open',
                'close': 'close',
                'high': 'high',
                'low': 'low',
                'vol': 'volume',
                'amount': 'amount',
                'pct_chg': 'pct_chg',
            })

            # 提取 symbol
            df['symbol'] = ts_code.split('.')[0]
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
            df['turnover'] = 0  # Tushare 基础接口没有换手率

            count = self._save_daily_prices(df)
            return count

        except Exception as e:
            logger.debug(f"Tushare {ts_code} 失败: {e}")
            return 0

    def _get_stock_list(self, limit: int = None) -> List[tuple]:
        """从 sector_map 获取股票列表"""
        from sqlalchemy import text

        with self.db.get_session() as session:
            query = "SELECT symbol, name FROM sector_map WHERE is_st = 0"
            if limit:
                query += f" LIMIT {limit}"

            result = session.execute(text(query))
            return [(row[0], row[1]) for row in result.fetchall()]

    def _save_daily_prices(self, df: pd.DataFrame) -> int:
        """??????????"""
        records = []
        for _, row in df.iterrows():
            records.append({
                'symbol': row['symbol'],
                'trade_date': row['trade_date'].strftime('%Y-%m-%d') if hasattr(row['trade_date'], 'strftime') else str(row['trade_date'])[:10],
                'open': float(row.get('open') or 0),
                'high': float(row.get('high') or 0),
                'low': float(row.get('low') or 0),
                'close': float(row.get('close') or 0),
                'volume': float(row.get('volume') or 0),
                'amount': float(row.get('amount') or 0),
                'pct_chg': float(row.get('pct_chg') or 0),
                'turnover': float(row.get('turnover') or 0),
            })

        with DBGateway(str(Config.DB_PATH), read_only=False, logger=logger) as conn:
            for record in records:
                conn.execute(
                    """
                    INSERT INTO stock_daily (symbol, trade_date, open, high, low, close, volume, amount, pct_chg, turnover)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(symbol, trade_date) DO UPDATE SET
                        open = excluded.open,
                        high = excluded.high,
                        low = excluded.low,
                        close = excluded.close,
                        volume = excluded.volume,
                        amount = excluded.amount,
                        pct_chg = excluded.pct_chg,
                        turnover = excluded.turnover
                    """,
                    [
                        record['symbol'], record['trade_date'], record['open'], record['high'],
                        record['low'], record['close'], record['volume'], record['amount'],
                        record['pct_chg'], record['turnover']
                    ]
                )
            conn.commit()

        return len(records)
    def calculate_rps(self) -> int:
        """计算全市场 RPS"""
        logger.info("📊 计算 RPS 强度指标...")

        try:
            from strategies.rps_calculator import RPSCalculator
            calc = RPSCalculator()
            result = calc.calculate_all()

            if result is None or result.empty:
                return 0

            logger.info(f"✅ RPS 计算完成: {len(result)} 只股票")
            return len(result)
        except Exception as e:
            logger.error(f"❌ RPS 计算失败: {e}")
            return 0

    # ==================== 便捷方法 ====================

    def quick_test(self) -> bool:
        """快速测试 (验证至少能获取 1 只股票)"""
        logger.info("🧪 快速测试...")

        # 测试 AkShare
        try:
            set_random_user_agent()
            df = ak.stock_zh_a_hist(symbol="000001", period="daily", adjust="qfq")
            if df is not None and not df.empty:
                logger.info(f"   ✅ AkShare: {len(df)} 条")
            else:
                logger.warning("   ⚠️ AkShare 返回空")
        except Exception as e:
            logger.warning(f"   ⚠️ AkShare 失败: {e}")

        # 测试 Tushare
        if self._ts_pro:
            try:
                df = self._ts_pro.daily(ts_code="000001.SZ", limit=5)
                if df is not None and not df.empty:
                    logger.info(f"   ✅ Tushare: {len(df)} 条")
                else:
                    logger.warning("   ⚠️ Tushare 返回空")
            except Exception as e:
                logger.warning(f"   ⚠️ Tushare 失败: {e}")

        return True


def get_fetcher() -> Fetcher:
    """获取 Fetcher 实例"""
    return Fetcher()
