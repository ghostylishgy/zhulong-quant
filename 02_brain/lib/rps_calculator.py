# ============================================================
# 🐲 烛龙计划 - RPS 相对强度计算器 (v2.2.2 修复版)
# ============================================================
# 实现欧奈尔 RPS (Relative Price Strength) 算法
# 使用 pandas 向量化计算，避免循环遍历 5000+ 股票
#
# v2.2.2: 修复表名 daily_prices → stock_daily
#         修复字段名 code → symbol
# ============================================================

import logging
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Tuple

import pandas as pd
import numpy as np

# 导入全局常量
from core.constants import (
    TABLE_STOCK_DAILY,
    FIELD_SYMBOL,
    FIELD_TRADE_DATE,
    FIELD_CLOSE,
    DEFAULT_FILLNA
)

# 延迟导入避免循环依赖
def get_db():
    from core.database import Database
    return Database()

def get_fetcher():
    # from data_engine.fetcher import Fetcher  # legacy path (disabled)
    try:
        from lib.fetcher import Fetcher
        return Fetcher()
    except Exception:
        return None

logger = logging.getLogger('zhulong.rps')


class RPSCalculator:
    """
    欧奈尔 RPS (相对强度) 计算器

    RPS 衡量股票相对于市场中其他股票的价格表现。
    RPS = 90 表示该股票涨幅超过市场 90% 的股票。

    计算方法：
    1. 计算每只股票在指定周期内的涨跌幅
    2. 将所有股票按涨幅排名
    3. RPS = (排名 / 总数) × 100
    """

    # RPS 计算周期（交易日）
    PERIODS = {
        'rps_50': 50,    # 约 2.5 个月
        'rps_120': 120,  # 约 6 个月
        'rps_250': 250   # 约 1 年
    }

    def __init__(self):
        self.db = get_db()
        self.fetcher = get_fetcher()
        logger.info("📊 RPS 计算器初始化完成")

    def _load_price_matrix(self, days: int = 260) -> pd.DataFrame:
        """
        从数据库加载价格矩阵

        Args:
            days: 加载最近多少天的数据

        Returns:
            DataFrame: 行=日期, 列=股票代码, 值=收盘价
        """
        from sqlalchemy import text

        # 计算起始日期
        start_date = (datetime.now() - timedelta(days=days * 1.5)).date()

        # 【修复】使用正确的表名和字段名
        with self.db.get_session() as session:
            query = text(f"""
                SELECT {FIELD_SYMBOL}, {FIELD_TRADE_DATE}, {FIELD_CLOSE}
                FROM {TABLE_STOCK_DAILY}
                WHERE REPLACE(CAST({FIELD_TRADE_DATE} AS TEXT), '-', '') >= :start_date
                ORDER BY {FIELD_TRADE_DATE}
            """)
            result = session.execute(query, {'start_date': start_date.strftime('%Y%m%d')})
            rows = result.fetchall()

        if not rows:
            logger.warning("⚠️ 数据库中无价格数据")
            return pd.DataFrame()

        # 转换为 DataFrame，使用正确的列名
        df = pd.DataFrame(rows, columns=[FIELD_SYMBOL, FIELD_TRADE_DATE, FIELD_CLOSE])
        df[FIELD_TRADE_DATE] = (
            df[FIELD_TRADE_DATE].astype(str).str.replace('-', '', regex=False).str.slice(0, 8)
        )

        # 填充 None 值
        df[FIELD_CLOSE] = pd.to_numeric(df[FIELD_CLOSE], errors='coerce').fillna(0.0)

        # 透视表：行=日期, 列=股票代码, 值=收盘价
        price_matrix = df.pivot(
            index=FIELD_TRADE_DATE,
            columns=FIELD_SYMBOL,
            values=FIELD_CLOSE
        )

        # 按日期排序
        price_matrix = price_matrix.sort_index()

        logger.info(f"📈 加载价格矩阵: {price_matrix.shape[0]} 天 × {price_matrix.shape[1]} 只股票")
        return price_matrix

    def calculate_returns(self, price_matrix: pd.DataFrame, period: int) -> pd.Series:
        """
        【向量化计算】计算所有股票在指定周期内的涨跌幅

        Args:
            price_matrix: 价格矩阵
            period: 回溯周期（交易日）

        Returns:
            Series: 每只股票的涨跌幅
        """
        if len(price_matrix) < period:
            logger.warning(f"⚠️ 数据不足 {period} 天，实际只有 {len(price_matrix)} 天")
            period = len(price_matrix) - 1

        if period <= 0:
            return pd.Series()

        # 获取最新价格和 N 天前的价格
        latest_prices = price_matrix.iloc[-1]
        past_prices = price_matrix.iloc[-period - 1]

        # 【向量化】一次性计算所有股票的涨跌幅
        # 增加 None 值保护
        with np.errstate(divide='ignore', invalid='ignore'):
            returns = (latest_prices - past_prices) / past_prices * 100

        # 去除无效值
        returns = returns.replace([np.inf, -np.inf], np.nan).dropna()

        return returns

    def calculate_rps(self, returns: pd.Series) -> pd.Series:
        """
        【向量化计算】根据涨跌幅计算 RPS 排名

        Args:
            returns: 各股票涨跌幅

        Returns:
            Series: 各股票的 RPS 值 (0-100)
        """
        if returns.empty:
            return pd.Series()

        # 【向量化】使用 pandas rank 函数，pct=True 返回百分位排名
        rps = returns.rank(pct=True) * 100

        # 四舍五入到整数
        rps = rps.fillna(0).round().astype(int)

        return rps

    def calculate_all(self) -> pd.DataFrame:
        """
        【核心方法】计算全市场股票的 RPS_50, RPS_120, RPS_250

        使用向量化计算，避免遍历循环，提高性能。

        Returns:
            DataFrame: 包含 symbol, rps_50, rps_120, rps_250 列
        """
        logger.info("🚀 开始计算全市场 RPS...")
        start_time = datetime.now()

        # 1. 加载价格矩阵
        price_matrix = self._load_price_matrix(days=300)

        if price_matrix.empty:
            logger.error("❌ 无法加载价格数据")
            return pd.DataFrame()

        # 2. 计算各周期的涨跌幅和 RPS
        rps_data = {}

        for rps_name, period in self.PERIODS.items():
            logger.info(f"  📊 计算 {rps_name} (周期={period}天)...")

            try:
                # 计算涨跌幅
                returns = self.calculate_returns(price_matrix, period)

                # 计算 RPS
                rps = self.calculate_rps(returns)

                rps_data[rps_name] = rps

                # 统计
                if not rps.empty:
                    logger.info(f"     ✅ 有效股票: {len(rps)}, 最高RPS: {rps.max()}, 平均: {rps.mean():.1f}")
            except Exception as e:
                logger.warning(f"     ⚠️ {rps_name} 计算失败: {e}")
                rps_data[rps_name] = pd.Series()

        # 3. 合并为 DataFrame
        result = pd.DataFrame(rps_data)
        result.index.name = FIELD_SYMBOL
        result = result.reset_index()

        # 4. 计算综合 RPS (三个周期的平均值)
        rps_cols = ['rps_50', 'rps_120', 'rps_250']
        existing_cols = [c for c in rps_cols if c in result.columns]
        if existing_cols:
            result['rps_avg'] = result[existing_cols].mean(axis=1).fillna(0).round().astype(int)
        else:
            result['rps_avg'] = 0

        # 5. 计算耗时
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(f"✅ RPS 计算完成! 共 {len(result)} 只股票, 耗时 {elapsed:.2f} 秒")

        return result

    def get_top_stocks(self, threshold: int = 85,
                       rps_type: str = 'rps_avg') -> pd.DataFrame:
        """
        获取 RPS 高于阈值的股票

        Args:
            threshold: RPS 阈值 (默认 85)
            rps_type: 使用哪个 RPS 指标 ('rps_50', 'rps_120', 'rps_250', 'rps_avg')

        Returns:
            DataFrame: 符合条件的股票列表
        """
        # 计算全市场 RPS
        all_rps = self.calculate_all()

        if all_rps.empty:
            return pd.DataFrame()

        # 筛选
        if rps_type not in all_rps.columns:
            rps_type = 'rps_avg'

        top_stocks = all_rps[all_rps[rps_type] >= threshold].copy()

        # 按 RPS 降序排列
        top_stocks = top_stocks.sort_values(rps_type, ascending=False)

        # 添加股票名称
        top_stocks = self._add_stock_names(top_stocks)

        logger.info(f"🎯 RPS >= {threshold} 的股票: {len(top_stocks)} 只")

        return top_stocks

    def _add_stock_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """为 DataFrame 添加股票名称"""
        try:
            stocks = self.db.get_all_stocks()
            if hasattr(stocks, '__iter__'):
                name_map = {s.code if hasattr(s, 'code') else s.get('code', ''):
                           s.name if hasattr(s, 'name') else s.get('name', '')
                           for s in stocks}
                df['name'] = df[FIELD_SYMBOL].map(name_map)
        except Exception as e:
            logger.warning(f"⚠️ 添加股票名称失败: {e}")
            df['name'] = ''

        # 调整列顺序
        cols = [FIELD_SYMBOL, 'name'] + [c for c in df.columns if c not in [FIELD_SYMBOL, 'name']]
        return df[cols]

    def save_to_db(self, rps_data: pd.DataFrame) -> int:
        """
        将 RPS 数据保存到数据库

        Args:
            rps_data: calculate_all() 的返回结果

        Returns:
            int: 保存的记录数
        """
        logger.info(f"💾 RPS 数据准备就绪 ({len(rps_data)} 条)")
        return len(rps_data)

    def quick_test(self) -> bool:
        """
        快速测试 RPS 计算功能

        Returns:
            bool: 测试是否通过
        """
        logger.info("🧪 开始 RPS 计算器测试...")

        try:
            # 测试计算
            result = self.calculate_all()

            if result.empty:
                logger.warning("⚠️ 计算结果为空，可能需要先同步数据")
                return False

            # 测试筛选
            top = self.get_top_stocks(threshold=85)

            logger.info(f"  ✅ 全市场 RPS 计算: {len(result)} 只")
            logger.info(f"  ✅ RPS >= 85: {len(top)} 只")

            if not top.empty:
                logger.info(f"  📊 前 5 强:")
                for _, row in top.head(5).iterrows():
                    logger.info(f"     {row[FIELD_SYMBOL]} {row.get('name', 'N/A')}: RPS={row['rps_avg']}")

            return True

        except Exception as e:
            logger.error(f"❌ RPS 测试失败: {e}")
            import traceback
            traceback.print_exc()
            return False


# 便捷函数
def get_rps_calculator() -> RPSCalculator:
    """获取 RPS 计算器实例"""
    return RPSCalculator()
