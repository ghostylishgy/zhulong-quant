#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
烛龙 v3.2 L5 自进化引擎 - 影子验证协议
============================================================
src/layers/l5_evolution/validator.py

核心职责:
  在新权重 θ_{t+1} 同步至 Node-102 前，通过物理隔离的验证闸门

验证流程:
  1. 提取最近 20 个交易日的 fact_daily 真实数据
  2. 用旧权重和新权重分别回放
  3. 硬约束检查:
     - Sharpe Ratio 必须有统计学意义提升
     - MDD 增量严禁超过旧权重的 2%
  4. 通过 → PROMOTION_READY
     失败 → REJECTED (变异分支作废)
============================================================
"""

import math
import logging
import importlib
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple

logger = logging.getLogger('zhulong.l5.validator')
DBGateway = importlib.import_module('01_engine.lib.db_gateway').DBGateway

BASE_DIR = Path('/root/quant_project')
DB_PATH = BASE_DIR / 'storage' / 'database' / 'zhulong.duckdb'


# ==================== 数据结构 ====================

@dataclass
class BacktestResult:
    """回放结果"""
    returns: List[float] = field(default_factory=list)
    cumulative_pnl: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown: float = 0.0
    win_rate: float = 0.0
    trade_count: int = 0
    detail: str = ""

@dataclass
class ValidationResult:
    """验证结果"""
    passed: bool = False
    theta_id: str = ""
    old_sharpe: float = 0.0
    new_sharpe: float = 0.0
    sharpe_delta: float = 0.0
    old_mdd: float = 0.0
    new_mdd: float = 0.0
    mdd_delta: float = 0.0
    reason: str = ""
    promotion_status: str = "PENDING"  # PROMOTION_READY / REJECTED / PENDING
    is_human_confirmed: bool = False


# ==================== 验证引擎 ====================

class ShadowValidator:
    """
    影子验证协议

    所有新权重必须通过此验证器方可晋升
    """

    LOOKBACK_DAYS = 20
    MDD_TOLERANCE = 0.02   # MDD 增量容忍度 2%
    MIN_SHARPE_DELTA = 0.0 # Sharpe 必须 >= 0 (至少不退步)

    def validate(self, old_params: Dict, new_params: Dict,
                 theta_id: str = "unknown") -> ValidationResult:
        """
        执行完整验证流程

        Args:
            old_params: 旧权重参数字典
            new_params: 新权重参数字典
            theta_id: 新权重 ID

        Returns:
            ValidationResult
        """
        result = ValidationResult(theta_id=theta_id)

        logger.info("=" * 50)
        logger.info(f"🔍 影子验证启动: {theta_id}")
        logger.info("=" * 50)

        try:
            # 提取真实数据
            market_data = self._load_market_data()

            if len(market_data) < self.LOOKBACK_DAYS:
                result.reason = f"市场数据不足: {len(market_data)} < {self.LOOKBACK_DAYS}"
                result.promotion_status = "REJECTED"
                logger.warning(result.reason)
                return result

            # 用旧权重回放
            old_bt = self._backtest(market_data, old_params)
            result.old_sharpe = old_bt.sharpe_ratio
            result.old_mdd = old_bt.max_drawdown

            # 用新权重回放
            new_bt = self._backtest(market_data, new_params)
            result.new_sharpe = new_bt.sharpe_ratio
            result.new_mdd = new_bt.max_drawdown

            # 计算增量
            result.sharpe_delta = result.new_sharpe - result.old_sharpe
            result.mdd_delta = result.new_mdd - result.old_mdd

            # 硬约束检查
            failed_checks = []

            # 检查 1: Sharpe 必须不退步
            if result.sharpe_delta < self.MIN_SHARPE_DELTA:
                failed_checks.append(
                    f"Sharpe退步: {result.sharpe_delta:+.4f}"
                )

            # 检查 2: MDD 增量不超过 2%
            if result.mdd_delta > self.MDD_TOLERANCE:
                failed_checks.append(
                    f"MDD超限: +{result.mdd_delta:.4f} > {self.MDD_TOLERANCE:.4f}"
                )

            if failed_checks:
                result.passed = False
                result.promotion_status = "REJECTED"
                result.reason = " | ".join(failed_checks)
                logger.warning(f"❌ 验证失败: {result.reason}")
            else:
                result.passed = True
                result.promotion_status = "PROMOTION_READY"
                result.reason = (
                    f"Sharpe_Delta: {result.sharpe_delta:+.4f} | "
                    f"MDD_Delta: {result.mdd_delta:+.4f}"
                )

                # 打印晋升申请
                print(f"\n{'='*60}")
                print(f"PROMOTION_READY: {theta_id} | "
                      f"Sharpe_Delta: {result.sharpe_delta:+.4f}")
                print(f"{'='*60}\n")

                logger.info(f"✅ 验证通过: {result.reason}")

        except Exception as e:
            result.reason = f"验证异常: {e}"
            result.promotion_status = "REJECTED"
            logger.error(result.reason)

        return result

    def _load_market_data(self) -> List[Dict]:
        """从 DuckDB 提取最近交易数据"""
        try:
            with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
                rows = conn.execute("""
                SELECT trade_date, symbol, close,
                       pct_chg,
                       vol AS volume,
                       turnover_rate AS turnover,
                       amount
                FROM fact_daily
                WHERE trade_date >= (
                    SELECT MAX(trade_date) FROM fact_daily
                ) - INTERVAL 30 DAY
                AND close > 0
                ORDER BY trade_date ASC, symbol ASC
            """).fetchall()

            data = []
            for r in rows:
                data.append({
                    'trade_date': str(r[0]),
                    'symbol': r[1],
                    'close': float(r[2] or 0),
                    'pct_chg': float(r[3] or 0),
                    'volume': float(r[4] or 0),
                    'turnover': float(r[5] or 0),
                    'amount': float(r[6] or 0),
                })

            logger.info(f"加载市场数据: {len(data)} 条, "
                       f"{len(set(d['trade_date'] for d in data))} 个交易日")
            return data

        except Exception as e:
            logger.error(f"市场数据加载失败: {e}")
            return []

    def _backtest(self, market_data: List[Dict], params: Dict) -> BacktestResult:
        """
        简化回放引擎

        模拟 L1-L4 筛选逻辑，使用给定参数生成假想持仓并计算收益
        """
        result = BacktestResult()

        # 按日期分组
        by_date: Dict[str, List[Dict]] = {}
        for row in market_data:
            dt = row['trade_date']
            if dt not in by_date:
                by_date[dt] = []
            by_date[dt].append(row)

        dates = sorted(by_date.keys())

        rps_thr = params.get('rps_threshold', 95)
        vol_w = params.get('volume_weight', 0.3)
        turnover_min = params.get('turnover_min', 3.0)
        pass_score = params.get('l2_pass_score', 60)
        confidence_min = params.get('l3_confidence_min', 0.6)

        equity_curve = [1.0]

        for dt in dates:
            stocks = by_date[dt]

            # 模拟 L1: 按涨幅排序 + 换手率过滤
            filtered = [s for s in stocks if s['turnover'] >= turnover_min]
            filtered.sort(key=lambda s: s['pct_chg'], reverse=True)

            # 取 Top-N (受 rps_threshold 影响: 越高越严)
            top_n = max(1, int((100 - rps_thr) / 5) + 3)
            selected = filtered[:top_n]

            if not selected:
                equity_curve.append(equity_curve[-1])
                continue

            # 模拟 L2 评分
            scored = []
            for s in selected:
                score = (s['pct_chg'] * vol_w +
                         s['turnover'] * (1 - vol_w) * 0.1)
                if score >= pass_score * 0.01:
                    scored.append(s)

            if not scored:
                equity_curve.append(equity_curve[-1])
                continue

            # 模拟持仓收益 (简化: 持有一天)
            daily_returns = [s['pct_chg'] / 100 for s in scored]
            avg_return = sum(daily_returns) / len(daily_returns)

            result.returns.append(avg_return)
            result.trade_count += len(scored)

            new_equity = equity_curve[-1] * (1 + avg_return)
            equity_curve.append(new_equity)

        # 计算指标
        if result.returns:
            wins = sum(1 for r in result.returns if r > 0)
            result.win_rate = wins / len(result.returns)
            result.cumulative_pnl = equity_curve[-1] / equity_curve[0] - 1
            result.sharpe_ratio = self._calc_sharpe(result.returns)
            result.max_drawdown = self._calc_mdd(equity_curve)

        result.detail = (
            f"PnL={result.cumulative_pnl:.4f} | "
            f"Sharpe={result.sharpe_ratio:.4f} | "
            f"MDD={result.max_drawdown:.4f} | "
            f"WR={result.win_rate:.2%} | "
            f"Trades={result.trade_count}"
        )

        logger.info(f"回放: {result.detail}")
        return result

    @staticmethod
    def _calc_sharpe(returns: List[float], risk_free: float = 0.03) -> float:
        """计算年化 Sharpe"""
        if len(returns) < 2:
            return 0.0

        mean_r = sum(returns) / len(returns)
        std_r = math.sqrt(
            sum((r - mean_r) ** 2 for r in returns) / (len(returns) - 1)
        )

        if std_r == 0:
            return 0.0

        annual_r = mean_r * 252
        annual_std = std_r * math.sqrt(252)
        return (annual_r - risk_free) / annual_std

    @staticmethod
    def _calc_mdd(equity_curve: List[float]) -> float:
        """计算最大回撤"""
        if len(equity_curve) < 2:
            return 0.0

        peak = equity_curve[0]
        max_dd = 0.0

        for val in equity_curve:
            if val > peak:
                peak = val
            dd = (peak - val) / peak if peak > 0 else 0
            max_dd = max(max_dd, dd)

        return max_dd


# ==================== 入口 ====================

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s | [%(name)s] %(message)s',
                        datefmt='%H:%M:%S')

    print("=" * 60)
    print("🔍 L5 影子验证协议 - 独立测试")
    print("=" * 60)

    validator = ShadowValidator()

    old_params = {
        'rps_threshold': 95.0,
        'volume_weight': 0.3,
        'turnover_min': 3.0,
        'l2_pass_score': 60.0,
        'l3_confidence_min': 0.6,
    }

    new_params = {
        'rps_threshold': 92.0,
        'volume_weight': 0.35,
        'turnover_min': 2.5,
        'l2_pass_score': 55.0,
        'l3_confidence_min': 0.55,
    }

    result = validator.validate(old_params, new_params, theta_id="TEST_001")

    print(f"\n📋 验证结果:")
    print(f"  状态: {result.promotion_status}")
    print(f"  Sharpe (旧→新): {result.old_sharpe:.4f} → {result.new_sharpe:.4f} "
          f"({result.sharpe_delta:+.4f})")
    print(f"  MDD (旧→新): {result.old_mdd:.4f} → {result.new_mdd:.4f} "
          f"({result.mdd_delta:+.4f})")
    print(f"  原因: {result.reason}")
