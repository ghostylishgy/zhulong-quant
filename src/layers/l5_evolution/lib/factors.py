#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
烛龙 v3.2 L5 自进化引擎 - 三体共振因子
============================================================
src/layers/l5_evolution/lib/factors.py

核心职责:
  1. Review (绩效因子): 影子账本净值 Z-Score 偏离度
  2. Tide   (风格因子): 市场特征向量余弦相似度
  3. Echo   (记忆因子): 历史相似场景检索

数学基础:
  Δθ = η_adj · (ω₁·R + ω₂·T + ω₃·E)
============================================================
"""

import math
import logging
import importlib
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Tuple

logger = logging.getLogger('zhulong.l5.factors')
DBGateway = importlib.import_module('01_engine.lib.db_gateway').DBGateway

BASE_DIR = Path('/root/quant_project')
DB_PATH = BASE_DIR / 'storage' / 'database' / 'zhulong.duckdb'


# ==================== 数据结构 ====================

@dataclass
class ReviewFactor:
    """绩效因子输出"""
    z_score: float = 0.0
    pnl_shadow: float = 0.0
    pnl_bench: float = 0.0
    sigma_bench: float = 0.01
    triggered: bool = False    # |Z| >= 1.2 时触发
    detail: str = ""

@dataclass
class TideFactor:
    """风格因子输出"""
    cosine_sim: float = 0.0
    eta_adj: float = 0.01      # 自适应学习率
    drift_warning: bool = False
    current_vector: List[float] = field(default_factory=list)
    detail: str = ""

@dataclass
class EchoFactor:
    """记忆因子输出"""
    similarity: float = 0.0
    matched_scenario: str = ""
    historical_pnl: float = 0.0
    triggered: bool = False    # similarity > 0.8 且曾亏损
    detail: str = ""

@dataclass
class TriResonanceResult:
    """三体共振结果"""
    review: ReviewFactor = field(default_factory=ReviewFactor)
    tide: TideFactor = field(default_factory=TideFactor)
    echo: EchoFactor = field(default_factory=EchoFactor)
    delta_theta: float = 0.0
    eta_adjusted: float = 0.01
    should_evolve: bool = False
    trigger_reason: str = ""


# ==================== 绩效因子 (Review) ====================

class ReviewModule:
    """
    计算影子账本净值曲线与基准曲线的 Z-Score 偏离度

    公式: Factor_Review = (PnL_shadow - PnL_bench) / σ_bench
    触发: |Z| >= 1.2
    """

    Z_TRIGGER = 1.2

    def calculate(self, lookback_days: int = 20) -> ReviewFactor:
        """?? Review ??"""
        result = ReviewFactor()

        try:
            with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
                # ????????
                shadow_returns = self._get_shadow_returns(conn, lookback_days)

                # ?????? (??300)
                bench_returns = self._get_benchmark_returns(conn, lookback_days)

            if len(shadow_returns) < 5 or len(bench_returns) < 5:
                result.detail = f"????: shadow={len(shadow_returns)}, bench={len(bench_returns)}"
                logger.warning(result.detail)
                return result

            # ???? PnL
            result.pnl_shadow = sum(shadow_returns)
            result.pnl_bench = sum(bench_returns)

            # ???????
            mean_bench = sum(bench_returns) / len(bench_returns)
            variance = sum((r - mean_bench) ** 2 for r in bench_returns) / max(len(bench_returns) - 1, 1)
            result.sigma_bench = max(math.sqrt(variance), 0.0001)

            # Z-Score
            result.z_score = (result.pnl_shadow - result.pnl_bench) / result.sigma_bench

            # ????
            result.triggered = abs(result.z_score) >= self.Z_TRIGGER

            result.detail = (
                f"Review Z={result.z_score:.3f} | "
                f"Shadow={result.pnl_shadow:.4f} | "
                f"Bench={result.pnl_bench:.4f} | "
                f"?={result.sigma_bench:.4f} | "
                f"{'?? TRIGGERED' if result.triggered else '? NORMAL'}"
            )

            logger.info(result.detail)

        except Exception as e:
            result.detail = f"Review ????: {e}"
            logger.error(result.detail)

        return result


    def _get_shadow_returns(self, conn, days: int) -> List[float]:
        """从 shadow_metrics 提取影子净值 (用 total_equity 差值计算日收益)"""
        try:
            rows = conn.execute(f"""
                SELECT trade_date, total_equity FROM shadow_metrics
                WHERE trade_date >= CURRENT_DATE - INTERVAL '{days}' DAY
                ORDER BY trade_date ASC
            """).fetchall()
            if len(rows) < 2:
                logger.warning("shadow_metrics 数据不足，回退到 fact_daily 全市场均值")
                return self._fallback_shadow_returns(conn, days)
            equities = [r[1] for r in rows if r[1] is not None and r[1] > 0]
            returns = []
            for i in range(1, len(equities)):
                returns.append((equities[i] - equities[i-1]) / equities[i-1])
            return returns
        except Exception:
            logger.warning("shadow_metrics 表不可用，回退到全市场均值")
            return self._fallback_shadow_returns(conn, days)

    def _fallback_shadow_returns(self, conn, days: int) -> List[float]:
        """回退策略: 用全 A 股当日均涨幅模拟影子收益"""
        try:
            rows = conn.execute(f"""
                SELECT trade_date, AVG(pct_chg) / 100.0 as avg_ret
                FROM fact_daily
                WHERE trade_date >= (SELECT MAX(trade_date) FROM fact_daily) - INTERVAL '{days}' DAY
                GROUP BY trade_date
                ORDER BY trade_date ASC
            """).fetchall()
            return [r[1] for r in rows if r[1] is not None]
        except Exception:
            return [0.0] * days

    def _get_benchmark_returns(self, conn, days: int) -> List[float]:
        """从 fact_daily 提取基准 (优先 000300.SH, 回退全市场均值)"""
        # 尝试 000300.SH
        try:
            rows = conn.execute(f"""
                SELECT pct_chg / 100.0
                FROM fact_daily
                WHERE symbol = '000300.SH'
                AND trade_date >= (SELECT MAX(trade_date) FROM fact_daily) - INTERVAL '{days}' DAY
                ORDER BY trade_date ASC
            """).fetchall()
            if rows:
                return [r[0] for r in rows if r[0] is not None]
        except Exception as e:
            logger.debug(f"000300.SH 查询异常: {e}")

        # 回退: 全市场均值 (与 _fallback_shadow_returns 同逻辑)
        try:
            logger.info("使用全市场均涨幅作为基准")
            rows = conn.execute(f"""
                SELECT trade_date, AVG(pct_chg) / 100.0 as avg_ret
                FROM fact_daily
                WHERE trade_date >= (SELECT MAX(trade_date) FROM fact_daily) - INTERVAL '{days}' DAY
                GROUP BY trade_date
                ORDER BY trade_date ASC
            """).fetchall()
            result = [r[1] for r in rows if r[1] is not None]
            if result:
                logger.info(f"基准数据: {len(result)} 个交易日")
                return result
        except Exception as e:
            logger.warning(f"全市场基准查询异常: {e}")

        logger.warning("所有基准源不可用，使用零收益")
        return [0.0] * days


# ==================== 风格因子 (Tide) ====================

class TideModule:
    """
    计算当前市场特征向量与历史盈利特征库的余弦相似度

    若相似度漂移过大 → 降低学习率 η_adj 防止过拟合
    η_adj = η_base · (1 + 0.2 · tanh(cosine_sim - 0.5))
    """

    ETA_BASE = 0.01
    DRIFT_THRESHOLD = 0.3  # 余弦相似度低于此值视为漂移

    def calculate(self) -> TideFactor:
        """?? Tide ??"""
        result = TideFactor()

        try:
            with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
                # ??????????
                current_vec = self._get_current_market_vector(conn)

                # ???????????
                profit_vec = self._get_profit_period_vector(conn)

            result.current_vector = current_vec

            if not current_vec or not profit_vec:
                result.detail = "??????"
                result.eta_adj = self.ETA_BASE
                return result

            # ?????
            result.cosine_sim = self._cosine_similarity(current_vec, profit_vec)

            # ??????
            result.eta_adj = self.ETA_BASE * (1 + 0.2 * math.tanh(result.cosine_sim - 0.5))

            # ????
            result.drift_warning = result.cosine_sim < self.DRIFT_THRESHOLD

            if result.drift_warning:
                # ??????????
                result.eta_adj *= 0.3

            result.detail = (
                f"Tide cos_sim={result.cosine_sim:.3f} | "
                f"?_adj={result.eta_adj:.5f} | "
                f"{'?? DRIFT' if result.drift_warning else '? ALIGNED'}"
            )

            logger.info(result.detail)

        except Exception as e:
            result.detail = f"Tide ????: {e}"
            result.eta_adj = self.ETA_BASE
            logger.error(result.detail)

        return result


    def _get_current_market_vector(self, conn) -> List[float]:
        """
        提取当前市场特征向量 (使用 HFS 数值列):
        [平均MA20偏离度, 平均涨幅, 平均换手率, 涨跌家数比]
        """
        try:
            # 使用 historical_factor_segments 的数值列
            row = conn.execute("""
                SELECT
                    AVG(ma20_bias) as avg_ma20_bias,
                    AVG(f.pct_chg) as avg_pct,
                    AVG(f.turnover_rate) as avg_turnover,
                    SUM(CASE WHEN f.pct_chg > 0 THEN 1.0 ELSE 0.0 END) /
                    NULLIF(COUNT(*), 0) as advance_ratio
                FROM fact_daily f
                LEFT JOIN historical_factor_segments h
                    ON f.symbol = h.symbol AND f.trade_date = h.trade_date
                WHERE f.trade_date = (SELECT MAX(trade_date) FROM fact_daily)
            """).fetchone()

            if row and row[0] is not None:
                return [float(row[0] or 0), float(row[1] or 0),
                        float(row[2] or 0), float(row[3] or 0)]
        except Exception as e:
            logger.warning(f"当前市场向量提取失败: {e}")

        return [0.5, 0.0, 5.0, 0.5]  # 默认中性

    def _get_profit_period_vector(self, conn) -> List[float]:
        """
        提取历史盈利期特征向量 (使用 HFS 数值列)
        d4_tide_state = 'BULL' 视为盈利期
        """
        try:
            rows = conn.execute("""
                SELECT AVG(ma20_bias), AVG(rps_approx),
                       AVG(vol_ratio)
                FROM historical_factor_segments
                WHERE d4_tide_state = 'BULL'
                AND trade_date >= (SELECT MAX(trade_date) FROM historical_factor_segments) - INTERVAL 60 DAY
            """).fetchone()

            if rows and rows[0] is not None:
                return [float(rows[0] or 0), float(rows[1] or 50) / 100.0,
                        float(rows[2] or 1.0), 0.65]
        except Exception:
            pass

        # 回退: A股牛市典型特征
        return [0.7, 0.015, 8.0, 0.65]

    @staticmethod
    def _cosine_similarity(a: List[float], b: List[float]) -> float:
        """计算余弦相似度"""
        if len(a) != len(b) or not a:
            return 0.0

        dot = sum(x * y for x, y in zip(a, b))
        norm_a = math.sqrt(sum(x ** 2 for x in a))
        norm_b = math.sqrt(sum(x ** 2 for x in b))

        if norm_a == 0 or norm_b == 0:
            return 0.0

        return dot / (norm_a * norm_b)


# ==================== 记忆因子 (Echo) ====================

class EchoModule:
    """
    从 fact_strategic_memory 提取历史相似场景

    预留向量检索接口(为 Node-103 Operator RAG)
    触发: similarity > 0.8 且曾发生重大亏损
    """

    SIMILARITY_THRESHOLD = 0.8
    LOSS_THRESHOLD = -0.03  # 历史场景亏损超过 3% 视为重大

    def calculate(self, current_features: List[float] = None) -> EchoFactor:
        """计算 Echo 因子"""
        result = EchoFactor()

        try:
            with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
                scenarios = self._load_historical_scenarios(conn)

            if not scenarios or not current_features:
                result.detail = "记忆库为空或无当前特征"
                return result

            # 搜索最相似场景
            best_sim = 0.0
            best_scenario = None

            for scenario in scenarios:
                sim = TideModule._cosine_similarity(
                    current_features,
                    scenario.get('features', [])
                )
                if sim > best_sim:
                    best_sim = sim
                    best_scenario = scenario

            result.similarity = best_sim

            if best_scenario:
                result.matched_scenario = best_scenario.get('tag', 'unknown')
                result.historical_pnl = best_scenario.get('pnl', 0.0)

                # 触发: 高相似度 + 曾亏损
                result.triggered = (
                    result.similarity > self.SIMILARITY_THRESHOLD and
                    result.historical_pnl < self.LOSS_THRESHOLD
                )

            result.detail = (
                f"Echo sim={result.similarity:.3f} | "
                f"matched={result.matched_scenario} | "
                f"hist_pnl={result.historical_pnl:.3f} | "
                f"{'🔴 DANGER ECHO' if result.triggered else '⬜ SAFE'}"
            )

            logger.info(result.detail)

        except Exception as e:
            result.detail = f"Echo 计算异常: {e}"
            logger.error(result.detail)

        return result

    def _load_historical_scenarios(self, conn) -> List[Dict]:
        """
        加载历史场景快照
        主源: fact_strategic_memory (ssd_tags, tide_status)
        辅源: historical_factor_segments 聚合
        预留: Node-103 ChromaDB 向量检索
        """
        # 主源: fact_strategic_memory
        try:
            rows = conn.execute("""
                SELECT ssd_tags, tide_status, l4_score,
                       trade_date
                FROM fact_strategic_memory
                ORDER BY created_at DESC
                LIMIT 50
            """).fetchall()

            if rows:
                return [
                    {
                        'tag': r[0] or 'unknown',
                        'pnl': float(r[2] or 0) / 100.0 - 0.5,  # 归一化
                        'features': [
                            1.0 if r[1] == 'BULL' else (0.5 if r[1] == 'CHOPPY' else 0.0),
                            0.5, 0.5, 0.5
                        ]
                    }
                    for r in rows
                ]
        except Exception:
            pass

        # 辅源: HFS 聚合为场景
        try:
            rows = conn.execute("""
                SELECT d4_tide_state, AVG(ma20_bias), AVG(rps_approx),
                       AVG(vol_ratio), trade_date
                FROM historical_factor_segments
                GROUP BY d4_tide_state, trade_date
                ORDER BY trade_date DESC
                LIMIT 50
            """).fetchall()

            return [
                {
                    'tag': r[0] or 'unknown',
                    'pnl': float(r[1] or 0) / 100.0,  # ma20_bias 作为 PnL 代理
                    'features': [float(r[1] or 0), float(r[2] or 50) / 100.0,
                                 float(r[3] or 1.0), 0.5]
                }
                for r in rows
            ]
        except Exception:
            logger.warning("历史场景源均不可用")
            return []

    def vector_search(self, query_vector: List[float], top_k: int = 5) -> List[Dict]:
        """
        预留接口: Node-103 Operator RAG 向量检索
        当 ChromaDB 上线后，此方法将对接真实向量库
        """
        logger.info(f"[预留] 向量检索请求: dim={len(query_vector)}, top_k={top_k}")
        return []


# ==================== 三体共振协调器 ====================

class TriResonanceCalculator:
    """
    三体共振因子协调器

    输出: Δθ = η_adj · (ω₁·R + ω₂·T + ω₃·E)

    权重:
      ω₁ (Review) = 0.5  ← 绩效最重要
      ω₂ (Tide)   = 0.3  ← 风格其次
      ω₃ (Echo)   = 0.2  ← 记忆辅助
    """

    W_REVIEW = 0.5
    W_TIDE = 0.3
    W_ECHO = 0.2

    def __init__(self):
        self.review = ReviewModule()
        self.tide = TideModule()
        self.echo = EchoModule()

    def calculate(self, lookback_days: int = 20) -> TriResonanceResult:
        """执行三体共振计算"""
        result = TriResonanceResult()

        logger.info("=" * 50)
        logger.info("🔬 三体共振因子计算开始")
        logger.info("=" * 50)

        # 1. Review
        result.review = self.review.calculate(lookback_days)

        # 2. Tide
        result.tide = self.tide.calculate()

        # 3. Echo (使用 Tide 的当前向量作为输入)
        result.echo = self.echo.calculate(result.tide.current_vector)

        # 自适应学习率
        result.eta_adjusted = result.tide.eta_adj

        # 三体共振: Δθ = η · (ω₁·R + ω₂·T + ω₃·E)
        r_score = result.review.z_score
        t_score = result.tide.cosine_sim
        e_score = result.echo.similarity

        weighted_sum = (
            self.W_REVIEW * r_score +
            self.W_TIDE * t_score +
            self.W_ECHO * e_score
        )

        result.delta_theta = result.eta_adjusted * weighted_sum

        # 进化触发判定 (任一条件满足)
        triggers = []
        if result.review.triggered:
            triggers.append(f"绩效异常 Z={result.review.z_score:.2f}")
        if result.echo.triggered:
            triggers.append(f"危险记忆重合 sim={result.echo.similarity:.2f}")

        result.should_evolve = len(triggers) > 0
        result.trigger_reason = " + ".join(triggers) if triggers else "无触发"

        logger.info(f"Δθ = {result.delta_theta:.6f}")
        logger.info(f"进化触发: {'🔥 YES' if result.should_evolve else '⬜ NO'}")
        if result.trigger_reason:
            logger.info(f"原因: {result.trigger_reason}")

        return result


# ==================== 入口 ====================

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | [%(name)s] %(message)s',
        datefmt='%H:%M:%S'
    )

    print("=" * 60)
    print("🔬 L5 三体共振因子 - 独立测试")
    print("=" * 60)

    calc = TriResonanceCalculator()
    result = calc.calculate()

    print(f"\n📊 结果汇总:")
    print(f"  Review Z-Score : {result.review.z_score:.4f}")
    print(f"  Tide cos_sim   : {result.tide.cosine_sim:.4f}")
    print(f"  Echo similarity: {result.echo.similarity:.4f}")
    print(f"  η_adjusted     : {result.eta_adjusted:.6f}")
    print(f"  Δθ             : {result.delta_theta:.6f}")
    print(f"  进化触发       : {'🔥 YES' if result.should_evolve else '⬜ NO'}")
    print(f"  触发原因       : {result.trigger_reason}")
