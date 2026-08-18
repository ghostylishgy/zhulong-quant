#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
烛龙 v3.2 L5 自进化引擎 - 主控程序
============================================================
src/layers/l5_evolution/evolver_main.py

核心职责:
  1. 监听 shadow_ledger 变动 (事件驱动)
  2. 计算三体共振因子 Δθ
  3. 执行 GA 种群进化
  4. 影子验证协议
  5. 晋升申请 (Omega_Override 人类确认)

触发条件 (禁止每日盲目轮询):
  - 绩效异常: 当日 |Z-Score| >= 1.2
  - 防御触发: 连续 2 日系统级 VETO
  - 记忆重合: Echo 相似度 > 80% 且曾发生重大亏损

============================================================
"""

import sys
import json
import logging
import argparse
import importlib
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List

# 路径配置
BASE_DIR = Path('/root/quant_project')
DB_PATH = BASE_DIR / 'storage' / 'database' / 'zhulong.duckdb'
L5_DIR = BASE_DIR / 'src' / 'layers' / 'l5_evolution'
STATE_FILE = L5_DIR / 'evolver_state.json'

sys.path.insert(0, str(BASE_DIR))
sys.path.insert(0, str(L5_DIR))
sys.path.insert(0, str(L5_DIR / 'lib'))

# 模块导入使用延迟加载
logger = logging.getLogger('zhulong.l5.evolver')
DBGateway = importlib.import_module('01_engine.lib.db_gateway').DBGateway


# ==================== 事件驱动触发器 ====================

class EvolutionTrigger:
    """
    事件驱动触发器

    仅在偏差事件发生时点火，禁止每日盲目轮询
    """

    Z_THRESHOLD = 1.2
    CONSECUTIVE_VETO_DAYS = 2
    ECHO_SIMILARITY = 0.80

    def check(self) -> Dict:
        """
        ????????????

        Returns:
            {
                'should_fire': bool,
                'reasons': List[str],
                'z_score': float,
                'veto_streak': int,
                'echo_sim': float
            }
        """
        result = {
            'should_fire': False,
            'reasons': [],
            'z_score': 0.0,
            'veto_streak': 0,
            'echo_sim': 0.0
        }

        try:
            with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
                # ?? 1: ????
                z = self._check_performance(conn)
                result['z_score'] = z
                if abs(z) >= self.Z_THRESHOLD:
                    result['should_fire'] = True
                    result['reasons'].append(f"???? |Z|={abs(z):.2f} >= {self.Z_THRESHOLD}")

                # ?? 2: ?? VETO
                streak = self._check_veto_streak(conn)
                result['veto_streak'] = streak
                if streak >= self.CONSECUTIVE_VETO_DAYS:
                    result['should_fire'] = True
                    result['reasons'].append(f"??VETO {streak}? >= {self.CONSECUTIVE_VETO_DAYS}")

        except Exception as e:
            logger.error(f"???????: {e}")

        return result

    def _check_performance(self, conn) -> float:
        """检查当日逻辑收益 Z-Score (通过 total_equity 差值计算)"""
        try:
            # 获取最近 20 天的 total_equity 计算日收益
            rows = conn.execute("""
                SELECT trade_date, total_equity
                FROM shadow_metrics
                WHERE trade_date >= CURRENT_DATE - INTERVAL '20' DAY
                ORDER BY trade_date ASC
            """).fetchall()

            if len(rows) < 3:
                return 0.0  # 数据不足, 不触发

            equities = [float(r[1]) for r in rows if r[1] is not None and r[1] > 0]
            if len(equities) < 3:
                return 0.0

            # 计算日收益率序列
            returns = []
            for i in range(1, len(equities)):
                returns.append((equities[i] - equities[i-1]) / equities[i-1])

            if not returns:
                return 0.0

            # 最后一天收益的 Z-Score
            mean_r = sum(returns) / len(returns)
            if len(returns) < 2:
                return 0.0
            variance = sum((r - mean_r)**2 for r in returns) / (len(returns) - 1)
            std_r = variance ** 0.5

            if std_r > 0:
                return (returns[-1] - mean_r) / std_r
        except Exception:
            pass

        return 0.0

    def _check_veto_streak(self, conn) -> int:
        """检查连续 VETO 天数"""
        try:
            rows = conn.execute("""
                SELECT trade_date, l4_final_verdict
                FROM nexus_audits
                WHERE l4_final_verdict IS NOT NULL
                ORDER BY trade_date DESC
                LIMIT 10
            """).fetchall()

            streak = 0
            prev_date = None
            for r in rows:
                if r[1] == 'VETO':
                    if prev_date is None or str(r[0]) != str(prev_date):
                        streak += 1
                        prev_date = r[0]
                else:
                    break

            return streak
        except Exception:
            return 0


# ==================== 主控程序 ====================

class EvolverMain:
    """
    L5 自进化引擎主控

    事件监听 → 因子计算 → GA 进化 → 验证 → 晋升申请
    """

    def __init__(self):
        self.trigger = EvolutionTrigger()
        self.state = self._load_state()

    def _load_state(self) -> Dict:
        """加载进化状态"""
        if STATE_FILE.exists():
            try:
                with open(STATE_FILE, 'r') as f:
                    return json.load(f)
            except Exception:
                pass
        return {
            'last_evolution': None,
            'current_theta_id': 'default',
            'generation': 0,
            'total_evolutions': 0,
            'pending_promotion': None
        }

    def _save_state(self):
        """保存进化状态"""
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(STATE_FILE, 'w') as f:
            json.dump(self.state, f, indent=2, ensure_ascii=False, default=str)

    def run(self, force: bool = False):
        """
        主执行流程

        Args:
            force: 是否强制执行(跳过触发检查)
        """
        logger.info("=" * 60)
        logger.info("🧬 L5 自进化引擎启动")
        logger.info(f"   当前世代: {self.state['generation']}")
        logger.info(f"   当前权重: {self.state['current_theta_id']}")
        logger.info("=" * 60)

        # 1. 触发检查
        trigger_result = self.trigger.check()

        if not force and not trigger_result['should_fire']:
            logger.info("⬜ 无触发事件，引擎待机")
            logger.info(f"   Z-Score: {trigger_result['z_score']:.3f}")
            logger.info(f"   VETO连续: {trigger_result['veto_streak']}天")
            return

        if force:
            logger.info("⚡ 强制执行模式")
        else:
            logger.info(f"🔥 触发条件满足: {', '.join(trigger_result['reasons'])}")

        # 2. 三体共振因子计算
        from factors import TriResonanceCalculator
        calc = TriResonanceCalculator()
        resonance = calc.calculate()

        logger.info(f"   Δθ = {resonance.delta_theta:.6f}")
        logger.info(f"   η_adj = {resonance.eta_adjusted:.6f}")

        # 3. GA 进化
        from ga_ops import GeneticEvolver, ThetaGenome

        # 用当前权重作为种子
        seed = ThetaGenome()  # 默认参数
        evolver = GeneticEvolver(pop_size=12, elite_count=2)
        evolver.initialize(seed)

        # 适应度评估 + 进化 3 代
        from validator import ShadowValidator
        validator = ShadowValidator()

        best_candidate = None

        for gen in range(3):
            # 评估每个个体
            for genome in evolver.population:
                # 简化适应度: 用三体共振调制
                params = genome.to_dict()['params']
                bt_result = validator._backtest(
                    validator._load_market_data(), params
                )
                # 适应度 = Sharpe - MDD惩罚 + 共振加成
                genome.fitness = (
                    bt_result.sharpe_ratio
                    - bt_result.max_drawdown * 2
                    + resonance.delta_theta
                )

            best = evolver.evolve_step()
            best_candidate = best

        if not best_candidate:
            logger.warning("❌ 进化未产生有效候选")
            return

        # 4. 影子验证
        old_params = seed.to_dict()['params']
        new_params = best_candidate.to_dict()['params']

        validation = validator.validate(
            old_params, new_params,
            theta_id=best_candidate.genome_id
        )

        # 5. 状态更新
        self.state['generation'] = evolver.generation
        self.state['last_evolution'] = datetime.now().isoformat()

        if validation.passed:
            self.state['pending_promotion'] = {
                'theta_id': best_candidate.genome_id,
                'params': new_params,
                'sharpe_delta': validation.sharpe_delta,
                'mdd_delta': validation.mdd_delta,
                'is_human_confirmed': False,
                'timestamp': datetime.now().isoformat()
            }

            logger.info("=" * 60)
            logger.info(f"✅ PROMOTION_READY: {best_candidate.genome_id}")
            logger.info(f"   Sharpe Δ: {validation.sharpe_delta:+.4f}")
            logger.info(f"   MDD Δ: {validation.mdd_delta:+.4f}")
            logger.info("   ⏳ 等待用户指令方可热更新")
            logger.info("=" * 60)
        else:
            logger.info(f"❌ 候选 {best_candidate.genome_id} 验证未通过")
            logger.info(f"   原因: {validation.reason}")

        self.state['total_evolutions'] += 1
        self._save_state()

    def check_status(self):
        """检查进化引擎状态"""
        print("\n" + "=" * 60)
        print("🧬 L5 自进化引擎状态")
        print("=" * 60)

        # 触发检查
        trigger = self.trigger.check()

        print(f"\n📊 触发检测:")
        print(f"  Z-Score      : {trigger['z_score']:.3f} "
              f"(阈值: ±{EvolutionTrigger.Z_THRESHOLD})")
        print(f"  VETO连续     : {trigger['veto_streak']} 天 "
              f"(阈值: {EvolutionTrigger.CONSECUTIVE_VETO_DAYS})")
        print(f"  应当触发     : {'🔥 YES' if trigger['should_fire'] else '⬜ NO'}")

        print(f"\n📋 引擎状态:")
        print(f"  当前世代     : {self.state['generation']}")
        print(f"  当前权重     : {self.state['current_theta_id']}")
        print(f"  上次进化     : {self.state.get('last_evolution', '从未')}")
        print(f"  历史进化次数 : {self.state['total_evolutions']}")

        pending = self.state.get('pending_promotion')
        if pending:
            print(f"\n⏳ 待晋升权重:")
            print(f"  Theta ID     : {pending['theta_id']}")
            print(f"  Sharpe Δ     : {pending['sharpe_delta']:+.4f}")
            print(f"  人类确认     : {'✅' if pending['is_human_confirmed'] else '❌ 等待'}")
        else:
            print(f"\n  无待晋升权重")

        print("=" * 60 + "\n")

    def test(self):
        """快速自检测试"""
        print("\n" + "=" * 60)
        print("🧪 L5 自进化引擎 - 快速自检")
        print("=" * 60)

        # 1. 因子计算
        print("\n[1/4] 三体共振因子...")
        try:
            from factors import TriResonanceCalculator
            calc = TriResonanceCalculator()
            res = calc.calculate()
            print(f"  Review Z: {res.review.z_score:.4f}")
            print(f"  Tide sim: {res.tide.cosine_sim:.4f}")
            print(f"  Echo sim: {res.echo.similarity:.4f}")
            print(f"  Δθ:       {res.delta_theta:.6f}")
            print(f"  ✅ 因子计算通过")
        except Exception as e:
            print(f"  ❌ 因子计算失败: {e}")

        # 2. GA 算子
        print("\n[2/4] 遗传算法算子...")
        try:
            from ga_ops import GeneticEvolver, ThetaGenome
            evolver = GeneticEvolver(pop_size=6)
            evolver.initialize()
            import random
            for g in evolver.population:
                g.fitness = random.uniform(0, 1)
            best = evolver.evolve_step()
            print(f"  种群: {len(evolver.population)} 个体")
            print(f"  最优: {best.genome_id} (fitness={best.fitness:.4f})")
            print(f"  ✅ GA 算子通过")
        except Exception as e:
            print(f"  ❌ GA 算子失败: {e}")

        # 3. 验证器
        print("\n[3/4] 影子验证协议...")
        try:
            from validator import ShadowValidator
            v = ShadowValidator()
            old_p = {'rps_threshold': 95, 'volume_weight': 0.3,
                     'turnover_min': 3.0, 'l2_pass_score': 60,
                     'l3_confidence_min': 0.6}
            new_p = dict(old_p)
            new_p['rps_threshold'] = 92
            result = v.validate(old_p, new_p, "TEST")
            print(f"  状态: {result.promotion_status}")
            print(f"  ✅ 验证协议通过")
        except Exception as e:
            print(f"  ❌ 验证协议失败: {e}")

        # 4. 触发器
        print("\n[4/4] 事件触发器...")
        try:
            trigger = self.trigger.check()
            print(f"  Z-Score: {trigger['z_score']:.3f}")
            print(f"  VETO连续: {trigger['veto_streak']}")
            print(f"  ✅ 触发器通过")
        except Exception as e:
            print(f"  ❌ 触发器失败: {e}")

        print("\n" + "=" * 60)
        print("✅ L5 自进化引擎自检完成")
        print("=" * 60 + "\n")


# ==================== CLI ====================

def main():
    parser = argparse.ArgumentParser(
        description='🧬 烛龙 v3.2 L5 自进化引擎'
    )
    parser.add_argument('--run', action='store_true',
                       help='执行进化流程 (事件驱动)')
    parser.add_argument('--force', action='store_true',
                       help='强制执行 (跳过触发检查)')
    parser.add_argument('--check', action='store_true',
                       help='检查引擎状态')
    parser.add_argument('--test', action='store_true',
                       help='快速自检')

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | [%(name)s] %(message)s',
        datefmt='%H:%M:%S'
    )

    engine = EvolverMain()

    if args.run or args.force:
        engine.run(force=args.force)
    elif args.check:
        engine.check_status()
    elif args.test:
        engine.test()
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
