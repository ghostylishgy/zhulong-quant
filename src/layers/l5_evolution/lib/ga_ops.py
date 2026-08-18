#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
烛龙 v3.2 L5 自进化引擎 - 遗传算法算子
============================================================
src/layers/l5_evolution/lib/ga_ops.py

重构自: archive/legacy_project/maintenance/evolution_engine.py
         archive/legacy_project/governance/shadow_evolver.py

核心算子:
  1. 基因组编码 (Genome Encoding)
  2. 交叉 (Crossover)
  3. 变异 (Mutation)
  4. 选择 (Selection)
  5. 种群进化循环
============================================================
"""

import math
import random
import hashlib
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Optional
from datetime import datetime

logger = logging.getLogger('zhulong.l5.ga')

# 随机种子固定 (可复现)
random.seed(42)


# ==================== 权重基因组 ====================

@dataclass
class ThetaGenome:
    """
    权重基因组 - 映射到 L1-L4 逻辑参数

    每个基因对应一个可调节的系统参数
    """
    # L1 参数
    rps_threshold: float = 95.0       # RPS 精英门槛 [80, 99]
    volume_weight: float = 0.3        # 量价权重 [0.1, 0.8]
    turnover_min: float = 3.0         # 最低换手率 [1.0, 15.0]

    # L2 参数
    l2_pass_score: float = 60.0       # L2 通过分 [40, 80]
    momentum_weight: float = 0.4      # 动量权重 [0.1, 0.8]

    # L3 参数
    l3_confidence_min: float = 0.6    # L3 最低置信度 [0.3, 0.9]
    falsification_weight: float = 0.3 # 证伪权重 [0.1, 0.6]

    # L4 参数
    l4_red_weight: float = 0.5        # L4 红方权重 [0.2, 0.8]
    l4_blue_weight: float = 0.5       # L4 蓝方权重 [0.2, 0.8]
    veto_threshold: float = 0.4       # 否决阈值 [0.2, 0.7]

    # 元数据
    genome_id: str = ""
    generation: int = 0
    fitness: float = 0.0
    parent_ids: List[str] = field(default_factory=list)
    created_at: str = ""

    def __post_init__(self):
        if not self.genome_id:
            self.genome_id = self._generate_id()
        if not self.created_at:
            self.created_at = datetime.now().isoformat()

    def _generate_id(self) -> str:
        """生成基因组 ID"""
        content = f"{self.rps_threshold:.2f}_{self.volume_weight:.4f}_{self.l2_pass_score:.2f}"
        return f"G_{hashlib.sha1(content.encode()).hexdigest()[:8]}"

    def to_vector(self) -> List[float]:
        """转换为浮点向量"""
        return [
            self.rps_threshold, self.volume_weight, self.turnover_min,
            self.l2_pass_score, self.momentum_weight,
            self.l3_confidence_min, self.falsification_weight,
            self.l4_red_weight, self.l4_blue_weight, self.veto_threshold
        ]

    @classmethod
    def from_vector(cls, vec: List[float], generation: int = 0,
                    parent_ids: List[str] = None) -> 'ThetaGenome':
        """从浮点向量构建"""
        g = cls(
            rps_threshold=vec[0], volume_weight=vec[1], turnover_min=vec[2],
            l2_pass_score=vec[3], momentum_weight=vec[4],
            l3_confidence_min=vec[5], falsification_weight=vec[6],
            l4_red_weight=vec[7], l4_blue_weight=vec[8], veto_threshold=vec[9],
            generation=generation,
            parent_ids=parent_ids or []
        )
        return g

    def to_dict(self) -> Dict:
        return {
            'genome_id': self.genome_id,
            'generation': self.generation,
            'fitness': round(self.fitness, 4),
            'params': {
                'rps_threshold': self.rps_threshold,
                'volume_weight': self.volume_weight,
                'turnover_min': self.turnover_min,
                'l2_pass_score': self.l2_pass_score,
                'momentum_weight': self.momentum_weight,
                'l3_confidence_min': self.l3_confidence_min,
                'falsification_weight': self.falsification_weight,
                'l4_red_weight': self.l4_red_weight,
                'l4_blue_weight': self.l4_blue_weight,
                'veto_threshold': self.veto_threshold
            },
            'parent_ids': self.parent_ids,
            'created_at': self.created_at
        }


# ==================== 基因范围约束 ====================

GENE_BOUNDS = {
    'rps_threshold':       (80.0, 99.0),
    'volume_weight':       (0.1, 0.8),
    'turnover_min':        (1.0, 15.0),
    'l2_pass_score':       (40.0, 80.0),
    'momentum_weight':     (0.1, 0.8),
    'l3_confidence_min':   (0.3, 0.9),
    'falsification_weight':(0.1, 0.6),
    'l4_red_weight':       (0.2, 0.8),
    'l4_blue_weight':      (0.2, 0.8),
    'veto_threshold':      (0.2, 0.7),
}


def clamp_genome(genome: ThetaGenome) -> ThetaGenome:
    """将基因组参数约束在合法范围内"""
    for gene_name, (lo, hi) in GENE_BOUNDS.items():
        val = getattr(genome, gene_name)
        setattr(genome, gene_name, max(lo, min(hi, val)))

    # L4 红蓝权重归一化
    total = genome.l4_red_weight + genome.l4_blue_weight
    if total > 0:
        genome.l4_red_weight /= total
        genome.l4_blue_weight /= total

    return genome


# ==================== 遗传算子 ====================

def crossover(parent_a: ThetaGenome, parent_b: ThetaGenome,
              crossover_rate: float = 0.7) -> Tuple[ThetaGenome, ThetaGenome]:
    """
    双点交叉

    以 crossover_rate 概率执行交叉，否则直接复制
    """
    vec_a = parent_a.to_vector()
    vec_b = parent_b.to_vector()
    n = len(vec_a)

    if random.random() > crossover_rate:
        # 不交叉，直接复制
        child_a = ThetaGenome.from_vector(vec_a[:], parent_a.generation + 1,
                                           [parent_a.genome_id])
        child_b = ThetaGenome.from_vector(vec_b[:], parent_b.generation + 1,
                                           [parent_b.genome_id])
        return child_a, child_b

    # 双点交叉
    pt1, pt2 = sorted(random.sample(range(n), 2))

    child_vec_a = vec_a[:pt1] + vec_b[pt1:pt2] + vec_a[pt2:]
    child_vec_b = vec_b[:pt1] + vec_a[pt1:pt2] + vec_b[pt2:]

    gen = max(parent_a.generation, parent_b.generation) + 1
    parents = [parent_a.genome_id, parent_b.genome_id]

    child_a = ThetaGenome.from_vector(child_vec_a, gen, parents)
    child_b = ThetaGenome.from_vector(child_vec_b, gen, parents)

    return clamp_genome(child_a), clamp_genome(child_b)


def mutate(genome: ThetaGenome, mutation_rate: float = 0.1,
           mutation_strength: float = 0.05) -> ThetaGenome:
    """
    高斯变异

    对每个基因以 mutation_rate 概率施加 N(0, σ) 扰动
    σ = (bound_hi - bound_lo) * mutation_strength
    """
    vec = genome.to_vector()
    bounds = list(GENE_BOUNDS.values())

    mutated = False
    for i in range(len(vec)):
        if random.random() < mutation_rate:
            lo, hi = bounds[i]
            sigma = (hi - lo) * mutation_strength
            vec[i] += random.gauss(0, sigma)
            mutated = True

    child = ThetaGenome.from_vector(vec, genome.generation, genome.parent_ids[:])
    child = clamp_genome(child)

    if mutated:
        child.genome_id = child._generate_id()
        logger.debug(f"变异: {genome.genome_id} -> {child.genome_id}")

    return child


def tournament_select(population: List[ThetaGenome],
                      tournament_size: int = 3) -> ThetaGenome:
    """
    锦标赛选择

    从种群中随机选择 tournament_size 个个体，返回适应度最高者
    """
    candidates = random.sample(population, min(tournament_size, len(population)))
    return max(candidates, key=lambda g: g.fitness)


def elitism_select(population: List[ThetaGenome],
                   elite_count: int = 2) -> List[ThetaGenome]:
    """精英保留: 保留适应度最高的 elite_count 个个体"""
    sorted_pop = sorted(population, key=lambda g: g.fitness, reverse=True)
    return [ThetaGenome.from_vector(g.to_vector(), g.generation, g.parent_ids[:])
            for g in sorted_pop[:elite_count]]


# ==================== 种群进化 ====================

class GeneticEvolver:
    """
    种群进化管理器

    执行完整的遗传算法循环:
    1. 初始化种群
    2. 适应度评估 (由外部回调提供)
    3. 选择 → 交叉 → 变异
    4. 精英保留
    5. 输出最优个体
    """

    def __init__(self, pop_size: int = 12, elite_count: int = 2,
                 crossover_rate: float = 0.7, mutation_rate: float = 0.15,
                 mutation_strength: float = 0.05):
        self.pop_size = pop_size
        self.elite_count = elite_count
        self.crossover_rate = crossover_rate
        self.mutation_rate = mutation_rate
        self.mutation_strength = mutation_strength
        self.population: List[ThetaGenome] = []
        self.best_ever: Optional[ThetaGenome] = None
        self.generation = 0

    def initialize(self, seed_genome: ThetaGenome = None):
        """初始化种群"""
        self.population = []

        if seed_genome:
            self.population.append(seed_genome)

        # 填充剩余个体 (随机初始化)
        while len(self.population) < self.pop_size:
            vec = []
            for _, (lo, hi) in GENE_BOUNDS.items():
                vec.append(random.uniform(lo, hi))
            genome = ThetaGenome.from_vector(vec, generation=0)
            genome = clamp_genome(genome)
            self.population.append(genome)

        logger.info(f"种群初始化: {self.pop_size} 个个体")

    def evolve_step(self) -> ThetaGenome:
        """
        执行一代进化

        前提: 种群中每个个体的 fitness 已由外部评估器设置

        Returns:
            当前最优个体
        """
        self.generation += 1

        # 精英保留
        elites = elitism_select(self.population, self.elite_count)

        # 更新全局最优
        current_best = max(self.population, key=lambda g: g.fitness)
        if self.best_ever is None or current_best.fitness > self.best_ever.fitness:
            self.best_ever = current_best

        # 生育下一代
        offspring = list(elites)

        while len(offspring) < self.pop_size:
            # 选择
            parent_a = tournament_select(self.population)
            parent_b = tournament_select(self.population)

            # 交叉
            child_a, child_b = crossover(parent_a, parent_b, self.crossover_rate)

            # 变异
            child_a = mutate(child_a, self.mutation_rate, self.mutation_strength)
            child_b = mutate(child_b, self.mutation_rate, self.mutation_strength)

            child_a.generation = self.generation
            child_b.generation = self.generation

            offspring.append(child_a)
            if len(offspring) < self.pop_size:
                offspring.append(child_b)

        self.population = offspring[:self.pop_size]

        logger.info(
            f"🧬 Gen {self.generation} | "
            f"Best={current_best.fitness:.4f} ({current_best.genome_id}) | "
            f"HistBest={self.best_ever.fitness:.4f}"
        )

        return current_best

    def get_best(self) -> Optional[ThetaGenome]:
        """获取当前种群最优"""
        if not self.population:
            return None
        return max(self.population, key=lambda g: g.fitness)


# ==================== 入口 ====================

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s | [%(name)s] %(message)s',
                        datefmt='%H:%M:%S')

    print("=" * 60)
    print("🧬 L5 遗传算法算子 - 独立测试")
    print("=" * 60)

    evolver = GeneticEvolver(pop_size=8)
    evolver.initialize()

    # 模拟 3 代进化 (随机适应度)
    for gen in range(3):
        for g in evolver.population:
            g.fitness = random.uniform(0, 1)

        best = evolver.evolve_step()
        print(f"  Gen {gen+1} best: {best.genome_id} fitness={best.fitness:.4f}")

    print(f"\n🏆 历史最优: {evolver.best_ever.genome_id} "
          f"fitness={evolver.best_ever.fitness:.4f}")
    print(f"   参数: {evolver.best_ever.to_dict()['params']}")
