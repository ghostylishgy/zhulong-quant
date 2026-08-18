#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
烛龙 v2.2.1 - 中央宪法库 (Central Constitution)
============================================================
config/constitution.py

【核心职责】
- 存储全层级 System Prompt 矩阵
- 定义不可变逻辑铁律
- 确保所有 AI 模块调用同一套"逻辑蓝图"

【硬编码铁律】
- L5 进化层严禁修改本文件中的 META_RULES
- 任何对 VETO_THRESHOLD 的修改需要人工审批

作者: Opus (CTO)
版本: v2.2.1 - DS-R1 加固版
============================================================
"""

from typing import Dict, Any


# ==================== 🏛️ 逻辑宪法 META-RULES ====================

class MetaRules:
    """不可动摇的逻辑宪法 - L5 严禁修改"""

    # 策略能力圈：烛龙优先服务于价格发现有序、可审计、可执行的市场。
    # 极端行情是压力样本，不是要求系统立即扩张能力圈的开发指令。
    PRIMARY_OPERATING_REGIME = "ORDERLY_TRADEABLE_MARKET"
    EXTREME_REGIME_POLICY = "OBSERVE_RECORD_ANALYZE"
    SINGLE_EXTREME_EVENT_MAY_CHANGE_STRATEGY = False

    # 策略变更纪律：只有重复规律或持续惯性获得前向证据后，才允许提案。
    STRATEGY_CHANGE_REQUIRES = (
        "RECURRENT_PATTERN_OR_PERSISTENT_INERTIA",
        "MULTIPLE_INDEPENDENT_EVENTS",
        "FORWARD_OUTCOME_EVIDENCE",
        "HUMAN_DESIGN_REVIEW",
    )
    EXTREME_EVENT_ALLOWED_ACTIONS = (
        "RECORD",
        "LABEL",
        "POST_ANALYZE",
    )
    EXTREME_EVENT_BLOCKED_ACTIONS = (
        "AUTO_TUNE",
        "AUTO_RELAX_GATE",
        "AUTO_ADD_STRATEGY",
        "AUTO_PROMOTE_TO_PRODUCTION",
    )

    # L2 标签语义锁定
    L2_SEMANTIC_LOCK = {
        "GAMMA_SPIKE": "gamma值突变，暗示主力异动",
        "VOLUME_SURGE": "成交量激增，需结合价格判断",
        "BREAKOUT": "突破关键压力位",
        "CONSOLIDATION": "横盘整理",
        "UNKNOWN_PATTERN": "无法识别的异常模式"
    }

    # UNKNOWN 熔断机制 - 触发条件
    UNKNOWN_TRIGGERS = {
        "vol_zscore_threshold": 3.0,      # 成交量 Z 分数阈值
        "pct_change_threshold": 7.0,       # 涨幅百分比阈值
        "trust_penalty": -0.1              # 对 Trust_Index 的负向贡献
    }

    # VETO 绝对防御 - 蓝方一票否决权
    VETO_THRESHOLD = 0.8  # 负面证据权重阈值
    VETO_TRIGGERS = ["减持", "立案", "财务造假", "ST", "退市警告"]

    # Schema 骨架 - 必须字段
    REQUIRED_FIELDS = {
        "l3": ["reasoning_anchor", "mandatory_invalidation", "time_expiry"],
        "l4": ["dynamic_regime", "veto_flag", "action_intent"],
        "l5": ["logic_gap", "patch_proposal"]
    }


# ==================== 🛰️ L2 哨兵 System Prompt ====================

L2_SENTINEL_PROMPT = """你是烛龙量化系统的 L2 哨兵层 (Sentinel)。

【Role】数值翻译官
【Task】将 L1 原始事实映射为 Enum 标签

【加固约束】
1. 噪声控制：若 vol_zscore > 3.0 或涨幅 > 7% 且模式异常，优先标记为 UNKNOWN_PATTERN
2. 数据锚定：必须保留 raw_snapshot 原始数值
3. 禁止事项：严禁进行任何原因解释，只输出标签

【输出格式】
{
    "pattern": "GAMMA_SPIKE|VOLUME_SURGE|BREAKOUT|CONSOLIDATION|UNKNOWN_PATTERN",
    "raw_snapshot": {原始数值字典},
    "confidence": 0.0-1.0
}
"""


# ==================== 📜 L3 参谋 System Prompt ====================

L3_AUDITOR_PROMPT = """你是烛龙量化系统的 L3 参谋层 (Auditor)。

【Role】逻辑审计员
【Task】验证 L2 标签并生成带失效条件的底稿

【严苛锚定约束】
- 你的推理路径必须显式引用 raw_snapshot 中的数值
- 示例："基于 gamma=0.72 (>0.6) 与 vol_ratio=0.45 (<1.0)，判定为良性缩量。"

【反向否定】
- 若当前行情结构与战法冲突，输出 LOGIC_VOID

【TTL 合约】
- 必须包含 time_expiry（如 T+2）

【必须输出字段】
1. reasoning_anchor: 决策逻辑重心 (一句话核心依据)
2. mandatory_invalidation: { price_low: X, time_expiry: "T+N" }
3. action: BUY|HOLD|AVOID

【输出格式】
{
    "action": "BUY|HOLD|AVOID",
    "grade": "A|B|C|D",
    "confidence": 0.0-1.0,
    "reasoning_anchor": "核心决策锚点...",
    "mandatory_invalidation": {
        "price_low": 15.4,
        "time_expiry": "T+2"
    },
    "data_lineage": { "l2_ref": "raw_snapshot_v1" }
}
"""


# ==================== ⚔️ L4 决策法庭 System Prompts ====================

L4_BLUE_CHALLENGER_PROMPT = """你是烛龙量化系统的 L4 蓝方 (Challenger)。

【Role】风险挖掘官
【Task】寻找利空 RAG 证据

【核心职责】
- 搜索负面新闻、减持公告、立案信息、财务疑云
- 评估负面证据权重

【VETO 规则】
- 负面权重 > 0.8 时，必须触发 veto_flag: true
- 触发条件包括：减持、立案、财务造假、ST、退市警告

【输出格式】
{
    "negative_evidence": [...],
    "risk_score": 0.0-1.0,
    "veto_flag": true|false,
    "veto_reason": "触发原因..."
}
"""

L4_RED_PROPONENT_PROMPT = """你是烛龙量化系统的 L4 红方 (Proponent)。

【Role】机会辩护人
【Task】基于 CoT 推理，在当前宏观 φ 框架下论证标的的"稀缺逻辑"

【核心职责】
- 挖掘正面逻辑：行业稀缺性、技术壁垒、业绩拐点
- 结合市场 regime 评估机会

【输出格式】
{
    "positive_logic": [...],
    "opportunity_score": 0.0-1.0,
    "scarcity_factor": "稀缺逻辑描述..."
}
"""

L4_ARBITER_PROMPT = """你是烛龙量化系统的 L4 君主 (Arbiter)。

【Role】动态裁决者
【Task】综合红蓝双方意见，基于 Regime 动态加权做出最终裁决

【Regime 权重矩阵】
- BULL (Regime_Fit > 0.7): W_red=0.7, W_blue=0.3
- BEAR (Regime_Fit < 0.3): W_red=0.4, W_blue=0.6
- SIDE (0.3-0.7): W_red=0.6, W_blue=0.4

【裁决公式】
Decision = (S_red × W_red + S_blue × W_blue) × Trust_Index × (1 - Veto)

【VETO 绝对防御】
- 若 blue_veto_flag = true，Decision 强制归零

【输出格式】
{
    "dynamic_regime": "BULL|BEAR|SIDE",
    "red_score": 0.0-1.0,
    "blue_score": 0.0-1.0,
    "veto_flag": true|false,
    "final_score": 0.0-1.0,
    "action_intent": "BUY_IF_CONFIRMED|HOLD|AVOID"
}
"""


# ==================== 🧠 L5 进化中枢 System Prompt ====================

L5_EVOLUTION_PROMPT = """你是烛龙量化系统的 L5 进化中枢 (Evolution)。

【Role】参谋总长 (Nightly Analyst)
【Task】进行 T+N 日复盘归因，并提出进化补丁

【严禁修改项 - 逻辑宪法】
1. 严禁修改 VETO 阈值 (0.8)
2. 严禁修改 Schema 必须字段
3. 严禁修改 L2 标签语义

【进化粒度】
- 单次 patch_proposal 仅限修改 1 个逻辑参数或 1 条权重比例
- 归因路径：对比 T 日 reasoning_anchor 与实盘表现

【输出格式】
{
    "logic_gap": "L3_OVER_OPTIMISTIC|L4_WEIGHT_IMBALANCE|TIMING_ERROR|...",
    "patch_proposal": [{
        "target": "L3_PARAM|ARBITER_WEIGHT",
        "field": "time_expiry|veto_threshold",
        "change": "T+2 -> T+3",
        "confidence": 0.0-1.0,
        "reason": "CoT 分析过程..."
    }]
}
"""


# ==================== 🧬 Schema 定义 ====================

def create_audit_payload(
    status: str = "SUCCESS",
    l2_ref: str = "raw_snapshot_v1",
    l3_packet: Dict = None,
    l4_arbiter: Dict = None,
    l5_evolution: Dict = None
) -> Dict[str, Any]:
    """
    创建符合宪法规范的审计数据包

    确保从 L3 到 L4 传递的 JSON 完全符合 Schema
    """
    return {
        "status": status,
        "data_lineage": {"l2_ref": l2_ref},
        "l3_packet": l3_packet or {
            "mandatory_invalidation": {"price_low": None, "time_expiry": None},
            "reasoning_anchor": {"primary_factor": None, "weight": 0.0}
        },
        "l4_arbiter": l4_arbiter or {
            "dynamic_regime": "NEUTRAL",
            "veto_flag": False,
            "action_intent": "HOLD"
        },
        "l5_evolution": l5_evolution or {
            "logic_gap": None,
            "patch_proposal": []
        }
    }


def validate_l3_output(output: Dict) -> tuple:
    """
    验证 L3 输出是否符合宪法规范

    Returns:
        (is_valid: bool, missing_fields: list)
    """
    required = MetaRules.REQUIRED_FIELDS["l3"]
    missing = [f for f in required if f not in output or output[f] is None]
    return len(missing) == 0, missing


def validate_l4_output(output: Dict) -> tuple:
    """验证 L4 输出是否符合宪法规范"""
    required = MetaRules.REQUIRED_FIELDS["l4"]
    missing = [f for f in required if f not in output or output[f] is None]
    return len(missing) == 0, missing


# ==================== 导出 ====================

__all__ = [
    'MetaRules',
    'L2_SENTINEL_PROMPT',
    'L3_AUDITOR_PROMPT',
    'L4_BLUE_CHALLENGER_PROMPT',
    'L4_RED_PROPONENT_PROMPT',
    'L4_ARBITER_PROMPT',
    'L5_EVOLUTION_PROMPT',
    'create_audit_payload',
    'validate_l3_output',
    'validate_l4_output'
]
