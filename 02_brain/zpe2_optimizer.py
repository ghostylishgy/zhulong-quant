#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
02_brain/zpe2_optimizer.py
==========================
ZPE-2 (Zhulong Prompt Engineering v2) — 三合一优化模块
Module A: PromptDehydrator  — Prompt 脱水压缩
Module B: SafetyGate        — 异常值安全闸门
Module C: EngineConfig      — Ollama 性能对齐
"""

import os
import re
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional

logger = logging.getLogger('zhulong.zpe2')

# ═══════════════════════════════════════════════════════
# Module C: Engine Config — Ollama 性能对齐
# ═══════════════════════════════════════════════════════

def get_l3_options() -> dict:
    """N100 物理核心对齐参数"""
    return {
        "num_batch": 256,       # Prefill 批大小, 提升吞吐
        "num_thread": 4,        # 对齐 N100 四核, 严禁超频
        "num_ctx": 2048,        # 锁定上下文窗口
        "temperature": 0.3,     # 低温精准输出
        "num_predict": int(os.getenv("L3_NUM_PREDICT", "4096")),    # 最大生成长度 (N150 night whitelist)
    }

# ZPE-2 方言词典 (注入 System Prompt, ~50 tokens 固定成本)
ZPE2_DIALECT = (
    "KV编码词典: "
    "D/A=Debt-to-Assets%, ROE=净资产收益率%, Z=Altman Z-Score, "
    "MARGIN_SURGE=融资激增, INST_ACC=机构增仓, HOT_MONEY=游资介入, "
    "LHB_NET=龙虎榜净额(万), VOL_SPIKE=量能突增, TRAP=多头陷阱"
)

ZPE2_SYSTEM_PROMPT = (
    "你是本地门控哨兵(L3)。任务：只根据用户消息中实际出现的字段，对L2传感器标签做逻辑证伪。\n"
    "未出现的财务、技术指标、行业、新闻、机构、游资或融资数据一律视为UNKNOWN，禁止引用或补零。\n"
    "RPS是相对强度排名，不是交易信号；VR是量比，不是资金流；FIN:N/A和RAG缺失都不是独立硬风险。\n"
    "当前是候选审计，不得输出买入、卖出、持有、退出、建仓或加减仓建议。\n"
    "MUST: Output ONLY strict JSON. No markdown, no explanation.\n"
    "Field verdict MUST be exactly one of: PASS, HOLD, VETO.\n"
    "Keep reasoning <= 600 Chinese chars; put verdict/audit_score before reasoning.\n"
    "audit_score是质量分且越高越安全：PASS=55-100，HOLD=35-54，VETO=0-34。\n"
    'Schema: {"verdict":"PASS|HOLD|VETO","audit_score":<integer 0-100>,"risk_level":"LOW|MEDIUM|HIGH","falsifiable_conditions":[],"reasoning":"text"}'
)

# ═══════════════════════════════════════════════════════
# Module A: Prompt Dehydrator — Prompt 脱水压缩
# ═══════════════════════════════════════════════════════

# 语义原子化映射表
_SEMANTIC_ATOMS = {
    '融资激增': 'MARGIN_SURGE', '融资余额': 'MARGIN_BAL',
    '机构增仓': 'INST_ACC', '机构减仓': 'INST_DEC',
    '游资介入': 'HOT_MONEY', '龙虎榜': 'LHB',
    '涨停': 'LIMIT_UP', '跌停': 'LIMIT_DOWN',
    '量能突增': 'VOL_SPIKE', '缩量': 'VOL_SHRINK',
    '突破压力位': 'BREAKOUT', '跌破支撑': 'BREAKDOWN',
    '金叉': 'GOLDEN_X', '死叉': 'DEATH_X',
    '多头陷阱': 'TRAP', '量价背离': 'VOL_DIVERGE',
    '主力流入': 'MAIN_INFLOW', '主力流出': 'MAIN_OUTFLOW',
    '高位放量': 'HIGH_VOL', '低位缩量': 'LOW_VOL',
    '连续上涨': 'STREAK_UP', '连续下跌': 'STREAK_DOWN',
    '大宗交易': 'BLOCK_TRADE', '股东减持': 'INSIDER_SELL',
    '形态特征': 'PATTERN', '多空逻辑': 'BULL_BEAR',
    '极端风险': 'EXTREME_RISK',
}

# 财务因子 KV 映射
_FINA_KEYS = {
    'debt_to_assets': 'D/A',
    'ebit_of_gr': 'EBIT/GR',
    'op_income_of_gr': 'OI/GR',
    'quick_ratio': 'QR',
    'roe': 'ROE',
    'z_score': 'Z',
}


class PromptDehydrator:
    """将冗长的 L2 Report + fina_factors 压缩为 KV 编码"""

    def dehydrate_candidate(self, candidate) -> str:
        """候选标的 -> 紧凑 KV 行 (~80 chars)"""
        return (
            f"SYM:{candidate.symbol} "
            f"C:{candidate.close:.2f} "
            f"PCT:{candidate.pct_chg:+.2f}% "
            f"TO:{candidate.turnover:.2f}% "
            f"VR:{candidate.vol_ratio:.2f} "
            f"RPS:{candidate.rps_10:.1f}"
        )

    def dehydrate_l2(self, l2_result) -> str:
        """L2 结果 -> 压缩标签行 + 语义摘要 (~160 chars)"""
        tags = l2_result.fact_tags if l2_result.fact_tags else []
        compact_tags = []
        for tag in tags[:5]:
            atom = tag
            for cn, en in _SEMANTIC_ATOMS.items():
                if cn in tag:
                    atom = en
                    break
            compact_tags.append(atom)
        tags_str = ",".join(compact_tags) if compact_tags else "NONE"

        raw_reason = getattr(l2_result, 'detailed_reasoning', '') or getattr(l2_result, 'thinking_trace', '')
        reason = raw_reason.strip().replace("\n", " ")
        for cn, en in _SEMANTIC_ATOMS.items():
            reason = reason.replace(cn, en)
        reason = reason[:120]
        reason_part = f"|RSN:{reason}" if reason else ""

        return f"L2:{l2_result.pattern[:20]}|R:{l2_result.risk_score}|T:[{tags_str}]{reason_part}"

    def dehydrate_fina(self, fina_factors: Optional[dict]) -> str:
        """Altman Z 五因子 -> KV 行 (~50 chars)"""
        if not fina_factors or fina_factors.get('fina_source') == 'MISSING':
            return "FIN:N/A"
        parts = []
        for raw_key, short_key in _FINA_KEYS.items():
            val = fina_factors.get(raw_key)
            if val is not None:
                try:
                    parts.append(f"{short_key}:{float(val):.1f}")
                except Exception as e:
                    logger.error(f'Critical Logical Gap: {str(e)}', exc_info=True)
        return "FIN:" + "|".join(parts) if parts else "FIN:N/A"

    def compress_rag(self, intel_summary: str, max_chars: int = 100) -> str:
        """RAG 情报 -> 关键词摘要 (~100 chars)"""
        if not intel_summary:
            return ""
        # 语义原子化替换
        compressed = intel_summary
        for cn, en in _SEMANTIC_ATOMS.items():
            compressed = compressed.replace(cn, en)
        # 截断到 max_chars
        if len(compressed) > max_chars:
            compressed = compressed[:max_chars] + "..."
        return f"RAG:{compressed}"


    def dehydrate_zeta(self, candidate) -> str:
        """Zeta资金信号 -> 紧凑 KV 行 (仅输出非零字段)"""
        parts = []
        lhb = getattr(candidate, "zeta_lhb_net", 0.0) or 0.0
        inst = getattr(candidate, "zeta_inst_buy", 0) or 0
        hot = getattr(candidate, "zeta_hot_money", 0) or 0
        marg = getattr(candidate, "zeta_margin_delta", 0.0) or 0.0
        if abs(lhb) > 1e5:
            parts.append(f"LHB={lhb/1e8:.2f}亿")
        if inst != 0:
            parts.append(f"INST={inst:+d}")
        if hot != 0:
            parts.append(f"HOT={hot:+d}")
        if abs(marg) > 1e5:
            parts.append(f"MARG={marg/1e8:.2f}亿")
        if not parts:
            return ""
        return "ZETA:" + " ".join(parts)

    def dehydrate_poc(self, poc_data: dict) -> str:
        """POC 筹码密集区 → 全精度 KV 行 (Rabbit 协议, 不截断)"""
        if not poc_data:
            return ""
        return (
            f"POC:{poc_data.get('poc_price', 0):.2f}"
            f"|LO:{poc_data.get('poc_lower', 0):.2f}"
            f"|HI:{poc_data.get('poc_upper', 0):.2f}"
            f"|S:{poc_data.get('support', 0):.2f}"
            f"|ATR:{poc_data.get('atr_14', 0):.3f}"
            f"|DIST:{poc_data.get('distance_pct', 0):+.1f}%"
        )

    def build_l3_prompt(self, candidate, l2_result, intel_summary: str = "",
                        fina_factors: dict = None, poc_data: dict = None) -> str:
        """构建 ZPE-2 脱水 L3 Prompt (目标 < 600 tokens)"""
        lines = [
            self.dehydrate_candidate(candidate),
            self.dehydrate_l2(l2_result),
            self.dehydrate_fina(fina_factors),
        ]
        # POC 全精度行 (Rabbit)
        poc_line = self.dehydrate_poc(poc_data) if poc_data else ""
        if poc_line:
            lines.append(poc_line)

        rag_line = self.compress_rag(intel_summary)
        if rag_line:
            lines.append(rag_line)

        zeta_line = self.dehydrate_zeta(candidate)
        if zeta_line:
            lines.append(zeta_line)

        lines.append(
            "EVIDENCE_SCOPE:仅使用以上字段;缺失项=UNKNOWN;禁止补齐MACD/RSI/均线/财报/行业/新闻"
        )
        lines.append(
            "TASK:1.识别多头陷阱/量价背离 "
            "2.三态判定(PASS/HOLD/VETO) "
            "3.输出JSON{verdict,audit_score(0-100),reasoning,falsifiable_conditions[]}"
        )
        return "\n".join(lines)


# ═══════════════════════════════════════════════════════
# Module B: Safety Gate — 异常值安全闸门
# ═══════════════════════════════════════════════════════

@dataclass
class GateResult:
    color: str = "GREEN"         # GREEN / RED
    alerts: List[str] = field(default_factory=list)
    raw_snippets: List[str] = field(default_factory=list)

    @property
    def is_red(self) -> bool:
        return self.color == "RED"

    def inject_alerts(self, prompt: str) -> str:
        """Red 通道: 将异常因子原始片段压入 Prompt 头部"""
        if not self.alerts:
            return prompt
        alert_block = "RED_GATE:" + "|".join(self.alerts)
        if self.raw_snippets:
            alert_block += "\nRAW:" + " ".join(s[:80] for s in self.raw_snippets[:2])
        return alert_block + "\n" + prompt


class SafetyGate:
    """静态红线检测器 - 在发送 Ollama 前拦截"""

    # 红线阈值
    Z_SCORE_THRESHOLD = 1.8        # 破产预警
    MARGIN_INCREASE_THRESHOLD = 20  # 杠杆风险 (%)

    def evaluate(self, candidate=None, fina_factors: dict = None,
                 zeta_data=None, rag_intel: str = "") -> GateResult:
        """执行三重红线检测"""
        result = GateResult()

        # --- 检测 1: Altman Z-Score 破产预警 ---
        if fina_factors and isinstance(fina_factors, dict):
            z_score = fina_factors.get('z_score')
            if z_score is not None:
                try:
                    z_val = float(z_score)
                    if z_val < self.Z_SCORE_THRESHOLD:
                        result.color = "RED"
                        result.alerts.append(f"Z_DISTRESS:{z_val:.2f}<{self.Z_SCORE_THRESHOLD}")
                        logger.warning(f"SafetyGate RED: Z-Score {z_val:.2f} < {self.Z_SCORE_THRESHOLD}")
                except Exception as e:
                    logger.error(f'Critical Logical Gap: {str(e)}', exc_info=True)
            roe_trend = fina_factors.get('roe_trend')
            if roe_trend is not None:
                try:
                    if float(roe_trend) < 0:
                        result.color = "RED"
                        result.alerts.append(f"ROE_DECLINE:trend={roe_trend}")
                except Exception as e:
                    logger.error(f'Critical Logical Gap: {str(e)}', exc_info=True)
            roe_vals = []
            for k in ['roe_q1', 'roe_q2', 'roe_q3']:
                v = fina_factors.get(k)
                if v is not None:
                    try:
                        roe_vals.append(float(v))
                    except Exception as e:
                        logger.error(f'Critical Logical Gap: {str(e)}', exc_info=True)
            if len(roe_vals) >= 3:
                if all(roe_vals[i] > roe_vals[i+1] for i in range(len(roe_vals)-1)):
                    result.color = "RED"
                    result.alerts.append(f"ROE_3Q_DECLINE:{roe_vals}")

        # --- 检测 3: 融资盘激增 > 20% ---
        if zeta_data:
            rzmre = getattr(zeta_data, 'rzmre', 0) or 0
            rzye = getattr(zeta_data, 'rzye', 0) or 0
            if rzye > 0 and rzmre > 0:
                try:
                    margin_pct = (float(rzmre) / float(rzye)) * 100
                    if margin_pct > self.MARGIN_INCREASE_THRESHOLD:
                        result.color = "RED"
                        result.alerts.append(f"MARGIN_SURGE:{margin_pct:.1f}%>{self.MARGIN_INCREASE_THRESHOLD}%")
                except Exception as e:
                    logger.error(f'Critical Logical Gap: {str(e)}', exc_info=True)
        if result.is_red and rag_intel:
            keyword_map = {
                'Z_DISTRESS': ['破产', '资不抵债', '偿债'],
                'ROE_DECLINE': ['ROE', '净资产收益', '盈利下滑'],
                'ROE_3Q_DECLINE': ['ROE', '连续', '下滑'],
                'MARGIN_SURGE': ['融资', '杠杆', '两融'],
            }
            for alert in result.alerts:
                key = alert.split(":")[0]
                keywords = keyword_map.get(key, [])
                for kw in keywords:
                    for sentence in rag_intel.split('。'):
                        if kw in sentence and len(sentence) > 5:
                            result.raw_snippets.append(sentence.strip()[:80])
                            break

        return result


# ═══════════════════════════════════════════════════════
# 集成接口
# ═══════════════════════════════════════════════════════

def build_optimized_l3_payload(candidate, l2_result,
                                intel_summary: str = "",
                                fina_factors: dict = None,
                                zeta_data=None,
                                model: str = "deepseek-r1:1.5b") -> tuple:
    """
    一站式构建优化后的 L3 请求 payload。
    返回 (payload_dict, gate_result)。
    """
    dehydrator = PromptDehydrator()
    gate = SafetyGate()

    # Step 1: 脱水压缩
    compact_prompt = dehydrator.build_l3_prompt(
        candidate, l2_result, intel_summary, fina_factors
    )

    # Step 2: 安全闸门
    gate_result = gate.evaluate(candidate, fina_factors, zeta_data, intel_summary)
    if gate_result.is_red:
        compact_prompt = gate_result.inject_alerts(compact_prompt)
        logger.warning(f"SafetyGate RED for {candidate.symbol}: {gate_result.alerts}")

    # Step 3: 构建 payload
    payload = {
        "model": model,
        "system": ZPE2_SYSTEM_PROMPT,
        "prompt": compact_prompt,
        "stream": False,
        "options": get_l3_options(),
    }

    return payload, gate_result
