# ============================================================
# 🐲 烛龙计划 - AI 董事会
# ============================================================
# 封装 LLM API 调用，实现智能决策分工：
# - 📚 Librarian (GLM-4): 研报清洗
# - 👮 Commander (DeepSeek-V3): 投资决策
# - ⚖️ Prosecutor (DeepSeek-R1): 风控审计
#
# 特性：
# - 自动检测 API Key，无 Key 时降级为 mock 模式
# - 内置重试和异常处理
# ============================================================

import logging
import time
import json
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from functools import wraps
from core.ollama_client import get_ollama_client

# 延迟导入
def get_config():
    from config.settings import Config
    return Config

logger = logging.getLogger('zhulong.ai')


try:
    from .module_loader import load_attr_from_path, resolve_project_root
except Exception:
    _core_dir = Path(__file__).resolve().parent
    if str(_core_dir) not in sys.path:
        sys.path.append(str(_core_dir))
    from module_loader import load_attr_from_path, resolve_project_root

_PROJECT_ROOT = resolve_project_root(Path(__file__))
ComputeGateway = load_attr_from_path(
    "compute_gateway_02",
    _PROJECT_ROOT / "02_brain" / "lib" / "compute_gateway.py",
    "ComputeGateway",
)
COMPUTE_GATEWAY = ComputeGateway(logger=logger, max_slots=3)


# ==================== 数据结构 ====================

@dataclass
class LibrarianResult:
    """Librarian 清洗结果"""
    summary: str           # 核心摘要
    sentiment_score: int   # 情感评分 (0-100, 50=中性)
    key_points: List[str]  # 三条脱水干货
    contradictions: List[str] = None  # 矛盾点
    risk_factors: List[str] = None    # 潜在风险
    profit_forecast: str = ""         # 盈利预测摘要
    raw_reports: List[Dict] = None    # 原始研报数据
    is_mock: bool = False  # 是否为模拟数据


@dataclass
class CommanderDecision:
    """Commander 决策结果"""
    action: str            # BUY / HOLD / PASS
    confidence: int        # 置信度 (0-100)
    reasoning: str         # 决策理由
    risk_level: str        # LOW / MEDIUM / HIGH
    is_mock: bool = False


@dataclass
class ProsecutorAudit:
    """Prosecutor 审计结果"""
    approved: bool         # 是否通过审计
    risk_score: int        # 风险评分 (0-100, 越低越好)
    warnings: List[str]    # 风险警告
    logic_flaws: List[str] # 逻辑漏洞
    is_mock: bool = False


# ==================== 重试装饰器 ====================

def retry_on_failure(times: int = 3, delay: float = 2):
    """API 调用重试装饰器"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            Config = get_config()
            last_exception = None

            for attempt in range(times):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < times - 1:
                        logger.warning(f"[{func.__name__}] 第 {attempt + 1}/{times} 次失败: {e}")
                        time.sleep(delay * (attempt + 1))

            logger.error(f"[{func.__name__}] 最终失败: {last_exception}")
            raise last_exception
        return wrapper
    return decorator


# ==================== AI 董事会 ====================

class AIBoard:
    """
    烛龙 AI 董事会

    集成多个 LLM 角色，实现智能决策流水线。
    自动检测 API Key，无 Key 时降级为 mock 模式。
    """

    def __init__(self):
        Config = get_config()

        # 检测 API Key 可用性
        self.deepseek_available = bool(Config.DEEPSEEK_API_KEY)
        self.zhipu_available = bool(Config.ZHIPU_API_KEY)

        # 初始化客户端（如果可用）
        self._deepseek_client = None
        self._zhipu_client = None
        if self.deepseek_available:
            try:
                self._deepseek_client = COMPUTE_GATEWAY.create_openai_client(
                    api_key=Config.DEEPSEEK_API_KEY,
                    base_url=Config.DEEPSEEK_BASE_URL,
                )
                logger.info("?? DeepSeek API ??? (Commander & Prosecutor)")
            except Exception as e:
                logger.warning(f"?? DeepSeek ?????: {e}")
                self.deepseek_available = False

        if self.zhipu_available:
            try:
                self._zhipu_client = COMPUTE_GATEWAY.create_openai_client(
                    api_key=Config.ZHIPU_API_KEY,
                    base_url=Config.ZHIPU_BASE_URL,
                )
                logger.info("?? ?? GLM-4 API ??? (Librarian)")
            except Exception as e:
                logger.warning(f"?? ???????: {e}")
                self.zhipu_available = False

        # 模式状态
        if not self.deepseek_available and not self.zhipu_available:
            logger.warning("⚠️ 所有 AI 服务不可用，将使用 Mock 模式")

        logger.info("🐲 AI 董事会初始化完成")

    @property
    def mock_mode(self) -> bool:
        """是否处于完全 mock 模式"""
        return False

    # ==================== Librarian (GLM-4) ====================

    @retry_on_failure(times=3, delay=2)
    def librarian_clean(self, raw_text: str) -> LibrarianResult:
        """
        📚 Librarian: 清洗研报/新闻

        使用 GLM-4 提取核心摘要和情感评分

        Args:
            raw_text: 原始文本（研报/新闻）

        Returns:
            LibrarianResult: 清洗后的结构化数据
        """
        if not self.zhipu_available:
            return self._mock_librarian(raw_text)

        Config = get_config()

        prompt = f"""你是一位专业的金融研究分析师。请分析以下文本，提取关键信息。

【文本内容】
{raw_text[:3000]}

【输出要求】
请以 JSON 格式输出：
{{
    "summary": "核心摘要（100字以内）",
    "sentiment_score": 0-100的数字（50=中性，>50=看涨，<50=看跌）,
    "key_points": ["要点1", "要点2", "要点3"]
}}

只输出 JSON，不要其他内容。"""

        try:
            response = self._zhipu_client.chat.completions.create(
                model=Config.ZHIPU_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=500
            )

            content = response.choices[0].message.content
            data = self._parse_json(content)

            return LibrarianResult(
                summary=data.get('summary', ''),
                sentiment_score=int(data.get('sentiment_score', 50)),
                key_points=data.get('key_points', []),
                is_mock=False
            )

        except Exception as e:
            logger.error(f"❌ Librarian 调用失败: {e}")
            return self._mock_librarian(raw_text)

    @retry_on_failure(times=3, delay=2)
    def librarian_analyze_reports(self, code: str, name: str = "") -> LibrarianResult:
        """
        📚 Librarian: 深度情报分析

        自动抓取研报，如果研报不足则抓取新闻进行分析

        Args:
            code: 股票代码
            name: 股票名称

        Returns:
            LibrarianResult: 深度分析结果
        """
        # 抓取情报（研报优先，新闻备用）
        from data_engine.fetcher import Fetcher
        fetcher = Fetcher()

        intel_text, data_source = fetcher.fetch_intelligence_text(code)
        intel = fetcher.fetch_intelligence(code)

        # 合并原始数据
        raw_data = intel['reports'] if intel['reports'] else intel['news']

        # 完全无数据
        if data_source == 'none':
            logger.warning(f"⚠️ {code} 无研报和新闻数据")
            return LibrarianResult(
                summary="该股暂无公开研报或新闻",
                sentiment_score=50,
                key_points=["⚠️ 该股暂无公开研报或新闻，建议手动搜索公告"],
                raw_reports=[],
                is_mock=False
            )

        if not self.zhipu_available:
            return self._mock_librarian_reports(raw_data)

        Config = get_config()

        # 根据数据源类型选择不同 Prompt
        if data_source == 'report':
            prompt = f"""你是一位资深的卖方研究员"脱水器"。你的任务是从多篇机构研报中提取核心价值信息。

【股票】{code} {name}

【研报内容】
{intel_text[:4000]}

【分析要求】
1. 脱水摘要：剔除所有官话、套话，只保留最干货的 3 条信息
2. 盈利预测：提取各机构对未来 1-2 年的盈利预测（EPS、净利润增速等）
3. 情感评分：综合所有研报给出 0-100 分（50=中性，>70=看涨，<30=看跌）
4. 矛盾点检测：
   - 机构分歧：不同机构评级差异大
   - 股价背离：集体看多但股价下跌
   - 逻辑冲突：同一研报内自相矛盾
5. 潜在风险：研报中提到但容易被忽视的风险因素

【输出格式】
请以 JSON 格式输出：
{{
    "summary": "一句话总结所有研报的核心观点（30字内）",
    "sentiment_score": 0-100,
    "key_points": [
        "🔥 干货1（最重要的核心逻辑）",
        "📈 干货2（业绩或催化剂）",
        "💡 干货3（被忽视的价值点）"
    ],
    "profit_forecast": "盈利预测摘要",
    "contradictions": ["矛盾点1", "矛盾点2"],
    "risk_factors": ["风险1", "风险2"]
}}

只输出 JSON。"""
        else:
            # 新闻模式 - 特殊 Prompt
            prompt = f"""你是一位资深的财经记者和股票分析师。由于该股票没有机构研报，请基于最新的个股新闻进行深度分析。

【股票】{code} {name}

【最新新闻】
{intel_text[:4000]}

【分析要求】
1. 异动解读：分析该股近期暴涨/下跌/异动的市场逻辑
2. 核心事件：提取影响股价的关键事件（重组、政策、业绩等）
3. 情感评分：综合新闻情绪给出 0-100 分（50=中性，>70=利好，<30=利空）
4. 财务健康度评估：基于新闻信息推断公司财务状况
5. 风险点：新闻中隐含的风险信号（负面舆情、监管问询等）

【输出格式】
请以 JSON 格式输出：
{{
    "summary": "一句话总结近期核心事件（30字内）",
    "sentiment_score": 0-100,
    "key_points": [
        "🔥 核心事件（最重要的市场逻辑）",
        "📈 催化剂（推动股价的关键因素）",
        "💡 隐藏信息（被忽视但重要的细节）"
    ],
    "profit_forecast": "财务健康度评估",
    "contradictions": ["矛盾点或疑点"],
    "risk_factors": ["风险信号1", "风险信号2"]
}}

只输出 JSON。"""

        try:
            response = self._zhipu_client.chat.completions.create(
                model=Config.ZHIPU_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=800
            )

            content = response.choices[0].message.content
            data = self._parse_json(content)

            # 添加数据源标记
            summary = data.get('summary', '')
            if data_source == 'news':
                summary = f"[基于新闻] {summary}"

            return LibrarianResult(
                summary=summary,
                sentiment_score=int(data.get('sentiment_score', 50)),
                key_points=data.get('key_points', []),
                profit_forecast=data.get('profit_forecast', ''),
                contradictions=data.get('contradictions', []),
                risk_factors=data.get('risk_factors', []),
                raw_reports=raw_data,
                is_mock=False
            )

        except Exception as e:
            logger.error(f"❌ Librarian 分析失败: {e}")
            return self._mock_librarian_reports(raw_data)

    def _mock_librarian(self, raw_text: str) -> LibrarianResult:
        """Librarian Mock 模式"""
        return LibrarianResult(
            summary=f"[MOCK] {raw_text[:100]}...",
            sentiment_score=50,
            key_points=["[MOCK] 无法解析"],
            is_mock=True
        )

    def _mock_librarian_reports(self, reports: List[Dict]) -> LibrarianResult:
        """研报分析 Mock 模式"""
        return LibrarianResult(
            summary=f"[MOCK] 共 {len(reports)} 篇研报",
            sentiment_score=50,
            key_points=[f"[MOCK] {r.get('title', 'N/A')}" for r in reports[:3]],
            raw_reports=reports,
            is_mock=True
        )

    # ==================== Commander (DeepSeek-V3) ====================

    @retry_on_failure(times=3, delay=2)
    def commander_evaluate(self, stock_info: Dict[str, Any]) -> CommanderDecision:
        """
        👮 Commander: 投资决策

        根据股票信息做出 BUY/HOLD/PASS 决策

        Args:
            stock_info: 股票信息 (包含 code, name, rps, pattern 等)

        Returns:
            CommanderDecision: 决策结果
        """
        if not self.deepseek_available:
            return self._mock_commander(stock_info)

        Config = get_config()

        prompt = f"""你是一位严谨的量化投资决策者。请根据以下股票数据做出投资决策。

【股票信息】
代码: {stock_info.get('code', 'N/A')}
名称: {stock_info.get('name', 'N/A')}
RPS强度: 50日={stock_info.get('rps_50', 0)}, 120日={stock_info.get('rps_120', 0)}, 250日={stock_info.get('rps_250', 0)}
技术形态: {stock_info.get('pattern', 'N/A')}
形态得分: {stock_info.get('pattern_score', 0)}
均线多头: {stock_info.get('ma_alignment', False)}
量比: {stock_info.get('volume_ratio', 0)}
当前价格: {stock_info.get('close_price', 0)}
涨跌幅: {stock_info.get('pct_chg', 0)}%

【决策规则】
1. RPS_avg >= 90 且形态得分 >= 60 → 可考虑 BUY
2. RPS_avg >= 85 且形态得分 >= 40 → 建议 HOLD (观望)
3. 其他情况 → PASS

【输出要求】
请以 JSON 格式输出：
{{
    "action": "BUY/HOLD/PASS",
    "confidence": 0-100,
    "reasoning": "决策理由（50字以内）",
    "risk_level": "LOW/MEDIUM/HIGH"
}}

只输出 JSON。"""

        try:
            response = self._deepseek_client.chat.completions.create(
                model=Config.DEEPSEEK_MODEL_V3,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
                max_tokens=300
            )

            content = response.choices[0].message.content
            data = self._parse_json(content)

            return CommanderDecision(
                action=data.get('action', 'HOLD'),
                confidence=int(data.get('confidence', 50)),
                reasoning=data.get('reasoning', ''),
                risk_level=data.get('risk_level', 'MEDIUM'),
                is_mock=False
            )

        except Exception as e:
            logger.error(f"❌ Commander 调用失败: {e}")
            return self._mock_commander(stock_info)

    def _mock_commander(self, stock_info: Dict) -> CommanderDecision:
        """Commander Mock 模式"""
        rps_avg = stock_info.get('rps_avg', 0)
        pattern_score = stock_info.get('pattern_score', 0)

        # 简单规则模拟
        if rps_avg >= 90 and pattern_score >= 60:
            action = 'BUY'
            confidence = 70
        elif rps_avg >= 85 and pattern_score >= 40:
            action = 'HOLD'
            confidence = 50
        else:
            action = 'PASS'
            confidence = 60

        return CommanderDecision(
            action=action,
            confidence=confidence,
            reasoning=f"[MOCK] RPS={rps_avg}, 形态={pattern_score}",
            risk_level='MEDIUM',
            is_mock=True
        )

    # ==================== Prosecutor (DeepSeek-R1) ====================

    @retry_on_failure(times=3, delay=2)
    def prosecutor_audit(self, decision: CommanderDecision,
                         stock_info: Dict[str, Any]) -> ProsecutorAudit:
        """
        ⚖️ Prosecutor: 风控审计

        对 Commander 的决策进行二次审核，检测逻辑漏洞

        Args:
            decision: Commander 的决策结果
            stock_info: 股票信息

        Returns:
            ProsecutorAudit: 审计结果
        """
        if not self.deepseek_available:
            return self._mock_prosecutor(decision, stock_info)

        Config = get_config()

        prompt = f"""你是一位风险控制审计官。请审核以下投资决策是否存在逻辑漏洞或风险。

【原始决策】
股票: {stock_info.get('code')} {stock_info.get('name')}
决策: {decision.action}
置信度: {decision.confidence}%
理由: {decision.reasoning}
风险等级: {decision.risk_level}

【股票数据】
RPS: {stock_info.get('rps_avg', 0)}
形态: {stock_info.get('pattern', 'N/A')}
量比: {stock_info.get('volume_ratio', 0)}

【审计要点】
1. 决策逻辑是否自洽？
2. 是否存在过度乐观？
3. 是否忽略了某些风险因素？

【输出要求】
请以 JSON 格式输出：
{{
    "approved": true/false,
    "risk_score": 0-100 (越低越好),
    "warnings": ["警告1", "警告2"],
    "logic_flaws": ["漏洞1", "漏洞2"]
}}

只输出 JSON。"""

        try:
            response = self._deepseek_client.chat.completions.create(
                model=Config.DEEPSEEK_MODEL_R1,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=400
            )

            content = response.choices[0].message.content
            data = self._parse_json(content)

            return ProsecutorAudit(
                approved=data.get('approved', False),
                risk_score=int(data.get('risk_score', 50)),
                warnings=data.get('warnings', []),
                logic_flaws=data.get('logic_flaws', []),
                is_mock=False
            )

        except Exception as e:
            logger.error(f"❌ Prosecutor 调用失败: {e}")
            return self._mock_prosecutor(decision, stock_info)

    def _mock_prosecutor(self, decision: CommanderDecision,
                         stock_info: Dict) -> ProsecutorAudit:
        """Prosecutor Mock 模式"""
        # 简单规则：BUY 决策自动警告
        if decision.action == 'BUY':
            return ProsecutorAudit(
                approved=True,
                risk_score=40,
                warnings=["[MOCK] 请验证消息来源"],
                logic_flaws=[],
                is_mock=True
            )
        else:
            return ProsecutorAudit(
                approved=True,
                risk_score=20,
                warnings=[],
                logic_flaws=[],
                is_mock=True
            )

    # ==================== 工具方法 ====================

    def _parse_json(self, text: str) -> Dict:
        """从文本中提取 JSON"""
        try:
            # 尝试直接解析
            return json.loads(text)
        except Exception as e:
            logger.error(f'Critical Logical Gap: {str(e)}', exc_info=True)
        import re
        json_match = re.search(r'\{[\s\S]*\}', text)
        if json_match:
            try:
                return json.loads(json_match.group())
            except Exception as e:
                logger.error(f'Critical Logical Gap: {str(e)}', exc_info=True)
        logger.warning(f"⚠️ JSON 解析失败: {text[:100]}")
        return {}

    def evaluate_candidate(self, candidate: Dict) -> Dict:
        """
        完整评估一只候选股票

        流程: Commander 决策 → Prosecutor 审计

        Args:
            candidate: 候选股票信息 (from TrendHunter)

        Returns:
            完整评估结果
        """
        # Step 1: Commander 决策
        decision = self.commander_evaluate(candidate)

        # Step 2: 如果不是 PASS，进行 Prosecutor 审计
        audit = None
        if decision.action != 'PASS':
            audit = self.prosecutor_audit(decision, candidate)

        return {
            'stock': candidate,
            'decision': decision,
            'audit': audit,
            'final_action': decision.action if (audit is None or audit.approved) else 'BLOCKED'
        }

    def batch_evaluate(self, candidates: List[Dict],
                       max_count: int = 30) -> List[Dict]:
        """
        批量评估候选股票

        【成本控制】: 最多评估 max_count 只，避免 Token 浪费

        Args:
            candidates: 候选列表
            max_count: 最大评估数量

        Returns:
            评估结果列表
        """
        # 截取前 N 只
        to_evaluate = candidates[:max_count]

        logger.info(f"🤖 AI 董事会开始评估 {len(to_evaluate)} 只候选...")

        results = []
        for i, candidate in enumerate(to_evaluate, 1):
            try:
                result = self.evaluate_candidate(candidate)
                results.append(result)

                # 进度日志
                action = result['final_action']
                code = candidate.get('code', 'N/A')
                logger.info(f"  [{i}/{len(to_evaluate)}] {code}: {action}")

                # API 调用间隔
                if not self.mock_mode:
                    time.sleep(0.5)

            except Exception as e:
                logger.error(f"❌ 评估 {candidate.get('code')} 失败: {e}")
                continue

        # 统计
        buy_count = sum(1 for r in results if r['final_action'] == 'BUY')
        hold_count = sum(1 for r in results if r['final_action'] == 'HOLD')

        logger.info(f"✅ 评估完成! BUY: {buy_count}, HOLD: {hold_count}")

        return results

    def quick_test(self) -> bool:
        """快速测试 AI 董事会"""
        logger.info("🧪 开始 AI 董事会测试...")

        # 测试数据
        test_stock = {
            'code': '000001',
            'name': '测试股票',
            'rps_50': 92,
            'rps_120': 88,
            'rps_250': 85,
            'rps_avg': 88,
            'pattern': '均线多头+口袋支点',
            'pattern_score': 70,
            'ma_alignment': True,
            'volume_ratio': 1.5,
            'close_price': 10.5,
            'pct_chg': 2.3
        }

        try:
            # 测试 Commander
            decision = self.commander_evaluate(test_stock)
            logger.info(f"  👮 Commander: {decision.action} (置信度={decision.confidence}%) {'[MOCK]' if decision.is_mock else ''}")

            # 测试 Prosecutor
            audit = self.prosecutor_audit(decision, test_stock)
            logger.info(f"  ⚖️ Prosecutor: {'通过' if audit.approved else '拒绝'} (风险={audit.risk_score}) {'[MOCK]' if audit.is_mock else ''}")

            logger.info("✅ AI 董事会测试通过!")
            return True

        except Exception as e:
            logger.error(f"❌ AI 董事会测试失败: {e}")
            return False


# 便捷函数
def get_ai_board() -> AIBoard:
    """获取 AI 董事会实例"""
    return AIBoard()
