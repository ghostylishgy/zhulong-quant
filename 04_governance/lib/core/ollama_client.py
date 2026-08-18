# ============================================================
# 🐲 烛龙计划 v2 - 本地 AI 客户端
# ============================================================
# 连接到本地 Ollama 节点 (192.0.2.20:11434)
# 实现 ```json``` 标签剥离和超时容错
# ============================================================

import logging
import json
import re
import sys
from pathlib import Path
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from functools import wraps


from config.settings import Config

logger = logging.getLogger('zhulong.ollama_client')


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
class OllamaResponse:
    """Ollama 响应结果"""
    success: bool
    content: str
    thought: Optional[str] = None
    raw_json: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    response_time: float = 0


# ==================== 重试装饰器 ====================

def retry_on_ollama(times: int = 3, delay: float = 2):
    """
    Ollama 调用重试装饰器

    Args:
        times: 重试次数
        delay: 重试间隔（秒）
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(times):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e

                    if attempt < times - 1:
                        wait_time = delay * (attempt + 1)
                        logger.warning(
                            f"[{func.__name__}] 第 {attempt + 1}/{times} 次失败: {e}, "
                            f"等待 {wait_time:.2f}s 后重试"
                        )

                        # 连接失败时延长等待时间
                        if 'Connection' in str(e) or 'Timeout' in str(e):
                            wait_time = min(wait_time * 2, 30)

                        import time
                        time.sleep(wait_time)

            logger.error(f"[{func.__name__}] 最终失败: {last_exception}")
            raise last_exception
        return wrapper
    return decorator


# ==================== Ollama 客户端 ====================

class OllamaClient:
    """
    本地 Ollama AI 客户端

    连接到本地节点：http://192.0.2.20:11434
    模型：deepseek-r1:7b

    核心特性：
    1. 自动剥离 ```json``` 标签
    2. 超时统一设为 30s
    3. 自动 JSON 解析和容错
    4. 连接失败时自动降级
    """

    def __init__(self, base_url: str = None, model: str = None):
        """
        初始化 Ollama 客户端

        Args:
            base_url: Ollama 服务地址
            model: 模型名称
        """
        self.base_url = base_url or Config.OLLAMA_BASE_URL
        self.model = model or Config.OLLAMA_MODEL
        self.timeout = Config.OLLAMA_TIMEOUT

        self._check_connection()

        logger.info(f"🤖 Ollama 客户端初始化完成: {self.base_url} ({self.model})")

    def _check_connection(self):
        """检查 Ollama 服务是否可用"""
        try:
            response = COMPUTE_GATEWAY.http_get(
                f"{self.base_url}/api/tags",
                timeout=5,
                layer='OLLAMA',
                decision_id='ollama_client:check_connection',
            )

            if response.status_code == 200:
                models = response.json().get('models', [])
                model_names = [m['name'] for m in models]

                if self.model not in model_names:
                    logger.warning(
                        f"⚠️ 模型 {self.model} 不在可用列表中: {model_names}"
                    )

                logger.info(f"✅ Ollama 服务连接成功: {len(models)} 个模型可用")
                return True
            else:
                logger.error(f"❌ Ollama 服务连接失败: HTTP {response.status_code}")
                return False

        except Exception as e:
            logger.error(f"❌ Ollama 服务连接失败: {e}")
            return False

    def _extract_json(self, text: str) -> Optional[Dict[str, Any]]:
        """
        从文本中提取 JSON（支持多种格式）

        支持的格式：
        1. ```json\n{...}\n```
        2. ```\n{...}\n```
        3. { ... }

        Args:
            text: 原始文本

        Returns:
            dict: 解析后的 JSON 对象，失败返回 None
        """
        if not text:
            return None

        # 方案1: 提取 ```json``` 代码块
        json_match = re.search(r'```json\s*\n([\s\S]*?)\n```', text, re.IGNORECASE)
        if json_match:
            try:
                return json.loads(json_match.group(1))
            except Exception as e:
                logger.error(f'Critical Logical Gap: {str(e)}', exc_info=True)
        code_match = re.search(r'```\s*\n([\s\S]*?)\n```', text)
        if code_match:
            try:
                content = code_match.group(1)
                if content.strip().startswith('{') or content.strip().startswith('['):
                    return json.loads(content)
            except Exception as e:
                logger.error(f'Critical Logical Gap: {str(e)}', exc_info=True)
        try:
            return json.loads(text.strip())
        except Exception as e:
            logger.error(f'Critical Logical Gap: {str(e)}', exc_info=True)
        obj_match = re.search(r'\{[\s\S]*\}', text)
        if obj_match:
            try:
                return json.loads(obj_match.group(0))
            except Exception as e:
                logger.error(f'Critical Logical Gap: {str(e)}', exc_info=True)
        logger.warning(f"⚠️ 无法从文本中提取 JSON: {text[:100]}...")
        return None

    @retry_on_ollama(times=3, delay=2)
    def chat(self, prompt: str,
             temperature: float = 0.3,
             max_tokens: int = 1000,
             system_prompt: str = None) -> OllamaResponse:
        """
        发送聊天请求到 Ollama

        Args:
            prompt: 用户提示词
            temperature: 温度参数（0-1）
            max_tokens: 最大生成 Token 数
            system_prompt: 系统提示词

        Returns:
            OllamaResponse: 响应结果
        """
        import time
        start_time = time.time()

        try:
            # 构建请求
            messages = []

            if system_prompt:
                messages.append({
                    'role': 'system',
                    'content': system_prompt
                })

            messages.append({
                'role': 'user',
                'content': prompt
            })

            # 发送请求
            response = COMPUTE_GATEWAY.http_post(
                f"{self.base_url}/api/chat",
                timeout=self.timeout,
                json_payload={
                    'model': self.model,
                    'messages': messages,
                    'stream': False,
                    'options': {
                        'temperature': temperature,
                        'num_predict': max_tokens
                    }
                },
                layer='OLLAMA',
                decision_id='ollama_client:chat',
            )

            response.raise_for_status()

            # 解析响应
            result = response.json()

            # 提取内容
            thought = ""  # 预设一个空的思考内容
            if 'message' in result:
                content = result['message'].get('content', '')
                thought = result['message'].get('reasoning_content', '')
            else:
                content = str(result)

            # 尝试提取 JSON
            raw_json = self._extract_json(content)

            # 计算响应时间
            elapsed = time.time() - start_time

            logger.debug(
                f"✅ Ollama 响应成功: "
                f"tokens={len(content)}, thought_len={len(thought)}, 耗时={elapsed:.2f}s"
            )

            return OllamaResponse(
                success=True,
                content=content,
                thought=thought,
                raw_json=raw_json,
                response_time=elapsed
            )

        except Exception as e:
            elapsed = time.time() - start_time
            if COMPUTE_GATEWAY.is_timeout_error(e):
                logger.error(f"? Ollama ???? ({self.timeout}s): {e}")
                return OllamaResponse(
                    success=False,
                    error=f"????: {str(e)}",
                    response_time=elapsed
                )
            if 'ConnectionError' in str(e):
                logger.error(f"? Ollama ????: {e}")
                return OllamaResponse(
                    success=False,
                    error=f"????: {str(e)}",
                    response_time=elapsed
                )
            logger.error(f"? Ollama ????: {e}")
            return OllamaResponse(
                success=False,
                error=f"????: {str(e)}",
                response_time=elapsed
            )

    def generate(self, prompt: str, **kwargs) -> OllamaResponse:
        """
        生成文本（chat 的别名）

        Args:
            prompt: 提示词
            **kwargs: 其他参数

        Returns:
            OllamaResponse: 响应结果
        """
        return self.chat(prompt, **kwargs)

    # ==================== 专用方法 ====================

    def analyze_sentiment(self, text: str) -> OllamaResponse:
        """
        分析文本情感（简化的方法）

        Args:
            text: 待分析的文本

        Returns:
            OllamaResponse: 情感分析结果
        """
        prompt = f"""请分析以下文本的情感倾向。

【文本】
{text[:500]}

【输出要求】
请以 JSON 格式输出：
{{
    "sentiment": "positive/negative/neutral",
    "confidence": 0-100的数字,
    "reasoning": "简短理由"
}}

只输出 JSON，不要其他内容。"""

        return self.chat(prompt, temperature=0.2, max_tokens=200)

    def extract_key_points(self, text: str, count: int = 3) -> OllamaResponse:
        """
        提取关键点

        Args:
            text: 待分析的文本
            count: 提取关键点数量

        Returns:
            OllamaResponse: 关键点提取结果
        """
        prompt = f"""请从以下文本中提取 {count} 个关键点。

【文本】
{text[:800]}

【输出要求】
请以 JSON 格式输出：
{{
    "key_points": [
        "关键点1",
        "关键点2",
        "关键点3"
    ]
}}

只输出 JSON，不要其他内容。"""

        return self.chat(prompt, temperature=0.3, max_tokens=300)

    def evaluate_stock(self, stock_info: Dict[str, Any]) -> OllamaResponse:
        """
        评估股票（简化版 Commander）

        Args:
            stock_info: 股票信息

        Returns:
            OllamaResponse: 评估结果
        """
        prompt = f"""你是一位量化投资决策者。请根据以下股票数据做出投资决策。

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

只输出 JSON，不要其他内容。"""

        return self.chat(prompt, temperature=0.2, max_tokens=300)

    def audit_risk(self, decision: Dict[str, Any],
                    stock_info: Dict[str, Any]) -> OllamaResponse:
        """
        审计投资决策（简化版 Prosecutor）

        Args:
            decision: Commander 的决策
            stock_info: 股票信息

        Returns:
            OllamaResponse: 审计结果
        """
        prompt = f"""你是一位风险控制审计官。请审核以下投资决策是否存在逻辑漏洞或风险。

【原始决策】
股票: {stock_info.get('code')} {stock_info.get('name')}
决策: {decision.get('action')}
置信度: {decision.get('confidence')}%
理由: {decision.get('reasoning')}
风险等级: {decision.get('risk_level')}

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

只输出 JSON，不要其他内容。"""

        return self.chat(prompt, temperature=0.1, max_tokens=400)

    # ==================== 便捷方法 ====================

    def is_available(self) -> bool:
        """检查服务是否可用"""
        return self._check_connection()

    def get_model_info(self) -> Optional[Dict[str, Any]]:
        """获取模型信息"""
        try:
            response = COMPUTE_GATEWAY.http_get(
                f"{self.base_url}/api/tags",
                timeout=5,
                layer='OLLAMA',
                decision_id='ollama_client:check_connection',
            )

            if response.status_code == 200:
                models = response.json().get('models', [])

                for model in models:
                    if self.model in model['name']:
                        return model

            return None

        except Exception as e:
            logger.error(f"❌ 获取模型信息失败: {e}")
            return None

    def quick_test(self) -> bool:
        """
        快速测试客户端

        Returns:
            bool: 测试是否通过
        """
        logger.info("🧪 开始 Ollama 客户端测试...")

        try:
            # 检查连接
            if not self.is_available():
                logger.error("❌ Ollama 服务不可用")
                return False

            # 测试简单对话
            prompt = "你好，请用一句话介绍你自己。"
            response = self.chat(prompt, max_tokens=50)

            if not response.success:
                logger.error(f"❌ Ollama 对话测试失败: {response.error}")
                return False

            logger.info(f"  ✅ 连接测试通过")
            logger.info(f"  🤖 模型响应: {response.content[:100]}")

            # 测试 JSON 提取
            json_prompt = "请返回一个简单的 JSON：{\"test\": 123}"
            json_response = self.chat(json_prompt, max_tokens=50)

            if not response.success:
                logger.error(f"❌ JSON 提取测试失败: {response.error}")
                return False

            if json_response.raw_json:
                logger.info(f"  ✅ JSON 提取测试通过: {json_response.raw_json}")
            else:
                logger.warning(f"  ⚠️ JSON 提取失败，但模型响应正常")

            logger.info("🎉 Ollama 客户端测试通过！")
            return True

        except Exception as e:
            logger.error(f"❌ Ollama 客户端测试失败: {e}")
            return False


# 便捷函数
def get_ollama_client() -> OllamaClient:
    """获取 Ollama 客户端实例"""
    return OllamaClient()
