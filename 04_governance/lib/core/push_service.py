# ============================================================
# 🐲 烛龙计划 - PushPlus 推送服务
# ============================================================
# 分级推送通知，支持 Info/Error 两种级别
# ============================================================

import logging
import time
from typing import Optional
from functools import wraps

import os
import sys
from pathlib import Path

def _get_proxies():
    p = os.environ.get('HTTP_PROXY') or os.environ.get('http_proxy')
    return {'http': p, 'https': p} if p else None

# 延迟导入避免循环依赖
def get_config():
    from config.settings import Config
    return Config

logger = logging.getLogger('zhulong.push')


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


def retry(times: int = 3, delay: float = 2):
    """重试装饰器"""
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
                        logger.warning(f"推送失败 (尝试 {attempt + 1}/{times}): {e}")
                        time.sleep(delay)
            logger.error(f"推送最终失败: {last_exception}")
            raise last_exception
        return wrapper
    return decorator


class Notifier:
    """
    烛龙通知服务

    支持两种推送级别:
    - info(): 普通消息 (L2/L3 级别)
    - error(): 高优先级报警 (L1 级别)
    """

    _instance = None

    def __new__(cls):
        """单例模式"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        Config = get_config()
        self.token = Config.PUSHPLUS_TOKEN
        self.url = Config.PUSHPLUS_URL
        self.retry_times = Config.RETRY_TIMES
        self.retry_delay = Config.RETRY_DELAY

        if not self.token:
            logger.warning("⚠️ PushPlus Token 未配置，推送功能禁用")

        self._initialized = True

    def _send(self, title: str, content: str, template: str = 'html') -> bool:
        """
        发送推送

        Args:
            title: 推送标题
            content: 推送内容 (支持 HTML)
            template: 模板类型 (html/txt/json/markdown)

        Returns:
            是否发送成功
        """
        if not self.token:
            logger.warning(f"[跳过推送] {title}")
            return False

        payload = {
            'token': self.token,
            'title': title,
            'content': content,
            'template': template
        }

        try:
            response = COMPUTE_GATEWAY.http_post(
                self.url,
                timeout=10,
                json_payload=payload,
                proxies=_get_proxies(),
                layer='PUSH',
                decision_id='push_service:send',
            )
            result = response.json()

            if result.get('code') == 200:
                logger.info(f"✅ 推送成功: {title}")
                return True
            else:
                logger.error(f"❌ 推送失败: {result.get('msg')}")
                return False

        except Exception as e:
            logger.error(f"❌ 推送请求异常: {e}")
            raise

    @retry(times=3, delay=2)
    def info(self, title: str, content: str, level: int = 2) -> bool:
        """
        发送普通信息

        Args:
            title: 标题
            content: 内容
            level: 推送级别 (2=每日简报, 3=静默记录)

        Returns:
            是否发送成功
        """
        Config = get_config()

        # L3 级别只记录日志，不推送
        if level >= Config.PUSH_LEVEL_SILENT:
            logger.info(f"[L3 静默记录] {title}: {content[:100]}...")
            return True

        # 添加级别标识
        formatted_title = f"📋 {title}"
        return self._send(formatted_title, content)

    @retry(times=3, delay=2)
    def error(self, title: str, content: str) -> bool:
        """
        发送高优先级报警 (L1 级别)

        用于：买卖信号、止损提醒、系统错误

        Args:
            title: 标题
            content: 内容

        Returns:
            是否发送成功
        """
        # L1 级别：添加醒目标识
        formatted_title = f"🔴 {title}"
        formatted_content = f"""
        <div style="border-left: 4px solid #ff4444; padding-left: 10px;">
            <strong>⚠️ 高优先级通知</strong><br>
            {content}
        </div>
        """
        return self._send(formatted_title, formatted_content)

    def signal(self, signal_type: str, code: str, name: str,
               price: float, reason: str = '') -> bool:
        """
        发送交易信号通知 (L1 级别)

        Args:
            signal_type: 信号类型 (BUY/SELL)
            code: 股票代码
            name: 股票名称
            price: 当前价格
            reason: 信号原因

        Returns:
            是否发送成功
        """
        emoji = '🟢' if signal_type == 'BUY' else '🔴'
        title = f"{emoji} {signal_type} 信号 - {name}"

        content = f"""
        <table style="width:100%; border-collapse:collapse;">
            <tr><td><strong>股票</strong></td><td>{code} {name}</td></tr>
            <tr><td><strong>信号</strong></td><td>{signal_type}</td></tr>
            <tr><td><strong>价格</strong></td><td>¥{price:.2f}</td></tr>
            <tr><td><strong>原因</strong></td><td>{reason}</td></tr>
            <tr><td><strong>时间</strong></td><td>{time.strftime('%Y-%m-%d %H:%M:%S')}</td></tr>
        </table>
        """

        return self.error(title, content)

    def daily_report(self, title: str, content: str) -> bool:
        """
        发送每日简报 (L2 级别)

        Args:
            title: 标题
            content: HTML 格式内容

        Returns:
            是否发送成功
        """
        return self.info(title, content, level=2)

    def test(self) -> bool:
        """
        发送测试消息

        Returns:
            是否发送成功
        """
        return self.info(
            "🐲 烛龙系统测试",
            f"<p>如果你看到这条消息，说明 PushPlus 通知配置成功！</p>"
            f"<p>测试时间: {time.strftime('%Y-%m-%d %H:%M:%S')}</p>",
            level=2
        )


# 便捷函数
def get_notifier() -> Notifier:
    """获取通知服务单例"""
    return Notifier()


def push_info(title: str, content: str, level: int = 2) -> bool:
    """快捷推送普通消息"""
    return get_notifier().info(title, content, level)


def push_error(title: str, content: str) -> bool:
    """快捷推送报警消息"""
    return get_notifier().error(title, content)
