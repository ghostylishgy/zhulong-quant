#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
 烛龙计划 V1.5.2 - 消息推送模块
============================================================
core/notification.py

支持渠道:
1. PushPlus (微信推送) - 默认
2. Webhook (通用接口) - 预留

使用:
    from core.notification import Notifier
    notifier = Notifier()
    notifier.send_daily_report(dragon_list, value_list, market_stats)
============================================================
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Any
from dataclasses import dataclass

import pandas as pd
from config.settings import Config

logger = logging.getLogger('zhulong.notification')


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

# ==================== 配置 ====================

PUSHPLUS_URL = "https://www.pushplus.plus/send"
DASHBOARD_URL = "http://192.0.2.10:8501/"


@dataclass
class MarketStats:
    """市场统计数据"""
    total_stocks: int = 0
    up_count: int = 0
    down_count: int = 0
    limit_up_count: int = 0
    limit_down_count: int = 0
    bull_count: int = 0  # 站上年线数


class Notifier:
    """
    烛龙消息推送服务

    默认使用 PushPlus 推送到微信
    """

    def __init__(self, token: str = None):
        """
        初始化推送器

        Args:
            token: PushPlus Token，默认从环境变量读取
        """
        self.token = token or Config.PUSHPLUS_TOKEN
        self.webhook_url = str(getattr(Config, "WEBHOOK_URL", "") or "")

        if not self.token:
            logger.warning(" PUSHPLUS_TOKEN 未配置，推送功能将不可用")

    # ==================== PushPlus 推送 ====================

    def send_pushplus(self, title: str, content: str, template: str = "html") -> bool:
        """
        发送 PushPlus 消息

        Args:
            title: 消息标题
            content: 消息内容 (支持 HTML)
            template: 模板类型 (html/txt/json/markdown)

        Returns:
            bool: 是否发送成功
        """
        if not self.token:
            logger.error(" PushPlus Token 未配置")
            return False

        try:
            payload = {
                "token": self.token,
                "title": title,
                "content": content,
                "template": template
            }

            response = COMPUTE_GATEWAY.http_post(PUSHPLUS_URL, timeout=10, json_payload=payload, layer='PUSH', decision_id='notification:pushplus')
            result = response.json()

            if result.get("code") == 200:
                logger.info(f" PushPlus 推送成功: {title}")
                return True
            else:
                logger.error(f" PushPlus 推送失败: {result.get('msg')}")
                return False

        except Exception as e:
            logger.error(f" PushPlus 推送异常: {e}")
            return False

    # ==================== Webhook 推送 ====================

    def send_webhook(self, data: Dict[str, Any]) -> bool:
        """
        发送通用 Webhook 消息

        Args:
            data: JSON 数据

        Returns:
            bool: 是否发送成功
        """
        if not self.webhook_url:
            logger.warning(" WEBHOOK_URL 未配置")
            return False

        try:
            response = COMPUTE_GATEWAY.http_post(self.webhook_url, timeout=10, json_payload=data, layer='PUSH', decision_id='notification:webhook')
            return response.status_code == 200
        except Exception as e:
            logger.error(f" Webhook 推送异常: {e}")
            return False

    # ==================== 日报生成 ====================

    def format_daily_report(
        self,
        dragon_list: pd.DataFrame,
        value_list: pd.DataFrame,
        market_stats: MarketStats
    ) -> str:
        """
        生成每日选股日报 HTML

        Args:
            dragon_list: 龙头轨道结果
            value_list: 价值轨道结果
            market_stats: 市场统计

        Returns:
            str: HTML 格式内容
        """
        today = datetime.now().strftime("%Y-%m-%d")

        html = f"""
        <div style="font-family: -apple-system, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
            <h2 style="color: #3b82f6; border-bottom: 2px solid #3b82f6; padding-bottom: 10px;">
                 烛龙选股日报
            </h2>
            <p style="color: #6b7280; font-size: 14px;">{today} 收盘分析</p>

            <!-- 市场概况 -->
            <div style="background: #f8fafc; border-radius: 8px; padding: 15px; margin: 15px 0;">
                <h3 style="color: #1e293b; margin-top: 0;"> 市场概况</h3>
                <table style="width: 100%; font-size: 14px;">
                    <tr>
                        <td>总数</td><td><strong>{market_stats.total_stocks}</strong></td>
                        <td>上涨</td><td style="color: #ef4444;"><strong>{market_stats.up_count}</strong></td>
                        <td>下跌</td><td style="color: #10b981;"><strong>{market_stats.down_count}</strong></td>
                    </tr>
                    <tr>
                        <td>涨停</td><td style="color: #ef4444;"><strong>{market_stats.limit_up_count}</strong></td>
                        <td>跌停</td><td style="color: #10b981;"><strong>{market_stats.limit_down_count}</strong></td>
                        <td>站上年线</td><td><strong>{market_stats.bull_count}</strong></td>
                    </tr>
                </table>
            </div>
        """

        # 龙头轨道
        html += """
            <div style="background: #fef3c7; border-radius: 8px; padding: 15px; margin: 15px 0;">
                <h3 style="color: #92400e; margin-top: 0;"> 龙头轨道 (RPS > 95)</h3>
        """

        if dragon_list is not None and not dragon_list.empty:
            top5 = dragon_list.head(5)
            html += '<table style="width: 100%; font-size: 13px; border-collapse: collapse;">'
            html += '<tr style="background: #fde68a;"><th>名称</th><th>现价</th><th>涨幅</th><th>RPS</th></tr>'

            for _, row in top5.iterrows():
                name = row.get('name', '-')
                close = row.get('close', 0)
                pct = row.get('pct_chg', 0)
                rps = row.get('rps_10', 0)
                pct_color = '#ef4444' if pct > 0 else '#10b981'
                pct_str = f"+{pct:.2f}%" if pct > 0 else f"{pct:.2f}%"

                html += f'''
                <tr style="border-bottom: 1px solid #fde68a;">
                    <td><strong>{name}</strong></td>
                    <td>{close:.2f}</td>
                    <td style="color: {pct_color};">{pct_str}</td>
                    <td><strong>{int(rps)}</strong></td>
                </tr>
                '''
            html += '</table>'
        else:
            html += '<p style="color: #92400e;">暂无符合条件的龙头股</p>'

        html += '</div>'

        # 价值轨道
        html += """
            <div style="background: #d1fae5; border-radius: 8px; padding: 15px; margin: 15px 0;">
                <h3 style="color: #065f46; margin-top: 0;"> 价值轨道 (PE < 60)</h3>
        """

        if value_list is not None and not value_list.empty:
            top5 = value_list.head(5)
            html += '<table style="width: 100%; font-size: 13px; border-collapse: collapse;">'
            html += '<tr style="background: #a7f3d0;"><th>名称</th><th>现价</th><th>PE</th><th>行业</th></tr>'

            for _, row in top5.iterrows():
                name = row.get('name', '-')
                close = row.get('close', 0)
                pe = row.get('pe', 0)
                industry = row.get('industry', '-')

                html += f'''
                <tr style="border-bottom: 1px solid #a7f3d0;">
                    <td><strong>{name}</strong></td>
                    <td>{close:.2f}</td>
                    <td>{pe:.1f}</td>
                    <td>{industry}</td>
                </tr>
                '''
            html += '</table>'
        else:
            html += '<p style="color: #065f46;">暂无符合条件的价值股</p>'

        html += '</div>'

        # 底部链接
        html += f"""
            <div style="text-align: center; margin-top: 20px; padding-top: 15px; border-top: 1px solid #e5e7eb;">
                <a href="{DASHBOARD_URL}" style="background: #3b82f6; color: white; padding: 10px 20px; border-radius: 6px; text-decoration: none; font-weight: bold;">
                     查看完整看板
                </a>
                <p style="color: #9ca3af; font-size: 12px; margin-top: 15px;">
                    烛龙 V1.5.2 | 自动生成于 {datetime.now().strftime('%H:%M:%S')}
                </p>
            </div>
        </div>
        """

        return html

    # ==================== 发送日报 ====================

    def send_daily_report(
        self,
        dragon_list: pd.DataFrame,
        value_list: pd.DataFrame,
        market_stats: MarketStats
    ) -> bool:
        """
        发送每日选股日报

        Args:
            dragon_list: 龙头轨道结果
            value_list: 价值轨道结果
            market_stats: 市场统计

        Returns:
            bool: 是否发送成功
        """
        today = datetime.now().strftime("%Y-%m-%d")
        title = f" 烛龙选股日报 [{today}]"
        content = self.format_daily_report(dragon_list, value_list, market_stats)

        return self.send_pushplus(title, content, template="html")

    # ==================== 快捷方法 ====================

    def send_alert(self, title: str, message: str) -> bool:
        """发送告警消息"""
        return self.send_pushplus(f" {title}", f"<p>{message}</p>")

    def send_signal(self, stock_name: str, signal_type: str, price: float, reason: str) -> bool:
        """发送交易信号"""
        emoji = "" if signal_type == "买入" else ""
        title = f"{emoji} {signal_type}信号: {stock_name}"
        content = f"""
        <div style="font-family: sans-serif;">
            <h3>{stock_name} - {signal_type}</h3>
            <p> 价格: <strong>{price:.2f}</strong></p>
            <p> 理由: {reason}</p>
            <p style="color: #6b7280; font-size: 12px;">
                {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            </p>
        </div>
        """
        return self.send_pushplus(title, content)


# ==================== 便捷函数 ====================

def get_notifier() -> Notifier:
    """获取 Notifier 实例"""
    return Notifier()


if __name__ == "__main__":
    # 测试
    logging.basicConfig(level=logging.INFO)

    notifier = Notifier()

    # 测试发送
    stats = MarketStats(total_stocks=5000, up_count=2500, down_count=2300, limit_up_count=50)

    # 模拟数据
    dragon_df = pd.DataFrame({
        'name': ['测试股1', '测试股2'],
        'close': [10.5, 20.3],
        'pct_chg': [5.5, -2.1],
        'rps_10': [98, 96]
    })

    value_df = pd.DataFrame({
        'name': ['价值股1'],
        'close': [15.0],
        'pe': [8.5],
        'industry': ['银行']
    })

    print(notifier.format_daily_report(dragon_df, value_df, stats))
