# ============================================================
# 🐲 烛龙计划 - 风控铁律
# ============================================================
# 实现仓位管理、止损止盈、盘中监控
# 这些规则是"硬编码"在代码中的死逻辑，不可覆盖
# ============================================================

import logging
from datetime import datetime, date, time as dt_time
from typing import Optional, List, Dict, Tuple
from dataclasses import dataclass
from enum import Enum

# 延迟导入
def get_config():
    from config.settings import Config
    return Config

def get_db():
    from core.database import Database
    return Database()

def get_notifier():
    from core.push_service import Notifier
    return Notifier()

logger = logging.getLogger('zhulong.risk')


# ==================== 风险信号类型 ====================

class RiskSignal(Enum):
    """风险信号类型"""
    STOP_LOSS = "STOP_LOSS"              # 止损触发
    TAKE_PROFIT = "TAKE_PROFIT"          # 止盈触发
    TRAILING_STOP = "TRAILING_STOP"      # 移动止盈触发
    POSITION_LIMIT = "POSITION_LIMIT"    # 仓位超限
    DAILY_LIMIT = "DAILY_LIMIT"          # 每日亏损限制


@dataclass
class RiskAlert:
    """风险警报"""
    signal_type: RiskSignal
    code: str
    name: str
    current_price: float
    buy_price: float
    profit_pct: float      # 盈亏比例
    message: str
    level: int = 1         # 推送级别 (1=最高)

    def to_push_content(self) -> str:
        """生成推送内容"""
        emoji = "🔴" if self.profit_pct < 0 else "🟢"
        return f"""
        <h3>{emoji} {self.signal_type.value}</h3>
        <table>
            <tr><td><b>股票</b></td><td>{self.code} {self.name}</td></tr>
            <tr><td><b>买入价</b></td><td>¥{self.buy_price:.2f}</td></tr>
            <tr><td><b>当前价</b></td><td>¥{self.current_price:.2f}</td></tr>
            <tr><td><b>盈亏</b></td><td style="color:{'red' if self.profit_pct < 0 else 'green'}">{self.profit_pct*100:+.2f}%</td></tr>
            <tr><td><b>建议</b></td><td>{self.message}</td></tr>
        </table>
        """


# ==================== 风控铁律 ====================

class RiskControl:
    """
    烛龙风控模块

    铁律（不可覆盖）：
    1. 硬止损：亏损 7% 自动触发卖出信号
    2. 移动止盈：盈利 5% 后开启，回撤 3% 触发
    3. 仓位限制：单股最大 25%，最多持 4 只
    """

    # ==================== 仓位规则 (硬编码) ====================
    MAX_SINGLE_POSITION = 0.25    # 单股最大仓位 25%
    MAX_TOTAL_POSITIONS = 4       # 最多同时持有 4 只
    NEW_POSITION_SIZE = 0.05      # 新建仓位 5%

    def __init__(self):
        Config = get_config()
        self.db = get_db()
        self.notifier = get_notifier()

        # 风控参数（从配置读取）
        self.hard_stop_loss = Config.HARD_STOP_LOSS        # -7%
        self.take_profit_start = Config.TAKE_PROFIT_START  # +5%
        self.trailing_stop = Config.TRAILING_STOP          # 3%

        logger.info(f"🛡️ 风控模块初始化: 止损={self.hard_stop_loss*100:.0f}%, 止盈={self.take_profit_start*100:.0f}%")

    # ==================== 仓位管理 ====================

    def check_can_buy(self, total_capital: float,
                      new_amount: float) -> Tuple[bool, str]:
        """
        检查是否可以买入

        Args:
            total_capital: 总资金
            new_amount: 拟买入金额

        Returns:
            (是否可以买入, 原因)
        """
        positions = self.db.get_holding_positions()

        # 规则 1: 持仓数量限制
        if len(positions) >= self.MAX_TOTAL_POSITIONS:
            return False, f"已持有 {len(positions)} 只，达到上限 {self.MAX_TOTAL_POSITIONS}"

        # 规则 2: 单股仓位限制
        position_ratio = new_amount / total_capital if total_capital > 0 else 0
        if position_ratio > self.MAX_SINGLE_POSITION:
            return False, f"单股仓位 {position_ratio*100:.1f}% 超过限制 {self.MAX_SINGLE_POSITION*100:.0f}%"

        return True, "允许买入"

    def calculate_position_size(self, total_capital: float,
                                risk_level: str = 'MEDIUM') -> float:
        """
        计算建议仓位大小

        Args:
            total_capital: 总资金
            risk_level: 风险等级 (LOW/MEDIUM/HIGH)

        Returns:
            建议买入金额
        """
        # 基础仓位
        base_size = self.NEW_POSITION_SIZE

        # 根据风险等级调整
        if risk_level == 'LOW':
            size = base_size * 1.5    # 低风险可以加大仓位
        elif risk_level == 'HIGH':
            size = base_size * 0.5    # 高风险减半
        else:
            size = base_size

        # 不超过单股上限
        size = min(size, self.MAX_SINGLE_POSITION)

        return total_capital * size

    # ==================== 止损止盈监控 ====================

    def check_position(self, code: str, name: str,
                       buy_price: float, current_price: float,
                       highest_price: float) -> Optional[RiskAlert]:
        """
        检查单个持仓的风险状态

        Args:
            code: 股票代码
            name: 股票名称
            buy_price: 买入价格
            current_price: 当前价格
            highest_price: 持仓期间最高价

        Returns:
            RiskAlert 如果触发风控，否则 None
        """
        profit_pct = (current_price - buy_price) / buy_price

        # 检查 1: 硬止损
        if profit_pct <= self.hard_stop_loss:
            return RiskAlert(
                signal_type=RiskSignal.STOP_LOSS,
                code=code,
                name=name,
                current_price=current_price,
                buy_price=buy_price,
                profit_pct=profit_pct,
                message=f"触发硬止损 ({self.hard_stop_loss*100:.0f}%)，建议立即卖出",
                level=1
            )

        # 检查 2: 移动止盈
        if profit_pct >= self.take_profit_start:
            # 计算从最高点的回撤
            drawdown = (highest_price - current_price) / highest_price

            if drawdown >= self.trailing_stop:
                return RiskAlert(
                    signal_type=RiskSignal.TRAILING_STOP,
                    code=code,
                    name=name,
                    current_price=current_price,
                    buy_price=buy_price,
                    profit_pct=profit_pct,
                    message=f"移动止盈触发 (回撤 {drawdown*100:.1f}%)，建议卖出锁定利润",
                    level=1
                )

        return None

    def scan_all_positions(self, price_fetcher=None) -> List[RiskAlert]:
        """
        扫描所有持仓，检查风险

        Args:
            price_fetcher: 可选的实时价格获取函数

        Returns:
            风险警报列表
        """
        positions = self.db.get_holding_positions()

        if not positions:
            logger.info("📭 当前无持仓")
            return []

        alerts = []

        for pos in positions:
            # 获取当前价格（如果有 fetcher 则获取实时价，否则用数据库价格）
            current_price = pos.current_price

            if price_fetcher:
                try:
                    current_price = price_fetcher(pos.code)
                except Exception as e:
                    logger.error(f'Critical Logical Gap: {str(e)}', exc_info=True)
            alert = self.check_position(
                code=pos.code,
                name=pos.name,
                buy_price=pos.buy_price,
                current_price=current_price,
                highest_price=pos.highest_price or pos.buy_price
            )

            if alert:
                alerts.append(alert)
                logger.warning(f"⚠️ {alert.signal_type.value}: {pos.code} {pos.name}")

        return alerts

    def send_alerts(self, alerts: List[RiskAlert]) -> int:
        """
        发送风险警报

        Args:
            alerts: 警报列表

        Returns:
            成功发送数量
        """
        sent = 0

        for alert in alerts:
            try:
                title = f"{alert.signal_type.value} - {alert.code} {alert.name}"
                content = alert.to_push_content()

                if alert.level == 1:
                    self.notifier.error(title, content)
                else:
                    self.notifier.info(title, content, level=alert.level)

                sent += 1

            except Exception as e:
                logger.error(f"❌ 发送警报失败: {e}")

        return sent

    # ==================== 盘中监控 ====================

    def is_trading_time(self) -> bool:
        """检查当前是否在交易时间"""
        Config = get_config()
        now = datetime.now().time()

        # 解析交易时间
        start = dt_time(*map(int, Config.TRADE_START_TIME.split(':')))
        end = dt_time(*map(int, Config.TRADE_END_TIME.split(':')))

        # A股有午休，分两段
        morning_end = dt_time(11, 30)
        afternoon_start = dt_time(13, 0)

        is_morning = start <= now <= morning_end
        is_afternoon = afternoon_start <= now <= end

        return is_morning or is_afternoon

    def run_monitoring_cycle(self, price_fetcher=None) -> Dict:
        """
        运行一次监控周期

        Returns:
            监控结果统计
        """
        result = {
            'is_trading_time': self.is_trading_time(),
            'positions_checked': 0,
            'alerts_triggered': 0,
            'alerts_sent': 0
        }

        # 扫描持仓
        alerts = self.scan_all_positions(price_fetcher)
        result['positions_checked'] = len(self.db.get_holding_positions())
        result['alerts_triggered'] = len(alerts)

        # 发送警报
        if alerts:
            result['alerts_sent'] = self.send_alerts(alerts)

        return result

    # ==================== 证据检查 ====================

    def validate_evidence(self, evidence_level: str) -> Tuple[bool, str]:
        """
        验证证据等级是否足以支持买入

        铁律：没有 S/A 级证据，不输出 BUY 信号

        Args:
            evidence_level: 证据等级 (S/A/B/C)

        Returns:
            (是否通过, 原因)
        """
        if evidence_level in ['S', 'A']:
            return True, f"证据等级 {evidence_level}，可以买入"
        else:
            return False, f"证据等级 {evidence_level} 不足，需要 S 或 A 级证据"

    def quick_test(self) -> bool:
        """快速测试风控模块"""
        logger.info("🧪 开始风控模块测试...")

        try:
            # 测试止损检测
            alert = self.check_position(
                code='000001',
                name='测试股票',
                buy_price=10.0,
                current_price=9.2,  # -8%，触发止损
                highest_price=10.0
            )
            assert alert is not None and alert.signal_type == RiskSignal.STOP_LOSS
            logger.info("  ✅ 止损检测: 通过")

            # 测试移动止盈
            alert = self.check_position(
                code='000001',
                name='测试股票',
                buy_price=10.0,
                current_price=10.8,  # +8%
                highest_price=11.5   # 最高曾到 +15%，现在回撤超过 3%
            )
            assert alert is not None and alert.signal_type == RiskSignal.TRAILING_STOP
            logger.info("  ✅ 移动止盈: 通过")

            # 测试正常持仓（无警报）
            alert = self.check_position(
                code='000001',
                name='测试股票',
                buy_price=10.0,
                current_price=10.3,  # +3%
                highest_price=10.5
            )
            assert alert is None
            logger.info("  ✅ 正常持仓: 通过")

            # 测试仓位检查
            can_buy, reason = self.check_can_buy(100000, 30000)
            logger.info(f"  ✅ 仓位检查: {can_buy} - {reason}")

            logger.info("✅ 风控模块测试通过!")
            return True

        except Exception as e:
            logger.error(f"❌ 风控模块测试失败: {e}")
            return False


# 便捷函数
def get_risk_control() -> RiskControl:
    """获取风控模块实例"""
    return RiskControl()
