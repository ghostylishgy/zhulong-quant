# ============================================================
# 🐲 烛龙计划 v2 - SQLAlchemy 数据模型定义
# ============================================================
# 定义所有数据表结构，包含 v1 基础表 + v2 新增表
# Phase 1.6: 基于物理核盘结果修正核心模型
# ============================================================

from datetime import datetime, date
from typing import Optional

from sqlalchemy import (
    Column, Integer, String, Float, Date, DateTime,
    Boolean, Text, Index, ForeignKey, JSON, Enum
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import enum


Base = declarative_base()


# ==================== v1 基础模型 (继承) ====================

class Stock(Base):
    """股票基础信息表"""
    __tablename__ = 'stock_basic'

    ts_code = Column(String(10), primary_key=True)
    name = Column(String(50), nullable=False)
    industry = Column(String(50), nullable=False)
    market = Column(String(10), nullable=False)
    list_date = Column(String(8), nullable=False)
    is_st = Column(Integer, nullable=False, default=0)

    def __repr__(self):
        return f"<Stock {self.ts_code} {self.name}>"


class DailyPrice(Base):
    """日K线数据表"""
    __tablename__ = 'stock_daily'

    symbol = Column(String(10), primary_key=True)
    trade_date = Column(String(8), primary_key=True)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)
    amount = Column(Float, nullable=False)
    pct_chg = Column(Float, nullable=True)
    turnover = Column(Float, nullable=True)

    def __repr__(self):
        return f"<DailyPrice {self.symbol} {self.trade_date} close={self.close}>"


class Signal(Base):
    """交易信号记录表"""
    __tablename__ = 'signals'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    name = Column(String(50))
    signal_type = Column(String(10), nullable=False)
    strategy = Column(String(50))
    evidence_level = Column(String(5))
    reason = Column(Text)
    price = Column(Float)
    created_at = Column(DateTime, default=datetime.now)
    is_executed = Column(Boolean, default=False)
    executed_at = Column(DateTime)

    def __repr__(self):
        return f"<Signal {self.signal_type} {self.code} {self.created_at}>"


class Position(Base):
    """持仓记录表"""
    __tablename__ = 'positions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    name = Column(String(50))
    buy_price = Column(Float, nullable=False)
    buy_date = Column(Date, nullable=False)
    quantity = Column(Integer, nullable=False)
    current_price = Column(Float)
    profit_pct = Column(Float)
    highest_price = Column(Float)
    status = Column(String(10), default='HOLDING')
    sell_price = Column(Float)
    sell_date = Column(Date)
    sell_reason = Column(String(50))
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    def __repr__(self):
        return f"<Position {self.code} qty={self.quantity} profit={self.profit_pct:.2%}>"


# ==================== v2 新增模型 ====================

class RPSResult(Base):
    """RPS计算结果缓存表

    缓存每日的RPS计算结果，避免重复计算
    """
    __tablename__ = 'rps_results'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    trade_date = Column(Date, nullable=False, index=True)  # 计算基准日期

    # RPS 值（标准 Minervini 公式）
    rps_50 = Column(Integer)  # 50日RPS
    rps_120 = Column(Integer)  # 120日RPS
    rps_250 = Column(Integer)  # 250日RPS
    rps_avg = Column(Integer)  # 平均RPS

    # 涨跌幅（用于验证）
    return_50 = Column(Float)  # 50日涨跌幅
    return_120 = Column(Float)  # 120日涨跌幅
    return_250 = Column(Float)  # 250日涨跌幅

    # 统计信息
    rank_50 = Column(Integer)  # 50日排名
    rank_120 = Column(Integer)  # 120日排名
    rank_250 = Column(Integer)  # 250日排名
    total_count = Column(Integer)  # 市场总股票数

    created_at = Column(DateTime, default=datetime.now)

    # 复合索引
    __table_args__ = (
        Index('idx_code_date_rps', 'code', 'trade_date', unique=True),
        Index('idx_trade_date_avg', 'trade_date', 'rps_avg'),
    )

    def __repr__(self):
        return f"<RPSResult {self.code} {self.trade_date} avg={self.rps_avg}>"


class ROEAuditRecord(Base):
    """ROE审计记录表

    记录股票的三年ROE数据，判断是否符合"ROE持续>10%"的红线
    """
    __tablename__ = 'roe_audit_records'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    name = Column(String(50))

    # ROE 数据（最近3年）
    roe_year1 = Column(Float, nullable=True)  # 最新年度
    roe_year2 = Column(Float, nullable=True)  # 前一年度
    roe_year3 = Column(Float, nullable=True)  # 前两年度

    # 年份标签
    year1 = Column(Integer, nullable=True)
    year2 = Column(Integer, nullable=True)
    year3 = Column(Integer, nullable=True)

    # 审计结果
    roe_avg = Column(Float, nullable=True)  # 三年平均ROE
    roe_pass = Column(Boolean, default=False)  # 是否通过审计（平均>10%）
    roe_score = Column(Integer, default=0)  # ROE得分（10-15%:1, 15-20%:2, >20%:3）

    # 趋势判断
    roe_trend = Column(String(10), nullable=True)  # UP/DOWN/FLAT

    # 数据来源
    data_source = Column(String(20), default='akshare')  # akshare/manual
    report_date = Column(Date, nullable=True)  # 财报发布日期

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # 复合索引
    __table_args__ = (
        Index('idx_code_audit_date', 'code', 'updated_at'),
        Index('idx_audit_pass_date', 'roe_pass', 'updated_at'),
    )

    def __repr__(self):
        return f"<ROEAuditRecord {self.code} avg={self.roe_avg:.2%} pass={self.roe_pass}>"


class CashFlowCheck(Base):
    """现金流背离检查表

    检测净利润与经营现金流的背离度，判断财务真实性
    """
    __tablename__ = 'cashflow_checks'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    name = Column(String(50))

    # 财务数据（最新季度/年度）
    report_period = Column(String(10), nullable=True)  # 报告期：2024Q3/2024
    net_profit = Column(Float, nullable=True)  # 净利润（万元）
    operating_cashflow = Column(Float, nullable=True)  # 经营现金流（万元）

    # 背离度计算
    divergence = Column(Float, nullable=True)  # 背离度 = |净利润-经营现金流| / max(|净利润|, |经营现金流|)

    # 风险判断
    risk_level = Column(String(10), nullable=True)  # LOW/MEDIUM/HIGH
    risk_score = Column(Integer, default=0)  # 风险评分（0-100，越高越危险）
    is_divergent = Column(Boolean, default=False)  # 是否存在背离（>30%）

    # 风险类型
    divergence_type = Column(String(20), nullable=True)  # PROFIT_POSITIVE_CASH_NEGATIVE / PROFIT_NEGATIVE_CASH_POSITIVE / BOTH_POSITIVE_GAP / BOTH_NEGATIVE_GAP

    # 数据来源
    data_source = Column(String(20), default='akshare')
    report_date = Column(Date, nullable=True)

    # 审计结论
    audit_notes = Column(Text, nullable=True)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # 复合索引
    __table_args__ = (
        Index('idx_code_check_date', 'code', 'updated_at'),
        Index('idx_risk_level', 'risk_level', 'is_divergent'),
    )

    def __repr__(self):
        return f"<CashFlowCheck {self.code} div={self.divergence:.2%} risk={self.risk_level}>"


class AIPromptLog(Base):
    """AI调用日志表

    记录所有AI调用，用于成本控制和审计
    """
    __tablename__ = 'ai_prompt_logs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=True, index=True)

    # 调用信息
    ai_provider = Column(String(20), nullable=False)  # ollama/deepseek/zhipu
    ai_model = Column(String(50), nullable=False)  # deepseek-r1:7b / deepseek-chat
    ai_role = Column(String(20), nullable=False)  # librarian/commander/prosecutor

    # Token 统计
    input_tokens = Column(Integer, default=0)
    output_tokens = Column(Integer, default=0)
    total_tokens = Column(Integer, default=0)

    # 成本估算（DeepSeek按Token计费）
    estimated_cost = Column(Float, default=0.0)  # 单位：元

    # 调用结果
    is_success = Column(Boolean, default=False)
    response_time = Column(Float, nullable=True)  # 响应时间（秒）
    error_message = Column(Text, nullable=True)

    # 内容摘要（用于缓存key）
    content_hash = Column(String(32), nullable=True, index=True)  # MD5哈希，用于去重

    # 详细内容（可选存储，避免过大可留空）
    prompt = Column(Text, nullable=True)
    response = Column(Text, nullable=True)

    created_at = Column(DateTime, default=datetime.now)

    # 复合索引
    __table_args__ = (
        Index('idx_provider_model', 'ai_provider', 'ai_model'),
        Index('idx_content_hash_date', 'content_hash', 'created_at'),
        Index('idx_aipromptlogs_created_at', 'created_at'),
    )

    def __repr__(self):
        return f"<AIPromptLog {self.ai_provider}/{self.ai_model} tokens={self.total_tokens}>"


class SyncTask(Base):
    """数据同步任务表

    记录每次数据同步任务的执行情况
    """
    __tablename__ = 'sync_tasks'

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_type = Column(String(20), nullable=False)  # stock_list/daily_prices/rps_calculation
    task_status = Column(String(20), nullable=False)  # RUNNING/SUCCESS/FAILED

    # 统计信息
    total_count = Column(Integer, default=0)  # 总数量
    success_count = Column(Integer, default=0)  # 成功数量
    failed_count = Column(Integer, default=0)  # 失败数量
    skipped_count = Column(Integer, default=0)  # 跳过数量

    # 时间统计
    start_time = Column(DateTime, nullable=True)
    end_time = Column(DateTime, nullable=True)
    duration_seconds = Column(Float, nullable=True)

    # 并发配置
    worker_count = Column(Integer, default=1)
    batch_size = Column(Integer, default=100)

    # 错误信息
    error_message = Column(Text, nullable=True)

    # 额外信息（JSON格式）
    extra_info = Column(JSON, nullable=True)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # 复合索引
    __table_args__ = (
        Index('idx_type_status_time', 'task_type', 'task_status', 'created_at'),
        Index('idx_synctasks_created_at', 'created_at'),
    )

    def __repr__(self):
        return f"<SyncTask {self.task_type} {self.task_status} {self.success_count}/{self.total_count}>"


class RiskAlertLog(Base):
    """风控警报日志表

    记录所有触发的风控警报
    """
    __tablename__ = 'risk_alert_logs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    name = Column(String(50))

    # 警报类型
    alert_type = Column(String(30), nullable=False)  # STOP_LOSS/TAKE_PROFIT/TRAILING_STOP/POSITION_LIMIT/DAILY_LIMIT/ROE_DECLINE/CASHFLOW_RISK

    # 警报详情
    current_price = Column(Float, nullable=True)
    buy_price = Column(Float, nullable=True)
    profit_pct = Column(Float, nullable=True)
    trigger_value = Column(Float, nullable=True)  # 触发值（如-7%止损）

    # 风险等级
    risk_level = Column(String(10), nullable=False)  # LOW/MEDIUM/HIGH/CRITICAL

    # 处理状态
    alert_status = Column(String(20), default='PENDING')  # PENDING/ACKNOWLEDGED/DISMISSED/EXECUTED
    action_taken = Column(String(50), nullable=True)  # 采取的行动

    # 推送状态
    is_notified = Column(Boolean, default=False)
    notified_at = Column(DateTime, nullable=True)

    # 额外信息
    alert_message = Column(Text, nullable=True)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # 复合索引
    __table_args__ = (
        Index('idx_code_alert_type', 'code', 'alert_type', 'created_at'),
        Index('idx_status_risk', 'alert_status', 'risk_level'),
        Index('idx_riskalertlogs_created_at', 'created_at'),
    )

    def __repr__(self):
        return f"<RiskAlertLog {self.code} {self.alert_type} {self.risk_level}>"


# ==================== 枚举类型定义 ====================

class TaskStatus(enum.Enum):
    """任务状态"""
    PENDING = 'PENDING'
    RUNNING = 'RUNNING'
    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'
    CANCELLED = 'CANCELLED'


class RiskLevel(enum.Enum):
    """风险等级"""
    LOW = 'LOW'
    MEDIUM = 'MEDIUM'
    HIGH = 'HIGH'
    CRITICAL = 'CRITICAL'


class AIProvider(enum.Enum):
    """AI提供商"""
    OLLAMA = 'ollama'
    DEEPSEEK = 'deepseek'
    ZHIPU = 'zhipu'


class AIRole(enum.Enum):
    """AI角色"""
    LIBRARIAN = 'librarian'
    COMMANDER = 'commander'
    PROSECUTOR = 'prosecutor'
    BLOGGER = 'blogger'


# ==================== 表创建函数 ====================

def create_all_tables(engine):
    """创建所有表"""
    Base.metadata.create_all(bind=engine)


def drop_all_tables(engine):
    """删除所有表（慎用）"""
    Base.metadata.drop_all(bind=engine)

# ============================================================
# 🐲 烛龙 v1.8 - 核心模型层全量部署完成 | Phase 1.6 修正
# ============================================================


# ==================== v1.8 新增模型 ====================

class FunnelSnapshot(Base):
    """筛选漏斗快照表

    记录每日筛选漏斗各层级的结果，支持漏斗可视化
    """
    __tablename__ = 'funnel_snapshots'

    id = Column(Integer, primary_key=True, autoincrement=True)
    trade_date = Column(Date, nullable=False, index=True)
    track_type = Column(String(20), nullable=False)

    layer_name = Column(String(50), nullable=False)
    layer_order = Column(Integer, default=0)

    before_count = Column(Integer, default=0)
    after_count = Column(Integer, default=0)

    stock_codes = Column(JSON, nullable=True)
    filter_criteria = Column(JSON, nullable=True)

    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        Index('idx_date_track', 'trade_date', 'track_type'),
        Index('idx_date_layer', 'trade_date', 'layer_order'),
    )

    def __repr__(self):
        return f'<FunnelSnapshot {self.trade_date} {self.track_type} {self.layer_name} {self.before_count}->{self.after_count}>'


class BacktestResult(Base):
    """回测结果表

    记录策略回测结果
    """
    __tablename__ = 'backtest_results'

    id = Column(Integer, primary_key=True, autoincrement=True)
    strategy_name = Column(String(100), nullable=False)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)

    total_return = Column(Float, default=0.0)
    sharpe_ratio = Column(Float, default=0.0)
    max_drawdown = Column(Float, default=0.0)
    win_rate = Column(Float, default=0.0)
    trade_count = Column(Integer, default=0)

    params = Column(JSON, nullable=True)
    timestamp = Column(DateTime, default=datetime.now)

    __table_args__ = (
        Index('idx_strategy_timestamp', 'strategy_name', 'timestamp'),
    )

    def __repr__(self):
        return f'<BacktestResult {self.strategy_name} {self.total_return:.2%}>'


class PortfolioRecord(Base):
    """组合记录表

    记录组合状态变化
    """
    __tablename__ = 'portfolio_records'

    id = Column(Integer, primary_key=True, autoincrement=True)
    strategy_name = Column(String(100), nullable=False)
    timestamp = Column(DateTime, nullable=False, index=True)

    total_value = Column(Float, default=0.0)
    cash = Column(Float, default=0.0)
    positions_count = Column(Integer, default=0)

    pnl = Column(Float, default=0.0)
    pnl_pct = Column(Float, default=0.0)

    positions = Column(JSON, nullable=True)

    __table_args__ = (
        Index('idx_strategy_timestamp', 'strategy_name', 'timestamp'),
    )

    def __repr__(self):
        return f'<PortfolioRecord {self.strategy_name} {self.total_value:.2f}>'
