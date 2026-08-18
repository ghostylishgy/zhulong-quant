# ============================================================
# 🐲 烛龙计划 v2 - 高性能数据库封装
# ============================================================
# 基于 bulk_insert_mappings 实现批量写入
# 包含连接池管理和事务控制
# ============================================================

import logging
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any, Tuple
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

from .models import (
    Base, Stock, DailyPrice, Signal, Position,
    RPSResult, ROEAuditRecord, CashFlowCheck,
    AIPromptLog, SyncTask, RiskAlertLog
)
from config.settings import Config

logger = logging.getLogger('zhulong.database_v2')


class Database:
    """
    烛龙 v2 数据库管理器

    核心特性：
    1. 使用 bulk_insert_mappings 实现高性能批量写入
    2. 连接池管理（QueuePool）
    3. 事务自动控制
    4. 异常处理和重试机制

    性能目标：
    - 批量写入速度：>1000条/秒
    - 连接复用率：>90%
    - 查询响应时间：<100ms
    """

    _instance = None

    def __new__(cls):
        """单例模式，确保全局只有一个数据库连接池"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self.engine = None
        self.SessionLocal = None
        self._initialized = True
        self._init_engine()

    def _init_engine(self):
        """初始化数据库引擎和连接池"""
        db_path = Config.DB_PATH

        # 确保 db_path 的父目录存在
        db_path.parent.mkdir(parents=True, exist_ok=True)

        # 构建 SQLAlchemy URL
        db_url = f'sqlite:///{db_path}'

        # 创建引擎（带连接池）
        self.engine = create_engine(
            db_url,
            echo=Config.DB_ECHO,
            connect_args={
                'check_same_thread': False,  # 允许多线程访问
                'timeout': Config.DB_POOL_TIMEOUT,
            },
            poolclass=QueuePool,
            pool_size=Config.DB_POOL_SIZE,
            max_overflow=Config.DB_MAX_OVERFLOW,
            pool_timeout=Config.DB_POOL_TIMEOUT,
            pool_recycle=3600,  # 1小时回收连接
            pool_pre_ping=True,  # 连接前ping检查
        )

        # 创建所有表
        Base.metadata.create_all(bind=self.engine)

        # 创建会话工厂
        self.SessionLocal = sessionmaker(
            bind=self.engine,
            autocommit=False,
            autoflush=False
        )

        logger.info(f"📦 数据库引擎初始化完成: {db_path}")
        logger.info(f"   连接池: size={Config.DB_POOL_SIZE}, max_overflow={Config.DB_MAX_OVERFLOW}")

    @contextmanager
    def get_session(self) -> Session:
        """
        获取数据库会话（上下文管理器）

        使用示例：
            with db.get_session() as session:
                result = session.query(Stock).all()

        Returns:
            Session: SQLAlchemy 会话对象
        """
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"数据库操作失败，已回滚: {e}")
            raise
        finally:
            session.close()

    # ==================== 高性能批量写入 ====================

    def bulk_insert_stock_list(self, stocks: List[Dict[str, Any]],
                                on_conflict='update') -> Tuple[int, int]:
        """
        批量插入/更新股票列表

        【P0关键】使用 bulk_insert_mappings 实现高性能写入

        Args:
            stocks: 股票数据列表，每个元素是字典
                [{"code": "000001", "name": "平安银行", "market": "SZ", ...}, ...]
            on_conflict: 冲突处理策略
                'update': 更新已存在的记录（默认）
                'ignore': 忽略重复记录
                'replace': 删除并重新插入

        Returns:
            (inserted_count, updated_count)
        """
        if not stocks:
            logger.warning("bulk_insert_stock_list: 输入为空")
            return 0, 0

        start_time = datetime.now()
        inserted = 0
        updated = 0

        with self.get_session() as session:
            # 查询已存在的股票
            existing_codes = set(
                row[0] for row in session.query(Stock.code)
                .filter(Stock.code.in_([s['code'] for s in stocks]))
                .all()
            )

            # 分批处理（避免内存溢出）
            batch_size = Config.SYNC_STOCK_BATCH_SIZE

            for i in range(0, len(stocks), batch_size):
                batch = stocks[i:i + batch_size]

                if on_conflict == 'ignore':
                    # 只插入不存在的
                    new_stocks = [s for s in batch if s['code'] not in existing_codes]
                    if new_stocks:
                        session.bulk_insert_mappings(Stock, new_stocks)
                        inserted += len(new_stocks)
                elif on_conflict == 'replace':
                    # 删除已存在的，重新插入
                    batch_codes = [s['code'] for s in batch]
                    session.query(Stock).filter(Stock.code.in_(batch_codes)).delete(
                        synchronize_session=False
                    )
                    session.bulk_insert_mappings(Stock, batch)
                    inserted += len(batch)
                else:  # update
                    # 分别处理
                    for stock in batch:
                        if stock['code'] in existing_codes:
                            # 更新
                            session.query(Stock).filter(Stock.code == stock['code']).update(
                                stock, synchronize_session=False
                            )
                            updated += 1
                        else:
                            # 插入
                            session.add(Stock(**stock))
                            inserted += 1

                # 每10批提交一次
                if (i // batch_size) % 10 == 0:
                    session.commit()

        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(
            f"📦 股票列表批量写入: 插入={inserted}, 更新={updated}, "
            f"耗时={elapsed:.2f}s, 速度={(inserted+updated)/elapsed:.0f}条/秒"
        )

        return inserted, updated

    def bulk_insert_prices(self, prices: List[Dict[str, Any]],
                          on_conflict='upsert') -> Tuple[int, int]:
        """
        批量插入/更新K线数据（支持真正的 UPSERT）

        【P0关键】使用 bulk_insert_mappings 实现高性能写入

        Args:
            prices: K线数据列表
            on_conflict: 冲突处理策略
                - 'upsert': 如果不存在则插入，如果已存在则更新（默认）
                - 'ignore': 只插入不存在的，跳过已存在的
                - 'replace': 删除已存在的，重新插入

        Returns:
            (inserted_count, updated_count)
        """
        if not prices:
            return 0, 0

        start_time = datetime.now()
        inserted = 0
        updated = 0

        with self.get_session() as session:
            # 分批处理
            batch_size = Config.SYNC_PRICE_BATCH_SIZE

            for i in range(0, len(prices), batch_size):
                batch = prices[i:i + batch_size]

                if on_conflict == 'ignore':
                    # 策略1: 只插入不存在的（使用批量查询优化）
                    batch_pairs = [(p['code'], p['trade_date']) for p in batch]
                    existing = set(
                        (row.code, row.trade_date)
                        for row in session.query(DailyPrice.code, DailyPrice.trade_date)
                        .filter(
                            DailyPrice.code.in_([p[0] for p in batch_pairs]),
                            DailyPrice.trade_date.in_([p[1] for p in batch_pairs])
                        )
                        .all()
                    )

                    # 过滤已存在的
                    new_prices = [
                        p for p in batch
                        if (p['code'], p['trade_date']) not in existing
                    ]

                    if new_prices:
                        session.bulk_insert_mappings(DailyPrice, new_prices)
                        inserted += len(new_prices)
                    updated = len(batch) - len(new_prices)
                    logger.debug(f"  批次 {i//batch_size}: 插入={len(new_prices)}, 跳过={updated}")

                elif on_conflict == 'replace':
                    # 策略2: 删除已存在的，重新插入（全量覆盖）
                    batch_pairs = [(p['code'], p['trade_date']) for p in batch]

                    # 批量删除已存在的记录
                    deleted = session.query(DailyPrice).filter(
                        DailyPrice.code.in_([p[0] for p in batch_pairs]),
                        DailyPrice.trade_date.in_([p[1] for p in batch_pairs])
                    ).delete(synchronize_session=False)

                    # 批量插入新记录
                    session.bulk_insert_mappings(DailyPrice, batch)
                    inserted += len(batch)
                    updated += deleted
                    logger.debug(f"  批次 {i//batch_size}: 插入={len(batch)}, 替换={deleted}")

                else:  # 'upsert' 默认策略
                    # 策略3: 真正的 UPSERT（不存在则插入，已存在则更新）
                    # SQLite 不支持原生的 INSERT ... ON CONFLICT UPDATE
                    # 实现方式：批量查询 → 分离新旧 → 批量插入 → 批量删除 → 批量插入

                    batch_pairs = [(p['code'], p['trade_date']) for p in batch]

                    # 批量查询已存在的记录
                    existing = set(
                        (row.code, row.trade_date)
                        for row in session.query(DailyPrice.code, DailyPrice.trade_date)
                        .filter(
                            DailyPrice.code.in_([p[0] for p in batch_pairs]),
                            DailyPrice.trade_date.in_([p[1] for p in batch_pairs])
                        )
                        .all()
                    )

                    # 分离新记录和已存在记录
                    new_prices = [
                        p for p in batch
                        if (p['code'], p['trade_date']) not in existing
                    ]
                    existing_prices = [
                        p for p in batch
                        if (p['code'], p['trade_date']) in existing
                    ]

                    # 批量插入新记录
                    if new_prices:
                        session.bulk_insert_mappings(DailyPrice, new_prices)
                        inserted += len(new_prices)
                        logger.debug(f"  批次 {i//batch_size}: 新增={len(new_prices)}")

                    # 批量更新已存在记录（删除旧记录 + 插入新记录）
                    if existing_prices:
                        existing_pairs = [(p['code'], p['trade_date']) for p in existing_prices]

                        # 删除旧记录
                        deleted = session.query(DailyPrice).filter(
                            DailyPrice.code.in_([p[0] for p in existing_pairs]),
                            DailyPrice.trade_date.in_([p[1] for p in existing_pairs])
                        ).delete(synchronize_session=False)

                        # 插入新记录
                        session.bulk_insert_mappings(DailyPrice, existing_prices)
                        updated += deleted
                        logger.debug(f"  批次 {i//batch_size}: 更新={deleted}")

                # 每5批提交一次
                if (i // batch_size) % 5 == 0:
                    session.commit()

        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(
            f"📦 K线批量写入 [{on_conflict}]: 插入={inserted}, 更新={updated}, "
            f"耗时={elapsed:.2f}s, 速度={(inserted+updated)/elapsed:.0f}条/秒"
        )

        return inserted, updated

    def bulk_insert_rps_results(self, rps_results: List[Dict[str, Any]],
                                 on_conflict='replace') -> Tuple[int, int]:
        """
        批量插入RPS计算结果

        Args:
            rps_results: RPS结果列表
            on_conflict: 冲突处理策略

        Returns:
            (inserted_count, updated_count)
        """
        if not rps_results:
            return 0, 0

        start_time = datetime.now()

        with self.get_session() as session:
            if on_conflict == 'replace':
                # 删除同一trade_date的所有数据
                trade_dates = set(r['trade_date'] for r in rps_results)
                session.query(RPSResult).filter(
                    RPSResult.trade_date.in_(trade_dates)
                ).delete(synchronize_session=False)

                session.bulk_insert_mappings(RPSResult, rps_results)
                inserted = len(rps_results)
                updated = 0
            else:
                # 批量插入（忽略重复）
                session.bulk_insert_mappings(RPSResult, rps_results,
                                             return_defaults=False)
                inserted = len(rps_results)
                updated = 0

        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(
            f"📊 RPS结果批量写入: {inserted}条, 耗时={elapsed:.2f}s"
        )

        return inserted, updated

    def bulk_insert_roe_audits(self, audits: List[Dict[str, Any]],
                               on_conflict='replace') -> Tuple[int, int]:
        """批量插入ROE审计记录"""
        if not audits:
            return 0, 0

        with self.get_session() as session:
            if on_conflict == 'replace':
                codes = [a['code'] for a in audits]
                session.query(ROEAuditRecord).filter(
                    ROEAuditRecord.code.in_(codes)
                ).delete(synchronize_session=False)
                session.bulk_insert_mappings(ROEAuditRecord, audits)
                inserted = len(audits)
            else:
                session.bulk_insert_mappings(ROEAuditRecord, audits)
                inserted = len(audits)
            updated = 0

        logger.info(f"📊 ROE审计批量写入: {inserted}条")
        return inserted, updated

    def bulk_insert_cashflow_checks(self, checks: List[Dict[str, Any]],
                                    on_conflict='replace') -> Tuple[int, int]:
        """批量插入现金流检查记录"""
        if not checks:
            return 0, 0

        with self.get_session() as session:
            if on_conflict == 'replace':
                codes = [c['code'] for c in checks]
                session.query(CashFlowCheck).filter(
                    CashFlowCheck.code.in_(codes)
                ).delete(synchronize_session=False)
                session.bulk_insert_mappings(CashFlowCheck, checks)
                inserted = len(checks)
            else:
                session.bulk_insert_mappings(CashFlowCheck, checks)
                inserted = len(checks)
            updated = 0

        logger.info(f"📊 现金流检查批量写入: {inserted}条")
        return inserted, updated

    # ==================== 查询方法 ====================

    def get_all_stocks(self) -> List[Dict]:
        """获取所有股票"""
        with self.get_session() as session:
            stocks = session.query(Stock).all()
            return [
                {
                    'ts_code': s.ts_code,
                    'name': s.name,
                    'market': s.market,
                    'industry': s.industry,
                    'is_st': s.is_st,
                    'list_date': s.list_date
                }
                for s in stocks
            ]

    def get_stock(self, code: str) -> Optional[Stock]:
        """根据代码获取股票"""
        with self.get_session() as session:
            return session.query(Stock).filter(Stock.code == code).first()

    def get_stock_count(self) -> int:
        """获取股票数量"""
        with self.get_session() as session:
            return session.query(func.count(Stock.id)).scalar()

    def get_prices(self, code: str, start_date: date = None,
                   end_date: date = None, limit: int = None) -> List[DailyPrice]:
        """获取指定股票的K线数据"""
        with self.get_session() as session:
            query = session.query(DailyPrice).filter(DailyPrice.code == code)

            if start_date:
                query = query.filter(DailyPrice.trade_date >= start_date)
            if end_date:
                query = query.filter(DailyPrice.trade_date <= end_date)

            query = query.order_by(DailyPrice.trade_date.desc())

            if limit:
                query = query.limit(limit)

            return query.all()

    def get_latest_price_date(self, code: str) -> Optional[date]:
        """获取某只股票最新的K线日期"""
        with self.get_session() as session:
            result = session.query(DailyPrice.trade_date)\
                .filter(DailyPrice.code == code)\
                .order_by(DailyPrice.trade_date.desc())\
                .first()
            return result[0] if result else None

    def get_latest_prices(self, codes: List[str]) -> Dict[str, DailyPrice]:
        """
        批量获取多只股票的最新价格

        Returns:
            Dict[code, DailyPrice]
        """
        with self.get_session() as session:
            # 使用窗口函数获取每只股票的最新记录
            subq = session.query(
                DailyPrice.code,
                DailyPrice.trade_date,
                func.row_number().over(
                    partition_by=DailyPrice.code,
                    order_by=DailyPrice.trade_date.desc()
                ).label('rn')
            ).filter(DailyPrice.code.in_(codes))\
             .subquery()

            latest = session.query(DailyPrice).join(
                subq,
                (DailyPrice.code == subq.c.code) &
                (DailyPrice.trade_date == subq.c.trade_date)
            ).filter(subq.c.rn == 1).all()

            return {p.code: p for p in latest}

    def get_holding_positions(self) -> List[Position]:
        """获取当前持仓"""
        with self.get_session() as session:
            return session.query(Position)\
                .filter(Position.status == 'HOLDING')\
                .all()

    def get_pending_signals(self) -> List[Signal]:
        """获取待执行的信号"""
        with self.get_session() as session:
            return session.query(Signal)\
                .filter(Signal.is_executed == False)\
                .order_by(Signal.created_at.desc())\
                .all()

    def get_rps_result(self, code: str, trade_date: date = None) -> Optional[RPSResult]:
        """
        获取RPS计算结果

        Args:
            code: 股票代码
            trade_date: 计算基准日期（默认最新）

        Returns:
            RPSResult
        """
        with self.get_session() as session:
            query = session.query(RPSResult).filter(RPSResult.code == code)

            if trade_date:
                query = query.filter(RPSResult.trade_date == trade_date)
            else:
                query = query.order_by(RPSResult.trade_date.desc())

            return query.first()

    def get_roe_audit(self, code: str) -> Optional[ROEAuditRecord]:
        """获取ROE审计记录（最新）"""
        with self.get_session() as session:
            return session.query(ROEAuditRecord)\
                .filter(ROEAuditRecord.code == code)\
                .order_by(ROEAuditRecord.updated_at.desc())\
                .first()

    def get_cashflow_check(self, code: str) -> Optional[CashFlowCheck]:
        """获取现金流检查记录（最新）"""
        with self.get_session() as session:
            return session.query(CashFlowCheck)\
                .filter(CashFlowCheck.code == code)\
                .order_by(CashFlowCheck.updated_at.desc())\
                .first()

    # ==================== 信号和持仓操作 ====================

    def add_signal(self, code: str, name: str, signal_type: str,
                   strategy: str, evidence_level: str = None,
                   reason: str = None, price: float = None) -> Signal:
        """添加交易信号"""
        with self.get_session() as session:
            signal = Signal(
                code=code, name=name, signal_type=signal_type,
                strategy=strategy, evidence_level=evidence_level,
                reason=reason, price=price
            )
            session.add(signal)
            session.commit()
            session.refresh(signal)
            logger.info(f"📊 新信号: {signal_type} {code} {name}")
            return signal

    def add_position(self, code: str, name: str, buy_price: float,
                     buy_date: date, quantity: int) -> Position:
        """添加持仓"""
        with self.get_session() as session:
            position = Position(
                code=code, name=name, buy_price=buy_price,
                buy_date=buy_date, quantity=quantity,
                current_price=buy_price, highest_price=buy_price,
                profit_pct=0.0
            )
            session.add(position)
            session.commit()
            session.refresh(position)
            return position

    def update_position_price(self, code: str, current_price: float):
        """更新持仓当前价格"""
        with self.get_session() as session:
            positions = session.query(Position).filter(
                Position.code == code,
                Position.status == 'HOLDING'
            ).all()

            for pos in positions:
                pos.current_price = current_price
                pos.profit_pct = (current_price - pos.buy_price) / pos.buy_price
                if current_price > pos.highest_price:
                    pos.highest_price = current_price
                pos.updated_at = datetime.now()

            session.commit()

    # ==================== 日志和任务操作 ====================

    def log_ai_call(self, ai_provider: str, ai_model: str, ai_role: str,
                    input_tokens: int, output_tokens: int,
                    is_success: bool, response_time: float = None,
                    error_message: str = None, content_hash: str = None,
                    code: str = None) -> AIPromptLog:
        """记录AI调用日志"""
        with self.get_session() as session:
            log = AIPromptLog(
                code=code,
                ai_provider=ai_provider,
                ai_model=ai_model,
                ai_role=ai_role,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                total_tokens=input_tokens + output_tokens,
                is_success=is_success,
                response_time=response_time,
                error_message=error_message,
                content_hash=content_hash
            )
            session.add(log)
            session.commit()
            return log

    def create_sync_task(self, task_type: str, total_count: int = 0,
                         worker_count: int = 1) -> SyncTask:
        """创建同步任务"""
        with self.get_session() as session:
            task = SyncTask(
                task_type=task_type,
                task_status='RUNNING',
                total_count=total_count,
                start_time=datetime.now(),
                worker_count=worker_count
            )
            session.add(task)
            session.commit()
            session.refresh(task)
            return task

    def update_sync_task(self, task_id: int, success_count: int = 0,
                         failed_count: int = 0, skipped_count: int = 0,
                         task_status: str = None, error_message: str = None):
        """更新同步任务"""
        with self.get_session() as session:
            task = session.query(SyncTask).filter(SyncTask.id == task_id).first()
            if task:
                task.success_count = success_count
                task.failed_count = failed_count
                task.skipped_count = skipped_count

                if task_status:
                    task.task_status = task_status

                if error_message:
                    task.error_message = error_message

                if task_status in ['SUCCESS', 'FAILED', 'CANCELLED']:
                    task.end_time = datetime.now()
                    task.duration_seconds = (
                        task.end_time - task.start_time
                    ).total_seconds() if task.start_time else None

                session.commit()

    def add_risk_alert(self, code: str, name: str, alert_type: str,
                      current_price: float, buy_price: float,
                      profit_pct: float, risk_level: str,
                      alert_message: str) -> RiskAlertLog:
        """添加风控警报日志"""
        with self.get_session() as session:
            alert = RiskAlertLog(
                code=code,
                name=name,
                alert_type=alert_type,
                current_price=current_price,
                buy_price=buy_price,
                profit_pct=profit_pct,
                risk_level=risk_level,
                alert_message=alert_message
            )
            session.add(alert)
            session.commit()
            session.refresh(alert)
            return alert

    # ==================== 统计和健康检查 ====================

    def get_statistics(self) -> Dict[str, Any]:
        """获取数据库统计信息"""
        with self.get_session() as session:
            stats = {
                'stocks': session.query(func.count(Stock.id)).scalar(),
                'daily_prices': session.query(func.count(DailyPrice.id)).scalar(),
                'signals': session.query(func.count(Signal.id)).scalar(),
                'positions': session.query(
                    func.count(Position.id)
                ).filter(Position.status == 'HOLDING').scalar(),
                'rps_results': session.query(func.count(RPSResult.id)).scalar(),
                'roe_audits': session.query(func.count(ROEAuditRecord.id)).scalar(),
                'cashflow_checks': session.query(func.count(CashFlowCheck.id)).scalar(),
                'ai_logs': session.query(func.count(AIPromptLog.id)).scalar(),
                'sync_tasks': session.query(func.count(SyncTask.id)).scalar(),
                'risk_alerts': session.query(func.count(RiskAlertLog.id)).scalar(),
            }
            return stats

    def health_check(self) -> bool:
        """数据库健康检查"""
        try:
            with self.get_session() as session:
                session.execute(text('SELECT 1'))
            return True
        except Exception as e:
            logger.error(f"数据库健康检查失败: {e}")
            return False

    def get_pool_status(self) -> Dict[str, Any]:
        """获取连接池状态"""
        pool = self.engine.pool
        return {
            'pool_size': pool.size(),
            'checked_in': pool.checkedin(),
            'checked_out': pool.checkedout(),
            'overflow': pool.overflow(),
            'status': pool.status()
        }

    # ==================== 维护操作 ====================

    def vacuum(self):
        """优化数据库（SQLite特有）"""
        try:
            with self.get_session() as session:
                session.execute(text('VACUUM'))
                session.commit()
            logger.info("🧹 数据库优化完成 (VACUUM)")
        except Exception as e:
            logger.error(f"数据库优化失败: {e}")

    def cleanup_old_data(self, days: int = 90):
        """清理旧数据（AI日志、同步任务等）"""
        cutoff_date = datetime.now() - timedelta(days=days)

        with self.get_session() as session:
            # 清理旧AI日志
            deleted_logs = session.query(AIPromptLog)\
                .filter(AIPromptLog.created_at < cutoff_date)\
                .delete(synchronize_session=False)

            # 清理旧同步任务
            deleted_tasks = session.query(SyncTask)\
                .filter(SyncTask.created_at < cutoff_date)\
                .delete(synchronize_session=False)

            session.commit()

            logger.info(
                f"🧹 清理旧数据: AI日志={deleted_logs}条, 同步任务={deleted_tasks}条"
            )


# 便捷函数
def get_db() -> Database:
    """获取数据库单例"""
    return Database()


# 向后兼容
Database = Database
