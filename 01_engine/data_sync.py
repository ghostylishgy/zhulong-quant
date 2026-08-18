#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
01_engine/data_sync.py
ZhuLong v3.0 - 生产级数据同步引擎
============================================================
永久修复:
  - 直接对接 Tushare Pro → DuckDB (fact_daily)
  - 严格 15 列 Schema 对齐 (缺失列自动填 NULL)
  - daily_basic 补齐 turnover_rate
  - 增量同步 (只补缺失日期)
  - UPSERT 防重复
  - 指数退避重试
"""
import os, sys, time, logging, argparse
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import datetime, timedelta

# ====== 环境初始化 ======
CURRENT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = CURRENT_DIR.parent.resolve()
sys.path.append(str(PROJECT_ROOT))
from lib.db_gateway import DBGateway

# Load unified settings
from config.settings import Config


def _resolve_db_path() -> str:
    # Priority: explicit env override -> production DuckDB -> legacy config path
    env_db = os.getenv('DB_PATH', '').strip()
    if env_db:
        return env_db

    preferred = PROJECT_ROOT / 'storage' / 'database' / 'zhulong.duckdb'
    if preferred.exists():
        return str(preferred)

    cfg_db = Path(str(getattr(Config, 'DB_PATH', '') or ''))
    if str(cfg_db) and cfg_db.exists():
        return str(cfg_db)

    return str(preferred)


DB_PATH = _resolve_db_path()
TUSHARE_TOKEN = str(getattr(Config, 'TUSHARE_TOKEN', '') or '')

if not TUSHARE_TOKEN:
    raise RuntimeError('Config.TUSHARE_TOKEN is required for data_sync')

LOG_DIR = PROJECT_ROOT / 'logs'
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        RotatingFileHandler(str(LOG_DIR / 'data_sync.log'), maxBytes=20*1024*1024, backupCount=5, encoding='utf-8')
    ]
)
logger = logging.getLogger('data_sync')

# ====== fact_daily 15列 Schema 定义 (单一真相源) ======
FACT_DAILY_SCHEMA = [
    'symbol', 'trade_date', 'open', 'high', 'low', 'close',
    'pre_close', 'pct_chg', 'vol', 'amount',
    'turnover_rate', 'lhb_net', 'margin_delta', 'ma20', 'vol_ma5'
]

# Tushare daily() 返回的列 → 我们的列名映射
TUSHARE_DAILY_MAP = {
    'ts_code': 'symbol',
    'trade_date': 'trade_date',
    'open': 'open',
    'high': 'high',
    'low': 'low',
    'close': 'close',
    'pre_close': 'pre_close',
    'pct_chg': 'pct_chg',
    'vol': 'vol',
    'amount': 'amount',
}

MAX_RETRIES = 3
RETRY_BASE_DELAY = 2.0


def retry_with_backoff(func, *args, retries=MAX_RETRIES, **kwargs):
    """指数退避重试"""
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if attempt == retries - 1:
                raise
            delay = RETRY_BASE_DELAY * (2 ** attempt)
            logger.warning(f"  重试 {attempt+1}/{retries}: {e} (等待 {delay:.0f}s)")
            time.sleep(delay)


class DataSyncEngine:
    """生产级数据同步引擎: Tushare → DuckDB"""

    def __init__(self):
        import tushare as ts

        ts.set_token(TUSHARE_TOKEN)
        self.pro = ts.pro_api()
        self.db_path = DB_PATH
        logger.info(f"DataSync 初始化 | DB: {DB_PATH}")

    def _force_checkpoint(self, stage: str):
        """Force WAL merge after heavy writes to avoid db file bloat."""
        try:
            with DBGateway(self.db_path, read_only=False, logger=logger) as conn:
                conn.execute("CHECKPOINT;")
            logger.info(f"[{stage}] CHECKPOINT done")
        except Exception as e:
            logger.warning(f"[{stage}] CHECKPOINT failed: {e}")

    def get_missing_dates(self, lookback_days=30):
        """找出缺失的交易日"""
        with DBGateway(self.db_path, read_only=True, logger=logger) as conn:
            # 当前 DB 最新日期
            max_dt = conn.execute("SELECT MAX(trade_date) FROM fact_daily").fetchone()[0]
            max_dt_str = str(max_dt).replace('-', '')

            today = datetime.now().strftime('%Y%m%d')

            # 获取交易日历
            cal = retry_with_backoff(
                self.pro.trade_cal,
                start_date=max_dt_str, end_date=today, is_open='1'
            )
            trade_days = sorted(cal[cal['is_open'] == 1]['cal_date'].tolist())

            # 排除已有的日期
            if trade_days and trade_days[0] == max_dt_str:
                trade_days = trade_days[1:]

            # 排除今天未收盘 (15:30 之前)
            now_hour = datetime.now().hour
            now_min = datetime.now().minute
            if today in trade_days and (now_hour < 15 or (now_hour == 15 and now_min < 30)):
                trade_days.remove(today)
                logger.info(f"  排除今日 {today} (未收盘)")

        logger.info(f"DB 最新: {max_dt} | 缺失交易日: {len(trade_days)}")
        return trade_days

    def sync_daily(self, target_dates=None, lookback_days=30):
        """
        ???K??? fact_daily
        ???? 15 ? Schema
        """
        import pandas as pd

        if target_dates is None:
            target_dates = self.get_missing_dates(lookback_days)

        if not target_dates:
            logger.info("? ??????, ????")
            return 0

        logger.info(f"???? {len(target_dates)} ?: {target_dates[0]} ~ {target_dates[-1]}")

        total_inserted = 0

        # Keep external Tushare calls outside RW DB handles. Only short
        # transactions below touch DuckDB.
        with DBGateway(self.db_path, read_only=True, logger=logger) as conn:
            sample_row = conn.execute("SELECT trade_date FROM fact_daily LIMIT 1").fetchone()
        sample = str(sample_row[0]) if sample_row else ''
        use_dash = '-' in sample

        for td in target_dates:
            logger.info(f"  ?? {td} ... ", )
            try:
                # 1. ???K????
                df = retry_with_backoff(self.pro.daily, trade_date=td)
                if df is None or df.empty:
                    logger.warning(f"    ??? (?????)")
                    continue

                # 2. ?? daily_basic ? turnover_rate
                try:
                    basic = retry_with_backoff(
                        self.pro.daily_basic,
                        trade_date=td, fields='ts_code,turnover_rate'
                    )
                    if basic is not None and not basic.empty:
                        df = df.merge(basic, on='ts_code', how='left')
                except Exception as e:
                    logger.warning(f"    daily_basic ??: {e}")

                # 3. ???: ts_code ? symbol
                df = df.rename(columns={'ts_code': 'symbol'})

                # 4. ??????
                if use_dash and 'trade_date' in df.columns:
                    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')

                # 5. ?? Schema ??: ?? 15 ?????
                for col in FACT_DAILY_SCHEMA:
                    if col not in df.columns:
                        df[col] = None

                # 6. ? Schema ????, ?????
                df_insert = df[FACT_DAILY_SCHEMA].copy()

                # 7. DuckDB write in explicit short transaction.
                td_formatted = td if not use_dash else f"{td[:4]}-{td[4:6]}-{td[6:8]}"
                with DBGateway(self.db_path, read_only=False, logger=logger) as conn:
                    conn.execute('BEGIN TRANSACTION')
                    try:
                        conn.execute(
                            "DELETE FROM fact_daily WHERE trade_date = ?",
                            [td_formatted]
                        )
                        conn.insert_dataframe("fact_daily", df_insert, "df_insert")
                        conn.execute('COMMIT')
                    except Exception:
                        try:
                            conn.execute('ROLLBACK')
                        except Exception as rollback_exc:
                            logger.error(f"    rollback failed: {rollback_exc}")
                        raise

                total_inserted += len(df_insert)
                logger.info(f"    ? {len(df_insert)} ?")

            except Exception as e:
                logger.error(f"    ? {e}")

            time.sleep(0.4)  # Tushare ????

        logger.info(f"????: {total_inserted} ???")
        self._force_checkpoint("sync_daily")
        return total_inserted

    def update_derived_features(self):
        """更新衍生列: ma20, vol_ma5"""
        t0 = time.time()
        with DBGateway(self.db_path, read_only=False, logger=logger) as conn:
            conn.execute("""
        WITH target_dates AS (
            SELECT trade_date
            FROM (
                SELECT DISTINCT trade_date
                FROM fact_daily
                ORDER BY trade_date DESC
                LIMIT 10
            )
        ),
        history_dates AS (
            SELECT trade_date
            FROM (
                SELECT DISTINCT trade_date
                FROM fact_daily
                ORDER BY trade_date DESC
                LIMIT 60
            )
        ),
        calc AS (
            SELECT symbol, trade_date,
                   AVG(close) OVER (
                       PARTITION BY symbol ORDER BY trade_date
                       ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                   ) AS ma20_val,
                   AVG(vol) OVER (
                       PARTITION BY symbol ORDER BY trade_date
                       ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
                   ) AS vol5_val
            FROM fact_daily
            WHERE trade_date IN (SELECT trade_date FROM history_dates)
        )
        UPDATE fact_daily
        SET ma20 = calc.ma20_val,
            vol_ma5 = calc.vol5_val
        FROM calc
        WHERE fact_daily.symbol = calc.symbol
        AND fact_daily.trade_date = calc.trade_date
        AND fact_daily.trade_date IN (SELECT trade_date FROM target_dates)
        """)

            conn.commit()
            self._force_checkpoint("derived_features")
        elapsed = time.time() - t0
        logger.info(f"衍生特征更新完成 (ma20/vol_ma5): {elapsed:.1f}s")

    def verify(self):
        """验证数据完整性"""
        with DBGateway(self.db_path, read_only=True, logger=logger) as conn:
            max_dt = conn.execute("SELECT MAX(trade_date) FROM fact_daily").fetchone()[0]
            total = conn.execute("SELECT COUNT(*) FROM fact_daily").fetchone()[0]

            recent = conn.execute("""
            SELECT trade_date, COUNT(*) as cnt
            FROM fact_daily
            GROUP BY trade_date
            ORDER BY trade_date DESC LIMIT 5
        """).fetchall()

        logger.info(f"数据验证 | 最新: {max_dt} | 总行数: {total:,}")
        for row in recent:
            logger.info(f"  {row[0]}: {row[1]} 只")

        return max_dt, total

    def check_db_connectivity(self):
        """DB 连通性检查"""
        try:
            with DBGateway(self.db_path, read_only=True, logger=logger) as conn:
                tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
            logger.info(f"DB 连通: {len(tables)} 张表 | {tables[:5]}...")
            return True
        except Exception as e:
            logger.error(f"DB 连接失败: {e}")
            return False


def main():
    parser = argparse.ArgumentParser(description="🐲 烛龙 DataSync v3.0")
    parser.add_argument("--check-db", action="store_true", help="DB 连通性检查")
    parser.add_argument("--sync", action="store_true", help="增量同步日K数据")
    parser.add_argument("--derived", action="store_true", help="更新衍生特征")
    parser.add_argument("--full", action="store_true", help="完整流程: 同步 + 衍生")
    parser.add_argument("--verify", action="store_true", help="验证数据完整性")
    parser.add_argument("--date", type=str, help="指定同步日期 (YYYYMMDD)")
    args = parser.parse_args()

    engine = DataSyncEngine()

    if args.check_db:
        engine.check_db_connectivity()
        return

    if args.sync or args.full:
        dates = [args.date] if args.date else None
        engine.sync_daily(target_dates=dates)

    if args.derived or args.full:
        engine.update_derived_features()

    if args.verify or args.full:
        engine.verify()

    # 默认行为: 完整流程
    if not any([args.check_db, args.sync, args.derived, args.full, args.verify]):
        logger.info("执行完整同步流程 (sync + derived + verify)")
        engine.sync_daily()
        engine.update_derived_features()
        engine.verify()


if __name__ == "__main__":
    main()
