#!/usr/bin/env python3
"""
烛龙计划 - RPS ETL (v2.0)
- 通过 DBGateway 访问 DuckDB（统一连接路径，进 retry/lock telemetry）
- 缺失检测改为 (symbol, trade_date) 维度 anti-join，避免某日部分写入被视为"完成"
"""
import sys
import logging
from pathlib import Path

BASE_DIR = Path("/root/quant_project")
DB_PATH  = BASE_DIR / "storage" / "database" / "zhulong.duckdb"

# DBGateway 路径
sys.path.insert(0, str(BASE_DIR / "01_engine" / "lib"))
from db_gateway import DBGateway  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s [%(asctime)s] %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("rps_etl")


# 缺失 (symbol, trade_date) 子查询：fact_daily 中存在但 fact_rps_results 中缺失的对
_MISSING_PAIR_SQL = """
    SELECT d.symbol, d.trade_date
    FROM fact_daily d
    LEFT JOIN fact_rps_results r
      ON d.symbol = r.symbol AND d.trade_date = r.trade_date
    WHERE r.symbol IS NULL
"""


def run_rps_etl(db_path=DB_PATH, dry_run=False):
    with DBGateway(str(db_path), read_only=True, logger=log) as conn:
        # 1. 找 (symbol, trade_date) 缺失对
        miss_count = conn.execute(
            f"SELECT COUNT(*) FROM ({_MISSING_PAIR_SQL}) m"
        ).fetchone()[0]
        if not miss_count:
            log.info("fact_rps_results 已是最新，无 (symbol, trade_date) 缺失")
            return 0

        date_range = conn.execute(
            f"SELECT MIN(trade_date), MAX(trade_date) FROM ({_MISSING_PAIR_SQL}) m"
        ).fetchone()
        log.info(f"发现 {miss_count} 个缺失 (symbol, trade_date) 对，日期范围 {date_range[0]} ~ {date_range[1]}")


    # Real repair needs RW; dry-run keeps a read-only connection.
    with DBGateway(str(db_path), read_only=dry_run, logger=log) as conn:
        # 2. 计算 RPS（覆盖整段时间窗的相关交易日，含历史 LAG 上下文）
        log.info("开始计算 RPS（含 rps_10/20/50/120/250）...")
        result_df = conn.execute(f"""
            WITH missing_pairs AS (
                {_MISSING_PAIR_SQL}
            ),
            target_dates AS (
                SELECT DISTINCT trade_date FROM missing_pairs
            ),
            lagged AS (
                SELECT
                    symbol,
                    trade_date,
                    close,
                    LAG(close, 10)  OVER (PARTITION BY symbol ORDER BY trade_date) AS c10,
                    LAG(close, 20)  OVER (PARTITION BY symbol ORDER BY trade_date) AS c20,
                    LAG(close, 50)  OVER (PARTITION BY symbol ORDER BY trade_date) AS c50,
                    LAG(close, 120) OVER (PARTITION BY symbol ORDER BY trade_date) AS c120,
                    LAG(close, 250) OVER (PARTITION BY symbol ORDER BY trade_date) AS c250
                FROM fact_daily
                WHERE close > 0
            ),
            rets AS (
                SELECT
                    symbol,
                    trade_date,
                    CASE WHEN c10  > 0 THEN (close / c10  - 1) * 100 END AS r10,
                    CASE WHEN c20  > 0 THEN (close / c20  - 1) * 100 END AS r20,
                    CASE WHEN c50  > 0 THEN (close / c50  - 1) * 100 END AS r50,
                    CASE WHEN c120 > 0 THEN (close / c120 - 1) * 100 END AS r120,
                    CASE WHEN c250 > 0 THEN (close / c250 - 1) * 100 END AS r250
                FROM lagged
                WHERE trade_date IN (SELECT trade_date FROM target_dates)
            ),
            ranked AS (
                SELECT
                    symbol,
                    trade_date,
                    ROUND(PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY r10  NULLS FIRST) * 100, 2) AS rps_10,
                    ROUND(PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY r20  NULLS FIRST) * 100, 2) AS rps_20,
                    ROUND(PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY r50  NULLS FIRST) * 100, 2) AS rps_50,
                    ROUND(PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY r120 NULLS FIRST) * 100, 2) AS rps_120,
                    ROUND(PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY r250 NULLS FIRST) * 100, 2) AS rps_250
                FROM rets
            )
            SELECT
                ranked.symbol,
                ranked.trade_date,
                ranked.rps_10,
                ranked.rps_20,
                ranked.rps_50,
                ranked.rps_120,
                ranked.rps_250
            FROM ranked
            INNER JOIN missing_pairs m
              ON ranked.symbol = m.symbol AND ranked.trade_date = m.trade_date
        """).fetchdf()

        log.info(f"计算完成，共 {len(result_df)} 行（已限定到缺失对）")

        if dry_run:
            log.info("[DRY RUN] 不写入数据库")
            return len(result_df)

        if len(result_df) == 0:
            log.warning("缺失对存在但计算结果为 0 行，疑似数据异常")
            return 0

        # 3. 写入（INSERT，靠上面的 anti-join 保证不重复）
        log.info("写入 fact_rps_results ...")
        conn.register("rps_result", result_df)
        try:
            conn.execute("""
                INSERT INTO fact_rps_results (symbol, trade_date, rps_10, rps_20, rps_50, rps_120, rps_250)
                SELECT symbol, trade_date, rps_10, rps_20, rps_50, rps_120, rps_250
                FROM rps_result
            """)
        finally:
            conn.unregister("rps_result")

        # 4. 复核
        remaining = conn.execute(
            f"SELECT COUNT(*) FROM ({_MISSING_PAIR_SQL}) m"
        ).fetchone()[0]
        new_max = conn.execute("SELECT MAX(trade_date) FROM fact_rps_results").fetchone()[0]
        total = conn.execute("SELECT COUNT(*) FROM fact_rps_results").fetchone()[0]
        log.info(f"完成: max_date={new_max}, total={total:,} 行, 剩余缺失对={remaining}")

        if remaining > 0:
            log.warning(f"仍有 {remaining} 个 (symbol, trade_date) 缺失（多为 LAG 历史不足，可接受）")

        return len(result_df)


if __name__ == "__main__":
    dry = "--dry-run" in sys.argv
    n = run_rps_etl(dry_run=dry)
    log.info(f"ETL 完成，处理 {n} 行")
