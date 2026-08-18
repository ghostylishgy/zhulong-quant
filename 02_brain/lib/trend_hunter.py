# ============================================================
# 🐲 烛龙计划 - 趋势猎人 (Trend Hunter) v3.0 DuckDB 原生版
# ============================================================
# 形态识别模块：对传入的 Candidate 逐一做均线/量能形态评分
#
# v3.0: 完全移除 SQLAlchemy / core.database / strategies.rps_calculator
#       改用 DuckDB 直查 fact_daily，接口对齐 decision_engine 调用规范
#       _identify_pattern(symbol, trade_date=None) -> dict
# ============================================================

import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Tuple
import sys

import pandas as pd
import numpy as np

logger = logging.getLogger('zhulong.hunter')

# ── 常量（与 decision_engine 保持一致）──────────────────────
_BASE_DIR = Path(__file__).resolve().parent.parent.parent
_ENGINE_LIB = _BASE_DIR / "01_engine" / "lib"
if str(_ENGINE_LIB) not in sys.path:
    sys.path.insert(0, str(_ENGINE_LIB))
try:
    from db_gateway import DBGateway
except Exception:  # pragma: no cover - standalone emergency fallback
    DBGateway = None

DB_PATH = _BASE_DIR / "storage" / "database" / "zhulong.duckdb"
TABLE_STOCK_DAILY = "fact_daily"
FIELD_SYMBOL = "symbol"
FIELD_TRADE_DATE = "trade_date"


class TrendHunter:
    """
    趋势猎人 v3.0 — DuckDB 原生形态识别

    核心形态：
      1. 口袋支点 (Pocket Pivot)：缩量回调后放量突破
      2. 均线多头：MA5 > MA10 > MA20 > MA60

    对外接口（与 decision_engine L1.5 对齐）：
      hunter._identify_pattern(symbol, trade_date=None) -> dict
        返回 {"score": float, "ma_alignment": bool, "pattern_name": str}
    """

    MA_PERIODS = [5, 10, 20, 60]
    VOLUME_RATIO_MIN = 1.2  # 口袋支点最低量比
    HISTORY_DAYS = 120      # 拉取最近 N 个交易日

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = str(db_path)
        logger.info("🎯 趋势猎人 v3.0 (DuckDB) 初始化完成")

    # ── 数据获取 ──────────────────────────────────────────────

    # DBGateway keeps pattern scans on the shared retry/lock path.

    @staticmethod
    def _normalize_trade_date(trade_date: Optional[str]) -> Optional[str]:
        if trade_date is None:
            return None
        text = str(trade_date).strip()
        if not text:
            return None
        for fmt, size in (("%Y-%m-%d", 10), ("%Y%m%d", 8)):
            try:
                return datetime.strptime(text[:size], fmt).strftime("%Y-%m-%d")
            except Exception:
                continue
        try:
            return datetime.fromisoformat(text).strftime("%Y-%m-%d")
        except Exception as exc:
            raise ValueError(f"Unsupported trade_date: {trade_date}") from exc

    def _connect(self):
        if DBGateway is not None:
            # TrendHunter is read-only, but still uses DBGateway for retry and lock telemetry.
            return DBGateway(self.db_path, read_only=True, logger=logger)
        import duckdb
        return duckdb.connect(self.db_path, read_only=True)

    def _get_price_history(self, symbol: str,
                           trade_date: Optional[str] = None,
                           days: int = HISTORY_DAYS) -> pd.DataFrame:
        """Fetch the latest OHLCV rows up to trade_date.

        trade_date accepts YYYY-MM-DD, YYYYMMDD, or ISO-like strings.
        """
        try:
            cutoff = self._normalize_trade_date(trade_date)
            if cutoff:
                start_date = (datetime.strptime(cutoff, "%Y-%m-%d")
                              - timedelta(days=int(days * 2.2))).strftime("%Y-%m-%d")
                sql = f"""
                    SELECT * FROM (
                        SELECT {FIELD_TRADE_DATE}, open, high, low, close,
                               vol AS volume, pct_chg
                        FROM {TABLE_STOCK_DAILY}
                        WHERE {FIELD_SYMBOL} = ?
                          AND CAST({FIELD_TRADE_DATE} AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
                        ORDER BY CAST({FIELD_TRADE_DATE} AS DATE) DESC
                        LIMIT ?
                    ) recent
                    ORDER BY CAST({FIELD_TRADE_DATE} AS DATE)
                """
                params = [symbol, start_date, cutoff, int(days)]
            else:
                sql = f"""
                    SELECT * FROM (
                        SELECT {FIELD_TRADE_DATE}, open, high, low, close,
                               vol AS volume, pct_chg
                        FROM {TABLE_STOCK_DAILY}
                        WHERE {FIELD_SYMBOL} = ?
                        ORDER BY CAST({FIELD_TRADE_DATE} AS DATE) DESC
                        LIMIT ?
                    ) recent
                    ORDER BY CAST({FIELD_TRADE_DATE} AS DATE)
                """
                params = [symbol, int(days)]

            with self._connect() as conn:
                df = conn.execute(sql, params).df()
        except Exception as e:
            logger.warning(f"TrendHunter _get_price_history({symbol}, {trade_date}) error: {e}")
            return pd.DataFrame()

        if df.empty:
            return df

        # Normalize numeric columns returned from DuckDB.
        for col in ["open", "high", "low", "close", "volume", "pct_chg"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        return df.reset_index(drop=True)


    def _calculate_mas(self, df: pd.DataFrame) -> pd.DataFrame:
        for p in self.MA_PERIODS:
            df[f"ma{p}"] = df["close"].rolling(window=p).mean()
        return df

    def _check_ma_alignment(self, df: pd.DataFrame) -> bool:
        """MA5 > MA10 > MA20 > MA60（最后一行）"""
        if len(df) < 60:
            return False
        row = df.iloc[-1]
        cols = ["ma5", "ma10", "ma20", "ma60"]
        if any(col not in df.columns or pd.isna(row[col]) for col in cols):
            return False
        return row["ma5"] > row["ma10"] > row["ma20"] > row["ma60"]

    def _check_pocket_pivot(self, df: pd.DataFrame) -> Tuple[bool, float]:
        """
        口袋支点：今日上涨 + 成交量超过前10日内所有下跌日中最大量 + 量比达标
        """
        if len(df) < 20:
            return False, 0.0

        recent = df.tail(15).copy()
        latest = recent.iloc[-1]

        # 条件1：今日上涨
        if pd.isna(latest["pct_chg"]) or latest["pct_chg"] <= 0:
            return False, 0.0

        # 量比
        avg_vol = recent["volume"].iloc[:-1].mean()
        if avg_vol <= 0 or pd.isna(avg_vol):
            return False, 0.0
        vol_ratio = latest["volume"] / avg_vol

        # 条件3：今日量 > 近期所有下跌日中的最大量
        down_days = recent[recent["pct_chg"] < 0]
        if len(down_days) == 0:
            is_pp = vol_ratio >= self.VOLUME_RATIO_MIN
        else:
            is_pp = (latest["volume"] > down_days["volume"].max()
                     and vol_ratio >= self.VOLUME_RATIO_MIN)

        return is_pp, vol_ratio

    # ── 对外接口 ─────────────────────────────────────────────

    def _identify_pattern(self, symbol: str,
                          trade_date: Optional[str] = None) -> dict:
        """
        主接口，供 decision_engine L1.5 调用。

        Returns:
            {
              "score":         float  (0-100),
              "ma_alignment":  bool,
              "pattern_name":  str,
              "volume_ratio":  float,
            }
        """
        default = {"score": 0.0, "ma_alignment": False,
                   "pattern_name": "数据不足", "volume_ratio": 0.0}

        try:
            df = self._get_price_history(symbol, trade_date)
            if df.empty or len(df) < 60:
                return default

            df = self._calculate_mas(df)
            ma_ok = self._check_ma_alignment(df)
            pp_ok, vol_ratio = self._check_pocket_pivot(df)

            score = 0.0
            tags = []

            if ma_ok:
                score += 40
                tags.append("均线多头")
            if pp_ok:
                score += 40
                tags.append("口袋支点")
            if vol_ratio >= 1.5:
                score += 20
                tags.append("放量")
            elif vol_ratio >= 1.2:
                score += 10
                tags.append("温和放量")

            pattern_name = "+".join(tags) if tags else "无明显形态"

            return {
                "score":        float(score),
                "ma_alignment": bool(ma_ok),
                "pattern_name": pattern_name,
                "volume_ratio": round(float(vol_ratio), 3),
            }
        except Exception as e:
            logger.warning(f"TrendHunter._identify_pattern({symbol}) error: {e}")
            return default
