#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
07_macro/lib/factor_engine.py
Phase 4 factor aggregation to topic granularity.
"""

from __future__ import annotations

import runpy
import json
import logging
import math
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from config.settings import Config

logger = logging.getLogger("zhulong.macro.factor_engine")

_current = Path(__file__).resolve()
PROJECT_ROOT = next(
    (p for p in _current.parents if (p / ".git").exists() or (p / "storage").exists()),
    _current.parents[2],
)
DEFAULT_DB_PATH = str(PROJECT_ROOT / "storage" / "database" / "zhulong.duckdb")


_loader_ns = runpy.run_path(str(PROJECT_ROOT / "04_governance" / "lib" / "core" / "module_loader.py"))
load_module_from_path = _loader_ns["load_module_from_path"]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "07_macro" / "config" / "macro_resonance.json"


def _load_module(path: Path, module_name: str):
    return load_module_from_path(module_name, path)


_dbgw_mod = _load_module(PROJECT_ROOT / "01_engine" / "lib" / "db_gateway.py", "db_gateway_macro_factor")
DBGateway = _dbgw_mod.DBGateway

_topic_mod = _load_module(_current.parent / "topic_mapper.py", "topic_mapper_macro_factor")
TopicMapper = _topic_mod.TopicMapper

_tu_mod = _load_module(_current.parent / "tushare_client.py", "tushare_client_macro_factor")
MacroTushareClient = _tu_mod.MacroTushareClient


@dataclass
class FactorResult:
    status: str
    trade_date: str
    topic_rows: List[Dict[str, object]]
    moneyflow_rows: int = 0
    limit_rows: int = 0
    filtered_topic_count: int = 0


class FactorEngine:
    """Compute topic-level raw factors from moneyflow/limit data + local daily data."""

    def __init__(
        self,
        db_path: Optional[str] = None,
        topic_mapper: Optional[TopicMapper] = None,
        config_path: Optional[str] = None,
    ):
        self.db_path = str(db_path or os.getenv("DB_PATH", "") or Config.DB_PATH or DEFAULT_DB_PATH)
        self.topic_mapper = topic_mapper or TopicMapper(db_path=self.db_path)
        self.client = MacroTushareClient(timeout_sec=30, max_retries=4, base_delay=1.0, jitter_max=0.5)
        self.config_path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH

        cfg = self._load_config(self.config_path)
        blacklist_raw = (cfg or {}).get("topic_blacklist", [])
        self.topic_blacklist = {str(x).strip() for x in blacklist_raw if str(x).strip()}

    def compute(self, trade_date: str) -> FactorResult:
        td = self._normalize_date(trade_date)

        symbol_topics, topic_meta, filtered_topic_count = self._load_topic_index(td)
        if not topic_meta:
            logger.error("Topic mapping is empty after blacklist filtering")
            return FactorResult(
                status="NO_DATA",
                trade_date=td,
                topic_rows=[],
                filtered_topic_count=filtered_topic_count,
            )

        pct_map = self._load_daily_pct_map(td)
        moneyflow_map, moneyflow_rows = self._load_moneyflow_map(td)
        limit_map, limit_rows = self._load_limit_map(td)

        topic_stats: Dict[Tuple[str, str], Dict[str, object]] = {}
        for key, meta in topic_meta.items():
            topic_stats[key] = {
                "topic_type": meta["topic_type"],
                "topic_id": meta["topic_id"],
                "topic_name": meta["topic_name"],
                "member_cnt": int(meta["member_cnt"]),
                "up_cnt": 0,
                "limit_up_cnt": 0,
                "open_board_cnt": 0,
                "net_mf_amount": 0.0,
                "sum_pct": 0.0,
                "pct_n": 0,
                "heat_hits": 0,
                "freshness_days": int(meta["freshness_days"]),
            }

        for symbol, topics in symbol_topics.items():
            pct = pct_map.get(symbol)
            net_mf = moneyflow_map.get(symbol, 0.0)
            limit_info = limit_map.get(symbol)

            for key in topics:
                stat = topic_stats[key]
                stat["net_mf_amount"] = float(stat["net_mf_amount"]) + net_mf

                if pct is not None:
                    stat["sum_pct"] = float(stat["sum_pct"]) + pct
                    stat["pct_n"] = int(stat["pct_n"]) + 1
                    if pct > 0:
                        stat["up_cnt"] = int(stat["up_cnt"]) + 1

                if limit_info is not None:
                    if limit_info["limit_up"] > 0:
                        stat["limit_up_cnt"] = int(stat["limit_up_cnt"]) + 1
                    if limit_info["open_board"] > 0:
                        stat["open_board_cnt"] = int(stat["open_board_cnt"]) + 1
                    stat["heat_hits"] = int(stat["heat_hits"]) + int(limit_info["heat_hits"])

        topic_rows: List[Dict[str, object]] = []
        for stat in topic_stats.values():
            pct_n = int(stat["pct_n"])
            avg_pct = float(stat["sum_pct"]) / pct_n if pct_n > 0 else 0.0

            topic_rows.append(
                {
                    "trade_date": td,
                    "topic_type": stat["topic_type"],
                    "topic_id": stat["topic_id"],
                    "topic_name": stat["topic_name"],
                    "member_cnt": int(stat["member_cnt"]),
                    "up_cnt": int(stat["up_cnt"]),
                    "limit_up_cnt": int(stat["limit_up_cnt"]),
                    "open_board_cnt": int(stat["open_board_cnt"]),
                    "net_mf_amount": float(stat["net_mf_amount"]),
                    "avg_pct_chg": float(avg_pct),
                    "heat_hits": int(stat["heat_hits"]),
                    "freshness_days": int(stat["freshness_days"]),
                }
            )

        status = "OK" if topic_rows else "NO_DATA"
        return FactorResult(
            status=status,
            trade_date=td,
            topic_rows=topic_rows,
            moneyflow_rows=moneyflow_rows,
            limit_rows=limit_rows,
            filtered_topic_count=filtered_topic_count,
        )

    def _load_topic_index(self, trade_date: str):
        symbol_topics: Dict[str, List[Tuple[str, str]]] = {}
        topic_meta: Dict[Tuple[str, str], Dict[str, object]] = {}
        trade_dt = datetime.strptime(trade_date, "%Y-%m-%d")

        filtered_topics: set[Tuple[str, str]] = set()
        for row in self.topic_mapper.iter_active_mapping_rows(batch_size=4000):
            symbol = str(row.get("symbol", "") or "").strip()
            topic_type = str(row.get("topic_type", "") or "").strip()
            topic_id = str(row.get("topic_id", "") or "").strip()
            topic_name = str(row.get("topic_name", "") or topic_id).strip()
            updated_at = str(row.get("updated_at", "") or "").strip()

            if not symbol or not topic_type or not topic_id:
                continue
            if topic_name in self.topic_blacklist:
                filtered_topics.add((topic_type, topic_id))
                continue

            key = (topic_type, topic_id)
            symbol_topics.setdefault(symbol, []).append(key)

            if key not in topic_meta:
                freshness_days = self._calc_freshness_days(trade_dt, updated_at)
                topic_meta[key] = {
                    "topic_type": topic_type,
                    "topic_id": topic_id,
                    "topic_name": topic_name,
                    "member_cnt": 0,
                    "freshness_days": freshness_days,
                }

            topic_meta[key]["member_cnt"] = int(topic_meta[key]["member_cnt"]) + 1

        if filtered_topics:
            logger.info("Topic blacklist filtered %s topics", len(filtered_topics))
        return symbol_topics, topic_meta, len(filtered_topics)

    def _load_daily_pct_map(self, trade_date: str) -> Dict[str, float]:
        pct_map: Dict[str, float] = {}
        with DBGateway(self.db_path, read_only=True, logger=logger) as conn:
            cursor = conn.execute(
                """
                SELECT symbol, pct_chg
                FROM fact_daily
                WHERE trade_date = CAST(? AS DATE)
                """,
                [trade_date],
            )
            while True:
                rows = cursor.fetchmany(4000)
                if not rows:
                    break
                for symbol, pct in rows:
                    if symbol is None:
                        continue
                    pct_map[str(symbol)] = self._to_float(pct, default=0.0)
        return pct_map

    def _load_moneyflow_map(self, trade_date: str) -> tuple[Dict[str, float], int]:
        mapping: Dict[str, float] = {}
        try:
            df = self.client.get_moneyflow(trade_date)
        except Exception as exc:
            logger.error("Moneyflow fetch failed: %s", exc, exc_info=True)
            return mapping, 0

        if df is None or df.empty:
            return mapping, 0

        for row in df.itertuples(index=False):
            ts_code = str(getattr(row, "ts_code", "") or "").strip()
            if not ts_code:
                continue

            if hasattr(row, "net_mf_amount"):
                net_val = self._to_float(getattr(row, "net_mf_amount"), default=0.0)
            else:
                buy_lg = self._to_float(getattr(row, "buy_lg_amount", 0.0), default=0.0)
                sell_lg = self._to_float(getattr(row, "sell_lg_amount", 0.0), default=0.0)
                buy_elg = self._to_float(getattr(row, "buy_elg_amount", 0.0), default=0.0)
                sell_elg = self._to_float(getattr(row, "sell_elg_amount", 0.0), default=0.0)
                net_val = (buy_lg + buy_elg) - (sell_lg + sell_elg)

            mapping[ts_code] = net_val

        return mapping, len(df)

    def _load_limit_map(self, trade_date: str) -> tuple[Dict[str, Dict[str, int]], int]:
        mapping: Dict[str, Dict[str, int]] = {}
        try:
            df = self.client.get_limit_list_d(trade_date)
        except Exception as exc:
            logger.error("Limit list fetch failed: %s", exc, exc_info=True)
            return mapping, 0

        if df is None or df.empty:
            return mapping, 0

        for row in df.itertuples(index=False):
            ts_code = str(getattr(row, "ts_code", "") or "").strip()
            if not ts_code:
                continue

            limit_flag = str(getattr(row, "limit", "") or "").strip().upper()
            open_times = int(self._to_float(getattr(row, "open_times", 0.0), default=0.0))
            limit_times = int(self._to_float(getattr(row, "limit_times", 0.0), default=0.0))

            is_limit_up = 1 if limit_flag == "U" else 0
            heat_hits = max(limit_times, is_limit_up)

            mapping[ts_code] = {
                "limit_up": is_limit_up,
                "open_board": 1 if open_times > 0 else 0,
                "heat_hits": int(heat_hits),
            }

        return mapping, len(df)

    @staticmethod
    def _normalize_date(value: str) -> str:
        val = str(value or "").strip()
        if len(val) == 8 and val.isdigit():
            return f"{val[0:4]}-{val[4:6]}-{val[6:8]}"
        if len(val) >= 10 and val[4] == "-" and val[7] == "-":
            return val[:10]
        raise ValueError(f"Unsupported trade_date: {value}")

    @staticmethod
    def _calc_freshness_days(trade_dt: datetime, updated_at: str) -> int:
        text = str(updated_at or "").strip()
        if not text:
            return 999
        try:
            dt = datetime.fromisoformat(text.replace("Z", ""))
            return max(0, (trade_dt.date() - dt.date()).days)
        except Exception:
            return 999

    @staticmethod
    def _to_float(value, default: float = 0.0) -> float:
        try:
            out = float(value)
            if math.isnan(out) or math.isinf(out):
                return default
            return out
        except Exception:
            return default

    @staticmethod
    def _load_config(path: Path) -> Dict[str, object]:
        try:
            if not path.exists():
                return {}
            with open(path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, dict):
                return data
            return {}
        except Exception as exc:
            logger.warning("Factor config load failed path=%s err=%s", path, exc)
            return {}
