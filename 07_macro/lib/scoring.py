#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
07_macro/lib/scoring.py
Topic scoring + persistence for Macro Resonance V1.
"""

from __future__ import annotations

import runpy
import json
import logging
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from config.settings import Config

logger = logging.getLogger("zhulong.macro.scoring")

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


_safe_mod = _load_module(
    PROJECT_ROOT / "04_governance" / "lib" / "core" / "safe_writer.py",
    "safe_writer_macro_scoring",
)
duckdb_safe = _safe_mod.duckdb_safe
sanitize_row = _safe_mod.sanitize_row


@dataclass
class ScoreArtifacts:
    scored_rows: List[Dict[str, object]]
    top_rows: List[Dict[str, object]]


class ScoringEngine:
    """Compute base scores and write fact_macro_topic_daily + fact_macro_top5_daily."""

    def __init__(
        self,
        db_path: Optional[str] = None,
        config_path: Optional[str] = None,
        top_k: int = 5,
    ):
        self.db_path = str(db_path or os.getenv("DB_PATH", "") or Config.DB_PATH or DEFAULT_DB_PATH)
        self.config_path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH

        cfg = self._load_config(self.config_path)
        score_cfg = (cfg or {}).get("scoring", {})
        output_cfg = (cfg or {}).get("output", {})

        self.w_moneyflow = self._to_float(score_cfg.get("w_moneyflow", 0.35), 0.35)
        self.w_limit_strength = self._to_float(score_cfg.get("w_limit_strength", 0.30), 0.30)
        self.w_breadth = self._to_float(score_cfg.get("w_breadth", 0.20), 0.20)
        self.w_heat = self._to_float(score_cfg.get("w_heat", 0.15), 0.15)
        self.top_k = int(output_cfg.get("top_k", top_k) or top_k)
        if self.top_k <= 0:
            self.top_k = 5

    def score(self, factor_rows: List[Dict[str, object]]) -> ScoreArtifacts:
        if not factor_rows:
            return ScoreArtifacts(scored_rows=[], top_rows=[])

        limit_raw = [
            self._to_float(r.get("limit_up_cnt", 0)) + 0.5 * self._to_float(r.get("open_board_cnt", 0))
            for r in factor_rows
        ]
        money_raw = [self._to_float(r.get("net_mf_amount", 0.0)) for r in factor_rows]
        heat_raw = [self._to_float(r.get("heat_hits", 0.0)) for r in factor_rows]

        lim_min, lim_max = self._min_max(limit_raw)
        mf_min, mf_max = self._min_max(money_raw)
        heat_min, heat_max = self._min_max(heat_raw)

        scored_rows: List[Dict[str, object]] = []
        for row in factor_rows:
            member_cnt = max(1, int(self._to_float(row.get("member_cnt", 0), 0)))
            up_cnt = int(self._to_float(row.get("up_cnt", 0), 0))
            limit_cnt = int(self._to_float(row.get("limit_up_cnt", 0), 0))
            open_cnt = int(self._to_float(row.get("open_board_cnt", 0), 0))
            heat_hits = int(self._to_float(row.get("heat_hits", 0), 0))
            net_mf = self._to_float(row.get("net_mf_amount", 0.0), 0.0)

            limit_strength = float(limit_cnt) + 0.5 * float(open_cnt)
            limit_norm = self._normalize(limit_strength, lim_min, lim_max)
            mf_norm = self._normalize(net_mf, mf_min, mf_max)
            breadth_norm = min(100.0, max(0.0, (up_cnt / float(member_cnt)) * 100.0))
            heat_norm = self._normalize(float(heat_hits), heat_min, heat_max)

            base_score = (
                limit_norm * self.w_limit_strength
                + mf_norm * self.w_moneyflow
                + breadth_norm * self.w_breadth
                + heat_norm * self.w_heat
            )

            scored_rows.append(
                {
                    "trade_date": row.get("trade_date"),
                    "topic_type": row.get("topic_type"),
                    "topic_id": row.get("topic_id"),
                    "topic_name": row.get("topic_name"),
                    "member_cnt": member_cnt,
                    "up_cnt": up_cnt,
                    "limit_up_cnt": limit_cnt,
                    "open_board_cnt": open_cnt,
                    "net_mf_amount": net_mf,
                    "avg_pct_chg": self._to_float(row.get("avg_pct_chg", 0.0), 0.0),
                    "heat_hits": heat_hits,
                    "freshness_days": int(self._to_float(row.get("freshness_days", 0), 0)),
                    "anti_fake_score": 0.0,
                    "anti_fake_flag": 0,
                    "anti_fake_reason": "",
                    "resonance_score": round(base_score, 4),
                    "resonance_level": self._resonance_level(base_score),
                    "llm_resonance_score": None,
                    "is_event_driven_trap": False,
                }
            )

        scored_rows.sort(key=lambda x: float(x.get("resonance_score", 0.0)), reverse=True)
        top_rows = scored_rows[: self.top_k]
        return ScoreArtifacts(scored_rows=scored_rows, top_rows=top_rows)

    def persist(self, trade_date: str, artifacts: ScoreArtifacts) -> None:
        td = self._normalize_date(trade_date)

        with duckdb_safe(self.db_path, read_only=False, retries=6, base_delay=0.5) as conn:
            conn.execute("DELETE FROM fact_macro_topic_daily WHERE trade_date = CAST(? AS DATE)", [td])
            conn.execute("DELETE FROM fact_macro_top5_daily WHERE trade_date = CAST(? AS DATE)", [td])

            for row in artifacts.scored_rows:
                clean = sanitize_row(dict(row))
                conn.execute(
                    """
                    INSERT INTO fact_macro_topic_daily (
                        trade_date,
                        topic_type,
                        topic_id,
                        topic_name,
                        member_cnt,
                        up_cnt,
                        limit_up_cnt,
                        open_board_cnt,
                        net_mf_amount,
                        avg_pct_chg,
                        heat_hits,
                        freshness_days,
                        anti_fake_score,
                        anti_fake_flag,
                        anti_fake_reason,
                        resonance_score,
                        resonance_level,
                        llm_resonance_score,
                        is_event_driven_trap,
                        updated_at
                    )
                    VALUES (
                        CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW()
                    )
                    ON CONFLICT (trade_date, topic_type, topic_id) DO UPDATE SET
                        topic_name = excluded.topic_name,
                        member_cnt = excluded.member_cnt,
                        up_cnt = excluded.up_cnt,
                        limit_up_cnt = excluded.limit_up_cnt,
                        open_board_cnt = excluded.open_board_cnt,
                        net_mf_amount = excluded.net_mf_amount,
                        avg_pct_chg = excluded.avg_pct_chg,
                        heat_hits = excluded.heat_hits,
                        freshness_days = excluded.freshness_days,
                        anti_fake_score = excluded.anti_fake_score,
                        anti_fake_flag = excluded.anti_fake_flag,
                        anti_fake_reason = excluded.anti_fake_reason,
                        resonance_score = excluded.resonance_score,
                        resonance_level = excluded.resonance_level,
                        llm_resonance_score = excluded.llm_resonance_score,
                        is_event_driven_trap = excluded.is_event_driven_trap,
                        updated_at = NOW()
                    """,
                    [
                        td,
                        clean.get("topic_type"),
                        clean.get("topic_id"),
                        clean.get("topic_name"),
                        clean.get("member_cnt"),
                        clean.get("up_cnt"),
                        clean.get("limit_up_cnt"),
                        clean.get("open_board_cnt"),
                        clean.get("net_mf_amount"),
                        clean.get("avg_pct_chg"),
                        clean.get("heat_hits"),
                        clean.get("freshness_days"),
                        clean.get("anti_fake_score"),
                        clean.get("anti_fake_flag"),
                        clean.get("anti_fake_reason"),
                        clean.get("resonance_score"),
                        clean.get("resonance_level"),
                        clean.get("llm_resonance_score"),
                        clean.get("is_event_driven_trap"),
                    ],
                )

            for rank, row in enumerate(artifacts.top_rows, start=1):
                clean = sanitize_row(dict(row))
                conn.execute(
                    """
                    INSERT INTO fact_macro_top5_daily (
                        trade_date,
                        rank,
                        topic_type,
                        topic_id,
                        topic_name,
                        resonance_score,
                        anti_fake_flag,
                        reason_tags,
                        llm_resonance_score,
                        is_event_driven_trap,
                        generated_at
                    )
                    VALUES (
                        CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW()
                    )
                    ON CONFLICT (trade_date, rank) DO UPDATE SET
                        topic_type = excluded.topic_type,
                        topic_id = excluded.topic_id,
                        topic_name = excluded.topic_name,
                        resonance_score = excluded.resonance_score,
                        anti_fake_flag = excluded.anti_fake_flag,
                        reason_tags = excluded.reason_tags,
                        llm_resonance_score = excluded.llm_resonance_score,
                        is_event_driven_trap = excluded.is_event_driven_trap,
                        generated_at = NOW()
                    """,
                    [
                        td,
                        rank,
                        clean.get("topic_type"),
                        clean.get("topic_id"),
                        clean.get("topic_name"),
                        clean.get("resonance_score"),
                        clean.get("anti_fake_flag"),
                        self._reason_tags(clean),
                        clean.get("llm_resonance_score"),
                        clean.get("is_event_driven_trap"),
                    ],
                )

            conn.commit()

    @staticmethod
    def _normalize(value: float, vmin: float, vmax: float) -> float:
        if math.isclose(vmax, vmin, rel_tol=1e-12, abs_tol=1e-12):
            if value <= 0:
                return 0.0
            return 50.0
        score = (value - vmin) / (vmax - vmin) * 100.0
        return max(0.0, min(100.0, score))

    @staticmethod
    def _min_max(values: List[float]) -> tuple[float, float]:
        if not values:
            return 0.0, 0.0
        return min(values), max(values)

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
    def _normalize_date(value: str) -> str:
        text = str(value or "").strip()
        if len(text) == 8 and text.isdigit():
            return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"
        if len(text) >= 10 and text[4] == "-" and text[7] == "-":
            return text[:10]
        raise ValueError(f"Unsupported trade_date format: {value}")

    @staticmethod
    def _resonance_level(score: float) -> str:
        if score >= 70:
            return "HIGH"
        if score >= 45:
            return "MID"
        return "LOW"

    @staticmethod
    def _reason_tags(row: Dict[str, object]) -> str:
        tags = [
            f"limit_up={int(float(row.get('limit_up_cnt', 0) or 0))}",
            f"net_mf={round(float(row.get('net_mf_amount', 0.0) or 0.0), 2)}",
            f"heat={int(float(row.get('heat_hits', 0) or 0))}",
            f"anti_fake={int(float(row.get('anti_fake_flag', 0) or 0))}",
        ]
        return "|".join(tags)

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
            logger.warning("Score config load failed path=%s err=%s", path, exc)
            return {}
