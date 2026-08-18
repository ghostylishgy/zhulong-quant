#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
07_macro/lib/anti_fake.py
Phase 5 anti-fake rule engine.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger("zhulong.macro.anti_fake")

_current = Path(__file__).resolve()
PROJECT_ROOT = next(
    (p for p in _current.parents if (p / ".git").exists() or (p / "storage").exists()),
    _current.parents[2],
)
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "07_macro" / "config" / "macro_resonance.json"


def _as_bool(value, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {'1', 'true', 'yes', 'on'}:
        return True
    if text in {'0', 'false', 'no', 'off', ''}:
        return False
    return default


@dataclass
class AntiFakeResult:
    scored_rows: List[Dict[str, object]]
    top_rows: List[Dict[str, object]]
    flagged_count: int
    divergence_hits: int
    bleeding_hits: int


class AntiFakeEngine:
    """Rule-based anti-fake detector for macro topics."""

    def __init__(self, config_path: Optional[str] = None):
        self.config_path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
        cfg = self._load_config(self.config_path)
        anti_cfg = (cfg or {}).get("anti_fake", {})
        output_cfg = (cfg or {}).get("output", {})

        self.enable = _as_bool(anti_cfg.get("enable", True), default=True)
        self.divergence_ratio = float(anti_cfg.get("divergence_open_board_ratio", 1.5) or 1.5)
        self.bleeding_top_n = int(anti_cfg.get("bleeding_top_n", 10) or 10)
        self.bleeding_net_mf_threshold = float(anti_cfg.get("bleeding_net_mf_threshold", -30000) or -30000)
        self.top_k = int(output_cfg.get("top_k", 5) or 5)

    def apply(self, scored_rows: List[Dict[str, object]]) -> AntiFakeResult:
        if not scored_rows:
            return AntiFakeResult([], [], 0, 0, 0)

        ranked = sorted(
            [dict(x) for x in scored_rows],
            key=lambda x: float(x.get("resonance_score", 0.0) or 0.0),
            reverse=True,
        )

        if not self.enable:
            return AntiFakeResult(ranked, ranked[: self.top_k], 0, 0, 0)

        divergence_hits = 0
        bleeding_hits = 0
        flagged_count = 0

        for rank, row in enumerate(ranked, start=1):
            reasons: List[str] = []
            anti_score = 0.0

            limit_cnt = int(float(row.get("limit_up_cnt", 0) or 0))
            open_cnt = int(float(row.get("open_board_cnt", 0) or 0))
            net_mf = float(row.get("net_mf_amount", 0.0) or 0.0)

            if limit_cnt > 0 and float(open_cnt) > float(limit_cnt) * self.divergence_ratio:
                reasons.append("DIVERGENCE_OPEN_BOARD")
                anti_score = max(anti_score, 70.0)
                divergence_hits += 1

            if rank <= self.bleeding_top_n and net_mf < self.bleeding_net_mf_threshold:
                reasons.append("BLEEDING_TOP10_NEG_MF")
                anti_score = max(anti_score, 80.0)
                bleeding_hits += 1

            if reasons:
                flagged_count += 1
                row["anti_fake_flag"] = 1
                row["anti_fake_reason"] = "|".join(reasons)
                row["anti_fake_score"] = round(anti_score, 2)
            else:
                row["anti_fake_flag"] = 0
                row["anti_fake_reason"] = ""
                row["anti_fake_score"] = 0.0

        return AntiFakeResult(
            scored_rows=ranked,
            top_rows=ranked[: self.top_k],
            flagged_count=flagged_count,
            divergence_hits=divergence_hits,
            bleeding_hits=bleeding_hits,
        )

    @staticmethod
    def _load_config(path: Path) -> Dict[str, object]:
        try:
            if not path.exists():
                return {}
            with open(path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            return data if isinstance(data, dict) else {}
        except Exception as exc:
            logger.warning("AntiFake config load failed path=%s err=%s", path, exc)
            return {}
