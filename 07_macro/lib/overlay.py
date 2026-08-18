#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
07_macro/lib/overlay.py
Phase 6 macro-micro overlay and signal labeling.
"""

from __future__ import annotations

import runpy
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from config.settings import Config

logger = logging.getLogger("zhulong.macro.overlay")

_current = Path(__file__).resolve()
PROJECT_ROOT = next(
    (p for p in _current.parents if (p / ".git").exists() or (p / "storage").exists()),
    _current.parents[2],
)
DEFAULT_DB_PATH = str(PROJECT_ROOT / "storage" / "database" / "zhulong.duckdb")


_loader_ns = runpy.run_path(str(PROJECT_ROOT / "04_governance" / "lib" / "core" / "module_loader.py"))
load_module_from_path = _loader_ns["load_module_from_path"]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "07_macro" / "config" / "macro_resonance.json"


SIG_CORE = "[\U0001f680 \u6838\u5fc3\u51fa\u51fb]"
SIG_COORD = "[\u2694\ufe0f \u534f\u540c\u5173\u6ce8]"
SIG_DEF = "[\U0001f6e1 \u9632\u5b88\u89c2\u5bdf]"


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


def _load_module(path: Path, module_name: str):
    return load_module_from_path(module_name, path)


_dbgw_mod = _load_module(PROJECT_ROOT / "01_engine" / "lib" / "db_gateway.py", "db_gateway_macro_overlay")
DBGateway = _dbgw_mod.DBGateway

_safe_mod = _load_module(
    PROJECT_ROOT / "04_governance" / "lib" / "core" / "safe_writer.py",
    "safe_writer_macro_overlay",
)
duckdb_safe = _safe_mod.duckdb_safe
sanitize_row = _safe_mod.sanitize_row


@dataclass
class OverlayResult:
    rows_written: int
    symbol_labels: Dict[str, str]
    symbol_best_topic: Dict[str, Dict[str, object]]


class OverlayEngine:
    """Build macro-micro overlay labels and persist fact_macro_micro_overlay."""

    def __init__(self, db_path: Optional[str] = None, config_path: Optional[str] = None):
        self.db_path = str(db_path or os.getenv("DB_PATH", "") or Config.DB_PATH or DEFAULT_DB_PATH)
        self.config_path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH

        cfg = self._load_config(self.config_path)
        output_cfg = (cfg or {}).get("output", {})
        self.top_k = int(output_cfg.get("top_k", 5) or 5)

    def build(self, trade_date: str) -> OverlayResult:
        td = self._normalize_date(trade_date)

        top5_set = self._load_top5_set(td)
        symbol_topics = self._load_symbol_topics(td)
        micro_rows = self._load_micro_states(td)

        rows_to_write: List[Dict[str, object]] = []
        symbol_labels: Dict[str, str] = {}
        symbol_best_topic: Dict[str, Dict[str, object]] = {}

        for micro in micro_rows:
            symbol = micro["symbol"]
            micro_state = micro["micro_verdict"]
            micro_score = float(micro["micro_score"])
            candidates = symbol_topics.get(symbol, [])
            if not candidates:
                continue

            best = max(candidates, key=lambda x: float(x.get("macro_score", 0.0)))
            best_is_fake = int(best.get("anti_fake_flag", 0)) == 1
            best_is_trap = _as_bool(best.get("is_event_driven_trap", False), default=False)
            any_top5 = any((x["topic_type"], x["topic_id"]) in top5_set for x in candidates)
            any_top5_nonfake = any(
                ((x["topic_type"], x["topic_id"]) in top5_set) and int(x.get("anti_fake_flag", 0)) == 0
                for x in candidates
            )

            if best_is_trap and micro_state == "PASS":
                logger.warning(
                    "[L4_VETO] 标的 %s 量价达标，但触发宏观事件陷阱否决。 topic=%s:%s",
                    symbol,
                    best.get("topic_type"),
                    best.get("topic_id"),
                )

            if best_is_fake or best_is_trap:
                signal_label = SIG_DEF
            elif micro_state == "PASS" and any_top5_nonfake:
                signal_label = SIG_CORE
            elif micro_state == "PASS" or (micro_state == "WATCH" and any_top5):
                signal_label = SIG_COORD
            else:
                signal_label = SIG_DEF

            composite_score = round(micro_score * 0.55 + float(best.get("macro_score", 0.0)) * 0.45, 4)

            payload = {
                "trade_date": td,
                "symbol": symbol,
                "topic_type": best.get("topic_type"),
                "topic_id": best.get("topic_id"),
                "micro_verdict": micro_state,
                "micro_score": micro_score,
                "macro_score": float(best.get("macro_score", 0.0)),
                "anti_fake_flag": int(best.get("anti_fake_flag", 0)),
                "composite_score": composite_score,
                "signal_label": signal_label,
                "pushed": 0,
            }
            rows_to_write.append(payload)
            symbol_labels[symbol] = signal_label
            symbol_best_topic[symbol] = {
                "topic_type": best.get("topic_type"),
                "topic_id": best.get("topic_id"),
                "topic_name": best.get("topic_name"),
                "macro_score": float(best.get("macro_score", 0.0)),
                "anti_fake_flag": int(best.get("anti_fake_flag", 0)),
                "signal_label": signal_label,
            }

        self._persist_overlay(td, rows_to_write)
        return OverlayResult(
            rows_written=len(rows_to_write),
            symbol_labels=symbol_labels,
            symbol_best_topic=symbol_best_topic,
        )

    def _persist_overlay(self, trade_date: str, rows_to_write: List[Dict[str, object]]) -> None:
        with duckdb_safe(self.db_path, read_only=False, retries=6, base_delay=0.5) as conn:
            conn.execute(
                "DELETE FROM fact_macro_micro_overlay WHERE trade_date = CAST(? AS DATE)",
                [trade_date],
            )

            for row in rows_to_write:
                clean = sanitize_row(dict(row))
                conn.execute(
                    """
                    INSERT INTO fact_macro_micro_overlay (
                        trade_date,
                        symbol,
                        topic_type,
                        topic_id,
                        micro_verdict,
                        micro_score,
                        macro_score,
                        anti_fake_flag,
                        composite_score,
                        signal_label,
                        pushed,
                        updated_at
                    )
                    VALUES (
                        CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW()
                    )
                    ON CONFLICT (trade_date, symbol, topic_type, topic_id) DO UPDATE SET
                        micro_verdict = excluded.micro_verdict,
                        micro_score = excluded.micro_score,
                        macro_score = excluded.macro_score,
                        anti_fake_flag = excluded.anti_fake_flag,
                        composite_score = excluded.composite_score,
                        signal_label = excluded.signal_label,
                        pushed = excluded.pushed,
                        updated_at = NOW()
                    """,
                    [
                        trade_date,
                        clean.get("symbol"),
                        clean.get("topic_type"),
                        clean.get("topic_id"),
                        clean.get("micro_verdict"),
                        clean.get("micro_score"),
                        clean.get("macro_score"),
                        clean.get("anti_fake_flag"),
                        clean.get("composite_score"),
                        clean.get("signal_label"),
                        clean.get("pushed"),
                    ],
                )

            conn.commit()

    def _load_top5_set(self, trade_date: str) -> set[Tuple[str, str]]:
        out: set[Tuple[str, str]] = set()
        with DBGateway(self.db_path, read_only=True, logger=logger) as conn:
            rows = conn.execute(
                """
                SELECT topic_type, topic_id
                FROM fact_macro_top5_daily
                WHERE trade_date = CAST(? AS DATE)
                """,
                [trade_date],
            ).fetchall()
        for topic_type, topic_id in rows:
            out.add((str(topic_type or ""), str(topic_id or "")))
        return out

    def _load_symbol_topics(self, trade_date: str) -> Dict[str, List[Dict[str, object]]]:
        out: Dict[str, List[Dict[str, object]]] = {}
        with DBGateway(self.db_path, read_only=True, logger=logger) as conn:
            cursor = conn.execute(
                """
                SELECT
                    m.symbol,
                    d.topic_type,
                    d.topic_id,
                    d.topic_name,
                    d.resonance_score,
                    d.anti_fake_flag,
                    COALESCE(d.is_event_driven_trap, FALSE) AS is_event_driven_trap
                FROM dim_macro_topic_member m
                JOIN fact_macro_topic_daily d
                  ON d.trade_date = CAST(? AS DATE)
                 AND d.topic_type = m.topic_type
                 AND d.topic_id = m.topic_id
                WHERE m.is_active = 1
                """,
                [trade_date],
            )

            while True:
                rows = cursor.fetchmany(4000)
                if not rows:
                    break
                for symbol, topic_type, topic_id, topic_name, score, anti_fake_flag, is_event_driven_trap in rows:
                    sym = str(symbol or "").strip()
                    if not sym:
                        continue
                    out.setdefault(sym, []).append(
                        {
                            "topic_type": str(topic_type or ""),
                            "topic_id": str(topic_id or ""),
                            "topic_name": str(topic_name or topic_id or ""),
                            "macro_score": float(score or 0.0),
                            "anti_fake_flag": int(anti_fake_flag or 0),
                            "is_event_driven_trap": bool(is_event_driven_trap),
                        }
                    )

        return out

    def _load_micro_states(self, trade_date: str) -> List[Dict[str, object]]:
        out: List[Dict[str, object]] = []
        seen = set()

        with DBGateway(self.db_path, read_only=True, logger=logger) as conn:
            rows = conn.execute(
                """
                SELECT
                    symbol,
                    COALESCE(l4_final_verdict, l4_verdict, l3_verdict, status) AS raw_verdict,
                    l3_audit_score,
                    l4_blue_score,
                    completed_at,
                    created_at
                FROM nexus_audits
                WHERE trade_date = ?
                  AND symbol IS NOT NULL
                ORDER BY COALESCE(completed_at, created_at) DESC
                """,
                [trade_date],
            ).fetchall()

        for symbol, raw_verdict, l3_score, l4_blue, _, _ in rows:
            sym = str(symbol or "").strip()
            if not sym or sym in seen:
                continue

            micro_verdict = self._normalize_micro_verdict(raw_verdict)
            if micro_verdict is None:
                continue

            if l3_score is not None:
                micro_score = float(l3_score)
            elif l4_blue is not None:
                micro_score = float(l4_blue) * 100.0
            else:
                micro_score = 50.0

            out.append(
                {
                    "symbol": sym,
                    "micro_verdict": micro_verdict,
                    "micro_score": micro_score,
                }
            )
            seen.add(sym)

        return out

    @staticmethod
    def _normalize_micro_verdict(raw_value) -> Optional[str]:
        text = str(raw_value or "").strip().upper()
        if not text:
            return None
        if "PASS" in text:
            return "PASS"
        if "WATCH" in text or "HOLD" in text:
            return "WATCH"
        return None

    @staticmethod
    def _normalize_date(value: str) -> str:
        text = str(value or "").strip()
        if len(text) == 8 and text.isdigit():
            return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"
        if len(text) >= 10 and text[4] == "-" and text[7] == "-":
            return text[:10]
        raise ValueError(f"Unsupported trade_date format: {value}")

    @staticmethod
    def _load_config(path: Path) -> Dict[str, object]:
        try:
            if not path.exists():
                return {}
            with open(path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            return data if isinstance(data, dict) else {}
        except Exception as exc:
            logger.warning("Overlay config load failed path=%s err=%s", path, exc)
            return {}
