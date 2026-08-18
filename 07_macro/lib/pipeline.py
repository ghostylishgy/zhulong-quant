#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
07_macro/lib/pipeline.py
Macro Resonance V1 pipeline orchestration (Phase 3 + Phase 6) with Macro V2 sidecar enrichment.
"""

from __future__ import annotations

import sys
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from config.settings import Config

logger = logging.getLogger("zhulong.macro.pipeline")

_current = Path(__file__).resolve()
PROJECT_ROOT = next(
    (p for p in _current.parents if (p / ".git").exists() or (p / "storage").exists()),
    _current.parents[2],
)
DEFAULT_DB_PATH = str(PROJECT_ROOT / "storage" / "database" / "zhulong.duckdb")

_loader_name = 'zhulong_core_module_loader'
if _loader_name in sys.modules:
    _module_loader = sys.modules[_loader_name]
else:
    import runpy as _loader_runpy

    _loader_path = PROJECT_ROOT / '04_governance' / 'lib' / 'core' / 'module_loader.py'
    _loader_ns = _loader_runpy.run_path(str(_loader_path))

    class _ModuleLoaderShim:
        @staticmethod
        def load_module_from_path(module_name, path):
            return _loader_ns["load_module_from_path"](module_name, path)

        @staticmethod
        def load_attr_from_path(module_name, path, attr_name):
            return _loader_ns["load_attr_from_path"](module_name, path, attr_name)

    _module_loader = _ModuleLoaderShim()


def _load_module(path: Path, module_name: str):
    return _module_loader.load_module_from_path(module_name, path)


_boot_mod = _load_module(_current.parent / "schema_bootstrap.py", "zhulong_macro_schema_bootstrap")
_topic_mod = _load_module(_current.parent / "topic_mapper.py", "zhulong_macro_topic_mapper")
_factor_mod = _load_module(_current.parent / "factor_engine.py", "zhulong_macro_factor_engine")
_score_mod = _load_module(_current.parent / "scoring.py", "zhulong_macro_scoring")
_fake_mod = _load_module(_current.parent / "anti_fake.py", "zhulong_macro_anti_fake")
_overlay_mod = _load_module(_current.parent / "overlay.py", "zhulong_macro_overlay")
_v2_mod = _load_module(_current.parent / "macro_v2_enricher.py", "zhulong_macro_v2_enricher")

TopicMapper = _topic_mod.TopicMapper
FactorEngine = _factor_mod.FactorEngine
ScoringEngine = _score_mod.ScoringEngine
AntiFakeEngine = _fake_mod.AntiFakeEngine
OverlayEngine = _overlay_mod.OverlayEngine
MacroV2Enricher = _v2_mod.MacroV2Enricher


@dataclass
class PipelineResult:
    status: str
    trade_date: str
    top_rows: List[dict]
    scored_count: int
    topic_count: int
    mapping_used_cache: bool
    mapping_used_fallback: bool
    filtered_topic_count: int
    anti_fake_flagged_count: int
    overlay_rows: int
    symbol_labels: Dict[str, str]
    symbol_best_topic: Dict[str, Dict[str, object]]
    macro_v2_updated_topics: int = 0
    macro_v2_fallback_topics: int = 0
    macro_v2_skipped_no_news: int = 0
    message: str = ""


class MacroResonancePipeline:
    """One-shot pipeline: schema -> mapping -> factors -> scoring -> anti-fake -> (persist) -> macro_v2 -> overlay."""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = str(db_path or os.getenv("DB_PATH", "") or Config.DB_PATH or DEFAULT_DB_PATH)
        self.mapper = TopicMapper(db_path=self.db_path)
        self.factor_engine = FactorEngine(db_path=self.db_path, topic_mapper=self.mapper)
        self.scoring_engine = ScoringEngine(db_path=self.db_path)
        self.anti_fake_engine = AntiFakeEngine()
        self.overlay_engine = OverlayEngine(db_path=self.db_path)

        self.macro_v2_enricher = None
        try:
            self.macro_v2_enricher = MacroV2Enricher(db_path=self.db_path)
        except Exception as exc:
            logger.warning("Macro V2 enricher init failed, continue with V1 only: %s", exc)

    def run(self, trade_date: str) -> PipelineResult:
        td = self._normalize_date(trade_date)

        _boot_mod.bootstrap_schema(db_path=self.db_path)
        map_result = self.mapper.ensure_mapping()

        factor_result = self.factor_engine.compute(td)
        if factor_result.status != "OK":
            msg = f"factor status={factor_result.status}"
            logger.warning("Macro pipeline no data for trade_date=%s (%s)", td, msg)
            return PipelineResult(
                status="NO_DATA",
                trade_date=td,
                top_rows=[],
                scored_count=0,
                topic_count=map_result.topic_count,
                mapping_used_cache=map_result.used_cache,
                mapping_used_fallback=map_result.used_fallback,
                filtered_topic_count=factor_result.filtered_topic_count,
                anti_fake_flagged_count=0,
                overlay_rows=0,
                symbol_labels={},
                symbol_best_topic={},
                macro_v2_updated_topics=0,
                macro_v2_fallback_topics=0,
                macro_v2_skipped_no_news=0,
                message=msg,
            )

        artifacts = self.scoring_engine.score(factor_result.topic_rows)
        if not artifacts.scored_rows:
            msg = "scored_rows is empty"
            logger.warning("Macro pipeline no scored rows trade_date=%s", td)
            return PipelineResult(
                status="NO_DATA",
                trade_date=td,
                top_rows=[],
                scored_count=0,
                topic_count=map_result.topic_count,
                mapping_used_cache=map_result.used_cache,
                mapping_used_fallback=map_result.used_fallback,
                filtered_topic_count=factor_result.filtered_topic_count,
                anti_fake_flagged_count=0,
                overlay_rows=0,
                symbol_labels={},
                symbol_best_topic={},
                macro_v2_updated_topics=0,
                macro_v2_fallback_topics=0,
                macro_v2_skipped_no_news=0,
                message=msg,
            )

        anti_result = self.anti_fake_engine.apply(artifacts.scored_rows)
        artifacts.scored_rows = anti_result.scored_rows
        artifacts.top_rows = anti_result.top_rows

        # Must persist V1 top5 first, then start Macro V2 sidecar enrichment.
        self.scoring_engine.persist(td, artifacts)

        macro_v2_updated = 0
        macro_v2_fallback = 0
        macro_v2_skipped = 0
        if self.macro_v2_enricher is not None:
            try:
                v2_stats = self.macro_v2_enricher.enrich_top5(td, artifacts.top_rows)
                macro_v2_updated = int(v2_stats.updated_topics)
                macro_v2_fallback = int(v2_stats.fallback_topics)
                macro_v2_skipped = int(v2_stats.skipped_no_news)
            except Exception as exc:
                # Silent downgrade by contract: keep V1 and overlay path alive.
                logger.warning("Macro V2 enrich failed, silent downgrade enabled: %s", exc)

        overlay_result = self.overlay_engine.build(td)

        return PipelineResult(
            status="OK",
            trade_date=td,
            top_rows=artifacts.top_rows,
            scored_count=len(artifacts.scored_rows),
            topic_count=map_result.topic_count,
            mapping_used_cache=map_result.used_cache,
            mapping_used_fallback=map_result.used_fallback,
            filtered_topic_count=factor_result.filtered_topic_count,
            anti_fake_flagged_count=anti_result.flagged_count,
            overlay_rows=overlay_result.rows_written,
            symbol_labels=overlay_result.symbol_labels,
            symbol_best_topic=overlay_result.symbol_best_topic,
            macro_v2_updated_topics=macro_v2_updated,
            macro_v2_fallback_topics=macro_v2_fallback,
            macro_v2_skipped_no_news=macro_v2_skipped,
            message=(
                f"topics={map_result.topic_count} rows={len(artifacts.scored_rows)} "
                f"filtered_by_blacklist={factor_result.filtered_topic_count} "
                f"anti_fake_flagged={anti_result.flagged_count} "
                f"overlay_rows={overlay_result.rows_written} "
                f"moneyflow_rows={factor_result.moneyflow_rows} limit_rows={factor_result.limit_rows} "
                f"macro_v2_updated={macro_v2_updated} macro_v2_fallback={macro_v2_fallback} macro_v2_skipped={macro_v2_skipped}"
            ),
        )

    @staticmethod
    def _normalize_date(value: str) -> str:
        text = str(value or "").strip()
        if len(text) == 8 and text.isdigit():
            return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"
        if len(text) >= 10 and text[4] == "-" and text[7] == "-":
            return text[:10]
        raise ValueError(f"Unsupported trade_date format: {value}")
