#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Smoke test for P1 RAG memory pipeline on node 121.

Checks:
1) run pending embedding backfill in rag_refresher
2) verify embedding status changes in DuckDB
3) build macro prompt preview with MEMORY_CONTEXT and print full prompt
"""

from __future__ import annotations

import runpy
import logging
import sys
from pathlib import Path
from typing import Dict


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "storage" / "database" / "zhulong.duckdb"

_loader_ns = runpy.run_path(str(PROJECT_ROOT / "04_governance" / "lib" / "core" / "module_loader.py"))
load_attr_from_path = _loader_ns["load_attr_from_path"]
load_module_from_path = _loader_ns["load_module_from_path"]

DBGateway = load_attr_from_path(
    "db_gateway_smoke_p1",
    PROJECT_ROOT / "01_engine" / "lib" / "db_gateway.py",
    "DBGateway",
)


def _embedding_status_counts() -> Dict[str, int]:
    with DBGateway(DB_PATH, read_only=True, logger=logging.getLogger("smoke_p1")) as conn:
        rows = conn.execute(
            """
            SELECT COALESCE(embedding_status, 'NULL') AS embedding_status,
                   COUNT(*) AS cnt
            FROM fact_strategic_memory
            GROUP BY 1
            ORDER BY 2 DESC
            """
        ).fetchall()
    return {str(k): int(v) for k, v in rows}


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    rag_mod = load_module_from_path("rag_refresher_smoke", PROJECT_ROOT / "01_engine" / "lib" / "rag_refresher.py")
    macro_mod = load_module_from_path("macro_v2_enricher_smoke", PROJECT_ROOT / "07_macro" / "lib" / "macro_v2_enricher.py")

    before = _embedding_status_counts()
    refresher = rag_mod.get_refresher()
    embedded_count = int(refresher.backfill_pending_embeddings(limit=30))
    after = _embedding_status_counts()

    sample_topic = "人工智能算力基础设施"
    sample_news = (
        "国家层面继续强调算力基础设施建设与产业链自主可控，"
        "多地发布新一轮算力中心配套政策，市场关注资金回流与估值切换。"
        "同时，部分个股短期涨幅较大，存在交易拥挤与兑现压力。"
    )

    enricher = macro_mod.MacroV2Enricher(db_path=str(DB_PATH))
    preview = enricher.build_prompt_preview(topic_name=sample_topic, merged_news_text=sample_news)

    print("===P1_SMOKE_RAG_CONTEXT===")
    print(f"DB_PATH={DB_PATH}")
    print(f"EMBED_STATUS_BEFORE={before}")
    print(f"EMBED_BACKFILL_COUNT={embedded_count}")
    print(f"EMBED_STATUS_AFTER={after}")
    print(f"MEMORY_SOURCE={preview.get('memory_source')}")
    print(f"MEMORY_HIT_COUNT={len(preview.get('memory_hits') or [])}")

    print("===MEMORY_HITS_BEGIN===")
    for idx, row in enumerate(preview.get("memory_hits") or [], start=1):
        print(f"{idx}. {row}")
    print("===MEMORY_HITS_END===")

    print("===FULL_PROMPT_BEGIN===")
    print(preview.get("prompt") or "")
    print("===FULL_PROMPT_END===")

    if not preview.get("memory_hits"):
        print("SMOKE_RESULT=FAIL(memory_hits=0)")
        return 2

    print("SMOKE_RESULT=PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
