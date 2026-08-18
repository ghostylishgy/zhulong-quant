#!/usr/bin/env python3

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb
from unittest.mock import patch

from tests import _test_log_isolation  # noqa: F401

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "01_engine" / "lib" / "rag_refresher.py"
SPEC = importlib.util.spec_from_file_location("zhulong_test_rag_refresher", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class RagMemoryQualityTest(unittest.TestCase):
    def test_placeholder_and_short_narratives_are_rejected(self):
        self.assertEqual(MODULE.narrative_quality_issue("..."), "punctuation_only")
        self.assertEqual(MODULE.narrative_quality_issue("详细审计报告"), "placeholder")
        self.assertEqual(MODULE.narrative_quality_issue("走势偏弱"), "too_short")

    def test_substantive_narrative_is_accepted(self):
        narrative = (
            "审计显示价格仍在中期均线下方，成交量未形成有效扩张，"
            "且潮汐门保持压制，因此本次只保留观察结论，不形成交易建议。"
        )
        self.assertEqual(MODULE.narrative_quality_issue(narrative), "")

    def test_embedding_fails_closed_before_chroma_write(self):
        refresher = MODULE.RAGRefresher.__new__(MODULE.RAGRefresher)
        refresher._chroma_available = True

        class _Collection:
            def upsert(self, **_kwargs):
                raise AssertionError("low-quality narrative must not reach Chroma")

        refresher._chroma_collection = _Collection()
        self.assertFalse(
            refresher.embed_to_chroma(
                "000001.SZ", "2026-06-18", "详细描述", "", source="audit"
            )
        )

    def test_embedding_metadata_binds_source_version_and_hashes(self):
        refresher = MODULE.RAGRefresher.__new__(MODULE.RAGRefresher)
        refresher._chroma_available = True

        class _Collection:
            def __init__(self):
                self.payload = None

            def delete(self, **_kwargs):
                return None

            def upsert(self, **kwargs):
                self.payload = kwargs

        collection = _Collection()
        refresher._chroma_collection = collection
        refresher._mark_embedding_status = lambda **_kwargs: None
        narrative = (
            "This sufficiently detailed audit narrative is deterministic and "
            "safe for an embedding metadata contract test."
        )
        self.assertTrue(
            refresher.embed_to_chroma(
                "000001.SZ",
                "2026-06-18",
                narrative,
                "#sample",
                source="audit",
                enrichment_status="TEMPLATE_READY",
            )
        )
        metadata = collection.payload["metadatas"][0]
        self.assertEqual(metadata["source"], "audit")
        self.assertEqual(metadata["memory_key"], "000001.SZ|2026-06-18|audit")
        self.assertEqual(metadata["document_version"], MODULE.RAG_DOCUMENT_VERSION)
        self.assertEqual(metadata["quarantine_status"], "CLEAR")
        self.assertEqual(len(metadata["document_sha256"]), 64)
        self.assertEqual(len(metadata["chunk_sha256"]), 64)

    def test_enrichment_update_rejects_bad_narrative_before_db_write(self):
        refresher = MODULE.RAGRefresher.__new__(MODULE.RAGRefresher)
        self.assertFalse(
            refresher.update_enriched_memory(
                {"symbol": "000001.SZ", "trade_date": "2026-06-18"},
                {"narrative": "英文记忆叙述", "enriched": True},
            )
        )

    def test_store_memory_cannot_overwrite_quarantined_identity(self):
        old_db_path = MODULE.DB_PATH
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                db_path = str(Path(tmpdir) / "rag_store.duckdb")
                conn = duckdb.connect(db_path)
                conn.execute(
                    """
                    CREATE TABLE fact_strategic_memory (
                        symbol VARCHAR, trade_date DATE, tide_status VARCHAR,
                        facts_json VARCHAR, narrative_text VARCHAR, ssd_tags VARCHAR,
                        source VARCHAR, l4_verdict VARCHAR, l4_score INTEGER,
                        embedding_status VARCHAR, created_at TIMESTAMP,
                        enrichment_status VARCHAR, enrichment_attempts INTEGER,
                        enrichment_error VARCHAR, enrichment_model VARCHAR,
                        enriched_at TIMESTAMP,
                        UNIQUE(symbol, trade_date, source)
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO fact_strategic_memory VALUES (
                        '000001.SZ', DATE '2026-07-01', '', '{}',
                        'preserved narrative', '', 'audit', 'HOLD', 50,
                        'quarantined', CURRENT_TIMESTAMP,
                        'QUARANTINED_SEMANTIC', 0, 'confirmed', 'legacy',
                        CURRENT_TIMESTAMP
                    )
                    """
                )
                conn.close()
                MODULE.DB_PATH = db_path
                refresher = MODULE.RAGRefresher.__new__(MODULE.RAGRefresher)
                stored = refresher.store_memory(
                    {
                        "symbol": "000001.SZ",
                        "trade_date": "2026-07-01",
                        "source": "audit",
                        "l4_verdict": "PASS",
                        "l4_final_score": 90,
                    },
                    {"narrative": "replacement narrative", "tags": "#strong_rps"},
                    "HOT",
                    enrichment_status="TEMPLATE_READY",
                )
                self.assertFalse(stored)
                conn = duckdb.connect(db_path, read_only=True)
                row = conn.execute(
                    """
                    SELECT narrative_text, enrichment_status
                    FROM fact_strategic_memory
                    WHERE symbol='000001.SZ'
                    """
                ).fetchone()
                conn.close()
                self.assertEqual(row, ("preserved narrative", "QUARANTINED_SEMANTIC"))
        finally:
            MODULE.DB_PATH = old_db_path

    def test_quarantined_identity_resists_all_status_update_paths(self):
        old_db_path = MODULE.DB_PATH
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                db_path = str(Path(tmpdir) / "rag_updates.duckdb")
                conn = duckdb.connect(db_path)
                conn.execute(
                    """
                    CREATE TABLE fact_strategic_memory (
                        symbol VARCHAR, trade_date DATE, tide_status VARCHAR,
                        facts_json VARCHAR, narrative_text VARCHAR, ssd_tags VARCHAR,
                        source VARCHAR, l4_verdict VARCHAR, l4_score INTEGER,
                        embedding_status VARCHAR, created_at TIMESTAMP,
                        enrichment_status VARCHAR, enrichment_attempts INTEGER,
                        enrichment_error VARCHAR, enrichment_model VARCHAR,
                        enriched_at TIMESTAMP,
                        UNIQUE(symbol, trade_date, source)
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO fact_strategic_memory VALUES (
                        '000001.SZ', DATE '2026-07-01', '', '{}',
                        'preserved narrative', '#weak_rps', 'audit', 'HOLD', 50,
                        'quarantined', CURRENT_TIMESTAMP,
                        'QUARANTINED_SEMANTIC', 2, 'confirmed', 'legacy',
                        CURRENT_TIMESTAMP
                    )
                    """
                )
                conn.close()
                MODULE.DB_PATH = db_path
                refresher = MODULE.RAGRefresher.__new__(MODULE.RAGRefresher)
                record = {
                    "symbol": "000001.SZ",
                    "trade_date": "2026-07-01",
                    "source": "audit",
                }
                updated = refresher.update_enriched_memory(
                    record,
                    {
                        "narrative": (
                            "\u5ba1\u8ba1\u8bc1\u636e\u4ecd\u7136\u7ed1\u5b9a\u539f\u59cb\u6765\u6e90\uff0c"
                            "\u9694\u79bb\u8bb0\u5fc6\u4e0d\u5141\u8bb8\u88ab\u6a21\u578b\u6da6\u8272\u8986\u76d6\uff0c"
                            "\u672c\u6b21\u4ec5\u9a8c\u8bc1\u72b6\u6001\u4e0d\u53d8\u5f0f\uff0c"
                            "\u4efb\u4f55\u540e\u7eed\u5237\u65b0\u90fd\u53ea\u80fd\u4fdd\u6301\u539f\u6709\u5185\u5bb9\u3002"
                        ),
                        "tags": "#strong_rps",
                        "model": "test",
                    },
                )
                refresher.mark_enrichment_failed(record, "test failure", "test")
                refresher._mark_embedding_status(
                    "000001.SZ", "2026-07-01", "failed", source="audit"
                )
                self.assertFalse(updated)
                conn = duckdb.connect(db_path, read_only=True)
                row = conn.execute(
                    """
                    SELECT narrative_text, ssd_tags, embedding_status,
                           enrichment_status, enrichment_attempts,
                           enrichment_error, enrichment_model
                    FROM fact_strategic_memory
                    WHERE symbol='000001.SZ'
                    """
                ).fetchone()
                conn.close()
                self.assertEqual(
                    row,
                    (
                        "preserved narrative", "#weak_rps", "quarantined",
                        "QUARANTINED_SEMANTIC", 2, "confirmed", "legacy",
                    ),
                )
        finally:
            MODULE.DB_PATH = old_db_path


    def test_quality_quarantine_resists_all_memory_update_paths(self):
        old_db_path = MODULE.DB_PATH
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                db_path = str(Path(tmpdir) / "rag_quality_quarantine.duckdb")
                conn = duckdb.connect(db_path)
                conn.execute(
                    """
                    CREATE TABLE fact_strategic_memory (
                        symbol VARCHAR, trade_date DATE, tide_status VARCHAR,
                        facts_json VARCHAR, narrative_text VARCHAR, ssd_tags VARCHAR,
                        source VARCHAR, l4_verdict VARCHAR, l4_score INTEGER,
                        embedding_status VARCHAR, created_at TIMESTAMP,
                        enrichment_status VARCHAR, enrichment_attempts INTEGER,
                        enrichment_error VARCHAR, enrichment_model VARCHAR,
                        enriched_at TIMESTAMP,
                        UNIQUE(symbol, trade_date, source)
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO fact_strategic_memory VALUES (
                        '000001.SZ', DATE '2026-06-18', '', '{}',
                        'preserved low-quality narrative', '#legacy', 'audit',
                        'HOLD', 50, 'quarantined', CURRENT_TIMESTAMP,
                        'ENRICH_DEFERRED', 1,
                        'narrative_quality_rejected:placeholder', 'legacy',
                        CURRENT_TIMESTAMP
                    )
                    """
                )
                conn.close()
                MODULE.DB_PATH = db_path
                refresher = MODULE.RAGRefresher.__new__(MODULE.RAGRefresher)
                record = {
                    "symbol": "000001.SZ",
                    "trade_date": "2026-06-18",
                    "source": "audit",
                    "_enrichment_status": "ENRICH_DEFERRED",
                    "_enrichment_attempts": 1,
                }
                synthesis = {
                    "narrative": (
                        "审计证据仍然绑定原始来源，质量隔离记忆不可被模板或模型覆盖，"
                        "本测试只验证各条写入路径都保持原始状态和叙事内容。"
                    ),
                    "tags": "#replacement",
                    "model": "test",
                }

                self.assertFalse(
                    refresher.store_memory(
                        record,
                        synthesis,
                        "SIDE",
                        enrichment_status="TEMPLATE_READY",
                    )
                )
                self.assertFalse(refresher.update_enriched_memory(record, synthesis))
                self.assertFalse(refresher.update_template_memory(record, synthesis))
                refresher.mark_enrichment_failed(record, "test failure", "test")
                self.assertEqual(
                    refresher.mark_backfill_failed(record, "test backfill failure", "test"),
                    "ENRICH_DEFERRED",
                )
                refresher._mark_embedding_status(
                    "000001.SZ", "2026-06-18", "embedded", source="audit"
                )

                conn = duckdb.connect(db_path, read_only=True)
                row = conn.execute(
                    """
                    SELECT narrative_text, ssd_tags, embedding_status,
                           enrichment_status, enrichment_attempts,
                           enrichment_error, enrichment_model
                    FROM fact_strategic_memory
                    WHERE symbol = '000001.SZ'
                    """
                ).fetchone()
                conn.close()
                self.assertEqual(
                    row,
                    (
                        "preserved low-quality narrative",
                        "#legacy",
                        "quarantined",
                        "ENRICH_DEFERRED",
                        1,
                        "narrative_quality_rejected:placeholder",
                        "legacy",
                    ),
                )
        finally:
            MODULE.DB_PATH = old_db_path

    def test_weekly_backfill_excludes_quality_quarantined_memory(self):
        old_db_path = MODULE.DB_PATH
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                db_path = str(Path(tmpdir) / "rag_backfill_candidates.duckdb")
                conn = duckdb.connect(db_path)
                conn.execute(
                    """
                    CREATE TABLE fact_strategic_memory (
                        symbol VARCHAR, trade_date DATE, tide_status VARCHAR,
                        facts_json VARCHAR, narrative_text VARCHAR, ssd_tags VARCHAR,
                        source VARCHAR, l4_verdict VARCHAR, l4_score INTEGER,
                        embedding_status VARCHAR, created_at TIMESTAMP,
                        enrichment_status VARCHAR, enrichment_attempts INTEGER,
                        enrichment_error VARCHAR, enrichment_model VARCHAR,
                        enriched_at TIMESTAMP,
                        UNIQUE(symbol, trade_date, source)
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO fact_strategic_memory VALUES
                    ('000001.SZ', DATE '2026-06-18', '', '{}',
                     'bad narrative', '', 'audit', 'HOLD', 50,
                     'quarantined', CURRENT_TIMESTAMP, 'ENRICH_DEFERRED', 1,
                     'narrative_quality_rejected:placeholder', 'legacy', NULL),
                    ('000002.SZ', DATE '2026-06-18', '', '{}',
                     'retryable narrative', '', 'audit', 'HOLD', 50,
                     'pending', CURRENT_TIMESTAMP, 'ENRICH_DEFERRED', 1,
                     'timeout', 'legacy', NULL)
                    """
                )
                conn.close()
                MODULE.DB_PATH = db_path
                refresher = MODULE.RAGRefresher.__new__(MODULE.RAGRefresher)
                candidates = refresher._fetch_backfill_candidates(
                    12,
                    include_deferred=True,
                    deferred_only=True,
                )
                self.assertEqual(
                    [candidate["symbol"] for candidate in candidates],
                    ["000002.SZ"],
                )
        finally:
            MODULE.DB_PATH = old_db_path

    def test_empty_daily_extract_still_repairs_and_backfills_embeddings(self):
        refresher = MODULE.RAGRefresher.__new__(MODULE.RAGRefresher)
        refresher._chroma_available = True
        refresher._chroma_collection = object()
        refresher._reset_synthesis_guard = lambda: None
        refresher.quarantine_low_quality_narratives = lambda: {
            "quarantined": 0,
            "vectors_removed": 0,
        }
        refresher.extract_daily_logs = lambda _trade_date: []
        refresher._check_ram = lambda: True
        repairs = []

        def reconcile(repair_missing=False):
            repairs.append(repair_missing)
            return {
                "db_embedded": 2,
                "chroma_groups": 2,
                "missing_groups": 0,
                "orphan_groups": 0,
                "requeued_groups": 1 if repair_missing else 0,
            }

        refresher.reconcile_embedding_inventory = reconcile
        refresher.backfill_pending_embeddings = lambda limit: 1
        with patch.object(MODULE, "_force_checkpoint"):
            stats = refresher.refresh("2026-06-21")
        self.assertEqual(repairs, [True, False])
        self.assertEqual(stats["inventory_requeued_groups"], 1)
        self.assertEqual(stats["embedded_backfill"], 1)


if __name__ == "__main__":
    unittest.main()
