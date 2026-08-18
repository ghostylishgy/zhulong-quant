#!/usr/bin/env python3

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb

from tests import _test_log_isolation  # noqa: F401

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "02_brain" / "lib" / "rag_pipeline.py"
SPEC = importlib.util.spec_from_file_location("zhulong_test_rag_pipeline", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class _NoDrift:
    @staticmethod
    def check_drift():
        return None


class RagSemanticGateTest(unittest.TestCase):
    def pipeline(self, semantic_hits, chroma_available=True):
        pipeline = MODULE.RAGPipeline.__new__(MODULE.RAGPipeline)
        pipeline._chroma = object() if chroma_available else None
        pipeline.drift = _NoDrift()
        pipeline._semantic_search = lambda query, symbol, top_k: list(semantic_hits)
        pipeline._validate_semantic_hits = lambda hits: list(hits)
        pipeline._factual_search = lambda symbol, top_k: [{
            "symbol": symbol,
            "trade_date": "2026-06-18",
            "tide_status": "SIDE",
            "facts": '{"close": 10.0}',
            "narrative": "historical AI narrative must be gated",
            "tags": "#sample",
            "verdict": "HOLD",
            "score": 60,
            "source": "audit",
            "enrichment_status": "ENRICHED",
        }]
        return pipeline

    def test_high_similarity_includes_narrative(self):
        pipeline = self.pipeline([{"score": 0.80, "doc": "semantic matched narrative"}, {"score": 0.70, "doc": "secondary semantic narrative"}])
        result = pipeline.retrieve("000725.SZ", query_text="related query")
        self.assertTrue(result.injection_allowed)
        self.assertFalse(result.amnesia_triggered)
        self.assertIn("[语义层", result.prompt_block)
        self.assertIn("semantic matched narrative", result.prompt_block)
        self.assertNotIn("historical AI narrative must be gated", result.prompt_block)
        self.assertIn("双阈值通过", result.prompt_block)

    def test_low_similarity_keeps_facts_but_blocks_narrative(self):
        pipeline = self.pipeline([{"score": 0.56, "doc": "low semantic narrative"}, {"score": 0.54, "doc": "secondary low narrative"}])
        result = pipeline.retrieve("000725.SZ", query_text="unrelated query")
        self.assertFalse(result.injection_allowed)
        self.assertTrue(result.amnesia_triggered)
        self.assertIn("[事实层", result.prompt_block)
        self.assertNotIn("[语义层", result.prompt_block)
        self.assertNotIn("historical AI narrative", result.prompt_block)

    def test_unavailable_semantic_reader_fails_closed_for_narrative(self):
        pipeline = self.pipeline([], chroma_available=False)
        result = pipeline.retrieve("000725.SZ", query_text="related query")
        self.assertFalse(result.injection_allowed)
        self.assertTrue(result.amnesia_triggered)
        self.assertIn("[事实层", result.prompt_block)
        self.assertNotIn("[语义层", result.prompt_block)
        self.assertNotIn("双阈值通过", result.prompt_block)

    def test_empty_query_is_explicit_facts_only(self):
        pipeline = self.pipeline([], chroma_available=False)
        result = pipeline.retrieve("000725.SZ")
        self.assertFalse(result.injection_allowed)
        self.assertFalse(result.amnesia_triggered)
        self.assertIn("RAG_FACTS_ONLY", result.prompt_block)
        self.assertNotIn("[语义层", result.prompt_block)


    def test_semantic_search_queries_all_retrievable_vectors(self):
        class _Chroma:
            def __init__(self):
                self.where = None

            def query(self, **kwargs):
                self.where = kwargs["where"]
                return {"documents": [[]], "distances": [[]], "metadatas": [[]]}

        pipeline = MODULE.RAGPipeline.__new__(MODULE.RAGPipeline)
        pipeline._chroma = _Chroma()
        pipeline._semantic_search("query", "000001.SZ")
        self.assertEqual(
            pipeline._chroma.where,
            {
                "$and": [
                    {"symbol": "000001.SZ"},
                    {"enrichment_status": {"$in": ["ENRICHED", "TEMPLATE_READY"]}},
                ]
            },
        )

    def test_semantic_identity_gate_binds_hit_to_current_db_row(self):
        old_db_path = MODULE.DB_PATH
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                db_path = str(Path(tmpdir) / "rag_identity.duckdb")
                conn = duckdb.connect(db_path)
                conn.execute(
                    """
                    CREATE TABLE fact_strategic_memory (
                        symbol VARCHAR, trade_date DATE, narrative_text VARCHAR,
                        ssd_tags VARCHAR, source VARCHAR,
                        enrichment_status VARCHAR, embedding_status VARCHAR
                    )
                    """
                )
                conn.executemany(
                    "INSERT INTO fact_strategic_memory VALUES (?, ?, ?, ?, ?, ?, ?)",
                    [
                        ["000001.SZ", "2026-06-18", "current narrative", "#sample", "audit", "ENRICHED", "embedded"],
                        ["000002.SZ", "2026-06-18", "quarantined narrative", "", "audit", "QUARANTINED_SEMANTIC", "quarantined"],
                    ],
                )
                conn.close()
                MODULE.DB_PATH = db_path
                pipeline = MODULE.RAGPipeline.__new__(MODULE.RAGPipeline)
                valid = {
                    "id": "valid",
                    "doc": "current narrative",
                    "score": 0.8,
                    "symbol": "000001.SZ",
                    "trade_date": "2026-06-18",
                    "source": "audit",
                }
                stale = dict(valid, id="stale", doc="stale unrelated narrative")
                quarantined = {
                    "id": "quarantined",
                    "doc": "quarantined narrative",
                    "score": 0.9,
                    "symbol": "000002.SZ",
                    "trade_date": "2026-06-18",
                    "source": "audit",
                }
                hits = pipeline._validate_semantic_hits([stale, quarantined, valid])
                self.assertEqual([hit["id"] for hit in hits], ["valid"])
                self.assertTrue(hits[0]["db_validated"])
        finally:
            MODULE.DB_PATH = old_db_path

    def test_factual_search_hides_non_retrievable_narratives(self):
        old_db_path = MODULE.DB_PATH
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                db_path = str(Path(tmpdir) / "rag.duckdb")
                conn = duckdb.connect(db_path)
                conn.execute(
                    """
                    CREATE TABLE fact_strategic_memory (
                        symbol VARCHAR, trade_date DATE, tide_status VARCHAR,
                        facts_json VARCHAR, narrative_text VARCHAR, ssd_tags VARCHAR,
                        l4_verdict VARCHAR, l4_score INTEGER, source VARCHAR,
                        enrichment_status VARCHAR
                    )
                    """
                )
                conn.executemany(
                    "INSERT INTO fact_strategic_memory VALUES (?, ?, '', '{}', ?, '', '', 0, 'audit', ?)",
                    [
                        ["000001.SZ", "2026-06-18", "approved narrative", "ENRICHED"],
                        ["000001.SZ", "2026-06-17", "template narrative", "TEMPLATE_READY"],
                        ["000001.SZ", "2026-06-16", "quarantined narrative", "ENRICH_DEFERRED"],
                    ],
                )
                conn.close()
                MODULE.DB_PATH = db_path
                pipeline = MODULE.RAGPipeline.__new__(MODULE.RAGPipeline)
                facts = pipeline._factual_search("000001.SZ", top_k=5)
                self.assertEqual(facts[0]["narrative"], "approved narrative")
                self.assertEqual(facts[1]["narrative"], "template narrative")
                self.assertEqual(facts[2]["narrative"], "")
        finally:
            MODULE.DB_PATH = old_db_path


if __name__ == "__main__":
    unittest.main()
