#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
02_brain/lib/rag_pipeline.py
RAG Pipeline v2 - Bayesian Prior Hub

Core:
    Dual-Threshold: Top1 >= 0.65 AND Top1-Top2 >= 0.03 -> inject
    STRATEGIC_AMNESIA: insufficient similarity -> block hallucination
    Facts > Narrative: raw SQL fields shown before AI narrative
    Tide Context: compare historical vs current tide state
    Drift Monitor: 3 consecutive tag failures -> system alert
"""

import hashlib
import json
import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, field

logger = logging.getLogger('zhulong.rag_pipeline')

_current = Path(__file__).resolve()
PROJECT_ROOT = next(
    (p for p in _current.parents if (p / ".git").exists() or (p / "storage").exists()),
    _current.parents[2]
)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))
from config.settings import Config
from config.rag_contract import (
    COLLECTION_NAME,
    EMBEDDING_DIMENSION,
    EMBEDDING_MODEL,
    collection_creation_metadata,
    validate_reader_contract,
)
OLLAMA_SERVER = str(Config.OLLAMA_URL).split("/api/", 1)[0]
DB_PATH = str(PROJECT_ROOT / "storage" / "database" / "zhulong.duckdb")
CHROMA_DIR = str(PROJECT_ROOT / "storage" / "chromadb")
SSD_PATH = PROJECT_ROOT / "04_governance" / "config" / "semantic_dictionary.json"

_CORE_DIR = PROJECT_ROOT / "04_governance" / "lib" / "core"
if str(_CORE_DIR) not in sys.path:
    sys.path.append(str(_CORE_DIR))
from module_loader import load_attr_from_path

DBGateway = load_attr_from_path(
    "db_gateway_01",
    PROJECT_ROOT / "01_engine" / "lib" / "db_gateway.py",
    "DBGateway",
)
ComputeGateway = load_attr_from_path(
    "compute_gateway_02",
    PROJECT_ROOT / "02_brain" / "lib" / "compute_gateway.py",
    "ComputeGateway",
)
COMPUTE_GATEWAY = ComputeGateway(logger=logger, max_slots=3)

# Dual threshold
SIMILARITY_THRESHOLD = 0.65
DELTA_THRESHOLD = 0.03
INDEXABLE_ENRICHMENT_STATUSES = ("ENRICHED", "TEMPLATE_READY")
RAG_DOCUMENT_VERSION = "rag_memory_document_v1"

# Drift monitoring
DRIFT_STRIKE_LIMIT = 3


# ==================== Data Structures ====================

@dataclass
class RetrievalResult:
    """Result of a RAG retrieval"""
    symbol: str
    has_case_file: bool = False
    injection_allowed: bool = False
    amnesia_triggered: bool = False
    prompt_block: str = ""
    facts: List[Dict] = field(default_factory=list)
    semantic_hits: List[Dict] = field(default_factory=list)
    similarity_scores: List[float] = field(default_factory=list)
    tide_comparison: str = ""
    drift_alert: str = ""


# ==================== Cognitive Drift Monitor ====================

class DriftMonitor:
    """Track consecutive tag failures for cognitive drift detection"""

    def __init__(self):
        self._tag_failures: Dict[str, int] = {}
        self._ssd_config = {}
        self._load_ssd()

    def _load_ssd(self):
        if SSD_PATH.exists():
            with open(SSD_PATH, 'r', encoding='utf-8') as f:
                data = json.load(f)
            self._ssd_config = data.get("drift_thresholds", {})

    def record_outcome(self, tags: List[str], success: bool):
        """Record whether a tagged prediction was correct"""
        for tag in tags:
            if success:
                self._tag_failures[tag] = 0
            else:
                self._tag_failures[tag] = self._tag_failures.get(tag, 0) + 1

    def check_drift(self) -> Optional[str]:
        """Return alert message if any tag has consecutive failures >= limit"""
        limit = self._ssd_config.get("consecutive_failures_before_alert", DRIFT_STRIKE_LIMIT)
        template = self._ssd_config.get(
            "alert_message",
            "COGNITIVE_DRIFT: Tag {tag} consecutive failures: {count}"
        )
        alerts = []
        for tag, count in self._tag_failures.items():
            if count >= limit:
                alerts.append(template.format(tag=tag, count=count))
        return "\n".join(alerts) if alerts else None

    def get_status(self) -> Dict[str, int]:
        return dict(self._tag_failures)


# ==================== RAG Pipeline ====================

class RAGPipeline:
    """
    Bayesian Prior Hub for L4 Decision Injection

    1. Retrieve case files (facts + narrative) for a symbol
    2. Apply dual-threshold (similarity >= 0.65, delta >= 0.03)
    3. If passed: build injection prompt (facts first, then narrative)
    4. If failed: [STRATEGIC_AMNESIA] - block injection
    5. Always compare tide context (then vs now)
    """

    def __init__(self):
        self.drift = DriftMonitor()
        self._chroma = None
        self._init_chroma()

    def _init_chroma(self):
        """Initialize ChromaDB reader under the shared vector contract."""
        try:
            import logging
            import chromadb
            from chromadb import Documents, EmbeddingFunction, Embeddings
            from chromadb.config import Settings

            logging.getLogger("chromadb").setLevel(logging.CRITICAL)
            logging.getLogger("chromadb.telemetry").setLevel(logging.CRITICAL)
            logging.getLogger("chromadb.telemetry.product.posthog").setLevel(logging.CRITICAL)

            class OllamaMxbaiEmbedding(EmbeddingFunction):
                """Custom embedding through the configured Ollama model."""
                def __call__(self, input: Documents) -> Embeddings:
                    embeddings = []
                    for text in input:
                        try:
                            resp = COMPUTE_GATEWAY.ollama_embeddings(
                                server=OLLAMA_SERVER,
                                payload={"model": EMBEDDING_MODEL, "prompt": text},
                                timeout=60,
                                layer="RAG",
                                decision_id="rag_pipeline:embedding",
                            )
                            if resp.status_code != 200:
                                detail = str(getattr(resp, "text", ""))[:200]
                                raise RuntimeError(f"Embedding HTTP {resp.status_code}: {detail}")
                            payload = resp.json() if resp.content else {}
                            embedding = payload.get("embedding")
                            if not isinstance(embedding, list) or not embedding:
                                raise RuntimeError("Embedding response missing vector")
                            if not any(abs(float(v or 0.0)) > 0.0 for v in embedding):
                                raise RuntimeError("Embedding response is all-zero vector")
                            embeddings.append(embedding)
                        except Exception as exc:
                            logger.warning(f"Query embedding failed closed: {exc}")
                            raise
                    return embeddings

            client = chromadb.PersistentClient(
                path=CHROMA_DIR,
                settings=Settings(anonymized_telemetry=False),
            )
            self._embed_fn = OllamaMxbaiEmbedding()
            self._chroma = client.get_or_create_collection(
                name=COLLECTION_NAME,
                metadata=collection_creation_metadata(),
                embedding_function=self._embed_fn
            )
            validate_reader_contract(self._chroma)
            logger.info(
                f"ChromaDB reader ready collection={COLLECTION_NAME} "
                f"model={EMBEDDING_MODEL} dim={EMBEDDING_DIMENSION}"
            )
        except Exception as e:
            self._chroma = None
            logger.info(f"ChromaDB unavailable: {e}, using DuckDB-only retrieval")

    # ==================== Retrieval ====================

    def retrieve(self, symbol: str, current_tide: str = "",
                 query_text: str = "", top_k: int = 5) -> RetrievalResult:
        """
        Main retrieval entry point.
        Dual path: ChromaDB semantic + DuckDB factual
        """
        result = RetrievalResult(symbol=symbol)

        # Path A: ChromaDB semantic search (if available + query)
        semantic_hits = []
        if self._chroma and query_text:
            raw_semantic_hits = self._semantic_search(query_text, symbol, top_k)
            semantic_hits = self._validate_semantic_hits(raw_semantic_hits)
            result.semantic_hits = semantic_hits

        # Path B: DuckDB factual search (always available)
        fact_hits = self._factual_search(symbol, top_k)

        if not fact_hits and not semantic_hits:
            result.amnesia_triggered = True
            result.prompt_block = f"[STRATEGIC_AMNESIA] No case file for {symbol}. Zero prior."
            return result

        result.has_case_file = True
        result.facts = fact_hits

        # Dual threshold check
        if semantic_hits:
            scores = [h["score"] for h in semantic_hits]
            result.similarity_scores = scores

            top1 = scores[0] if scores else 0
            top2 = scores[1] if len(scores) > 1 else 0
            delta = top1 - top2

            if top1 >= SIMILARITY_THRESHOLD and delta >= DELTA_THRESHOLD:
                result.injection_allowed = True
                logger.info(
                    f"RAG INJECT: {symbol} top1={top1:.3f} delta={delta:.3f} PASS"
                )
            else:
                result.amnesia_triggered = True
                logger.info(
                    f"RAG AMNESIA: {symbol} top1={top1:.3f} delta={delta:.3f} "
                    f"(need top1>={SIMILARITY_THRESHOLD}, delta>={DELTA_THRESHOLD})"
                )
        elif query_text:
            # Semantic relevance was requested but could not be established.
            # Keep raw facts available, but fail closed for AI narratives.
            result.amnesia_triggered = True
            logger.info(f"RAG FACTS-ONLY: {symbol} semantic relevance unavailable")
        else:
            logger.info(f"RAG FACTS-ONLY: {symbol} no semantic query supplied")

        # Tide comparison
        if fact_hits and current_tide:
            result.tide_comparison = self._compare_tide(fact_hits, current_tide)

        # Drift check
        drift_alert = self.drift.check_drift()
        if drift_alert:
            result.drift_alert = drift_alert

        # Build prompt block
        result.prompt_block = self._build_prompt(result, current_tide)

        return result

    def _semantic_search(self, query: str, symbol: str,
                         top_k: int = 5) -> List[Dict]:
        """ChromaDB semantic similarity search"""
        try:
            results = self._chroma.query(
                query_texts=[query],
                n_results=top_k,
                where={
                    "$and": [
                        {"symbol": symbol},
                        {"enrichment_status": {"$in": list(INDEXABLE_ENRICHMENT_STATUSES)}},
                    ]
                },
            )
            hits = []
            if results and results["documents"]:
                result_ids = (results.get("ids") or [[]])[0]
                for i, doc in enumerate(results["documents"][0]):
                    dist = results["distances"][0][i] if results["distances"] else 1.0
                    score = 1.0 - dist  # cosine distance -> similarity
                    meta = results["metadatas"][0][i] if results["metadatas"] else {}
                    hit_id = result_ids[i] if i < len(result_ids) else ""
                    hits.append({
                        "id": hit_id,
                        "doc": doc,
                        "score": score,
                        "symbol": meta.get("symbol", symbol),
                        "trade_date": meta.get("trade_date", ""),
                        "source": meta.get("source", ""),
                        "tags": meta.get("tags", ""),
                        "enrichment_status": meta.get("enrichment_status", ""),
                        "document_version": meta.get("document_version", ""),
                        "document_sha256": meta.get("document_sha256", ""),
                        "chunk_sha256": meta.get("chunk_sha256", ""),
                        "memory_key": meta.get("memory_key", ""),
                        "quarantine_status": meta.get("quarantine_status", ""),
                    })
            return sorted(hits, key=lambda x: x["score"], reverse=True)
        except Exception as e:
            logger.warning(f"Semantic search failed: {e}")
            return []

    def _validate_semantic_hits(self, hits: List[Dict]) -> List[Dict]:
        """Bind every Chroma hit to its current DuckDB row before it can be injected."""
        if not hits:
            return []
        validated: List[Dict] = []
        rejected = 0
        try:
            with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
                for hit in hits:
                    symbol = str(hit.get("symbol") or "").strip()
                    trade_date = str(hit.get("trade_date") or "").strip()
                    source = str(hit.get("source") or "").strip()
                    document = str(hit.get("doc") or "")
                    if not symbol or not trade_date or not source or not document:
                        rejected += 1
                        continue
                    row = conn.execute(
                        """
                        SELECT narrative_text, ssd_tags, enrichment_status, embedding_status
                        FROM fact_strategic_memory
                        WHERE symbol = ?
                          AND trade_date = CAST(? AS DATE)
                          AND source = ?
                        """,
                        [symbol, trade_date, source],
                    ).fetchone()
                    if not row:
                        rejected += 1
                        continue
                    narrative = str(row[0] or "")
                    tags = str(row[1] or "")
                    enrichment_status = str(row[2] or "").upper()
                    embedding_status = str(row[3] or "").lower()
                    if (
                        enrichment_status not in INDEXABLE_ENRICHMENT_STATUSES
                        or embedding_status != "embedded"
                    ):
                        rejected += 1
                        continue
                    if str(hit.get("quarantine_status") or "").upper() not in {"", "CLEAR"}:
                        rejected += 1
                        continue

                    full_text = f"{narrative} {tags}"
                    document_sha256 = hashlib.sha256(
                        full_text.encode("utf-8")
                    ).hexdigest()
                    chunk_sha256 = hashlib.sha256(
                        document.encode("utf-8")
                    ).hexdigest()
                    expected_key = f"{symbol}|{trade_date}|{source}"
                    metadata_version = str(hit.get("document_version") or "")
                    metadata_doc_sha = str(hit.get("document_sha256") or "")
                    metadata_chunk_sha = str(hit.get("chunk_sha256") or "")
                    metadata_key = str(hit.get("memory_key") or "")

                    if metadata_version and metadata_version != RAG_DOCUMENT_VERSION:
                        rejected += 1
                        continue
                    if metadata_doc_sha and metadata_doc_sha != document_sha256:
                        rejected += 1
                        continue
                    if metadata_chunk_sha and metadata_chunk_sha != chunk_sha256:
                        rejected += 1
                        continue
                    if metadata_key and metadata_key != expected_key:
                        rejected += 1
                        continue
                    # Legacy vectors do not carry hashes. Exact content membership
                    # still prevents a stale or unrelated Chroma chunk from injection.
                    if document not in full_text:
                        rejected += 1
                        continue

                    valid_hit = dict(hit)
                    valid_hit["db_validated"] = True
                    valid_hit["document_sha256"] = document_sha256
                    valid_hit["memory_key"] = expected_key
                    validated.append(valid_hit)
        except Exception as exc:
            logger.warning(f"Semantic hit identity validation failed closed: {exc}")
            return []

        if rejected:
            logger.warning(
                f"RAG semantic identity gate rejected={rejected} accepted={len(validated)}"
            )
        return sorted(validated, key=lambda item: item["score"], reverse=True)

    def _factual_search(self, symbol: str, top_k: int = 5) -> List[Dict]:
        """DuckDB fact-based retrieval (always available)"""
        try:
            with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
                rows = conn.execute(
                    """
                    SELECT symbol, trade_date, tide_status, facts_json,
                           CASE
                               WHEN UPPER(COALESCE(enrichment_status, '')) IN ('ENRICHED', 'TEMPLATE_READY')
                               THEN narrative_text
                               ELSE ''
                           END AS narrative_text,
                           ssd_tags, l4_verdict, l4_score,
                           source, enrichment_status
                    FROM fact_strategic_memory
                    WHERE symbol = ?
                      AND UPPER(COALESCE(enrichment_status, '')) NOT LIKE 'RETIRED%'
                      AND UPPER(COALESCE(enrichment_status, '')) NOT LIKE 'QUARANTINED%'
                    ORDER BY trade_date DESC
                    LIMIT ?
                    """,
                    [symbol, top_k],
                ).fetchall()

            return [
                {
                    "symbol": r[0],
                    "trade_date": str(r[1]),
                    "tide_status": r[2] or "",
                    "facts": r[3] or "{}",
                    "narrative": r[4] or "",
                    "tags": r[5] or "",
                    "verdict": r[6] or "",
                    "score": r[7] or 0,
                    "source": r[8] or "audit",
                    "enrichment_status": r[9] or "",
                }
                for r in rows
            ]
        except Exception as e:
            logger.warning(f"Factual search failed: {e}")
            return []

    def _compare_tide(self, facts: List[Dict], current_tide: str) -> str:
        """Compare historical tide state vs current"""
        comparisons = []
        for f in facts[:3]:
            hist_tide = f.get("tide_status", "UNKNOWN")
            comparisons.append(
                f"  [{f['trade_date']}] 当时潮汐={hist_tide} → 当前潮汐={current_tide}"
            )
        return "\n".join(comparisons)

    # ==================== Prompt Builder ====================

    def _build_prompt(self, result: RetrievalResult, current_tide: str) -> str:
        """
        Build injection prompt block.
        ORDER: Facts FIRST, then Narrative (per architect directive)
        """
        if result.amnesia_triggered and not result.facts:
            return f"[STRATEGIC_AMNESIA] {result.symbol}: 无历史案底，禁止脑补。"

        lines = [f"### 📋 战略记忆: {result.symbol}"]

        # 1. FACTS FIRST (raw SQL fields)
        if result.facts:
            lines.append("\n**[事实层 — 原始数据]**")
            for f in result.facts[:3]:
                try:
                    facts_data = json.loads(f["facts"]) if isinstance(f["facts"], str) else f["facts"]
                except (json.JSONDecodeError, TypeError):
                    facts_data = {}

                source = str(f.get("source") or "audit")
                source_label = self._source_label(source)
                enrichment = str(f.get("enrichment_status") or "").strip()
                enrichment_part = f"记忆状态={enrichment} | " if enrichment else ""
                score_part = (
                    f"评分={f.get('score',0)} | "
                    if source == "audit"
                    else ""
                )
                lines.append(
                    f"  📌 {f['trade_date']} | "
                    f"来源={source_label} | "
                    f"结论={f.get('verdict','?')} | "
                    f"{enrichment_part}"
                    f"{score_part}"
                    f"标签={f.get('tags','无')} | "
                    f"潮汐={f.get('tide_status','?')}"
                )

        # 2. NARRATIVE SECOND (AI synthesis). Raw facts may degrade gracefully,
        # but generated narratives require an explicit semantic gate pass.
        narratives = (
            [hit["doc"] for hit in result.semantic_hits if hit.get("doc")]
            if result.injection_allowed
            else []
        )
        if narratives:
            lines.append("\n**[语义层 — AI审判词]**")
            for n in narratives[:2]:
                lines.append(f"  🔍 {n}")

        # 3. TIDE COMPARISON
        if result.tide_comparison:
            lines.append(f"\n**[环境对账]**\n{result.tide_comparison}")

        # 4. INJECTION STATUS
        if result.injection_allowed:
            lines.append("\n✅ 记忆注入已授权 (双阈值通过)")
        elif result.amnesia_triggered:
            lines.append("\n🔒 [STRATEGIC_AMNESIA] 语义叙事未授权；仅保留事实层")
        else:
            lines.append("\nℹ️ [RAG_FACTS_ONLY] 未执行语义校验；仅保留事实层")

        # 5. DRIFT ALERT
        if result.drift_alert:
            lines.append(f"\n⚠️ {result.drift_alert}")

        return "\n".join(lines)

    @staticmethod
    def _source_label(source: str) -> str:
        source = str(source or "").strip()
        if source == "audit":
            return "L4审计"
        if source.startswith("strategy:"):
            return "模拟盘绩效"
        if source.startswith("execution:"):
            return "模拟盘执行"
        if source == "echo":
            return "Echo观察"
        return source or "未知"

    # ==================== Convenience: get_intel_summary ====================

    def get_intel_summary(self, symbol: str, current_tide: str = "", query_text: str = "") -> str:
        """
        Drop-in replacement for the old RAGIntegration.get_intel_summary().
        query_text enables Chroma semantic retrieval; empty query keeps DuckDB factual mode.
        """
        result = self.retrieve(symbol, current_tide=current_tide, query_text=query_text)
        return result.prompt_block

    # ==================== Status ====================

    def get_status(self) -> Dict[str, Any]:
        """System status for monitoring"""
        try:
            with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
                total = conn.execute("SELECT COUNT(*) FROM fact_strategic_memory").fetchone()[0]
                by_tag = conn.execute("""
                    SELECT ssd_tags, COUNT(*) as cnt
                    FROM fact_strategic_memory
                    WHERE ssd_tags != ''
                    GROUP BY ssd_tags
                    ORDER BY cnt DESC
                    LIMIT 10
                """).fetchall()
        except Exception:
            total = 0
            by_tag = []

        return {
            "total_memories": total,
            "chromadb_available": self._chroma is not None,
            "tag_distribution": {r[0]: r[1] for r in by_tag},
            "drift_status": self.drift.get_status(),
            "thresholds": {
                "similarity": SIMILARITY_THRESHOLD,
                "delta": DELTA_THRESHOLD
            }
        }


# ==================== Backward Compatibility ====================

class RAGIntegration:
    """
    Backward-compatible wrapper for decision_engine.py
    Replaces the old RAGIntegration class
    """
    def __init__(self):
        self.pipeline = RAGPipeline()
        self.rag_available = True

    def get_intel_summary(self, symbol: str) -> str:
        return self.pipeline.get_intel_summary(symbol)


# ==================== Singleton ====================

_pipeline = None
def get_pipeline() -> RAGPipeline:
    global _pipeline
    if _pipeline is None:
        _pipeline = RAGPipeline()
    return _pipeline


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    pipeline = get_pipeline()

    print("=" * 60)
    print("  RAG PIPELINE v2 SELF-TEST")
    print("=" * 60)

    # Status
    status = pipeline.get_status()
    print(f"\nMemories: {status['total_memories']}")
    print(f"ChromaDB: {'available' if status['chromadb_available'] else 'DuckDB-only'}")
    print(f"Thresholds: sim>={status['thresholds']['similarity']} delta>={status['thresholds']['delta']}")

    # Test retrieval
    print("\n--- Retrieval Test: 000001.SZ ---")
    result = pipeline.retrieve("000001.SZ", current_tide="CAUTION|s=0.437|bias=0.9625")
    print(f"Has case file: {result.has_case_file}")
    print(f"Injection allowed: {result.injection_allowed}")
    print(f"Amnesia: {result.amnesia_triggered}")
    print(f"\nPrompt block:\n{result.prompt_block}")

    # Backward compat test
    print("\n--- Backward Compatibility ---")
    rag = RAGIntegration()
    summary = rag.get_intel_summary("000001.SZ")
    print(f"get_intel_summary: {summary[:200]}...")

    # Drift test
    print("\n--- Drift Monitor ---")
    pipeline.drift.record_outcome(["#BREAKOUT_TRAP"], False)
    pipeline.drift.record_outcome(["#BREAKOUT_TRAP"], False)
    pipeline.drift.record_outcome(["#BREAKOUT_TRAP"], False)
    alert = pipeline.drift.check_drift()
    print(f"After 3 failures: {alert}")
