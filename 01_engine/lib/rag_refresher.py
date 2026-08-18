#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
01_engine/lib/rag_refresher.py
RAG 19:00 Refresher - Daily Strategic Memory Builder

Flow:
    1. Extract today's audit_logs + echo_logs from DuckDB
    2. Call Fin-R1-7B for narrative synthesis with SSD tags
    3. Store facts to fact_strategic_memory (DuckDB)
    4. Embed narrative to ChromaDB (if available)

Degradation: RAM > 85% -> facts-only mode (no embedding)
"""

import hashlib
import json
import logging
import os
import time
import re
import random
import runpy
import sys
import psutil
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, field

logger = logging.getLogger('zhulong.rag_refresher')

_current = Path(__file__).resolve()
PROJECT_ROOT = next(
    (p for p in _current.parents if (p / ".git").exists() or (p / "storage").exists()),
    _current.parents[2]
)
DB_PATH = str(PROJECT_ROOT / "storage" / "database" / "zhulong.duckdb")
SSD_PATH = PROJECT_ROOT / "04_governance" / "config" / "semantic_dictionary.json"
CHROMA_DIR = str(PROJECT_ROOT / "storage" / "chromadb")
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))
from config.rag_contract import (
    COLLECTION_NAME,
    EMBEDDING_DIMENSION,
    EMBEDDING_MODEL,
    collection_creation_metadata,
    initialize_or_validate_writer_contract,
)

_loader_ns = runpy.run_path(str(PROJECT_ROOT / "04_governance" / "lib" / "core" / "module_loader.py"))
load_attr_from_path = _loader_ns["load_attr_from_path"]
load_module_from_path = _loader_ns["load_module_from_path"]

DBGateway = load_attr_from_path(
    "db_gateway_01_rag_refresher",
    PROJECT_ROOT / "01_engine" / "lib" / "db_gateway.py",
    "DBGateway",
)
ComputeGateway = load_attr_from_path(
    "compute_gateway_02_rag_refresher",
    PROJECT_ROOT / "02_brain" / "lib" / "compute_gateway.py",
    "ComputeGateway",
)
COMPUTE_GATEWAY = ComputeGateway(logger=logger, max_slots=3)
try:
    build_strategy_memory_events = load_attr_from_path(
        "strategy_memory_builder_05_rag_refresher",
        PROJECT_ROOT / "05_shadow" / "lib" / "strategy_memory_builder.py",
        "build_strategy_memory_events",
    )
    STRATEGY_MEMORY_AVAILABLE = True
except Exception as exc:
    logger.warning(f"[RAG] strategy memory builder unavailable: {exc}")
    build_strategy_memory_events = None
    STRATEGY_MEMORY_AVAILABLE = False


# Ollama endpoint (Node-102/116)
OLLAMA_BASE_URL = str(os.getenv("RAG_OLLAMA_BASE_URL", "http://192.0.2.20:11434")).rstrip("/")
OLLAMA_URL = f"{OLLAMA_BASE_URL}/api/generate"
EMBEDDING_URL = f"{OLLAMA_BASE_URL}/api/embeddings"
SYNTHESIS_MODEL = str(os.getenv("RAG_SYNTHESIS_MODEL", "fin-auditor:latest")).strip()
FALLBACK_MODEL = str(os.getenv("RAG_FALLBACK_MODEL", "qwen2.5:1.5b")).strip()
MAX_EMBED_BACKFILL = int(os.getenv("RAG_MAX_EMBED_BACKFILL", "200"))

RAG_ENRICH_ENABLED = int(os.getenv("RAG_ENRICH_ENABLED", "1")) == 1
RAG_ENRICH_BATCH_BUDGET_SEC = max(0, int(os.getenv("RAG_ENRICH_BATCH_BUDGET_SEC", "18000")))
RAG_ENRICH_RECORD_TIMEOUT_SEC = max(30, int(os.getenv("RAG_ENRICH_RECORD_TIMEOUT_SEC", "800")))
RAG_ENRICH_MAX_ATTEMPTS = max(1, int(os.getenv("RAG_ENRICH_MAX_ATTEMPTS", "3")))
RAG_BACKFILL_ENABLED = int(os.getenv("RAG_BACKFILL_ENABLED", "1")) == 1
RAG_BACKFILL_BATCH_BUDGET_SEC = max(0, int(os.getenv("RAG_BACKFILL_BATCH_BUDGET_SEC", "15000")))
RAG_BACKFILL_RECORD_TIMEOUT_SEC = max(30, int(os.getenv(
    "RAG_BACKFILL_RECORD_TIMEOUT_SEC", str(RAG_ENRICH_RECORD_TIMEOUT_SEC)
)))
RAG_BACKFILL_MAX_RECORDS = max(0, int(os.getenv("RAG_BACKFILL_MAX_RECORDS", "120")))
RAG_BACKFILL_MAX_ATTEMPTS = max(1, int(os.getenv("RAG_BACKFILL_MAX_ATTEMPTS", "5")))
RAG_BACKFILL_DEFERRED_MAX_ATTEMPTS = max(
    RAG_BACKFILL_MAX_ATTEMPTS,
    int(os.getenv("RAG_BACKFILL_DEFERRED_MAX_ATTEMPTS", "8")),
)
RAG_BACKFILL_DEFERRED_WEEKLY_LIMIT = max(0, int(os.getenv("RAG_BACKFILL_DEFERRED_WEEKLY_LIMIT", "12")))
RAG_BACKFILL_MAX_CONSECUTIVE_TIMEOUTS = max(1, int(os.getenv("RAG_BACKFILL_MAX_CONSECUTIVE_TIMEOUTS", "3")))
RAG_BACKFILL_SOFT_DEADLINE = str(os.getenv("RAG_BACKFILL_SOFT_DEADLINE", "05:40")).strip()
RAG_BACKFILL_PRIORITY_MODE = str(os.getenv("RAG_BACKFILL_PRIORITY_MODE", "value_first")).strip().lower()

SYNTH_TIMEOUT_RETRY_MAX = max(0, int(os.getenv("RAG_SYNTH_TIMEOUT_RETRY_MAX", "0")))
SYNTH_TIMEOUT_GROWTH = max(1.0, float(os.getenv("RAG_SYNTH_TIMEOUT_GROWTH", "1.5")))
SYNTH_TIMEOUT_CAP = max(120, int(os.getenv("RAG_SYNTH_TIMEOUT_CAP", "900")))
SYNTH_TIMEOUT_STREAK_LIMIT = max(1, int(os.getenv("RAG_SYNTH_TIMEOUT_STREAK_LIMIT", "3")))
SYNTH_PRIMARY_NUM_PREDICT = max(64, int(os.getenv("RAG_SYNTH_NUM_PREDICT_PRIMARY", "256")))
SYNTH_FALLBACK_NUM_PREDICT = max(32, int(os.getenv("RAG_SYNTH_NUM_PREDICT_FALLBACK", "256")))
RAG_FALLBACK_COOLDOWN_SEC = max(0, int(os.getenv("RAG_FALLBACK_COOLDOWN_SEC", "180")))
RAG_FALLBACK_TIMEOUT_SEC = max(30, int(os.getenv("RAG_FALLBACK_TIMEOUT_SEC", "240")))
RAG_FALLBACK_SKIP_IF_BUSY = int(os.getenv("RAG_FALLBACK_SKIP_IF_BUSY", "1")) == 1

# RAM threshold for degradation
RAM_THRESHOLD = 85.0

# L3 数据进入 RAG 的开关 (当前阶段关闭; 激活条件: 本地模型升级至 >=7B 且连续3天L3一致性>=85%)
RAG_INCLUDE_L3 = int(os.getenv("RAG_INCLUDE_L3", "0")) == 1

TEMPLATE_READY_STATUS = "TEMPLATE_READY"
INDEXABLE_ENRICHMENT_STATUSES = ("ENRICHED", TEMPLATE_READY_STATUS)
RAG_TEMPLATE_VERSION = str(os.getenv("RAG_TEMPLATE_VERSION", "audit_memory_template_v1")).strip()
RAG_DOCUMENT_VERSION = "rag_memory_document_v1"
RAG_LLM_POLISH_ENABLED = int(os.getenv("RAG_LLM_POLISH_ENABLED", "0")) == 1

# Structured output + timeout policy
MIN_STRUCTURED_SCHEMA_VERSION = (0, 3, 0)
_OLLAMA_VERSION_CACHE_TTL = 300
_OLLAMA_VERSION_CACHE = {
    "version": "0.0.0",
    "schema_supported": False,
    "checked_at": 0.0,
}

NARRATIVE_MIN_CHARS = max(20, int(os.getenv("RAG_NARRATIVE_MIN_CHARS", "40")))
_NARRATIVE_PLACEHOLDERS = {
    "详细描述",
    "详细审计记录",
    "详细审计报告",
    "英文记忆叙述",
    "中文详细记忆叙述",
    "详尽的英文记忆叙述",
    "详尽的中文记忆叙述",
}


def narrative_quality_issue(value: Any) -> str:
    """Return a stable rejection reason for narratives unsafe to index."""
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if not text:
        return "empty"
    compact = re.sub(r"[\W_]+", "", text, flags=re.UNICODE).lower()
    if not compact:
        return "punctuation_only"
    if compact in _NARRATIVE_PLACEHOLDERS:
        return "placeholder"
    if len(text) < NARRATIVE_MIN_CHARS:
        return "too_short"
    return ""


def _seconds_until_local_hhmm(hhmm: str) -> int:
    try:
        hour_s, minute_s = str(hhmm or "").split(":", 1)
        hour = int(hour_s)
        minute = int(minute_s)
    except Exception:
        return 0
    now = datetime.now()
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        return 0
    return int((target - now).total_seconds())


def _is_retryable_duckdb_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return ("locked" in msg) or ("busy" in msg) or ("conflict" in msg)


def with_duckdb_retry(fn, retries: int = 5, base_delay: float = 0.5):
    """
    DuckDB retry wrapper (aligned with decision_engine.py):
    retry on locked/busy/conflict with exponential backoff + jitter.
    """
    last_err = None
    for attempt in range(retries):
        try:
            return fn()
        except Exception as exc:
            if _is_retryable_duckdb_error(exc):
                last_err = exc
                delay = base_delay * (2 ** attempt) + random.uniform(0, 0.5)
                logger.warning(
                    f"[RAG] DuckDB locked (attempt {attempt + 1}/{retries}), wait {delay:.1f}s"
                )
                time.sleep(delay)
                continue
            raise
    if last_err is not None:
        raise last_err


def _clamp_json_value(value: Any, max_text: int = 600) -> Any:
    """Keep facts_json valid while bounding noisy raw payload fields."""
    if isinstance(value, str):
        return value if len(value) <= max_text else value[:max_text] + "...[truncated]"
    if isinstance(value, dict):
        return {str(k): _clamp_json_value(v, max_text=max_text) for k, v in value.items()}
    if isinstance(value, list):
        return [_clamp_json_value(v, max_text=max_text) for v in value[:50]]
    return value


def _dump_facts_json(record: Dict[str, Any], max_chars: int = 2000) -> str:
    payload = {
        k: _clamp_json_value(v)
        for k, v in record.items()
        if k not in ("l3_reasoning",)
    }
    facts = json.dumps(payload, ensure_ascii=False, default=str)
    if len(facts) <= max_chars:
        return facts
    payload = {
        k: _clamp_json_value(v, max_text=240)
        for k, v in record.items()
        if k not in ("l3_reasoning",)
    }
    payload["_truncated"] = True
    facts = json.dumps(payload, ensure_ascii=False, default=str)
    if len(facts) <= max_chars:
        return facts
    minimal = {
        "symbol": record.get("symbol", ""),
        "trade_date": record.get("trade_date", ""),
        "source": record.get("source", ""),
        "l4_verdict": record.get("l4_verdict", ""),
        "memory_quality": record.get("memory_quality", ""),
        "veto_reason": str(record.get("veto_reason", ""))[:500],
        "decision_tags": record.get("decision_tags", ""),
        "_truncated": True,
    }
    return json.dumps(minimal, ensure_ascii=False, default=str)


def _memory_score(record: Dict[str, Any]) -> int:
    source = str(record.get("source") or "")
    if source.startswith("strategy:") or source.startswith("execution:"):
        return int(record.get("return_score", 0) or 0)
    return int(record.get("l4_final_score", 0) or 0)


def _parse_synthesis_response(raw: str) -> Optional[Dict[str, Any]]:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        pass
    start = text.find("{")
    end = text.rfind("}")
    if 0 <= start < end:
        try:
            return json.loads(text[start:end + 1])
        except Exception:
            pass
    narrative_match = re.search(r'"narrative"\s*:\s*"([^"]{1,800})', text, re.S)
    tags_match = re.search(r'"tags"\s*:\s*\[([^\]]*)', text, re.S)
    if not narrative_match:
        return None
    tags = []
    if tags_match:
        tags = re.findall(r'"(#[A-Z0-9_]+)"', tags_match.group(1))
    return {
        "reasoning": "partial_json_recovered",
        "narrative": narrative_match.group(1),
        "tags": tags,
    }


def _parse_semver(version_text: str) -> Tuple[int, int, int]:
    if not version_text:
        return (0, 0, 0)
    match = re.search(r"(\d+)\.(\d+)\.(\d+)", str(version_text))
    if not match:
        return (0, 0, 0)
    return (int(match.group(1)), int(match.group(2)), int(match.group(3)))


def _timeout_floor_for_model(model_name: str) -> int:
    model = str(model_name or "").lower()
    if "1.5b" in model or "lfm-sentinel" in model or "lfm2.5" in model or "lfm2-" in model:
        return 120
    if "7b" in model or "fin-r1-7b" in model or "fin-auditor" in model:
        return 300
    return 0


def _apply_timeout_floor(model_name: str, requested_timeout: int) -> int:
    floor = _timeout_floor_for_model(model_name)
    return max(int(requested_timeout), floor)


def _ollama_server() -> str:
    return OLLAMA_URL.rsplit("/api/generate", 1)[0]


def _probe_ollama_version(timeout: int = 5) -> str:
    try:
        resp = COMPUTE_GATEWAY.http_get(
            f"{_ollama_server()}/api/version",
            timeout=timeout,
            layer="RAG",
            decision_id="probe_ollama_version",
        )
        if resp.status_code != 200:
            return "0.0.0"
        body = resp.json() if resp.content else {}
        return str(body.get("version") or "0.0.0")
    except Exception as exc:
        logger.error("Non-fatal: rag ollama version probe failed: %s", exc, exc_info=True)
        return "0.0.0"


def _ollama_active_models(timeout: int = 5) -> List[str]:
    try:
        resp = COMPUTE_GATEWAY.http_get(
            f"{_ollama_server()}/api/ps",
            timeout=timeout,
            layer="RAG",
            decision_id="probe_ollama_ps",
        )
        if resp.status_code != 200:
            logger.warning(f"[RAG] Ollama /api/ps status={resp.status_code}; fallback busy check ignored")
            return []
        body = resp.json() if resp.content else {}
        models = body.get("models") or []
        active = []
        for item in models:
            if isinstance(item, dict):
                name = str(item.get("name") or item.get("model") or "").strip()
                if name:
                    active.append(name)
        return active
    except Exception as exc:
        logger.warning(f"[RAG] Ollama busy probe failed: {exc}")
        return []


def _is_model_active(model_name: str) -> bool:
    target = str(model_name or "").strip().lower()
    if not target:
        return False
    for active in _ollama_active_models(timeout=5):
        active_l = active.lower()
        if active_l == target or active_l.startswith(target.split(":", 1)[0]):
            return True
    return False


def _resolve_structured_output_mode() -> Tuple[bool, str]:
    now = time.time()
    if (now - float(_OLLAMA_VERSION_CACHE.get("checked_at", 0.0))) < _OLLAMA_VERSION_CACHE_TTL:
        return bool(_OLLAMA_VERSION_CACHE.get("schema_supported")), str(_OLLAMA_VERSION_CACHE.get("version"))

    version = _probe_ollama_version(timeout=5)
    schema_supported = _parse_semver(version) >= MIN_STRUCTURED_SCHEMA_VERSION
    _OLLAMA_VERSION_CACHE.update(
        {
            "version": version,
            "schema_supported": schema_supported,
            "checked_at": now,
        }
    )
    if schema_supported:
        logger.info(f"[RAG] Ollama {version} supports JSON Schema format")
    else:
        logger.warning(f"[RAG] Ollama {version} < 0.3.0, fallback to format=json (upgrade recommended)")
    return schema_supported, version


def _resolve_ollama_format(schema: Dict[str, Any]) -> Tuple[Any, str]:
    schema_supported, version = _resolve_structured_output_mode()
    if schema_supported:
        return schema, f"SCHEMA(v{version})"
    return "json", f"JSON_FALLBACK(v{version})"


def _force_checkpoint(stage: str = "RAG") -> None:
    def _do_checkpoint():
        with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
            conn.execute("CHECKPOINT;")

    try:
        with_duckdb_retry(_do_checkpoint, retries=6, base_delay=0.5)
        logger.info(f"[{stage}] CHECKPOINT done")
    except Exception as e:
        logger.warning(f"[{stage}] CHECKPOINT failed: {e}")


def _build_rag_synthesis_schema() -> Dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "reasoning": {"type": "string", "minLength": 8},
            "narrative": {"type": "string", "minLength": 1},
            "tags": {"type": "array", "items": {"type": "string"}, "maxItems": 3},
        },
        "required": ["reasoning", "narrative", "tags"],
        "additionalProperties": False,
    }

# ==================== Data Structures ====================

@dataclass
class MemoryRecord:
    """Single strategic memory record"""
    symbol: str
    trade_date: str
    tide_status: str = ""
    facts_json: str = "{}"
    narrative_text: str = ""
    ssd_tags: str = ""
    source: str = ""         # "audit" | "echo" | "manual"
    l4_verdict: str = ""
    l4_score: int = 0
    embedding_status: str = "pending"  # pending | embedded | degraded


# ==================== SSD Loader ====================

class SSDLoader:
    """Load and validate SSD v1.0 tags"""
    def __init__(self):
        self.tags = {}
        self.prompt_rules = {}
        self._load()

    def _load(self):
        if SSD_PATH.exists():
            with open(SSD_PATH, 'r', encoding='utf-8') as f:
                data = json.load(f)
            self.tags = data.get("core_tags", {})
            self.prompt_rules = data.get("prompt_rules", {})
            logger.info(f"SSD v1.0 loaded: {len(self.tags)} tags")
        else:
            logger.warning(f"SSD not found at {SSD_PATH}")

    def get_tag_list(self) -> str:
        """Return formatted tag list for LLM prompt"""
        lines = []
        for key, info in self.tags.items():
            lines.append(f"  {info['label']}: {info['description']}")
        return "\n".join(lines)

    def validate_tags(self, tags: List[str]) -> List[str]:
        """Filter tags to only valid SSD tags"""
        valid_labels = {v["label"] for v in self.tags.values()}
        return [t for t in tags if t in valid_labels]


# ==================== Table Init ====================

def ensure_strategic_memory_table():
    """Create fact_strategic_memory table in DuckDB"""
    def _do_ensure():
        with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS fact_strategic_memory (
                    id              INTEGER,
                    symbol          VARCHAR NOT NULL,
                    trade_date      DATE NOT NULL,
                    tide_status     VARCHAR DEFAULT '',
                    facts_json      VARCHAR DEFAULT '{}',
                    narrative_text  VARCHAR DEFAULT '',
                    ssd_tags        VARCHAR DEFAULT '',
                    source          VARCHAR DEFAULT 'audit',
                    l4_verdict      VARCHAR DEFAULT '',
                    l4_score        INTEGER DEFAULT 0,
                    embedding_status VARCHAR DEFAULT 'pending',
                    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, trade_date, source)
                )
            """)
            conn.execute("ALTER TABLE fact_strategic_memory ADD COLUMN IF NOT EXISTS enrichment_status VARCHAR DEFAULT 'RAW_SYNC'")
            conn.execute("ALTER TABLE fact_strategic_memory ADD COLUMN IF NOT EXISTS enrichment_attempts INTEGER DEFAULT 0")
            conn.execute("ALTER TABLE fact_strategic_memory ADD COLUMN IF NOT EXISTS enrichment_error VARCHAR DEFAULT ''")
            conn.execute("ALTER TABLE fact_strategic_memory ADD COLUMN IF NOT EXISTS enrichment_model VARCHAR DEFAULT ''")
            conn.execute("ALTER TABLE fact_strategic_memory ADD COLUMN IF NOT EXISTS enriched_at TIMESTAMP")
            conn.execute("""
                UPDATE fact_strategic_memory
                SET enrichment_status = 'ENRICHED',
                    enrichment_model = 'legacy'
                WHERE source = 'audit'
                  AND COALESCE(enrichment_status, 'RAW_SYNC') = 'RAW_SYNC'
                  AND COALESCE(narrative_text, '') != ''
                  AND LENGTH(TRIM(narrative_text)) >= ?
                  AND narrative_text NOT LIKE '[RAW_SYNC%'
            """, [NARRATIVE_MIN_CHARS])
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_strmem_symbol
                ON fact_strategic_memory(symbol)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_strmem_date
                ON fact_strategic_memory(trade_date)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_strmem_tags
                ON fact_strategic_memory(ssd_tags)
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ops_rag_enrichment_runs (
                    run_id VARCHAR PRIMARY KEY,
                    mode VARCHAR DEFAULT '',
                    started_at TIMESTAMP,
                    ended_at TIMESTAMP,
                    raw_before INTEGER DEFAULT 0,
                    raw_after INTEGER DEFAULT 0,
                    deferred_before INTEGER DEFAULT 0,
                    deferred_after INTEGER DEFAULT 0,
                    daily_new_avg DOUBLE DEFAULT 0,
                    attempted INTEGER DEFAULT 0,
                    enriched INTEGER DEFAULT 0,
                    failed INTEGER DEFAULT 0,
                    deferred INTEGER DEFAULT 0,
                    skipped INTEGER DEFAULT 0,
                    embedded INTEGER DEFAULT 0,
                    circuit_breaker_reason VARCHAR DEFAULT '',
                    trend_status VARCHAR DEFAULT '',
                    notes VARCHAR DEFAULT ''
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_rag_enrich_runs_started
                ON ops_rag_enrichment_runs(started_at)
            """)

    with_duckdb_retry(_do_ensure, retries=8, base_delay=0.5)
    logger.info("fact_strategic_memory table ready")


# ==================== Refresher ====================

class RAGRefresher:
    """
    19:00 Daily Refresher
    Extract -> Synthesize -> Store -> (Embed)
    """

    def __init__(self):
        ensure_strategic_memory_table()
        self.ssd = SSDLoader()
        self._chroma_available = False
        self._chroma_collection = None
        self._init_chroma()
        self._synth_timeout_streak = 0
        self._synth_timeout_guard_open = False

    def _init_chroma(self):
        """Initialize ChromaDB with mxbai-embed-large (1024-dim) via Ollama"""
        try:
            import chromadb
            from chromadb import Documents, EmbeddingFunction, Embeddings
            Path(CHROMA_DIR).mkdir(parents=True, exist_ok=True)

            class OllamaMxbaiEmbedding(EmbeddingFunction):
                """Custom embedding via Node-102 mxbai-embed-large (1024-dim)"""
                def __call__(self, input: Documents) -> Embeddings:
                    embeddings = []
                    for text in input:
                        try:
                            embed_timeout = _apply_timeout_floor("mxbai-embed-large", 60)
                            with COMPUTE_GATEWAY.ollama_embeddings(
                                server=OLLAMA_BASE_URL,
                                payload={"model": EMBEDDING_MODEL, "prompt": text},
                                timeout=embed_timeout,
                                layer="RAG",
                                decision_id=f"rag-embed:{EMBEDDING_MODEL}",
                            ) as resp:
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
                        except Exception as e:
                            logger.warning(f"Embedding failed closed: {e}")
                            raise
                    return embeddings

            os.environ.setdefault("ANONYMIZED_TELEMETRY", "False")
            logging.getLogger("chromadb").setLevel(logging.CRITICAL)
            logging.getLogger("chromadb.telemetry").setLevel(logging.CRITICAL)
            logging.getLogger("chromadb.telemetry.product.posthog").setLevel(logging.CRITICAL)
            logging.getLogger("posthog").setLevel(logging.CRITICAL)
            from chromadb.config import Settings
            client = chromadb.PersistentClient(
                path=CHROMA_DIR,
                settings=Settings(anonymized_telemetry=False),
            )
            self._embed_fn = OllamaMxbaiEmbedding()
            self._chroma_collection = client.get_or_create_collection(
                name=COLLECTION_NAME,
                metadata=collection_creation_metadata(),
                embedding_function=self._embed_fn
            )
            initialize_or_validate_writer_contract(self._chroma_collection, logger)
            self._chroma_available = True
            logger.info(
                f"ChromaDB writer ready collection={COLLECTION_NAME} "
                f"model={EMBEDDING_MODEL} dim={EMBEDDING_DIMENSION} at {CHROMA_DIR}"
            )
        except ImportError:
            logger.warning("chromadb not installed, vector search disabled")
        except Exception as e:
            self._chroma_collection = None
            logger.warning(f"ChromaDB init failed: {e}")

    def _mark_embedding_status(self, symbol: str, trade_date: str, status: str, source: str = "") -> None:
        def _do_mark():
            with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
                conn.execute(
                    """
                    UPDATE fact_strategic_memory
                    SET embedding_status = ?
                    WHERE symbol = ?
                      AND trade_date = CAST(? AS DATE)
                      AND (? = '' OR source = ?)
                      AND UPPER(COALESCE(enrichment_status, '')) NOT LIKE 'QUARANTINED%'
                      AND LOWER(COALESCE(embedding_status, '')) <> 'quarantined'
                      AND LOWER(COALESCE(enrichment_error, '')) NOT LIKE 'narrative_quality_rejected:%'
                    """,
                    [status, symbol, trade_date, source, source],
                )

        with_duckdb_retry(_do_mark, retries=6, base_delay=0.5)

    def quarantine_low_quality_narratives(self) -> Dict[str, int]:
        """Fail closed on bad narratives while preserving their factual record."""
        def _do_fetch():
            with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
                return conn.execute(
                    """
                    SELECT symbol, CAST(trade_date AS VARCHAR), source, narrative_text
                    FROM fact_strategic_memory
                    WHERE UPPER(COALESCE(enrichment_status, 'RAW_SYNC')) = 'ENRICHED'
                    """
                ).fetchall()

        invalid = []
        for symbol, trade_date, source, narrative in with_duckdb_retry(_do_fetch):
            issue = narrative_quality_issue(narrative)
            if issue:
                invalid.append((str(symbol), str(trade_date), str(source or ""), issue))
        if not invalid:
            return {"quarantined": 0, "vectors_removed": 0}

        def _do_quarantine():
            with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
                conn.executemany(
                    """
                    UPDATE fact_strategic_memory
                    SET enrichment_status = 'ENRICH_DEFERRED',
                        enrichment_error = ?,
                        embedding_status = 'quarantined'
                    WHERE symbol = ?
                      AND trade_date = CAST(? AS DATE)
                      AND source = ?
                      AND UPPER(COALESCE(enrichment_status, 'RAW_SYNC')) = 'ENRICHED'
                    """,
                    [
                        [f"narrative_quality_rejected:{issue}", symbol, trade_date, source]
                        for symbol, trade_date, source, issue in invalid
                    ],
                )

        with_duckdb_retry(_do_quarantine, retries=6, base_delay=0.5)
        removed = 0
        if self._chroma_available and self._chroma_collection:
            for symbol, trade_date, source, _issue in invalid:
                try:
                    self._chroma_collection.delete(where={
                        "$and": [
                            {"symbol": symbol},
                            {"trade_date": trade_date},
                            {"source": source},
                        ]
                    })
                    removed += 1
                except Exception as exc:
                    logger.warning(
                        f"[RAG] quarantine vector cleanup failed "
                        f"symbol={symbol} date={trade_date} source={source}: {exc}"
                    )
        logger.warning(
            f"[RAG] quarantined low-quality narratives={len(invalid)} "
            f"vector_groups_removed={removed}"
        )
        return {"quarantined": len(invalid), "vectors_removed": removed}

    def _check_ram(self) -> bool:
        """Return True if RAM usage is below threshold"""
        usage = psutil.virtual_memory().percent
        if usage > RAM_THRESHOLD:
            logger.warning(f"RAM usage {usage:.1f}% > {RAM_THRESHOLD}%, entering degraded mode")
            return False
        return True

    def _get_tide_status(self, trade_date: str) -> str:
        """Get tide status for context"""
        try:
            tide_mod = load_module_from_path(
                "tide_sensor_rag_refresher",
                PROJECT_ROOT / "04_governance" / "lib" / "tide_sensor.py",
            )
            state = tide_mod.get_sensor().get_risk_gate(trade_date)
            return f"{state.risk_gate}|s={state.ma20_ratio:.3f}|bias={state.style_bias:.4f}"
        except Exception as exc:
            logger.error("Non-fatal: tide status probe failed: %s", exc, exc_info=True)
            return "UNKNOWN"

    # ==================== Extract ====================

    @staticmethod
    def _classify_memory_quality(rag_intel: Any, veto_reason: Any = "") -> str:
        text = f"{rag_intel or ''} {veto_reason or ''}".upper()
        if not str(rag_intel or "").strip() or "RAG_EMPTY" in text or "STRATEGIC_AMNESIA" in text:
            return "LOW_MEMORY_COVERAGE"
        if "信息真空" in text or "零信息" in text or "无历史" in text or "无任何可验证" in text:
            return "LOW_MEMORY_COVERAGE"
        return "MEMORY_AVAILABLE"

    @staticmethod
    def _clean_audit_veto_reason(veto_reason: Any, notary_payload: Any = "") -> str:
        """Keep RAG memory narratives semantic, not JSON-tail artifacts."""
        reason = str(veto_reason or "").strip()
        payload = str(notary_payload or "").strip()
        looks_fragment = reason.startswith('":"') or reason.startswith('":') or reason.endswith('"}')
        if looks_fragment and payload:
            try:
                obj = json.loads(payload)
                dominant = str(obj.get("dominant_logic") or "").strip()
                if dominant:
                    reason = dominant
            except Exception:
                pass
        return reason[:500]

    def extract_daily_logs(self, trade_date: str = None) -> List[Dict[str, Any]]:
        """Extract today's audit + echo logs from DuckDB"""
        def _do_extract() -> Tuple[List[Dict[str, Any]], str]:
            local_trade_date = trade_date
            if not local_trade_date:
                with DBGateway(DB_PATH, read_only=True, logger=logger) as date_conn:
                    td = date_conn.execute("SELECT MAX(trade_date) FROM fact_daily").fetchone()[0]
                if td is None:
                    return [], ""
                local_trade_date = td.strftime("%Y-%m-%d") if hasattr(td, "strftime") else str(td)

            if STRATEGY_MEMORY_AVAILABLE and callable(build_strategy_memory_events):
                try:
                    build_stats = build_strategy_memory_events(local_trade_date)
                    logger.info(f"[RAG] strategy memory build: {build_stats}")
                except Exception as exc:
                    logger.warning(f"[RAG] strategy memory build skipped: {exc}", exc_info=True)

            with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
                records: List[Dict[str, Any]] = []

                # 1. Audit logs (from nexus_audits)
                try:
                    audits = conn.execute(
                        """
                        SELECT symbol, trade_date,
                               l4_final_verdict, l4_veto_reason,
                               l1_close, l1_pct_chg, l1_turnover,
                               rag_intel, l4_notary_verdict, l4_notary_payload,
                               task_id, l2_error_code,
                               COALESCE(l4_final_score, final_score, 0) AS l4_final_score,
                               COALESCE(l2_fact_tags, '') AS decision_tags
                        FROM (
                            SELECT *, ROW_NUMBER() OVER (
                                PARTITION BY symbol, trade_date
                                ORDER BY completed_at DESC NULLS LAST, created_at DESC NULLS LAST, task_id DESC
                            ) AS rn
                            FROM nexus_audits
                            WHERE CAST(trade_date AS DATE) = CAST(? AS DATE)
                        ) latest
                        WHERE rn = 1
                          AND COALESCE(l4_final_verdict, '') != ''
                          AND COALESCE(l2_error_code, '') NOT IN ('PARSE_ERROR', 'RUNTIME_EXCEPTION')
                        """,
                        [local_trade_date],
                    ).fetchall()

                    for row in audits:
                        clean_reason = self._clean_audit_veto_reason(row[3], row[9])
                        memory_quality = self._classify_memory_quality(row[7], clean_reason)
                        records.append({
                            "symbol": row[0],
                            "trade_date": str(row[1]),
                            "source": "audit",
                            "l4_verdict": row[2] or "",
                            "veto_reason": clean_reason,
                            "memory_quality": memory_quality,
                            "close": row[4] or 0,
                            "pct_chg": row[5] or 0,
                            "turnover": row[6] or 0,
                            "rag_intel": row[7] or "",
                            "notary_verdict": row[8] or "",
                            "notary_payload": row[9] or "",
                            "task_id": row[10] or "",
                            "l2_error_code": row[11] or "",
                            "l4_final_score": int(row[12] or 0),
                            "decision_tags": row[13] or "",
                        })
                except Exception as e:
                    if _is_retryable_duckdb_error(e):
                        raise
                    logger.warning(f"Audit log extraction failed: {e}")

                # 2. Strategy memory events (verified paper-trading outcomes)
                try:
                    strategy_events = conn.execute(
                        """
                        SELECT
                            event_id,
                            symbol,
                            CAST(event_trade_date AS VARCHAR) AS event_trade_date,
                            CAST(signal_trade_date AS VARCHAR) AS signal_trade_date,
                            source_event,
                            action,
                            horizon_days,
                            outcome_label,
                            entry_price,
                            eval_price,
                            return_pct,
                            max_return_pct,
                            max_drawdown_pct,
                            decision_quality,
                            evidence_json,
                            narrative_text,
                            COALESCE((
                                SELECT a.l2_fact_tags
                                FROM nexus_audits a
                                WHERE a.task_id = json_extract_string(
                                    TRY_CAST(COALESCE(e.evidence_json, '{}') AS JSON),
                                    '$.signal_task_id'
                                )
                                  AND COALESCE(a.l2_fact_tags, '') != ''
                                ORDER BY TRY_CAST(a.completed_at AS TIMESTAMP) DESC NULLS LAST,
                                         TRY_CAST(a.created_at AS TIMESTAMP) DESC NULLS LAST,
                                         a.task_id DESC
                                LIMIT 1
                            ), (
                                SELECT a.l2_fact_tags
                                FROM nexus_audits a
                                WHERE COALESCE(
                                        json_extract_string(
                                            TRY_CAST(COALESCE(e.evidence_json, '{}') AS JSON),
                                            '$.signal_task_id'
                                        ),
                                        ''
                                      ) = ''
                                  AND a.symbol = e.symbol
                                  AND CAST(a.trade_date AS DATE) = CAST(e.signal_trade_date AS DATE)
                                  AND COALESCE(a.l2_fact_tags, '') != ''
                                ORDER BY TRY_CAST(a.completed_at AS TIMESTAMP) DESC NULLS LAST,
                                         TRY_CAST(a.created_at AS TIMESTAMP) DESC NULLS LAST,
                                         a.task_id DESC
                                LIMIT 1
                            ), '') AS decision_tags
                        FROM fact_strategy_memory_events e
                        WHERE CAST(e.event_trade_date AS DATE) = CAST(? AS DATE)
                          AND COALESCE(e.memory_status, 'READY') = 'READY'
                          AND COALESCE(e.narrative_text, '') != ''
                        ORDER BY e.symbol, e.horizon_days, e.action
                        """,
                        [local_trade_date],
                    ).fetchall()

                    for row in strategy_events:
                        evidence = {}
                        try:
                            evidence = json.loads(row[14] or '{}')
                        except Exception:
                            evidence = {}
                        horizon = int(row[6] or 0)
                        records.append({
                            "symbol": row[1],
                            "trade_date": str(row[2]),
                            "source": f"strategy:{row[4] or row[5] or 'event'}:T{horizon}",
                            "event_id": row[0] or "",
                            "signal_trade_date": str(row[3] or ""),
                            "source_event": row[4] or "",
                            "action": row[5] or "",
                            "horizon_days": horizon,
                            "outcome_label": row[7] or "",
                            "entry_price": row[8] or 0,
                            "eval_price": row[9] or 0,
                            "return_pct": row[10] or 0,
                            "max_return_pct": row[11] or 0,
                            "max_drawdown_pct": row[12] or 0,
                            "decision_quality": row[13] or "",
                            "strategy_evidence": evidence,
                            "strategy_narrative": row[15] or "",
                            "decision_tags": row[16] or "",
                            "l4_verdict": row[7] or "",
                            "return_score": int(round(float(row[10] or 0) * 100)),
                            "memory_quality": "VERIFIED_STRATEGY_OUTCOME",
                        })
                except Exception as e:
                    if _is_retryable_duckdb_error(e):
                        raise
                    logger.warning(f"Strategy memory extraction failed: {e}")

                # 3. T+1 paper-fill execution facts
                try:
                    has_fill_events = conn.execute(
                        """
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_name = 'fact_shadow_fill_events'
                        LIMIT 1
                        """
                    ).fetchone()
                    fill_events = []
                    if has_fill_events:
                        fill_events = conn.execute(
                            """
                        SELECT
                            fill_id,
                            signal_id,
                            task_id,
                            run_id,
                            symbol,
                            name,
                            CAST(signal_trade_date AS VARCHAR) AS signal_trade_date,
                            CAST(fill_date AS VARCHAR) AS fill_date,
                            action,
                            status,
                            reason,
                            base_price,
                            fill_price,
                            qty,
                            allocated_cash,
                            gross_amount,
                            slippage_cost,
                            pricing_mode,
                            data_quality,
                            evidence_json
                        FROM fact_shadow_fill_events
                        WHERE CAST(fill_date AS DATE) = CAST(? AS DATE)
                          AND COALESCE(status, '') IN ('FILLED', 'UNFILLED', 'SKIPPED')
                        ORDER BY symbol, fill_id
                        """,
                            [local_trade_date],
                        ).fetchall()

                    for row in fill_events:
                        evidence = {}
                        try:
                            evidence = json.loads(row[19] or '{}')
                        except Exception:
                            evidence = {}
                        status = str(row[9] or "").strip().upper()
                        reason = str(row[10] or "").strip()
                        symbol = str(row[4] or "").strip().upper()
                        qty = int(row[13] or 0)
                        fill_price = float(row[12] or 0)
                        gross_amount = float(row[15] or 0)
                        narrative = (
                            f"{symbol} T+1 paper execution: status={status}, "
                            f"reason={reason}, qty={qty}, fill_price={fill_price:.4f}, "
                            f"amount={gross_amount:.2f}, mode={row[17] or ''}."
                        )
                        records.append({
                            "symbol": symbol,
                            "trade_date": str(row[7]),
                            "source": f"execution:t1_fill:{status.lower()}",
                            "fill_id": row[0] or "",
                            "signal_id": row[1] or "",
                            "task_id": row[2] or "",
                            "run_id": row[3] or "",
                            "name": row[5] or "",
                            "signal_trade_date": str(row[6] or ""),
                            "action": row[8] or "",
                            "execution_status": status,
                            "l4_verdict": status,
                            "reason": reason,
                            "base_price": row[11] or 0,
                            "fill_price": fill_price,
                            "qty": qty,
                            "allocated_cash": row[14] or 0,
                            "gross_amount": gross_amount,
                            "slippage_cost": row[16] or 0,
                            "pricing_mode": row[17] or "",
                            "data_quality": row[18] or "",
                            "execution_evidence": evidence,
                            "strategy_narrative": narrative,
                            "memory_quality": "EXECUTION_FACT",
                        })
                except Exception as e:
                    if _is_retryable_duckdb_error(e):
                        raise
                    logger.warning(f"Shadow fill-event extraction failed: {e}")

                # 4. Echo logs
                try:
                    echos = conn.execute(
                        """
                        SELECT symbol, trade_date, current_state, veto_reason,
                               price_at_entry, vol_ma5_at_entry, original_score
                        FROM fact_echo_logs
                        WHERE trade_date = CAST(? AS DATE)
                        """,
                        [local_trade_date],
                    ).fetchall()

                    for row in echos:
                        records.append({
                            "symbol": row[0],
                            "trade_date": str(row[1]),
                            "source": "echo",
                            "echo_state": row[2] or "",
                            "veto_reason": row[3] or "",
                            "price": row[4] or 0,
                            "vol_ma5": row[5] or 0,
                            "score": row[6] or 0
                        })
                except Exception as e:
                    if _is_retryable_duckdb_error(e):
                        raise
                    logger.warning(f"Echo log extraction failed: {e}")

                return records, local_trade_date

        records, resolved_trade_date = with_duckdb_retry(_do_extract, retries=6, base_delay=0.5)
        self._last_resolved_trade_date = str(resolved_trade_date or trade_date or "")
        logger.info(f"Extracted {len(records)} records for {resolved_trade_date}")
        return records

    def _reset_synthesis_guard(self):
        self._synth_timeout_streak = 0
        self._synth_timeout_guard_open = False

    def _note_synthesis_success(self):
        if self._synth_timeout_streak > 0:
            logger.info(f"[RAG] synthesis timeout streak reset: {self._synth_timeout_streak} -> 0")
        self._synth_timeout_streak = 0

    def _note_synthesis_timeout(self, model: str, timeout_s: int, detail: str):
        self._synth_timeout_streak += 1
        logger.warning(
            f"[RAG] synthesis timeout streak={self._synth_timeout_streak}/{SYNTH_TIMEOUT_STREAK_LIMIT} "
            f"model={model} timeout={timeout_s}s detail={detail}"
        )
        if self._synth_timeout_streak >= SYNTH_TIMEOUT_STREAK_LIMIT:
            if not self._synth_timeout_guard_open:
                logger.warning(
                    "[RAG] synthesis timeout guard OPEN: switch to RAW_SYNC_ONLY mode for current refresh"
                )
            self._synth_timeout_guard_open = True

    @staticmethod
    def _is_retryable_http(status_code: int) -> bool:
        return int(status_code) in {408, 409, 425, 429, 500, 502, 503, 504}

    @staticmethod
    def _build_raw_sync_payload(record: Dict[str, Any], reason: str) -> Dict[str, str]:
        raw_reasoning = str(record.get("veto_reason") or "").strip()
        if not raw_reasoning:
            raw_reasoning = (
                f"{record.get('symbol', '?')} {record.get('l4_verdict', '?')} "
                f"l4_score={record.get('l4_final_score', 0)}"
            )
        return {
            "narrative": f"[RAW_SYNC:{reason}] {raw_reasoning[:500]}",
            "tags": "",
            "enriched": False,
            "error": reason,
        }

    def _run_synthesis_model(
        self,
        *,
        record: Dict[str, Any],
        base_payload: Dict[str, Any],
        model: str,
        timeout_base: int,
        attempts: int,
        decision_prefix: str,
        layer: str = "RAG",
    ) -> Tuple[int, str, int, bool]:
        last_status = 0
        last_timeout = int(timeout_base)
        attempts = max(1, int(attempts))
        growth = float(SYNTH_TIMEOUT_GROWTH)
        for attempt in range(attempts):
            step_timeout = min(SYNTH_TIMEOUT_CAP, max(60, int(timeout_base * (growth ** attempt))))
            last_timeout = step_timeout
            try:
                with COMPUTE_GATEWAY.ollama_generate(
                    server=OLLAMA_BASE_URL,
                    payload={**base_payload, "model": model},
                    timeout=step_timeout,
                    layer=layer,
                    decision_id=f"{decision_prefix}:{record.get('symbol', 'NA')}:{attempt}",
                ) as resp:
                    last_status = int(resp.status_code or 0)
                    if last_status == 200:
                        return last_status, str(resp.json().get("response", "")), step_timeout, False
                    if self._is_retryable_http(last_status) and (attempt + 1) < attempts:
                        wait_s = min(4, 2 ** attempt)
                        logger.warning(
                            f"[RAG] synthesis http={last_status}, retry in {wait_s}s "
                            f"({attempt + 1}/{attempts - 1}) model={model}"
                        )
                        time.sleep(wait_s)
                        continue
                    return last_status, "", step_timeout, False
            except Exception as exc:
                if COMPUTE_GATEWAY.is_timeout_error(exc):
                    self._note_synthesis_timeout(model=model, timeout_s=step_timeout, detail=str(exc)[:200])
                    if self._synth_timeout_guard_open:
                        return 0, "", step_timeout, True
                    if (attempt + 1) < attempts:
                        wait_s = min(4, 2 ** attempt)
                        logger.warning(
                            f"[RAG] synthesis timeout retry in {wait_s}s "
                            f"({attempt + 1}/{attempts - 1}) model={model}"
                        )
                        time.sleep(wait_s)
                        continue
                    return 0, "", step_timeout, False

                logger.warning(f"Synthesis call failed ({model}) attempt={attempt + 1}: {exc}")
                if (attempt + 1) < attempts:
                    wait_s = min(3, 1 + attempt)
                    time.sleep(wait_s)
                    continue
                return 0, "", step_timeout, False

        return last_status, "", last_timeout, self._synth_timeout_guard_open

    # ==================== Synthesize ====================

    def raw_sync_synthesis(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Deterministic template used before any optional model enrichment."""
        source = str(record.get("source", ""))
        if source.startswith("strategy:"):
            return self._strategy_event_synthesis(record)
        if source.startswith("execution:"):
            return self._execution_event_synthesis(record)
        if source == "echo":
            return self._rule_based_tags(record)

        tags = self._rule_based_tags(record).get("tags", "")
        reason = str(record.get("veto_reason") or "").strip()
        verdict = str(record.get("l4_verdict") or "").strip()
        score = int(record.get("l4_final_score") or 0)
        quality = str(record.get("memory_quality") or "").strip()
        if reason:
            detail = reason[:320]
        else:
            detail = (
                f"close={float(record.get('close') or 0):.4f}, "
                f"pct_chg={float(record.get('pct_chg') or 0):.2f}%, "
                f"turnover={float(record.get('turnover') or 0):.2f}"
            )
        return {
            "narrative": (
                f"[RAW_SYNC] {record.get('symbol', '?')} L4={verdict or '?'} "
                f"score={score} quality={quality or 'UNKNOWN'}; {detail}"
            )[:1000],
            "tags": tags,
            "enriched": False,
        }

    def synthesize_narrative(self, record: Dict[str, Any]) -> Dict[str, str]:
        """
        Optional enrichment via Fin-R1-7B/Qwen-7B.
        Returns {"narrative": str, "tags": str}
        """
        source = str(record.get("source", ""))
        if source.startswith("strategy:"):
            return self._strategy_event_synthesis(record)
        if source.startswith("execution:"):
            return self._execution_event_synthesis(record)

        if self._synth_timeout_guard_open:
            return self._build_raw_sync_payload(record, reason="timeout_guard_open")

        tag_list = self.ssd.get_tag_list()
        prompt = f"""You are a financial audit memory summarizer.
Generate a concise narrative (<=300 Chinese chars) and select 1-3 best SSD tags.
RAG_EMPTY / STRATEGIC_AMNESIA / LOW_MEMORY_COVERAGE means low memory coverage only; do not convert it into a negative trading label.
If memory_quality=LOW_MEMORY_COVERAGE, summarize it as "待验证/低记忆覆盖" rather than a standalone bearish cause.

Record:
  symbol: {record.get('symbol', 'N/A')}
  trade_date: {record.get('trade_date', 'N/A')}
  source: {record.get('source', 'N/A')}
  l4_verdict: {record.get('l4_verdict', 'N/A')}
  veto_reason: {record.get('veto_reason', 'N/A')}
  rag_intel: {record.get('rag_intel') or 'RAG_EMPTY: memory coverage is unavailable; do not treat this as negative evidence.'}
  memory_quality: {record.get('memory_quality', 'UNKNOWN')}
  notary_verdict: {record.get('notary_verdict', 'N/A')}
  l4_final_score: {record.get('l4_final_score', 'N/A')}
  close: {record.get('close', 'N/A')}
  pct_chg: {record.get('pct_chg', 'N/A')}%

SSD Dictionary:
{tag_list}

Output strict JSON only (field order fixed):
{{"reasoning":"brief rationale","narrative":"audit narrative","tags":["#TAG1","#TAG2"]}}"""

        try:
            schema = _build_rag_synthesis_schema()
            response_format, format_mode = _resolve_ollama_format(schema)
            base_payload = {
                "prompt": prompt,
                "stream": False,
                "format": response_format,
                "options": {
                    "temperature": 0.0,
                    "num_ctx": 4096,
                    "num_predict": SYNTH_PRIMARY_NUM_PREDICT,
                },
            }

            attempts = max(1, SYNTH_TIMEOUT_RETRY_MAX + 1)
            primary_timeout = RAG_ENRICH_RECORD_TIMEOUT_SEC
            used_model = SYNTHESIS_MODEL
            status_code, raw, used_timeout, guard_trip = self._run_synthesis_model(
                record=record,
                base_payload=base_payload,
                model=SYNTHESIS_MODEL,
                timeout_base=primary_timeout,
                attempts=attempts,
                decision_prefix="rag-synthesis:primary",
            )

            if guard_trip:
                return self._build_raw_sync_payload(record, reason="primary_timeout_guard")

            if status_code != 200:
                if RAG_FALLBACK_COOLDOWN_SEC > 0:
                    logger.warning(
                        f"[RAG] primary synthesis failed status={status_code}; "
                        f"fallback cooldown {RAG_FALLBACK_COOLDOWN_SEC}s"
                    )
                    time.sleep(RAG_FALLBACK_COOLDOWN_SEC)
                if RAG_FALLBACK_SKIP_IF_BUSY and _is_model_active(SYNTHESIS_MODEL):
                    logger.warning(
                        f"[RAG] fallback skipped: primary model still active after cooldown "
                        f"model={SYNTHESIS_MODEL}"
                    )
                    return self._build_raw_sync_payload(record, reason="fallback_skipped_ollama_busy")

                fallback_timeout = RAG_FALLBACK_TIMEOUT_SEC
                used_model = FALLBACK_MODEL
                fallback_payload = dict(base_payload)
                fallback_options = dict(base_payload.get("options") or {})
                fallback_options["num_predict"] = SYNTH_FALLBACK_NUM_PREDICT
                fallback_payload["options"] = fallback_options
                status_code, raw, used_timeout, guard_trip = self._run_synthesis_model(
                    record=record,
                    base_payload=fallback_payload,
                    model=FALLBACK_MODEL,
                    timeout_base=fallback_timeout,
                    attempts=max(1, min(2, attempts)),
                    decision_prefix="rag-synthesis:fallback",
                )
                if guard_trip:
                    return self._build_raw_sync_payload(record, reason="fallback_timeout_guard")

            if status_code == 200:
                parsed = _parse_synthesis_response(raw)
                if not parsed:
                    raise ValueError("synthesis_json_parse_failed")
                tag_values = parsed.get("tags", [])
                if not isinstance(tag_values, list):
                    tag_values = []
                tags = self.ssd.validate_tags([str(t).strip() for t in tag_values if str(t).strip()])
                narrative = str(parsed.get("narrative", "")).strip()
                if narrative:
                    quality_issue = narrative_quality_issue(narrative)
                    if quality_issue:
                        raise ValueError(f"synthesis_narrative_low_quality:{quality_issue}")
                    self._note_synthesis_success()
                    logger.info(
                        f"[RAG] synthesis model={used_model} mode={format_mode} timeout={used_timeout}s"
                    )
                    return {
                        "narrative": narrative[:500],
                        "tags": ",".join(tags) if tags else "",
                        "enriched": True,
                        "model": used_model,
                    }
                logger.warning(f"Synthesis empty narrative ({used_model})")
            else:
                logger.warning(f"Synthesis HTTP {status_code} ({used_model})")
        except Exception as e:
            logger.warning(f"Synthesis failed: {e}")

        if self._synth_timeout_guard_open:
            return self._build_raw_sync_payload(record, reason="timeout_guard_open")

        # Degraded: rule-based tagging
        degraded = self._rule_based_tags(record)
        degraded["enriched"] = False
        degraded["error"] = "synthesis_failed"
        return degraded

    def _strategy_event_synthesis(self, record: Dict[str, Any]) -> Dict[str, str]:
        """Build post-outcome memory descriptors without using them as predictors.

        Predictive performance uses facts_json.decision_tags captured before outcome.
        """
        tags = []
        quality = str(record.get("decision_quality") or "").upper()
        outcome = str(record.get("outcome_label") or "").upper()
        action = str(record.get("action") or "").upper()
        if "BUY_VALIDATED" in quality or outcome == "WIN":
            tags.append("#MOMENTUM_CONFIRM")
        if "BUY_WEAK" in quality or outcome == "LOSS":
            tags.append("#BREAKOUT_TRAP")
        if "SELL_VALIDATED" in quality:
            tags.append("#RISK_CONTROL_VALID")
        if "SELL_TOO_EARLY" in quality:
            tags.append("#STOP_TOO_EARLY")
        if "RAISE_STOP" in action:
            tags.append("#TREND_FOLLOWING")
        narrative = str(record.get("strategy_narrative") or "").strip()
        if not narrative:
            narrative = (
                f"{record.get('symbol', '?')} strategy event {action} outcome={outcome} "
                f"return={float(record.get('return_pct') or 0):.2%}"
            )
        return {"narrative": narrative[:1000], "tags": ",".join(dict.fromkeys(tags[:3])), "enriched": False}

    def _execution_event_synthesis(self, record: Dict[str, Any]) -> Dict[str, str]:
        """T+1 fill events are execution facts; keep them factual and cheap."""
        tags = []
        status = str(record.get("execution_status") or "").upper()
        reason = str(record.get("reason") or "").upper()
        if status == "UNFILLED":
            tags.append("#LIQUIDITY_TRAP")
        if "LIMIT" in reason:
            tags.append("#LIQUIDITY_TRAP")
        if "WAIT" in reason or "DATA" in reason:
            tags.append("#HIGH_FRICTION_REJECT")
        narrative = str(record.get("strategy_narrative") or "").strip()
        if not narrative:
            narrative = (
                f"{record.get('symbol', '?')} T+1 execution status={status}, "
                f"reason={record.get('reason', '')}, qty={int(record.get('qty') or 0)}, "
                f"fill_price={float(record.get('fill_price') or 0):.4f}."
            )
        return {"narrative": narrative[:1000], "tags": ",".join(dict.fromkeys(tags[:3])), "enriched": False}

    def _rule_based_tags(self, record: Dict[str, Any]) -> Dict[str, str]:
        """Fallback rule-based tagging when LLM unavailable"""
        tags = []
        reason = str(record.get("veto_reason", "") or "").upper()

        if "OVERBOUGHT" in reason or "过热" in reason:
            tags.append("#OVERBOUGHT_REJECT")
        if "BREAKOUT" in reason or "假突破" in reason:
            tags.append("#BREAKOUT_TRAP")
        if "VOLUME" in reason or "放量" in reason or "量能" in reason:
            tags.append("#VOLUME_FRAUD")
        if "MOMENTUM" in reason or "动量" in reason:
            tags.append("#MOMENTUM_EXHAUSTION")
        if "FRICTION" in reason or "摩擦" in reason:
            tags.append("#HIGH_FRICTION_REJECT")
        if "TIDE" in reason or "潮汐" in reason:
            tags.append("#TIDE_REJECTION")

        narrative = f"{record.get('symbol','?')} {record.get('l4_verdict','?')} (l4_score={record.get('l4_final_score',0)})"
        return {"narrative": narrative, "tags": ",".join(tags[:3]), "enriched": False}

    # ==================== Store ====================

    def store_memory(
        self,
        record: Dict[str, Any],
        synthesis: Dict[str, Any],
        tide_status: str,
        enrichment_status: str = "RAW_SYNC",
        enrichment_error: str = "",
        enrichment_model: str = "",
    ) -> bool:
        """Store to fact_strategic_memory"""
        facts_record = dict(record)
        if enrichment_status == TEMPLATE_READY_STATUS:
            facts_record["memory_template_version"] = RAG_TEMPLATE_VERSION
            facts_record["memory_template_text"] = str(synthesis.get("narrative") or "")[:700]
            if not enrichment_model:
                enrichment_model = RAG_TEMPLATE_VERSION
        facts = _dump_facts_json(facts_record)

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        enriched_at = (
            now
            if enrichment_status in {"ENRICHED", TEMPLATE_READY_STATUS}
            else None
        )
        existing_attempts = (
            self._current_enrichment_attempts(record)
            if enrichment_status == "RAW_SYNC"
            else 0
        )

        try:
            def _do_store():
                with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
                    existing = conn.execute(
                        """
                        SELECT enrichment_status, embedding_status, enrichment_error
                        FROM fact_strategic_memory
                        WHERE symbol = ?
                          AND trade_date = CAST(? AS DATE)
                          AND source = ?
                        """,
                        [
                            record.get("symbol", ""),
                            record.get("trade_date", ""),
                            record.get("source", "audit"),
                        ],
                    ).fetchone()
                    if (
                        existing
                        and (
                            str(existing[0] or "").upper().startswith("QUARANTINED")
                            or str(existing[1] or "").lower() == "quarantined"
                            or str(existing[2] or "").lower().startswith(
                                "narrative_quality_rejected:"
                            )
                        )
                    ):
                        logger.warning(
                            "[RAG] quarantined memory is immutable "
                            f"symbol={record.get('symbol', '')} "
                            f"date={record.get('trade_date', '')} "
                            f"source={record.get('source', 'audit')}"
                        )
                        return False
                    conn.execute("""
                        INSERT INTO fact_strategic_memory
                            (symbol, trade_date, tide_status, facts_json,
                             narrative_text, ssd_tags, source, l4_verdict,
                             l4_score, embedding_status, created_at,
                             enrichment_status, enrichment_attempts, enrichment_error,
                             enrichment_model, enriched_at)
                        VALUES (?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS TIMESTAMP),
                                ?, ?, ?, ?, CAST(? AS TIMESTAMP))
                        ON CONFLICT (symbol, trade_date, source) DO UPDATE SET
                            tide_status = excluded.tide_status,
                            facts_json = CASE
                                WHEN (
                                    UPPER(COALESCE(fact_strategic_memory.enrichment_status, '')) = 'ENRICHED'
                                    AND UPPER(COALESCE(excluded.enrichment_status, '')) IN ('RAW_SYNC', 'TEMPLATE_READY')
                                  ) OR (
                                    UPPER(COALESCE(fact_strategic_memory.enrichment_status, '')) = 'TEMPLATE_READY'
                                    AND UPPER(COALESCE(excluded.enrichment_status, '')) = 'RAW_SYNC'
                                  )
                                THEN fact_strategic_memory.facts_json
                                ELSE excluded.facts_json
                            END,
                            l4_verdict = excluded.l4_verdict,
                            l4_score = excluded.l4_score,
                            narrative_text = CASE
                                WHEN (
                                    UPPER(COALESCE(fact_strategic_memory.enrichment_status, '')) = 'ENRICHED'
                                    AND UPPER(COALESCE(excluded.enrichment_status, '')) IN ('RAW_SYNC', 'TEMPLATE_READY')
                                  ) OR (
                                    UPPER(COALESCE(fact_strategic_memory.enrichment_status, '')) = 'TEMPLATE_READY'
                                    AND UPPER(COALESCE(excluded.enrichment_status, '')) = 'RAW_SYNC'
                                  )
                                THEN fact_strategic_memory.narrative_text
                                ELSE excluded.narrative_text
                            END,
                            ssd_tags = CASE
                                WHEN (
                                    UPPER(COALESCE(fact_strategic_memory.enrichment_status, '')) = 'ENRICHED'
                                    AND UPPER(COALESCE(excluded.enrichment_status, '')) IN ('RAW_SYNC', 'TEMPLATE_READY')
                                  ) OR (
                                    UPPER(COALESCE(fact_strategic_memory.enrichment_status, '')) = 'TEMPLATE_READY'
                                    AND UPPER(COALESCE(excluded.enrichment_status, '')) = 'RAW_SYNC'
                                  )
                                THEN fact_strategic_memory.ssd_tags
                                ELSE excluded.ssd_tags
                            END,
                            embedding_status = CASE
                                WHEN (
                                    UPPER(COALESCE(fact_strategic_memory.enrichment_status, '')) = 'ENRICHED'
                                    AND UPPER(COALESCE(excluded.enrichment_status, '')) IN ('RAW_SYNC', 'TEMPLATE_READY')
                                  ) OR (
                                    UPPER(COALESCE(fact_strategic_memory.enrichment_status, '')) = 'TEMPLATE_READY'
                                    AND UPPER(COALESCE(excluded.enrichment_status, '')) = 'RAW_SYNC'
                                  )
                                THEN fact_strategic_memory.embedding_status
                                ELSE excluded.embedding_status
                            END,
                            created_at = fact_strategic_memory.created_at,
                            enrichment_status = CASE
                                WHEN (
                                    UPPER(COALESCE(fact_strategic_memory.enrichment_status, '')) = 'ENRICHED'
                                    AND UPPER(COALESCE(excluded.enrichment_status, '')) IN ('RAW_SYNC', 'TEMPLATE_READY')
                                  ) OR (
                                    UPPER(COALESCE(fact_strategic_memory.enrichment_status, '')) = 'TEMPLATE_READY'
                                    AND UPPER(COALESCE(excluded.enrichment_status, '')) = 'RAW_SYNC'
                                  )
                                THEN fact_strategic_memory.enrichment_status
                                ELSE excluded.enrichment_status
                            END,
                            enrichment_attempts = CASE
                                WHEN excluded.enrichment_status IN ('RAW_SYNC', 'TEMPLATE_READY')
                                THEN GREATEST(
                                    COALESCE(fact_strategic_memory.enrichment_attempts, 0),
                                    COALESCE(excluded.enrichment_attempts, 0)
                                )
                                ELSE COALESCE(excluded.enrichment_attempts, 0)
                            END,
                            enrichment_error = CASE
                                WHEN (
                                    UPPER(COALESCE(fact_strategic_memory.enrichment_status, '')) = 'ENRICHED'
                                    AND UPPER(COALESCE(excluded.enrichment_status, '')) IN ('RAW_SYNC', 'TEMPLATE_READY')
                                  ) OR (
                                    UPPER(COALESCE(fact_strategic_memory.enrichment_status, '')) = 'TEMPLATE_READY'
                                    AND UPPER(COALESCE(excluded.enrichment_status, '')) = 'RAW_SYNC'
                                  )
                                THEN fact_strategic_memory.enrichment_error
                                ELSE excluded.enrichment_error
                            END,
                            enrichment_model = CASE
                                WHEN (
                                    UPPER(COALESCE(fact_strategic_memory.enrichment_status, '')) = 'ENRICHED'
                                    AND UPPER(COALESCE(excluded.enrichment_status, '')) IN ('RAW_SYNC', 'TEMPLATE_READY')
                                  ) OR (
                                    UPPER(COALESCE(fact_strategic_memory.enrichment_status, '')) = 'TEMPLATE_READY'
                                    AND UPPER(COALESCE(excluded.enrichment_status, '')) = 'RAW_SYNC'
                                  )
                                THEN fact_strategic_memory.enrichment_model
                                ELSE excluded.enrichment_model
                            END,
                            enriched_at = CASE
                                WHEN (
                                    UPPER(COALESCE(fact_strategic_memory.enrichment_status, '')) = 'ENRICHED'
                                    AND UPPER(COALESCE(excluded.enrichment_status, '')) IN ('RAW_SYNC', 'TEMPLATE_READY')
                                  ) OR (
                                    UPPER(COALESCE(fact_strategic_memory.enrichment_status, '')) = 'TEMPLATE_READY'
                                    AND UPPER(COALESCE(excluded.enrichment_status, '')) = 'RAW_SYNC'
                                  )
                                THEN fact_strategic_memory.enriched_at
                                ELSE excluded.enriched_at
                            END
                    """, [
                        record.get("symbol", ""),
                        record.get("trade_date", ""),
                        tide_status,
                        facts[:2000],
                        synthesis.get("narrative", "")[:1000],
                        synthesis.get("tags", ""),
                        record.get("source", "audit"),
                        record.get("l4_verdict", ""),
                        _memory_score(record),
                        "pending",
                        now,
                        enrichment_status,
                        existing_attempts,
                        str(enrichment_error or "")[:500],
                        str(enrichment_model or "")[:80],
                        enriched_at,
                    ])
                    return True

            return bool(with_duckdb_retry(_do_store, retries=6, base_delay=0.5))
        except Exception as e:
            logger.error(f"Store failed: {e}")
            return False

    def _eligible_for_enrichment(self, record: Dict[str, Any]) -> bool:
        if not (RAG_ENRICH_ENABLED and RAG_LLM_POLISH_ENABLED):
            return False
        if str(record.get("source") or "") != "audit":
            return False
        if self._synth_timeout_guard_open:
            return False
        return True

    def _current_enrichment_attempts(self, record: Dict[str, Any]) -> int:
        def _do_read():
            with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
                row = conn.execute(
                    """
                    SELECT COALESCE(enrichment_attempts, 0)
                    FROM fact_strategic_memory
                    WHERE symbol = ?
                      AND trade_date = CAST(? AS DATE)
                      AND source = ?
                    """,
                    [
                        record.get("symbol", ""),
                        record.get("trade_date", ""),
                        record.get("source", "audit"),
                    ],
                ).fetchone()
                return int(row[0] or 0) if row else 0

        try:
            return int(with_duckdb_retry(_do_read, retries=4, base_delay=0.3) or 0)
        except Exception as exc:
            logger.warning(f"[RAG] enrichment attempt read failed: {exc}")
            return RAG_ENRICH_MAX_ATTEMPTS

    def update_enriched_memory(self, record: Dict[str, Any], synthesis: Dict[str, Any]) -> bool:
        quality_issue = narrative_quality_issue(synthesis.get("narrative"))
        if quality_issue:
            logger.warning(f"[RAG] enrichment update rejected: {quality_issue}")
            return False
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        def _do_update():
            with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
                row = conn.execute(
                    """
                    UPDATE fact_strategic_memory
                    SET narrative_text = ?,
                        ssd_tags = ?,
                        enrichment_status = 'ENRICHED',
                        enrichment_attempts = COALESCE(enrichment_attempts, 0) + 1,
                        enrichment_error = '',
                        enrichment_model = ?,
                        enriched_at = CAST(? AS TIMESTAMP),
                        embedding_status = 'pending',
                        created_at = created_at
                    WHERE symbol = ?
                      AND trade_date = CAST(? AS DATE)
                      AND source = ?
                      AND UPPER(COALESCE(enrichment_status, '')) NOT LIKE 'QUARANTINED%'
                      AND LOWER(COALESCE(embedding_status, '')) <> 'quarantined'
                      AND LOWER(COALESCE(enrichment_error, '')) NOT LIKE 'narrative_quality_rejected:%'
                    RETURNING enrichment_status
                    """,
                    [
                        str(synthesis.get("narrative") or "")[:1000],
                        str(synthesis.get("tags") or ""),
                        str(synthesis.get("model") or "")[:80],
                        now,
                        record.get("symbol", ""),
                        record.get("trade_date", ""),
                        record.get("source", "audit"),
                    ],
                ).fetchone()
                return bool(row)

        try:
            return bool(with_duckdb_retry(_do_update, retries=6, base_delay=0.5))
        except Exception as exc:
            logger.warning(f"[RAG] enrichment update failed: {exc}")
            return False

    def mark_enrichment_failed(self, record: Dict[str, Any], error: str, model: str = "") -> None:
        def _do_mark():
            with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
                conn.execute(
                    """
                    UPDATE fact_strategic_memory
                    SET enrichment_status = CASE
                            WHEN UPPER(COALESCE(enrichment_status, '')) = 'TEMPLATE_READY' THEN 'TEMPLATE_READY'
                            WHEN COALESCE(enrichment_attempts, 0) + 1 >= ? THEN 'ENRICH_FAILED'
                            ELSE 'RAW_SYNC'
                        END,
                        enrichment_attempts = COALESCE(enrichment_attempts, 0) + 1,
                        enrichment_error = ?,
                        enrichment_model = ?
                    WHERE symbol = ?
                      AND trade_date = CAST(? AS DATE)
                      AND source = ?
                      AND UPPER(COALESCE(enrichment_status, '')) NOT LIKE 'QUARANTINED%'
                      AND LOWER(COALESCE(embedding_status, '')) <> 'quarantined'
                      AND LOWER(COALESCE(enrichment_error, '')) NOT LIKE 'narrative_quality_rejected:%'
                    """,
                    [
                        RAG_ENRICH_MAX_ATTEMPTS,
                        str(error or "")[:500],
                        str(model or "")[:80],
                        record.get("symbol", ""),
                        record.get("trade_date", ""),
                        record.get("source", "audit"),
                    ],
                )

        try:
            with_duckdb_retry(_do_mark, retries=6, base_delay=0.5)
        except Exception as exc:
            logger.warning(f"[RAG] enrichment failure mark failed: {exc}")

    # ==================== Enrichment Backfill ====================

    @staticmethod
    def _json_dict(value: Any) -> Dict[str, Any]:
        if isinstance(value, dict):
            return value
        if not value:
            return {}
        try:
            parsed = json.loads(str(value))
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    @staticmethod
    def _brief_value(value: Any, max_chars: int = 260) -> str:
        if value is None:
            return ""
        if isinstance(value, float):
            return f"{value:.6g}"
        if isinstance(value, (dict, list)):
            text = json.dumps(_clamp_json_value(value, max_text=180), ensure_ascii=False, default=str)
        else:
            text = str(value)
        text = re.sub(r"\s+", " ", text).strip()
        return text[:max_chars]

    def _backfill_counts(self) -> Dict[str, int]:
        def _do_count():
            with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
                row = conn.execute(
                    """
                    SELECT
                        SUM(CASE WHEN UPPER(COALESCE(enrichment_status, 'RAW_SYNC')) = 'RAW_SYNC' THEN 1 ELSE 0 END),
                        SUM(CASE WHEN UPPER(COALESCE(enrichment_status, '')) = 'ENRICH_DEFERRED' THEN 1 ELSE 0 END),
                        COUNT(*)
                    FROM fact_strategic_memory
                    WHERE UPPER(COALESCE(enrichment_status, 'RAW_SYNC')) IN ('RAW_SYNC', 'ENRICHED', 'ENRICH_DEFERRED')
                      AND LOWER(COALESCE(embedding_status, '')) <> 'quarantined'
                      AND LOWER(COALESCE(enrichment_error, '')) NOT LIKE 'narrative_quality_rejected:%'
                    """
                ).fetchone()
                return {
                    "raw_sync": int(row[0] or 0) if row else 0,
                    "deferred": int(row[1] or 0) if row else 0,
                    "active_total": int(row[2] or 0) if row else 0,
                }

        return with_duckdb_retry(_do_count, retries=4, base_delay=0.3)

    def _recent_daily_new_avg(self, days: int = 5) -> float:
        def _do_avg():
            with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
                row = conn.execute(
                    f"""
                    WITH daily AS (
                        SELECT CAST(created_at AS DATE) AS d, COUNT(*) AS active_new
                        FROM fact_strategic_memory
                        WHERE UPPER(COALESCE(enrichment_status, 'RAW_SYNC')) IN ('RAW_SYNC', 'ENRICHED', 'ENRICH_DEFERRED')
                          AND LOWER(COALESCE(embedding_status, '')) <> 'quarantined'
                          AND LOWER(COALESCE(enrichment_error, '')) NOT LIKE 'narrative_quality_rejected:%'
                        GROUP BY 1
                        HAVING COUNT(*) > 0
                        ORDER BY d DESC
                        LIMIT {max(1, int(days))}
                    )
                    SELECT COALESCE(AVG(active_new), 0) FROM daily
                    """
                ).fetchone()
                return float(row[0] or 0) if row else 0.0

        try:
            return float(with_duckdb_retry(_do_avg, retries=4, base_delay=0.3) or 0.0)
        except Exception as exc:
            logger.warning(f"[RAG-BACKFILL] daily-new baseline failed: {exc}")
            return 0.0

    def _fetch_backfill_candidates(
        self,
        limit: int,
        *,
        include_deferred: bool = False,
        deferred_only: bool = False,
    ) -> List[Dict[str, Any]]:
        if limit <= 0:
            return []
        include_deferred_i = 1 if include_deferred else 0
        deferred_only_i = 1 if deferred_only else 0

        def _do_fetch():
            with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
                rows = conn.execute(
                    """
                    SELECT symbol,
                           CAST(trade_date AS VARCHAR) AS trade_date,
                           COALESCE(tide_status, '') AS tide_status,
                           source,
                           l4_verdict,
                           l4_score,
                           facts_json,
                           narrative_text,
                           ssd_tags,
                           COALESCE(enrichment_attempts, 0) AS attempts,
                           COALESCE(enrichment_error, '') AS error_text,
                           COALESCE(enrichment_status, 'RAW_SYNC') AS status,
                           CAST(created_at AS VARCHAR) AS created_at,
                           CASE
                               WHEN source = 'execution:t1_fill:filled' THEN 10
                               WHEN source LIKE 'execution:%' THEN 20
                               WHEN source LIKE 'strategy:intraday_%' THEN 30
                               WHEN source LIKE 'strategy:%shadow_sell_outcome%' THEN 40
                               WHEN source LIKE 'strategy:L4_PASS%' THEN 50
                               WHEN source LIKE 'strategy:%shadow_position_review%' THEN 60
                               WHEN source = 'audit' AND UPPER(COALESCE(l4_verdict, '')) IN ('PASS', 'VETO') THEN 70
                               WHEN source = 'audit' THEN 80
                               ELSE 90
                           END AS priority
                    FROM fact_strategic_memory
                    WHERE (
                        (? = 0 AND UPPER(COALESCE(enrichment_status, 'RAW_SYNC')) = 'RAW_SYNC'
                         AND COALESCE(enrichment_attempts, 0) < ?)
                        OR
                        (? = 0 AND ? = 1 AND UPPER(COALESCE(enrichment_status, '')) = 'ENRICH_DEFERRED'
                         AND COALESCE(enrichment_attempts, 0) < ?)
                        OR
                        (? = 1 AND UPPER(COALESCE(enrichment_status, '')) = 'ENRICH_DEFERRED'
                         AND COALESCE(enrichment_attempts, 0) < ?)
                    )
                      AND LOWER(COALESCE(embedding_status, '')) <> 'quarantined'
                      AND LOWER(COALESCE(enrichment_error, '')) NOT LIKE 'narrative_quality_rejected:%'
                    ORDER BY
                        CASE WHEN ? = 'value_first' THEN priority ELSE 50 END ASC,
                        created_at DESC,
                        symbol ASC
                    LIMIT ?
                    """,
                    [
                        deferred_only_i,
                        RAG_BACKFILL_MAX_ATTEMPTS,
                        deferred_only_i,
                        include_deferred_i,
                        RAG_BACKFILL_DEFERRED_MAX_ATTEMPTS,
                        deferred_only_i,
                        RAG_BACKFILL_DEFERRED_MAX_ATTEMPTS,
                        RAG_BACKFILL_PRIORITY_MODE,
                        int(limit),
                    ],
                ).fetchall()
                records = []
                for row in rows:
                    facts = self._json_dict(row[6])
                    rec = dict(facts)
                    rec.update({
                        "symbol": str(row[0] or facts.get("symbol") or ""),
                        "trade_date": str(row[1] or facts.get("trade_date") or ""),
                        "_tide_status": str(row[2] or facts.get("tide_status") or ""),
                        "source": str(row[3] or facts.get("source") or "audit"),
                        "l4_verdict": str(row[4] or facts.get("l4_verdict") or ""),
                        "l4_score": int(row[5] or facts.get("l4_score") or facts.get("l4_final_score") or 0),
                        "_existing_narrative": str(row[7] or ""),
                        "_existing_tags": str(row[8] or ""),
                        "_enrichment_attempts": int(row[9] or 0),
                        "_enrichment_error": str(row[10] or ""),
                        "_enrichment_status": str(row[11] or "RAW_SYNC"),
                        "_created_at": str(row[12] or ""),
                        "_priority": int(row[13] or 90),
                    })
                    records.append(rec)
                return records

        return with_duckdb_retry(_do_fetch, retries=4, base_delay=0.3)

    def _build_backfill_context(self, record: Dict[str, Any]) -> str:
        source = str(record.get("source") or "")
        lines = [
            f"symbol: {record.get('symbol', '')}",
            f"trade_date: {record.get('trade_date', '')}",
            f"source: {source}",
            f"verdict_or_status: {record.get('l4_verdict') or record.get('outcome_label') or record.get('execution_status') or ''}",
            f"score: {record.get('l4_score') or record.get('l4_final_score') or record.get('return_score') or 0}",
            f"tide_status: {record.get('_tide_status') or record.get('tide_status') or ''}",
            f"memory_quality: {record.get('memory_quality', '')}",
        ]

        def add(label: str, value: Any, max_chars: int = 260) -> None:
            text = self._brief_value(value, max_chars=max_chars)
            if text:
                lines.append(f"{label}: {text}")

        if source.startswith("execution:"):
            evidence = self._json_dict(record.get("execution_evidence"))
            add("name", record.get("name"))
            add("signal_trade_date", record.get("signal_trade_date"))
            add("action", record.get("action"))
            add("execution_status", record.get("execution_status"))
            add("reason", record.get("reason"))
            add("qty", record.get("qty"))
            add("base_price", record.get("base_price"))
            add("fill_price", record.get("fill_price"))
            add("gross_amount", record.get("gross_amount"))
            add("slippage_cost", record.get("slippage_cost"))
            add("pricing_mode", record.get("pricing_mode"))
            add("data_quality", record.get("data_quality"))
            add("entry_tide_gate", evidence.get("entry_tide_gate"))
            add("entry_tide_ratio", evidence.get("entry_tide_ratio"))
            add("minute_rows", evidence.get("minute_rows"))
            add("impact_pct", evidence.get("impact_pct"))
            add("participation", evidence.get("participation"))
            add("execution_notes", evidence.get("notes"), max_chars=360)
            add("raw_narrative", record.get("strategy_narrative") or record.get("_existing_narrative"), max_chars=420)
        elif source.startswith("strategy:"):
            evidence = self._json_dict(record.get("strategy_evidence"))
            add("signal_trade_date", record.get("signal_trade_date"))
            add("decision_tags", record.get("decision_tags"))
            add("source_event", record.get("source_event"))
            add("action", record.get("action"))
            add("outcome_label", record.get("outcome_label"))
            add("horizon_days", record.get("horizon_days"))
            add("return_pct", record.get("return_pct"))
            add("max_return_pct", record.get("max_return_pct"))
            add("max_drawdown_pct", record.get("max_drawdown_pct"))
            add("decision_quality", record.get("decision_quality"))
            add("entry_price", record.get("entry_price"))
            add("eval_price", record.get("eval_price"))
            add("rule_reason", evidence.get("reason"))
            add("sell_rule_id", evidence.get("sell_rule_id"))
            add("strength_tier", evidence.get("strength_tier"))
            add("qty_sold", evidence.get("qty_sold"))
            add("qty_after", evidence.get("qty_after"))
            add("days_held", evidence.get("days_held"))
            add("entry_score", evidence.get("entry_score"))
            add("realized_pnl", evidence.get("realized_pnl"))
            add("raw_narrative", record.get("strategy_narrative") or record.get("_existing_narrative"), max_chars=520)
        else:
            add("l4_verdict", record.get("l4_verdict"))
            add("l4_final_score", record.get("l4_final_score") or record.get("l4_score"))
            add("veto_reason", record.get("veto_reason"), max_chars=520)
            add("close", record.get("close"))
            add("pct_chg", record.get("pct_chg"))
            add("turnover", record.get("turnover"))
            add("notary_verdict", record.get("notary_verdict"))
            add("rag_intel", record.get("rag_intel"), max_chars=620)
            add("raw_narrative", record.get("_existing_narrative"), max_chars=420)

        return "\n".join(lines)[:2600]

    @staticmethod
    def _score_band(score: Any) -> str:
        try:
            value = int(float(score or 0))
        except Exception:
            value = 0
        if value >= 75:
            return "高分区间(>=75)"
        if value >= 65:
            return "中高分区间(65-74)"
        if value >= 50:
            return "观察区间(50-64)"
        return "低分区间(<50)"

    @staticmethod
    def _pct_band(value: Any) -> str:
        try:
            pct = float(value or 0)
        except Exception:
            pct = 0.0
        if pct >= 8:
            return "大涨"
        if pct >= 3:
            return "上涨"
        if pct <= -5:
            return "大跌"
        if pct <= -2:
            return "下跌"
        return "震荡"

    @staticmethod
    def _turnover_band(value: Any) -> str:
        try:
            turnover = float(value or 0)
        except Exception:
            turnover = 0.0
        if turnover >= 20:
            return "极高换手"
        if turnover >= 10:
            return "高换手"
        if turnover >= 3:
            return "中等换手"
        return "低换手"

    @staticmethod
    def _split_tag_text(value: Any) -> List[str]:
        tokens = []
        for token in re.split(r"[,，;；\s]+", str(value or "")):
            token = token.strip()
            if token.startswith("#") and len(token) > 1:
                tokens.append(token)
        return tokens

    def _audit_template_synthesis(self, record: Dict[str, Any]) -> Dict[str, Any]:
        symbol = str(record.get("symbol") or "").strip()
        trade_date = str(record.get("trade_date") or "").strip()
        verdict = str(record.get("l4_verdict") or "UNKNOWN").strip().upper()
        score = int(record.get("l4_final_score") or record.get("l4_score") or 0)
        pct_chg = float(record.get("pct_chg") or 0)
        turnover = float(record.get("turnover") or 0)
        close = float(record.get("close") or 0)
        memory_quality = str(record.get("memory_quality") or "UNKNOWN").strip()
        notary = str(record.get("notary_verdict") or "").strip()
        tide = str(record.get("_tide_status") or record.get("tide_status") or "UNKNOWN").strip()
        decision_tags = self._split_tag_text(record.get("decision_tags"))
        rule_tags = self._split_tag_text(self._rule_based_tags(record).get("tags"))
        tags = list(dict.fromkeys(rule_tags[:3]))
        fact_tags_text = "、".join(decision_tags[:8]) if decision_tags else "未记录"
        veto_reason = self._brief_value(record.get("veto_reason"), max_chars=260)
        if not veto_reason:
            veto_reason = "未记录明确否决理由"

        verdict_text = {
            "PASS": "裁决通过，属于可跟踪的正向审计样本",
            "HOLD": "裁决观察，属于边缘或待确认样本",
            "VETO": "裁决否决，属于风险或无优势样本",
        }.get(verdict, "裁决未知，作为低置信审计样本保存")
        movement_text = self._pct_band(pct_chg)
        turnover_text = self._turnover_band(turnover)
        narrative = (
            f"审计记忆 {RAG_TEMPLATE_VERSION}。{trade_date} 标的{symbol}：{verdict_text}；"
            f"L4分数{score}，处于{self._score_band(score)}；"
            f"当日价格表现为{movement_text}，涨跌幅{pct_chg:.2f}%，换手率{turnover:.2f}%({turnover_text})，收盘价{close:.4f}；"
            f"L2事实标签包括{fact_tags_text}；记忆覆盖状态为{memory_quality}；"
            f"Notary结论为{notary or '未记录'}；主要风险或裁决依据：{veto_reason}；"
            f"审计时市场潮汐为{tide}。该模板仅记录审计时点已知字段。"
        )
        return {
            "narrative": narrative[:1000],
            "tags": ",".join(tags),
            "enriched": True,
            "template_ready": True,
            "model": RAG_TEMPLATE_VERSION,
            "error": "",
        }

    def _deterministic_backfill_synthesis(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        source = str(record.get("source") or "")
        if source.startswith("strategy:"):
            synthesis = dict(self._strategy_event_synthesis(record))
        elif source.startswith("execution:"):
            synthesis = dict(self._execution_event_synthesis(record))
        elif source == "audit":
            synthesis = self._audit_template_synthesis(record)
        else:
            return None

        existing_narrative = str(record.get("_existing_narrative") or "").strip()
        existing_tags = str(record.get("_existing_tags") or "").strip()
        if not str(synthesis.get("narrative") or "").strip() and existing_narrative:
            synthesis["narrative"] = existing_narrative[:1000]
        if not str(synthesis.get("tags") or "").strip() and existing_tags:
            synthesis["tags"] = existing_tags
        synthesis["enriched"] = True
        synthesis["template_ready"] = True
        synthesis["model"] = RAG_TEMPLATE_VERSION
        synthesis["error"] = ""
        quality_issue = narrative_quality_issue(synthesis.get("narrative"))
        if quality_issue:
            logger.warning(f"[RAG-BACKFILL] deterministic template rejected: {quality_issue}")
            return None
        return synthesis

    def update_template_memory(self, record: Dict[str, Any], synthesis: Dict[str, Any]) -> bool:
        quality_issue = narrative_quality_issue(synthesis.get("narrative"))
        if quality_issue:
            logger.warning(f"[RAG] template update rejected: {quality_issue}")
            return False
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        def _do_update():
            with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
                row = conn.execute(
                    """
                    UPDATE fact_strategic_memory
                    SET narrative_text = ?,
                        ssd_tags = ?,
                        enrichment_status = 'TEMPLATE_READY',
                        enrichment_error = '',
                        enrichment_model = ?,
                        enriched_at = CAST(? AS TIMESTAMP),
                        embedding_status = 'pending',
                        created_at = created_at
                    WHERE symbol = ?
                      AND trade_date = CAST(? AS DATE)
                      AND source = ?
                      AND UPPER(COALESCE(enrichment_status, 'RAW_SYNC')) IN ('RAW_SYNC', 'ENRICH_DEFERRED')
                      AND LOWER(COALESCE(embedding_status, '')) <> 'quarantined'
                      AND LOWER(COALESCE(enrichment_error, '')) NOT LIKE 'narrative_quality_rejected:%'
                    RETURNING enrichment_status
                    """,
                    [
                        str(synthesis.get("narrative") or "")[:1000],
                        str(synthesis.get("tags") or ""),
                        str(synthesis.get("model") or RAG_TEMPLATE_VERSION)[:80],
                        now,
                        record.get("symbol", ""),
                        record.get("trade_date", ""),
                        record.get("source", "audit"),
                    ],
                ).fetchone()
                return bool(row)

        try:
            return bool(with_duckdb_retry(_do_update, retries=6, base_delay=0.5))
        except Exception as exc:
            logger.warning(f"[RAG] template update failed: {exc}")
            return False

    def synthesize_backfill_narrative(self, record: Dict[str, Any]) -> Dict[str, Any]:
        deterministic = self._deterministic_backfill_synthesis(record)
        if deterministic is not None:
            return deterministic

        if self._synth_timeout_guard_open:
            return self._build_raw_sync_payload(record, reason="timeout_guard_open")

        tag_list = self.ssd.get_tag_list()
        context = self._build_backfill_context(record)
        prompt = f"""You are Zhulong's trade-memory enrichment worker.
Facts are authoritative. Do not invent prices, dates, actions, or outcomes.
Summarize the durable lesson for future retrieval in concise Chinese (<=300 chars).
For execution facts: focus on fill quality, liquidity/friction, data quality, and realism.
For strategy/outcome facts: focus on rule, outcome, return/drawdown, and whether the decision was validated.
For audit facts: focus on the L4 decision evidence and risk/edge structure.
Select 1-3 SSD tags only when supported by facts.

Record facts:
{context}

SSD Dictionary:
{tag_list}

Output strict JSON only (field order fixed):
{{"reasoning":"brief rationale","narrative":"Chinese memory narrative","tags":["#TAG1","#TAG2"]}}"""

        used_model = SYNTHESIS_MODEL
        try:
            schema = _build_rag_synthesis_schema()
            response_format, format_mode = _resolve_ollama_format(schema)
            base_payload = {
                "prompt": prompt,
                "stream": False,
                "format": response_format,
                "options": {
                    "temperature": 0.0,
                    "num_ctx": 4096,
                    "num_predict": SYNTH_PRIMARY_NUM_PREDICT,
                },
            }
            status_code, raw, used_timeout, guard_trip = self._run_synthesis_model(
                record=record,
                base_payload=base_payload,
                model=SYNTHESIS_MODEL,
                timeout_base=RAG_BACKFILL_RECORD_TIMEOUT_SEC,
                attempts=max(1, SYNTH_TIMEOUT_RETRY_MAX + 1),
                decision_prefix="rag-backfill:primary",
            )
            if guard_trip:
                return self._build_raw_sync_payload(record, reason="primary_timeout_guard")

            if status_code != 200:
                if RAG_FALLBACK_COOLDOWN_SEC > 0:
                    logger.warning(
                        f"[RAG-BACKFILL] primary failed status={status_code}; "
                        f"fallback cooldown {RAG_FALLBACK_COOLDOWN_SEC}s"
                    )
                    time.sleep(RAG_FALLBACK_COOLDOWN_SEC)
                if RAG_FALLBACK_SKIP_IF_BUSY and _is_model_active(SYNTHESIS_MODEL):
                    return self._build_raw_sync_payload(record, reason="fallback_skipped_ollama_busy")
                used_model = FALLBACK_MODEL
                fallback_payload = dict(base_payload)
                fallback_options = dict(base_payload.get("options") or {})
                fallback_options["num_predict"] = SYNTH_FALLBACK_NUM_PREDICT
                fallback_payload["options"] = fallback_options
                status_code, raw, used_timeout, guard_trip = self._run_synthesis_model(
                    record=record,
                    base_payload=fallback_payload,
                    model=FALLBACK_MODEL,
                    timeout_base=RAG_FALLBACK_TIMEOUT_SEC,
                    attempts=1,
                    decision_prefix="rag-backfill:fallback",
                )
                if guard_trip:
                    return self._build_raw_sync_payload(record, reason="fallback_timeout_guard")

            if status_code == 200:
                parsed = _parse_synthesis_response(raw)
                if not parsed:
                    return {"enriched": False, "error": "synthesis_json_parse_failed", "model": used_model}
                tag_values = parsed.get("tags", [])
                if not isinstance(tag_values, list):
                    tag_values = []
                tags = self.ssd.validate_tags([str(t).strip() for t in tag_values if str(t).strip()])
                narrative = str(parsed.get("narrative", "")).strip()
                if narrative:
                    quality_issue = narrative_quality_issue(narrative)
                    if quality_issue:
                        return {
                            "enriched": False,
                            "error": f"synthesis_narrative_low_quality:{quality_issue}",
                            "model": used_model,
                        }
                    self._note_synthesis_success()
                    logger.info(
                        f"[RAG-BACKFILL] synthesis model={used_model} mode={format_mode} timeout={used_timeout}s"
                    )
                    return {
                        "narrative": narrative[:500],
                        "tags": ",".join(tags) if tags else "",
                        "enriched": True,
                        "model": used_model,
                    }
                return {"enriched": False, "error": "empty_narrative", "model": used_model}
            return {"enriched": False, "error": f"http_{status_code}", "model": used_model}
        except Exception as exc:
            logger.warning(f"[RAG-BACKFILL] synthesis failed: {exc}")
            return {"enriched": False, "error": str(exc)[:160] or "synthesis_failed", "model": used_model}

    def mark_backfill_failed(self, record: Dict[str, Any], error: str, model: str = "") -> str:
        current_status = str(record.get("_enrichment_status") or "RAW_SYNC").upper()
        current_attempts = int(record.get("_enrichment_attempts") or 0)
        next_status = current_status
        if current_status == "RAW_SYNC" and current_attempts + 1 >= RAG_BACKFILL_MAX_ATTEMPTS:
            next_status = "ENRICH_DEFERRED"
        elif current_status not in {"RAW_SYNC", "ENRICH_DEFERRED"}:
            next_status = "RAW_SYNC"

        def _do_mark():
            with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
                row = conn.execute(
                    """
                    UPDATE fact_strategic_memory
                    SET enrichment_status = ?,
                        enrichment_attempts = COALESCE(enrichment_attempts, 0) + 1,
                        enrichment_error = ?,
                        enrichment_model = ?
                    WHERE symbol = ?
                      AND trade_date = CAST(? AS DATE)
                      AND source = ?
                      AND UPPER(COALESCE(enrichment_status, 'RAW_SYNC')) IN ('RAW_SYNC', 'ENRICH_DEFERRED')
                      AND LOWER(COALESCE(embedding_status, '')) <> 'quarantined'
                      AND LOWER(COALESCE(enrichment_error, '')) NOT LIKE 'narrative_quality_rejected:%'
                    RETURNING enrichment_status
                    """,
                    [
                        next_status,
                        str(error or "backfill_failed")[:500],
                        str(model or "")[:80],
                        record.get("symbol", ""),
                        record.get("trade_date", ""),
                        record.get("source", "audit"),
                    ],
                ).fetchone()
                return str(row[0] or current_status) if row else current_status

        return str(
            with_duckdb_retry(_do_mark, retries=6, base_delay=0.5) or current_status
        )

    @staticmethod
    def _is_timeoutish_error(error: str) -> bool:
        text = str(error or "").lower()
        return any(token in text for token in ("timeout", "timed out", "guard", "ollama_busy", "http_0"))

    def _trend_status(
        self,
        raw_before: int,
        raw_after: int,
        daily_new_avg: float,
        attempted: int = 0,
        enriched: int = 0,
        failed: int = 0,
        deferred: int = 0,
    ) -> str:
        attempted = max(0, int(attempted or 0))
        enriched = max(0, int(enriched or 0))
        failed = max(0, int(failed or 0))
        deferred = max(0, int(deferred or 0))
        unresolved = failed + deferred
        if attempted > 0:
            if enriched == 0 and unresolved >= attempted:
                return "ALL_FAILED" if failed >= attempted else "ALL_UNRESOLVED"
            if unresolved / attempted >= 0.50:
                return "HIGH_FAILURE_RATE"
        if raw_after < raw_before:
            return "HEALTHY_CONVERGING"
        low_bar = max(20.0, daily_new_avg * 2.0)
        if raw_after <= low_bar:
            return "HEALTHY_LOW_STABLE"
        if raw_after > raw_before:
            return "CAPACITY_WARNING"
        return "WATCH_STABLE"

    def _record_backfill_run(self, stats: Dict[str, Any]) -> None:
        def _do_insert():
            with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
                conn.execute(
                    """
                    INSERT INTO ops_rag_enrichment_runs
                        (run_id, mode, started_at, ended_at, raw_before, raw_after,
                         deferred_before, deferred_after, daily_new_avg, attempted,
                         enriched, failed, deferred, skipped, embedded,
                         circuit_breaker_reason, trend_status, notes)
                    VALUES (?, ?, CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (run_id) DO UPDATE SET
                        ended_at = excluded.ended_at,
                        raw_after = excluded.raw_after,
                        deferred_after = excluded.deferred_after,
                        attempted = excluded.attempted,
                        enriched = excluded.enriched,
                        failed = excluded.failed,
                        deferred = excluded.deferred,
                        skipped = excluded.skipped,
                        embedded = excluded.embedded,
                        circuit_breaker_reason = excluded.circuit_breaker_reason,
                        trend_status = excluded.trend_status,
                        notes = excluded.notes
                    """,
                    [
                        stats.get("run_id", ""),
                        stats.get("mode", ""),
                        stats.get("started_at", ""),
                        stats.get("ended_at", ""),
                        int(stats.get("raw_before", 0) or 0),
                        int(stats.get("raw_after", 0) or 0),
                        int(stats.get("deferred_before", 0) or 0),
                        int(stats.get("deferred_after", 0) or 0),
                        float(stats.get("daily_new_avg", 0) or 0),
                        int(stats.get("attempted", 0) or 0),
                        int(stats.get("enriched", 0) or 0),
                        int(stats.get("failed", 0) or 0),
                        int(stats.get("deferred", 0) or 0),
                        int(stats.get("skipped", 0) or 0),
                        int(stats.get("embedded", 0) or 0),
                        str(stats.get("circuit_breaker_reason", "") or "")[:500],
                        str(stats.get("trend_status", "") or "")[:80],
                        json.dumps(stats.get("notes", {}), ensure_ascii=False, default=str)[:1600],
                    ],
                )

        with_duckdb_retry(_do_insert, retries=6, base_delay=0.5)

    def backfill_raw_sync_enrichment(
        self,
        *,
        max_records: Optional[int] = None,
        budget_sec: Optional[int] = None,
        include_deferred: bool = False,
        deferred_only: bool = False,
        mode: str = "nightly",
        soft_deadline: Optional[str] = None,
    ) -> Dict[str, Any]:
        started_at = datetime.now()
        self._reset_synthesis_guard()
        counts_before = self._backfill_counts()
        daily_new_avg = self._recent_daily_new_avg(days=5)
        run_id = f"{started_at.strftime('%Y%m%d%H%M%S')}_{mode}"
        stats: Dict[str, Any] = {
            "run_id": run_id,
            "mode": mode,
            "started_at": started_at.strftime("%Y-%m-%d %H:%M:%S"),
            "ended_at": started_at.strftime("%Y-%m-%d %H:%M:%S"),
            "raw_before": counts_before.get("raw_sync", 0),
            "raw_after": counts_before.get("raw_sync", 0),
            "deferred_before": counts_before.get("deferred", 0),
            "deferred_after": counts_before.get("deferred", 0),
            "daily_new_avg": daily_new_avg,
            "attempted": 0,
            "enriched": 0,
            "failed": 0,
            "deferred": 0,
            "skipped": 0,
            "embedded": 0,
            "circuit_breaker_reason": "",
            "trend_status": "",
            "notes": {
                "priority_mode": RAG_BACKFILL_PRIORITY_MODE,
                "include_deferred": include_deferred,
                "deferred_only": deferred_only,
            },
        }

        if not RAG_BACKFILL_ENABLED:
            stats["circuit_breaker_reason"] = "disabled"
            stats["trend_status"] = self._trend_status(stats["raw_before"], stats["raw_after"], daily_new_avg)
            stats["ended_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self._record_backfill_run(stats)
            return stats

        record_limit = RAG_BACKFILL_MAX_RECORDS if max_records is None else max(0, int(max_records))
        if deferred_only and max_records is None:
            record_limit = RAG_BACKFILL_DEFERRED_WEEKLY_LIMIT
        budget = RAG_BACKFILL_BATCH_BUDGET_SEC if budget_sec is None else max(0, int(budget_sec))
        deadline_budget = _seconds_until_local_hhmm(soft_deadline or RAG_BACKFILL_SOFT_DEADLINE)
        if deadline_budget > 0:
            budget = min(budget, deadline_budget) if budget > 0 else deadline_budget
        if record_limit <= 0 or budget <= 0:
            stats["circuit_breaker_reason"] = "no_budget_or_limit"
            stats["trend_status"] = self._trend_status(stats["raw_before"], stats["raw_after"], daily_new_avg)
            stats["ended_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self._record_backfill_run(stats)
            return stats

        candidates = self._fetch_backfill_candidates(
            record_limit,
            include_deferred=include_deferred,
            deferred_only=deferred_only,
        )
        if not candidates:
            stats["circuit_breaker_reason"] = "no_candidates"
            stats["trend_status"] = self._trend_status(stats["raw_before"], stats["raw_after"], daily_new_avg)
            stats["ended_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self._record_backfill_run(stats)
            return stats

        ram_ok = self._check_ram()
        deadline = time.monotonic() + budget
        timeout_streak = 0
        for idx, rec in enumerate(candidates):
            if time.monotonic() >= deadline:
                stats["skipped"] += len(candidates) - idx
                stats["circuit_breaker_reason"] = "soft_deadline"
                break
            if self._synth_timeout_guard_open:
                stats["skipped"] += len(candidates) - idx
                stats["circuit_breaker_reason"] = "timeout_guard_open"
                break

            stats["attempted"] += 1
            synthesis = self.synthesize_backfill_narrative(rec)
            if bool(synthesis.get("template_ready")):
                timeout_streak = 0
                if self.update_template_memory(rec, synthesis):
                    stats["enriched"] += 1
                    if ram_ok and synthesis.get("narrative"):
                        if self.embed_to_chroma(
                            rec.get("symbol", ""),
                            rec.get("trade_date", ""),
                            synthesis["narrative"],
                            synthesis.get("tags", ""),
                            source=rec.get("source", ""),
                            enrichment_status=TEMPLATE_READY_STATUS,
                        ):
                            stats["embedded"] += 1
                else:
                    stats["failed"] += 1
                continue

            if bool(synthesis.get("enriched")):
                timeout_streak = 0
                if self.update_enriched_memory(rec, synthesis):
                    stats["enriched"] += 1
                    if ram_ok and synthesis.get("narrative"):
                        if self.embed_to_chroma(
                            rec.get("symbol", ""),
                            rec.get("trade_date", ""),
                            synthesis["narrative"],
                            synthesis.get("tags", ""),
                            source=rec.get("source", ""),
                            enrichment_status="ENRICHED",
                        ):
                            stats["embedded"] += 1
                else:
                    stats["failed"] += 1
                continue

            error = str(synthesis.get("error") or "synthesis_failed")
            model = str(synthesis.get("model") or SYNTHESIS_MODEL)
            next_status = self.mark_backfill_failed(rec, error, model=model)
            if next_status == "ENRICH_DEFERRED" and str(rec.get("_enrichment_status", "")).upper() != "ENRICH_DEFERRED":
                stats["deferred"] += 1
            else:
                stats["failed"] += 1
            if self._is_timeoutish_error(error):
                timeout_streak += 1
            else:
                timeout_streak = 0
            if timeout_streak >= RAG_BACKFILL_MAX_CONSECUTIVE_TIMEOUTS:
                stats["skipped"] += len(candidates) - idx - 1
                stats["circuit_breaker_reason"] = f"consecutive_timeout_{timeout_streak}"
                break

        if ram_ok and self._chroma_available:
            try:
                stats["embedded"] += self.backfill_pending_embeddings(limit=MAX_EMBED_BACKFILL)
            except Exception as exc:
                logger.warning(f"[RAG-BACKFILL] embedding backfill skipped: {exc}")

        counts_after = self._backfill_counts()
        stats["raw_after"] = counts_after.get("raw_sync", 0)
        stats["deferred_after"] = counts_after.get("deferred", 0)
        stats["trend_status"] = self._trend_status(
            stats["raw_before"],
            stats["raw_after"],
            daily_new_avg,
            attempted=stats["attempted"],
            enriched=stats["enriched"],
            failed=stats["failed"],
            deferred=stats["deferred"],
        )
        stats["ended_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        stats["notes"].update({
            "candidate_count": len(candidates),
            "record_limit": record_limit,
            "budget_sec": budget,
            "max_attempts": RAG_BACKFILL_MAX_ATTEMPTS,
            "deferred_max_attempts": RAG_BACKFILL_DEFERRED_MAX_ATTEMPTS,
        })
        self._record_backfill_run(stats)
        logger.info(f"[RAG-BACKFILL] done stats={stats}")
        _force_checkpoint("RAG_BACKFILL")
        return stats

    # ==================== Embed ====================

    @staticmethod
    def _chunk_text(text: str, chunk_size: int = 512, chunk_overlap: int = 64) -> list:
        """Split text into overlapping chunks (semantic contract: 512/64)"""
        if len(text) <= chunk_size:
            return [text]
        chunks = []
        start = 0
        while start < len(text):
            end = start + chunk_size
            chunks.append(text[start:end])
            start = end - chunk_overlap
        return chunks

    def embed_to_chroma(self, symbol: str, trade_date: str,
                        narrative: str, tags: str, source: str = "",
                        enrichment_status: str = "") -> bool:
        """Embed narrative chunks into ChromaDB (512/64 split, mxbai 1024d)"""
        quality_issue = narrative_quality_issue(narrative)
        if quality_issue:
            logger.warning(
                f"[RAG] embedding rejected symbol={symbol} date={trade_date}: {quality_issue}"
            )
            return False
        if not self._chroma_available or not self._chroma_collection:
            return False

        full_text = f"{narrative} {tags}"
        chunks = self._chunk_text(full_text, chunk_size=512, chunk_overlap=64)
        document_sha256 = hashlib.sha256(full_text.encode("utf-8")).hexdigest()
        memory_key = f"{symbol}|{trade_date}|{source}"

        try:
            source_key = re.sub(r"[^A-Za-z0-9_:-]+", "_", str(source or "memory"))[:80]
            chunk_ids = [f"{symbol}_{trade_date}_{source_key}_c{i}" for i in range(len(chunks))]
            try:
                self._chroma_collection.delete(where={
                    "$and": [
                        {"symbol": symbol},
                        {"trade_date": trade_date},
                        {"source": source},
                    ]
                })
            except Exception as cleanup_exc:
                logger.warning(f"ChromaDB stale chunk cleanup skipped: {cleanup_exc}")
            chunk_metas = [{
                "symbol": symbol,
                "trade_date": trade_date,
                "source": source,
                "enrichment_status": enrichment_status,
                "tags": tags,
                "document_version": RAG_DOCUMENT_VERSION,
                "document_sha256": document_sha256,
                "chunk_sha256": hashlib.sha256(chunks[i].encode("utf-8")).hexdigest(),
                "memory_key": memory_key,
                "quarantine_status": "CLEAR",
                "chunk_index": i,
                "total_chunks": len(chunks)
            } for i in range(len(chunks))]

            self._chroma_collection.upsert(
                ids=chunk_ids,
                documents=chunks,
                metadatas=chunk_metas
            )
            # Update embedding status
            self._mark_embedding_status(symbol=symbol, trade_date=trade_date, status="embedded", source=source)
            return True
        except Exception as e:
            logger.warning(f"ChromaDB embed failed: {e}")
            try:
                self._mark_embedding_status(symbol=symbol, trade_date=trade_date, status="degraded", source=source)
            except Exception as mark_exc:
                logger.warning(f"Failed to mark embedding degraded: {mark_exc}")
            return False

    def _fetch_pending_embeddings(self, limit: int) -> List[Dict[str, str]]:
        def _do_fetch():
            with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
                rows = conn.execute(
                    """
                    SELECT symbol,
                           CAST(trade_date AS VARCHAR) AS trade_date,
                           narrative_text,
                           ssd_tags,
                           source,
                           enrichment_status
                    FROM fact_strategic_memory
                    WHERE COALESCE(embedding_status, 'pending') != 'embedded'
                      AND UPPER(COALESCE(enrichment_status, 'RAW_SYNC')) IN ('ENRICHED', 'TEMPLATE_READY')
                      AND LOWER(COALESCE(embedding_status, '')) <> 'quarantined'
                      AND LOWER(COALESCE(enrichment_error, '')) NOT LIKE 'narrative_quality_rejected:%'
                      AND COALESCE(narrative_text, '') != ''
                    ORDER BY trade_date DESC, created_at DESC
                    LIMIT ?
                    """,
                    [int(limit)],
                ).fetchall()
                return rows

        rows = with_duckdb_retry(_do_fetch, retries=6, base_delay=0.5)
        return [
            {
                "symbol": str(r[0] or ""),
                "trade_date": str(r[1] or ""),
                "narrative_text": str(r[2] or ""),
                "ssd_tags": str(r[3] or ""),
                "source": str(r[4] or ""),
                "enrichment_status": str(r[5] or ""),
            }
            for r in rows
            if not narrative_quality_issue(r[2])
        ]

    def backfill_pending_embeddings(self, limit: int = MAX_EMBED_BACKFILL) -> int:
        if not self._chroma_available or not self._chroma_collection:
            return 0
        pending_rows = self._fetch_pending_embeddings(limit=limit)
        if not pending_rows:
            return 0
        embedded = 0
        for row in pending_rows:
            if self.embed_to_chroma(
                symbol=row["symbol"],
                trade_date=row["trade_date"],
                narrative=row["narrative_text"],
                tags=row["ssd_tags"],
                source=row.get("source", ""),
                enrichment_status=row.get("enrichment_status", ""),
            ):
                embedded += 1
        logger.info(f"[RAG] pending embedding backfill done: {embedded}/{len(pending_rows)}")
        return embedded

    def reconcile_embedding_inventory(self, repair_missing: bool = False) -> Dict[str, int]:
        """Compare embedded DuckDB memories with ENRICHED Chroma groups."""
        if not self._chroma_available or not self._chroma_collection:
            return {
                "db_embedded": 0,
                "chroma_groups": 0,
                "missing_groups": 0,
                "orphan_groups": 0,
                "requeued_groups": 0,
            }

        def _do_fetch():
            with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
                return conn.execute(
                    """
                    SELECT symbol, CAST(trade_date AS VARCHAR), source
                    FROM fact_strategic_memory
                    WHERE UPPER(COALESCE(enrichment_status, '')) IN ('ENRICHED', 'TEMPLATE_READY')
                      AND COALESCE(embedding_status, '') = 'embedded'
                    """
                ).fetchall()

        db_groups = {
            (str(symbol), str(trade_date), str(source or ""))
            for symbol, trade_date, source in with_duckdb_retry(_do_fetch)
        }
        payload = self._chroma_collection.get(
            where={"enrichment_status": {"$in": list(INDEXABLE_ENRICHMENT_STATUSES)}},
            include=["metadatas"],
        )
        chroma_groups = {
            (
                str(meta.get("symbol") or ""),
                str(meta.get("trade_date") or ""),
                str(meta.get("source") or ""),
            )
            for meta in (payload.get("metadatas") or [])
        }
        missing_groups = db_groups - chroma_groups
        if repair_missing and missing_groups:
            def _do_requeue():
                with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
                    conn.executemany(
                        """
                        UPDATE fact_strategic_memory
                        SET embedding_status = 'pending'
                        WHERE symbol = ?
                          AND trade_date = CAST(? AS DATE)
                          AND source = ?
                          AND UPPER(COALESCE(enrichment_status, '')) IN ('ENRICHED', 'TEMPLATE_READY')
                        """,
                        [list(group) for group in sorted(missing_groups)],
                    )

            with_duckdb_retry(_do_requeue, retries=6, base_delay=0.5)
        stats = {
            "db_embedded": len(db_groups),
            "chroma_groups": len(chroma_groups),
            "missing_groups": len(missing_groups),
            "orphan_groups": len(chroma_groups - db_groups),
            "requeued_groups": len(missing_groups) if repair_missing else 0,
        }
        level = logging.WARNING if stats["missing_groups"] or stats["orphan_groups"] else logging.INFO
        logger.log(level, f"[RAG] embedding inventory {stats}")
        return stats

    # ==================== Main Refresh ====================

    def refresh(self, trade_date: str = None) -> Dict[str, int]:
        """
        Main refresh entry point (called at 19:00).
        Returns stats for raw sync, optional enrichment, and embedding
        """
        stats = {
            "extracted": 0,
            "raw_synced": 0,
            "stored": 0,
            "embedded": 0,
            "template_ready": 0,
            "template_failed": 0,
            "enrich_attempted": 0,
            "enriched": 0,
            "enrich_failed": 0,
            "enrich_skipped": 0,
            "embedded_backfill": 0,
            "quality_quarantined": 0,
            "quality_vectors_removed": 0,
            "inventory_db_embedded": 0,
            "inventory_chroma_groups": 0,
            "inventory_missing_groups": 0,
            "inventory_orphan_groups": 0,
            "inventory_requeued_groups": 0,
        }

        self._reset_synthesis_guard()
        quality_stats = self.quarantine_low_quality_narratives()
        stats["quality_quarantined"] = quality_stats["quarantined"]
        stats["quality_vectors_removed"] = quality_stats["vectors_removed"]
        records = self.extract_daily_logs(trade_date)
        stats["extracted"] = len(records)

        if not records:
            logger.info("No records to process")

        resolved_trade_date = str(trade_date or self._last_resolved_trade_date or "")
        tide_status = self._get_tide_status(resolved_trade_date) if records else ""
        ram_ok = self._check_ram()

        for rec in records:
            rec["_tide_status"] = tide_status
            synthesis = self._deterministic_backfill_synthesis(rec)
            status = TEMPLATE_READY_STATUS if synthesis and synthesis.get("template_ready") else "RAW_SYNC"
            if synthesis is None:
                stats["template_failed"] += 1
                synthesis = self.raw_sync_synthesis(rec)

            # Store deterministic facts first; this path must not depend on any model.
            if self.store_memory(rec, synthesis, tide_status, enrichment_status=status):
                stats["stored"] += 1
                stats["raw_synced"] += 1
                if status == TEMPLATE_READY_STATUS:
                    stats["template_ready"] += 1
                    if ram_ok and synthesis.get("narrative"):
                        if self.embed_to_chroma(
                            rec.get("symbol", ""),
                            rec.get("trade_date", ""),
                            synthesis["narrative"],
                            synthesis.get("tags", ""),
                            source=rec.get("source", ""),
                            enrichment_status=TEMPLATE_READY_STATUS,
                        ):
                            stats["embedded"] += 1

        enrich_deadline = time.monotonic() + RAG_ENRICH_BATCH_BUDGET_SEC
        if RAG_ENRICH_ENABLED and RAG_ENRICH_BATCH_BUDGET_SEC > 0:
            for rec in records:
                if time.monotonic() >= enrich_deadline:
                    stats["enrich_skipped"] += 1
                    continue
                if not self._eligible_for_enrichment(rec):
                    continue
                attempts = self._current_enrichment_attempts(rec)
                if attempts >= RAG_ENRICH_MAX_ATTEMPTS:
                    stats["enrich_skipped"] += 1
                    continue

                stats["enrich_attempted"] += 1
                synthesis = self.synthesize_narrative(rec)
                if bool(synthesis.get("enriched")):
                    if self.update_enriched_memory(rec, synthesis):
                        stats["enriched"] += 1
                        if ram_ok and synthesis.get("narrative"):
                            if self.embed_to_chroma(
                                rec.get("symbol", ""),
                                rec.get("trade_date", ""),
                                synthesis["narrative"],
                                synthesis.get("tags", ""),
                                source=rec.get("source", ""),
                                enrichment_status="ENRICHED",
                            ):
                                stats["embedded"] += 1
                    else:
                        stats["enrich_failed"] += 1
                else:
                    stats["enrich_failed"] += 1
                    self.mark_enrichment_failed(
                        rec,
                        str(synthesis.get("error") or "synthesis_failed"),
                        model=str(synthesis.get("model") or SYNTHESIS_MODEL),
                    )

        if ram_ok and self._chroma_available:
            repair = self.reconcile_embedding_inventory(repair_missing=True)
            stats["inventory_requeued_groups"] = repair["requeued_groups"]
            stats["embedded_backfill"] = self.backfill_pending_embeddings(limit=MAX_EMBED_BACKFILL)

        inventory = self.reconcile_embedding_inventory()
        stats["inventory_db_embedded"] = inventory["db_embedded"]
        stats["inventory_chroma_groups"] = inventory["chroma_groups"]
        stats["inventory_missing_groups"] = inventory["missing_groups"]
        stats["inventory_orphan_groups"] = inventory["orphan_groups"]

        logger.info(
            f"RAG REFRESH complete: extracted={stats['extracted']} "
            f"raw_synced={stats['raw_synced']} stored={stats['stored']} "
            f"template_ready={stats['template_ready']} template_failed={stats['template_failed']} "
            f"enriched={stats['enriched']}/{stats['enrich_attempted']} "
            f"enrich_failed={stats['enrich_failed']} "
            f"embedded={stats['embedded']} backfill={stats['embedded_backfill']}"
        )
        _force_checkpoint("RAG_REFRESH")
        return stats


# ==================== Singleton ====================

_refresher = None
def get_refresher() -> RAGRefresher:
    global _refresher
    if _refresher is None:
        _refresher = RAGRefresher()
    return _refresher


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    refresher = get_refresher()

    print("=" * 60)
    print("  RAG REFRESHER SELF-TEST")
    print("=" * 60)

    # Extract
    records = refresher.extract_daily_logs()
    print(f"\nExtracts: {len(records)} records")
    for r in records[:3]:
        print(f"  {r['symbol']} | {r['source']} | {r.get('l4_verdict','?')}")

    # Rule-based synthesis test
    if records:
        synth = refresher._rule_based_tags(records[0])
        print(f"\nSynthesis test: {synth}")

    # RAM check
    ram = psutil.virtual_memory()
    print(f"\nRAM: {ram.percent:.1f}% (threshold: {RAM_THRESHOLD}%)")
    print(f"ChromaDB: {'available' if refresher._chroma_available else 'not installed'}")

    # Full refresh
    print("\n--- Full Refresh ---")
    stats = refresher.refresh()
    print(f"Stats: {stats}")
