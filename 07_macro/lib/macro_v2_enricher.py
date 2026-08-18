#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
07_macro/lib/macro_v2_enricher.py
Macro V2 sidecar enrichment:
- Consume V1 persisted Top5 topics.
- Fetch dual-source topic news (Sina + CCTV only).
- Run token-juicer summary.
- Call node-102 Fin-R1 (Ollama API) with strict JSON contract.
- Fallback silently on any network/timeout/JSON parsing error.
"""

from __future__ import annotations

import runpy
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from config.settings import Config

logger = logging.getLogger("zhulong.macro.macro_v2_enricher")

_current = Path(__file__).resolve()
PROJECT_ROOT = next(
    (p for p in _current.parents if (p / ".git").exists() or (p / "storage").exists()),
    _current.parents[2],
)
DEFAULT_DB_PATH = str(PROJECT_ROOT / "storage" / "database" / "zhulong.duckdb")


_loader_ns = runpy.run_path(str(PROJECT_ROOT / "04_governance" / "lib" / "core" / "module_loader.py"))
load_module_from_path = _loader_ns["load_module_from_path"]


def _load_module(path: Path, module_name: str):
    return load_module_from_path(module_name, path)


_news_mod = _load_module(_current.parent / "news_adapter.py", "macro_news_adapter_v2_enricher")
_tu_mod = _load_module(_current.parent / "tushare_client.py", "macro_tushare_client_v2_enricher")
_safe_mod = _load_module(
    PROJECT_ROOT / "04_governance" / "lib" / "core" / "safe_writer.py",
    "safe_writer_macro_v2_enricher",
)
_cgw_mod = _load_module(
    PROJECT_ROOT / "02_brain" / "lib" / "compute_gateway.py",
    "compute_gateway_macro_v2_enricher",
)

MacroNewsAdapter = _news_mod.MacroNewsAdapter
MacroTushareClient = _tu_mod.MacroTushareClient
duckdb_safe = _safe_mod.duckdb_safe
ComputeGateway = _cgw_mod.ComputeGateway
COMPUTE_GATEWAY = ComputeGateway(logger=logger, max_slots=3)
OLLAMA_TIMEOUT_SECONDS = max(300, int(os.getenv("MACRO_V2_OLLAMA_TIMEOUT", "300")))

VALID_POLICY = {"NATIONAL", "MINISTRY", "LOCAL", "NONE"}
VALID_NOVELTY = {"NEW", "REHASH"}

POLICY_SCORE = {
    "NATIONAL": 15,
    "MINISTRY": 10,
    "LOCAL": 6,
    "NONE": 0,
}
NOVELTY_SCORE = {
    "NEW": 10,
    "REHASH": 2,
}

MEMORY_CONTEXT_TOPK = max(1, int(os.getenv("MACRO_V2_MEMORY_TOPK", "3")))
MEMORY_CONTEXT_TEXT_LIMIT = max(80, int(os.getenv("MACRO_V2_MEMORY_TEXT_LIMIT", "220")))


@dataclass
class MacroV2Stats:
    processed_topics: int = 0
    updated_topics: int = 0
    skipped_no_news: int = 0
    fallback_topics: int = 0


class MacroV2Enricher:
    """Best-effort Macro V2 sidecar enrichment."""

    def __init__(
        self,
        db_path: Optional[str] = None,
    ):
        self.db_path = str(db_path or os.getenv("DB_PATH", "") or Config.DB_PATH or DEFAULT_DB_PATH)

        self.enabled = str(os.getenv("MACRO_V2_ENABLED", "true")).strip().lower() not in {
            "0",
            "false",
            "off",
            "no",
        }

        node102_ip = str(os.getenv("MACRO_V2_NODE102_IP", "") or os.getenv("NODE102_IP", "")).strip()
        forced_url = str(os.getenv("MACRO_V2_OLLAMA_URL", "")).strip()
        if forced_url:
            self.ollama_url = forced_url
        elif node102_ip:
            self.ollama_url = f"http://{node102_ip}:11434/api/generate"
        else:
            self.ollama_url = str(getattr(Config, "OLLAMA_URL", "http://127.0.0.1:11434/api/generate"))
            logger.warning(
                "MACRO_V2_NODE102_IP missing, fallback ollama_url=%s (recommend setting MACRO_V2_NODE102_IP in .env)",
                self.ollama_url,
            )

        self.ollama_model = str(
            os.getenv("MACRO_V2_OLLAMA_MODEL", os.getenv("OLLAMA_MODEL", "fin-r1:7b"))
        ).strip()

        self.tushare_client = MacroTushareClient(timeout_sec=30, max_retries=2, base_delay=0.8, jitter_max=0.3)
        self.news_adapter = MacroNewsAdapter(
            min_body_chars=50,
            max_news_items=3,
            title_similarity_threshold=0.88,
            max_prompt_chars=1500,
            tushare_client=self.tushare_client,
        )
        self.memory_context_enabled = str(os.getenv("MACRO_V2_MEMORY_CONTEXT_ENABLED", "true")).strip().lower() not in {
            "0", "false", "off", "no"
        }
        self.memory_topk = MEMORY_CONTEXT_TOPK
        self.memory_text_limit = MEMORY_CONTEXT_TEXT_LIMIT
        self.memory_log_prompt = str(os.getenv("MACRO_V2_LOG_PROMPT", "0")).strip().lower() in {"1", "true", "on", "yes"}
        self.memory_embed_model = str(os.getenv("MACRO_V2_MEMORY_EMBED_MODEL", "mxbai-embed-large:latest")).strip()
        self.memory_source = "none"

    def enrich_top5(self, trade_date: str, top_rows: List[Dict[str, object]]) -> MacroV2Stats:
        stats = MacroV2Stats()
        if not self.enabled:
            logger.info("macro_v2 disabled by MACRO_V2_ENABLED")
            return stats
        if not top_rows:
            logger.info("macro_v2 skip: empty top_rows")
            return stats

        for row in list(top_rows)[:5]:
            topic_type = str(row.get("topic_type", "") or "")
            topic_id = str(row.get("topic_id", "") or "")
            topic_name = str(row.get("topic_name", topic_id) or topic_id)
            if not topic_type or not topic_id:
                continue

            stats.processed_topics += 1
            logger.info(
                "macro_v2 enrich start trade_date=%s topic=%s:%s(%s)",
                trade_date,
                topic_type,
                topic_id,
                topic_name,
            )

            try:
                raw_news = self.news_adapter.fetch_news_dual_source(topic_type, topic_id, topic_name)
                if not raw_news:
                    stats.skipped_no_news += 1
                    self._safe_persist_default(trade_date, topic_type, topic_id)
                    logger.warning(
                        "macro_v2 no matched news topic=%s:%s(%s), fallback default tags",
                        topic_type,
                        topic_id,
                        topic_name,
                    )
                    continue

                digest = self.news_adapter.build_digest_for_topic(topic_type, topic_id, topic_name, raw_news)
                logger.info(
                    "macro_v2 digest topic=%s raw=%s kept=%s dedup_drop=%s selected=%s chars=%s",
                    topic_name,
                    digest.raw_count,
                    digest.kept_after_noise,
                    digest.dedup_dropped,
                    digest.selected_count,
                    digest.merged_chars,
                )

                if not digest.merged_text:
                    stats.skipped_no_news += 1
                    self._safe_persist_default(trade_date, topic_type, topic_id)
                    logger.warning(
                        "macro_v2 empty digest topic=%s:%s(%s), fallback default tags",
                        topic_type,
                        topic_id,
                        topic_name,
                    )
                    continue

                ai_json = self._call_finr1_extract(topic_name, digest.merged_text)
                llm_score = self._lookup_llm_score(ai_json)

                self._persist_ai_tags(
                    trade_date=trade_date,
                    topic_type=topic_type,
                    topic_id=topic_id,
                    llm_resonance_score=llm_score,
                    is_event_driven_trap=bool(ai_json["is_event_driven_trap"]),
                )
                stats.updated_topics += 1

                logger.info(
                    "macro_v2 parsed topic=%s policy=%s novelty=%s trap=%s score=%s core_driver=%s risk_warning=%s",
                    topic_name,
                    ai_json["policy_level"],
                    ai_json["novelty"],
                    ai_json["is_event_driven_trap"],
                    llm_score,
                    ai_json["ai_diagnosis"]["core_driver"],
                    ai_json["ai_diagnosis"]["risk_warning"],
                )
            except Exception as exc:
                stats.fallback_topics += 1
                self._safe_persist_default(trade_date, topic_type, topic_id)
                logger.warning(
                    "macro_v2 fallback topic=%s:%s(%s) err=%s",
                    topic_type,
                    topic_id,
                    topic_name,
                    exc,
                )

        logger.info(
            "macro_v2 done trade_date=%s processed=%s updated=%s skipped_no_news=%s fallback=%s",
            trade_date,
            stats.processed_topics,
            stats.updated_topics,
            stats.skipped_no_news,
            stats.fallback_topics,
        )
        return stats

    @staticmethod
    def _tokenize_query(text: str) -> List[str]:
        raw = re.split(r"[^0-9A-Za-z\u4e00-\u9fff]+", str(text or "").lower())
        uniq: List[str] = []
        for token in raw:
            t = token.strip()
            if len(t) < 2:
                continue
            if t not in uniq:
                uniq.append(t)
        return uniq[:20]

    def _ollama_base(self) -> str:
        return str(self.ollama_url).split("/api/", 1)[0].rstrip("/")

    def _query_embedding(self, query_text: str) -> Optional[List[float]]:
        try:
            resp = COMPUTE_GATEWAY.ollama_embeddings(
                server=self._ollama_base(),
                payload={"model": self.memory_embed_model, "prompt": query_text},
                timeout=OLLAMA_TIMEOUT_SECONDS,
                layer="MACRO",
                decision_id="macro_v2:embedding",
            )
            if resp.status_code != 200:
                return None
            body = resp.json() if resp.content else {}
            emb = body.get("embedding")
            return emb if isinstance(emb, list) and emb else None
        except Exception as exc:
            if COMPUTE_GATEWAY.is_timeout_error(exc):
                logger.warning("[TIMEOUT] macro_v2 embeddings timeout: %s", exc)
                return None
            logger.error("Non-fatal: macro_v2 embeddings query failed: %s", exc, exc_info=True)
            return None

    def _memory_from_chroma(self, query_text: str, top_k: int) -> List[str]:
        try:
            import chromadb

            os.environ.setdefault("ANONYMIZED_TELEMETRY", "False")
            logging.getLogger("chromadb").setLevel(logging.CRITICAL)
            logging.getLogger("chromadb.telemetry").setLevel(logging.CRITICAL)
            logging.getLogger("chromadb.telemetry.product.posthog").setLevel(logging.CRITICAL)
            logging.getLogger("posthog").setLevel(logging.CRITICAL)
            from chromadb.config import Settings
            client = chromadb.PersistentClient(
                path=str(PROJECT_ROOT / "storage" / "chromadb"),
                settings=Settings(anonymized_telemetry=False),
            )
            coll = client.get_or_create_collection(name="strategic_memory_v2")
            emb = self._query_embedding(query_text)
            if not emb:
                return []

            result = coll.query(
                query_embeddings=[emb],
                n_results=top_k,
                include=["documents", "metadatas", "distances"],
            )
            docs = (result.get("documents") or [[]])[0]
            metas = (result.get("metadatas") or [[]])[0]
            dists = (result.get("distances") or [[]])[0]

            rows: List[str] = []
            for idx, doc in enumerate(docs):
                meta = metas[idx] if idx < len(metas) and isinstance(metas[idx], dict) else {}
                dist = float(dists[idx]) if idx < len(dists) and dists[idx] is not None else 1.0
                score = max(0.0, 1.0 - dist)
                narrative = str(doc or "").strip().replace("\n", " ")
                if not narrative:
                    continue
                rows.append(
                    f"score={score:.3f} date={meta.get('trade_date', '?')} symbol={meta.get('symbol', '?')} "
                    f"tags={meta.get('tags', '')} | {narrative[:self.memory_text_limit]}"
                )

            if rows:
                self.memory_source = "chroma"
            return rows[:top_k]
        except Exception as exc:
            logger.info("macro_v2 memory chroma unavailable: %s", exc)
            return []

    def _memory_from_duckdb(self, query_text: str, top_k: int) -> List[str]:
        tokens = self._tokenize_query(query_text)
        rows: List[str] = []

        with duckdb_safe(self.db_path, read_only=True, retries=4, base_delay=0.3) as conn:
            recs = conn.execute(
                """
                SELECT symbol,
                       CAST(trade_date AS VARCHAR) AS trade_date,
                       COALESCE(ssd_tags, '') AS ssd_tags,
                       COALESCE(narrative_text, '') AS narrative_text
                FROM fact_strategic_memory
                WHERE COALESCE(narrative_text, '') != ''
                ORDER BY trade_date DESC, created_at DESC
                LIMIT 120
                """
            ).fetchall()

        scored = []
        for sym, trade_date, tags, narrative in recs:
            hay = f"{sym} {trade_date} {tags} {narrative}".lower()
            score = sum(1 for t in tokens if t in hay)
            if score <= 0 and tokens:
                continue
            scored.append((score, str(sym or ""), str(trade_date or ""), str(tags or ""), str(narrative or "")))

        if not scored:
            for sym, trade_date, tags, narrative in recs[:top_k]:
                rows.append(
                    f"score=0.000 date={trade_date} symbol={sym} tags={tags} | {str(narrative)[:self.memory_text_limit]}"
                )
            if rows:
                self.memory_source = "duckdb"
            return rows

        scored.sort(key=lambda x: (-x[0], x[2]), reverse=False)
        for score, sym, trade_date, tags, narrative in scored[:top_k]:
            rows.append(
                f"score={float(score):.3f} date={trade_date} symbol={sym} tags={tags} | {narrative[:self.memory_text_limit]}"
            )

        if rows:
            self.memory_source = "duckdb"
        return rows

    def _build_memory_context(self, topic_name: str, merged_news_text: str, top_k: Optional[int] = None) -> List[str]:
        self.memory_source = "none"
        if not self.memory_context_enabled:
            return []

        k = int(top_k or self.memory_topk or 3)
        query_text = f"{topic_name}\n{merged_news_text[:1200]}"
        rows = self._memory_from_chroma(query_text=query_text, top_k=k)
        if rows:
            return rows
        return self._memory_from_duckdb(query_text=query_text, top_k=k)

    def build_prompt_preview(self, topic_name: str, merged_news_text: str) -> Dict[str, object]:
        memory_rows = self._build_memory_context(topic_name=topic_name, merged_news_text=merged_news_text, top_k=self.memory_topk)
        prompt = self._build_prompt(topic_name=topic_name, merged_news_text=merged_news_text, memory_context_rows=memory_rows)
        return {
            "prompt": prompt,
            "memory_source": self.memory_source,
            "memory_hits": memory_rows,
        }

    def _call_finr1_extract(self, topic_name: str, merged_news_text: str) -> Dict[str, object]:
        memory_rows = self._build_memory_context(topic_name=topic_name, merged_news_text=merged_news_text, top_k=self.memory_topk)
        prompt = self._build_prompt(topic_name=topic_name, merged_news_text=merged_news_text, memory_context_rows=memory_rows)
        if self.memory_log_prompt:
            logger.info(
                "macro_v2 prompt topic=%s memory_source=%s hits=%s\n%s",
                topic_name,
                self.memory_source,
                len(memory_rows),
                prompt,
            )

        payload = {
            "model": self.ollama_model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": 0,
            },
        }

        t0 = time.perf_counter()
        try:
            response = COMPUTE_GATEWAY.ollama_generate(
                server=self._ollama_base(),
                payload=payload,
                timeout=OLLAMA_TIMEOUT_SECONDS,
                layer="MACRO",
                decision_id=f"macro_v2:{topic_name}",
            )
        except Exception as exc:
            elapsed = time.perf_counter() - t0
            if COMPUTE_GATEWAY.is_timeout_error(exc):
                logger.warning("[TIMEOUT] macro_v2 llm timeout topic=%s elapsed=%.3fs err=%s", topic_name, elapsed, exc)
            else:
                logger.error("Non-fatal: macro_v2 llm call failed topic=%s err=%s", topic_name, exc, exc_info=True)
            raise
        elapsed = time.perf_counter() - t0
        response.raise_for_status()

        body = response.json() if response.content else {}
        raw_text = str(body.get("response", "") or "").strip()
        if not raw_text:
            raise ValueError("Empty response from Fin-R1")

        parsed = self._parse_response_json(raw_text)
        logger.info(
            "macro_v2 llm ok topic=%s elapsed=%.3fs",
            topic_name,
            elapsed,
        )
        return parsed

    def _parse_response_json(self, text: str) -> Dict[str, object]:
        candidate = str(text or "").strip()
        if not candidate:
            raise ValueError("LLM output is empty")

        # Guard against occasional markdown wrapping from smaller models.
        if candidate.startswith("```"):
            candidate = re.sub(r"^```(?:json)?", "", candidate, flags=re.IGNORECASE).strip()
            if candidate.endswith("```"):
                candidate = candidate[:-3].strip()

        obj: Optional[Dict[str, object]] = None
        try:
            maybe = json.loads(candidate)
            if isinstance(maybe, dict):
                obj = maybe
        except Exception:
            obj = None

        if obj is None:
            m = re.search(r"\{.*\}", candidate, flags=re.DOTALL)
            if m:
                maybe = json.loads(m.group(0))
                if isinstance(maybe, dict):
                    obj = maybe

        if obj is None:
            raise ValueError(f"LLM output is not valid JSON object: {candidate[:200]}")

        policy_level = str(obj.get("policy_level", "") or "").strip().upper()
        novelty = str(obj.get("novelty", "") or "").strip().upper()
        trap_raw = obj.get("is_event_driven_trap", False)
        ai_diag_raw = obj.get("ai_diagnosis", {})
        if not isinstance(ai_diag_raw, dict):
            raise ValueError(f"Invalid ai_diagnosis type: {type(ai_diag_raw)}")
        core_driver = str(ai_diag_raw.get("core_driver", "") or "").strip()
        risk_warning = str(ai_diag_raw.get("risk_warning", "") or "").strip()

        if policy_level not in VALID_POLICY:
            raise ValueError(f"Invalid policy_level: {policy_level}")
        if novelty not in VALID_NOVELTY:
            raise ValueError(f"Invalid novelty: {novelty}")

        if isinstance(trap_raw, bool):
            trap = trap_raw
        elif isinstance(trap_raw, str):
            t = trap_raw.strip().lower()
            if t in {"true", "1", "yes"}:
                trap = True
            elif t in {"false", "0", "no", ""}:
                trap = False
            else:
                raise ValueError(f"Invalid is_event_driven_trap: {trap_raw}")
        else:
            raise ValueError(f"Invalid is_event_driven_trap type: {type(trap_raw)}")

        core_driver = re.sub(r"\s+", " ", core_driver)
        risk_warning = re.sub(r"\s+", " ", risk_warning)
        if len(core_driver) > 80:
            core_driver = core_driver[:80]
        if len(risk_warning) > 80:
            risk_warning = risk_warning[:80]
        if not core_driver:
            raise ValueError("ai_diagnosis.core_driver is empty")
        if not risk_warning:
            raise ValueError("ai_diagnosis.risk_warning is empty")

        return {
            "policy_level": policy_level,
            "novelty": novelty,
            "is_event_driven_trap": trap,
            "ai_diagnosis": {
                "core_driver": core_driver,
                "risk_warning": risk_warning,
            },
        }

    def _lookup_llm_score(self, ai_json: Dict[str, object]) -> int:
        policy = str(ai_json.get("policy_level", "NONE") or "NONE").upper()
        novelty = str(ai_json.get("novelty", "REHASH") or "REHASH").upper()
        trap = bool(ai_json.get("is_event_driven_trap", False))

        score = POLICY_SCORE.get(policy, 0) + NOVELTY_SCORE.get(novelty, 0)
        if trap:
            score -= 8
        return int(max(0, min(100, score)))

    def _safe_persist_default(self, trade_date: str, topic_type: str, topic_id: str) -> None:
        try:
            self._persist_ai_tags(
                trade_date=trade_date,
                topic_type=topic_type,
                topic_id=topic_id,
                llm_resonance_score=None,
                is_event_driven_trap=False,
            )
        except Exception as exc:
            logger.warning(
                "macro_v2 persist default failed topic=%s:%s err=%s",
                topic_type,
                topic_id,
                exc,
            )

    def _persist_ai_tags(
        self,
        trade_date: str,
        topic_type: str,
        topic_id: str,
        llm_resonance_score: Optional[int],
        is_event_driven_trap: bool,
    ) -> None:
        with duckdb_safe(self.db_path, read_only=False, retries=6, base_delay=0.5) as conn:
            conn.execute(
                """
                UPDATE fact_macro_topic_daily
                SET llm_resonance_score = ?,
                    is_event_driven_trap = ?,
                    updated_at = NOW()
                WHERE trade_date = CAST(? AS DATE)
                  AND topic_type = ?
                  AND topic_id = ?
                """,
                [
                    llm_resonance_score,
                    bool(is_event_driven_trap),
                    trade_date,
                    topic_type,
                    topic_id,
                ],
            )
            conn.execute(
                """
                UPDATE fact_macro_top5_daily
                SET llm_resonance_score = ?,
                    is_event_driven_trap = ?,
                    generated_at = NOW()
                WHERE trade_date = CAST(? AS DATE)
                  AND topic_type = ?
                  AND topic_id = ?
                """,
                [
                    llm_resonance_score,
                    bool(is_event_driven_trap),
                    trade_date,
                    topic_type,
                    topic_id,
                ],
            )
            conn.commit()

    @staticmethod
    def _build_prompt(topic_name: str, merged_news_text: str, memory_context_rows: Optional[List[str]] = None) -> str:
        system_text = (
            "你是一个客观的新闻事实抽取器。"
            "你只能输出合法的 JSON 格式，绝不允许输出任何 markdown 标记、思考过程或额外文本。"
        )

        schema_text = (
            "输出 JSON 必须严格包含且仅包含以下字段：\n"
            "{\n"
            "  \"policy_level\": \"NATIONAL\" | \"MINISTRY\" | \"LOCAL\" | \"NONE\",\n"
            "  \"novelty\": \"NEW\" | \"REHASH\",\n"
            "  \"is_event_driven_trap\": true | false,\n"
            "  \"ai_diagnosis\": {\n"
            "    \"core_driver\": \"string (一句话概括核心上涨逻辑)\",\n"
            "    \"risk_warning\": \"string (一句话指出最大的潜在利空或风险)\"\n"
            "  }\n"
            "}\n"
            "约束：ai_diagnosis.core_driver 与 ai_diagnosis.risk_warning 均必须为非空字符串，且不超过80字。"
        )

        rows = memory_context_rows or []
        if rows:
            memory_text = "\n".join([f"{idx + 1}. {row}" for idx, row in enumerate(rows[:3])])
        else:
            memory_text = "NO_MATCH"

        user_text = (
            f"题材名称: {topic_name}\n"
            "最近24小时新闻摘要如下：\n"
            f"{merged_news_text}\n"
            "请结合 MEMORY_CONTEXT 进行稳健抽取，再按约束输出 JSON。"
        )

        return (
            f"[SYSTEM]\n{system_text}\n\n"
            f"[RULES]\n{schema_text}\n\n"
            f"[MEMORY_CONTEXT]\n{memory_text}\n\n"
            f"[USER]\n{user_text}"
        )
