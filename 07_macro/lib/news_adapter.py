#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
07_macro/lib/news_adapter.py
Macro V2 news adapter with dual-source fusion + aggressive token-juicing.

Hard rules:
1. Only use pro.news(src='sina') + pro.cctv_news() in past 24h window.
2. Match topic by topic_name keywords against title/content (contains).
3. Drop short body (< 50 chars).
4. De-duplicate near-identical titles via SequenceMatcher.
5. Keep at most top 3 high-quality news and hard-truncate merged corpus to 1500 chars.
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from typing import Callable, Dict, Iterable, List, Optional

logger = logging.getLogger("zhulong.macro.news_adapter")


@dataclass
class NewsItem:
    topic_type: str
    topic_id: str
    topic_name: str
    title: str
    body: str
    source: str = ""
    published_at: str = ""
    url: str = ""


@dataclass
class NewsDigest:
    topic_type: str
    topic_id: str
    topic_name: str
    raw_count: int
    kept_after_noise: int
    dedup_dropped: int
    selected_count: int
    merged_text: str
    merged_chars: int
    selected_items: List[Dict[str, str]]


class MacroNewsAdapter:
    """Prepare compact topic-news corpus for Fin-R1 classification."""

    def __init__(
        self,
        min_body_chars: int = 50,
        max_news_items: int = 3,
        title_similarity_threshold: float = 0.88,
        max_prompt_chars: int = 1500,
        tushare_client: Optional[object] = None,
    ):
        self.min_body_chars = max(1, int(min_body_chars))
        self.max_news_items = max(1, int(max_news_items))
        self.title_similarity_threshold = float(title_similarity_threshold)
        self.max_prompt_chars = max(200, int(max_prompt_chars))
        self.tushare_client = tushare_client

    def build_for_top_topics(
        self,
        top_topics: Iterable[Dict[str, object]],
        fetch_news: Callable[[str, str, str], List[Dict[str, object]]],
        top_topic_limit: int = 5,
    ) -> List[NewsDigest]:
        """
        Build compact news digests for top-N topics.

        `fetch_news(topic_type, topic_id, topic_name)` should return list of raw news dict.
        """
        digests: List[NewsDigest] = []
        for row in list(top_topics)[: max(1, int(top_topic_limit))]:
            topic_type = str(row.get("topic_type", "") or "")
            topic_id = str(row.get("topic_id", "") or "")
            topic_name = str(row.get("topic_name", topic_id) or topic_id)
            if not topic_type or not topic_id:
                continue

            try:
                raw_news = fetch_news(topic_type, topic_id, topic_name) or []
            except Exception as exc:
                logger.warning(
                    "fetch_news failed topic=%s:%s err=%s",
                    topic_type,
                    topic_id,
                    exc,
                )
                raw_news = []

            digest = self._build_topic_digest(topic_type, topic_id, topic_name, raw_news)
            digests.append(digest)

        return digests

    def build_digest_for_topic(
        self,
        topic_type: str,
        topic_id: str,
        topic_name: str,
        raw_news: List[Dict[str, object]],
    ) -> NewsDigest:
        return self._build_topic_digest(topic_type, topic_id, topic_name, raw_news)

    def fetch_news_dual_source(
        self,
        topic_type: str,
        topic_id: str,
        topic_name: str,
        now: Optional[datetime] = None,
    ) -> List[Dict[str, object]]:
        """
        Pull only Sina + CCTV news in past 24h, then filter by topic_name keywords.
        Returns [] on any failure or empty-match (no exception bubble-up by design).
        """
        if self.tushare_client is None:
            logger.warning(
                "news_adapter fetch skipped: tushare_client missing topic=%s:%s",
                topic_type,
                topic_id,
            )
            return []

        end_dt = now or datetime.now()
        start_dt = end_dt - timedelta(hours=24)
        start_dt_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
        end_dt_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")
        start_day = start_dt.strftime("%Y%m%d")
        end_day = end_dt.strftime("%Y%m%d")

        logger.info(
            "macro_v2 news fetch start topic=%s window=%s~%s",
            topic_name,
            start_dt_str,
            end_dt_str,
        )

        sina_rows = self._fetch_sina_rows(start_dt_str, end_dt_str, topic_name)
        cctv_rows = self._fetch_cctv_rows(start_day, end_day, topic_name)

        merged = sina_rows + cctv_rows
        matched = self._filter_rows_by_topic_name(merged, topic_name)

        logger.info(
            "macro_v2 news fused topic=%s raw=%s matched=%s",
            topic_name,
            len(merged),
            len(matched),
        )

        if not matched:
            # Hard contract: return [] instead of raising.
            return []
        return matched

    def _fetch_sina_rows(self, start_dt: str, end_dt: str, topic_name: str) -> List[Dict[str, object]]:
        t0 = time.perf_counter()
        try:
            df = self.tushare_client.get_news_sina(start_dt, end_dt)
            elapsed = time.perf_counter() - t0
            rows = 0 if df is None else int(len(df))
            if elapsed >= 6.0:
                logger.warning(
                    "macro_v2 news sina slow topic=%s elapsed=%.3fs rows=%s",
                    topic_name,
                    elapsed,
                    rows,
                )
            else:
                logger.info(
                    "macro_v2 news sina ok topic=%s elapsed=%.3fs rows=%s",
                    topic_name,
                    elapsed,
                    rows,
                )
            if df is None or df.empty:
                return []

            out: List[Dict[str, object]] = []
            for row in df.itertuples(index=False):
                out.append(
                    {
                        "title": getattr(row, "title", "") or "",
                        "content": getattr(row, "content", "") or "",
                        "source": "sina",
                        "published_at": getattr(row, "datetime", "") or "",
                        "url": "",
                    }
                )
            return out
        except Exception as exc:
            elapsed = time.perf_counter() - t0
            logger.warning(
                "macro_v2 news sina failed topic=%s elapsed=%.3fs err=%s",
                topic_name,
                elapsed,
                exc,
            )
            return []

    def _fetch_cctv_rows(self, start_day: str, end_day: str, topic_name: str) -> List[Dict[str, object]]:
        t0 = time.perf_counter()
        try:
            df = self.tushare_client.get_cctv_news(start_day, end_day)
            elapsed = time.perf_counter() - t0
            rows = 0 if df is None else int(len(df))
            if elapsed >= 6.0:
                logger.warning(
                    "macro_v2 news cctv slow topic=%s elapsed=%.3fs rows=%s",
                    topic_name,
                    elapsed,
                    rows,
                )
            else:
                logger.info(
                    "macro_v2 news cctv ok topic=%s elapsed=%.3fs rows=%s",
                    topic_name,
                    elapsed,
                    rows,
                )
            if df is None or df.empty:
                return []

            out: List[Dict[str, object]] = []
            for row in df.itertuples(index=False):
                out.append(
                    {
                        "title": getattr(row, "title", "") or "",
                        "content": getattr(row, "content", "") or "",
                        "source": "cctv",
                        "published_at": getattr(row, "date", "") or "",
                        "url": "",
                    }
                )
            return out
        except Exception as exc:
            elapsed = time.perf_counter() - t0
            logger.warning(
                "macro_v2 news cctv failed topic=%s elapsed=%.3fs err=%s",
                topic_name,
                elapsed,
                exc,
            )
            return []

    def _filter_rows_by_topic_name(self, rows: List[Dict[str, object]], topic_name: str) -> List[Dict[str, object]]:
        if not rows:
            return []

        keywords = self._extract_topic_keywords(topic_name)
        if not keywords:
            return rows

        matched: List[Dict[str, object]] = []
        for row in rows:
            title = self._normalize_text(row.get("title", ""))
            body = self._normalize_text(row.get("content", ""))
            haystack = f"{title} {body}".lower()
            if not haystack:
                continue
            if any(kw in haystack for kw in keywords):
                matched.append(row)
        return matched

    @staticmethod
    def _extract_topic_keywords(topic_name: str) -> List[str]:
        raw = MacroNewsAdapter._normalize_text(topic_name)
        if not raw:
            return []

        kws: List[str] = [raw.lower()]
        for token in re.split(r"[\s,，、/|+&()（）\-]+", raw):
            t = token.strip().lower()
            if not t:
                continue
            if len(t) >= 2:
                kws.append(t)

        uniq = []
        seen = set()
        for k in kws:
            if k in seen:
                continue
            seen.add(k)
            uniq.append(k)
        return uniq

    def _build_topic_digest(
        self,
        topic_type: str,
        topic_id: str,
        topic_name: str,
        raw_news: List[Dict[str, object]],
    ) -> NewsDigest:
        normalized = [self._normalize_news_item(topic_type, topic_id, topic_name, x) for x in raw_news]
        normalized = [x for x in normalized if x is not None]

        after_noise = [x for x in normalized if len(x.body) >= self.min_body_chars]
        deduped, dedup_dropped = self._dedupe_by_title(after_noise)
        selected = deduped[: self.max_news_items]
        merged_text = self._merge_and_truncate(selected)

        return NewsDigest(
            topic_type=topic_type,
            topic_id=topic_id,
            topic_name=topic_name,
            raw_count=len(raw_news),
            kept_after_noise=len(after_noise),
            dedup_dropped=dedup_dropped,
            selected_count=len(selected),
            merged_text=merged_text,
            merged_chars=len(merged_text),
            selected_items=[
                {
                    "title": item.title,
                    "source": item.source,
                    "published_at": item.published_at,
                    "url": item.url,
                    "body_chars": str(len(item.body)),
                }
                for item in selected
            ],
        )

    def _dedupe_by_title(self, news_items: List[NewsItem]) -> tuple[List[NewsItem], int]:
        if not news_items:
            return [], 0

        # High-quality first: longer body wins, then longer title.
        ranked = sorted(news_items, key=lambda x: (len(x.body), len(x.title)), reverse=True)

        kept: List[NewsItem] = []
        dropped = 0
        for cand in ranked:
            is_dup = False
            for prior in kept:
                sim = self._title_similarity(cand.title, prior.title)
                if sim >= self.title_similarity_threshold:
                    is_dup = True
                    dropped += 1
                    break
            if not is_dup:
                kept.append(cand)

        return kept, dropped

    def _merge_and_truncate(self, news_items: List[NewsItem]) -> str:
        if not news_items:
            return ""

        parts = []
        for idx, item in enumerate(news_items, start=1):
            parts.append(f"[{idx}] {item.title}\n{item.body}")

        merged = "\n\n".join(parts)
        if len(merged) > self.max_prompt_chars:
            return merged[: self.max_prompt_chars]
        return merged

    @staticmethod
    def _title_similarity(a: str, b: str) -> float:
        ta = MacroNewsAdapter._normalize_text(a)
        tb = MacroNewsAdapter._normalize_text(b)
        if not ta or not tb:
            return 0.0
        return SequenceMatcher(None, ta, tb).ratio()

    @staticmethod
    def _normalize_news_item(
        topic_type: str,
        topic_id: str,
        topic_name: str,
        raw: Dict[str, object],
    ) -> Optional[NewsItem]:
        if not isinstance(raw, dict):
            return None

        title = MacroNewsAdapter._normalize_text(raw.get("title", ""))
        body = MacroNewsAdapter._normalize_text(
            raw.get("body") or raw.get("content") or raw.get("summary") or ""
        )
        if not title and not body:
            return None

        return NewsItem(
            topic_type=topic_type,
            topic_id=topic_id,
            topic_name=topic_name,
            title=title,
            body=body,
            source=MacroNewsAdapter._normalize_text(raw.get("source", "")),
            published_at=MacroNewsAdapter._normalize_text(raw.get("published_at", "")),
            url=MacroNewsAdapter._normalize_text(raw.get("url", "")),
        )

    @staticmethod
    def _normalize_text(value: object) -> str:
        text = str(value or "")
        text = text.replace("\u3000", " ").replace("\xa0", " ")
        text = re.sub(r"\s+", " ", text)
        return text.strip()
