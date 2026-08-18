#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Read-only Web evidence provider adapters for Zhulong capability probes.

Search hits and fetched pages are intentionally *unverified*.  This module has
no DuckDB, daemon, RAG, Shadow, Nexus, or decision-engine dependency.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import os
import re
import socket
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Callable
from urllib.parse import urlsplit, urlunsplit

ANYSEARCH_SEARCH_URL = "https://api.anysearch.com/v1/search"
FIRECRAWL_SEARCH_URL = "https://api.firecrawl.dev/v2/search"
FIRECRAWL_SCRAPE_URL = "https://api.firecrawl.dev/v2/scrape"

MAX_RESPONSE_BYTES = 2_000_000
MAX_TEXT_CHARS = 4_000
MAX_TITLE_CHARS = 500
MAX_PROVIDER_RESULTS = 5
MAX_PROVIDER_TIMEOUT_SECONDS = 12.0
MAX_QUERY_CHARS = 500

DISCOVERED_UNVERIFIED = "DISCOVERED_UNVERIFIED"
FETCHED_UNVERIFIED = "FETCHED_UNVERIFIED"
REJECTED_AFTER_AS_OF = "REJECTED_AFTER_AS_OF"
REJECTED_UNSAFE_URL = "REJECTED_UNSAFE_URL"


class ProviderError(RuntimeError):
    """A provider failure safe to record without response bodies or secrets."""


Transport = Callable[[str, dict[str, Any], dict[str, str], float], dict[str, Any]]


@dataclass(frozen=True)
class ProviderConfig:
    name: str
    api_key: str = ""
    allow_anonymous: bool = False


def clean_text(value: Any, limit: int = MAX_TEXT_CHARS) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    return text[:limit]


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def validate_provider_bounds(*, timeout: float, limit: int | None = None, query: str | None = None) -> None:
    try:
        bounded_timeout = float(timeout)
    except (TypeError, ValueError):
        raise ProviderError("INVALID_TIMEOUT") from None
    if not 1.0 <= bounded_timeout <= MAX_PROVIDER_TIMEOUT_SECONDS:
        raise ProviderError("TIMEOUT_OUT_OF_BOUNDS")
    if limit is not None:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= MAX_PROVIDER_RESULTS:
            raise ProviderError("RESULT_LIMIT_OUT_OF_BOUNDS")
    if query is not None:
        if not isinstance(query, str) or not query.strip() or len(query.strip()) > MAX_QUERY_CHARS:
            raise ProviderError("QUERY_OUT_OF_BOUNDS")


def normalize_public_url(raw_url: Any) -> str:
    """Return a canonical public HTTP(S) URL or raise ValueError.

    Hostname DNS is deliberately not resolved here: provider-returned URLs are
    recorded, not fetched locally.  Direct fetches are guarded again by the
    provider's own threat controls and this syntactic/private-IP check.
    """

    raw = clean_text(raw_url, 4096)
    if not raw:
        raise ValueError("empty_url")
    parsed = urlsplit(raw)
    if parsed.scheme.lower() not in {"http", "https"}:
        raise ValueError("unsupported_url_scheme")
    if parsed.username or parsed.password:
        raise ValueError("url_credentials_forbidden")
    host = (parsed.hostname or "").strip().lower().rstrip(".")
    if not host:
        raise ValueError("missing_url_host")
    if host == "localhost" or host.endswith(".localhost") or host.endswith(".local"):
        raise ValueError("local_url_host_forbidden")
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        ip = None
    if ip is not None and (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
    ):
        raise ValueError("non_public_ip_forbidden")
    netloc = host
    if parsed.port:
        netloc = f"{host}:{parsed.port}"
    return urlunsplit((parsed.scheme.lower(), netloc, parsed.path or "/", parsed.query, ""))


def parse_reported_date(value: Any) -> date | None:
    text = clean_text(value, 200)
    if not text:
        return None
    candidates = [text, text.replace("Z", "+00:00")]
    for candidate in candidates:
        try:
            return datetime.fromisoformat(candidate).date()
        except ValueError:
            pass
    try:
        return parsedate_to_datetime(text).date()
    except (TypeError, ValueError, OverflowError):
        pass
    match = re.search(r"\b(20\d{2})[-/](\d{1,2})[-/](\d{1,2})\b", text)
    if match:
        try:
            return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        except ValueError:
            return None
    return None


def safe_error(exc: BaseException) -> str:
    if isinstance(exc, urllib.error.HTTPError):
        return f"HTTP_{exc.code}"
    if isinstance(exc, (TimeoutError, socket.timeout)):
        return "TIMEOUT"
    if isinstance(exc, urllib.error.URLError):
        reason = exc.reason
        if isinstance(reason, (TimeoutError, socket.timeout)):
            return "TIMEOUT"
        return "NETWORK_ERROR"
    if isinstance(exc, (json.JSONDecodeError, UnicodeDecodeError)):
        return "INVALID_JSON_RESPONSE"
    if isinstance(exc, ProviderError):
        return clean_text(exc, 200)
    return exc.__class__.__name__.upper()


def post_json(url: str, payload: dict[str, Any], headers: dict[str, str], timeout: float) -> dict[str, Any]:
    validate_provider_bounds(timeout=timeout)
    request = urllib.request.Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json", **headers},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read(MAX_RESPONSE_BYTES + 1)
    except Exception as exc:
        raise ProviderError(safe_error(exc)) from None
    if len(raw) > MAX_RESPONSE_BYTES:
        raise ProviderError("RESPONSE_TOO_LARGE")
    try:
        decoded = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ProviderError(safe_error(exc)) from None
    if not isinstance(decoded, dict):
        raise ProviderError("SCHEMA_INVALID_TOP_LEVEL")
    return decoded


def call_transport(
    transport: Transport,
    url: str,
    payload: dict[str, Any],
    headers: dict[str, str],
    timeout: float,
) -> dict[str, Any]:
    try:
        response = transport(url, payload, headers, timeout)
    except ProviderError:
        raise
    except Exception as exc:
        raise ProviderError(safe_error(exc)) from None
    if not isinstance(response, dict):
        raise ProviderError("SCHEMA_INVALID_TOP_LEVEL")
    return response


def _result_row(
    *,
    provider: str,
    query_id: str,
    title: Any,
    raw_url: Any,
    snippet: Any,
    published_at: Any,
    as_of: date,
    status: str,
    content: Any = "",
    provider_request_id: Any = "",
) -> dict[str, Any]:
    try:
        url = normalize_public_url(raw_url)
        url_warning = ""
    except ValueError as exc:
        url = ""
        url_warning = str(exc)
        status = REJECTED_UNSAFE_URL
    reported_date = parse_reported_date(published_at)
    if reported_date and reported_date > as_of:
        status = REJECTED_AFTER_AS_OF
    raw_body = str(content or "")
    body = clean_text(raw_body)
    if status == REJECTED_AFTER_AS_OF:
        time_binding_status = "REJECTED_AFTER_AS_OF"
    elif reported_date:
        time_binding_status = "PROVIDER_REPORTED_UNVERIFIED"
    else:
        time_binding_status = "UNKNOWN"
    return {
        "provider": provider,
        "query_id": query_id,
        "title": clean_text(title, MAX_TITLE_CHARS),
        "url": url,
        "source_domain": urlsplit(url).hostname or "" if url else "",
        "snippet": clean_text(snippet),
        "content_excerpt": body or None,
        "published_at_reported": clean_text(published_at, 200) or None,
        "published_date_parsed": reported_date.isoformat() if reported_date else None,
        "published_at_verified": False,
        "time_binding_status": time_binding_status,
        "entity_binding_status": "UNVERIFIED",
        "content_sha256": sha256_text(raw_body) if raw_body else None,
        "content_length_chars": len(raw_body),
        "content_trust": "TAINTED_EXTERNAL_UNVERIFIED",
        "prompt_use_allowed": False,
        "retrieved_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "provider_request_id": clean_text(provider_request_id, 200) or None,
        "evidence_status": status,
        "warnings": [url_warning] if url_warning else [],
        "business_relevance_evidence_present": False,
        "manual_review_required": True,
        "generate_task": False,
        "no_trade_signal": True,
    }


def search_anysearch(
    query: str,
    *,
    query_id: str,
    as_of: date,
    limit: int,
    config: ProviderConfig,
    timeout: float,
    transport: Transport = post_json,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    validate_provider_bounds(timeout=timeout, limit=limit, query=query)
    if not config.api_key and not config.allow_anonymous:
        raise ProviderError("KEY_MISSING")
    headers = {"Authorization": f"Bearer {config.api_key}"} if config.api_key else {}
    payload = {"query": query, "max_results": limit}
    response = call_transport(transport, ANYSEARCH_SEARCH_URL, payload, headers, timeout)
    if response.get("success") is False:
        raise ProviderError("UPSTREAM_REPORTED_FAILURE")
    raw_results = response.get("results")
    if raw_results is None:
        raw_results = response.get("data")
    if isinstance(raw_results, dict):
        raw_results = raw_results.get("results") or raw_results.get("web") or []
    if not isinstance(raw_results, list):
        raise ProviderError("SCHEMA_INVALID_RESULTS")
    request_id = response.get("id") or response.get("request_id")
    rows = []
    for raw in raw_results[:limit]:
        if not isinstance(raw, dict):
            continue
        rows.append(_result_row(
            provider="anysearch",
            query_id=query_id,
            title=raw.get("title"),
            raw_url=raw.get("url") or raw.get("link"),
            snippet=raw.get("snippet") or raw.get("description") or raw.get("content"),
            published_at=raw.get("published_at") or raw.get("date"),
            as_of=as_of,
            status=DISCOVERED_UNVERIFIED,
            provider_request_id=request_id,
        ))
    return rows, {"provider_request_id": clean_text(request_id, 200) or None, "raw_count": len(raw_results)}


def search_firecrawl(
    query: str,
    *,
    query_id: str,
    as_of: date,
    limit: int,
    config: ProviderConfig,
    timeout: float,
    transport: Transport = post_json,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    validate_provider_bounds(timeout=timeout, limit=limit, query=query)
    if not config.api_key:
        raise ProviderError("KEY_MISSING")
    response = call_transport(
        transport,
        FIRECRAWL_SEARCH_URL,
        {"query": query, "limit": limit, "sources": ["web"], "safe": True, "timeout": int(timeout * 1000)},
        {"Authorization": f"Bearer {config.api_key}"},
        timeout,
    )
    if response.get("success") is not True:
        raise ProviderError("UPSTREAM_REPORTED_FAILURE")
    data = response.get("data") or {}
    raw_results = data.get("web") if isinstance(data, dict) else None
    if not isinstance(raw_results, list):
        raise ProviderError("SCHEMA_INVALID_RESULTS")
    request_id = response.get("id")
    rows = []
    for raw in raw_results[:limit]:
        if not isinstance(raw, dict):
            continue
        metadata = raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {}
        rows.append(_result_row(
            provider="firecrawl",
            query_id=query_id,
            title=raw.get("title") or metadata.get("title"),
            raw_url=raw.get("url") or metadata.get("sourceURL") or metadata.get("url"),
            snippet=raw.get("description") or metadata.get("description"),
            published_at=metadata.get("publishedTime") or metadata.get("date"),
            as_of=as_of,
            status=DISCOVERED_UNVERIFIED,
            provider_request_id=request_id,
        ))
    return rows, {
        "provider_request_id": clean_text(request_id, 200) or None,
        "raw_count": len(raw_results),
        "credits_used": response.get("creditsUsed"),
        "provider_warning_present": bool(response.get("warning")),
    }


def scrape_firecrawl(
    raw_url: str,
    *,
    query_id: str,
    as_of: date,
    config: ProviderConfig,
    timeout: float,
    transport: Transport = post_json,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    validate_provider_bounds(timeout=timeout)
    if not config.api_key:
        raise ProviderError("KEY_MISSING")
    url = normalize_public_url(raw_url)
    response = call_transport(
        transport,
        FIRECRAWL_SCRAPE_URL,
        {"url": url, "formats": ["markdown"], "onlyMainContent": True, "timeout": int(timeout * 1000)},
        {"Authorization": f"Bearer {config.api_key}"},
        timeout,
    )
    if response.get("success") is not True:
        raise ProviderError("UPSTREAM_REPORTED_FAILURE")
    data = response.get("data")
    if not isinstance(data, dict):
        raise ProviderError("SCHEMA_INVALID_DATA")
    metadata = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
    content = data.get("markdown") or ""
    request_id = response.get("id")
    row = _result_row(
        provider="firecrawl",
        query_id=query_id,
        title=metadata.get("title"),
        raw_url=metadata.get("sourceURL") or metadata.get("url") or url,
        snippet=metadata.get("description"),
        published_at=metadata.get("publishedTime") or metadata.get("date"),
        as_of=as_of,
        status=FETCHED_UNVERIFIED,
        content=content,
        provider_request_id=request_id,
    )
    return [row], {
        "provider_request_id": clean_text(request_id, 200) or None,
        "content_chars": len(str(content)),
        "credits_used": response.get("creditsUsed"),
    }


def load_provider_configs() -> dict[str, ProviderConfig]:
    return {
        "anysearch": ProviderConfig(
            name="anysearch",
            api_key=os.getenv("ANYSEARCH_API_KEY", "").strip(),
            allow_anonymous=False,
        ),
        "firecrawl": ProviderConfig(
            name="firecrawl",
            api_key=os.getenv("FIRECRAWL_API_KEY", "").strip(),
            allow_anonymous=False,
        ),
    }
