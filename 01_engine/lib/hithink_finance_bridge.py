#!/usr/bin/env python3
"""Bounded read-only client for the HiThink financial data service."""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable

import requests

BEIJING_TZ = timezone(timedelta(hours=8))
DEFAULT_BASE_URL = "https://fuyao.aicubes.cn"
RETRYABLE_CODES = {4001, 5001, 5002, 5003}
BOARD_TAGS = {"cn_concept", "region", "tszs", "industry"}
BOARD_TYPES = {"all", "org", "hot_money"}
A_SHARE_CODE = re.compile(r"^[0-9]{6}\.(SH|SZ|BJ)$")
INDEX_CODE = re.compile(r"^[0-9]{6}\.(SH|SZ|TI)$")
_ALLOWED_ENDPOINTS = {
    "/api/meta/tickers/search",
    "/api/a-share-index/catalog/ths-index-list",
    "/api/a-share-index/constituents/ths-stock-list",
    "/api/a-share-index/prices/snapshot",
    "/api/a-share/auction/snapshot",
    "/api/a-share/auction/short-term-benchmark",
    "/api/a-share/special-data/dragon-tiger-list",
    "/api/a-share/special-data/limit-up-pool",
    "/api/a-share/special-data/limit-break-pool",
    "/api/a-share/special-data/limit-up-ladder",
}


class HithinkBridgeError(RuntimeError):
    """Safe error that never contains credentials or response bodies."""

    def __init__(self, message: str, *, code: int | None = None,
                 request_id: str = "", retryable: bool = False) -> None:
        super().__init__(message)
        self.code = code
        self.request_id = str(request_id or "")
        self.retryable = bool(retryable)


class HithinkCredentialError(HithinkBridgeError):
    """Network access was requested without a configured API key."""


@dataclass(frozen=True)
class HithinkResult:
    endpoint: str
    request_id: str
    retrieved_at: str
    elapsed_ms: int
    source_timestamp: int | None
    content_sha256: str
    data: Any

    def metadata(self) -> dict[str, Any]:
        return {
            "endpoint": self.endpoint,
            "request_id": self.request_id,
            "retrieved_at": self.retrieved_at,
            "elapsed_ms": self.elapsed_ms,
            "source_timestamp": self.source_timestamp,
            "content_sha256": self.content_sha256,
        }


def _canonical_sha256(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True,
                     separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _source_timestamp(data: Any) -> int | None:
    if not isinstance(data, dict):
        return None
    try:
        value = data.get("timestamp")
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _normalise_codes(values: Iterable[str], *, pattern: re.Pattern[str],
                     maximum: int, label: str) -> list[str]:
    output: list[str] = []
    for raw in values:
        value = str(raw or "").strip().upper()
        if not pattern.fullmatch(value):
            raise ValueError(f"invalid {label}: {raw!r}")
        if value not in output:
            output.append(value)
            if len(output) > maximum:
                raise ValueError(f"{label} count must not exceed {maximum}")
    if not output:
        raise ValueError(f"at least one {label} is required")
    return output


def trade_date_ms(value: str) -> int:
    try:
        parsed = date.fromisoformat(str(value))
    except ValueError as exc:
        raise ValueError("trade date must be YYYY-MM-DD") from exc
    midnight = datetime(parsed.year, parsed.month, parsed.day, tzinfo=BEIJING_TZ)
    return int(midnight.timestamp() * 1000)


class HithinkFinanceBridge:
    """Strict REST adapter for bounded external-evidence reads."""

    def __init__(self, *, api_key: str | None = None,
                 base_url: str = DEFAULT_BASE_URL, timeout: float = 8.0,
                 max_retries: int = 2,
                 session: requests.Session | None = None) -> None:
        self._api_key = str(
            api_key if api_key is not None else os.getenv("HITHINK_FINANCE_API_KEY", "")
        ).strip()
        self.base_url = str(base_url).rstrip("/")
        if self.base_url != DEFAULT_BASE_URL:
            raise ValueError("custom HiThink base URL is not allowed")
        if not 1.0 <= float(timeout) <= 15.0:
            raise ValueError("timeout must be between 1 and 15 seconds")
        if not 0 <= int(max_retries) <= 3:
            raise ValueError("max_retries must be between 0 and 3")
        self.timeout = float(timeout)
        self.max_retries = int(max_retries)
        self.session = session or requests.Session()

    @property
    def credential_configured(self) -> bool:
        return bool(self._api_key)

    def _get(self, endpoint: str, params: dict[str, Any]) -> HithinkResult:
        if endpoint not in _ALLOWED_ENDPOINTS:
            raise ValueError(f"endpoint is not allowlisted: {endpoint}")
        if not self._api_key:
            raise HithinkCredentialError("HITHINK_FINANCE_API_KEY is not configured")

        clean_params = {k: v for k, v in params.items() if v is not None}
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            started = time.monotonic()
            try:
                response = self.session.get(
                    f"{self.base_url}{endpoint}",
                    params=clean_params,
                    headers={"X-api-key": self._api_key,
                             "User-Agent": "Zhulong-HiThink-Evidence/0.1"},
                    timeout=self.timeout,
                    allow_redirects=False,
                )
                elapsed_ms = int((time.monotonic() - started) * 1000)
                if 300 <= int(response.status_code) < 400:
                    raise HithinkBridgeError("HiThink redirect response rejected")
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise HithinkBridgeError("HiThink returned a non-object envelope")
                code = int(payload.get("code", -1))
                request_id = str(payload.get("request_id") or "")
                if code == 0:
                    if "data" not in payload:
                        raise HithinkBridgeError(
                            "HiThink success envelope is missing data",
                            request_id=request_id)
                    data = payload.get("data")
                    return HithinkResult(
                        endpoint=endpoint,
                        request_id=request_id,
                        retrieved_at=datetime.now(BEIJING_TZ).isoformat(timespec="seconds"),
                        elapsed_ms=elapsed_ms,
                        source_timestamp=_source_timestamp(data),
                        content_sha256=_canonical_sha256(data),
                        data=data,
                    )
                retryable = code in RETRYABLE_CODES
                error = HithinkBridgeError(
                    f"HiThink business error code={code}", code=code,
                    request_id=request_id, retryable=retryable)
                if retryable and attempt < self.max_retries:
                    last_error = error
                    time.sleep(0.5 * (2**attempt))
                    continue
                raise error
            except HithinkBridgeError:
                raise
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_error = exc
                if attempt < self.max_retries:
                    time.sleep(0.5 * (2**attempt))
                    continue
                raise HithinkBridgeError(
                    "HiThink network request failed", retryable=True) from exc
            except requests.HTTPError as exc:
                raise HithinkBridgeError(
                    "HiThink response validation failed") from exc
            except requests.RequestException as exc:
                raise HithinkBridgeError("HiThink network request failed") from exc
            except (TypeError, ValueError) as exc:
                raise HithinkBridgeError(
                    "HiThink response validation failed") from exc
        raise HithinkBridgeError("HiThink retry budget exhausted") from last_error

    def ticker_search(self, query: str, *, limit: int = 3) -> HithinkResult:
        text = str(query or "").strip()
        if not 1 <= len(text) <= 80:
            raise ValueError("ticker query must contain 1 to 80 characters")
        if any(char in text for char in "\r\n\t"):
            raise ValueError("ticker query contains a control character")
        if not 1 <= int(limit) <= 5:
            raise ValueError("ticker search limit must be between 1 and 5")
        return self._get("/api/meta/tickers/search",
                         {"q": text, "limit": int(limit)})

    def ths_index_list(self, *, tag: str = "cn_concept") -> HithinkResult:
        value = str(tag or "").strip().lower()
        if value not in BOARD_TAGS:
            raise ValueError(f"tag must be one of {sorted(BOARD_TAGS)}")
        return self._get("/api/a-share-index/catalog/ths-index-list",
                         {"tag": value})

    def ths_constituents(self, thscode: str) -> HithinkResult:
        code = _normalise_codes([thscode], pattern=INDEX_CODE, maximum=1,
                                label="index code")[0]
        return self._get("/api/a-share-index/constituents/ths-stock-list",
                         {"thscode": code})

    def index_snapshot(self, thscodes: Iterable[str]) -> HithinkResult:
        codes = _normalise_codes(thscodes, pattern=INDEX_CODE, maximum=5,
                                 label="index code")
        return self._get("/api/a-share-index/prices/snapshot",
                         {"thscodes": ",".join(codes)})

    def auction_snapshot(self, thscodes: Iterable[str],
                         *, stage: str = "final") -> HithinkResult:
        codes = _normalise_codes(thscodes, pattern=A_SHARE_CODE, maximum=5,
                                 label="A-share code")
        value = str(stage or "").strip().lower()
        if value not in {"live", "final"}:
            raise ValueError("auction stage must be live or final")
        return self._get("/api/a-share/auction/snapshot",
                         {"thscodes": ",".join(codes), "stage": value})

    def auction_benchmark(self, trade_date: str) -> HithinkResult:
        date.fromisoformat(str(trade_date))
        return self._get("/api/a-share/auction/short-term-benchmark",
                         {"date": str(trade_date)})

    def dragon_tiger(self, trade_date: str,
                     *, board_type: str = "all") -> HithinkResult:
        date.fromisoformat(str(trade_date))
        value = str(board_type or "").strip().lower()
        if value not in BOARD_TYPES:
            raise ValueError(f"board_type must be one of {sorted(BOARD_TYPES)}")
        return self._get("/api/a-share/special-data/dragon-tiger-list",
                         {"date": str(trade_date), "board_type": value})

    def limit_pool(self, trade_date: str, *, pool: str,
                   size: int = 20) -> HithinkResult:
        endpoint_by_pool = {
            "limit_up": "/api/a-share/special-data/limit-up-pool",
            "limit_break": "/api/a-share/special-data/limit-break-pool",
        }
        if pool not in endpoint_by_pool:
            raise ValueError("pool must be limit_up or limit_break")
        if not 1 <= int(size) <= 20:
            raise ValueError("sidecar pool size must be between 1 and 20")
        sort_field = "last_price" if pool == "limit_up" else "price_change_ratio_pct"
        return self._get(endpoint_by_pool[pool], {
            "date_ms": trade_date_ms(trade_date),
            "page": 1,
            "size": int(size),
            "sort_field": sort_field,
            "sort_dir": "desc",
        })

    def limit_up_ladder(self) -> HithinkResult:
        return self._get("/api/a-share/special-data/limit-up-ladder", {})
