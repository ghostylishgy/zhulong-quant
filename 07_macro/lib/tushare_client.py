#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
07_macro/lib/tushare_client.py
Macro Tushare client:
1) Reuse global TushareBridge instance.
2) Enforce timeout + retry(backoff+jitter).
3) Provide REST DNS fallback path when SDK path fails.
"""

from __future__ import annotations

import importlib.util
import runpy
import logging
import os
import random
import socket
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from pathlib import Path
from typing import Any, Callable, Dict, Optional
from urllib.parse import urlparse, urlunparse

import pandas as pd

from config.settings import Config

logger = logging.getLogger("zhulong.macro.tushare_client")

_current = Path(__file__).resolve()
PROJECT_ROOT = next(
    (p for p in _current.parents if (p / ".git").exists() or (p / "storage").exists()),
    _current.parents[2],
)


_loader_ns = runpy.run_path(str(PROJECT_ROOT / "04_governance" / "lib" / "core" / "module_loader.py"))
load_attr_from_path = _loader_ns["load_attr_from_path"]
load_module_from_path = _loader_ns["load_module_from_path"]
ComputeGateway = load_attr_from_path(
    "compute_gateway_macro_tushare_client",
    PROJECT_ROOT / "02_brain" / "lib" / "compute_gateway.py",
    "ComputeGateway",
)
COMPUTE_GATEWAY = ComputeGateway(logger=logger, max_slots=2)


def _load_bridge_getter() -> Callable[[], Any]:
    bridge_mod = load_module_from_path(
        "tushare_bridge_01_macro",
        PROJECT_ROOT / "01_engine" / "lib" / "tushare_bridge.py",
    )
    return bridge_mod.get_bridge


class TushareClientError(RuntimeError):
    """Macro Tushare client error."""


def _normalize_trade_date(trade_date: str) -> str:
    val = str(trade_date or "").strip()
    if not val:
        raise ValueError("trade_date is required")
    if len(val) == 8 and val.isdigit():
        return val
    if len(val) >= 10 and val[4] == "-" and val[7] == "-":
        return f"{val[0:4]}{val[5:7]}{val[8:10]}"
    raise ValueError(f"Unsupported trade_date format: {trade_date}")


def _normalize_news_datetime(dt_value: str) -> str:
    text = str(dt_value or "").strip()
    if not text:
        raise ValueError("news datetime is required")

    if len(text) == 19 and text[4] == "-" and text[7] == "-":
        return text
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return f"{text[:10]} 00:00:00"
    if len(text) == 8 and text.isdigit():
        return f"{text[0:4]}-{text[4:6]}-{text[6:8]} 00:00:00"
    raise ValueError(f"Unsupported news datetime format: {dt_value}")


def _is_dns_error(exc: Exception) -> bool:
    text = str(exc).lower()
    markers = (
        "name resolution",
        "name or service not known",
        "temporary failure in name resolution",
        "failed to resolve",
        "nodename nor servname",
    )
    return any(marker in text for marker in markers)


class MacroTushareClient:
    """Resilient Tushare API wrapper for Macro V1/V2."""

    def __init__(
        self,
        timeout_sec: int = 30,
        max_retries: int = 4,
        base_delay: float = 1.0,
        jitter_max: float = 0.6,
    ):
        self.timeout_sec = max(5, int(timeout_sec))
        self.max_retries = max(1, int(max_retries))
        self.base_delay = max(0.2, float(base_delay))
        self.jitter_max = max(0.0, float(jitter_max))

        bridge_getter = _load_bridge_getter()
        self.bridge = bridge_getter()
        self.api = getattr(self.bridge, "api", None)
        self.available = bool(getattr(self.bridge, "available", False) and self.api is not None)

        self.token = str(getattr(Config, "TUSHARE_TOKEN", "") or "").strip()
        self.api_url = str(os.getenv("TUSHARE_API_URL", "http://api.waditu.com")).strip()
        self.api_ip = str(os.getenv("TUSHARE_API_IP", "")).strip()

    def get_moneyflow(self, trade_date: str) -> pd.DataFrame:
        return self.call("moneyflow", params={"trade_date": _normalize_trade_date(trade_date)})

    def get_limit_list_d(self, trade_date: str) -> pd.DataFrame:
        return self.call("limit_list_d", params={"trade_date": _normalize_trade_date(trade_date)})

    def get_news_sina(self, start_date: str, end_date: str) -> pd.DataFrame:
        return self.call(
            "news",
            params={
                "src": "sina",
                "start_date": _normalize_news_datetime(start_date),
                "end_date": _normalize_news_datetime(end_date),
            },
        )

    def get_cctv_news(self, start_date: str, end_date: str) -> pd.DataFrame:
        return self.call(
            "cctv_news",
            params={
                "start_date": _normalize_trade_date(start_date),
                "end_date": _normalize_trade_date(end_date),
            },
        )

    def get_concept_list(self) -> pd.DataFrame:
        return self.call("concept", params={})

    def get_concept_detail(self, concept_id: str) -> pd.DataFrame:
        return self.call("concept_detail", params={"id": str(concept_id or "").strip()})

    def call(
        self,
        api_name: str,
        params: Optional[Dict[str, Any]] = None,
        fields: str = "",
    ) -> pd.DataFrame:
        params = dict(params or {})
        last_err: Optional[Exception] = None

        for attempt in range(1, self.max_retries + 1):
            try:
                if self.available:
                    try:
                        return self._call_via_bridge(api_name, params=params, fields=fields)
                    except Exception as bridge_exc:
                        logger.warning(
                            "Bridge call failed api=%s attempt=%s/%s: %s",
                            api_name,
                            attempt,
                            self.max_retries,
                            bridge_exc,
                        )
                return self._call_via_rest(api_name, params=params, fields=fields)
            except Exception as exc:
                last_err = exc
                if attempt >= self.max_retries:
                    break
                delay = self.base_delay * (2 ** (attempt - 1)) + random.uniform(0, self.jitter_max)
                logger.warning(
                    "Tushare retry api=%s attempt=%s/%s delay=%.2fs err=%s",
                    api_name,
                    attempt,
                    self.max_retries,
                    delay,
                    exc,
                )
                time.sleep(delay)

        raise TushareClientError(
            f"Tushare call failed api={api_name} retries={self.max_retries}: {last_err}"
        ) from last_err

    def _call_via_bridge(
        self,
        api_name: str,
        params: Dict[str, Any],
        fields: str,
    ) -> pd.DataFrame:
        if self.api is None:
            raise TushareClientError("Tushare bridge API is unavailable")
        if not hasattr(self.api, api_name):
            raise TushareClientError(f"Tushare bridge missing api: {api_name}")

        fn = getattr(self.api, api_name)
        call_kwargs = dict(params)
        if fields:
            call_kwargs["fields"] = fields

        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(fn, **call_kwargs)
            try:
                df = future.result(timeout=self.timeout_sec)
            except FuturesTimeout as exc:
                raise TimeoutError(f"Tushare bridge timeout api={api_name} timeout={self.timeout_sec}s") from exc

        if df is None:
            return pd.DataFrame()
        if isinstance(df, pd.DataFrame):
            return df
        raise TushareClientError(f"Tushare bridge returned non-DataFrame api={api_name}: {type(df)}")

    def _call_via_rest(
        self,
        api_name: str,
        params: Dict[str, Any],
        fields: str,
    ) -> pd.DataFrame:
        if not self.token:
            raise TushareClientError("Config.TUSHARE_TOKEN is empty, REST fallback unavailable")

        payload = {
            "api_name": api_name,
            "token": self.token,
            "params": params,
            "fields": fields or "",
        }
        response = self._post_with_dns_fallback(self.api_url, payload)
        body = response.json() if response.content else {}

        code = int(body.get("code", -1))
        if code != 0:
            msg = str(body.get("msg", "") or "")
            raise TushareClientError(f"Tushare REST error api={api_name} code={code} msg={msg}")

        data = body.get("data", {}) or {}
        columns = data.get("fields", []) or []
        items = data.get("items", []) or []
        return pd.DataFrame(items, columns=columns)

    def _post_with_dns_fallback(self, url: str, payload: Dict[str, Any]):
        try:
            response = COMPUTE_GATEWAY.http_post(
                url,
                timeout=self.timeout_sec,
                json_payload=payload,
                layer="MACRO",
                decision_id="tushare_primary",
            )
            response.raise_for_status()
            return response
        except Exception as exc:
            if not _is_dns_error(exc):
                raise
            parsed = urlparse(url)
            host = parsed.hostname or ""
            ip = self._resolve_host_with_secondary_dns(host)
            if not ip:
                raise

            netloc = parsed.netloc.replace(host, ip)
            fallback_url = urlunparse(
                (
                    parsed.scheme,
                    netloc,
                    parsed.path,
                    parsed.params,
                    parsed.query,
                    parsed.fragment,
                )
            )
            headers = {"Host": host}
            logger.warning("Tushare DNS fallback host=%s ip=%s", host, ip)
            response = COMPUTE_GATEWAY.http_post(
                fallback_url,
                timeout=self.timeout_sec,
                headers=headers,
                json_payload=payload,
                layer="MACRO",
                decision_id="tushare_dns_fallback",
            )
            response.raise_for_status()
            return response

    def _resolve_host_with_secondary_dns(self, host: str) -> Optional[str]:
        if not host:
            return None
        if self.api_ip:
            return self.api_ip

        try:
            import dns.resolver  # type: ignore

            resolver = dns.resolver.Resolver(configure=False)
            resolver.nameservers = ["8.8.8.8", "1.1.1.1"]
            answer = resolver.resolve(host, "A", lifetime=3)
            for item in answer:
                ip = str(item)
                if ip:
                    return ip
        except Exception as exc:
            logger.error("Non-fatal: fallback resolution failed: %s", exc, exc_info=True)

        try:
            return socket.gethostbyname(host)
        except Exception:
            return None
