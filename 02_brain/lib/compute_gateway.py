#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
02_brain/lib/compute_gateway.py
Daemon-aligned compute gateway for local Ollama calls.
"""

from __future__ import annotations

import os
import time
import fcntl
import logging
import threading
from contextlib import contextmanager
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import requests
import psutil


class ComputeGatewayError(RuntimeError):
    """Base compute gateway exception."""


class ComputeSlotAcquireError(ComputeGatewayError):
    """Raised when compute slot cannot be acquired in time."""


class ComputeRequestError(ComputeGatewayError):
    """Raised when downstream compute request fails."""


class ComputeGateway:
    """
    Stable gateway for local Ollama calls.

    - Keep timeout budget delegated by caller (do not override).
    - Align concurrency budget with daemon semaphore (default: 3).
    - Emit unified millisecond-level runtime logs.
    """

    _local_lock = threading.Lock()
    _local_slots: Dict[int, threading.BoundedSemaphore] = {}
    LOW_RAM_BYTES = 2 * 1024 * 1024 * 1024
    DEFAULT_NUM_THREAD = 2
    THREAD_BY_HOST = {
        "192.0.2.20": 3,  # compute node
        "192.0.2.10": 2,  # local node
    }
    GPU_TOPLEVEL_KEYS = {
        "num_gpu",
        "gpu_layers",
        "main_gpu",
        "tensor_split",
        "use_gpu",
        "cuda",
        "device",
    }
    GPU_OPTION_KEYS = {
        "num_gpu",
        "gpu_layers",
        "main_gpu",
        "tensor_split",
        "low_vram",
        "f16_kv",
        "flash_attn",
        "use_mmap",
        "use_mlock",
        "device",
    }
    PRIMARY_OLLAMA_BASE = os.getenv("OLLAMA_PRIMARY_BASE_URL", "http://192.0.2.20:11434").rstrip("/")
    BACKUP_OLLAMA_BASE = os.getenv("OLLAMA_BACKUP_BASE_URL", "http://192.0.2.21:11434").rstrip("/")
    FAILOVER_THRESHOLD = max(1, int(os.getenv("OLLAMA_FAILOVER_THRESHOLD", "3")))
    _route_lock = threading.Lock()
    _host_failures: Dict[str, int] = {}
    _active_ollama_base: Optional[str] = None

    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        max_slots: int = 3,
        acquire_timeout: float = 60.0,
        slot_prefix: str = "/tmp/zhulong_ollama_slot",
    ):
        self.logger = logger or logging.getLogger("zhulong.compute_gateway")
        self.max_slots = max(1, int(max_slots))
        self.acquire_timeout = max(1.0, float(acquire_timeout))
        self.slot_prefix = slot_prefix
        self._thread_sem = self._get_local_semaphore(self.max_slots)

    @staticmethod
    def _available_ram_bytes() -> Optional[int]:
        try:
            return int(psutil.virtual_memory().available)
        except Exception:
            return None

    def _resolve_num_thread(self, url: str) -> int:
        try:
            host = (urlparse(url).hostname or "").strip()
        except Exception:
            host = ""
        return int(self.THREAD_BY_HOST.get(host, self.DEFAULT_NUM_THREAD))

    @classmethod
    def _host_key(cls, url: str) -> str:
        parsed = urlparse(str(url or ""))
        host = (parsed.hostname or "").strip()
        port = parsed.port
        if not host:
            return ""
        return f"{host}:{port}" if port else host

    @classmethod
    def _replace_base(cls, url: str, base: str) -> str:
        parsed = urlparse(str(url or ""))
        path = parsed.path or ""
        query = f"?{parsed.query}" if parsed.query else ""
        return f"{str(base or '').rstrip('/')}{path}{query}"

    @classmethod
    def _record_primary_failure(cls, logger: logging.Logger, reason: str) -> None:
        primary_key = cls._host_key(cls.PRIMARY_OLLAMA_BASE)
        backup_key = cls._host_key(cls.BACKUP_OLLAMA_BASE)
        if not primary_key or not backup_key:
            return
        with cls._route_lock:
            cls._host_failures[primary_key] = cls._host_failures.get(primary_key, 0) + 1
            failure_count = cls._host_failures[primary_key]
            if failure_count < cls.FAILOVER_THRESHOLD:
                return
            if cls._active_ollama_base == cls.BACKUP_OLLAMA_BASE:
                return
            cls._active_ollama_base = cls.BACKUP_OLLAMA_BASE
            os.environ["OLLAMA_BASE_URL"] = cls.BACKUP_OLLAMA_BASE
            os.environ["OLLAMA_URL"] = f"{cls.BACKUP_OLLAMA_BASE}/api/generate"
        logger.critical(
            "[GATE] [ComputeGateway] failover OPEN primary=%s backup=%s threshold=%s reason=%s",
            cls.PRIMARY_OLLAMA_BASE,
            cls.BACKUP_OLLAMA_BASE,
            cls.FAILOVER_THRESHOLD,
            reason,
        )

    @classmethod
    def _clear_primary_failures(cls, url: str) -> None:
        if cls._host_key(url) != cls._host_key(cls.PRIMARY_OLLAMA_BASE):
            return
        with cls._route_lock:
            cls._host_failures[cls._host_key(cls.PRIMARY_OLLAMA_BASE)] = 0

    @classmethod
    def resolve_url(cls, url: str) -> str:
        if cls._host_key(url) != cls._host_key(cls.PRIMARY_OLLAMA_BASE):
            return url
        with cls._route_lock:
            active_base = cls._active_ollama_base
        if active_base:
            return cls._replace_base(url, active_base)
        return url

    @classmethod
    def resolve_server(cls, server: str) -> str:
        server_text = str(server or "").rstrip("/")
        probe_url = f"{server_text}/api/generate"
        resolved_url = cls.resolve_url(probe_url)
        return resolved_url.rsplit("/api/generate", 1)[0]

    def _normalize_payload(
        self, payload: Dict[str, Any], url: str
    ) -> tuple[Dict[str, Any], bool, Optional[int]]:
        normalized = dict(payload or {})
        options_raw = normalized.get("options")
        options = dict(options_raw) if isinstance(options_raw, dict) else {}

        for key in self.GPU_TOPLEVEL_KEYS:
            normalized.pop(key, None)
        for key in self.GPU_OPTION_KEYS:
            options.pop(key, None)

        options["num_thread"] = self._resolve_num_thread(url)

        available_ram = self._available_ram_bytes()
        degraded = bool(available_ram is not None and available_ram < self.LOW_RAM_BYTES)
        if degraded:
            np_raw = options.get("num_predict")
            if isinstance(np_raw, (int, float)):
                options["num_predict"] = max(32, min(int(np_raw), 128))
            else:
                options["num_predict"] = 128
            normalized["keep_alive"] = 0

        normalized["options"] = options
        return normalized, degraded, available_ram

    @classmethod
    def _get_local_semaphore(cls, max_slots: int) -> threading.BoundedSemaphore:
        with cls._local_lock:
            sem = cls._local_slots.get(max_slots)
            if sem is None:
                sem = threading.BoundedSemaphore(max_slots)
                cls._local_slots[max_slots] = sem
            return sem

    @contextmanager
    def _acquire_slot(self, op: str):
        if not self._thread_sem.acquire(timeout=self.acquire_timeout):
            raise ComputeSlotAcquireError(
                f"local compute semaphore timeout ({self.acquire_timeout}s): {op}"
            )

        lock_fd = None
        slot_id = -1
        deadline = time.time() + self.acquire_timeout
        try:
            while time.time() < deadline:
                for idx in range(self.max_slots):
                    path = f"{self.slot_prefix}_{idx}.lock"
                    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o666)
                    try:
                        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        lock_fd = fd
                        slot_id = idx
                        break
                    except BlockingIOError:
                        os.close(fd)
                        continue
                if lock_fd is not None:
                    break
                time.sleep(0.05)

            if lock_fd is None:
                raise ComputeSlotAcquireError(
                    f"daemon-aligned slot acquire timeout ({self.acquire_timeout}s): {op}"
                )
            yield slot_id
        finally:
            if lock_fd is not None:
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_UN)
                finally:
                    os.close(lock_fd)
            self._thread_sem.release()

    def _log(self, msg: str, *, layer: str = "GATE", level: str = "info"):
        fn = getattr(self.logger, level, self.logger.info)
        try:
            fn(msg, extra={"layer": layer})
        except Exception:
            fn(msg)
    @staticmethod
    def is_timeout_error(exc: Exception) -> bool:
        if isinstance(exc, requests.Timeout):
            return True
        cause = getattr(exc, "__cause__", None)
        if isinstance(cause, requests.Timeout):
            return True
        return "Read timed out" in str(exc)

    @staticmethod
    def create_openai_client(*, api_key: str, base_url: str):
        try:
            import openai
            factory = getattr(openai, "OpenAI")
            return factory(api_key=api_key, base_url=base_url)
        except Exception as exc:
            raise ComputeRequestError(f"openai client init failed: {exc}") from exc

    def http_request(
        self,
        method: str,
        url: str,
        timeout: int | float,
        *,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        json_payload: Optional[Dict[str, Any]] = None,
        data: Any = None,
        proxies: Optional[Dict[str, str]] = None,
        layer: str = "NET",
        decision_id: str = "",
    ) -> requests.Response:
        method_u = str(method or "GET").upper()
        t0 = time.perf_counter()
        try:
            resp = requests.request(
                method_u,
                str(url),
                headers=headers,
                params=params,
                json=json_payload,
                data=data,
                timeout=timeout,
                proxies=proxies,
            )
            elapsed_ms = (time.perf_counter() - t0) * 1000
            self._log(
                f"[{layer}] [ComputeGateway] HTTP {method_u} status={resp.status_code} "
                f"timeout={timeout}s elapsed={elapsed_ms:.1f}ms decision={decision_id} url={url}",
                layer=layer,
            )
            return resp
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - t0) * 1000
            self._log(
                f"[{layer}] [ComputeGateway] HTTP {method_u} FAIL timeout={timeout}s "
                f"elapsed={elapsed_ms:.1f}ms decision={decision_id} url={url} err={exc}",
                layer=layer,
                level="warning",
            )
            raise ComputeRequestError(
                f"http request failed ({method_u}, timeout={timeout}s, url={url}): {exc}"
            ) from exc

    def http_get(
        self,
        url: str,
        timeout: int | float,
        *,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        proxies: Optional[Dict[str, str]] = None,
        layer: str = "NET",
        decision_id: str = "",
    ) -> requests.Response:
        return self.http_request(
            "GET",
            url,
            timeout,
            headers=headers,
            params=params,
            proxies=proxies,
            layer=layer,
            decision_id=decision_id,
        )

    def http_post(
        self,
        url: str,
        timeout: int | float,
        *,
        headers: Optional[Dict[str, str]] = None,
        json_payload: Optional[Dict[str, Any]] = None,
        data: Any = None,
        proxies: Optional[Dict[str, str]] = None,
        layer: str = "NET",
        decision_id: str = "",
    ) -> requests.Response:
        return self.http_request(
            "POST",
            url,
            timeout,
            headers=headers,
            json_payload=json_payload,
            data=data,
            proxies=proxies,
            layer=layer,
            decision_id=decision_id,
        )

    def http_head(
        self,
        url: str,
        timeout: int | float,
        *,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        proxies: Optional[Dict[str, str]] = None,
        layer: str = "NET",
        decision_id: str = "",
    ) -> requests.Response:
        return self.http_request(
            "HEAD",
            url,
            timeout,
            headers=headers,
            params=params,
            proxies=proxies,
            layer=layer,
            decision_id=decision_id,
        )



    def post_json(
        self,
        url: str,
        payload: Dict[str, Any],
        timeout: int | float,
        *,
        layer: str = "GATE",
        decision_id: str = "",
    ) -> requests.Response:
        resolved_url = self.resolve_url(url)
        normalized_payload, degraded, available_ram = self._normalize_payload(payload, resolved_url)
        op_name = normalized_payload.get("model", "unknown-model")
        with self._acquire_slot(op_name) as slot_id:
            t0 = time.perf_counter()
            try:
                if degraded:
                    ram_gb = (available_ram / (1024 ** 3)) if available_ram is not None else -1
                    self._log(
                        f"[{layer}] [ComputeGateway] LOW_RAM degrade activated "
                        f"available={ram_gb:.2f}GB threshold=2.00GB model={op_name}",
                        layer=layer,
                        level="warning",
                    )
                resp = requests.post(resolved_url, json=normalized_payload, timeout=timeout)
                elapsed_ms = (time.perf_counter() - t0) * 1000
                ram_gb = (available_ram / (1024 ** 3)) if available_ram is not None else -1
                self._log(
                    f"[{layer}] [ComputeGateway] model={op_name} status={resp.status_code} "
                    f"slot={slot_id} timeout={timeout}s elapsed={elapsed_ms:.1f}ms "
                    f"ram_avail={ram_gb:.2f}GB degraded={int(degraded)} decision={decision_id}",
                    layer=layer,
                )
                if resp.status_code >= 500:
                    self._record_primary_failure(
                        self.logger,
                        f"HTTP_{resp.status_code}@{self._host_key(resolved_url)}",
                    )
                elif resp.status_code < 500:
                    self._clear_primary_failures(resolved_url)
                return resp
            except Exception as exc:
                elapsed_ms = (time.perf_counter() - t0) * 1000
                ram_gb = (available_ram / (1024 ** 3)) if available_ram is not None else -1
                self._log(
                    f"[{layer}] [ComputeGateway] model={op_name} FAIL "
                    f"slot={slot_id} timeout={timeout}s elapsed={elapsed_ms:.1f}ms "
                    f"ram_avail={ram_gb:.2f}GB degraded={int(degraded)} "
                    f"decision={decision_id} err={exc}",
                    layer=layer,
                    level="warning",
                )
                if isinstance(exc, requests.ConnectionError):
                    self._record_primary_failure(
                        self.logger,
                        f"CONNECTION_ERROR@{self._host_key(resolved_url)}:{exc}",
                    )
                raise ComputeRequestError(
                    f"compute request failed ({op_name}, timeout={timeout}s): {exc}"
                ) from exc

    def ollama_generate(
        self,
        server: str,
        payload: Dict[str, Any],
        timeout: int | float,
        *,
        layer: str = "GATE",
        decision_id: str = "",
    ) -> requests.Response:
        endpoint = f"{server.rstrip('/')}/api/generate"
        return self.post_json(
            endpoint,
            payload,
            timeout,
            layer=layer,
            decision_id=decision_id,
        )

    def ollama_embeddings(
        self,
        server: str,
        payload: Dict[str, Any],
        timeout: int | float,
        *,
        layer: str = "RAG",
        decision_id: str = "",
    ) -> requests.Response:
        endpoint = f"{server.rstrip('/')}/api/embeddings"
        return self.post_json(
            endpoint,
            payload,
            timeout,
            layer=layer,
            decision_id=decision_id,
        )

    def unload_model(
        self,
        server: str,
        model_name: str,
        timeout: int | float = 10,
        *,
        layer: str = "GATE",
        decision_id: str = "",
    ) -> requests.Response:
        payload = {"model": model_name, "prompt": "", "keep_alive": 0}
        return self.ollama_generate(
            server=server,
            payload=payload,
            timeout=timeout,
            layer=layer,
            decision_id=decision_id or f"unload:{model_name}",
        )
