#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Ollama probe utilities used by daemon phase_flush/phase_audit."""

import logging
import os
import runpy
import subprocess
import threading
import time
from pathlib import Path
from urllib.parse import urlparse

logger = logging.getLogger('zhulong.ollama_probe')

PROJECT_ROOT = Path(__file__).resolve().parents[1]
_loader_ns = runpy.run_path(str(PROJECT_ROOT / "04_governance" / "lib" / "core" / "module_loader.py"))
load_attr_from_path = _loader_ns["load_attr_from_path"]
ComputeGateway = load_attr_from_path(
    "compute_gateway_ollama_probe",
    PROJECT_ROOT / "02_brain" / "lib" / "compute_gateway.py",
    "ComputeGateway",
)
COMPUTE_GATEWAY = ComputeGateway(logger=logger, max_slots=1)

def _best_effort_decision_log(message: str, level: str = 'INFO') -> None:
    """Log through decision_engine.log if available, otherwise local logger."""
    try:
        from decision_engine import log as de_log  # type: ignore
        de_log(message, 'PROBE', level)
        return
    except Exception as exc:
        logger.error("Non-fatal: decision log fallback failed: %s", exc, exc_info=True)

    level_u = str(level or 'INFO').upper()
    if level_u == 'ERROR':
        logger.error(message)
    elif level_u == 'WARNING':
        logger.warning(message)
    else:
        logger.info(message)


def _server_from_env(default: str = 'http://192.0.2.20:11434') -> str:
    host = str(os.getenv('OLLAMA_HOST', '')).strip()
    if host:
        return host.rstrip('/')
    base = str(os.getenv('OLLAMA_BASE_URL', '')).strip()
    if base:
        return base.rstrip('/')
    return default.rstrip('/')


def _ssh_target_from_server(server: str) -> str:
    parsed = urlparse(server if '://' in server else f'http://{server}')
    host = parsed.hostname or '192.0.2.20'
    return f'root@{host}'


def check_ollama_health(server: str | None = None, timeout: int = 5) -> bool:
    server = (server or _server_from_env()).rstrip('/')
    try:
        resp = COMPUTE_GATEWAY.http_get(f'{server}/api/tags', timeout=timeout, layer='PROBE', decision_id='check_ollama_health')
        return resp.status_code == 200
    except Exception:
        return False


def restart_ollama_if_needed(force: bool = False,
                             server: str | None = None,
                             timeout: int = 5,
                             wait_seconds: int = 30) -> bool:
    server = (server or _server_from_env()).rstrip('/')

    if not force and check_ollama_health(server=server, timeout=timeout):
        return True

    _best_effort_decision_log('?? Ollama ???', 'WARNING')

    ssh_target = _ssh_target_from_server(server)
    cmd = ['ssh', ssh_target, 'systemctl', 'restart', 'ollama']

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        if result.returncode != 0:
            _best_effort_decision_log(
                f'Ollama ?????? rc={result.returncode}: {result.stderr.strip()}',
                'ERROR',
            )
            return False

        _best_effort_decision_log(f'?? {wait_seconds}s ??????')
        time.sleep(wait_seconds)

        if check_ollama_health(server=server, timeout=timeout):
            _best_effort_decision_log('Ollama ????')
            return True

        _best_effort_decision_log('Ollama ?????????', 'ERROR')
        return False
    except Exception as exc:
        _best_effort_decision_log(f'Ollama ????: {exc}', 'ERROR')
        return False
