#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
05_shadow/lib/db_contract.py
Shared DB contract helpers for shadow module.
"""

from __future__ import annotations

import importlib
import importlib.util
import os
from pathlib import Path
from typing import Optional


_current = Path(__file__).resolve()
PROJECT_ROOT = next(
    (p for p in _current.parents if (p / '.git').exists() or (p / 'storage').exists()),
    _current.parents[2],
)
DB_PATH = PROJECT_ROOT / 'storage' / 'database' / 'zhulong.duckdb'


def _load_audit_pass() -> str:
    try:
        mod = importlib.import_module('config.settings')
        return str(getattr(mod, 'AUDIT_PASS', 'PASS')).upper()
    except Exception:
        mod_path = PROJECT_ROOT / 'config' / 'settings.py'
        spec = importlib.util.spec_from_file_location('settings_bridge_shadow', str(mod_path))
        if spec is None or spec.loader is None:
            return 'PASS'
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return str(getattr(mod, 'AUDIT_PASS', 'PASS')).upper()

SIGNAL_VERDICT_PASS = _load_audit_pass()


def _load_l4_pass_threshold() -> int:
    """Load L4_PASS_THRESHOLD from settings to ensure Shadow gate aligns with L4 verdict."""
    try:
        mod = importlib.import_module('config.settings')
        return int(getattr(mod, 'L4_PASS_THRESHOLD', 68))
    except Exception:
        mod_path = PROJECT_ROOT / 'config' / 'settings.py'
        spec = importlib.util.spec_from_file_location('settings_bridge_l4', str(mod_path))
        if spec is None or spec.loader is None:
            return 68  # fallback to default
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return int(getattr(mod, 'L4_PASS_THRESHOLD', 68))


# Shadow入场阈值与L4 PASS阈值对齐，确保所有PASS标的都能进入回测
DEFAULT_MIN_FINAL_SCORE = int(os.getenv('SHADOW_MIN_FINAL_SCORE',
                                         os.getenv('TACTICS_MIN_FINAL_SCORE',
                                                   str(_load_l4_pass_threshold()))))


def _load_dbgateway():
    """Load DBGateway from 01_engine with runtime import fallback."""
    try:
        mod = importlib.import_module('01_engine.lib.db_gateway')
        return mod.DBGateway
    except Exception:
        mod_path = PROJECT_ROOT / '01_engine' / 'lib' / 'db_gateway.py'
        spec = importlib.util.spec_from_file_location('db_gateway_01_shadow', str(mod_path))
        if spec is None or spec.loader is None:
            raise RuntimeError(f'DBGateway loader unavailable: {mod_path}')
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod.DBGateway


DBGateway = _load_dbgateway()


def detect_nexus_score_column(conn) -> Optional[str]:
    """
    Contracted bridge score selector:
    prefer l4_final_score, row-level fallback to l3_audit_score.
    """
    rows = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'nexus_audits'
        """
    ).fetchall()
    cols = {str(r[0]).lower() for r in rows}

    has_l4 = 'l4_final_score' in cols
    has_l3 = 'l3_audit_score' in cols
    if has_l4 and has_l3:
        return 'COALESCE(l4_final_score, l3_audit_score)'
    if has_l4:
        return 'l4_final_score'
    if has_l3:
        return 'l3_audit_score'
    return None
