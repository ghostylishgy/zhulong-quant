#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
03_tactics/db_contract.py
Shared DB contract helpers for execution layer.
"""

from __future__ import annotations

import importlib
import runpy
from pathlib import Path
from typing import Optional


_current = Path(__file__).resolve()
PROJECT_ROOT = next(
    (p for p in _current.parents if (p / '.git').exists() or (p / 'storage').exists()),
    _current.parents[2],
)
DB_PATH = PROJECT_ROOT / 'storage' / 'database' / 'zhulong.duckdb'


def _load_dbgateway():
    """Load DBGateway from 01_engine with runtime import fallback."""
    try:
        mod = importlib.import_module('01_engine.lib.db_gateway')
        return mod.DBGateway
    except Exception:
        _loader_ns = runpy.run_path(str(PROJECT_ROOT / '04_governance' / 'lib' / 'core' / 'module_loader.py'))
        return _loader_ns['load_attr_from_path'](
            'db_gateway_01',
            PROJECT_ROOT / '01_engine' / 'lib' / 'db_gateway.py',
            'DBGateway',
        )


DBGateway = _load_dbgateway()


def detect_nexus_score_column(conn) -> Optional[str]:
    """Resolve score columns from nexus_audits schema with explicit contract checks."""
    rows = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'nexus_audits'
        """
    ).fetchall()
    cols = {str(r[0]).lower() for r in rows}

    preferred = ['l4_blue_score', 'l4_final_score', 'l3_audit_score']
    available = [c for c in preferred if c in cols]
    if not available:
        raise RuntimeError(
            'nexus_audits missing score columns; expected one of '
            + ', '.join(preferred)
        )

    if len(available) == 1:
        return available[0]
    return 'COALESCE(' + ', '.join(available) + ')'
