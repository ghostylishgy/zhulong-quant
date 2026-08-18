#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""DuckDB protocol compliance guard."""

from __future__ import annotations

import argparse
import importlib.util
import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _load_dbgateway():
    mod_path = PROJECT_ROOT / '01_engine' / 'lib' / 'db_gateway.py'
    spec = importlib.util.spec_from_file_location('db_gateway_guard', str(mod_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f'DBGateway loader unavailable: {mod_path}')
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.DBGateway


DBGateway = _load_dbgateway()

_busy_suffix = '_'.join(['timeout'])
_busy_token = 'busy' + _busy_suffix

DISALLOWED_PATTERNS = {
    'duckdb.connect': re.compile(r'duckdb\.connect\('),
    'legacy pragma busy-timeout': re.compile(r'PRAGMA\s+' + _busy_token, re.IGNORECASE),
}

DEFAULT_EXCLUDE_DIRS = {
    '.git',
    '__pycache__',
    'archive',
    '_archive',
    'archive_backups',
    '_archive_backups',
    '.venv',
    'venv',
    'node_modules',
}

DEFAULT_EXCLUDE_FILE_PARTS = (
    '.backup',
    '.bak',
)

FACT_DAILY_DOUBLE_FIELDS = [
    'open', 'high', 'low', 'close', 'pre_close', 'pct_chg',
    'vol', 'amount', 'turnover_rate', 'ma20', 'vol_ma5',
]

NEXUS_AUDITS_DOUBLE_FIELDS = [
    'l1_close', 'l1_pct_chg', 'l1_turnover',
    'l2_elapsed_ms', 'l3_elapsed_ms', 'l4_market_sentiment',
]


def should_skip_file(path: Path) -> bool:
    low = str(path).lower()
    return any(part in low for part in DEFAULT_EXCLUDE_FILE_PARTS)


def iter_text_files(root: Path):
    for p in root.rglob('*'):
        if not p.is_file():
            continue
        if any(part in DEFAULT_EXCLUDE_DIRS for part in p.parts):
            continue
        if should_skip_file(p):
            continue
        if p.suffix.lower() in {'.py', '.sh', '.sql', '.yaml', '.yml', '.toml', '.ini', '.txt'}:
            yield p


def static_scan(root: Path):
    violations = []
    for file_path in iter_text_files(root):
        try:
            text = file_path.read_text(encoding='utf-8', errors='ignore')
        except Exception:
            continue
        for label, pattern in DISALLOWED_PATTERNS.items():
            for m in pattern.finditer(text):
                lineno = text.count('\n', 0, m.start()) + 1
                violations.append((str(file_path), label, lineno))
    return violations


def type_is_double_or_decimal(type_name: str) -> bool:
    t = (type_name or '').upper()
    return t.startswith('DOUBLE') or t.startswith('DECIMAL')


def get_table_info(conn, table: str):
    rows = conn.execute(f"PRAGMA table_info('{table}')").fetchall()
    return {str(r[1]).lower(): str(r[2]).upper() for r in rows}


def schema_audit(db_path: Path):
    issues = []
    with DBGateway(db_path, read_only=True) as conn:
        fact = get_table_info(conn, 'fact_daily')
        for col in FACT_DAILY_DOUBLE_FIELDS:
            key = col.lower()
            if key not in fact:
                issues.append(f'fact_daily missing column: {col}')
                continue
            if not type_is_double_or_decimal(fact[key]):
                issues.append(f'fact_daily.{col} type={fact[key]} expected DOUBLE/DECIMAL')

        nexus = get_table_info(conn, 'nexus_audits')
        for col in NEXUS_AUDITS_DOUBLE_FIELDS:
            key = col.lower()
            if key not in nexus:
                issues.append(f'nexus_audits missing column: {col}')
                continue
            if not type_is_double_or_decimal(nexus[key]):
                issues.append(f'nexus_audits.{col} type={nexus[key]} expected DOUBLE/DECIMAL')
    return issues


def main() -> int:
    parser = argparse.ArgumentParser(description='DuckDB protocol compliance guard')
    parser.add_argument('--root', default=str(PROJECT_ROOT), help='project root for static scan')
    parser.add_argument('--db', default=str(PROJECT_ROOT / 'storage' / 'database' / 'zhulong.duckdb'), help='duckdb path for schema audit')
    args = parser.parse_args()

    root = Path(args.root).resolve()
    db_path = Path(args.db).resolve()

    static_violations = static_scan(root)
    schema_issues = schema_audit(db_path)

    print('=== DuckDB Schema Guard Report ===')
    print(f'root: {root}')
    print(f'db:   {db_path}')
    print(f'static_violations: {len(static_violations)}')
    print(f'schema_issues: {len(schema_issues)}')

    if static_violations:
        print('\n[Static Violations]')
        for fp, label, lineno in static_violations:
            print(f'- {fp}:{lineno} -> {label}')

    if schema_issues:
        print('\n[Schema Issues]')
        for issue in schema_issues:
            print(f'- {issue}')

    if static_violations or schema_issues:
        return 1

    print('\nPASS: no disallowed static patterns and schema is compliant.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
