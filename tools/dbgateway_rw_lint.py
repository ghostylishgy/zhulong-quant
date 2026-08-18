#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Static lint for long-running work inside DBGateway(read_only=False) blocks."""

from __future__ import annotations

import ast
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCAN_DIRS = (
    '01_engine',
    '02_brain',
    '03_tactics',
    '04_governance',
    '05_shadow',
    'zhulong_daemon.py',
)
FORBIDDEN_TOKENS = (
    'requests.',
    'realtime_quote',
    'cloud_fast_call',
    'ollama',
    'COMPUTE_GATEWAY',
    'time.sleep',
    'threading.Timer',
    'Timer(',
    'subprocess.',
)


def _iter_python_files(paths: list[str]) -> list[Path]:
    files: list[Path] = []
    for raw in paths:
        path = (PROJECT_ROOT / raw).resolve()
        if not path.exists():
            continue
        if path.is_file() and path.suffix == '.py':
            files.append(path)
        elif path.is_dir():
            files.extend(p for p in path.rglob('*.py') if '.venv' not in p.parts and 'venv-rag' not in p.parts)
    return sorted(files)


def _is_dbgateway_rw_with(item: ast.withitem) -> bool:
    text = ast.unparse(item.context_expr) if hasattr(ast, 'unparse') else ''
    return 'DBGateway' in text and 'read_only=False' in text.replace(' ', '')


def _line_span(lines: list[str], node: ast.AST) -> str:
    start = getattr(node, 'lineno', 1)
    end = getattr(node, 'end_lineno', start)
    return '\n'.join(lines[start - 1:end])


def main(argv: list[str]) -> int:
    scan_paths = argv[1:] or list(DEFAULT_SCAN_DIRS)
    findings: list[tuple[str, int, str]] = []
    for file_path in _iter_python_files(scan_paths):
        try:
            text = file_path.read_text(encoding='utf-8-sig')
            tree = ast.parse(text)
        except Exception as exc:
            findings.append((str(file_path.relative_to(PROJECT_ROOT)), 1, f'PARSE_ERROR:{exc}'))
            continue
        lines = text.splitlines()
        for node in ast.walk(tree):
            if not isinstance(node, ast.With):
                continue
            if not any(_is_dbgateway_rw_with(item) for item in node.items):
                continue
            block = _line_span(lines, node)
            hits = [token for token in FORBIDDEN_TOKENS if token in block]
            if hits:
                findings.append((str(file_path.relative_to(PROJECT_ROOT)), node.lineno, ','.join(hits)))
    if findings:
        print('DBGateway RW long-work lint findings:')
        for path, line, tokens in findings:
            print(f'{path}:{line}: {tokens}')
        return 1
    print('DBGateway RW long-work lint passed')
    return 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv))
