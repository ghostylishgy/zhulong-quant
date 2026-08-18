#!/usr/bin/env python3
"""Runtime smoke checks for Zhulong hot paths.

This is intentionally stronger than py_compile and lighter than a full
pipeline run. It imports modules, touches runtime branches, and catches the
kind of missing-global/import errors that only appear after a daemon tick.
"""

from __future__ import annotations

import ast
import builtins
import os
import py_compile
import sys
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]
BUILTINS = set(dir(builtins)) | {'__file__', '__name__', '__package__'}


class SmokeFailure(RuntimeError):
    pass


def _print(name: str, ok: bool, detail: str = '') -> None:
    status = 'OK' if ok else 'FAIL'
    suffix = f' - {detail}' if detail else ''
    print(f'[{status}] {name}{suffix}')


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise SmokeFailure(message)


def _add_paths() -> None:
    for path in [
        ROOT,
        ROOT / '05_shadow' / 'lib',
        ROOT / '03_tactics',
        ROOT / '01_engine' / 'lib',
        ROOT / '02_brain' / 'lib',
        ROOT / '04_governance' / 'lib',
        ROOT / '04_governance' / 'lib' / 'core',
    ]:
        text = str(path)
        if text not in sys.path:
            sys.path.insert(0, text)


def check_compile() -> None:
    targets = [
        ROOT / '03_tactics',
        ROOT / '05_shadow',
        ROOT / '04_governance' / 'lib' / 'core',
        ROOT / 'zhulong_daemon.py',
    ]
    files: list[Path] = []
    for target in targets:
        if target.is_file():
            files.append(target)
        elif target.exists():
            files.extend(p for p in target.rglob('*.py') if '__pycache__' not in p.parts)
    for path in files:
        py_compile.compile(str(path), doraise=True)
    _print('py_compile hot paths', True, f'{len(files)} files')


def check_shadow_protection_line() -> None:
    from intraday_manager import PAPER_INITIAL_STOP_MULTIPLIER, ShadowIntradayManager

    manager = ShadowIntradayManager()
    _require(abs(PAPER_INITIAL_STOP_MULTIPLIER - 0.95) < 1e-9, 'unexpected initial stop multiplier')
    cases = {
        'HOLD_FULL': 95.0,
        'TAKE_1_DONE': 100.0,
        'RUNNER_LEFT': 103.4,
        None: 95.0,
    }
    for stage, expected in cases.items():
        got = manager._protection_line(100.0, 110.0, 0.0, stage)  # noqa: SLF001
        _require(got == expected, f'protection line {stage!r}: got {got}, expected {expected}')
    _print('shadow protection-line branches', True)


def check_t1_math_branches() -> None:
    from t1_fill_engine import _choose_amount_volume_units  # noqa: PLC2701

    rows = [
        {'close': 10.0, 'vol': 100.0, 'amount': 100000.0},
        {'close': 10.2, 'vol': 100.0, 'amount': 102000.0},
    ]
    price, vol_shares = _choose_amount_volume_units(rows)
    _require(9.5 <= price <= 10.5, f'unexpected minute price {price}')
    _require(vol_shares > 0, 'minute volume should be positive')
    _print('shadow T1 minute unit inference', True, f'price={price:.4f} vol={vol_shares:.0f}')


def check_saferunner_failure_contract() -> None:
    os.environ['PUSHPLUS_TOKEN'] = ''
    import tactics_bridge

    def crash():
        raise RuntimeError('smoke failure')

    future = tactics_bridge.fire_and_forget('SmokeCrash', crash)
    _require(future is not None, 'fire_and_forget did not return a future')
    payload = future.result(timeout=10)
    _require(isinstance(payload, dict), f'expected structured failure dict, got {type(payload)!r}')
    _require(payload.get('ok') is False, f'expected ok=False, got {payload}')
    _require('smoke failure' in str(payload.get('error')), f'wrong error payload: {payload}')
    _print('SafeRunner structured failure contract', True)


class _Scope:
    def __init__(self, parent: '_Scope | None' = None) -> None:
        self.parent = parent
        self.bound: set[str] = set()
        self.used: list[tuple[str, int]] = []
        self.children: list[_Scope] = []


class _NameVisitor(ast.NodeVisitor):
    def __init__(self) -> None:
        self.scope = _Scope()
        self.module = self.scope

    def _bind_target(self, node: ast.AST) -> None:
        if isinstance(node, ast.Name):
            self.scope.bound.add(node.id)
        elif isinstance(node, (ast.Tuple, ast.List)):
            for item in node.elts:
                self._bind_target(item)

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            self.scope.bound.add(alias.asname or alias.name.split('.')[0])

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        for alias in node.names:
            if alias.name != '*':
                self.scope.bound.add(alias.asname or alias.name)

    def visit_Assign(self, node: ast.Assign) -> None:
        for target in node.targets:
            self._bind_target(target)
        self.visit(node.value)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        self._bind_target(node.target)
        if node.value:
            self.visit(node.value)

    def visit_AugAssign(self, node: ast.AugAssign) -> None:
        self._bind_target(node.target)
        self.visit(node.value)

    def visit_For(self, node: ast.For) -> None:
        self._bind_target(node.target)
        self.visit(node.iter)
        for stmt in node.body + node.orelse:
            self.visit(stmt)

    def visit_With(self, node: ast.With) -> None:
        for item in node.items:
            self.visit(item.context_expr)
            if item.optional_vars:
                self._bind_target(item.optional_vars)
        for stmt in node.body:
            self.visit(stmt)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.scope.bound.add(node.name)
        child = _Scope(self.scope)
        self.scope.children.append(child)
        old = self.scope
        self.scope = child
        for arg in node.args.posonlyargs + node.args.args + node.args.kwonlyargs:
            self.scope.bound.add(arg.arg)
        if node.args.vararg:
            self.scope.bound.add(node.args.vararg.arg)
        if node.args.kwarg:
            self.scope.bound.add(node.args.kwarg.arg)
        for deco in node.decorator_list:
            self.visit(deco)
        for stmt in node.body:
            self.visit(stmt)
        self.scope = old

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.scope.bound.add(node.name)
        for base in node.bases:
            self.visit(base)
        for keyword in node.keywords:
            self.visit(keyword)
        child = _Scope(self.scope)
        self.scope.children.append(child)
        old = self.scope
        self.scope = child
        for stmt in node.body:
            self.visit(stmt)
        self.scope = old

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:
        if node.name:
            self.scope.bound.add(node.name)
        for stmt in node.body:
            self.visit(stmt)

    def visit_Name(self, node: ast.Name) -> None:
        if isinstance(node.ctx, ast.Load):
            self.scope.used.append((node.id, node.lineno))
        elif isinstance(node.ctx, ast.Store):
            self.scope.bound.add(node.id)


def _walk_scopes(scope: _Scope) -> Iterable[_Scope]:
    yield scope
    for child in scope.children:
        yield from _walk_scopes(child)


def _resolved(scope: _Scope, name: str) -> bool:
    current: _Scope | None = scope
    while current is not None:
        if name in current.bound:
            return True
        current = current.parent
    return name in BUILTINS


def check_obvious_undefined_names() -> None:
    paths = [ROOT / '03_tactics', ROOT / '05_shadow']
    ignored = {'x', 'item'}
    missing: list[str] = []
    for base in paths:
        for path in sorted(base.rglob('*.py')):
            if '__pycache__' in path.parts or path.suffix != '.py':
                continue
            tree = ast.parse(path.read_text(encoding='utf-8-sig'), filename=str(path))
            visitor = _NameVisitor()
            visitor.visit(tree)
            for scope in _walk_scopes(visitor.module):
                for name, line in scope.used:
                    if name in ignored:
                        continue
                    if not _resolved(scope, name):
                        missing.append(f'{path.relative_to(ROOT)}:{line}:{name}')
    _require(not missing, 'undefined-name candidates: ' + '; '.join(missing[:20]))
    _print('obvious undefined-name scan', True)


def main() -> int:
    _add_paths()
    checks = [
        check_compile,
        check_shadow_protection_line,
        check_t1_math_branches,
        check_saferunner_failure_contract,
        check_obvious_undefined_names,
    ]
    for check in checks:
        try:
            check()
        except Exception as exc:
            _print(check.__name__, False, str(exc))
            return 1
    print('Runtime smoke passed')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
