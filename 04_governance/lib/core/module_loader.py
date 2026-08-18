#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Shared singleton module loader for path-based internal modules."""

from __future__ import annotations

import importlib.util
from importlib.machinery import SourceFileLoader
import sys
from pathlib import Path
from threading import RLock
from types import ModuleType
from typing import Any

_LOCK = RLock()
_BY_REAL_PATH: dict[str, ModuleType] = {}


def resolve_project_root(start: Path | None = None) -> Path:
    """Resolve project root by walking up for known Zhulong markers."""
    anchor = Path(start or __file__).resolve()
    if anchor.is_file():
        anchor = anchor.parent

    for parent in (anchor, *anchor.parents):
        if (parent / '.git').exists() or (parent / 'storage').exists() or (parent / 'config').exists():
            return parent
    raise RuntimeError(f'Cannot resolve project root from: {start or __file__}')


def _normalize_module_path(module_path: str | Path, project_root: Path | None = None) -> Path:
    path = Path(module_path)
    if not path.is_absolute():
        if project_root is None:
            project_root = resolve_project_root()
        path = project_root / path
    return path.resolve()


def load_module_from_path(
    module_name: str,
    module_path: str | Path,
    *,
    singleton: bool = True,
    project_root: Path | None = None,
) -> ModuleType:
    """Load a module from filesystem path with singleton-by-realpath semantics."""
    path = _normalize_module_path(module_path, project_root=project_root)
    real_key = str(path)

    with _LOCK:
        if singleton:
            loaded = _BY_REAL_PATH.get(real_key)
            if loaded is not None:
                if module_name not in sys.modules:
                    sys.modules[module_name] = loaded
                return loaded

            named = sys.modules.get(module_name)
            if named is not None:
                named_file = getattr(named, '__file__', None)
                if named_file and Path(named_file).resolve() == path:
                    _BY_REAL_PATH[real_key] = named
                    return named

            path_matches: list[tuple[str, ModuleType]] = []
            for loaded_name, loaded_mod in list(sys.modules.items()):
                loaded_file = getattr(loaded_mod, '__file__', None)
                if not loaded_file:
                    continue
                try:
                    if Path(loaded_file).resolve() == path:
                        path_matches.append((loaded_name, loaded_mod))
                except Exception:
                    continue
            if path_matches:
                preferred = None
                for loaded_name, loaded_mod in path_matches:
                    if loaded_name == module_name:
                        preferred = loaded_mod
                        break
                if preferred is None:
                    for _, loaded_mod in path_matches:
                        if getattr(loaded_mod, '__name__', '') == module_name:
                            preferred = loaded_mod
                            break
                if preferred is None:
                    preferred = path_matches[0][1]
                _BY_REAL_PATH[real_key] = preferred
                if module_name not in sys.modules:
                    sys.modules[module_name] = preferred
                return preferred
        loader = SourceFileLoader(module_name, str(path))
        spec = importlib.util.spec_from_loader(module_name, loader)
        if spec is None:
            raise RuntimeError(f'Module loader unavailable: {path}')

        mod = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = mod
        loader.exec_module(mod)

        if singleton:
            _BY_REAL_PATH[real_key] = mod
        return mod


def load_attr_from_path(
    module_name: str,
    module_path: str | Path,
    attr_name: str,
    *,
    singleton: bool = True,
    project_root: Path | None = None,
) -> Any:
    """Load one attribute from path-based module."""
    mod = load_module_from_path(
        module_name,
        module_path,
        singleton=singleton,
        project_root=project_root,
    )
    if not hasattr(mod, attr_name):
        raise RuntimeError(f'Module {module_path} missing attribute: {attr_name}')
    return getattr(mod, attr_name)
