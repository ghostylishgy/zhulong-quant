"""Configuration helpers for the US radar sidecar."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    root: Path
    database_path: Path
    report_dir: Path
    log_dir: Path
    sec: dict
    scheduling: dict


@dataclass(frozen=True)
class WatchItem:
    ticker: str
    cik: str
    name: str
    theme: str


def resolve_project_root(root: str | None = None) -> Path:
    if root:
        return Path(root).expanduser().resolve()
    cwd = Path.cwd().resolve()
    if (cwd / "10_us_radar").exists():
        return cwd
    for parent in [cwd, *cwd.parents]:
        if (parent / "10_us_radar").exists():
            return parent
    return cwd


def _load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8-sig") as fh:
        return json.load(fh)


def _resolve_path(root: Path, value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return root / path


def load_settings(root: str | Path | None = None) -> Settings:
    root_path = resolve_project_root(str(root) if root else None)
    raw = _load_json(root_path / "10_us_radar" / "config" / "settings.json")
    sec = dict(raw.get("sec", {}))
    env_user_agent = os.environ.get("US_RADAR_SEC_USER_AGENT")
    if env_user_agent:
        sec["user_agent"] = env_user_agent

    settings = Settings(
        root=root_path,
        database_path=_resolve_path(root_path, raw["database_path"]),
        report_dir=_resolve_path(root_path, raw["report_dir"]),
        log_dir=_resolve_path(root_path, raw["log_dir"]),
        sec=sec,
        scheduling=dict(raw.get("scheduling", {})),
    )
    settings.database_path.parent.mkdir(parents=True, exist_ok=True)
    settings.report_dir.mkdir(parents=True, exist_ok=True)
    settings.log_dir.mkdir(parents=True, exist_ok=True)
    return settings


def load_watchlist(root: str | Path | None = None) -> list[WatchItem]:
    root_path = resolve_project_root(str(root) if root else None)
    raw = _load_json(root_path / "10_us_radar" / "config" / "watchlist.json")
    items = []
    for row in raw.get("watchlist", []):
        cik = str(row["cik"]).zfill(10)
        items.append(
            WatchItem(
                ticker=str(row["ticker"]).upper(),
                cik=cik,
                name=str(row["name"]),
                theme=str(row.get("theme", "unknown")),
            )
        )
    return items
