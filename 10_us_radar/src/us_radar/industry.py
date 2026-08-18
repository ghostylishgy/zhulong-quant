"""Industry graph helpers for signal target expansion."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path

from .config import resolve_project_root
from .schema import NormalizedEvent


@dataclass(frozen=True)
class SignalTarget:
    event_id: str
    target_market: str
    target_ticker: str
    target_name: str | None
    target_role: str
    theme: str | None
    link_reason: str
    confidence: float
    transmission_type: str | None = None
    source_event_signals: str | None = None


def load_industry_graph(root: str | Path | None = None) -> dict:
    root_path = resolve_project_root(str(root) if root else None)
    path = root_path / "10_us_radar" / "config" / "industry_graph.json"
    with path.open("r", encoding="utf-8-sig") as fh:
        return json.load(fh)


def expand_event_targets(event: NormalizedEvent, graph: dict) -> list[SignalTarget]:
    targets: list[SignalTarget] = []
    source_ticker = (event.ticker or "").upper()
    seen: set[tuple[str, str, str, str]] = set()

    for theme in graph.get("themes", []):
        theme_name = str(theme.get("name", "unknown"))
        theme_targets = theme.get("targets", [])
        matched_theme = any(
            str(item.get("market", "")).upper() == "US"
            and str(item.get("ticker", "")).upper() == source_ticker
            for item in theme_targets
        )
        if not matched_theme and source_ticker:
            continue

        for item in theme_targets:
            market = str(item.get("market", "")).upper()
            ticker = str(item.get("ticker", "")).upper()
            role = str(item.get("role", "second_order"))
            transmission_type = item.get("transmission_type") or ("source" if market == "US" else "unknown")
            key = (market, ticker, role, str(transmission_type))
            if not market or not ticker or key in seen:
                continue
            seen.add(key)
            confidence = _default_confidence(market, ticker, source_ticker, role, str(transmission_type))
            link_logic = str(item.get("link_logic") or f"{theme_name}:{role}")
            source_event_signals = _join_signals(item.get("event_signals") or theme.get("event_signals") or [])
            targets.append(
                SignalTarget(
                    event_id=event.event_id,
                    target_market=market,
                    target_ticker=ticker,
                    target_name=item.get("name"),
                    target_role=role,
                    theme=theme_name,
                    link_reason=link_logic,
                    confidence=confidence,
                    transmission_type=str(transmission_type),
                    source_event_signals=source_event_signals,
                )
            )
    return targets


def _default_confidence(market: str, ticker: str, source_ticker: str, role: str, transmission_type: str) -> float:
    if market == "US" and ticker == source_ticker:
        return 0.75
    if transmission_type == "true_business":
        return 0.58
    if transmission_type == "sentiment":
        return 0.35
    if role == "second_order":
        return 0.42
    return 0.45


def _join_signals(value: list | tuple | str) -> str:
    if isinstance(value, str):
        return value
    return ",".join(str(item) for item in value)
