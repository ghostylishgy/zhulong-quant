"""Shared data shapes for normalized US radar events."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json


@dataclass(frozen=True)
class NormalizedEvent:
    event_id: str
    source: str
    event_type: str
    ticker: str | None
    company: str | None
    cik: str | None
    accession: str | None
    event_time: str
    title: str
    url: str
    summary: str
    raw_payload: str


def stable_event_id(
    source: str,
    event_type: str,
    accession: str | None,
    url: str | None,
    title: str | None,
    event_time: str | None,
) -> str:
    if accession:
        material = f"{source}|{event_type}|{accession}"
    else:
        material = json.dumps(
            {
                "source": source,
                "event_type": event_type,
                "url": url or "",
                "title": title or "",
                "event_time": event_time or "",
            },
            sort_keys=True,
        )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()
