"""SEC EDGAR Atom feed collection."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import re
from typing import Iterable
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

from .config import Settings
from .schema import NormalizedEvent, stable_event_id


ATOM_NS = {"atom": "http://www.w3.org/2005/Atom"}
SEC_SOURCE = "sec_edgar_atom"
PLACEHOLDER_USER_AGENT = "SET_US_RADAR_SEC_USER_AGENT"


class SecConfigError(RuntimeError):
    """Raised when SEC access config is unsafe or incomplete."""


@dataclass(frozen=True)
class SecEdgarClient:
    base_atom_url: str
    user_agent: str
    timeout_seconds: int = 20

    @classmethod
    def from_settings(cls, settings: Settings) -> "SecEdgarClient":
        user_agent = str(settings.sec.get("user_agent", "")).strip()
        if not user_agent or user_agent == PLACEHOLDER_USER_AGENT:
            raise SecConfigError(
                "SEC fetch requires US_RADAR_SEC_USER_AGENT, e.g. "
                "zhulong-us-radar/0.1 your_email@example.com"
            )
        return cls(
            base_atom_url=str(settings.sec.get("base_atom_url", "https://www.sec.gov/cgi-bin/browse-edgar")),
            user_agent=user_agent,
        )

    def fetch_current_filings(self, form_type: str, limit: int = 40) -> list[NormalizedEvent]:
        params = {
            "action": "getcurrent",
            "type": form_type,
            "owner": "include",
            "count": str(limit),
            "output": "atom",
        }
        return self._fetch(params=params, form_type=form_type, ticker=None, cik=None)[:limit]

    def fetch_company_filings(
        self,
        cik: str,
        form_type: str,
        limit: int = 40,
        ticker: str | None = None,
    ) -> list[NormalizedEvent]:
        params = {
            "action": "getcompany",
            "CIK": cik,
            "type": form_type,
            "owner": "include",
            "count": str(limit),
            "output": "atom",
        }
        return self._fetch(params=params, form_type=form_type, ticker=ticker, cik=cik)[:limit]

    def _fetch(
        self,
        params: dict[str, str],
        form_type: str,
        ticker: str | None,
        cik: str | None,
    ) -> list[NormalizedEvent]:
        url = self.base_atom_url + "?" + urlencode(params)
        request = Request(
            url,
            headers={
                "User-Agent": self.user_agent,
                "Accept-Encoding": "identity",
                "Accept": "application/atom+xml,application/xml,text/xml",
            },
        )
        with urlopen(request, timeout=self.timeout_seconds) as response:
            data = response.read()
        return parse_atom_feed(data, form_type=form_type, ticker=ticker, cik=cik)


def parse_atom_feed(
    data: bytes | str,
    form_type: str,
    ticker: str | None = None,
    cik: str | None = None,
) -> list[NormalizedEvent]:
    if isinstance(data, bytes):
        text = data.decode("utf-8", errors="replace")
    else:
        text = data
    root = ET.fromstring(text)
    events = []
    for entry in root.findall("atom:entry", ATOM_NS):
        actual_form_type = _entry_form_type(entry)
        if not _matches_requested_form(actual_form_type, form_type):
            continue
        title = _text(entry, "atom:title")
        updated = _normalize_event_time(_text(entry, "atom:updated") or _text(entry, "atom:published"))
        summary = _text(entry, "atom:summary")
        link = _entry_link(entry)
        accession = _extract_accession(title, link, summary)
        company = _extract_company(title)
        event_id = stable_event_id(
            source=SEC_SOURCE,
            event_type=form_type,
            accession=accession,
            url=link,
            title=title,
            event_time=updated,
        )
        events.append(
            NormalizedEvent(
                event_id=event_id,
                source=SEC_SOURCE,
                event_type=form_type,
                ticker=ticker,
                company=company,
                cik=cik,
                accession=accession,
                event_time=updated,
                title=title,
                url=link,
                summary=summary,
                raw_payload=ET.tostring(entry, encoding="unicode"),
            )
        )
    return events


def _entry_form_type(entry: ET.Element) -> str:
    category = entry.find("atom:category", ATOM_NS)
    if category is None:
        return ""
    return str(category.attrib.get("term", "")).strip().upper()


def _matches_requested_form(actual_form_type: str, requested_form_type: str) -> bool:
    actual = str(actual_form_type or "").strip().upper()
    requested = str(requested_form_type or "").strip().upper()
    if not actual or not requested:
        return False
    if actual == requested:
        return True
    return not requested.endswith("/A") and actual == f"{requested}/A"


def _text(entry: ET.Element, path: str) -> str:
    node = entry.find(path, ATOM_NS)
    if node is None or node.text is None:
        return ""
    return " ".join(node.text.split())


def _entry_link(entry: ET.Element) -> str:
    link = entry.find("atom:link", ATOM_NS)
    if link is None:
        return ""
    return str(link.attrib.get("href", ""))


def _normalize_event_time(value: str) -> str:
    if not value:
        return datetime.now(timezone.utc).isoformat()
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return value
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def _extract_accession(*parts: str) -> str | None:
    joined = " ".join(part or "" for part in parts)
    match = re.search(r"\b\d{10}-\d{2}-\d{6}\b", joined)
    if match:
        return match.group(0)
    match = re.search(r"accession(?:-number)?[=/ ]+(\d{10}\d{2}\d{6})", joined, re.IGNORECASE)
    if match:
        raw = match.group(1)
        return f"{raw[:10]}-{raw[10:12]}-{raw[12:]}"
    return None


def _extract_company(title: str) -> str | None:
    if not title:
        return None
    if " - " in title:
        tail = title.split(" - ", 1)[1]
    else:
        tail = title
    tail = re.sub(r"\s*\(\d{10}\).*", "", tail).strip()
    return tail or None


def filter_events_by_cik(events: Iterable[NormalizedEvent], cik: str) -> list[NormalizedEvent]:
    padded = cik.zfill(10)
    return [event for event in events if (event.cik or "").zfill(10) == padded]
