"""SEC daily master-index reconciliation for the US radar sidecar."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import re
from urllib.error import HTTPError
from urllib.request import Request, urlopen


class SecDailyIndexNotAvailable(RuntimeError):
    """Raised when the requested SEC daily index has not been published."""


@dataclass(frozen=True)
class SecDailyIndexEntry:
    cik: str
    company: str
    form_type: str
    filed_date: str
    filename: str
    accession: str | None


@dataclass(frozen=True)
class SecDailyReconciliation:
    total_index_rows: int
    watched_rows: int
    known_rows: int
    missing_rows: tuple[SecDailyIndexEntry, ...]


def daily_index_url(filed_date: date) -> str:
    quarter = (filed_date.month - 1) // 3 + 1
    stamp = filed_date.strftime("%Y%m%d")
    return (
        "https://www.sec.gov/Archives/edgar/daily-index/"
        f"{filed_date.year}/QTR{quarter}/master.{stamp}.idx"
    )


def fetch_daily_index(
    filed_date: date,
    user_agent: str,
    timeout_seconds: int = 20,
) -> list[SecDailyIndexEntry]:
    request = Request(
        daily_index_url(filed_date),
        headers={
            "User-Agent": user_agent,
            "Accept-Encoding": "identity",
            "Accept": "text/plain",
        },
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            payload = response.read()
    except HTTPError as exc:
        if exc.code == 404:
            raise SecDailyIndexNotAvailable(
                f"SEC daily index is not available for {filed_date.isoformat()}"
            ) from exc
        raise
    return parse_daily_index(payload)


def parse_daily_index(data: bytes | str) -> list[SecDailyIndexEntry]:
    text = data.decode("latin-1", errors="replace") if isinstance(data, bytes) else data
    entries: list[SecDailyIndexEntry] = []
    for line in text.splitlines():
        if line.count("|") != 4:
            continue
        cik, company, form_type, filed_date, filename = (
            item.strip() for item in line.split("|", 4)
        )
        if not cik.isdigit() or not filename:
            continue
        entries.append(
            SecDailyIndexEntry(
                cik=cik.zfill(10),
                company=company,
                form_type=form_type.upper(),
                filed_date=filed_date,
                filename=filename,
                accession=_accession_from_filename(filename),
            )
        )
    return entries


def reconcile_watchlist(
    entries: list[SecDailyIndexEntry],
    watchlist_ciks: set[str],
    requested_forms: set[str],
    known_accessions: set[str],
) -> SecDailyReconciliation:
    watched = []
    normalized_ciks = {str(cik).zfill(10) for cik in watchlist_ciks}
    forms = {str(form).strip().upper() for form in requested_forms if str(form).strip()}
    for entry in entries:
        if entry.cik not in normalized_ciks:
            continue
        if not any(_matches_requested_form(entry.form_type, form) for form in forms):
            continue
        watched.append(entry)

    known = [entry for entry in watched if entry.accession and entry.accession in known_accessions]
    missing = tuple(
        entry for entry in watched if not entry.accession or entry.accession not in known_accessions
    )
    return SecDailyReconciliation(
        total_index_rows=len(entries),
        watched_rows=len(watched),
        known_rows=len(known),
        missing_rows=missing,
    )


def _matches_requested_form(actual_form: str, requested_form: str) -> bool:
    actual = str(actual_form or "").strip().upper()
    requested = str(requested_form or "").strip().upper()
    if not actual or not requested:
        return False
    return actual == requested or (
        not requested.endswith("/A") and actual == f"{requested}/A"
    )


def _accession_from_filename(filename: str) -> str | None:
    match = re.search(r"/(\d{10})(\d{2})(\d{6})(?:/|\.|$)", filename)
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    match = re.search(r"/(\d{10}-\d{2}-\d{6})(?:/|\.|$)", filename)
    return match.group(1) if match else None
