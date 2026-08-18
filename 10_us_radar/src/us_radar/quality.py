"""Event quality classification for the US radar MVP."""

from __future__ import annotations

from dataclasses import dataclass

from .schema import NormalizedEvent


@dataclass(frozen=True)
class EventQuality:
    event_id: str
    quality_class: str
    quality_score: float
    form_category: str
    transmission_window: str
    reason: str


def classify_event(
    event: NormalizedEvent,
    form4_open_market_count: int = 0,
    form4_transaction_count: int = 0,
) -> EventQuality:
    event_type = (event.event_type or "").upper()
    text = " ".join([event.title or "", event.summary or "", event.company or ""]).lower()

    if event_type == "4":
        if form4_open_market_count > 0:
            return EventQuality(event.event_id, "form4_open_market", 0.82, "insider_transaction", "3-10个交易日", "Form 4 contains open-market transaction records")
        if form4_transaction_count > 0:
            return EventQuality(event.event_id, "form4_non_open_market", 0.40, "insider_transaction", "3-10个交易日", "Form 4 was parsed but contains no open-market transaction")
        return EventQuality(event.event_id, "form4_unparsed", 0.55, "insider_transaction", "3-10个交易日", "Form 4 exists but transaction details are not parsed yet")

    if event_type == "8-K":
        high_terms = ["agreement", "contract", "guidance", "earnings", "results", "acquisition", "merger", "resignation"]
        hits = [term for term in high_terms if term in text]
        if hits:
            return EventQuality(event.event_id, "8k_high_signal", 0.68, "current_report", "1-3个交易日", "8-K keyword hits: " + ",".join(hits[:4]))
        return EventQuality(event.event_id, "8k_generic", 0.42, "current_report", "1-3个交易日", "Generic 8-K current report")

    if event_type in {"10-Q", "10-K"}:
        return EventQuality(event.event_id, "periodic_report", 0.6, "periodic_report", "即日反应，1-5个交易日扩散", "Periodic filing for capex/revenue/inventory review")

    return EventQuality(event.event_id, "low_signal_unknown", 0.25, "other", "待确认", "Unclassified SEC event")
