"""Dry-run evidence extraction for normalized US events.

The production LLM adapter will replace this module later. The current extractor
keeps the Phase 1 loop deterministic and auditable without requiring API keys.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re

from .schema import NormalizedEvent


PROMPT_VERSION = "us_event_evidence_v0.1"
MODEL_NAME = "dry-run-evidence-v0"

BULLISH_TERMS = {
    "contract",
    "agreement",
    "partnership",
    "guidance",
    "increase",
    "growth",
    "expansion",
    "capacity",
    "buyback",
    "repurchase",
}

BEARISH_TERMS = {
    "resignation",
    "investigation",
    "subpoena",
    "restatement",
    "impairment",
    "decrease",
    "termination",
    "default",
    "layoff",
    "lawsuit",
}

FORM_BASE_STRENGTH = {
    "8-K": 3,
    "4": 2,
    "13F-HR": 2,
}


@dataclass(frozen=True)
class EvidenceSignal:
    signal_id: str
    event_id: str
    prompt_version: str
    model_name: str
    input_hash: str
    output_json: str


def extract_dry_run_signal(event: NormalizedEvent, secondary_tickers: list[str] | None = None) -> EvidenceSignal:
    output = build_evidence_payload(event, secondary_tickers=secondary_tickers or [])
    output_json = json.dumps(output, ensure_ascii=False, sort_keys=True)
    input_hash = hashlib.sha256(event.raw_payload.encode("utf-8", errors="replace")).hexdigest()
    signal_id = hashlib.sha256(f"{event.event_id}|{PROMPT_VERSION}|{MODEL_NAME}".encode("utf-8")).hexdigest()
    return EvidenceSignal(
        signal_id=signal_id,
        event_id=event.event_id,
        prompt_version=PROMPT_VERSION,
        model_name=MODEL_NAME,
        input_hash=input_hash,
        output_json=output_json,
    )


def build_evidence_payload(event: NormalizedEvent, secondary_tickers: list[str]) -> dict:
    text = " ".join([event.title or "", event.summary or "", event.company or ""]).lower()
    bullish_hits = sorted(term for term in BULLISH_TERMS if re.search(rf"\b{re.escape(term)}\b", text))
    bearish_hits = sorted(term for term in BEARISH_TERMS if re.search(rf"\b{re.escape(term)}\b", text))

    if bearish_hits and len(bearish_hits) >= len(bullish_hits):
        direction = "bearish"
    elif bullish_hits:
        direction = "bullish"
    else:
        direction = "neutral"

    base_strength = FORM_BASE_STRENGTH.get(event.event_type, 1)
    signal_strength = min(5, max(1, base_strength + (1 if bullish_hits or bearish_hits else 0)))
    confidence = 0.35 if direction == "neutral" else 0.55
    if event.accession:
        confidence += 0.1
    confidence = min(confidence, 0.75)

    primary = [event.ticker] if event.ticker else []
    evidence_terms = bullish_hits + bearish_hits
    thesis = f"{event.event_type} event from SEC EDGAR for {event.company or event.ticker or 'unknown issuer'}."
    if evidence_terms:
        thesis += " Keyword evidence: " + ", ".join(evidence_terms[:5]) + "."

    return {
        "primary_tickers": primary,
        "secondary_tickers": secondary_tickers,
        "signal_direction": direction,
        "signal_strength": signal_strength,
        "confidence": round(confidence, 2),
        "thesis": thesis,
        "key_evidence": [event.title] + ([event.summary] if event.summary else []),
        "risk_flags": ["dry_run_extractor", "requires_human_review"],
        "time_horizon": "immediate" if event.event_type in {"8-K", "4"} else "3M",
        "action_suggestion": "research" if direction != "neutral" else "watch",
    }


def default_prompt_record() -> dict:
    return {
        "prompt_version": PROMPT_VERSION,
        "purpose": "dry_run_event_evidence_extraction",
        "model_name": MODEL_NAME,
        "prompt_text": (
            "Extract structured investment evidence from a US market event. "
            "Output JSON with direction, strength, confidence, evidence, risks, "
            "time horizon, and advisory action. Never create orders."
        ),
    }
