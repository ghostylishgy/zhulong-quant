#!/usr/bin/env python3
"""Immutable entry/holding/exit contract derived from an ex-ante archetype."""

from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass
from typing import Any, Dict, List

CONTRACT_VERSION = "trade_contract_v0.1"

PLAYBOOKS = {
    "EMOTION_RELAY": {
        "holding": [1, 3],
        "entry": ["next_session_relay_confirmed", "sector_breadth_not_collapsing", "no_excessive_open_gap"],
        "invalidation": ["relay_failed", "leader_breakdown", "high_risk_seat_concentration", "news_or_regulatory_risk"],
        "exit": ["exit_on_relay_failure", "protect_profit_early", "do_not_convert_loss_to_long_term"],
    },
    "EVENT_CATALYST": {
        "holding": [2, 10],
        "entry": ["event_source_verified", "business_relevance_present", "price_not_fully_priced_in"],
        "invalidation": ["event_denied_or_clarified", "business_relevance_disproved", "catalyst_fully_priced", "news_or_regulatory_risk"],
        "exit": ["exit_on_catalyst_invalidation", "review_at_target", "time_exit_if_no_follow_through"],
    },
    "TREND_INITIATION": {
        "holding": [5, 15],
        "entry": ["breakout_or_pullback_confirmed", "sector_tide_supportive", "risk_reward_still_valid"],
        "invalidation": ["breakout_failed", "sector_tide_reversal", "relative_strength_lost", "news_or_regulatory_risk"],
        "exit": ["allow_normal_pullback", "trail_after_profit", "time_exit_if_trend_does_not_form"],
    },
    "TREND_CONTINUATION": {
        "holding": [5, 20],
        "entry": ["trend_structure_intact", "no_exhaustion_or_excessive_gap", "relative_strength_persistent"],
        "invalidation": ["trend_structure_broken", "relative_strength_deterioration", "crowding_exhaustion", "news_or_regulatory_risk"],
        "exit": ["hold_while_structure_intact", "review_at_target", "trailing_exit_on_structure_break"],
    },
}


@dataclass(frozen=True)
class TradeContract:
    contract_id: str
    task_id: str
    symbol: str
    signal_trade_date: str
    primary_archetype: str
    archetype_confidence: float
    expected_holding_days: List[int]
    entry_confirmation: List[str]
    invalidation_conditions: List[str]
    exit_playbook: List[str]
    actionable: bool
    block_reason: str
    no_loss_driven_reclassification: bool = True
    contract_version: str = CONTRACT_VERSION
    no_trade_signal: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def build_trade_contract(
    task_id: str,
    symbol: str,
    signal_trade_date: str,
    primary_archetype: str,
    archetype_confidence: float,
) -> TradeContract:
    archetype = str(primary_archetype or "").strip().upper()
    confidence = float(archetype_confidence or 0)
    playbook = PLAYBOOKS.get(archetype)
    actionable = bool(playbook and confidence >= 0.55)
    reason = "" if actionable else (
        "TRADE_ARCHETYPE_UNCLASSIFIED" if not playbook else "TRADE_ARCHETYPE_LOW_CONFIDENCE"
    )
    seed = "|".join([str(task_id), str(symbol).upper(), str(signal_trade_date)[:10], archetype, CONTRACT_VERSION])
    contract_id = hashlib.sha256(seed.encode("utf-8")).hexdigest()[:24]
    return TradeContract(
        contract_id=contract_id,
        task_id=str(task_id), symbol=str(symbol).upper(),
        signal_trade_date=str(signal_trade_date)[:10],
        primary_archetype=archetype or "UNCLASSIFIED",
        archetype_confidence=round(confidence, 4),
        expected_holding_days=list(playbook["holding"]) if playbook else [],
        entry_confirmation=list(playbook["entry"]) if playbook else [],
        invalidation_conditions=list(playbook["invalidation"]) if playbook else [],
        exit_playbook=list(playbook["exit"]) if playbook else [],
        actionable=actionable, block_reason=reason,
    )


def validate_trade_contract(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    return bool(
        payload.get("contract_version") == CONTRACT_VERSION
        and payload.get("contract_id")
        and payload.get("task_id")
        and payload.get("symbol")
        and payload.get("primary_archetype") in PLAYBOOKS
        and payload.get("actionable") is True
        and payload.get("no_loss_driven_reclassification") is True
    )
