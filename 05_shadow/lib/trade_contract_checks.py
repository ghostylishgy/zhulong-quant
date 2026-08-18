#!/usr/bin/env python3
"""Evidence-status checklist for immutable Trade Contract entry conditions."""

from __future__ import annotations

from typing import Any, Dict, List

SIGNAL_TIME_CONDITIONS = {
    "event_source_verified",
    "business_relevance_present",
    "breakout_or_pullback_confirmed",
    "trend_structure_intact",
    "relative_strength_persistent",
}

DYNAMIC_CONDITIONS = {
    "next_session_relay_confirmed",
    "sector_breadth_not_collapsing",
    "no_excessive_open_gap",
    "price_not_fully_priced_in",
    "sector_tide_supportive",
    "risk_reward_still_valid",
    "no_exhaustion_or_excessive_gap",
}


def evaluate_contract_conditions(contract: Dict[str, Any], context: Dict[str, Any] | None = None) -> Dict[str, Any]:
    context = dict(context or {})
    checks: List[Dict[str, Any]] = []
    sector_status = str(context.get("sector_tide_status") or "").upper()
    open_gap_pct = context.get("open_gap_pct")

    for condition in list(contract.get("entry_confirmation") or []):
        status = "UNVERIFIED_RUNTIME"
        evidence = "no runtime evidence supplied"
        if condition in SIGNAL_TIME_CONDITIONS:
            status = "SIGNAL_TIME_EVIDENCE"
            evidence = "condition belongs to the deterministic archetype evidence at signal time"
        elif condition == "sector_tide_supportive":
            if sector_status == "SECTOR_SUPPORTIVE":
                status = "VERIFIED_OBSERVER"
                evidence = "same-day sector breadth is supportive; observer-only"
            elif sector_status == "SECTOR_ADVERSE":
                status = "CONTRADICTED_OBSERVER"
                evidence = "same-day sector breadth is adverse; observer-only"
            else:
                evidence = "sector breadth is mixed or unavailable"
        elif condition in {"no_excessive_open_gap", "no_exhaustion_or_excessive_gap"}:
            if open_gap_pct is not None and float(open_gap_pct) <= 0:
                status = "VERIFIED_RUNTIME"
                evidence = f"open gap {float(open_gap_pct):.4f}% is non-positive"
            elif open_gap_pct is not None:
                status = "OBSERVED_NO_THRESHOLD"
                evidence = f"open gap {float(open_gap_pct):.4f}% observed; no validated excessive-gap threshold"
        elif condition in DYNAMIC_CONDITIONS:
            evidence = "dynamic condition requires a validated point-in-time rule"
        checks.append({"condition": str(condition), "status": status, "evidence": evidence})

    counts: Dict[str, int] = {}
    for check in checks:
        counts[check["status"]] = counts.get(check["status"], 0) + 1
    return {
        "checks": checks,
        "counts": counts,
        "runtime_block": False,
        "observer_only": True,
        "unknown_conditions_do_not_pass_silently": True,
    }
