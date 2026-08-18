#!/usr/bin/env python3
"""Minimal v0.3 contracts for Zhulong's deterministic evidence path."""

from __future__ import annotations

import re
from datetime import date
from typing import Any

CONTRACT_VERSION = "L2_L3_EVIDENCE_CONTRACT_V0_3"
RELATIONS = ("SUPPORTS", "CONTRADICTS", "UNRELATED")
CLAIM_ROLES = ("SUPPORTING", "RISK")
CLAIM_STATES = ("ACTIVE", "INVALIDATED", "UNRESOLVED")
EVIDENCE_QUALITIES = ("VALID", "STALE", "MISSING", "AMBIGUOUS")
DIVERGENCE_TYPES = ("CROSS_DIMENSION_DIVERGENCE", "SAME_DIMENSION_CONTRADICTION")
DATA_CONFLICT_TYPES = ("SAME_SOURCE_CONTRADICTION", "SAME_DIMENSION_CONFLICT")
_ATOMIC_CONNECTOR_RE = re.compile(r"(?:并且|同时|以及|或者|且|或|并|；|;)")
_NEGATED_CLAIM_RE = re.compile(r"(?:^|[，, ])(?:未|没有|并非|不再|不是|可能|或许)")
_HYPOTHESIS_RE = re.compile(r"^(?:若|如果)")


class ContractError(ValueError):
    """Raised when a v0.3 contract violates a hard boundary."""


def _require_object(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ContractError(f"{label}_NOT_OBJECT")
    return value


def _exact_keys(value: dict[str, Any], required: set[str], label: str) -> None:
    missing = sorted(required - set(value))
    extra = sorted(set(value) - required)
    if missing:
        raise ContractError(f"{label}_MISSING_KEYS:{','.join(missing)}")
    if extra:
        raise ContractError(f"{label}_UNEXPECTED_KEYS:{','.join(extra)}")


def _date(value: Any, label: str) -> str:
    text = str(value or "").strip()
    try:
        date.fromisoformat(text)
    except ValueError as exc:
        raise ContractError(f"{label}_INVALID_DATE") from exc
    return text


def _id_list(value: Any, label: str, *, allow_empty: bool = True) -> list[str]:
    if not isinstance(value, list):
        raise ContractError(f"{label}_NOT_LIST")
    result: list[str] = []
    for item in value:
        text = str(item or "").strip()
        if not re.fullmatch(r"[A-Z][A-Z0-9_.-]+", text):
            raise ContractError(f"{label}_INVALID_ID:{text}")
        if text in result:
            raise ContractError(f"{label}_DUPLICATE_ID:{text}")
        result.append(text)
    if not allow_empty and not result:
        raise ContractError(f"{label}_EMPTY")
    return result


def _atomic_text(value: Any, label: str, *, hypothesis: bool = False) -> str:
    text = str(value or "").strip()
    if len(text) < 4 or "\n" in text or "\r" in text:
        raise ContractError(f"{label}_INVALID_TEXT")
    if _ATOMIC_CONNECTOR_RE.search(text):
        raise ContractError(f"{label}_NOT_ATOMIC")
    if hypothesis:
        if not _HYPOTHESIS_RE.search(text):
            raise ContractError(f"{label}_NOT_HYPOTHESIS")
    elif _HYPOTHESIS_RE.search(text) or _NEGATED_CLAIM_RE.search(text):
        raise ContractError(f"{label}_NOT_AFFIRMATIVE")
    return text


def validate_evidence_card(card: dict[str, Any]) -> dict[str, Any]:
    value = _require_object(card, "EVIDENCE_CARD")
    _exact_keys(value, {"evidence_id", "dimension", "normalized_fact", "quality", "as_of"}, "EVIDENCE_CARD")
    evidence_id = str(value["evidence_id"]).strip()
    if not re.fullmatch(r"[A-Z][A-Z0-9_.-]+", evidence_id):
        raise ContractError("EVIDENCE_CARD_INVALID_ID")
    dimension = str(value["dimension"]).strip().upper()
    if not re.fullmatch(r"[A-Z][A-Z0-9_.-]+", dimension):
        raise ContractError("EVIDENCE_CARD_INVALID_DIMENSION")
    fact = str(value["normalized_fact"] or "").strip()
    if len(fact) < 2:
        raise ContractError("EVIDENCE_CARD_EMPTY_FACT")
    quality = str(value["quality"]).strip().upper()
    if quality not in EVIDENCE_QUALITIES:
        raise ContractError(f"EVIDENCE_CARD_INVALID_QUALITY:{quality}")
    return {
        "evidence_id": evidence_id,
        "dimension": dimension,
        "normalized_fact": fact,
        "quality": quality,
        "as_of": _date(value["as_of"], "EVIDENCE_CARD_AS_OF"),
    }


def derive_missing_evidence(expected_ids: list[str], cards: list[dict[str, Any]]) -> list[str]:
    expected = _id_list(expected_ids, "EXPECTED_EVIDENCE_IDS")
    present = {validate_evidence_card(card)["evidence_id"] for card in cards}
    return sorted(set(expected) - present)


def _validate_pair(item: dict[str, Any], known_ids: set[str], label: str) -> dict[str, Any]:
    _exact_keys(item, {"left_id", "right_id", "type"}, label)
    left = str(item["left_id"]).strip()
    right = str(item["right_id"]).strip()
    if left == right or left not in known_ids or right not in known_ids:
        raise ContractError(f"{label}_UNBOUND_PAIR")
    kind = str(item["type"]).strip().upper()
    if kind not in DIVERGENCE_TYPES:
        raise ContractError(f"{label}_INVALID_TYPE:{kind}")
    return {"left_id": left, "right_id": right, "type": kind}


def _validate_data_conflict(item: dict[str, Any], known_ids: set[str]) -> dict[str, Any]:
    _exact_keys(item, {"evidence_ids", "type"}, "DATA_CONFLICT")
    ids = _id_list(item["evidence_ids"], "DATA_CONFLICT_EVIDENCE_IDS", allow_empty=False)
    if len(ids) < 2 or not set(ids).issubset(known_ids):
        raise ContractError("DATA_CONFLICT_UNBOUND_IDS")
    kind = str(item["type"]).strip().upper()
    if kind not in DATA_CONFLICT_TYPES:
        raise ContractError(f"DATA_CONFLICT_INVALID_TYPE:{kind}")
    return {"evidence_ids": ids, "type": kind}


def build_evidence_packet(
    *,
    symbol: str,
    as_of: str,
    evidence_cards: list[dict[str, Any]],
    expected_evidence_ids: list[str] | None = None,
    signal_divergences: list[dict[str, Any]] | None = None,
    data_conflicts: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    symbol_text = str(symbol or "").strip()
    if not symbol_text:
        raise ContractError("EVIDENCE_PACKET_EMPTY_SYMBOL")
    packet_as_of = _date(as_of, "EVIDENCE_PACKET_AS_OF")
    cards = [validate_evidence_card(card) for card in evidence_cards]
    ids = [card["evidence_id"] for card in cards]
    if len(ids) != len(set(ids)):
        raise ContractError("EVIDENCE_PACKET_DUPLICATE_IDS")
    known_ids = set(ids)
    pairs = [_validate_pair(item, known_ids, "SIGNAL_DIVERGENCE") for item in (signal_divergences or [])]
    conflicts = [_validate_data_conflict(item, known_ids) for item in (data_conflicts or [])]
    return {
        "contract_version": CONTRACT_VERSION,
        "symbol": symbol_text,
        "as_of": packet_as_of,
        "evidence_cards": cards,
        "missing_evidence_ids": derive_missing_evidence(expected_evidence_ids or [], cards),
        "signal_divergences": pairs,
        "data_conflicts": conflicts,
    }


def validate_atomic_claim(claim: dict[str, Any]) -> dict[str, Any]:
    value = _require_object(claim, "CLAIM")
    _exact_keys(value, {"claim_id", "claim_text", "dimension", "variable", "claim_role", "claim_state", "as_of"}, "CLAIM")
    claim_id = str(value["claim_id"]).strip()
    if not re.fullmatch(r"C\.[A-Z0-9_.-]+", claim_id):
        raise ContractError("CLAIM_INVALID_ID")
    claim_text = _atomic_text(value["claim_text"], "CLAIM_TEXT")
    dimension = str(value["dimension"]).strip().upper()
    variable = str(value["variable"]).strip().upper()
    if not dimension or not variable:
        raise ContractError("CLAIM_EMPTY_VARIABLE")
    role = str(value["claim_role"]).strip().upper()
    state = str(value["claim_state"]).strip().upper()
    if role not in CLAIM_ROLES:
        raise ContractError(f"CLAIM_INVALID_ROLE:{role}")
    if state not in CLAIM_STATES:
        raise ContractError(f"CLAIM_INVALID_STATE:{state}")
    return {
        "claim_id": claim_id,
        "claim_text": claim_text,
        "dimension": dimension,
        "variable": variable,
        "claim_role": role,
        "claim_state": state,
        "as_of": _date(value["as_of"], "CLAIM_AS_OF"),
    }


def validate_condition_card(condition: dict[str, Any]) -> dict[str, Any]:
    value = _require_object(condition, "CONDITION")
    _exact_keys(value, {"condition_id", "hypothesis", "dimension", "variable", "evidence_ids", "as_of"}, "CONDITION")
    condition_id = str(value["condition_id"]).strip()
    if not re.fullmatch(r"K\.[A-Z0-9_.-]+", condition_id):
        raise ContractError("CONDITION_INVALID_ID")
    hypothesis = _atomic_text(value["hypothesis"], "CONDITION_HYPOTHESIS", hypothesis=True)
    dimension = str(value["dimension"]).strip().upper()
    variable = str(value["variable"]).strip().upper()
    if not dimension or not variable:
        raise ContractError("CONDITION_EMPTY_VARIABLE")
    evidence_ids = _id_list(value["evidence_ids"], "CONDITION_EVIDENCE_IDS", allow_empty=False)
    return {
        "condition_id": condition_id,
        "hypothesis": hypothesis,
        "dimension": dimension,
        "variable": variable,
        "evidence_ids": evidence_ids,
        "as_of": _date(value["as_of"], "CONDITION_AS_OF"),
    }


def build_l3_model_payload(claim: dict[str, Any], condition: dict[str, Any]) -> dict[str, Any]:
    normalized_claim = validate_atomic_claim(claim)
    normalized_condition = validate_condition_card(condition)
    if normalized_claim["dimension"] != normalized_condition["dimension"]:
        raise ContractError("L3_DIMENSION_MISMATCH")
    if normalized_claim["variable"] != normalized_condition["variable"]:
        raise ContractError("L3_VARIABLE_MISMATCH")
    if normalized_claim["as_of"] != normalized_condition["as_of"]:
        raise ContractError("L3_AS_OF_MISMATCH")
    return {
        "claim": {"claim_id": normalized_claim["claim_id"], "claim_text": normalized_claim["claim_text"]},
        "condition_card": {
            "condition_id": normalized_condition["condition_id"],
            "hypothesis": normalized_condition["hypothesis"],
            "evidence_ids": normalized_condition["evidence_ids"],
        },
    }


def validate_l3_model_output(payload: dict[str, Any], allowed_evidence_ids: list[str]) -> dict[str, Any]:
    value = _require_object(payload, "L3_OUTPUT")
    _exact_keys(value, {"relation", "bound_evidence_ids"}, "L3_OUTPUT")
    relation = str(value["relation"]).strip().upper()
    if relation not in RELATIONS:
        raise ContractError(f"L3_OUTPUT_INVALID_RELATION:{relation}")
    allowed = set(_id_list(allowed_evidence_ids, "ALLOWED_EVIDENCE_IDS"))
    bound = _id_list(value["bound_evidence_ids"], "BOUND_EVIDENCE_IDS")
    if not set(bound).issubset(allowed):
        raise ContractError("L3_OUTPUT_UNBOUND_EVIDENCE")
    return {"relation": relation, "bound_evidence_ids": bound}


def reduce_relation(*, claim_role: str, claim_state: str, relation: str) -> dict[str, str | None]:
    role = str(claim_role).strip().upper()
    state = str(claim_state).strip().upper()
    normalized_relation = str(relation).strip().upper()
    if role not in CLAIM_ROLES or state not in CLAIM_STATES or normalized_relation not in RELATIONS:
        raise ContractError("REDUCER_INVALID_INPUT")
    if normalized_relation == "UNRELATED":
        packet_type = "UNRELATED"
    elif role == "SUPPORTING" and normalized_relation == "SUPPORTS":
        packet_type = "CLAIM_SUPPORT_CONFIRMED"
    elif role == "SUPPORTING" and normalized_relation == "CONTRADICTS":
        packet_type = "CLAIM_SUPPORT_CHALLENGED"
    elif role == "RISK" and normalized_relation == "SUPPORTS":
        packet_type = "CLAIM_RISK_CONFIRMED"
    else:
        packet_type = "CLAIM_RISK_RELIEVED"
    lifecycle_hint: str | None = None
    if state == "ACTIVE" and packet_type == "CLAIM_SUPPORT_CHALLENGED":
        lifecycle_hint = "INVALIDATION_CANDIDATE"
    elif state == "INVALIDATED" and packet_type == "CLAIM_SUPPORT_CONFIRMED":
        lifecycle_hint = "REVALIDATION_CANDIDATE"
    return {"packet_type": packet_type, "lifecycle_hint": lifecycle_hint}
