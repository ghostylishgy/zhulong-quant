"""Closed local-model micro-task contracts for non-authoritative L2/L3 observers."""

from __future__ import annotations

import json
import re
from typing import Any


L2_CONTRACT_VERSION = "L2_MISSING_EVIDENCE_OBSERVER_V1"
L3_CONTRACT_VERSION = "L3_INVALIDATION_SELECTOR_OBSERVER_V1"

_LEDGER_ROW_RE = re.compile(r"^\[([A-Z0-9_.-]+)\]\s+(.*)$")


class MicrotaskContractError(ValueError):
    """Raised when a local observer escapes its closed selection contract."""


def _exact_keys(payload: dict[str, Any], expected: set[str], label: str) -> None:
    if not isinstance(payload, dict):
        raise MicrotaskContractError(f"{label}_NOT_OBJECT")
    missing = sorted(expected.difference(payload))
    extra = sorted(set(payload).difference(expected))
    if missing:
        raise MicrotaskContractError(f"{label}_MISSING_FIELDS:{','.join(missing)}")
    if extra:
        raise MicrotaskContractError(f"{label}_UNEXPECTED_FIELDS:{','.join(extra)}")


def _closed_id_list(value: Any, allowed: set[str], label: str) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise MicrotaskContractError(f"{label}_NOT_STRING_LIST")
    normalized = [item.strip().upper() for item in value]
    if any(not item for item in normalized):
        raise MicrotaskContractError(f"{label}_EMPTY_ID")
    if len(normalized) != len(set(normalized)):
        raise MicrotaskContractError(f"{label}_DUPLICATE_ID")
    unknown = sorted(set(normalized).difference(allowed))
    if unknown:
        raise MicrotaskContractError(f"{label}_UNBOUND_ID:{','.join(unknown)}")
    return normalized


def _ledger_rows(evidence_ledger: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for line in str(evidence_ledger or "").splitlines():
        match = _LEDGER_ROW_RE.match(line.strip())
        if match:
            rows.append({"evidence_id": match.group(1), "fact": match.group(2)[:240]})
    if not rows:
        raise MicrotaskContractError("L2_EMPTY_EVIDENCE_LEDGER")
    return rows


def build_l2_missing_task(
    *,
    symbol: str,
    evidence_ledger: str,
    deterministic_interpretation: str,
) -> dict[str, Any]:
    """Build one L2 task from gaps already established by deterministic code."""
    options = [
        {"missing_id": "M.MULTIDAY_PATH", "description": "审计日前三至二十个交易日的完整价格路径与回撤结构"},
        {"missing_id": "M.SECTOR_CONTEXT", "description": "所属板块强度、广度及个股相对板块表现"},
        {"missing_id": "M.CURRENT_NEWS", "description": "截至审计日的公告、监管与公司新闻原文"},
        {"missing_id": "M.FUNDAMENTAL_QUALITY", "description": "最新财务质量、主营业务暴露及报告期"},
    ]
    required = {
        "M.MULTIDAY_PATH",
        "M.SECTOR_CONTEXT",
        "M.CURRENT_NEWS",
        "M.FUNDAMENTAL_QUALITY",
    }
    payload = {
        "symbol": str(symbol),
        "deterministic_interpretation": str(deterministic_interpretation)[:600],
        "evidence_envelope": _ledger_rows(evidence_ledger),
        "missing_evidence_options": options,
    }
    return {
        "payload": payload,
        "allowed_ids": [item["missing_id"] for item in options],
        "required_missing_ids": sorted(required),
    }


def l2_missing_schema(task: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "contract_version": {"type": "string", "enum": [L2_CONTRACT_VERSION]},
            "missing_evidence_ids": {
                "type": "array",
                "items": {"type": "string", "enum": list(task["allowed_ids"])},
                "uniqueItems": True,
                "maxItems": len(task["allowed_ids"]),
            },
        },
        "required": ["contract_version", "missing_evidence_ids"],
        "additionalProperties": False,
    }


def l2_missing_prompt(task: dict[str, Any]) -> str:
    return (
        "你是烛龙 L2 的缺失证据提取器。确定性程序已经完成数值、单位、方向、风险分和标签判断。\n"
        "missing_evidence_options 已由程序确认不在 evidence_envelope 中。只返回当前解释仍需要后续核验的 missing_id。\n"
        "不得创建 ID，不得分类结构，不得评分，不得给出交易或裁决建议。\n"
        "只输出一个 JSON 对象，不要摘要、理由、Markdown 或思维过程。\n\n"
        "INPUT:\n"
        + json.dumps(task["payload"], ensure_ascii=False, indent=2)
    )


def validate_l2_missing_output(payload: dict[str, Any], task: dict[str, Any]) -> dict[str, Any]:
    _exact_keys(payload, {"contract_version", "missing_evidence_ids"}, "L2_OUTPUT")
    if payload["contract_version"] != L2_CONTRACT_VERSION:
        raise MicrotaskContractError("L2_OUTPUT_VERSION")
    selected = _closed_id_list(
        payload["missing_evidence_ids"], set(task["allowed_ids"]), "L2_MISSING_IDS"
    )
    required = set(task["required_missing_ids"])
    if not required.issubset(selected):
        raise MicrotaskContractError(
            "L2_REQUIRED_MISSING_NOT_SELECTED:" + ",".join(sorted(required.difference(selected)))
        )
    false_missing = sorted(set(selected).difference(required))
    if false_missing:
        raise MicrotaskContractError("L2_PRESENT_EVIDENCE_SELECTED:" + ",".join(false_missing))
    return {
        "contract_version": L2_CONTRACT_VERSION,
        "missing_evidence_ids": selected,
    }


def build_l3_invalidation_task(
    *,
    symbol: str,
    deterministic_reasoning: str,
    falsifiable_conditions: list[str],
) -> dict[str, Any]:
    conditions = [str(item).strip() for item in falsifiable_conditions if str(item).strip()][:6]
    if not conditions:
        raise MicrotaskContractError("L3_NO_DETERMINISTIC_CONDITIONS")
    options: list[dict[str, str]] = []
    invalidation_ids: list[str] = []
    for index, condition in enumerate(conditions, start=1):
        condition_id = f"K.{index:03d}"
        options.append({"condition_id": condition_id, "condition": condition[:300]})
        invalidation_ids.append(condition_id)
    options.extend([
        {"condition_id": "K.901", "condition": "若补齐当前缺失证据后重新评估候选论点"},
        {"condition_id": "K.902", "condition": "若当前建设性量价与相对强度继续保持"},
    ])
    return {
        "payload": {
            "symbol": str(symbol),
            "candidate_thesis": "当前候选的建设性审计论点仍然有效",
            "deterministic_context": str(deterministic_reasoning)[:900],
            "condition_options": options,
        },
        "allowed_ids": [item["condition_id"] for item in options],
        "invalidation_ids": invalidation_ids,
        "condition_by_id": {item["condition_id"]: item["condition"] for item in options},
    }


def l3_invalidation_schema(task: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "contract_version": {"type": "string", "enum": [L3_CONTRACT_VERSION]},
            "selected_invalidation_ids": {
                "type": "array",
                "items": {"type": "string", "enum": list(task["allowed_ids"])},
                "uniqueItems": True,
                "maxItems": len(task["allowed_ids"]),
            },
        },
        "required": ["contract_version", "selected_invalidation_ids"],
        "additionalProperties": False,
    }


def l3_invalidation_prompt(task: dict[str, Any]) -> str:
    return (
        "你是烛龙 L3 的失效条件选择器。确定性程序已经完成事实解释、风险状态和裁决。\n"
        "只从 condition_options 中选择会使 candidate_thesis 失效的 condition_id。\n"
        "重新确认、维持现状或补齐证据的条件不是失效条件。不得创建 ID，不得改写条件，不得输出生命周期、评分、PASS/VETO 或交易动作。\n"
        "只输出一个 JSON 对象，不要摘要、理由、Markdown 或思维过程。\n\n"
        "INPUT:\n"
        + json.dumps(task["payload"], ensure_ascii=False, indent=2)
    )


def validate_l3_invalidation_output(
    payload: dict[str, Any], task: dict[str, Any]
) -> dict[str, Any]:
    _exact_keys(payload, {"contract_version", "selected_invalidation_ids"}, "L3_OUTPUT")
    if payload["contract_version"] != L3_CONTRACT_VERSION:
        raise MicrotaskContractError("L3_OUTPUT_VERSION")
    selected = _closed_id_list(
        payload["selected_invalidation_ids"], set(task["allowed_ids"]), "L3_SELECTED_IDS"
    )
    valid = set(task["invalidation_ids"])
    if not set(selected).intersection(valid):
        raise MicrotaskContractError("L3_INVALIDATION_RECALL_EMPTY")
    false_selection = sorted(set(selected).difference(valid))
    if false_selection:
        raise MicrotaskContractError("L3_NON_INVALIDATION_SELECTED:" + ",".join(false_selection))
    return {
        "contract_version": L3_CONTRACT_VERSION,
        "selected_invalidation_ids": selected,
        "selected_conditions": [task["condition_by_id"][item] for item in selected],
    }
