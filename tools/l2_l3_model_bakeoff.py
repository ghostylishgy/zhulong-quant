#!/usr/bin/env python3
"""Offline L2/L3 evidence-review model bake-off.

The V2 benchmark separates deterministic facts from model judgment. It calls
Ollama and writes report artifacts only; it never writes production databases,
changes daemon configuration, or grants model authority.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import statistics
import time
import urllib.error
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CASES = PROJECT_ROOT / "tests" / "fixtures" / "l2_l3_evidence_review_bakeoff_v2.json"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "storage" / "reports" / "model_benchmarks"
DESIGN_PATH = PROJECT_ROOT / "docs" / "design" / "bl030_model_evaluation_contract_v0.2.md"
SUITE_VERSION = "L2_L3_EVIDENCE_REVIEW_BAKEOFF_V2_2"
L2_CONTRACT = "L2_INTERPRETATION_REVIEW_V2_1"
L3_CONTRACT = "L3_ARGUMENT_REVIEW_V2_1"
L2_INTERPRETATION_STATUSES = ("SUPPORTED", "REFUTED", "UNVERIFIABLE")
L3_LIFECYCLES = ("INITIATION", "CONTINUATION", "EXHAUSTION", "UNCLEAR")
L3_THESIS_STATES = ("SUPPORTED", "MIXED", "BROKEN", "UNKNOWN")
CONFIDENCE_VALUES = ("LOW", "MEDIUM", "HIGH")
TRADE_ACTION_PATTERN = re.compile(
    r"(?:建议|应当|应该|可以|需要|适合|立即|择机)?\s*"
    r"(?:买入|卖出|建仓|加仓|减仓|清仓|止损|止盈|持有|申购|下单|目标价)",
    re.I,
)
REFUTATION_PREFIX_PATTERN = re.compile(
    r"(?:无法|不能|不可|不足以|并非|不应|不宜|没有证据|缺少证据|缺乏证据|"
    r"缺少|缺乏|未见|尚无|不支持|未支持|反驳|否定|"
    r"候选(?:论点|解释)?(?:认为|声称)|(?:解释|论点)(?:认为|声称)|"
    r"错误地|错误认为)",
    re.I,
)
REFUTATION_SUFFIX_PATTERN = re.compile(
    r"(?:并不成立|不能成立|无法成立|不可靠|不完全可靠|缺乏证据|证据不足|"
    r"与证据矛盾|与事实矛盾|属于过度外推|属于方向颠倒)",
    re.I,
)
ATTRIBUTED_CLAIM_PATTERN = re.compile(
    r"(?:候选(?:论点|解释)?|解释|论点)(?:认为|声称)",
    re.I,
)
ADVERSATIVE_PATTERN = re.compile(r"(?:但是|但|然而|不过|可是|反而)", re.I)
MATCH_INTERNAL_NEGATION_PATTERN = re.compile(
    r"(?:(?:没有|尚未|未)(?:同步)?(?:改善|确认|支持|有效)|无(?:其他|任何)?有效)",
    re.I,
)
BLOCKED_ACTIONS = [
    "write_duckdb",
    "change_daemon_config",
    "restart_daemon",
    "change_l2_authority",
    "change_l3_authority",
    "write_rag_memory",
    "write_shadow",
    "write_nexus_audits",
    "generate_trade_signal",
]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil(fraction * len(ordered)) - 1))
    return float(ordered[index])


def load_suite(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if payload.get("suite_version") != SUITE_VERSION:
        raise ValueError("UNSUPPORTED_SUITE_VERSION")
    cases = payload.get("cases")
    if not isinstance(cases, list) or len(cases) < 20:
        raise ValueError("INSUFFICIENT_BAKEOFF_CASES")
    case_ids = [str(item.get("case_id", "")) for item in cases]
    if not all(case_ids) or len(case_ids) != len(set(case_ids)):
        raise ValueError("INVALID_OR_DUPLICATE_CASE_ID")
    for case in cases:
        validate_case(case)
    return payload


def validate_case(case: dict[str, Any]) -> None:
    evidence = case.get("evidence")
    if not isinstance(evidence, dict) or not evidence:
        raise ValueError(f"EMPTY_EVIDENCE:{case.get('case_id')}")
    for evidence_id, item in evidence.items():
        if not re.fullmatch(r"[A-Z][A-Z0-9_.-]+", str(evidence_id)):
            raise ValueError(f"INVALID_EVIDENCE_ID:{evidence_id}")
        if not isinstance(item, dict) or item.get("stance") not in {
            "SUPPORT", "RISK", "NEUTRAL", "UNKNOWN"
        }:
            raise ValueError(f"INVALID_EVIDENCE_STANCE:{evidence_id}")
        if item.get("quality") not in {"VALID", "MISSING", "STALE", "AMBIGUOUS"}:
            raise ValueError(f"INVALID_EVIDENCE_QUALITY:{evidence_id}")

    missing_ids = set((case.get("missing_evidence") or {}).keys())
    condition_options = case.get("condition_options")
    if not isinstance(condition_options, dict) or not condition_options:
        raise ValueError(f"INVALID_CONDITION_OPTIONS:{case.get('case_id')}")
    condition_ids = set(condition_options)
    for condition_id, item in condition_options.items():
        if not isinstance(item, dict) or item.get("condition_kind") not in {
            "THESIS_INVALIDATION",
            "REVALIDATION",
            "EVIDENCE_COMPLETION",
        }:
            raise ValueError(f"INVALID_CONDITION_KIND:{case.get('case_id')}:{condition_id}")
        if not isinstance(item.get("condition"), str) or not item["condition"].strip():
            raise ValueError(f"INVALID_CONDITION_TEXT:{case.get('case_id')}:{condition_id}")
        referenced = set(item.get("evidence_ids", []))
        unknown_evidence = referenced.difference(evidence)
        if unknown_evidence:
            raise ValueError(
                f"UNBOUND_CONDITION_EVIDENCE:{case.get('case_id')}:{condition_id}:{sorted(unknown_evidence)}"
            )
    evidence_ids = set(evidence)
    for layer in ("l2", "l3"):
        expected = case.get(layer, {}).get("expected", {})
        for key in (
            "required_any_evidence_ids",
            "required_any_bull_ids",
            "required_any_bear_ids",
            "required_any_conflict_ids",
        ):
            unknown = set(expected.get(key, [])).difference(evidence_ids)
            if unknown:
                raise ValueError(f"UNBOUND_EXPECTED_EVIDENCE:{case['case_id']}:{sorted(unknown)}")
        unknown_missing = set(expected.get("required_missing_ids", [])).difference(missing_ids)
        if unknown_missing:
            raise ValueError(f"UNBOUND_EXPECTED_MISSING:{case['case_id']}:{sorted(unknown_missing)}")
        if layer != "l3":
            continue
        required_invalidation = set(expected.get("required_any_invalidation_ids", []))
        allowed_invalidation = set(expected.get("allowed_invalidation_ids", required_invalidation))
        forbidden_invalidation = set(expected.get("forbidden_invalidation_ids", []))
        unknown_invalidation = (required_invalidation | allowed_invalidation | forbidden_invalidation).difference(
            condition_ids
        )
        if unknown_invalidation:
            raise ValueError(
                f"UNBOUND_EXPECTED_INVALIDATION:{case['case_id']}:{sorted(unknown_invalidation)}"
            )
        if not required_invalidation.issubset(allowed_invalidation):
            raise ValueError(f"REQUIRED_INVALIDATION_NOT_ALLOWED:{case['case_id']}")
        if allowed_invalidation.intersection(forbidden_invalidation):
            raise ValueError(f"OVERLAPPING_INVALIDATION_EXPECTATIONS:{case['case_id']}")
        if allowed_invalidation | forbidden_invalidation != condition_ids:
            raise ValueError(f"UNCATEGORIZED_CONDITION_OPTION:{case['case_id']}")


def http_json(url: str, payload: dict[str, Any] | None, timeout: int) -> dict[str, Any]:
    data = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="GET" if payload is None else "POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP_{exc.code}:{body[:400]}") from exc
    parsed = json.loads(body)
    if not isinstance(parsed, dict):
        raise RuntimeError("OLLAMA_NON_OBJECT_RESPONSE")
    return parsed


def model_catalog(server: str) -> tuple[str, dict[str, dict[str, Any]]]:
    version = str(http_json(f"{server}/api/version", None, 15).get("version", "UNKNOWN"))
    rows = http_json(f"{server}/api/tags", None, 30).get("models", [])
    catalog = {}
    for row in rows if isinstance(rows, list) else []:
        if isinstance(row, dict):
            name = str(row.get("name", ""))
            if name:
                catalog[name] = row
    return version, catalog


def _case_payload(case: dict[str, Any], layer: str) -> dict[str, Any]:
    evidence = [
        {
            "evidence_id": evidence_id,
            "fact": item["fact"],
            "stance": item["stance"],
            "quality": item["quality"],
        }
        for evidence_id, item in case["evidence"].items()
    ]
    common = {
        "case_id": case["case_id"],
        "market_regime": case["market_regime"],
        "evidence_envelope": evidence,
        "missing_evidence_options": [
            {"missing_id": item_id, "description": description}
            for item_id, description in (case.get("missing_evidence") or {}).items()
        ],
    }
    if layer == "l2":
        common.update(
            {
                "deterministic_state": case["l2"]["deterministic_state"],
                "candidate_interpretation": case["l2"]["candidate_interpretation"],
            }
        )
    else:
        common.update(
            {
                "l2_deterministic_state": case["l2"]["deterministic_state"],
                "candidate_thesis": case["l3"]["candidate_thesis"],
                "condition_options": [
                    {
                        "condition_id": item_id,
                        "condition": item["condition"],
                        "evidence_ids": item["evidence_ids"],
                    }
                    for item_id, item in case["condition_options"].items()
                ],
            }
        )
    return common


def build_prompt(case: dict[str, Any], layer: str) -> str:
    payload = json.dumps(_case_payload(case, layer), ensure_ascii=False, indent=2)
    if layer == "l2":
        instructions = f"""你是烛龙 L2 证据审查员。输入事实已经由确定性代码解释，禁止重算单位、方向或阈值。
你的任务不是给股票评分，而是审查 candidate_interpretation 与 evidence_envelope 是否一致。

INTERPRETATION_STATUS 定义：
- SUPPORTED：解释与有效证据一致。
- REFUTED：有效证据明确反驳解释，或解释包含方向颠倒和证据外推。
- UNVERIFIABLE：缺失、陈旧或含混证据使解释目前无法验证。

conflict_present 独立表示 evidence envelope 内是否有实质正反冲突，不能用它代替 interpretation_status。

只允许引用输入中的 evidence_id 和 missing_id。不得输出 PASS/VETO、风险分、仓位或交易动作。
SUMMARY 不得出现输入之外的数字，不得补写新闻、财务、资金或价格事实。
只输出一个符合 {L2_CONTRACT} 的 JSON 对象，不要 Markdown、前言或思维过程。

INPUT:
{payload}"""
    else:
        instructions = f"""你是烛龙 L3 论证审查员。输入事实已经由确定性代码解释，禁止重算单位、方向或阈值。
你的任务是审查 candidate_thesis 的生命周期、论点强弱、正反证据、矛盾和证伪路径。

LIFECYCLE_STAGE：INITIATION 启动、CONTINUATION 延续、EXHAUSTION 衰竭、UNCLEAR 无法确定。
THESIS_STATE：SUPPORTED 证据支持、MIXED 正反并存、BROKEN 核心论点被破坏、UNKNOWN 证据不足。
只有 UNKNOWN 应设置 abstain=true；其他状态设置 false。

只允许引用输入中的 evidence_id、missing_id 和 condition_id。只有会削弱或推翻 candidate_thesis 的 condition_id 才能放入 selected_invalidation_ids；重新确认或补齐证据的条件不得选择。
不得输出建议门、质量分、PASS/VETO、仓位或交易动作。
SUMMARY 不得出现输入之外的数字，不得补写新闻、财务、资金或价格事实。
只输出一个符合 {L3_CONTRACT} 的 JSON 对象，不要 Markdown、前言或思维过程。

INPUT:
{payload}"""
    return instructions


def build_schema(case: dict[str, Any], layer: str) -> dict[str, Any]:
    evidence_ids = sorted(case["evidence"])
    missing_ids = sorted((case.get("missing_evidence") or {}).keys())
    id_array = lambda values: {  # noqa: E731
        "type": "array",
        "items": {"type": "string", "enum": values},
        "uniqueItems": True,
        "maxItems": 8,
    }
    if layer == "l2":
        properties = {
            "contract_version": {"type": "string", "enum": [L2_CONTRACT]},
            "interpretation_status": {
                "type": "string",
                "enum": list(L2_INTERPRETATION_STATUSES),
            },
            "conflict_present": {"type": "boolean"},
            "evidence_ids": id_array(evidence_ids),
            "conflict_evidence_ids": id_array(evidence_ids),
            "missing_evidence_ids": id_array(missing_ids),
            "confidence": {"type": "string", "enum": list(CONFIDENCE_VALUES)},
            "summary": {"type": "string", "minLength": 20, "maxLength": 400},
        }
    else:
        invalidation_ids = sorted(case["condition_options"])
        properties = {
            "contract_version": {"type": "string", "enum": [L3_CONTRACT]},
            "lifecycle_stage": {"type": "string", "enum": list(L3_LIFECYCLES)},
            "thesis_state": {"type": "string", "enum": list(L3_THESIS_STATES)},
            "bull_evidence_ids": id_array(evidence_ids),
            "bear_evidence_ids": id_array(evidence_ids),
            "contradiction_evidence_ids": id_array(evidence_ids),
            "missing_evidence_ids": id_array(missing_ids),
            "selected_invalidation_ids": id_array(invalidation_ids),
            "confidence": {"type": "string", "enum": list(CONFIDENCE_VALUES)},
            "abstain": {"type": "boolean"},
            "summary": {"type": "string", "minLength": 40, "maxLength": 500},
        }
    return {
        "type": "object",
        "properties": properties,
        "required": list(properties),
        "additionalProperties": False,
    }


def parse_and_validate_contract(raw: str, case: dict[str, Any], layer: str) -> dict[str, Any]:
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("NON_OBJECT_RESPONSE")
    schema = build_schema(case, layer)
    expected_fields = set(schema["properties"])
    if set(payload) != expected_fields:
        raise ValueError("FIELD_SET_MISMATCH")
    for name, spec in schema["properties"].items():
        value = payload[name]
        value_type = spec["type"]
        if value_type == "string" and not isinstance(value, str):
            raise ValueError(f"FIELD_TYPE:{name}")
        if value_type == "boolean" and not isinstance(value, bool):
            raise ValueError(f"FIELD_TYPE:{name}")
        if value_type == "array":
            if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
                raise ValueError(f"FIELD_TYPE:{name}")
            if len(value) != len(set(value)) or len(value) > int(spec.get("maxItems", 8)):
                raise ValueError(f"FIELD_ARRAY_CONTRACT:{name}")
            unknown = set(value).difference(spec["items"].get("enum", []))
            if unknown:
                raise ValueError(f"UNBOUND_ID:{name}:{sorted(unknown)}")
        if "enum" in spec and value not in spec["enum"]:
            raise ValueError(f"FIELD_ENUM:{name}")
        if isinstance(value, str):
            if len(value) < int(spec.get("minLength", 0)) or len(value) > int(spec.get("maxLength", 999999)):
                raise ValueError(f"FIELD_LENGTH:{name}")
    return payload


def _required_any(actual: set[str], required: list[str]) -> bool:
    return not required or bool(actual.intersection(required))


def _asserts_forbidden_pattern(text: str, pattern: str) -> bool:
    for sentence in re.split(r"[。；;\n]", text):
        for match in re.finditer(pattern, sentence, re.I):
            prefix = sentence[max(0, match.start() - 96):match.start()]
            suffix = sentence[match.end():min(len(sentence), match.end() + 96)]
            matched = match.group(0)
            if REFUTATION_PREFIX_PATTERN.search(prefix) or REFUTATION_SUFFIX_PATTERN.search(suffix):
                continue
            if REFUTATION_PREFIX_PATTERN.search(matched):
                continue
            if MATCH_INTERNAL_NEGATION_PATTERN.search(matched):
                continue
            if ADVERSATIVE_PATTERN.search(matched):
                continue
            attribution_starts = [
                item.start() for item in ATTRIBUTED_CLAIM_PATTERN.finditer(sentence[:match.start()])
            ]
            adversative_starts = [
                item.start() for item in ADVERSATIVE_PATTERN.finditer(sentence[:match.start()])
            ]
            if attribution_starts and attribution_starts[-1] > (
                adversative_starts[-1] if adversative_starts else -1
            ):
                continue
            # "No evidence of institutional activity" is evidence absence, not
            # an assertion that institutions did not participate. Keep an
            # actual "no institutional participation" claim detectable.
            if re.search(r"(?:无|没有)(?:机构|游资|资金)$", matched) and re.match(
                r"(?:行为|参与)?证据", suffix
            ):
                continue
            return True
    return False


def evaluate_payload(payload: dict[str, Any], case: dict[str, Any], layer: str) -> dict[str, Any]:
    expected = case[layer]["expected"]
    summary = str(payload.get("summary", ""))
    fatal_errors: list[str] = []
    direction_errors: list[str] = []
    judgment_errors: list[str] = []
    checks: dict[str, bool] = {}

    if TRADE_ACTION_PATTERN.search(summary):
        fatal_errors.append("TRADE_INSTRUCTION")
    if re.search(r"(?<![A-Za-z])\d+(?:\.\d+)?(?![A-Za-z])", summary):
        fatal_errors.append("INVENTED_NUMBER")
    for pattern in expected.get("forbidden_summary_patterns", []):
        if _asserts_forbidden_pattern(summary, str(pattern)):
            direction_errors.append(f"FORBIDDEN_CLAIM:{pattern}")

    if layer == "l2":
        status = str(payload.get("interpretation_status", ""))
        allowed = set(expected.get("allowed_interpretation_status", []))
        forbidden = set(expected.get("forbidden_interpretation_status", []))
        checks["primary_label"] = status in allowed
        if status in forbidden:
            direction_errors.append(f"FORBIDDEN_INTERPRETATION_STATUS:{status}")
        elif status not in allowed:
            judgment_errors.append(f"INTERPRETATION_STATUS_MISMATCH:{status}")

        conflict_expected = bool(expected.get("conflict_expected"))
        checks["conflict"] = bool(payload.get("conflict_present")) == conflict_expected
        if not checks["conflict"]:
            judgment_errors.append("CONFLICT_MISMATCH")

        evidence_ids = set(payload.get("evidence_ids", []))
        conflict_ids = set(payload.get("conflict_evidence_ids", []))
        checks["evidence"] = _required_any(
            evidence_ids.union(conflict_ids), expected.get("required_any_evidence_ids", [])
        )
        if not checks["evidence"]:
            judgment_errors.append("REQUIRED_EVIDENCE_MISSING")
        checks["conflict_evidence"] = _required_any(
            conflict_ids, expected.get("required_any_conflict_ids", [])
        )
        if not checks["conflict_evidence"]:
            judgment_errors.append("REQUIRED_CONFLICT_EVIDENCE_MISSING")
    else:
        lifecycle = str(payload.get("lifecycle_stage", ""))
        thesis = str(payload.get("thesis_state", ""))
        allowed_lifecycle = set(expected.get("allowed_lifecycle", []))
        forbidden_lifecycle = set(expected.get("forbidden_lifecycle", []))
        allowed_thesis = set(expected.get("allowed_thesis_states", []))
        forbidden_thesis = set(expected.get("forbidden_thesis_states", []))
        checks["lifecycle"] = lifecycle in allowed_lifecycle
        checks["primary_label"] = thesis in allowed_thesis
        if lifecycle in forbidden_lifecycle:
            direction_errors.append(f"FORBIDDEN_LIFECYCLE:{lifecycle}")
        elif lifecycle not in allowed_lifecycle:
            judgment_errors.append(f"LIFECYCLE_MISMATCH:{lifecycle}")
        if thesis in forbidden_thesis:
            direction_errors.append(f"FORBIDDEN_THESIS:{thesis}")
        elif thesis not in allowed_thesis:
            judgment_errors.append(f"THESIS_MISMATCH:{thesis}")

        checks["abstain"] = bool(payload.get("abstain")) == bool(expected.get("abstain_expected"))
        if not checks["abstain"]:
            judgment_errors.append("ABSTAIN_MISMATCH")
        fields = (
            ("bull_evidence_ids", "required_any_bull_ids", "BULL_EVIDENCE_MISSING"),
            ("bear_evidence_ids", "required_any_bear_ids", "BEAR_EVIDENCE_MISSING"),
            (
                "contradiction_evidence_ids",
                "required_any_conflict_ids",
                "CONTRADICTION_EVIDENCE_MISSING",
            ),
        )
        for output_key, expected_key, error_code in fields:
            check_key = output_key.removesuffix("_ids")
            checks[check_key] = _required_any(
                set(payload.get(output_key, [])), expected.get(expected_key, [])
            )
            if not checks[check_key]:
                judgment_errors.append(error_code)

        selected_invalidation = set(payload.get("selected_invalidation_ids", []))
        required_invalidation = set(expected.get("required_any_invalidation_ids", []))
        allowed_invalidation = set(
            expected.get("allowed_invalidation_ids", required_invalidation)
        )
        forbidden_invalidation = set(expected.get("forbidden_invalidation_ids", []))
        empty_allowed = bool(expected.get("empty_selection_allowed", not required_invalidation))
        checks["selected_invalidation"] = (
            (not required_invalidation or bool(selected_invalidation.intersection(required_invalidation)))
            and selected_invalidation.issubset(allowed_invalidation)
            and not selected_invalidation.intersection(forbidden_invalidation)
            and (bool(selected_invalidation) or empty_allowed)
        )
        if required_invalidation and not selected_invalidation.intersection(required_invalidation):
            judgment_errors.append("INVALIDATION_SELECTION_MISSING")
        unexpected_invalidation = sorted(selected_invalidation.difference(allowed_invalidation))
        if unexpected_invalidation:
            judgment_errors.append(
                "UNEXPECTED_INVALIDATION_SELECTION:" + ",".join(unexpected_invalidation)
            )
        forbidden_selected = sorted(selected_invalidation.intersection(forbidden_invalidation))
        if forbidden_selected:
            judgment_errors.append(
                "FORBIDDEN_INVALIDATION_SELECTION:" + ",".join(forbidden_selected)
            )
        if not selected_invalidation and not empty_allowed:
            judgment_errors.append("EMPTY_INVALIDATION_SELECTION_NOT_ALLOWED")

    missing_ids = set(payload.get("missing_evidence_ids", []))
    required_missing = set(expected.get("required_missing_ids", []))
    checks["missing_evidence"] = required_missing.issubset(missing_ids)
    if not checks["missing_evidence"]:
        judgment_errors.append("REQUIRED_MISSING_EVIDENCE_NOT_REPORTED")

    return {
        "strict_pass": not fatal_errors and not direction_errors and not judgment_errors,
        "safe_pass": not fatal_errors and not direction_errors,
        "fatal_errors": fatal_errors,
        "direction_errors": direction_errors,
        "judgment_errors": judgment_errors,
        "checks": checks,
    }


def _binary_f1(expected: list[bool], actual: list[bool]) -> float:
    tp = sum(e and a for e, a in zip(expected, actual))
    fp = sum((not e) and a for e, a in zip(expected, actual))
    fn = sum(e and (not a) for e, a in zip(expected, actual))
    denominator = 2 * tp + fp + fn
    return (2 * tp / denominator) if denominator else 1.0


def _dummy_accuracy(cases: list[dict[str, Any]], layer: str, field: str) -> tuple[str, float]:
    if layer == "l2":
        labels = L2_INTERPRETATION_STATUSES
        expected_key = "allowed_interpretation_status"
    elif field == "primary":
        labels = L3_THESIS_STATES
        expected_key = "allowed_thesis_states"
    else:
        labels = L3_LIFECYCLES
        expected_key = "allowed_lifecycle"
    scores = {
        label: sum(label in case[layer]["expected"].get(expected_key, []) for case in cases)
        for label in labels
    }
    best = max(scores, key=scores.get)
    return best, scores[best] / len(cases)


def aggregate_model(model: str, runs: list[dict[str, Any]], cases: list[dict[str, Any]], layer: str) -> dict[str, Any]:
    total = len(runs)
    accepted = [row for row in runs if row["contract_accepted"]]
    evaluations = [row["evaluation"] for row in accepted]
    strict_rate = sum(item["strict_pass"] for item in evaluations) / total if total else 0.0
    safe_rate = sum(item["safe_pass"] for item in evaluations) / total if total else 0.0
    contract_rate = len(accepted) / total if total else 0.0
    fatal_count = sum(len(item["fatal_errors"]) for item in evaluations)
    direction_count = sum(len(item["direction_errors"]) for item in evaluations)
    elapsed = [float(row.get("elapsed_ms", 0.0)) for row in runs]
    token_rates = [
        float(row["eval_tokens_per_second"])
        for row in runs if row.get("eval_tokens_per_second")
    ]

    if layer == "l2":
        primary_field = "interpretation_status"
        dummy_label, dummy_rate = _dummy_accuracy(cases, layer, "primary")
        expected_conflict = [
            bool(next(case for case in cases if case["case_id"] == row["case_id"])["l2"]["expected"]["conflict_expected"])
            for row in accepted
        ]
        actual_conflict = [bool(row["payload"].get("conflict_present")) for row in accepted]
        conflict_f1 = _binary_f1(expected_conflict, actual_conflict)
        secondary_metric = {"conflict_f1": round(conflict_f1, 4)}
        secondary_gate = conflict_f1 >= 0.75
    else:
        primary_field = "thesis_state"
        dummy_label, dummy_rate = _dummy_accuracy(cases, layer, "primary")
        lifecycle_dummy_label, lifecycle_dummy_rate = _dummy_accuracy(cases, layer, "lifecycle")
        lifecycle_rate = (
            sum(item["checks"].get("lifecycle", False) for item in evaluations) / total
            if total else 0.0
        )
        secondary_metric = {
            "lifecycle_accuracy": round(lifecycle_rate, 4),
            "lifecycle_dummy_label": lifecycle_dummy_label,
            "lifecycle_dummy_accuracy": round(lifecycle_dummy_rate, 4),
        }
        secondary_gate = lifecycle_rate >= 0.75 and lifecycle_rate - lifecycle_dummy_rate >= 0.20

    primary_counts = Counter(str(row["payload"].get(primary_field, "")) for row in accepted)
    primary_total = sum(primary_counts.values())
    max_primary_share = max(primary_counts.values()) / primary_total if primary_total else 1.0
    primary_accuracy = (
        sum(item["checks"].get("primary_label", False) for item in evaluations) / total
        if total else 0.0
    )
    evidence_checks = []
    for item in evaluations:
        for key, value in item["checks"].items():
            if "evidence" in key or key in {"bull_evidence", "bear_evidence", "contradiction_evidence"}:
                evidence_checks.append(bool(value))
    evidence_coverage = sum(evidence_checks) / len(evidence_checks) if evidence_checks else 1.0

    gates = {
        "contract_accept_rate_ge_0_95": contract_rate >= 0.95,
        "fatal_fact_errors_zero": fatal_count == 0,
        "direction_errors_zero": direction_count == 0,
        "strict_pass_rate_ge_0_75": strict_rate >= 0.75,
        "primary_accuracy_ge_0_75": primary_accuracy >= 0.75,
        "primary_margin_over_dummy_ge_0_20": primary_accuracy - dummy_rate >= 0.20,
        "max_primary_share_le_0_60": max_primary_share <= 0.60,
        "evidence_coverage_ge_0_75": evidence_coverage >= 0.75,
        "secondary_metric_gate": secondary_gate,
    }
    return {
        "model": model,
        "runs": total,
        "contract_accept_rate": round(contract_rate, 4),
        "safe_pass_rate": round(safe_rate, 4),
        "strict_pass_rate": round(strict_rate, 4),
        "primary_accuracy": round(primary_accuracy, 4),
        "primary_counts": dict(sorted(primary_counts.items())),
        "max_primary_share": round(max_primary_share, 4),
        "dummy_label": dummy_label,
        "dummy_accuracy": round(dummy_rate, 4),
        "primary_margin_over_dummy": round(primary_accuracy - dummy_rate, 4),
        "evidence_coverage": round(evidence_coverage, 4),
        "fatal_fact_errors": fatal_count,
        "direction_errors": direction_count,
        "schema_failures": total - len(accepted),
        "thinking_nonempty_rate": round(
            sum(bool(row.get("thinking_chars")) for row in runs) / total if total else 0.0,
            4,
        ),
        "latency_ms_p50": round(statistics.median(elapsed), 2) if elapsed else 0.0,
        "latency_ms_p95": round(percentile(elapsed, 0.95), 2),
        "eval_tokens_per_second_avg": round(statistics.mean(token_rates), 3) if token_rates else 0.0,
        **secondary_metric,
        "gates": gates,
        "status": "PROVISIONAL_CANDIDATE" if all(gates.values()) else "DISQUALIFIED",
    }


def run_one(
    server: str,
    model: str,
    case: dict[str, Any],
    layer: str,
    repeat: int,
    timeout: int,
    think: bool,
) -> dict[str, Any]:
    prompt = build_prompt(case, layer)
    schema = build_schema(case, layer)
    contract = L2_CONTRACT if layer == "l2" else L3_CONTRACT
    request_payload = {
        "model": model,
        "prompt": prompt,
        "system": (
            f"Return only one {contract} JSON object. "
            "Use only supplied IDs and facts. You have no scoring or trading authority."
        ),
        "stream": False,
        "format": schema,
        "think": think,
        "keep_alive": "5m",
        "options": {
            "temperature": 0.0,
            "top_p": 0.1,
            "num_ctx": 4096,
            "num_predict": 512 if layer == "l3" else 320,
            "num_gpu": 0,
            "gpu_layers": 0,
        },
    }
    started = time.monotonic()
    response: dict[str, Any] = {}
    raw = ""
    parsed: dict[str, Any] = {}
    contract_accepted = False
    contract_error = ""
    evaluation = {
        "strict_pass": False,
        "safe_pass": False,
        "fatal_errors": [],
        "direction_errors": [],
        "judgment_errors": [],
        "checks": {},
    }
    try:
        response = http_json(f"{server}/api/generate", request_payload, timeout)
        raw = str(response.get("response", "") or "").strip()
        parsed = parse_and_validate_contract(raw, case, layer)
        contract_accepted = True
        evaluation = evaluate_payload(parsed, case, layer)
    except Exception as exc:
        contract_error = f"{type(exc).__name__}:{str(exc)[:500]}"
    elapsed_ms = (time.monotonic() - started) * 1000.0
    eval_count = int(response.get("eval_count", 0) or 0)
    eval_duration = int(response.get("eval_duration", 0) or 0)
    token_rate = eval_count / (eval_duration / 1_000_000_000) if eval_count and eval_duration else 0.0
    return {
        "case_id": case["case_id"],
        "repeat": repeat,
        "contract_accepted": contract_accepted,
        "contract_error": contract_error,
        "evaluation": evaluation,
        "payload": parsed,
        "elapsed_ms": round(elapsed_ms, 2),
        "eval_count": eval_count,
        "eval_tokens_per_second": round(token_rate, 3),
        "thinking_chars": len(str(response.get("thinking", "") or "")),
        "raw_response": raw[:5000],
        "prompt_sha256": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
    }


def unload_model(server: str, model: str) -> None:
    try:
        http_json(
            f"{server}/api/generate",
            {"model": model, "prompt": "", "stream": False, "keep_alive": 0},
            60,
        )
    except Exception:
        pass


def render_markdown(report: dict[str, Any]) -> str:
    layer = report["layer"].upper()
    lines = [
        f"# {layer} Evidence Review Model Bake-off · {report['run_id']}",
        "",
        f"- suite: `{report['suite_version']}`",
        f"- contract: `{report['contract_version']}`",
        f"- cases: `{report['case_count']}`",
        f"- repeats: `{report['repeats']}`",
        f"- ollama_version: `{report['ollama_version']}`",
        f"- dry_run: `{str(report['dry_run']).lower()}`",
        f"- no_trade_signal: `{str(report['no_trade_signal']).lower()}`",
        "",
        "## Summary",
        "",
        "| Model | Status | Schema | Safe | Strict | Primary | Dummy | Margin | Fatal | Direction | Max share | p50 ms | p95 ms |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for item in report["summaries"]:
        lines.append(
            "| {model} | {status} | {contract_accept_rate:.1%} | {safe_pass_rate:.1%} | "
            "{strict_pass_rate:.1%} | {primary_accuracy:.1%} | {dummy_accuracy:.1%} | "
            "{primary_margin_over_dummy:.1%} | {fatal_fact_errors} | {direction_errors} | "
            "{max_primary_share:.1%} | {latency_ms_p50:.0f} | {latency_ms_p95:.0f} |".format(**item)
        )
    lines.extend(["", "## Failure Ledger", ""])
    failures = 0
    for model_row in report["models"]:
        for row in model_row["runs"]:
            if row["contract_accepted"] and row["evaluation"]["strict_pass"]:
                continue
            failures += 1
            if not row["contract_accepted"]:
                reason = row["contract_error"]
            else:
                reason = json.dumps(
                    {
                        "fatal": row["evaluation"]["fatal_errors"],
                        "direction": row["evaluation"]["direction_errors"],
                        "judgment": row["evaluation"]["judgment_errors"],
                    },
                    ensure_ascii=False,
                )
            lines.append(
                f"- `{model_row['model']}` / `{row['case_id']}` / repeat {row['repeat']}: {reason}"
            )
    if not failures:
        lines.append("- None")
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "This is an offline model-quality comparison. It does not authorize a production model, change L2/L3 authority, write DuckDB/RAG/Shadow, or generate a trade signal.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--layer", choices=("l2", "l3"), required=True)
    parser.add_argument("--model", action="append", default=[])
    parser.add_argument(
        "--regrade-report",
        type=Path,
        help="Re-evaluate one prior raw-response report without calling Ollama",
    )
    parser.add_argument("--server", default=os.getenv("ZHULONG_OLLAMA_SERVER", "http://192.0.2.20:11434"))
    parser.add_argument("--cases", type=Path, default=DEFAULT_CASES)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--timeout", type=int, default=720)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--think", action="store_true", help="Enable native thinking; raw thinking is never persisted")
    args = parser.parse_args()
    if args.repeats < 1 or args.repeats > 3:
        parser.error("--repeats must be between 1 and 3")
    if not args.regrade_report and not args.model:
        parser.error("at least one --model is required unless --regrade-report is used")
    if args.regrade_report and args.model:
        parser.error("--regrade-report cannot be combined with --model")

    suite = load_suite(args.cases)
    server = args.server.rstrip("/")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    model_rows = []
    summaries = []
    regraded_from: dict[str, str] | None = None
    if args.regrade_report:
        source = json.loads(args.regrade_report.read_text(encoding="utf-8"))
        if source.get("layer") != args.layer:
            raise SystemExit("Regrade layer does not match source report")
        current_suite_sha = sha256_file(args.cases)
        if source.get("suite_sha256") != current_suite_sha:
            raise SystemExit("Regrade suite SHA does not match source report")
        cases_by_id = {case["case_id"]: case for case in suite["cases"]}
        for model_row in source.get("models", []):
            model = str(model_row.get("model", ""))
            runs = json.loads(json.dumps(model_row.get("runs", []), ensure_ascii=False))
            for row in runs:
                if row.get("contract_accepted"):
                    row["evaluation"] = evaluate_payload(
                        row.get("payload", {}),
                        cases_by_id[str(row.get("case_id", ""))],
                        args.layer,
                    )
            summary = aggregate_model(model, runs, suite["cases"], args.layer)
            summaries.append(summary)
            model_rows.append(
                {
                    "model": model,
                    "metadata": model_row.get("metadata", {}),
                    "summary": summary,
                    "runs": runs,
                }
            )
        version = str(source.get("ollama_version", "UNKNOWN"))
        server = str(source.get("server", server))
        repeats = int(source.get("repeats", 1) or 1)
        thinking_enabled = bool(source.get("thinking_enabled"))
        regraded_from = {
            "run_id": str(source.get("run_id", "")),
            "report_sha256": sha256_file(args.regrade_report),
        }
    else:
        version, catalog = model_catalog(server)
        missing = [model for model in args.model if model not in catalog]
        if missing:
            raise SystemExit("Models are not installed: " + ", ".join(missing))
        for model in args.model:
            runs = []
            for repeat in range(1, args.repeats + 1):
                for case in suite["cases"]:
                    runs.append(
                        run_one(server, model, case, args.layer, repeat, args.timeout, args.think)
                    )
            unload_model(server, model)
            summary = aggregate_model(model, runs, suite["cases"], args.layer)
            summaries.append(summary)
            model_rows.append(
                {"model": model, "metadata": catalog[model], "summary": summary, "runs": runs}
            )
        repeats = args.repeats
        thinking_enabled = bool(args.think)

    report = {
        "run_id": run_id,
        "layer": args.layer,
        "suite_version": suite["suite_version"],
        "contract_version": L2_CONTRACT if args.layer == "l2" else L3_CONTRACT,
        "suite_sha256": sha256_file(args.cases),
        "tool_sha256": sha256_file(Path(__file__)),
        "design_sha256": sha256_file(DESIGN_PATH),
        "ollama_version": version,
        "server": server,
        "case_count": len(suite["cases"]),
        "repeats": repeats,
        "thinking_enabled": thinking_enabled,
        "dry_run": True,
        "no_trade_signal": True,
        "blocked_actions": BLOCKED_ACTIONS,
        "summaries": summaries,
        "models": model_rows,
    }
    if regraded_from:
        report["regraded_from"] = regraded_from
    args.output_dir.mkdir(parents=True, exist_ok=True)
    prefix = f"{args.layer}_evidence_review_bakeoff_{run_id}"
    json_path = args.output_dir / f"{prefix}.json"
    md_path = args.output_dir / f"{prefix}.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    md_path.write_text(render_markdown(report), encoding="utf-8")
    print(json.dumps(summaries, ensure_ascii=False, indent=2))
    print(f"json_report={json_path}")
    print(f"markdown_report={md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
