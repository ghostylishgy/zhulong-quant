#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
04_governance/lib/core/llm_parser.py
Unified zero-trust LLM parser.
"""

import hashlib
import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("zhulong.llm_parser")


# ==================== Exceptions ====================


class LLMParserError(Exception):
    """Base exception for parser failures."""


class JsonExtractionError(LLMParserError):
    """No valid JSON object can be extracted."""


class TruncatedResponseError(JsonExtractionError):
    """Response contains unterminated JSON object."""


class InvalidJsonError(JsonExtractionError):
    """Response contains malformed JSON text."""


class IncompleteResponseError(LLMParserError):
    """Required schema fields are missing or empty."""


class SchemaViolationError(LLMParserError):
    """Field value type or range violates schema constraints."""


# ==================== Core JSON Extraction ====================


def _strip_transport_noise(raw: str) -> str:
    """Remove markdown fences and think tags while keeping payload text."""
    if not isinstance(raw, str):
        return ""
    text = raw.strip()
    text = re.sub(r"```(?:json)?\s*", "", text, flags=re.IGNORECASE)
    text = text.replace("```", "")
    text = re.sub(r"</?think>", "", text, flags=re.IGNORECASE)
    return text.strip()


def _iter_json_candidates(raw: str) -> Tuple[List[str], bool]:
    """Depth-scan JSON object candidates. Returns (candidates, truncated_flag)."""
    text = _strip_transport_noise(raw)
    candidates: List[str] = []

    depth = 0
    start = -1
    in_string = False
    escaped = False

    for idx, ch in enumerate(text):
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
            continue

        if ch == "{":
            if depth == 0:
                start = idx
            depth += 1
            continue

        if ch == "}" and depth > 0:
            depth -= 1
            if depth == 0 and start >= 0:
                candidates.append(text[start : idx + 1])
                start = -1

    truncated = depth > 0 and start >= 0
    return candidates, truncated


def extract_json_object_strict(raw: str, label: str = "LLM") -> Dict[str, Any]:
    """
    Zero-trust JSON extractor.
    - Uses brace-depth scanning (string-aware)
    - Rejects truncated / malformed payloads with explicit exceptions
    """
    if not raw or not isinstance(raw, str):
        raise IncompleteResponseError(f"[{label}] empty response")

    candidates, truncated = _iter_json_candidates(raw)
    if not candidates:
        if truncated or ("{" in raw and "}" not in raw):
            raise TruncatedResponseError(f"[{label}] unterminated json object")
        raise InvalidJsonError(f"[{label}] no json object found")

    decode_errors: List[str] = []
    for cand in candidates:
        try:
            parsed = json.loads(cand)
            if isinstance(parsed, dict):
                return parsed
        except (json.JSONDecodeError, ValueError) as e:
            decode_errors.append(str(e))

    if truncated or ("{" in raw and "}" not in raw):
        raise TruncatedResponseError(f"[{label}] json appears truncated")
    raise InvalidJsonError(
        f"[{label}] json decode failed for {len(candidates)} candidate(s): "
        f"{'; '.join(decode_errors[:2])}"
    )


def safe_extract_json(raw: str, label: str = "LLM") -> Optional[Dict[str, Any]]:
    """Compatibility wrapper: returns None instead of raising exceptions."""
    try:
        return extract_json_object_strict(raw, label=label)
    except LLMParserError as e:
        logger.warning(f"[{label}] JSON extraction failed: {e}")
        return None


def _parse_reverse_brace(raw: str) -> Optional[Dict[str, Any]]:
    """Legacy reverse-brace parser kept for compatibility tests."""
    if not isinstance(raw, str) or not raw:
        return None

    last_close = raw.rfind("}")
    if last_close < 0:
        return None

    depth = 0
    for i in range(last_close, -1, -1):
        if raw[i] == "}":
            depth += 1
        elif raw[i] == "{":
            depth -= 1
        if depth == 0:
            candidate = raw[i : last_close + 1]
            try:
                parsed = json.loads(candidate)
                if isinstance(parsed, dict) and len(parsed) > 0:
                    return parsed
            except (json.JSONDecodeError, ValueError):
                return None
    return None


def extract_thinking_trace(raw: str) -> str:
    """Extract <think>...</think> reasoning content."""
    if not raw:
        return ""
    match = re.search(r"<think>([\s\S]*?)</think>", raw, re.IGNORECASE)
    return match.group(1).strip() if match else ""


# ==================== L2 Response Parser ====================


def parse_l2_response(raw: str) -> Dict[str, Any]:
    defaults = {
        "pattern": "UNKNOWN",
        "risk_score": 50,
        "fact_tags": [],
        "thinking_trace": "",
    }

    if not raw:
        return defaults

    defaults["thinking_trace"] = extract_thinking_trace(raw)

    parsed = safe_extract_json(raw, "L2")
    if not parsed:
        return defaults

    return {
        "pattern": str(parsed.get("pattern", "UNKNOWN")),
        "risk_score": _safe_int(parsed.get("risk_score", 50), 0, 100, 50),
        "fact_tags": _safe_list(parsed.get("fact_tags", [])),
        "thinking_trace": defaults["thinking_trace"],
    }


# ==================== L3 Response Parser ====================


L3_VERDICT_MAP = {
    "TRAP": "VETO",
    "SAFE": "APPROVE",
    "CLOUD": "HOLD",
    "APPROVE": "APPROVE",
    "HOLD": "HOLD",
    "VETO": "VETO",
}


def build_l2_extraction_schema() -> Dict[str, Any]:
    """
    Canonical L2 structured output schema.
    Keep reasoning first to bias models toward explicit thought before extraction.
    """
    return {
        "type": "object",
        "properties": {
            "reasoning": {"type": "string", "minLength": 8},
            "pattern": {"type": "string"},
            "risk_score": {"type": "integer", "minimum": 0, "maximum": 100},
            "fact_tags": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["reasoning", "pattern", "risk_score", "fact_tags"],
        "additionalProperties": False,
    }


def build_l3_audit_schema() -> Dict[str, Any]:
    """
    L3 结构化输出 schema (2026-05 精简)。
    - falsifiable_conditions 移出 required: llama3.2:3b 无需强制生成，减少截断风险
    - reasoning maxLength 缩减至 200: L3 不进 RAG，无需长推理
    - risk_level 保留 property 但移出 required: 可选附加信息
    """
    return {
        "type": "object",
        "properties": {
            "verdict": {"type": "string", "enum": ["APPROVE", "HOLD", "VETO"]},
            "audit_score": {"type": "integer", "minimum": 0, "maximum": 100},
            "risk_level": {"type": "string", "enum": ["LOW", "MEDIUM", "HIGH"]},
            "falsifiable_conditions": {
                "type": "array",
                "items": {"type": "string"},
                "maxItems": 3,
            },
            "reasoning": {"type": "string", "minLength": 8, "maxLength": 200},
        },
        "required": [
            "verdict",
            "audit_score",
            "reasoning",
        ],
        "additionalProperties": False,
    }


def parse_l3_response_strict(raw: str) -> Dict[str, Any]:
    """
    Zero-trust L3 parser with strict schema validation.
    Raises custom exceptions on malformed / incomplete payload.
    """
    if not raw or not isinstance(raw, str):
        raise IncompleteResponseError("[L3] empty response")

    thinking = extract_thinking_trace(raw)
    parsed = extract_json_object_strict(raw, label="L3")

    required_fields = ("verdict", "audit_score", "reasoning")
    missing = [k for k in required_fields if k not in parsed]
    if missing:
        raise IncompleteResponseError(
            f"[L3] missing required field(s): {', '.join(missing)}"
        )

    raw_v = str(parsed.get("verdict", "")).upper().strip()
    verdict = L3_VERDICT_MAP.get(raw_v)
    if not verdict:
        raise SchemaViolationError(f"[L3] invalid verdict: {parsed.get('verdict')}")

    score_raw = parsed.get("audit_score")
    if isinstance(score_raw, bool):
        raise SchemaViolationError("[L3] audit_score must be integer, got bool")
    try:
        audit_score = int(score_raw)
    except (ValueError, TypeError):
        raise SchemaViolationError(
            f"[L3] audit_score must be integer, got: {score_raw!r}"
        )
    if audit_score < 0 or audit_score > 100:
        raise SchemaViolationError(f"[L3] audit_score out of range: {audit_score}")

    # 2026-05: verdict/score 一致性守卫 — 防止模型字段独立生成导致的矛盾
    if verdict == "VETO" and audit_score > 45:
        logger.warning(
            "[L3] consistency guard: VETO with score=%d > 45, clamped to 30"
            " (verdict/score likely generated independently)", audit_score
        )
        audit_score = 30
    elif verdict == "APPROVE" and audit_score < 55:
        logger.warning(
            "[L3] consistency guard: APPROVE with score=%d < 55, clamped to 65",
            audit_score
        )
        audit_score = 65

    reasoning_raw = parsed.get("reasoning")
    if not isinstance(reasoning_raw, str) or not reasoning_raw.strip():
        raise IncompleteResponseError("[L3] reasoning is required and must be non-empty")
    reasoning = reasoning_raw.strip()

    fc = _safe_list(parsed.get("falsifiable_conditions", []))
    hash_input = thinking or reasoning
    logic_hash = hashlib.sha256(hash_input.encode()).hexdigest()[:16]

    return {
        "verdict": verdict,
        "audit_score": audit_score,
        "reasoning": reasoning,
        "falsifiable_conditions": fc,
        "thinking_trace": thinking,
        "logic_hash": logic_hash,
    }


def parse_l3_response(raw: str) -> Dict[str, Any]:
    """Compatibility wrapper that degrades to UNKNOWN/0 on strict parser errors."""
    defaults = {
        "verdict": "UNKNOWN",
        "audit_score": 0,
        "reasoning": "",
        "falsifiable_conditions": [],
        "thinking_trace": "",
        "logic_hash": "",
    }

    try:
        return parse_l3_response_strict(raw)
    except LLMParserError as e:
        thinking = extract_thinking_trace(raw)
        hash_input = thinking or ""
        defaults["thinking_trace"] = thinking
        defaults["logic_hash"] = hashlib.sha256(hash_input.encode()).hexdigest()[:16]
        logger.warning(
            f"[L3] strict parser failed, downgrade to defaults: {type(e).__name__}: {e}"
        )
        return defaults


# ==================== L4 Response Parser ====================


def parse_l4_court_response(raw: str, role: str = "judge") -> Dict[str, Any]:
    defaults = {"verdict": "HOLD", "score": 50, "reasoning": "parse_failed"}

    parsed = safe_extract_json(raw, f"L4-{role}")
    if not parsed:
        return defaults

    result = dict(parsed)
    result["verdict"] = str(result.get("verdict", "HOLD")).upper()
    result["score"] = _safe_int(result.get("score", result.get("S_v3", 50)), 0, 100, 50)
    result["reasoning"] = str(result.get("reasoning", result.get("ruling", "")))
    return result


# ==================== Helper Functions ====================


def _safe_int(val: Any, lo: int = 0, hi: int = 100, default: int = 50) -> int:
    try:
        v = int(val)
        return max(lo, min(hi, v))
    except (ValueError, TypeError):
        return default


def _safe_list(val: Any) -> List[str]:
    if isinstance(val, list):
        return [str(x) for x in val]
    if isinstance(val, str):
        return [s.strip() for s in val.split(",") if s.strip()]
    return []


# ==================== Self-Test ====================


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    print("=" * 60)
    print("  LLM Parser Self-Test")
    print("=" * 60)

    print("\\n--- parse_l3_response_strict ---")
    good = '<think>分析量价关系</think> {"verdict":"SAFE","audit_score":85,"reasoning":"量价齐升","falsifiable_conditions":["条件1"]}'
    r = parse_l3_response_strict(good)
    assert r["verdict"] == "APPROVE", f"Expected APPROVE, got {r['verdict']}"
    assert r["audit_score"] == 85
    print(f"  Good parse: verdict={r['verdict']} score={r['audit_score']}")

    partial = '{"verdict":"CLOUD"}'
    try:
        parse_l3_response_strict(partial)
        raise AssertionError("Expected IncompleteResponseError")
    except IncompleteResponseError:
        print("  Partial: rejected by strict schema")

    truncated = '{"verdict":"AP'
    try:
        parse_l3_response_strict(truncated)
        raise AssertionError("Expected parser error")
    except LLMParserError as e:
        print(f"  Truncated: rejected ({type(e).__name__})")

    print("\\n--- parse_l2_response ---")
    l2 = '<think>思考中</think>{"pattern":"CUP_HANDLE","risk_score":30,"fact_tags":["#VOL_BREAKOUT"]}'
    r = parse_l2_response(l2)
    assert r["pattern"] == "CUP_HANDLE"
    assert r["risk_score"] == 30
    print(f"  L2 parse: pattern={r['pattern']} risk={r['risk_score']} tags={r['fact_tags']}")

    print("\\n--- _parse_reverse_brace ---")
    messy = 'some garbage text {"pattern":"X","risk_score":20,"fact_tags":[]} trailing text'
    r = _parse_reverse_brace(messy)
    print(f"  Reverse brace: {r}")

    print("\\nLLM Parser self-test complete")
