"""Post-event validation rules for the US radar MVP."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from statistics import median


CN_DEFAULT_BENCHMARK = "000300.SH"
CN_CHAIN_BENCHMARKS = {
    "ai_compute": "931071.CSI",
    "memory_hbm": "H30184.CSI",
    "advanced_node": "H30184.CSI",
    "optical_interconnect": "931071.CSI",
    "datacenter_power": "000935.SH",
    "inference_asic": "931071.CSI",
}
US_BENCHMARKS = ("QQQ", "SPY", "SOXX")
FROZEN_HORIZONS = (1, 3, 5, 20)
VALIDATION_RULE_VERSION = "validation_v3_direction_confidence_20260712"
PRICE_RULE_VERSION = "price_v3_cn_session_corp_action_guard_20260726"
CHAIN_SUMMARY_RULE_VERSION = "chain_summary_v1_median_excess_20260712"
SIGNIFICANT_LABELS = {"single_event_signal", "short_window_signal", "medium_window_signal"}
RETRYABLE_DATA_QUALITIES = (
    "PENDING_MARKET_DATA",
    "INSUFFICIENT_FORWARD_DATA",
    "RATE_LIMITED",
    "PROVIDER_ERROR",
    "NO_PRICE_DATA",
    "NO_BASE_PRICE",
    "PARTIAL_BENCHMARK_DATA",
    "BENCHMARK_MISSING",
    "CORPORATE_ACTION_UNADJUSTED",
)
TEXT_BULLISH_TERMS = {
    "award", "backlog", "buyback", "capacity expansion", "contract",
    "demand growth", "guidance raise", "increased guidance", "new customer",
    "order growth", "partnership", "record revenue", "repurchase",
    "revenue growth", "strategic agreement",
}
TEXT_BEARISH_TERMS = {
    "default", "delay", "guidance cut", "impairment", "investigation",
    "layoff", "lowered guidance", "recall", "resignation", "restatement",
    "revenue decline", "subpoena", "termination", "warning",
}


@dataclass(frozen=True)
class ValidationTask:
    validation_id: str
    target_id: str
    event_id: str
    target_market: str
    target_ticker: str
    horizon_days: int
    benchmark: str | None
    data_quality: str
    measured_at: str


@dataclass(frozen=True)
class ValidationUpdate:
    validation_id: str
    benchmark: str | None
    target_return: float | None
    benchmark_return: float | None
    excess_return: float | None
    data_quality: str
    measured_at: str
    base_date: str | None = None
    horizon_date: str | None = None
    base_close: float | None = None
    horizon_close: float | None = None
    price_provider: str | None = None
    excess_vs_csi300: float | None = None
    excess_vs_industry: float | None = None
    excess_vs_chain_median: float | None = None
    excess_vs_qqq: float | None = None
    excess_vs_spy: float | None = None
    excess_vs_soxx: float | None = None
    primary_excess: float | None = None
    signal_label: str | None = None
    direction_label: str | None = None
    event_direction: str | None = None
    direction_source: str | None = None
    direction_confidence: float | None = None
    window_label: str | None = None
    transmission_type: str = "unknown"
    benchmark_data_quality: str | None = None
    quality_gate: str | None = None
    is_effective_sample: int = 0
    validation_rule_version: str = VALIDATION_RULE_VERSION
    price_rule_version: str = PRICE_RULE_VERSION


@dataclass(frozen=True)
class EventChainValidationSummary:
    chain_validation_id: str
    event_id: str
    theme: str
    target_market: str
    transmission_type: str
    horizon_days: int
    total_targets: int
    effective_targets: int
    significant_targets: int
    significant_share: float | None
    median_primary_excess: float | None
    signal_label: str
    event_direction: str
    direction_source: str
    direction_confidence: float
    direction_label: str
    data_quality: str
    source_validation_versions: str
    chain_rule_version: str = CHAIN_SUMMARY_RULE_VERSION


def build_validation_task(row: dict, horizon_days: int, benchmark: str | None) -> ValidationTask:
    material = "|".join([row["target_id"], str(horizon_days), benchmark or ""])
    return ValidationTask(
        validation_id=hashlib.sha256(material.encode("utf-8")).hexdigest(),
        target_id=row["target_id"],
        event_id=row["event_id"],
        target_market=row["target_market"],
        target_ticker=row["target_ticker"],
        horizon_days=horizon_days,
        benchmark=benchmark,
        data_quality="PENDING_MARKET_DATA",
        measured_at=datetime.now(timezone.utc).isoformat(),
    )


def now_utc_text() -> str:
    return datetime.now(timezone.utc).isoformat()


def classify_signal(primary_excess: float | None, horizon_days: int) -> str:
    if primary_excess is None:
        return "pending_market_data"
    value = abs(primary_excess)
    if horizon_days == 1 and value > 0.02:
        return "single_event_signal"
    if horizon_days == 3 and value > 0.03:
        return "short_window_signal"
    if horizon_days == 5 and value > 0.05:
        return "medium_window_signal"
    if horizon_days == 20:
        return "long_window_observation"
    return "below_threshold"


def classify_quality_gate(data_quality: str, primary_excess: float | None) -> tuple[str, int]:
    if data_quality == "DATA_OK" and primary_excess is not None:
        return "effective_sample", 1
    if data_quality in RETRYABLE_DATA_QUALITIES:
        return "pending_validation", 0
    return "excluded_quality", 0


def direction_threshold(horizon_days: int) -> float:
    if horizon_days == 1:
        return 0.02
    if horizon_days == 3:
        return 0.03
    if horizon_days == 5:
        return 0.05
    return 0.05


def classify_direction(event_direction: str, primary_excess: float | None, horizon_days: int) -> str:
    if primary_excess is None:
        return "direction_pending"
    if event_direction not in {"bullish", "bearish"}:
        return "direction_unknown"
    threshold = direction_threshold(horizon_days)
    if abs(primary_excess) <= threshold:
        return "no_significant_move"
    if event_direction == "bullish" and primary_excess > 0:
        return "positive_transmission"
    if event_direction == "bearish" and primary_excess < 0:
        return "negative_transmission"
    return "reverse_to_signal"


def event_direction_from_quality(row: dict) -> tuple[str, str, float]:
    quality = row.get("quality_class")
    if quality == "form4_open_market" and row.get("net_form4_direction") in {"bullish", "bearish"}:
        return str(row["net_form4_direction"]), "form4_transaction", 0.95
    if quality == "form4_non_open_market":
        return "unknown", "form4_non_open_market", 0.0
    evidence_direction, evidence_confidence = _direction_from_evidence(row.get("evidence_output_json"))
    if row.get("event_type") in {"8-K", "10-Q", "10-K"} and evidence_direction in {"bullish", "bearish"}:
        quality_score = _clamp_confidence(row.get("quality_score"), default=1.0)
        return evidence_direction, "evidence_heuristic", min(evidence_confidence, quality_score)
    text_direction, text_confidence = _direction_from_event_text(row)
    if row.get("event_type") in {"8-K", "10-Q", "10-K"} and text_direction in {"bullish", "bearish"}:
        return text_direction, "8k_text_heuristic", text_confidence
    if row.get("event_type") in {"8-K", "10-Q", "10-K"} and evidence_direction == "neutral":
        return "unknown", "evidence_neutral", evidence_confidence
    return "unknown", "none", 0.0


def _direction_from_evidence(output_json: object) -> tuple[str, float]:
    if not output_json:
        return "unknown", 0.0
    try:
        payload = json.loads(str(output_json))
    except (TypeError, ValueError):
        return "unknown", 0.0
    direction = payload.get("signal_direction")
    if direction in {"bullish", "bearish", "neutral"}:
        return str(direction), _clamp_confidence(payload.get("confidence"), default=0.0)
    return "unknown", 0.0


def _direction_from_event_text(row: dict) -> tuple[str, float]:
    text = " ".join(
        str(row.get(field) or "")
        for field in ("event_title", "event_summary", "quality_reason")
    ).lower()
    bullish_hits = [term for term in TEXT_BULLISH_TERMS if term in text]
    bearish_hits = [term for term in TEXT_BEARISH_TERMS if term in text]
    if bullish_hits and not bearish_hits:
        direction = "bullish"
        hits = len(bullish_hits)
    elif bearish_hits and not bullish_hits:
        direction = "bearish"
        hits = len(bearish_hits)
    else:
        return "unknown", 0.0
    confidence = 0.35 + min(0.15, hits * 0.05)
    if row.get("quality_class") == "8k_high_signal":
        confidence += 0.05
    return direction, round(min(confidence, 0.55), 2)


def build_event_chain_summary(rows: list[dict]) -> EventChainValidationSummary:
    if not rows:
        raise ValueError("event-chain summary requires at least one validation row")
    first = rows[0]
    key_parts = [
        str(first["event_id"]),
        str(first.get("theme") or "unknown"),
        str(first["target_market"]),
        str(first.get("target_transmission_type") or "unknown"),
        str(first["horizon_days"]),
    ]
    effective_rows = [
        row for row in rows
        if int(row.get("is_effective_sample") or 0) == 1 and row.get("primary_excess") is not None
    ]
    excesses = [float(row["primary_excess"]) for row in effective_rows]
    median_excess = float(median(excesses)) if excesses else None
    horizon_days = int(first["horizon_days"])
    signal_label = classify_signal(median_excess, horizon_days)
    significant_targets = sum(1 for row in effective_rows if row.get("signal_label") in SIGNIFICANT_LABELS)

    direction_rows = [row for row in rows if row.get("event_direction") in {"bullish", "bearish"}]
    if direction_rows:
        best = max(direction_rows, key=lambda row: _stored_direction_confidence(row))
        event_direction = str(best["event_direction"])
        direction_source = str(best.get("direction_source") or "none")
        direction_confidence = _stored_direction_confidence(best)
    else:
        event_direction = "unknown"
        direction_source = next(
            (str(row.get("direction_source")) for row in rows if row.get("direction_source") not in {None, "none"}),
            "none",
        )
        direction_confidence = max((_stored_direction_confidence(row) for row in rows), default=0.0)

    if effective_rows:
        data_quality = "DATA_OK"
    elif any(row.get("data_quality") in RETRYABLE_DATA_QUALITIES for row in rows):
        data_quality = "INSUFFICIENT_FORWARD_DATA"
    else:
        data_quality = "EXCLUDED_QUALITY"
    versions = sorted({str(row.get("validation_rule_version") or "unknown") for row in rows})
    return EventChainValidationSummary(
        chain_validation_id=hashlib.sha256("|".join(key_parts).encode("utf-8")).hexdigest(),
        event_id=str(first["event_id"]),
        theme=str(first.get("theme") or "unknown"),
        target_market=str(first["target_market"]),
        transmission_type=str(first.get("target_transmission_type") or "unknown"),
        horizon_days=horizon_days,
        total_targets=len(rows),
        effective_targets=len(effective_rows),
        significant_targets=significant_targets,
        significant_share=(significant_targets / len(effective_rows)) if effective_rows else None,
        median_primary_excess=median_excess,
        signal_label=signal_label,
        event_direction=event_direction,
        direction_source=direction_source,
        direction_confidence=direction_confidence,
        direction_label=classify_direction(event_direction, median_excess, horizon_days),
        data_quality=data_quality,
        source_validation_versions=",".join(versions),
    )


def _stored_direction_confidence(row: dict) -> float:
    value = row.get("direction_confidence")
    if value is not None:
        return _clamp_confidence(value, default=0.0)
    return {
        "form4_transaction": 0.95,
        "evidence_heuristic": 0.55,
        "8k_text_heuristic": 0.40,
    }.get(str(row.get("direction_source") or "none"), 0.0)


def _clamp_confidence(value: object, default: float) -> float:
    try:
        return min(1.0, max(0.0, float(value)))
    except (TypeError, ValueError):
        return default


def classify_window(event_type: str | None, horizon_days: int) -> str:
    if event_type == "8-K":
        if horizon_days in {1, 3}:
            return "main_window"
        if horizon_days == 5:
            return "late_response"
        return "extended_observation"
    if event_type == "4":
        if horizon_days in {1, 2}:
            return "early_response"
        if 3 <= horizon_days <= 10:
            return "main_window"
        return "late_response"
    if event_type in {"10-Q", "10-K"}:
        if horizon_days <= 5:
            return "main_window"
        return "late_response"
    return "window_unknown"


def combine_quality(target_quality: str, benchmark_qualities: list[str]) -> str:
    if target_quality != "DATA_OK":
        return target_quality
    if not benchmark_qualities:
        return "DATA_OK"
    ok_count = sum(1 for item in benchmark_qualities if item == "DATA_OK")
    if ok_count == len(benchmark_qualities):
        return "DATA_OK"
    if ok_count:
        return "PARTIAL_BENCHMARK_DATA"
    return "BENCHMARK_MISSING"
