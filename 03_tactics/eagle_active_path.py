#!/usr/bin/env python3
"""Deterministic, observation-only Eagle Active Path.

This module consumes real intraday cumulative quote snapshots and produces a
durable daily manifest. It does not call DuckDB, LLMs, Shadow, RAG, or the
audit chain. Cumulative volume/amount are converted to interval deltas only
when two real snapshots for the same symbol are available.
"""

from __future__ import annotations

import json
import math
import os
import tempfile
import threading
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


SCHEMA_VERSION = "eagle_active_path_v0.4"
RULE_VERSION = "eagle_active_rules_v0.4"
REPORT_DIR = Path(__file__).resolve().parents[1] / "storage" / "reports" / "eagle_active_path"
ACTIVE_DELTA_PERCENTILE = 0.85
MIN_REALTIME_COVERAGE = 0.80
MIN_PERCENTILE_SAMPLE_COUNT = 20
MAX_WINDOW_GAP_MINUTES = 15
MIN_PERSISTENT_WINDOWS = 2
MIN_PERSISTENT_SPAN_MINUTES = 20
LATE_SESSION_MINUTE = 14 * 60 + 30
LATE_CLIMAX_DRAWDOWN_PCT = -2.0
MIN_LISTING_AGE_DAYS = 20
A_SHARE_MARKETS = {
    "主板",
    "创业板",
    "科创板",
    "CDR",
    "沪市",
    "深市",
    "A股",
    "主板A股",
    "创业板A股",
    "科创板A股",
}
BLOCKED_ACTIONS = [
    "write_duckdb",
    "generate_validation_task",
    "call_decision_engine",
    "call_nexus_run",
    "write_shadow",
    "write_rag_memory",
    "write_nexus_audits",
    "trigger_daemon",
    "trade",
]


def _float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
        return number if math.isfinite(number) else default
    except (TypeError, ValueError):
        return default


def _date_text(value: Any) -> str:
    text = str(value or "").strip()
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10]
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return ""


def _time_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if " " in text:
        text = text.rsplit(" ", 1)[-1]
    if "T" in text:
        text = text.rsplit("T", 1)[-1]
    text = text.split(".", 1)[0]
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) == 4:
        digits += "00"
    if len(digits) != 6:
        return ""
    return f"{digits[:2]}:{digits[2:4]}:{digits[4:6]}"


def _minute(value: Any) -> int:
    text = _time_text(value)
    if not text:
        return -1
    return int(text[:2]) * 60 + int(text[3:5])


def _trading_session(value: Any) -> str:
    """Return the A-share continuous-auction session for a quote clock."""
    minute = _minute(value)
    if 9 * 60 <= minute <= 11 * 60 + 30:
        return "MORNING"
    if 13 * 60 <= minute <= 15 * 60:
        return "AFTERNOON"
    return "OUTSIDE_SESSION"


def _scan_time(value: Any, trade_date: str) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    text = str(value or "").strip()
    if len(text) >= 19 and text[4] == "-" and text[7] == "-":
        return text[:19]
    clock = _time_text(text)
    if clock:
        return f"{trade_date} {clock}"
    return f"{trade_date} 00:00:00"


def _percentile(values: Iterable[float], quantile: float) -> Optional[float]:
    numbers = sorted(_float(value) for value in values if _float(value) > 0)
    if not numbers:
        return None
    if len(numbers) == 1:
        return numbers[0]
    position = (len(numbers) - 1) * max(0.0, min(1.0, quantile))
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return numbers[lower]
    weight = position - lower
    return numbers[lower] * (1.0 - weight) + numbers[upper] * weight


def _scan_quality(scan: Dict[str, Any]) -> Dict[str, Any]:
    """Determine whether a realtime scan can participate in cross-sectional ranking."""
    stats = dict(scan.get("quote_stats") or {})
    universe_count = max(0, int(_float(stats.get("universe_count"))))
    valid_quote_count = len(scan.get("quotes") or [])
    reported_realtime_count = max(
        0,
        int(_float(stats.get("realtime_count", stats.get("quote_count", 0)))),
    )
    if universe_count > 0:
        coverage_ratio = round(valid_quote_count / universe_count, 6)
        coverage_ok = coverage_ratio >= MIN_REALTIME_COVERAGE
    else:
        coverage_ratio = None
        coverage_ok = False
    if universe_count > 0 and valid_quote_count > universe_count:
        coverage_ok = False
    return {
        "universe_count": universe_count,
        "valid_quote_count": valid_quote_count,
        "reported_realtime_count": reported_realtime_count,
        "coverage_ratio": coverage_ratio,
        "coverage_ok": coverage_ok,
        "coverage_mismatch": reported_realtime_count != valid_quote_count,
        "quality_status": (
            "COMPLETE"
            if coverage_ok
            else "PARTIAL_LOW_COVERAGE"
            if universe_count > 0
            else "PARTIAL_MISSING_COVERAGE"
        ),
    }


def _candidate_id(trade_date: str, symbol: str) -> str:
    return f"EAGLE-{trade_date.replace('-', '')}-{symbol.upper()}"


def _normalise_quote(item: Dict[str, Any], trade_date: str) -> Optional[Dict[str, Any]]:
    symbol = str(item.get("symbol") or "").strip().upper()
    quote_date = _date_text(item.get("quote_date"))
    quote_time = _time_text(item.get("quote_time"))
    source = str(item.get("quote_source") or "").strip().lower()
    if not symbol or quote_date != trade_date or not quote_time:
        return None
    if source not in {"tushare_realtime", "tushare_realtime_quote"}:
        return None
    price = _float(item.get("price", item.get("close")))
    cumulative_volume = _float(item.get("cumulative_volume", item.get("volume")))
    cumulative_amount = _float(item.get("cumulative_amount", item.get("amount")))
    if price <= 0 or cumulative_volume <= 0 or cumulative_amount <= 0:
        return None
    return {
        "symbol": symbol,
        "quote_date": quote_date,
        "quote_time": quote_time,
        "price": price,
        "pct_chg": _float(item.get("pct_chg")),
        "cumulative_volume": cumulative_volume,
        "cumulative_amount": cumulative_amount,
        "industry": str(item.get("industry") or "").strip(),
        "market": str(item.get("market") or "").strip(),
        "is_st": bool(item.get("is_st", False)),
        "list_date": _date_text(item.get("list_date")),
        "historical_pct_chg": (
            _float(item.get("historical_pct_chg"))
            if item.get("historical_pct_chg") is not None
            else None
        ),
        "quote_source": source,
    }


def _normalise_scan(record: Dict[str, Any], trade_date: str) -> Optional[Dict[str, Any]]:
    scan_time = _scan_time(record.get("scan_time"), trade_date)
    quotes: Dict[str, Dict[str, Any]] = {}
    for raw in record.get("quotes", []) or []:
        if not isinstance(raw, dict):
            continue
        quote = _normalise_quote(raw, trade_date)
        if quote:
            quotes[quote["symbol"]] = quote
    if not quotes:
        return None
    return {
        "scan_id": f"{trade_date}|{scan_time[:16]}",
        "trade_date": trade_date,
        "scan_time": scan_time,
        "quotes": [quotes[key] for key in sorted(quotes)],
        "quote_stats": dict(record.get("quote_stats") or {}),
    }


def _sector_origin(points: List[Dict[str, Any]]) -> str:
    if not points:
        return "ORIGIN_UNRESOLVED"
    resonance_windows = sum(
        1
        for point in points
        if point.get("peer_active_count", 0) >= 2
        and point.get("peer_observed_count", 0) >= 2
        and point.get("peer_active_ratio", 0.0) >= 0.2
    )
    if resonance_windows >= MIN_PERSISTENT_WINDOWS:
        return "SECTOR_RESONANCE"
    if sum(bool(point.get("active_window")) for point in points) >= MIN_PERSISTENT_WINDOWS:
        return "STOCK_SPECIFIC"
    return "ORIGIN_UNRESOLVED"


def _primary_morphology(points: List[Dict[str, Any]]) -> tuple[str, List[str]]:
    active = [point for point in points if point.get("active_window")]
    if not active:
        return "UNCLASSIFIED", []
    flags: List[str] = []
    active_windows = len(active)
    span_minutes = max(_minute(point["quote_time"]) for point in active) - min(
        _minute(point["quote_time"]) for point in active
    )
    first_minute = min(_minute(point["quote_time"]) for point in active)
    first_price = _float(active[0].get("price"))
    latest = max(points, key=lambda point: point["quote_time"])
    latest_price = _float(latest.get("price"))
    latest_minute = _minute(latest["quote_time"])
    peak = max(_float(point.get("price")) for point in points)
    drawdown = (latest_price / peak - 1.0) * 100.0 if peak > 0 else 0.0

    if first_minute >= 0 and first_minute <= 10 * 60 + 30:
        flags.append("EARLY_WINDOW_ACTIVITY")
    if latest_minute >= LATE_SESSION_MINUTE:
        flags.append("LATE_SESSION_OBSERVATION")
    if drawdown <= LATE_CLIMAX_DRAWDOWN_PCT:
        flags.append("PRICE_RETRACE_AFTER_PEAK")
    if active_windows >= MIN_PERSISTENT_WINDOWS and span_minutes >= MIN_PERSISTENT_SPAN_MINUTES:
        flags.append("PERSISTENT_ACTIVITY")
    else:
        flags.append("SINGLE_OR_SHORT_PULSE")

    if "PRICE_RETRACE_AFTER_PEAK" in flags and "LATE_SESSION_OBSERVATION" in flags:
        primary = "LATE_CLIMAX"
    elif active_windows < MIN_PERSISTENT_WINDOWS:
        primary = "UNSTABLE_SPIKE"
    elif (
        "EARLY_WINDOW_ACTIVITY" in flags
        and active[0].get("historical_pct_chg") is not None
        and _float(active[0].get("historical_pct_chg")) <= 1.0
    ):
        primary = "EARLY_ACTIVATION"
    elif "PERSISTENT_ACTIVITY" in flags and latest_price >= first_price:
        primary = "TREND_CONTINUATION"
    else:
        primary = "UNCLASSIFIED"
    return primary, flags


def _listing_age_days(point: Dict[str, Any], trade_date: str) -> Optional[int]:
    list_date = _date_text(point.get("list_date"))
    if not list_date:
        return None
    try:
        age = (
            datetime.strptime(trade_date, "%Y-%m-%d").date()
            - datetime.strptime(list_date, "%Y-%m-%d").date()
        ).days
    except ValueError:
        return None
    return age if age >= 0 else None


def _eligibility(point: Dict[str, Any], trade_date: str) -> str:
    market = str(point.get("market") or "").upper()
    symbol = str(point.get("symbol") or "").upper()
    if bool(point.get("is_st")):
        return "OBSERVE_ONLY_ST"
    if market in {"BJ", "北交所"} or symbol.endswith(".BJ"):
        return "OBSERVE_ONLY_BJ"
    if not (symbol.endswith(".SH") or symbol.endswith(".SZ")):
        return "OBSERVE_ONLY_UNSUPPORTED_MARKET"
    if market not in A_SHARE_MARKETS:
        return "OBSERVE_ONLY_UNSUPPORTED_MARKET"
    listing_age_days = _listing_age_days(point, trade_date)
    if listing_age_days is not None and listing_age_days < MIN_LISTING_AGE_DAYS:
        return "OBSERVE_ONLY_NEW_LISTING"
    return "OBSERVE_ONLY_A_SHARE"


def _derive_manifest(
    scans: List[Dict[str, Any]], trade_date: str
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Return quality-annotated scans and candidate diagnostics."""
    normalised_scans = []
    for raw_scan in scans:
        normalised = _normalise_scan(raw_scan, trade_date)
        if normalised:
            normalised_scans.append(normalised)
    ordered = sorted(normalised_scans, key=lambda item: item["scan_time"])
    previous: Dict[str, Dict[str, Any]] = {}
    session_scan_counts: Counter[str] = Counter()
    derived_scans: List[Dict[str, Any]] = []
    for scan in ordered:
        points: List[Dict[str, Any]] = []
        scan_quality = _scan_quality(scan)
        session = _trading_session(scan["scan_time"])
        session_scan_index = session_scan_counts[session]
        session_scan_counts[session] += 1
        session_baseline = session_scan_index == 0
        scan_quality.update({
            "scan_time": scan["scan_time"],
            "trading_session": session,
            "session_scan_index": session_scan_index,
            "session_baseline": session_baseline,
        })
        for quote in scan["quotes"]:
            symbol = quote["symbol"]
            prior = previous.get(symbol)
            current_volume = _float(quote["cumulative_volume"])
            current_amount = _float(quote["cumulative_amount"])
            delta_volume = None
            delta_amount = None
            counter_reset = False
            window_gap = False
            session_reset = False
            gap_minutes = None
            if prior is not None:
                prior_minute = _minute(prior["quote_time"])
                current_minute = _minute(quote["quote_time"])
                gap_minutes = current_minute - prior_minute
                session_reset = prior.get("trading_session") != session
                window_gap = bool(
                    not session_reset
                    and (
                        prior_minute < 0
                        or current_minute < 0
                        or gap_minutes <= 0
                        or gap_minutes > MAX_WINDOW_GAP_MINUTES
                    )
                )
                if not window_gap and not session_reset:
                    delta_volume = current_volume - prior["volume"]
                    delta_amount = current_amount - prior["amount"]
                    if delta_volume < 0 or delta_amount < 0:
                        counter_reset = True
                        delta_volume = None
                        delta_amount = None
            previous[symbol] = {
                "volume": current_volume,
                "amount": current_amount,
                "quote_time": quote["quote_time"],
                "trading_session": session,
            }
            points.append({
                **quote,
                "delta_volume": delta_volume,
                "delta_amount": delta_amount,
                "counter_reset": counter_reset,
                "window_gap": window_gap,
                "session_reset": session_reset,
                "session_baseline": session_baseline,
                "gap_minutes": gap_minutes,
                "coverage_ok": scan_quality["coverage_ok"],
                "active_window": False,
            })
        positive = [
            point["delta_amount"]
            for point in points
            if point["delta_amount"] is not None and point["delta_amount"] > 0
        ]
        required_sample_count = min(
            MIN_PERCENTILE_SAMPLE_COUNT,
            max(1, scan_quality["universe_count"]),
        )
        comparable_delta_count = sum(
            point.get("delta_amount") is not None
            and not point.get("counter_reset")
            and not point.get("window_gap")
            for point in points
        )
        if scan_quality["universe_count"] > 0:
            delta_coverage_ratio = round(
                comparable_delta_count / scan_quality["universe_count"],
                6,
            )
            delta_coverage_ok = delta_coverage_ratio >= MIN_REALTIME_COVERAGE
        else:
            delta_coverage_ratio = None
            delta_coverage_ok = False
        session_warmup = bool(
            session_scan_index == 1
            and scan_quality["coverage_ok"]
            and not delta_coverage_ok
        )
        sample_ok = len(positive) >= required_sample_count
        window_eligible = (
            not session_baseline
            and scan_quality["coverage_ok"]
            and delta_coverage_ok
            and sample_ok
        )
        threshold = _percentile(positive, ACTIVE_DELTA_PERCENTILE) if window_eligible else None
        for point in points:
            delta_amount = point.get("delta_amount")
            point["active_threshold_amount"] = threshold
            point["active_window"] = bool(
                window_eligible
                and delta_amount is not None
                and delta_amount > 0
                and delta_amount >= threshold
                and not point["counter_reset"]
                and not point["window_gap"]
            )
        counter_reset_count = sum(bool(point["counter_reset"]) for point in points)
        window_gap_count = sum(bool(point["window_gap"]) for point in points)
        scan_quality.update({
            "positive_delta_count": len(positive),
            "comparable_delta_count": comparable_delta_count,
            "delta_coverage_ratio": delta_coverage_ratio,
            "delta_coverage_ok": delta_coverage_ok,
            "session_warmup": session_warmup,
            "required_percentile_sample_count": required_sample_count,
            "percentile_sample_ok": sample_ok,
            "window_eligible": window_eligible,
            "counter_reset_count": counter_reset_count,
            "window_gap_count": window_gap_count,
            "active_window_count": sum(bool(point["active_window"]) for point in points),
            "active_threshold_amount": threshold,
        })
        if session_baseline:
            scan_quality["quality_status"] = "WAITING_SESSION_BASELINE"
        elif session_warmup:
            scan_quality["quality_status"] = "WAITING_SESSION_COVERAGE"
        elif scan_quality["coverage_ok"] and not delta_coverage_ok:
            scan_quality["quality_status"] = "PARTIAL_INSUFFICIENT_DELTA_COVERAGE"
        elif scan_quality["coverage_ok"] and not positive:
            scan_quality["quality_status"] = "WAITING_BASELINE"
        elif scan_quality["coverage_ok"] and not sample_ok:
            scan_quality["quality_status"] = "PARTIAL_INSUFFICIENT_SAMPLE"
        derived_scans.append({
            **scan,
            "points": points,
            "scan_quality": scan_quality,
        })

    for scan in derived_scans:
        by_industry: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for point in scan["points"]:
            industry = str(point.get("industry") or "").strip()
            if industry:
                by_industry[industry].append(point)
        for point in scan["points"]:
            industry = str(point.get("industry") or "").strip()
            peers = by_industry.get(industry, []) if industry else []
            peer_points = [peer for peer in peers if peer["symbol"] != point["symbol"]]
            peer_active = sum(bool(peer["active_window"]) for peer in peer_points)
            peer_observed = len(peer_points)
            point["peer_active_count"] = peer_active
            point["peer_observed_count"] = peer_observed
            point["peer_active_ratio"] = round(peer_active / peer_observed, 6) if peer_observed else 0.0

    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for scan in derived_scans:
        for point in scan["points"]:
            grouped[point["symbol"]].append(point)

    candidates: List[Dict[str, Any]] = []
    for symbol, points in sorted(grouped.items()):
        active = [point for point in points if point.get("active_window")]
        if not active:
            continue
        primary, flags = _primary_morphology(points)
        first_active = min(active, key=lambda point: point["quote_time"])
        last_active = max(active, key=lambda point: point["quote_time"])
        latest_quote = max(points, key=lambda point: point["quote_time"])
        prices = [_float(point["price"]) for point in points if _float(point["price"]) > 0]
        peak = max(prices) if prices else 0.0
        last_price = _float(last_active["price"])
        latest_price = _float(latest_quote["price"])
        drawdown = round((latest_price / peak - 1.0) * 100.0, 6) if peak else None
        active_minutes = [_minute(point["quote_time"]) for point in active]
        span_minutes = max(active_minutes) - min(active_minutes) if active_minutes else 0
        cumulative_vwap = None
        last_volume = _float(latest_quote.get("cumulative_volume"))
        last_amount = _float(latest_quote.get("cumulative_amount"))
        if last_volume > 0 and last_amount > 0:
            cumulative_vwap = round(last_amount * 10.0 / last_volume, 6)
        window_gap_count = sum(bool(point.get("window_gap")) for point in points)
        counter_reset_count = sum(bool(point.get("counter_reset")) for point in points)
        coverage_issue_count = sum(
            not bool(point.get("coverage_ok"))
            and not bool(point.get("session_baseline"))
            for point in points
        )
        if window_gap_count:
            data_quality = "PARTIAL_WINDOW_GAP"
        elif counter_reset_count:
            data_quality = "PARTIAL_COUNTER_RESET"
        elif coverage_issue_count:
            data_quality = "PARTIAL_LOW_COVERAGE"
        else:
            data_quality = "COMPLETE"
        eligible = _eligibility(first_active, trade_date)
        candidates.append({
            "candidate_id": _candidate_id(trade_date, symbol),
            "symbol": symbol,
            "trade_date": trade_date,
            "origin_type": _sector_origin(points),
            "primary_morphology": primary,
            "morphology_flags": flags,
            "candidate_status": (
                "PERSISTENT_ACTIVITY"
                if len(active) >= MIN_PERSISTENT_WINDOWS and span_minutes >= MIN_PERSISTENT_SPAN_MINUTES
                else "PULSE_ONLY"
            ),
            "eligibility": eligible,
            "observation_only": True,
            "no_trade_signal": True,
            "active_window_count": len(active),
            "valid_window_count": len(points),
            "active_span_minutes": span_minutes,
            "first_active_time": first_active["quote_time"],
            "last_active_time": last_active["quote_time"],
            "first_active_price": _float(first_active["price"]),
            "last_active_price": last_price,
            "last_observed_time": latest_quote["quote_time"],
            "last_observed_price": latest_price,
            "intraday_peak_price": peak,
            "intraday_peak_drawdown_pct": drawdown,
            "last_cumulative_vwap": cumulative_vwap,
            "active_delta_amount_total": round(sum(_float(point.get("delta_amount")) for point in active), 6),
            "industry": str(first_active.get("industry") or ""),
            "market": str(first_active.get("market") or ""),
            "is_st": bool(first_active.get("is_st")),
            "list_date": _date_text(first_active.get("list_date")),
            "listing_age_days": _listing_age_days(first_active, trade_date),
            "window_gap_count": window_gap_count,
            "counter_reset_count": counter_reset_count,
            "coverage_issue_count": coverage_issue_count,
            "observation_eligible": data_quality == "COMPLETE",
            "data_quality": data_quality,
            "blocked_actions": list(BLOCKED_ACTIONS),
        })
    return derived_scans, candidates


def derive_manifest_candidates(scans: List[Dict[str, Any]], trade_date: str) -> List[Dict[str, Any]]:
    """Derive point deltas, active windows, and daily candidate diagnostics."""
    _derived_scans, candidates = _derive_manifest(scans, trade_date)
    return candidates


def build_manifest(scans: List[Dict[str, Any]], trade_date: str, generated_at: Optional[str] = None) -> Dict[str, Any]:
    derived_scans, diagnostic_candidates = _derive_manifest(scans, trade_date)
    candidates = [
        candidate
        for candidate in diagnostic_candidates
        if candidate.get("observation_eligible")
    ]
    times = [scan["scan_time"] for scan in derived_scans]
    invalid = sum(int((scan.get("quote_stats") or {}).get("stale_quote_count", 0) or 0) for scan in derived_scans)
    scan_qualities = [scan["scan_quality"] for scan in derived_scans]
    coverage_values = [
        quality["coverage_ratio"]
        for quality in scan_qualities
        if quality.get("coverage_ratio") is not None
    ]
    baseline_scan_count = sum(bool(quality.get("session_baseline")) for quality in scan_qualities)
    warmup_scan_count = sum(bool(quality.get("session_warmup")) for quality in scan_qualities)
    low_coverage_count = sum(
        not bool(quality.get("coverage_ok"))
        and not bool(quality.get("session_baseline"))
        for quality in scan_qualities
    )
    gap_count = sum(int(quality.get("window_gap_count", 0) or 0) for quality in scan_qualities)
    reset_count = sum(int(quality.get("counter_reset_count", 0) or 0) for quality in scan_qualities)
    insufficient_sample_count = sum(
        not bool(quality.get("percentile_sample_ok"))
        for quality in scan_qualities
        if quality.get("coverage_ok")
        and quality.get("delta_coverage_ok")
        and not quality.get("session_baseline")
    )
    insufficient_delta_coverage_count = sum(
        quality.get("coverage_ok")
        and not quality.get("delta_coverage_ok")
        and not quality.get("session_baseline")
        and not quality.get("session_warmup")
        for quality in scan_qualities
    )
    if not derived_scans:
        data_quality = "PARTIAL_NO_VALID_SCANS"
    elif low_coverage_count:
        data_quality = "PARTIAL_LOW_COVERAGE"
    elif gap_count:
        data_quality = "PARTIAL_WINDOW_GAP"
    elif reset_count:
        data_quality = "PARTIAL_COUNTER_RESET"
    elif insufficient_delta_coverage_count:
        data_quality = "PARTIAL_INSUFFICIENT_DELTA_COVERAGE"
    elif insufficient_sample_count:
        data_quality = "PARTIAL_INSUFFICIENT_SAMPLE"
    elif len(derived_scans) < 2:
        data_quality = "PARTIAL_INSUFFICIENT_SCANS"
    else:
        data_quality = "COMPLETE"
    by_origin = Counter(candidate["origin_type"] for candidate in candidates)
    by_morphology = Counter(candidate["primary_morphology"] for candidate in candidates)
    return {
        "schema_version": SCHEMA_VERSION,
        "rule_version": RULE_VERSION,
        "trade_date": trade_date,
        "generated_at": generated_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "Eagle/realtime_snapshot",
        "snapshot_policy": "real_cumulative_quote_only; interval_delta_requires_prior_real_snapshot",
        "scan_count": len(derived_scans),
        "first_scan_time": min(times) if times else None,
        "last_scan_time": max(times) if times else None,
        "invalid_quote_count_reported": invalid,
        "candidate_count": len(candidates),
        "diagnostic_candidate_count": len(diagnostic_candidates) - len(candidates),
        "candidate_count_by_origin": dict(by_origin),
        "candidate_count_by_morphology": dict(by_morphology),
        "coverage": {
            "minimum": min(coverage_values) if coverage_values else None,
            "average": round(sum(coverage_values) / len(coverage_values), 6) if coverage_values else None,
            "minimum_required": MIN_REALTIME_COVERAGE,
            "low_coverage_scan_count": low_coverage_count,
        },
        "scan_quality": scan_qualities,
        "session_baseline_scan_count": baseline_scan_count,
        "session_warmup_scan_count": warmup_scan_count,
        "window_gap_count": gap_count,
        "counter_reset_count": reset_count,
        "insufficient_delta_coverage_scan_count": insufficient_delta_coverage_count,
        "insufficient_sample_scan_count": insufficient_sample_count,
        "percentile_sample_minimum": MIN_PERCENTILE_SAMPLE_COUNT,
        "observation_only": True,
        "no_trade_signal": True,
        "data_quality": data_quality,
        "blocked_actions": list(BLOCKED_ACTIONS),
        "candidates": candidates,
        "diagnostic_candidates": [
            candidate
            for candidate in diagnostic_candidates
            if not candidate.get("observation_eligible")
        ],
    }


def render_preview(manifest: Dict[str, Any]) -> str:
    lines = [
        f"# Eagle Active Path Preview - {manifest['trade_date']}",
        "",
        "- schema_version: " + str(manifest["schema_version"]),
        "- rule_version: " + str(manifest["rule_version"]),
        "- scan_count: " + str(manifest["scan_count"]),
        "- candidate_count: " + str(manifest["candidate_count"]),
        "- diagnostic_candidate_count: " + str(manifest.get("diagnostic_candidate_count", 0)),
        "- data_quality: " + str(manifest["data_quality"]),
        "- coverage: " + str(manifest.get("coverage", {})),
        "- session_baseline_scan_count: " + str(manifest.get("session_baseline_scan_count", 0)),
        "- session_warmup_scan_count: " + str(manifest.get("session_warmup_scan_count", 0)),
        "- window_gap_count: " + str(manifest.get("window_gap_count", 0)),
        "- counter_reset_count: " + str(manifest.get("counter_reset_count", 0)),
        "- observation_only: true",
        "- no_trade_signal: true",
        "",
        "## Candidate Summary",
        "",
        "| Symbol | Origin | Morphology | Windows | Span min | Eligibility | Quality |",
        "|---|---|---|---:|---:|---|---|",
    ]
    for candidate in manifest["candidates"]:
        lines.append(
            "| {symbol} | {origin_type} | {primary_morphology} | {active_window_count} | {active_span_minutes} | {eligibility} | {data_quality} |".format(**candidate)
        )
    if manifest.get("diagnostic_candidate_count", 0):
        lines.extend([
            "",
            "## Diagnostic Candidates Excluded From Evaluation",
            "",
            "These rows were observed but excluded from the comparable candidate set because their window continuity or coverage failed.",
            "",
            "| Symbol | Quality | Gap windows | Counter resets | Coverage issues |",
            "|---|---|---:|---:|---:|",
        ])
        for candidate in manifest.get("diagnostic_candidates", []):
            lines.append(
                "| {symbol} | {data_quality} | {window_gap_count} | {counter_reset_count} | {coverage_issue_count} |".format(**candidate)
            )
    lines.extend([
        "",
        "This artifact is an observation-only discovery record.",
        "It does not create validation tasks, call L2-L4, write DuckDB, Shadow, RAG, nexus_audits, or trigger trades.",
    ])
    return "\n".join(lines) + "\n"


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent), text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)


class EagleActivePathObserver:
    """Durable daily accumulator for the existing Eagle scan."""

    def __init__(self, trade_date: str, report_dir: Path | str = REPORT_DIR):
        self.trade_date = _date_text(trade_date)
        if not self.trade_date:
            raise ValueError(f"invalid trade_date: {trade_date}")
        self.report_dir = Path(report_dir)
        self.window_path = self.report_dir / "windows" / f"eagle_windows_{self.trade_date}.jsonl"
        self.manifest_path = self.report_dir / "manifests" / f"eagle_candidates_{self.trade_date}.json"
        self.preview_path = self.report_dir / "previews" / f"eagle_preview_{self.trade_date}.md"
        self._lock = threading.Lock()
        self._scans: List[Dict[str, Any]] = []
        self._scan_ids: set[str] = set()
        self.recovery_warnings: List[str] = []
        self._load_existing()

    def _load_existing(self) -> None:
        if not self.window_path.exists():
            return
        try:
            with self.window_path.open("r", encoding="utf-8") as handle:
                for line_number, line in enumerate(handle, 1):
                    if not line.strip():
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        self.recovery_warnings.append(f"invalid_json_line:{line_number}")
                        continue
                    normalised = _normalise_scan(record, self.trade_date)
                    if not normalised:
                        self.recovery_warnings.append(f"invalid_scan_line:{line_number}")
                        continue
                    if normalised["scan_id"] not in self._scan_ids:
                        self._scan_ids.add(normalised["scan_id"])
                        self._scans.append(normalised)
        except OSError as exc:
            self.recovery_warnings.append(f"read_error:{type(exc).__name__}")

    def ingest_scan(
        self,
        watchlist: Iterable[Dict[str, Any]],
        *,
        scan_time: Any = None,
        quote_stats: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        raw = {
            "scan_time": scan_time or datetime.now(),
            "quotes": list(watchlist),
            "quote_stats": dict(quote_stats or {}),
        }
        record = _normalise_scan(raw, self.trade_date)
        if not record:
            return {"status": "NO_VALID_REALTIME_QUOTES", "recorded": False}
        with self._lock:
            if record["scan_id"] in self._scan_ids:
                return {"status": "DUPLICATE_SCAN", "recorded": False, "scan_id": record["scan_id"]}
            self.window_path.parent.mkdir(parents=True, exist_ok=True)
            line = json.dumps({"schema_version": SCHEMA_VERSION, **record}, ensure_ascii=False, sort_keys=True)
            with self.window_path.open("a", encoding="utf-8", newline="\n") as handle:
                handle.write(line + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            self._scan_ids.add(record["scan_id"])
            self._scans.append(record)
        return {
            "status": "RECORDED",
            "recorded": True,
            "scan_id": record["scan_id"],
            "quote_count": len(record["quotes"]),
        }

    def finalize(self, generated_at: Optional[str] = None) -> Dict[str, Any]:
        with self._lock:
            manifest = build_manifest(self._scans, self.trade_date, generated_at=generated_at)
            if self.recovery_warnings:
                manifest["data_quality"] = "PARTIAL_RECOVERY"
                manifest["recovery_warnings"] = list(self.recovery_warnings)
            manifest["window_file"] = str(self.window_path)
            _atomic_write(self.manifest_path, json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
            _atomic_write(self.preview_path, render_preview(manifest))
            return manifest


__all__ = [
    "BLOCKED_ACTIONS",
    "EagleActivePathObserver",
    "REPORT_DIR",
    "RULE_VERSION",
    "SCHEMA_VERSION",
    "build_manifest",
    "derive_manifest_candidates",
    "render_preview",
]
