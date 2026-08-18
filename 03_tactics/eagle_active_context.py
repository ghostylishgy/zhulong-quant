#!/usr/bin/env python3
"""Post-close context reconstruction for Eagle Active Path candidates.

The intraday observer answers only whether a stock was persistently active.
This module combines that frozen evidence with point-in-time daily/RPS and
industry context to decide whether the candidate deserves later audit work.
It remains observation-only and never emits a trade or validation task.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import statistics
import tempfile
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence


SCHEMA_VERSION = "eagle_active_context_v0.1"
RULE_VERSION = "eagle_active_context_rules_v0.1"
MIN_HISTORY_DAYS = 10
MIN_PERSISTENT_WINDOWS = 4
MIN_PERSISTENT_SPAN_MINUTES = 30
MAX_CONSTRUCTIVE_RET_3D_PCT = 12.0
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


def _pct(entry: float, exit_price: float) -> Optional[float]:
    if entry <= 0 or exit_price <= 0:
        return None
    return round((exit_price / entry - 1.0) * 100.0, 6)


def _mean(values: Iterable[Any]) -> Optional[float]:
    numbers = [_float(value) for value in values if value is not None]
    return round(statistics.fmean(numbers), 6) if numbers else None


def _median(values: Iterable[Any]) -> Optional[float]:
    numbers = [_float(value) for value in values if value is not None]
    return round(statistics.median(numbers), 6) if numbers else None


def _zscore(value: Any, baseline: Iterable[Any]) -> Optional[float]:
    numbers = [_float(item) for item in baseline if item is not None]
    if len(numbers) < 5:
        return None
    std = statistics.pstdev(numbers)
    return round((_float(value) - statistics.fmean(numbers)) / std, 4) if std > 0 else 0.0


def _sha256_json(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
    )
    temp_path = Path(handle.name)
    try:
        with handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
    finally:
        if temp_path.exists():
            temp_path.unlink()


def _history_features(history: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    ordered = sorted(history, key=lambda row: str(row.get("trade_date") or ""))
    current = ordered[-1]
    closes = [_float(row.get("close")) for row in ordered]
    recent3 = ordered[-3:]
    recent5 = ordered[-5:]
    current_close = closes[-1]
    current_ma20 = _float(current.get("ma20"))
    prior_ma20 = _float(ordered[-4].get("ma20")) if len(ordered) >= 4 else 0.0
    ret_3d = _pct(_float(ordered[-4].get("close")), current_close) if len(ordered) >= 4 else None
    ret_5d = _pct(_float(ordered[-6].get("close")), current_close) if len(ordered) >= 6 else None
    prior_volumes = [
        _float(row.get("vol"))
        for row in ordered[-6:-1]
        if _float(row.get("vol")) > 0
    ]
    avg_volume = statistics.fmean(prior_volumes) if prior_volumes else 0.0
    volume_ratio = _float(current.get("vol")) / avg_volume if avg_volume > 0 else None
    current_open = _float(current.get("open"))
    current_high = _float(current.get("high"))
    current_low = _float(current.get("low"))
    span = current_high - current_low
    rps_current = _float(current.get("rps_10"))
    rps_old = _float(ordered[-4].get("rps_10")) if len(ordered) >= 4 else 0.0
    pct_changes = [_float(row.get("pct_chg")) for row in recent3]
    total_abs_change = sum(abs(value) for value in pct_changes)

    return {
        "history_days": len(ordered),
        "ret_1d_pct": _float(current.get("pct_chg")),
        "ret_3d_pct": ret_3d,
        "ret_5d_pct": ret_5d,
        "positive_days_3d": sum(value > 0 for value in pct_changes),
        "today_move_share_3d": round(abs(pct_changes[-1]) / total_abs_change, 4) if total_abs_change else 0.0,
        "volume_ratio_prev5": round(volume_ratio, 4) if volume_ratio is not None else None,
        "turnover_zscore_20": _zscore(
            current.get("turnover_rate"),
            [row.get("turnover_rate") for row in ordered[-21:-1]],
        ),
        "rps_10": round(rps_current, 4),
        "rps_10_change_3d": round(rps_current - rps_old, 4) if len(ordered) >= 4 else None,
        "close_above_ma20": bool(current_ma20 > 0 and current_close > current_ma20),
        "crossed_above_ma20_3d": bool(
            current_ma20 > 0
            and current_close > current_ma20
            and any(
                _float(row.get("ma20")) > 0
                and _float(row.get("close")) <= _float(row.get("ma20"))
                for row in ordered[-4:-1]
            )
        ),
        "ma20_slope_3d_pct": (
            round((current_ma20 / prior_ma20 - 1.0) * 100.0, 6)
            if current_ma20 > 0 and prior_ma20 > 0
            else None
        ),
        "distance_from_ma20_pct": (
            round((current_close / current_ma20 - 1.0) * 100.0, 6)
            if current_close > 0 and current_ma20 > 0
            else None
        ),
        "close_location": round((current_close - current_low) / span, 4) if span > 0 else 0.5,
        "upper_shadow_ratio": (
            round((current_high - max(current_open, current_close)) / span, 4)
            if span > 0
            else 0.0
        ),
        "continuous_volume_expansion_3d": bool(
            len(recent3) == 3
            and _float(recent3[1].get("vol")) > _float(recent3[0].get("vol"))
            and _float(recent3[2].get("vol")) > _float(recent3[1].get("vol"))
        ),
        "five_day_positive_ratio": round(
            sum(_float(row.get("pct_chg")) > 0 for row in recent5) / len(recent5),
            4,
        ),
    }


def _build_market_context(
    market_rows: Sequence[Mapping[str, Any]],
    trade_date: str,
    raw_candidates: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    by_symbol: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    current_rows: List[Mapping[str, Any]] = []
    for row in market_rows:
        symbol = str(row.get("symbol") or "").upper()
        row_date = str(row.get("trade_date") or "")[:10]
        if not symbol:
            continue
        by_symbol[symbol].append(row)
        if row_date == trade_date:
            current_rows.append(row)

    stock_ret3: Dict[str, Optional[float]] = {}
    for symbol, rows in by_symbol.items():
        ordered = sorted(rows, key=lambda item: str(item.get("trade_date") or ""))
        if ordered and str(ordered[-1].get("trade_date") or "")[:10] == trade_date:
            stock_ret3[symbol] = (
                _pct(_float(ordered[-4].get("close")), _float(ordered[-1].get("close")))
                if len(ordered) >= 4
                else None
            )

    market_median_1d = _median(row.get("pct_chg") for row in current_rows)
    market_median_3d = _median(stock_ret3.get(str(row.get("symbol") or "").upper()) for row in current_rows)
    industry_rows: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in current_rows:
        industry = str(row.get("industry") or "").strip()
        if industry:
            industry_rows[industry].append(row)

    active_counts: Counter[str] = Counter()
    persistent_counts: Counter[str] = Counter()
    for candidate in raw_candidates:
        industry = str(candidate.get("industry") or "").strip()
        if not industry:
            continue
        active_counts[industry] += 1
        if str(candidate.get("candidate_status") or "") == "PERSISTENT_ACTIVITY":
            persistent_counts[industry] += 1

    industries: Dict[str, Dict[str, Any]] = {}
    for industry, rows in industry_rows.items():
        one_day = [_float(row.get("pct_chg")) for row in rows]
        three_day = [
            stock_ret3.get(str(row.get("symbol") or "").upper())
            for row in rows
        ]
        valid_three_day = [value for value in three_day if value is not None]
        ma_states = [
            _float(row.get("close")) > _float(row.get("ma20"))
            for row in rows
            if _float(row.get("ma20")) > 0
        ]
        advancer_ratio = sum(value > 0 for value in one_day) / len(one_day) if one_day else None
        median_1d = _median(one_day)
        median_3d = _median(valid_three_day)
        above_ma20 = sum(ma_states) / len(ma_states) if ma_states else None
        relative_3d = (
            round(median_3d - market_median_3d, 6)
            if median_3d is not None and market_median_3d is not None
            else None
        )
        if (
            advancer_ratio is not None
            and advancer_ratio >= 0.55
            and relative_3d is not None
            and relative_3d >= 0.5
        ):
            status = "SECTOR_SUPPORTIVE"
        elif (
            advancer_ratio is not None
            and advancer_ratio <= 0.35
            and relative_3d is not None
            and relative_3d <= -0.5
        ):
            status = "SECTOR_ADVERSE"
        else:
            status = "SECTOR_MIXED"
        industries[industry] = {
            "industry": industry,
            "status": status,
            "stock_count": len(rows),
            "advancer_ratio": round(advancer_ratio, 4) if advancer_ratio is not None else None,
            "median_1d_pct": median_1d,
            "median_3d_pct": median_3d,
            "sector_minus_market_3d_pct": relative_3d,
            "above_ma20_ratio": round(above_ma20, 4) if above_ma20 is not None else None,
            "eagle_active_count": active_counts[industry],
            "eagle_persistent_count": persistent_counts[industry],
            "mapping_quality": "current_industry_approximation",
        }

    return {
        "by_symbol": by_symbol,
        "stock_ret3": stock_ret3,
        "market": {
            "stock_count": len(current_rows),
            "median_1d_pct": market_median_1d,
            "median_3d_pct": market_median_3d,
        },
        "industries": industries,
    }


def _route_candidate(
    raw: Mapping[str, Any],
    features: Optional[Mapping[str, Any]],
    sector: Mapping[str, Any],
) -> Dict[str, Any]:
    evidence: List[str] = []
    risks: List[str] = []
    missing: List[str] = []

    eligibility = str(raw.get("eligibility") or "")
    persistent = (
        str(raw.get("candidate_status") or "") == "PERSISTENT_ACTIVITY"
        and int(raw.get("active_window_count") or 0) >= MIN_PERSISTENT_WINDOWS
        and int(raw.get("active_span_minutes") or 0) >= MIN_PERSISTENT_SPAN_MINUTES
    )
    if persistent:
        evidence.append("INTRADAY_PERSISTENCE")
    else:
        risks.append("INTRADAY_PULSE_ONLY")
    if str(raw.get("origin_type") or "") == "SECTOR_RESONANCE":
        evidence.append("INTRADAY_SECTOR_RESONANCE")
    if str(raw.get("primary_morphology") or "") == "LATE_CLIMAX":
        risks.append("INTRADAY_LATE_CLIMAX")
    if _float(raw.get("intraday_peak_drawdown_pct")) <= -2.0:
        risks.append("INTRADAY_PEAK_DRAWDOWN")

    if not features:
        missing.append("DAILY_HISTORY")
    else:
        ret3 = features.get("ret_3d_pct")
        rps_change = features.get("rps_10_change_3d")
        if (
            features.get("positive_days_3d", 0) >= 2
            and ret3 is not None
            and 0 < _float(ret3) <= MAX_CONSTRUCTIVE_RET_3D_PCT
        ):
            evidence.append("THREE_DAY_CONSTRUCTIVE_PATH")
        if _float(features.get("rps_10")) >= 60 and rps_change is not None and _float(rps_change) >= 0:
            evidence.append("RPS_RISING_OR_STABLE_HIGH")
        if features.get("close_above_ma20") and _float(features.get("ma20_slope_3d_pct")) >= 0:
            evidence.append("MA20_CONSTRUCTIVE")
        if features.get("crossed_above_ma20_3d"):
            evidence.append("MA20_RECENT_RECLAIM")
        volume_ratio = features.get("volume_ratio_prev5")
        if volume_ratio is not None and 1.0 <= _float(volume_ratio) <= 3.0:
            evidence.append("VOLUME_CONFIRMATION")
        if _float(features.get("close_location"), 0.5) >= 0.55:
            evidence.append("CLOSE_LOCATION_HEALTHY")
        if ret3 is not None and _float(ret3) <= -2.0:
            risks.append("THREE_DAY_PATH_WEAK")
        if ret3 is not None and _float(ret3) > MAX_CONSTRUCTIVE_RET_3D_PCT:
            risks.append("THREE_DAY_OVEREXTENDED")
        if _float(features.get("ret_1d_pct")) > 8.0:
            risks.append("SINGLE_DAY_OVEREXTENDED")
        if (
            _float(features.get("ret_1d_pct")) > 5.0
            and _float(features.get("today_move_share_3d")) > 0.75
        ):
            risks.append("SINGLE_DAY_DOMINATES_PATH")
        if _float(features.get("rps_10")) < 50 and rps_change is not None and _float(rps_change) < 0:
            risks.append("RPS_LOW_AND_FALLING")
        if _float(features.get("upper_shadow_ratio")) >= 0.45:
            risks.append("LONG_UPPER_SHADOW")
        if volume_ratio is not None and _float(volume_ratio) >= 3.5 and _float(features.get("close_location"), 0.5) < 0.55:
            risks.append("VOLUME_EXHAUSTION_RISK")
        if features.get("history_days", 0) < MIN_HISTORY_DAYS:
            missing.append("MINIMUM_HISTORY")

    sector_status = str(sector.get("status") or "SECTOR_UNAVAILABLE")
    if sector_status == "SECTOR_SUPPORTIVE":
        evidence.append("POST_CLOSE_SECTOR_SUPPORT")
    elif sector_status == "SECTOR_ADVERSE":
        risks.append("POST_CLOSE_SECTOR_ADVERSE")
    if int(sector.get("eagle_persistent_count") or 0) >= 2:
        evidence.append("MULTI_STOCK_ACTIVE_SECTOR")

    stock_ret3 = features.get("ret_3d_pct") if features else None
    sector_ret3 = sector.get("median_3d_pct")
    stock_minus_sector = (
        round(_float(stock_ret3) - _float(sector_ret3), 6)
        if stock_ret3 is not None and sector_ret3 is not None
        else None
    )
    if stock_minus_sector is not None and stock_minus_sector >= 0.5:
        evidence.append("STOCK_OUTPERFORMS_SECTOR_3D")

    hard_watch = eligibility != "OBSERVE_ONLY_A_SHARE" or bool(missing)
    risk_watch = (
        "INTRADAY_LATE_CLIMAX" in risks
        or "INTRADAY_PEAK_DRAWDOWN" in risks
        or "THREE_DAY_PATH_WEAK" in risks
        or "THREE_DAY_OVEREXTENDED" in risks
        or "SINGLE_DAY_OVEREXTENDED" in risks
        or "SINGLE_DAY_DOMINATES_PATH" in risks
        or "RPS_LOW_AND_FALLING" in risks
        or "LONG_UPPER_SHADOW" in risks
        or "VOLUME_EXHAUSTION_RISK" in risks
        or "POST_CLOSE_SECTOR_ADVERSE" in risks
    )
    path_evidence = {
        "THREE_DAY_CONSTRUCTIVE_PATH",
        "RPS_RISING_OR_STABLE_HIGH",
        "MA20_CONSTRUCTIVE",
        "MA20_RECENT_RECLAIM",
    }
    context_evidence = {
        "POST_CLOSE_SECTOR_SUPPORT",
        "MULTI_STOCK_ACTIVE_SECTOR",
        "STOCK_OUTPERFORMS_SECTOR_3D",
    }
    path_count = len(path_evidence.intersection(evidence))
    context_count = len(context_evidence.intersection(evidence))

    constructive_path = "THREE_DAY_CONSTRUCTIVE_PATH" in evidence
    structure_confirmation = bool({
        "RPS_RISING_OR_STABLE_HIGH",
        "MA20_CONSTRUCTIVE",
        "MA20_RECENT_RECLAIM",
    }.intersection(evidence))
    relative_confirmation = (
        "POST_CLOSE_SECTOR_SUPPORT" in evidence
        or (
            "STOCK_OUTPERFORMS_SECTOR_3D" in evidence
            and features is not None
            and _float(features.get("rps_10_change_3d")) >= 5.0
        )
    )

    if hard_watch:
        route = "WATCH_ONLY"
    elif not persistent:
        route = "REJECTED_NOISE"
    elif risk_watch:
        route = "WATCH_ONLY"
    elif (
        constructive_path
        and structure_confirmation
        and relative_confirmation
        and "CLOSE_LOCATION_HEALTHY" in evidence
    ):
        route = "AUDIT_READY_OBSERVER"
    else:
        route = "WATCH_ONLY"

    if route == "AUDIT_READY_OBSERVER":
        if features and features.get("crossed_above_ma20_3d"):
            archetype = "EARLY_MOMENTUM_RECLAIM"
        elif "POST_CLOSE_SECTOR_SUPPORT" in evidence:
            archetype = "SECTOR_LED_MOMENTUM"
        else:
            archetype = "STOCK_SPECIFIC_MOMENTUM"
    elif "INTRADAY_LATE_CLIMAX" in risks or "VOLUME_EXHAUSTION_RISK" in risks:
        archetype = "LATE_CLIMAX_RISK"
    elif not persistent:
        archetype = "PULSE_NOISE"
    else:
        archetype = "UNCONFIRMED_ACTIVITY"

    return {
        "route": route,
        "context_archetype": archetype,
        "positive_evidence": sorted(set(evidence)),
        "risk_evidence": sorted(set(risks)),
        "missing_evidence": sorted(set(missing)),
        "path_evidence_count": path_count,
        "context_evidence_count": context_count,
        "stock_minus_sector_3d_pct": stock_minus_sector,
    }


def build_context_manifest(
    raw_manifest: Mapping[str, Any],
    market_rows: Sequence[Mapping[str, Any]],
    *,
    generated_at: Optional[str] = None,
) -> Dict[str, Any]:
    """Build a deterministic post-close observer manifest."""
    trade_date = str(raw_manifest.get("trade_date") or "")[:10]
    if len(trade_date) != 10:
        raise ValueError("EAGLE_CONTEXT_INVALID_TRADE_DATE")
    if not raw_manifest.get("observation_only") or not raw_manifest.get("no_trade_signal"):
        raise ValueError("EAGLE_CONTEXT_UNSAFE_SOURCE_MANIFEST")

    raw_candidates = [
        candidate
        for candidate in raw_manifest.get("candidates") or []
        if isinstance(candidate, Mapping)
    ]
    source_quality = str(raw_manifest.get("data_quality") or "")
    market_context = _build_market_context(market_rows, trade_date, raw_candidates)
    output_candidates: List[Dict[str, Any]] = []
    for raw in raw_candidates:
        symbol = str(raw.get("symbol") or "").upper()
        history = [
            row
            for row in market_context["by_symbol"].get(symbol, [])
            if str(row.get("trade_date") or "")[:10] <= trade_date
        ]
        has_current = bool(
            history and str(history[-1].get("trade_date") or "")[:10] == trade_date
        )
        features = _history_features(history) if has_current and len(history) >= 4 else None
        industry = str(raw.get("industry") or "").strip()
        sector = dict(
            market_context["industries"].get(
                industry,
                {
                    "industry": industry,
                    "status": "SECTOR_UNAVAILABLE",
                    "mapping_quality": "unavailable",
                },
            )
        )
        route = _route_candidate(raw, features, sector)
        if source_quality != "COMPLETE" or str(raw.get("data_quality") or "") != "COMPLETE":
            route["route"] = "WATCH_ONLY"
            route["context_archetype"] = "SOURCE_QUALITY_INCOMPLETE"
            route["missing_evidence"] = sorted(
                set(route["missing_evidence"] + ["SOURCE_MANIFEST_QUALITY"])
            )
        output_candidates.append({
            "candidate_id": str(raw.get("candidate_id") or ""),
            "symbol": symbol,
            "trade_date": trade_date,
            "industry": industry,
            "market": str(raw.get("market") or ""),
            "eligibility": str(raw.get("eligibility") or ""),
            "raw_origin_type": str(raw.get("origin_type") or ""),
            "raw_morphology": str(raw.get("primary_morphology") or ""),
            "raw_candidate_status": str(raw.get("candidate_status") or ""),
            "active_window_count": int(raw.get("active_window_count") or 0),
            "active_span_minutes": int(raw.get("active_span_minutes") or 0),
            "intraday_peak_drawdown_pct": raw.get("intraday_peak_drawdown_pct"),
            "daily_path": features,
            "sector_context": sector,
            **route,
            "observation_only": True,
            "no_trade_signal": True,
            "generate_task": False,
            "manual_review_required": True,
            "blocked_actions": list(BLOCKED_ACTIONS),
        })

    route_counts = Counter(candidate["route"] for candidate in output_candidates)
    archetype_counts = Counter(candidate["context_archetype"] for candidate in output_candidates)
    raw_sha = _sha256_json(raw_manifest)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "rule_version": RULE_VERSION,
        "trade_date": trade_date,
        "generated_at": generated_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "EagleActivePath/post_close_context",
        "source_manifest_sha256": raw_sha,
        "source_schema_version": str(raw_manifest.get("schema_version") or ""),
        "source_rule_version": str(raw_manifest.get("rule_version") or ""),
        "source_candidate_count": len(raw_candidates),
        "source_data_quality": source_quality,
        "candidate_count": len(output_candidates),
        "route_counts": dict(sorted(route_counts.items())),
        "archetype_counts": dict(sorted(archetype_counts.items())),
        "market_context": market_context["market"],
        "context_window": {
            "primary_path_days": 3,
            "normalization_days": [5, 10, 20],
            "point_in_time_as_of": trade_date,
            "industry_mapping": "current_industry_approximation",
        },
        "routing_policy": (
            "intraday persistence plus constructive multi-day path and sector/relative context; "
            "no fixed quota; zero audit-ready candidates is valid"
        ),
        "observation_only": True,
        "no_trade_signal": True,
        "generate_task": False,
        "manual_review_required": True,
        "blocked_actions": list(BLOCKED_ACTIONS),
        "candidates": output_candidates,
    }
    manifest["content_sha256"] = _sha256_json(manifest)
    return manifest


def render_preview(manifest: Mapping[str, Any]) -> str:
    rows = list(manifest.get("candidates") or [])
    lines = [
        f"# Eagle Active Context Preview - {manifest.get('trade_date', '')}",
        "",
        "## 1. Batch",
        "",
        f"- schema: `{manifest.get('schema_version', '')}`",
        f"- rule: `{manifest.get('rule_version', '')}`",
        f"- source candidates: `{manifest.get('source_candidate_count', 0)}`",
        f"- source sha256: `{manifest.get('source_manifest_sha256', '')}`",
        "- mode: `OBSERVE_ONLY`",
        "- no_trade_signal: `true`",
        "- generate_task: `false`",
        "",
        "## 2. Routing Summary",
        "",
        "| Route | Count |",
        "|---|---:|",
    ]
    for route in ("AUDIT_READY_OBSERVER", "WATCH_ONLY", "REJECTED_NOISE"):
        lines.append(f"| {route} | {int((manifest.get('route_counts') or {}).get(route, 0))} |")

    lines.extend([
        "",
        "## 3. Audit-ready Observer Candidates",
        "",
        "| Symbol | Industry | Archetype | 3d Return | RPS10 | Evidence | Risks |",
        "|---|---|---|---:|---:|---|---|",
    ])
    ready = [row for row in rows if row.get("route") == "AUDIT_READY_OBSERVER"]
    for row in ready:
        path = row.get("daily_path") or {}
        lines.append(
            "| {symbol} | {industry} | {archetype} | {ret3} | {rps} | {evidence} | {risks} |".format(
                symbol=row.get("symbol", ""),
                industry=str(row.get("industry") or "").replace("|", "/"),
                archetype=row.get("context_archetype", ""),
                ret3=path.get("ret_3d_pct"),
                rps=path.get("rps_10"),
                evidence="; ".join(row.get("positive_evidence") or []),
                risks="; ".join(row.get("risk_evidence") or []),
            )
        )
    if not ready:
        lines.append("| - | - | - | - | - | No candidate met the evidence gates | - |")

    lines.extend([
        "",
        "## 4. Boundary",
        "",
        "This artifact identifies candidates worth later audit review. It does not predict the next-day direction,",
        "does not create validation tasks, and does not enter L1/L1.5, L2-L4, Shadow, RAG, Nexus or trading.",
        "",
    ])
    return "\n".join(lines)


def write_artifacts(manifest: Mapping[str, Any], output_dir: Path | str) -> Dict[str, str]:
    root = Path(output_dir)
    trade_date = str(manifest.get("trade_date") or "")
    manifest_path = root / "manifests" / f"eagle_context_{trade_date}.json"
    preview_path = root / "previews" / f"eagle_context_{trade_date}.md"
    _atomic_write(
        manifest_path,
        json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2, default=str) + "\n",
    )
    _atomic_write(preview_path, render_preview(manifest))
    return {
        "manifest_path": str(manifest_path),
        "preview_path": str(preview_path),
    }


__all__ = [
    "BLOCKED_ACTIONS",
    "RULE_VERSION",
    "SCHEMA_VERSION",
    "build_context_manifest",
    "render_preview",
    "write_artifacts",
]
