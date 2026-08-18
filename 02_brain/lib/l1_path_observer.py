#!/usr/bin/env python3
"""Deterministic path scoring for the read-only L1 Path Observer."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, List

PATHS = (
    "FRESH_IGNITION",
    "PERSISTENT_LEADER",
    "ACCELERATING",
    "EXHAUSTED_SPIKE",
    "REVERSAL_BOUNCE",
)
UNCLASSIFIED = "UNCLASSIFIED"
OBSERVER_VERSION = "l1_path_observer_v0.1"
FIXED_QUOTAS = {"A1": 10, "A2": 20, "B": 30, "C": 20}


def clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, float(value)))


def scaled(value: float, start: float, full: float) -> float:
    if full <= start:
        return float(value >= full)
    return clamp((float(value) - start) / (full - start))


def board_limit_pct(symbol: str, is_st: bool = False) -> float:
    if is_st:
        return 5.0
    code = str(symbol).split(".")[0]
    if str(symbol).endswith(".BJ") or code.startswith(("4", "8", "9")):
        return 30.0
    if code.startswith(("300", "301", "688")):
        return 20.0
    return 10.0


def account_context(symbol: str, is_st: bool) -> Dict[str, Any]:
    if is_st:
        return {"status": "OBSERVE_ONLY_ST", "allow_execution": False}
    if str(symbol).endswith(".BJ"):
        return {"status": "OBSERVE_ONLY_BJ", "allow_execution": False}
    return {"status": "ELIGIBLE_A_SHARE", "allow_execution": True}


@dataclass(frozen=True)
class PathFeatures:
    ret_1d: float = 0.0
    ret_2d: float = 0.0
    ret_3d: float = 0.0
    positive_days_3d: int = 0
    today_gain_share_of_3d: float = 0.0
    rps_10: float = 0.0
    rps_10_change_3d: float = 0.0
    turnover_zscore_20: float = 0.0
    volume_ratio_prev5: float = 1.0
    continuous_volume_expansion: bool = False
    single_day_volume_explosion: bool = False
    close_above_ma20: bool = False
    breakout_above_ma20: bool = False
    breakout_above_20d_high: bool = False
    distance_from_ma20_pct: float = 0.0
    distance_from_3d_high_pct: float = 0.0
    bar_efficiency: float = 0.0
    close_location: float = 0.5
    upper_shadow_ratio: float = 0.0
    gap_ratio_pct: float = 0.0
    near_limit_up: bool = False
    prior_2d_return: float = 0.0
    data_completeness: float = 1.0


def score_paths(f: PathFeatures) -> Dict[str, float]:
    fresh = (
        0.32 * float(f.breakout_above_ma20)
        + 0.28 * float(f.breakout_above_20d_high)
        + 0.15 * scaled(f.ret_1d, 0.5, 5.0)
        + 0.12 * (1.0 if 1.1 <= f.volume_ratio_prev5 <= 3.0 else 0.0)
        + 0.13 * scaled(f.rps_10_change_3d, 0.0, 12.0)
    )
    persistent = (
        0.22 * scaled(f.positive_days_3d, 1.0, 3.0)
        + 0.23 * scaled(f.ret_3d, 2.0, 10.0)
        + 0.20 * scaled(f.rps_10, 70.0, 95.0)
        + 0.10 * scaled(f.rps_10_change_3d, -5.0, 8.0)
        + 0.15 * float(f.close_above_ma20)
        + 0.10 * float(0.8 <= f.volume_ratio_prev5 <= 2.2)
    )
    accelerating = (
        0.25 * scaled(f.ret_1d - f.ret_2d, 0.0, 5.0)
        + 0.20 * float(f.continuous_volume_expansion)
        + 0.20 * scaled(f.today_gain_share_of_3d, 0.30, 0.65)
        + 0.15 * scaled(f.close_location, 0.55, 0.95)
        + 0.20 * scaled(f.rps_10_change_3d, 0.0, 12.0)
    )
    exhausted = (
        0.24 * scaled(f.today_gain_share_of_3d, 0.60, 0.90)
        + 0.22 * scaled(f.turnover_zscore_20, 1.5, 3.5)
        + 0.20 * float(f.single_day_volume_explosion)
        + 0.18 * scaled(f.upper_shadow_ratio, 0.15, 0.50)
        + 0.16 * scaled(0.45 - f.bar_efficiency, 0.0, 0.45)
    )
    reversal = (
        0.35 * scaled(-f.prior_2d_return, 2.0, 8.0)
        + 0.30 * scaled(f.ret_1d, 2.0, 7.0)
        + 0.15 * float(not f.close_above_ma20)
        + 0.10 * scaled(f.volume_ratio_prev5, 1.0, 2.5)
        + 0.10 * scaled(f.close_location, 0.55, 0.95)
    )
    return {
        "FRESH_IGNITION": round(clamp(fresh), 4),
        "PERSISTENT_LEADER": round(clamp(persistent), 4),
        "ACCELERATING": round(clamp(accelerating), 4),
        "EXHAUSTED_SPIKE": round(clamp(exhausted), 4),
        "REVERSAL_BOUNCE": round(clamp(reversal), 4),
    }


def classify_path(
    features: PathFeatures,
    *,
    min_score: float = 0.45,
    min_margin: float = 0.08,
) -> Dict[str, Any]:
    scores = score_paths(features)
    ranked = sorted(PATHS, key=lambda name: (-scores[name], name))
    top, second = ranked[:2]
    margin = scores[top] - scores[second]
    warnings: List[str] = []
    primary = top
    if scores[top] < min_score:
        primary = UNCLASSIFIED
        warnings.append("insufficient_path_evidence")
    elif margin < min_margin:
        primary = UNCLASSIFIED
        warnings.append("mixed_path_evidence")
    secondary = [name for name in ranked if name != primary and scores[name] >= 0.35]
    completeness = clamp(features.data_completeness)
    confidence = 0.0 if primary == UNCLASSIFIED else clamp(
        (0.55 * scores[top] + 0.30 * min(1.0, margin / 0.25) + 0.15 * completeness)
    )
    return {
        "primary_path": primary,
        "primary_confidence": round(confidence, 4),
        "secondary_path_flags": secondary,
        "path_scores_type": "independent_evidence_scores",
        "path_scores": scores,
        "top_score_margin": round(margin, 4),
        "warnings": warnings,
        "classifier_version": OBSERVER_VERSION,
        "observer_only": True,
        "generate_trade": False,
    }


def feature_dict(features: PathFeatures) -> Dict[str, Any]:
    return asdict(features)
