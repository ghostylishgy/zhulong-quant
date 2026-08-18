#!/usr/bin/env python3
"""Deterministic, observation-only ex-ante trade archetype classifier."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List

EMOTION_RELAY = "EMOTION_RELAY"
EVENT_CATALYST = "EVENT_CATALYST"
TREND_INITIATION = "TREND_INITIATION"
TREND_CONTINUATION = "TREND_CONTINUATION"
UNCLASSIFIED = "UNCLASSIFIED"
ARCHETYPES = (EMOTION_RELAY, EVENT_CATALYST, TREND_INITIATION, TREND_CONTINUATION)
EVENT_TAGS = {"BID_WIN", "BUYBACK", "CONTRACT", "INCREASE_HOLDING", "PROFIT_INCREASE"}


@dataclass(frozen=True)
class ArchetypeFeatures:
    pct_chg: float = 0.0
    near_limit_up: bool = False
    turnover: float = 0.0
    vol_ratio: float = 1.0
    rps_10: float = 0.0
    close_above_ma20: bool = False
    breakout_above_ma20: bool = False
    ma20_slope_positive: bool = False
    above_ma20_days: int = 0
    limit_up_streak: int = 0
    lhb_present: bool = False
    positive_news_tags: List[str] = field(default_factory=list)
    official_event_evidence: bool = False
    data_as_of: str = ""


@dataclass(frozen=True)
class ArchetypeResult:
    primary_archetype: str
    secondary_archetype: str
    confidence: float
    scores: Dict[str, int]
    reasons: Dict[str, List[str]]
    warnings: List[str]
    classifier_version: str = "trade_archetype_v0.1"
    observer_only: bool = True
    generate_trade: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def classify_trade_archetype(f: ArchetypeFeatures) -> ArchetypeResult:
    scores = {name: 0 for name in ARCHETYPES}
    reasons = {name: [] for name in ARCHETYPES}
    warnings: List[str] = []

    def add(name: str, points: int, reason: str) -> None:
        scores[name] += points
        reasons[name].append(reason)

    if f.near_limit_up:
        add(EMOTION_RELAY, 3, "near_or_at_limit_up")
    elif f.pct_chg >= 6:
        add(EMOTION_RELAY, 1, "large_single_day_gain")
    if f.turnover >= 15:
        add(EMOTION_RELAY, 2, "high_turnover")
    elif f.turnover >= 8:
        add(EMOTION_RELAY, 1, "elevated_turnover")
    if f.vol_ratio >= 1.8:
        add(EMOTION_RELAY, 1, "volume_expansion")
    if f.limit_up_streak:
        add(EMOTION_RELAY, min(4, f.limit_up_streak * 2), "limit_up_streak")
    if f.lhb_present:
        add(EMOTION_RELAY, 1, "dragon_tiger_list_present")
    if f.rps_10 >= 95:
        add(EMOTION_RELAY, 1, "extreme_short_term_strength")

    event_tags = sorted({str(x).strip().upper() for x in f.positive_news_tags} & EVENT_TAGS)
    if event_tags:
        add(EVENT_CATALYST, 4, "structured_positive_event:" + ",".join(event_tags))
    if f.official_event_evidence and event_tags:
        add(EVENT_CATALYST, 2, "official_source_event_evidence")
    if event_tags and 1 <= f.pct_chg < 9.5:
        add(EVENT_CATALYST, 1, "event_with_price_confirmation")

    if f.breakout_above_ma20:
        add(TREND_INITIATION, 3, "fresh_ma20_breakout")
    if f.ma20_slope_positive:
        add(TREND_INITIATION, 1, "ma20_slope_positive")
    if 2 <= f.pct_chg < 9.5:
        add(TREND_INITIATION, 1, "constructive_non_limit_gain")
    if 1.2 <= f.vol_ratio <= 3:
        add(TREND_INITIATION, 1, "controlled_volume_confirmation")
    if f.rps_10 >= 90:
        add(TREND_INITIATION, 2, "high_relative_strength")
    if f.close_above_ma20 and 1 <= f.above_ma20_days <= 3:
        add(TREND_INITIATION, 1, "early_days_above_ma20")

    if f.close_above_ma20 and f.above_ma20_days >= 4:
        add(TREND_CONTINUATION, 3, "sustained_above_ma20")
    if f.ma20_slope_positive:
        add(TREND_CONTINUATION, 2, "rising_ma20")
    if f.rps_10 >= 85:
        add(TREND_CONTINUATION, 2, "persistent_relative_strength")
    if -3 <= f.pct_chg < 8:
        add(TREND_CONTINUATION, 1, "non_exhaustive_daily_move")
    if 0.8 <= f.vol_ratio <= 2:
        add(TREND_CONTINUATION, 1, "sustainable_volume_range")

    ranked = sorted(ARCHETYPES, key=lambda name: (-scores[name], name))
    top, second = ranked[:2]
    margin = scores[top] - scores[second]
    if scores[top] < 4:
        primary = UNCLASSIFIED
        warnings.append("insufficient_classification_evidence")
    elif margin < 2:
        primary = UNCLASSIFIED
        warnings.append("mixed_or_conflicting_archetype_evidence")
    else:
        primary = top
    if not f.data_as_of:
        warnings.append("missing_data_as_of")
    confidence = 0.0 if primary == UNCLASSIFIED else min(
        0.95, 0.45 + min(scores[top], 10) * 0.035 + min(margin, 5) * 0.04
    )
    return ArchetypeResult(
        primary_archetype=primary,
        secondary_archetype=second if scores[second] else "",
        confidence=round(confidence, 4),
        scores=scores,
        reasons=reasons,
        warnings=warnings,
    )
