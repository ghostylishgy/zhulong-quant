#!/usr/bin/env python3
"""Read-only sidecar comparison for the current and proposed L1 recall."""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import statistics
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
for path in (ROOT, ROOT / "01_engine/lib"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
from db_gateway import DBGateway

DB_PATH = str(ROOT / "storage/database/zhulong.duckdb")
REPORT_DIR = ROOT / "storage/reports/l1_path_observer"
BLOCKED_ACTIONS = [
    "replace_production_l1", "write_duckdb", "call_l2_l3_l4",
    "generate_validation_task", "write_shadow", "write_rag_memory",
    "write_nexus_audits", "trigger_daemon", "change_trade_contract",
    "generate_trade",
]


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


PATH = load_module(
    "zhulong_l1_path_observer", ROOT / "02_brain/lib/l1_path_observer.py"
)


def fnum(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
        return number if math.isfinite(number) else default
    except (TypeError, ValueError):
        return default


def pct(entry: float, exit_price: float) -> float | None:
    if entry <= 0 or exit_price <= 0:
        return None
    return round((exit_price / entry - 1.0) * 100.0, 6)


def zscore(value: float, baseline: Iterable[float]) -> float:
    values = [fnum(item) for item in baseline if item is not None]
    if len(values) < 5:
        return 0.0
    std = statistics.pstdev(values)
    return round((value - statistics.fmean(values)) / std, 4) if std > 0 else 0.0


def resolve_dates(conn, start: str, end: str) -> Tuple[List[str], List[str]]:
    targets = [str(row[0])[:10] for row in conn.execute(
        """SELECT DISTINCT trade_date FROM fact_daily
           WHERE CAST(trade_date AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
           ORDER BY trade_date""", [start, end]
    ).fetchall()]
    if not targets:
        raise ValueError("no fact_daily trade dates in requested range")
    history = [str(row[0])[:10] for row in conn.execute(
        """SELECT trade_date FROM (
             SELECT DISTINCT trade_date FROM fact_daily
             WHERE CAST(trade_date AS DATE)<CAST(? AS DATE)
             ORDER BY trade_date DESC LIMIT 25
           ) x ORDER BY trade_date""", [targets[0]]
    ).fetchall()]
    future = [str(row[0])[:10] for row in conn.execute(
        """SELECT trade_date FROM (
             SELECT DISTINCT trade_date FROM fact_daily
             WHERE CAST(trade_date AS DATE)>CAST(? AS DATE)
             ORDER BY trade_date LIMIT 5
           ) x ORDER BY trade_date""", [targets[-1]]
    ).fetchall()]
    return targets, history + targets + future


def load_market(conn, dates: List[str]):
    rows = conn.execute(
        """
        SELECT CAST(d.trade_date AS VARCHAR),d.symbol,COALESCE(b.name,d.symbol),
               COALESCE(b.industry,''),COALESCE(b.market,''),COALESCE(b.is_st,FALSE),
               d.open,d.high,d.low,d.close,d.pre_close,d.pct_chg,d.vol,d.amount,
               d.turnover_rate,d.ma20,d.vol_ma5,COALESCE(r.rps_10,0)
        FROM fact_daily d
        LEFT JOIN fact_stock_basic b ON b.symbol=d.symbol
        LEFT JOIN fact_rps_results r ON r.symbol=d.symbol AND r.trade_date=d.trade_date
        WHERE d.trade_date IN (SELECT UNNEST(?::DATE[])) AND d.close>0
        ORDER BY d.symbol,d.trade_date
        """, [dates]
    ).fetchall()
    keys = [
        "trade_date", "symbol", "name", "industry", "market", "is_st",
        "open", "high", "low", "close", "pre_close", "pct_chg", "vol",
        "amount", "turnover_rate", "ma20", "vol_ma5", "rps_10",
    ]
    market = defaultdict(dict)
    by_symbol = defaultdict(dict)
    for raw in rows:
        item = dict(zip(keys, raw))
        item["trade_date"] = str(item["trade_date"])[:10]
        item["symbol"] = str(item["symbol"])
        market[item["trade_date"]][item["symbol"]] = item
        by_symbol[item["symbol"]][item["trade_date"]] = item
    return dict(market), dict(by_symbol)


def build_features(history: List[Dict[str, Any]]):
    current = history[-1]
    previous = history[-2]
    recent3 = history[-3:]
    returns = [fnum(row["pct_chg"]) for row in recent3]
    ret3 = pct(fnum(history[-4]["close"]), fnum(current["close"]))
    prior2_return = sum(returns[:-1])
    total_abs = sum(abs(value) for value in returns)
    today_share = abs(returns[-1]) / total_abs if total_abs else 0.0
    prior_volumes = [fnum(row["vol"]) for row in history[-6:-1] if fnum(row["vol"]) > 0]
    avg_prev5 = statistics.fmean(prior_volumes) if prior_volumes else 0.0
    vol_ratio = fnum(current["vol"]) / avg_prev5 if avg_prev5 else 1.0
    open_, high, low, close = (fnum(current[key]) for key in ("open", "high", "low", "close"))
    span = high - low
    ma20, previous_ma20 = fnum(current["ma20"]), fnum(previous["ma20"])
    prior_highs = [fnum(row["high"]) for row in history[-21:-1] if fnum(row["high"]) > 0]
    rps_old = fnum(history[-4]["rps_10"])
    limit_pct = PATH.board_limit_pct(current["symbol"], bool(current["is_st"]))
    required = (
        "open", "high", "low", "close", "pre_close", "pct_chg", "vol",
        "amount", "turnover_rate", "ma20", "vol_ma5", "rps_10",
    )
    return PATH.PathFeatures(
        ret_1d=returns[-1], ret_2d=returns[-2], ret_3d=fnum(ret3),
        positive_days_3d=sum(value > 0 for value in returns),
        today_gain_share_of_3d=round(today_share, 4),
        rps_10=fnum(current["rps_10"]),
        rps_10_change_3d=fnum(current["rps_10"]) - rps_old,
        turnover_zscore_20=zscore(
            fnum(current["turnover_rate"]),
            [row["turnover_rate"] for row in history[-21:-1]],
        ),
        volume_ratio_prev5=round(vol_ratio, 4),
        continuous_volume_expansion=all(
            fnum(recent3[index]["vol"]) > fnum(recent3[index - 1]["vol"])
            for index in (1, 2)
        ),
        single_day_volume_explosion=vol_ratio >= 2.5,
        close_above_ma20=ma20 > 0 and close > ma20,
        breakout_above_ma20=(
            ma20 > 0 and previous_ma20 > 0 and close > ma20
            and fnum(previous["close"]) <= previous_ma20
        ),
        breakout_above_20d_high=bool(prior_highs and close > max(prior_highs)),
        distance_from_ma20_pct=round((close / ma20 - 1) * 100, 4) if ma20 else 0.0,
        distance_from_3d_high_pct=round(
            (close / max(fnum(row["high"]) for row in recent3) - 1) * 100, 4
        ),
        bar_efficiency=round((close - open_) / span, 4) if span > 0 else 0.0,
        close_location=round((close - low) / span, 4) if span > 0 else 0.5,
        upper_shadow_ratio=round((high - max(open_, close)) / span, 4) if span > 0 else 0.0,
        gap_ratio_pct=round(
            (open_ / fnum(current["pre_close"]) - 1) * 100, 4
        ) if fnum(current["pre_close"]) > 0 else 0.0,
        near_limit_up=returns[-1] >= limit_pct * 0.98,
        prior_2d_return=round(prior2_return, 4),
        data_completeness=round(
            sum(current.get(key) is not None for key in required) / len(required), 4
        ),
    )


def sector_context(daily_rows: List[Dict[str, Any]], current: Dict[str, Any]):
    industry = str(current.get("industry") or "")
    peers = [row for row in daily_rows if str(row.get("industry") or "") == industry] if industry else []
    changes = [fnum(row["pct_chg"]) for row in peers]
    ma_states = [
        fnum(row["close"]) > fnum(row["ma20"])
        for row in peers if fnum(row["ma20"]) > 0
    ]
    adv = sum(value > 0 for value in changes) / len(changes) if changes else None
    avg = statistics.fmean(changes) if changes else None
    above = sum(ma_states) / len(ma_states) if ma_states else None
    status = "SECTOR_TIDE_UNAVAILABLE"
    if adv is not None and avg is not None:
        if adv >= 0.55 and avg >= 0.5:
            status = "SECTOR_SUPPORTIVE"
        elif adv <= 0.35 and avg <= -0.5:
            status = "SECTOR_ADVERSE"
        else:
            status = "SECTOR_MIXED"
    return {
        "industry": industry, "status": status, "stock_count": len(peers),
        "advancer_ratio": round(adv, 4) if adv is not None else None,
        "average_pct_chg": round(avg, 4) if avg is not None else None,
        "above_ma20_ratio": round(above, 4) if above is not None else None,
        "mapping_quality": "current_composition_approximated" if industry else "unavailable",
        "observer_only": True, "block_candidate": False,
    }


def channel_score(channel: str, row: Dict[str, Any]) -> float:
    features = row["features"]
    if channel == "A1":
        return features["ret_1d"] + 2 * features["close_location"]
    if channel == "A2":
        return (
            features["ret_1d"]
            + min(3.0, features["turnover_zscore_20"]) * 0.4
            + min(3.0, features["volume_ratio_prev5"]) * 0.3
        )
    if channel == "B":
        return features["ret_3d"] + features["rps_10"] * 0.08 + features["positive_days_3d"]
    return (
        row["classification"]["path_scores"]["FRESH_IGNITION"] * 100
        + features["rps_10"] * 0.05
    )


def select_channels(rows: List[Dict[str, Any]], quotas: Dict[str, int]):
    pools = {channel: [] for channel in quotas}
    for row in rows:
        features = row["features"]
        if features["near_limit_up"]:
            pools["A1"].append(row)
        elif features["ret_1d"] >= 3.0:
            pools["A2"].append(row)
        if (
            features["ret_3d"] >= 3.0
            and features["positive_days_3d"] >= 2
            and features["rps_10"] >= 70
        ):
            pools["B"].append(row)
        if (
            (features["breakout_above_ma20"] or features["breakout_above_20d_high"])
            and features["ret_1d"] > 0
            and not features["near_limit_up"]
        ):
            pools["C"].append(row)
    result = {}
    for channel, items in pools.items():
        ranked = sorted(
            items, key=lambda row: (-channel_score(channel, row), row["symbol"])
        )
        result[channel] = [row["symbol"] for row in ranked[:quotas[channel]]]
    return result


def build_scorecard_sets(
    rows: List[Dict[str, Any]],
    channels: Dict[str, List[str]],
    limit: int = 50,
):
    """Build equal-budget, execution-eligible teams for the simple scorecard."""
    record_map = {row["symbol"]: row for row in rows}
    eligible = [
        row for row in rows if row["account_context"]["allow_execution"]
    ]
    legacy = [
        row["symbol"] for row in sorted(
            eligible,
            key=lambda item: (-item["features"]["ret_1d"], item["symbol"]),
        )[:limit]
    ]
    channel_rank = {}
    for channel, symbols in channels.items():
        count = max(1, len(symbols))
        for index, symbol in enumerate(symbols):
            channel_rank.setdefault(symbol, {})[channel] = round(
                (count - index) / count, 6
            )
    admissions = []
    for symbol, ranks in channel_rank.items():
        row = record_map.get(symbol)
        if not row or not row["account_context"]["allow_execution"]:
            continue
        admissions.append({
            "symbol": symbol,
            "best_channel_percentile": max(ranks.values()),
            "channel_count": len(ranks),
            "channel_percentiles": ranks,
            "primary_confidence": row["classification"]["primary_confidence"],
        })
    admissions.sort(key=lambda item: (
        -item["best_channel_percentile"],
        -item["channel_count"],
        -item["primary_confidence"],
        item["symbol"],
    ))
    admitted = admissions[:limit]
    path_symbols = [item["symbol"] for item in admitted]
    research_only = sorted(
        symbol for symbol in channel_rank
        if symbol in record_map
        and not record_map[symbol]["account_context"]["allow_execution"]
    )
    return {
        "schema_version": "l1_scorecard_teams_v0.1",
        "universe_policy": "PERSONAL_MAINLAND_A_SHARE",
        "team_size_target": limit,
        "legacy_l1_top50": legacy,
        "path_l1_top50": path_symbols,
        "path_admission_method": "best_normalized_channel_rank_then_multi_channel_confidence",
        "path_admission": admitted,
        "research_only_st_bj": research_only,
        "ready": len(legacy) == limit and len(path_symbols) == limit,
    }


def market_regime(rows: List[Dict[str, Any]]) -> str:
    eligible = [row for row in rows if fnum(row.get("amount")) > 10000]
    if not eligible:
        return "UNAVAILABLE"
    adv = sum(fnum(row["pct_chg"]) > 0 for row in eligible) / len(eligible)
    avg = statistics.fmean(fnum(row["pct_chg"]) for row in eligible)
    if adv >= 0.60 and avg >= 0.8:
        return "HOT"
    if adv <= 0.35 and avg <= -0.7:
        return "COLD"
    return "NORMAL"


def counterfactual_quotas(regime: str):
    alternatives = {
        "HOT": {"A1": 8, "A2": 12, "B": 35, "C": 25},
        "COLD": {"A1": 10, "A2": 10, "B": 20, "C": 10},
    }
    return alternatives.get(regime, dict(PATH.FIXED_QUOTAS))


def observe_date(
    trade_date: str,
    all_dates: List[str],
    market: Dict[str, Dict[str, Dict[str, Any]]],
    by_symbol: Dict[str, Dict[str, Any]],
):
    date_index = all_dates.index(trade_date)
    available = all_dates[:date_index + 1]
    daily_rows = list(market.get(trade_date, {}).values())
    records = []
    for current in daily_rows:
        if fnum(current["close"]) <= 0 or fnum(current["amount"]) <= 10000:
            continue
        history = [
            by_symbol[current["symbol"]][date]
            for date in available[-25:]
            if date in by_symbol[current["symbol"]]
        ]
        if len(history) < 4:
            continue
        features = build_features(history)
        row = {
            "symbol": current["symbol"], "name": str(current["name"]),
            "trade_date": trade_date, "industry": str(current["industry"]),
            "market": str(current["market"]), "is_st": bool(current["is_st"]),
            "close": fnum(current["close"]), "amount": fnum(current["amount"]),
            "features": PATH.feature_dict(features),
            "classification": PATH.classify_path(features),
            "account_context": PATH.account_context(
                current["symbol"], bool(current["is_st"])
            ),
            "a1_intraday_quality": {
                "status": "UNAVAILABLE",
                "reason": "no_minute_tick_or_limit_order_table",
                "seal_time": None, "seal_break_count": None,
                "seal_order_ratio": None,
            },
        }
        row["sector_context"] = sector_context(daily_rows, current)
        flags = ["A1_INTRADAY_QUALITY_UNAVAILABLE"]
        if features.near_limit_up:
            flags.append("NEAR_LIMIT_UP")
        if row["sector_context"]["status"] in ("SECTOR_SUPPORTIVE", "SECTOR_ADVERSE"):
            flags.append(row["sector_context"]["status"])
        if row["account_context"]["status"] != "ELIGIBLE_A_SHARE":
            flags.append(row["account_context"]["status"])
        row["context_flags"] = flags
        records.append(row)
    fixed = select_channels(records, dict(PATH.FIXED_QUOTAS))
    scorecard_sets = build_scorecard_sets(records, fixed)
    regime = market_regime(daily_rows)
    counter = select_channels(records, counterfactual_quotas(regime))
    selected = {symbol for symbols in fixed.values() for symbol in symbols}
    old_raw = [
        row["symbol"] for row in sorted(
            records, key=lambda item: (-item["features"]["ret_1d"], item["symbol"])
        )[:50]
    ]
    output = []
    record_map = {row["symbol"]: row for row in records}
    scorecard_symbols = set(scorecard_sets["legacy_l1_top50"]) | set(
        scorecard_sets["path_l1_top50"]
    )
    for symbol in sorted(selected | set(old_raw) | scorecard_symbols):
        row = record_map[symbol]
        channels = [channel for channel, symbols in fixed.items() if symbol in symbols]
        row["source_channels"] = channels
        row["selected_by_new_l1"] = bool(channels)
        row["selected_by_old_l1_raw"] = symbol in old_raw
        row["scorecard_team_membership"] = {
            "legacy_l1_top50": symbol in scorecard_sets["legacy_l1_top50"],
            "path_l1_top50": symbol in scorecard_sets["path_l1_top50"],
        }
        row["comparison_bucket_raw"] = (
            "intersection" if row["selected_by_new_l1"] and row["selected_by_old_l1_raw"]
            else "new_only" if row["selected_by_new_l1"] else "old_only"
        )
        output.append(row)
    return {
        "trade_date": trade_date, "market_regime_observed": regime,
        "fixed_quota": dict(PATH.FIXED_QUOTAS),
        "counterfactual_quota": counterfactual_quotas(regime),
        "counterfactual_hypothesis_id": "TIDE_COUNTERFACTUAL_V01_FROZEN",
        "channel_symbols": fixed, "counterfactual_channel_symbols": counter,
        "old_l1_raw_symbols": old_raw, "scorecard_sets": scorecard_sets,
        "candidates": output,
    }


def add_trendhunter(snapshot: Dict[str, Any], enabled: bool):
    if not enabled:
        snapshot["old_l1_after_trendhunter_symbols"] = []
        snapshot["trendhunter_comparison_status"] = "SKIPPED_BY_CLI"
        return
    module = load_module(
        "zhulong_l1_trendhunter", ROOT / "02_brain/lib/trend_hunter.py"
    )
    hunter = module.TrendHunter()
    old_after = []
    for row in snapshot["candidates"]:
        result = hunter._identify_pattern(row["symbol"], snapshot["trade_date"])
        observation = {
            "score": fnum(result.get("score")),
            "ma_alignment": bool(result.get("ma_alignment")),
            "pattern_name": str(result.get("pattern_name") or ""),
        }
        observation["passes_current_gate"] = (
            observation["score"] > 40 and observation["ma_alignment"]
        )
        row["trendhunter_observation"] = observation
        if row["selected_by_old_l1_raw"] and observation["passes_current_gate"]:
            old_after.append(row["symbol"])
    snapshot["old_l1_after_trendhunter_symbols"] = old_after
    snapshot["trendhunter_comparison_status"] = "AVAILABLE"
    for row in snapshot["candidates"]:
        new = row["selected_by_new_l1"]
        old = row["symbol"] in old_after
        row["selected_by_old_l1_production_proxy"] = old
        row["comparison_bucket_production_proxy"] = (
            "intersection" if new and old else "new_only" if new else "old_only"
        )
        if new and not row["trendhunter_observation"]["passes_current_gate"]:
            row["context_flags"].append("BELOW_CURRENT_TRENDHUNTER_GATE")


def assign_episodes(snapshots: List[Dict[str, Any]]):
    states = {}
    episodes = {}
    for day_index, snapshot in enumerate(snapshots):
        for row in snapshot["candidates"]:
            if not row["selected_by_new_l1"]:
                continue
            symbol = row["symbol"]
            state = states.get(symbol)
            if state is None or day_index - state["last_seen_index"] > 5:
                episode_id = f"L1P-{symbol}-{snapshot['trade_date'].replace('-', '')}"
                state = {
                    "episode_id": episode_id, "last_seen_index": day_index,
                    "last_path": row["classification"]["primary_path"],
                }
                states[symbol] = state
                episodes[episode_id] = {
                    "episode_id": episode_id, "symbol": symbol, "name": row["name"],
                    "episode_scope": "report_range_local",
                    "anchor_date": snapshot["trade_date"],
                    "last_seen_date": snapshot["trade_date"],
                    "appearance_count": 0, "path_transitions": [],
                    "anchor_industry": row["industry"],
                    "anchor_rps_10": row["features"]["rps_10"],
                    "anchor_account_eligible": row["account_context"]["allow_execution"],
                    "selected_by_old_l1_production_proxy": row.get(
                        "selected_by_old_l1_production_proxy", False
                    ),
                }
            episode = episodes[state["episode_id"]]
            path = row["classification"]["primary_path"]
            if path != state["last_path"]:
                episode["path_transitions"].append({
                    "trade_date": snapshot["trade_date"],
                    "from": state["last_path"], "to": path,
                })
            state["last_seen_index"] = day_index
            state["last_path"] = path
            episode["last_seen_date"] = snapshot["trade_date"]
            episode["appearance_count"] += 1
            row["episode_id"] = state["episode_id"]
    return list(episodes.values())


def percentile_threshold(values: List[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil(percentile * len(ordered)) - 1))
    return ordered[index]


def add_future_strength(episodes, all_dates, by_symbol):
    for episode in episodes:
        symbol, anchor = episode["symbol"], episode["anchor_date"]
        dates = [
            date for date in all_dates
            if date >= anchor and date in by_symbol.get(symbol, {})
        ]
        base = fnum(by_symbol.get(symbol, {}).get(anchor, {}).get("close"))
        outcomes = {
            "benchmark": "T_CLOSE_TO_FUTURE_CLOSE_NOT_EXECUTION_RETURN",
            "t1_vwap_0931_0945": None,
            "t1_vwap_status": "UNAVAILABLE_NO_INTRADAY_DATA",
        }
        for horizon in (1, 3, 5):
            outcomes[f"return_t{horizon}_close_pct"] = (
                pct(base, fnum(by_symbol[symbol][dates[horizon]]["close"]))
                if len(dates) > horizon else None
            )
        outcomes["t1_open_premium_pct"] = (
            pct(base, fnum(by_symbol[symbol][dates[1]]["open"]))
            if len(dates) > 1 else None
        )
        ret5 = outcomes["return_t5_close_pct"]
        cross_sectional = None
        sector_relative = None
        sector_excess = None
        if len(dates) > 5:
            t5_date = dates[5]
            universe_returns = []
            sector_returns = []
            for peer_symbol, peer_rows in by_symbol.items():
                anchor_row = peer_rows.get(anchor)
                future_row = peer_rows.get(t5_date)
                if not anchor_row or not future_row or fnum(anchor_row.get("amount")) <= 10000:
                    continue
                peer_return = pct(
                    fnum(anchor_row.get("close")), fnum(future_row.get("close"))
                )
                if peer_return is None:
                    continue
                universe_returns.append(peer_return)
                if (
                    episode["anchor_industry"]
                    and str(anchor_row.get("industry") or "") == episode["anchor_industry"]
                ):
                    sector_returns.append(peer_return)
            top_decile = percentile_threshold(universe_returns, 0.90)
            cross_sectional = bool(
                ret5 is not None and top_decile is not None and ret5 >= top_decile
            )
            if ret5 is not None and sector_returns:
                sector_excess = round(ret5 - statistics.fmean(sector_returns), 6)
                sector_relative = sector_excess >= 3.0
        absolute = bool(ret5 is not None and ret5 >= 5.0)
        votes = [value for value in (absolute, cross_sectional, sector_relative) if value is not None]
        future_winner = bool(len(votes) == 3 and sum(votes) >= 2)
        ret3 = outcomes["return_t3_close_pct"]
        future_rps = fnum(by_symbol[symbol][dates[5]]["rps_10"]) if len(dates) > 5 else None
        behavior_confirmed = bool(
            ret3 is not None and ret5 is not None and future_rps is not None
            and ret3 > 0 and ret5 > 0 and future_rps >= episode["anchor_rps_10"]
        )
        outcomes["future_strength_labels"] = {
            "absolute_strength": absolute if ret5 is not None else None,
            "cross_sectional_strength": cross_sectional,
            "sector_relative_strength": sector_relative,
            "sector_excess_t5_pct": sector_excess,
            "future_return_winner": future_winner if len(votes) == 3 else None,
            "future_behavior_confirmed": behavior_confirmed if future_rps is not None else None,
            "actionable_missed_opportunity": (
                future_winner and behavior_confirmed
                and episode["anchor_account_eligible"]
                and not episode["selected_by_old_l1_production_proxy"]
            ) if len(votes) == 3 and future_rps is not None else None,
            "status": "COMPLETE" if len(votes) == 3 else "PENDING_T5",
        }
        episode["forward_outcomes"] = outcomes


def summarize(snapshot):
    comparison = defaultdict(int)
    paths = defaultdict(int)
    for row in snapshot["candidates"]:
        if row["selected_by_new_l1"]:
            comparison[row.get(
                "comparison_bucket_production_proxy", row["comparison_bucket_raw"]
            )] += 1
            paths[row["classification"]["primary_path"]] += 1
    return {
        "new_unique_count": sum(
            row["selected_by_new_l1"] for row in snapshot["candidates"]
        ),
        "old_raw_count": len(snapshot["old_l1_raw_symbols"]),
        "old_production_proxy_count": len(
            snapshot.get("old_l1_after_trendhunter_symbols", [])
        ),
        "comparison_counts": dict(comparison),
        "primary_path_counts": dict(paths),
    }


def render_markdown(payload):
    lines = [
        f"# L1 Path Observer - {payload['batch_id']}", "",
        "## 1. Safety Boundary", "", "- observer_only: true",
        "- no_trade_signal: true", "- production_l1_changed: false",
        "- DuckDB writes: 0",
        "- T+1 VWAP outcome: unavailable because no minute/tick table exists",
        "", "## 2. Daily Summary", "",
        "| Date | Regime | New unique | Old raw | Old after TrendHunter | New only | Intersection |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for snapshot in payload["daily_snapshots"]:
        summary = snapshot["summary"]
        lines.append(
            f"| {snapshot['trade_date']} | {snapshot['market_regime_observed']} | "
            f"{summary['new_unique_count']} | {summary['old_raw_count']} | "
            f"{summary['old_production_proxy_count']} | "
            f"{summary['comparison_counts'].get('new_only', 0)} | "
            f"{summary['comparison_counts'].get('intersection', 0)} |"
        )
    latest = payload["daily_snapshots"][-1]
    lines.extend([
        "", "## 3. Latest New L1 Candidates", "",
        "| Symbol | Name | Channels | Primary path | Confidence | Old production proxy | Sector | Eligibility |",
        "|---|---|---|---|---:|---|---|---|",
    ])
    for row in latest["candidates"]:
        if row["selected_by_new_l1"]:
            lines.append(
                f"| {row['symbol']} | {row['name']} | {','.join(row['source_channels'])} | "
                f"{row['classification']['primary_path']} | "
                f"{row['classification']['primary_confidence']:.3f} | "
                f"{str(row.get('selected_by_old_l1_production_proxy', False)).lower()} | "
                f"{row['sector_context']['status']} | {row['account_context']['status']} |"
            )
    lines.extend([
        "", "## 4. Frozen Counterfactual", "",
        f"- hypothesis_id: `{latest['counterfactual_hypothesis_id']}`",
        f"- observed regime: `{latest['market_regime_observed']}`",
        f"- fixed quota: `{json.dumps(latest['fixed_quota'], ensure_ascii=False)}`",
        f"- counterfactual quota: `{json.dumps(latest['counterfactual_quota'], ensure_ascii=False)}`",
        "- Counterfactual quotas do not change the real observer pool.",
        "", "## 5. Deferred Data", "",
        "A1 seal time, break count, and seal-order ratio remain unavailable. "
        "They are not inferred from daily bars. Sector membership is a current-composition "
        "approximation and cannot be used as a historical hard gate.",
        "Episode IDs are local to this report range. Formal review must rebuild one complete "
        "40-60 trading-day range rather than joining daily provisional episode IDs.",
        "", "## 6. Blocked Actions", "",
    ])
    lines.extend(f"- `{action}`" for action in payload["blocked_actions"])
    return "\n".join(lines) + "\n"


def frozen_team_signature(payload: Dict[str, Any]) -> str:
    teams = [
        {
            "trade_date": snapshot.get("trade_date"),
            "scorecard_sets": snapshot.get("scorecard_sets"),
        }
        for snapshot in payload.get("daily_snapshots") or []
    ]
    return json.dumps(teams, sort_keys=True, ensure_ascii=False)


def verify_frozen_teams(existing: Dict[str, Any], proposed: Dict[str, Any]) -> None:
    if existing.get("schema_version") != "l1_path_observer_report_v0.1":
        raise ValueError("existing observer report has unsupported schema")
    if frozen_team_signature(existing) != frozen_team_signature(proposed):
        raise ValueError(
            "existing observer report has different frozen scorecard teams; "
            "refusing point-in-time overwrite"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only L1 A1/A2/B/C path observer")
    parser.add_argument("--trade-date", default="")
    parser.add_argument("--start-date", default="")
    parser.add_argument("--end-date", default="")
    parser.add_argument("--skip-trendhunter", action="store_true")
    parser.add_argument("--output-dir", default=str(REPORT_DIR))
    args = parser.parse_args()
    with DBGateway(DB_PATH, read_only=True) as conn:
        latest = conn.execute(
            "SELECT CAST(MAX(trade_date) AS VARCHAR) FROM fact_daily"
        ).fetchone()[0]
        single = args.trade_date.strip() or str(latest)[:10]
        start, end = args.start_date.strip() or single, args.end_date.strip() or single
        target_dates, all_dates = resolve_dates(conn, start, end)
        if len(target_dates) > 60:
            raise SystemExit("maximum observation range is 60 trading days")
        market, by_symbol = load_market(conn, all_dates)
    snapshots = []
    for trade_date in target_dates:
        snapshot = observe_date(trade_date, all_dates, market, by_symbol)
        add_trendhunter(snapshot, not args.skip_trendhunter)
        snapshot["summary"] = summarize(snapshot)
        snapshots.append(snapshot)
    episodes = assign_episodes(snapshots)
    add_future_strength(episodes, all_dates, by_symbol)
    batch = (
        target_dates[0].replace("-", "") if len(target_dates) == 1
        else f"{target_dates[0].replace('-', '')}_{target_dates[-1].replace('-', '')}"
    )
    payload = {
        "schema_version": "l1_path_observer_report_v0.1", "batch_id": batch,
        "generated_at": datetime.now().astimezone().isoformat(),
        "mode": "read_only_sidecar", "observer_only": True,
        "no_trade_signal": True, "production_l1_changed": False,
        "feature_as_of": "each_trade_date_close",
        "episode_scope": "report_range_local",
        "path_scores_type": "independent_evidence_scores",
        "fixed_quota": dict(PATH.FIXED_QUOTAS), "blocked_actions": BLOCKED_ACTIONS,
        "data_capabilities": {
            "daily_ohlcv": "AVAILABLE", "historical_rps": "AVAILABLE",
            "industry": "CURRENT_COMPOSITION_APPROXIMATED",
            "minute_tick": "UNAVAILABLE", "limit_order_book": "UNAVAILABLE",
        },
        "daily_snapshots": snapshots, "episodes": episodes,
    }
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"l1_path_observer_{batch}.json"
    md_path = output_dir / f"l1_path_observer_{batch}.md"
    report_action = "CREATED_FROZEN"
    if json_path.exists():
        existing = json.loads(json_path.read_text(encoding="utf-8"))
        verify_frozen_teams(existing, payload)
        payload = existing
        report_action = "REUSED_FROZEN"
    else:
        json_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n",
            encoding="utf-8",
        )
        md_path.write_text(render_markdown(payload), encoding="utf-8")
    reported_snapshots = payload.get("daily_snapshots") or []
    print(json.dumps({
        "json": str(json_path), "markdown": str(md_path),
        "dates": [item.get("trade_date") for item in reported_snapshots],
        "latest_summary": reported_snapshots[-1]["summary"],
        "episode_count": len(payload.get("episodes") or []), "observer_only": True,
        "production_l1_changed": False, "report_action": report_action,
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
