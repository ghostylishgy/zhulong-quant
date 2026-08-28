#!/usr/bin/env python3
"""Read-only league table comparing legacy and path-based L1 selections."""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List

ROOT = Path(__file__).resolve().parents[1]
for path in (ROOT, ROOT / "01_engine/lib"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
from db_gateway import DBGateway

DB_PATH = str(ROOT / "storage/database/zhulong.duckdb")
REPORT_DIR = ROOT / "storage/reports/l1_scorecard"
TEAM_TARGET = 50
MIN_COVERAGE = 0.80
DRAW_MARGIN = 0.25
CROSS_SECTION_MIN_DAYS = 20
CROSS_SECTION_MIN_COUNT = 10
CROSS_SECTION_TOP_K = 10
CROSS_SECTION_GROUPS = 5
BLOCKED_ACTIONS = [
    "write_duckdb", "call_l2_l3_l4", "write_shadow", "write_rag_memory",
    "write_nexus_audits", "trigger_daemon", "generate_trade",
    "change_l1_selection", "tune_l1_thresholds",
]
REQUIRED_SOURCE_BLOCKS = {
    "write_duckdb", "call_l2_l3_l4", "write_shadow", "write_rag_memory",
    "write_nexus_audits", "trigger_daemon", "generate_trade",
}
POINT_RULES = {
    "t1_up": 1, "t1_down": -1,
    "t3_up": 2, "t3_down": -2,
    "t5_up": 3, "t5_down": -3,
    "t5_market_excess_positive": 2, "t5_market_excess_negative": -2,
    "mfe3_at_least_3pct": 1,
    "failed_spike": -1,
    "mae5_at_most_minus_5pct": -2,
    "mae5_at_most_minus_10pct_extra": -2,
}


def fnum(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def pct(entry: float | None, exit_price: float | None) -> float | None:
    if entry is None or exit_price is None or entry <= 0 or exit_price <= 0:
        return None
    return round((exit_price / entry - 1.0) * 100.0, 6)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_source(path: Path, payload: Dict[str, Any]) -> None:
    if payload.get("schema_version") != "l1_path_observer_report_v0.1":
        raise ValueError(f"{path}: unsupported observer schema")
    if payload.get("observer_only") is not True:
        raise ValueError(f"{path}: observer_only must be true")
    if payload.get("no_trade_signal") is not True:
        raise ValueError(f"{path}: no_trade_signal must be true")
    if payload.get("production_l1_changed") is not False:
        raise ValueError(f"{path}: production_l1_changed must be false")
    blocked = set(payload.get("blocked_actions") or [])
    if not REQUIRED_SOURCE_BLOCKS.issubset(blocked):
        raise ValueError(f"{path}: source safety blocks are incomplete")
    for snapshot in payload.get("daily_snapshots") or []:
        teams = snapshot.get("scorecard_sets") or {}
        if teams.get("schema_version") != "l1_scorecard_teams_v0.1":
            raise ValueError(f"{path}: snapshot lacks frozen scorecard teams")


def discover_input_paths(explicit: Iterable[str], input_dir: str = "") -> List[Path]:
    candidates = [Path(value).resolve() for value in explicit]
    if str(input_dir or "").strip():
        directory = Path(input_dir).resolve()
        candidates.extend(sorted(directory.glob("l1_path_observer_*.json")))
    unique = []
    seen = set()
    for path in candidates:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        unique.append(resolved)
    if not unique:
        raise ValueError("no L1 observer input reports found")
    return unique


def load_snapshots(paths: Iterable[Path], forward_start: str = ""):
    sources = []
    snapshots = {}
    for path in paths:
        payload = json.loads(path.read_text(encoding="utf-8"))
        validate_source(path, payload)
        contributed = False
        for snapshot in payload.get("daily_snapshots") or []:
            trade_date = str(snapshot.get("trade_date") or "")[:10]
            if not trade_date:
                raise ValueError(f"{path}: snapshot trade_date missing")
            if forward_start and trade_date < forward_start:
                continue
            contributed = True
            frozen = json.dumps(
                snapshot.get("scorecard_sets"), sort_keys=True, ensure_ascii=False
            )
            if trade_date in snapshots:
                existing = json.dumps(
                    snapshots[trade_date].get("scorecard_sets"),
                    sort_keys=True,
                    ensure_ascii=False,
                )
                if existing != frozen:
                    raise ValueError(f"conflicting frozen teams for {trade_date}")
                continue
            snapshots[trade_date] = snapshot
        if contributed:
            sources.append({"path": str(path), "sha256": sha256_file(path)})
    if not snapshots:
        raise ValueError("no daily snapshots found")
    return sources, [snapshots[key] for key in sorted(snapshots)]


def future_dates(conn, trade_date: str) -> List[str]:
    return [str(row[0])[:10] for row in conn.execute(
        """SELECT trade_date FROM (
             SELECT DISTINCT trade_date FROM fact_daily
             WHERE CAST(trade_date AS DATE)>CAST(? AS DATE)
             ORDER BY trade_date LIMIT 5
           ) x ORDER BY trade_date""",
        [trade_date],
    ).fetchall()]


def market_median_t5(conn, trade_date: str, t5_date: str) -> float | None:
    row = conn.execute(
        """
        SELECT MEDIAN((d5.close/d0.close-1.0)*100.0)
        FROM fact_daily d0
        JOIN fact_daily d5 ON d5.symbol=d0.symbol AND d5.trade_date=CAST(? AS DATE)
        LEFT JOIN fact_stock_basic b ON b.symbol=d0.symbol
        WHERE d0.trade_date=CAST(? AS DATE)
          AND d0.close>0 AND d5.close>0 AND d0.amount>10000
          AND COALESCE(b.is_st,FALSE)=FALSE
          AND (d0.symbol LIKE '%.SZ' OR d0.symbol LIKE '%.SH')
        """,
        [t5_date, trade_date],
    ).fetchone()
    return round(float(row[0]), 6) if row and row[0] is not None else None


def load_prices(conn, symbols: List[str], dates: List[str]):
    if not symbols or not dates:
        return {}
    placeholders_symbols = ",".join(["?"] * len(symbols))
    placeholders_dates = ",".join(["CAST(? AS DATE)"] * len(dates))
    rows = conn.execute(
        f"""SELECT symbol,CAST(trade_date AS VARCHAR),open,high,low,close
            FROM fact_daily
            WHERE symbol IN ({placeholders_symbols})
              AND trade_date IN ({placeholders_dates})
            ORDER BY symbol,trade_date""",
        [*symbols, *dates],
    ).fetchall()
    result = defaultdict(dict)
    for symbol, trade_date, open_, high, low, close in rows:
        result[str(symbol)][str(trade_date)[:10]] = {
            "open": fnum(open_), "high": fnum(high), "low": fnum(low),
            "close": fnum(close),
        }
    return dict(result)


def score_pick(outcome: Dict[str, Any]) -> Dict[str, Any]:
    required = ("return_t1_pct", "return_t3_pct", "return_t5_pct", "market_excess_t5_pct")
    if any(outcome.get(key) is None for key in required):
        return {
            "status": "PENDING_T5", "points": None, "breakdown": {},
            "followthrough_success": None, "failed_spike": None,
        }
    points = 0
    breakdown = {}

    def add(name: str) -> None:
        nonlocal points
        value = POINT_RULES[name]
        breakdown[name] = value
        points += value

    for horizon, up_name, down_name in (
        (1, "t1_up", "t1_down"),
        (3, "t3_up", "t3_down"),
        (5, "t5_up", "t5_down"),
    ):
        value = outcome[f"return_t{horizon}_pct"]
        if value > 0:
            add(up_name)
        elif value < 0:
            add(down_name)
    if outcome["market_excess_t5_pct"] > 0:
        add("t5_market_excess_positive")
    elif outcome["market_excess_t5_pct"] < 0:
        add("t5_market_excess_negative")
    if outcome.get("mfe3_pct") is not None and outcome["mfe3_pct"] >= 3.0:
        add("mfe3_at_least_3pct")
    failed_spike = bool(
        outcome.get("mfe3_pct") is not None
        and outcome["mfe3_pct"] >= 3.0
        and outcome["return_t3_pct"] <= 0
    )
    if failed_spike:
        add("failed_spike")
    mae5 = outcome.get("mae5_pct")
    if mae5 is not None and mae5 <= -5.0:
        add("mae5_at_most_minus_5pct")
    if mae5 is not None and mae5 <= -10.0:
        add("mae5_at_most_minus_10pct_extra")
    positive_horizons = sum(
        outcome[f"return_t{horizon}_pct"] > 0 for horizon in (1, 3, 5)
    )
    return {
        "status": "SETTLED_T5", "points": points, "breakdown": breakdown,
        "followthrough_success": bool(
            positive_horizons >= 2 and outcome["market_excess_t5_pct"] > 0
        ),
        "failed_spike": failed_spike,
    }


def evaluate_symbols(
    conn,
    trade_date: str,
    symbols: List[str],
    future: List[str],
    market_median: float | None,
):
    all_dates = [trade_date, *future]
    prices = load_prices(conn, symbols, all_dates)
    output = []
    for symbol in symbols:
        rows = prices.get(symbol, {})
        base = rows.get(trade_date, {}).get("close")
        outcome = {
            "symbol": symbol,
            "status": "PENDING_T5" if len(future) < 5 else "DATA_INCOMPLETE",
            "return_t1_pct": None, "return_t3_pct": None,
            "return_t5_pct": None, "market_median_t5_pct": market_median,
            "market_excess_t5_pct": None, "mfe3_pct": None, "mae5_pct": None,
        }
        if len(future) >= 1:
            outcome["return_t1_pct"] = pct(base, rows.get(future[0], {}).get("close"))
        if len(future) >= 3:
            outcome["return_t3_pct"] = pct(base, rows.get(future[2], {}).get("close"))
            highs = [rows.get(date, {}).get("high") for date in future[:3]]
            valid_highs = [value for value in highs if value is not None]
            outcome["mfe3_pct"] = pct(base, max(valid_highs)) if valid_highs else None
        if len(future) >= 5:
            outcome["return_t5_pct"] = pct(base, rows.get(future[4], {}).get("close"))
            lows = [rows.get(date, {}).get("low") for date in future[:5]]
            valid_lows = [value for value in lows if value is not None]
            outcome["mae5_pct"] = pct(base, min(valid_lows)) if valid_lows else None
            if outcome["return_t5_pct"] is not None and market_median is not None:
                outcome["market_excess_t5_pct"] = round(
                    outcome["return_t5_pct"] - market_median, 6
                )
        score = score_pick(outcome)
        outcome.update(score)
        output.append(outcome)
    return output


def safe_median(values: Iterable[float | None]) -> float | None:
    valid = [float(value) for value in values if value is not None]
    return round(statistics.median(valid), 4) if valid else None


def safe_mean(values: Iterable[float | None]) -> float | None:
    valid = [float(value) for value in values if value is not None]
    return round(statistics.fmean(valid), 6) if valid else None


def ranked(values: List[float]) -> List[float]:
    """Return average ranks for ties without adding a scipy dependency."""
    ordered = sorted(enumerate(values), key=lambda item: item[1])
    result = [0.0] * len(values)
    start = 0
    while start < len(ordered):
        end = start + 1
        while end < len(ordered) and ordered[end][1] == ordered[start][1]:
            end += 1
        average_rank = (start + 1 + end) / 2.0
        for index in range(start, end):
            result[ordered[index][0]] = average_rank
        start = end
    return result


def spearman_pairs(pairs: Iterable[tuple[float, float]], minimum: int = CROSS_SECTION_MIN_COUNT):
    clean = [(float(left), float(right)) for left, right in pairs]
    count = len(clean)
    if count < minimum:
        return {"count": count, "rank_ic": None, "status": "INSUFFICIENT_COUNT"}
    left_ranks = ranked([item[0] for item in clean])
    right_ranks = ranked([item[1] for item in clean])
    left_mean = statistics.fmean(left_ranks)
    right_mean = statistics.fmean(right_ranks)
    numerator = sum(
        (left - left_mean) * (right - right_mean)
        for left, right in zip(left_ranks, right_ranks)
    )
    left_ss = sum((value - left_mean) ** 2 for value in left_ranks)
    right_ss = sum((value - right_mean) ** 2 for value in right_ranks)
    if left_ss <= 0 or right_ss <= 0:
        return {"count": count, "rank_ic": None, "status": "CONSTANT_INPUT"}
    return {
        "count": count,
        "rank_ic": round(numerator / (left_ss * right_ss) ** 0.5, 6),
        "status": "OK",
    }


def annotate_pick(
    outcome: Dict[str, Any],
    candidate: Dict[str, Any],
    signal_score: float | None,
    signal_source: str,
) -> Dict[str, Any]:
    classification = candidate.get("classification") or {}
    return {
        **outcome,
        "signal_score": signal_score,
        "signal_source": signal_source,
        "primary_path": classification.get("primary_path") or "UNCLASSIFIED",
        "source_channels": list(candidate.get("source_channels") or []),
    }


def group_returns(picks: List[Dict[str, Any]], return_key: str) -> List[Dict[str, Any]]:
    eligible = [
        pick for pick in picks
        if pick.get("signal_score") is not None and pick.get(return_key) is not None
    ]
    eligible.sort(key=lambda pick: (-float(pick["signal_score"]), pick["symbol"]))
    groups = min(CROSS_SECTION_GROUPS, len(eligible))
    output = []
    for group_index in range(groups):
        start = group_index * len(eligible) // groups
        end = (group_index + 1) * len(eligible) // groups
        rows = eligible[start:end]
        output.append({
            "group": f"Q{group_index + 1}",
            "rank_order": "highest_signal_first",
            "count": len(rows),
            "mean_return_pct": safe_mean(row.get(return_key) for row in rows),
        })
    return output


def daily_cross_section(picks: List[Dict[str, Any]]) -> Dict[str, Any]:
    ordered = sorted(
        (pick for pick in picks if pick.get("signal_score") is not None),
        key=lambda pick: (-float(pick["signal_score"]), pick["symbol"]),
    )
    metrics = {}
    for return_key in ("return_t1_pct", "return_t3_pct", "return_t5_pct"):
        pairs = [
            (pick["signal_score"], pick[return_key])
            for pick in ordered if pick.get(return_key) is not None
        ]
        metrics[return_key] = {
            **spearman_pairs(pairs),
            "top_k": min(CROSS_SECTION_TOP_K, len(pairs)),
            "top_k_mean_return_pct": safe_mean(
                pick.get(return_key) for pick in ordered[:CROSS_SECTION_TOP_K]
            ),
            "all_mean_return_pct": safe_mean(pick.get(return_key) for pick in ordered),
            "groups": group_returns(ordered, return_key),
        }
    return metrics


def transition_metrics(daily: List[Dict[str, Any]], team_key: str) -> Dict[str, Any]:
    transitions = []
    for previous, current in zip(daily, daily[1:]):
        previous_picks = previous[team_key]["picks"]
        current_picks = current[team_key]["picks"]
        previous_symbols = {pick["symbol"] for pick in previous_picks}
        current_symbols = {pick["symbol"] for pick in current_picks}
        union = previous_symbols | current_symbols
        intersection = previous_symbols & current_symbols
        turnover = 1.0 - len(intersection) / len(union) if union else None
        previous_scores = {
            pick["symbol"]: pick.get("signal_score") for pick in previous_picks
            if pick.get("signal_score") is not None
        }
        current_scores = {
            pick["symbol"]: pick.get("signal_score") for pick in current_picks
            if pick.get("signal_score") is not None
        }
        shared = sorted(set(previous_scores) & set(current_scores))
        autocorrelation = spearman_pairs(
            [(previous_scores[symbol], current_scores[symbol]) for symbol in shared]
        )
        transitions.append({
            "from_trade_date": previous["trade_date"],
            "to_trade_date": current["trade_date"],
            "shared_count": len(intersection),
            "candidate_turnover": round(turnover, 6) if turnover is not None else None,
            "signal_rank_autocorrelation": autocorrelation,
        })
    return {
        "transition_count": len(transitions),
        "mean_candidate_turnover": safe_mean(
            item.get("candidate_turnover") for item in transitions
        ),
        "mean_signal_rank_autocorrelation": safe_mean(
            item["signal_rank_autocorrelation"].get("rank_ic") for item in transitions
        ),
        "transitions": transitions,
    }


def stratified_summary(rows: Iterable[tuple[str, Dict[str, Any]]]) -> Dict[str, Any]:
    grouped = defaultdict(list)
    for label, pick in rows:
        grouped[str(label or "UNKNOWN")].append(pick)
    return {
        label: {
            "count": len(picks),
            "settled_t5_count": sum(pick.get("return_t5_pct") is not None for pick in picks),
            "mean_t1_pct": safe_mean(pick.get("return_t1_pct") for pick in picks),
            "mean_t3_pct": safe_mean(pick.get("return_t3_pct") for pick in picks),
            "mean_t5_pct": safe_mean(pick.get("return_t5_pct") for pick in picks),
            "mean_market_excess_t5_pct": safe_mean(
                pick.get("market_excess_t5_pct") for pick in picks
            ),
        }
        for label, picks in sorted(grouped.items())
    }


def build_cross_sectional_evaluation(daily: List[Dict[str, Any]]) -> Dict[str, Any]:
    settled_days = sum(item.get("settlement_status") == "SETTLED_T5" for item in daily)
    teams = {}
    for team_key, signal_definition in (
        ("legacy_l1", "frozen_signal_day_ret_1d"),
        ("path_l1", "frozen_best_channel_percentile"),
    ):
        daily_metrics = [
            {"trade_date": item["trade_date"], "market_regime": item["market_regime"],
             "metrics": daily_cross_section(item[team_key]["picks"])}
            for item in daily
        ]
        teams[team_key] = {
            "signal_definition": signal_definition,
            "daily": daily_metrics,
            "mean_rank_ic": {
                return_key: safe_mean(
                    row["metrics"][return_key].get("rank_ic") for row in daily_metrics
                )
                for return_key in ("return_t1_pct", "return_t3_pct", "return_t5_pct")
            },
            "stability": transition_metrics(daily, team_key),
        }
    path_rows = [
        (pick.get("primary_path") or "UNCLASSIFIED", pick)
        for item in daily for pick in item["path_l1"]["picks"]
    ]
    regime_rows = [
        (item.get("market_regime") or "UNKNOWN", pick)
        for item in daily for pick in item["path_l1"]["picks"]
    ]
    return {
        "schema_version": "l1_cross_sectional_evaluation_v0.1",
        "status": "ACTIVE_READ_ONLY" if settled_days >= CROSS_SECTION_MIN_DAYS else "INSUFFICIENT_SETTLED_DAYS",
        "settled_days": settled_days,
        "minimum_settled_days": CROSS_SECTION_MIN_DAYS,
        "point_in_time_policy": {
            "signal_source": "frozen_l1_path_observer_artifact",
            "legacy_signal": "signal-day ret_1d stored in candidate features",
            "path_signal": "signal-day best_channel_percentile stored in path_admission",
            "outcome_source": "future fact_daily rows after signal trade_date",
            "current_attributes_used_for_historical_ranking": False,
        },
        "teams": teams,
        "path_stratification": stratified_summary(path_rows),
        "market_regime_stratification": stratified_summary(regime_rows),
        "decision_authority": "NONE_OBSERVATION_ONLY",
    }


def summarize_team(picks: List[Dict[str, Any]], target_count: int = TEAM_TARGET):
    settled = [pick for pick in picks if pick.get("status") == "SETTLED_T5"]
    scores = [float(pick["points"]) for pick in settled]
    return {
        "target_count": target_count, "selected_count": len(picks),
        "settled_count": len(settled),
        "coverage": round(len(settled) / target_count, 4) if target_count else 0.0,
        "mean_points": round(statistics.fmean(scores), 4) if scores else None,
        "positive_score_rate": round(
            sum(score > 0 for score in scores) / len(scores), 4
        ) if scores else None,
        "followthrough_success_rate": round(
            sum(bool(pick["followthrough_success"]) for pick in settled) / len(settled), 4
        ) if settled else None,
        "median_t3_pct": safe_median(pick.get("return_t3_pct") for pick in settled),
        "median_t5_pct": safe_median(pick.get("return_t5_pct") for pick in settled),
        "median_market_excess_t5_pct": safe_median(
            pick.get("market_excess_t5_pct") for pick in settled
        ),
        "failed_spike_rate": round(
            sum(bool(pick["failed_spike"]) for pick in settled) / len(settled), 4
        ) if settled else None,
        "tail_loss_rate_mae5": round(
            sum(pick.get("mae5_pct") is not None and pick["mae5_pct"] <= -5.0 for pick in settled)
            / len(settled), 4
        ) if settled else None,
    }


def day_winner(legacy: Dict[str, Any], path: Dict[str, Any]):
    if legacy["coverage"] < MIN_COVERAGE or path["coverage"] < MIN_COVERAGE:
        return "PENDING_COVERAGE", None
    difference = round(path["mean_points"] - legacy["mean_points"], 4)
    if difference > DRAW_MARGIN:
        return "PATH_L1", difference
    if difference < -DRAW_MARGIN:
        return "LEGACY_L1", difference
    return "DRAW", difference


def episode_anchors(daily_matches: List[Dict[str, Any]], team_key: str):
    last_seen = {}
    anchors = []
    for day_index, match in enumerate(daily_matches):
        for pick in match[team_key]["picks"]:
            symbol = pick["symbol"]
            if symbol not in last_seen or day_index - last_seen[symbol] > 5:
                anchors.append(pick)
            last_seen[symbol] = day_index
    return anchors


def build_scorecard(conn, snapshots: List[Dict[str, Any]]):
    daily = []
    for snapshot in snapshots:
        trade_date = snapshot["trade_date"]
        teams = snapshot["scorecard_sets"]
        legacy_symbols = list(teams["legacy_l1_top50"])
        path_symbols = list(teams["path_l1_top50"])
        future = future_dates(conn, trade_date)
        median = market_median_t5(conn, trade_date, future[4]) if len(future) >= 5 else None
        union = sorted(set(legacy_symbols) | set(path_symbols))
        outcomes = {
            item["symbol"]: item
            for item in evaluate_symbols(conn, trade_date, union, future, median)
        }
        candidates = {
            str(candidate.get("symbol")): candidate
            for candidate in snapshot.get("candidates") or []
            if candidate.get("symbol")
        }
        path_admission = {
            str(item.get("symbol")): item
            for item in teams.get("path_admission") or []
            if item.get("symbol")
        }
        legacy_picks = [
            annotate_pick(
                outcomes[symbol], candidates.get(symbol, {}),
                fnum((candidates.get(symbol, {}).get("features") or {}).get("ret_1d")),
                "frozen_signal_day_ret_1d",
            )
            for symbol in legacy_symbols
        ]
        path_picks = [
            annotate_pick(
                outcomes[symbol], candidates.get(symbol, {}),
                fnum(path_admission.get(symbol, {}).get("best_channel_percentile")),
                "frozen_best_channel_percentile",
            )
            for symbol in path_symbols
        ]
        new_only_symbols = [symbol for symbol in path_symbols if symbol not in set(legacy_symbols)]
        path_pick_map = {pick["symbol"]: pick for pick in path_picks}
        new_only_picks = [path_pick_map[symbol] for symbol in new_only_symbols]
        legacy_summary = summarize_team(legacy_picks)
        path_summary = summarize_team(path_picks)
        winner, difference = day_winner(legacy_summary, path_summary)
        daily.append({
            "trade_date": trade_date,
            "market_regime": snapshot.get("market_regime_observed") or "UNKNOWN",
            "settlement_status": "SETTLED_T5" if len(future) >= 5 else "PENDING_T5",
            "future_dates": future, "market_median_t5_pct": median,
            "legacy_l1": {"summary": legacy_summary, "picks": legacy_picks},
            "path_l1": {"summary": path_summary, "picks": path_picks},
            "new_only": {"summary": summarize_team(new_only_picks, len(new_only_symbols)), "picks": new_only_picks},
            "winner": winner, "mean_points_difference_path_minus_legacy": difference,
        })
    settled_daily = [item for item in daily if item["winner"] != "PENDING_COVERAGE"]
    legacy_all = [pick for item in daily for pick in item["legacy_l1"]["picks"]]
    path_all = [pick for item in daily for pick in item["path_l1"]["picks"]]
    new_only_all = [pick for item in daily for pick in item["new_only"]["picks"]]
    league = {
        "settled_match_days": len(settled_daily),
        "path_wins": sum(item["winner"] == "PATH_L1" for item in settled_daily),
        "legacy_wins": sum(item["winner"] == "LEGACY_L1" for item in settled_daily),
        "draws": sum(item["winner"] == "DRAW" for item in settled_daily),
        "legacy_daily_pick_summary": summarize_team(legacy_all, len(daily) * TEAM_TARGET),
        "path_daily_pick_summary": summarize_team(path_all, len(daily) * TEAM_TARGET),
        "new_only_daily_pick_summary": summarize_team(new_only_all, len(new_only_all)),
        "legacy_episode_summary": summarize_team(
            episode_anchors(daily, "legacy_l1"),
            len(episode_anchors(daily, "legacy_l1")),
        ),
        "path_episode_summary": summarize_team(
            episode_anchors(daily, "path_l1"),
            len(episode_anchors(daily, "path_l1")),
        ),
    }
    path_summary = league["path_daily_pick_summary"]
    legacy_summary = league["legacy_daily_pick_summary"]
    new_summary = league["new_only_daily_pick_summary"]
    if league["settled_match_days"] < 20:
        promotion = {"status": "NOT_ENOUGH_SETTLED_DAYS", "eligible_for_l2_l4_canary": False}
    else:
        checks = {
            "path_wins_at_least_12": league["path_wins"] >= 12,
            "mean_points_lead_at_least_0_5": (
                path_summary["mean_points"] - legacy_summary["mean_points"] >= 0.5
            ),
            "followthrough_lead_at_least_5pp": (
                path_summary["followthrough_success_rate"]
                - legacy_summary["followthrough_success_rate"] >= 0.05
            ),
            "new_only_mean_points_positive": (
                new_summary["mean_points"] is not None and new_summary["mean_points"] > 0
            ),
            "tail_loss_rate_not_worse": (
                path_summary["tail_loss_rate_mae5"] <= legacy_summary["tail_loss_rate_mae5"]
            ),
        }
        passed = all(checks.values())
        promotion = {
            "status": "READY_FOR_DESIGN_REVIEW" if passed else "KEEP_OBSERVING",
            "eligible_for_l2_l4_canary": False,
            "checks": checks,
            "note": "Passing only permits a separate canary design review; it never activates L2-L4.",
        }
    return daily, league, promotion, build_cross_sectional_evaluation(daily)


def render_markdown(payload: Dict[str, Any]) -> str:
    league = payload["league_table"]
    legacy = league["legacy_daily_pick_summary"]
    path = league["path_daily_pick_summary"]
    new_only = league["new_only_daily_pick_summary"]

    def display(value: Any) -> Any:
        return "PENDING" if value is None else value

    lines = [
        f"# L1 Simple Scorecard - {payload['batch_id']}", "",
        "## 1. Match Status", "",
        f"- settled match days: {league['settled_match_days']}",
        f"- PATH wins: {league['path_wins']}",
        f"- LEGACY wins: {league['legacy_wins']}",
        f"- draws: {league['draws']}",
        f"- promotion status: `{payload['promotion_gate']['status']}`", "",
        "## 2. League Table", "",
        "| Team | Settled | Mean points | Follow-through | Median T+5 | Median market excess | Tail loss |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for name, summary in (("LEGACY_L1", legacy), ("PATH_L1", path), ("NEW_ONLY", new_only)):
        lines.append(
            f"| {name} | {summary['settled_count']} | {display(summary['mean_points'])} | "
            f"{display(summary['followthrough_success_rate'])} | "
            f"{display(summary['median_t5_pct'])} | "
            f"{display(summary['median_market_excess_t5_pct'])} | "
            f"{display(summary['tail_loss_rate_mae5'])} |"
        )
    lines.extend([
        "", "## 3. Cross-Sectional Evaluation", "",
        f"- status: `{payload['cross_sectional_evaluation']['status']}`",
        f"- settled days: {payload['cross_sectional_evaluation']['settled_days']}",
        "- authority: `NONE_OBSERVATION_ONLY`", "",
        "| Team | Mean IC T+1 | Mean IC T+3 | Mean IC T+5 | Turnover | Signal autocorrelation |",
        "|---|---:|---:|---:|---:|---:|",
    ])
    for team_key, label in (("legacy_l1", "LEGACY_L1"), ("path_l1", "PATH_L1")):
        team = payload["cross_sectional_evaluation"]["teams"][team_key]
        lines.append(
            f"| {label} | {display(team['mean_rank_ic']['return_t1_pct'])} | "
            f"{display(team['mean_rank_ic']['return_t3_pct'])} | "
            f"{display(team['mean_rank_ic']['return_t5_pct'])} | "
            f"{display(team['stability']['mean_candidate_turnover'])} | "
            f"{display(team['stability']['mean_signal_rank_autocorrelation'])} |"
        )
    lines.extend([
        "", "## 4. Daily Matches", "",
        "| Date | Status | Legacy points | Path points | Difference | Winner | New-only points |",
        "|---|---|---:|---:|---:|---|---:|",
    ])
    for item in payload["daily_matches"]:
        lines.append(
            f"| {item['trade_date']} | {item['settlement_status']} | "
            f"{display(item['legacy_l1']['summary']['mean_points'])} | "
            f"{display(item['path_l1']['summary']['mean_points'])} | "
            f"{display(item['mean_points_difference_path_minus_legacy'])} | "
            f"{item['winner']} | {display(item['new_only']['summary']['mean_points'])} |"
        )
    lines.extend([
        "", "## 5. Interpretation Boundary", "",
        "This scorecard measures post-selection price direction from the signal-day close. "
        "It is not an executable return, buy instruction, or L2-L4 verdict.",
        "", "## 6. Blocked Actions", "",
    ])
    lines.extend(f"- `{action}`" for action in payload["blocked_actions"])
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only OLD vs PATH L1 league table")
    parser.add_argument("--input", nargs="+", default=[], help="L1 observer JSON report(s)")
    parser.add_argument("--input-dir", default="", help="Directory containing frozen observer JSON reports")
    parser.add_argument("--forward-start", default="", help="Earliest signal date included in the formal scorecard")
    parser.add_argument("--output-dir", default=str(REPORT_DIR))
    args = parser.parse_args()
    if args.forward_start:
        datetime.strptime(args.forward_start, "%Y-%m-%d")
    paths = discover_input_paths(args.input, args.input_dir)
    sources, snapshots = load_snapshots(paths, args.forward_start)
    with DBGateway(DB_PATH, read_only=True) as conn:
        daily, league, promotion, cross_sectional = build_scorecard(conn, snapshots)
    start, end = snapshots[0]["trade_date"], snapshots[-1]["trade_date"]
    batch = start.replace("-", "") if start == end else f"{start.replace('-', '')}_{end.replace('-', '')}"
    payload = {
        "schema_version": "l1_simple_scorecard_v0.2", "batch_id": batch,
        "generated_at": datetime.now().astimezone().isoformat(),
        "mode": "read_only_evaluation", "observer_only": True,
        "no_trade_signal": True, "source_reports": sources,
        "team_policy": {
            "team_size": TEAM_TARGET,
            "universe": "PERSONAL_MAINLAND_A_SHARE",
            "legacy": "signal_day_pct_chg_top50",
            "path": "frozen_normalized_channel_rank_top50",
        },
        "point_rules": POINT_RULES, "daily_matches": daily,
        "league_table": league, "promotion_gate": promotion,
        "cross_sectional_evaluation": cross_sectional,
        "blocked_actions": BLOCKED_ACTIONS,
    }
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"l1_simple_scorecard_{batch}.json"
    md_path = output_dir / f"l1_simple_scorecard_{batch}.md"
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    md_path.write_text(render_markdown(payload), encoding="utf-8")
    print(json.dumps({
        "json": str(json_path), "markdown": str(md_path),
        "batch_id": batch, "settled_match_days": league["settled_match_days"],
        "path_wins": league["path_wins"], "legacy_wins": league["legacy_wins"],
        "draws": league["draws"], "promotion_status": promotion["status"],
        "observer_only": True, "no_trade_signal": True,
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
