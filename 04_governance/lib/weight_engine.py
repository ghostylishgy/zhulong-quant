#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
04_governance/lib/weight_engine.py
WeightEngine — Bayesian Memory-Weighted Decision Modifier

Architecture:
    1. SSD v1.1 tag penalties + severity scale
    2. agg_tag_performance (DuckDB): historical SR per tag per regime
    3. Temporal Lambda: market regime -> time-decay multiplier
    4. Blacklist combos: hard-coded meltdown circuits
    5. Blind Spot Score: tag coverage gap analysis

Flow:
    tags + regime -> get_adjustment_factor() -> multiplier [0.5, 1.0]
    L4_score * multiplier -> adjusted_score
"""

import json
import math
import logging
import os
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime

logger = logging.getLogger('zhulong.weight_engine')

_current = Path(__file__).resolve()
PROJECT_ROOT = next(
    (p for p in _current.parents if (p / ".git").exists() or (p / "storage").exists()),
    _current.parents[2]
)
DB_PATH = str(PROJECT_ROOT / "storage" / "database" / "zhulong.duckdb")
SSD_PATH = PROJECT_ROOT / "04_governance" / "config" / "semantic_dictionary.json"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))
_CORE_DIR = PROJECT_ROOT / "04_governance" / "lib" / "core"
if str(_CORE_DIR) not in sys.path:
    sys.path.append(str(_CORE_DIR))
from module_loader import load_attr_from_path

DBGateway = load_attr_from_path(
    "db_gateway_01",
    PROJECT_ROOT / "01_engine" / "lib" / "db_gateway.py",
    "DBGateway",
)

TAG_PERF_MIN_SAMPLE = max(1, int(os.getenv("TAG_PERF_MIN_SAMPLE", "8")))
TAG_PERF_PRIOR_STRENGTH = max(0.0, float(os.getenv("TAG_PERF_PRIOR_STRENGTH", "8")))
TAG_PERF_HORIZON_DAYS = max(1, int(os.getenv("TAG_PERF_HORIZON_DAYS", "3")))
TAG_PERF_TAG_BASIS = "EX_ANTE_L2"
TAG_PERF_METRIC_VERSION = "v3"


# ==================== Data Structures ====================

@dataclass
class WeightResult:
    """Output of the weight engine"""
    adjustment_factor: float = 1.0     # [0.5, 1.0]
    temporal_lambda: float = 1.0       # regime-based decay
    tag_penalties: Dict[str, float] = field(default_factory=dict)
    blacklist_hit: bool = False
    blacklist_detail: str = ""
    blind_spot_score: float = 0.0      # 0=full coverage, 1=blind
    introspection: str = ""


@dataclass
class TagPerformance:
    """Historical tag performance stats"""
    tag: str
    regime: str
    total_count: int = 0
    success_count: int = 0
    success_rate: float = 0.0


# ==================== Table Init ====================

def ensure_tag_performance_table():
    """Create agg_tag_performance table in DuckDB"""
    with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS agg_tag_performance (
                tag             VARCHAR NOT NULL,
                regime          VARCHAR NOT NULL,
                total_count     INTEGER DEFAULT 0,
                success_count   INTEGER DEFAULT 0,
                success_rate    DOUBLE DEFAULT 0.0,
                tag_basis       VARCHAR DEFAULT 'LEGACY_OUTCOME',
                metric_version  VARCHAR DEFAULT 'v1',
                last_updated    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (tag, regime)
            )
        """)
        conn.execute("ALTER TABLE agg_tag_performance ADD COLUMN IF NOT EXISTS tag_basis VARCHAR DEFAULT 'LEGACY_OUTCOME'")
        conn.execute("ALTER TABLE agg_tag_performance ADD COLUMN IF NOT EXISTS metric_version VARCHAR DEFAULT 'v1'")
    logger.info("agg_tag_performance table ready")


def _split_tags(tags_text: Any) -> List[str]:
    seen = set()
    tags = []
    raw_tags = tags_text if isinstance(tags_text, (list, tuple, set)) else str(tags_text or "").split(",")
    for raw in raw_tags:
        tag = raw.strip()
        if tag and tag not in seen:
            seen.add(tag)
            tags.append(tag)
    return tags


def _regime_from_tide(tide_status: str) -> str:
    text = str(tide_status or "").upper()
    if "FORCE_NO_EDGE" in text or "BEAR" in text:
        return "BEAR"
    if "AGGRESSIVE" in text or "BULL" in text:
        return "BULL"
    if "CAUTION" in text or "SIDE" in text:
        return "SIDE"
    return "ALL"


def _strategy_success_label(facts: Dict[str, Any]) -> Optional[bool]:
    outcome = str(facts.get("outcome_label") or "").upper()
    quality = str(facts.get("decision_quality") or "").upper()
    if outcome in {"WIN", "POST_EXIT_UP"}:
        return True
    if outcome in {"LOSS", "POST_EXIT_DOWN"}:
        return False
    if any(token in quality for token in (
        "BUY_VALIDATED",
        "WIN_RUNNING",
        "SELL_PROTECTED_PROFIT",
        "SELL_VALIDATED",
    )):
        return True
    if any(token in quality for token in (
        "BUY_WEAK",
        "RISK_NEEDS_WATCH",
        "STOP_LOSS_CONFIRMED",
    )):
        return False
    try:
        return_pct = float(facts.get("return_pct") or 0.0)
    except Exception:
        return None
    if abs(return_pct) >= 0.005:
        return return_pct > 0
    return None


def _strategy_signal_task_id(facts: Dict[str, Any]) -> str:
    evidence = facts.get("strategy_evidence")
    if not isinstance(evidence, dict):
        return ""
    return str(evidence.get("signal_task_id") or "").strip()


def _is_fixed_horizon_buy_event(facts: Dict[str, Any]) -> bool:
    action = str(facts.get("action") or "").strip().upper()
    try:
        horizon = int(facts.get("horizon_days") or 0)
    except Exception:
        return False
    return action.endswith("_BUY") and horizon == TAG_PERF_HORIZON_DAYS


def calc_tag_success_rate(
    min_sample: Optional[int] = None,
    prior_strength: Optional[float] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Recalculate tag success rates from verified structured Shadow memories.

    Conservative contract:
      - Group only by ex-ante L2 decision tags captured before outcome.
      - Use one fixed-horizon buy outcome per independent audit task.
      - Write only tags with enough independent decisions.
    """
    ensure_tag_performance_table()
    min_sample = TAG_PERF_MIN_SAMPLE if min_sample is None else max(1, int(min_sample))
    prior_strength = (
        TAG_PERF_PRIOR_STRENGTH
        if prior_strength is None
        else max(0.0, float(prior_strength))
    )

    try:
        with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
            records = conn.execute(
                """
                SELECT tide_status, facts_json
                FROM fact_strategic_memory
                WHERE source LIKE 'strategy:%'
                  AND COALESCE(facts_json, '') != ''
                """
            ).fetchall()

        decisions: Dict[str, Tuple[str, Dict[str, Any], bool, List[str]]] = {}
        conflicted_task_ids = set()
        eligible_events = 0
        duplicate_events = 0
        skipped_non_fixed_horizon = 0
        skipped_missing_task_id = 0
        skipped_missing_decision_tags = 0

        for tide_status, facts_json in records:
            try:
                facts = json.loads(facts_json or "{}")
            except Exception:
                continue
            if not _is_fixed_horizon_buy_event(facts):
                skipped_non_fixed_horizon += 1
                continue
            task_id = _strategy_signal_task_id(facts)
            if not task_id:
                skipped_missing_task_id += 1
                continue
            success = _strategy_success_label(facts)
            if success is None:
                continue
            tags = _split_tags(facts.get("decision_tags"))
            if not tags:
                skipped_missing_decision_tags += 1
                continue

            eligible_events += 1
            candidate = (str(tide_status or ""), facts, success, tags)
            existing = decisions.get(task_id)
            if existing is None:
                decisions[task_id] = candidate
                continue
            duplicate_events += 1
            if existing[2] != success or set(existing[3]) != set(tags):
                conflicted_task_ids.add(task_id)

        for task_id in conflicted_task_ids:
            decisions.pop(task_id, None)

        stats: Dict[Tuple[str, str], List[int]] = {}
        global_total = 0
        global_success = 0
        for tide_status, _facts, success, tags in decisions.values():
            global_total += 1
            global_success += 1 if success else 0
            regimes = ["ALL"]
            regime = _regime_from_tide(tide_status)
            if regime != "ALL":
                regimes.append(regime)
            for regime_name in regimes:
                for tag in tags:
                    bucket = stats.setdefault((tag, regime_name), [0, 0])
                    bucket[0] += 1
                    bucket[1] += 1 if success else 0

        prior_rate = (global_success / global_total) if global_total else 0.5
        now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows = []
        for (tag, regime), (total_count, success_count) in sorted(stats.items()):
            if total_count < min_sample:
                continue
            if prior_strength > 0:
                success_rate = (
                    success_count + prior_rate * prior_strength
                ) / (total_count + prior_strength)
            else:
                success_rate = success_count / total_count if total_count else prior_rate
            rows.append((
                tag,
                regime,
                int(total_count),
                int(success_count),
                round(float(success_rate), 4),
                TAG_PERF_TAG_BASIS,
                TAG_PERF_METRIC_VERSION,
                now_text,
            ))

        result = {
            "records": len(records),
            "eligible_events": eligible_events,
            "labeled_events": len(decisions),
            "independent_decisions": len(decisions),
            "duplicate_events": duplicate_events,
            "conflicted_tasks": len(conflicted_task_ids),
            "global_total": global_total,
            "global_success": global_success,
            "global_success_rate": round(prior_rate, 4),
            "min_sample": min_sample,
            "prior_strength": prior_strength,
            "fixed_horizon_days": TAG_PERF_HORIZON_DAYS,
            "tag_basis": TAG_PERF_TAG_BASIS,
            "metric_version": TAG_PERF_METRIC_VERSION,
            "skipped_non_fixed_horizon": skipped_non_fixed_horizon,
            "skipped_missing_task_id": skipped_missing_task_id,
            "skipped_missing_decision_tags": skipped_missing_decision_tags,
            "generated_rows": len(rows),
            "rows": [
                {
                    "tag": r[0],
                    "regime": r[1],
                    "total_count": r[2],
                    "success_count": r[3],
                    "success_rate": r[4],
                    "tag_basis": r[5],
                    "metric_version": r[6],
                }
                for r in rows
            ],
        }
        if dry_run:
            logger.info(f"Tag success rates dry-run: {result}")
            return result

        with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
            conn.execute("DELETE FROM agg_tag_performance")
            if rows:
                conn.executemany(
                    """
                    INSERT INTO agg_tag_performance
                        (tag, regime, total_count, success_count, success_rate,
                         tag_basis, metric_version, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, CAST(? AS TIMESTAMP))
                    """,
                    rows,
                )
        logger.info(f"Tag success rates recalculated: {result}")
        return result
    except Exception as e:
        logger.warning(f"calc_tag_success_rate error: {e}")
        return {"error": str(e)}


# ==================== SSD Loader ====================

class SSDConfig:
    """SSD v1.1 configuration loader"""

    def __init__(self):
        self.tags: Dict[str, Dict] = {}
        self.severity_scale: Dict[str, Dict] = {}
        self.blacklist_combos: List[Dict] = []
        self._load()

    def _load(self):
        if not SSD_PATH.exists():
            logger.warning(f"SSD not found: {SSD_PATH}")
            return
        with open(SSD_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        self.tags = data.get("core_tags", {})
        self.severity_scale = data.get("severity_scale", {})
        self.blacklist_combos = data.get("blacklist_combinations", [])
        logger.info(f"SSD v{data.get('_version','?')} loaded: {len(self.tags)} tags, "
                    f"{len(self.blacklist_combos)} blacklist combos")

    def get_base_penalty(self, tag_label: str) -> float:
        """Get base_penalty for a tag"""
        for info in self.tags.values():
            if info.get("label") == tag_label:
                return info.get("base_penalty", 0.1)
        return 0.1

    def get_severity(self, tag_label: str) -> str:
        for info in self.tags.values():
            if info.get("label") == tag_label:
                return info.get("severity", "MEDIUM")
        return "MEDIUM"

    def get_lambda_floor(self, severity: str) -> float:
        return self.severity_scale.get(severity, {}).get("lambda_floor", 0.85)


# ==================== Weight Engine ====================

class WeightEngine:
    """
    Bayesian Memory-Weighted Score Modifier

    Input:  L4 raw score + SSD tags + current regime
    Output: adjustment factor [0.5, 1.0]

    WARNING: factor < 1.0 means PENALTY
             factor = 1.0 means NO CHANGE
    """

    def __init__(self):
        ensure_tag_performance_table()
        self.ssd = SSDConfig()

    # ==================== Temporal Lambda ====================

    def calculate_temporal_lambda(self, market_regime: str) -> float:
        """
        Market regime -> temporal decay multiplier.
        λ controls how aggressively historical penalties are applied.

        AGGRESSIVE (bull):  λ = 0.6  (memories less punitive)
        CAUTION (neutral):  λ = 0.85 (moderate penalty scaling)
        BEARISH (bear):     λ = 1.0  (full penalty, maximum caution)
        """
        regime_lambdas = {
            "AGGRESSIVE": 0.6,
            "CAUTION": 0.85,
            "FORCE_NO_EDGE": 1.0,
            "BEARISH": 1.0,
            "UNKNOWN": 0.85,
        }
        return regime_lambdas.get(market_regime.upper(), 0.85)

    # ==================== Bayesian Adjustment ====================

    def get_adjustment_factor(self, tags: List[str],
                              current_regime: str) -> WeightResult:
        """
        Main entry point: compute memory-weighted adjustment factor.

        Formula:
            penalty_i = base_penalty_i * (1 - SR_i) * λ
            total_penalty = Σ penalty_i
            factor = max(0.5, 1.0 - total_penalty)

        Where:
            SR_i = historical success rate of tag i in current regime
            λ = temporal lambda (regime-dependent)
        """
        result = WeightResult()

        if not tags:
            result.introspection = "No tags, no memory penalty."
            return result

        # 1. Temporal lambda
        lam = self.calculate_temporal_lambda(current_regime)
        result.temporal_lambda = lam

        # 2. Blacklist check (hard circuit breaker)
        bl_hit = self._check_blacklist(tags)
        if bl_hit:
            result.blacklist_hit = True
            result.blacklist_detail = bl_hit
            result.adjustment_factor = 0.5  # max penalty
            result.introspection = f"MELTDOWN: {bl_hit}"
            logger.warning(f"BLACKLIST MELTDOWN: {bl_hit}")
            return result

        # 3. Get historical SR from DuckDB
        tag_sr = self._get_tag_success_rates(tags, current_regime)

        # 4. Compute penalties
        total_penalty = 0.0
        for tag in tags:
            tag_clean = tag.strip()
            if not tag_clean:
                continue
            base_penalty = self.ssd.get_base_penalty(tag_clean)
            sr = tag_sr.get(tag_clean, 0.5)  # default 50% if no data

            # Bayesian: higher SR -> lower penalty
            penalty = base_penalty * (1.0 - sr) * lam
            result.tag_penalties[tag_clean] = round(penalty, 4)
            total_penalty += penalty

        # 5. Clamp to [0.5, 1.0]
        result.adjustment_factor = round(max(0.5, 1.0 - total_penalty), 4)

        # 6. Blind spot score
        result.blind_spot_score = self._calc_blind_spot(tags)

        # 7. Introspection
        result.introspection = self._build_introspection(result, tags, current_regime)

        return result

    def _check_blacklist(self, tags: List[str]) -> Optional[str]:
        """Check if any blacklist combination is present"""
        tag_set = {t.strip() for t in tags}
        for combo in self.ssd.blacklist_combos:
            combo_tags = set(combo.get("tags", []))
            if combo_tags.issubset(tag_set):
                return combo.get("description", "Unknown blacklist combo")
        return None

    def _get_tag_success_rates(self, tags: List[str],
                                regime: str) -> Dict[str, float]:
        """Retrieve historical success rates from agg_tag_performance"""
        try:
            result = {}
            regime_norm = _regime_from_tide(regime or "ALL")
            with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
                for tag in tags:
                    tag_clean = tag.strip()
                    if not tag_clean:
                        continue
                    row = conn.execute("""
                        SELECT success_rate
                        FROM agg_tag_performance
                        WHERE tag = ? AND regime IN (?, 'ALL')
                        ORDER BY CASE WHEN regime = ? THEN 0 WHEN regime = 'ALL' THEN 1 ELSE 2 END
                        LIMIT 1
                    """, [tag_clean, regime_norm, regime_norm]).fetchone()
                    if row:
                        result[tag_clean] = float(row[0])
            return result
        except Exception as e:
            logger.warning(f"Tag SR retrieval failed: {e}")
            return {}

    def _calc_blind_spot(self, tags: List[str]) -> float:
        """
        Blind spot score: how many of the SSD categories are NOT covered.
        0.0 = all categories represented
        1.0 = no tags match any category (fully blind)
        """
        all_categories = set(self.ssd.tags.keys())
        if not all_categories:
            return 0.0

        covered = set()
        tag_set = {t.strip() for t in tags}
        for key, info in self.ssd.tags.items():
            if info.get("label") in tag_set:
                covered.add(info.get("category", ""))

        total_cats = len(set(info.get("category","") for info in self.ssd.tags.values()))
        if total_cats == 0:
            return 0.0
        return round(1.0 - len(covered) / total_cats, 4)

    def _build_introspection(self, result: WeightResult,
                              tags: List[str], regime: str) -> str:
        """Build cognitive introspection string"""
        lines = [f"WeightEngine | regime={regime} λ={result.temporal_lambda}"]
        for tag, penalty in result.tag_penalties.items():
            lines.append(f"  {tag}: penalty={penalty:.4f}")
        lines.append(f"  adjustment_factor={result.adjustment_factor:.4f}")
        lines.append(f"  blind_spot={result.blind_spot_score:.2f}")
        return "\n".join(lines)

    # ==================== Probe Quota ====================

    def get_probe_quota_adjustment(self, blind_spot_score: float) -> Dict[str, Any]:
        """
        Blind Spot Hook: suggest probe quota adjustments.
        High blind spot -> increase L1 exploration breadth.
        """
        if blind_spot_score > 0.7:
            return {
                "action": "EXPAND",
                "l1_top_n_delta": +5,
                "reason": f"High blind spot ({blind_spot_score:.2f}): expand L1 for discovery"
            }
        elif blind_spot_score > 0.4:
            return {
                "action": "NORMAL",
                "l1_top_n_delta": 0,
                "reason": f"Moderate blind spot ({blind_spot_score:.2f})"
            }
        else:
            return {
                "action": "NORMAL",
                "l1_top_n_delta": 0,
                "reason": f"Good coverage ({blind_spot_score:.2f})"
            }


# ==================== Singleton ====================

_engine = None
def get_weight_engine() -> WeightEngine:
    global _engine
    if _engine is None:
        _engine = WeightEngine()
    return _engine


# ==================== CLI ====================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    engine = get_weight_engine()

    print("=" * 60)
    print("  WEIGHT ENGINE SELF-TEST")
    print("=" * 60)

    # Test temporal lambda
    print("\n--- Temporal Lambda ---")
    for regime in ["AGGRESSIVE", "CAUTION", "FORCE_NO_EDGE", "BEARISH"]:
        lam = engine.calculate_temporal_lambda(regime)
        print(f"  {regime:15s} -> λ = {lam}")

    # Test single tag
    print("\n--- Single Tag (CAUTION regime) ---")
    r = engine.get_adjustment_factor(["#BREAKOUT_TRAP"], "CAUTION")
    print(f"  Factor: {r.adjustment_factor}")
    print(f"  Penalties: {r.tag_penalties}")
    print(f"  Introspection:\n{r.introspection}")

    # Test multiple tags
    print("\n--- Multiple Tags ---")
    r = engine.get_adjustment_factor(
        ["#VOLUME_FRAUD", "#MOMENTUM_EXHAUSTION", "#TIDE_DRAG"],
        "CAUTION"
    )
    print(f"  Factor: {r.adjustment_factor}")
    print(f"  Blacklist: {r.blacklist_hit}")

    # Test blacklist
    print("\n--- Blacklist Combo ---")
    r = engine.get_adjustment_factor(
        ["#LIQUIDITY_TRAP", "#TIDE_DRAG"],
        "CAUTION"
    )
    print(f"  Factor: {r.adjustment_factor}")
    print(f"  Blacklist: {r.blacklist_hit}")
    print(f"  Detail: {r.blacklist_detail}")

    # Test blind spot
    print("\n--- Blind Spot ---")
    r = engine.get_adjustment_factor(["#BREAKOUT_TRAP"], "CAUTION")
    print(f"  Blind spot: {r.blind_spot_score}")
    quota = engine.get_probe_quota_adjustment(r.blind_spot_score)
    print(f"  Probe quota: {quota}")

    # Calc tag SR
    print("\n--- Tag SR Calculation ---")
    calc_tag_success_rate()
    print("  Done")
