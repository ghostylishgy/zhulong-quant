from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Any
from uuid import uuid4

import duckdb

from spark_etf.db.db_init import DEFAULT_DB_PATH, init_db

STRATEGIES = (
    "dca_naive",
    "hold_only",
    "dca_dynamic_amount",
    "dca_dynamic_plus_valuation_tp",
    "dca_dynamic_plus_free_ride",
)
VALUATION_DEPENDENT_STRATEGIES = {
    "dca_dynamic_amount",
    "dca_dynamic_plus_valuation_tp",
    "dca_dynamic_plus_free_ride",
}


@dataclass(slots=True)
class StrategyDistribution:
    strategy: str
    xirr_values: list[float]
    max_dd_values: list[float]
    dd_recovery_days_values: list[int]
    wrong_sell_opportunity_cost: float = 0.0
    drawdown_saved: float = 0.0
    has_valuation_series: bool = True


@dataclass(slots=True)
class Verdict:
    verdict: str
    rationale: str


class BacktestService:
    def __init__(self, db_path: Path | str | None = None) -> None:
        self.db_path = init_db(db_path or DEFAULT_DB_PATH)

    def create_run(self, params: dict[str, Any], universe: list[dict[str, Any]], start_dates: list[str]) -> str:
        run_id = str(uuid4())
        conn = duckdb.connect(str(self.db_path))
        try:
            conn.execute(
                """
                INSERT INTO spark_backtest_run (
                    run_id, params_json, universe_json, start_dates_json
                ) VALUES (?, ?, ?, ?)
                """,
                [
                    run_id,
                    json.dumps(params, ensure_ascii=False),
                    json.dumps(universe, ensure_ascii=False),
                    json.dumps(start_dates, ensure_ascii=False),
                ],
            )
        finally:
            conn.close()
        return run_id

    def persist_verdict(self, run_id: str, bucket: str, strategy: str, verdict: Verdict) -> None:
        conn = duckdb.connect(str(self.db_path))
        try:
            conn.execute(
                """
                INSERT INTO spark_backtest_verdict (
                    run_id, bucket, strategy, verdict, rationale
                ) VALUES (?, ?, ?, ?, ?)
                """,
                [run_id, bucket, strategy, verdict.verdict, verdict.rationale],
            )
        finally:
            conn.close()

    @staticmethod
    def judge_distribution(
        candidate: StrategyDistribution,
        baseline: StrategyDistribution,
    ) -> Verdict:
        if candidate.strategy in VALUATION_DEPENDENT_STRATEGIES and not candidate.has_valuation_series:
            return Verdict("SKIPPED", "valuation-dependent strategy skipped because valuation series is missing")
        if not candidate.xirr_values or not baseline.xirr_values:
            return Verdict("SKIPPED", "missing XIRR distribution")

        candidate_xirr = median(candidate.xirr_values)
        baseline_xirr = median(baseline.xirr_values)
        xirr_pass = candidate_xirr >= baseline_xirr

        candidate_dd = median(candidate.max_dd_values) if candidate.max_dd_values else None
        baseline_dd = median(baseline.max_dd_values) if baseline.max_dd_values else None
        candidate_recovery = median(candidate.dd_recovery_days_values) if candidate.dd_recovery_days_values else None
        baseline_recovery = median(baseline.dd_recovery_days_values) if baseline.dd_recovery_days_values else None

        dd_pass = False
        if candidate_dd is not None and baseline_dd is not None and candidate_dd <= baseline_dd:
            dd_pass = True
        if candidate_recovery is not None and baseline_recovery is not None and candidate_recovery < baseline_recovery:
            dd_pass = True

        sell_cost_pass = candidate.wrong_sell_opportunity_cost < candidate.drawdown_saved

        if xirr_pass and dd_pass and sell_cost_pass:
            return Verdict(
                "DISCIPLINE",
                "median XIRR beats baseline, drawdown/recovery gate passes, and wrong-sell cost is below drawdown saved",
            )

        failed = []
        if not xirr_pass:
            failed.append(f"median_xirr {candidate_xirr:.6f} < baseline {baseline_xirr:.6f}")
        if not dd_pass:
            failed.append("drawdown/recovery gate failed")
        if not sell_cost_pass:
            failed.append("wrong-sell opportunity cost >= drawdown saved")
        return Verdict("ADVISORY_ONLY", "; ".join(failed))


def main() -> int:
    parser = argparse.ArgumentParser(description="Spark ETF backtest service scaffold")
    parser.add_argument("--db", type=str, default=None, help="DuckDB path")
    parser.add_argument("--create-run", action="store_true", help="Create an empty scaffold run")
    args = parser.parse_args()

    service = BacktestService(db_path=args.db)
    if args.create_run:
        run_id = service.create_run(
            params={"status": "scaffold", "methodology": "synthetic_dca_adjusted_nav"},
            universe=[],
            start_dates=[],
        )
        print(json.dumps({"run_id": run_id}, ensure_ascii=False))
    else:
        print(json.dumps({"strategies": STRATEGIES}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
