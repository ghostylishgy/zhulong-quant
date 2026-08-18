from __future__ import annotations

import argparse
import json
from pathlib import Path
from uuid import uuid4

import duckdb

from spark_etf.config.loader import PortfolioItem, load_spark_config
from spark_etf.db.db_init import (
    DEFAULT_DB_PATH,
    EVENT_INSERT_SQL,
    SNAPSHOT_UPSERT_SQL,
    init_db,
)


def _derive_state(item: PortfolioItem) -> tuple[str, bool]:
    if item.avg_cost <= 0 or item.status_override:
        return "FREE_RIDE", True
    return "ACCUMULATING", False


def run_cold_start(config_path: Path | str | None = None, db_path: Path | str | None = None) -> int:
    config = load_spark_config(config_path)
    db_file = init_db(db_path or DEFAULT_DB_PATH)

    written = 0
    conn = duckdb.connect(str(db_file))
    try:
        conn.execute("BEGIN")
        for item in config.portfolios:
            state, principal_recovered = _derive_state(item)

            conn.execute(
                SNAPSHOT_UPSERT_SQL,
                [
                    item.etf_code,
                    state,
                    item.avg_cost,
                    item.total_invested,
                    item.current_shares,
                    principal_recovered,
                    0.0,
                ],
            )

            snapshot_payload = json.dumps(
                {
                    "source": "cold_start",
                    "avg_cost": item.avg_cost,
                    "current_shares": item.current_shares,
                    "total_invested": item.total_invested,
                    "tags": list(item.tags),
                    "status_override": item.status_override,
                },
                ensure_ascii=False,
            )

            conn.execute(
                EVENT_INSERT_SQL,
                [
                    str(uuid4()),
                    item.etf_code,
                    "STATE_TRANSITION",
                    snapshot_payload,
                    "Cold Start",
                ],
            )
            written += 1

        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()

    return written


def main() -> None:
    parser = argparse.ArgumentParser(description="Spark cold-start bootstrap")
    parser.add_argument("--config", type=str, default=None, help="Path to spark.yaml")
    parser.add_argument("--db", type=str, default=None, help="Path to DuckDB file")
    args = parser.parse_args()

    count = run_cold_start(config_path=args.config, db_path=args.db)
    print(f"[spark] cold start complete: {count} portfolios initialized")


if __name__ == "__main__":
    main()
