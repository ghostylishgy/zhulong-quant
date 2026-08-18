from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = BASE_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from spark_etf.config.loader import load_spark_config
from spark_etf.services.nav_service import NavService


def _parse_date(value: str):
    return datetime.strptime(value, "%Y-%m-%d").date()


def _default_codes(config_path: str | None) -> list[str]:
    config = load_spark_config(config_path)
    return [item.etf_code for item in config.portfolios]


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh Spark ETF NAV history into spark_nav_daily")
    parser.add_argument("--codes", nargs="*", default=None, help="Fund codes. Defaults to all spark.yaml portfolios.")
    parser.add_argument("--config", type=str, default=None, help="Spark config path")
    parser.add_argument("--db", type=str, default=None, help="DuckDB path")
    parser.add_argument("--lookback-days", type=int, default=180, help="History window if --start-date is omitted")
    parser.add_argument("--start-date", type=_parse_date, default=None, help="YYYY-MM-DD")
    parser.add_argument("--end-date", type=_parse_date, default=None, help="YYYY-MM-DD")
    parser.add_argument("--json-log", type=Path, default=None, help="Optional path for JSON summary")
    args = parser.parse_args()

    codes = args.codes if args.codes else _default_codes(args.config)
    service = NavService(db_path=args.db)
    result = service.refresh_nav_history(
        fund_codes=codes,
        lookback_days=args.lookback_days,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    result["generated_at"] = datetime.now().isoformat(timespec="seconds")

    text = json.dumps(result, ensure_ascii=False, default=str, indent=2)
    print(text)

    if args.json_log is not None:
        args.json_log.parent.mkdir(parents=True, exist_ok=True)
        args.json_log.write_text(text + "\n", encoding="utf-8")

    return 0 if not result["failures"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
