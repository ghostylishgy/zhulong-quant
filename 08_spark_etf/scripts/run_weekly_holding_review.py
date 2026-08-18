from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = BASE_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from spark_etf.services.holding_report_service import HoldingReportService


def _parse_date(value: str):
    return datetime.strptime(value, "%Y-%m-%d").date()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Spark ETF weekly holding review and optionally push it")
    parser.add_argument("--date", type=_parse_date, default=None, help="Snapshot date, YYYY-MM-DD; latest if omitted")
    parser.add_argument("--db", type=str, default=None, help="DuckDB path")
    parser.add_argument("--push", action="store_true", help="Send pushplus message")
    parser.add_argument("--no-refresh-nav", action="store_true", help="Use cached NAV only")
    args = parser.parse_args()

    service = HoldingReportService(db_path=args.db)
    result = service.build_weekly_review(snapshot_date=args.date, refresh_nav=(not args.no_refresh_nav))
    pushed = service.send_push(result.title, result.content) if args.push else None
    print(result.content)
    print(f"\npush_status: {'not_requested' if pushed is None else ('sent' if pushed else 'failed')}")
    return 0 if pushed is not False else 2


if __name__ == "__main__":
    raise SystemExit(main())
