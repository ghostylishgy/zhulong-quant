from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb

from spark_etf.data.tushare_client import DATA_OK, NO_DATA, STALE_DATA, TushareClient
from spark_etf.db.db_init import DEFAULT_DB_PATH, init_db


class NavService:
    def __init__(self, db_path: Path | str | None = None, tushare_client: TushareClient | None = None) -> None:
        self.db_path = init_db(db_path or DEFAULT_DB_PATH)
        self.tushare_client = tushare_client or TushareClient()

    def fetch_latest_nav(self, fund_code: str) -> dict[str, Any]:
        try:
            if self.tushare_client.use_mock:
                payload = self.tushare_client.get_valuation_and_price(fund_code, valuation_proxy=None)
            else:
                payload = self.tushare_client.get_fund_nav(fund_code)
            nav_date = self._parse_tushare_date(str(payload["trade_date"]))
            result = {
                "fund_code": fund_code,
                "nav_date": nav_date,
                "unit_nav": float(payload.get("unit_nav", payload["nav"])),
                "adj_nav": float(payload.get("adj_nav", payload.get("unit_nav", payload["nav"]))),
                "source": "tushare_fund_nav",
                "data_quality": DATA_OK,
                "raw": payload,
            }
            if (date.today() - nav_date).days > 10:
                result["data_quality"] = STALE_DATA
            self.upsert_nav(result)
            return result
        except Exception as exc:
            return {
                "fund_code": fund_code,
                "nav_date": None,
                "unit_nav": None,
                "adj_nav": None,
                "source": "tushare_fund_nav",
                "data_quality": NO_DATA,
                "raw": {"error": str(exc)},
            }

    def fetch_nav_history(
        self,
        fund_code: str,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int = 120,
    ) -> dict[str, Any]:
        try:
            rows = self.tushare_client.get_fund_nav_history(
                fund_code,
                start_date=start_date,
                end_date=end_date,
                limit=limit,
            )
            payloads = []
            for row in rows:
                nav_date = self._parse_tushare_date(str(row["trade_date"]))
                payloads.append(
                    {
                        "fund_code": fund_code,
                        "nav_date": nav_date,
                        "unit_nav": float(row["unit_nav"]),
                        "adj_nav": float(row.get("adj_nav", row["unit_nav"])),
                        "source": "tushare_fund_nav",
                        "data_quality": DATA_OK,
                        "raw": row,
                    }
                )
            self.upsert_nav_rows(payloads)
            latest_date = max((item["nav_date"] for item in payloads), default=None)
            return {
                "fund_code": fund_code,
                "requested_limit": int(limit),
                "rows_fetched": len(rows),
                "rows_upserted": len(payloads),
                "latest_nav_date": latest_date,
                "data_quality": DATA_OK if payloads else NO_DATA,
            }
        except Exception as exc:
            return {
                "fund_code": fund_code,
                "requested_limit": int(limit),
                "rows_fetched": 0,
                "rows_upserted": 0,
                "latest_nav_date": None,
                "data_quality": NO_DATA,
                "error": str(exc),
            }

    def refresh_nav_history(
        self,
        fund_codes: list[str],
        lookback_days: int = 180,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> dict[str, Any]:
        effective_end = end_date or date.today()
        effective_start = start_date or (effective_end - timedelta(days=int(lookback_days)))
        limit = max(int(lookback_days) + 20, 60)
        results = [
            self.fetch_nav_history(
                fund_code,
                start_date=effective_start,
                end_date=effective_end,
                limit=limit,
            )
            for fund_code in fund_codes
        ]
        return {
            "start_date": effective_start,
            "end_date": effective_end,
            "fund_count": len(fund_codes),
            "rows_upserted": sum(int(item.get("rows_upserted", 0)) for item in results),
            "failures": [item for item in results if item.get("data_quality") == NO_DATA],
            "results": results,
        }

    def latest_cached_nav(self, fund_code: str) -> dict[str, Any] | None:
        conn = duckdb.connect(str(self.db_path), read_only=True)
        try:
            row = conn.execute(
                """
                SELECT fund_code, nav_date, unit_nav, adj_nav, source, data_quality, raw_json
                FROM spark_nav_daily
                WHERE fund_code = ?
                ORDER BY nav_date DESC, created_at DESC
                LIMIT 1
                """,
                [fund_code],
            ).fetchone()
        finally:
            conn.close()

        if row is None:
            return None
        return {
            "fund_code": str(row[0]),
            "nav_date": row[1],
            "unit_nav": float(row[2]) if row[2] is not None else None,
            "adj_nav": float(row[3]) if row[3] is not None else None,
            "source": str(row[4]),
            "data_quality": str(row[5]),
            "raw": row[6],
        }

    def get_latest_nav(self, fund_code: str, refresh: bool = True) -> dict[str, Any]:
        if refresh:
            fetched = self.fetch_latest_nav(fund_code)
            if fetched["data_quality"] != NO_DATA:
                return fetched
        cached = self.latest_cached_nav(fund_code)
        if cached is not None:
            return cached
        return {
            "fund_code": fund_code,
            "nav_date": None,
            "unit_nav": None,
            "adj_nav": None,
            "source": "cache",
            "data_quality": NO_DATA,
            "raw": {"error": "no cached nav"},
        }

    def upsert_nav(self, payload: dict[str, Any]) -> None:
        self.upsert_nav_rows([payload])

    def upsert_nav_rows(self, payloads: list[dict[str, Any]]) -> None:
        if not payloads:
            return
        rows: list[list[Any]] = []
        for payload in payloads:
            nav_date = payload.get("nav_date")
            if nav_date is None:
                continue
            rows.append(
                [
                    payload["fund_code"],
                    nav_date,
                    payload.get("unit_nav"),
                    payload.get("adj_nav"),
                    payload.get("source", "unknown"),
                    payload.get("data_quality", DATA_OK),
                    json.dumps(payload.get("raw", {}), ensure_ascii=False),
                ]
            )
        if not rows:
            return

        conn = duckdb.connect(str(self.db_path))
        try:
            conn.executemany(
                """
                INSERT INTO spark_nav_daily (
                    fund_code, nav_date, unit_nav, adj_nav, source, data_quality, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (fund_code, nav_date, source) DO UPDATE SET
                    unit_nav = EXCLUDED.unit_nav,
                    adj_nav = EXCLUDED.adj_nav,
                    data_quality = EXCLUDED.data_quality,
                    raw_json = EXCLUDED.raw_json,
                    created_at = now()
                """,
                rows,
            )
        finally:
            conn.close()

    @staticmethod
    def _parse_tushare_date(value: str) -> date:
        text = value.strip()[:10]
        for fmt in ("%Y%m%d", "%Y-%m-%d"):
            try:
                return datetime.strptime(text, fmt).date()
            except ValueError:
                continue
        raise ValueError(f"invalid NAV date: {value}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch Spark ETF latest NAV into spark_nav_daily")
    parser.add_argument("fund_codes", nargs="+", help="Fund codes such as 020255.OF")
    parser.add_argument("--db", type=str, default=None, help="DuckDB path")
    args = parser.parse_args()

    service = NavService(db_path=args.db)
    rows = [service.get_latest_nav(code, refresh=True) for code in args.fund_codes]
    print(json.dumps(rows, ensure_ascii=False, default=str, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
