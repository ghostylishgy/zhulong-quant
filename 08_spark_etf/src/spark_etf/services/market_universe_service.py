from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

from spark_etf.data.tushare_client import TushareClient
from spark_etf.db.db_init import DEFAULT_DB_PATH, init_db


THEME_RULES: dict[str, tuple[str, ...]] = {
    "港股创新药": ("港股通创新药", "恒生港股通创新药", "港股创新药"),
    "创新药医药": ("创新药", "生物医药", "医药创新", "医疗", "医药", "恒生医疗"),
    "人工智能": ("人工智能", "AI", "智能"),
    "机器人": ("机器人",),
    "云计算大数据": ("云计算", "大数据", "软件服务", "工业软件", "数据"),
    "半导体芯片": ("半导体", "芯片", "集成电路", "芯片设计"),
    "通信算力": ("通信", "5G", "算力", "卫星通信"),
    "恒生港股科技": ("恒生科技", "港股科技", "恒生互联网科技"),
    "中概港股互联网": ("中概", "港股通互联网", "港股互联网", "互联网"),
    "黄金商品": ("黄金", "有色", "商品", "原油"),
    "红利防御": ("红利", "高股息", "股息", "公用事业", "电力"),
    "消费": ("消费", "食品饮料", "酒", "旅游"),
    "军工": ("军工", "国防", "航天", "航空"),
    "新能源": ("新能源", "光伏", "电池", "锂电", "储能", "电动车"),
    "金融地产": ("银行", "证券", "保险", "金融", "地产"),
    "宽基指数": ("沪深300", "中证500", "中证1000", "创业板", "科创板", "A500"),
}


@dataclass(frozen=True, slots=True)
class MarketFundRow:
    fund_code: str
    fund_name: str
    market: str
    management: str | None
    fund_type: str | None
    found_date: str | None
    status: str | None
    raw: dict[str, Any]


class MarketUniverseService:
    def __init__(
        self,
        db_path: Path | str | None = None,
        tushare_client: TushareClient | None = None,
    ) -> None:
        self.db_path = init_db(db_path or DEFAULT_DB_PATH)
        self.tushare_client = tushare_client or TushareClient()

    def refresh(self, markets: list[str], active_only: bool = True, limit: int | None = None) -> dict[str, Any]:
        funds: list[MarketFundRow] = []
        for market in markets:
            for row in self.tushare_client.get_fund_basic(market=market):
                parsed = self._parse_fund_row(row, market)
                if parsed is None:
                    continue
                if active_only and parsed.status != "L":
                    continue
                funds.append(parsed)
                if limit is not None and len(funds) >= limit:
                    break
            if limit is not None and len(funds) >= limit:
                break

        holding_codes = self._load_current_holding_codes()
        tags = [tag for fund in funds for tag in self._classify_by_name(fund, holding_codes)]

        conn = duckdb.connect(str(self.db_path))
        try:
            conn.execute("BEGIN")
            self._upsert_funds(conn, funds)
            self._replace_theme_tags(conn, [fund.fund_code for fund in funds], tags)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

        return {
            "markets": markets,
            "active_only": active_only,
            "fund_count": len(funds),
            "tag_count": len(tags),
            "holding_overlap_count": sum(1 for fund in funds if fund.fund_code in holding_codes),
            "theme_summary": self._theme_summary(tags),
            "classification_note": "Current classifier is NAME_ONLY. It is discovery evidence, not a final verified theme.",
        }

    def _load_current_holding_codes(self) -> set[str]:
        conn = duckdb.connect(str(self.db_path), read_only=True)
        try:
            rows = conn.execute(
                """
                SELECT fund_code
                FROM spark_position_weekly_snapshot
                WHERE shares > 0
                  AND snapshot_date = (
                      SELECT max(snapshot_date)
                      FROM spark_position_weekly_snapshot
                  )
                """
            ).fetchall()
        finally:
            conn.close()
        return {str(row[0]) for row in rows}

    def _parse_fund_row(self, row: dict[str, Any], market: str) -> MarketFundRow | None:
        code = str(row.get("ts_code") or "").strip()
        name = str(row.get("name") or "").strip()
        if not code or not name:
            return None
        return MarketFundRow(
            fund_code=code,
            fund_name=name,
            market=str(row.get("market") or market),
            management=self._optional_text(row.get("management")),
            fund_type=self._optional_text(row.get("fund_type")),
            found_date=self._optional_text(row.get("found_date")),
            status=self._optional_text(row.get("status")),
            raw=row,
        )

    def _classify_by_name(self, fund: MarketFundRow, holding_codes: set[str]) -> list[dict[str, Any]]:
        tags: list[dict[str, Any]] = []
        name_upper = fund.fund_name.upper()
        for theme, terms in THEME_RULES.items():
            matched = [term for term in terms if term.upper() in name_upper]
            if not matched:
                continue
            tags.append(
                {
                    "fund_code": fund.fund_code,
                    "theme": theme,
                    "classification_confidence": "NAME_ONLY",
                    "match_method": "fund_name_keyword",
                    "matched_terms": matched,
                    "evidence": {
                        "fund_name": fund.fund_name,
                        "fund_type": fund.fund_type,
                        "management": fund.management,
                    },
                    "is_current_holding": fund.fund_code in holding_codes,
                }
            )
        return tags

    def _upsert_funds(self, conn: duckdb.DuckDBPyConnection, funds: list[MarketFundRow]) -> None:
        if not funds:
            return
        rows = [
            [
                fund.fund_code,
                fund.fund_name,
                fund.market,
                fund.management,
                fund.fund_type,
                self._parse_date(fund.found_date),
                fund.status,
                "tushare_fund_basic",
                json.dumps(fund.raw, ensure_ascii=False),
            ]
            for fund in funds
        ]
        conn.executemany(
            """
            INSERT INTO spark_market_fund_universe (
                fund_code, fund_name, market, management, fund_type, found_date,
                status, source, raw_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, now())
            ON CONFLICT (fund_code) DO UPDATE SET
                fund_name = EXCLUDED.fund_name,
                market = EXCLUDED.market,
                management = EXCLUDED.management,
                fund_type = EXCLUDED.fund_type,
                found_date = EXCLUDED.found_date,
                status = EXCLUDED.status,
                source = EXCLUDED.source,
                raw_json = EXCLUDED.raw_json,
                updated_at = now()
            """,
            rows,
        )

    def _replace_theme_tags(
        self,
        conn: duckdb.DuckDBPyConnection,
        fund_codes: list[str],
        tags: list[dict[str, Any]],
    ) -> None:
        if fund_codes:
            conn.executemany("DELETE FROM spark_market_theme_tag WHERE fund_code = ?", [[code] for code in fund_codes])
        if not tags:
            return
        rows = [
            [
                tag["fund_code"],
                tag["theme"],
                tag["classification_confidence"],
                tag["match_method"],
                json.dumps(tag["matched_terms"], ensure_ascii=False),
                json.dumps(tag["evidence"], ensure_ascii=False),
                bool(tag["is_current_holding"]),
            ]
            for tag in tags
        ]
        conn.executemany(
            """
            INSERT INTO spark_market_theme_tag (
                fund_code, theme, classification_confidence, match_method,
                matched_terms_json, evidence_json, is_current_holding, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, now())
            ON CONFLICT (fund_code, theme, match_method) DO UPDATE SET
                classification_confidence = EXCLUDED.classification_confidence,
                matched_terms_json = EXCLUDED.matched_terms_json,
                evidence_json = EXCLUDED.evidence_json,
                is_current_holding = EXCLUDED.is_current_holding,
                updated_at = now()
            """,
            rows,
        )

    @staticmethod
    def _theme_summary(tags: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
        summary: dict[str, dict[str, int]] = {}
        for tag in tags:
            theme = str(tag["theme"])
            item = summary.setdefault(theme, {"total": 0, "current_holding": 0, "non_holding": 0})
            item["total"] += 1
            if tag["is_current_holding"]:
                item["current_holding"] += 1
            else:
                item["non_holding"] += 1
        return dict(sorted(summary.items(), key=lambda pair: (-pair[1]["non_holding"], pair[0])))

    @staticmethod
    def _parse_date(value: str | None):
        if not value:
            return None
        for fmt in ("%Y%m%d", "%Y-%m-%d"):
            try:
                return datetime.strptime(value[:10], fmt).date()
            except ValueError:
                continue
        return None

    @staticmethod
    def _optional_text(value: object) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh Spark ETF all-market fund universe")
    parser.add_argument("--db", type=str, default=None, help="DuckDB path")
    parser.add_argument("--market", action="append", default=None, help="Tushare fund market, e.g. O or E")
    parser.add_argument("--include-inactive", action="store_true", help="Keep non-listed/inactive funds")
    parser.add_argument("--limit", type=int, default=None, help="Limit rows for smoke tests")
    args = parser.parse_args()

    markets = args.market or ["O"]
    service = MarketUniverseService(db_path=args.db)
    result = service.refresh(markets=markets, active_only=(not args.include_inactive), limit=args.limit)
    print(json.dumps(result, ensure_ascii=False, default=str, indent=2))
    return 0 if result["fund_count"] > 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
