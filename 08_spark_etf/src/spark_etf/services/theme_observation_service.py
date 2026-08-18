from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb

from spark_etf.db.db_init import DEFAULT_DB_PATH, init_db


@dataclass(frozen=True, slots=True)
class ThemeObservation:
    theme: str
    bucket_status: str
    total_fund_count: int
    non_holding_count: int
    current_holding_count: int
    name_only_count: int
    index_verified_count: int
    holding_verified_count: int
    unknown_count: int
    sample_funds: list[dict[str, Any]]
    evidence: dict[str, Any]


class ThemeObservationService:
    def __init__(self, db_path: Path | str | None = None) -> None:
        self.db_path = init_db(db_path or DEFAULT_DB_PATH)

    def refresh(
        self,
        observation_date: date | None = None,
        min_non_holding: int = 5,
        sample_size: int = 8,
    ) -> dict[str, Any]:
        observation_date = observation_date or date.today()
        rows = self._load_theme_rows()
        observations = self._build_observations(rows, min_non_holding=min_non_holding, sample_size=sample_size)

        conn = duckdb.connect(str(self.db_path))
        try:
            conn.execute("BEGIN")
            for item in observations:
                self._persist_observation(conn, observation_date, item)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

        return {
            "observation_date": observation_date.isoformat(),
            "theme_count": len(observations),
            "min_non_holding": min_non_holding,
            "status_summary": self._status_summary(observations),
            "themes": [self._to_dict(item) for item in observations],
            "note": "Observation bucket stores theme evidence only. It does not promote funds into candidates.",
        }

    def _load_theme_rows(self) -> list[dict[str, Any]]:
        conn = duckdb.connect(str(self.db_path), read_only=True)
        try:
            rows = conn.execute(
                """
                SELECT
                    t.theme,
                    t.classification_confidence,
                    t.match_method,
                    t.is_current_holding,
                    t.matched_terms_json,
                    t.evidence_json,
                    u.fund_code,
                    u.fund_name,
                    u.fund_type,
                    u.management,
                    u.found_date
                FROM spark_market_theme_tag t
                JOIN spark_market_fund_universe u
                  ON t.fund_code = u.fund_code
                WHERE u.status = 'L'
                ORDER BY t.theme, t.is_current_holding, u.found_date NULLS LAST, u.fund_code
                """
            ).fetchall()
        finally:
            conn.close()

        result: list[dict[str, Any]] = []
        for row in rows:
            result.append(
                {
                    "theme": str(row[0]),
                    "classification_confidence": str(row[1]),
                    "match_method": str(row[2]),
                    "is_current_holding": bool(row[3]),
                    "matched_terms": self._loads_json(row[4], []),
                    "evidence": self._loads_json(row[5], {}),
                    "fund_code": str(row[6]),
                    "fund_name": str(row[7]),
                    "fund_type": row[8],
                    "management": row[9],
                    "found_date": row[10],
                }
            )
        return result

    def _build_observations(
        self,
        rows: list[dict[str, Any]],
        min_non_holding: int,
        sample_size: int,
    ) -> list[ThemeObservation]:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(str(row["theme"]), []).append(row)

        observations: list[ThemeObservation] = []
        for theme, items in grouped.items():
            confidence_counts = {
                "NAME_ONLY": 0,
                "INDEX_VERIFIED": 0,
                "HOLDING_VERIFIED": 0,
                "UNKNOWN": 0,
            }
            for item in items:
                confidence = str(item["classification_confidence"])
                confidence_counts[confidence] = confidence_counts.get(confidence, 0) + 1

            non_holding_items = [item for item in items if not bool(item["is_current_holding"])]
            holding_items = [item for item in items if bool(item["is_current_holding"])]
            sample_funds = [
                {
                    "fund_code": item["fund_code"],
                    "fund_name": item["fund_name"],
                    "fund_type": item["fund_type"],
                    "management": item["management"],
                    "confidence": item["classification_confidence"],
                    "matched_terms": item["matched_terms"],
                }
                for item in non_holding_items[:sample_size]
            ]
            bucket_status = "DISCOVERED" if len(non_holding_items) >= min_non_holding else "REJECTED"
            evidence = {
                "classifier": "fund_name_keyword",
                "confidence_policy": "NAME_ONLY is discovery evidence, not verified theme classification.",
                "sample_size": len(sample_funds),
                "current_holding_samples": [
                    {
                        "fund_code": item["fund_code"],
                        "fund_name": item["fund_name"],
                        "matched_terms": item["matched_terms"],
                    }
                    for item in holding_items[: min(sample_size, 5)]
                ],
            }
            observations.append(
                ThemeObservation(
                    theme=theme,
                    bucket_status=bucket_status,
                    total_fund_count=len(items),
                    non_holding_count=len(non_holding_items),
                    current_holding_count=len(holding_items),
                    name_only_count=int(confidence_counts.get("NAME_ONLY", 0)),
                    index_verified_count=int(confidence_counts.get("INDEX_VERIFIED", 0)),
                    holding_verified_count=int(confidence_counts.get("HOLDING_VERIFIED", 0)),
                    unknown_count=int(confidence_counts.get("UNKNOWN", 0)),
                    sample_funds=sample_funds,
                    evidence=evidence,
                )
            )

        return sorted(
            observations,
            key=lambda item: (
                0 if item.bucket_status == "DISCOVERED" else 1,
                -item.non_holding_count,
                item.theme,
            ),
        )

    def _persist_observation(
        self,
        conn: duckdb.DuckDBPyConnection,
        observation_date: date,
        item: ThemeObservation,
    ) -> None:
        sample_json = json.dumps(item.sample_funds, ensure_ascii=False)
        evidence_json = json.dumps(item.evidence, ensure_ascii=False)
        conn.execute(
            """
            INSERT INTO spark_theme_observation_daily (
                observation_date, theme, bucket_status, total_fund_count,
                non_holding_count, current_holding_count, name_only_count,
                index_verified_count, holding_verified_count, unknown_count,
                sample_funds_json, evidence_json, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'market_theme_tag')
            ON CONFLICT (observation_date, theme) DO UPDATE SET
                bucket_status = EXCLUDED.bucket_status,
                total_fund_count = EXCLUDED.total_fund_count,
                non_holding_count = EXCLUDED.non_holding_count,
                current_holding_count = EXCLUDED.current_holding_count,
                name_only_count = EXCLUDED.name_only_count,
                index_verified_count = EXCLUDED.index_verified_count,
                holding_verified_count = EXCLUDED.holding_verified_count,
                unknown_count = EXCLUDED.unknown_count,
                sample_funds_json = EXCLUDED.sample_funds_json,
                evidence_json = EXCLUDED.evidence_json,
                source = EXCLUDED.source
            """,
            [
                observation_date,
                item.theme,
                item.bucket_status,
                item.total_fund_count,
                item.non_holding_count,
                item.current_holding_count,
                item.name_only_count,
                item.index_verified_count,
                item.holding_verified_count,
                item.unknown_count,
                sample_json,
                evidence_json,
            ],
        )
        conn.execute(
            """
            INSERT INTO spark_theme_observation_bucket (
                theme, bucket_status, first_seen_date, last_seen_date, observation_count,
                total_fund_count, non_holding_count, current_holding_count,
                name_only_count, index_verified_count, holding_verified_count, unknown_count,
                evidence_json, note, updated_at
            ) VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())
            ON CONFLICT (theme) DO UPDATE SET
                bucket_status = CASE
                    WHEN spark_theme_observation_bucket.bucket_status IN ('PAUSED', 'REJECTED')
                     AND EXCLUDED.bucket_status = 'DISCOVERED'
                    THEN spark_theme_observation_bucket.bucket_status
                    ELSE EXCLUDED.bucket_status
                END,
                last_seen_date = EXCLUDED.last_seen_date,
                observation_count = spark_theme_observation_bucket.observation_count + 1,
                total_fund_count = EXCLUDED.total_fund_count,
                non_holding_count = EXCLUDED.non_holding_count,
                current_holding_count = EXCLUDED.current_holding_count,
                name_only_count = EXCLUDED.name_only_count,
                index_verified_count = EXCLUDED.index_verified_count,
                holding_verified_count = EXCLUDED.holding_verified_count,
                unknown_count = EXCLUDED.unknown_count,
                evidence_json = EXCLUDED.evidence_json,
                note = EXCLUDED.note,
                updated_at = now()
            """,
            [
                item.theme,
                item.bucket_status,
                observation_date,
                observation_date,
                item.total_fund_count,
                item.non_holding_count,
                item.current_holding_count,
                item.name_only_count,
                item.index_verified_count,
                item.holding_verified_count,
                item.unknown_count,
                evidence_json,
                "auto observation from market theme tags; not a buy signal",
            ],
        )

    @staticmethod
    def _status_summary(observations: list[ThemeObservation]) -> dict[str, int]:
        summary: dict[str, int] = {}
        for item in observations:
            summary[item.bucket_status] = summary.get(item.bucket_status, 0) + 1
        return summary

    @staticmethod
    def _to_dict(item: ThemeObservation) -> dict[str, Any]:
        return {
            "theme": item.theme,
            "bucket_status": item.bucket_status,
            "total_fund_count": item.total_fund_count,
            "non_holding_count": item.non_holding_count,
            "current_holding_count": item.current_holding_count,
            "name_only_count": item.name_only_count,
            "index_verified_count": item.index_verified_count,
            "holding_verified_count": item.holding_verified_count,
            "unknown_count": item.unknown_count,
            "sample_funds": item.sample_funds,
        }

    @staticmethod
    def _loads_json(value: object, default: Any) -> Any:
        if value is None:
            return default
        if isinstance(value, (list, dict)):
            return value
        try:
            return json.loads(str(value))
        except Exception:
            return default


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh Spark ETF theme observation bucket")
    parser.add_argument("--db", type=str, default=None, help="DuckDB path")
    parser.add_argument("--date", type=_parse_date, default=None, help="Observation date, YYYY-MM-DD")
    parser.add_argument("--min-non-holding", type=int, default=5, help="Minimum non-held funds for DISCOVERED")
    parser.add_argument("--sample-size", type=int, default=8, help="Sample non-held funds kept as evidence")
    args = parser.parse_args()

    service = ThemeObservationService(db_path=args.db)
    result = service.refresh(
        observation_date=args.date,
        min_non_holding=args.min_non_holding,
        sample_size=args.sample_size,
    )
    print(json.dumps(result, ensure_ascii=False, default=str, indent=2))
    return 0 if result["theme_count"] > 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
