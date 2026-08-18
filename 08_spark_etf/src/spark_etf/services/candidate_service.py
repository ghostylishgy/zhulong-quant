from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import duckdb
import yaml

from spark_etf.data.tushare_client import DATA_OK, NO_DATA, STALE_DATA, VALUATION_PROXY_MISSING, TushareClient
from spark_etf.db.db_init import DEFAULT_DB_PATH, init_db
from spark_etf.services.nav_service import NavService

DEFAULT_CANDIDATE_CONFIG_PATH = Path(__file__).resolve().parents[3] / "config" / "candidates.yaml"


@dataclass(frozen=True, slots=True)
class CandidateItem:
    fund_code: str
    fund_name: str
    theme: str
    asset_bucket: str
    priority: int
    tags: tuple[str, ...]
    source: str
    thesis: str
    valuation_proxy: dict[str, Any] | None
    raw: dict[str, Any]


@dataclass(frozen=True, slots=True)
class CandidatePolicies:
    min_history_points: int = 30
    low_drawdown_60d: float = -0.18
    deep_drawdown_60d: float = -0.28
    trend_turn_return_20d: float = 0.04
    rebound_return_20d: float = 0.03
    low_valuation_percentile: float = 0.35


class CandidateService:
    def __init__(
        self,
        db_path: Path | str | None = None,
        config_path: Path | str | None = None,
        nav_service: NavService | None = None,
        tushare_client: TushareClient | None = None,
    ) -> None:
        self.db_path = init_db(db_path or DEFAULT_DB_PATH)
        self.config_path = Path(config_path).expanduser().resolve() if config_path else DEFAULT_CANDIDATE_CONFIG_PATH.resolve()
        self.tushare_client = tushare_client or TushareClient()
        self.nav_service = nav_service or NavService(db_path=self.db_path, tushare_client=self.tushare_client)
        self.candidates, self.policies = self._load_config()

    def refresh_candidates(
        self,
        signal_date: date | None = None,
        refresh_nav: bool = True,
        lookback_days: int = 180,
    ) -> dict[str, Any]:
        signal_date = signal_date or date.today()
        holding_codes = self._load_current_holding_codes()
        active_candidates = [item for item in self.candidates if item.fund_code not in holding_codes]
        excluded_holdings = [
            {
                "fund_code": item.fund_code,
                "fund_name": item.fund_name,
                "theme": item.theme,
                "reason": "CURRENT_HOLDING",
            }
            for item in self.candidates
            if item.fund_code in holding_codes
        ]

        if refresh_nav and active_candidates:
            self.nav_service.refresh_nav_history(
                fund_codes=[item.fund_code for item in active_candidates],
                lookback_days=lookback_days,
                end_date=signal_date,
            )

        signals = [self._build_signal(item, signal_date) for item in active_candidates]
        conn = duckdb.connect(str(self.db_path))
        try:
            conn.execute("BEGIN")
            self._clear_signal_date(conn, signal_date)
            self._deactivate_missing_candidates(conn, [item.fund_code for item in active_candidates])
            for item in active_candidates:
                self._upsert_candidate_config(conn, item)
            for signal in signals:
                self._persist_signal(conn, signal)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

        return {
            "signal_date": signal_date.isoformat(),
            "candidate_count": len(active_candidates),
            "configured_count": len(self.candidates),
            "excluded_holding_count": len(excluded_holdings),
            "excluded_holdings": excluded_holdings,
            "signals": signals,
            "summary": self._summary(signals),
        }

    def _load_config(self) -> tuple[list[CandidateItem], CandidatePolicies]:
        if not self.config_path.exists():
            raise FileNotFoundError(f"Candidate config not found: {self.config_path}")
        with self.config_path.open("r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh) or {}

        policies_raw = raw.get("policies", {}) if isinstance(raw.get("policies", {}), dict) else {}
        policies = CandidatePolicies(
            min_history_points=int(policies_raw.get("min_history_points", 30)),
            low_drawdown_60d=float(policies_raw.get("low_drawdown_60d", -0.18)),
            deep_drawdown_60d=float(policies_raw.get("deep_drawdown_60d", -0.28)),
            trend_turn_return_20d=float(policies_raw.get("trend_turn_return_20d", 0.04)),
            rebound_return_20d=float(policies_raw.get("rebound_return_20d", 0.03)),
            low_valuation_percentile=float(policies_raw.get("low_valuation_percentile", 0.35)),
        )

        candidates_raw = raw.get("candidates", [])
        if not isinstance(candidates_raw, list):
            raise ValueError("Field 'candidates' must be a list")

        candidates: list[CandidateItem] = []
        seen: set[str] = set()
        for row in candidates_raw:
            if not isinstance(row, dict):
                raise ValueError("Each candidate must be a mapping")
            code = str(row.get("fund_code", "")).strip()
            if not code:
                raise ValueError("Candidate missing fund_code")
            if code in seen:
                raise ValueError(f"Duplicate candidate fund_code: {code}")
            seen.add(code)
            tags_raw = row.get("tags", [])
            if not isinstance(tags_raw, list):
                raise ValueError(f"Candidate tags must be list: {code}")
            proxy = row.get("valuation_proxy") if isinstance(row.get("valuation_proxy"), dict) else None
            candidates.append(
                CandidateItem(
                    fund_code=code,
                    fund_name=str(row.get("fund_name") or code),
                    theme=str(row.get("theme") or "未分类"),
                    asset_bucket=str(row.get("asset_bucket") or "candidate_theme"),
                    priority=int(row.get("priority", 3)),
                    tags=tuple(str(tag) for tag in tags_raw),
                    source=str(row.get("source") or "manual"),
                    thesis=str(row.get("thesis") or ""),
                    valuation_proxy=proxy,
                    raw=row,
                )
            )
        return candidates, policies

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

    def _build_signal(self, item: CandidateItem, signal_date: date) -> dict[str, Any]:
        nav_rows = self._load_nav_rows(item.fund_code)
        nav_metrics = self._nav_metrics(nav_rows, signal_date)
        valuation = self._valuation_payload(item)
        risk_flags = self._risk_flags(nav_metrics, valuation)
        action, status, rationale = self._decide_candidate_action(nav_metrics, valuation, risk_flags)

        return {
            "signal_id": str(uuid4()),
            "signal_date": signal_date,
            "fund_code": item.fund_code,
            "fund_name": item.fund_name,
            "theme": item.theme,
            "asset_bucket": item.asset_bucket,
            "candidate_action": action,
            "candidate_status": status,
            "action_level": "WATCH_ONLY",
            "latest_nav": nav_metrics.get("latest_nav"),
            "nav_date": nav_metrics.get("nav_date"),
            "drawdown_60d": nav_metrics.get("drawdown_60d"),
            "return_20d": nav_metrics.get("return_20d"),
            "return_60d": nav_metrics.get("return_60d"),
            "ma20": nav_metrics.get("ma20"),
            "ma60": nav_metrics.get("ma60"),
            "valuation_percentile": valuation.get("pe_percentile"),
            "data_quality": nav_metrics.get("data_quality", NO_DATA),
            "valuation_quality": valuation.get("data_quality", VALUATION_PROXY_MISSING),
            "risk_flags": sorted(set(risk_flags)),
            "rationale": rationale,
        }

    def _load_nav_rows(self, fund_code: str) -> list[tuple[date, float]]:
        conn = duckdb.connect(str(self.db_path), read_only=True)
        try:
            rows = conn.execute(
                """
                SELECT nav_date, unit_nav
                FROM spark_nav_daily
                WHERE fund_code = ? AND source = 'tushare_fund_nav' AND unit_nav IS NOT NULL
                ORDER BY nav_date DESC
                LIMIT 90
                """,
                [fund_code],
            ).fetchall()
        finally:
            conn.close()
        return [(row[0], float(row[1])) for row in reversed(rows)]

    def _nav_metrics(self, rows: list[tuple[date, float]], signal_date: date) -> dict[str, Any]:
        if not rows:
            return {"history_points": 0, "data_quality": NO_DATA}

        dates = [row[0] for row in rows]
        values = [row[1] for row in rows]
        latest_nav = values[-1]
        nav_date = dates[-1]
        data_quality = DATA_OK if (signal_date - nav_date).days <= 10 else STALE_DATA
        window60 = values[-60:] if len(values) >= 60 else values
        peak60 = max(window60) if window60 else latest_nav

        return {
            "history_points": len(values),
            "data_quality": data_quality,
            "latest_nav": latest_nav,
            "nav_date": nav_date,
            "drawdown_60d": (latest_nav / peak60 - 1.0) if peak60 > 0 else None,
            "return_20d": self._window_return(values, 20),
            "return_60d": self._window_return(values, 60),
            "ma20": self._moving_average(values, 20),
            "ma60": self._moving_average(values, 60),
        }

    def _valuation_payload(self, item: CandidateItem) -> dict[str, Any]:
        proxy = item.valuation_proxy
        if not proxy or not proxy.get("ts_code"):
            return {
                "pe_percentile": None,
                "data_quality": VALUATION_PROXY_MISSING,
                "note": "candidate valuation proxy missing",
            }
        if self.tushare_client.use_mock:
            payload = self.tushare_client.get_valuation_and_price(item.fund_code, proxy)
            return {"pe_percentile": payload.get("pe_percentile"), "data_quality": payload.get("data_quality")}
        return self.tushare_client.get_valuation_proxy(proxy)

    def _risk_flags(self, nav_metrics: dict[str, Any], valuation: dict[str, Any]) -> list[str]:
        flags: list[str] = []
        if int(nav_metrics.get("history_points", 0)) < self.policies.min_history_points:
            flags.append("NAV_HISTORY_INSUFFICIENT")
        data_quality = str(nav_metrics.get("data_quality", NO_DATA))
        if data_quality != DATA_OK:
            flags.append(data_quality)
        valuation_quality = str(valuation.get("data_quality", VALUATION_PROXY_MISSING))
        if valuation_quality != DATA_OK:
            flags.append(valuation_quality)
        if valuation.get("note"):
            flags.append("VALUATION_NOTE")
        return flags

    def _decide_candidate_action(
        self,
        nav_metrics: dict[str, Any],
        valuation: dict[str, Any],
        risk_flags: list[str],
    ) -> tuple[str, str, str]:
        history_points = int(nav_metrics.get("history_points", 0))
        data_quality = str(nav_metrics.get("data_quality", NO_DATA))
        if history_points < self.policies.min_history_points or data_quality in {NO_DATA, STALE_DATA}:
            return "DATA_INSUFFICIENT", "DATA_INSUFFICIENT", "候选基金净值历史不足或数据过期，先只建档观察。"

        drawdown = nav_metrics.get("drawdown_60d")
        ret20 = nav_metrics.get("return_20d")
        ma20 = nav_metrics.get("ma20")
        ma60 = nav_metrics.get("ma60")
        pe = valuation.get("pe_percentile")

        if pe is not None and float(pe) <= self.policies.low_valuation_percentile:
            return "LOW_VALUATION_WATCH", "LOW_ZONE", "估值分位进入低位区，候选池提示观察，不给买入金额。"
        if (
            drawdown is not None
            and ret20 is not None
            and float(drawdown) <= self.policies.deep_drawdown_60d
            and float(ret20) >= self.policies.rebound_return_20d
        ):
            return "LOW_ZONE_REBOUND_WATCH", "TREND_CONFIRM", "60日回撤较深且近20日开始修复，进入低位反弹观察。"
        if drawdown is not None and float(drawdown) <= self.policies.low_drawdown_60d:
            return "LOW_ZONE_WATCH", "LOW_ZONE", "60日回撤进入低位观察区，等待趋势确认。"
        if (
            ret20 is not None
            and ma20 is not None
            and ma60 is not None
            and float(ret20) >= self.policies.trend_turn_return_20d
            and float(ma20) >= float(ma60)
        ):
            return "TREND_TURN_WATCH", "TREND_CONFIRM", "近20日收益和均线结构转强，进入趋势确认观察。"

        if "VALUATION_PROXY_MISSING" in risk_flags:
            return "OBSERVE", "WATCHING", "估值代理缺失，仅按净值趋势和回撤观察。"
        return "OBSERVE", "WATCHING", "候选基金未触发低位或趋势转强条件，继续观察。"

    def _upsert_candidate_config(self, conn: duckdb.DuckDBPyConnection, item: CandidateItem) -> None:
        tags_json = json.dumps(list(item.tags), ensure_ascii=False)
        proxy_json = json.dumps(item.valuation_proxy or {}, ensure_ascii=False)
        config_hash = self._config_hash(item.raw)
        conn.execute(
            """
            INSERT INTO spark_fund_universe (
                fund_code, fund_name, asset_bucket, theme, source, tags_json,
                valuation_proxy_json, note, is_active, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, TRUE, now())
            ON CONFLICT (fund_code) DO UPDATE SET
                fund_name = EXCLUDED.fund_name,
                asset_bucket = EXCLUDED.asset_bucket,
                theme = EXCLUDED.theme,
                source = EXCLUDED.source,
                tags_json = EXCLUDED.tags_json,
                valuation_proxy_json = EXCLUDED.valuation_proxy_json,
                note = EXCLUDED.note,
                is_active = TRUE,
                updated_at = now()
            """,
            [item.fund_code, item.fund_name, item.asset_bucket, item.theme, item.source, tags_json, proxy_json, item.thesis],
        )
        conn.execute(
            """
            INSERT INTO spark_fund_candidates (
                fund_code, candidate_id, fund_name, theme, asset_bucket,
                candidate_status, priority, thesis, source, config_hash, updated_at
            ) VALUES (?, ?, ?, ?, ?, 'WATCHING', ?, ?, ?, ?, now())
            ON CONFLICT (fund_code) DO UPDATE SET
                fund_name = EXCLUDED.fund_name,
                theme = EXCLUDED.theme,
                asset_bucket = EXCLUDED.asset_bucket,
                priority = EXCLUDED.priority,
                thesis = EXCLUDED.thesis,
                source = EXCLUDED.source,
                config_hash = EXCLUDED.config_hash,
                updated_at = now()
            """,
            [item.fund_code, str(uuid4()), item.fund_name, item.theme, item.asset_bucket, item.priority, item.thesis, item.source, config_hash],
        )

    def _persist_signal(self, conn: duckdb.DuckDBPyConnection, signal: dict[str, Any]) -> None:
        conn.execute(
            """
            INSERT INTO spark_candidate_signal (
                signal_id, signal_date, fund_code, fund_name, theme, asset_bucket,
                candidate_action, candidate_status, action_level, latest_nav, nav_date,
                drawdown_60d, return_20d, return_60d, ma20, ma60, valuation_percentile,
                data_quality, valuation_quality, risk_flags_json, rationale
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                signal["signal_id"],
                signal["signal_date"],
                signal["fund_code"],
                signal["fund_name"],
                signal["theme"],
                signal["asset_bucket"],
                signal["candidate_action"],
                signal["candidate_status"],
                signal["action_level"],
                signal["latest_nav"],
                signal["nav_date"],
                signal["drawdown_60d"],
                signal["return_20d"],
                signal["return_60d"],
                signal["ma20"],
                signal["ma60"],
                signal["valuation_percentile"],
                signal["data_quality"],
                signal["valuation_quality"],
                json.dumps(signal["risk_flags"], ensure_ascii=False),
                signal["rationale"],
            ],
        )
        conn.execute(
            """
            UPDATE spark_fund_candidates
            SET candidate_status = ?, updated_at = now()
            WHERE fund_code = ?
            """,
            [signal["candidate_status"], signal["fund_code"]],
        )

    def _clear_signal_date(self, conn: duckdb.DuckDBPyConnection, signal_date: date) -> None:
        conn.execute(
            """
            DELETE FROM spark_candidate_signal
            WHERE signal_date = ?
            """,
            [signal_date],
        )

    def _deactivate_missing_candidates(self, conn: duckdb.DuckDBPyConnection, active_codes: list[str]) -> None:
        if not active_codes:
            conn.execute("UPDATE spark_fund_candidates SET candidate_status = 'PAUSED', updated_at = now();")
            conn.execute("UPDATE spark_fund_universe SET is_active = FALSE, updated_at = now();")
            return

        placeholders = ", ".join("?" for _ in active_codes)
        conn.execute(
            f"""
            UPDATE spark_fund_candidates
            SET candidate_status = 'PAUSED', updated_at = now()
            WHERE fund_code NOT IN ({placeholders})
            """,
            active_codes,
        )
        conn.execute(
            f"""
            UPDATE spark_fund_universe
            SET is_active = FALSE, updated_at = now()
            WHERE fund_code NOT IN ({placeholders})
            """,
            active_codes,
        )

    @staticmethod
    def _summary(signals: list[dict[str, Any]]) -> dict[str, int]:
        result: dict[str, int] = {}
        for signal in signals:
            action = str(signal.get("candidate_action", "UNKNOWN"))
            result[action] = result.get(action, 0) + 1
        return result

    @staticmethod
    def _window_return(values: list[float], window: int) -> float | None:
        if len(values) <= window:
            return None
        base = values[-window - 1]
        if base <= 0:
            return None
        return values[-1] / base - 1.0

    @staticmethod
    def _moving_average(values: list[float], window: int) -> float | None:
        if len(values) < window:
            return None
        return sum(values[-window:]) / float(window)

    @staticmethod
    def _config_hash(raw: dict[str, Any]) -> str:
        text = json.dumps(raw, ensure_ascii=False, sort_keys=True)
        return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh Spark ETF candidate pool signals")
    parser.add_argument("--date", type=_parse_date, default=None, help="Signal date, YYYY-MM-DD; default today")
    parser.add_argument("--db", type=str, default=None, help="DuckDB path")
    parser.add_argument("--config", type=str, default=None, help="Candidate config path")
    parser.add_argument("--lookback-days", type=int, default=180, help="NAV refresh lookback window")
    parser.add_argument("--no-refresh-nav", action="store_true", help="Use cached NAV history only")
    args = parser.parse_args()

    service = CandidateService(db_path=args.db, config_path=args.config)
    result = service.refresh_candidates(
        signal_date=args.date,
        refresh_nav=(not args.no_refresh_nav),
        lookback_days=args.lookback_days,
    )
    print(json.dumps(result, ensure_ascii=False, default=str, indent=2))
    return 0 if result["candidate_count"] > 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
