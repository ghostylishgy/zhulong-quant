from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import duckdb

from spark_etf.config.loader import PortfolioItem, load_spark_config
from spark_etf.data.tushare_client import DATA_OK, NO_DATA, STALE_DATA, VALUATION_PROXY_MISSING, TushareClient
from spark_etf.db.db_init import DEFAULT_DB_PATH, init_db
from spark_etf.services.nav_service import NavService


@dataclass(slots=True)
class HoldingInput:
    snapshot_date: date
    fund_code: str
    fund_name: str
    shares: float
    unit_cost: float


class HoldingSignalService:
    def __init__(
        self,
        db_path: Path | str | None = None,
        config_path: Path | str | None = None,
        nav_service: NavService | None = None,
        tushare_client: TushareClient | None = None,
    ) -> None:
        self.db_path = init_db(db_path or DEFAULT_DB_PATH)
        self.config_path = Path(config_path).expanduser().resolve() if config_path else None
        self.config = load_spark_config(self.config_path)
        self.portfolio_by_code = {item.etf_code: item for item in self.config.portfolios}
        self.tushare_client = tushare_client or TushareClient()
        self.nav_service = nav_service or NavService(db_path=self.db_path, tushare_client=self.tushare_client)

    def generate_for_snapshot(self, snapshot_date: date | None = None, refresh_nav: bool = True) -> list[dict[str, Any]]:
        holdings = self._load_holdings(snapshot_date)
        signals = [
            self._build_signal(holding, self.portfolio_by_code.get(holding.fund_code), refresh_nav=refresh_nav)
            for holding in holdings
        ]
        conn = duckdb.connect(str(self.db_path))
        try:
            conn.execute("BEGIN")
            for signal in signals:
                self._persist_signal(conn, signal)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()
        return signals

    def _load_holdings(self, snapshot_date: date | None) -> list[HoldingInput]:
        conn = duckdb.connect(str(self.db_path), read_only=True)
        try:
            if snapshot_date is None:
                row = conn.execute("SELECT max(snapshot_date) FROM spark_position_weekly_snapshot").fetchone()
                if row is None or row[0] is None:
                    return []
                snapshot_date = row[0]
            rows = conn.execute(
                """
                SELECT snapshot_date, fund_code, fund_name, shares, unit_cost
                FROM spark_position_weekly_snapshot
                WHERE snapshot_date = ?
                ORDER BY fund_code
                """,
                [snapshot_date],
            ).fetchall()
        finally:
            conn.close()
        return [
            HoldingInput(
                snapshot_date=row[0],
                fund_code=str(row[1]),
                fund_name=str(row[2]),
                shares=float(row[3]),
                unit_cost=float(row[4]),
            )
            for row in rows
        ]

    def _build_signal(self, holding: HoldingInput, portfolio: PortfolioItem | None, refresh_nav: bool) -> dict[str, Any]:
        nav = self.nav_service.get_latest_nav(holding.fund_code, refresh=refresh_nav)
        valuation = self._valuation_payload(portfolio)
        nav_quality = str(nav.get("data_quality", NO_DATA))
        valuation_quality = str(valuation.get("data_quality", VALUATION_PROXY_MISSING))

        latest_nav = nav.get("unit_nav")
        nav_date = nav.get("nav_date")
        market_value = None
        cost_value = holding.shares * holding.unit_cost
        return_ratio = None
        holding_mode = portfolio.holding_mode if portfolio else "dca_active"
        nav_watch = self._nav_watch_metrics(holding.fund_code, latest_nav)
        if latest_nav is not None:
            latest_nav = float(latest_nav)
            market_value = holding.shares * latest_nav
            if holding.unit_cost > 0:
                return_ratio = latest_nav / holding.unit_cost - 1.0

        risk_flags = self._risk_flags(nav_quality, valuation_quality, valuation)
        if holding_mode == "sell_only":
            risk_flags.append("SELL_ONLY")
        if holding.unit_cost <= 0:
            risk_flags.append("NEGATIVE_COST")
        if holding_mode == "sell_only" and nav_watch["history_points"] < 4:
            risk_flags.append("INSUFFICIENT_NAV_HISTORY")
        action, action_level, rationale = self._decide_action(
            return_ratio=return_ratio,
            unit_cost=holding.unit_cost,
            valuation_percentile=valuation.get("pe_percentile"),
            nav_quality=nav_quality,
            valuation_quality=valuation_quality,
            portfolio=portfolio,
            holding_mode=holding_mode,
            nav_watch=nav_watch,
            risk_flags=risk_flags,
        )

        return {
            "signal_id": str(uuid4()),
            "snapshot_date": holding.snapshot_date,
            "fund_code": holding.fund_code,
            "fund_name": holding.fund_name,
            "shares": holding.shares,
            "unit_cost": holding.unit_cost,
            "latest_nav": latest_nav,
            "nav_date": nav_date,
            "market_value": market_value,
            "cost_value": cost_value,
            "return_ratio": return_ratio,
            "holding_mode": holding_mode,
            "action": action,
            "action_level": action_level,
            "data_quality": nav_quality,
            "valuation_quality": valuation_quality,
            "valuation_percentile": valuation.get("pe_percentile"),
            "asset_bucket": portfolio.asset_bucket if portfolio else "unknown",
            "risk_flags": sorted(set(risk_flags)),
            "rationale": rationale,
        }

    def _nav_watch_metrics(self, fund_code: str, latest_nav: float | None) -> dict[str, Any]:
        conn = duckdb.connect(str(self.db_path), read_only=True)
        try:
            rows = conn.execute(
                """
                SELECT nav_date, unit_nav
                FROM spark_nav_daily
                WHERE fund_code = ? AND unit_nav IS NOT NULL
                ORDER BY nav_date DESC
                LIMIT 12
                """,
                [fund_code],
            ).fetchall()
        finally:
            conn.close()

        values = [float(row[1]) for row in reversed(rows) if row[1] is not None]
        active_nav = float(latest_nav) if latest_nav is not None else (values[-1] if values else None)
        peak_nav = max(values) if values else active_nav
        drawdown = None
        if active_nav is not None and peak_nav is not None and peak_nav > 0:
            drawdown = active_nav / peak_nav - 1.0
        ma4 = sum(values[-4:]) / 4.0 if len(values) >= 4 else None
        ma8 = sum(values[-8:]) / 8.0 if len(values) >= 8 else None
        return {
            "history_points": len(values),
            "drawdown_from_cached_peak": drawdown,
            "ma4": ma4,
            "ma8": ma8,
            "ma4_below_ma8": bool(ma4 is not None and ma8 is not None and ma4 < ma8),
        }

    def _valuation_payload(self, portfolio: PortfolioItem | None) -> dict[str, Any]:
        if portfolio is None:
            return {"pe_percentile": None, "data_quality": VALUATION_PROXY_MISSING, "note": "missing portfolio config"}
        if self.tushare_client.use_mock:
            payload = self.tushare_client.get_valuation_and_price(portfolio.etf_code, portfolio.valuation_proxy)
            return {
                "pe_percentile": payload.get("pe_percentile"),
                "data_quality": payload.get("data_quality"),
                "valuation_proxy": payload.get("valuation_proxy"),
                "note": payload.get("note"),
            }
        return self.tushare_client.get_valuation_proxy(portfolio.valuation_proxy)

    @staticmethod
    def _risk_flags(nav_quality: str, valuation_quality: str, valuation: dict[str, Any]) -> list[str]:
        flags: list[str] = []
        if nav_quality != DATA_OK:
            flags.append(nav_quality)
        if valuation_quality != DATA_OK:
            flags.append(valuation_quality)
        if valuation.get("note"):
            flags.append("VALUATION_NOTE")
        return flags

    @staticmethod
    def _decide_action(
        return_ratio: float | None,
        unit_cost: float,
        valuation_percentile: Any,
        nav_quality: str,
        valuation_quality: str,
        portfolio: PortfolioItem | None,
        holding_mode: str,
        nav_watch: dict[str, Any],
        risk_flags: list[str],
    ) -> tuple[str, str, str]:
        if nav_quality in {NO_DATA, STALE_DATA}:
            return "DATA_REVIEW", "ADVISORY_ONLY", "NAV is missing or stale; do not issue strong advice."
        bucket = portfolio.asset_bucket if portfolio else "unknown"
        pe = float(valuation_percentile) if valuation_percentile is not None else None

        if holding_mode == "sell_only" or bucket == "legacy_free_ride" or unit_cost <= 0:
            return HoldingSignalService._decide_free_ride_exit(
                pe=pe,
                valuation_quality=valuation_quality,
                nav_watch=nav_watch,
                risk_flags=risk_flags,
            )

        if return_ratio is None:
            return "HOLD_REVIEW", "ADVISORY_ONLY", "Return cannot be computed from weekly snapshot cost."

        if pe is not None:
            if pe <= 0.35:
                if return_ratio >= 0.30:
                    return "HOLD_LOW_VALUATION", "ADVISORY_ONLY", "Return is high, but valuation is low; avoid mechanical profit taking."
                return "HOLD_OR_DCA_REVIEW", "ADVISORY_ONLY", "Valuation is low enough for hold/add review."
            if pe >= 0.90 and return_ratio >= 0.20:
                return "PARTIAL_SELL_REVIEW", "ADVISORY_ONLY", "Extreme high valuation plus profit cushion; review partial sell."
            if pe >= 0.75 and return_ratio >= 0.20:
                return "PROFIT_PROTECTION_WATCH", "ADVISORY_ONLY", "High valuation and profit cushion; watch for trend weakening."

        if valuation_quality != DATA_OK:
            risk_flags.append("NO_STRONG_LOW_VALUATION_CALL")

        if return_ratio >= 0.50:
            return "LAYERED_TAKE_PROFIT_REVIEW", "ADVISORY_ONLY", "Return crossed 50%; review layered take-profit, but wait for valuation/trend confirmation."
        if return_ratio >= 0.30:
            return "PRINCIPAL_RECOVERY_REVIEW", "ADVISORY_ONLY", "Return crossed 30%; principal recovery is optional and must be user-selected."
        if return_ratio >= 0.20:
            return "PROFIT_PROTECTION_WATCH", "ADVISORY_ONLY", "Return crossed 20%; enter profit-protection watch."
        if return_ratio <= -0.20:
            return "LOW_ZONE_REVIEW", "ADVISORY_ONLY", f"Return is below -20%; review bucket-specific thesis before adding. bucket={bucket}"
        return "HOLD", "ADVISORY_ONLY", "No strong sell trigger in weekly snapshot mode."

    @staticmethod
    def _decide_free_ride_exit(
        pe: float | None,
        valuation_quality: str,
        nav_watch: dict[str, Any],
        risk_flags: list[str],
    ) -> tuple[str, str, str]:
        drawdown = nav_watch.get("drawdown_from_cached_peak")
        ma4_below_ma8 = bool(nav_watch.get("ma4_below_ma8"))
        history_points = int(nav_watch.get("history_points", 0))

        if valuation_quality != DATA_OK:
            risk_flags.append("NO_STRONG_VALUATION_EXIT_CALL")
        if drawdown is not None and drawdown <= -0.15:
            return (
                "FREE_RIDE_PROFIT_LOCK_REVIEW",
                "ADVISORY_ONLY",
                "Sell-only free-ride holding has drawn down more than 15% from cached peak; review staged exit.",
            )
        if pe is not None and pe >= 0.90 and ma4_below_ma8:
            return (
                "FREE_RIDE_SELL_REVIEW",
                "ADVISORY_ONLY",
                "Sell-only free-ride holding is extremely valued and short trend is weakening; review partial sell.",
            )
        if pe is not None and pe >= 0.90:
            return (
                "FREE_RIDE_OVERVALUED_SELL_REVIEW",
                "ADVISORY_ONLY",
                "Sell-only free-ride holding is in an extreme valuation zone; review whether to sell in layers.",
            )
        if ma4_below_ma8:
            return (
                "FREE_RIDE_TREND_EXIT_WATCH",
                "ADVISORY_ONLY",
                "Sell-only free-ride holding has a weakening cached NAV trend; watch for exit confirmation.",
            )
        if history_points < 4:
            return (
                "FREE_RIDE_EXIT_WATCH",
                "ADVISORY_ONLY",
                "Sell-only free-ride holding is monitored for exit, but cached NAV history is not enough for trend judgment.",
            )
        return (
            "FREE_RIDE_HOLD_WATCH",
            "ADVISORY_ONLY",
            "Sell-only free-ride holding has no exit trigger from cached NAV/valuation checks this week.",
        )

    def _persist_signal(self, conn: duckdb.DuckDBPyConnection, signal: dict[str, Any]) -> None:
        conn.execute(
            """
            DELETE FROM spark_holding_signal
            WHERE snapshot_date = ? AND fund_code = ?
            """,
            [signal["snapshot_date"], signal["fund_code"]],
        )
        risk_flags_json = json.dumps(signal["risk_flags"], ensure_ascii=False)
        conn.execute(
            """
            INSERT INTO spark_holding_signal (
                signal_id, snapshot_date, fund_code, fund_name, shares, unit_cost,
                latest_nav, nav_date, market_value, cost_value, return_ratio,
                holding_mode, action, action_level, data_quality, valuation_quality,
                risk_flags_json, rationale
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                signal["signal_id"],
                signal["snapshot_date"],
                signal["fund_code"],
                signal["fund_name"],
                signal["shares"],
                signal["unit_cost"],
                signal["latest_nav"],
                signal["nav_date"],
                signal["market_value"],
                signal["cost_value"],
                signal["return_ratio"],
                signal["holding_mode"],
                signal["action"],
                signal["action_level"],
                signal["data_quality"],
                signal["valuation_quality"],
                risk_flags_json,
                signal["rationale"],
            ],
        )
        conn.execute(
            """
            DELETE FROM spark_action_review
            WHERE fund_code = ? AND signal_date = ? AND adoption_status = 'PENDING'
            """,
            [signal["fund_code"], signal["snapshot_date"]],
        )
        if signal["action"] != "HOLD":
            conn.execute(
                """
                INSERT INTO spark_action_review (
                    review_id, signal_id, fund_code, signal_date, recommendation, adoption_status
                ) VALUES (?, ?, ?, ?, ?, 'PENDING')
                """,
                [
                    str(uuid4()),
                    signal["signal_id"],
                    signal["fund_code"],
                    signal["snapshot_date"],
                    signal["action"],
                ],
            )


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate Spark ETF holding signals from weekly snapshot")
    parser.add_argument("--date", type=_parse_date, default=None, help="Snapshot date, YYYY-MM-DD; latest if omitted")
    parser.add_argument("--db", type=str, default=None, help="DuckDB path")
    parser.add_argument("--config", type=str, default=None, help="Spark config path")
    parser.add_argument("--no-refresh-nav", action="store_true", help="Use cached NAV only")
    args = parser.parse_args()

    service = HoldingSignalService(db_path=args.db, config_path=args.config)
    signals = service.generate_for_snapshot(snapshot_date=args.date, refresh_nav=(not args.no_refresh_nav))
    print(json.dumps({"count": len(signals), "signals": signals}, ensure_ascii=False, default=str, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
