from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import duckdb

from spark_etf.config.loader import PortfolioItem, load_spark_config
from spark_etf.data.tushare_client import DATA_OK, NAV_ONLY, NO_DATA, STALE_DATA, VALUATION_PROXY_MISSING, TushareClient
from spark_etf.db.db_init import DEFAULT_DB_PATH, EVENT_INSERT_SQL, SNAPSHOT_UPSERT_SQL, init_db

LOGGER = logging.getLogger(__name__)

SUPPORTED_EVENT_TYPES = {"DCA_BUY", "DCA_SKIP", "HARVEST_SELL", "STATE_TRANSITION"}
MIN_VALID_COST = 1e-6
DEFAULT_DCA_POLICY: dict[str, Any] = {
    "extreme_low_percentile": 0.20,
    "low_percentile": 0.35,
    "high_percentile": 0.75,
    "extreme_high_percentile": 0.90,
    "extreme_low_multiplier": 3.0,
    "low_multiplier": 2.0,
    "neutral_multiplier": 1.0,
    "high_multiplier": 0.5,
    "extreme_high_multiplier": 0.0,
}


@dataclass(slots=True)
class SnapshotRow:
    etf_code: str
    state: str
    avg_cost: float
    total_invested: float
    current_shares: float
    principal_recovered: bool
    peak_return: float


@dataclass(slots=True)
class SignalDecision:
    action: str
    event_type: str
    new_state: str
    principal_recovered: bool
    new_peak_return: float
    valuation: dict[str, float | str | None]
    current_return: float | None
    drawdown: float | None
    note: str
    suggested_amount: float
    action_reason: str
    next_trigger: str
    risk_flags: list[str]


class SignalService:
    def __init__(
        self,
        db_path: Path | str | None = None,
        tushare_client: TushareClient | None = None,
        config_path: Path | str | None = None,
    ) -> None:
        self.db_path = Path(db_path).expanduser().resolve() if db_path else DEFAULT_DB_PATH.resolve()
        self.tushare_client = tushare_client or TushareClient()
        self.config_path = Path(config_path).expanduser().resolve() if config_path else None
        self.portfolio_by_code = self._load_portfolios()
        init_db(self.db_path)

    def _load_portfolios(self) -> dict[str, PortfolioItem]:
        try:
            cfg = load_spark_config(self.config_path)
        except Exception:
            LOGGER.exception("Spark config load failed; signal service will run with defaults")
            return {}
        return {item.etf_code: item for item in cfg.portfolios}

    def generate_signals(self) -> list[dict[str, Any]]:
        conn = duckdb.connect(str(self.db_path))
        results: list[dict[str, Any]] = []
        try:
            for snapshot in self._load_snapshots(conn):
                portfolio = self.portfolio_by_code.get(snapshot.etf_code)
                decision = self._decide_with_guard(snapshot, portfolio)
                persisted = self._persist_decision(conn, snapshot, decision)
                results.append(
                    {
                        "etf_code": snapshot.etf_code,
                        "prev_state": snapshot.state,
                        "new_state": decision.new_state,
                        "action": decision.action,
                        "event_type": decision.event_type,
                        "current_return": decision.current_return,
                        "peak_return": decision.new_peak_return,
                        "drawdown": decision.drawdown,
                        "nav": decision.valuation["nav"],
                        "pe_percentile": decision.valuation.get("pe_percentile"),
                        "trade_date": decision.valuation["trade_date"],
                        "valuation_proxy": decision.valuation.get("valuation_proxy"),
                        "data_quality": decision.valuation.get("data_quality"),
                        "suggested_amount": decision.suggested_amount,
                        "action_reason": decision.action_reason,
                        "next_trigger": decision.next_trigger,
                        "risk_flags": decision.risk_flags,
                        "note": decision.note,
                        "persisted": persisted,
                    }
                )
        finally:
            conn.close()
        return results

    def _load_snapshots(self, conn: duckdb.DuckDBPyConnection) -> list[SnapshotRow]:
        rows = conn.execute(
            """
            SELECT etf_code, state, avg_cost, total_invested, current_shares, principal_recovered, peak_return
            FROM spark_state_snapshot
            ORDER BY etf_code
            """
        ).fetchall()
        return [
            SnapshotRow(
                etf_code=str(row[0]),
                state=str(row[1]),
                avg_cost=float(row[2]),
                total_invested=float(row[3]),
                current_shares=float(row[4]),
                principal_recovered=bool(row[5]),
                peak_return=float(row[6]),
            )
            for row in rows
        ]

    def _decide_with_guard(self, snapshot: SnapshotRow, portfolio: PortfolioItem | None) -> SignalDecision:
        try:
            valuation = self.tushare_client.get_valuation_and_price(
                snapshot.etf_code,
                valuation_proxy=(portfolio.valuation_proxy if portfolio else None),
            )
            return self._decide(snapshot, valuation, portfolio)
        except Exception as exc:
            LOGGER.exception("Data fetch failed for %s", snapshot.etf_code)
            fallback_nav = snapshot.avg_cost if snapshot.avg_cost > MIN_VALID_COST else 1.0
            valuation: dict[str, float | str | None] = {
                "nav": float(fallback_nav),
                "pe_percentile": None,
                "trade_date": datetime.now().strftime("%Y%m%d"),
                "valuation_proxy": "",
                "data_quality": NO_DATA,
                "note": str(exc),
            }
            return SignalDecision(
                action="DCA_SKIP",
                event_type="DCA_SKIP",
                new_state=snapshot.state,
                principal_recovered=snapshot.principal_recovered,
                new_peak_return=snapshot.peak_return,
                valuation=valuation,
                current_return=None,
                drawdown=None,
                note=f"Data fetch failed: {exc}",
                suggested_amount=0.0,
                action_reason="Data fetch failed; skip new capital.",
                next_trigger="Wait for DATA_OK or NAV_ONLY recovery.",
                risk_flags=[NO_DATA],
            )

    def _decide(
        self,
        snapshot: SnapshotRow,
        valuation: dict[str, float | str | None],
        portfolio: PortfolioItem | None,
    ) -> SignalDecision:
        nav = float(valuation["nav"])
        pe_raw = valuation.get("pe_percentile")
        pe_percentile = float(pe_raw) if pe_raw is not None else None
        data_quality = str(valuation.get("data_quality", DATA_OK))

        current_return = self._calc_current_return(snapshot.avg_cost, nav)
        peak_return = snapshot.peak_return
        if current_return is not None:
            peak_return = max(peak_return, current_return)

        action = "DCA_SKIP"
        new_state = snapshot.state
        principal_recovered = snapshot.principal_recovered
        drawdown: float | None = None
        note = "No rule matched"
        action_reason = "Hold and monitor."
        risk_flags = self._quality_risk_flags(data_quality)
        suggested_amount = 0.0

        if snapshot.state == "ACCUMULATING":
            if data_quality in {NO_DATA, STALE_DATA}:
                note = f"{data_quality}, skip DCA"
                action_reason = "Data is unavailable or stale; skip new capital."
            elif current_return is not None and current_return >= 0.30 and not snapshot.principal_recovered:
                action = "HARVEST_SELL"
                new_state = "FREE_RIDE"
                principal_recovered = True
                note = "Return >= 30%, harvest principal and switch to FREE_RIDE"
                action_reason = "Return crossed 30%; harvest principal first."
            elif pe_percentile is None:
                note = f"{data_quality}, valuation percentile unavailable"
                action_reason = "NAV exists but valuation proxy is unavailable; skip new capital."
            elif pe_percentile >= 0.85:
                action = "PAUSE_SIGNAL"
                new_state = "PAUSED_OVERVALUED"
                note = "PE percentile >= 0.85, switch to PAUSED_OVERVALUED"
                action_reason = "Valuation percentile is over 85%; pause DCA."
            else:
                suggested_amount, amount_flags = self._calc_suggested_amount(snapshot, nav, pe_percentile, portfolio)
                risk_flags.extend(amount_flags)
                if suggested_amount > 0:
                    action = "DCA_BUY"
                    note = "Continue DCA accumulation"
                    action_reason = self._dca_reason(pe_percentile, suggested_amount)
                else:
                    note = "Dynamic DCA amount is zero"
                    action_reason = "Dynamic DCA amount is zero after policy and position cap."

        elif snapshot.state == "PAUSED_OVERVALUED":
            if pe_percentile is None:
                note = f"{data_quality}, remain paused"
                action_reason = "Valuation proxy is unavailable; remain paused."
            elif pe_percentile <= 0.70:
                suggested_amount, amount_flags = self._calc_suggested_amount(snapshot, nav, pe_percentile, portfolio)
                risk_flags.extend(amount_flags)
                action = "STATE_TRANSITION" if suggested_amount <= 0 else "DCA_BUY"
                new_state = "ACCUMULATING"
                note = "PE percentile <= 0.70, resume ACCUMULATING"
                action_reason = "Valuation cooled below 70%; resume accumulation."
            else:
                action = "DCA_SKIP"
                note = "Remain paused, capital stays in bank account"
                action_reason = "Overvaluation pause remains active."

        elif snapshot.state == "FREE_RIDE":
            if snapshot.avg_cost <= MIN_VALID_COST:
                peak_nav = max(snapshot.peak_return if snapshot.peak_return > 0 else nav, nav)
                peak_return = peak_nav
                drawdown = 0.0 if peak_nav <= 0 else (peak_nav - nav) / peak_nav
            else:
                if current_return is None:
                    current_return = 0.0
                peak_return = max(snapshot.peak_return, current_return)
                denominator = 1.0 + peak_return
                drawdown = 0.0 if denominator <= 0 else (peak_return - current_return) / denominator

            if drawdown >= 0.15:
                action = "HARVEST_SELL"
                note = "FREE_RIDE drawdown >= 15%, lock profits"
                action_reason = "FREE_RIDE drawdown reached 15%; lock profit."
            else:
                action = "DCA_SKIP"
                note = "FREE_RIDE holding, drawdown below 15%"
                action_reason = "FREE_RIDE holding; no principal reinvestment."

        else:
            action = "DCA_SKIP"
            note = f"Unknown state '{snapshot.state}', fallback to DCA_SKIP"
            action_reason = "Unknown state; skip conservatively."
            risk_flags.append("UNKNOWN_STATE")

        event_type = action if action in SUPPORTED_EVENT_TYPES else "STATE_TRANSITION"
        payload: dict[str, float | str | None] = {
            "nav": nav,
            "pe_percentile": pe_percentile,
            "trade_date": str(valuation["trade_date"]),
            "valuation_proxy": valuation.get("valuation_proxy"),
            "data_quality": data_quality,
        }

        return SignalDecision(
            action=action,
            event_type=event_type,
            new_state=new_state,
            principal_recovered=principal_recovered,
            new_peak_return=float(peak_return),
            valuation=payload,
            current_return=current_return,
            drawdown=drawdown,
            note=note,
            suggested_amount=float(round(suggested_amount, 2)),
            action_reason=action_reason,
            next_trigger=self._next_trigger(new_state, pe_percentile, current_return, drawdown, data_quality),
            risk_flags=sorted(set(risk_flags)),
        )

    def _persist_decision(self, conn: duckdb.DuckDBPyConnection, snapshot: SnapshotRow, decision: SignalDecision) -> bool:
        event_payload = {
            "action": decision.action,
            "event_type": decision.event_type,
            "prev_state": snapshot.state,
            "new_state": decision.new_state,
            "avg_cost": snapshot.avg_cost,
            "current_shares": snapshot.current_shares,
            "total_invested": snapshot.total_invested,
            "principal_recovered": decision.principal_recovered,
            "peak_return": decision.new_peak_return,
            "current_return": decision.current_return,
            "drawdown": decision.drawdown,
            "valuation": decision.valuation,
            "suggested_amount": decision.suggested_amount,
            "action_reason": decision.action_reason,
            "next_trigger": decision.next_trigger,
            "risk_flags": decision.risk_flags,
            "source": "signal_service",
        }
        try:
            conn.execute("BEGIN")
            conn.execute(
                SNAPSHOT_UPSERT_SQL,
                [
                    snapshot.etf_code,
                    decision.new_state,
                    snapshot.avg_cost,
                    snapshot.total_invested,
                    snapshot.current_shares,
                    decision.principal_recovered,
                    decision.new_peak_return,
                ],
            )
            conn.execute(
                EVENT_INSERT_SQL,
                [str(uuid4()), snapshot.etf_code, decision.event_type, json.dumps(event_payload, ensure_ascii=False), decision.note],
            )
            conn.execute("COMMIT")
            return True
        except Exception:
            conn.execute("ROLLBACK")
            LOGGER.exception("Persist failed for %s", snapshot.etf_code)
            return False

    @staticmethod
    def _calc_current_return(avg_cost: float, nav: float) -> float | None:
        if avg_cost <= MIN_VALID_COST:
            return None
        return (nav - avg_cost) / avg_cost

    @staticmethod
    def _quality_risk_flags(data_quality: str) -> list[str]:
        return [] if data_quality == DATA_OK else [data_quality]

    def _calc_suggested_amount(self, snapshot: SnapshotRow, nav: float, pe_percentile: float, portfolio: PortfolioItem | None) -> tuple[float, list[str]]:
        policy = dict(DEFAULT_DCA_POLICY)
        if portfolio and portfolio.dca_policy:
            policy.update(portfolio.dca_policy)
        base = float(portfolio.base_dca_amount if portfolio else 200.0)
        if pe_percentile >= float(policy["extreme_high_percentile"]):
            multiplier = float(policy["extreme_high_multiplier"])
        elif pe_percentile >= float(policy["high_percentile"]):
            multiplier = float(policy["high_multiplier"])
        elif pe_percentile <= float(policy["extreme_low_percentile"]):
            multiplier = float(policy["extreme_low_multiplier"])
        elif pe_percentile <= float(policy["low_percentile"]):
            multiplier = float(policy["low_multiplier"])
        else:
            multiplier = float(policy["neutral_multiplier"])
        amount = max(0.0, base * multiplier)
        flags: list[str] = []
        max_position = portfolio.max_position_amount if portfolio else None
        if max_position is not None:
            market_value = snapshot.current_shares * nav
            room = max(0.0, float(max_position) - market_value)
            if room <= 0:
                amount = 0.0
                flags.append("POSITION_LIMIT_REACHED")
            elif amount > room:
                amount = room
                flags.append("POSITION_LIMIT_CLIPPED")
        return amount, flags

    @staticmethod
    def _dca_reason(pe_percentile: float, amount: float) -> str:
        if pe_percentile <= 0.20:
            return f"Extreme low valuation; strong DCA reminder, capped by position limit. Amount: {amount:.0f}."
        if pe_percentile <= 0.35:
            return f"Low valuation; increase DCA. Amount: {amount:.0f}."
        if pe_percentile >= 0.75:
            return f"High valuation; reduce DCA. Amount: {amount:.0f}."
        return f"Neutral valuation; standard DCA. Amount: {amount:.0f}."

    @staticmethod
    def _next_trigger(state: str, pe_percentile: float | None, current_return: float | None, drawdown: float | None, data_quality: str) -> str:
        if data_quality in {NO_DATA, STALE_DATA, NAV_ONLY, VALUATION_PROXY_MISSING}:
            return "Wait for DATA_OK before amount engine resumes"
        if state == "PAUSED_OVERVALUED":
            return "Resume when valuation percentile <= 70%"
        if state == "FREE_RIDE":
            return "Lock profit when FREE_RIDE drawdown >= 15%"
        if current_return is not None and current_return < 0.30:
            return f"Harvest principal at 30% return; gap {(0.30 - current_return) * 100:.2f}%"
        if pe_percentile is not None:
            if pe_percentile < 0.85:
                return f"Pause DCA at 85% valuation; gap {(0.85 - pe_percentile) * 100:.2f}%"
            return "Valuation is already in pause zone"
        return "Wait for next valid valuation"


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Spark ETF signals from FSM")
    parser.add_argument("--db", type=str, default=None, help="DuckDB file path")
    parser.add_argument("--config", type=str, default=None, help="Spark config file path")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    service = SignalService(db_path=args.db, config_path=args.config)
    signals = service.generate_signals()
    print(json.dumps({"count": len(signals), "signals": signals}, ensure_ascii=False))


if __name__ == "__main__":
    main()
