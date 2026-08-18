from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[3] / "config" / "spark.yaml"


@dataclass(frozen=True, slots=True)
class PortfolioItem:
    etf_code: str
    name: str
    tags: tuple[str, ...]
    avg_cost: float
    current_shares: float
    total_invested: float
    status_override: str | None = None
    asset_bucket: str = "satellite"
    holding_mode: str = "dca_active"
    valuation_proxy: dict[str, Any] | None = None
    dca_policy: dict[str, Any] | None = None
    risk_budget: float | None = None
    max_position_amount: float | None = None
    base_dca_amount: float = 200.0


@dataclass(frozen=True, slots=True)
class SparkConfig:
    portfolios: tuple[PortfolioItem, ...]


def _to_portfolio_item(raw: dict[str, Any]) -> PortfolioItem:
    required = ("etf_code", "name", "tags", "avg_cost", "current_shares", "total_invested")
    missing = [field for field in required if field not in raw]
    if missing:
        raise ValueError(f"Missing required fields in portfolio entry: {missing}")

    tags = raw["tags"]
    if not isinstance(tags, list):
        raise ValueError(f"Field 'tags' must be a list, got {type(tags).__name__}")

    return PortfolioItem(
        etf_code=str(raw["etf_code"]),
        name=str(raw["name"]),
        tags=tuple(str(tag) for tag in tags),
        avg_cost=float(raw["avg_cost"]),
        current_shares=float(raw["current_shares"]),
        total_invested=float(raw["total_invested"]),
        status_override=(str(raw["status_override"]) if raw.get("status_override") is not None else None),
        asset_bucket=str(raw.get("asset_bucket", "satellite")),
        holding_mode=str(raw.get("holding_mode", "dca_active")),
        valuation_proxy=(raw.get("valuation_proxy") if isinstance(raw.get("valuation_proxy"), dict) else None),
        dca_policy=(raw.get("dca_policy") if isinstance(raw.get("dca_policy"), dict) else None),
        risk_budget=(float(raw["risk_budget"]) if raw.get("risk_budget") is not None else None),
        max_position_amount=(float(raw["max_position_amount"]) if raw.get("max_position_amount") is not None else None),
        base_dca_amount=float(raw.get("base_dca_amount", 200.0)),
    )


def load_spark_config(config_path: Path | str | None = None) -> SparkConfig:
    path = Path(config_path).expanduser().resolve() if config_path else DEFAULT_CONFIG_PATH.resolve()
    if not path.exists():
        raise FileNotFoundError(f"Spark config not found: {path}")

    with path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}

    portfolios_raw = raw.get("portfolios", [])
    if not isinstance(portfolios_raw, list):
        raise ValueError("Field 'portfolios' must be a list")

    portfolios = tuple(_to_portfolio_item(item) for item in portfolios_raw)
    return SparkConfig(portfolios=portfolios)
