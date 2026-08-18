#!/usr/bin/env python3
"""Read-only industry breadth observer for Trade Action Plan context."""

from __future__ import annotations

from typing import Any, Dict, Iterable

try:
    from .db_contract import DBGateway, DB_PATH
except Exception:
    from db_contract import DBGateway, DB_PATH


def classify_sector_tide(*, stock_count: int, advancer_ratio: float, above_ma20_ratio: float | None, avg_pct_chg: float) -> str:
    if stock_count < 3:
        return "SECTOR_TIDE_UNAVAILABLE"
    if above_ma20_ratio is not None and advancer_ratio >= 0.60 and above_ma20_ratio >= 0.55 and avg_pct_chg > 0:
        return "SECTOR_SUPPORTIVE"
    if above_ma20_ratio is not None and advancer_ratio <= 0.35 and above_ma20_ratio <= 0.40 and avg_pct_chg < 0:
        return "SECTOR_ADVERSE"
    return "SECTOR_MIXED"


def load_sector_tides(trade_date: str, symbols: Iterable[str], db_path=DB_PATH) -> Dict[str, Dict[str, Any]]:
    clean = sorted({str(symbol or "").strip().upper() for symbol in symbols if str(symbol or "").strip()})
    if not clean:
        return {}
    placeholders = ",".join(["?"] * len(clean))
    with DBGateway(db_path, read_only=True) as conn:
        symbol_rows = conn.execute(
            f"SELECT symbol, COALESCE(industry, '') FROM fact_stock_basic WHERE symbol IN ({placeholders})",
            clean,
        ).fetchall()
        industries = sorted({str(row[1] or "").strip() for row in symbol_rows if str(row[1] or "").strip()})
        metrics = {}
        if industries:
            industry_placeholders = ",".join(["?"] * len(industries))
            rows = conn.execute(
                f"""
                SELECT b.industry,
                       COUNT(*) AS stock_count,
                       AVG(CASE WHEN d.pct_chg > 0 THEN 1.0 ELSE 0.0 END) AS advancer_ratio,
                       AVG(d.pct_chg) AS avg_pct_chg,
                       SUM(CASE WHEN COALESCE(d.ma20, 0) > 0 THEN 1 ELSE 0 END) AS ma20_covered,
                       AVG(CASE WHEN COALESCE(d.ma20, 0) > 0 THEN CASE WHEN d.close >= d.ma20 THEN 1.0 ELSE 0.0 END END) AS above_ma20_ratio
                FROM fact_daily d
                JOIN fact_stock_basic b ON b.symbol = d.symbol
                WHERE CAST(d.trade_date AS DATE) = CAST(? AS DATE)
                  AND b.industry IN ({industry_placeholders})
                  AND COALESCE(d.close, 0) > 0
                GROUP BY b.industry
                """,
                [trade_date, *industries],
            ).fetchall()
            for industry, count, adv, avg_pct, ma20_covered, above in rows:
                covered = int(ma20_covered or 0)
                above_ratio = float(above) if above is not None and covered >= 3 else None
                status = classify_sector_tide(
                    stock_count=int(count or 0),
                    advancer_ratio=float(adv or 0),
                    above_ma20_ratio=above_ratio,
                    avg_pct_chg=float(avg_pct or 0),
                )
                metrics[str(industry)] = {
                    "sector": str(industry),
                    "status": status,
                    "stock_count": int(count or 0),
                    "advancer_ratio": round(float(adv or 0), 4),
                    "avg_pct_chg": round(float(avg_pct or 0), 4),
                    "ma20_covered": covered,
                    "above_ma20_ratio": round(above_ratio, 4) if above_ratio is not None else None,
                    "observer_only": True,
                    "block_entry": False,
                }

    symbol_industry = {str(symbol).upper(): str(industry or "").strip() for symbol, industry in symbol_rows}
    result = {}
    for symbol in clean:
        industry = symbol_industry.get(symbol, "")
        result[symbol] = metrics.get(industry, {
            "sector": industry or "UNKNOWN",
            "status": "SECTOR_TIDE_UNAVAILABLE",
            "stock_count": 0,
            "advancer_ratio": None,
            "avg_pct_chg": None,
            "ma20_covered": 0,
            "above_ma20_ratio": None,
            "observer_only": True,
            "block_entry": False,
        })
    return result
