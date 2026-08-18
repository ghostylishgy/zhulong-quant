from __future__ import annotations

import hashlib
import os
import random
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

try:
    import tushare as ts
except Exception:  # pragma: no cover - optional dependency in mock mode
    ts = None

DATA_OK = "DATA_OK"
NAV_ONLY = "NAV_ONLY"
VALUATION_PROXY_MISSING = "VALUATION_PROXY_MISSING"
NO_DATA = "NO_DATA"
STALE_DATA = "STALE_DATA"
STALE_NAV_DAYS = 10


class TushareClient:
    """Standardized NAV and valuation client for Spark ETF FSM."""

    def __init__(self, token: str | None = None, use_mock: bool | None = None) -> None:
        self._load_env_files()
        self.token = token or os.getenv("TUSHARE_TOKEN")

        if use_mock is None:
            self.use_mock = os.getenv("MOCK_TUSHARE", "0") == "1"
        else:
            self.use_mock = use_mock

    def _load_env_files(self) -> None:
        here = Path(__file__).resolve()
        env_candidates = [
            here.parents[2] / ".env",  # ../../.env from current file
            here.parents[3] / ".env",  # 08_spark_etf/.env
            here.parents[4] / ".env",  # quant_project/.env
            Path.cwd() / ".env",
        ]

        for env_file in env_candidates:
            if env_file.exists():
                load_dotenv(dotenv_path=env_file, override=False)

    def get_valuation_and_price(
        self,
        etf_code: str,
        valuation_proxy: dict[str, Any] | None = None,
    ) -> dict[str, float | str | None]:
        if self.use_mock:
            return self._normalize_payload(self._mock_payload(etf_code, valuation_proxy))

        nav_payload = self.get_fund_nav(etf_code)
        proxy_payload = self.get_valuation_proxy(valuation_proxy)
        payload: dict[str, object] = {
            "nav": nav_payload["nav"],
            "trade_date": nav_payload["trade_date"],
            "pe_percentile": proxy_payload.get("pe_percentile"),
            "valuation_proxy": proxy_payload.get("valuation_proxy", ""),
            "data_quality": proxy_payload.get("data_quality", NAV_ONLY),
        }

        if self._is_stale_trade_date(str(nav_payload["trade_date"])):
            payload["data_quality"] = STALE_DATA
            payload["note"] = "NAV data is stale"
        elif proxy_payload.get("note"):
            payload["note"] = proxy_payload["note"]

        return self._normalize_payload(payload)

    def get_fund_nav(self, etf_code: str) -> dict[str, float | str]:
        if ts is None:
            raise RuntimeError("tushare is not installed. Install tushare or set MOCK_TUSHARE=1")
        if not self.token:
            raise RuntimeError("TUSHARE_TOKEN is missing. Check quant_project/.env or set MOCK_TUSHARE=1")

        pro = ts.pro_api(self.token)
        nav_df = pro.fund_nav(
            ts_code=etf_code,
            fields="ts_code,end_date,nav_date,adj_nav,unit_nav",
            limit=1,
        )
        if nav_df is None or nav_df.empty:
            raise RuntimeError(f"No NAV data from Tushare for {etf_code}")

        row = nav_df.iloc[0]
        nav_value = self._first_non_empty(row, ["unit_nav", "adj_nav"])
        if nav_value is None:
            for col in nav_df.columns:
                if "nav" not in str(col).lower():
                    continue
                nav_value = self._first_non_empty(row, [str(col)])
                if nav_value is not None:
                    break

        if nav_value is None:
            raise RuntimeError(f"NAV column missing/invalid for {etf_code}: {list(nav_df.columns)}")

        trade_date_value = self._first_non_empty(row, ["end_date", "nav_date"])
        if trade_date_value is None and len(nav_df.columns) > 0:
            trade_date_value = self._first_non_empty(row, [str(nav_df.columns[0])])

        adj_nav_value = self._first_non_empty(row, ["adj_nav"])
        return {
            "nav": float(nav_value),
            "unit_nav": float(nav_value),
            "adj_nav": float(adj_nav_value) if adj_nav_value is not None else float(nav_value),
            "trade_date": str(trade_date_value) if trade_date_value is not None else datetime.now().strftime("%Y%m%d"),
        }

    def get_fund_nav_history(
        self,
        etf_code: str,
        start_date: date | str | None = None,
        end_date: date | str | None = None,
        limit: int = 120,
    ) -> list[dict[str, float | str]]:
        if self.use_mock:
            return self._mock_nav_history(etf_code, limit=limit)
        if ts is None:
            raise RuntimeError("tushare is not installed. Install tushare or set MOCK_TUSHARE=1")
        if not self.token:
            raise RuntimeError("TUSHARE_TOKEN is missing. Check quant_project/.env or set MOCK_TUSHARE=1")

        kwargs: dict[str, object] = {
            "ts_code": etf_code,
            "fields": "ts_code,end_date,nav_date,adj_nav,unit_nav",
            "limit": int(limit),
        }
        if start_date is not None:
            kwargs["start_date"] = self._format_tushare_date(start_date)
        if end_date is not None:
            kwargs["end_date"] = self._format_tushare_date(end_date)

        pro = ts.pro_api(self.token)
        nav_df = pro.fund_nav(**kwargs)
        if nav_df is None or nav_df.empty:
            return []

        rows: list[dict[str, float | str]] = []
        for _, row in nav_df.iterrows():
            unit_nav_value = self._first_non_empty(row, ["unit_nav", "adj_nav"])
            if unit_nav_value is None:
                continue
            adj_nav_value = self._first_non_empty(row, ["adj_nav"])
            trade_date_value = self._first_non_empty(row, ["end_date", "nav_date"])
            if trade_date_value is None:
                continue
            rows.append(
                {
                    "trade_date": str(trade_date_value),
                    "unit_nav": float(unit_nav_value),
                    "adj_nav": float(adj_nav_value) if adj_nav_value is not None else float(unit_nav_value),
                }
            )

        return sorted(rows, key=lambda item: str(item["trade_date"]))

    def get_fund_basic(self, market: str = "O") -> list[dict[str, str | None]]:
        if self.use_mock:
            return self._mock_fund_basic(market)
        if ts is None:
            raise RuntimeError("tushare is not installed. Install tushare or set MOCK_TUSHARE=1")
        if not self.token:
            raise RuntimeError("TUSHARE_TOKEN is missing. Check quant_project/.env or set MOCK_TUSHARE=1")

        pro = ts.pro_api(self.token)
        fields = "ts_code,name,management,fund_type,found_date,status"
        df = pro.fund_basic(market=market, fields=fields)
        if df is None or df.empty:
            return []

        rows: list[dict[str, str | None]] = []
        for _, row in df.iterrows():
            rows.append(
                {
                    "ts_code": self._clean_optional_text(row.get("ts_code")),
                    "name": self._clean_optional_text(row.get("name")),
                    "management": self._clean_optional_text(row.get("management")),
                    "fund_type": self._clean_optional_text(row.get("fund_type")),
                    "found_date": self._clean_optional_text(row.get("found_date")),
                    "status": self._clean_optional_text(row.get("status")),
                    "market": market,
                }
            )
        return [row for row in rows if row["ts_code"]]

    def get_valuation_proxy(self, valuation_proxy: dict[str, Any] | None) -> dict[str, float | str | None]:
        if not valuation_proxy or not valuation_proxy.get("ts_code"):
            return {
                "pe_percentile": None,
                "valuation_proxy": "",
                "data_quality": VALUATION_PROXY_MISSING,
                "note": "valuation proxy missing in spark.yaml",
            }

        ts_code = str(valuation_proxy["ts_code"])
        metric = str(valuation_proxy.get("metric", "pe_ttm"))

        try:
            if ts is None:
                raise RuntimeError("tushare is not installed")
            if not self.token:
                raise RuntimeError("TUSHARE_TOKEN is missing")

            pro = ts.pro_api(self.token)
            pe_df = pro.index_dailybasic(
                ts_code=ts_code,
                fields=f"trade_date,{metric}",
                limit=int(valuation_proxy.get("lookback", 800)),
            )
            if pe_df is None or pe_df.empty:
                raise RuntimeError(f"No valuation data from Tushare for proxy {ts_code}")
            if metric not in pe_df.columns:
                raise RuntimeError(f"Missing {metric} in index_dailybasic for {ts_code}")

            series = pe_df[metric].dropna().astype(float)
            if series.empty:
                raise RuntimeError(f"Valuation series is empty for proxy {ts_code}")

            latest = float(series.iloc[0])
            percentile = round(float((series <= latest).sum()) / float(len(series)), 4)
            return {
                "pe_percentile": percentile,
                "valuation_proxy": ts_code,
                "data_quality": DATA_OK,
            }
        except Exception as exc:
            return {
                "pe_percentile": None,
                "valuation_proxy": ts_code,
                "data_quality": NAV_ONLY,
                "note": f"valuation proxy fetch failed: {exc}",
            }

    def _normalize_payload(self, payload: dict[str, object]) -> dict[str, float | str | None]:
        normalized: dict[str, float | str | None] = {
            "nav": float(payload["nav"]),
            "pe_percentile": (float(payload["pe_percentile"]) if payload.get("pe_percentile") is not None else None),
            "trade_date": str(payload["trade_date"]),
            "valuation_proxy": str(payload.get("valuation_proxy", "")),
            "data_quality": str(payload.get("data_quality", DATA_OK)),
        }
        note = payload.get("note")
        if note is not None and str(note).strip():
            normalized["note"] = str(note).strip()
        return normalized

    def _mock_payload(self, etf_code: str, valuation_proxy: dict[str, Any] | None) -> dict[str, float | str | None]:
        trade_date = datetime.now().strftime("%Y%m%d")
        seed_src = f"{etf_code}:{trade_date}"
        seed_val = int(hashlib.sha256(seed_src.encode("utf-8")).hexdigest()[:16], 16)
        rng = random.Random(seed_val)

        nav = round(rng.uniform(0.55, 3.80), 4)
        raw_proxy_code = (valuation_proxy or {}).get("ts_code")
        proxy_code = str(raw_proxy_code).strip() if raw_proxy_code is not None else ""
        pe_percentile = round(rng.uniform(0.05, 0.95), 4) if proxy_code else None
        return {
            "nav": nav,
            "pe_percentile": pe_percentile,
            "trade_date": trade_date,
            "valuation_proxy": proxy_code,
            "data_quality": DATA_OK if proxy_code else VALUATION_PROXY_MISSING,
        }

    def _mock_nav_history(self, etf_code: str, limit: int) -> list[dict[str, float | str]]:
        today = date.today()
        rows: list[dict[str, float | str]] = []
        seed_val = int(hashlib.sha256(etf_code.encode("utf-8")).hexdigest()[:16], 16)
        rng = random.Random(seed_val)
        nav = rng.uniform(0.55, 3.80)
        for offset in range(max(1, int(limit)) - 1, -1, -1):
            day = today - timedelta(days=offset)
            nav = max(0.05, nav * (1.0 + rng.uniform(-0.012, 0.012)))
            rows.append(
                {
                    "trade_date": day.strftime("%Y%m%d"),
                    "unit_nav": round(nav, 4),
                    "adj_nav": round(nav, 4),
                }
            )
        return rows

    def _mock_fund_basic(self, market: str) -> list[dict[str, str | None]]:
        return [
            {
                "ts_code": "021030.OF",
                "name": "汇添富国证港股通创新药ETF联接A",
                "management": "汇添富基金",
                "fund_type": "股票型",
                "found_date": "20240419",
                "status": "L",
                "market": market,
            },
            {
                "ts_code": "021090.OF",
                "name": "鹏华中证云计算与大数据主题ETF联接A",
                "management": "鹏华基金",
                "fund_type": "股票型",
                "found_date": "20240903",
                "status": "L",
                "market": market,
            },
            {
                "ts_code": "013402.OF",
                "name": "华夏恒生科技ETF联接A",
                "management": "华夏基金",
                "fund_type": "股票型",
                "found_date": "20210928",
                "status": "L",
                "market": market,
            },
        ]

    def _first_non_empty(self, row: object, columns: list[str]) -> object | None:
        for col in columns:
            if hasattr(row, "get"):
                value = row.get(col)
                if value is not None and str(value).strip() != "":
                    return value
        return None

    @staticmethod
    def _clean_optional_text(value: object) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _is_stale_trade_date(trade_date: str) -> bool:
        text = trade_date.strip()
        for fmt in ("%Y%m%d", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(text[:10], fmt)
                return (datetime.now() - dt).days > STALE_NAV_DAYS
            except ValueError:
                continue
        return False

    @staticmethod
    def _format_tushare_date(value: date | str) -> str:
        if isinstance(value, date):
            return value.strftime("%Y%m%d")
        text = str(value).strip()
        for fmt in ("%Y%m%d", "%Y-%m-%d"):
            try:
                return datetime.strptime(text[:10], fmt).strftime("%Y%m%d")
            except ValueError:
                continue
        raise ValueError(f"invalid Tushare date: {value}")
