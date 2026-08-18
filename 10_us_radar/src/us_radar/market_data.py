"""Market data adapter for US radar validation.

The sidecar reads A-share target prices from Zhulong's local DuckDB in read-only
mode. Index benchmarks fall back to Tushare when they are not present locally.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from pathlib import Path
import hashlib
import json
import logging
import os
import time
from typing import Iterable
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from zoneinfo import ZoneInfo


logger = logging.getLogger(__name__)
US_EASTERN = ZoneInfo("America/New_York")
US_MARKET_CLOSE = datetime_time(16, 0)
CN_MARKET_TIMEZONE = ZoneInfo("Asia/Shanghai")
CN_MARKET_CLOSE = datetime_time(15, 0)
BASE_LOOKBACK_CALENDAR_DAYS = 15
CORPORATE_ACTION_TOLERANCE = 0.0002


@dataclass(frozen=True)
class PricePoint:
    trade_date: date
    close: float
    reference_close: float | None = None


@dataclass(frozen=True)
class ReturnResult:
    symbol: str
    market: str
    horizon_days: int
    return_pct: float | None
    base_date: str | None
    horizon_date: str | None
    base_close: float | None
    horizon_close: float | None
    provider: str
    data_quality: str


class MarketDataClient:
    def __init__(self, root: Path | str):
        self.root = Path(root)
        self.env = _load_env_files([self.root / ".env", self.root / "10_us_radar" / ".env.local"])
        snapshot_db = self.root / "storage" / "database" / "zhulong_api_readonly.duckdb"
        live_db = self.root / "storage" / "database" / "zhulong.duckdb"
        snapshot_setting = self.env.get("US_RADAR_REQUIRE_SNAPSHOT")
        if snapshot_setting is None:
            snapshot_setting = os.environ.get("US_RADAR_REQUIRE_SNAPSHOT")
        require_snapshot = True if snapshot_setting is None else _truthy(snapshot_setting)
        if snapshot_db.exists():
            self.zhulong_db = snapshot_db
        elif require_snapshot:
            logger.warning("cn snapshot required but missing provider=zhulong.fact_daily path=%s", snapshot_db)
            self.zhulong_db = snapshot_db
        else:
            logger.warning("cn snapshot missing, falling back to live readonly db provider=zhulong.fact_daily path=%s", live_db)
            self.zhulong_db = live_db
        self._return_cache: dict[tuple[str, str, str, int], ReturnResult] = {}
        self._benchmark_cache: dict[tuple[str, str, str, int], ReturnResult] = {}
        self._last_quality: dict[tuple[str, str], str] = {}
        self._twelve_call_times: list[float] = []
        self._twelve_memory_cache: dict[str, list[PricePoint]] = {}
        self._market_cache_dir = self.root / "10_us_radar" / "data" / "cache" / "market_data"

    def get_return(self, market: str, symbol: str, event_time: str, horizon_days: int) -> ReturnResult:
        cache_key = (market, symbol, event_time, horizon_days)
        if cache_key in self._return_cache:
            return self._return_cache[cache_key]
        event_day = _event_base_date(event_time, market)
        start = event_day - timedelta(days=BASE_LOOKBACK_CALENDAR_DAYS)
        end = event_day + timedelta(days=max(10, horizon_days * 4 + 7))
        if market == "CN":
            prices = self._cn_local_prices(symbol, start, end)
            quality = self._last_quality.get(("CN_LOCAL", symbol), "NO_PRICE_DATA")
            result = _return_from_prices(
                symbol,
                market,
                horizon_days,
                event_day,
                prices,
                "zhulong.fact_daily",
                empty_quality=quality,
            )
            self._return_cache[cache_key] = result
            return result
        if market == "US":
            prices, provider, quality = self._us_prices(symbol, start, end)
            result = _return_from_prices(symbol, market, horizon_days, event_day, prices, provider, empty_quality=quality)
            self._return_cache[cache_key] = result
            return result
        result = _empty_result(symbol, market, horizon_days, "UNSUPPORTED_MARKET")
        self._return_cache[cache_key] = result
        return result

    def get_benchmark_return(self, market: str, symbol: str, event_time: str, horizon_days: int) -> ReturnResult:
        cache_key = (market, symbol, event_time, horizon_days)
        if cache_key in self._benchmark_cache:
            return self._benchmark_cache[cache_key]
        event_day = _event_base_date(event_time, market)
        start = event_day - timedelta(days=BASE_LOOKBACK_CALENDAR_DAYS)
        end = event_day + timedelta(days=max(10, horizon_days * 4 + 7))
        if market == "CN":
            prices = self._cn_local_prices(symbol, start, end)
            if prices:
                result = _return_from_prices(symbol, market, horizon_days, event_day, prices, "zhulong.fact_daily")
                self._benchmark_cache[cache_key] = result
                return result
            prices = self._tushare_index_prices(symbol, start, end)
            quality = self._last_quality.get(("CN_BENCHMARK", symbol), "NO_PRICE_DATA")
            result = _return_from_prices(
                symbol,
                market,
                horizon_days,
                event_day,
                prices,
                "tushare.index_daily",
                empty_quality=quality,
            )
            self._benchmark_cache[cache_key] = result
            return result
        result = self.get_return(market, symbol, event_time, horizon_days)
        self._benchmark_cache[cache_key] = result
        return result

    def _cn_local_prices(self, symbol: str, start: date, end: date) -> list[PricePoint]:
        self._last_quality[("CN_LOCAL", symbol)] = "NO_PRICE_DATA"
        if not self.zhulong_db.exists():
            self._last_quality[("CN_LOCAL", symbol)] = "PROVIDER_ERROR"
            logger.warning("cn local price db missing provider=zhulong.fact_daily symbol=%s path=%s", symbol, self.zhulong_db)
            return []
        try:
            import duckdb
        except ImportError as exc:
            self._last_quality[("CN_LOCAL", symbol)] = "PROVIDER_ERROR"
            logger.warning("cn local price provider error provider=zhulong.fact_daily symbol=%s reason=%s", symbol, exc)
            return []
        try:
            conn = duckdb.connect(str(self.zhulong_db), read_only=True)
            columns = {str(row[1]) for row in conn.execute("PRAGMA table_info('fact_daily')").fetchall()}
            reference_column = "pre_close" if "pre_close" in columns else "NULL AS pre_close"
            query = f"""
                SELECT trade_date, close, {reference_column}
                FROM fact_daily
                WHERE symbol = ? AND trade_date BETWEEN ? AND ?
                ORDER BY trade_date
            """
            rows = conn.execute(query, [symbol, start.isoformat(), end.isoformat()]).fetchall()
            conn.close()
        except Exception as exc:
            self._last_quality[("CN_LOCAL", symbol)] = "PROVIDER_ERROR"
            logger.warning("cn local price provider error provider=zhulong.fact_daily symbol=%s reason=%s", symbol, exc)
            return []
        points = [_point(row[0], row[1], row[2]) for row in rows if row[1] is not None]
        if not points:
            self._last_quality[("CN_LOCAL", symbol)] = "NO_PRICE_DATA"
        return points

    def _tushare_index_prices(self, symbol: str, start: date, end: date) -> list[PricePoint]:
        self._last_quality[("CN_BENCHMARK", symbol)] = "NO_PRICE_DATA"
        token = self.env.get("TUSHARE_TOKEN") or os.environ.get("TUSHARE_TOKEN")
        if not token:
            self._last_quality[("CN_BENCHMARK", symbol)] = "PROVIDER_ERROR"
            logger.warning("cn benchmark provider missing token provider=tushare.index_daily symbol=%s", symbol)
            return []
        try:
            import tushare as ts
        except ImportError as exc:
            self._last_quality[("CN_BENCHMARK", symbol)] = "PROVIDER_ERROR"
            logger.warning("cn benchmark provider error provider=tushare.index_daily symbol=%s reason=%s", symbol, exc)
            return []
        try:
            pro = ts.pro_api(token)
            frame = pro.index_daily(ts_code=symbol, start_date=_ts_date(start), end_date=_ts_date(end))
        except Exception as exc:
            self._last_quality[("CN_BENCHMARK", symbol)] = "PROVIDER_ERROR"
            logger.warning("cn benchmark provider error provider=tushare.index_daily symbol=%s reason=%s", symbol, exc)
            return []
        if frame is None or frame.empty:
            self._last_quality[("CN_BENCHMARK", symbol)] = "NO_PRICE_DATA"
            return []
        rows = []
        for _, item in frame.iterrows():
            reference_close = item.get("pre_close")
            rows.append(
                PricePoint(
                    _parse_day(str(item["trade_date"])),
                    float(item["close"]),
                    None if reference_close is None else float(reference_close),
                )
            )
        return sorted(rows, key=lambda point: point.trade_date)

    def _us_prices(self, symbol: str, start: date, end: date) -> tuple[list[PricePoint], str, str]:
        self._last_quality[("US", symbol)] = "NO_PRICE_DATA"
        disable_fmp = _truthy(self.env.get("US_RADAR_DISABLE_FMP") or os.environ.get("US_RADAR_DISABLE_FMP"))
        if not disable_fmp:
            prices = self._fmp_prices(symbol, start, end)
            if prices:
                return prices, "fmp.historical-price-full", "NO_PRICE_DATA"
        prices = self._yfinance_prices(symbol, start, end)
        if prices:
            return prices, "yfinance.download", "NO_PRICE_DATA"
        prices = self._alpha_vantage_prices(symbol, start, end)
        if prices:
            return prices, "alpha_vantage.daily", "NO_PRICE_DATA"
        prices = self._twelve_data_prices(symbol, start, end)
        if prices:
            return prices, "twelve_data.time_series", "NO_PRICE_DATA"
        quality = self._last_quality.get(("US", symbol), "NO_PRICE_DATA")
        return [], "us_api_unavailable", quality

    def _fmp_prices(self, symbol: str, start: date, end: date) -> list[PricePoint]:
        key = self.env.get("US_RADAR_FMP_API_KEY") or os.environ.get("US_RADAR_FMP_API_KEY")
        if not key:
            self._last_quality[("US", symbol)] = "PROVIDER_ERROR"
            logger.warning("us price provider missing key provider=fmp.historical-price-full symbol=%s", symbol)
            return []
        query = urlencode({"from": start.isoformat(), "to": end.isoformat(), "apikey": key})
        url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?{query}"
        try:
            payload = _get_json(url)
        except HTTPError as exc:
            quality = "RATE_LIMITED" if exc.code == 429 else "PROVIDER_ERROR"
            self._last_quality[("US", symbol)] = quality
            logger.warning("us price provider http error provider=fmp.historical-price-full symbol=%s status=%s quality=%s", symbol, exc.code, quality)
            return []
        except (URLError, TimeoutError) as exc:
            self._last_quality[("US", symbol)] = "PROVIDER_ERROR"
            logger.warning("us price provider network error provider=fmp.historical-price-full symbol=%s reason=%s", symbol, exc)
            return []
        except Exception as exc:
            self._last_quality[("US", symbol)] = "PROVIDER_ERROR"
            logger.warning("us price provider error provider=fmp.historical-price-full symbol=%s reason=%s", symbol, exc)
            return []
        rows = payload.get("historical") or []
        points = []
        for row in rows:
            if row.get("close") is not None and row.get("date"):
                points.append(PricePoint(_parse_day(row["date"]), float(row["close"])))
        return sorted(points, key=lambda point: point.trade_date)

    def _yfinance_prices(self, symbol: str, start: date, end: date) -> list[PricePoint]:
        try:
            import yfinance as yf
        except ImportError as exc:
            self._last_quality[("US", symbol)] = "PROVIDER_ERROR"
            logger.warning("us price provider missing dependency provider=yfinance.download symbol=%s reason=%s", symbol, exc)
            return []
        try:
            frame = yf.download(
                symbol,
                start=start.isoformat(),
                end=(end + timedelta(days=1)).isoformat(),
                interval="1d",
                auto_adjust=False,
                progress=False,
                threads=False,
            )
        except Exception as exc:
            self._last_quality[("US", symbol)] = "PROVIDER_ERROR"
            logger.warning("us price provider error provider=yfinance.download symbol=%s reason=%s", symbol, exc)
            return []
        if frame is None or frame.empty:
            self._last_quality[("US", symbol)] = "PROVIDER_ERROR"
            logger.warning("us price provider empty frame provider=yfinance.download symbol=%s", symbol)
            return []
        close_column = _yfinance_close_column(frame)
        if close_column is None:
            self._last_quality[("US", symbol)] = "PROVIDER_ERROR"
            logger.warning("us price provider malformed frame provider=yfinance.download symbol=%s", symbol)
            return []
        points: list[PricePoint] = []
        close_data = frame[close_column]
        if getattr(close_data, "ndim", 1) == 2:
            close_data = close_data.iloc[:, 0]
        for index, close_value in close_data.dropna().items():
            day = index.date() if hasattr(index, "date") else _parse_day(str(index))
            if start <= day <= end:
                points.append(PricePoint(day, float(close_value)))
        if not points:
            self._last_quality[("US", symbol)] = "NO_PRICE_DATA"
        return sorted(points, key=lambda point: point.trade_date)

    def provider_return(
        self,
        market: str,
        symbol: str,
        event_time: str,
        horizon_days: int,
        provider: str,
    ) -> ReturnResult:
        event_day = _event_base_date(event_time, market)
        start = event_day - timedelta(days=BASE_LOOKBACK_CALENDAR_DAYS)
        end = event_day + timedelta(days=max(10, horizon_days * 4 + 7))
        if provider == "duckdb":
            prices = self._cn_local_prices(symbol, start, end)
            quality = self._last_quality.get(("CN_LOCAL", symbol), "NO_PRICE_DATA")
            return _return_from_prices(symbol, market, horizon_days, event_day, prices, "zhulong.fact_daily", empty_quality=quality)
        if provider == "tushare":
            prices = self._tushare_index_prices(symbol, start, end)
            quality = self._last_quality.get(("CN_BENCHMARK", symbol), "NO_PRICE_DATA")
            return _return_from_prices(symbol, market, horizon_days, event_day, prices, "tushare.index_daily", empty_quality=quality)
        if provider == "fmp":
            self._last_quality[("US", symbol)] = "NO_PRICE_DATA"
            prices = self._fmp_prices(symbol, start, end)
            quality = self._last_quality.get(("US", symbol), "NO_PRICE_DATA")
            return _return_from_prices(symbol, market, horizon_days, event_day, prices, "fmp.historical-price-full", empty_quality=quality)
        if provider == "yfinance":
            self._last_quality[("US", symbol)] = "NO_PRICE_DATA"
            prices = self._yfinance_prices(symbol, start, end)
            quality = self._last_quality.get(("US", symbol), "NO_PRICE_DATA")
            return _return_from_prices(symbol, market, horizon_days, event_day, prices, "yfinance.download", empty_quality=quality)
        if provider == "alpha":
            self._last_quality[("US", symbol)] = "NO_PRICE_DATA"
            prices = self._alpha_vantage_prices(symbol, start, end)
            quality = self._last_quality.get(("US", symbol), "NO_PRICE_DATA")
            return _return_from_prices(symbol, market, horizon_days, event_day, prices, "alpha_vantage.daily", empty_quality=quality)
        if provider == "twelve":
            self._last_quality[("US", symbol)] = "NO_PRICE_DATA"
            prices = self._twelve_data_prices(symbol, start, end)
            quality = self._last_quality.get(("US", symbol), "NO_PRICE_DATA")
            return _return_from_prices(symbol, market, horizon_days, event_day, prices, "twelve_data.time_series", empty_quality=quality)
        if provider == "alpaca":
            self._last_quality[("US", symbol)] = "NO_PRICE_DATA"
            prices = self._alpaca_prices(symbol, start, end)
            quality = self._last_quality.get(("US", symbol), "NO_PRICE_DATA")
            return _return_from_prices(symbol, market, horizon_days, event_day, prices, "alpaca.sip.adjusted", empty_quality=quality)
        if provider == "massive":
            self._last_quality[("US", symbol)] = "NO_PRICE_DATA"
            prices = self._massive_prices(symbol, start, end)
            quality = self._last_quality.get(("US", symbol), "NO_PRICE_DATA")
            return _return_from_prices(symbol, market, horizon_days, event_day, prices, "massive.aggs.adjusted", empty_quality=quality)
        return _empty_result(symbol, market, horizon_days, "UNSUPPORTED_PROVIDER", provider)

    def _alpaca_prices(self, symbol: str, start: date, end: date) -> list[PricePoint]:
        key = self.env.get("US_RADAR_ALPACA_API_KEY") or os.environ.get("US_RADAR_ALPACA_API_KEY")
        secret = self.env.get("US_RADAR_ALPACA_SECRET_KEY") or os.environ.get("US_RADAR_ALPACA_SECRET_KEY")
        if not key or not secret:
            self._last_quality[("US", symbol)] = "PROVIDER_ERROR"
            logger.warning("us price provider missing credentials provider=alpaca.sip.adjusted symbol=%s", symbol)
            return []
        effective_end = min(end, date.today())
        query = urlencode({
            "timeframe": "1Day",
            "start": f"{start.isoformat()}T00:00:00Z",
            "end": f"{effective_end.isoformat()}T23:59:59Z",
            "adjustment": "all",
            "feed": "sip",
            "limit": 10000,
        })
        headers = {
            "APCA-API-KEY-ID": key,
            "APCA-API-SECRET-KEY": secret,
        }
        try:
            payload = _get_json(
                f"https://data.alpaca.markets/v2/stocks/{symbol}/bars?{query}",
                headers=headers,
            )
        except HTTPError as exc:
            quality = "RATE_LIMITED" if exc.code == 429 else "PROVIDER_ERROR"
            self._last_quality[("US", symbol)] = quality
            logger.warning("us price provider http error provider=alpaca.sip.adjusted symbol=%s status=%s quality=%s", symbol, exc.code, quality)
            return []
        except (URLError, TimeoutError) as exc:
            self._last_quality[("US", symbol)] = "PROVIDER_ERROR"
            logger.warning("us price provider network error provider=alpaca.sip.adjusted symbol=%s reason=%s", symbol, exc)
            return []
        except Exception as exc:
            self._last_quality[("US", symbol)] = "PROVIDER_ERROR"
            logger.warning("us price provider error provider=alpaca.sip.adjusted symbol=%s reason=%s", symbol, exc)
            return []
        points = [
            PricePoint(_parse_day(str(row["t"])), float(row["c"]))
            for row in payload.get("bars") or []
            if row.get("t") and row.get("c") is not None
        ]
        if not points:
            self._last_quality[("US", symbol)] = "NO_PRICE_DATA"
        return sorted(points, key=lambda point: point.trade_date)

    def _massive_prices(self, symbol: str, start: date, end: date) -> list[PricePoint]:
        key = self.env.get("US_RADAR_MASSIVE_API_KEY") or os.environ.get("US_RADAR_MASSIVE_API_KEY")
        if not key:
            self._last_quality[("US", symbol)] = "PROVIDER_ERROR"
            logger.warning("us price provider missing key provider=massive.aggs.adjusted symbol=%s", symbol)
            return []
        effective_end = min(end, date.today())
        query = urlencode({"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": key})
        url = (
            f"https://api.massive.com/v2/aggs/ticker/{symbol}/range/1/day/"
            f"{start.isoformat()}/{effective_end.isoformat()}?{query}"
        )
        try:
            payload = _get_json(url)
        except HTTPError as exc:
            quality = "RATE_LIMITED" if exc.code == 429 else "PROVIDER_ERROR"
            self._last_quality[("US", symbol)] = quality
            logger.warning("us price provider http error provider=massive.aggs.adjusted symbol=%s status=%s quality=%s", symbol, exc.code, quality)
            return []
        except (URLError, TimeoutError) as exc:
            self._last_quality[("US", symbol)] = "PROVIDER_ERROR"
            logger.warning("us price provider network error provider=massive.aggs.adjusted symbol=%s reason=%s", symbol, exc)
            return []
        except Exception as exc:
            self._last_quality[("US", symbol)] = "PROVIDER_ERROR"
            logger.warning("us price provider error provider=massive.aggs.adjusted symbol=%s reason=%s", symbol, exc)
            return []
        if str(payload.get("status") or "").upper() not in {"", "OK"}:
            self._last_quality[("US", symbol)] = "PROVIDER_ERROR"
            logger.warning("us price provider message provider=massive.aggs.adjusted symbol=%s", symbol)
            return []
        points = [
            PricePoint(
                datetime.fromtimestamp(float(row["t"]) / 1000, timezone.utc).astimezone(US_EASTERN).date(),
                float(row["c"]),
            )
            for row in payload.get("results") or []
            if row.get("t") is not None and row.get("c") is not None
        ]
        if not points:
            self._last_quality[("US", symbol)] = "NO_PRICE_DATA"
        return sorted(points, key=lambda point: point.trade_date)

    def _alpha_vantage_prices(self, symbol: str, start: date, end: date) -> list[PricePoint]:
        key = self.env.get("US_RADAR_ALPHA_VANTAGE_API_KEY") or os.environ.get("US_RADAR_ALPHA_VANTAGE_API_KEY")
        if not key:
            self._last_quality[("US", symbol)] = "PROVIDER_ERROR"
            logger.warning("us price provider missing key provider=alpha_vantage.daily symbol=%s", symbol)
            return []
        query = urlencode({
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "outputsize": "compact",
            "apikey": key,
        })
        try:
            payload = _get_json(f"https://www.alphavantage.co/query?{query}")
        except HTTPError as exc:
            quality = "RATE_LIMITED" if exc.code == 429 else "PROVIDER_ERROR"
            self._last_quality[("US", symbol)] = quality
            logger.warning("us price provider http error provider=alpha_vantage.daily symbol=%s status=%s quality=%s", symbol, exc.code, quality)
            return []
        except (URLError, TimeoutError) as exc:
            self._last_quality[("US", symbol)] = "PROVIDER_ERROR"
            logger.warning("us price provider network error provider=alpha_vantage.daily symbol=%s reason=%s", symbol, exc)
            return []
        except Exception as exc:
            self._last_quality[("US", symbol)] = "PROVIDER_ERROR"
            logger.warning("us price provider error provider=alpha_vantage.daily symbol=%s reason=%s", symbol, exc)
            return []
        if "Note" in payload or "Information" in payload:
            self._last_quality[("US", symbol)] = "RATE_LIMITED" if "rate limit" in json.dumps(payload).lower() else "PROVIDER_ERROR"
            logger.warning("us price provider message provider=alpha_vantage.daily symbol=%s quality=%s", symbol, self._last_quality[("US", symbol)])
            return []
        if "Error Message" in payload:
            self._last_quality[("US", symbol)] = "PROVIDER_ERROR"
            logger.warning("us price provider error message provider=alpha_vantage.daily symbol=%s", symbol)
            return []
        series = payload.get("Time Series (Daily)") or {}
        points = []
        for day_text, row in series.items():
            day = _parse_day(day_text)
            if start <= day <= end and row.get("4. close") is not None:
                points.append(PricePoint(day, float(row["4. close"])))
        if not points:
            self._last_quality[("US", symbol)] = "NO_PRICE_DATA"
        return sorted(points, key=lambda point: point.trade_date)

    def _twelve_data_prices(self, symbol: str, start: date, end: date) -> list[PricePoint]:
        effective_end = min(end, datetime.now(timezone.utc).date())
        cache_key = self._twelve_cache_key(symbol, start, effective_end)
        cached = self._read_twelve_cache(cache_key)
        if cached is not None:
            return cached

        key = self.env.get("US_RADAR_TWELVE_DATA_API_KEY") or os.environ.get("US_RADAR_TWELVE_DATA_API_KEY")
        if not key:
            self._last_quality[("US", symbol)] = "PROVIDER_ERROR"
            logger.warning("us price provider missing key provider=twelve_data.time_series symbol=%s", symbol)
            return []
        if not self._take_twelve_budget():
            self._last_quality[("US", symbol)] = "RATE_LIMITED"
            logger.warning("us price provider local rate limit provider=twelve_data.time_series symbol=%s", symbol)
            return []

        query = urlencode(
            {
                "symbol": symbol,
                "interval": "1day",
                "start_date": start.isoformat(),
                "end_date": effective_end.isoformat(),
                "outputsize": 5000,
                "format": "JSON",
                "apikey": key,
            }
        )
        try:
            payload = _get_json(f"https://api.twelvedata.com/time_series?{query}")
        except HTTPError as exc:
            quality = "RATE_LIMITED" if exc.code == 429 else "PROVIDER_ERROR"
            self._last_quality[("US", symbol)] = quality
            logger.warning("us price provider http error provider=twelve_data.time_series symbol=%s status=%s quality=%s", symbol, exc.code, quality)
            return []
        except (URLError, TimeoutError) as exc:
            self._last_quality[("US", symbol)] = "PROVIDER_ERROR"
            logger.warning("us price provider network error provider=twelve_data.time_series symbol=%s reason=%s", symbol, exc)
            return []
        except Exception as exc:
            self._last_quality[("US", symbol)] = "PROVIDER_ERROR"
            logger.warning("us price provider error provider=twelve_data.time_series symbol=%s reason=%s", symbol, exc)
            return []

        if payload.get("status") == "error" or payload.get("code"):
            code = int(payload.get("code") or 0)
            quality = "RATE_LIMITED" if code == 429 else "PROVIDER_ERROR"
            self._last_quality[("US", symbol)] = quality
            logger.warning("us price provider message provider=twelve_data.time_series symbol=%s code=%s quality=%s", symbol, code, quality)
            return []

        points = []
        for row in payload.get("values") or []:
            day_text = row.get("datetime")
            close_value = row.get("close")
            if not day_text or close_value is None:
                continue
            day = _parse_day(str(day_text))
            if start <= day <= effective_end:
                points.append(PricePoint(day, float(close_value)))
        points = sorted(points, key=lambda point: point.trade_date)
        if not points:
            self._last_quality[("US", symbol)] = "NO_PRICE_DATA"
            return []
        self._write_twelve_cache(cache_key, points)
        return points

    def _twelve_cache_key(self, symbol: str, start: date, end: date) -> str:
        material = f"{symbol.upper()}|{start.isoformat()}|{end.isoformat()}"
        return hashlib.sha256(material.encode("utf-8")).hexdigest()

    def _read_twelve_cache(self, cache_key: str) -> list[PricePoint] | None:
        if cache_key in self._twelve_memory_cache:
            return self._twelve_memory_cache[cache_key]
        path = self._market_cache_dir / f"twelve_{cache_key}.json"
        if not path.exists():
            return None
        ttl_seconds = int(self.env.get("US_RADAR_TWELVE_CACHE_TTL_SECONDS") or os.environ.get("US_RADAR_TWELVE_CACHE_TTL_SECONDS", "43200"))
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if time.time() - float(payload["fetched_at"]) > ttl_seconds:
                return None
            points = [PricePoint(_parse_day(item["date"]), float(item["close"])) for item in payload["prices"]]
        except (KeyError, TypeError, ValueError, OSError, json.JSONDecodeError) as exc:
            logger.warning("us price cache read error provider=twelve_data.time_series path=%s reason=%s", path, exc)
            return None
        self._twelve_memory_cache[cache_key] = points
        return points

    def _write_twelve_cache(self, cache_key: str, points: list[PricePoint]) -> None:
        self._twelve_memory_cache[cache_key] = points
        self._market_cache_dir.mkdir(parents=True, exist_ok=True)
        path = self._market_cache_dir / f"twelve_{cache_key}.json"
        temp_path = path.with_suffix(".tmp")
        payload = {
            "fetched_at": time.time(),
            "prices": [{"date": point.trade_date.isoformat(), "close": point.close} for point in points],
        }
        temp_path.write_text(json.dumps(payload, ensure_ascii=True, sort_keys=True), encoding="utf-8")
        temp_path.replace(path)

    def _take_twelve_budget(self) -> bool:
        configured = int(self.env.get("US_RADAR_TWELVE_MAX_CALLS_PER_MINUTE") or os.environ.get("US_RADAR_TWELVE_MAX_CALLS_PER_MINUTE", "7"))
        limit = min(7, max(1, configured))
        try:
            import fcntl

            self._market_cache_dir.mkdir(parents=True, exist_ok=True)
            state_path = self._market_cache_dir / "twelve_rate_limit.json"
            now = time.time()
            with state_path.open("a+", encoding="utf-8") as handle:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                handle.seek(0)
                try:
                    stamps = [float(item) for item in json.load(handle)]
                except (json.JSONDecodeError, TypeError, ValueError):
                    stamps = []
                stamps = [stamp for stamp in stamps if now - stamp < 60.0]
                if len(stamps) >= limit:
                    return False
                stamps.append(now)
                handle.seek(0)
                handle.truncate()
                json.dump(stamps, handle)
                handle.flush()
                os.fsync(handle.fileno())
            return True
        except (ImportError, OSError) as exc:
            logger.warning("us price local rate-limit state unavailable provider=twelve_data.time_series reason=%s", exc)
            now = time.monotonic()
            self._twelve_call_times = [stamp for stamp in self._twelve_call_times if now - stamp < 60.0]
            if len(self._twelve_call_times) >= limit:
                return False
            self._twelve_call_times.append(now)
            return True


def median_return(values: Iterable[float]) -> float | None:
    items = sorted(values)
    if not items:
        return None
    mid = len(items) // 2
    if len(items) % 2:
        return items[mid]
    return (items[mid - 1] + items[mid]) / 2


def _return_from_prices(
    symbol: str,
    market: str,
    horizon_days: int,
    event_day: date,
    prices: list[PricePoint],
    provider: str,
    empty_quality: str = "NO_PRICE_DATA",
) -> ReturnResult:
    ordered = sorted(prices, key=lambda item: item.trade_date)
    if not ordered:
        return _empty_result(symbol, market, horizon_days, empty_quality, provider)
    base_candidates = [point for point in ordered if point.trade_date <= event_day]
    future = [point for point in ordered if point.trade_date > event_day]
    if not base_candidates:
        return _empty_result(symbol, market, horizon_days, "NO_BASE_PRICE", provider)
    if not future:
        base = base_candidates[-1]
        return ReturnResult(
            symbol=symbol,
            market=market,
            horizon_days=horizon_days,
            return_pct=None,
            base_date=base.trade_date.isoformat(),
            horizon_date=None,
            base_close=base.close,
            horizon_close=None,
            provider=provider,
            data_quality="INSUFFICIENT_FORWARD_DATA",
        )
    target_index = horizon_days - 1
    if target_index >= len(future):
        base = base_candidates[-1]
        return ReturnResult(
            symbol=symbol,
            market=market,
            horizon_days=horizon_days,
            return_pct=None,
            base_date=base.trade_date.isoformat(),
            horizon_date=None,
            base_close=base.close,
            horizon_close=None,
            provider=provider,
            data_quality="INSUFFICIENT_FORWARD_DATA",
        )
    base = base_candidates[-1]
    horizon = future[target_index]
    if base.close == 0:
        return _empty_result(symbol, market, horizon_days, "BAD_BASE_PRICE", provider)
    if market == "CN" and _has_unadjusted_corporate_action(ordered, base, horizon):
        return ReturnResult(
            symbol=symbol,
            market=market,
            horizon_days=horizon_days,
            return_pct=None,
            base_date=base.trade_date.isoformat(),
            horizon_date=horizon.trade_date.isoformat(),
            base_close=base.close,
            horizon_close=horizon.close,
            provider=provider,
            data_quality="CORPORATE_ACTION_UNADJUSTED",
        )
    return ReturnResult(
        symbol=symbol,
        market=market,
        horizon_days=horizon_days,
        return_pct=(horizon.close / base.close) - 1,
        base_date=base.trade_date.isoformat(),
        horizon_date=horizon.trade_date.isoformat(),
        base_close=base.close,
        horizon_close=horizon.close,
        provider=provider,
        data_quality="DATA_OK",
    )


def _empty_result(
    symbol: str,
    market: str,
    horizon_days: int,
    quality: str,
    provider: str = "none",
) -> ReturnResult:
    return ReturnResult(symbol, market, horizon_days, None, None, None, None, None, provider, quality)


def _has_unadjusted_corporate_action(
    ordered: list[PricePoint],
    base: PricePoint,
    horizon: PricePoint,
) -> bool:
    for previous, current in zip(ordered, ordered[1:]):
        if current.trade_date <= base.trade_date:
            continue
        if current.trade_date > horizon.trade_date:
            break
        if current.reference_close is None or previous.close == 0:
            continue
        gap = abs((current.reference_close / previous.close) - 1)
        if gap > CORPORATE_ACTION_TOLERANCE:
            return True
    return False


def _point(
    day_value: object,
    close_value: object,
    reference_close_value: object | None = None,
) -> PricePoint:
    if isinstance(day_value, datetime):
        day = day_value.date()
    elif isinstance(day_value, date):
        day = day_value
    else:
        day = _parse_day(str(day_value))
    reference_close = None if reference_close_value is None else float(reference_close_value)
    return PricePoint(day, float(close_value), reference_close)


def _event_date(event_time: str) -> date:
    return _parse_event_datetime(event_time).astimezone(timezone.utc).date()


def _event_base_date(event_time: str, market: str) -> date:
    if market == "US":
        return _us_event_base_date(event_time)
    if market == "CN":
        return _cn_event_base_date(event_time)
    return _event_date(event_time)


def _us_event_base_date(event_time: str) -> date:
    event_dt = _parse_event_datetime(event_time).astimezone(US_EASTERN)
    event_day = event_dt.date()
    if event_day.weekday() >= 5:
        return _previous_weekday(event_day)
    if event_dt.time() < US_MARKET_CLOSE:
        return _previous_weekday(event_day)
    return event_day


def _cn_event_base_date(event_time: str) -> date:
    event_dt = _parse_event_datetime(event_time).astimezone(CN_MARKET_TIMEZONE)
    event_day = event_dt.date()
    if event_day.weekday() >= 5:
        return _previous_weekday(event_day)
    if event_dt.time() < CN_MARKET_CLOSE:
        return _previous_weekday(event_day)
    return event_day


def _previous_weekday(day: date) -> date:
    previous = day - timedelta(days=1)
    while previous.weekday() >= 5:
        previous -= timedelta(days=1)
    return previous


def _parse_event_datetime(event_time: str) -> datetime:
    text = event_time.replace("Z", "+00:00")
    value = datetime.fromisoformat(text)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value


def _yfinance_close_column(frame: object) -> object | None:
    columns = getattr(frame, "columns", [])
    if "Close" in columns:
        return "Close"
    for column in columns:
        if isinstance(column, tuple) and column and column[0] == "Close":
            return column
    return None


def _parse_day(text: str) -> date:
    if "-" in text:
        return datetime.strptime(text[:10], "%Y-%m-%d").date()
    return datetime.strptime(text[:8], "%Y%m%d").date()


def _ts_date(day: date) -> str:
    return day.strftime("%Y%m%d")


def _load_env_files(paths: list[Path]) -> dict[str, str]:
    values: dict[str, str] = {}
    for path in paths:
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, raw_value = stripped.split("=", 1)
            values[key.strip()] = raw_value.strip().strip("\"'")
    return values


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _get_json(url: str, headers: dict[str, str] | None = None) -> dict:
    request_headers = {"User-Agent": "zhulong-us-radar/0.1"}
    request_headers.update(headers or {})
    request = Request(url, headers=request_headers)
    with urlopen(request, timeout=20) as response:
        return json.loads(response.read().decode("utf-8"))
