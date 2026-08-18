#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Zhulong Nexus v2.4.0 - Zeta Collector
LHB + Margin + BlockTrade collection with timeout and retry hardening.
"""

import logging
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from config.settings import Config

logger = logging.getLogger('zhulong.zeta_collector')

try:
    import tushare as ts
    TUSHARE_OK = True
except ImportError:
    TUSHARE_OK = False


# ==================== Trading Calendar ====================

KNOWN_HOLIDAYS_2026 = {
    date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3),
    date(2026, 2, 14), date(2026, 2, 15), date(2026, 2, 16),
    date(2026, 2, 17), date(2026, 2, 18), date(2026, 2, 19), date(2026, 2, 20),
    date(2026, 4, 4), date(2026, 4, 5), date(2026, 4, 6),
    date(2026, 5, 1), date(2026, 5, 2), date(2026, 5, 3), date(2026, 5, 4), date(2026, 5, 5),
    date(2026, 6, 19), date(2026, 6, 20), date(2026, 6, 21),
    date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 3),
    date(2026, 10, 4), date(2026, 10, 5), date(2026, 10, 6), date(2026, 10, 7),
}


def get_last_trade_date(ref_date=None):
    """Return the most recent usable trade date.

    Default behavior:
    - If local time >= 15:00 and today is a trade day, use today.
    - Otherwise, fall back to the latest previous trade day.
    """

    def _is_trade_day(d: date) -> bool:
        return d.weekday() < 5 and d not in KNOWN_HOLIDAYS_2026

    now_local = datetime.now()
    allow_today = False
    if ref_date is None:
        ref_day = now_local.date()
        allow_today = now_local.hour >= 15
    elif isinstance(ref_date, datetime):
        ref_day = ref_date.date()
        allow_today = ref_date.hour >= 15
    else:
        ref_day = ref_date
        allow_today = True

    if allow_today and _is_trade_day(ref_day):
        return ref_day

    candidate = ref_day - timedelta(days=1)
    for _ in range(14):
        if _is_trade_day(candidate):
            return candidate
        candidate -= timedelta(days=1)

    logger.warning(f'Unable to resolve trade date, fallback to {ref_day - timedelta(days=1)}')
    return ref_day - timedelta(days=1)


# ==================== Data Structure ====================

@dataclass
class ZetaData:
    """Single-symbol Zeta structure."""

    ts_code: str
    trade_date: date
    lhb_net: float = 0.0
    lhb_buy: float = 0.0
    lhb_sell: float = 0.0
    seat_count: int = 0
    inst_buy: int = 0
    hot_money: int = 0
    rzye: float = 0.0
    rzmre: float = 0.0
    margin_delta: float = 0.0
    block_trade_vol: float = 0.0
    block_trade_premium: float = 0.0
    data_source: str = 'tushare'
    collected_at: str = ''
    top_seat_tier: int = 3


class ZetaExternalDataError(RuntimeError):
    """Raised when external Zeta data cannot be fetched reliably."""


# ==================== Normalizer ====================

def ensure_zeta_dict(zeta_data):
    """Normalize dataclass payload to plain dict."""

    return {
        'ts_code': str(zeta_data.ts_code),
        'trade_date': zeta_data.trade_date.strftime('%Y-%m-%d')
        if isinstance(zeta_data.trade_date, date) else str(zeta_data.trade_date),
        'lhb_net': float(zeta_data.lhb_net or 0.0),
        'lhb_buy': float(zeta_data.lhb_buy or 0.0),
        'lhb_sell': float(zeta_data.lhb_sell or 0.0),
        'seat_count': int(zeta_data.seat_count or 0),
        'inst_buy': int(zeta_data.inst_buy or 0),
        'hot_money': int(zeta_data.hot_money or 0),
        'rzye': float(zeta_data.rzye or 0.0),
        'rzmre': float(zeta_data.rzmre or 0.0),
        'margin_delta': float(zeta_data.margin_delta or 0.0),
        'block_trade_vol': float(zeta_data.block_trade_vol or 0.0),
        'block_trade_premium': float(zeta_data.block_trade_premium or 0.0),
        'data_source': zeta_data.data_source,
        'collected_at': zeta_data.collected_at,
    }


# ==================== Collector ====================

class ZetaCollector:
    """Zeta collector with timeout + retry guards."""

    def __init__(self):
        self.pro = None
        self.retry_times = max(1, int(getattr(Config, 'TUSHARE_RETRY_TIMES', 3) or 3))
        self.request_timeout = max(5, int(getattr(Config, 'TUSHARE_TIMEOUT', 30) or 30))
        self.retry_backoff_base = max(
            1.0, float(getattr(Config, 'TUSHARE_RETRY_BACKOFF_BASE', 10.0) or 10.0)
        )
        self.retry_backoff_cap = max(
            self.retry_backoff_base,
            float(getattr(Config, 'TUSHARE_RETRY_BACKOFF_CAP', 60.0) or 60.0),
        )

        if TUSHARE_OK:
            token = str(getattr(Config, 'TUSHARE_TOKEN', '') or '')
            if token:
                try:
                    # tushare 1.4.x supports timeout in pro_api
                    self.pro = ts.pro_api(token=token, timeout=self.request_timeout)
                    logger.info(
                        'ZetaCollector: TuShare API connected '
                        f'(timeout={self.request_timeout}s, retries={self.retry_times})'
                    )
                except Exception as exc:
                    logger.warning(f'ZetaCollector: TuShare init fail: {exc}')

    def _backoff_seconds(self, attempt: int) -> float:
        return min(self.retry_backoff_cap, self.retry_backoff_base * (2 ** (attempt - 1)))

    def _call_tushare_with_retry(self, api_name: str, **kwargs):
        if not self.pro:
            raise ZetaExternalDataError('TuShare API unavailable')
        api = getattr(self.pro, api_name, None)
        if api is None:
            raise ZetaExternalDataError(f'TuShare API missing method: {api_name}')

        last_exc = None
        for attempt in range(1, self.retry_times + 1):
            try:
                return api(**kwargs)
            except Exception as exc:
                last_exc = exc
                if attempt >= self.retry_times:
                    break
                sleep_s = self._backoff_seconds(attempt)
                logger.warning(
                    f'{api_name} retry {attempt}/{self.retry_times} failed: {exc}; '
                    f'backoff={sleep_s:.1f}s'
                )
                time.sleep(sleep_s)

        raise ZetaExternalDataError(
            f'{api_name} failed after {self.retry_times} attempts: {last_exc}'
        )

    @staticmethod
    def _pick_col(columns: set[str], *candidates: str) -> str | None:
        for name in candidates:
            if name in columns:
                return name
        return None

    @staticmethod
    def _safe_float(value, default=0.0) -> float:
        try:
            return float(value if value is not None else default)
        except Exception:
            return float(default)

    def fetch_daily_frames(self, trade_date) -> tuple:
        """Fetch whole-market frames used by batch mode with retry + timeout."""

        if trade_date is None:
            trade_date = get_last_trade_date()
        date_str = trade_date.strftime('%Y%m%d') if isinstance(trade_date, date) else str(trade_date)

        top_df = self._call_tushare_with_retry('top_list', trade_date=date_str)
        margin_df = self._call_tushare_with_retry('margin_detail', trade_date=date_str)
        block_df = self._call_tushare_with_retry('block_trade', trade_date=date_str)
        return top_df, margin_df, block_df

    def collect(self, ts_code, trade_date=None):
        """Collect single-symbol Zeta data (strict mode: segment failure raises)."""

        if trade_date is None:
            trade_date = get_last_trade_date()
            logger.info(f'ZetaCollector: auto fetch {trade_date}')

        data = ZetaData(
            ts_code=ts_code,
            trade_date=trade_date,
            collected_at=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        )

        if not self.pro:
            raise ZetaExternalDataError(f'ZetaCollector: TuShare N/A for {ts_code}')

        date_str = trade_date.strftime('%Y%m%d')

        lhb = self._call_tushare_with_retry('top_list', trade_date=date_str, ts_code=ts_code)
        if lhb is not None and not lhb.empty:
            cols = set(lhb.columns)
            buy_col = self._pick_col(cols, 'buy_amount', 'l_buy')
            sell_col = self._pick_col(cols, 'sell_amount', 'l_sell')
            if buy_col is None or sell_col is None:
                raise ZetaExternalDataError(
                    f'top_list schema mismatch for {ts_code}: missing buy/sell columns'
                )
            data.lhb_net = self._safe_float(lhb['net_amount'].sum()) if 'net_amount' in cols else 0.0
            data.lhb_buy = self._safe_float(lhb[buy_col].sum())
            data.lhb_sell = self._safe_float(lhb[sell_col].sum())
            data.seat_count = int(len(lhb))
            if 'reason' in cols:
                data.inst_buy = int(lhb['reason'].astype(str).str.contains('\u673a\u6784', na=False).sum())
                data.hot_money = max(data.seat_count - data.inst_buy, 0)

        mgn = self._call_tushare_with_retry('margin', trade_date=date_str, ts_code=ts_code)
        if mgn is not None and not mgn.empty:
            cols = set(mgn.columns)
            data.rzye = self._safe_float(mgn['rzye'].iloc[0]) if 'rzye' in cols else 0.0
            data.rzmre = self._safe_float(mgn['rzmre'].iloc[0]) if 'rzmre' in cols else 0.0
            rzche = self._safe_float(mgn['rzche'].iloc[0]) if 'rzche' in cols else 0.0
            data.margin_delta = self._safe_float(data.rzmre - rzche)

        blk = self._call_tushare_with_retry('block_trade', trade_date=date_str, ts_code=ts_code)
        if blk is not None and not blk.empty:
            cols = set(blk.columns)
            data.block_trade_vol = self._safe_float(blk['vol'].sum()) if 'vol' in cols else 0.0
            if 'premium' in cols:
                data.block_trade_premium = self._safe_float(blk['premium'].mean())

        return data

    def collect_batch(self, ts_codes, trade_date=None):
        """Batch collect; any symbol failure is surfaced to caller."""

        if trade_date is None:
            trade_date = get_last_trade_date()
        result = {}
        for code in ts_codes:
            result[code] = self.collect(code, trade_date)
        return result
