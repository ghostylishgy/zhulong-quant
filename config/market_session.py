#!/usr/bin/env python3
"""Trading-session and realtime-quote freshness guards."""

from __future__ import annotations

from datetime import datetime


DEFAULT_MAX_QUOTE_AGE_SECONDS = 300
MAX_FUTURE_SKEW_SECONDS = 60


def is_intraday_scan_time(now: datetime | None = None) -> bool:
    current = now or datetime.now()
    minute = current.hour * 60 + current.minute
    morning = 9 * 60 + 20 <= minute <= 11 * 60 + 30
    afternoon = 13 * 60 <= minute <= 14 * 60 + 50
    return morning or afternoon


def validate_realtime_quote_time(
    quote_date,
    quote_time,
    *,
    now: datetime | None = None,
    max_age_seconds: int = DEFAULT_MAX_QUOTE_AGE_SECONDS,
) -> tuple[bool, str, str, float | None]:
    current = now or datetime.now()
    date_text = str(quote_date or '').strip()
    time_text = str(quote_time or '').strip()
    if not date_text or not time_text:
        return False, 'QUOTE_MISSING_TIMESTAMP', 'quote DATE/TIME is missing', None

    quote_day = _parse_quote_date(date_text)
    quote_clock = _parse_quote_time(time_text)
    if quote_day is None or quote_clock is None:
        return False, 'QUOTE_INVALID_TIMESTAMP', f'invalid quote timestamp: {date_text} {time_text}', None

    quote_at = datetime.combine(quote_day, quote_clock)
    age_seconds = (current - quote_at).total_seconds()
    if quote_day != current.date():
        return False, 'QUOTE_STALE_DATE', f'quote date {quote_day} != {current.date()}', age_seconds
    if age_seconds < -MAX_FUTURE_SKEW_SECONDS:
        return False, 'QUOTE_FUTURE_TIMESTAMP', f'quote timestamp is {-age_seconds:.0f}s ahead', age_seconds
    if age_seconds > max(0, int(max_age_seconds)):
        return False, 'QUOTE_STALE_TIME', f'quote age {age_seconds:.0f}s exceeds {max_age_seconds}s', age_seconds
    return True, 'QUOTE_FRESH', f'quote age {max(0.0, age_seconds):.0f}s', age_seconds


def _parse_quote_date(value: str):
    compact = ''.join(ch for ch in value if ch.isdigit())
    if len(compact) < 8:
        return None
    try:
        return datetime.strptime(compact[:8], '%Y%m%d').date()
    except ValueError:
        return None


def _parse_quote_time(value: str):
    base = value.split('.', 1)[0]
    compact = ''.join(ch for ch in base if ch.isdigit())
    if len(compact) == 4:
        compact += '00'
    if len(compact) != 6:
        return None
    try:
        return datetime.strptime(compact, '%H%M%S').time()
    except ValueError:
        return None
