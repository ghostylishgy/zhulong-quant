#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Authoritative A-share trading-calendar cache and fail-closed decisions."""

from __future__ import annotations

import hashlib
import json
import logging
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable, Iterable, Mapping


LIB_DIR = Path(__file__).resolve().parent
if str(LIB_DIR) not in sys.path:
    sys.path.insert(0, str(LIB_DIR))

from db_gateway import DBGateway  # noqa: E402


logger = logging.getLogger("zhulong.trade_calendar")

SCOPE_AUDIT = "AUDIT"
SCOPE_ENTRY = "ENTRY"
VALID_SCOPES = {SCOPE_AUDIT, SCOPE_ENTRY}
AUTHORITATIVE_SOURCE = "TUSHARE_TRADE_CAL"
DEFAULT_EXCHANGE = "SSE"
DEFAULT_MAX_CACHE_AGE_DAYS = 14
DEFAULT_MIN_HORIZON_DAYS = 90


@dataclass(frozen=True)
class CalendarDecision:
    cal_date: str
    scope: str
    is_open: bool
    status: str
    reason: str
    source: str | None = None
    fetched_at: str | None = None
    override_id: str | None = None

    @property
    def should_alert(self) -> bool:
        return self.status in {
            "UNKNOWN_DATE",
            "STALE_CACHE",
            "SOURCE_DISAGREEMENT",
            "CACHE_ERROR",
            "INVALID_DATE",
        }

    def as_dict(self) -> dict:
        payload = asdict(self)
        payload["should_alert"] = self.should_alert
        return payload


def _coerce_date(value: date | datetime | str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raw = str(value or "").strip()
    if len(raw) == 8 and raw.isdigit():
        raw = f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
    return datetime.strptime(raw, "%Y-%m-%d").date()


def _scope(value: str) -> str:
    normalized = str(value or "").strip().upper()
    if normalized not in VALID_SCOPES:
        raise ValueError(f"unsupported calendar scope: {value!r}")
    return normalized


def _timestamp_text(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat(timespec="seconds")
    return str(value)


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = ?
        LIMIT 1
        """,
        [table_name],
    ).fetchone()
    return bool(row)


def _create_schema(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_trade_calendar (
            exchange VARCHAR NOT NULL,
            cal_date DATE NOT NULL,
            is_open BOOLEAN NOT NULL,
            pretrade_date DATE,
            source VARCHAR NOT NULL,
            fetched_at TIMESTAMP NOT NULL,
            payload_sha256 VARCHAR NOT NULL,
            PRIMARY KEY (exchange, cal_date)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ops_trade_calendar_override (
            override_id VARCHAR PRIMARY KEY,
            cal_date DATE NOT NULL,
            scope VARCHAR NOT NULL CHECK (scope IN ('AUDIT', 'ENTRY')),
            is_open BOOLEAN NOT NULL,
            reason VARCHAR NOT NULL,
            created_by VARCHAR NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
        """
    )


def ensure_calendar_schema(db_path: str | Path) -> None:
    with DBGateway(db_path, read_only=False, logger=logger) as conn:
        _create_schema(conn)


def _canonical_rows(frame, exchange: str = DEFAULT_EXCHANGE) -> list[dict]:
    if frame is None:
        raise ValueError("trade_cal returned no payload")
    if hasattr(frame, "to_dict"):
        raw_rows = frame.to_dict("records")
    elif isinstance(frame, Iterable):
        raw_rows = list(frame)
    else:
        raise TypeError("trade_cal payload must be a DataFrame or iterable of mappings")
    if not raw_rows:
        raise ValueError("trade_cal returned an empty payload")

    normalized: dict[str, dict] = {}
    for raw in raw_rows:
        if not isinstance(raw, Mapping):
            raise TypeError("trade_cal row is not a mapping")
        row_exchange = str(raw.get("exchange") or exchange).strip().upper()
        cal_date = _coerce_date(raw.get("cal_date"))
        is_open_raw = raw.get("is_open")
        if str(is_open_raw).strip() not in {"0", "1", "False", "True", "false", "true"}:
            raise ValueError(f"invalid is_open for {cal_date}: {is_open_raw!r}")
        is_open = str(is_open_raw).strip().lower() in {"1", "true"}
        pretrade_raw = raw.get("pretrade_date")
        pretrade_date = _coerce_date(pretrade_raw) if str(pretrade_raw or "").strip() else None
        key = cal_date.isoformat()
        candidate = {
            "exchange": row_exchange,
            "cal_date": cal_date,
            "is_open": is_open,
            "pretrade_date": pretrade_date,
        }
        if key in normalized and normalized[key] != candidate:
            raise ValueError(f"conflicting duplicate trade_cal row: {key}")
        normalized[key] = candidate

    rows = sorted(normalized.values(), key=lambda item: item["cal_date"])
    first = rows[0]["cal_date"]
    last = rows[-1]["cal_date"]
    expected_count = (last - first).days + 1
    if len(rows) != expected_count:
        present = {row["cal_date"] for row in rows}
        cursor = first
        while cursor <= last:
            if cursor not in present:
                raise ValueError(f"trade_cal coverage gap: {cursor.isoformat()}")
            cursor += timedelta(days=1)
    return rows


def _payload_sha256(rows: list[dict]) -> str:
    canonical = [
        {
            "exchange": row["exchange"],
            "cal_date": row["cal_date"].isoformat(),
            "is_open": row["is_open"],
            "pretrade_date": row["pretrade_date"].isoformat() if row["pretrade_date"] else None,
        }
        for row in rows
    ]
    raw = json.dumps(canonical, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def replace_calendar_rows(
    db_path: str | Path,
    rows,
    *,
    source: str = AUTHORITATIVE_SOURCE,
    fetched_at: datetime | None = None,
    requested_start: date | datetime | str | None = None,
) -> dict:
    canonical = _canonical_rows(rows)
    first = canonical[0]["cal_date"]
    last = canonical[-1]["cal_date"]
    if requested_start is not None and first != _coerce_date(requested_start):
        raise ValueError(
            f"trade_cal response starts at {first.isoformat()}, expected {_coerce_date(requested_start).isoformat()}"
        )
    stamp = fetched_at or datetime.now()
    payload_sha = _payload_sha256(canonical)
    exchange = canonical[0]["exchange"]
    if any(row["exchange"] != exchange for row in canonical):
        raise ValueError("mixed exchanges in one calendar refresh are not supported")

    with DBGateway(db_path, read_only=False, logger=logger) as conn:
        conn.execute("BEGIN TRANSACTION")
        try:
            _create_schema(conn)
            conn.execute(
                "DELETE FROM fact_trade_calendar WHERE exchange = ? AND cal_date BETWEEN ? AND ?",
                [exchange, first, last],
            )
            for row in canonical:
                conn.execute(
                    """
                    INSERT INTO fact_trade_calendar (
                        exchange, cal_date, is_open, pretrade_date,
                        source, fetched_at, payload_sha256
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        row["exchange"],
                        row["cal_date"],
                        row["is_open"],
                        row["pretrade_date"],
                        source,
                        stamp,
                        payload_sha,
                    ],
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    return {
        "rows_written": len(canonical),
        "exchange": exchange,
        "min_date": first.isoformat(),
        "max_date": last.isoformat(),
        "open_days": sum(1 for row in canonical if row["is_open"]),
        "closed_days": sum(1 for row in canonical if not row["is_open"]),
        "payload_sha256": payload_sha,
        "fetched_at": stamp.isoformat(timespec="seconds"),
    }


def refresh_trade_calendar(
    api,
    db_path: str | Path,
    *,
    start_date: date | datetime | str,
    end_date: date | datetime | str,
    exchange: str = DEFAULT_EXCHANGE,
    fetched_at: datetime | None = None,
) -> dict:
    start = _coerce_date(start_date)
    end = _coerce_date(end_date)
    if end < start:
        raise ValueError("end_date is before start_date")
    frame = api.trade_cal(
        exchange=exchange,
        start_date=start.strftime("%Y%m%d"),
        end_date=end.strftime("%Y%m%d"),
        fields="exchange,cal_date,is_open,pretrade_date",
    )
    result = replace_calendar_rows(
        db_path,
        frame,
        fetched_at=fetched_at,
        requested_start=start,
    )
    result["requested_start"] = start.isoformat()
    result["requested_end"] = end.isoformat()
    warnings = []
    if _coerce_date(result["max_date"]) < end:
        warnings.append(
            f"SOURCE_HORIZON_TRUNCATED:{result['max_date']}<{end.isoformat()}"
        )
    result["warnings"] = warnings
    return result


def add_calendar_override(
    db_path: str | Path,
    *,
    cal_date: date | datetime | str,
    scope: str,
    is_open: bool,
    reason: str,
    created_by: str,
    created_at: datetime | None = None,
) -> str:
    target = _coerce_date(cal_date)
    scope_norm = _scope(scope)
    reason_clean = str(reason or "").strip()
    actor = str(created_by or "").strip()
    if not reason_clean:
        raise ValueError("override reason is required")
    if not actor:
        raise ValueError("override created_by is required")
    if target.weekday() >= 5 and bool(is_open):
        raise ValueError("weekend cannot be opened by calendar override")
    stamp = created_at or datetime.now()
    identity = "|".join(
        [target.isoformat(), scope_norm, str(bool(is_open)), reason_clean, actor, stamp.isoformat()]
    )
    override_id = "TCAL-" + hashlib.sha256(identity.encode("utf-8")).hexdigest()[:20]
    with DBGateway(db_path, read_only=False, logger=logger) as conn:
        _create_schema(conn)
        conn.execute(
            """
            INSERT INTO ops_trade_calendar_override (
                override_id, cal_date, scope, is_open, reason, created_by, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [override_id, target, scope_norm, bool(is_open), reason_clean, actor, stamp],
        )
    return override_id


def _public_holiday_state(target: date) -> bool | None:
    try:
        from chinese_calendar import is_holiday

        return bool(is_holiday(target))
    except Exception:
        return None


def decide_trade_day(
    db_path: str | Path,
    cal_date: date | datetime | str,
    *,
    scope: str = SCOPE_AUDIT,
    exchange: str = DEFAULT_EXCHANGE,
    now: datetime | None = None,
    max_cache_age_days: int = DEFAULT_MAX_CACHE_AGE_DAYS,
    public_holiday_checker: Callable[[date], bool | None] | None = None,
) -> CalendarDecision:
    scope_norm = _scope(scope)
    try:
        target = _coerce_date(cal_date)
    except Exception as exc:
        return CalendarDecision(
            cal_date=str(cal_date),
            scope=scope_norm,
            is_open=False,
            status="INVALID_DATE",
            reason=f"invalid calendar date: {exc}",
        )

    if target.weekday() >= 5:
        return CalendarDecision(
            cal_date=target.isoformat(),
            scope=scope_norm,
            is_open=False,
            status="WEEKEND_CLOSED",
            reason="weekend is hard-closed and cannot be opened by override",
            source="WEEKEND_RULE",
        )

    try:
        with DBGateway(db_path, read_only=True, logger=logger) as conn:
            fact_exists = _table_exists(conn, "fact_trade_calendar")
            override_exists = _table_exists(conn, "ops_trade_calendar_override")
            fact_row = None
            if fact_exists:
                fact_row = conn.execute(
                    """
                    SELECT is_open, source, fetched_at
                    FROM fact_trade_calendar
                    WHERE exchange = ? AND cal_date = ?
                    LIMIT 1
                    """,
                    [exchange, target],
                ).fetchone()
            override_row = None
            if override_exists:
                override_row = conn.execute(
                    """
                    SELECT override_id, is_open, reason, created_by, created_at
                    FROM ops_trade_calendar_override
                    WHERE cal_date = ? AND scope = ?
                    ORDER BY created_at DESC, override_id DESC
                    LIMIT 1
                    """,
                    [target, scope_norm],
                ).fetchone()
    except Exception as exc:
        return CalendarDecision(
            cal_date=target.isoformat(),
            scope=scope_norm,
            is_open=False,
            status="CACHE_ERROR",
            reason=f"calendar cache read failed: {exc}",
        )

    if fact_row is None:
        base = CalendarDecision(
            cal_date=target.isoformat(),
            scope=scope_norm,
            is_open=False,
            status="UNKNOWN_DATE",
            reason="authoritative trade_cal cache has no row for this date",
        )
    else:
        cached_open = bool(fact_row[0])
        source = str(fact_row[1] or AUTHORITATIVE_SOURCE)
        fetched_at = fact_row[2]
        stamp = now or datetime.now()
        age_days = max(0, (stamp.date() - fetched_at.date()).days) if fetched_at else 999999
        if age_days > max_cache_age_days:
            base = CalendarDecision(
                cal_date=target.isoformat(),
                scope=scope_norm,
                is_open=False,
                status="STALE_CACHE",
                reason=f"calendar cache age {age_days}d exceeds {max_cache_age_days}d",
                source=source,
                fetched_at=_timestamp_text(fetched_at),
            )
        else:
            checker = public_holiday_checker or _public_holiday_state
            holiday_state = checker(target)
            secondary_open = None if holiday_state is None else not bool(holiday_state)
            if secondary_open is not None and secondary_open != cached_open:
                base = CalendarDecision(
                    cal_date=target.isoformat(),
                    scope=scope_norm,
                    is_open=False,
                    status="SOURCE_DISAGREEMENT",
                    reason=(
                        f"Tushare says {'open' if cached_open else 'closed'} but "
                        f"public-holiday calendar says {'open' if secondary_open else 'closed'}"
                    ),
                    source=source,
                    fetched_at=_timestamp_text(fetched_at),
                )
            else:
                base = CalendarDecision(
                    cal_date=target.isoformat(),
                    scope=scope_norm,
                    is_open=cached_open,
                    status="OPEN" if cached_open else "CLOSED",
                    reason="authoritative Tushare trade_cal cache decision",
                    source=source,
                    fetched_at=_timestamp_text(fetched_at),
                )

    if override_row is None:
        return base

    override_id, override_open, override_reason, created_by, created_at = override_row
    return CalendarDecision(
        cal_date=target.isoformat(),
        scope=scope_norm,
        is_open=bool(override_open),
        status="MANUAL_OVERRIDE_OPEN" if bool(override_open) else "MANUAL_OVERRIDE_CLOSED",
        reason=(
            f"manual {scope_norm} override by {created_by}: {override_reason}; "
            f"base_status={base.status}"
        ),
        source="MANUAL_OVERRIDE",
        fetched_at=_timestamp_text(created_at),
        override_id=str(override_id),
    )


def inspect_calendar_coverage(
    db_path: str | Path,
    *,
    as_of: date | datetime | str | None = None,
    exchange: str = DEFAULT_EXCHANGE,
    minimum_horizon_days: int = DEFAULT_MIN_HORIZON_DAYS,
) -> dict:
    anchor = _coerce_date(as_of or date.today())
    try:
        with DBGateway(db_path, read_only=True, logger=logger) as conn:
            if not _table_exists(conn, "fact_trade_calendar"):
                return {
                    "status": "MISSING_TABLE",
                    "as_of": anchor.isoformat(),
                    "exchange": exchange,
                    "should_alert": True,
                }
            row = conn.execute(
                """
                WITH scoped AS (
                    SELECT * FROM fact_trade_calendar WHERE exchange = ?
                ), latest_batch AS (
                    SELECT MAX(fetched_at) AS fetched_at FROM scoped
                )
                SELECT
                    MIN(cal_date), MAX(cal_date), COUNT(*),
                    date_diff('day', MIN(cal_date), MAX(cal_date)) + 1 - COUNT(*),
                    MAX(fetched_at),
                    MAX(cal_date) FILTER (
                        WHERE fetched_at = (SELECT fetched_at FROM latest_batch)
                    ),
                    COUNT(*) FILTER (WHERE cal_date = ?)
                FROM scoped
                """,
                [exchange, anchor],
            ).fetchone()
    except Exception as exc:
        return {
            "status": "CACHE_ERROR",
            "as_of": anchor.isoformat(),
            "exchange": exchange,
            "reason": str(exc),
            "should_alert": True,
        }

    if not row or row[0] is None:
        return {
            "status": "EMPTY",
            "as_of": anchor.isoformat(),
            "exchange": exchange,
            "should_alert": True,
        }
    min_date, max_date, row_count, gap_count, latest_fetch, fresh_max, contains_anchor = row
    effective_max = fresh_max or max_date
    horizon_days = (effective_max - anchor).days
    if gap_count:
        status = "GAP"
    elif not contains_anchor:
        status = "ANCHOR_MISSING"
    elif horizon_days < 0:
        status = "EXPIRED"
    elif horizon_days < minimum_horizon_days:
        status = "HORIZON_SHORT"
    else:
        status = "OK"
    return {
        "status": status,
        "as_of": anchor.isoformat(),
        "exchange": exchange,
        "min_date": min_date.isoformat(),
        "max_date": max_date.isoformat(),
        "fresh_max_date": effective_max.isoformat(),
        "row_count": int(row_count),
        "gap_count": int(gap_count),
        "horizon_days": int(horizon_days),
        "minimum_horizon_days": int(minimum_horizon_days),
        "contains_as_of": bool(contains_anchor),
        "latest_fetched_at": _timestamp_text(latest_fetch),
        "should_alert": status != "OK",
    }
