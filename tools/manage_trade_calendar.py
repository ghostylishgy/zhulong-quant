#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Manage the authoritative Tushare trade-calendar cache."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, datetime
from pathlib import Path

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
ENGINE_LIB = ROOT / "01_engine" / "lib"
if str(ENGINE_LIB) not in sys.path:
    sys.path.insert(0, str(ENGINE_LIB))

from trade_calendar import (  # noqa: E402
    DEFAULT_MIN_HORIZON_DAYS,
    SCOPE_AUDIT,
    SCOPE_ENTRY,
    add_calendar_override,
    decide_trade_day,
    inspect_calendar_coverage,
    refresh_trade_calendar,
)


DEFAULT_DB = ROOT / "storage" / "database" / "zhulong.duckdb"
OVERRIDE_CONFIRMATION = "MANUAL_CALENDAR_OVERRIDE"


def _date_text(value: str) -> str:
    raw = str(value or "").strip()
    if len(raw) == 8 and raw.isdigit():
        raw = f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
    datetime.strptime(raw, "%Y-%m-%d")
    return raw


def _default_start() -> str:
    return f"{date.today().year}-01-01"


def _default_end() -> str:
    return f"{date.today().year + 1}-12-31"


def _load_token(env_file: str) -> str:
    path = Path(env_file).expanduser()
    if path.exists():
        load_dotenv(path, override=False)
    token = str(os.getenv("TUSHARE_TOKEN") or "").strip()
    if not token:
        raise RuntimeError("TUSHARE_TOKEN is not configured")
    return token


def _refresh_with_retry(api, args) -> dict:
    last_exc = None
    for attempt in range(1, 4):
        try:
            return refresh_trade_calendar(
                api,
                args.db,
                start_date=args.start,
                end_date=args.end,
            )
        except Exception as exc:
            last_exc = exc
            if attempt < 3:
                time.sleep(2 ** (attempt - 1))
    raise RuntimeError(f"trade_cal refresh failed after 3 attempts: {last_exc}")


def cmd_refresh(args) -> int:
    import tushare as ts

    token = _load_token(args.env_file)
    api = ts.pro_api(token)
    result = _refresh_with_retry(api, args)
    coverage = inspect_calendar_coverage(
        args.db,
        as_of=args.as_of,
        minimum_horizon_days=args.minimum_horizon_days,
    )
    payload = {
        "command": "refresh",
        "result": result,
        "coverage": coverage,
        "no_trade_signal": True,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 2 if coverage.get("status") in {"MISSING_TABLE", "EMPTY", "GAP", "ANCHOR_MISSING", "EXPIRED"} else 0


def cmd_status(args) -> int:
    decision = decide_trade_day(args.db, args.date, scope=args.scope)
    coverage = inspect_calendar_coverage(
        args.db,
        as_of=args.date,
        minimum_horizon_days=args.minimum_horizon_days,
    )
    payload = {
        "command": "status",
        "decision": decision.as_dict(),
        "coverage": coverage,
        "no_trade_signal": True,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if not decision.should_alert else 2


def cmd_override(args) -> int:
    if args.confirm != OVERRIDE_CONFIRMATION:
        raise RuntimeError(
            f"override requires --confirm {OVERRIDE_CONFIRMATION}"
        )
    override_id = add_calendar_override(
        args.db,
        cal_date=args.date,
        scope=args.scope,
        is_open=args.state == "open",
        reason=args.reason,
        created_by=args.created_by,
    )
    decision = decide_trade_day(args.db, args.date, scope=args.scope)
    print(
        json.dumps(
            {
                "command": "override",
                "override_id": override_id,
                "decision": decision.as_dict(),
                "warning": (
                    "AUDIT and ENTRY are independent. This override applies only "
                    f"to {args.scope}."
                ),
                "no_trade_signal": True,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Tushare trade_cal cache manager; calendar only, no trade signal."
    )
    parser.add_argument("--db", default=str(DEFAULT_DB))
    sub = parser.add_subparsers(dest="command", required=True)

    refresh = sub.add_parser("refresh", help="Atomically refresh fact_trade_calendar")
    refresh.add_argument("--start", type=_date_text, default=_default_start())
    refresh.add_argument("--end", type=_date_text, default=_default_end())
    refresh.add_argument("--as-of", type=_date_text, default=date.today().isoformat())
    refresh.add_argument("--env-file", default=str(ROOT / ".env"))
    refresh.add_argument("--minimum-horizon-days", type=int, default=DEFAULT_MIN_HORIZON_DAYS)
    refresh.set_defaults(func=cmd_refresh)

    status = sub.add_parser("status", help="Read one decision and cache coverage")
    status.add_argument("--date", type=_date_text, default=date.today().isoformat())
    status.add_argument("--scope", choices=[SCOPE_AUDIT, SCOPE_ENTRY], default=SCOPE_AUDIT)
    status.add_argument("--minimum-horizon-days", type=int, default=DEFAULT_MIN_HORIZON_DAYS)
    status.set_defaults(func=cmd_status)

    override = sub.add_parser("override", help="Append one auditable scope-specific override")
    override.add_argument("--date", type=_date_text, required=True)
    override.add_argument("--scope", choices=[SCOPE_AUDIT, SCOPE_ENTRY], required=True)
    override.add_argument("--state", choices=["open", "closed"], required=True)
    override.add_argument("--reason", required=True)
    override.add_argument("--created-by", required=True)
    override.add_argument("--confirm", required=True)
    override.set_defaults(func=cmd_override)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
