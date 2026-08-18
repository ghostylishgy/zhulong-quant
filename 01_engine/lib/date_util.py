#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Trading-date anchor utilities for engine/governance call chains."""

from __future__ import annotations

import os
import re
from datetime import date, datetime
from typing import Any, Optional


def normalize_trade_date(value: Any) -> Optional[str]:
    """Normalize to YYYY-MM-DD, return None when input is empty/invalid."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")

    text = str(value).strip()
    if not text:
        return None
    if re.fullmatch(r"\d{8}", text):
        return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text
    return None


def resolve_trade_date(explicit: Any = None) -> str:
    """
    Resolve trading date from explicit argument or env override.

    Resolution order:
    1) explicit argument
    2) TRADE_DATE
    3) ZHULONG_TRADE_DATE

    Raises ValueError when unresolved. No datetime.now() fallback by design.
    """
    for candidate in (
        explicit,
        os.getenv("TRADE_DATE", ""),
        os.getenv("ZHULONG_TRADE_DATE", ""),
    ):
        normalized = normalize_trade_date(candidate)
        if normalized:
            return normalized

    raise ValueError(
        "trade_date unresolved: set TRADE_DATE/ZHULONG_TRADE_DATE "
        "or pass explicit trade_date"
    )
