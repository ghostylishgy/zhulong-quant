#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Centralized date normalization utilities for governance layer."""

import re
from datetime import date, datetime
from typing import Any, Optional


def normalize_date(val: Any) -> Optional[str]:
    """
    Normalize input date to YYYY-MM-DD string.
    Supports: YYYYMMDD, YYYY-MM-DD, datetime, date
    """
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.strftime("%Y-%m-%d")
    if isinstance(val, date):
        return val.strftime("%Y-%m-%d")
    if isinstance(val, str):
        val = val.strip()
        if not val:
            return None
        if re.match(r"^\d{8}$", val):
            return f"{val[:4]}-{val[4:6]}-{val[6:8]}"
        if re.match(r"^\d{4}-\d{2}-\d{2}", val):
            return val[:10]
    return str(val) if val else None
