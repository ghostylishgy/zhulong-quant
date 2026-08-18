#!/usr/bin/env python3
"""Account execution eligibility with research-observation retention."""

from __future__ import annotations

import os
from dataclasses import asdict, dataclass
from typing import Any, Dict

DEFAULT_PROFILE = "PERSONAL_MAINLAND"
BJ_ENABLED_PROFILE = "BJ_ENABLED"


@dataclass(frozen=True)
class EligibilityResult:
    allow_execution: bool
    qualification_status: str
    reason_code: str
    reason_cn: str
    account_profile: str
    market: str
    is_st: bool
    observation_retained: bool = True
    evaluator_version: str = "account_eligibility_v0.1"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _market(symbol: str) -> str:
    symbol = str(symbol or "").strip().upper()
    if symbol.endswith(".BJ"):
        return "BJ"
    if symbol.endswith(".SH"):
        return "SH"
    if symbol.endswith(".SZ"):
        return "SZ"
    return "UNSUPPORTED"


def evaluate_account_eligibility(
    symbol: str,
    name: str = "",
    is_st: Any = None,
    account_profile: str = "",
) -> EligibilityResult:
    symbol = str(symbol or "").strip().upper()
    name = str(name or "").strip()
    profile = str(account_profile or os.getenv("ZHULONG_ACCOUNT_PROFILE", DEFAULT_PROFILE)).strip().upper()
    if profile not in {DEFAULT_PROFILE, BJ_ENABLED_PROFILE}:
        profile = DEFAULT_PROFILE
    market = _market(symbol)
    normalized_name = name.upper().replace(" ", "")
    st_flag = bool(is_st) or "ST" in normalized_name

    if st_flag:
        return EligibilityResult(
            False, "OBSERVE_ONLY_ST", "ACCOUNT_INELIGIBLE_ST",
            "风险警示股票保留观察，但默认账户禁止执行", profile, market, True)
    if market == "BJ" and profile != BJ_ENABLED_PROFILE:
        return EligibilityResult(
            False, "OBSERVE_ONLY_BJ", "ACCOUNT_INELIGIBLE_BJ",
            "北交所标的保留观察，但当前账户没有执行资格", profile, market, False)
    if market not in {"SH", "SZ", "BJ"}:
        return EligibilityResult(
            False, "OBSERVE_ONLY_UNSUPPORTED", "ACCOUNT_INELIGIBLE_MARKET",
            "市场不在当前账户执行范围，标的仅保留观察", profile, market, False)
    return EligibilityResult(
        True, "ELIGIBLE", "ACCOUNT_ELIGIBLE",
        "当前账户资格检查通过", profile, market, False)
