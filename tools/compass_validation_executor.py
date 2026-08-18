#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Execute reviewed Compass validation tasks as read-only evidence snapshots."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
ENGINE_LIB = ROOT / "01_engine" / "lib"
for location in (ROOT, ENGINE_LIB):
    if str(location) not in sys.path:
        sys.path.insert(0, str(location))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / "config" / ".env")
    load_dotenv(ROOT / ".env")
except Exception:
    pass

from db_gateway import DBGateway
from tushare_bridge import get_bridge

DB_PATH = str(ROOT / "storage" / "database" / "zhulong.duckdb")
OUT_DIR = ROOT / "storage" / "reports" / "compass_ingest"
BEIJING_TZ = timezone(timedelta(hours=8))
INPUT_VERSION = "compass_reviewed_validation_tasks_v0.3"
OUTPUT_VERSION = "compass_readonly_validation_results_v0.2"
MAX_TASKS_PER_RUN = 30
REQUIRED_TASK_GATES = {
    "reviewed_artifact_sha256_matches",
    "source_is_human_reviewed_theme_discovery",
    "market_is_a_share",
    "ticker_manual_resolved",
    "business_relevance_confirmed",
    "immutable_mapping_evidence_bound",
    "evidence_medium_or_strong",
    "dry_run_preview_reviewed",
    "manual_review_required_false",
    "validation_as_of_bound_to_review_manifest",
    "no_trade_signal_true",
}
REQUIRED_READONLY_MODULES = {
    "fact_stock_basic", "fact_daily", "fact_rps_results",
    "fact_zeta_signals", "fact_quantile_snapshot",
    "financial_snapshots_readonly",
}
ELIGIBLE_EVIDENCE_LEVELS = {"medium", "strong"}
ELIGIBLE_MAPPING_EVIDENCE_LEVELS = {"L1", "L2", "L3"}
REQUIRED_REVIEW_ASSERTIONS = {
    "source_is_human_reviewed_theme_discovery": True,
    "ticker_manual_resolved": True,
    "business_relevance_confirmed": True,
    "immutable_mapping_evidence_bound": True,
    "market_metrics_not_used_as_business_evidence": True,
    "dry_run_preview_reviewed": True,
    "manual_review_required": False,
}
REQUIRED_BLOCKS = {
    "trade", "write_shadow", "write_rag_memory", "write_nexus_audits",
    "write_duckdb", "trigger_daemon", "call_decision_engine",
    "call_nexus_run", "auto_promote_to_trade_candidate",
}
BLOCKED_ACTIONS = sorted(REQUIRED_BLOCKS | {
    "change_l4_verdict", "generate_trade_signal", "execute_shadow_order",
    "write_validation_result_to_database",
})
STRUCTURED_TERMS = {
    "股价", "涨幅", "换手", "估值", "分位", "市盈率", "市净率", "rps",
    "财务", "现金流", "应收", "存货", "毛利", "roe", "负债", "利润", "营收",
}
EXTERNAL_TERMS = {
    "业务占比", "订单", "客户", "认证", "产能", "中标", "合同", "公告",
    "政策", "地缘", "监管", "授权", "资质", "商业化", "量产", "招股书",
}


def clean(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).strip())


def safe_batch(value: Any) -> str:
    return re.sub(r"[^0-9A-Za-zW_-]", "", clean(value) or "batch") or "batch"


def now_iso() -> str:
    return datetime.now(BEIJING_TZ).isoformat(timespec="seconds")


def sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def canonical_sha256(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return sha256_bytes(raw)


def safe_number(value: Any) -> float | int | None:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return int(number) if number.is_integer() else round(number, 6)


def json_value(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        try:
            value = value.item()
        except (TypeError, ValueError):
            pass
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (int, float)):
        return safe_number(value)
    return value


def make_record(columns: list[str], row: Any) -> dict[str, Any] | None:
    if row is None:
        return None
    return {columns[index]: json_value(value) for index, value in enumerate(row)}


def percentile(values: list[Any], current: Any) -> float | None:
    current_value = safe_number(current)
    numbers = [safe_number(value) for value in values]
    numbers = [float(value) for value in numbers if value is not None and float(value) > 0]
    if current_value is None or not numbers:
        return None
    count = sum(value <= float(current_value) for value in numbers)
    return round(count / len(numbers) * 100, 2)


def validate_timestamp(value: Any, field: str) -> None:
    text = clean(value)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} must be timezone-aware ISO-8601") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field} must be timezone-aware ISO-8601")


def validate_date(value: Any, field: str) -> str:
    text = clean(value)
    try:
        datetime.strptime(text, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"{field} must be YYYY-MM-DD") from exc
    return text


def resolve_validation_as_of(payload: dict[str, Any], requested: Any = None) -> str:
    approved = validate_date(payload.get("validation_as_of"), "validation_as_of")
    requested_text = clean(requested)
    if requested_text:
        requested_text = validate_date(requested_text, "as_of")
        if requested_text != approved:
            raise ValueError("as_of must match task artifact validation_as_of")
    return approved


def require_sha256(value: Any, field: str) -> str:
    digest = clean(value).lower()
    if not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ValueError(f"{field} must be 64 lowercase hex")
    return digest


def validate_task_artifact(
    payload: dict[str, Any], actual_sha256: str, expected_sha256: str
) -> list[dict[str, Any]]:
    expected = clean(expected_sha256).lower()
    if not re.fullmatch(r"[0-9a-f]{64}", expected):
        raise ValueError("expected task artifact SHA256 must be 64 lowercase hex")
    if actual_sha256 != expected:
        raise ValueError("task artifact SHA256 mismatch")
    if payload.get("protocol_version") != INPUT_VERSION:
        raise ValueError(f"unsupported task protocol: {payload.get('protocol_version')}")
    if payload.get("mode") != "dry_run" or payload.get("dry_run") is not True:
        raise ValueError("task artifact must be dry_run")
    if payload.get("no_trade_signal") is not True:
        raise ValueError("task artifact must preserve no_trade_signal=true")
    if not REQUIRED_BLOCKS.issubset(set(payload.get("blocked_actions") or [])):
        raise ValueError("task artifact lost required blocked actions")
    require_sha256(
        payload.get("source_reviewed_artifact_sha256"),
        "source_reviewed_artifact_sha256",
    )
    require_sha256(payload.get("source_preview_sha256"), "source_preview_sha256")
    require_sha256(
        payload.get("review_manifest_sha256"), "review_manifest_sha256"
    )
    source_as_of = validate_date(
        payload.get("compass_source_as_of"), "compass_source_as_of"
    )
    validation_as_of = resolve_validation_as_of(payload)
    if validation_as_of < source_as_of:
        raise ValueError("validation_as_of cannot precede compass_source_as_of")

    tasks = payload.get("validation_tasks") or []
    if int((payload.get("stats") or {}).get("generated_tasks") or 0) != len(tasks):
        raise ValueError("generated task count does not match payload")
    if len(tasks) > MAX_TASKS_PER_RUN:
        raise ValueError(f"task count exceeds safety limit: {len(tasks)}")
    seen = set()
    for task in tasks:
        task_id = clean(task.get("task_id"))
        symbol = clean(task.get("symbol"))
        checks = {
            "task_id": bool(task_id) and task_id not in seen,
            "task_type": task.get("task_type") == "stock_validation",
            "origin": task.get("origin") == "human_reviewed_theme_discovery",
            "execution_status": (
                task.get("execution_status") == "NOT_EXECUTED_DRY_RUN_ARTIFACT"
            ),
            "symbol": bool(re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", symbol)),
            "market": task.get("market") == "A股",
            "no_trade_signal": task.get("no_trade_signal") is True,
            "reviewer_id": bool(clean(task.get("reviewer_id"))),
            "review_assertions": all(
                (task.get("review_assertions") or {}).get(key) is value
                for key, value in REQUIRED_REVIEW_ASSERTIONS.items()
            ),
            "evidence_levels": bool(task.get("evidence_levels")) and all(
                clean(value).lower() in ELIGIBLE_EVIDENCE_LEVELS
                for value in task.get("evidence_levels") or []
            ),
            "mapping_evidence_levels": bool(task.get("mapping_evidence_levels")) and all(
                clean(value) in ELIGIBLE_MAPPING_EVIDENCE_LEVELS
                for value in task.get("mapping_evidence_levels") or []
            ),
            "supporting_evidence_items": bool(task.get("supporting_evidence_items")),
            "no_market_metric_evidence": not bool(task.get("supporting_metrics")),
            "reviewed_candidates": bool(
                task.get("source_reviewed_candidate_ids")
            ),
            "candidate_hashes": bool(
                task.get("source_reviewed_candidate_sha256")
            ) and all(
                bool(re.fullmatch(r"[0-9a-f]{64}", clean(value).lower()))
                for value in task.get("source_reviewed_candidate_sha256") or []
            ),
            "gate_checks": REQUIRED_TASK_GATES.issubset(
                set(task.get("gate_checks") or [])
            ),
            "readonly_modules": REQUIRED_READONLY_MODULES.issubset(
                set(task.get("required_readonly_modules") or [])
            ),
            "blocked_actions": REQUIRED_BLOCKS.issubset(
                set(task.get("blocked_actions") or [])
            ),
            "compass_source_as_of": clean(task.get("compass_source_as_of")) == source_as_of,
            "validation_as_of": clean(task.get("validation_as_of")) == validation_as_of,
        }
        failed = sorted(name for name, passed in checks.items() if not passed)
        if failed:
            raise ValueError(f"unsafe validation task {task_id}: {','.join(failed)}")
        evidence_items = task.get("evidence_items") or []
        evidence_by_id = {
            clean(evidence.get("evidence_item_id")): evidence
            for evidence in evidence_items
            if clean(evidence.get("evidence_item_id"))
        }
        if not evidence_by_id or len(evidence_by_id) != len(evidence_items):
            raise ValueError(f"missing or duplicate immutable evidence item id: {task_id}")
        if any(
            clean(evidence.get("evidence_item_id")) not in evidence_by_id
            or canonical_sha256(evidence) != canonical_sha256(
                evidence_by_id[clean(evidence.get("evidence_item_id"))]
            )
            for evidence in task.get("supporting_evidence_items") or []
        ):
            raise ValueError(f"supporting evidence does not match immutable evidence items: {task_id}")
        validate_timestamp(task.get("reviewed_at"), f"{task_id}.reviewed_at")
        if len(task.get("source_reviewed_candidate_ids") or []) != len(
            task.get("source_reviewed_candidate_sha256") or []
        ):
            raise ValueError(f"candidate lineage length mismatch: {task_id}")
        seen.add(task_id)
    return tasks


def collect_local_snapshot(
    conn: Any, symbol: str, as_of: str
) -> tuple[dict[str, Any], list[str]]:
    warnings = []
    basic = make_record(
        ["symbol", "name", "industry", "market", "list_date", "is_st", "updated_at"],
        conn.execute(
            """
            SELECT symbol, name, industry, market, list_date, is_st, updated_at
            FROM fact_stock_basic WHERE symbol = ?
            """,
            [symbol],
        ).fetchone(),
    )
    if basic is None:
        warnings.append("fact_stock_basic_missing")

    daily_rows = conn.execute(
        """
        SELECT trade_date, open, high, low, close, pre_close, pct_chg, vol,
               amount, turnover_rate, ma20, vol_ma5
        FROM fact_daily
        WHERE symbol = ? AND trade_date <= CAST(? AS DATE)
        ORDER BY trade_date DESC LIMIT 121
        """,
        [symbol, as_of],
    ).fetchall()
    daily = None
    path_metrics: dict[str, Any] = {}
    if daily_rows:
        daily = make_record(
            ["trade_date", "open", "high", "low", "close", "pre_close", "pct_chg",
             "vol", "amount", "turnover_rate", "ma20", "vol_ma5"],
            daily_rows[0],
        )
        latest_close = safe_number(daily_rows[0][4])
        for horizon in (20, 60, 120):
            metric = None
            if len(daily_rows) > horizon and latest_close is not None:
                old_close = safe_number(daily_rows[horizon][4])
                if old_close not in (None, 0):
                    metric = round((float(latest_close) / float(old_close) - 1) * 100, 4)
            path_metrics[f"return_{horizon}d_pct"] = metric
        ma20 = safe_number(daily_rows[0][10])
        path_metrics["distance_from_ma20_pct"] = (
            round((float(latest_close) / float(ma20) - 1) * 100, 4)
            if latest_close not in (None, 0) and ma20 not in (None, 0)
            else None
        )
        path_metrics["turnover_120d_percentile"] = percentile(
            [row[9] for row in daily_rows], daily_rows[0][9]
        )
    else:
        warnings.append("fact_daily_missing")

    rps = make_record(
        ["trade_date", "rps_10", "rps_20", "rps_50", "rps_120", "rps_250"],
        conn.execute(
            """
            SELECT trade_date, rps_10, rps_20, rps_50, rps_120, rps_250
            FROM fact_rps_results
            WHERE symbol = ? AND trade_date <= CAST(? AS DATE)
            ORDER BY trade_date DESC LIMIT 1
            """,
            [symbol, as_of],
        ).fetchone(),
    )
    if rps is None:
        warnings.append("fact_rps_results_missing")

    zeta = make_record(
        ["trade_date", "lhb_net", "lhb_buy", "lhb_sell", "seat_count", "inst_buy",
         "hot_money", "rzye", "rzmre", "margin_delta", "block_trade_vol",
         "block_trade_premium", "data_source", "collected_at"],
        conn.execute(
            """
            SELECT trade_date, lhb_net, lhb_buy, lhb_sell, seat_count, inst_buy,
                   hot_money, rzye, rzmre, margin_delta, block_trade_vol,
                   block_trade_premium, data_source, collected_at
            FROM fact_zeta_signals
            WHERE ts_code = ? AND trade_date <= CAST(? AS DATE)
            ORDER BY trade_date DESC LIMIT 1
            """,
            [symbol, as_of],
        ).fetchone(),
    )
    if zeta is None:
        warnings.append("fact_zeta_signals_missing")

    quantile = make_record(
        ["trade_date", "rps_p85", "rps_p90", "rps_p95", "vol_median", "vol_p90"],
        conn.execute(
            """
            SELECT trade_date, rps_p85, rps_p90, rps_p95, vol_median, vol_p90
            FROM fact_quantile_snapshot
            WHERE trade_date <= CAST(? AS DATE)
            ORDER BY trade_date DESC LIMIT 1
            """,
            [as_of],
        ).fetchone(),
    )
    return {
        "stock_basic": basic,
        "latest_daily": daily,
        "path_metrics": path_metrics,
        "latest_rps": rps,
        "latest_zeta": zeta,
        "market_quantiles": quantile,
        "point_in_time_scope": {
            "fact_daily": "trade_date_lte_as_of",
            "fact_rps_results": "trade_date_lte_as_of",
            "fact_quantile_snapshot": "trade_date_lte_as_of",
            "fact_zeta_signals": (
                "trade_date_lte_as_of_ingestion_time_not_reconstructed"
            ),
            "fact_stock_basic": "current_snapshot_not_historical",
        },
    }, warnings


def compact_date(value: Any) -> str:
    digits = re.sub(r"[^0-9]", "", clean(value))
    return digits[:8] if len(digits) >= 8 else ""


def normalize_frame(
    frame: pd.DataFrame | None, date_field: str, as_of: str
) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    output = frame.copy()
    if date_field in output.columns:
        output[date_field] = output[date_field].map(compact_date)
        output = output[
            output[date_field].str.fullmatch(r"\d{8}", na=False)
            & (output[date_field] <= as_of.replace("-", ""))
        ]
        output = output.sort_values(date_field, ascending=False)
    return output


def normalize_disclosure_frame(
    frame: pd.DataFrame | None, as_of: str
) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    output = frame.copy()
    for field in ("ann_date", "f_ann_date", "end_date"):
        if field in output.columns:
            output[field] = output[field].map(compact_date)
    if "ann_date" not in output.columns:
        return pd.DataFrame()
    if "f_ann_date" in output.columns:
        actual = output["f_ann_date"].where(
            output["f_ann_date"].str.fullmatch(r"\d{8}", na=False),
            output["ann_date"],
        )
    else:
        actual = output["ann_date"]
    output["availability_date"] = actual
    cutoff = as_of.replace("-", "")
    output = output[
        output["availability_date"].str.fullmatch(r"\d{8}", na=False)
        & (output["availability_date"] <= cutoff)
    ]
    sort_fields = [
        field for field in ("end_date", "availability_date", "ann_date")
        if field in output.columns
    ]
    return output.sort_values(sort_fields, ascending=False)


def frame_record(frame: pd.DataFrame, index: int = 0) -> dict[str, Any] | None:
    if frame is None or frame.empty or index >= len(frame):
        return None
    return {
        str(key): json_value(value)
        for key, value in frame.iloc[index].to_dict().items()
    }


def fetch_tushare_snapshot(
    api: Any, symbol: str, as_of: str
) -> tuple[dict[str, Any], list[str]]:
    warnings = []
    as_of_compact = as_of.replace("-", "")
    start_compact = (
        datetime.fromisoformat(as_of) - timedelta(days=1095)
    ).strftime("%Y%m%d")

    def call(name: str, **kwargs: Any) -> pd.DataFrame:
        try:
            frame = getattr(api, name)(**kwargs)
            return frame if frame is not None else pd.DataFrame()
        except Exception as exc:
            warnings.append(
                f"{name}_failed:{type(exc).__name__}:{clean(exc)[:160]}"
            )
            return pd.DataFrame()

    daily_basic = normalize_frame(
        call(
            "daily_basic",
            ts_code=symbol,
            start_date=start_compact,
            end_date=as_of_compact,
            fields=(
                "ts_code,trade_date,close,turnover_rate,pe_ttm,pb,ps_ttm,"
                "dv_ttm,total_mv,circ_mv"
            ),
        ),
        "trade_date",
        as_of,
    )
    valuation = frame_record(daily_basic)
    if valuation:
        valuation["pe_ttm_3y_percentile"] = percentile(
            daily_basic.get("pe_ttm", pd.Series(dtype=float)).tolist(),
            valuation.get("pe_ttm"),
        )
        valuation["pb_3y_percentile"] = percentile(
            daily_basic.get("pb", pd.Series(dtype=float)).tolist(),
            valuation.get("pb"),
        )
    else:
        warnings.append("daily_basic_empty")

    common = {
        "ts_code": symbol,
        "start_date": start_compact,
        "end_date": as_of_compact,
    }
    indicator = normalize_disclosure_frame(
        call(
            "fina_indicator",
            **common,
            fields=(
                "ts_code,ann_date,end_date,roe,grossprofit_margin,debt_to_assets,"
                "current_ratio,q_sales_yoy,q_profit_yoy,ocf_to_or,ar_turn,inv_turn"
            ),
        ),
        as_of,
    )
    income = normalize_disclosure_frame(
        call(
            "income",
            **common,
            fields=(
                "ts_code,ann_date,f_ann_date,end_date,report_type,revenue,"
                "n_income_attr_p"
            ),
        ),
        as_of,
    )
    if not income.empty and "report_type" in income.columns:
        standard = income[income["report_type"].astype(str) == "1"]
        if not standard.empty:
            income = standard
    balance = normalize_disclosure_frame(
        call(
            "balancesheet",
            **common,
            fields=(
                "ts_code,ann_date,f_ann_date,end_date,total_assets,total_liab,"
                "total_cur_assets,total_cur_liab,accounts_receiv,inventories,"
                "contract_liab"
            ),
        ),
        as_of,
    )
    cashflow = normalize_disclosure_frame(
        call(
            "cashflow",
            **common,
            fields="ts_code,ann_date,f_ann_date,end_date,n_cashflow_act",
        ),
        as_of,
    )

    latest_indicator = frame_record(indicator)
    latest_income = frame_record(income)
    report_end = clean((latest_income or {}).get("end_date"))
    latest_balance = latest_for_period(balance, report_end)
    latest_cashflow = latest_for_period(cashflow, report_end)
    if latest_income and latest_balance is None:
        warnings.append("balance_same_period_missing")
    if latest_income and latest_cashflow is None:
        warnings.append("cashflow_same_period_missing")

    derived: dict[str, Any] = {}
    if latest_income:
        profit = safe_number(latest_income.get("n_income_attr_p"))
        operating_cash = safe_number((latest_cashflow or {}).get("n_cashflow_act"))
        derived["operating_cash_to_net_profit"] = (
            round(float(operating_cash) / float(profit), 4)
            if operating_cash is not None and profit not in (None, 0)
            else None
        )
    if latest_balance:
        assets = safe_number(latest_balance.get("total_assets"))
        for field, output_name in (
            ("accounts_receiv", "accounts_receivable_to_assets"),
            ("inventories", "inventory_to_assets"),
            ("contract_liab", "contract_liability_to_assets"),
        ):
            value = safe_number(latest_balance.get(field))
            derived[output_name] = (
                round(float(value) / float(assets), 6)
                if value is not None and assets not in (None, 0)
                else None
            )

    return {
        "valuation": valuation,
        "latest_financial_indicator": latest_indicator,
        "latest_income": latest_income,
        "latest_balance": latest_balance,
        "latest_cashflow": latest_cashflow,
        "derived_financial_metrics": derived,
        "fetched_at": now_iso(),
        "point_in_time_rule": (
            "trade_date_or_disclosure_availability_date_lte_as_of"
        ),
    }, warnings


def latest_for_period(
    frame: pd.DataFrame, report_end: str
) -> dict[str, Any] | None:
    if frame is None or frame.empty:
        return None
    if report_end and "end_date" in frame.columns:
        matched = frame[frame["end_date"].astype(str) == report_end]
        if not matched.empty:
            return frame_record(matched)
        return None
    return frame_record(frame)


def question_coverage(question: str) -> dict[str, Any]:
    text = clean(question)
    lowered = text.lower()
    has_structured = any(term in lowered for term in STRUCTURED_TERMS)
    has_external = any(term in lowered for term in EXTERNAL_TERMS)
    if has_structured and has_external:
        status = "PARTIAL_STRUCTURED_SUPPORT"
    elif has_structured:
        status = "STRUCTURED_EVIDENCE_AVAILABLE"
    elif has_external:
        status = "EXTERNAL_EVIDENCE_REQUIRED"
    else:
        status = "MANUAL_ANALYSIS_REQUIRED"
    return {"question": text, "coverage_status": status}


def account_observation(
    symbol: str, basic: dict[str, Any] | None
) -> str:
    name = clean((basic or {}).get("name")).upper()
    if bool((basic or {}).get("is_st")) or re.match(
        r"^(?:S\*)?\*?ST", name
    ):
        return "OBSERVE_ONLY_ST"
    if symbol.endswith(".BJ"):
        return "OBSERVE_ONLY_BJ_ACCOUNT_RESTRICTED"
    return "ELIGIBILITY_NOT_EVALUATED"


def build_result(
    task: dict[str, Any],
    local_snapshot: dict[str, Any],
    tushare_snapshot: dict[str, Any] | None,
    warnings: list[str],
    as_of: str,
) -> dict[str, Any]:
    questions = [
        question_coverage(question)
        for question in (task.get("questions_for_zhulong") or [])
        if clean(question)
    ]
    needs_external = {
        "EXTERNAL_EVIDENCE_REQUIRED",
        "PARTIAL_STRUCTURED_SUPPORT",
        "MANUAL_ANALYSIS_REQUIRED",
    }
    external_count = sum(
        item["coverage_status"] in needs_external for item in questions
    )
    local_evidence = any(
        local_snapshot.get(key)
        for key in ("stock_basic", "latest_daily", "latest_rps", "latest_zeta")
    )
    online_evidence = bool(tushare_snapshot) and any(
        (tushare_snapshot or {}).get(key)
        for key in ("valuation", "latest_financial_indicator", "latest_income")
    )
    if local_evidence and online_evidence:
        validation_status = "STRUCTURED_SNAPSHOT_READY"
    elif local_evidence or online_evidence:
        validation_status = "PARTIAL_STRUCTURED_SNAPSHOT"
    else:
        validation_status = "NO_STRUCTURED_EVIDENCE"
    return {
        "task_id": task.get("task_id"),
        "symbol": task.get("symbol"),
        "name": (
            (local_snapshot.get("stock_basic") or {}).get("name")
            or task.get("name")
        ),
        "source_theme_names": task.get("source_theme_names") or [],
        "compass_lines": task.get("compass_lines") or [],
        "lineage": {
            "source_reviewed_candidate_ids": (
                task.get("source_reviewed_candidate_ids") or []
            ),
            "source_reviewed_candidate_sha256": (
                task.get("source_reviewed_candidate_sha256") or []
            ),
            "source_preview_row_ids": task.get("source_preview_row_ids") or [],
            "reviewer_id": task.get("reviewer_id"),
            "reviewed_at": task.get("reviewed_at"),
            "compass_source_as_of": task.get("compass_source_as_of"),
            "validation_as_of": task.get("validation_as_of"),
        },
        "as_of": task.get("validation_as_of"),
        "validation_status": validation_status,
        "account_observation": account_observation(
            clean(task.get("symbol")), local_snapshot.get("stock_basic")
        ),
        "business_relevance_confirmed_by_source_review": True,
        "business_relevance_revalidated_by_executor": False,
        "local_snapshot": local_snapshot,
        "tushare_snapshot": tushare_snapshot,
        "question_coverage": questions,
        "external_evidence_required_count": external_count,
        "warnings": sorted(set(warnings)),
        "no_trade_signal": True,
        "generate_trade": False,
        "decision_verdict": None,
        "decision_score": None,
        "blocked_actions": BLOCKED_ACTIONS,
    }


def build_payload(
    task_payload: dict[str, Any],
    task_path: Path,
    task_sha256: str,
    tasks: list[dict[str, Any]],
    db_path: str,
    as_of: str,
    api: Any | None,
) -> dict[str, Any]:
    results = []
    warnings = []
    local_rows = []
    with DBGateway(db_path, read_only=True) as conn:
        for task in tasks:
            symbol = clean(task.get("symbol"))
            local, local_warnings = collect_local_snapshot(conn, symbol, as_of)
            local_rows.append((task, symbol, local, local_warnings))

    for task, symbol, local, local_warnings in local_rows:
        online = None
        online_warnings = []
        if api is not None:
            online, online_warnings = fetch_tushare_snapshot(
                api, symbol, as_of
            )
        else:
            online_warnings.append("tushare_disabled_or_unavailable")
        result = build_result(
            task,
            local,
            online,
            local_warnings + online_warnings,
            as_of,
        )
        results.append(result)
        warnings.extend(
            f"{symbol}:{item}" for item in result["warnings"]
        )
    return {
        "protocol_version": OUTPUT_VERSION,
        "batch_id": task_payload.get("batch_id"),
        "source": (
            "Compass/ima + Zhulong reviewed tasks + readonly market data"
        ),
        "generated_at": now_iso(),
        "compass_source_as_of": task_payload.get("compass_source_as_of"),
        "validation_as_of": as_of,
        "as_of": as_of,
        "mode": "dry_run_readonly_validation",
        "dry_run": True,
        "read_only": True,
        "no_trade_signal": True,
        "tool": "tools/compass_validation_executor.py",
        "source_task_artifact": str(task_path),
        "source_task_artifact_sha256": task_sha256,
        "data_sources": {
            "duckdb": [
                "fact_stock_basic", "fact_daily", "fact_rps_results",
                "fact_zeta_signals", "fact_quantile_snapshot",
            ],
            "tushare": [
                "daily_basic", "fina_indicator", "income",
                "balancesheet", "cashflow",
            ] if api is not None else [],
        },
        "allowed_actions": [
            "read_only_validation", "render_validation_report"
        ],
        "blocked_actions": BLOCKED_ACTIONS,
        "stats": {
            "input_tasks": len(tasks),
            "completed_snapshots": len(results),
            "tasks_with_warnings": sum(
                bool(item["warnings"]) for item in results
            ),
            "trade_signals": 0,
            "database_writes": 0,
        },
        "warnings": sorted(set(warnings)),
        "results": results,
    }


def md_table(
    headers: list[str], rows: list[list[Any]]
) -> list[str]:
    output = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        values = [
            clean(value).replace("|", "/") if value is not None else ""
            for value in row
        ]
        output.append("| " + " | ".join(values) + " |")
    return output


def render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        f"# Compass Read-only Validation Report - {payload.get('batch_id')}",
        "",
        "## Batch",
        "",
        f"- compass_source_as_of: {payload.get('compass_source_as_of')}",
        f"- validation_as_of: {payload.get('validation_as_of')}",
        (
            "- source_task_artifact_sha256: "
            f"{payload.get('source_task_artifact_sha256')}"
        ),
        "- mode: dry_run_readonly_validation",
        "- no_trade_signal: true",
        "",
        "## Summary",
        "",
    ]
    lines += md_table(
        ["metric", "value"],
        [
            [key, value]
            for key, value in (payload.get("stats") or {}).items()
        ],
    )
    summary_rows = []
    for item in payload.get("results") or []:
        local = item.get("local_snapshot") or {}
        daily = local.get("latest_daily") or {}
        path = local.get("path_metrics") or {}
        rps = local.get("latest_rps") or {}
        online = item.get("tushare_snapshot") or {}
        valuation = online.get("valuation") or {}
        financial = online.get("latest_financial_indicator") or {}
        summary_rows.append([
            item.get("symbol"),
            item.get("name"),
            item.get("account_observation"),
            daily.get("trade_date"),
            path.get("return_20d_pct"),
            path.get("return_60d_pct"),
            rps.get("rps_10"),
            valuation.get("pe_ttm"),
            valuation.get("pe_ttm_3y_percentile"),
            financial.get("roe"),
            financial.get("q_sales_yoy"),
            financial.get("q_profit_yoy"),
            item.get("external_evidence_required_count"),
        ])
    lines += ["", "## Structured Snapshot", ""] + md_table(
        [
            "symbol", "name", "account", "market_date", "ret20",
            "ret60", "rps10", "pe_ttm", "pe_3y_pct", "roe",
            "sales_yoy", "profit_yoy", "external_questions",
        ],
        summary_rows,
    )
    question_rows = []
    for item in payload.get("results") or []:
        for question in item.get("question_coverage") or []:
            question_rows.append([
                item.get("symbol"),
                question.get("coverage_status"),
                question.get("question"),
            ])
    lines += ["", "## Question Coverage", ""] + md_table(
        ["symbol", "coverage_status", "question"], question_rows
    )
    warning_rows = [[value] for value in payload.get("warnings") or []]
    lines += ["", "## Warnings", ""] + md_table(
        ["warning"], warning_rows
    )
    lines += [
        "",
        "## Safety",
        "",
        "This report is a point-in-time evidence snapshot, not an audit "
        "verdict or buy list. It writes no DuckDB, Shadow, RAG, or "
        "nexus_audits data, does not call decision_engine or Nexus, and "
        "cannot generate a trade.",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Execute reviewed Compass tasks as read-only evidence snapshots."
        )
    )
    parser.add_argument("--tasks", required=True, type=Path)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument(
        "--as-of",
        help="Optional assertion; must equal the task artifact validation_as_of.",
    )
    parser.add_argument("--db-path", default=DB_PATH)
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=OUT_DIR)
    parser.add_argument("--json-output", type=Path)
    parser.add_argument("--preview-output", type=Path)
    args = parser.parse_args()

    task_path = args.tasks.resolve()
    raw = task_path.read_bytes()
    task_payload = json.loads(raw.decode("utf-8"))
    actual_sha256 = sha256_bytes(raw)
    tasks = validate_task_artifact(
        task_payload, actual_sha256, args.expected_sha256
    )
    as_of = resolve_validation_as_of(task_payload, args.as_of)

    api = None
    if not args.offline:
        bridge = get_bridge()
        if bridge.available and bridge.api is not None:
            api = bridge.api

    payload = build_payload(
        task_payload, task_path, actual_sha256, tasks,
        args.db_path, as_of, api,
    )
    output_dir = args.output_dir.resolve()
    json_output = args.json_output or output_dir / (
        f"compass_validation_results_{safe_batch(payload['batch_id'])}.json"
    )
    preview_output = args.preview_output or output_dir / (
        f"zhulong_compass_validation_results_{safe_batch(payload['batch_id'])}.md"
    )
    json_output.parent.mkdir(parents=True, exist_ok=True)
    preview_output.parent.mkdir(parents=True, exist_ok=True)
    json_output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    preview_output.write_text(
        render_markdown(payload), encoding="utf-8"
    )
    print(json.dumps({
        "mode": payload["mode"],
        "json_output": str(json_output),
        "preview_output": str(preview_output),
        "stats": payload["stats"],
        "warnings": payload["warnings"],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise SystemExit(
            f"compass validation execution failed: {exc}"
        ) from None
