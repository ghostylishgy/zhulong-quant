#!/usr/bin/env python3
"""Build a read-only, point-in-time audit funnel observation artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = ROOT / "storage/database/zhulong.duckdb"
DEFAULT_STATE_PATH = ROOT / "data/nexus_state.json"
DEFAULT_OUTPUT_DIR = ROOT / "storage/reports/audit_funnel_observer"
OBSERVER_VERSION = "bl020_audit_funnel_observer_v0.1"
L1_RULE_VERSION = "pct_chg_desc_amount_gt_10000_limit50_v1"
L15_PROXY_VERSION = "raw_ma_stack_and_prev14_volume_ratio_v1"
BLOCKED_ACTIONS = [
    "change_l1",
    "change_l1_5",
    "change_l2_l3_l4",
    "write_duckdb",
    "write_shadow",
    "write_rag_memory",
    "write_nexus_audits",
    "generate_trade",
    "trigger_daemon",
    "auto_tune_thresholds",
]


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _canonical_sha256(value: Any) -> str:
    raw = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")
    return _sha256_bytes(raw)


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _canary(code: str, status: str, detail: str, **metrics: Any) -> Dict[str, Any]:
    return {"code": code, "status": status, "detail": detail, "metrics": metrics}


def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and row[0])


def _git_head() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return "UNKNOWN"


def load_receipt(path: Path) -> Tuple[Dict[str, Any], str]:
    raw = path.read_bytes()
    payload = json.loads(raw.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("nexus state receipt must be a JSON object")
    return payload, _sha256_bytes(raw)


def _l3_non_veto_count(rows: Iterable[Any]) -> int:
    count = 0
    for item in rows:
        payload = item[1] if isinstance(item, (list, tuple)) and len(item) > 1 else {}
        verdict = str(payload.get("verdict") or "") if isinstance(payload, dict) else ""
        if verdict.upper() != "VETO":
            count += 1
    return count


def validate_receipt(
    receipt: Dict[str, Any], trade_date: str, run_id: str
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    candidates = receipt.get("candidates")
    l2_passed = receipt.get("l2_passed")
    l3_results = receipt.get("l3_results")
    gate = receipt.get("l1_gate_stats")
    lists_valid = all(isinstance(value, list) for value in (candidates, l2_passed, l3_results))
    gate_valid = isinstance(gate, dict) and {
        "raw_count", "passed_count", "candidate_errors", "fatal_error"
    }.issubset(gate)
    exact_scope = (
        str(receipt.get("trade_date") or "") == trade_date
        and str(receipt.get("run_id") or "") == run_id
    )
    phase_complete = str(receipt.get("current_phase") or "").upper() == "COMPLETED"
    checkpoint_present = bool(str(receipt.get("last_checkpoint") or "").strip())
    canaries: List[Dict[str, Any]] = []
    receipt_ok = exact_scope and phase_complete and checkpoint_present and lists_valid and gate_valid
    canaries.append(
        _canary(
            "RECEIPT_SCOPE_COMPLETE",
            "PASS" if receipt_ok else "FAIL_CLOSED",
            "receipt is exact and completed" if receipt_ok else "receipt scope or structure is invalid",
            exact_scope=exact_scope,
            phase_complete=phase_complete,
            checkpoint_present=checkpoint_present,
            stage_lists_valid=lists_valid,
            gate_stats_valid=gate_valid,
        )
    )
    gate = gate if isinstance(gate, dict) else {}
    raw_count = _int(gate.get("raw_count"), -1)
    passed_count = _int(gate.get("passed_count"), -1)
    candidate_count = len(candidates) if isinstance(candidates, list) else -1
    l2_count = len(l2_passed) if isinstance(l2_passed, list) else -1
    l3_count = len(l3_results) if isinstance(l3_results, list) else -1
    counts_ok = (
        min(raw_count, passed_count, candidate_count, l2_count, l3_count) >= 0
        and passed_count == candidate_count
        and l2_count <= candidate_count
        and l3_count <= l2_count
    )
    gate_error_free = (
        _int(gate.get("candidate_errors"), -1) == 0
        and not str(gate.get("fatal_error") or "").strip()
    )
    canaries.append(
        _canary(
            "RECEIPT_COUNTS_CONSISTENT",
            "PASS" if counts_ok and gate_error_free else "FAIL_CLOSED",
            "stage counts and gate errors are consistent"
            if counts_ok and gate_error_free
            else "stage counts conflict or L1.5 reported an error",
            raw_count=raw_count,
            passed_count=passed_count,
            candidate_count=candidate_count,
            l2_count=l2_count,
            l3_count=l3_count,
            candidate_errors=_int(gate.get("candidate_errors"), -1),
            fatal_error=str(gate.get("fatal_error") or ""),
        )
    )
    return {
        "raw_count": raw_count,
        "l15_count": passed_count,
        "candidate_count": candidate_count,
        "l2_count": l2_count,
        "l3_count": l3_count,
        "l3_non_veto_count": _l3_non_veto_count(l3_results or []),
        "gate_stats": gate,
    }, canaries


def audit_contract_provenance(
    receipt: Dict[str, Any], trade_date: str, run_id: str
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    raw = receipt.get("audit_contract_binding")
    binding = dict(raw) if isinstance(raw, dict) else {}
    valid = bool(
        binding.get("binding_valid") is True
        and str(binding.get("provenance_status") or "") == "BOUND_VERIFIED"
        and str(binding.get("binding_status") or "") == "BOUND_AT_AUDIT_START"
        and str(binding.get("trade_date") or "") == trade_date
        and str(binding.get("run_id") or "") == run_id
        and len(str(binding.get("contract_sha256") or "")) == 64
        and len(str(binding.get("binding_sha256") or "")) == 64
        and bool(str(binding.get("evidence_as_of") or "").strip())
    )
    normalized = {
        "binding_valid": valid,
        "provenance_status": (
            "BOUND_VERIFIED" if valid
            else str(binding.get("provenance_status") or "UNBOUND_LEGACY_OR_DISABLED")
        ),
        "binding_status": str(binding.get("binding_status") or "UNBOUND"),
        "contract_schema_version": str(binding.get("contract_schema_version") or ""),
        "contract_sha256": str(binding.get("contract_sha256") or ""),
        "binding_schema_version": str(binding.get("schema_version") or ""),
        "binding_sha256": str(binding.get("binding_sha256") or ""),
        "trade_date": str(binding.get("trade_date") or trade_date),
        "run_id": str(binding.get("run_id") or run_id),
        "evidence_as_of": str(binding.get("evidence_as_of") or ""),
        "artifact_name": str(binding.get("artifact_name") or ""),
    }
    canary = _canary(
        "AUDIT_CONTRACT_PROVENANCE",
        "PASS" if valid else "WARN",
        "audit contract is cryptographically bound to the exact run"
        if valid else "run has no verified contract binding; exclude it from versioned counterfactual scoring",
        provenance_status=normalized["provenance_status"],
    )
    return normalized, canary


def collect_source_quality(
    conn: duckdb.DuckDBPyConnection, trade_date: str
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    latest = conn.execute(
        "SELECT CAST(MAX(trade_date) AS VARCHAR) FROM fact_daily"
    ).fetchone()[0]
    row = conn.execute(
        """
        SELECT COUNT(*) AS row_count,
               COUNT(DISTINCT symbol) AS symbol_count,
               SUM(CASE WHEN close IS NULL OR close <= 0 THEN 1 ELSE 0 END) AS invalid_close,
               SUM(CASE WHEN pct_chg IS NULL THEN 1 ELSE 0 END) AS missing_pct_chg,
               SUM(CASE WHEN vol IS NULL THEN 1 ELSE 0 END) AS missing_vol,
               SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS missing_amount,
               SUM(CASE WHEN close > 0 AND amount > 10000 THEN 1 ELSE 0 END) AS l1_eligible
        FROM fact_daily WHERE CAST(trade_date AS DATE) = CAST(? AS DATE)
        """,
        [trade_date],
    ).fetchone()
    row_count, symbol_count = _int(row[0]), _int(row[1])
    critical_invalid = sum(_int(value) for value in row[2:6])
    l1_eligible = _int(row[6])
    baseline = conn.execute(
        """
        SELECT MEDIAN(row_count) FROM (
            SELECT trade_date, COUNT(*) AS row_count
            FROM fact_daily
            WHERE CAST(trade_date AS DATE) < CAST(? AS DATE)
            GROUP BY trade_date ORDER BY trade_date DESC LIMIT 20
        ) recent
        """,
        [trade_date],
    ).fetchone()[0]
    baseline_rows = _float(baseline)
    coverage_ratio = row_count / baseline_rows if baseline_rows > 0 else 0.0
    rps_missing = -1
    if _table_exists(conn, "fact_rps_results"):
        rps_missing = _int(
            conn.execute(
                """
                SELECT COUNT(*) FROM fact_daily d
                LEFT JOIN fact_rps_results r
                  ON r.symbol=d.symbol AND r.trade_date=d.trade_date
                WHERE CAST(d.trade_date AS DATE)=CAST(? AS DATE)
                  AND r.symbol IS NULL
                """,
                [trade_date],
            ).fetchone()[0]
        )
    canaries = [
        _canary(
            "SOURCE_DATE_MATCH",
            "PASS" if str(latest)[:10] == trade_date else "FAIL_CLOSED",
            "latest fact_daily date matches audit date"
            if str(latest)[:10] == trade_date
            else "fact_daily is stale or audit date is ahead of source data",
            latest_trade_date=str(latest)[:10], requested_trade_date=trade_date,
        ),
        _canary(
            "SOURCE_PRIMARY_KEY_UNIQUE",
            "PASS" if row_count > 0 and row_count == symbol_count else "FAIL_CLOSED",
            "one row per symbol" if row_count > 0 and row_count == symbol_count else "missing or duplicate daily rows",
            row_count=row_count, distinct_symbols=symbol_count,
        ),
        _canary(
            "SOURCE_BATCH_COVERAGE",
            "PASS" if baseline_rows > 0 and coverage_ratio >= 0.80 else "FAIL_CLOSED",
            "daily batch is at least 80% of the recent median"
            if baseline_rows > 0 and coverage_ratio >= 0.80
            else "daily batch is severely below the recent median",
            row_count=row_count, recent_median_rows=baseline_rows,
            coverage_ratio=round(coverage_ratio, 6),
        ),
        _canary(
            "SOURCE_CRITICAL_FIELDS",
            "PASS" if critical_invalid == 0 else "FAIL_CLOSED",
            "critical daily fields are populated" if critical_invalid == 0 else "critical daily fields are missing or invalid",
            invalid_close=_int(row[2]), missing_pct_chg=_int(row[3]),
            missing_vol=_int(row[4]), missing_amount=_int(row[5]),
        ),
        _canary(
            "RPS_EXACT_COVERAGE",
            "PASS" if rps_missing == 0 else "FAIL_CLOSED",
            "RPS coverage is exact" if rps_missing == 0 else "RPS table is absent or has missing symbol-date pairs",
            missing_pairs=rps_missing,
        ),
    ]
    return {
        "latest_trade_date": str(latest)[:10],
        "source_row_count": row_count,
        "distinct_symbol_count": symbol_count,
        "l1_eligible_source_count": l1_eligible,
        "recent_median_row_count": baseline_rows,
        "source_coverage_ratio": round(coverage_ratio, 6),
        "rps_missing_pairs": rps_missing,
    }, canaries


def collect_market_context(conn: duckdb.DuckDBPyConnection, trade_date: str) -> Dict[str, Any]:
    stock_basic_join = ""
    stock_name = "''"
    is_st = "FALSE"
    if _table_exists(conn, "fact_stock_basic"):
        stock_basic_join = "LEFT JOIN fact_stock_basic s ON s.symbol=d.symbol"
        stock_name = "COALESCE(s.name,'')"
        is_st = "COALESCE(s.is_st,FALSE)"
    sql = f"""
        WITH base AS (
            SELECT d.symbol, CAST(d.trade_date AS DATE) AS trade_date,
                   d.close, d.pct_chg, d.vol, d.amount,
                   {stock_name} AS stock_name, {is_st} AS is_st,
                   COUNT(d.close) OVER (
                       PARTITION BY d.symbol ORDER BY CAST(d.trade_date AS DATE)
                       ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                   ) AS n60,
                   AVG(d.close) OVER (PARTITION BY d.symbol ORDER BY CAST(d.trade_date AS DATE) ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS ma5,
                   AVG(d.close) OVER (PARTITION BY d.symbol ORDER BY CAST(d.trade_date AS DATE) ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS ma10,
                   AVG(d.close) OVER (PARTITION BY d.symbol ORDER BY CAST(d.trade_date AS DATE) ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
                   AVG(d.close) OVER (PARTITION BY d.symbol ORDER BY CAST(d.trade_date AS DATE) ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ma60,
                   AVG(d.vol) OVER (PARTITION BY d.symbol ORDER BY CAST(d.trade_date AS DATE) ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS prev14_vol
            FROM fact_daily d
            {stock_basic_join}
            WHERE CAST(d.trade_date AS DATE) <= CAST(? AS DATE)
        ), eligible AS (
            SELECT *,
                   n60 >= 60 AND ma5 > ma10 AND ma10 > ma20 AND ma20 > ma60 AS ma_ok,
                   prev14_vol > 0 AND vol / prev14_vol >= 1.2 AS volume_ok,
                   ROW_NUMBER() OVER (PARTITION BY trade_date ORDER BY pct_chg DESC, symbol) AS l1_rank
            FROM base WHERE close > 0 AND amount > 10000
        )
        SELECT COUNT(*) AS eligible_count,
               SUM(CASE WHEN ma_ok THEN 1 ELSE 0 END) AS market_ma_count,
               SUM(CASE WHEN ma_ok AND volume_ok THEN 1 ELSE 0 END) AS market_l15_proxy_count,
               SUM(CASE WHEN l1_rank <= 50 AND ma_ok THEN 1 ELSE 0 END) AS l1_ma_count,
               SUM(CASE WHEN l1_rank <= 50 AND volume_ok THEN 1 ELSE 0 END) AS l1_volume_count,
               SUM(CASE WHEN l1_rank <= 50 AND ma_ok AND volume_ok THEN 1 ELSE 0 END) AS l1_l15_proxy_count,
               MIN(CASE WHEN ma_ok AND volume_ok THEN l1_rank END) AS first_l15_proxy_rank,
               SUM(CASE WHEN l1_rank <= 50 AND symbol LIKE '%.BJ' THEN 1 ELSE 0 END) AS bj_slots,
               SUM(CASE WHEN l1_rank <= 50 AND (is_st OR UPPER(stock_name) LIKE '%ST%') THEN 1 ELSE 0 END) AS st_slots,
               SUM(CASE WHEN l1_rank <= 50 AND n60 < 60 THEN 1 ELSE 0 END) AS short_history_slots
        FROM eligible WHERE trade_date=CAST(? AS DATE)
    """
    row = conn.execute(sql, [trade_date, trade_date]).fetchone()
    eligible_count = _int(row[0])
    market_ma_count = _int(row[1])
    return {
        "authority": "diagnostic_only_not_production_gate",
        "l15_proxy_version": L15_PROXY_VERSION,
        "eligible_count": eligible_count,
        "market_ma_count": market_ma_count,
        "market_ma_breadth": round(market_ma_count / eligible_count, 6) if eligible_count else 0.0,
        "market_l15_proxy_count": _int(row[2]),
        "l1_top50_ma_count": _int(row[3]),
        "l1_top50_volume_count": _int(row[4]),
        "l1_top50_l15_proxy_count": _int(row[5]),
        "first_l15_proxy_rank": _int(row[6], -1),
        "l1_top50_bj_slots": _int(row[7]),
        "l1_top50_st_slots": _int(row[8]),
        "l1_top50_short_history_slots": _int(row[9]),
    }


def collect_audit_rows(
    conn: duckdb.DuckDBPyConnection, trade_date: str, run_id: str
) -> Dict[str, Any]:
    row = conn.execute(
        """
        SELECT COUNT(*) AS total_rows,
               SUM(CASE WHEN UPPER(COALESCE(status,''))='L4_DONE'
                         OR RIGHT(UPPER(COALESCE(status,'')),9)='_TERMINAL'
                        THEN 1 ELSE 0 END) AS terminal_rows,
               SUM(CASE WHEN COALESCE(l2_passed,0)=1 THEN 1 ELSE 0 END) AS l2_passed_rows,
               SUM(CASE WHEN COALESCE(l3_verdict,'')<>'' THEN 1 ELSE 0 END) AS l3_rows,
               SUM(CASE WHEN COALESCE(l4_final_verdict,'')<>'' THEN 1 ELSE 0 END) AS l4_rows,
               SUM(CASE WHEN UPPER(COALESCE(l4_final_verdict,''))='PASS' THEN 1 ELSE 0 END) AS pass_rows,
               SUM(CASE WHEN UPPER(COALESCE(l4_final_verdict,''))='HOLD' THEN 1 ELSE 0 END) AS hold_rows,
               SUM(CASE WHEN UPPER(COALESCE(l4_final_verdict,''))='VETO' THEN 1 ELSE 0 END) AS veto_rows
        FROM nexus_audits WHERE trade_date=? AND run_id=?
        """,
        [trade_date, run_id],
    ).fetchone()
    keys = [
        "total_rows", "terminal_rows", "l2_passed_rows", "l3_rows",
        "l4_rows", "pass_rows", "hold_rows", "veto_rows",
    ]
    return {key: _int(value) for key, value in zip(keys, row)}


def build_payload(
    db_path: Path, state_path: Path, trade_date: str = "",
    run_id: str = "", code_version: str = "",
) -> Dict[str, Any]:
    receipt, state_sha = load_receipt(state_path)
    td = str(trade_date or receipt.get("trade_date") or "").strip()
    rid = str(run_id or receipt.get("run_id") or "").strip()
    if not td or not rid:
        raise ValueError("trade_date and run_id are required")
    receipt_metrics, canaries = validate_receipt(receipt, td, rid)
    contract_provenance, contract_canary = audit_contract_provenance(receipt, td, rid)
    canaries.append(contract_canary)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        source, source_canaries = collect_source_quality(conn, td)
        market = collect_market_context(conn, td)
        audit = collect_audit_rows(conn, td, rid)
    canaries.extend(source_canaries)
    zero_rows_valid = (
        audit["total_rows"] == 0
        and receipt_metrics["l15_count"] == 0
        and receipt_metrics["l2_count"] == 0
        and receipt_metrics["l3_count"] == 0
    )
    row_completion_valid = (
        audit["total_rows"] > 0
        and audit["terminal_rows"] == audit["total_rows"]
    )
    completion_ok = zero_rows_valid or row_completion_valid
    canaries.append(
        _canary(
            "AUDIT_ROWS_COMPLETE",
            "PASS" if completion_ok else "FAIL_CLOSED",
            "audit rows are terminal or the exact zero-candidate receipt is valid"
            if completion_ok else "audit rows and receipt do not prove completion",
            zero_rows_valid=zero_rows_valid,
            row_completion_valid=row_completion_valid,
            total_rows=audit["total_rows"], terminal_rows=audit["terminal_rows"],
        )
    )
    if receipt_metrics["l15_count"] == 0:
        canaries.append(
            _canary(
                "L1_5_ZERO_PASS", "WARN",
                "L1.5 produced zero candidates; record and observe without changing thresholds",
                l1_raw_count=receipt_metrics["raw_count"],
                market_ma_breadth=market["market_ma_breadth"],
                first_l15_proxy_rank=market["first_l15_proxy_rank"],
            )
        )
    if market["market_ma_breadth"] < 0.03:
        canaries.append(
            _canary(
                "MARKET_TREND_SCARCE", "WARN",
                "fewer than 3% of liquid symbols have the production MA stack; this is context, not a gate",
                market_ma_breadth=market["market_ma_breadth"],
            )
        )
    ineligible_slots = (
        market["l1_top50_bj_slots"] + market["l1_top50_st_slots"]
        + market["l1_top50_short_history_slots"]
    )
    if ineligible_slots:
        canaries.append(
            _canary(
                "L1_TOP50_ELIGIBILITY_NOISE", "WARN",
                "BJ, ST, or short-history rows occupy raw L1 slots; counts may overlap",
                summed_slots=ineligible_slots,
            )
        )
    hard_failures = [item for item in canaries if item["status"] == "FAIL_CLOSED"]
    warnings = [item for item in canaries if item["status"] == "WARN"]
    quality = "DATA_FAIL_CLOSED" if hard_failures else "DATA_OK"
    observation_status = "OBSERVE_WARN" if warnings else "OBSERVE_CLEAR"
    gate = receipt_metrics["gate_stats"]
    funnel = [
        {"stage": "SOURCE_BATCH", "input_count": source["source_row_count"],
         "output_count": source["l1_eligible_source_count"]},
        {"stage": "L1_RAW", "input_count": source["l1_eligible_source_count"],
         "output_count": receipt_metrics["raw_count"], "rule_version": L1_RULE_VERSION},
        {
            "stage": "L1_5_TREND_HUNTER", "input_count": receipt_metrics["raw_count"],
            "output_count": receipt_metrics["l15_count"], "reason_counts_overlap": True,
            "reason_counts": {
                "ma_alignment_false": _int(gate.get("ma_alignment_false")),
                "score_not_above_threshold": _int(gate.get("score_not_above_threshold")),
                "invalid_score": _int(gate.get("invalid_score")),
                "candidate_errors": _int(gate.get("candidate_errors")),
            },
        },
        {"stage": "L2", "input_count": receipt_metrics["candidate_count"],
         "evaluated_rows": audit["total_rows"], "output_count": receipt_metrics["l2_count"]},
        {"stage": "L3", "input_count": receipt_metrics["l2_count"],
         "evaluated_count": receipt_metrics["l3_count"],
         "non_veto_count": receipt_metrics["l3_non_veto_count"]},
        {"stage": "L4", "input_count": receipt_metrics["l3_count"],
         "terminal_rows": audit["terminal_rows"],
         "verdict_counts": {"PASS": audit["pass_rows"], "HOLD": audit["hold_rows"],
                            "VETO": audit["veto_rows"]}},
    ]
    payload = {
        "schema_version": OBSERVER_VERSION, "trade_date": td, "run_id": rid,
        "as_of": str(receipt.get("last_checkpoint") or ""),
        "generated_at": datetime.now().astimezone().isoformat(),
        "source_state": str(state_path), "source_state_sha256": state_sha,
        "source_database": str(db_path), "code_version": code_version or _git_head(),
        "mode": "observer_only_read_only", "observer_only": True,
        "no_trade_signal": True, "batch_quality": quality,
        "observation_status": observation_status,
        "hard_failure_count": len(hard_failures), "warning_count": len(warnings),
        "rules": {
            "l1_rule_version": L1_RULE_VERSION,
            "l15_diagnostic_proxy_version": L15_PROXY_VERSION,
            "candidate_volume_anomaly_policy": "warn_only_never_auto_tune",
        },
        "source_quality": source, "receipt_metrics": receipt_metrics,
        "audit_contract_provenance": contract_provenance,
        "audit_rows": audit, "market_context": market, "funnel": funnel,
        "canaries": canaries, "blocked_actions": BLOCKED_ACTIONS,
    }
    payload["artifact_identity_sha256"] = _canonical_sha256({
        "schema_version": OBSERVER_VERSION, "trade_date": td, "run_id": rid,
        "source_state_sha256": state_sha, "code_version": payload["code_version"],
        "evidence_sha256": _canonical_sha256({
            "source_quality": source, "audit_rows": audit, "market_context": market,
            "funnel": funnel, "canaries": canaries,
            "audit_contract_provenance": contract_provenance,
        }),
    })
    return payload


def render_markdown(payload: Dict[str, Any]) -> str:
    lines = [
        f"# Audit Funnel Observer - {payload['trade_date']}", "", "## Batch", "",
        f"- run_id: `{payload['run_id']}`",
        f"- batch_quality: `{payload['batch_quality']}`",
        f"- observation_status: `{payload['observation_status']}`",
        "- observer_only: true", "- no_trade_signal: true",
        f"- source_state_sha256: `{payload['source_state_sha256']}`",
        f"- code_version: `{payload['code_version']}`",
        f"- audit_contract_provenance: `{payload['audit_contract_provenance']['provenance_status']}`",
        f"- audit_contract_sha256: `{payload['audit_contract_provenance']['contract_sha256']}`",
        f"- audit_binding_sha256: `{payload['audit_contract_provenance']['binding_sha256']}`",
        "", "## Funnel", "",
        "| Stage | Input | Output / evaluated | Notes |",
        "|---|---:|---:|---|",
    ]
    for row in payload["funnel"]:
        output = row.get("output_count", row.get("evaluated_count", row.get("terminal_rows", 0)))
        notes = ""
        if row["stage"] == "L1_5_TREND_HUNTER":
            reasons = row["reason_counts"]
            notes = (
                f"MA false={reasons['ma_alignment_false']}; "
                f"score reject={reasons['score_not_above_threshold']}; counts overlap"
            )
        if row["stage"] == "L4":
            notes = json.dumps(row["verdict_counts"], ensure_ascii=False)
        lines.append(f"| {row['stage']} | {row['input_count']} | {output} | {notes} |")
    market = payload["market_context"]
    lines.extend([
        "", "## Market Context", "",
        f"- market_ma_breadth: `{market['market_ma_breadth']:.4f}`",
        f"- L1 Top50 MA aligned: `{market['l1_top50_ma_count']}`",
        f"- L1 Top50 diagnostic L1.5 pass: `{market['l1_top50_l15_proxy_count']}`",
        f"- first diagnostic L1.5 pass rank: `{market['first_l15_proxy_rank']}`",
        f"- BJ / ST / short-history slots: `{market['l1_top50_bj_slots']} / {market['l1_top50_st_slots']} / {market['l1_top50_short_history_slots']}`",
        "- These metrics are diagnostic context only. They do not change any gate.",
        "", "## Canaries", "", "| Code | Status | Detail |", "|---|---|---|",
    ])
    for item in payload["canaries"]:
        lines.append(f"| {item['code']} | {item['status']} | {item['detail']} |")
    lines.extend(["", "## Blocked Actions", ""])
    lines.extend(f"- `{action}`" for action in payload["blocked_actions"])
    return "\n".join(lines) + "\n"


def _atomic_write(path: Path, content: str) -> None:
    temp_path = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temp_path.write_text(content, encoding="utf-8")
    os.replace(temp_path, path)


def write_artifacts(payload: Dict[str, Any], output_dir: Path) -> Tuple[Path, Path, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = f"audit_funnel_{payload['trade_date'].replace('-', '')}_{payload['run_id']}"
    json_path = output_dir / f"{stem}.json"
    md_path = output_dir / f"{stem}.md"
    action = "CREATED_FROZEN"
    if json_path.exists():
        existing = json.loads(json_path.read_text(encoding="utf-8"))
        if existing.get("artifact_identity_sha256") != payload.get("artifact_identity_sha256"):
            raise ValueError("existing audit funnel artifact has a different frozen identity")
        if not md_path.exists():
            _atomic_write(md_path, render_markdown(existing))
        action = "REUSED_FROZEN"
    else:
        _atomic_write(json_path, json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n")
        _atomic_write(md_path, render_markdown(payload))
    return json_path, md_path, action


def main() -> int:
    parser = argparse.ArgumentParser(description="BL-020 read-only audit funnel observer")
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--state", default=str(DEFAULT_STATE_PATH))
    parser.add_argument("--trade-date", default="")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--write-artifacts", action="store_true")
    args = parser.parse_args()
    payload = build_payload(Path(args.db), Path(args.state), args.trade_date, args.run_id)
    result = {
        "trade_date": payload["trade_date"], "run_id": payload["run_id"],
        "batch_quality": payload["batch_quality"],
        "observation_status": payload["observation_status"],
        "hard_failure_count": payload["hard_failure_count"],
        "warning_count": payload["warning_count"], "observer_only": True,
        "wrote_artifacts": False,
    }
    if args.write_artifacts:
        json_path, md_path, action = write_artifacts(payload, Path(args.output_dir))
        result.update({"json": str(json_path), "markdown": str(md_path),
                       "report_action": action, "wrote_artifacts": True})
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 2 if payload["batch_quality"] == "DATA_FAIL_CLOSED" else 0


if __name__ == "__main__":
    raise SystemExit(main())
