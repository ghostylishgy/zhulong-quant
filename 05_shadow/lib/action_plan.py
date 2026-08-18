#!/usr/bin/env python3
"""Read-only Trade Lifecycle action-plan renderer for one completed audit run."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

try:
    from .db_contract import DBGateway, DB_PATH
    from .shadow_config import load_shadow_rules
    from .sector_tide import load_sector_tides
    from .trade_contract_checks import evaluate_contract_conditions
except Exception:
    from db_contract import DBGateway, DB_PATH
    from shadow_config import load_shadow_rules
    from sector_tide import load_sector_tides
    from trade_contract_checks import evaluate_contract_conditions

REPORT_DIR = Path("/root/quant_project/storage/reports/trade_action_plan")
PENDING_MAX_TRADING_DAYS = 2

CONDITION_CN = {
    "next_session_relay_confirmed": "次日接力环境确认",
    "sector_breadth_not_collapsing": "板块广度未快速退潮",
    "no_excessive_open_gap": "开盘未出现过度高开",
    "event_source_verified": "事件来源已核验",
    "business_relevance_present": "主营相关性成立",
    "price_not_fully_priced_in": "价格尚未充分计价",
    "breakout_confirmed": "突破确认",
    "breakout_or_pullback_confirmed": "突破或回踩确认",
    "sector_tide_supportive": "板块潮汐支持",
    "risk_reward_still_valid": "风险收益比仍有效",
    "trend_structure_intact": "趋势结构完整",
    "no_exhaustion_or_excessive_gap": "无衰竭或过度高开",
    "relative_strength_persistent": "相对强度持续",
    "relay_failed": "接力失败",
    "leader_breakdown": "龙头结构破坏",
    "high_risk_seat_concentration": "高风险席位集中",
    "event_denied_or_clarified": "事件被否认或澄清",
    "business_relevance_disproved": "主营相关性被证伪",
    "catalyst_fully_priced": "催化已充分计价",
    "breakout_failed": "突破失败",
    "sector_tide_reversal": "板块潮汐反转",
    "relative_strength_lost": "相对强度丢失",
    "trend_structure_broken": "趋势结构破坏",
    "relative_strength_deterioration": "相对强度恶化",
    "crowding_exhaustion": "拥挤交易衰竭",
    "news_or_regulatory_risk": "新闻或监管风险",
}

PLAN_STATUS_CN = {
    "WAIT_T1_ENTRY": "等待 T+1 进入条件",
    "WAIT_NEWS_CHECK": "等待新闻复核",
    "OBSERVE_ONLY_ST": "ST 风险，仅观察",
    "OBSERVE_ONLY_BJ": "北交所资格不符，仅观察",
    "BLOCKED_BY_NEWS": "新闻风险拦截",
    "BLOCKED_BY_TIDE": "市场潮汐拦截",
    "BLOCKED_BY_CONTRACT": "交易类型或契约不成立",
    "SKIPPED_BY_ENTRY_GATE": "买入前安全门拦截",
    "NO_NEW_SHADOW_ENTRY": "无新增影子盘动作",
}
ARCHETYPE_CN = {
    "EMOTION_RELAY": "情绪接力",
    "EVENT_CATALYST": "事件催化",
    "TREND_INITIATION": "趋势启动",
    "TREND_CONTINUATION": "趋势延续",
    "UNCLASSIFIED": "未分类",
}
NEWS_CN = {
    "NEWS_CLEAR": "新闻无明显风险",
    "NEWS_SIGNAL": "新闻信号，仅观察",
    "NEWS_CAUTION": "新闻谨慎",
    "NEWS_CRITICAL_CANDIDATE": "新闻重大风险候选",
    "NEWS_QUEUED": "新闻待检查",
    "NEWS_UNAVAILABLE": "新闻不可用",
    "NEWS_NOT_CHECKED": "新闻未检查",
}
TIDE_CN = {
    "AGGRESSIVE": "市场偏强",
    "CAUTION": "市场谨慎",
    "FORCE_NO_EDGE": "市场无优势",
    "UNKNOWN": "市场状态未知",
}
SECTOR_TIDE_CN = {
    "SECTOR_SUPPORTIVE": "板块环境支持",
    "SECTOR_MIXED": "板块环境混合",
    "SECTOR_ADVERSE": "板块环境不利",
    "SECTOR_TIDE_UNAVAILABLE": "板块潮汐不可用",
}


def _json(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    try:
        parsed = json.loads(str(raw or "{}"))
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _table_exists(conn, table: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table],
    ).fetchone()
    return bool(row and int(row[0] or 0) > 0)


def _reason_status(reason: str) -> str:
    upper = str(reason or "").upper()
    if "ACCOUNT_INELIGIBLE_ST" in upper:
        return "OBSERVE_ONLY_ST"
    if "ACCOUNT_INELIGIBLE_BJ" in upper:
        return "OBSERVE_ONLY_BJ"
    if "NEWS" in upper:
        return "BLOCKED_BY_NEWS"
    if "TIDE" in upper:
        return "BLOCKED_BY_TIDE"
    if "ARCHETYPE" in upper or "CONTRACT" in upper:
        return "BLOCKED_BY_CONTRACT"
    return "SKIPPED_BY_ENTRY_GATE"


def _conditions_cn(values: List[Any]) -> List[str]:
    return [CONDITION_CN.get(str(value), str(value)) for value in values]


def _display(mapping: Dict[str, str], value: Any, default: str) -> str:
    raw = str(value or "").strip().upper()
    return mapping.get(raw, raw or default)


def compose_plan_item(
    audit: Dict[str, Any],
    pending: Dict[str, Any] | None,
    skipped: Dict[str, Any] | None,
    *,
    cash_reserve: float,
    max_single_pct: float,
    sector_tide: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    pending = pending or {}
    skipped = skipped or {}
    evidence = _json(skipped.get("evidence_json"))
    contract = _json(pending.get("trade_contract_json")) or _json(evidence.get("trade_contract"))
    status = str(pending.get("status") or "").upper()
    skip_reason = str(skipped.get("reason") or "")

    if status == "PENDING":
        plan_status = "WAIT_T1_ENTRY"
        qualification_status = "ELIGIBLE_FOR_SHADOW_PLAN"
    elif status == "WAIT_NEWS_CHECK":
        plan_status = "WAIT_NEWS_CHECK"
        qualification_status = "ELIGIBLE_PENDING_NEWS"
    elif skipped:
        plan_status = _reason_status(skip_reason)
        qualification_status = plan_status
    else:
        plan_status = "NO_NEW_SHADOW_ENTRY"
        qualification_status = "ALREADY_HANDLED_OR_REINFORCEMENT"

    actionable = status == "PENDING"
    reference_close = float(audit.get("l1_close") or pending.get("l1_close") or 0)
    earliest = str(pending.get("earliest_fill_date") or "")
    max_amount = round(max(0.0, float(cash_reserve)) * max(0.0, float(max_single_pct)), 2) if actionable else 0.0

    archetype = str(contract.get("primary_archetype") or audit.get("trade_archetype") or "UNCLASSIFIED")
    news_status = str(audit.get("l4_news_status") or "NEWS_NOT_CHECKED")
    tide_status = str(pending.get("entry_tide_gate") or skipped.get("entry_tide_gate") or "UNKNOWN")
    sector_tide = dict(sector_tide or {})
    sector_tide_status = str(sector_tide.get("status") or "SECTOR_TIDE_UNAVAILABLE")
    contract_condition_evaluation = evaluate_contract_conditions(
        contract, {"sector_tide_status": sector_tide_status}
    )
    return {
        "task_id": str(audit.get("task_id") or ""),
        "symbol": str(audit.get("symbol") or ""),
        "name": str(audit.get("name") or audit.get("symbol") or ""),
        "l4_verdict": str(audit.get("l4_final_verdict") or ""),
        "audit_safety_score": float(audit.get("final_score") or 0),
        "plan_status": plan_status,
        "plan_status_cn": _display(PLAN_STATUS_CN, plan_status, "状态未知"),
        "qualification_status": qualification_status,
        "trade_archetype": archetype,
        "trade_archetype_cn": _display(ARCHETYPE_CN, archetype, "未分类"),
        "news_status": news_status,
        "news_status_cn": _display(NEWS_CN, news_status, "新闻状态未知"),
        "news_gate": str(audit.get("l4_news_gate") or "NONE"),
        "tide_status": tide_status,
        "tide_status_cn": _display(TIDE_CN, tide_status, "市场状态未知"),
        "sector": str(sector_tide.get("sector") or "UNKNOWN"),
        "sector_tide_status": sector_tide_status,
        "sector_tide_status_cn": _display(SECTOR_TIDE_CN, sector_tide_status, "板块潮汐不可用"),
        "sector_tide_evidence": sector_tide,
        "entry_conditions": list(contract.get("entry_confirmation") or []),
        "entry_conditions_cn": _conditions_cn(list(contract.get("entry_confirmation") or [])),
        "contract_condition_evaluation": contract_condition_evaluation,
        "invalidation_conditions": list(contract.get("invalidation_conditions") or []),
        "invalidation_conditions_cn": _conditions_cn(list(contract.get("invalidation_conditions") or [])),
        "reference_close": reference_close,
        "price_rule": "T+1 09:31-09:45 VWAP; no fixed numeric range is inferred",
        "suggested_max_amount": max_amount,
        "amount_semantics": "portfolio max-single-position cap, not committed allocation",
        "earliest_fill_date": earliest,
        "plan_validity": (
            f"from {earliest}, up to {PENDING_MAX_TRADING_DAYS} trading days"
            if earliest else "not_applicable"
        ),
        "skip_reason": skip_reason,
        "shadow_actionable": actionable,
        "no_trade_signal": True,
    }


def _read_rows(trade_date: str, run_id: str) -> tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], float]:
    with DBGateway(DB_PATH, read_only=True) as conn:
        audits_raw = conn.execute(
            """
            SELECT n.task_id, n.symbol,
                   COALESCE(NULLIF(TRIM(n.name), ''), NULLIF(TRIM(b.name), ''), n.symbol),
                   n.l4_final_verdict,
                   COALESCE(n.l4_final_score, n.final_score, n.l3_audit_score, 0),
                   COALESCE(n.l1_close, 0),
                   COALESCE(n.l4_news_status, ''), COALESCE(n.l4_news_gate, ''),
                   COALESCE(a.primary_archetype, 'UNCLASSIFIED')
            FROM nexus_audits n
            LEFT JOIN fact_stock_basic b ON b.symbol = n.symbol
            LEFT JOIN fact_trade_archetype_observations a
              ON a.task_id = n.task_id AND a.classifier_version = 'trade_archetype_v0.1'
            WHERE CAST(n.trade_date AS DATE) = CAST(? AS DATE)
              AND n.run_id = ?
              AND UPPER(COALESCE(n.l4_final_verdict, '')) IN ('PASS', 'APPROVE')
            ORDER BY COALESCE(n.l4_final_score, n.final_score, n.l3_audit_score, 0) DESC, n.symbol
            """,
            [trade_date, run_id],
        ).fetchall()
        audits = [
            dict(zip(
                ["task_id", "symbol", "name", "l4_final_verdict", "final_score", "l1_close", "l4_news_status", "l4_news_gate", "trade_archetype"],
                row,
            ))
            for row in audits_raw
        ]

        pending: Dict[str, Dict[str, Any]] = {}
        if _table_exists(conn, "fact_shadow_pending_signals"):
            for row in conn.execute(
                """
                SELECT task_id, status, CAST(earliest_fill_date AS VARCHAR), l1_close,
                       entry_tide_gate, trade_contract_json
                FROM fact_shadow_pending_signals
                WHERE run_id = ? AND CAST(signal_trade_date AS DATE) = CAST(? AS DATE)
                """,
                [run_id, trade_date],
            ).fetchall():
                pending[str(row[0])] = dict(zip(
                    ["task_id", "status", "earliest_fill_date", "l1_close", "entry_tide_gate", "trade_contract_json"], row
                ))

        skipped: Dict[str, Dict[str, Any]] = {}
        if _table_exists(conn, "fact_shadow_skipped_signals"):
            for row in conn.execute(
                """
                SELECT task_id, reason, entry_tide_gate, evidence_json
                FROM fact_shadow_skipped_signals
                WHERE run_id = ? AND CAST(trade_date AS DATE) = CAST(? AS DATE)
                """,
                [run_id, trade_date],
            ).fetchall():
                skipped[str(row[0])] = dict(zip(
                    ["task_id", "reason", "entry_tide_gate", "evidence_json"], row
                ))

        metric = conn.execute(
            "SELECT COALESCE(cash_reserve, 0) FROM shadow_metrics ORDER BY trade_date DESC LIMIT 1"
        ).fetchone()
        cash = float(metric[0] or 0) if metric else 0.0
    return audits, pending, skipped, cash


def render_markdown(payload: Dict[str, Any]) -> str:
    lines = [
        f"# Trade Action Plan · {payload['trade_date']}", "",
        f"- run_id: `{payload['run_id']}`",
        "- mode: Shadow dry-run lifecycle plan",
        "- no_trade_signal: true", "",
        "| 标的 | 计划状态 | 资格 | 新闻 | 潮汐 | 类型 | 参考价 | 最大金额 | 有效期 |",
        "|---|---|---|---|---|---|---:|---:|---|",
    ]
    for item in payload["items"]:
        lines.append(
            f"| {item['name']} {item['symbol']} | {item['plan_status_cn']} | {item['qualification_status']} | "
            f"{item['news_status_cn']} | {item['tide_status_cn']} / {item['sector_tide_status_cn']} | {item['trade_archetype_cn']} | "
            f"{item['reference_close']:.4f} | {item['suggested_max_amount']:.2f} | {item['plan_validity']} |"
        )
        if item["entry_conditions_cn"]:
            lines.append(f"\n进入条件：`{' / '.join(item['entry_conditions_cn'])}`")
        if item["invalidation_conditions_cn"]:
            lines.append(f"\n失效条件：`{' / '.join(item['invalidation_conditions_cn'])}`")
    lines.extend(["", "价格口径：T+1 09:31-09:45 VWAP；当前不推导未经验证的固定数值买入区间。", "", "本报告仅描述影子盘生命周期计划，不构成真实交易指令。"])
    return "\n".join(lines) + "\n"


def render_push(payload: Dict[str, Any]) -> tuple[str, str]:
    items = payload["items"]
    title = f"烛龙最终计划 | {payload['trade_date']} {len(items)}只"
    lines = ["[Shadow 安全门结果]", f"交易日：{payload['trade_date']}", ""]
    for index, item in enumerate(items[:8], 1):
        lines.append(
            f"{index}. {item['name']} {item['symbol']} | {item['plan_status_cn']} | "
            f"{item['trade_archetype_cn']} | {item['news_status_cn']} | {item['tide_status_cn']}"
        )
        lines.append(f"   板块：{item['sector']} | {item['sector_tide_status_cn']}（观察项）")
        if item["shadow_actionable"]:
            lines.append(
                f"   参考收盘:{item['reference_close']:.2f} | 最大金额上限:{item['suggested_max_amount']:,.0f} | "
                f"有效期:{item['plan_validity']}"
            )
            if item["entry_conditions_cn"]:
                pending_count = int((item["contract_condition_evaluation"].get("counts") or {}).get("UNVERIFIED_RUNTIME", 0))
                lines.append(f"   契约条件：{'；'.join(item['entry_conditions_cn'][:2])}（待运行验证 {pending_count} 项）")
            if item["invalidation_conditions_cn"]:
                lines.append(f"   失效条件：{'；'.join(item['invalidation_conditions_cn'][:2])}")
            lines.append("   价格范围：不预设固定区间，按 T+1 09:31-09:45 VWAP 条件成交")
    if len(items) > 8:
        lines.append(f"...另有 {len(items) - 8} 只见计划报告")
    lines.extend(["", "PASS 不是买入指令；只有“等待 T+1 进入条件”表示已通过当前安全门。", "价格按既有 T+1 VWAP 口径，未编造固定买入区间。"])
    return title, "\n".join(lines)


def build_action_plan(trade_date: str, run_id: str, output_dir: Path = REPORT_DIR) -> Dict[str, Any]:
    rules = load_shadow_rules()
    max_single_pct = float((rules.get("position") or {}).get("max_single_pct", 0.35))
    audits, pending, skipped, cash = _read_rows(trade_date, run_id)
    sector_tides = load_sector_tides(trade_date, [str(audit["symbol"]) for audit in audits])
    items = [
        compose_plan_item(
            audit,
            pending.get(str(audit["task_id"])),
            skipped.get(str(audit["task_id"])),
            cash_reserve=cash,
            max_single_pct=max_single_pct,
            sector_tide=sector_tides.get(str(audit["symbol"]).upper()),
        )
        for audit in audits
    ]
    payload = {
        "trade_date": str(trade_date),
        "run_id": str(run_id),
        "generated_at": datetime.now().astimezone().isoformat(),
        "mode": "shadow_dry_run_action_plan",
        "no_trade_signal": True,
        "cash_reserve_snapshot": cash,
        "max_single_pct": max_single_pct,
        "items": items,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = f"trade_action_plan_{str(trade_date).replace('-', '')}_{str(run_id)[:8]}"
    json_path = output_dir / f"{stem}.json"
    md_path = output_dir / f"{stem}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    md_path.write_text(render_markdown(payload), encoding="utf-8")
    payload["json_path"] = str(json_path)
    payload["markdown_path"] = str(md_path)
    return payload


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Render one read-only Shadow Trade Action Plan")
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output-dir", default=str(REPORT_DIR))
    args = parser.parse_args()
    payload = build_action_plan(args.trade_date, args.run_id, Path(args.output_dir))
    title, content = render_push(payload)
    print(json.dumps({
        "items": len(payload["items"]),
        "json_path": payload["json_path"],
        "markdown_path": payload["markdown_path"],
        "push_title": title,
        "push_preview": content,
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
