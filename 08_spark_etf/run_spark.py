from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

BASE_DIR = Path(__file__).resolve().parent
SRC_DIR = BASE_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from spark_etf.config.loader import load_spark_config
from spark_etf.db.db_init import DEFAULT_DB_PATH, init_db
from spark_etf.services.news_service import NewsService
from spark_etf.services.signal_service import SignalService


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value * 100:.2f}%"


def _event_count(conn: duckdb.DuckDBPyConnection) -> int:
    return int(conn.execute("SELECT COUNT(*) FROM spark_event_log").fetchone()[0])


def _collect_weekly_cloud_audits(db_path: Path) -> list[dict[str, Any]]:
    conn = duckdb.connect(str(db_path))
    try:
        rows = conn.execute(
            """
            SELECT
                etf_code,
                timestamp,
                json_extract_string(tushare_snapshot, '$.impact') AS local_impact,
                try_cast(json_extract(tushare_snapshot, '$.score') AS DOUBLE) AS local_score,
                json_extract_string(tushare_snapshot, '$.cloud_audit_feedback.provider') AS provider,
                json_extract_string(tushare_snapshot, '$.cloud_audit_feedback.impact_3_6m') AS cloud_impact,
                try_cast(json_extract(tushare_snapshot, '$.cloud_audit_feedback.score') AS DOUBLE) AS cloud_score,
                json_extract_string(tushare_snapshot, '$.cloud_audit_feedback.reason') AS cloud_reason
            FROM spark_event_log
            WHERE event_type = 'MACRO_SENTIMENT'
              AND timestamp >= now() - INTERVAL '7 days'
              AND json_extract(tushare_snapshot, '$.cloud_audit_feedback') IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT 80
            """
        ).fetchall()
    finally:
        conn.close()

    result: list[dict[str, Any]] = []
    for row in rows:
        result.append(
            {
                "etf_code": row[0],
                "timestamp": str(row[1]),
                "local_impact": row[2],
                "local_score": float(row[3]) if row[3] is not None else None,
                "provider": row[4],
                "cloud_impact": row[5],
                "cloud_score": float(row[6]) if row[6] is not None else None,
                "cloud_reason": row[7],
            }
        )
    return result


def _load_name_map() -> dict[str, str]:
    try:
        cfg = load_spark_config()
    except Exception:
        return {}
    return {item.etf_code: item.name for item in cfg.portfolios}


def _push_text_message(title: str, content: str) -> bool:
    pusher_path = BASE_DIR.parent / "utils" / "pusher.py"
    if not pusher_path.exists():
        return False

    try:
        spec = importlib.util.spec_from_file_location("spark_utils_pusher", str(pusher_path))
        if spec is None or spec.loader is None:
            return False
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        send_fn = getattr(mod, "send_push", None)
        if callable(send_fn):
            return bool(send_fn(title, content, template="txt"))
    except Exception:
        return False
    return False


def _state_label(state: str) -> str:
    mapping = {
        "ACCUMULATING": "🟢 正常吸筹",
        "FREE_RIDE": "🚀 利润放飞",
        "PAUSED_OVERVALUED": "⏸️ 高估停泊",
        "WATCHLIST": "🔍 观察清单",
    }
    return mapping.get(state, state)


def _recommendation(signal: dict[str, Any]) -> str:
    action = str(signal.get("action", ""))
    state = str(signal.get("new_state", ""))
    prev_state = str(signal.get("prev_state", ""))
    amount = signal.get("suggested_amount")
    amount_text = f"{float(amount):.0f} 元" if isinstance(amount, (int, float)) else "N/A"

    if action == "DCA_BUY":
        return f"定投 {amount_text}"
    if action == "PAUSE_SIGNAL" or state == "PAUSED_OVERVALUED":
        return "暂停，资金留存"
    if action == "STATE_TRANSITION" and state == "ACCUMULATING":
        return f"恢复吸筹，金额 {amount_text}"
    if action == "HARVEST_SELL" and prev_state == "ACCUMULATING":
        return "卖出回收本金"
    if action == "HARVEST_SELL":
        return "移动止盈锁利"
    if state == "FREE_RIDE":
        return "利润放飞持有"
    return "维持观望"


def _table_cell(value: object, width: int) -> str:
    text = str(value)
    if len(text) > width:
        return text[: max(0, width - 1)] + "..."
    return text


def _fmt_amount(value: object) -> str:
    if isinstance(value, (int, float)):
        return f"{float(value):.0f}"
    return "0"

def _safety_margin_text(signal: dict[str, Any]) -> str:
    state = str(signal.get("new_state", ""))
    current_return = signal.get("current_return")
    pe_percentile = signal.get("pe_percentile")
    drawdown = signal.get("drawdown")

    if state == "FREE_RIDE" and isinstance(drawdown, (int, float)):
        gap = 0.15 - float(drawdown)
        if gap >= 0:
            return f"距 15% 移动止盈线尚有 {gap * 100:.2f}% 空间"
        return f"已超过 15% 移动止盈线 {abs(gap) * 100:.2f}%，建议锁润"

    if state == "PAUSED_OVERVALUED" and isinstance(pe_percentile, (int, float)):
        gap = float(pe_percentile) - 0.70
        if gap > 0:
            return f"估值较恢复线高 {gap * 100:.2f}%，继续停泊"
        return f"估值已回到恢复区间，距离约 {abs(gap) * 100:.2f}%"

    if isinstance(current_return, (int, float)):
        gap = 0.30 - float(current_return)
        if gap > 0:
            return f"距离 30% 本金回收线还有 {gap * 100:.2f}%"
        return f"已突破 30% 本金回收线 {abs(gap) * 100:.2f}%"

    if isinstance(pe_percentile, (int, float)):
        gap = 0.85 - float(pe_percentile)
        if gap >= 0:
            return f"距离 85% 高估阶段线尚有 {gap * 100:.2f}%"
        return f"已高于 85% 高估线 {abs(gap) * 100:.2f}%"

    return "暂无足够数据评估安全边际"


def _note_to_cn(note: str) -> str:
    if not note:
        return "无额外备注"

    mapping = {
        "PE percentile >= 0.85, switch to PAUSED_OVERVALUED": "估值分位达到高估区，转入停泊状态",
        "Return >= 30%, harvest principal and switch to FREE_RIDE": "收益率突破 30%，建议回收本金转入利润放飞",
        "Continue DCA accumulation": "估值仍在合理区间，继续定投",
        "PE percentile <= 0.70, resume ACCUMULATING": "估值回落至吸筹区间，恢复定投",
        "Remain paused, capital stays in bank account": "高估状态未解除，资金继续留存银行",
        "FREE_RIDE drawdown >= 15%, lock profits": "利润放飞阶段回撤达到 15%，建议锁定利润",
        "FREE_RIDE holding, drawdown below 15%": "利润放飞阶段回撤未达止盈线，继续持有",
    }
    return mapping.get(note, note)


def _build_text_report(
    signals: list[dict[str, Any]],
    audit: dict[str, Any],
    macro_hotspots: dict[str, str],
    news_summary: dict[str, Any],
    weekly_cloud: list[dict[str, Any]],
) -> str:
    name_map = _load_name_map()

    lines: list[str] = []
    lines.append("=== 烛龙·星火 ETF 周度持仓行动表 ===")
    lines.append(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"资产规模: 共计 {len(signals)} 只标的")
    lines.append("")
    lines.append("| 基金 | 状态 | 本期动作 | 建议金额 | 估值分位 | 收益率 | 下一触发线 | 数据质量 |")
    lines.append("| --- | --- | --- | ---: | ---: | ---: | --- | --- |")

    for signal in signals:
        code = str(signal.get("etf_code", ""))
        name = name_map.get(code, "未知标的")
        fund = _table_cell(f"{name}({code})", 24)
        state = _table_cell(_state_label(str(signal.get("new_state", ""))), 12)
        action = _table_cell(_recommendation(signal), 14)
        amount = _fmt_amount(signal.get("suggested_amount"))
        pe = _fmt_pct(signal.get("pe_percentile"))
        ret = _fmt_pct(signal.get("current_return"))
        trigger = _table_cell(signal.get("next_trigger", "N/A"), 24)
        quality = _table_cell(signal.get("data_quality", "N/A"), 24)
        lines.append(f"| {fund} | {state} | {action} | {amount} | {pe} | {ret} | {trigger} | {quality} |")

    lines.append("")
    lines.append("行动依据：")
    for signal in signals:
        code = str(signal.get("etf_code", ""))
        name = name_map.get(code, code)
        reason = signal.get("action_reason") or _note_to_cn(str(signal.get("note", "")))
        risk_flags = signal.get("risk_flags") or []
        risk_text = f" | 风险标记: {', '.join(str(item) for item in risk_flags)}" if risk_flags else ""
        lines.append(f"- {name}: {reason}{risk_text}")
        if code in macro_hotspots:
            lines.append(f"  宏观辅助: {macro_hotspots[code]}")

    lines.append("")
    lines.append("----------------------------------")
    lines.append("[云端首席审计官：DeepSeek/Kimi]")
    lines.append("近7天深度叙事审计：")

    if weekly_cloud:
        for row in weekly_cloud[:20]:
            code = str(row.get("etf_code", ""))
            name = name_map.get(code, code)
            reason = str(row.get("cloud_reason") or "暂无云端理由")
            score_raw = row.get("cloud_score")
            if isinstance(score_raw, (int, float)):
                score_text = f"{float(score_raw):.1f}/10"
            else:
                score_text = "N/A"
            lines.append(f"- {name}: {reason} (评分: {score_text})")
    else:
        lines.append("- 本周无云端二次会审记录")

    lines.append("----------------------------------")
    lines.append(f"安全审计结论: {'PASS' if audit['signal_audit_pass'] else 'FAIL'}")
    lines.append(f"本周操作事件入库: {audit['signal_events_created']} 条")
    lines.append(
        "新闻审计摘要: "
        f"拉取 {news_summary.get('raw_items', 0)} 条 | "
        f"命中 {news_summary.get('matched_items', 0)} 条 | "
        f"云端会审 {news_summary.get('cloud_verified', 0)} 条"
    )

    return "\n".join(lines)
def main() -> int:
    parser = argparse.ArgumentParser(description="Spark weekly runner")
    parser.add_argument("--force-push", action="store_true", help="Force sending full report push")
    args = parser.parse_args()

    db_path = init_db(DEFAULT_DB_PATH)
    signal_service = SignalService(db_path=db_path)

    conn = duckdb.connect(str(db_path))
    try:
        before_signal_count = _event_count(conn)
    finally:
        conn.close()

    signals = signal_service.generate_signals()

    conn = duckdb.connect(str(db_path))
    try:
        after_signal_count = _event_count(conn)
    finally:
        conn.close()

    persisted_count = sum(1 for item in signals if bool(item.get("persisted")))
    signal_events_created = after_signal_count - before_signal_count
    signal_audit_pass = signal_events_created == len(signals) and persisted_count == len(signals)

    news_summary: dict[str, Any] = {
        "raw_items": 0,
        "matched_items": 0,
        "audited_items": 0,
        "cloud_verified": 0,
        "persisted_events": 0,
        "errors": {},
    }
    macro_hotspots: dict[str, str] = {}

    conn = duckdb.connect(str(db_path))
    try:
        before_macro_count = _event_count(conn)
    finally:
        conn.close()

    try:
        news_service = NewsService(db_path=db_path, ollama_timeout=30)
        news_summary = news_service.run_daily_pipeline(hours=24, alert_threshold=8, enable_alert=True)
        macro_hotspots = news_service.get_hot_wind_annotations(days=3, min_score=7.0)
    except Exception as exc:
        news_summary = {
            "raw_items": 0,
            "matched_items": 0,
            "audited_items": 0,
            "cloud_verified": 0,
            "persisted_events": 0,
            "errors": {"news_service": str(exc)},
        }

    conn = duckdb.connect(str(db_path))
    try:
        after_macro_count = _event_count(conn)
    finally:
        conn.close()

    macro_events_created = after_macro_count - before_macro_count
    weekly_cloud = _collect_weekly_cloud_audits(db_path)

    audit = {
        "signal_count": len(signals),
        "persisted_count": persisted_count,
        "signal_events_created": signal_events_created,
        "signal_event_count_before": before_signal_count,
        "signal_event_count_after": after_signal_count,
        "signal_audit_pass": signal_audit_pass,
        "macro_events_created": macro_events_created,
        "macro_event_count_before": before_macro_count,
        "macro_event_count_after": after_macro_count,
        "weekly_cloud_count": len(weekly_cloud),
    }

    report_text = _build_text_report(
        signals=signals,
        audit=audit,
        macro_hotspots=macro_hotspots,
        news_summary=news_summary,
        weekly_cloud=weekly_cloud,
    )
    print(report_text)

    logs_dir = BASE_DIR / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    text_path = logs_dir / f"weekly_audit_{ts}.txt"
    json_path = logs_dir / f"weekly_audit_{ts}.json"

    text_path.write_text(report_text, encoding="utf-8")
    json_path.write_text(
        json.dumps(
            {
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "db_path": str(db_path),
                "audit": audit,
                "news_summary": news_summary,
                "macro_hotspots": macro_hotspots,
                "weekly_cloud": weekly_cloud,
                "signals": signals,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    should_push = args.force_push or datetime.now().weekday() == 5
    push_sent = False
    if should_push:
        push_sent = _push_text_message("[星火系统周报]", report_text)
        print(f"weekly_push: {'sent' if push_sent else 'failed'}")

    print(f"\nreport_file: {text_path}")
    print(f"json_log: {json_path}")

    return 0 if signal_audit_pass else 2


if __name__ == "__main__":
    raise SystemExit(main())
