#!/usr/bin/env python3
"""Render a read-only, non-annualized Shadow performance tear sheet."""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SHADOW_LIB = PROJECT_ROOT / "05_shadow" / "lib"
DEFAULT_DB = PROJECT_ROOT / "storage" / "database" / "zhulong.duckdb"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "storage" / "reports" / "shadow_performance"
SCHEMA_VERSION = "shadow_performance_tear_sheet_v0.1"
MIN_DESCRIPTIVE_TRADES = 30

if str(SHADOW_LIB) not in sys.path:
    sys.path.insert(0, str(SHADOW_LIB))

from performance import closed_trade_rows  # noqa: E402
from portfolio_metrics import compute_shadow_equity_curve  # noqa: E402


def canonical_sha(payload: dict[str, Any]) -> str:
    body = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def parse_date(value: str) -> date:
    return date.fromisoformat(str(value).strip()[:10])


def max_streak(trades: Iterable[dict[str, Any]], predicate) -> int:
    longest = 0
    current = 0
    for trade in trades:
        if predicate(float(trade.get("net_pnl") or 0)):
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def drawdown_profile(curve: list[dict[str, Any]]) -> dict[str, Any]:
    max_ratio = max((float(row.get("daily_drawdown") or 0) for row in curve), default=0.0)
    periods: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    for row in curve:
        ratio = float(row.get("daily_drawdown") or 0)
        trade_date = str(row.get("trade_date") or "")
        if ratio > 0:
            if current is None:
                current = {
                    "start_date": trade_date,
                    "end_date": trade_date,
                    "sessions": 0,
                    "peak_drawdown_ratio": 0.0,
                    "recovered_on": None,
                }
            current["end_date"] = trade_date
            current["sessions"] += 1
            current["peak_drawdown_ratio"] = max(
                current["peak_drawdown_ratio"], ratio
            )
        elif current is not None:
            current["recovered_on"] = trade_date
            periods.append(current)
            current = None
    if current is not None:
        periods.append(current)
    longest = max(
        periods,
        key=lambda item: (item["sessions"], item["peak_drawdown_ratio"]),
        default=None,
    )
    return {
        "max_drawdown_ratio": round(max_ratio, 6),
        "max_drawdown_duration_sessions": int(longest["sessions"]) if longest else 0,
        "max_drawdown_period": longest,
        "current_underwater": bool(curve and float(curve[-1].get("daily_drawdown") or 0) > 0),
    }


def exit_rule_breakdown(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = {}
    for trade in trades:
        key = str(trade.get("sell_rule") or "UNSPECIFIED")
        buckets.setdefault(key, []).append(trade)
    result = []
    for rule, rows in sorted(buckets.items()):
        wins = sum(1 for row in rows if float(row.get("net_pnl") or 0) > 0)
        total = sum(float(row.get("net_pnl") or 0) for row in rows)
        result.append(
            {
                "sell_rule": rule,
                "closed_trades": len(rows),
                "win_rate": round(wins / len(rows), 4),
                "net_pnl_amount": round(total, 2),
                "avg_net_pnl_amount": round(total / len(rows), 2),
            }
        )
    return result


def build_report(conn: Any, as_of: str) -> dict[str, Any]:
    as_of_date = parse_date(as_of)
    trades = [
        trade
        for trade in closed_trade_rows(conn)
        if trade.get("exit_date") and parse_date(trade["exit_date"]) <= as_of_date
    ]
    trades.sort(key=lambda item: (item.get("exit_date") or "", item.get("symbol") or ""))
    curve = compute_shadow_equity_curve(conn, through_date=as_of_date)

    wins = [item for item in trades if float(item.get("net_pnl") or 0) > 0]
    losses = [item for item in trades if float(item.get("net_pnl") or 0) < 0]
    breakeven = [item for item in trades if float(item.get("net_pnl") or 0) == 0]
    gross_profit = sum(float(item["net_pnl"]) for item in wins)
    gross_loss = abs(sum(float(item["net_pnl"]) for item in losses))
    avg_win = gross_profit / len(wins) if wins else 0.0
    avg_loss = -gross_loss / len(losses) if losses else 0.0
    total_net = gross_profit - gross_loss
    average_equity = statistics.fmean(
        float(row.get("total_equity") or 0) for row in curve
    ) if curve else 0.0
    traded_notional = sum(
        float(item.get("entry_total_cost") or 0)
        + float(item.get("realized_gross_amount") or 0)
        for item in trades
    )
    active_sessions = sum(1 for row in curve if int(row.get("active_positions") or 0) > 0)
    warnings = []
    if len(trades) < MIN_DESCRIPTIVE_TRADES:
        warnings.append(
            f"INSUFFICIENT_SAMPLE:closed_trades={len(trades)}:minimum={MIN_DESCRIPTIVE_TRADES}"
        )
    if not curve:
        warnings.append("EQUITY_CURVE_UNAVAILABLE")
    if not losses and trades:
        warnings.append("PROFIT_FACTOR_UNAVAILABLE:NO_LOSS_TRADES")
    if not wins and trades:
        warnings.append("PAYOFF_RATIO_UNAVAILABLE:NO_WIN_TRADES")

    sample_status = (
        "NO_CLOSED_TRADES"
        if not trades
        else "DESCRIPTIVE_READY"
        if len(trades) >= MIN_DESCRIPTIVE_TRADES
        else "INSUFFICIENT_SAMPLE"
    )
    quality_status = "DATA_OK" if trades and curve else "VALID_EMPTY" if not trades and not curve else "PARTIAL_DATA"
    drawdown = drawdown_profile(curve)
    trade_metrics = {
        "closed_trades": len(trades),
        "win_trades": len(wins),
        "loss_trades": len(losses),
        "breakeven_trades": len(breakeven),
        "win_rate": round(len(wins) / len(trades), 4) if trades else 0.0,
        "gross_profit_amount": round(gross_profit, 2),
        "gross_loss_amount": round(gross_loss, 2),
        "net_pnl_amount": round(total_net, 2),
        "profit_factor": round(gross_profit / gross_loss, 4) if gross_loss else None,
        "avg_win_amount": round(avg_win, 2),
        "avg_loss_amount": round(avg_loss, 2),
        "payoff_ratio": round(avg_win / abs(avg_loss), 4) if avg_win and avg_loss else None,
        "expectancy_amount": round(total_net / len(trades), 2) if trades else 0.0,
        "median_trade_pnl_amount": round(
            statistics.median(float(item["net_pnl"]) for item in trades), 2
        ) if trades else 0.0,
        "max_consecutive_wins": max_streak(trades, lambda value: value > 0),
        "max_consecutive_losses": max_streak(trades, lambda value: value < 0),
        "largest_winner_profit_share": round(
            max((float(item["net_pnl"]) for item in wins), default=0.0) / gross_profit,
            4,
        ) if gross_profit else None,
    }
    exposure_metrics = {
        "curve_sessions": len(curve),
        "active_position_sessions": active_sessions,
        "position_day_exposure_ratio": round(active_sessions / len(curve), 4) if curve else 0.0,
        "average_active_positions": round(
            statistics.fmean(int(row.get("active_positions") or 0) for row in curve), 4
        ) if curve else 0.0,
        "max_active_positions": max(
            (int(row.get("active_positions") or 0) for row in curve), default=0
        ),
        "closed_trade_notional_turnover_proxy": round(
            traded_notional / average_equity, 6
        ) if average_equity > 0 else None,
        "turnover_proxy_definition": "(closed_entry_total_cost + closed_exit_gross_amount) / average_equity",
    }
    report = {
        "schema_version": SCHEMA_VERSION,
        "as_of": as_of_date.isoformat(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": "read_only_descriptive_review",
        "observer_only": True,
        "no_trade_signal": True,
        "quality_status": quality_status,
        "sample_status": sample_status,
        "minimum_descriptive_trades": MIN_DESCRIPTIVE_TRADES,
        "source_semantics": "CURRENT_SHADOW_LEDGER_RETROSPECTIVE_AS_OF",
        "trade_metrics": trade_metrics,
        "drawdown_metrics": drawdown,
        "exposure_metrics": exposure_metrics,
        "exit_rule_breakdown": exit_rule_breakdown(trades),
        "warnings": warnings,
        "known_limitations": [
            "historical_partial_exits_are_recognized_on_the_final_exit_date",
            "unapplied_corporate_action_receivables_are_not_added_to_equity",
            "turnover_is_a_closed_trade_notional_proxy",
        ],
        "annualized_metrics_omitted": ["sharpe", "sortino", "calmar", "annualized_return"],
        "blocked_actions": [
            "write_duckdb",
            "change_shadow_state",
            "change_strategy_parameter",
            "write_rag_memory",
            "write_nexus_audits",
            "trigger_daemon",
        ],
    }
    report["payload_sha256"] = canonical_sha(
        {key: value for key, value in report.items() if key not in {"generated_at", "payload_sha256"}}
    )
    return report


def render_markdown(report: dict[str, Any]) -> str:
    trade = report["trade_metrics"]
    drawdown = report["drawdown_metrics"]
    exposure = report["exposure_metrics"]
    lines = [
        f"# Shadow 绩效复盘 · {report['as_of']}",
        "",
        f"- quality_status: `{report['quality_status']}`",
        f"- sample_status: `{report['sample_status']}`",
        "- mode: `read_only_descriptive_review`",
        "- no_trade_signal: `true`",
        f"- payload_sha256: `{report['payload_sha256']}`",
        "",
        "## 已平仓交易结构",
        "",
        "| Closed | Win Rate | Net PnL | Profit Factor | Payoff | Expectancy | Max Loss Streak |",
        "|---:|---:|---:|---:|---:|---:|---:|",
        f"| {trade['closed_trades']} | {trade['win_rate']:.2%} | {trade['net_pnl_amount']:.2f} | "
        f"{trade['profit_factor'] if trade['profit_factor'] is not None else 'N/A'} | "
        f"{trade['payoff_ratio'] if trade['payoff_ratio'] is not None else 'N/A'} | "
        f"{trade['expectancy_amount']:.2f} | {trade['max_consecutive_losses']} |",
        "",
        "## 回撤与持仓暴露",
        "",
        f"- max_drawdown_ratio: `{drawdown['max_drawdown_ratio']:.2%}`",
        f"- max_drawdown_duration_sessions: `{drawdown['max_drawdown_duration_sessions']}`",
        f"- current_underwater: `{str(drawdown['current_underwater']).lower()}`",
        f"- position_day_exposure_ratio: `{exposure['position_day_exposure_ratio']:.2%}`",
        f"- average_active_positions: `{exposure['average_active_positions']}`",
        f"- closed_trade_notional_turnover_proxy: `{exposure['closed_trade_notional_turnover_proxy']}`",
        "",
        "## 卖出规则分组",
        "",
        "| Sell Rule | Closed | Win Rate | Net PnL | Average PnL |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in report["exit_rule_breakdown"]:
        lines.append(
            f"| {row['sell_rule']} | {row['closed_trades']} | {row['win_rate']:.2%} | "
            f"{row['net_pnl_amount']:.2f} | {row['avg_net_pnl_amount']:.2f} |"
        )
    if not report["exit_rule_breakdown"]:
        lines.append("| - | 0 | 0.00% | 0.00 | 0.00 |")
    lines.extend(["", "## 数据警告", ""])
    lines.extend(f"- `{warning}`" for warning in report["warnings"])
    if not report["warnings"]:
        lines.append("- none")
    lines.extend(
        [
            "",
            "## 安全边界",
            "",
            "本报告只做回顾性描述，不写 DuckDB、不改变 Shadow 状态、不调整参数、"
            "不生成交易信号。样本量和观察跨度成熟前，故意不输出年化 Sharpe、"
            "Sortino、Calmar 或年化收益率。",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--write-artifacts", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    import duckdb

    with duckdb.connect(str(args.db), read_only=True) as conn:
        report = build_report(conn, args.as_of)
    if not args.write_artifacts:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0
    args.output_dir.mkdir(parents=True, exist_ok=True)
    stem = f"shadow_performance_tear_sheet_{report['as_of'].replace('-', '')}"
    json_path = args.output_dir / f"{stem}.json"
    markdown_path = args.output_dir / f"{stem}.md"
    json_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    markdown_path.write_text(render_markdown(report), encoding="utf-8")
    print(json_path)
    print(markdown_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
