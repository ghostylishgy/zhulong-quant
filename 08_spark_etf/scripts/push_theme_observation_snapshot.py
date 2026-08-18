from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb

BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = BASE_DIR / "data" / "spark.duckdb"
DEFAULT_MARKER_DIR = BASE_DIR / "logs"


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _load_json(value: object, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (list, dict)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return default


def _send_push(title: str, content: str) -> bool:
    pusher_path = BASE_DIR.parents[0] / "utils" / "pusher.py"
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


def _latest_observation_date(conn: duckdb.DuckDBPyConnection) -> date | None:
    row = conn.execute("SELECT max(observation_date) FROM spark_theme_observation_daily").fetchone()
    return row[0] if row and row[0] is not None else None


def _load_observations(conn: duckdb.DuckDBPyConnection, observation_date: date) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            theme,
            bucket_status,
            total_fund_count,
            non_holding_count,
            current_holding_count,
            name_only_count,
            index_verified_count,
            holding_verified_count,
            unknown_count,
            sample_funds_json
        FROM spark_theme_observation_daily
        WHERE observation_date = ?
        ORDER BY non_holding_count DESC, theme
        """,
        [observation_date],
    ).fetchall()

    result: list[dict[str, Any]] = []
    for row in rows:
        result.append(
            {
                "theme": str(row[0]),
                "bucket_status": str(row[1]),
                "total_fund_count": int(row[2]),
                "non_holding_count": int(row[3]),
                "current_holding_count": int(row[4]),
                "name_only_count": int(row[5]),
                "index_verified_count": int(row[6]),
                "holding_verified_count": int(row[7]),
                "unknown_count": int(row[8]),
                "sample_funds": _load_json(row[9], []),
            }
        )
    return result


def _sample_text(sample_funds: list[dict[str, Any]], limit: int = 2) -> str:
    names = [str(item.get("fund_name") or item.get("fund_code")) for item in sample_funds[:limit]]
    if not names:
        return "暂无样本"
    return "、".join(name[:16] for name in names)


def build_content(db_path: Path, observation_date: date | None = None, top_n: int = 12) -> tuple[str, str]:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        effective_date = observation_date or _latest_observation_date(conn)
        if effective_date is None:
            title = "星火ETF｜观察桶三周复盘"
            return title, "【观察结论】\n暂无观察桶数据，本次不产生方向判断。"
        observations = _load_observations(conn, effective_date)
    finally:
        conn.close()

    discovered = [item for item in observations if item["bucket_status"] == "DISCOVERED"]
    focus_ready = [
        item
        for item in discovered
        if item["index_verified_count"] > 0 or item["holding_verified_count"] > 0
    ]
    title = "星火ETF｜观察桶三周复盘"
    lines = [
        "【观察结论】",
        f"观察日期：{effective_date.isoformat()}",
        f"观察主题：{len(observations)} 个",
        f"已发现方向：{len(discovered)} 个",
        f"已验证方向：{len(focus_ready)} 个",
        "建议级别：仅供观察，不给买入金额，不影响已有持仓买卖建议",
        "",
        "【重要说明】",
        "当前主题主要来自基金名关键词召回，多数仍是 NAME_ONLY。",
        "这代表“可观察线索”，不是已验证低估或 alpha 结论。",
        "",
        "【观察桶主题】",
        "主题｜状态｜未持仓｜持仓重叠｜证据｜样本",
    ]

    for item in observations[:top_n]:
        evidence = (
            f"名称{item['name_only_count']}"
            f"/指数{item['index_verified_count']}"
            f"/持仓{item['holding_verified_count']}"
        )
        lines.append(
            "｜".join(
                [
                    item["theme"],
                    item["bucket_status"],
                    str(item["non_holding_count"]),
                    str(item["current_holding_count"]),
                    evidence,
                    _sample_text(item["sample_funds"]),
                ]
            )
        )

    lines.extend(
        [
            "",
            "【后续动作】",
            "1. 持仓线继续只做已有持仓的持有/止盈/卖出复核。",
            "2. 观察桶继续沉淀，不自动升级为买入候选。",
            "3. 下一步应补主题验证和趋势信号，再决定哪些方向进入重点观察。",
        ]
    )
    return title, "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Push one-time Spark ETF theme observation snapshot")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH, help="DuckDB path")
    parser.add_argument("--date", type=_parse_date, default=None, help="Observation date to render; latest if omitted")
    parser.add_argument("--run-on", type=_parse_date, default=None, help="Only run on this wall-clock date")
    parser.add_argument("--today", type=_parse_date, default=None, help="Test override for wall-clock date")
    parser.add_argument("--top-n", type=int, default=12, help="Number of themes to include")
    parser.add_argument("--marker-dir", type=Path, default=DEFAULT_MARKER_DIR, help="Directory for one-shot marker")
    parser.add_argument("--no-push", action="store_true", help="Print only; do not send pushplus message")
    args = parser.parse_args()

    today = args.today or date.today()
    if args.run_on is not None and today != args.run_on:
        print(f"skip: today={today.isoformat()} run_on={args.run_on.isoformat()}")
        return 0

    marker_date = args.run_on or today
    marker = args.marker_dir / f"theme_observation_push_{marker_date.isoformat()}.sent"
    if marker.exists() and not args.no_push:
        print(f"skip: marker_exists={marker}")
        return 0

    title, content = build_content(args.db, observation_date=args.date, top_n=args.top_n)
    pushed = False if args.no_push else _send_push(title, content)
    print(content)
    print(f"\npush_status: {'skipped' if args.no_push else ('sent' if pushed else 'failed')}")
    if pushed:
        args.marker_dir.mkdir(parents=True, exist_ok=True)
        marker.write_text(datetime.now().isoformat() + "\n", encoding="utf-8")
    return 0 if args.no_push or pushed else 2


if __name__ == "__main__":
    raise SystemExit(main())
