#!/usr/bin/env python3
"""Read-only Tushare top_inst observer for Dragon-Tiger seat evidence."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List

import duckdb
import tushare as ts
from dotenv import load_dotenv

ROOT = Path("/root/quant_project")
DB_PATH = ROOT / "storage/database/zhulong.duckdb"
REPORT_DIR = ROOT / "storage/reports/lhb_seat_observer"
RETAIL_PROXY_PATTERNS = ("拉萨", "团结路", "东环路", "江苏大道")


def _float(value: Any) -> float:
    try:
        return float(value or 0)
    except Exception:
        return 0.0


def load_risk_patterns(path: str | None) -> List[str]:
    if not path:
        return []
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(file_path)
    return [line.strip() for line in file_path.read_text(encoding="utf-8").splitlines() if line.strip() and not line.lstrip().startswith("#")]


def analyze_seats(symbol: str, rows: Iterable[Dict[str, Any]], risk_patterns: List[str] | None = None) -> Dict[str, Any]:
    items = [dict(row) for row in rows]
    risk_patterns = list(risk_patterns or [])
    if not items:
        return {
            "symbol": symbol,
            "status": "NO_LHB_DETAIL",
            "manual_review_required": False,
            "generate_trade": False,
            "seat_count": 0,
            "warnings": [],
            "seats": [],
        }

    institution = []
    retail_proxy = []
    explicit_matches = []
    normalized = []
    for row in items:
        name = str(row.get("exalter") or "").strip()
        item = {
            "seat_name": name,
            "side": str(row.get("side") or ""),
            "buy": _float(row.get("buy")),
            "sell": _float(row.get("sell")),
            "net_buy": _float(row.get("net_buy")),
            "buy_rate": _float(row.get("buy_rate")),
            "sell_rate": _float(row.get("sell_rate")),
            "reason": str(row.get("reason") or ""),
        }
        item["institution_seat"] = name == "机构专用" or name.startswith("机构专用")
        item["retail_hot_seat_proxy"] = any(pattern in name for pattern in RETAIL_PROXY_PATTERNS)
        item["explicit_risk_pattern_matches"] = [pattern for pattern in risk_patterns if re.search(pattern, name)]
        if item["institution_seat"]:
            institution.append(item)
        if item["retail_hot_seat_proxy"]:
            retail_proxy.append(item)
        if item["explicit_risk_pattern_matches"]:
            explicit_matches.append(item)
        normalized.append(item)

    total_buy = sum(item["buy"] for item in normalized)
    total_sell = sum(item["sell"] for item in normalized)
    institution_net = sum(item["net_buy"] for item in institution)
    warnings = []
    if explicit_matches:
        warnings.append("EXPLICIT_RISK_PATTERN_MATCH")
    if len(retail_proxy) >= 2:
        warnings.append("RETAIL_HOT_SEAT_CLUSTER_PROXY")
    if institution and institution_net < 0:
        warnings.append("INSTITUTION_NET_SELL")
    if max((item["sell_rate"] for item in normalized), default=0) >= 15:
        warnings.append("SINGLE_SEAT_SELL_CONCENTRATION")

    status = "KNOWN_RISK_PATTERN_REVIEW" if explicit_matches else ("LHB_CAUTION_REVIEW" if warnings else "LHB_PRESENT_REVIEW")
    return {
        "symbol": symbol,
        "status": status,
        "manual_review_required": True,
        "generate_trade": False,
        "seat_count": len(normalized),
        "total_buy": round(total_buy, 2),
        "total_sell": round(total_sell, 2),
        "net_buy": round(total_buy - total_sell, 2),
        "institution_seat_count": len(institution),
        "institution_net_buy": round(institution_net, 2),
        "retail_hot_seat_proxy_count": len(retail_proxy),
        "explicit_risk_match_count": len(explicit_matches),
        "max_buy_rate": round(max((item["buy_rate"] for item in normalized), default=0), 4),
        "max_sell_rate": round(max((item["sell_rate"] for item in normalized), default=0), 4),
        "warnings": warnings,
        "seats": normalized,
    }


def resolve_symbols(trade_date: str, run_id: str, raw_symbols: str) -> List[str]:
    symbols = [value.strip().upper() for value in raw_symbols.split(",") if value.strip()]
    if symbols:
        return sorted(set(symbols))
    if not run_id:
        raise ValueError("provide --symbols or --run-id")
    with duckdb.connect(str(DB_PATH), read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT symbol
            FROM nexus_audits
            WHERE CAST(trade_date AS DATE) = CAST(? AS DATE)
              AND run_id = ?
              AND UPPER(COALESCE(l4_final_verdict, '')) IN ('PASS', 'APPROVE')
            ORDER BY symbol
            """,
            [trade_date, run_id],
        ).fetchall()
    return [str(row[0]).upper() for row in rows]


def render_markdown(report: Dict[str, Any]) -> str:
    lines = [
        f"# Dragon-Tiger Seat Observer · {report['trade_date']}", "",
        "- source: Tushare top_inst",
        "- observer_only: true",
        "- generate_trade: false", "",
        "| 标的 | 状态 | 席位数 | 机构净买 | 活跃席位代理 | 明确风险名单命中 | 警告 |",
        "|---|---|---:|---:|---:|---:|---|",
    ]
    for item in report["items"]:
        lines.append(
            f"| {item['symbol']} | {item['status']} | {item['seat_count']} | "
            f"{item.get('institution_net_buy', 0):.2f} | {item.get('retail_hot_seat_proxy_count', 0)} | "
            f"{item.get('explicit_risk_match_count', 0)} | {' / '.join(item.get('warnings') or []) or '无'} |"
        )
    lines.extend(["", "活跃席位代理仅用于人工复核，不等于恶意席位或确定性风险。", ""])
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only top_inst seat-risk observer")
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--run-id", default="")
    parser.add_argument("--symbols", default="")
    parser.add_argument("--risk-patterns-file", default="")
    parser.add_argument("--output-dir", default=str(REPORT_DIR))
    args = parser.parse_args()

    trade_date = args.trade_date.replace("-", "")[:8]
    display_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}"
    symbols = resolve_symbols(display_date, args.run_id, args.symbols)
    patterns = load_risk_patterns(args.risk_patterns_file or None)
    load_dotenv(ROOT / ".env")
    token = str(os.getenv("TUSHARE_TOKEN") or "").strip()
    if not token:
        raise RuntimeError("TUSHARE_TOKEN missing")
    pro = ts.pro_api(token)
    items = []
    warnings = []
    for symbol in symbols:
        try:
            frame = pro.top_inst(trade_date=trade_date, ts_code=symbol)
            rows = frame.to_dict("records") if frame is not None and len(frame) else []
            items.append(analyze_seats(symbol, rows, patterns))
        except Exception as exc:
            items.append({
                "symbol": symbol, "status": "TOP_INST_UNAVAILABLE", "manual_review_required": True,
                "generate_trade": False, "seat_count": 0, "warnings": [type(exc).__name__], "seats": [],
            })
            warnings.append(f"{symbol}:{type(exc).__name__}:{str(exc)[:160]}")

    report = {
        "trade_date": display_date,
        "run_id": args.run_id,
        "generated_at": datetime.now().astimezone().isoformat(),
        "source": "tushare.top_inst",
        "observer_only": True,
        "no_trade_signal": True,
        "risk_patterns_source": args.risk_patterns_file or "none",
        "items": items,
        "warnings": warnings,
        "blocked_actions": ["write_duckdb", "block_shadow", "generate_trade", "change_l4_verdict"],
    }
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = args.run_id[:8] or "manual"
    stem = f"lhb_seat_observer_{trade_date}_{suffix}"
    json_path = output_dir / f"{stem}.json"
    md_path = output_dir / f"{stem}.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    md_path.write_text(render_markdown(report), encoding="utf-8")
    print(json.dumps({"json": str(json_path), "markdown": str(md_path), **report}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
