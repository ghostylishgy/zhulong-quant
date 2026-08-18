"""Independent push summaries for the US radar sidecar."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .reports import (
    DIRECTION_LABELS,
    DIRECTION_SOURCE_LABELS,
    EVENT_LABELS,
    MARKET_LABELS,
    SIGNAL_LABELS,
    THEME_LABELS,
    TRANSMISSION_LABELS,
    VALIDATION_LABELS,
    _label,
)
from .storage import EventStore


SIGNIFICANT_LABELS = {"single_event_signal", "short_window_signal", "medium_window_signal"}
DISCLAIMER = "研究观察，非交易信号，不构成任何买卖建议。"


@dataclass(frozen=True)
class PushMessage:
    title: str
    content: str


def build_daily_push(store: EventStore, root: Path, hours: int = 24, limit: int = 5) -> PushMessage:
    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=hours)
    since_text = since.isoformat()
    with store.connect() as conn:
        event_counts = conn.execute(
            """
            SELECT event_type, COUNT(*)
            FROM us_events
            WHERE event_time >= ?
            GROUP BY event_type
            ORDER BY event_type
            """,
            (since_text,),
        ).fetchall()
        high_quality = conn.execute(
            """
            SELECT quality_class, COUNT(*)
            FROM event_quality q
            JOIN us_events e ON e.event_id = q.event_id
            WHERE e.event_time >= ?
              AND q.quality_class IN ('form4_open_market', '8k_high_signal')
            GROUP BY quality_class
            ORDER BY quality_class
            """,
            (since_text,),
        ).fetchall()
        validation_quality = conn.execute(
            """
            SELECT COALESCE(quality_gate, 'unclassified') AS quality_gate,
                   data_quality,
                   COUNT(*)
            FROM signal_validation_results
            WHERE updated_at >= ? OR measured_at >= ?
            GROUP BY COALESCE(quality_gate, 'unclassified'), data_quality
            ORDER BY quality_gate, data_quality
            """,
            (since_text, since_text),
        ).fetchall()
        chain_quality = conn.execute(
            """
            SELECT COUNT(*) AS total_units,
                   SUM(CASE WHEN cv.data_quality = 'DATA_OK' THEN 1 ELSE 0 END) AS effective_units,
                   SUM(CASE WHEN cv.data_quality = 'DATA_OK'
                             AND cv.signal_label IN ('single_event_signal', 'short_window_signal', 'medium_window_signal')
                            THEN 1 ELSE 0 END) AS significant_units
            FROM event_chain_validation_results cv
            JOIN us_events e ON e.event_id = cv.event_id
            WHERE e.event_time >= ?
            """,
            (since_text,),
        ).fetchone()
        top_rows = conn.execute(
            """
            SELECT e.ticker AS source_ticker, e.event_type, e.event_time, st.target_market,
                   st.target_ticker, st.target_name, st.theme,
                   COALESCE(st.transmission_type, vr.transmission_type, 'unknown') AS transmission_type,
                   vr.horizon_days, vr.primary_excess, vr.signal_label,
                   vr.direction_label, vr.direction_source,
                   vr.direction_confidence, vr.data_quality
            FROM signal_validation_results vr
            JOIN us_events e ON e.event_id = vr.event_id
            JOIN signal_targets st ON st.target_id = vr.target_id
            WHERE e.event_time >= ?
              AND (vr.updated_at >= ? OR vr.measured_at >= ?)
              AND vr.is_effective_sample = 1
              AND vr.data_quality = 'DATA_OK'
              AND vr.signal_label IN ('single_event_signal', 'short_window_signal', 'medium_window_signal')
            ORDER BY ABS(vr.primary_excess) DESC
            LIMIT ?
            """,
            (since_text, since_text, since_text, limit),
        ).fetchall()

    title = f"【美股雷达旁路·研究】{now.astimezone().strftime('%Y-%m-%d')}"
    lines = [
        title,
        "",
        f"回看窗口: {hours}小时",
        f"新增SEC事件: {_count_text(event_counts, EVENT_LABELS)}",
        f"高质量事件: {_count_text(high_quality)}",
        f"验证状态: {_validation_quality_text(validation_quality)}",
        f"事件×产业链单元: {_chain_quality_text(chain_quality)}",
        f"CN快照: {_snapshot_status(root)}",
        "",
        "显著传导 Top 5:",
    ]
    if not top_rows:
        lines.append("- 暂无 DATA_OK 且达阈值的有效样本")
    else:
        for row in top_rows:
            lines.append(
                "- "
                + f"{row['source_ticker']} -> {row['target_ticker']} "
                + f"{_label(EVENT_LABELS, row['event_type'])}/{_event_day(row['event_time'])} "
                + f"({_label(THEME_LABELS, row['theme'])}/{_label(MARKET_LABELS, row['target_market'])}) "
                + f"T+{row['horizon_days']} {_pct(row['primary_excess'])} "
                + f"{_label(SIGNAL_LABELS, row['signal_label'])} "
                + f"{_label(DIRECTION_LABELS, row['direction_label'])} "
                + f"来源:{_label(DIRECTION_SOURCE_LABELS, row['direction_source'])} "
                + f"方向置信度:{_pct(row['direction_confidence'])} "
                + f"传导:{_label(TRANSMISSION_LABELS, row['transmission_type'])} "
                + f"质量:{_label(VALIDATION_LABELS, row['data_quality'])}"
            )
    lines.extend(["", DISCLAIMER])
    return PushMessage(title=title, content="\n".join(lines))


def send_pushplus(message: PushMessage, token: str) -> dict:
    payload = urlencode(
        {
            "token": token,
            "title": message.title,
            "content": message.content,
            "template": "txt",
        }
    ).encode("utf-8")
    request = Request("https://www.pushplus.plus/send", data=payload, headers={"User-Agent": "zhulong-us-radar/0.1"})
    with urlopen(request, timeout=20) as response:
        return json.loads(response.read().decode("utf-8"))


def resolve_pushplus_token() -> str | None:
    return os.environ.get("US_RADAR_PUSHPLUS_TOKEN") or os.environ.get("PUSHPLUS_TOKEN")


def _count_text(rows: list, labels: dict[str, str] | None = None) -> str:
    if not rows:
        return "0"
    parts = []
    for key, count in rows:
        parts.append(f"{_label(labels or {}, key)}={count}")
    return ", ".join(parts)


def _validation_quality_text(rows: list) -> str:
    if not rows:
        return "无更新"
    return ", ".join(f"{gate}/{quality}={count}" for gate, quality, count in rows)


def _chain_quality_text(row: object) -> str:
    if not row:
        return "0"
    return f"有效={int(row[1] or 0)}/{int(row[0] or 0)}, 显著={int(row[2] or 0)}"


def _snapshot_status(root: Path) -> str:
    snapshot = root / "storage" / "database" / "zhulong_api_readonly.duckdb"
    return "ready" if snapshot.exists() else "missing"


def _pct(value: object) -> str:
    if value is None:
        return "待验证"
    return f"{float(value):.2%}"


def _event_day(event_time: object) -> str:
    if event_time is None:
        return ""
    return str(event_time)[:10]
