"""Markdown reports for the US radar sidecar."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from .config import Settings
from .storage import EventStore


QUALITY_LABELS = {
    "8k_high_signal": "8-K高信号",
    "8k_generic": "8-K普通公告",
    "form4_open_market": "Form4公开市场交易",
    "form4_non_open_market": "Form4非公开市场交易",
    "form4_unparsed": "Form4待解析",
    "periodic_report": "定期报告",
    "low_signal_unknown": "低信号/未分类",
}

TRANSMISSION_LABELS = {
    "source": "美股源标的",
    "true_business": "真实业务传导",
    "sentiment": "情绪传导",
    "unknown": "待确认",
}

MARKET_LABELS = {
    "US": "美股",
    "CN": "A股",
    "HK": "港股",
}

THEME_LABELS = {
    "ai_compute": "AI算力",
    "memory_hbm": "HBM/存储",
    "advanced_node": "先进制程",
    "optical_interconnect": "光学连接",
    "datacenter_power": "数据中心电力",
    "inference_asic": "推理芯片/ASIC",
}

EVENT_LABELS = {
    "4": "Form 4 内部人交易",
    "8-K": "8-K 重大公告",
    "10-Q": "10-Q 季报",
    "10-K": "10-K 年报",
}

VALIDATION_LABELS = {
    "DATA_OK": "已验证",
    "PENDING_MARKET_DATA": "待行情",
    "INSUFFICIENT_FORWARD_DATA": "等待窗口",
    "NO_PRICE_DATA": "无行情",
    "PARTIAL_BENCHMARK_DATA": "基准部分缺失",
    "BENCHMARK_MISSING": "基准缺失",
    "CORPORATE_ACTION_UNADJUSTED": "公司行为待复权",
}

SIGNAL_LABELS = {
    "single_event_signal": "T+1显著",
    "short_window_signal": "T+3显著",
    "medium_window_signal": "T+5显著",
    "long_window_observation": "T+20观察",
    "below_threshold": "未达阈值",
    "pending_market_data": "待行情",
}

DIRECTION_LABELS = {
    "positive_transmission": "正向传导",
    "negative_transmission": "负向传导",
    "reverse_to_signal": "反向显著",
    "no_significant_move": "无显著变动",
    "reverse_independent_movement": "反向独立",
    "direction_unknown": "方向待判",
    "direction_pending": "待行情",
}

DIRECTION_SOURCE_LABELS = {
    "form4_transaction": "Form4交易",
    "evidence_heuristic": "公告启发",
    "8k_text_heuristic": "8-K文本兜底",
    "evidence_neutral": "公告中性",
    "form4_non_open_market": "Form4非公开市场",
    "none": "无方向",
}

WINDOW_LABELS = {
    "main_window": "主窗口",
    "early_response": "提前反应",
    "late_response": "延迟反应",
    "extended_observation": "扩展观察",
    "window_unknown": "窗口待判",
}

QUALITY_GATE_LABELS = {
    "effective_sample": "有效样本",
    "pending_validation": "待验证",
    "excluded_quality": "质量排除",
}


def _label(mapping: dict[str, str], value: object) -> str:
    if value is None:
        return ""
    text = str(value)
    return mapping.get(text, text)


def _escape_cell(value: object) -> str:
    text = "" if value is None else str(value)
    return text.replace("|", "\\|").replace("\n", " ").strip()


def _pct(value: object) -> str:
    if value is None:
        return ""
    try:
        return f"{float(value):.2%}"
    except (TypeError, ValueError):
        return str(value)


def write_event_report(store: EventStore, settings: Settings, hours: int = 24) -> Path:
    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=hours)
    rows = store.report_rows(since)
    chain_rows = store.event_chain_report_rows(since)

    report_day = now.astimezone().strftime("%Y%m%d")
    path = settings.report_dir / f"us_transmission_{report_day}.md"

    lines = [
        "# 美股领先信号传导观察表",
        "",
        f"生成时间 UTC: {now.isoformat(timespec='seconds')}",
        f"回看小时: {hours}",
        f"行数: {len(rows)}",
        f"事件×产业链统计单元: {len(chain_rows)}",
        "",
        "| 验证 | 样本门 | T+1主超额 | 判定 | 方向 | 方向来源 | 方向置信度 | 窗口 | 源时间 | 源标的 | 事件 | 质量 | 产业链 | 目标市场 | 目标标的 | 目标名称 | 传导类型 | 传导理由 | 映射置信度 | 链接 |",
        "|---|---|---:|---|---|---|---:|---|---|---:|---|---|---|---|---:|---|---|---|---:|---|",
    ]

    if not rows:
        lines.append("| n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | 暂无事件或target | n/a | n/a |")
    else:
        for row in rows:
            quality = _label(QUALITY_LABELS, row["quality_class"]) or "未分类"
            score = "" if row["quality_score"] is None else f"{row['quality_score']:.2f}"
            source = f"[SEC]({row['url']})" if row["url"] else ""
            lines.append(
                "| "
                + " | ".join(
                    [
                        _escape_cell(_label(VALIDATION_LABELS, row["validation_quality"])),
                        _escape_cell(_label(QUALITY_GATE_LABELS, row["quality_gate"])),
                        _escape_cell(_pct(row["primary_excess"])),
                        _escape_cell(_label(SIGNAL_LABELS, row["signal_label"])),
                        _escape_cell(_label(DIRECTION_LABELS, row["direction_label"])),
                        _escape_cell(_label(DIRECTION_SOURCE_LABELS, row["direction_source"])),
                        _escape_cell(_pct(row["direction_confidence"])),
                        _escape_cell(_label(WINDOW_LABELS, row["window_label"])),
                        _escape_cell(row["event_time"]),
                        _escape_cell(row["source_ticker"]),
                        _escape_cell(_label(EVENT_LABELS, row["event_type"])),
                        _escape_cell(f"{quality} {score}".strip()),
                        _escape_cell(_label(THEME_LABELS, row["theme"])),
                        _escape_cell(_label(MARKET_LABELS, row["target_market"])),
                        _escape_cell(row["target_ticker"]),
                        _escape_cell(row["target_name"]),
                        _escape_cell(_label(TRANSMISSION_LABELS, row["transmission_type"])),
                        _escape_cell(row["link_reason"]),
                        _escape_cell(row["confidence"]),
                        source,
                    ]
                )
                + " |"
            )

    lines.extend(
        [
            "",
            "## 事件×产业链汇总",
            "",
            "链级统计以同一事件、产业链、市场、传导类型和窗口为一个单元，使用有效目标的主超额中位数，避免逐标的行数放大样本量。",
            "",
            "| 源时间 | 源标的 | 事件 | 产业链 | 市场 | 传导类型 | 窗口 | 有效/总目标 | 显著目标占比 | 中位主超额 | 链级判定 | 方向 | 方向来源 | 方向置信度 | 数据质量 |",
            "|---|---:|---|---|---|---|---:|---:|---:|---:|---|---|---|---:|---|",
        ]
    )
    if not chain_rows:
        lines.append("| n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | 暂无链级统计 |")
    else:
        for row in chain_rows:
            lines.append(
                "| "
                + " | ".join(
                    [
                        _escape_cell(row["event_time"]),
                        _escape_cell(row["source_ticker"]),
                        _escape_cell(_label(EVENT_LABELS, row["event_type"])),
                        _escape_cell(_label(THEME_LABELS, row["theme"])),
                        _escape_cell(_label(MARKET_LABELS, row["target_market"])),
                        _escape_cell(_label(TRANSMISSION_LABELS, row["transmission_type"])),
                        _escape_cell(f"T+{row['horizon_days']}"),
                        _escape_cell(f"{row['effective_targets']}/{row['total_targets']}"),
                        _escape_cell(_pct(row["significant_share"])),
                        _escape_cell(_pct(row["median_primary_excess"])),
                        _escape_cell(_label(SIGNAL_LABELS, row["signal_label"])),
                        _escape_cell(_label(DIRECTION_LABELS, row["direction_label"])),
                        _escape_cell(_label(DIRECTION_SOURCE_LABELS, row["direction_source"])),
                        _escape_cell(_pct(row["direction_confidence"])),
                        _escape_cell(_label(VALIDATION_LABELS, row["data_quality"])),
                    ]
                )
                + " |"
            )

    lines.extend(
        [
            "",
            "## 备注",
            "",
            "- `true_business` 表示有业务链条基础的传导。",
            "- `sentiment` 表示主题或情绪共振传导。",
            "- `T+1主超额`：A股优先相对行业指数，美股优先相对 QQQ。",
            "- `等待窗口` 表示 T+5/T+20 等未来交易日还没完全到齐，会在后续运行中自动补齐。",
            "- 本报告仅用于研究验证，不构成交易指令。",
        ]
    )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    store.insert_report(str(path), "transmission_report", now.isoformat(), len(rows))
    return path
