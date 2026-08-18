from __future__ import annotations

import importlib.util
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import duckdb

from spark_etf.db.db_init import DEFAULT_DB_PATH, init_db
from spark_etf.services.holding_signal_service import HoldingSignalService


@dataclass(frozen=True, slots=True)
class PushResult:
    title: str
    content: str
    pushed: bool | None = None


class HoldingReportService:
    def __init__(self, db_path: Path | str | None = None) -> None:
        self.db_path = init_db(db_path or DEFAULT_DB_PATH)

    def build_snapshot_reminder(self, today: date | None = None) -> PushResult:
        today = today or date.today()
        title = "星火ETF｜请填写本周持仓快照"
        content = "\n".join(
            [
                "【本周任务】",
                "请更新本周 ETF/基金持仓快照。",
                "",
                "【需要填写】",
                "1. 当前持有份额",
                "2. 天天基金显示的持仓成本",
                "3. 工银核心价值只需确认份额/市值变化",
                "",
                "【填写完成后】",
                "告诉 Codex 读取桌面模板，系统会导入数据并生成周日持仓分析。",
                "",
                "【提醒】",
                "工银核心价值按“只卖不买利润仓”处理，不参与定投判断。",
            ]
        )
        return PushResult(title=title, content=content)

    def build_weekly_review(
        self,
        snapshot_date: date | None = None,
        today: date | None = None,
        refresh_nav: bool = True,
    ) -> PushResult:
        today = today or date.today()
        latest_snapshot = snapshot_date or self.latest_snapshot_date()
        title = "星火ETF｜本周持仓分析"

        if latest_snapshot is None:
            return PushResult(title=title, content=self._missing_snapshot_text(None, today))
        if not self._is_current_week(latest_snapshot, today):
            return PushResult(title=title, content=self._missing_snapshot_text(latest_snapshot, today))

        signal_service = HoldingSignalService(db_path=self.db_path)
        signals = signal_service.generate_for_snapshot(snapshot_date=latest_snapshot, refresh_nav=refresh_nav)
        content = self._render_review(signals=signals, snapshot_date=latest_snapshot, today=today)
        return PushResult(title=title, content=content)

    def send_push(self, title: str, content: str) -> bool:
        pusher_path = Path(__file__).resolve().parents[4] / "utils" / "pusher.py"
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

    def latest_snapshot_date(self) -> date | None:
        conn = duckdb.connect(str(self.db_path), read_only=True)
        try:
            row = conn.execute("SELECT max(snapshot_date) FROM spark_position_weekly_snapshot").fetchone()
        finally:
            conn.close()
        return row[0] if row and row[0] is not None else None

    def _missing_snapshot_text(self, latest_snapshot: date | None, today: date) -> str:
        week_start, week_end = self._week_range(today)
        latest_text = latest_snapshot.isoformat() if latest_snapshot else "暂无"
        return "\n".join(
            [
                "【本周结论】",
                "本周持仓分析已跳过。",
                "",
                "【原因】",
                f"本周范围：{week_start.isoformat()} 至 {week_end.isoformat()}",
                f"最新已导入快照：{latest_text}",
                "本周持仓快照尚未导入，系统不会用旧快照生成买卖建议。",
                "",
                "【下一步】",
                "请先填写并导入本周桌面模板，再运行周日持仓分析。",
            ]
        )

    def _render_review(self, signals: list[dict[str, Any]], snapshot_date: date, today: date) -> str:
        review_items = sorted(
            [item for item in signals if str(item.get("action", "")) != "HOLD"],
            key=self._review_priority,
        )
        lines = [
            "【本周结论】",
            f"快照日期：{snapshot_date.isoformat()}",
            f"持仓数量：{len(signals)} 只",
            f"需要复核：{len(review_items)} 项",
            "建议级别：仅供复核，不自动交易",
            "",
            "【重点关注】",
        ]

        if review_items:
            for idx, signal in enumerate(review_items[:6], start=1):
                name = str(signal.get("fund_name") or signal.get("fund_code"))
                action = self._action_label(str(signal.get("action", "")))
                reason = self._reason_text(signal)
                lines.append(f"{idx}. {name}：{action}")
                lines.append(f"   原因：{reason}")
        else:
            lines.append("本周无持仓触发复核动作。")

        lines.extend(
            [
                "",
                "【持仓行动表】",
                "基金｜模式｜动作｜市值｜收益率｜数据",
            ]
        )
        for signal in signals:
            name = str(signal.get("fund_name") or signal.get("fund_code"))
            mode = self._mode_label(str(signal.get("holding_mode", "")))
            action = self._action_label(str(signal.get("action", "")))
            value = self._fmt_money(signal.get("market_value"))
            ret = self._fmt_pct(signal.get("return_ratio"))
            quality = self._quality_text(signal)
            short_name = self._short_name(name)
            lines.append(f"{short_name}｜{mode}｜{action}｜{value}｜{ret}｜{quality}")

        lines.extend(
            [
                "",
                "【数据说明】",
                "数据正常：净值和估值代理都可用。",
                "仅净值：净值可用，但估值代理暂缺。",
                "估值缺失：不能据此判断低估或高估。",
                "",
                "【执行纪律】",
                "定投中：定投金额由天天基金策略执行，星火只做持仓复核。",
                "只卖不买：只做卖出复核，不参与新增定投。",
                "新闻和宏观只做辅助，不硬触发买卖。",
            ]
        )
        return "\n".join(lines)

    @staticmethod
    def _week_range(day: date) -> tuple[date, date]:
        week_start = day - timedelta(days=day.weekday())
        return week_start, week_start + timedelta(days=6)

    @classmethod
    def _is_current_week(cls, snapshot_date: date, today: date) -> bool:
        week_start, week_end = cls._week_range(today)
        return week_start <= snapshot_date <= week_end

    @staticmethod
    def _fmt_money(value: object) -> str:
        if isinstance(value, (int, float)):
            return f"{float(value):,.0f}"
        return "--"

    @staticmethod
    def _fmt_pct(value: object) -> str:
        if isinstance(value, (int, float)):
            return f"{float(value) * 100:.2f}%"
        return "--"

    @staticmethod
    def _mode_label(mode: str) -> str:
        return {
            "dca_active": "定投中",
            "sell_only": "只卖不买",
        }.get(mode, mode or "未知")

    @staticmethod
    def _action_label(action: str) -> str:
        mapping = {
            "HOLD": "持有",
            "HOLD_REVIEW": "持仓复核",
            "HOLD_LOW_VALUATION": "低估持有",
            "HOLD_OR_DCA_REVIEW": "低估加仓复核",
            "PROFIT_PROTECTION_WATCH": "利润保护观察",
            "PRINCIPAL_RECOVERY_REVIEW": "回收本金复核",
            "LAYERED_TAKE_PROFIT_REVIEW": "分层止盈复核",
            "PARTIAL_SELL_REVIEW": "部分卖出复核",
            "LOW_ZONE_REVIEW": "低位复核",
            "DATA_REVIEW": "数据复核",
            "FREE_RIDE_EXIT_WATCH": "利润仓退出观察",
            "FREE_RIDE_HOLD_WATCH": "利润仓持有观察",
            "FREE_RIDE_TREND_EXIT_WATCH": "利润仓趋势退出观察",
            "FREE_RIDE_SELL_REVIEW": "利润仓卖出复核",
            "FREE_RIDE_OVERVALUED_SELL_REVIEW": "高估利润仓卖出复核",
            "FREE_RIDE_PROFIT_LOCK_REVIEW": "利润仓锁利复核",
        }
        return mapping.get(action, action or "未知动作")

    @staticmethod
    def _quality_text(signal: dict[str, Any]) -> str:
        data_quality = str(signal.get("data_quality", "N/A"))
        valuation_quality = str(signal.get("valuation_quality", "N/A"))
        if data_quality == "DATA_OK" and valuation_quality == "DATA_OK":
            return "数据正常"
        if data_quality == "DATA_OK" and valuation_quality == "NAV_ONLY":
            return "仅净值"
        if data_quality == "DATA_OK" and valuation_quality == "VALUATION_PROXY_MISSING":
            return "估值缺失"
        mapping = {
            "NO_DATA": "无数据",
            "STALE_DATA": "数据过期",
            "DATA_OK": "净值正常",
            "NAV_ONLY": "仅净值",
            "VALUATION_PROXY_MISSING": "估值缺失",
        }
        return f"{mapping.get(data_quality, '数据待查')}/{mapping.get(valuation_quality, '估值待查')}"

    @staticmethod
    def _short_name(name: str) -> str:
        replacements = [
            ("ETF联接(QDII)A", ""),
            ("ETF联接(QDII)", ""),
            ("ETF联接A", ""),
            ("指数发起A", ""),
            ("混合A", ""),
            ("联接A", ""),
        ]
        text = name
        for old, new in replacements:
            text = text.replace(old, new)
        return text[:16]

    @staticmethod
    def _reason_text(signal: dict[str, Any]) -> str:
        action = str(signal.get("action", ""))
        reason_map = {
            "LAYERED_TAKE_PROFIT_REVIEW": "收益已超过 50%，建议检查是否需要分批止盈。",
            "PRINCIPAL_RECOVERY_REVIEW": "收益已超过 30%，可以复核是否需要回收部分本金。",
            "PROFIT_PROTECTION_WATCH": "收益已超过 20%，进入利润保护观察区。",
            "PARTIAL_SELL_REVIEW": "估值偏高且已有盈利垫，建议复核是否部分卖出。",
            "LOW_ZONE_REVIEW": "跌幅较大，先复核基金逻辑和风险，再决定是否继续持有。",
            "DATA_REVIEW": "净值数据缺失或过期，本周不做强判断。",
            "FREE_RIDE_EXIT_WATCH": "该仓位是只卖不买利润仓，继续观察退出机会。",
            "FREE_RIDE_HOLD_WATCH": "该利润仓暂未触发退出信号，继续观察。",
            "FREE_RIDE_TREND_EXIT_WATCH": "该利润仓短期趋势转弱，建议观察是否确认退出。",
            "FREE_RIDE_SELL_REVIEW": "该利润仓估值偏高且趋势转弱，建议复核是否分批卖出。",
            "FREE_RIDE_OVERVALUED_SELL_REVIEW": "该仓位已是负成本利润仓，当前估值偏高，建议考虑是否分批卖出。",
            "FREE_RIDE_PROFIT_LOCK_REVIEW": "该利润仓从阶段高点回撤较大，建议复核是否锁定利润。",
        }
        return reason_map.get(action, "本周触发持仓复核，请结合实际账户情况确认。")

    @staticmethod
    def _review_priority(signal: dict[str, Any]) -> tuple[int, str]:
        action = str(signal.get("action", ""))
        mode = str(signal.get("holding_mode", ""))
        sell_actions = {
            "FREE_RIDE_OVERVALUED_SELL_REVIEW",
            "FREE_RIDE_SELL_REVIEW",
            "FREE_RIDE_PROFIT_LOCK_REVIEW",
            "PARTIAL_SELL_REVIEW",
            "LAYERED_TAKE_PROFIT_REVIEW",
        }
        if mode == "sell_only":
            return (0, str(signal.get("fund_code", "")))
        if action in sell_actions:
            return (1, str(signal.get("fund_code", "")))
        if action == "PRINCIPAL_RECOVERY_REVIEW":
            return (2, str(signal.get("fund_code", "")))
        if action == "PROFIT_PROTECTION_WATCH":
            return (3, str(signal.get("fund_code", "")))
        return (4, str(signal.get("fund_code", "")))
