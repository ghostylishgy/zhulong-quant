from __future__ import annotations

import argparse
import importlib.util
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class Reminder:
    title: str
    content: str


def build_snapshot_reminder() -> Reminder:
    return Reminder(
        title="星火ETF｜请填写本周持仓快照",
        content="\n".join(
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
                "工银核心价值按“只卖不买利润仓”处理，不参与新增定投。",
            ]
        ),
    )


def send_push(title: str, content: str) -> bool:
    pusher_path = Path(__file__).resolve().parents[2] / "utils" / "pusher.py"
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Push Saturday reminder for Spark ETF weekly snapshot")
    parser.add_argument("--db", type=str, default=None, help="Deprecated compatibility option; ignored")
    parser.add_argument("--no-push", action="store_true", help="Print only; do not send pushplus message")
    args = parser.parse_args()

    result = build_snapshot_reminder()
    pushed = False if args.no_push else send_push(result.title, result.content)
    print(result.content)
    print(f"\npush_status: {'skipped' if args.no_push else ('sent' if pushed else 'failed')}")
    return 0 if args.no_push or pushed else 2


if __name__ == "__main__":
    raise SystemExit(main())
