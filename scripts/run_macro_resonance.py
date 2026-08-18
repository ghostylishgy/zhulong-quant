#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/run_macro_resonance.py
Macro Resonance Engine V1 entrypoint.
"""

from __future__ import annotations

import argparse
import runpy
import logging
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_PATH = PROJECT_ROOT / "07_macro" / "lib" / "pipeline.py"

_loader_ns = runpy.run_path(str(PROJECT_ROOT / "04_governance" / "lib" / "core" / "module_loader.py"))
load_module_from_path = _loader_ns["load_module_from_path"]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))
if str(PROJECT_ROOT / "01_engine") not in sys.path:
    sys.path.append(str(PROJECT_ROOT / "01_engine"))
if str(PROJECT_ROOT / "01_engine" / "lib") not in sys.path:
    sys.path.append(str(PROJECT_ROOT / "01_engine" / "lib"))

COLOR_HIGHLIGHT = "\033[1;93m"
COLOR_TITLE = "\033[1;96m"
COLOR_RESET = "\033[0m"


def _load_pipeline_module():
    return load_module_from_path("macro_pipeline_runtime", PIPELINE_PATH)


def _normalize_trade_date(value: str) -> str:
    text = str(value or "").strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10]
    raise ValueError(f"Unsupported trade_date format: {value}")


def _print_top5(top_rows):
    print(f"{COLOR_TITLE}================ Macro Resonance Top 5 ================{COLOR_RESET}")
    if not top_rows:
        print("No Top 5 rows generated.")
        return

    for idx, row in enumerate(top_rows, start=1):
        topic_name = str(row.get("topic_name", ""))
        topic_type = str(row.get("topic_type", ""))
        topic_id = str(row.get("topic_id", ""))
        score = float(row.get("resonance_score", 0.0) or 0.0)
        anti_fake = int(float(row.get("anti_fake_flag", 0) or 0))
        line = (
            f"TOP{idx} | {topic_name:<20} | score={score:>7.2f} "
            f"| anti_fake={anti_fake} | {topic_type}:{topic_id}"
        )
        print(f"{COLOR_HIGHLIGHT}{line}{COLOR_RESET}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Macro Resonance Phase3-6 pipeline")
    parser.add_argument(
        "--trade_date",
        type=str,
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Trade date, supports YYYY-MM-DD or YYYYMMDD",
    )
    parser.add_argument(
        "--focus_symbol",
        type=str,
        default="688256.SH",
        help="Focus symbol for overlay label output",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    trade_date = _normalize_trade_date(args.trade_date)
    focus_symbol = str(args.focus_symbol or "").strip().upper()

    mod = _load_pipeline_module()
    pipeline = mod.MacroResonancePipeline()

    result = pipeline.run(trade_date)
    print(
        "Macro pipeline result | "
        f"trade_date={result.trade_date} "
        f"status={result.status} "
        f"scored_count={result.scored_count} "
        f"topic_count={result.topic_count} "
        f"mapping_used_cache={result.mapping_used_cache} "
        f"mapping_used_fallback={result.mapping_used_fallback} "
        f"filtered_topic_count={result.filtered_topic_count} "
        f"anti_fake_flagged_count={result.anti_fake_flagged_count} "
        f"overlay_rows={result.overlay_rows} "
        f"msg={result.message}"
    )

    _print_top5(result.top_rows)

    focus_label = result.symbol_labels.get(focus_symbol, "[N/A]")
    focus_topic = result.symbol_best_topic.get(focus_symbol, {})
    print(
        f"Focus Symbol | {focus_symbol} | signal_label={focus_label} "
        f"| best_topic={focus_topic.get('topic_name', 'N/A')} "
        f"| macro_score={focus_topic.get('macro_score', 'N/A')} "
        f"| anti_fake_flag={focus_topic.get('anti_fake_flag', 'N/A')}"
    )

    if result.status != "OK":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
