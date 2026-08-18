#!/usr/bin/env python3
"""Offline micro-task bake-off for the Zhulong L3 evidence reviewer.

The full L3 contract is intentionally decomposed into three narrow tasks:
lifecycle classification, evidence-role selection, and invalidation selection.
This tool calls Ollama and writes benchmark artifacts only. It never writes
production databases, changes model authority, or activates an audit path.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from tools.l2_l3_model_bakeoff import (  # noqa: E402
    BLOCKED_ACTIONS,
    DEFAULT_CASES,
    DEFAULT_OUTPUT_DIR,
    DESIGN_PATH,
    SUITE_VERSION,
    http_json,
    load_suite,
    model_catalog,
    percentile,
    sha256_file,
    unload_model,
)


TASK_VERSION = "L3_MICROTASK_REVIEW_V3"
TASKS = ("lifecycle", "evidence_roles", "invalidation")
LIFECYCLES = ("INITIATION", "CONTINUATION", "EXHAUSTION", "UNCLEAR")
CONFIDENCE = ("LOW", "MEDIUM", "HIGH")
TRADE_ACTION_PATTERN = re.compile(
    r"(?:建议|应当|应该|可以|需要|适合|立即|择机)?\s*"
    r"(?:买入|卖出|建仓|加仓|减仓|清仓|止损|止盈|持有|申购|下单|目标价)",
    re.I,
)
NUMBER_PATTERN = re.compile(r"(?<![A-Za-z])\d+(?:\.\d+)?(?![A-Za-z])")


def _evidence(case: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "evidence_id": evidence_id,
            "fact": item["fact"],
            "stance": item["stance"],
            "quality": item["quality"],
        }
        for evidence_id, item in case["evidence"].items()
    ]


def _condition_options(case: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Return the neutral condition pool used by the L3 selection task."""
    options = case.get("condition_options")
    if not isinstance(options, dict):
        raise ValueError(f"MISSING_CONDITION_OPTIONS:{case.get('case_id')}")
    return options


def task_payload(case: dict[str, Any], task: str) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "case_id": case["case_id"],
        "market_regime": case["market_regime"],
        "l2_deterministic_state": case["l2"]["deterministic_state"],
        "candidate_thesis": case["l3"]["candidate_thesis"],
        "evidence_envelope": _evidence(case),
    }
    if task == "evidence_roles":
        payload["role_definitions"] = {
            "bull": "直接支持 candidate_thesis 的有效证据",
            "bear": "削弱或反驳 candidate_thesis 的有效证据",
            "contradiction": "揭示论点内部矛盾或与核心论点方向冲突的有效证据",
        }
    elif task == "invalidation":
        payload["condition_options"] = [
            {
                "condition_id": item_id,
                "condition": item["condition"],
                "evidence_ids": item["evidence_ids"],
            }
            for item_id, item in _condition_options(case).items()
        ]
    return payload


def build_prompt(case: dict[str, Any], task: str) -> str:
    payload = json.dumps(task_payload(case, task), ensure_ascii=False, indent=2)
    if task == "lifecycle":
        instructions = """你是烛龙 L3 的生命周期单项审查器。确定性代码已经完成单位、方向和阈值解释。
只判断 candidate_thesis 当前最符合哪一个生命周期：
INITIATION=新启动或早期突破；CONTINUATION=已有趋势延续；EXHAUSTION=高潮、衰竭或派发风险；UNCLEAR=证据不足以区分。
不要输出 thesis_state，不要选择证据角色，不要推断输入之外的事实，不要输出交易动作。
"""
    elif task == "evidence_roles":
        instructions = """你是烛龙 L3 的证据角色标注器。确定性代码已经完成事实解释。
只把输入中的 evidence_id 分到 bull、bear、contradiction 三个角色；没有合适证据时使用空数组。
bull 必须是支持 candidate_thesis 的证据，bear 必须是削弱或反驳 candidate_thesis 的证据，contradiction 必须是直接揭示论点矛盾的证据。
不要修改事实方向，不要补写证据，不要输出生命周期、PASS/VETO 或交易动作。
"""
    else:
        instructions = """你是烛龙 L3 的失效条件选择器。确定性代码已经完成事实解释。
只从 condition_options 中选择会使 candidate_thesis 失效的条件；可以选择多个，也可以一个不选，但不得创建新 ID。
注意：重新确认或补齐证据的条件不是失效条件。当前已存在的风险证据不是失效条件本身，必须选择输入中定义的条件；不要输出生命周期、PASS/VETO 或交易动作。
"""
    schema = schema_for(task, case)
    summary_rule = (
        "summary 只能概括输入事实，不得出现输入之外的数字、新闻、财务、行情或交易结论。\n"
        if task != "invalidation"
        else "失效条件任务不得输出摘要、置信度或任何未绑定 ID。\n"
    )
    return (
        f"{instructions}\n只输出一个符合 {TASK_VERSION} 的 JSON 对象，不要 Markdown、前言或思维过程。\n"
        "数组中的每个 ID 最多出现一次；没有可报告的 ID 时使用空数组。\n"
        f"{summary_rule}"
        f"\nINPUT:\n{payload}\n"
        f"\n输出字段必须严格为：{', '.join(schema['properties'])}。"
    )


def schema_for(task: str, case: dict[str, Any]) -> dict[str, Any]:
    evidence_ids = sorted(case["evidence"])
    invalidation_ids = sorted(_condition_options(case))
    id_array = lambda values: {  # noqa: E731
        "type": "array",
        "items": {"type": "string", "enum": values},
        "uniqueItems": True,
        "maxItems": 8,
    }
    if task == "lifecycle":
        properties = {
            "contract_version": {"type": "string", "enum": [TASK_VERSION]},
            "lifecycle_stage": {"type": "string", "enum": list(LIFECYCLES)},
            "confidence": {"type": "string", "enum": list(CONFIDENCE)},
            "summary": {"type": "string", "minLength": 20, "maxLength": 300},
        }
    elif task == "evidence_roles":
        properties = {
            "contract_version": {"type": "string", "enum": [TASK_VERSION]},
            "bull_evidence_ids": id_array(evidence_ids),
            "bear_evidence_ids": id_array(evidence_ids),
            "contradiction_evidence_ids": id_array(evidence_ids),
            "confidence": {"type": "string", "enum": list(CONFIDENCE)},
            "summary": {"type": "string", "minLength": 20, "maxLength": 360},
        }
    else:
        properties = {
            "contract_version": {"type": "string", "enum": [TASK_VERSION]},
            "selected_invalidation_ids": id_array(invalidation_ids),
        }
    return {"type": "object", "properties": properties, "required": list(properties)}


def parse_contract(raw: str, case: dict[str, Any], task: str) -> dict[str, Any]:
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("NON_OBJECT_RESPONSE")
    schema = schema_for(task, case)
    if set(payload) != set(schema["properties"]):
        raise ValueError("FIELD_SET_MISMATCH")
    for name, spec in schema["properties"].items():
        value = payload[name]
        if spec["type"] == "string":
            if not isinstance(value, str):
                raise ValueError(f"FIELD_TYPE:{name}")
            if "enum" in spec and value not in spec["enum"]:
                raise ValueError(f"FIELD_VALUE:{name}")
            if not spec.get("minLength", 0) <= len(value) <= spec.get("maxLength", 10**9):
                raise ValueError(f"FIELD_LENGTH:{name}")
        elif spec["type"] == "array":
            if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
                raise ValueError(f"FIELD_TYPE:{name}")
            if len(value) != len(set(value)) or len(value) > spec["maxItems"]:
                raise ValueError(f"FIELD_ARRAY_CONTRACT:{name}")
            if set(value).difference(spec["items"].get("enum", [])):
                raise ValueError(f"UNBOUND_ID:{name}")
    return payload


def _summary_errors(summary: str, expected: dict[str, Any]) -> tuple[list[str], list[str]]:
    fatal: list[str] = []
    direction: list[str] = []
    if TRADE_ACTION_PATTERN.search(summary):
        fatal.append("TRADE_INSTRUCTION")
    if NUMBER_PATTERN.search(summary):
        fatal.append("INVENTED_NUMBER")
    for pattern in expected.get("forbidden_summary_patterns", []):
        if re.search(str(pattern), summary, re.I):
            direction.append(f"FORBIDDEN_CLAIM:{pattern}")
    return fatal, direction


def evaluate(payload: dict[str, Any], case: dict[str, Any], task: str) -> dict[str, Any]:
    expected = case["l3"]["expected"]
    fatal, direction = _summary_errors(str(payload.get("summary", "")), expected)
    judgment: list[str] = []
    checks: dict[str, bool] = {}
    if task == "lifecycle":
        actual = str(payload["lifecycle_stage"])
        allowed = set(expected.get("allowed_lifecycle", []))
        forbidden = set(expected.get("forbidden_lifecycle", []))
        checks["task_accuracy"] = actual in allowed
        if actual in forbidden:
            direction.append(f"FORBIDDEN_LIFECYCLE:{actual}")
        elif actual not in allowed:
            judgment.append(f"LIFECYCLE_MISMATCH:{actual}")
    elif task == "evidence_roles":
        evidence_by_id = case["evidence"]
        bull = set(payload["bull_evidence_ids"])
        bear = set(payload["bear_evidence_ids"])
        contradiction = set(payload["contradiction_evidence_ids"])
        required_bull = set(expected.get("required_any_bull_ids", []))
        required_bear = set(expected.get("required_any_bear_ids", []))
        required_conflict = set(expected.get("required_any_conflict_ids", []))
        checks["bull_recall"] = not required_bull or bool(bull.intersection(required_bull))
        checks["bear_recall"] = not required_bear or bool(bear.intersection(required_bear))
        checks["contradiction_recall"] = not required_conflict or bool(
            contradiction.intersection(required_conflict)
        )
        checks["task_accuracy"] = all(
            checks[key] for key in ("bull_recall", "bear_recall", "contradiction_recall")
        )
        if not checks["bull_recall"]:
            judgment.append("REQUIRED_BULL_EVIDENCE_MISSING")
        if not checks["bear_recall"]:
            judgment.append("REQUIRED_BEAR_EVIDENCE_MISSING")
        if not checks["contradiction_recall"]:
            judgment.append("REQUIRED_CONTRADICTION_EVIDENCE_MISSING")
        if not required_bull and bull:
            judgment.append("UNEXPECTED_BULL_EVIDENCE")
        if not required_bear and bear:
            judgment.append("UNEXPECTED_BEAR_EVIDENCE")
        if not required_conflict and contradiction:
            judgment.append("UNEXPECTED_CONTRADICTION_EVIDENCE")
        if bull.intersection(bear):
            judgment.append("BULL_BEAR_OVERLAP")
        for evidence_id in bull:
            if evidence_by_id[evidence_id]["stance"] == "RISK":
                direction.append(f"BULL_RISK_EVIDENCE:{evidence_id}")
        for evidence_id in bear:
            if evidence_by_id[evidence_id]["stance"] == "SUPPORT":
                direction.append(f"BEAR_SUPPORT_EVIDENCE:{evidence_id}")
    else:
        actual = set(payload["selected_invalidation_ids"])
        required = set(expected.get("required_any_invalidation_ids", []))
        allowed = set(expected.get("allowed_invalidation_ids", required))
        forbidden = set(expected.get("forbidden_invalidation_ids", []))
        empty_allowed = bool(expected.get("empty_selection_allowed", not required))
        checks["invalidation_recall"] = not required or bool(actual.intersection(required))
        checks["invalidation_precision"] = actual.issubset(allowed)
        checks["empty_selection"] = bool(actual) or empty_allowed
        checks["forbidden_selection"] = not bool(actual.intersection(forbidden))
        checks["task_accuracy"] = (
            checks["invalidation_recall"]
            and checks["invalidation_precision"]
            and checks["empty_selection"]
            and checks["forbidden_selection"]
        )
        if not checks["invalidation_recall"]:
            judgment.append("REQUIRED_INVALIDATION_SELECTION_MISSING")
        unexpected = sorted(actual.difference(allowed))
        if unexpected:
            judgment.append("UNEXPECTED_INVALIDATION_SELECTION:" + ",".join(unexpected))
        forbidden_selected = sorted(actual.intersection(forbidden))
        if forbidden_selected:
            judgment.append("FORBIDDEN_INVALIDATION_SELECTION:" + ",".join(forbidden_selected))
        if not checks["empty_selection"]:
            judgment.append("EMPTY_INVALIDATION_SELECTION_NOT_ALLOWED")

    return {
        "strict_pass": not fatal and not direction and not judgment,
        "safe_pass": not fatal and not direction,
        "fatal_errors": fatal,
        "direction_errors": direction,
        "judgment_errors": judgment,
        "checks": checks,
    }


def run_one(
    server: str,
    model: str,
    case: dict[str, Any],
    task: str,
    repeat: int,
    timeout: int,
    num_predict: int,
) -> dict[str, Any]:
    prompt = build_prompt(case, task)
    schema = schema_for(task, case)
    request = {
        "model": model,
        "prompt": prompt,
        "system": f"Return only one {TASK_VERSION} JSON object. Use only supplied IDs and facts.",
        "stream": False,
        "format": schema,
        "think": False,
        "keep_alive": "5m",
        "options": {
            "temperature": 0.0,
            "top_p": 0.1,
            "num_ctx": 4096,
            "num_predict": num_predict,
            "num_gpu": 0,
            "gpu_layers": 0,
        },
    }
    started = time.monotonic()
    response: dict[str, Any] = {}
    parsed: dict[str, Any] = {}
    accepted = False
    error = ""
    evaluation = {
        "strict_pass": False,
        "safe_pass": False,
        "fatal_errors": [],
        "direction_errors": [],
        "judgment_errors": [],
        "checks": {},
    }
    try:
        response = http_json(f"{server}/api/generate", request, timeout)
        parsed = parse_contract(str(response.get("response", "") or "").strip(), case, task)
        accepted = True
        evaluation = evaluate(parsed, case, task)
    except Exception as exc:
        error = f"{type(exc).__name__}:{str(exc)[:500]}"
    elapsed_ms = (time.monotonic() - started) * 1000.0
    eval_count = int(response.get("eval_count", 0) or 0)
    eval_duration = int(response.get("eval_duration", 0) or 0)
    token_rate = eval_count / (eval_duration / 1_000_000_000) if eval_count and eval_duration else 0.0
    return {
        "case_id": case["case_id"],
        "task": task,
        "repeat": repeat,
        "contract_accepted": accepted,
        "contract_error": error,
        "evaluation": evaluation,
        "payload": parsed,
        "elapsed_ms": round(elapsed_ms, 2),
        "eval_count": eval_count,
        "eval_tokens_per_second": round(token_rate, 3),
        "raw_response": str(response.get("response", "") or "")[:5000],
        "prompt_sha256": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
    }


def aggregate(model: str, task: str, runs: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(runs)
    accepted = [row for row in runs if row["contract_accepted"]]
    evaluations = [row["evaluation"] for row in accepted]
    contract = len(accepted) / total if total else 0.0
    strict = sum(item["strict_pass"] for item in evaluations) / total if total else 0.0
    safe = sum(item["safe_pass"] for item in evaluations) / total if total else 0.0
    primary = sum(item["checks"].get("task_accuracy", False) for item in evaluations) / total if total else 0.0
    fatal = sum(len(item["fatal_errors"]) for item in evaluations)
    direction = sum(len(item["direction_errors"]) for item in evaluations)
    elapsed = [float(row["elapsed_ms"]) for row in runs]
    result: dict[str, Any] = {
        "model": model,
        "task": task,
        "runs": total,
        "contract_accept_rate": round(contract, 4),
        "safe_pass_rate": round(safe, 4),
        "strict_pass_rate": round(strict, 4),
        "task_accuracy": round(primary, 4),
        "fatal_fact_errors": fatal,
        "direction_errors": direction,
        "latency_ms_p50": round(statistics.median(elapsed), 2) if elapsed else 0.0,
        "latency_ms_p95": round(percentile(elapsed, 0.95), 2),
        "status": (
            "MICROTASK_PASS"
            if contract >= 0.95 and strict >= 0.75 and fatal == 0 and direction == 0 and primary >= 0.75
            else "MICROTASK_FAIL"
        ),
    }
    if task == "evidence_roles":
        for key in ("bull_recall", "bear_recall", "contradiction_recall"):
            result[key] = round(
                sum(item["checks"].get(key, False) for item in evaluations) / total if total else 0.0,
                4,
            )
    elif task == "invalidation":
        for key in ("invalidation_precision", "invalidation_recall"):
            result[key] = round(
                sum(item["checks"].get(key, False) for item in evaluations) / total if total else 0.0,
                4,
            )
    return result


def render(report: dict[str, Any]) -> str:
    lines = [
        f"# L3 Micro-task Bake-off · {report['run_id']}",
        "",
        f"- suite: `{report['suite_version']}`",
        f"- task_version: `{report['task_version']}`",
        f"- cases: `{report['case_count']}`",
        f"- repeats: `{report['repeats']}`",
        f"- ollama_version: `{report['ollama_version']}`",
        "- dry_run: `true`",
        "- no_trade_signal: `true`",
        "",
        "## Summary",
        "",
        "| Model | Task | Status | Schema | Safe | Strict | Accuracy | Fatal | Direction | p50 ms | p95 ms |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in report["summaries"]:
        lines.append(
            f"| {row['model']} | {row['task']} | {row['status']} | "
            f"{row['contract_accept_rate']:.1%} | {row['safe_pass_rate']:.1%} | "
            f"{row['strict_pass_rate']:.1%} | {row['task_accuracy']:.1%} | "
            f"{row['fatal_fact_errors']} | {row['direction_errors']} | "
            f"{row['latency_ms_p50']:.0f} | {row['latency_ms_p95']:.0f} |"
        )
        if row["task"] == "evidence_roles":
            lines.append(
                f"  - role recall: bull={row['bull_recall']:.1%}, "
                f"bear={row['bear_recall']:.1%}, contradiction={row['contradiction_recall']:.1%}"
            )
        elif row["task"] == "invalidation":
            lines.append(
                f"  - invalidation selection: precision={row['invalidation_precision']:.1%}, "
                f"recall={row['invalidation_recall']:.1%}"
            )
    lines.extend(["", "## Failure Ledger", ""])
    failures = 0
    for model_row in report["models"]:
        for row in model_row["runs"]:
            if row["contract_accepted"] and row["evaluation"]["strict_pass"]:
                continue
            failures += 1
            reason = row["contract_error"] if not row["contract_accepted"] else json.dumps(
                {
                    "fatal": row["evaluation"]["fatal_errors"],
                    "direction": row["evaluation"]["direction_errors"],
                    "judgment": row["evaluation"]["judgment_errors"],
                },
                ensure_ascii=False,
            )
            lines.append(
                f"- `{model_row['model']}` / `{row['task']}` / `{row['case_id']}` / "
                f"repeat {row['repeat']}: {reason}"
            )
    if not failures:
        lines.append("- None")
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "This offline micro-task benchmark does not authorize a production model or write DuckDB, RAG, Shadow, nexus_audits, or daemon state.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task", choices=TASKS, action="append", default=[])
    parser.add_argument("--model", action="append", required=True)
    parser.add_argument(
        "--server",
        default=os.getenv("ZHULONG_OLLAMA_SERVER", "http://192.0.2.20:11434"),
    )
    parser.add_argument("--cases", type=Path, default=DEFAULT_CASES)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--num-predict", type=int, default=192)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()
    if not args.task:
        args.task = list(TASKS)
    if args.repeats < 1 or args.repeats > 3:
        parser.error("--repeats must be between 1 and 3")
    if args.num_predict < 96 or args.num_predict > 512:
        parser.error("--num-predict must be between 96 and 512")

    suite = load_suite(args.cases)
    server = args.server.rstrip("/")
    version, catalog = model_catalog(server)
    missing = [model for model in args.model if model not in catalog]
    if missing:
        raise SystemExit("Models are not installed: " + ", ".join(missing))

    summaries = []
    model_rows = []
    for model in args.model:
        for task in args.task:
            runs = [
                run_one(server, model, case, task, repeat, args.timeout, args.num_predict)
                for repeat in range(1, args.repeats + 1)
                for case in suite["cases"]
            ]
            summaries.append(aggregate(model, task, runs))
            model_rows.append({"model": model, "task": task, "runs": runs})
        unload_model(server, model)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report = {
        "run_id": run_id,
        "suite_version": suite["suite_version"],
        "task_version": TASK_VERSION,
        "suite_sha256": sha256_file(args.cases),
        "tool_sha256": sha256_file(Path(__file__)),
        "design_sha256": sha256_file(DESIGN_PATH),
        "ollama_version": version,
        "server": server,
        "tasks": args.task,
        "case_count": len(suite["cases"]),
        "repeats": args.repeats,
        "num_predict": args.num_predict,
        "dry_run": True,
        "no_trade_signal": True,
        "blocked_actions": BLOCKED_ACTIONS,
        "summaries": summaries,
        "models": model_rows,
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    prefix = f"l3_microtask_bakeoff_{run_id}"
    json_path = args.output_dir / f"{prefix}.json"
    md_path = args.output_dir / f"{prefix}.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    md_path.write_text(render(report), encoding="utf-8")
    print(json.dumps(summaries, ensure_ascii=False, indent=2))
    print(f"json_report={json_path}")
    print(f"markdown_report={md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
