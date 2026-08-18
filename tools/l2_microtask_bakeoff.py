#!/usr/bin/env python3
"""Offline micro-task bake-off for the Zhulong L2 evidence reviewer.

This tool reuses the frozen BL-030 cases but evaluates smaller, separable L2
tasks. It only calls Ollama and writes benchmark artifacts; it never writes
production databases, grants model authority, or changes daemon settings.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import statistics
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from tools.l2_l3_model_bakeoff import (
    BLOCKED_ACTIONS,
    DEFAULT_CASES,
    DEFAULT_OUTPUT_DIR,
    DESIGN_PATH,
    L2_CONTRACT,
    SUITE_VERSION,
    http_json,
    load_suite,
    model_catalog,
    percentile,
    sha256_file,
    unload_model,
)


TASK_VERSION = "L2_MICROTASK_REVIEW_V1"
TASKS = ("conflict", "missing", "claim")
CONFIDENCE = ("LOW", "MEDIUM", "HIGH")
INTERPRETATION = ("SUPPORTED", "REFUTED", "UNVERIFIABLE")
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


def task_payload(case: dict[str, Any], task: str) -> dict[str, Any]:
    payload = {
        "case_id": case["case_id"],
        "market_regime": case["market_regime"],
        "evidence_envelope": _evidence(case),
    }
    if task in {"conflict", "claim"}:
        payload["deterministic_state"] = case["l2"]["deterministic_state"]
    if task in {"claim", "missing"}:
        payload["candidate_interpretation"] = case["l2"]["candidate_interpretation"]
    if task == "missing":
        payload["missing_evidence_options"] = [
            {"missing_id": item_id, "description": description}
            for item_id, description in (case.get("missing_evidence") or {}).items()
        ]
    return payload


def build_prompt(case: dict[str, Any], task: str) -> str:
    payload = json.dumps(task_payload(case, task), ensure_ascii=False, indent=2)
    if task == "conflict":
        instructions = """你是烛龙 L2 的证据冲突检测器。确定性代码已经完成单位、方向和阈值解释。
只检查 evidence_envelope 内是否存在有效、实质性的正反证据冲突。
conflict_present=true 时，只列出构成冲突的 evidence_id；没有冲突时必须返回空数组。
不要判断候选解释是否成立，不要补写事实，不要输出交易建议。
"""
    elif task == "missing":
        instructions = """你是烛龙 L2 的缺失证据提取器。确定性代码已经完成事实解释。
只从 missing_evidence_options 中选择当前解释仍需要核验的 missing_id。
不得创建新 ID，不得把现有 evidence_id 当成 missing_id，不要输出交易建议。
如果输入证据已足够，不要为了填充而选择缺失项。
"""
    else:
        instructions = """你是烛龙 L2 的候选解释定性器。确定性代码已经完成单位、方向和阈值解释。
只判断 candidate_interpretation 相对于有效 evidence_envelope 的状态：
SUPPORTED=证据支持；REFUTED=有效证据明确反驳或解释方向颠倒；UNVERIFIABLE=证据缺失、陈旧或含混，当前无法确认。
不要把证据冲突本身自动等同于 REFUTED。只引用输入中的 evidence_id，不补写数字或事实，不输出交易建议。
"""
    schema = schema_for(task, case)
    return (
        f"{instructions}\n只输出一个符合 {TASK_VERSION} 的 JSON 对象，不要 Markdown、前言或思维过程。\n"
        "数组中的每个 ID 最多出现一次；没有可报告的 ID 时使用空数组。\n"
        f"\nINPUT:\n{payload}\n"
        f"\n输出字段必须严格为：{', '.join(schema['properties'])}。"
    )


def schema_for(task: str, case: dict[str, Any]) -> dict[str, Any]:
    ids = sorted(case["evidence"])
    missing_ids = sorted((case.get("missing_evidence") or {}).keys())
    if task == "conflict":
        props = {
            "contract_version": {"type": "string", "enum": [TASK_VERSION]},
            "conflict_present": {"type": "boolean"},
            "conflict_evidence_ids": {
                "type": "array",
                "items": {"type": "string", "enum": ids},
                "uniqueItems": True,
                "maxItems": 8,
            },
            "confidence": {"type": "string", "enum": list(CONFIDENCE)},
            "summary": {"type": "string", "minLength": 16, "maxLength": 320},
        }
    elif task == "missing":
        props = {
            "contract_version": {"type": "string", "enum": [TASK_VERSION]},
            "missing_evidence_ids": {
                "type": "array",
                "items": {"type": "string", "enum": missing_ids},
                "uniqueItems": True,
                "maxItems": 8,
            },
            "confidence": {"type": "string", "enum": list(CONFIDENCE)},
            "summary": {"type": "string", "minLength": 16, "maxLength": 320},
        }
    else:
        props = {
            "contract_version": {"type": "string", "enum": [TASK_VERSION]},
            "interpretation_status": {"type": "string", "enum": list(INTERPRETATION)},
            "evidence_ids": {
                "type": "array",
                "items": {"type": "string", "enum": ids},
                "uniqueItems": True,
                "maxItems": 8,
            },
            "confidence": {"type": "string", "enum": list(CONFIDENCE)},
            "summary": {"type": "string", "minLength": 16, "maxLength": 320},
        }
    return {"type": "object", "properties": props, "required": list(props)}


def parse_contract(raw: str, case: dict[str, Any], task: str) -> dict[str, Any]:
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("NON_OBJECT_RESPONSE")
    schema = schema_for(task, case)
    if set(payload) != set(schema["properties"]):
        raise ValueError("FIELD_SET_MISMATCH")
    for name, spec in schema["properties"].items():
        value = payload[name]
        if spec["type"] == "boolean" and not isinstance(value, bool):
            raise ValueError(f"FIELD_TYPE:{name}")
        if spec["type"] == "string":
            if not isinstance(value, str) or value not in spec.get("enum", [value]):
                raise ValueError(f"FIELD_VALUE:{name}")
            if not spec.get("minLength", 0) <= len(value) <= spec.get("maxLength", 10**9):
                raise ValueError(f"FIELD_LENGTH:{name}")
        if spec["type"] == "array":
            if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
                raise ValueError(f"FIELD_TYPE:{name}")
            if len(value) != len(set(value)) or len(value) > spec["maxItems"]:
                raise ValueError(f"FIELD_ARRAY_CONTRACT:{name}")
            if set(value).difference(spec["items"].get("enum", [])):
                raise ValueError(f"UNBOUND_ID:{name}")
    return payload


def evaluate(payload: dict[str, Any], case: dict[str, Any], task: str) -> dict[str, Any]:
    expected = case["l2"]["expected"]
    summary = str(payload.get("summary", ""))
    fatal = []
    direction = []
    judgment = []
    if TRADE_ACTION_PATTERN.search(summary):
        fatal.append("TRADE_INSTRUCTION")
    if NUMBER_PATTERN.search(summary):
        fatal.append("INVENTED_NUMBER")
    if task == "conflict":
        actual = bool(payload["conflict_present"])
        expected_conflict = bool(expected["conflict_expected"])
        conflict_ids = set(payload["conflict_evidence_ids"])
        required = set(expected.get("required_any_conflict_ids", []))
        primary = actual == expected_conflict
        if actual and required and not conflict_ids.intersection(required):
            judgment.append("REQUIRED_CONFLICT_EVIDENCE_MISSING")
        if not actual and conflict_ids:
            judgment.append("UNEXPECTED_CONFLICT_EVIDENCE")
        checks = {"primary": primary, "evidence": not judgment}
    elif task == "missing":
        actual = set(payload["missing_evidence_ids"])
        required = set(expected.get("required_missing_ids", []))
        primary = required.issubset(actual)
        checks = {"required_recall": primary, "ids_bound": True}
        if not primary:
            judgment.append("REQUIRED_MISSING_EVIDENCE_NOT_REPORTED")
    else:
        status = str(payload["interpretation_status"])
        allowed = set(expected.get("allowed_interpretation_status", []))
        forbidden = set(expected.get("forbidden_interpretation_status", []))
        primary = status in allowed
        if status in forbidden:
            direction.append(f"FORBIDDEN_INTERPRETATION_STATUS:{status}")
        elif not primary:
            judgment.append(f"INTERPRETATION_STATUS_MISMATCH:{status}")
        required = set(expected.get("required_any_evidence_ids", []))
        actual = set(payload["evidence_ids"])
        checks = {"primary": primary, "evidence": not required or bool(actual.intersection(required))}
        if not checks["evidence"]:
            judgment.append("REQUIRED_EVIDENCE_MISSING")
    return {
        "strict_pass": not fatal and not direction and not judgment,
        "safe_pass": not fatal and not direction,
        "fatal_errors": fatal,
        "direction_errors": direction,
        "judgment_errors": judgment,
        "checks": checks,
    }


def run_one(server: str, model: str, case: dict[str, Any], task: str, repeat: int, timeout: int) -> dict[str, Any]:
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
        "options": {"temperature": 0.0, "top_p": 0.1, "num_ctx": 4096, "num_predict": 320, "num_gpu": 0, "gpu_layers": 0},
    }
    started = time.monotonic()
    response = {}
    parsed = {}
    accepted = False
    error = ""
    evaluation = {"strict_pass": False, "safe_pass": False, "fatal_errors": [], "direction_errors": [], "judgment_errors": [], "checks": {}}
    try:
        response = http_json(f"{server}/api/generate", request, timeout)
        parsed = parse_contract(str(response.get("response", "") or "").strip(), case, task)
        accepted = True
        evaluation = evaluate(parsed, case, task)
    except Exception as exc:
        error = f"{type(exc).__name__}:{str(exc)[:400]}"
    return {
        "case_id": case["case_id"], "task": task, "repeat": repeat,
        "contract_accepted": accepted, "contract_error": error,
        "evaluation": evaluation, "payload": parsed,
        "elapsed_ms": round((time.monotonic() - started) * 1000.0, 2),
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
    fatal = sum(len(item["fatal_errors"]) for item in evaluations)
    direction = sum(len(item["direction_errors"]) for item in evaluations)
    if task == "missing":
        primary = sum(item["checks"].get("required_recall", False) for item in evaluations) / total if total else 0.0
        primary_name = "required_missing_recall"
    else:
        primary = sum(item["checks"].get("primary", False) for item in evaluations) / total if total else 0.0
        primary_name = "task_accuracy"
    elapsed = [float(row["elapsed_ms"]) for row in runs]
    return {
        "model": model, "task": task, "runs": total,
        "contract_accept_rate": round(contract, 4), "safe_pass_rate": round(safe, 4),
        "strict_pass_rate": round(strict, 4), primary_name: round(primary, 4),
        "fatal_fact_errors": fatal, "direction_errors": direction,
        "latency_ms_p50": round(statistics.median(elapsed), 2) if elapsed else 0.0,
        "latency_ms_p95": round(percentile(elapsed, 0.95), 2),
        "status": "MICROTASK_PASS" if contract >= 0.95 and strict >= 0.75 and fatal == 0 and direction == 0 and primary >= 0.75 else "MICROTASK_FAIL",
    }


def render(report: dict[str, Any]) -> str:
    lines = [
        f"# L2 Micro-task Bake-off · {report['run_id']}", "",
        f"- suite: `{report['suite_version']}`", f"- task_version: `{report['task_version']}`",
        f"- cases: `{report['case_count']}`", f"- repeats: `{report['repeats']}`",
        f"- ollama_version: `{report['ollama_version']}`", "- dry_run: `true`", "- no_trade_signal: `true`", "",
        "## Summary", "", "| Model | Task | Status | Schema | Safe | Strict | Task metric | Fatal | Direction | p50 ms | p95 ms |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in report["summaries"]:
        metric = row.get("task_accuracy", row.get("required_missing_recall", 0.0))
        lines.append(f"| {row['model']} | {row['task']} | {row['status']} | {row['contract_accept_rate']:.1%} | {row['safe_pass_rate']:.1%} | {row['strict_pass_rate']:.1%} | {metric:.1%} | {row['fatal_fact_errors']} | {row['direction_errors']} | {row['latency_ms_p50']:.0f} | {row['latency_ms_p95']:.0f} |")
    lines.extend(["", "## Failure Ledger", ""])
    for model_row in report["models"]:
        for row in model_row["runs"]:
            if row["contract_accepted"] and row["evaluation"]["strict_pass"]:
                continue
            reason = row["contract_error"] if not row["contract_accepted"] else json.dumps({"fatal": row["evaluation"]["fatal_errors"], "direction": row["evaluation"]["direction_errors"], "judgment": row["evaluation"]["judgment_errors"]}, ensure_ascii=False)
            lines.append(f"- `{model_row['model']}` / `{row['task']}` / `{row['case_id']}` / repeat {row['repeat']}: {reason}")
    lines.extend(["", "## Boundary", "", "This offline micro-task benchmark does not authorize production model use or write DuckDB/RAG/Shadow/nexus_audits.", ""])
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task", choices=TASKS, action="append", default=[])
    parser.add_argument("--model", action="append", required=True)
    parser.add_argument("--server", default=os.getenv("ZHULONG_OLLAMA_SERVER", "http://192.0.2.20:11434"))
    parser.add_argument("--cases", type=Path, default=DEFAULT_CASES)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--timeout", type=int, default=720)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()
    if not args.task:
        args.task = list(TASKS)
    if args.repeats < 1 or args.repeats > 3:
        parser.error("--repeats must be between 1 and 3")
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
            runs = [run_one(server, model, case, task, repeat, args.timeout) for repeat in range(1, args.repeats + 1) for case in suite["cases"]]
            summaries.append(aggregate(model, task, runs))
            model_rows.append({"model": model, "task": task, "runs": runs})
        unload_model(server, model)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report = {
        "run_id": run_id, "suite_version": suite["suite_version"], "task_version": TASK_VERSION,
        "suite_sha256": sha256_file(args.cases), "tool_sha256": sha256_file(Path(__file__)),
        "design_sha256": sha256_file(DESIGN_PATH), "ollama_version": version, "server": server,
        "tasks": args.task, "case_count": len(suite["cases"]), "repeats": args.repeats,
        "dry_run": True, "no_trade_signal": True, "blocked_actions": BLOCKED_ACTIONS,
        "summaries": summaries, "models": model_rows,
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    prefix = f"l2_microtask_bakeoff_{run_id}"
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
