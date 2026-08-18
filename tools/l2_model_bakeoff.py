#!/usr/bin/env python3
"""Offline L2_OBSERVER_V5 model bake-off.

The tool calls Ollama and writes report artifacts only. It never writes DuckDB,
changes daemon configuration, or grants model authority.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import math
import os
import re
import statistics
import sys
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CASES = PROJECT_ROOT / "tests" / "fixtures" / "l2_observer_bakeoff_v1.json"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "storage" / "reports" / "model_benchmarks"
DECISION_ENGINE_PATH = PROJECT_ROOT / "02_brain" / "decision_engine.py"
CONTRACT_PATH = PROJECT_ROOT / "docs" / "design" / "bl030_l2_l3_rag_reasoning_contract_v0.1.md"
HARD_ERROR_TERMS = (
    "AUTHORITY_FIELD",
    "UNBOUND_EVIDENCE_ID",
    "MALFORMED_EVIDENCE_ID",
    "TRADE_INSTRUCTION",
    "METRIC_SEMANTICS",
    "UNSUPPORTED_EVIDENCE",
)
BLOCKED_ACTIONS = [
    "write_duckdb",
    "change_daemon_config",
    "restart_daemon",
    "change_l2_authority",
    "write_rag_memory",
    "write_shadow",
    "write_nexus_audits",
    "generate_trade_signal",
]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil(fraction * len(ordered)) - 1))
    return float(ordered[index])


def load_decision_engine() -> Any:
    os.environ.setdefault("ZHULONG_DAEMON_LOG_PATH", "/tmp/zhulong_l2_bakeoff_daemon.log")
    name = "zhulong_l2_bakeoff_decision_engine"
    spec = importlib.util.spec_from_file_location(name, DECISION_ENGINE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("DECISION_ENGINE_IMPORT_SPEC_FAILED")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def load_cases(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if payload.get("suite_version") != "L2_OBSERVER_BAKEOFF_V1":
        raise ValueError("UNSUPPORTED_SUITE_VERSION")
    cases = payload.get("cases")
    if not isinstance(cases, list) or not cases:
        raise ValueError("EMPTY_BAKEOFF_CASES")
    case_ids = [str(item.get("case_id", "")) for item in cases]
    if not all(case_ids) or len(case_ids) != len(set(case_ids)):
        raise ValueError("INVALID_OR_DUPLICATE_CASE_ID")
    return payload


def http_json(url: str, payload: dict[str, Any] | None, timeout: int) -> dict[str, Any]:
    data = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="GET" if payload is None else "POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP_{exc.code}:{body[:400]}") from exc
    parsed = json.loads(body)
    if not isinstance(parsed, dict):
        raise RuntimeError("OLLAMA_NON_OBJECT_RESPONSE")
    return parsed


def model_catalog(server: str) -> tuple[str, dict[str, dict[str, Any]]]:
    version = str(http_json(f"{server}/api/version", None, 15).get("version", "UNKNOWN"))
    rows = http_json(f"{server}/api/tags", None, 30).get("models", [])
    catalog = {}
    for row in rows if isinstance(rows, list) else []:
        if isinstance(row, dict):
            name = str(row.get("name", ""))
            if name:
                catalog[name] = row
    return version, catalog


def evaluate_semantics(payload: dict[str, Any], expected: dict[str, Any]) -> dict[str, Any]:
    state = str(payload.get("structure_state", "")).upper()
    support = {str(item).upper() for item in payload.get("supporting_evidence_ids", [])}
    risk = {str(item).upper() for item in payload.get("risk_evidence_ids", [])}
    summary = str(payload.get("summary", ""))
    checks: dict[str, bool] = {}

    allowed_states = {str(item).upper() for item in expected.get("allowed_states", [])}
    checks["state"] = not allowed_states or state in allowed_states

    required_support = {str(item).upper() for item in expected.get("required_any_support", [])}
    checks["support"] = not required_support or bool(support.intersection(required_support))

    required_risk = {str(item).upper() for item in expected.get("required_any_risk", [])}
    checks["risk"] = not required_risk or bool(risk.intersection(required_risk))

    min_missing = int(expected.get("min_missing", 0) or 0)
    checks["missing"] = len(payload.get("missing_evidence", [])) >= min_missing

    min_conflicts = int(expected.get("min_conflicts", 0) or 0)
    checks["conflicts"] = len(payload.get("evidence_conflicts", [])) >= min_conflicts

    forbidden = [str(item) for item in expected.get("forbidden_summary_patterns", [])]
    checks["forbidden_summary"] = not any(re.search(pattern, summary, re.I) for pattern in forbidden)
    return {"passed": all(checks.values()), "checks": checks}


def aggregate_model(model: str, runs: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(runs)
    accepted = [row for row in runs if row.get("contract_accepted")]
    semantic_passed = [row for row in runs if row.get("semantic", {}).get("passed")]
    states = Counter(str(row.get("payload", {}).get("structure_state", "")) for row in accepted)
    state_total = sum(states.values())
    max_state_share = max(states.values()) / state_total if state_total else 1.0
    elapsed = [float(row.get("elapsed_ms", 0.0)) for row in runs]
    token_rates = [float(row.get("eval_tokens_per_second", 0.0)) for row in runs if row.get("eval_tokens_per_second")]
    hard_errors = [
        row for row in runs
        if any(term in str(row.get("error", "")) for term in HARD_ERROR_TERMS)
    ]

    by_case: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in runs:
        by_case[str(row.get("case_id", ""))].append(row)
    stable_cases = 0
    for rows in by_case.values():
        case_states = {
            str(row.get("payload", {}).get("structure_state", ""))
            for row in rows if row.get("contract_accepted")
        }
        if len(case_states) == 1 and len(rows) == sum(bool(row.get("contract_accepted")) for row in rows):
            stable_cases += 1
    stability = stable_cases / len(by_case) if by_case else 0.0

    accept_rate = len(accepted) / total if total else 0.0
    semantic_rate = len(semantic_passed) / total if total else 0.0
    gates = {
        "contract_accept_rate_ge_0_90": accept_rate >= 0.90,
        "semantic_pass_rate_ge_0_75": semantic_rate >= 0.75,
        "hard_contract_violations_zero": not hard_errors,
        "max_state_share_le_0_75": max_state_share <= 0.75,
    }
    return {
        "model": model,
        "runs": total,
        "contract_accept_rate": round(accept_rate, 4),
        "semantic_pass_rate": round(semantic_rate, 4),
        "repeat_state_stability": round(stability, 4),
        "distinct_states": sorted(key for key in states if key),
        "state_counts": dict(sorted(states.items())),
        "max_state_share": round(max_state_share, 4),
        "hard_contract_violations": len(hard_errors),
        "thinking_nonempty_rate": round(
            sum(bool(row.get("thinking_chars")) for row in runs) / total if total else 0.0,
            4,
        ),
        "latency_ms_p50": round(statistics.median(elapsed), 2) if elapsed else 0.0,
        "latency_ms_p95": round(percentile(elapsed, 0.95), 2),
        "eval_tokens_per_second_avg": round(statistics.mean(token_rates), 3) if token_rates else 0.0,
        "gates": gates,
        "status": "PROVISIONAL_CANDIDATE" if all(gates.values()) else "DISQUALIFIED",
    }


def run_one(
    module: Any,
    server: str,
    model: str,
    case: dict[str, Any],
    repeat: int,
    timeout: int,
    think: bool,
) -> dict[str, Any]:
    candidate = module.Candidate(**case["candidate"])
    auditor = module.L2SentinelAuditor(server=server, model=model)
    prompt, ledger = auditor._build_observer_prompt(candidate)
    allowed_ids = sorted(auditor._allowed_evidence_ids(ledger))
    schema = module._default_l2_observer_schema(allowed_ids)
    request_payload = {
        "model": model,
        "prompt": prompt,
        "system": "Return only the L2_OBSERVER_V5 JSON object. You have no scoring or trading authority.",
        "stream": False,
        "format": schema,
        "think": think,
        "keep_alive": "5m",
        "options": {
            "temperature": 0.0,
            "top_p": 0.1,
            "num_ctx": 4096,
            "num_predict": 384,
            "num_gpu": 0,
            "gpu_layers": 0,
        },
    }
    started = time.monotonic()
    response: dict[str, Any] = {}
    raw = ""
    normalized: dict[str, Any] = {}
    accepted = False
    error = ""
    try:
        response = http_json(f"{server}/api/generate", request_payload, timeout)
        raw = str(response.get("response", "") or "").strip()
        parsed = auditor._extract_final_json(raw)
        normalized = auditor._validate_observer_payload(parsed, ledger)
        accepted = True
    except Exception as exc:
        error = f"{type(exc).__name__}:{str(exc)[:500]}"
    elapsed_ms = (time.monotonic() - started) * 1000.0
    eval_count = int(response.get("eval_count", 0) or 0)
    eval_duration = int(response.get("eval_duration", 0) or 0)
    token_rate = eval_count / (eval_duration / 1_000_000_000) if eval_count and eval_duration else 0.0
    semantic = evaluate_semantics(normalized, case.get("expected", {})) if accepted else {"passed": False, "checks": {}}
    return {
        "case_id": case["case_id"],
        "repeat": repeat,
        "contract_accepted": accepted,
        "semantic": semantic,
        "payload": normalized,
        "error": error,
        "elapsed_ms": round(elapsed_ms, 2),
        "eval_count": eval_count,
        "eval_tokens_per_second": round(token_rate, 3),
        "thinking_chars": len(str(response.get("thinking", "") or "")),
        "raw_response": raw[:4000],
        "prompt_sha256": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
    }


def unload_model(server: str, model: str) -> None:
    try:
        http_json(
            f"{server}/api/generate",
            {"model": model, "prompt": "", "stream": False, "keep_alive": 0},
            60,
        )
    except Exception:
        pass


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        f"# L2 Model Bake-off Report · {report['run_id']}",
        "",
        f"- suite: `{report['suite_version']}`",
        f"- contract: `L2_OBSERVER_V5`",
        f"- ollama_version: `{report['ollama_version']}`",
        f"- repeats: `{report['repeats']}`",
        f"- thinking_enabled: `{str(report['thinking_enabled']).lower()}`",
        f"- dry_run: `{str(report['dry_run']).lower()}`",
        f"- no_trade_signal: `{str(report['no_trade_signal']).lower()}`",
        "",
        "## Summary",
        "",
        "| Model | Status | Contract | Semantic | Stability | States | Max share | p50 ms | p95 ms | tok/s |",
        "|---|---|---:|---:|---:|---|---:|---:|---:|---:|",
    ]
    for item in report["summaries"]:
        lines.append(
            "| {model} | {status} | {contract_accept_rate:.2%} | {semantic_pass_rate:.2%} | "
            "{repeat_state_stability:.2%} | {states} | {max_state_share:.2%} | {latency_ms_p50:.0f} | "
            "{latency_ms_p95:.0f} | {eval_tokens_per_second_avg:.2f} |".format(
                states=", ".join(item["distinct_states"]) or "-", **item
            )
        )
    lines.extend(["", "## Failed Cases", ""])
    failures = 0
    for model_row in report["models"]:
        for row in model_row["runs"]:
            if row["contract_accepted"] and row["semantic"]["passed"]:
                continue
            failures += 1
            reason = row["error"] or json.dumps(row["semantic"]["checks"], ensure_ascii=False)
            lines.append(f"- `{model_row['model']}` / `{row['case_id']}` / repeat {row['repeat']}: {reason}")
    if not failures:
        lines.append("- None")
    lines.extend([
        "",
        "## Boundary",
        "",
        "This report is an offline model-quality comparison. It does not authorize a production model, change L2 authority, write DuckDB/RAG/Shadow, or generate a trade signal.",
        "",
    ])
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", action="append", required=True, help="Repeat for each installed Ollama model")
    parser.add_argument("--server", default=os.getenv("ZHULONG_OLLAMA_SERVER", "http://192.0.2.20:11434"))
    parser.add_argument("--cases", type=Path, default=DEFAULT_CASES)
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--timeout", type=int, default=480)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--think",
        action="store_true",
        help="Enable a model's native thinking channel. Raw thinking is never persisted.",
    )
    args = parser.parse_args()
    if args.repeats < 1 or args.repeats > 5:
        parser.error("--repeats must be between 1 and 5")

    suite = load_cases(args.cases)
    module = load_decision_engine()
    server = args.server.rstrip("/")
    version, catalog = model_catalog(server)
    missing = [model for model in args.model if model not in catalog]
    if missing:
        raise SystemExit("Models are not installed: " + ", ".join(missing))

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    model_rows = []
    summaries = []
    for model in args.model:
        runs = []
        for repeat in range(1, args.repeats + 1):
            for case in suite["cases"]:
                runs.append(run_one(module, server, model, case, repeat, args.timeout, args.think))
        unload_model(server, model)
        summary = aggregate_model(model, runs)
        summaries.append(summary)
        model_rows.append({"model": model, "metadata": catalog[model], "summary": summary, "runs": runs})

    report = {
        "run_id": run_id,
        "suite_version": suite["suite_version"],
        "suite_sha256": sha256_file(args.cases),
        "decision_engine_sha256": sha256_file(DECISION_ENGINE_PATH),
        "contract_sha256": sha256_file(CONTRACT_PATH),
        "ollama_version": version,
        "server": server,
        "repeats": args.repeats,
        "thinking_enabled": bool(args.think),
        "dry_run": True,
        "no_trade_signal": True,
        "blocked_actions": BLOCKED_ACTIONS,
        "summaries": summaries,
        "models": model_rows,
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    json_path = args.output_dir / f"l2_model_bakeoff_{run_id}.json"
    md_path = args.output_dir / f"l2_model_bakeoff_{run_id}.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    md_path.write_text(render_markdown(report), encoding="utf-8")
    print(json.dumps(summaries, ensure_ascii=False, indent=2))
    print(f"json_report={json_path}")
    print(f"markdown_report={md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
