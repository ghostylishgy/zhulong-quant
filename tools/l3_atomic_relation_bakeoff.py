#!/usr/bin/env python3
"""Offline L3 v0.3 atomic relation bakeoff.

Only calls the configured Ollama endpoint and writes benchmark artifacts. It
never writes DuckDB, changes daemon configuration, or grants model authority.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import statistics
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tools.l2_l3_model_bakeoff import http_json, model_catalog, unload_model
from tools.l2_l3_v03_contract import (
    CONTRACT_VERSION,
    RELATIONS,
    build_l3_model_payload,
    validate_atomic_claim,
    validate_condition_card,
    validate_l3_model_output,
)

DEFAULT_CASES = PROJECT_ROOT / "tests" / "fixtures" / "l2_l3_atomic_relation_v03.json"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "storage" / "reports" / "model_benchmarks"
CONTRACT = "L3_ATOMIC_RELATION_V0_3"
BLOCKED_ACTIONS = [
    "write_duckdb",
    "change_daemon_config",
    "restart_daemon",
    "change_l2_authority",
    "change_l3_authority",
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


def load_cases(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("contract_version") != CONTRACT_VERSION:
        raise ValueError("UNSUPPORTED_CONTRACT_VERSION")
    cases = payload.get("cases")
    if not isinstance(cases, list) or len(cases) < 24:
        raise ValueError("INSUFFICIENT_ATOMIC_CASES")
    ids = [str(case.get("case_id", "")) for case in cases]
    if not ids or len(ids) != len(set(ids)):
        raise ValueError("INVALID_CASE_IDS")
    for case in cases:
        validate_atomic_claim(case["claim"])
        validate_condition_card(case["condition"])
        build_l3_model_payload(case["claim"], case["condition"])
        cards = case.get("evidence_cards")
        if not isinstance(cards, list) or not cards:
            raise ValueError(f"MISSING_EVIDENCE_CARDS:{case['case_id']}")
        allowed = {str(card["evidence_id"]) for card in cards}
        if not set(case["condition"]["evidence_ids"]).issubset(allowed):
            raise ValueError(f"UNBOUND_CASE_EVIDENCE:{case['case_id']}")
        if case.get("expected_relation") not in RELATIONS:
            raise ValueError(f"INVALID_EXPECTED_RELATION:{case['case_id']}")
    return cases


def model_payload(case: dict[str, Any]) -> dict[str, Any]:
    return build_l3_model_payload(case["claim"], case["condition"])


def schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "relation": {"type": "string", "enum": list(RELATIONS)},
            "bound_evidence_ids": {
                "type": "array",
                "items": {"type": "string"},
                "uniqueItems": True,
                "maxItems": 4,
            },
        },
        "required": ["relation", "bound_evidence_ids"],
        "additionalProperties": False,
    }


def prompt_for(case: dict[str, Any]) -> str:
    payload = json.dumps(model_payload(case), ensure_ascii=False, indent=2)
    return (
        "你是烛龙 L3 的原子命题关系审查员。只做文本命题关系判断，不做投资方向判断，不做生命周期判断，不做交易建议。\n"
        "关系基准：条件确认后，claim_text 命题为真的可信度如何变化。\n"
        "SUPPORTS=命题更可能为真；CONTRADICTS=命题更不可能为真；UNRELATED=对命题真假没有直接影响。\n"
        "claim_text 和 hypothesis 都是同一变量的肯定式原子陈述。不要推断跨变量因果，不要把语言表面的负面词转换成买卖建议。\n"
        "只从 condition_card.evidence_ids 中选择 bound_evidence_ids；没有可绑定证据时返回空数组。\n"
        "只输出一个 JSON 对象，不要 Markdown、解释或思维过程。\n\n"
        f"INPUT:\n{payload}"
    )


def run_one(server: str, model: str, case: dict[str, Any], timeout: int) -> dict[str, Any]:
    started = time.monotonic()
    raw = ""
    try:
        response = http_json(
            f"{server.rstrip('/')}/api/generate",
            {
                "model": model,
                "prompt": prompt_for(case),
                "stream": False,
                "format": schema(),
                "options": {"temperature": 0, "top_p": 0.1, "num_predict": 180},
            },
            timeout,
        )
        raw = str(response.get("response", ""))
        output = validate_l3_model_output(json.loads(raw), case["condition"]["evidence_ids"])
        expected = str(case["expected_relation"])
        actual_ids = set(output["bound_evidence_ids"])
        expected_ids = set(case["condition"]["evidence_ids"])
        return {
            "case_id": case["case_id"],
            "contract_accepted": True,
            "relation": output["relation"],
            "expected_relation": expected,
            "relation_correct": output["relation"] == expected,
            "evidence_recall": expected_ids.issubset(actual_ids),
            "evidence_precision": actual_ids.issubset(expected_ids),
            "raw_response": raw[:2000],
            "elapsed_ms": round((time.monotonic() - started) * 1000, 1),
        }
    except Exception as exc:
        return {
            "case_id": case["case_id"],
            "contract_accepted": False,
            "error": f"{type(exc).__name__}:{exc}",
            "raw_response": raw[:2000],
            "elapsed_ms": round((time.monotonic() - started) * 1000, 1),
        }


def summarize(model: str, rows: list[dict[str, Any]], cases: list[dict[str, Any]]) -> dict[str, Any]:
    expected_counts = Counter(str(case["expected_relation"]) for case in cases)
    accepted = [row for row in rows if row.get("contract_accepted")]
    actual_counts = Counter(str(row.get("relation")) for row in accepted)
    matrix = {label: {item: 0 for item in RELATIONS} for label in RELATIONS}
    for row in accepted:
        matrix[str(row["expected_relation"])][str(row["relation"])] += 1
    correct = sum(int(bool(row.get("relation_correct"))) for row in accepted)
    per_class = {}
    for label in RELATIONS:
        total = expected_counts[label]
        per_class[label] = {
            "count": total,
            "recall": matrix[label][label] / total if total else 0.0,
        }
    majority = max(expected_counts.values()) / len(cases)
    accuracy = correct / len(accepted) if accepted else 0.0
    elapsed = [float(row["elapsed_ms"]) for row in rows]
    return {
        "model": model,
        "case_count": len(cases),
        "contract_acceptance_rate": len(accepted) / len(rows) if rows else 0.0,
        "relation_accuracy_on_accepted": accuracy,
        "evidence_recall": statistics.mean(bool(row["evidence_recall"]) for row in accepted) if accepted else 0.0,
        "evidence_precision": statistics.mean(bool(row["evidence_precision"]) for row in accepted) if accepted else 0.0,
        "expected_class_counts": dict(expected_counts),
        "actual_class_counts": dict(actual_counts),
        "per_class": per_class,
        "confusion_matrix": matrix,
        "majority_baseline": majority,
        "model_above_majority_by": accuracy - majority,
        "max_actual_class_share": max(actual_counts.values()) / len(accepted) if accepted else 1.0,
        "mean_elapsed_ms": statistics.mean(elapsed) if elapsed else 0.0,
        "p95_elapsed_ms": sorted(elapsed)[max(0, int(len(elapsed) * 0.95) - 1)] if elapsed else 0.0,
        "status": "PASS" if len(accepted) == len(cases) and accuracy >= 0.90 else "OBSERVE",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", action="append", required=True)
    parser.add_argument("--server", default=os.getenv("ZHULONG_OLLAMA_SERVER", "http://192.0.2.20:11434"))
    parser.add_argument("--cases", type=Path, default=DEFAULT_CASES)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--timeout", type=int, default=720)
    args = parser.parse_args()
    cases = load_cases(args.cases)
    server = args.server.rstrip("/")
    version, catalog = model_catalog(server)
    missing = [model for model in args.model if model not in catalog]
    if missing:
        raise SystemExit("Models are not installed: " + ", ".join(missing))
    model_rows = []
    for model in args.model:
        rows = [run_one(server, model, case, args.timeout) for case in cases]
        unload_model(server, model)
        model_rows.append({
            "model": model,
            "metadata": catalog[model],
            "summary": summarize(model, rows, cases),
            "runs": rows,
        })
    report = {
        "run_id": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
        "tool": "l3_atomic_relation_bakeoff.py",
        "contract_version": CONTRACT_VERSION,
        "cases_sha256": sha256_file(args.cases),
        "ollama_version": version,
        "server": server,
        "dry_run": True,
        "no_trade_signal": True,
        "blocked_actions": BLOCKED_ACTIONS,
        "models": model_rows,
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    output = args.output_dir / f"l3_atomic_relation_{report['run_id']}.json"
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report["models"], ensure_ascii=False, indent=2))
    print(f"REPORT={output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
