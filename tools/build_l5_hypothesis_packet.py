#!/usr/bin/env python3
"""Create one bounded, human-gated L5 hypothesis packet from Strategy Episodes."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

ROOT = Path(__file__).resolve().parents[1]
REPORT_DIR = ROOT / "storage/reports/l5_hypotheses"
INPUT_VERSION = "strategy_episode_v0.1"
PACKET_VERSION = "l5_hypothesis_packet_v0.1"
MIN_ARCHETYPE_HYPOTHESIS = 10
MIN_ARCHETYPE_REPLAY = 30
BLOCKED_ACTIONS = ["write_duckdb", "write_rag_memory", "change_strategy", "change_threshold",
                   "generate_trade", "write_shadow", "trigger_daemon", "auto_replay", "auto_promote"]


def validate_source(payload: Dict[str, Any]) -> None:
    if payload.get("schema_version") != INPUT_VERSION:
        raise ValueError("unsupported Strategy Episode schema")
    if payload.get("read_only") is not True or payload.get("no_trade_signal") is not True:
        raise ValueError("unsafe Strategy Episode source flags")
    if "post_outcome" not in str(payload.get("post_outcome_isolation", "")):
        raise ValueError("missing post-outcome isolation contract")


def aggregate(payload: Dict[str, Any]) -> Dict[str, Any]:
    outcomes, categories = Counter(), Counter()
    by_archetype = defaultdict(Counter)
    closed = 0
    for episode in payload.get("episodes", []):
        attribution = episode.get("attribution") or {}
        outcome = str(attribution.get("outcome_class") or "UNKNOWN")
        category = str(attribution.get("attribution_category") or "UNKNOWN")
        archetype = str(((episode.get("ex_ante") or {}).get("archetype") or {}).get("primary") or "UNCLASSIFIED")
        outcomes[outcome] += 1; categories[category] += 1; by_archetype[archetype][outcome] += 1
        if outcome in {"WIN", "LOSS", "FLAT"}: closed += 1
    per_archetype_closed = {key: sum(value.get(name, 0) for name in ("WIN", "LOSS", "FLAT"))
                            for key, value in sorted(by_archetype.items())}
    return {
        "episodes": len(payload.get("episodes", [])), "closed_outcomes": closed,
        "outcomes": dict(sorted(outcomes.items())), "attribution_categories": dict(sorted(categories.items())),
        "by_archetype_outcome": {key: dict(sorted(value.items())) for key, value in sorted(by_archetype.items())},
        "per_archetype_closed": per_archetype_closed,
    }


def build_prompt(stats: Dict[str, Any]) -> str:
    return """You are a bounded strategy research analyst. Analyze only the aggregate historical statistics below.
Do not name or recommend any stock. Do not provide buy/sell instructions, prices, position sizes, or direct parameter changes.
Do not infer causality from correlation. Sample insufficiency must be stated. Produce at most 3 falsifiable research hypotheses.
Each hypothesis must contain: hypothesis_id, observation, affected_archetypes, supporting_counts, proposed_test,
failure_condition, overfitting_risk, and status exactly HUMAN_REVIEW_REQUIRED.
Return strict JSON: {"hypotheses": [...], "sample_warning": "..."}.

Aggregate Strategy Episode statistics:
""" + json.dumps(stats, ensure_ascii=False, sort_keys=True)


def call_ollama_once(prompt: str, url: str, model: str, timeout: int) -> Dict[str, Any]:
    body = json.dumps({"model": model, "prompt": prompt, "stream": False, "format": "json"}).encode("utf-8")
    request = urllib.request.Request(url.rstrip("/") + "/api/generate", data=body,
                                     headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        wrapper = json.loads(response.read().decode("utf-8"))
    result = json.loads(str(wrapper.get("response") or "{}"))
    hypotheses = result.get("hypotheses")
    if not isinstance(hypotheses, list) or len(hypotheses) > 3:
        raise ValueError("LLM response violated hypothesis count contract")
    required = {"hypothesis_id", "observation", "affected_archetypes", "supporting_counts",
                "proposed_test", "failure_condition", "overfitting_risk", "status"}
    for item in hypotheses:
        if not isinstance(item, dict) or not required.issubset(item) or item["status"] != "HUMAN_REVIEW_REQUIRED":
            raise ValueError("LLM response violated hypothesis schema")
    return result


def build_packet(source_path: Path, invoke_llm: bool, model: str, url: str, timeout: int) -> Dict[str, Any]:
    raw = source_path.read_bytes(); source = json.loads(raw.decode("utf-8")); validate_source(source)
    stats = aggregate(source); prompt = build_prompt(stats)
    hypothesis_ready = sorted(key for key, value in stats["per_archetype_closed"].items()
                              if key != "UNCLASSIFIED" and value >= MIN_ARCHETYPE_HYPOTHESIS)
    replay_ready = sorted(key for key, value in stats["per_archetype_closed"].items()
                          if key != "UNCLASSIFIED" and value >= MIN_ARCHETYPE_REPLAY)
    ready = bool(hypothesis_ready)
    if invoke_llm and not ready:
        raise ValueError(f"no archetype has {MIN_ARCHETYPE_HYPOTHESIS}+ closed outcomes")
    result = call_ollama_once(prompt, url, model, timeout) if invoke_llm else None
    return {
        "schema_version": PACKET_VERSION, "generated_at": datetime.now().astimezone().isoformat(),
        "source_report": str(source_path), "source_sha256": hashlib.sha256(raw).hexdigest(),
        "mode": "single_bounded_llm_synthesis" if invoke_llm else "prompt_only",
        "no_trade_signal": True, "read_only": True, "human_review_required": True,
        "auto_replay": False, "auto_promote": False, "sample_gate": {
            "hypothesis_minimum_per_archetype": MIN_ARCHETYPE_HYPOTHESIS,
            "offline_replay_minimum_per_archetype": MIN_ARCHETYPE_REPLAY,
            "observed_closed_outcomes": stats["closed_outcomes"],
            "per_archetype_closed": stats["per_archetype_closed"],
            "hypothesis_ready_archetypes": hypothesis_ready,
            "offline_replay_ready_archetypes": replay_ready,
            "ready_for_llm_synthesis": ready},
        "aggregate_statistics": stats, "prompt": prompt,
        "llm": {"called": invoke_llm, "model": model if invoke_llm else None,
                "max_calls": 1, "result": result},
        "review": {"decision": "PENDING", "reviewer": None, "reviewed_at": None,
                   "selected_hypothesis_ids": [], "note": None},
        "blocked_actions": BLOCKED_ACTIONS,
    }


def render_markdown(packet: Dict[str, Any]) -> str:
    gate = packet["sample_gate"]; stats = packet["aggregate_statistics"]
    lines = ["# L5 Hypothesis Packet", "", f"- mode: `{packet['mode']}`", "- no_trade_signal: true",
             "- human_review_required: true", "- auto_replay: false", "- auto_promote: false", "",
             "## Sample Gate", "", f"- total closed outcomes: {gate['observed_closed_outcomes']}",
             f"- hypothesis minimum per archetype: {gate['hypothesis_minimum_per_archetype']}",
             f"- offline replay minimum per archetype: {gate['offline_replay_minimum_per_archetype']}",
             f"- hypothesis-ready archetypes: {gate['hypothesis_ready_archetypes']}",
             f"- replay-ready archetypes: {gate['offline_replay_ready_archetypes']}",
             f"- ready_for_llm_synthesis: {str(gate['ready_for_llm_synthesis']).lower()}", "",
             "## Aggregate Statistics", "", f"- outcomes: `{json.dumps(stats['outcomes'], ensure_ascii=False)}`",
             f"- attribution: `{json.dumps(stats['attribution_categories'], ensure_ascii=False)}`", "",
             "## Hypotheses", ""]
    result = packet["llm"]["result"] or {}
    if not packet["llm"]["called"]:
        lines.append("No LLM call was made. This packet contains a bounded prompt only.")
    for item in result.get("hypotheses", []):
        lines += [f"### {item['hypothesis_id']}", "", f"- observation: {item['observation']}",
                  f"- proposed_test: {item['proposed_test']}", f"- failure_condition: {item['failure_condition']}",
                  f"- status: `{item['status']}`", ""]
    lines += ["", "## Human Review", "", "- decision: `PENDING`", "- No hypothesis may enter replay until separately reviewed.", ""]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True); parser.add_argument("--batch", required=True)
    parser.add_argument("--output-dir", default=str(REPORT_DIR)); parser.add_argument("--invoke-llm", action="store_true")
    parser.add_argument("--model", default=os.getenv("L5_HYPOTHESIS_MODEL", "qwen2.5:7b"))
    parser.add_argument("--ollama-url", default=os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434"))
    parser.add_argument("--timeout", type=int, default=180); args = parser.parse_args()
    packet = build_packet(Path(args.input).resolve(), args.invoke_llm, args.model, args.ollama_url, args.timeout)
    out = Path(args.output_dir); out.mkdir(parents=True, exist_ok=True)
    json_path = out / f"l5_hypothesis_packet_{args.batch}.json"; md_path = out / f"l5_hypothesis_packet_{args.batch}.md"
    json_path.write_text(json.dumps(packet, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    md_path.write_text(render_markdown(packet), encoding="utf-8")
    print(json.dumps({"json": str(json_path), "markdown": str(md_path), "mode": packet["mode"],
                      "sample_gate": packet["sample_gate"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__": raise SystemExit(main())
