#!/usr/bin/env python3
"""Initialize or apply a hash-bound human review for L5 hypotheses."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

VERSION = "l5_hypothesis_review_v0.1"
PACKET_VERSION = "l5_hypothesis_packet_v0.1"
DECISIONS = {"APPROVE_FOR_OFFLINE_REPLAY", "REJECT", "DEFER"}
BLOCKED_ACTIONS = ["run_replay", "auto_promote", "change_strategy", "change_threshold",
                   "generate_trade", "write_duckdb", "write_rag_memory", "write_shadow", "trigger_daemon"]


def sha_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def row_sha(item: Dict[str, Any]) -> str:
    return sha_bytes(json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8"))


def load_packet(path: Path) -> tuple[Dict[str, Any], str]:
    raw = path.read_bytes(); packet = json.loads(raw.decode("utf-8"))
    if packet.get("schema_version") != PACKET_VERSION or packet.get("no_trade_signal") is not True:
        raise ValueError("unsafe or unsupported hypothesis packet")
    hypotheses = ((packet.get("llm") or {}).get("result") or {}).get("hypotheses")
    if not isinstance(hypotheses, list) or not hypotheses:
        raise ValueError("packet has no schema-validated LLM hypotheses to review")
    return packet, sha_bytes(raw)


def initialize(packet_path: Path) -> Dict[str, Any]:
    packet, packet_sha = load_packet(packet_path)
    rows = []
    for item in packet["llm"]["result"]["hypotheses"]:
        rows.append({"hypothesis_id": item["hypothesis_id"], "hypothesis_sha256": row_sha(item),
                     "decision": "PENDING", "review_note": ""})
    return {"schema_version": VERSION, "mode": "human_review_manifest",
            "source_packet": str(packet_path), "source_packet_sha256": packet_sha,
            "reviewer": "", "reviewed_at": "", "rows": rows,
            "no_trade_signal": True, "run_replay": False, "auto_promote": False,
            "blocked_actions": BLOCKED_ACTIONS}


def apply_review(packet_path: Path, manifest_path: Path) -> Dict[str, Any]:
    packet, packet_sha = load_packet(packet_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != VERSION or manifest.get("source_packet_sha256") != packet_sha:
        raise ValueError("stale or unsupported review manifest")
    if manifest.get("no_trade_signal") is not True or manifest.get("run_replay") is not False:
        raise ValueError("unsafe review manifest flags")
    reviewer = str(manifest.get("reviewer") or "").strip(); reviewed_at = str(manifest.get("reviewed_at") or "").strip()
    if not reviewer or not reviewed_at:
        raise ValueError("reviewer and timezone-aware reviewed_at are required")
    parsed_time = datetime.fromisoformat(reviewed_at.replace("Z", "+00:00"))
    if parsed_time.tzinfo is None:
        raise ValueError("reviewed_at must include timezone")
    source = {item["hypothesis_id"]: item for item in packet["llm"]["result"]["hypotheses"]}
    replay_ready = set((packet.get("sample_gate") or {}).get("offline_replay_ready_archetypes") or [])
    rows = manifest.get("rows") or []
    if len(rows) != len(source) or len({row.get("hypothesis_id") for row in rows}) != len(rows):
        raise ValueError("review rows must exactly cover unique source hypotheses")
    reviewed, approved = [], []
    for row in rows:
        hypothesis_id = row.get("hypothesis_id"); item = source.get(hypothesis_id)
        if not item or row.get("hypothesis_sha256") != row_sha(item):
            raise ValueError(f"unknown or modified hypothesis: {hypothesis_id}")
        decision = str(row.get("decision") or "").upper(); note = str(row.get("review_note") or "").strip()
        if decision not in DECISIONS or not note:
            raise ValueError(f"final decision and review note required: {hypothesis_id}")
        affected = set(item.get("affected_archetypes") or [])
        if decision == "APPROVE_FOR_OFFLINE_REPLAY" and (not affected or not affected.issubset(replay_ready)):
            raise ValueError(f"archetype sample gate blocks offline replay approval: {hypothesis_id}")
        reviewed.append({"hypothesis_id": hypothesis_id, "decision": decision, "review_note": note})
        if decision == "APPROVE_FOR_OFFLINE_REPLAY": approved.append(item)
    return {"schema_version": VERSION, "generated_at": datetime.now().astimezone().isoformat(),
            "source_packet": str(packet_path), "source_packet_sha256": packet_sha,
            "reviewer": reviewer, "reviewed_at": reviewed_at, "reviewed_rows": reviewed,
            "approved_for_offline_replay": approved, "approved_count": len(approved),
            "no_trade_signal": True, "run_replay": False, "auto_promote": False,
            "blocked_actions": BLOCKED_ACTIONS}


def render_markdown(result: Dict[str, Any]) -> str:
    lines = ["# L5 Hypothesis Human Review", "", f"- reviewer: {result['reviewer']}",
             f"- reviewed_at: {result['reviewed_at']}", f"- approved_for_offline_replay: {result['approved_count']}",
             "- replay executed: false", "- auto promotion: false", "", "| Hypothesis | Decision | Note |", "|---|---|---|"]
    lines += [f"| {row['hypothesis_id']} | {row['decision']} | {row['review_note']} |" for row in result["reviewed_rows"]]
    lines += ["", "Approval only authorizes a separate offline replay design. It does not change the live strategy.", ""]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__); sub = parser.add_subparsers(dest="command", required=True)
    init = sub.add_parser("init"); init.add_argument("--packet", required=True); init.add_argument("--output", required=True)
    apply = sub.add_parser("apply"); apply.add_argument("--packet", required=True); apply.add_argument("--manifest", required=True)
    apply.add_argument("--output-json", required=True); apply.add_argument("--output-md", required=True)
    args = parser.parse_args()
    if args.command == "init":
        result = initialize(Path(args.packet).resolve()); Path(args.output).write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({"manifest": args.output, "rows": len(result["rows"])}, ensure_ascii=False)); return 0
    result = apply_review(Path(args.packet).resolve(), Path(args.manifest).resolve())
    Path(args.output_json).write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    Path(args.output_md).write_text(render_markdown(result), encoding="utf-8")
    print(json.dumps({"review": args.output_json, "preview": args.output_md, "approved_count": result["approved_count"]}, ensure_ascii=False)); return 0


if __name__ == "__main__": raise SystemExit(main())
