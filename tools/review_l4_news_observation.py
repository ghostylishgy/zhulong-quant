#!/usr/bin/env python3
"""Read-only graduation review for L4 news observation evidence."""

import argparse
import json
import logging
import os
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = str(ROOT / "storage" / "database" / "zhulong.duckdb")
TOOLS_DIR = ROOT / "tools"
ENGINE_LIB = ROOT / "01_engine" / "lib"
for path in (ROOT, TOOLS_DIR, ENGINE_LIB):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from db_gateway import DBGateway
import process_l4_news_observations as observation_worker

logger = logging.getLogger("zhulong.news_observation_review")
BEIJING_TZ = timezone(timedelta(hours=8))
RISK_GATES = {"WOULD_CAP_HOLD", "WOULD_VETO"}

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / "config" / ".env")
    load_dotenv(ROOT / ".env")
except Exception:
    pass


def ensure_review_schema() -> None:
    observation_worker.DB_PATH = DB_PATH
    observation_worker.ensure_schema()
    with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ops_l4_news_graduation_reviews (
                review_run_id VARCHAR PRIMARY KEY,
                evaluated_at TIMESTAMP,
                start_date DATE,
                end_date DATE,
                trading_days INTEGER DEFAULT 0,
                eligible_candidates INTEGER DEFAULT 0,
                completed_candidates INTEGER DEFAULT 0,
                risk_events INTEGER DEFAULT 0,
                manual_reviews INTEGER DEFAULT 0,
                queue_completion DOUBLE DEFAULT 0,
                cninfo_availability DOUBLE DEFAULT 0,
                best_media_availability DOUBLE DEFAULT 0,
                material_false_vetoes INTEGER DEFAULT 0,
                hardening_complete BOOLEAN DEFAULT FALSE,
                decision VARCHAR DEFAULT 'NOT_READY',
                details_json VARCHAR DEFAULT '{}'
            )
            """
        )


def load_rows(start_date: str, end_date: str):
    with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
        rows = conn.execute(
            """
            SELECT task_id, trade_date, COALESCE(l4_news_status, ''),
                   COALESCE(l4_news_gate, 'NONE'), COALESCE(l4_news_evidence, '{}'),
                   COALESCE(l4_news_prompt_injected, FALSE),
                   COALESCE(l4_news_gate_applied, FALSE)
            FROM nexus_audits
            WHERE COALESCE(l4_news_policy, '') = 'OBSERVE_ONLY'
              AND COALESCE(status, '') = 'L4_DONE'
              AND CAST(trade_date AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
            ORDER BY trade_date, task_id
            """,
            [start_date, end_date],
        ).fetchall()
        reviews = conn.execute(
            """
            SELECT task_id, review_label, COALESCE(material_false_veto, FALSE)
            FROM ops_l4_news_manual_reviews
            WHERE length(COALESCE(source_row_sha256, '')) = 64
              AND length(COALESCE(source_rows_sha256, '')) = 64
              AND length(COALESCE(manifest_sha256, '')) = 64
              AND COALESCE(manifest_version, '') != ''
            """
        ).fetchall()
        control = conn.execute(
            """
            SELECT COALESCE(control_value, '0')
            FROM ops_l4_news_control_state
            WHERE control_key = 'sentence_entity_hardening_complete'
            """
        ).fetchone()
    return rows, {str(row[0]): row for row in reviews}, str(control[0] if control else "0")


def source_sets(raw_payload: str) -> tuple[set[str], set[str]]:
    try:
        payload = json.loads(raw_payload or "{}")
    except (TypeError, json.JSONDecodeError):
        return set(), set()
    ok = {str(value).upper() for value in (payload.get("sources_ok") or [])}
    failed_raw = payload.get("sources_failed") or {}
    failed = {str(value).upper() for value in failed_raw}
    return ok, failed


def build_report(start_date: str, end_date: str) -> dict:
    rows, manual_reviews, hardening_value = load_rows(start_date, end_date)
    completed_rows = [row for row in rows if str(row[2]).upper() not in {"", "NEWS_QUEUED", "NEWS_NOT_CHECKED"}]
    risk_rows = [row for row in completed_rows if str(row[3]).upper() in RISK_GATES]
    source_ok = {"CNINFO": 0, "CLS": 0, "EASTMONEY": 0}
    source_failed = {"CNINFO": 0, "CLS": 0, "EASTMONEY": 0}
    invalid_payloads = 0
    for row in completed_rows:
        ok, failed = source_sets(row[4])
        if not ok and not failed:
            invalid_payloads += 1
        for provider in source_ok:
            source_ok[provider] += int(provider in ok)
            source_failed[provider] += int(provider in failed)
    denominator = len(completed_rows)
    availability = {
        provider: (source_ok[provider] / denominator if denominator else 0.0)
        for provider in source_ok
    }
    risk_task_ids = {str(row[0]) for row in risk_rows}
    reviewed_risks = risk_task_ids & set(manual_reviews)
    material_false_vetoes = sum(
        bool(manual_reviews[task_id][2])
        for task_id in reviewed_risks
        if str(next(row[3] for row in risk_rows if str(row[0]) == task_id)).upper() == "WOULD_VETO"
    )
    safety_violations = sum(bool(row[5]) or bool(row[6]) for row in rows)
    trading_days = len({str(row[1]) for row in rows})
    queue_completion = denominator / len(rows) if rows else 0.0
    manual_coverage = len(reviewed_risks) / len(risk_rows) if risk_rows else 0.0
    hardening_complete = hardening_value.strip().lower() in {"1", "true", "yes", "on"}
    deployed_policy = str(os.getenv("L4_NEWS_POLICY", "OBSERVE_ONLY") or "OBSERVE_ONLY").upper()
    conditions = {
        "trading_days": {"value": trading_days, "target": 15, "passed": trading_days >= 15},
        "eligible_candidates": {"value": len(rows), "target": 100, "passed": len(rows) >= 100},
        "risk_events": {"value": len(risk_rows), "target": 20, "passed": len(risk_rows) >= 20},
        "queue_completion": {"value": queue_completion, "target": 0.99, "passed": queue_completion >= 0.99},
        "cninfo_availability": {"value": availability["CNINFO"], "target": 0.95, "passed": availability["CNINFO"] >= 0.95},
        "best_media_availability": {
            "value": max(availability["CLS"], availability["EASTMONEY"]),
            "target": 0.90,
            "passed": max(availability["CLS"], availability["EASTMONEY"]) >= 0.90,
        },
        "manual_review_coverage": {"value": manual_coverage, "target": 1.0, "passed": bool(risk_rows) and manual_coverage >= 1.0},
        "zero_material_false_veto": {"value": material_false_vetoes, "target": 0, "passed": bool(risk_rows) and manual_coverage >= 1.0 and material_false_vetoes == 0},
        "sentence_entity_hardening": {"value": hardening_complete, "target": True, "passed": hardening_complete},
        "observation_safety": {"value": safety_violations, "target": 0, "passed": safety_violations == 0},
        "deployment_policy": {"value": deployed_policy, "target": "OBSERVE_ONLY", "passed": deployed_policy == "OBSERVE_ONLY"},
    }
    ready = all(item["passed"] for item in conditions.values())
    return {
        "review_run_id": uuid.uuid4().hex[:12],
        "evaluated_at": datetime.now(BEIJING_TZ).isoformat(),
        "start_date": start_date,
        "end_date": end_date,
        "decision": "READY_FOR_DESIGN_REVIEW" if ready else "NOT_READY",
        "policy_unchanged": deployed_policy == "OBSERVE_ONLY",
        "metrics": {
            "trading_days": trading_days,
            "eligible_candidates": len(rows),
            "completed_candidates": denominator,
            "risk_events": len(risk_rows),
            "manual_reviews": len(reviewed_risks),
            "manual_review_coverage": manual_coverage,
            "queue_completion": queue_completion,
            "source_ok": source_ok,
            "source_failed": source_failed,
            "source_availability": availability,
            "material_false_vetoes": material_false_vetoes,
            "safety_violations": safety_violations,
            "invalid_payloads": invalid_payloads,
            "hardening_complete": hardening_complete,
        },
        "conditions": conditions,
    }


def persist_report(report: dict) -> None:
    metrics = report["metrics"]
    with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
        conn.execute(
            """
            INSERT INTO ops_l4_news_graduation_reviews
                (review_run_id, evaluated_at, start_date, end_date, trading_days,
                 eligible_candidates, completed_candidates, risk_events, manual_reviews,
                 queue_completion, cninfo_availability, best_media_availability,
                 material_false_vetoes, hardening_complete, decision, details_json)
            VALUES (?, CAST(? AS TIMESTAMP), CAST(? AS DATE), CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                report["review_run_id"],
                report["evaluated_at"],
                report["start_date"],
                report["end_date"],
                metrics["trading_days"],
                metrics["eligible_candidates"],
                metrics["completed_candidates"],
                metrics["risk_events"],
                metrics["manual_reviews"],
                metrics["queue_completion"],
                metrics["source_availability"]["CNINFO"],
                max(metrics["source_availability"]["CLS"], metrics["source_availability"]["EASTMONEY"]),
                metrics["material_false_vetoes"],
                metrics["hardening_complete"],
                report["decision"],
                json.dumps(report, ensure_ascii=False, default=str),
            ],
        )


def render_markdown(report: dict) -> str:
    lines = [
        "# L4 News Observation Graduation Review",
        "",
        f"- Window: `{report['start_date']}` to `{report['end_date']}`",
        f"- Decision: **{report['decision']}**",
        "- Policy changed: **NO**",
        "",
        "| Condition | Value | Target | Passed |",
        "| --- | ---: | ---: | :---: |",
    ]
    for name, item in report["conditions"].items():
        lines.append(
            f"| {name} | {item['value']} | {item['target']} | {'YES' if item['passed'] else 'NO'} |"
        )
    return "\n".join(lines) + "\n"


def run(start_date: str, end_date: str, output: Path | None = None) -> dict:
    ensure_review_schema()
    report = build_report(start_date, end_date)
    persist_report(report)
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(render_markdown(report), encoding="utf-8")
    logger.info(
        "[L4-NEWS-REVIEW] decision=%s days=%s eligible=%s completed=%s risk=%s reviewed=%s policy_unchanged=true",
        report["decision"],
        report["metrics"]["trading_days"],
        report["metrics"]["eligible_candidates"],
        report["metrics"]["completed_candidates"],
        report["metrics"]["risk_events"],
        report["metrics"]["manual_reviews"],
    )
    return report


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", default="2026-06-22")
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    print(json.dumps(run(args.start_date, args.end_date, args.output), ensure_ascii=False, indent=2))
