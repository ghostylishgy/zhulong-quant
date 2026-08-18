#!/usr/bin/env python3
"""Process queued OBSERVE_ONLY L4 news checks outside the verdict path."""

import argparse
import importlib.util
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = str(ROOT / "storage" / "database" / "zhulong.duckdb")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

ENGINE_LIB = ROOT / "01_engine" / "lib"
if str(ENGINE_LIB) not in sys.path:
    sys.path.insert(0, str(ENGINE_LIB))
from db_gateway import DBGateway

logger = logging.getLogger("zhulong.news_observation_worker")
BEIJING_TZ = timezone(timedelta(hours=8))
PROVIDERS = ("CNINFO", "CLS", "EASTMONEY")


class NewsPersistenceDeferred(RuntimeError):
    """A verified result could not be persisted and must remain queued."""

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / "config" / ".env")
    load_dotenv(ROOT / ".env")
except Exception:
    pass


def load_news_module():
    path = ROOT / "02_brain" / "lib" / "news_verifier.py"
    spec = importlib.util.spec_from_file_location("zhulong_news_observation_verifier", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def parse_as_of(raw: str, trade_date: str) -> datetime:
    text = str(raw or "").strip()
    if text:
        parsed = datetime.fromisoformat(text)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=BEIJING_TZ)
    return datetime.fromisoformat(f"{trade_date}T21:00:00+08:00")


def fetch_queued(limit: int):
    with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
        return conn.execute(
            """
            SELECT task_id, symbol, COALESCE(name, ''), COALESCE(trade_date, ''),
                   COALESCE(l4_news_as_of, ''), COALESCE(l4_final_verdict, 'UNKNOWN'),
                   COALESCE(l4_news_prompt_injected, FALSE),
                   COALESCE(l4_news_gate_applied, FALSE)
            FROM nexus_audits
            WHERE COALESCE(l4_news_policy, '') = 'OBSERVE_ONLY'
              AND COALESCE(l4_news_status, '') = 'NEWS_QUEUED'
              AND COALESCE(status, '') = 'L4_DONE'
            ORDER BY created_at, task_id
            LIMIT ?
            """,
            [int(limit)],
        ).fetchall()


def ensure_schema() -> None:
    with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
        conn.execute(
            "ALTER TABLE nexus_audits ADD COLUMN IF NOT EXISTS l4_news_as_of TEXT"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ops_l4_news_observation_runs (
                worker_run_id VARCHAR PRIMARY KEY,
                started_at TIMESTAMP,
                ended_at TIMESTAMP,
                scheduled_window VARCHAR,
                elapsed_ms DOUBLE DEFAULT 0,
                queued_before INTEGER DEFAULT 0,
                attempted INTEGER DEFAULT 0,
                completed INTEGER DEFAULT 0,
                unavailable INTEGER DEFAULT 0,
                failed INTEGER DEFAULT 0,
                pending_after INTEGER DEFAULT 0,
                cninfo_ok INTEGER DEFAULT 0,
                cls_ok INTEGER DEFAULT 0,
                eastmoney_ok INTEGER DEFAULT 0,
                cninfo_failed INTEGER DEFAULT 0,
                cls_failed INTEGER DEFAULT 0,
                eastmoney_failed INTEGER DEFAULT 0,
                evidence_items INTEGER DEFAULT 0,
                official_items INTEGER DEFAULT 0,
                media_items INTEGER DEFAULT 0,
                gate_none INTEGER DEFAULT 0,
                would_cap_hold INTEGER DEFAULT 0,
                would_veto INTEGER DEFAULT 0,
                counterfactual_changes INTEGER DEFAULT 0,
                safety_violations INTEGER DEFAULT 0,
                latency_p50_ms DOUBLE DEFAULT 0,
                latency_p95_ms DOUBLE DEFAULT 0,
                latency_max_ms DOUBLE DEFAULT 0,
                status VARCHAR DEFAULT 'DONE',
                error_summary VARCHAR DEFAULT ''
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ops_l4_news_manual_reviews (
                task_id VARCHAR PRIMARY KEY,
                source_row_sha256 VARCHAR DEFAULT '',
                source_rows_sha256 VARCHAR DEFAULT '',
                manifest_version VARCHAR DEFAULT '',
                manifest_sha256 VARCHAR DEFAULT '',
                review_label VARCHAR NOT NULL,
                entity_correct BOOLEAN,
                negation_correct BOOLEAN,
                material_false_veto BOOLEAN DEFAULT FALSE,
                reviewer VARCHAR DEFAULT '',
                notes VARCHAR DEFAULT '',
                reviewed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        review_columns = {
            str(row[0]).lower()
            for row in conn.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'ops_l4_news_manual_reviews'
                """
            ).fetchall()
        }
        for column in (
            "source_row_sha256", "source_rows_sha256",
            "manifest_version", "manifest_sha256",
        ):
            if column not in review_columns:
                conn.execute(
                    f"ALTER TABLE ops_l4_news_manual_reviews "
                    f"ADD COLUMN {column} VARCHAR DEFAULT ''"
                )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ops_l4_news_control_state (
                control_key VARCHAR PRIMARY KEY,
                control_value VARCHAR DEFAULT '',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            INSERT INTO ops_l4_news_control_state (control_key, control_value)
            VALUES ('sentence_entity_hardening_complete', '0')
            ON CONFLICT (control_key) DO NOTHING
            """
        )


def persist_result(task_id: str, as_of: datetime, payload: dict) -> None:
    with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
        conn.execute(
            """
            UPDATE nexus_audits
            SET l4_news_status = ?,
                l4_news_risk_level = ?,
                l4_news_risk_score = ?,
                l4_news_gate = ?,
                l4_news_summary = ?,
                l4_news_evidence = ?,
                l4_news_as_of = ?,
                l4_news_checked_at = ?,
                l4_news_prompt_injected = FALSE,
                l4_news_gate_applied = FALSE,
                l4_news_gate_reason = ''
            WHERE task_id = ?
              AND COALESCE(l4_news_policy, '') = 'OBSERVE_ONLY'
              AND COALESCE(l4_news_status, '') = 'NEWS_QUEUED'
            """,
            [
                str(payload.get("status") or "NEWS_UNAVAILABLE")[:64],
                str(payload.get("risk_level") or "UNAVAILABLE")[:64],
                int(payload.get("risk_score") or 0),
                str(payload.get("hypothetical_gate") or "NONE")[:64],
                str(payload.get("summary") or "")[:1000],
                json.dumps(payload, ensure_ascii=False, default=str)[:12000],
                as_of.isoformat(),
                str(payload.get("checked_at") or "")[:32],
                task_id,
            ],
        )


def persist_result_with_retry(
    task_id: str,
    as_of: datetime,
    payload: dict,
    attempts: int | None = None,
    delay_seconds: float | None = None,
) -> None:
    retry_attempts = max(
        1,
        int(attempts or os.getenv("L4_NEWS_PERSIST_RETRY_ATTEMPTS", "3") or 3),
    )
    retry_delay = max(
        0.0,
        float(
            delay_seconds
            if delay_seconds is not None
            else os.getenv("L4_NEWS_PERSIST_RETRY_DELAY_SECONDS", "2.0") or 2.0
        ),
    )
    last_error = None
    for attempt in range(1, retry_attempts + 1):
        try:
            persist_result(task_id, as_of, payload)
            if attempt > 1:
                logger.info(
                    "news observation persistence recovered task_id=%s attempt=%s/%s",
                    task_id,
                    attempt,
                    retry_attempts,
                )
            return
        except Exception as exc:
            last_error = exc
            logger.warning(
                "news observation persistence retry task_id=%s attempt=%s/%s error=%s",
                task_id,
                attempt,
                retry_attempts,
                exc,
            )
            if attempt < retry_attempts:
                time.sleep(retry_delay * attempt)
    raise NewsPersistenceDeferred(
        f"task_id={task_id} persistence failed after {retry_attempts} attempts"
    ) from last_error


def persist_failure(task_id: str, as_of: datetime, exc: Exception) -> None:
    payload = {
        "status": "NEWS_UNAVAILABLE",
        "risk_level": "UNAVAILABLE",
        "risk_score": 0,
        "hypothetical_gate": "NONE",
        "summary": f"Observation worker failed closed: {type(exc).__name__}: {str(exc)[:160]}",
        "checked_at": datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "news_as_of": as_of.isoformat(),
    }
    persist_result_with_retry(task_id, as_of, payload)


def pending_count() -> int:
    with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM nexus_audits
            WHERE COALESCE(l4_news_policy, '') = 'OBSERVE_ONLY'
              AND COALESCE(l4_news_status, '') = 'NEWS_QUEUED'
              AND COALESCE(status, '') = 'L4_DONE'
            """
        ).fetchone()
    return int((row[0] if row else 0) or 0)


def counterfactual_verdict(original: str, hypothetical: str) -> str:
    verdict = str(original or "UNKNOWN").upper()
    gate = str(hypothetical or "NONE").upper()
    if gate == "WOULD_VETO" and verdict != "VETO":
        return "VETO"
    if gate == "WOULD_CAP_HOLD" and verdict == "PASS":
        return "HOLD"
    return verdict


def percentile(values, fraction: float) -> float:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return 0.0
    index = max(0, min(len(ordered) - 1, int(round((len(ordered) - 1) * fraction))))
    return round(ordered[index], 2)


def persist_run(stats: dict) -> None:
    columns = [
        "worker_run_id", "started_at", "ended_at", "scheduled_window", "elapsed_ms",
        "queued_before", "attempted", "completed", "unavailable", "failed", "pending_after",
        "cninfo_ok", "cls_ok", "eastmoney_ok", "cninfo_failed", "cls_failed", "eastmoney_failed",
        "evidence_items", "official_items", "media_items", "gate_none", "would_cap_hold",
        "would_veto", "counterfactual_changes", "safety_violations", "latency_p50_ms",
        "latency_p95_ms", "latency_max_ms", "status", "error_summary",
    ]
    update_columns = [column for column in columns if column != "worker_run_id"]
    with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
        conn.execute(
            f"INSERT INTO ops_l4_news_observation_runs ({','.join(columns)}) "
            f"VALUES ({','.join('?' for _ in columns)}) "
            f"ON CONFLICT (worker_run_id) DO UPDATE SET "
            + ",".join(f"{column}=excluded.{column}" for column in update_columns),
            [stats.get(column) for column in columns],
        )


def _new_worker_stats(worker_run_id: str, started: datetime, window: str) -> dict:
    return {
        "worker_run_id": worker_run_id,
        "started_at": started.strftime("%Y-%m-%d %H:%M:%S"),
        "ended_at": None,
        "scheduled_window": window,
        "queued_before": 0,
        "attempted": 0,
        "completed": 0,
        "unavailable": 0,
        "failed": 0,
        "pending_after": 0,
        "cninfo_ok": 0,
        "cls_ok": 0,
        "eastmoney_ok": 0,
        "cninfo_failed": 0,
        "cls_failed": 0,
        "eastmoney_failed": 0,
        "evidence_items": 0,
        "official_items": 0,
        "media_items": 0,
        "gate_none": 0,
        "would_cap_hold": 0,
        "would_veto": 0,
        "counterfactual_changes": 0,
        "safety_violations": 0,
        "latency_p50_ms": 0.0,
        "latency_p95_ms": 0.0,
        "latency_max_ms": 0.0,
        "elapsed_ms": 0.0,
        "status": "STARTED",
        "error_summary": "",
    }


def rolling_cninfo_availability() -> tuple[int, float]:
    with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
        rows = conn.execute(
            """
            SELECT run_date, SUM(cninfo_ok), SUM(attempted)
            FROM (
                SELECT CAST(ended_at AS DATE) AS run_date, cninfo_ok, attempted
                FROM ops_l4_news_observation_runs
                WHERE attempted > 0
            )
            GROUP BY run_date
            ORDER BY run_date DESC
            LIMIT 5
            """
        ).fetchall()
    attempted = sum(int(row[2] or 0) for row in rows)
    succeeded = sum(int(row[1] or 0) for row in rows)
    return len(rows), (succeeded / attempted if attempted else 0.0)


def send_alert(title: str, content: str) -> bool:
    token = str(os.getenv("PUSHPLUS_TOKEN", "") or "").strip()
    if not token:
        logger.warning("[L4-NEWS-ALERT] PushPlus token unavailable title=%s", title)
        return False
    try:
        response = requests.post(
            "https://www.pushplus.plus/send",
            json={"token": token, "title": title, "content": content, "template": "txt"},
            timeout=10,
        )
        ok = response.status_code == 200
        logger.warning("[L4-NEWS-ALERT] sent=%s status=%s", ok, response.status_code)
        return ok
    except Exception as exc:
        logger.warning("[L4-NEWS-ALERT] send failed: %s", exc)
        return False


def evaluate_alerts(stats: dict) -> list[str]:
    reasons = []
    final_window = str(stats.get("scheduled_window") or "").startswith("01:35")
    if final_window and int(stats.get("pending_after") or 0) > 0:
        reasons.append(f"最后补跑窗口仍有 {stats['pending_after']} 条新闻观察待处理")
    provider_ok = sum(int(stats.get(f"{name}_ok") or 0) for name in ("cninfo", "cls", "eastmoney"))
    attempted = int(stats.get("attempted") or 0)
    if attempted > 0 and provider_ok == 0:
        reasons.append(f"本轮已尝试 {attempted} 只标的，但三路新闻源均未返回有效结果")
    if int(stats.get("safety_violations") or 0) > 0:
        reasons.append(f"安全隔离检查发现 {stats['safety_violations']} 项异常")
    day_count, availability = rolling_cninfo_availability()
    if attempted > 0 and day_count >= 5 and availability < 0.95:
        reasons.append(f"最近 5 个有任务交易日中，巨潮监管公告源可用率为 {availability:.1%}")
    return reasons


def render_alert_content(stats: dict, alerts: list[str]) -> str:
    lines = [
        f"时间窗口：{stats.get('scheduled_window')}",
        f"本轮待观察：{stats.get('queued_before', 0)} 条",
        f"本轮已尝试：{stats.get('attempted', 0)} 条",
        f"完成：{stats.get('completed', 0)} 条",
        f"失败：{stats.get('failed', 0)} 条",
        f"仍待处理：{stats.get('pending_after', 0)} 条",
        "",
        "需要关注：",
    ]
    lines.extend([f"- {item}" for item in alerts])
    lines.extend([
        "",
        "说明：新闻源当前仍处于观察模式，不参与 L4 最终裁决；该提醒只表示观察链路需要排查。",
    ])
    return "\n".join(lines)


def render_worker_failed_content(exc: Exception) -> str:
    return "\n".join([
        "新闻观察后台任务未能正常完成。",
        f"错误类型：{type(exc).__name__}",
        "状态：新闻源仍处于观察模式，不会直接改变 L4 裁决。",
        "",
        "说明：完整错误详情已写入 daemon 日志；手机推送只保留排障摘要。",
    ])


def _run_worker(limit: int, scheduled_window: str, skip_reason: str, stats: dict, started_clock: float) -> dict:
    worker_run_id = str(stats["worker_run_id"])
    queued_total = pending_count()
    rows = fetch_queued(limit)
    stats["queued_before"] = queued_total
    errors = []
    latencies = []
    if skip_reason:
        stats["status"] = f"SKIPPED_{str(skip_reason).upper()}"[:64]
        stats["pending_after"] = pending_count()
        ended = datetime.now(BEIJING_TZ)
        stats["ended_at"] = ended.strftime("%Y-%m-%d %H:%M:%S")
        stats["elapsed_ms"] = round((time.monotonic() - started_clock) * 1000, 2)
        persist_run(stats)
        logger.info(
            "[L4-NEWS-SUMMARY] worker=%s queued=%s completed=0 unavailable=0 "
            "pending=%s status=%s",
            worker_run_id,
            queued_total,
            stats["pending_after"],
            stats["status"],
        )
        return stats

    module = load_news_module()
    timeout = float(os.getenv("L4_NEWS_TIMEOUT_SECONDS", "4.0") or 4.0)
    verifier = module.NewsVerifier(
        cache_path=ROOT / "storage" / "news" / "l4_news.sqlite",
        timeout=max(1.0, min(timeout, 10.0)),
    )
    stats["attempted"] = len(rows)
    for task_id, symbol, name, trade_date, raw_as_of, original_verdict, prompt_injected, gate_applied in rows:
        item_started = time.monotonic()
        try:
            as_of = parse_as_of(raw_as_of, trade_date)
            result = verifier.verify(symbol=symbol, stock_name=name, cutoff_at=as_of)
            payload = result.to_dict()
            payload["policy"] = "OBSERVE_ONLY"
            payload["news_as_of"] = as_of.isoformat()
            elapsed_ms = round((time.monotonic() - item_started) * 1000, 2)
            latencies.append(elapsed_ms)
            hypothetical = str(payload.get("hypothetical_gate") or "NONE").upper()
            derived_verdict = counterfactual_verdict(original_verdict, hypothetical)
            payload["worker_run_id"] = worker_run_id
            payload["fetch_elapsed_ms"] = elapsed_ms
            payload["original_l4_verdict"] = str(original_verdict or "UNKNOWN").upper()
            payload["counterfactual_verdict"] = derived_verdict
            payload["would_change_verdict"] = derived_verdict != str(original_verdict or "UNKNOWN").upper()
            persist_result_with_retry(task_id, as_of, payload)
            stats["completed"] += 1
            if str(payload.get("status") or "").upper() == "NEWS_UNAVAILABLE":
                stats["unavailable"] += 1
            sources_ok = {str(value).upper() for value in (payload.get("sources_ok") or [])}
            sources_failed = {str(value).upper() for value in (payload.get("sources_failed") or {})}
            for provider in PROVIDERS:
                key = provider.lower()
                if provider in sources_ok:
                    stats[f"{key}_ok"] += 1
                if provider in sources_failed:
                    stats[f"{key}_failed"] += 1
            evidence = list(payload.get("evidence") or [])
            stats["evidence_items"] += len(evidence)
            stats["official_items"] += sum(
                str(item.get("source_grade") or "").upper() == "A" for item in evidence
            )
            stats["media_items"] += sum(
                str(item.get("source_grade") or "").upper() in {"B", "C"} for item in evidence
            )
            gate_key = {
                "WOULD_CAP_HOLD": "would_cap_hold",
                "WOULD_VETO": "would_veto",
            }.get(hypothetical, "gate_none")
            stats[gate_key] += 1
            stats["counterfactual_changes"] += int(bool(payload["would_change_verdict"]))
            stats["safety_violations"] += int(bool(prompt_injected) or bool(gate_applied))
            logger.info(
                "[L4-NEWS-OBS] worker=%s task=%s symbol=%s as_of=%s status=%s "
                "sources_ok=%s sources_failed=%s evidence=%s official=%s media=%s "
                "hypothetical=%s elapsed_ms=%.2f",
                worker_run_id,
                task_id,
                symbol,
                as_of.isoformat(),
                payload.get("status") or "NEWS_UNAVAILABLE",
                ",".join(sorted(sources_ok)) or "none",
                ",".join(sorted(sources_failed)) or "none",
                len(evidence),
                sum(str(item.get("source_grade") or "").upper() == "A" for item in evidence),
                sum(str(item.get("source_grade") or "").upper() in {"B", "C"} for item in evidence),
                hypothetical,
                elapsed_ms,
            )
        except NewsPersistenceDeferred as exc:
            logger.error(
                "news observation persistence deferred task_id=%s symbol=%s; "
                "keeping NEWS_QUEUED for the next worker window",
                task_id,
                symbol,
                exc_info=True,
            )
            errors.append(f"{task_id}:PERSIST_DEFERRED:{str(exc)[:100]}")
            stats["failed"] += 1
        except Exception as exc:
            logger.exception("news observation failed task_id=%s symbol=%s", task_id, symbol)
            errors.append(f"{task_id}:{type(exc).__name__}:{str(exc)[:100]}")
            try:
                as_of = parse_as_of(raw_as_of, trade_date)
                persist_failure(task_id, as_of, exc)
            except Exception:
                logger.exception("news observation failure persistence failed task_id=%s", task_id)
            stats["failed"] += 1
    stats["pending_after"] = pending_count()
    stats["latency_p50_ms"] = percentile(latencies, 0.50)
    stats["latency_p95_ms"] = percentile(latencies, 0.95)
    stats["latency_max_ms"] = round(max(latencies), 2) if latencies else 0.0
    stats["status"] = "DONE"
    if stats["failed"] and stats["failed"] >= stats["attempted"]:
        stats["status"] = "FAILED"
    elif stats["failed"] or stats["unavailable"] or stats["pending_after"]:
        stats["status"] = "PARTIAL"
    stats["error_summary"] = " | ".join(errors)[:2000]
    ended = datetime.now(BEIJING_TZ)
    stats["ended_at"] = ended.strftime("%Y-%m-%d %H:%M:%S")
    stats["elapsed_ms"] = round((time.monotonic() - started_clock) * 1000, 2)
    persist_run(stats)
    alerts = evaluate_alerts(stats)
    if alerts:
        send_alert(
            "烛龙新闻观察链路提醒",
            render_alert_content(stats, alerts),
        )
    logger.info(
        "[L4-NEWS-SUMMARY] worker=%s queued=%s completed=%s unavailable=%s pending=%s "
        "cninfo=%s/%s cls=%s/%s eastmoney=%s/%s none=%s cap_hold=%s veto=%s "
        "counterfactual_changes=%s p50_ms=%.2f p95_ms=%.2f status=%s",
        worker_run_id,
        stats["queued_before"],
        stats["completed"],
        stats["unavailable"],
        stats["pending_after"],
        stats["cninfo_ok"],
        stats["attempted"],
        stats["cls_ok"],
        stats["attempted"],
        stats["eastmoney_ok"],
        stats["attempted"],
        stats["gate_none"],
        stats["would_cap_hold"],
        stats["would_veto"],
        stats["counterfactual_changes"],
        stats["latency_p50_ms"],
        stats["latency_p95_ms"],
        stats["status"],
    )
    return stats


def run(limit: int, scheduled_window: str = "manual", skip_reason: str = "") -> dict:
    ensure_schema()
    started = datetime.now(BEIJING_TZ)
    started_clock = time.monotonic()
    window = (
        f"{started.hour:02d}:35"
        if str(scheduled_window or "").lower() == "auto"
        else str(scheduled_window or "manual")
    )
    stats = _new_worker_stats(uuid.uuid4().hex[:12], started, window)
    persist_run(stats)
    try:
        return _run_worker(limit, scheduled_window, skip_reason, stats, started_clock)
    except Exception as exc:
        ended = datetime.now(BEIJING_TZ)
        stats["status"] = "FAILED"
        stats["failed"] = max(1, int(stats.get("failed") or 0))
        stats["ended_at"] = ended.strftime("%Y-%m-%d %H:%M:%S")
        stats["elapsed_ms"] = round((time.monotonic() - started_clock) * 1000, 2)
        stats["error_summary"] = (
            f"FATAL:{type(exc).__name__}:{str(exc)[:500]}"
        )[:2000]
        try:
            persist_run(stats)
        except Exception:
            logger.exception(
                "news observation fatal-run persistence failed worker=%s",
                stats["worker_run_id"],
            )
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=30)
    parser.add_argument("--scheduled-window", default="manual")
    parser.add_argument("--skip-reason", default="")
    args = parser.parse_args()
    try:
        print(json.dumps(
            run(
                max(1, args.limit),
                scheduled_window=args.scheduled_window,
                skip_reason=args.skip_reason,
            ),
            ensure_ascii=False,
        ))
    except Exception as exc:
        logger.exception("news observation worker fatal error")
        send_alert(
            "烛龙新闻观察任务异常",
            render_worker_failed_content(exc),
        )
        raise
