#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Final ignition preflight drill for Zhulong."""

from __future__ import annotations

import argparse
import importlib.util
import os
import platform
import re
import signal
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH_DEFAULT = PROJECT_ROOT / "storage" / "database" / "zhulong.duckdb"
DB_GATEWAY_PATH = PROJECT_ROOT / "01_engine" / "lib" / "db_gateway.py"
DECISION_ENGINE_PATH = PROJECT_ROOT / "02_brain" / "decision_engine.py"
DAEMON_PATH = PROJECT_ROOT / "zhulong_daemon.py"

EXCLUDE_DIRS = {
    ".git",
    "__pycache__",
    "archive",
    "_archive",
    "archive_backups",
    "_archive_backups",
    ".venv",
    "venv",
    "node_modules",
}

RESIDUAL_TEST_PATTERNS = (
    "preflight_drill.py",
    "prelaunch_audit.py",
    "replay_audit.py",
    "probe_l2.py",
    "inspect_zhulong.py",
    "decision_engine.py --mode dry-run",
)


def _print_check(name: str, ok: bool, detail: str) -> None:
    status = "PASS" if ok else "FAIL"
    print(f"[{status}] {name}: {detail}")


def _load_dbgateway():
    spec = importlib.util.spec_from_file_location("db_gateway_preflight", str(DB_GATEWAY_PATH))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"DBGateway loader unavailable: {DB_GATEWAY_PATH}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.DBGateway


def _iter_py_files(root: Path) -> Iterable[Path]:
    for path in root.rglob("*.py"):
        if any(part in EXCLUDE_DIRS for part in path.parts):
            continue
        yield path


def _scan_db_contracts(root: Path) -> Tuple[List[str], List[str]]:
    direct_duck = []
    missing_read_only = []

    duck_pat = re.compile(r"duckdb\.connect\(")
    dbgw_pat = re.compile(r"DBGateway\(")

    for fp in _iter_py_files(root):
        try:
            text = fp.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue

        if duck_pat.search(text):
            direct_duck.append(str(fp))

        lines = text.splitlines()
        for idx, line in enumerate(lines, 1):
            if not dbgw_pat.search(line):
                continue
            if "read_only=" in line:
                continue
            if "def _load_dbgateway" in line:
                continue
            if "DBGateway = _load_dbgateway()" in line:
                continue
            missing_read_only.append(f"{fp}:{idx}")

    return direct_duck, missing_read_only


def _check_decision_engine_contract(path: Path) -> Tuple[bool, str]:
    text = path.read_text(encoding="utf-8", errors="ignore")
    required_snippets = (
        "必须输出字段: pattern, risk_score, fact_tags, detailed_reasoning。",
        "detailed_reasoning必须是中文逻辑推演，不少于200字。",
        '"format": "json"',
        "result.detailed_reasoning = self._enforce_reasoning_floor(",
    )
    missing = [s for s in required_snippets if s not in text]
    if missing:
        return False, f"missing snippets: {len(missing)}"
    return True, "L2 RSN JSON/200-char/Chinese constraints present"


def _check_daemon_schedule_contract(path: Path) -> Tuple[bool, str]:
    text = path.read_text(encoding="utf-8", errors="ignore")
    harvest_ok = "s.add_job(phase_harvest" in text and "CronTrigger(hour=20, minute=30" in text
    audit_ok = "s.add_job(phase_audit" in text and "CronTrigger(hour=21, minute=0" in text
    if harvest_ok and audit_ok:
        return True, "harvest=20:30 and audit=21:00 are locked"
    return False, "missing cron lock for 20:30 harvest or 21:00 audit"


def _ensure_ops_pipeline_state(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ops_pipeline_state (
            trade_date DATE,
            phase VARCHAR,
            status VARCHAR,
            detail VARCHAR,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, phase)
        )
        """
    )


def _ensure_shadow_tables(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_shadow_ledger (
            timestamp TIMESTAMP,
            trade_date DATE,
            trace_id VARCHAR,
            symbol VARCHAR,
            action VARCHAR,
            price_logical DOUBLE,
            price_shadow DOUBLE,
            qty INTEGER,
            tide_mode VARCHAR,
            strategy_tag VARCHAR,
            slippage_cost DOUBLE,
            pricing_mode VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS shadow_dedup_guard (
            dedup_key VARCHAR PRIMARY KEY,
            trace_id VARCHAR,
            symbol VARCHAR,
            action VARCHAR,
            trade_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def _wal_enforce_check(DBGateway, db_path: Path) -> Tuple[bool, str]:
    try:
        with DBGateway(db_path, read_only=False) as conn:
            try:
                conn.execute("PRAGMA journal_mode=WAL")
                return True, "journal_mode=WAL enforced"
            except Exception:
                conn.execute("PRAGMA wal_autocheckpoint='100MB'")
                conn.execute("PRAGMA checkpoint_threshold='1GB'")
                wal_auto = conn.execute("SELECT current_setting('wal_autocheckpoint')").fetchone()
                chk_th = conn.execute("SELECT current_setting('checkpoint_threshold')").fetchone()
                return True, (
                    "duckdb wal policy tuned "
                    f"(wal_autocheckpoint={wal_auto[0] if wal_auto else 'unknown'}, "
                    f"checkpoint_threshold={chk_th[0] if chk_th else 'unknown'})"
                )
    except Exception as exc:
        return False, str(exc)


def _simulate_harvest_done(DBGateway, db_path: Path, trade_date: str) -> Tuple[bool, str]:
    with DBGateway(db_path, read_only=False) as conn:
        _ensure_ops_pipeline_state(conn)
        conn.execute(
            """
            INSERT INTO ops_pipeline_state (trade_date, phase, status, detail)
            VALUES (?, 'phase_harvest', 'DONE', ?)
            ON CONFLICT (trade_date, phase)
            DO UPDATE SET status=EXCLUDED.status, detail=EXCLUDED.detail, updated_at=now()
            """,
            [trade_date, "preflight:phase_harvest:DONE"],
        )
        row = conn.execute(
            "SELECT status, detail FROM ops_pipeline_state WHERE trade_date=? AND phase='phase_harvest'",
            [trade_date],
        ).fetchone()
    if not row:
        return False, "phase_harvest row missing after write"
    return str(row[0]).upper() == "DONE", f"state={row[0]} detail={row[1]}"


def _seed_fact_daily_for_gate(DBGateway, db_path: Path, trade_date: str) -> Tuple[bool, str]:
    with DBGateway(db_path, read_only=False) as conn:
        cnt = conn.execute("SELECT COUNT(*) FROM fact_daily WHERE trade_date=?", [trade_date]).fetchone()
        rows_today = int(cnt[0] or 0)
        if rows_today > 0:
            return True, f"fact_daily already has rows={rows_today}"

        info = conn.execute("PRAGMA table_info('fact_daily')").fetchall()
        if not info:
            return False, "fact_daily table missing"

        columns = [str(r[1]) for r in info]
        types = {str(r[1]): str(r[2]).upper() for r in info}
        lower_map = {c.lower(): c for c in columns}

        if "symbol" not in lower_map or "trade_date" not in lower_map:
            return False, "fact_daily missing symbol/trade_date columns"

        insert_cols = []
        values = []

        for col in columns:
            c_low = col.lower()
            c_type = types[col]
            if c_low == "symbol":
                insert_cols.append(col)
                values.append("__PREFLIGHT__")
            elif c_low == "trade_date":
                insert_cols.append(col)
                values.append(trade_date)
            elif "DOUBLE" in c_type or "FLOAT" in c_type or "DECIMAL" in c_type:
                insert_cols.append(col)
                values.append(0.0)
            elif "INT" in c_type or "BIGINT" in c_type:
                insert_cols.append(col)
                values.append(0)
            elif "DATE" in c_type:
                insert_cols.append(col)
                values.append(trade_date)
            elif "TIMESTAMP" in c_type:
                insert_cols.append(col)
                values.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            elif "VARCHAR" in c_type or "TEXT" in c_type:
                insert_cols.append(col)
                values.append("")

        if len(insert_cols) < 2:
            return False, "fact_daily insert columns unresolved"

        placeholders = ",".join(["?"] * len(insert_cols))
        col_expr = ",".join(insert_cols)
        conn.execute(f"INSERT INTO fact_daily ({col_expr}) VALUES ({placeholders})", values)

    return True, "seeded one __PREFLIGHT__ fact_daily row for gate probe"


def _audit_gate_probe(DBGateway, db_path: Path, trade_date: str) -> Tuple[bool, str]:
    with DBGateway(db_path, read_only=True) as conn:
        row = conn.execute(
            "SELECT status, COALESCE(detail,'') FROM ops_pipeline_state WHERE trade_date=? AND phase='phase_harvest'",
            [trade_date],
        ).fetchone()
        if not row:
            return False, "phase_harvest state missing"
        st = str(row[0] or "").upper()
        detail = str(row[1] or "")
        if st != "DONE":
            return False, f"phase_harvest={st} {detail}".strip()
        daily_count = conn.execute("SELECT COUNT(*) FROM fact_daily WHERE trade_date=?", [trade_date]).fetchone()
        cnt = int(daily_count[0] or 0)
        if cnt <= 0:
            return False, "fact_daily empty for today"
    return True, f"gate opened with fact_daily rows={cnt}"


def _dedup_probe(DBGateway, db_path: Path, trade_date: str) -> Tuple[bool, str, str]:
    trace_id = f"PRECHECK_{trade_date.replace('-', '')}_{int(time.time())}"
    with DBGateway(db_path, read_only=False) as conn:
        _ensure_shadow_tables(conn)
        first = conn.execute(
            """
            INSERT INTO shadow_dedup_guard (dedup_key, trace_id, symbol, action, trade_date)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (dedup_key) DO NOTHING
            RETURNING dedup_key
            """,
            [trace_id, trace_id, "000001.SZ", "BUY", trade_date],
        ).fetchone()
        second = conn.execute(
            """
            INSERT INTO shadow_dedup_guard (dedup_key, trace_id, symbol, action, trade_date)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (dedup_key) DO NOTHING
            RETURNING dedup_key
            """,
            [trace_id, trace_id, "000001.SZ", "BUY", trade_date],
        ).fetchone()

        conn.execute(
            "INSERT INTO fact_shadow_ledger (timestamp, trade_date, trace_id, symbol, action, price_logical, price_shadow, qty, tide_mode, strategy_tag, slippage_cost, pricing_mode) VALUES (CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [trade_date, trace_id, "000001.SZ", "BUY", 10.0, 10.01, 100, "Standard", "preflight", 1.0, "sim"],
        )

    ok = bool(first) and (second is None)
    detail = "first-insert=ok, second-insert=blocked" if ok else "dedup collision not blocked"
    return ok, detail, trace_id


def _cleanup_preflight_rows(DBGateway, db_path: Path, trade_date: str, trace_id: str, mode: str) -> Tuple[bool, str]:
    if mode == "none":
        return True, "cleanup skipped"

    with DBGateway(db_path, read_only=False) as conn:
        conn.execute("DELETE FROM shadow_dedup_guard WHERE trace_id=? OR dedup_key=?", [trace_id, trace_id])
        conn.execute("DELETE FROM fact_shadow_ledger WHERE trace_id=?", [trace_id])
        conn.execute("DELETE FROM fact_daily WHERE trade_date=? AND symbol='__PREFLIGHT__'", [trade_date])

        if mode == "soft":
            conn.execute(
                "DELETE FROM ops_pipeline_state WHERE trade_date=? AND detail LIKE 'preflight:%'",
                [trade_date],
            )
            conn.execute(
                "DELETE FROM nexus_audits WHERE trade_date=? AND (task_id LIKE 'preflight:%' OR l2_error_code LIKE 'PREFLIGHT%')",
                [trade_date],
            )
            return True, "soft cleanup done (preflight-tagged rows)"

        conn.execute("DELETE FROM ops_pipeline_state WHERE trade_date=?", [trade_date])
        conn.execute("DELETE FROM nexus_audits WHERE trade_date=?", [trade_date])
        return True, "hard cleanup done (today rows in ops_pipeline_state + nexus_audits)"


def _read_process_table() -> List[Tuple[int, str]]:
    if platform.system().lower() != "linux":
        return []
    proc = subprocess.run(["ps", "-eo", "pid=,args="], capture_output=True, text=True, check=False)
    rows: List[Tuple[int, str]] = []
    for raw in proc.stdout.splitlines():
        line = raw.strip()
        if not line:
            continue
        parts = line.split(None, 1)
        if not parts:
            continue
        try:
            pid = int(parts[0])
        except ValueError:
            continue
        cmd = parts[1] if len(parts) > 1 else ""
        rows.append((pid, cmd))
    return rows


def _process_probe(expected_daemon_pid: int | None, kill_residual: bool) -> Tuple[bool, str]:
    rows = _read_process_table()
    if not rows:
        return False, "process table unavailable (non-linux or ps failed)"

    daemon = [(pid, cmd) for pid, cmd in rows if "zhulong_daemon.py" in cmd and "grep" not in cmd]
    residual = [
        (pid, cmd)
        for pid, cmd in rows
        if any(token in cmd for token in RESIDUAL_TEST_PATTERNS)
        and pid != os.getpid()
    ]

    killed = 0
    if kill_residual:
        for pid, _ in residual:
            try:
                os.kill(pid, signal.SIGTERM)
                killed += 1
            except Exception:
                pass

    daemon_pids = [pid for pid, _ in daemon]

    if expected_daemon_pid is not None:
        ok = len(daemon_pids) == 1 and daemon_pids[0] == expected_daemon_pid
    else:
        ok = len(daemon_pids) == 1

    detail = (
        f"daemon_pids={daemon_pids}; residual={len(residual)}; killed={killed}; "
        f"expected={expected_daemon_pid if expected_daemon_pid is not None else 'auto-singleton'}"
    )
    return ok, detail


def _ollama_probe(ollama_url: str, model: str, warm: bool) -> Tuple[bool, str]:
    try:
        import requests
    except Exception as exc:
        return False, f"requests import failed: {exc}"

    tags_url = f"{ollama_url.rstrip('/')}/api/tags"
    try:
        resp = requests.get(tags_url, timeout=8)
        resp.raise_for_status()
        models = [m.get("name", "") for m in resp.json().get("models", [])]
    except Exception as exc:
        return False, f"ollama tags probe failed: {exc}"

    if model not in models:
        return False, f"model not loaded: {model}; loaded={models[:6]}"

    if not warm:
        return True, f"model loaded and standby: {model}"

    gen_url = f"{ollama_url.rstrip('/')}/api/generate"
    try:
        payload = {
            "model": model,
            "prompt": "warm",
            "stream": False,
            "keep_alive": "15m",
            "options": {"temperature": 0.0, "num_predict": 1},
        }
        w = requests.post(gen_url, json=payload, timeout=90)
        w.raise_for_status()
    except Exception as exc:
        return False, f"warmup failed: {exc}"

    return True, f"model prewarmed: {model}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Zhulong final ignition preflight drill")
    parser.add_argument("--db", default=str(DB_PATH_DEFAULT), help="DuckDB path")
    parser.add_argument("--trade-date", default=datetime.now().strftime("%Y-%m-%d"), help="trade date YYYY-MM-DD")
    parser.add_argument("--cleanup-mode", choices=("none", "soft", "hard"), default="hard", help="today data cleanup strategy")
    parser.add_argument("--expected-daemon-pid", type=int, default=None, help="expected stable daemon pid")
    parser.add_argument("--kill-residual", action="store_true", help="kill residual test processes")
    parser.add_argument("--ollama-url", default="http://192.0.2.20:11434", help="ollama base url")
    parser.add_argument("--ollama-model", default="lfm-sentinel:latest", help="model expected on ollama")
    parser.add_argument("--warm-ollama", action="store_true", help="trigger lightweight warmup generate call")
    args = parser.parse_args()

    db_path = Path(args.db).resolve()
    if not db_path.exists():
        print(f"[FAIL] DB path missing: {db_path}")
        return 2

    DBGateway = _load_dbgateway()
    failed = 0

    direct_duck, missing_read_only = _scan_db_contracts(PROJECT_ROOT)
    ok_static = (len(direct_duck) == 0) and (len(missing_read_only) == 0)
    detail_static = f"duckdb.connect={len(direct_duck)}, missing_read_only={len(missing_read_only)}"
    _print_check("External Read Contract", ok_static, detail_static)
    if not ok_static:
        failed += 1

    ok_l2, detail_l2 = _check_decision_engine_contract(DECISION_ENGINE_PATH)
    _print_check("L2 RSN Contract", ok_l2, detail_l2)
    if not ok_l2:
        failed += 1

    ok_sched, detail_sched = _check_daemon_schedule_contract(DAEMON_PATH)
    _print_check("Daemon Schedule Lock", ok_sched, detail_sched)
    if not ok_sched:
        failed += 1

    ok_wal, detail_wal = _wal_enforce_check(DBGateway, db_path)
    _print_check("DB WAL Enforce", ok_wal, detail_wal)
    if not ok_wal:
        failed += 1

    ok_harvest, detail_harvest = _simulate_harvest_done(DBGateway, db_path, args.trade_date)
    _print_check("20:30 Harvest Drill", ok_harvest, detail_harvest)
    if not ok_harvest:
        failed += 1

    ok_seed, detail_seed = _seed_fact_daily_for_gate(DBGateway, db_path, args.trade_date)
    _print_check("FactDaily Gate Seed", ok_seed, detail_seed)
    if not ok_seed:
        failed += 1

    ok_gate, detail_gate = _audit_gate_probe(DBGateway, db_path, args.trade_date)
    _print_check("21:00 Audit Gate", ok_gate, detail_gate)
    if not ok_gate:
        failed += 1

    ok_dedup, detail_dedup, trace_id = _dedup_probe(DBGateway, db_path, args.trade_date)
    _print_check("Shadow Dedup Guard", ok_dedup, f"{detail_dedup}; trace_id={trace_id}")
    if not ok_dedup:
        failed += 1

    ok_cleanup, detail_cleanup = _cleanup_preflight_rows(
        DBGateway, db_path, args.trade_date, trace_id, args.cleanup_mode
    )
    _print_check("Data Cleanup", ok_cleanup, detail_cleanup)
    if not ok_cleanup:
        failed += 1

    ok_proc, detail_proc = _process_probe(args.expected_daemon_pid, args.kill_residual)
    _print_check("Process Discipline", ok_proc, detail_proc)
    if not ok_proc:
        failed += 1

    ok_ollama, detail_ollama = _ollama_probe(args.ollama_url, args.ollama_model, args.warm_ollama)
    _print_check("116 Ollama Standby", ok_ollama, detail_ollama)
    if not ok_ollama:
        failed += 1

    print("\n=== Preflight Summary ===")
    print(f"trade_date={args.trade_date}")
    print(f"db={db_path}")
    print(f"cleanup_mode={args.cleanup_mode}")
    print(f"failed_checks={failed}")

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
