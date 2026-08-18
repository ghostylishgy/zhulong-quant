#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Dual-mode L2 benchmark: prod (Ollama) vs shadow (OpenVINO).

Run on node 121.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import runpy
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = str(PROJECT_ROOT / "storage" / "database" / "zhulong.duckdb")
LOCAL_DB_PATH = str(PROJECT_ROOT / "storage" / "database" / "zhulong.duckdb")
DB_PATH = os.getenv("ZHULONG_DB_PATH", DEFAULT_DB_PATH)
if not Path(DB_PATH).exists() and Path(LOCAL_DB_PATH).exists():
    DB_PATH = LOCAL_DB_PATH

CPU_OLLAMA_URL = os.getenv("ZHULONG_OLLAMA_URL", "http://192.0.2.20:11434/api/generate")
CPU_MODEL = os.getenv("ZHULONG_OLLAMA_MODEL", "deepseek-r1:1.5b")
OV_MODEL_DIR = os.getenv("ZHULONG_OV_MODEL_DIR", "/root/intel/models/deepseek-r1-1.5b-ir")
OV_PYTHON = os.getenv("ZHULONG_OV_PYTHON", "/opt/openvino_venv102/bin/python")
OV_HOST = os.getenv("ZHULONG_OV_HOST", "root@192.0.2.20")
PASS_THRESHOLD = int(os.getenv("ZHULONG_L2_PASS_THRESHOLD", "60"))


_loader_ns = runpy.run_path(str(PROJECT_ROOT / "04_governance" / "lib" / "core" / "module_loader.py"))
load_attr_from_path = _loader_ns["load_attr_from_path"]
DBGateway = load_attr_from_path(
    "db_gateway_dual_mode",
    PROJECT_ROOT / "01_engine" / "lib" / "db_gateway.py",
    "DBGateway",
)
ComputeGateway = load_attr_from_path(
    "compute_gateway_dual_mode",
    PROJECT_ROOT / "02_brain" / "lib" / "compute_gateway.py",
    "ComputeGateway",
)
LOGGER = logging.getLogger("dual_mode_l2_benchmark")
COMPUTE_GATEWAY = ComputeGateway(logger=LOGGER, max_slots=3)

PROMPT_TEMPLATE = """你是烛龙L2主审计官。请基于输入特征进行风险打标，必须输出严格JSON，不要markdown，不要额外文本。
必须字段: reasoning, pattern, risk_score, fact_tags。
约束: risk_score为0-100整数；fact_tags为2-8个标签；reasoning不少于180字。

输入特征:
symbol={symbol}
trade_date={trade_date}
pct_chg={pct_chg:+.4f}
turnover={turnover:.4f}
amount={amount:.2f}
has_top_list={has_top}

输出JSON模板:
{{"reasoning":"中文推演","pattern":"形态名称","risk_score":0,"fact_tags":["#TAG1","#TAG2"]}}
"""


KEYWORDS = [
    "量价",
    "换手",
    "涨跌",
    "成交额",
    "波动",
    "趋势",
    "风险",
    "资金",
    "形态",
    "动量",
]


@dataclass
class Sample:
    symbol: str
    trade_date: str
    pct_chg: float
    turnover: float
    amount: float
    has_top: int

    def prompt(self) -> str:
        return PROMPT_TEMPLATE.format(
            symbol=self.symbol,
            trade_date=self.trade_date,
            pct_chg=float(self.pct_chg),
            turnover=float(self.turnover),
            amount=float(self.amount),
            has_top=int(self.has_top),
        )


def bucket_pct(v: float) -> str:
    if v <= -7:
        return "A_dn7"
    if v <= -3:
        return "B_dn3"
    if v <= 0:
        return "C_flat_dn"
    if v <= 3:
        return "D_flat_up"
    if v <= 7:
        return "E_up7"
    return "F_up7p"


def bucket_turn(v: float) -> str:
    if v < 1:
        return "T0"
    if v < 3:
        return "T1"
    if v < 8:
        return "T2"
    return "T3"


def strict_json_extract(text: str) -> dict[str, Any] | None:
    if not isinstance(text, str) or not text.strip():
        return None
    raw = text.strip().replace("```json", "").replace("```", "")
    depth = 0
    start = -1
    in_str = False
    esc = False
    cands: list[str] = []
    for i, ch in enumerate(raw):
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
            continue
        if ch == "{":
            if depth == 0:
                start = i
            depth += 1
            continue
        if ch == "}" and depth > 0:
            depth -= 1
            if depth == 0 and start >= 0:
                cands.append(raw[start : i + 1])
                start = -1
    for c in cands:
        try:
            p = json.loads(c)
            if isinstance(p, dict):
                return p
        except Exception as exc:
            LOGGER.error("Non-fatal: strict_json_extract candidate parse failed: %s", exc, exc_info=True)
            continue
    return None


def validate_l2(p: dict[str, Any] | None) -> tuple[bool, dict[str, Any]]:
    if not p:
        return False, {}
    required = ("reasoning", "pattern", "risk_score", "fact_tags")
    if any(k not in p for k in required):
        return False, p
    try:
        score = int(p.get("risk_score"))
    except Exception:
        return False, p
    if score < 0 or score > 100:
        return False, p
    if not isinstance(p.get("reasoning"), str) or not p["reasoning"].strip():
        return False, p
    if not isinstance(p.get("pattern"), str) or not p["pattern"].strip():
        return False, p
    tags = p.get("fact_tags")
    if isinstance(tags, str):
        tags = [x.strip() for x in tags.split(",") if x.strip()]
    if not isinstance(tags, list) or len(tags) == 0:
        return False, p
    p["risk_score"] = score
    p["fact_tags"] = [str(x) for x in tags]
    return True, p


def decision_from(parsed: dict[str, Any]) -> int:
    return 1 if int(parsed.get("risk_score", 100)) <= PASS_THRESHOLD else 0


def keyword_density(txt: str) -> float:
    if not txt:
        return 0.0
    n = len(txt)
    hit = sum(txt.count(k) for k in KEYWORDS)
    return float(hit) / float(max(n, 1))


def evidence_tokens(txt: str) -> set[str]:
    if not txt:
        return set()
    nums = re.findall(r"\d+(?:\.\d+)?%?", txt)
    words = [k for k in KEYWORDS if k in txt]
    return set(nums + words)


def jaccard(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 1.0
    u = a | b
    if not u:
        return 1.0
    return len(a & b) / len(u)


def fetch_samples(trade_date: str, max_samples: int, ticker: str | None = None) -> list[Sample]:
    where_clause = "WHERE d.trade_date = CAST(? AS DATE)"
    params: list[Any] = [trade_date]
    if ticker:
        where_clause += " AND d.symbol = ?"
        params.append(ticker)

    sql = f"""
    SELECT
      d.symbol,
      CAST(d.trade_date AS VARCHAR) AS trade_date,
      COALESCE(d.pct_chg, 0) AS pct_chg,
      COALESCE(d.turnover_rate, 0) AS turnover,
      COALESCE(d.amount, 0) AS amount,
      CASE WHEN t.symbol IS NULL THEN 0 ELSE 1 END AS has_top
    FROM fact_daily d
    LEFT JOIN fact_top_list t
      ON d.symbol = t.symbol AND d.trade_date = t.trade_date
    {where_clause}
    """

    with DBGateway(DB_PATH, read_only=True, logger=LOGGER) as conn:
        rows = conn.execute(sql, params).fetchall()

    out: list[Sample] = []
    for r in rows:
        out.append(
            Sample(
                symbol=str(r[0]),
                trade_date=str(r[1]),
                pct_chg=float(r[2] or 0.0),
                turnover=float(r[3] or 0.0),
                amount=float(r[4] or 0.0),
                has_top=int(r[5] or 0),
            )
        )

    if ticker:
        return out[:1]

    by_bucket: dict[str, list[Sample]] = {}
    for s in out:
        k = f"{bucket_pct(s.pct_chg)}|{bucket_turn(s.turnover)}|top{s.has_top}"
        by_bucket.setdefault(k, []).append(s)

    keys = sorted(by_bucket.keys())
    idx = {k: 0 for k in keys}
    selected: list[Sample] = []

    while len(selected) < max_samples:
        progressed = False
        for k in keys:
            arr = by_bucket[k]
            i = idx[k]
            if i < len(arr) and len(selected) < max_samples:
                selected.append(arr[i])
                idx[k] = i + 1
                progressed = True
        if not progressed:
            break

    return selected


def run_cpu(prompts: list[str], timeout: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for p in prompts:
        payload = {
            "model": CPU_MODEL,
            "prompt": p,
            "stream": False,
            "format": "json",
            "options": {
                "temperature": 0.0,
                "num_predict": 384,
                "num_gpu": 0,
                "gpu_layers": 0,
            },
        }
        t0 = time.time()
        try:
            cpu_server = str(CPU_OLLAMA_URL).rsplit("/api/generate", 1)[0]
            r = COMPUTE_GATEWAY.ollama_generate(server=cpu_server, payload=payload, timeout=timeout, layer="BENCH", decision_id="dual_mode_cpu")
            raw = r.json().get("response", "") if r.status_code == 200 else ""
        except Exception:
            raw = ""
        elapsed_ms = (time.time() - t0) * 1000.0
        p0 = strict_json_extract(raw)
        ok, parsed = validate_l2(p0)
        out.append({"ok": ok, "raw": raw, "parsed": parsed, "elapsed_ms": elapsed_ms})
    return out


def run_igpu(prompts: list[str], timeout: int) -> list[dict[str, Any]]:
    remote_code = r"""
import json, sys
import openvino_genai as ovg
model_dir = '/root/intel/models/deepseek-r1-1.5b-ir'
pipe = ovg.LLMPipeline(model_dir, 'GPU')
cfg = ovg.GenerationConfig()
cfg.temperature = 0.0
cfg.max_new_tokens = 384
prompts = json.loads(sys.stdin.read())
outs = []
for p in prompts:
    try:
        txt = str(pipe.generate(p, cfg))
    except Exception:
        txt = ""
    outs.append(txt)
print(json.dumps(outs, ensure_ascii=False))
"""
    t0 = time.time()
    proc = subprocess.run(
        ["ssh", OV_HOST, OV_PYTHON, "-c", remote_code],
        input=json.dumps(prompts, ensure_ascii=False),
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    elapsed_total_ms = (time.time() - t0) * 1000.0
    raw_out = proc.stdout.strip()
    rows: list[str] = []
    try:
        rows = json.loads(raw_out)
    except Exception:
        rows = [""] * len(prompts)

    per_item_ms = elapsed_total_ms / max(1, len(prompts))
    out: list[dict[str, Any]] = []
    for raw in rows[: len(prompts)]:
        p0 = strict_json_extract(raw)
        ok, parsed = validate_l2(p0)
        out.append({"ok": ok, "raw": raw, "parsed": parsed, "elapsed_ms": per_item_ms})
    while len(out) < len(prompts):
        out.append({"ok": False, "raw": "", "parsed": {}, "elapsed_ms": per_item_ms})
    return out


def ensure_nexus_schema(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS nexus_audits (
            id INTEGER PRIMARY KEY,
            task_id TEXT UNIQUE NOT NULL,
            symbol TEXT NOT NULL,
            trade_date TEXT,
            l2_pattern TEXT,
            l2_risk_score INTEGER,
            l2_passed INTEGER,
            l2_elapsed_ms DOUBLE,
            l2_raw_response TEXT,
            l2_parse_ok BOOLEAN,
            l2_device_path TEXT,
            status TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    existing = {
        str(r[1]).lower(): str(r[2]).upper()
        for r in conn.execute("PRAGMA table_info('nexus_audits')").fetchall()
    }
    required = {
        "l2_parse_ok": "BOOLEAN",
        "l2_device_path": "TEXT",
    }
    for col, ctype in required.items():
        if col.lower() not in existing:
            conn.execute(f"ALTER TABLE nexus_audits ADD COLUMN {col} {ctype}")


def upsert_prod_audits(samples: list[Sample], results: list[dict[str, Any]], trade_date: str) -> None:
    with DBGateway(DB_PATH, read_only=False, logger=LOGGER) as conn:
        ensure_nexus_schema(conn)
        now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for s, r in zip(samples, results):
            task_id = f"bench_prod_{trade_date}_{s.symbol}"
            parsed = r.get("parsed") or {}
            parse_ok = bool(r.get("ok"))
            l2_pattern = str(parsed.get("pattern", "L2_PARSE_ERROR")) if parse_ok else "L2_PARSE_ERROR"
            l2_risk = int(parsed.get("risk_score", 100)) if parse_ok else 100
            l2_passed = 1 if (parse_ok and decision_from(parsed) == 1) else 0
            conn.execute(
                """
                INSERT INTO nexus_audits (
                    task_id, symbol, trade_date,
                    l2_pattern, l2_risk_score, l2_passed,
                    l2_elapsed_ms, l2_raw_response,
                    l2_parse_ok, l2_device_path,
                    status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(task_id) DO UPDATE SET
                    l2_pattern=excluded.l2_pattern,
                    l2_risk_score=excluded.l2_risk_score,
                    l2_passed=excluded.l2_passed,
                    l2_elapsed_ms=excluded.l2_elapsed_ms,
                    l2_raw_response=excluded.l2_raw_response,
                    l2_parse_ok=excluded.l2_parse_ok,
                    l2_device_path=excluded.l2_device_path,
                    status=excluded.status,
                    created_at=excluded.created_at
                """,
                [
                    task_id,
                    s.symbol,
                    trade_date,
                    l2_pattern,
                    l2_risk,
                    l2_passed,
                    float(r.get("elapsed_ms", 0.0) or 0.0),
                    str(r.get("raw", ""))[:3000],
                    parse_ok,
                    "ollama_prod",
                    "L2_DONE",
                    now_ts,
                ],
            )


def compute_dual_metrics(samples: list[Sample], cpu: list[dict[str, Any]], igpu: list[dict[str, Any]]) -> dict[str, Any]:
    n = len(samples)
    if n == 0:
        return {}

    struct_cpu = sum(1 for x in cpu if x["ok"]) / n
    struct_igpu = sum(1 for x in igpu if x["ok"]) / n

    drift = []
    evidence = []
    len_ratio = []
    dens_ratio = []
    floor_flags = 0

    for s, c, g in zip(samples, cpu, igpu):
        both_ok = bool(c["ok"] and g["ok"])
        if both_ok:
            dc = decision_from(c["parsed"])
            dg = decision_from(g["parsed"])
            drift.append(1 if dc != dg else 0)
            rc = str(c["parsed"].get("reasoning", ""))
            rg = str(g["parsed"].get("reasoning", ""))
            evidence.append(jaccard(evidence_tokens(rc), evidence_tokens(rg)))
            lc = max(1, len(rc))
            lg = max(1, len(rg))
            len_ratio.append(float(lg) / float(lc))
            kd_c = keyword_density(rc)
            kd_g = keyword_density(rg)
            dens_ratio.append((kd_g + 1e-9) / (kd_c + 1e-9))
            if len(rg) >= 180 and keyword_density(rg) < 0.006:
                floor_flags += 1
        else:
            drift.append(1)
            evidence.append(0.0)
            len_ratio.append(0.0)
            dens_ratio.append(0.0)

    consistency = 1.0 - (sum(drift) / n)
    json_survival = min(struct_cpu, struct_igpu)

    return {
        "json_schema_pass_cpu": round(struct_cpu, 6),
        "json_schema_pass_igpu": round(struct_igpu, 6),
        "json_survival_rate": round(json_survival, 6),
        "decision_consistency_rate": round(consistency, 6),
        "decision_drift_rate": round(1.0 - consistency, 6),
        "evidence_overlap_jaccard": round(sum(evidence) / n, 6),
        "cot_len_ratio_avg": round(sum(len_ratio) / n, 6),
        "cot_keyword_density_ratio_avg": round(sum(dens_ratio) / n, 6),
        "floor_injection_flags": int(floor_flags),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trade-date", default="2026-04-03")
    ap.add_argument("--ticker", "--symbol", dest="ticker", default=None)
    ap.add_argument("--max-samples", type=int, default=20)
    ap.add_argument("--cpu-timeout", type=int, default=180)
    ap.add_argument("--igpu-timeout", type=int, default=3600)
    ap.add_argument(
        "--mode",
        choices=["dual_mode", "prod_only", "shadow_only"],
        default="dual_mode",
        help="dual_mode=CPU+OpenVINO, prod_only=CPU only, shadow_only=OpenVINO only",
    )
    ap.add_argument("--no-write-audit", action="store_true", help="Disable nexus_audits upsert in prod_only")
    ap.add_argument("--out-json", default=None)
    args = ap.parse_args()

    t0 = time.time()
    samples = fetch_samples(args.trade_date, args.max_samples, args.ticker)
    prompts = [s.prompt() for s in samples]

    if len(samples) == 0:
        print("No samples selected.")
        return 2

    cpu: list[dict[str, Any]] = []
    igpu: list[dict[str, Any]] = []

    if args.mode in {"dual_mode", "prod_only"}:
        cpu = run_cpu(prompts, timeout=args.cpu_timeout)

    if args.mode in {"dual_mode", "shadow_only"}:
        igpu = run_igpu(prompts, timeout=args.igpu_timeout)

    report = {
        "mode": args.mode,
        "trade_date": args.trade_date,
        "samples": len(samples),
        "cpu_model": CPU_MODEL,
        "igpu_model_dir": OV_MODEL_DIR,
        "metrics": {},
        "gates": {},
        "elapsed_sec": round(time.time() - t0, 2),
        "samples_detail": [],
    }

    if args.mode == "dual_mode":
        metrics = compute_dual_metrics(samples, cpu, igpu)
        report["metrics"] = metrics
        report["gates"] = {
            "json_hard_fail": metrics.get("json_survival_rate", 0.0) < 1.0,
            "consistency_ge_0p97": metrics.get("decision_consistency_rate", 0.0) >= 0.97,
            "floor_injection_ok": int(metrics.get("floor_injection_flags", 1)) == 0,
        }
        report["samples_detail"] = [
            {
                "symbol": s.symbol,
                "cpu_ok": c["ok"],
                "igpu_ok": g["ok"],
                "cpu_risk": c["parsed"].get("risk_score") if c["ok"] else None,
                "igpu_risk": g["parsed"].get("risk_score") if g["ok"] else None,
            }
            for s, c, g in zip(samples, cpu, igpu)
        ]
    elif args.mode == "prod_only":
        ok_rate = sum(1 for x in cpu if x["ok"]) / len(cpu)
        report["metrics"] = {
            "json_schema_pass_prod": round(ok_rate, 6),
            "decision_pass_rate_prod": round(
                sum(1 for x in cpu if x["ok"] and decision_from(x["parsed"]) == 1) / len(cpu),
                6,
            ),
            "l2_elapsed_ms_avg_prod": round(sum(x.get("elapsed_ms", 0.0) for x in cpu) / len(cpu), 3),
        }
        report["gates"] = {
            "prod_parse_ok_all": ok_rate >= 1.0,
        }
        report["samples_detail"] = [
            {
                "symbol": s.symbol,
                "prod_ok": c["ok"],
                "prod_risk": c["parsed"].get("risk_score") if c["ok"] else None,
                "prod_pattern": c["parsed"].get("pattern") if c["ok"] else None,
            }
            for s, c in zip(samples, cpu)
        ]
        if not args.no_write_audit:
            upsert_prod_audits(samples, cpu, args.trade_date)
    else:
        ok_rate = sum(1 for x in igpu if x["ok"]) / len(igpu)
        report["metrics"] = {
            "json_schema_pass_shadow": round(ok_rate, 6),
            "l2_elapsed_ms_avg_shadow": round(sum(x.get("elapsed_ms", 0.0) for x in igpu) / len(igpu), 3),
        }
        report["gates"] = {
            "shadow_parse_ok_all": ok_rate >= 1.0,
        }
        report["samples_detail"] = [
            {
                "symbol": s.symbol,
                "shadow_ok": g["ok"],
                "shadow_risk": g["parsed"].get("risk_score") if g["ok"] else None,
            }
            for s, g in zip(samples, igpu)
        ]

    out_json = args.out_json
    if not out_json:
        out_json = str(PROJECT_ROOT / "logs" / f"dual_mode_l2_benchmark_{args.trade_date.replace('-', '')}_{args.mode}.json")
    out_path = Path(out_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(report["metrics"], ensure_ascii=False, indent=2))
    print(json.dumps(report["gates"], ensure_ascii=False, indent=2))
    print(f"report={out_path}")
    print(f"db_path={DB_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
