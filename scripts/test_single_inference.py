#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Single inference probe for raw output (no filtering)."""

from __future__ import annotations

import argparse
import logging
import os
import runpy
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = str(PROJECT_ROOT / "storage" / "database" / "zhulong.duckdb")
LOCAL_DB_PATH = str(PROJECT_ROOT / "storage" / "database" / "zhulong.duckdb")
DB_PATH = os.getenv("ZHULONG_DB_PATH", DEFAULT_DB_PATH)
if not Path(DB_PATH).exists() and Path(LOCAL_DB_PATH).exists():
    DB_PATH = LOCAL_DB_PATH

CPU_URL = os.getenv("ZHULONG_OLLAMA_URL", "http://192.0.2.20:11434/api/generate")
CPU_MODEL = os.getenv("ZHULONG_OLLAMA_MODEL", "deepseek-r1:1.5b")
OV_HOST = os.getenv("ZHULONG_OV_HOST", "root@192.0.2.20")
OV_PY = os.getenv("ZHULONG_OV_PYTHON", "/opt/openvino_venv102/bin/python")
OV_MODEL_DIR = os.getenv("ZHULONG_OV_MODEL_DIR", "/root/intel/models/deepseek-r1-1.5b-ir")

_loader_ns = runpy.run_path(str(PROJECT_ROOT / "04_governance" / "lib" / "core" / "module_loader.py"))
load_attr_from_path = _loader_ns["load_attr_from_path"]
DBGateway = load_attr_from_path(
    "db_gateway_single_probe",
    PROJECT_ROOT / "01_engine" / "lib" / "db_gateway.py",
    "DBGateway",
)
ComputeGateway = load_attr_from_path(
    "compute_gateway_single_probe",
    PROJECT_ROOT / "02_brain" / "lib" / "compute_gateway.py",
    "ComputeGateway",
)
LOGGER = logging.getLogger("test_single_inference")
COMPUTE_GATEWAY = ComputeGateway(logger=LOGGER, max_slots=3)

def load_feature(symbol: str, trade_date: str | None):
    with DBGateway(DB_PATH, read_only=True, logger=LOGGER) as conn:
        if trade_date:
            row = conn.execute(
                """
                SELECT CAST(d.trade_date AS VARCHAR), d.pct_chg, d.turnover_rate, d.amount,
                       CASE WHEN t.symbol IS NULL THEN 0 ELSE 1 END AS has_top
                FROM fact_daily d
                LEFT JOIN fact_top_list t ON d.symbol=t.symbol AND d.trade_date=t.trade_date
                WHERE d.symbol=? AND d.trade_date=CAST(? AS DATE)
                LIMIT 1
                """,
                [symbol, trade_date],
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT CAST(d.trade_date AS VARCHAR), d.pct_chg, d.turnover_rate, d.amount,
                       CASE WHEN t.symbol IS NULL THEN 0 ELSE 1 END AS has_top
                FROM fact_daily d
                LEFT JOIN fact_top_list t ON d.symbol=t.symbol AND d.trade_date=t.trade_date
                WHERE d.symbol=?
                ORDER BY d.trade_date DESC
                LIMIT 1
                """,
                [symbol],
            ).fetchone()
    if not row:
        raise RuntimeError(f"No fact_daily row for {symbol}")
    return {
        "trade_date": str(row[0]),
        "pct_chg": float(row[1] or 0.0),
        "turnover": float(row[2] or 0.0),
        "amount": float(row[3] or 0.0),
        "has_top": int(row[4] or 0),
    }


def build_prompt(symbol: str, f: dict):
    return f"""你是烛龙L2主审计官。请基于输入特征进行风险打标，必须输出严格JSON，不要markdown，不要额外文本。
必须字段: reasoning, pattern, risk_score, fact_tags。
约束: risk_score为0-100整数；fact_tags为2-8个标签；reasoning不少于180字。

输入特征:
symbol={symbol}
trade_date={f['trade_date']}
pct_chg={f['pct_chg']:+.4f}
turnover={f['turnover']:.4f}
amount={f['amount']:.2f}
has_top_list={f['has_top']}

输出JSON模板:
{{"reasoning":"中文推演","pattern":"形态名称","risk_score":0,"fact_tags":["#TAG1","#TAG2"]}}
"""


def infer_cpu(prompt: str) -> str:
    payload = {
        "model": CPU_MODEL,
        "prompt": prompt,
        "stream": False,
        "format": "json",
        "options": {"temperature": 0.0, "num_gpu": 0, "gpu_layers": 0, "num_predict": 512},
    }
    cpu_server = str(CPU_URL).rsplit("/api/generate", 1)[0]
    r = COMPUTE_GATEWAY.ollama_generate(server=cpu_server, payload=payload, timeout=240, layer="SMOKE", decision_id="single_probe_cpu")
    if r.status_code != 200:
        return f"HTTP_{r.status_code}: {r.text[:500]}"
    return str(r.json().get("response", ""))


def infer_igpu(prompt: str) -> str:
    remote_script = f"""
import openvino_genai as ovg
prompt = {prompt!r}
pipe = ovg.LLMPipeline({OV_MODEL_DIR!r}, 'GPU')
cfg = ovg.GenerationConfig()
cfg.temperature = 0.0
cfg.max_new_tokens = 512
print(str(pipe.generate(prompt, cfg)))
"""
    p = subprocess.run(
        ["ssh", OV_HOST, OV_PY, "-"],
        input=remote_script,
        text=True,
        capture_output=True,
        timeout=1800,
        check=False,
    )
    if p.returncode != 0:
        return f"SSH_RC_{p.returncode}\nSTDERR:\n{p.stderr[:1200]}\nSTDOUT:\n{p.stdout[:400]}"
    return p.stdout


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", "--ticker", dest="symbol", required=True)
    ap.add_argument("--device", default="iGPU", choices=["iGPU", "CPU"])
    ap.add_argument("--trade-date", default="2026-04-03")
    args = ap.parse_args()

    f = load_feature(args.symbol, args.trade_date)
    prompt = build_prompt(args.symbol, f)

    print("=== INPUT ===")
    print(
        f"symbol={args.symbol} trade_date={f['trade_date']} pct_chg={f['pct_chg']:+.4f} "
        f"turnover={f['turnover']:.4f} amount={f['amount']:.2f} has_top={f['has_top']}"
    )
    print("=== DEVICE ===")
    print(args.device)
    print("=== RAW OUTPUT (NO FILTER) ===")

    if args.device == "CPU":
        out = infer_cpu(prompt)
    else:
        out = infer_igpu(prompt)

    print(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
