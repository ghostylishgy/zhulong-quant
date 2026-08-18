#!/usr/bin/env python3
"""Point-in-time Trade Archetype observer.

Read-only by default. --apply writes only fact_trade_archetype_observations;
no production decision or Shadow path consumes that table.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
for path in (ROOT, ROOT / "01_engine/lib"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
from db_gateway import DBGateway

DB_PATH = str(ROOT / "storage/database/zhulong.duckdb")
logger = logging.getLogger("zhulong.trade_archetype_observer")


def load_classifier():
    path = ROOT / "02_brain/lib/trade_archetype.py"
    spec = importlib.util.spec_from_file_location("zhulong_trade_archetype", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


ARCH = load_classifier()


def json_dict(value: Any) -> Dict[str, Any]:
    try:
        result = value if isinstance(value, dict) else json.loads(str(value or "{}"))
        return result if isinstance(result, dict) else {}
    except Exception:
        return {}


def news_context(raw: Any) -> Tuple[List[str], bool]:
    payload = json_dict(raw)
    tags = {str(x).strip().upper() for x in payload.get("positive_tags") or []}
    official = False
    for item in payload.get("evidence") or []:
        if not isinstance(item, dict):
            continue
        item_tags = [str(x).strip().upper() for x in item.get("positive_tags") or []]
        tags.update(item_tags)
        official = official or bool(item_tags and str(item.get("source_grade") or "").upper() == "A")
    return sorted(x for x in tags if x), official


def limit_pct(symbol: str) -> float:
    code = symbol.split(".")[0]
    if symbol.endswith(".BJ") or code.startswith(("4", "8", "9")):
        return 30.0
    return 20.0 if code.startswith(("300", "301", "688")) else 10.0


def market_features(conn, symbol: str, trade_date: str) -> Dict[str, Any]:
    rows = conn.execute(
        """
        SELECT COALESCE(close,0), COALESCE(pct_chg,0), COALESCE(turnover_rate,0),
               COALESCE(vol,0), COALESCE(vol_ma5,0), COALESCE(ma20,0)
        FROM fact_daily
        WHERE symbol=? AND CAST(trade_date AS DATE)<=CAST(? AS DATE) AND close>0
        ORDER BY trade_date DESC LIMIT 12
        """, [symbol, trade_date]).fetchall()
    if not rows:
        return {}
    cur = rows[0]
    prev = rows[1] if len(rows) > 1 else (0, 0, 0, 0, 0, 0)
    close, ma20 = float(cur[0]), float(cur[5])
    prev_close, prev_ma20 = float(prev[0]), float(prev[5])
    above_days = 0
    for row in rows:
        if float(row[5]) > 0 and float(row[0]) > float(row[5]):
            above_days += 1
        else:
            break
    streak = 0
    threshold = limit_pct(symbol) * 0.98
    for row in rows:
        if float(row[1]) >= threshold:
            streak += 1
        else:
            break
    rps = conn.execute(
        """SELECT COALESCE(rps_10,0) FROM fact_rps_results
           WHERE symbol=? AND CAST(trade_date AS DATE)<=CAST(? AS DATE)
           ORDER BY trade_date DESC LIMIT 1""", [symbol, trade_date]).fetchone()
    zeta = conn.execute(
        """SELECT COALESCE(lhb_buy,0),COALESCE(lhb_sell,0),COALESCE(seat_count,0)
           FROM fact_zeta_signals
           WHERE ts_code=? AND CAST(trade_date AS DATE)<=CAST(? AS DATE)
           ORDER BY trade_date DESC LIMIT 1""", [symbol, trade_date]).fetchone()
    return {
        "pct_chg": float(cur[1]), "near_limit_up": bool(float(cur[1]) >= threshold), "turnover": float(cur[2]),
        "vol_ratio": float(cur[3]) / float(cur[4]) if float(cur[4]) > 0 else 1.0,
        "rps_10": float(rps[0]) if rps else 0.0,
        "close_above_ma20": bool(ma20 > 0 and close > ma20),
        "breakout_above_ma20": bool(ma20 > 0 and close > ma20 and prev_ma20 > 0 and prev_close <= prev_ma20),
        "ma20_slope_positive": bool(ma20 > 0 and prev_ma20 > 0 and ma20 > prev_ma20),
        "above_ma20_days": above_days, "limit_up_streak": streak,
        "lhb_present": bool(zeta and (float(zeta[0]) or float(zeta[1]) or int(zeta[2]))),
    }


def load_observations(trade_date: str, run_id: str, limit: int) -> List[Dict[str, Any]]:
    where = ["CAST(n.trade_date AS DATE)=CAST(? AS DATE)"]
    params: List[Any] = [trade_date]
    if run_id:
        where.append("n.run_id=?")
        params.append(run_id)
    with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
        rows = conn.execute(
            f"""
            WITH x AS (
              SELECT n.task_id,COALESCE(n.run_id,''),n.symbol,
                     COALESCE(NULLIF(TRIM(n.name),''),NULLIF(TRIM(b.name),''),n.symbol),
                     CAST(n.trade_date AS VARCHAR),COALESCE(n.l4_final_verdict,''),
                     COALESCE(n.l4_news_evidence,'{{}}'),
                     ROW_NUMBER() OVER(PARTITION BY n.symbol ORDER BY n.created_at DESC,n.task_id DESC) rn
              FROM nexus_audits n LEFT JOIN fact_stock_basic b ON b.symbol=n.symbol
              WHERE {' AND '.join(where)}
            )
            SELECT * EXCLUDE(rn) FROM x WHERE rn=1 ORDER BY symbol LIMIT ?
            """, params + [limit]).fetchall()
        output = []
        for task_id, rid, symbol, name, td, verdict, news in rows:
            features = market_features(conn, str(symbol), str(td)[:10])
            tags, official = news_context(news)
            feature_obj = ARCH.ArchetypeFeatures(
                **features, positive_news_tags=tags,
                official_event_evidence=official, data_as_of=str(td)[:10])
            result = ARCH.classify_trade_archetype(feature_obj)
            output.append({
                "observation_id": f"{task_id}|trade_archetype_v0.1",
                "task_id": str(task_id), "run_id": str(rid), "symbol": str(symbol),
                "name": str(name), "trade_date": str(td)[:10],
                "l4_final_verdict": str(verdict),
                "features": feature_obj.__dict__, "classification": result.to_dict(),
            })
    return output


def persist(rows: List[Dict[str, Any]]) -> None:
    with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS fact_trade_archetype_observations(
          observation_id VARCHAR PRIMARY KEY, task_id VARCHAR, run_id VARCHAR,
          symbol VARCHAR, name VARCHAR, trade_date DATE,
          primary_archetype VARCHAR, secondary_archetype VARCHAR,
          confidence DOUBLE, classifier_version VARCHAR,
          observer_only BOOLEAN DEFAULT TRUE, features_json VARCHAR,
          result_json VARCHAR, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
        """)
        for row in rows:
            result = row["classification"]
            conn.execute("""
            INSERT INTO fact_trade_archetype_observations
            VALUES(?,?,?,?,?,CAST(? AS DATE),?,?,?,?,TRUE,?,?,now(),now())
            ON CONFLICT(observation_id) DO UPDATE SET
              primary_archetype=EXCLUDED.primary_archetype,
              secondary_archetype=EXCLUDED.secondary_archetype,
              confidence=EXCLUDED.confidence,features_json=EXCLUDED.features_json,
              result_json=EXCLUDED.result_json,updated_at=now()
            """, [row["observation_id"], row["task_id"], row["run_id"], row["symbol"],
                  row["name"], row["trade_date"], result["primary_archetype"],
                  result["secondary_archetype"], result["confidence"],
                  result["classifier_version"], json.dumps(row["features"], ensure_ascii=False),
                  json.dumps(result, ensure_ascii=False)])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trade-date", default="")
    ap.add_argument("--run-id", default="")
    ap.add_argument("--limit", type=int, default=50)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--output", default="")
    args = ap.parse_args()
    trade_date = args.trade_date.strip()
    if not trade_date:
        with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
            row = conn.execute("SELECT CAST(MAX(trade_date) AS VARCHAR) FROM nexus_audits").fetchone()
        trade_date = str(row[0])[:10] if row and row[0] else ""
    if not trade_date:
        raise SystemExit("no audit date available")
    rows = load_observations(trade_date, args.run_id.strip(), max(1, args.limit))
    payload = {
        "trade_date": trade_date, "run_id": args.run_id.strip(),
        "mode": "apply_observation" if args.apply else "dry_run",
        "observer_only": True, "no_trade_signal": True,
        "generated_at": datetime.now().astimezone().isoformat(),
        "count": len(rows), "observations": rows,
    }
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    if args.output:
        Path(args.output).write_text(text + "\n", encoding="utf-8")
    else:
        print(text)
    if args.apply:
        persist(rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
