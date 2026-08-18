#!/usr/bin/env python3
"""Read-only calibration of RAG query representations against production vectors."""

import argparse
import importlib.util
import json
import statistics
from datetime import datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "storage" / "database" / "zhulong.duckdb"
PIPELINE_PATH = ROOT / "02_brain" / "lib" / "rag_pipeline.py"


def load_pipeline_module():
    spec = importlib.util.spec_from_file_location("rag_calibration_pipeline", PIPELINE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def split_tags(raw):
    text = str(raw or "").replace("，", ",")
    return {part.strip() for part in text.split(",") if part.strip()}


def build_queries(row, narrative):
    symbol, name, trade_date, pct_chg, turnover, pattern, tags, reasoning, falsifiable, score, verdict = row
    kv = " | ".join(
        [
            f"symbol={symbol}",
            f"pct_chg={float(pct_chg or 0):+.2f}%",
            f"turnover={float(turnover or 0):.2f}%",
            f"pattern={pattern or 'UNKNOWN'}",
            f"l2_tags={tags or ''}",
            f"l3_score={int(score or 0)}",
            f"l3_verdict={verdict or 'UNKNOWN'}",
            f"l3_reasoning={str(reasoning or '')[:260]}",
            f"falsifiable={str(falsifiable or '')[:180]}",
        ]
    )
    natural = (
        f"{name or symbol}在{trade_date}的审计场景中，涨跌幅为{float(pct_chg or 0):+.2f}%，"
        f"换手率为{float(turnover or 0):.2f}%，技术形态为{pattern or '未识别'}。"
        f"事实标签包括{tags or '无'}，L3结论为{verdict or 'UNKNOWN'}，评分{int(score or 0)}。"
        f"审计理由：{str(reasoning or '')[:220]}。"
        f"需要验证的条件：{str(falsifiable or '')[:140]}。"
        "请检索历史上环境、风险结构和判断依据相近的案例。"
    )
    clean_reasoning = str(reasoning or "").strip()[:700]
    fragment_length = max(40, min(len(narrative), max(80, len(narrative) * 2 // 3)))
    narrative_fragment = str(narrative or "")[:fragment_length]
    return {
        "kv": kv[:900],
        "reasoning": clean_reasoning,
        "natural": natural[:900],
        "narrative_fragment_upper_bound": narrative_fragment,
    }


def fetch_samples(limit):
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    rows = conn.execute(
        """
        WITH audits AS (
            SELECT symbol, name, trade_date, l1_pct_chg, l1_turnover,
                   l2_pattern, l2_fact_tags, l3_reasoning, l3_falsifiable,
                   l3_audit_score, l3_verdict, created_at,
                   ROW_NUMBER() OVER (
                       PARTITION BY symbol, trade_date ORDER BY created_at DESC
                   ) AS rn
            FROM nexus_audits
            WHERE COALESCE(l3_reasoning, '') != ''
              AND COALESCE(status, '') = 'L4_DONE'
        )
        SELECT a.symbol, a.name, a.trade_date, a.l1_pct_chg, a.l1_turnover,
               a.l2_pattern, a.l2_fact_tags, a.l3_reasoning, a.l3_falsifiable,
               a.l3_audit_score, a.l3_verdict
        FROM audits a
        WHERE a.rn = 1
          AND EXISTS (
              SELECT 1 FROM fact_strategic_memory m
              WHERE m.symbol = a.symbol
                AND m.enrichment_status = 'ENRICHED'
          )
        ORDER BY a.trade_date DESC, a.symbol
        LIMIT ?
        """,
        [int(limit)],
    ).fetchall()
    conn.close()
    return rows


def summarize(records, threshold, delta_threshold):
    output = {}
    forms = sorted({record["form"] for record in records})
    for form in forms:
        current = [record for record in records if record["form"] == form]
        valid = [record for record in current if record["status"] == "OK"]
        top1 = [record["top1"] for record in valid]
        deltas = [record["delta"] for record in valid]
        output[form] = {
            "attempted": len(current),
            "valid": len(valid),
            "failed": len(current) - len(valid),
            "mean_top1": round(statistics.mean(top1), 4) if top1 else None,
            "median_top1": round(statistics.median(top1), 4) if top1 else None,
            "median_delta": round(statistics.median(deltas), 4) if deltas else None,
            "top2_available": sum(record["top2"] != 0 for record in valid),
            "authorized": sum(
                record["top1"] >= threshold and record["delta"] >= delta_threshold
                for record in valid
            ),
            "tag_relevant_top1": sum(bool(record["tag_overlap"]) for record in valid),
            "expected_tags_available": sum(
                bool(record.get("expected_tags_available")) for record in valid
            ),
        }
    return output


def run(limit, output_path):
    module = load_pipeline_module()
    pipeline = module.RAGPipeline()
    if pipeline._chroma is None:
        raise RuntimeError("Chroma reader unavailable; calibration aborted")
    samples = fetch_samples(limit)
    records = []
    for row in samples:
        symbol = str(row[0])
        stored = pipeline._chroma.get(
            where={"$and": [{"symbol": symbol}, {"enrichment_status": "ENRICHED"}]},
            include=["documents", "metadatas"],
        )
        documents = stored.get("documents") or []
        metadatas = stored.get("metadatas") or []
        narrative = max((str(doc or "") for doc in documents), key=len, default="")
        queries = build_queries(row, narrative)
        usable = {key: value for key, value in queries.items() if value.strip()}
        if not documents or not usable:
            continue
        try:
            result = pipeline._chroma.query(
                query_texts=list(usable.values()),
                n_results=min(5, len(documents)),
                where={"$and": [{"symbol": symbol}, {"enrichment_status": "ENRICHED"}]},
            )
            distances = result.get("distances") or []
            result_meta = result.get("metadatas") or []
            for index, form in enumerate(usable):
                scores = sorted(
                    [1.0 - float(distance) for distance in (distances[index] or [])],
                    reverse=True,
                )
                if not scores or not any(abs(score) > 1e-9 for score in scores):
                    records.append({"symbol": symbol, "trade_date": str(row[2]), "form": form, "status": "ZERO_VECTOR"})
                    continue
                top_tags = split_tags((result_meta[index] or [{}])[0].get("tags", ""))
                expected_tags = split_tags(row[6])
                records.append(
                    {
                        "symbol": symbol,
                        "trade_date": str(row[2]),
                        "form": form,
                        "status": "OK",
                        "top1": round(scores[0], 6),
                        "top2": round(scores[1], 6) if len(scores) > 1 else 0.0,
                        "delta": round(scores[0] - (scores[1] if len(scores) > 1 else 0.0), 6),
                        "tag_overlap": sorted(expected_tags & top_tags),
                        "expected_tags_available": bool(expected_tags),
                    }
                )
        except Exception as exc:
            for form in usable:
                records.append(
                    {
                        "symbol": symbol,
                        "trade_date": str(row[2]),
                        "form": form,
                        "status": "ERROR",
                        "error": f"{type(exc).__name__}: {str(exc)[:180]}",
                    }
                )
    payload = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "read_only": True,
        "sample_limit": limit,
        "samples_selected": len(samples),
        "thresholds": {"top1": module.SIMILARITY_THRESHOLD, "delta": module.DELTA_THRESHOLD},
        "upper_bound_note": "narrative_fragment_upper_bound contains source wording and is not used to set thresholds",
        "summary": summarize(records, module.SIMILARITY_THRESHOLD, module.DELTA_THRESHOLD),
        "records": records,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(payload["summary"], ensure_ascii=False, indent=2))
    print(f"wrote {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=40)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    run(max(30, args.limit), args.output)
