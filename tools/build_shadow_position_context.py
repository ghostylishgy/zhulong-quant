#!/usr/bin/env python3
"""Build EOD context snapshots for next-day Shadow position management."""

from __future__ import annotations

import argparse
import importlib.util
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = str(ROOT / "storage" / "database" / "zhulong.duckdb")
ENGINE_LIB = ROOT / "01_engine" / "lib"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ENGINE_LIB) not in sys.path:
    sys.path.insert(0, str(ENGINE_LIB))

from db_gateway import DBGateway

logger = logging.getLogger("zhulong.shadow_position_context")
BEIJING_TZ = timezone(timedelta(hours=8))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / "config" / ".env")
    load_dotenv(ROOT / ".env")
except Exception:
    pass

MIN_AVG_AMOUNT_YUAN = float(os.getenv("SHADOW_CONTEXT_MIN_AVG_AMOUNT_YUAN", "50000000"))
MIN_LATEST_AMOUNT_YUAN = float(os.getenv("SHADOW_CONTEXT_MIN_LATEST_AMOUNT_YUAN", "30000000"))
MIN_VOLUME_RATIO_3D = float(os.getenv("SHADOW_CONTEXT_MIN_VOLUME_RATIO_3D", "1.10"))

REGULATORY_CRITICAL_TERMS = {
    "立案": "INVESTIGATION",
    "行政处罚": "ADMINISTRATIVE_PENALTY",
    "退市": "DELISTING",
    "重大违法": "MAJOR_VIOLATION",
    "财务造假": "FINANCIAL_FRAUD",
    "风险警示": "RISK_WARNING",
    "实施ST": "ST_DESIGNATION",
    "*ST": "ST_DESIGNATION",
}
REGULATORY_CAUTION_TERMS = {
    "问询函": "INQUIRY_LETTER",
    "监管函": "REGULATORY_LETTER",
    "关注函": "ATTENTION_LETTER",
    "警示函": "WARNING_LETTER",
    "风险提示": "RISK_NOTICE",
    "诉讼": "LITIGATION",
    "冻结": "FREEZE",
    "减持": "REDUCTION",
}


@dataclass
class PositionRow:
    symbol: str
    name: str
    position_trade_date: str
    strength_tier: str
    entry_tide_gate: str
    entry_policy: str
    signal_task_id: str
    qty: int
    entry_price: float
    trade_archetype: str = "UNCLASSIFIED"
    trade_contract_json: str = "{}"


def now_beijing() -> datetime:
    return datetime.now(BEIJING_TZ)


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"module loader unavailable: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def ensure_schema() -> None:
    with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS fact_shadow_position_contexts (
                context_id VARCHAR PRIMARY KEY,
                trade_date DATE NOT NULL,
                symbol VARCHAR NOT NULL,
                name VARCHAR DEFAULT '',
                position_trade_date DATE,
                strength_tier VARCHAR DEFAULT '',
                entry_tide_gate VARCHAR DEFAULT '',
                entry_policy VARCHAR DEFAULT '',
                market_gate VARCHAR DEFAULT 'UNKNOWN',
                market_ma20_ratio DOUBLE DEFAULT 0,
                news_status VARCHAR DEFAULT 'NEWS_UNAVAILABLE',
                news_risk_level VARCHAR DEFAULT 'UNAVAILABLE',
                news_risk_score INTEGER DEFAULT 0,
                news_gate VARCHAR DEFAULT 'NONE',
                news_checked_at VARCHAR DEFAULT '',
                news_as_of VARCHAR DEFAULT '',
                regulatory_status VARCHAR DEFAULT 'REG_UNAVAILABLE',
                regulatory_risk_level VARCHAR DEFAULT 'UNAVAILABLE',
                regulatory_risk_score INTEGER DEFAULT 0,
                regulatory_hits_json VARCHAR DEFAULT '[]',
                liquidity_ok BOOLEAN DEFAULT FALSE,
                latest_amount_yuan DOUBLE DEFAULT 0,
                avg_amount_5d_yuan DOUBLE DEFAULT 0,
                volume_expansion_ok BOOLEAN DEFAULT FALSE,
                volume_ratio_3d DOUBLE DEFAULT 0,
                limit_up_streak INTEGER DEFAULT 0,
                abnormal_risk BOOLEAN DEFAULT TRUE,
                runner_gate VARCHAR DEFAULT 'DENY',
                runner_gate_reason VARCHAR DEFAULT '',
                context_quality VARCHAR DEFAULT 'UNAVAILABLE',
                missing_reasons_json VARCHAR DEFAULT '[]',
                evidence_json VARCHAR DEFAULT '{}',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_shadow_position_context_symbol_date
            ON fact_shadow_position_contexts(symbol, trade_date)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS fact_shadow_thesis_reviews (
                review_id VARCHAR PRIMARY KEY,
                trade_date DATE NOT NULL,
                symbol VARCHAR NOT NULL,
                position_trade_date DATE NOT NULL,
                signal_task_id VARCHAR DEFAULT '',
                trade_archetype VARCHAR DEFAULT 'UNCLASSIFIED',
                contract_id VARCHAR DEFAULT '',
                hold_trading_days INTEGER DEFAULT 0,
                thesis_state VARCHAR DEFAULT 'MANUAL_REVIEW',
                management_action VARCHAR DEFAULT 'MANUAL_REVIEW',
                thesis_reason VARCHAR DEFAULT '',
                observer_only BOOLEAN DEFAULT TRUE,
                evidence_json VARCHAR DEFAULT '{}',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )


def latest_trade_date() -> str:
    with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
        row = conn.execute(
            "SELECT CAST(MAX(trade_date) AS VARCHAR) FROM fact_daily WHERE close > 0"
        ).fetchone()
    if row and row[0]:
        return str(row[0])[:10]
    return now_beijing().strftime("%Y-%m-%d")


def open_positions(limit: int = 50) -> List[PositionRow]:
    with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
        rows = conn.execute(
            """
            SELECT p.symbol,
                   COALESCE(NULLIF(TRIM(b.name), ''), p.symbol) AS name,
                   CAST(p.trade_date AS VARCHAR) AS position_trade_date,
                   COALESCE(p.strength_tier, '') AS strength_tier,
                   COALESCE(p.entry_tide_gate, '') AS entry_tide_gate,
                   COALESCE(p.entry_policy, '') AS entry_policy,
                   COALESCE(p.signal_task_id, '') AS signal_task_id,
                   COALESCE(p.qty, 0) AS qty,
                   COALESCE(p.entry_price, 0) AS entry_price,
                   COALESCE(q.trade_archetype, 'UNCLASSIFIED') AS trade_archetype,
                   COALESCE(q.trade_contract_json, '{}') AS trade_contract_json
            FROM fact_paper_positions p
            LEFT JOIN fact_stock_basic b ON b.symbol = p.symbol
            LEFT JOIN fact_shadow_pending_signals q ON q.task_id = p.signal_task_id
            WHERE UPPER(COALESCE(p.status, '')) = 'HOLD'
            ORDER BY p.trade_date, p.symbol
            LIMIT ?
            """,
            [int(limit)],
        ).fetchall()
    return [
        PositionRow(
            symbol=str(row[0] or "").strip().upper(),
            name=str(row[1] or "").strip(),
            position_trade_date=str(row[2] or "")[:10],
            strength_tier=str(row[3] or "").strip().upper(),
            entry_tide_gate=str(row[4] or "").strip().upper(),
            entry_policy=str(row[5] or "").strip().upper(),
            signal_task_id=str(row[6] or "").strip(),
            qty=int(row[7] or 0),
            entry_price=float(row[8] or 0),
            trade_archetype=str(row[9] or "UNCLASSIFIED").strip().upper(),
            trade_contract_json=str(row[10] or "{}"),
        )
        for row in rows
    ]


def load_tide(trade_date: str) -> Dict[str, Any]:
    try:
        module = load_module("zhulong_shadow_context_tide", ROOT / "04_governance" / "lib" / "tide_sensor.py")
        state = module.get_sensor().get_risk_gate(trade_date=trade_date)
        return {
            "market_gate": str(getattr(state, "risk_gate", "UNKNOWN") or "UNKNOWN").upper(),
            "market_ma20_ratio": float(getattr(state, "ma20_ratio", 0) or 0),
            "evidence": state.to_dict() if hasattr(state, "to_dict") else {},
            "missing": [],
        }
    except Exception as exc:
        logger.warning("tide context unavailable: %s", exc, exc_info=True)
        return {
            "market_gate": "UNKNOWN",
            "market_ma20_ratio": 0.0,
            "evidence": {"error": f"{type(exc).__name__}: {str(exc)[:160]}"},
            "missing": ["market_gate_unavailable"],
        }


def load_news_verifier():
    try:
        module = load_module("zhulong_shadow_context_news", ROOT / "02_brain" / "lib" / "news_verifier.py")
        return module.NewsVerifier(
            cache_path=ROOT / "storage" / "news" / "shadow_position_news.sqlite",
            timeout=float(os.getenv("SHADOW_CONTEXT_NEWS_TIMEOUT_SECONDS", "4.0")),
        )
    except Exception as exc:
        logger.warning("news verifier unavailable: %s", exc, exc_info=True)
        return None


def collect_news(verifier, position: PositionRow, cutoff: datetime) -> Tuple[Dict[str, Any], List[str]]:
    if verifier is None:
        return {
            "news_status": "NEWS_UNAVAILABLE",
            "news_risk_level": "UNAVAILABLE",
            "news_risk_score": 0,
            "news_gate": "NONE",
            "news_checked_at": "",
            "news_as_of": cutoff.isoformat(),
            "evidence": {},
        }, ["news_unavailable"]
    try:
        result = verifier.verify(position.symbol, position.name, cutoff_at=cutoff)
        payload = result.to_dict()
        return {
            "news_status": str(payload.get("status") or "NEWS_UNAVAILABLE"),
            "news_risk_level": str(payload.get("risk_level") or "UNAVAILABLE"),
            "news_risk_score": int(payload.get("risk_score") or 0),
            "news_gate": str(payload.get("hypothetical_gate") or "NONE"),
            "news_checked_at": str(payload.get("checked_at") or ""),
            "news_as_of": cutoff.isoformat(),
            "evidence": payload,
        }, []
    except Exception as exc:
        logger.warning("news context failed closed %s: %s", position.symbol, exc, exc_info=True)
        return {
            "news_status": "NEWS_UNAVAILABLE",
            "news_risk_level": "UNAVAILABLE",
            "news_risk_score": 0,
            "news_gate": "NONE",
            "news_checked_at": "",
            "news_as_of": cutoff.isoformat(),
            "evidence": {"error": f"{type(exc).__name__}: {str(exc)[:160]}"},
        }, ["news_unavailable"]


def amount_yuan(raw_amount: float) -> float:
    return float(raw_amount or 0) * 1000.0


def load_liquidity_volume(symbol: str, trade_date: str) -> Tuple[Dict[str, Any], List[str]]:
    with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
        rows = conn.execute(
            """
            SELECT CAST(trade_date AS VARCHAR), COALESCE(vol, 0), COALESCE(amount, 0),
                   COALESCE(close, 0), COALESCE(ma20, 0)
            FROM fact_daily
            WHERE symbol = ?
              AND CAST(trade_date AS DATE) <= CAST(? AS DATE)
              AND COALESCE(close, 0) > 0
            ORDER BY trade_date DESC
            LIMIT 10
            """,
            [symbol, trade_date],
        ).fetchall()
    missing: List[str] = []
    if len(rows) < 6:
        missing.append("daily_volume_history_insufficient")
    latest_amount = amount_yuan(rows[0][2]) if rows else 0.0
    avg_amount_5d = sum(amount_yuan(row[2]) for row in rows[:5]) / len(rows[:5]) if rows[:5] else 0.0
    latest_vol = float(rows[0][1] or 0) if rows else 0.0
    last3 = [float(row[1] or 0) for row in rows[:3]]
    prev5 = [float(row[1] or 0) for row in rows[3:8]]
    prev5_avg = sum(prev5) / len(prev5) if prev5 else 0.0
    volume_ratio_3d = (sum(last3) / len(last3)) / prev5_avg if last3 and prev5_avg > 0 else 0.0
    liquidity_ok = latest_amount >= MIN_LATEST_AMOUNT_YUAN and avg_amount_5d >= MIN_AVG_AMOUNT_YUAN
    volume_expansion_ok = bool(latest_vol > 0 and volume_ratio_3d >= MIN_VOLUME_RATIO_3D)
    if not liquidity_ok:
        missing.append("liquidity_not_confirmed")
    if not volume_expansion_ok:
        missing.append("volume_expansion_not_confirmed")
    return {
        "liquidity_ok": liquidity_ok,
        "latest_amount_yuan": round(latest_amount, 2),
        "avg_amount_5d_yuan": round(avg_amount_5d, 2),
        "volume_expansion_ok": volume_expansion_ok,
        "volume_ratio_3d": round(volume_ratio_3d, 4),
        "latest_close": float(rows[0][3] or 0) if rows else 0.0,
        "latest_ma20": float(rows[0][4] or 0) if rows else 0.0,
        "close_above_ma20": bool(rows and float(rows[0][4] or 0) > 0 and float(rows[0][3] or 0) > float(rows[0][4] or 0)),
        "evidence": {
            "rows": [
                {"trade_date": str(row[0])[:10], "vol": float(row[1] or 0), "amount_yuan": amount_yuan(row[2])}
                for row in rows[:8]
            ],
        },
    }, missing


def limit_pct_for_symbol(symbol: str) -> float:
    code = str(symbol or "").split(".")[0]
    sym = str(symbol or "").upper()
    if sym.endswith(".BJ") or code.startswith(("4", "8", "9")):
        return 0.30
    if sym.endswith(".SH") and code.startswith("688"):
        return 0.20
    if sym.endswith(".SZ") and code.startswith(("300", "301")):
        return 0.20
    return 0.10


def load_limit_up_streak(symbol: str, trade_date: str) -> int:
    threshold = limit_pct_for_symbol(symbol) * 0.98
    with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
        rows = conn.execute(
            """
            SELECT COALESCE(close, 0), COALESCE(pre_close, 0), COALESCE(pct_chg, 0)
            FROM fact_daily
            WHERE symbol = ?
              AND CAST(trade_date AS DATE) <= CAST(? AS DATE)
              AND COALESCE(close, 0) > 0
            ORDER BY trade_date DESC
            LIMIT 10
            """,
            [symbol, trade_date],
        ).fetchall()
    streak = 0
    for close_raw, pre_raw, pct_raw in rows:
        close = float(close_raw or 0)
        pre = float(pre_raw or 0)
        pct = (close / pre - 1.0) if pre > 0 else float(pct_raw or 0) / 100.0
        if pct >= threshold:
            streak += 1
        else:
            break
    return streak


def first_existing(row: Dict[str, Any], candidates: Iterable[str]) -> str:
    for key in candidates:
        if key in row and row[key] is not None:
            return str(row[key])
    for key, value in row.items():
        if value is not None and ("标题" in str(key) or "title" in str(key).lower()):
            return str(value)
    return ""


def classify_regulatory_hits(rows: List[Dict[str, Any]]) -> Tuple[str, str, int, List[Dict[str, Any]]]:
    hits: List[Dict[str, Any]] = []
    critical = set()
    caution = set()
    for row in rows:
        title = first_existing(row, ["公告标题", "title", "announcementTitle", "公告名称"])
        if not title:
            continue
        text = title + " " + json.dumps(row, ensure_ascii=False, default=str)
        row_tags = []
        for term, tag in REGULATORY_CRITICAL_TERMS.items():
            if term in text:
                critical.add(tag)
                row_tags.append(tag)
        for term, tag in REGULATORY_CAUTION_TERMS.items():
            if term in text:
                caution.add(tag)
                row_tags.append(tag)
        if row_tags:
            hits.append({"title": title[:180], "tags": sorted(set(row_tags))})
    if critical:
        return "REG_CRITICAL", "CRITICAL", 90, hits[:20]
    if caution:
        return "REG_CAUTION", "CAUTION", 65, hits[:20]
    return "REG_CLEAR", "CLEAR", 0, hits[:20]


def fetch_cninfo_disclosures(
    symbol: str,
    start_date: str,
    end_date: str,
    timeout: float = 4.0,
) -> List[Dict[str, Any]]:
    """Fetch official disclosures without relying on AkShare's empty-frame schema."""
    code = str(symbol or "").split(".")[0].strip()
    if not code.isdigit() or len(code) != 6:
        raise ValueError(f"invalid A-share symbol: {symbol}")

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; Zhulong/1.0)",
        "Referer": "https://www.cninfo.com.cn/",
    }
    lookup = requests.post(
        "https://www.cninfo.com.cn/new/information/topSearch/query",
        data={"keyWord": code, "maxNum": 10},
        headers=headers,
        timeout=timeout,
    )
    lookup.raise_for_status()
    lookup_rows = lookup.json()
    if not isinstance(lookup_rows, list):
        raise ValueError("unexpected CNINFO entity response")
    matches = [row for row in lookup_rows if str(row.get("code", "")) == code]
    if not matches or not str(matches[0].get("orgId", "")).strip():
        raise ValueError(f"CNINFO entity not resolved: {code}")

    org_id = str(matches[0]["orgId"]).strip()
    exchange_column = "sse" if str(symbol or "").upper().endswith(".SH") else "szse"
    query_columns = [exchange_column, "regulator"]
    rows: List[Dict[str, Any]] = []
    seen = set()
    for column in query_columns:
        payload = {
            "pageNum": 1,
            "pageSize": 30,
            "column": column,
            "tabName": "fulltext",
            "plate": "",
            "stock": f"{code},{org_id}",
            "searchkey": "",
            "secid": "",
            "category": "",
            "trade": "",
            "seDate": f"{start_date}~{end_date}",
            "sortName": "",
            "sortType": "",
            "isHLtitle": "true",
        }
        response = requests.post(
            "https://www.cninfo.com.cn/new/hisAnnouncement/query",
            data=payload,
            headers=headers,
            timeout=timeout,
        )
        response.raise_for_status()
        response_payload = response.json()
        if not isinstance(response_payload, dict):
            raise ValueError(f"unexpected CNINFO disclosure response: {column}")
        announcements = response_payload.get("announcements") or []
        if not isinstance(announcements, list):
            raise ValueError(f"unexpected CNINFO announcements schema: {column}")
        for row in announcements:
            if not isinstance(row, dict):
                continue
            identity = str(row.get("announcementId") or row.get("adjunctUrl") or "").strip()
            if not identity:
                identity = json.dumps(row, ensure_ascii=False, sort_keys=True, default=str)
            if identity in seen:
                continue
            seen.add(identity)
            rows.append({
                "code": str(row.get("secCode") or code),
                "name": str(row.get("secName") or ""),
                "title": str(row.get("announcementTitle") or ""),
                "published_at": row.get("announcementTime"),
                "announcement_id": str(row.get("announcementId") or ""),
                "org_id": str(row.get("orgId") or org_id),
                "source_column": column,
            })
    return rows


def collect_regulatory(position: PositionRow, trade_date: str, skip: bool = False) -> Tuple[Dict[str, Any], List[str]]:
    if skip:
        return {
            "regulatory_status": "REG_UNAVAILABLE",
            "regulatory_risk_level": "UNAVAILABLE",
            "regulatory_risk_score": 0,
            "regulatory_hits": [],
            "evidence": {"skipped": True},
        }, ["regulatory_unavailable"]
    end = datetime.strptime(trade_date, "%Y-%m-%d").date()
    start = end - timedelta(days=7)
    rows: List[Dict[str, Any]] = []
    failures: Dict[str, str] = {}
    try:
        rows = fetch_cninfo_disclosures(
            position.symbol,
            start.strftime("%Y-%m-%d"),
            end.strftime("%Y-%m-%d"),
            timeout=float(os.getenv("SHADOW_CONTEXT_REGULATORY_TIMEOUT_SECONDS", "4.0")),
        )
    except Exception as exc:
        failures["CNINFO"] = f"{type(exc).__name__}: {str(exc)[:160]}"
    status, level, score, hits = classify_regulatory_hits(rows)
    missing = ["regulatory_unavailable"] if failures and not rows else []
    return {
        "regulatory_status": status if rows or not failures else "REG_UNAVAILABLE",
        "regulatory_risk_level": level if rows or not failures else "UNAVAILABLE",
        "regulatory_risk_score": score if rows or not failures else 0,
        "regulatory_hits": hits,
        "evidence": {"rows": len(rows), "failures": failures, "hits": hits},
    }, missing


def _contract_payload(position: PositionRow) -> Dict[str, Any]:
    try:
        payload = json.loads(position.trade_contract_json or "{}")
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _hold_trading_days(position: PositionRow, trade_date: str) -> int:
    with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
        row = conn.execute(
            """SELECT COUNT(*) FROM fact_daily WHERE symbol=?
               AND CAST(trade_date AS DATE)>=CAST(? AS DATE)
               AND CAST(trade_date AS DATE)<=CAST(? AS DATE) AND close>0""",
            [position.symbol, position.position_trade_date, trade_date],
        ).fetchone()
    return max(0, int(row[0] or 0) - 1) if row else 0


def evaluate_position_thesis(
    position: PositionRow,
    trade_date: str,
    tide: Dict[str, Any],
    news: Dict[str, Any],
    regulatory: Dict[str, Any],
    liquidity: Dict[str, Any],
) -> Dict[str, Any]:
    archetype = str(position.trade_archetype or "UNCLASSIFIED").upper()
    contract = _contract_payload(position)
    hold_days = _hold_trading_days(position, trade_date)
    result = {
        "trade_archetype": archetype,
        "contract_id": str(contract.get("contract_id") or ""),
        "hold_trading_days": hold_days,
        "thesis_state": "THESIS_INTACT",
        "management_action": "HOLD",
        "thesis_reason": "original_thesis_still_intact",
        "observer_only": True,
    }
    if archetype == "UNCLASSIFIED" or not contract:
        result.update(thesis_state="MANUAL_REVIEW", management_action="MANUAL_REVIEW", thesis_reason="missing_or_legacy_trade_contract")
        return result
    news_level = str(news.get("news_risk_level") or "UNAVAILABLE").upper()
    reg_level = str(regulatory.get("regulatory_risk_level") or "UNAVAILABLE").upper()
    market_gate = str(tide.get("market_gate") or "UNKNOWN").upper()
    if "CRITICAL" in {news_level, reg_level}:
        result.update(thesis_state="THESIS_INVALIDATED", management_action="EXIT_NEXT_SESSION", thesis_reason="critical_news_or_regulatory_risk")
        return result
    if "CAUTION" in {news_level, reg_level}:
        result.update(thesis_state="THESIS_WEAKENED", management_action="REDUCE_OR_TIGHTEN_STOP", thesis_reason="news_or_regulatory_caution")
        return result
    window = list(contract.get("expected_holding_days") or [])
    max_days = int(window[-1]) if window else 0
    if max_days and hold_days > max_days:
        result.update(thesis_state="THESIS_WEAKENED", management_action="TIME_EXIT_REVIEW", thesis_reason="expected_holding_window_expired")
        return result
    if archetype == "EMOTION_RELAY":
        if market_gate != "AGGRESSIVE" or not liquidity.get("volume_expansion_ok"):
            result.update(thesis_state="THESIS_WEAKENED", management_action="TAKE_PROFIT_OR_EXIT", thesis_reason="relay_environment_or_volume_failed")
        else:
            result.update(thesis_state="THESIS_CONFIRMED", management_action="HOLD_WITH_TIGHT_PROTECTION", thesis_reason="relay_conditions_confirmed")
    elif archetype == "EVENT_CATALYST":
        result.update(management_action="HOLD_PENDING_CATALYST_REVIEW", thesis_reason="no_catalyst_invalidation_detected")
    elif archetype == "TREND_INITIATION":
        if not liquidity.get("close_above_ma20"):
            result.update(thesis_state="THESIS_WEAKENED", management_action="TIGHTEN_STOP", thesis_reason="trend_initiation_not_holding_ma20")
        elif market_gate == "FORCE_NO_EDGE":
            result.update(thesis_state="THESIS_WEAKENED", management_action="REDUCE_OR_TIGHTEN_STOP", thesis_reason="market_tide_suppresses_new_trend")
        else:
            result.update(management_action="HOLD_WITH_TRAILING_STOP", thesis_reason="trend_initiation_structure_intact")
    elif archetype == "TREND_CONTINUATION":
        if not liquidity.get("close_above_ma20"):
            result.update(thesis_state="THESIS_INVALIDATED", management_action="EXIT_NEXT_SESSION", thesis_reason="trend_structure_broken_below_ma20")
        else:
            result.update(management_action="HOLD_WITH_TRAILING_STOP", thesis_reason="trend_continuation_structure_intact")
    return result


def persist_thesis_review(position: PositionRow, trade_date: str, thesis: Dict[str, Any], evidence: Dict[str, Any]) -> None:
    now_ts = now_beijing().strftime("%Y-%m-%d %H:%M:%S")
    with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
        conn.execute(
            """
            INSERT INTO fact_shadow_thesis_reviews VALUES(
              ?,CAST(? AS DATE),?,CAST(? AS DATE),?,?,?,?,?,?,?,TRUE,?,CAST(? AS TIMESTAMP),CAST(? AS TIMESTAMP))
            ON CONFLICT(review_id) DO UPDATE SET
              trade_archetype=EXCLUDED.trade_archetype,contract_id=EXCLUDED.contract_id,
              hold_trading_days=EXCLUDED.hold_trading_days,thesis_state=EXCLUDED.thesis_state,
              management_action=EXCLUDED.management_action,thesis_reason=EXCLUDED.thesis_reason,
              evidence_json=EXCLUDED.evidence_json,updated_at=EXCLUDED.updated_at
            """,
            [f"{trade_date}|{position.symbol}|{position.position_trade_date}", trade_date, position.symbol,
             position.position_trade_date, position.signal_task_id, thesis["trade_archetype"],
             thesis["contract_id"], thesis["hold_trading_days"], thesis["thesis_state"],
             thesis["management_action"], thesis["thesis_reason"],
             json.dumps(evidence, ensure_ascii=False, default=str)[:12000], now_ts, now_ts],
        )


def evaluate_runner_gate(
    position: PositionRow,
    tide: Dict[str, Any],
    news: Dict[str, Any],
    regulatory: Dict[str, Any],
    liquidity: Dict[str, Any],
    limit_up_streak: int,
    missing: List[str],
) -> Tuple[str, str, bool, str]:
    reasons: List[str] = []
    if position.strength_tier != "STRONG":
        reasons.append("strength_not_strong")
    if tide.get("market_gate") != "AGGRESSIVE":
        reasons.append(f"market_gate_{tide.get('market_gate', 'UNKNOWN')}")
    if str(news.get("news_risk_level") or "").upper() != "CLEAR":
        reasons.append(f"news_{news.get('news_risk_level', 'UNAVAILABLE')}")
    if str(regulatory.get("regulatory_risk_level") or "").upper() != "CLEAR":
        reasons.append(f"regulatory_{regulatory.get('regulatory_risk_level', 'UNAVAILABLE')}")
    if not bool(liquidity.get("liquidity_ok")):
        reasons.append("liquidity_not_confirmed")
    if not bool(liquidity.get("volume_expansion_ok")):
        reasons.append("volume_expansion_not_confirmed")
    if int(limit_up_streak or 0) >= 2:
        reasons.append("two_limit_up_full_exit")
    for item in missing:
        if item not in reasons:
            reasons.append(item)
    abnormal = any(
        token.startswith("news_") or token.startswith("regulatory_") or token == "two_limit_up_full_exit"
        for token in reasons
    )
    gate = "ALLOW" if not reasons else "DENY"
    quality = "DATA_OK" if not missing else "PARTIAL"
    return gate, ",".join(reasons) if reasons else "all_runner_conditions_met", abnormal, quality


def cn_status(value: Any) -> str:
    text = str(value or "").strip().upper()
    mapping = {
        "ALLOW": "允许保留小观察仓",
        "DENY": "不保留观察仓",
        "AGGRESSIVE": "强势",
        "CAUTION": "谨慎",
        "FORCE_NO_EDGE": "潮汐压制",
        "UNKNOWN": "未知",
        "CLEAR": "无明显风险",
        "CAUTION": "谨慎",
        "CRITICAL": "高风险",
        "SIGNAL": "单源风险信号",
        "PARTIAL": "部分来源可用",
        "UNAVAILABLE": "不可用",
        "DATA_OK": "数据完整",
        "PARTIAL": "数据不完整",
        "EMOTION_RELAY": "情绪接力",
        "EVENT_CATALYST": "事件催化",
        "TREND_INITIATION": "趋势启动",
        "TREND_CONTINUATION": "趋势延续",
        "UNCLASSIFIED": "未分类",
        "THESIS_CONFIRMED": "原假设增强",
        "THESIS_INTACT": "原假设仍成立",
        "THESIS_WEAKENED": "原假设减弱",
        "THESIS_INVALIDATED": "原假设失效",
        "MANUAL_REVIEW": "需人工复核",
        "HOLD": "继续持有观察",
        "HOLD_WITH_TIGHT_PROTECTION": "持有并收紧保护",
        "HOLD_PENDING_CATALYST_REVIEW": "持有并继续验证催化",
        "HOLD_WITH_TRAILING_STOP": "持有并跟踪保护",
        "TAKE_PROFIT_OR_EXIT": "考虑止盈或退出",
        "REDUCE_OR_TIGHTEN_STOP": "考虑减仓或收紧保护",
        "TIGHTEN_STOP": "收紧保护",
        "TIME_EXIT_REVIEW": "到期退出复核",
        "EXIT_NEXT_SESSION": "次日退出复核",
    }
    return mapping.get(text, text or "未知")


def cn_reason(reason: str) -> str:
    raw = str(reason or "")
    if raw == "all_runner_conditions_met":
        return "强势、市场、新闻、监管、流动性和放量条件均满足"
    parts = [item.strip() for item in raw.split(",") if item.strip()]
    mapping = {
        "strength_not_strong": "持仓未达到强势档",
        "market_gate_FORCE_NO_EDGE": "市场处于潮汐压制",
        "market_gate_CAUTION": "市场环境偏谨慎",
        "market_gate_UNKNOWN": "市场状态不可用",
        "news_UNAVAILABLE": "新闻风险不可用",
        "news_CAUTION": "新闻存在谨慎信号",
        "news_CRITICAL": "新闻存在高风险信号",
        "news_SIGNAL": "新闻存在单源风险信号",
        "news_PARTIAL": "新闻来源不完整",
        "regulatory_UNAVAILABLE": "监管公告不可用",
        "regulatory_CAUTION": "监管公告存在谨慎信号",
        "regulatory_CRITICAL": "监管公告存在高风险信号",
        "liquidity_not_confirmed": "流动性不足或未确认",
        "volume_expansion_not_confirmed": "放量延续不足或未确认",
        "two_limit_up_full_exit": "已达到连续涨停落袋条件",
        "news_unavailable": "新闻来源不可用",
        "regulatory_unavailable": "监管公告来源不可用",
        "market_gate_unavailable": "市场潮汐不可用",
        "daily_volume_history_insufficient": "量能历史不足",
    }
    translated = [mapping.get(item, "") for item in parts]
    translated = [item for item in translated if item]
    return "；".join(translated) if translated else "盘后条件未全部满足"


def cn_amount(value: float) -> str:
    amount = float(value or 0)
    if amount >= 100_000_000:
        return f"{amount / 100_000_000:.2f}亿"
    if amount >= 10_000:
        return f"{amount / 10_000:.0f}万"
    return f"{amount:.0f}"


def render_context_push(stats: Dict[str, Any], contexts: List[Dict[str, Any]]) -> Tuple[str, str]:
    td = str(stats.get("trade_date") or "")
    title = f"烛龙模拟盘持仓复核 | {td}"
    allowed = int(stats.get("runner_allowed") or 0)
    denied = int(stats.get("runner_denied") or 0)
    lines = [
        "[模拟盘持仓复核]",
        f"交易日：{td}",
        f"持仓数量：{len(contexts)}",
        f"观察仓资格：允许 {allowed} 只 / 不允许 {denied} 只",
        f"市场状态：{cn_status(stats.get('market_gate'))}",
        "",
    ]
    for context in contexts[:10]:
        display = f"{context.get('name') or context.get('symbol')} {context.get('symbol')}"
        lines.extend([
            f"【{display}】",
            f"交易类型：{cn_status(context.get('trade_archetype'))}",
            f"原假设状态：{cn_status(context.get('thesis_state'))}",
            f"次日管理：{cn_status(context.get('management_action'))}",
            f"观察仓资格：{cn_status(context.get('runner_gate'))}",
            f"原因：{cn_reason(str(context.get('runner_gate_reason') or ''))}",
            f"新闻风险：{cn_status(context.get('news_risk_level'))}",
            f"监管公告：{cn_status(context.get('regulatory_risk_level'))}",
            f"流动性：{'满足' if context.get('liquidity_ok') else '不满足'}，近五日均额 {cn_amount(float(context.get('avg_amount_5d_yuan') or 0))}",
            f"放量延续：{'满足' if context.get('volume_expansion_ok') else '不满足'}，量能比 {float(context.get('volume_ratio_3d') or 0):.2f}",
            "",
        ])
    if len(contexts) > 10:
        lines.append(f"其余 {len(contexts) - 10} 只持仓已记录在系统表中。")
    lines.append("说明：这是盘后复核摘要，用于次日模拟盘利润保护；不代表真实交易指令。")
    return title, "\n".join(lines)


def push_context_summary(stats: Dict[str, Any], contexts: List[Dict[str, Any]]) -> bool:
    if not contexts:
        logger.info("context push skipped: no open positions")
        return False
    token = os.getenv("PUSHPLUS_TOKEN", "").strip()
    if not token:
        logger.warning("context push skipped: PUSHPLUS_TOKEN missing")
        return False
    push_url = os.getenv("PUSHPLUS_URL", "https://www.pushplus.plus/send").strip()
    if push_url.startswith("http://"):
        push_url = "https://" + push_url[len("http://"):]
    title, content = render_context_push(stats, contexts)
    try:
        response = requests.post(
            push_url,
            json={"token": token, "title": title, "content": content[:3000], "template": "txt"},
            timeout=int(os.getenv("PUSHPLUS_TIMEOUT", "10") or "10"),
        )
        ok = response.status_code == 200 and response.json().get("code") == 200
        if ok:
            logger.info("context push sent: positions=%s", len(contexts))
            return True
        logger.warning("context push failed: %s", response.text[:300])
    except Exception as exc:
        logger.warning("context push exception: %s", exc)
    return False


def persist_context(context: Dict[str, Any]) -> None:
    with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
        conn.execute(
            """
            INSERT INTO fact_shadow_position_contexts (
                context_id, trade_date, symbol, name, position_trade_date,
                strength_tier, entry_tide_gate, entry_policy, market_gate,
                market_ma20_ratio, news_status, news_risk_level, news_risk_score,
                news_gate, news_checked_at, news_as_of, regulatory_status,
                regulatory_risk_level, regulatory_risk_score, regulatory_hits_json,
                liquidity_ok, latest_amount_yuan, avg_amount_5d_yuan,
                volume_expansion_ok, volume_ratio_3d, limit_up_streak,
                abnormal_risk, runner_gate, runner_gate_reason, context_quality,
                missing_reasons_json, evidence_json, created_at, updated_at
            )
            VALUES (?, CAST(? AS DATE), ?, ?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP))
            ON CONFLICT (context_id) DO UPDATE SET
                name = EXCLUDED.name,
                strength_tier = EXCLUDED.strength_tier,
                entry_tide_gate = EXCLUDED.entry_tide_gate,
                entry_policy = EXCLUDED.entry_policy,
                market_gate = EXCLUDED.market_gate,
                market_ma20_ratio = EXCLUDED.market_ma20_ratio,
                news_status = EXCLUDED.news_status,
                news_risk_level = EXCLUDED.news_risk_level,
                news_risk_score = EXCLUDED.news_risk_score,
                news_gate = EXCLUDED.news_gate,
                news_checked_at = EXCLUDED.news_checked_at,
                news_as_of = EXCLUDED.news_as_of,
                regulatory_status = EXCLUDED.regulatory_status,
                regulatory_risk_level = EXCLUDED.regulatory_risk_level,
                regulatory_risk_score = EXCLUDED.regulatory_risk_score,
                regulatory_hits_json = EXCLUDED.regulatory_hits_json,
                liquidity_ok = EXCLUDED.liquidity_ok,
                latest_amount_yuan = EXCLUDED.latest_amount_yuan,
                avg_amount_5d_yuan = EXCLUDED.avg_amount_5d_yuan,
                volume_expansion_ok = EXCLUDED.volume_expansion_ok,
                volume_ratio_3d = EXCLUDED.volume_ratio_3d,
                limit_up_streak = EXCLUDED.limit_up_streak,
                abnormal_risk = EXCLUDED.abnormal_risk,
                runner_gate = EXCLUDED.runner_gate,
                runner_gate_reason = EXCLUDED.runner_gate_reason,
                context_quality = EXCLUDED.context_quality,
                missing_reasons_json = EXCLUDED.missing_reasons_json,
                evidence_json = EXCLUDED.evidence_json,
                updated_at = EXCLUDED.updated_at
            """,
            [
                context["context_id"],
                context["trade_date"],
                context["symbol"],
                context["name"],
                context["position_trade_date"],
                context["strength_tier"],
                context["entry_tide_gate"],
                context["entry_policy"],
                context["market_gate"],
                context["market_ma20_ratio"],
                context["news_status"],
                context["news_risk_level"],
                context["news_risk_score"],
                context["news_gate"],
                context["news_checked_at"],
                context["news_as_of"],
                context["regulatory_status"],
                context["regulatory_risk_level"],
                context["regulatory_risk_score"],
                json.dumps(context["regulatory_hits"], ensure_ascii=False, default=str)[:8000],
                context["liquidity_ok"],
                context["latest_amount_yuan"],
                context["avg_amount_5d_yuan"],
                context["volume_expansion_ok"],
                context["volume_ratio_3d"],
                context["limit_up_streak"],
                context["abnormal_risk"],
                context["runner_gate"],
                context["runner_gate_reason"][:500],
                context["context_quality"],
                json.dumps(context["missing_reasons"], ensure_ascii=False, default=str)[:4000],
                json.dumps(context["evidence"], ensure_ascii=False, default=str)[:12000],
                context["updated_at"],
                context["updated_at"],
            ],
        )


def build_contexts(trade_date: str, limit: int, skip_news: bool, skip_regulatory: bool, push: bool = True) -> Dict[str, Any]:
    ensure_schema()
    td = str(trade_date or latest_trade_date())[:10]
    positions = open_positions(limit=limit)
    tide = load_tide(td)
    cutoff = datetime.fromisoformat(f"{td}T21:00:00+08:00")
    verifier = None if skip_news else load_news_verifier()
    stats = {
        "trade_date": td,
        "positions": len(positions),
        "written": 0,
        "runner_allowed": 0,
        "runner_denied": 0,
        "market_gate": tide.get("market_gate", "UNKNOWN"),
        "pushed": False,
    }
    contexts: List[Dict[str, Any]] = []
    for position in positions:
        missing: List[str] = list(tide.get("missing") or [])
        news, news_missing = collect_news(verifier, position, cutoff)
        regulatory, reg_missing = collect_regulatory(position, td, skip=skip_regulatory)
        liquidity, liq_missing = load_liquidity_volume(position.symbol, td)
        limit_streak = load_limit_up_streak(position.symbol, td)
        missing.extend(news_missing)
        missing.extend(reg_missing)
        missing.extend(liq_missing)
        runner_gate, runner_reason, abnormal, quality = evaluate_runner_gate(
            position, tide, news, regulatory, liquidity, limit_streak, missing
        )
        thesis = evaluate_position_thesis(position, td, tide, news, regulatory, liquidity)
        now_ts = now_beijing().strftime("%Y-%m-%d %H:%M:%S")
        context = {
            "context_id": f"{td}|{position.symbol}|{position.position_trade_date}",
            "trade_date": td,
            "symbol": position.symbol,
            "name": position.name,
            "position_trade_date": position.position_trade_date,
            "strength_tier": position.strength_tier,
            "entry_tide_gate": position.entry_tide_gate,
            "entry_policy": position.entry_policy,
            "trade_archetype": thesis["trade_archetype"],
            "thesis_state": thesis["thesis_state"],
            "management_action": thesis["management_action"],
            "thesis_reason": thesis["thesis_reason"],
            "hold_trading_days": thesis["hold_trading_days"],
            "market_gate": tide.get("market_gate", "UNKNOWN"),
            "market_ma20_ratio": float(tide.get("market_ma20_ratio") or 0),
            "news_status": news["news_status"],
            "news_risk_level": news["news_risk_level"],
            "news_risk_score": news["news_risk_score"],
            "news_gate": news["news_gate"],
            "news_checked_at": news["news_checked_at"],
            "news_as_of": news["news_as_of"],
            "regulatory_status": regulatory["regulatory_status"],
            "regulatory_risk_level": regulatory["regulatory_risk_level"],
            "regulatory_risk_score": regulatory["regulatory_risk_score"],
            "regulatory_hits": regulatory["regulatory_hits"],
            "liquidity_ok": liquidity["liquidity_ok"],
            "latest_amount_yuan": liquidity["latest_amount_yuan"],
            "avg_amount_5d_yuan": liquidity["avg_amount_5d_yuan"],
            "volume_expansion_ok": liquidity["volume_expansion_ok"],
            "volume_ratio_3d": liquidity["volume_ratio_3d"],
            "limit_up_streak": limit_streak,
            "abnormal_risk": abnormal,
            "runner_gate": runner_gate,
            "runner_gate_reason": runner_reason,
            "context_quality": quality,
            "missing_reasons": sorted(set(missing)),
            "evidence": {
                "tide": tide.get("evidence", {}),
                "news": news.get("evidence", {}),
                "regulatory": regulatory.get("evidence", {}),
                "liquidity": liquidity.get("evidence", {}),
                "trade_contract": _contract_payload(position),
                "thesis_review": thesis,
            },
            "updated_at": now_ts,
        }
        persist_context(context)
        persist_thesis_review(position, td, thesis, context["evidence"])
        contexts.append(context)
        stats["written"] += 1
        if runner_gate == "ALLOW":
            stats["runner_allowed"] += 1
        else:
            stats["runner_denied"] += 1
        logger.info(
            "context %s gate=%s reason=%s market=%s news=%s reg=%s vol=%.2f",
            position.symbol,
            runner_gate,
            runner_reason,
            context["market_gate"],
            context["news_risk_level"],
            context["regulatory_risk_level"],
            context["volume_ratio_3d"],
        )
    if push:
        stats["pushed"] = push_context_summary(stats, contexts)
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Shadow EOD position context snapshots")
    parser.add_argument("--trade-date", default="", help="Context trade date, defaults to latest fact_daily date")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--skip-news", action="store_true", help="Do not call external news verifier")
    parser.add_argument("--skip-regulatory", action="store_true", help="Do not call AKShare/CNINFO regulatory queries")
    parser.add_argument("--no-push", action="store_true", help="Write context without sending PushPlus summary")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    stats = build_contexts(
        trade_date=args.trade_date,
        limit=max(1, int(args.limit or 50)),
        skip_news=bool(args.skip_news),
        skip_regulatory=bool(args.skip_regulatory),
        push=not bool(args.no_push),
    )
    print(json.dumps(stats, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
