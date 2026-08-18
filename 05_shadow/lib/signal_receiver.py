#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
05_shadow/lib/signal_receiver.py
Shadow signal receiver aligned with PASS + final_score contract.

Contract rules:
- Read-only scan of nexus_audits until terminal shadow handling.
- Mark nexus_audits.shadow_processed after paper execution or terminal skip.
- L4 PASS signals are queued for T+1 fill; fills still persist through the existing shadow ledger.
"""

from __future__ import annotations

import logging
import json
from collections import OrderedDict
from datetime import datetime
import requests
from logging.handlers import RotatingFileHandler
import sys
import time
from pathlib import Path
from typing import Dict, List

try:
    from .db_contract import (
        DBGateway,
        DB_PATH,
        DEFAULT_MIN_FINAL_SCORE,
        SIGNAL_VERDICT_PASS,
        detect_nexus_score_column,
    )
    from .engine import (
        calculate_shadow_fill,
        persist_fill,
        _load_rules,
        _ensure_paper_positions,
        has_open_paper_position,
        open_paper_position_from_fill,
    )
    from .t1_fill_engine import enqueue_pending_signal, WAIT_NEWS_STATUS
    from .news_entry_gate import evaluate_shadow_news_entry_gate, push_shadow_news_gate_skip
    from .account_eligibility import evaluate_account_eligibility
    from .trade_contract import build_trade_contract
    from .portfolio_metrics import rebuild_shadow_metrics
except Exception:
    from db_contract import (
        DBGateway,
        DB_PATH,
        DEFAULT_MIN_FINAL_SCORE,
        SIGNAL_VERDICT_PASS,
        detect_nexus_score_column,
    )
    from engine import (
        calculate_shadow_fill,
        persist_fill,
        _load_rules,
        _ensure_paper_positions,
        has_open_paper_position,
        open_paper_position_from_fill,
    )
    from t1_fill_engine import enqueue_pending_signal, WAIT_NEWS_STATUS
    from news_entry_gate import evaluate_shadow_news_entry_gate, push_shadow_news_gate_skip
    from account_eligibility import evaluate_account_eligibility
    from trade_contract import build_trade_contract
    from portfolio_metrics import rebuild_shadow_metrics


BASE_DIR = Path('/root/quant_project')
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from config.settings import Config, ensure_syspath

ensure_syspath()
LOG_DIR = Path(Config.LOG_DIR)
LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger('shadow.receiver')
_SEEN_TASK_LIMIT = 2000
_seen_task_ids: OrderedDict[str, None] = OrderedDict()
_SHADOW_SCHEMA_READY = False


def _entry_tide_context(raw_ratio) -> tuple[str, float, str]:
    """Normalize L4 market-sentiment ratio into a Shadow entry policy tag."""
    try:
        ratio = float(raw_ratio)
    except Exception:
        ratio = 0.0
    if ratio <= 0:
        return 'UNKNOWN', 0.0, 'STANDARD_ENTRY'
    if ratio < 0.30:
        return 'FORCE_NO_EDGE', ratio, 'TIDE_SUPPRESSED_EXPERIMENT'
    if ratio < 0.70:
        return 'CAUTION', ratio, 'STANDARD_ENTRY'
    return 'AGGRESSIVE', ratio, 'STANDARD_ENTRY'


def _remember_task_id(task_id: str) -> None:
    task = str(task_id or '').strip()
    if not task:
        return
    _seen_task_ids[task] = None
    _seen_task_ids.move_to_end(task)
    while len(_seen_task_ids) > _SEEN_TASK_LIMIT:
        _seen_task_ids.popitem(last=False)


def _ensure_nexus_shadow_columns() -> bool:
    """Ensure receiver-required nexus_audits columns exist before read-only scans."""
    global _SHADOW_SCHEMA_READY
    if _SHADOW_SCHEMA_READY:
        return True
    try:
        with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
            exists = conn.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = 'nexus_audits'
                LIMIT 1
                """
            ).fetchone()
            if not exists:
                logger.warning('[Receiver] nexus_audits absent; shadow signal scan blocked')
                return False
            cols = {
                str(row[1]).lower()
                for row in conn.execute("PRAGMA table_info('nexus_audits')").fetchall()
            }
            if 'shadow_processed' in cols:
                _SHADOW_SCHEMA_READY = True
                return True

        with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
            cols = {
                str(row[1]).lower()
                for row in conn.execute("PRAGMA table_info('nexus_audits')").fetchall()
            }
            if 'shadow_processed' not in cols:
                conn.execute("ALTER TABLE nexus_audits ADD COLUMN shadow_processed INTEGER DEFAULT 0")
                logger.info('[Receiver] patch schema add column: nexus_audits.shadow_processed')
        _SHADOW_SCHEMA_READY = True
        return True
    except Exception as exc:
        logger.error('[Receiver] nexus_audits shadow schema ensure failed: %s', exc, exc_info=True)
        return False


def fetch_pending_signals(
    limit: int = 20,
    trade_date: str | None = None,
    run_id: str | None = None,
) -> List[Dict]:
    """Fetch PASS signals only from one explicit, completed audit scope."""
    trade_date = str(trade_date or '').strip()
    run_id = str(run_id or '').strip()
    if not trade_date or not run_id:
        logger.error(
            '[Receiver] unscoped signal fetch blocked | trade_date=%s run_id=%s',
            trade_date or '<empty>', run_id or '<empty>',
        )
        return []
    if not _ensure_nexus_shadow_columns():
        return []
    with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
        score_col = detect_nexus_score_column(conn)
        if not score_col:
            logger.error('[Receiver] no score column found in nexus_audits, blocked')
            return []

        filters = [
            "UPPER(COALESCE(n.l4_final_verdict, '')) IN (?, 'APPROVE')",
            f"COALESCE({score_col}, 0) >= ?",
            "COALESCE(n.shadow_processed, 0) = 0",
            'CAST(n.trade_date AS DATE) = CAST(? AS DATE)',
            'n.run_id = ?',
        ]
        params = [
            SIGNAL_VERDICT_PASS, DEFAULT_MIN_FINAL_SCORE,
            trade_date, run_id,
        ]
        where_sql = ' AND '.join(filters)

        rows = conn.execute(
            f"""
            WITH ranked AS (
                SELECT
                    n.task_id,
                    n.run_id,
                    n.symbol,
                    COALESCE(NULLIF(TRIM(n.name), ''), NULLIF(TRIM(b.name), ''), n.symbol) AS name,
                    CAST(n.trade_date AS DATE) AS trade_date,
                    COALESCE({score_col}, 0) AS final_score,
                    n.l1_close,
                    COALESCE(n.l4_market_sentiment, 0) AS tide_ratio,
                    n.created_at,
                    COALESCE(a.primary_archetype, 'UNCLASSIFIED') AS trade_archetype,
                    COALESCE(a.confidence, 0) AS archetype_confidence,
                    ROW_NUMBER() OVER (
                        PARTITION BY n.symbol
                        ORDER BY n.created_at DESC, n.task_id DESC
                    ) AS rn
                FROM nexus_audits n
                LEFT JOIN fact_stock_basic b ON b.symbol = n.symbol
                LEFT JOIN fact_trade_archetype_observations a
                  ON a.task_id = n.task_id AND a.classifier_version = 'trade_archetype_v0.1'
                WHERE {where_sql}
            )
            SELECT task_id, run_id, symbol, name, trade_date, final_score, l1_close, tide_ratio, created_at,
                   trade_archetype, archetype_confidence
            FROM ranked
            WHERE rn = 1
            ORDER BY created_at DESC
            LIMIT ?
            """,
            params + [limit],
        ).fetchall()

    signals: List[Dict] = []
    for row in rows:
        task_id = str(row[0] or '').strip()
        if not task_id or task_id in _seen_task_ids:
            continue
        tide_gate, tide_ratio, entry_policy = _entry_tide_context(row[7])
        signals.append(
            {
                'task_id': task_id,
                'run_id': str(row[1] or ''),
                'symbol': str(row[2] or ''),
                'name': str(row[3] or row[2] or ''),
                'trade_date': str(row[4] or ''),
                'final_score': float(row[5] or 0),
                'close_price': float(row[6] or 0),
                'entry_tide_gate': tide_gate,
                'entry_tide_ratio': tide_ratio,
                'entry_policy': entry_policy,
                'created_at': str(row[8] or ''),
                'trade_archetype': str(row[9] or 'UNCLASSIFIED').strip().upper(),
                'archetype_confidence': float(row[10] or 0),
            }
        )
    return signals


def _signal_account_eligibility(signal: Dict):
    symbol = str(signal.get("symbol") or "").strip().upper()
    name = str(signal.get("name") or symbol).strip()
    is_st = None
    try:
        with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
            row = conn.execute(
                "SELECT COALESCE(name, ''), COALESCE(is_st, FALSE) FROM fact_stock_basic WHERE symbol=? LIMIT 1",
                [symbol],
            ).fetchone()
        if row:
            name = str(row[0] or name).strip()
            is_st = bool(row[1])
    except Exception as exc:
        logger.warning("[Receiver] account eligibility lookup failed closed %s: %s", symbol, exc)
        return evaluate_account_eligibility("", name, False)
    return evaluate_account_eligibility(symbol, name, is_st)


def _mark_shadow_processed(task_id: str) -> bool:
    task = str(task_id or '').strip()
    if not task:
        return False
    try:
        with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
            row = conn.execute(
                """
                UPDATE nexus_audits
                SET shadow_processed = 1
                WHERE task_id = ?
                  AND COALESCE(shadow_processed, 0) = 0
                RETURNING task_id
                """,
                [task],
            ).fetchone()
        if row:
            logger.info(f'[Receiver] nexus_audits.shadow_processed marked: {task}')
            return True
        logger.info(f'[Receiver] nexus_audits.shadow_processed already marked or missing: {task}')
        return False
    except Exception as exc:
        logger.error(f'[Receiver] mark shadow_processed failed: {task} | {exc}', exc_info=True)
        return False


def _has_shadow_ledger_trace(task_id: str) -> bool:
    task = str(task_id or '').strip()
    if not task:
        return False
    try:
        with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM fact_shadow_ledger
                WHERE trace_id = ?
                LIMIT 1
                """,
                [task],
            ).fetchone()
        return bool(row)
    except Exception as exc:
        logger.debug(f'[Receiver] ledger trace lookup failed: {task} | {exc}')
        return False


def _ensure_reinforcement_table(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_shadow_reinforcement_events (
            event_id VARCHAR PRIMARY KEY,
            event_type VARCHAR DEFAULT 'L4_REPEAT_PASS_WHILE_HOLDING',
            trade_date DATE NOT NULL,
            symbol VARCHAR NOT NULL,
            name VARCHAR DEFAULT '',
            task_id VARCHAR DEFAULT '',
            run_id VARCHAR DEFAULT '',
            l4_score DOUBLE DEFAULT 0,
            l1_close DOUBLE DEFAULT 0,
            position_trade_date DATE,
            position_qty INTEGER DEFAULT 0,
            entry_price DOUBLE DEFAULT 0,
            pnl_ratio DOUBLE DEFAULT 0,
            reinforcement_count INTEGER DEFAULT 0,
            strength_tier VARCHAR DEFAULT '',
            evidence_json VARCHAR DEFAULT '{}',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_shadow_reinforce_symbol_date
        ON fact_shadow_reinforcement_events(symbol, trade_date)
        """
    )


def _record_reinforcement_if_holding(signal: Dict) -> str:
    """Record repeated L4 PASS as hold reinforcement, not as another buy order."""
    symbol = str(signal.get('symbol') or '').strip().upper()
    task_id = str(signal.get('task_id') or '').strip()
    trade_date = str(signal.get('trade_date') or time.strftime('%Y-%m-%d'))[:10]
    if not symbol or not task_id:
        return 'NO_POSITION'

    try:
        with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
            _ensure_paper_positions(conn)
            _ensure_reinforcement_table(conn)

            existing = conn.execute(
                """
                SELECT 1
                FROM fact_shadow_reinforcement_events
                WHERE event_id = ?
                LIMIT 1
                """,
                [task_id],
            ).fetchone()
            if existing:
                logger.info('[Receiver] reinforcement already recorded | %s task=%s', symbol, task_id)
                return 'RECORDED'

            row = conn.execute(
                """
                SELECT
                    CAST(trade_date AS VARCHAR) AS position_trade_date,
                    COALESCE(qty, 0) AS qty,
                    COALESCE(entry_price, 0) AS entry_price,
                    COALESCE(entry_score, 0) AS entry_score,
                    COALESCE(strength_tier, 'NORMAL') AS strength_tier,
                    COALESCE(reinforcement_count, 0) AS reinforcement_count
                FROM fact_paper_positions
                WHERE symbol = ?
                  AND UPPER(COALESCE(status, '')) = 'HOLD'
                ORDER BY trade_date DESC
                LIMIT 1
                """,
                [symbol],
            ).fetchone()
            if not row:
                return 'NO_POSITION'

            position_trade_date = str(row[0] or '')[:10]
            qty = int(row[1] or 0)
            entry_price = float(row[2] or 0)
            strength_tier = str(row[4] or 'NORMAL').strip().upper() or 'NORMAL'
            previous_count = int(row[5] or 0)
            new_count = previous_count + 1
            score = float(signal.get('final_score') or 0)
            l1_close = float(signal.get('close_price') or 0)
            pnl_ratio = round((l1_close - entry_price) / entry_price, 6) if entry_price > 0 and l1_close > 0 else 0.0
            name = str(signal.get('name') or '').strip() or _lookup_stock_name(symbol)
            reason = f'L4_REPEAT_PASS score={score:.0f} count={new_count}'
            now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            evidence = {
                'context': 'held_position_repeat_l4_pass',
                'previous_reinforcement_count': previous_count,
                'signal_created_at': str(signal.get('created_at') or ''),
                'entry_score': float(row[3] or 0),
                'policy': 'record_only_no_auto_add',
            }

            conn.execute(
                """
                UPDATE fact_paper_positions
                SET reinforcement_count = ?,
                    last_reinforced_date = CAST(? AS DATE),
                    last_reinforced_score = ?,
                    reinforcement_reason = ?,
                    reinforcement_updated_at = CAST(? AS TIMESTAMP),
                    updated_at = CAST(? AS TIMESTAMP)
                WHERE symbol = ?
                  AND CAST(trade_date AS DATE) = CAST(? AS DATE)
                  AND UPPER(COALESCE(status, '')) = 'HOLD'
                """,
                [
                    new_count,
                    trade_date,
                    score,
                    reason,
                    now_ts,
                    now_ts,
                    symbol,
                    position_trade_date,
                ],
            )
            conn.execute(
                """
                INSERT INTO fact_shadow_reinforcement_events (
                    event_id, event_type, trade_date, symbol, name, task_id, run_id,
                    l4_score, l1_close, position_trade_date, position_qty,
                    entry_price, pnl_ratio, reinforcement_count, strength_tier,
                    evidence_json, created_at, updated_at
                )
                VALUES (?, 'L4_REPEAT_PASS_WHILE_HOLDING', CAST(? AS DATE), ?, ?, ?, ?, ?, ?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP))
                """,
                [
                    task_id,
                    trade_date,
                    symbol,
                    name,
                    task_id,
                    str(signal.get('run_id') or ''),
                    score,
                    l1_close,
                    position_trade_date,
                    qty,
                    entry_price,
                    pnl_ratio,
                    new_count,
                    strength_tier,
                    json.dumps(evidence, ensure_ascii=False, default=str)[:4000],
                    now_ts,
                    now_ts,
                ],
            )

        logger.info(
            '[Receiver] reinforcement recorded | %s signal_date=%s score=%s count=%s task=%s',
            symbol,
            trade_date,
            score,
            new_count,
            task_id,
        )
        return 'RECORDED'
    except Exception as exc:
        logger.error('[Receiver] reinforcement recording failed: %s task=%s | %s', symbol, task_id, exc, exc_info=True)
        return 'ERROR'


def _calculate_position_size(price: float, capital_available: float, max_pct: float = 0.25) -> int:
    if price <= 0:
        return 0
    max_amount = capital_available * max_pct
    shares = int(max_amount / price)
    lots = shares // 100
    return lots * 100


def _ensure_shadow_metrics(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS shadow_metrics (
            trade_date DATE PRIMARY KEY,
            total_equity DOUBLE,
            cash_reserve DOUBLE,
            daily_drawdown DOUBLE,
            active_positions INTEGER,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cols = {
        str(row[1]).lower()
        for row in conn.execute("PRAGMA table_info('shadow_metrics')").fetchall()
    }
    if 'updated_at' not in cols:
        conn.execute("ALTER TABLE shadow_metrics ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")


def _ensure_shadow_skipped_signal_table(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_shadow_skipped_signals (
            signal_id VARCHAR PRIMARY KEY,
            task_id VARCHAR DEFAULT '',
            run_id VARCHAR DEFAULT '',
            trade_date DATE,
            symbol VARCHAR NOT NULL,
            name VARCHAR DEFAULT '',
            reason VARCHAR DEFAULT '',
            final_score DOUBLE DEFAULT 0,
            l1_close DOUBLE DEFAULT 0,
            entry_tide_gate VARCHAR DEFAULT '',
            entry_tide_ratio DOUBLE DEFAULT 0,
            entry_policy VARCHAR DEFAULT '',
            evidence_json VARCHAR DEFAULT '{}',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_shadow_skipped_signals_date
        ON fact_shadow_skipped_signals(trade_date, reason)
        """
    )


def _record_shadow_skip(signal: Dict, reason: str) -> bool:
    task_id = str(signal.get('task_id') or '').strip()
    symbol = str(signal.get('symbol') or '').strip().upper()
    trade_date = str(signal.get('trade_date') or time.strftime('%Y-%m-%d'))[:10]
    if not task_id or not symbol:
        return False
    now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    evidence = {
        'context': 'shadow_entry_gate',
        'policy': 'record_only_no_t1_queue',
        'created_at': str(signal.get('created_at') or ''),
    }
    news_gate_evidence = signal.get('news_gate_evidence')
    if isinstance(news_gate_evidence, dict):
        evidence['news_gate'] = news_gate_evidence
    news_gate_reason_cn = str(signal.get('news_gate_reason_cn') or '').strip()
    if news_gate_reason_cn:
        evidence['news_gate_reason_cn'] = news_gate_reason_cn
    eligibility_evidence = signal.get('account_eligibility_evidence')
    if isinstance(eligibility_evidence, dict):
        evidence['account_eligibility'] = eligibility_evidence
    trade_contract = signal.get('trade_contract')
    if isinstance(trade_contract, dict):
        evidence['trade_contract'] = trade_contract
    try:
        with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
            _ensure_shadow_skipped_signal_table(conn)
            conn.execute(
                """
                INSERT INTO fact_shadow_skipped_signals (
                    signal_id, task_id, run_id, trade_date, symbol, name, reason,
                    final_score, l1_close, entry_tide_gate, entry_tide_ratio,
                    entry_policy, evidence_json, created_at, updated_at
                )
                VALUES (?, ?, ?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP))
                ON CONFLICT (signal_id) DO UPDATE SET
                    run_id = EXCLUDED.run_id,
                    name = EXCLUDED.name,
                    reason = EXCLUDED.reason,
                    final_score = EXCLUDED.final_score,
                    l1_close = EXCLUDED.l1_close,
                    entry_tide_gate = EXCLUDED.entry_tide_gate,
                    entry_tide_ratio = EXCLUDED.entry_tide_ratio,
                    entry_policy = EXCLUDED.entry_policy,
                    evidence_json = EXCLUDED.evidence_json,
                    updated_at = EXCLUDED.updated_at
                """,
                [
                    task_id,
                    task_id,
                    str(signal.get('run_id') or ''),
                    trade_date,
                    symbol,
                    str(signal.get('name') or symbol).strip(),
                    str(reason or '')[:300],
                    float(signal.get('final_score') or 0),
                    float(signal.get('close_price') or 0),
                    str(signal.get('entry_tide_gate') or '').strip().upper(),
                    float(signal.get('entry_tide_ratio') or 0),
                    str(signal.get('entry_policy') or '').strip().upper(),
                    json.dumps(evidence, ensure_ascii=False, default=str)[:4000],
                    now_ts,
                    now_ts,
                ],
            )
        return True
    except Exception as exc:
        logger.error('[Receiver] shadow skip record failed: %s task=%s | %s', symbol, task_id, exc, exc_info=True)
        return False


def _get_current_metrics() -> Dict:
    with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
        row = conn.execute(
            """
            SELECT total_equity, cash_reserve, active_positions
            FROM shadow_metrics
            ORDER BY trade_date DESC
            LIMIT 1
            """
        ).fetchone()

    if row:
        return {
            'total_equity': float(row[0] or 0),
            'cash_reserve': float(row[1] or 0),
            'active_positions': int(row[2] or 0),
        }

    rules = _load_rules()
    init_cap = float(rules.get('position', {}).get('initial_capital', 1_000_000.0))
    return {'total_equity': init_cap, 'cash_reserve': init_cap, 'active_positions': 0}


def _lookup_stock_name(symbol: str) -> str:
    sym = str(symbol or '').strip().upper()
    if not sym:
        return ''
    try:
        with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
            row = conn.execute(
                "SELECT name FROM fact_stock_basic WHERE symbol = ? LIMIT 1",
                [sym],
            ).fetchone()
        if row and row[0]:
            return str(row[0]).strip()
    except Exception as exc:
        logger.debug('[Receiver] stock name lookup failed %s: %s', sym, exc)
    return ''


def _push_shadow_buy(fill, signal: Dict, total_cost: float) -> bool:
    token = str(getattr(Config, 'PUSHPLUS_TOKEN', '') or '').strip()
    if not token:
        logger.warning('[Receiver] shadow BUY push skipped: PUSHPLUS_TOKEN missing')
        return False

    name = str(signal.get('name') or '').strip() or _lookup_stock_name(fill.symbol)
    display = f'{name} {fill.symbol}' if name and name != fill.symbol else fill.symbol
    title = f"\u70db\u9f99\u6a21\u62df\u76d8\u4e70\u5165 | {display}"
    content = "\n".join([
        '[\u6a21\u62df\u76d8\u4e70\u5165]',
        f"\u6807\u7684\uff1a{display}",
        f"\u4ea4\u6613\u65e5\uff1a{signal.get('trade_date', '')}",
        f"\u7cfb\u7edf\u8bc4\u5206\uff1a{float(signal.get('final_score') or 0):.0f}",
        f"\u4e70\u5165\u6570\u91cf\uff1a{fill.qty:,} \u80a1",
        f"\u53c2\u8003\u4ef7\u683c\uff1a{fill.price_logical:.4f}",
        f"\u6a21\u62df\u6210\u4ea4\u4ef7\uff1a{fill.price_shadow:.4f}",
        f"\u6210\u4ea4\u989d\uff1a{float(getattr(fill, 'gross_amount', total_cost) or 0):,.2f}",
        f"\u6ed1\u70b9\u6210\u672c\uff1a{fill.slippage_cost:,.2f}",
        f"\u4f63\u91d1\uff1a{float(getattr(fill, 'commission', 0) or 0):,.2f}",
        f"\u5370\u82b1\u7a0e\uff1a{float(getattr(fill, 'stamp_tax', 0) or 0):,.2f}",
        f"\u8fc7\u6237\u8d39\uff1a{float(getattr(fill, 'transfer_fee', 0) or 0):,.2f}",
        f"\u7a0e\u8d39\u5408\u8ba1\uff1a{float(getattr(fill, 'tax_total', 0) or 0):,.2f}",
        f"\u6a21\u62df\u5165\u8d26\u6210\u672c\uff1a{float(getattr(fill, 'net_amount', total_cost) or 0):,.2f}",
        '',
        '\u8bf4\u660e\uff1a\u4ec5\u4e3a\u5f71\u5b50\u6a21\u62df\u76d8\u8bb0\u5f55\uff0c\u4e0d\u4ee3\u8868\u771f\u5b9e\u4ea4\u6613\u6307\u4ee4\u3002',
    ])

    try:
        resp = requests.post(
            str(getattr(Config, 'PUSHPLUS_URL', 'https://www.pushplus.plus/send')),
            json={'token': token, 'title': title, 'content': content[:2000], 'template': 'txt'},
            timeout=int(getattr(Config, 'PUSHPLUS_TIMEOUT', 10) or 10),
        )
        ok = resp.status_code == 200 and resp.json().get('code') == 200
        if ok:
            logger.info(f'[Receiver] shadow BUY push sent: {fill.symbol}')
            return True
        logger.warning(f'[Receiver] shadow BUY push failed: {fill.symbol} | {resp.text[:300]}')
    except Exception as exc:
        logger.warning(f'[Receiver] shadow BUY push exception: {fill.symbol} | {exc}')
    return False


def _update_metrics(trade_date: str, cost: float, direction: int = 1) -> None:
    with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
        rebuild_shadow_metrics(conn, through_date=trade_date)


def process_signal(signal: Dict) -> bool:
    """Queue an L4 PASS signal for the next trading day's paper-fill cycle."""
    symbol = str(signal.get('symbol') or '').strip().upper()
    price = float(signal.get('close_price') or 0)
    trade_date = signal.get('trade_date') or time.strftime('%Y-%m-%d')
    task_id = signal.get('task_id') or ''

    if not symbol or not task_id:
        logger.warning(f'[Receiver] invalid signal for T1 queue: symbol={symbol} task_id={task_id}')
        return False

    reinforcement_status = _record_reinforcement_if_holding({**signal, 'symbol': symbol, 'trade_date': trade_date, 'close_price': price})
    if reinforcement_status == 'RECORDED':
        _mark_shadow_processed(task_id)
        _remember_task_id(task_id)
        logger.info(
            '[Receiver] L4 repeat PASS captured as reinforcement | %s signal_date=%s score=%s task=%s',
            symbol,
            trade_date,
            signal.get('final_score', 0),
            task_id,
        )
        return True
    if reinforcement_status == 'ERROR':
        logger.warning('[Receiver] reinforcement blocked queueing until retry | %s task=%s', symbol, task_id)
        return False

    eligibility = _signal_account_eligibility({**signal, "symbol": symbol})
    if not eligibility.allow_execution:
        gated_signal = {
            **signal, "symbol": symbol, "trade_date": trade_date, "close_price": price,
            "account_eligibility_evidence": eligibility.to_dict(),
        }
        if not _record_shadow_skip(gated_signal, eligibility.reason_code):
            logger.warning("[Receiver] account-ineligible skip record pending retry | %s task=%s", symbol, task_id)
            return False
        _mark_shadow_processed(task_id)
        _remember_task_id(task_id)
        logger.warning(
            "[Receiver] account eligibility blocked T+1 queue | %s task=%s status=%s reason=%s",
            symbol, task_id, eligibility.qualification_status, eligibility.reason_code,
        )
        return True

    contract = build_trade_contract(
        task_id, symbol, str(trade_date), signal.get("trade_archetype", "UNCLASSIFIED"),
        float(signal.get("archetype_confidence") or 0),
    )
    signal = {**signal, "trade_contract": contract.to_dict(), "trade_archetype": contract.primary_archetype}
    if not contract.actionable:
        if not _record_shadow_skip(signal, contract.block_reason):
            logger.warning("[Receiver] trade-contract skip record pending retry | %s task=%s", symbol, task_id)
            return False
        _mark_shadow_processed(task_id)
        _remember_task_id(task_id)
        logger.warning(
            "[Receiver] trade contract blocked T+1 queue | %s task=%s archetype=%s confidence=%.3f",
            symbol, task_id, contract.primary_archetype, contract.archetype_confidence,
        )
        return True

    news_decision = evaluate_shadow_news_entry_gate({**signal, 'symbol': symbol, 'trade_date': trade_date, 'close_price': price})
    if not news_decision.get('allow'):
        reason = str(news_decision.get('reason') or 'SKIPPED_NEWS_UNAVAILABLE')
        gated_signal = {
            **signal,
            'symbol': symbol,
            'trade_date': trade_date,
            'close_price': price,
            'news_gate_evidence': news_decision.get('evidence') or {},
            'news_gate_reason_cn': str(news_decision.get('reason_cn') or ''),
        }
        if reason == 'SKIPPED_NEWS_UNAVAILABLE':
            wait_signal = {
                **gated_signal,
                'status': WAIT_NEWS_STATUS,
                'last_error': 'WAIT_NEWS_CHECK: news observation incomplete before T+1 fill',
            }
            if not enqueue_pending_signal(wait_signal):
                logger.warning('[Receiver] news-wait signal not queued; pending retry | %s task=%s', symbol, task_id)
                return False
            _mark_shadow_processed(task_id)
            _remember_task_id(task_id)
            logger.warning(
                '[Receiver] news gate waiting before T+1 fill | %s task=%s status=%s gate=%s',
                symbol,
                task_id,
                (news_decision.get('evidence') or {}).get('l4_news_status', ''),
                (news_decision.get('evidence') or {}).get('l4_news_gate', ''),
            )
            return True

        if not _record_shadow_skip(gated_signal, reason):
            logger.warning('[Receiver] news-gated signal not queued; skip record pending retry | %s task=%s reason=%s', symbol, task_id, reason)
            return False
        _mark_shadow_processed(task_id)
        _remember_task_id(task_id)
        push_shadow_news_gate_skip(gated_signal, news_decision, stage='queue')
        logger.warning(
            '[Receiver] news gate skipped T+1 queue | %s task=%s reason=%s status=%s gate=%s',
            symbol,
            task_id,
            reason,
            (news_decision.get('evidence') or {}).get('l4_news_status', ''),
            (news_decision.get('evidence') or {}).get('l4_news_gate', ''),
        )
        return True

    entry_tide_gate = str(signal.get('entry_tide_gate') or '').strip().upper()
    entry_policy = str(signal.get('entry_policy') or '').strip().upper()
    if entry_tide_gate == 'FORCE_NO_EDGE' or entry_policy == 'TIDE_SUPPRESSED_EXPERIMENT':
        reason = f'TIDE_SUPPRESSED_NO_SHADOW_ENTRY gate={entry_tide_gate or "UNKNOWN"} policy={entry_policy or "UNKNOWN"}'
        if not _record_shadow_skip({**signal, 'symbol': symbol, 'trade_date': trade_date, 'close_price': price}, reason):
            logger.warning('[Receiver] tide-suppressed signal not queued; skip record pending retry | %s task=%s', symbol, task_id)
            return False
        _mark_shadow_processed(task_id)
        _remember_task_id(task_id)
        logger.info('[Receiver] tide-suppressed PASS skipped from T+1 queue | %s task=%s | %s', symbol, task_id, reason)
        return True

    queued = enqueue_pending_signal({**signal, 'symbol': symbol, 'trade_date': trade_date, 'close_price': price})
    if queued:
        _mark_shadow_processed(task_id)
        _remember_task_id(task_id)
        logger.info(
            '[Receiver] T+1 pending queued | %s signal_date=%s score=%s task=%s',
            symbol,
            trade_date,
            signal.get('final_score', 0),
            task_id,
        )
        return True

    logger.warning('[Receiver] T+1 queue failed | %s task=%s', symbol, task_id)
    return False


def poll_loop(interval: int = 5, max_cycles: int | None = None) -> None:
    logger.info(
        f'[Receiver] started | interval={interval}s | gate verdict={SIGNAL_VERDICT_PASS} '
        f'| min_final_score={DEFAULT_MIN_FINAL_SCORE}'
    )

    cycle = 0
    while True:
        cycle += 1
        if max_cycles and cycle > max_cycles:
            logger.info(f'[Receiver] reached max cycles: {max_cycles}')
            break

        try:
            signals = fetch_pending_signals()
            if signals:
                logger.info(f'[Receiver] fetched {len(signals)} pending signals')
                for sig in signals:
                    process_signal(sig)
            time.sleep(interval)
        except KeyboardInterrupt:
            logger.info('[Receiver] interrupted by keyboard')
            break
        except Exception as exc:
            logger.error(f'[Receiver] poll loop error: {exc}')
            time.sleep(interval)


def inject_test_signals() -> None:
    """Kept for CLI compatibility. Disabled by contract (nexus_audits is read-only)."""
    logger.warning('[Receiver] inject_test_signals disabled: nexus_audits is read-only by contract')


if __name__ == '__main__':
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(message)s',
        handlers=[
            RotatingFileHandler(str(LOG_DIR / 'shadow_engine.log'), maxBytes=20*1024*1024, backupCount=5, mode='a', encoding='utf-8'),
            logging.StreamHandler(),
        ],
    )

    parser = argparse.ArgumentParser(description='Shadow Signal Receiver')
    parser.add_argument('--poll', action='store_true', help='Start polling')
    parser.add_argument('--inject-test', action='store_true', help='No-op; disabled by contract')
    parser.add_argument('--dry-run', action='store_true', help='Run one-pass in-memory dry-run')
    parser.add_argument('--interval', type=int, default=5, help='Poll interval seconds')
    parser.add_argument('--cycles', type=int, default=None, help='Max polling cycles')
    parser.add_argument('--date', type=str, default=None, help='Restrict one-pass fetch to trade_date')
    parser.add_argument('--run-id', type=str, default=None, help='Restrict one-pass fetch to run_id')
    args = parser.parse_args()

    if args.inject_test:
        inject_test_signals()
    elif args.dry_run:
        sigs = fetch_pending_signals(trade_date=args.date, run_id=args.run_id)
        print(f'dry-run pending signals: {len(sigs)}')
        for sig in sigs:
            print(sig)
    elif args.poll:
        poll_loop(interval=args.interval, max_cycles=args.cycles)
    elif args.date or args.run_id:
        sigs = fetch_pending_signals(trade_date=args.date, run_id=args.run_id)
        print(f'signals: {len(sigs)}')
        ok = 0
        for sig in sigs:
            if process_signal(sig):
                ok += 1
        print(f'processed: {ok}/{len(sigs)}')
    else:
        print('usage: --poll | --inject-test | --dry-run | --date YYYY-MM-DD [--run-id RUN]')
