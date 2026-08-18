#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
05_shadow/lib/strategy_memory_builder.py
Build verified strategy-memory events for RAG evolution.

This module turns paper trading actions and subsequent market outcomes into
structured memory events. It does not call any model and never touches real
brokerage state.
"""

from __future__ import annotations

import json
import logging
import requests
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List

CURRENT_DIR = Path(__file__).resolve().parent
BASE_DIR = Path('/root/quant_project')
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

try:
    from .db_contract import DB_PATH, DBGateway
except Exception:
    from db_contract import DB_PATH, DBGateway

try:
    from config.settings import Config, ensure_syspath
    ensure_syspath()
except Exception:
    Config = None  # type: ignore

logger = logging.getLogger('shadow.strategy_memory')

HORIZONS = (1, 3, 5, 10)
WIN_THRESHOLD = 0.02
LOSS_THRESHOLD = -0.02
SELL_RAG_PUSH_THRESHOLD = 50


def _normalize_trade_date(trade_date: str | None) -> str:
    text = str(trade_date or '').strip()
    if not text:
        return datetime.now().strftime('%Y-%m-%d')
    if len(text) == 8 and text.isdigit():
        return f'{text[:4]}-{text[4:6]}-{text[6:8]}'
    if len(text) >= 10 and text[4] == '-' and text[7] == '-':
        return text[:10]
    raise ValueError(f'Unsupported trade_date: {trade_date}')


def ensure_strategy_memory_events_table(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_strategy_memory_events (
            event_id VARCHAR PRIMARY KEY,
            symbol VARCHAR NOT NULL,
            signal_trade_date DATE NOT NULL,
            event_trade_date DATE NOT NULL,
            source_event VARCHAR DEFAULT '',
            action VARCHAR DEFAULT '',
            horizon_days INTEGER DEFAULT 0,
            outcome_label VARCHAR DEFAULT 'PENDING',
            entry_price DOUBLE DEFAULT 0,
            eval_price DOUBLE DEFAULT 0,
            return_pct DOUBLE DEFAULT 0,
            max_return_pct DOUBLE DEFAULT 0,
            max_drawdown_pct DOUBLE DEFAULT 0,
            decision_quality VARCHAR DEFAULT '',
            evidence_json VARCHAR DEFAULT '{}',
            narrative_text VARCHAR DEFAULT '',
            memory_status VARCHAR DEFAULT 'READY',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_strategy_memory_event_date
        ON fact_strategy_memory_events(event_trade_date)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_strategy_memory_symbol
        ON fact_strategy_memory_events(symbol, signal_trade_date)
        """
    )


def ensure_strategy_memory_alerts_table(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_strategy_memory_alerts (
            alert_key VARCHAR PRIMARY KEY,
            alert_value VARCHAR DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = ?
        LIMIT 1
        """,
        [table_name],
    ).fetchone()
    return bool(row)


def _ensure_intraday_decision_columns(conn) -> None:
    existing = {
        str(row[1]).lower()
        for row in conn.execute("PRAGMA table_info('fact_shadow_intraday_decisions')").fetchall()
    }
    required = {
        'days_held': 'INTEGER DEFAULT 0',
        'max_gain_ratio': 'DOUBLE DEFAULT 0',
        'drawdown_from_high': 'DOUBLE DEFAULT 0',
        'action_intent': "VARCHAR DEFAULT 'HOLD'",
        'sell_rule_id': "VARCHAR DEFAULT ''",
        'strength_tier': "VARCHAR DEFAULT ''",
        'qty_sold': 'INTEGER DEFAULT 0',
        'qty_after': 'INTEGER DEFAULT 0',
        'sell_price': 'DOUBLE DEFAULT 0',
        'stop_after': 'DOUBLE DEFAULT 0',
    }
    for column, ddl in required.items():
        if column not in existing:
            conn.execute(f"ALTER TABLE fact_shadow_intraday_decisions ADD COLUMN {column} {ddl}")


def _label_return(return_pct: float) -> str:
    if return_pct >= WIN_THRESHOLD:
        return 'WIN'
    if return_pct <= LOSS_THRESHOLD:
        return 'LOSS'
    return 'FLAT'


def _quality_for_action(action: str, return_pct: float) -> str:
    action_u = str(action or '').upper()
    if action_u in {'BUY', 'L4_PASS'} or action_u.endswith('_BUY'):
        return 'BUY_VALIDATED' if return_pct > 0 else 'BUY_WEAK'
    if 'SELL' in action_u or 'STOP' in action_u:
        return 'SELL_VALIDATED' if return_pct <= 0 else 'SELL_TOO_EARLY'
    return 'OBSERVED'


def _label_post_sell_return(return_pct: float, max_return_pct: float, max_drawdown_pct: float) -> str:
    if max_return_pct >= 0.04 or return_pct >= WIN_THRESHOLD:
        return 'POST_EXIT_UP'
    if max_drawdown_pct <= -0.03 or return_pct <= LOSS_THRESHOLD:
        return 'POST_EXIT_DOWN'
    return 'POST_EXIT_FLAT'


def _quality_for_sell(rule_id: str, return_pct: float, max_return_pct: float) -> str:
    rule_u = str(rule_id or '').upper()
    if max_return_pct >= 0.04 or return_pct >= WIN_THRESHOLD:
        if 'WRONG_PICK' in rule_u or 'TIME_EFFICIENCY' in rule_u:
            return 'SELL_TOO_EARLY'
        return 'SELL_EARLY_REMAINDER_GAIN'
    if return_pct <= 0:
        return 'SELL_VALIDATED'
    return 'SELL_NEUTRAL'


def _make_narrative(
    *,
    symbol: str,
    action: str,
    horizon: int,
    outcome: str,
    ret: float,
    max_ret: float,
    max_dd: float,
    entry_price: float,
    eval_price: float,
) -> str:
    return (
        f"{symbol} strategy memory: action={action}, horizon=T+{horizon}, "
        f"outcome={outcome}, return={ret:.2%}, max_return={max_ret:.2%}, "
        f"max_drawdown={max_dd:.2%}, entry={entry_price:.4f}, eval={eval_price:.4f}."
    )


def _insert_event(conn, event: Dict[str, object]) -> bool:
    conn.execute(
        """
        INSERT INTO fact_strategy_memory_events (
            event_id, symbol, signal_trade_date, event_trade_date, source_event,
            action, horizon_days, outcome_label, entry_price, eval_price,
            return_pct, max_return_pct, max_drawdown_pct, decision_quality,
            evidence_json, narrative_text, memory_status, created_at, updated_at
        )
        VALUES (?, ?, CAST(? AS DATE), CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP))
        ON CONFLICT (event_id) DO UPDATE SET
            outcome_label = EXCLUDED.outcome_label,
            eval_price = EXCLUDED.eval_price,
            return_pct = EXCLUDED.return_pct,
            max_return_pct = EXCLUDED.max_return_pct,
            max_drawdown_pct = EXCLUDED.max_drawdown_pct,
            decision_quality = EXCLUDED.decision_quality,
            evidence_json = EXCLUDED.evidence_json,
            narrative_text = EXCLUDED.narrative_text,
            memory_status = EXCLUDED.memory_status,
            updated_at = EXCLUDED.updated_at
        """,
        [
            event['event_id'],
            event['symbol'],
            event['signal_trade_date'],
            event['event_trade_date'],
            event['source_event'],
            event['action'],
            int(event['horizon_days']),
            event['outcome_label'],
            float(event['entry_price']),
            float(event['eval_price']),
            float(event['return_pct']),
            float(event['max_return_pct']),
            float(event['max_drawdown_pct']),
            event['decision_quality'],
            event['evidence_json'],
            event['narrative_text'],
            event['memory_status'],
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        ],
    )
    return True


def _push_sell_rag_threshold(count: int, as_of: str) -> bool:
    if Config is None:
        return False
    token = str(getattr(Config, 'PUSHPLUS_TOKEN', '') or '').strip()
    if not token:
        return False
    content = '\n'.join([
        '[烛龙RAG提醒]',
        f'截至：{as_of}',
        f'卖出策略 RAG 记录：{count} 条',
        '',
        '卖出样本已经超过 50 条，可以考虑周末复盘：',
        '1. 哪些卖出规则最容易卖早；',
        '2. 哪些卖出规则最能保护收益；',
        '3. 是否允许 L4/RAG 在卖出阶段提供辅助建议。',
        '',
        '说明：这只是复盘提醒，不会自动改变当前卖出决策流程。',
    ])
    try:
        resp = requests.post(
            str(getattr(Config, 'PUSHPLUS_URL', 'https://www.pushplus.plus/send')),
            json={
                'token': token,
                'title': '烛龙卖出RAG样本提醒',
                'content': content[:2000],
                'template': 'txt',
            },
            timeout=int(getattr(Config, 'PUSHPLUS_TIMEOUT', 10) or 10),
        )
        ok = resp.status_code == 200 and resp.json().get('code') == 200
        if not ok:
            logger.warning(f'[strategy-memory] sell RAG threshold push failed: {resp.text[:300]}')
        return ok
    except Exception as exc:
        logger.warning(f'[strategy-memory] sell RAG threshold push exception: {exc}')
        return False


def _maybe_push_sell_rag_threshold(conn, as_of: str, stats: Dict[str, int | str]) -> None:
    ensure_strategy_memory_alerts_table(conn)
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM fact_strategy_memory_events
        WHERE source_event = 'shadow_sell_outcome'
          AND COALESCE(memory_status, 'READY') = 'READY'
        """
    ).fetchone()
    count = int((row[0] if row else 0) or 0)
    stats['sell_rag_records'] = count
    if count <= SELL_RAG_PUSH_THRESHOLD:
        stats['sell_rag_threshold_pushed'] = 0
        return
    alert_key = 'SELL_RAG_50_READY'
    exists = conn.execute(
        "SELECT 1 FROM fact_strategy_memory_alerts WHERE alert_key = ? LIMIT 1",
        [alert_key],
    ).fetchone()
    if exists:
        stats['sell_rag_threshold_pushed'] = 0
        return
    if _push_sell_rag_threshold(count, as_of):
        conn.execute(
            """
            INSERT INTO fact_strategy_memory_alerts (alert_key, alert_value, created_at)
            VALUES (?, ?, CAST(? AS TIMESTAMP))
            ON CONFLICT (alert_key) DO NOTHING
            """,
            [alert_key, str(count), datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
        )
        stats['sell_rag_threshold_pushed'] = 1
    else:
        stats['sell_rag_threshold_pushed'] = 0


def build_strategy_memory_events(as_of_date: str | None = None) -> Dict[str, int | str]:
    """Build matured strategy events up to as_of_date."""
    as_of = _normalize_trade_date(as_of_date)
    stats: Dict[str, int | str] = {
        'as_of_date': as_of,
        'positions_scanned': 0,
        'events_upserted': 0,
        'sell_outcome_events': 0,
        'sell_rag_records': 0,
        'sell_rag_threshold_pushed': 0,
        'skip_missing_daily': 0,
        'skip_no_tables': 0,
    }

    with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
        ensure_strategy_memory_events_table(conn)
        if not _table_exists(conn, 'fact_paper_positions') or not _table_exists(conn, 'fact_daily'):
            stats['skip_no_tables'] = 1
            return stats

        positions = conn.execute(
            """
            SELECT
                p.symbol,
                CAST(p.trade_date AS VARCHAR) AS signal_trade_date,
                COALESCE(p.entry_price, 0) AS entry_price,
                COALESCE(p.exit_price, 0) AS exit_price,
                COALESCE(p.pnl_ratio, NULL) AS realized_pnl,
                COALESCE(p.status, '') AS status,
                COALESCE(p.source, 'L4_PASS') AS source,
                COALESCE(p.strategy_tag, '') AS strategy_tag,
                COALESCE(p.signal_task_id, '') AS signal_task_id,
                COALESCE(p.entry_score, 0) AS entry_score,
                CAST(p.exit_trade_date AS VARCHAR) AS exit_trade_date
            FROM fact_paper_positions p
            WHERE CAST(p.trade_date AS DATE) <= CAST(? AS DATE)
              AND COALESCE(p.entry_price, 0) > 0
            ORDER BY p.trade_date, p.symbol
            """,
            [as_of],
        ).fetchall()
        stats['positions_scanned'] = len(positions)

        for row in positions:
            (
                symbol_raw,
                signal_td_raw,
                entry_raw,
                exit_raw,
                realized_raw,
                status_raw,
                source_raw,
                strategy_tag_raw,
                signal_task_raw,
                entry_score_raw,
                exit_td_raw,
            ) = row
            symbol = str(symbol_raw or '').strip().upper()
            signal_td = _normalize_trade_date(signal_td_raw)
            entry_price = float(entry_raw or 0)
            if not symbol or entry_price <= 0:
                continue

            daily_rows = conn.execute(
                """
                SELECT
                    CAST(trade_date AS VARCHAR) AS trade_date,
                    COALESCE(close, 0) AS close,
                    COALESCE(high, close, 0) AS high,
                    COALESCE(low, close, 0) AS low
                FROM fact_daily
                WHERE symbol = ?
                  AND CAST(trade_date AS DATE) >= CAST(? AS DATE)
                  AND CAST(trade_date AS DATE) <= CAST(? AS DATE)
                  AND COALESCE(close, 0) > 0
                ORDER BY trade_date
                """,
                [symbol, signal_td, as_of],
            ).fetchall()
            if not daily_rows:
                stats['skip_missing_daily'] += 1
                continue

            for horizon in HORIZONS:
                if len(daily_rows) <= horizon:
                    continue
                eval_row = daily_rows[horizon]
                event_td = _normalize_trade_date(eval_row[0])
                eval_price = float(eval_row[1] or 0)
                window = daily_rows[:horizon + 1]
                max_high = max(float(r[2] or 0) for r in window)
                min_low = min(float(r[3] or 0) for r in window)
                ret = round((eval_price - entry_price) / entry_price, 6)
                max_ret = round((max_high - entry_price) / entry_price, 6)
                max_dd = round((min_low - entry_price) / entry_price, 6)
                outcome = _label_return(ret)
                source_u = str(source_raw or 'L4_PASS').strip().upper() or 'UNKNOWN'
                action = 'L4_PASS_BUY' if source_u == 'L4_PASS' else f'{source_u}_BUY'
                source_event = f"{source_u}_T{horizon}"
                event_id = f'{symbol}|{signal_td}|T{horizon}|{action}'
                evidence = {
                    'source': source_raw,
                    'strategy_tag': strategy_tag_raw,
                    'signal_task_id': signal_task_raw,
                    'entry_score': float(entry_score_raw or 0),
                    'status': status_raw,
                    'exit_trade_date': str(exit_td_raw or ''),
                    'realized_pnl': realized_raw,
                }
                event = {
                    'event_id': event_id,
                    'symbol': symbol,
                    'signal_trade_date': signal_td,
                    'event_trade_date': event_td,
                    'source_event': source_event,
                    'action': action,
                    'horizon_days': horizon,
                    'outcome_label': outcome,
                    'entry_price': entry_price,
                    'eval_price': eval_price,
                    'return_pct': ret,
                    'max_return_pct': max_ret,
                    'max_drawdown_pct': max_dd,
                    'decision_quality': _quality_for_action(action, ret),
                    'evidence_json': json.dumps(evidence, ensure_ascii=False, default=str)[:2000],
                    'narrative_text': _make_narrative(
                        symbol=symbol,
                        action=action,
                        horizon=horizon,
                        outcome=outcome,
                        ret=ret,
                        max_ret=max_ret,
                        max_dd=max_dd,
                        entry_price=entry_price,
                        eval_price=eval_price,
                    ),
                    'memory_status': 'READY',
                }
                if _insert_event(conn, event):
                    stats['events_upserted'] += 1

        if _table_exists(conn, 'fact_shadow_position_reviews'):
            reviews = conn.execute(
                """
                SELECT
                    review_id,
                    symbol,
                    CAST(position_trade_date AS VARCHAR) AS signal_trade_date,
                    CAST(trade_date AS VARCHAR) AS event_trade_date,
                    COALESCE(position_status, '') AS position_status,
                    COALESCE(hold_days, 0) AS hold_days,
                    COALESCE(entry_price, 0) AS entry_price,
                    COALESCE(close_price, 0) AS close_price,
                    COALESCE(realized_pnl, NULL) AS realized_pnl,
                    COALESCE(unrealized_pnl, 0) AS unrealized_pnl,
                    COALESCE(max_gain_ratio, 0) AS max_gain_ratio,
                    COALESCE(max_drawdown_ratio, 0) AS max_drawdown_ratio,
                    COALESCE(decision_state, '') AS decision_state,
                    COALESCE(quality_label, '') AS quality_label,
                    COALESCE(evidence_json, '{}') AS evidence_json
                FROM fact_shadow_position_reviews
                WHERE CAST(trade_date AS DATE) <= CAST(? AS DATE)
                ORDER BY trade_date, symbol
                """,
                [as_of],
            ).fetchall()
            for row in reviews:
                review_id_raw = str(row[0] or '')
                symbol = str(row[1] or '').strip().upper()
                signal_td = _normalize_trade_date(row[2])
                event_td = _normalize_trade_date(row[3])
                status = str(row[4] or '').strip().upper()
                hold_days = int(row[5] or 0)
                entry_price = float(row[6] or 0)
                close_price = float(row[7] or 0)
                realized_raw = row[8]
                unrealized_pnl = float(row[9] or 0)
                max_gain_ratio = float(row[10] or 0)
                max_drawdown_ratio = float(row[11] or 0)
                decision_state = str(row[12] or '').strip().upper() or 'ACTIVE_HOLD'
                quality_label = str(row[13] or '').strip().upper() or _quality_for_action(decision_state, unrealized_pnl)
                evidence_json = str(row[14] or '{}')
                if not symbol or entry_price <= 0 or close_price <= 0:
                    continue
                if status == 'SOLD' and not review_id_raw.endswith('|FINAL'):
                    continue
                if decision_state == 'ACTIVE_HOLD' and quality_label in {'PENDING', 'BUY_VALIDATED_RUNNING', 'BUY_WEAK_RUNNING'}:
                    continue
                ret = float(realized_raw) if realized_raw is not None and status == 'SOLD' else unrealized_pnl
                outcome = _label_return(ret)
                action = f'POSITION_{decision_state}'
                event_key = 'FINAL' if status == 'SOLD' else event_td
                event_id = f'{symbol}|{signal_td}|{event_key}|POSITION_REVIEW'
                narrative = (
                    f"{symbol} paper position review: state={decision_state}, status={status}, "
                    f"hold_days={hold_days}, outcome={outcome}, return={ret:.2%}, "
                    f"max_gain={max_gain_ratio:.2%}, max_drawdown={max_drawdown_ratio:.2%}."
                )
                evidence = {
                    'position_status': status,
                    'hold_days': hold_days,
                    'decision_state': decision_state,
                    'quality_label': quality_label,
                    'review_evidence': evidence_json,
                }
                event = {
                    'event_id': event_id,
                    'symbol': symbol,
                    'signal_trade_date': signal_td,
                    'event_trade_date': event_td,
                    'source_event': 'shadow_position_review',
                    'action': action,
                    'horizon_days': hold_days,
                    'outcome_label': outcome,
                    'entry_price': entry_price,
                    'eval_price': close_price,
                    'return_pct': round(ret, 6),
                    'max_return_pct': round(max_gain_ratio, 6),
                    'max_drawdown_pct': round(max_drawdown_ratio, 6),
                    'decision_quality': quality_label,
                    'evidence_json': json.dumps(evidence, ensure_ascii=False, default=str)[:2000],
                    'narrative_text': narrative[:1000],
                    'memory_status': 'READY',
                }
                if _insert_event(conn, event):
                    stats['events_upserted'] += 1

        if _table_exists(conn, 'fact_shadow_intraday_decisions'):
            _ensure_intraday_decision_columns(conn)
            decisions = conn.execute(
                """
                SELECT
                    symbol,
                    CAST(position_trade_date AS VARCHAR) AS signal_trade_date,
                    CAST(trade_date AS VARCHAR) AS event_trade_date,
                    decision,
                    reason,
                    COALESCE(price, 0) AS price,
                    COALESCE(entry_price, 0) AS entry_price,
                    COALESCE(sell_price, price, 0) AS sell_price,
                    COALESCE(pnl_ratio, 0) AS pnl_ratio,
                    COALESCE(quote_source, '') AS quote_source,
                    COALESCE(signal_task_id, '') AS signal_task_id,
                    COALESCE(sell_rule_id, decision, '') AS sell_rule_id,
                    COALESCE(strength_tier, '') AS strength_tier,
                    COALESCE(qty_sold, 0) AS qty_sold,
                    COALESCE(qty_after, 0) AS qty_after,
                    COALESCE(action_intent, '') AS action_intent,
                    COALESCE(days_held, 0) AS days_held,
                    COALESCE(max_gain_ratio, 0) AS max_gain_ratio,
                    COALESCE(drawdown_from_high, 0) AS drawdown_from_high,
                    COALESCE(stop_after, 0) AS stop_after
                FROM fact_shadow_intraday_decisions
                WHERE CAST(trade_date AS DATE) <= CAST(? AS DATE)
                  AND (
                        UPPER(COALESCE(action_intent, '')) IN ('SELL', 'RAISE_STOP', 'WATCH_PROFIT', 'WATCH_RISK', 'EXEMPT')
                        OR UPPER(COALESCE(decision, '')) IN (
                            'SELL_STOP', 'RAISE_STOP', 'WRONG_PICK_STOP',
                            'TAKE_PROFIT_8_NORMAL', 'TAKE_PROFIT_8_STRONG',
                            'TAKE_PROFIT_12', 'TAKE_PROFIT_12_FULL',
                            'TAKE_PROFIT_12_LOCK', 'PROFIT_LOCK_TRIM',
                            'PROFIT_LOCK_CLEAR', 'PROFIT_LOCK_FLOOR_EXIT',
                            'PROFIT_LOCK_GAP_EXIT', 'PROFIT_LOCK_TRIM_SMALL_CLEAR',
                            'TWO_LIMIT_UP_FULL_EXIT', 'TRAILING_RUNNER_STOP',
                            'TIME_EFFICIENCY_EXIT', 'LIMIT_UP_EXEMPT'
                        )
                  )
                ORDER BY check_time
                """,
                [as_of],
            ).fetchall()
            for row in decisions:
                symbol = str(row[0] or '').strip().upper()
                signal_td = _normalize_trade_date(row[1])
                event_td = _normalize_trade_date(row[2])
                action = str(row[3] or '').strip().upper()
                reason = str(row[4] or '')
                price = float(row[5] or 0)
                entry_price = float(row[6] or 0)
                sell_price = float(row[7] or price)
                pnl_ratio = float(row[8] or 0)
                quote_source = str(row[9] or '')
                signal_task_id = str(row[10] or '')
                sell_rule_id = str(row[11] or action).strip().upper() or action
                strength_tier = str(row[12] or '').strip().upper()
                qty_sold = int(row[13] or 0)
                qty_after = int(row[14] or 0)
                action_intent = str(row[15] or '').strip().upper()
                days_held = int(row[16] or 0)
                max_gain_ratio = float(row[17] or 0)
                drawdown_from_high = float(row[18] or 0)
                stop_after = float(row[19] or 0)
                if not symbol or entry_price <= 0 or price <= 0:
                    continue
                outcome = _label_return(pnl_ratio)
                event_id = f'{symbol}|{signal_td}|{event_td}|{action}|INTRADAY'
                narrative = (
                    f"{symbol} intraday action={action}, outcome={outcome}, "
                    f"pnl_now={pnl_ratio:.2%}, qty_sold={qty_sold}, qty_after={qty_after}, "
                    f"rule={sell_rule_id}, tier={strength_tier}, reason={reason}, quote={quote_source}."
                )
                evidence = {
                    'reason': reason,
                    'quote_source': quote_source,
                    'signal_task_id': signal_task_id,
                    'sell_rule_id': sell_rule_id,
                    'strength_tier': strength_tier,
                    'qty_sold': qty_sold,
                    'qty_after': qty_after,
                    'action_intent': action_intent,
                    'days_held': days_held,
                    'max_gain_ratio': max_gain_ratio,
                    'drawdown_from_high': drawdown_from_high,
                    'stop_after': stop_after,
                }
                event = {
                    'event_id': event_id,
                    'symbol': symbol,
                    'signal_trade_date': signal_td,
                    'event_trade_date': event_td,
                    'source_event': f'intraday_{action.lower()}',
                    'action': action,
                    'horizon_days': 0,
                    'outcome_label': outcome,
                    'entry_price': entry_price,
                    'eval_price': price,
                    'return_pct': round(pnl_ratio, 6),
                    'max_return_pct': max(round(pnl_ratio, 6), 0.0),
                    'max_drawdown_pct': min(round(pnl_ratio, 6), 0.0),
                    'decision_quality': _quality_for_action(action, pnl_ratio),
                    'evidence_json': json.dumps(evidence, ensure_ascii=False, default=str)[:2000],
                    'narrative_text': narrative[:1000],
                    'memory_status': 'READY',
                }
                if _insert_event(conn, event):
                    stats['events_upserted'] += 1

                if action_intent != 'SELL' or qty_sold <= 0 or sell_price <= 0:
                    continue
                daily_rows = conn.execute(
                    """
                    SELECT
                        CAST(trade_date AS VARCHAR) AS trade_date,
                        close,
                        high,
                        low
                    FROM fact_daily
                    WHERE symbol = ?
                      AND CAST(trade_date AS DATE) >= CAST(? AS DATE)
                      AND CAST(trade_date AS DATE) <= CAST(? AS DATE)
                      AND close > 0
                    ORDER BY trade_date
                    """,
                    [symbol, event_td, as_of],
                ).fetchall()
                for horizon in HORIZONS:
                    if len(daily_rows) <= horizon:
                        continue
                    eval_row = daily_rows[horizon]
                    eval_td = _normalize_trade_date(eval_row[0])
                    eval_price = float(eval_row[1] or 0)
                    window = daily_rows[:horizon + 1]
                    max_high = max(float(r[2] or 0) for r in window)
                    min_low = min(float(r[3] or 0) for r in window)
                    post_ret = round((eval_price - sell_price) / sell_price, 6)
                    post_max_ret = round((max_high - sell_price) / sell_price, 6)
                    post_max_dd = round((min_low - sell_price) / sell_price, 6)
                    post_outcome = _label_post_sell_return(post_ret, post_max_ret, post_max_dd)
                    post_quality = _quality_for_sell(sell_rule_id, post_ret, post_max_ret)
                    post_event_id = f'{symbol}|{signal_td}|{event_td}|{sell_rule_id}|T{horizon}|SELL_OUTCOME'
                    post_evidence = {
                        'source_decision_date': event_td,
                        'sell_rule_id': sell_rule_id,
                        'reason': reason,
                        'strength_tier': strength_tier,
                        'qty_sold': qty_sold,
                        'qty_after': qty_after,
                        'entry_price': entry_price,
                        'sell_price': sell_price,
                        'pnl_at_sell': pnl_ratio,
                        'days_held_at_sell': days_held,
                        'max_gain_at_sell': max_gain_ratio,
                        'drawdown_from_high_at_sell': drawdown_from_high,
                        'stop_after': stop_after,
                        'quote_source': quote_source,
                        'signal_task_id': signal_task_id,
                    }
                    post_narrative = (
                        f"{symbol} shadow sell outcome: rule={sell_rule_id}, tier={strength_tier}, "
                        f"horizon=T+{horizon}, post_outcome={post_outcome}, "
                        f"post_return={post_ret:.2%}, post_max_return={post_max_ret:.2%}, "
                        f"post_max_drawdown={post_max_dd:.2%}, qty_sold={qty_sold}, qty_after={qty_after}."
                    )
                    post_event = {
                        'event_id': post_event_id,
                        'symbol': symbol,
                        'signal_trade_date': signal_td,
                        'event_trade_date': eval_td,
                        'source_event': 'shadow_sell_outcome',
                        'action': sell_rule_id,
                        'horizon_days': horizon,
                        'outcome_label': post_outcome,
                        'entry_price': sell_price,
                        'eval_price': eval_price,
                        'return_pct': post_ret,
                        'max_return_pct': post_max_ret,
                        'max_drawdown_pct': post_max_dd,
                        'decision_quality': post_quality,
                        'evidence_json': json.dumps(post_evidence, ensure_ascii=False, default=str)[:2000],
                        'narrative_text': post_narrative[:1000],
                        'memory_status': 'READY',
                    }
                    if _insert_event(conn, post_event):
                        stats['events_upserted'] += 1
                        stats['sell_outcome_events'] += 1

        _maybe_push_sell_rag_threshold(conn, as_of, stats)

    logger.info(f'[strategy-memory] build stats: {stats}')
    return stats


if __name__ == '__main__':
    import argparse
    logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
    parser = argparse.ArgumentParser(description='Build Zhulong strategy memory events')
    parser.add_argument('--date', type=str, default=None)
    args = parser.parse_args()
    print(build_strategy_memory_events(args.date))
