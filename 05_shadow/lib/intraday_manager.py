#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
05_shadow/lib/intraday_manager.py
Paper-position intraday manager for Zhulong shadow trading.

Contract:
- Paper trading only; no broker integration.
- SELL requires a realtime quote. Daily fallback is observation-only.
- All decisions are non-blocking and auditable.
"""

from __future__ import annotations

import json
import logging
import requests
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

CURRENT_DIR = Path(__file__).resolve().parent
BASE_DIR = Path('/root/quant_project')
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from config.market_session import validate_realtime_quote_time

try:
    from .db_contract import DB_PATH, DBGateway
    from .engine import (
        PAPER_SOLD_STATUS,
        PAPER_INITIAL_STOP_MULTIPLIER,
        ShadowFill,
        calculate_shadow_fill,
        classify_position_strength,
        persist_fill,
        _ensure_paper_positions,
        _normalize_trade_date,
    )
    from .market_limits import fetch_limit_info, is_price_near_down_limit, limit_pct_for_symbol, previous_close
    from .shadow_config import quote_health_rules, sell_rules
    from .portfolio_metrics import rebuild_shadow_metrics
except Exception:
    from db_contract import DB_PATH, DBGateway
    from engine import (
        PAPER_SOLD_STATUS,
        PAPER_INITIAL_STOP_MULTIPLIER,
        ShadowFill,
        calculate_shadow_fill,
        classify_position_strength,
        persist_fill,
        _ensure_paper_positions,
        _normalize_trade_date,
    )
    from market_limits import fetch_limit_info, is_price_near_down_limit, limit_pct_for_symbol, previous_close
    from shadow_config import quote_health_rules, sell_rules
    from portfolio_metrics import rebuild_shadow_metrics

try:
    from config.settings import Config, ensure_syspath
    ensure_syspath()
except Exception:
    Config = None  # type: ignore

logger = logging.getLogger('shadow.intraday')

_SELL_RULES = sell_rules()
_QUOTE_HEALTH_RULES = quote_health_rules()

# Observation label threshold: marks a HOLD as risk-zone but does not sell by itself.
DANGER_LOSS_RATIO = float(_SELL_RULES.get('danger_loss_ratio', -0.05))
TAKE_PROFIT_WATCH_RATIO = 0.12
FIRST_TAKE_PROFIT_RATIO = float(_SELL_RULES.get('first_take_profit_ratio', 0.08))
SECOND_TAKE_PROFIT_RATIO = float(_SELL_RULES.get('second_take_profit_ratio', 0.12))
PROFIT_LOCK_FLOOR_RATIO = float(_SELL_RULES.get('profit_lock_floor_ratio', 0.08))
PROFIT_LOCK_TRIM_DRAWDOWN = float(_SELL_RULES.get('profit_lock_trim_drawdown', 0.035))
PROFIT_LOCK_CLEAR_DRAWDOWN = float(_SELL_RULES.get('profit_lock_clear_drawdown', 0.05))
PROFIT_LOCK_RUNNER_MAX_PCT = float(_SELL_RULES.get('profit_lock_runner_max_pct', 0.10))
# Execution threshold: wrong-pick stop line in the active sell plan.
WRONG_PICK_STOP_RATIO = float(_SELL_RULES.get('wrong_pick_stop_ratio', -0.05))
OPENING_EXTREME_STOP_RATIO = float(_SELL_RULES.get('opening_extreme_stop_ratio', -0.07))
STOP_LOSS_CONFIRM_MODE = str(_SELL_RULES.get('stop_loss_confirm_mode', 'intraday') or 'intraday').strip().lower()
RUNNER_TRAIL_DRAWDOWN = 0.06
TIME_EXIT_DAYS_NORMAL = 5
TIME_EXIT_DAYS_STRONG = 8
TIME_EXIT_MIN_GAIN = 0.05
PROFIT_LOCK_STAGES = {'PROFIT_LOCK', 'PROFIT_LOCK_TRIMMED'}
QUOTE_ALERT_AFTER_0930_FAILURES = int(_QUOTE_HEALTH_RULES.get('alert_after_0930_failures', 3) or 3)
QUOTE_RAW_SNAPSHOT_LIMIT = int(_QUOTE_HEALTH_RULES.get('raw_snapshot_limit', 3000) or 3000)


class ShadowIntradayManager:
    """Manage open paper positions during intraday daemon ticks."""

    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path

    def run_once(self, trade_date: str | None = None) -> Dict[str, int | str]:
        td = _normalize_trade_date(trade_date)
        now = datetime.now()
        check_key = now.strftime('%Y%m%d%H%M')
        stats: Dict[str, int | str] = {
            'trade_date': td,
            'hold_scanned': 0,
            'no_realtime': 0,
            'raised_stop': 0,
            'sold': 0,
            'partial_sold': 0,
            'held': 0,
            'skip_invalid': 0,
            'quote_alerts': 0,
        }

        sell_receipts: List[Tuple[ShadowFill, str, str, str, float, bool]] = []
        sell_pushes: List[Dict[str, object]] = []

        with DBGateway(self.db_path, read_only=False, logger=logger) as conn:
            _ensure_paper_positions(conn)
            self._ensure_decision_table(conn)
            self._ensure_quote_health_tables(conn)
            self._ensure_liquidation_event_table(conn)
            rows = conn.execute(
                """
                SELECT
                    symbol,
                    CAST(trade_date AS VARCHAR) AS entry_trade_date,
                    COALESCE(entry_price, 0) AS entry_price,
                    COALESCE(highest_price, entry_price, 0) AS highest_price,
                    COALESCE(dynamic_stop_price, 0) AS dynamic_stop_price,
                    COALESCE(qty, 0) AS qty,
                    COALESCE(signal_task_id, '') AS signal_task_id,
                    COALESCE(entry_score, 0) AS entry_score,
                    COALESCE(strategy_tag, '') AS strategy_tag,
                    COALESCE(initial_qty, qty, 0) AS initial_qty,
                    COALESCE(realized_qty, 0) AS realized_qty,
                    COALESCE(realized_amount, 0) AS realized_amount,
                    COALESCE(realized_gross_amount, realized_amount, 0) AS realized_gross_amount,
                    COALESCE(realized_net_amount, realized_amount, 0) AS realized_net_amount,
                    COALESCE(realized_commission, 0) AS realized_commission,
                    COALESCE(realized_stamp_tax, 0) AS realized_stamp_tax,
                    COALESCE(realized_transfer_fee, 0) AS realized_transfer_fee,
                    COALESCE(realized_tax_total, 0) AS realized_tax_total,
                    COALESCE(entry_total_cost, entry_price * COALESCE(NULLIF(initial_qty, 0), qty, 0), 0) AS entry_total_cost,
                    COALESCE(sell_stage, 'HOLD_FULL') AS sell_stage,
                    COALESCE(strength_tier, '') AS strength_tier
                FROM fact_paper_positions
                WHERE UPPER(COALESCE(status, '')) = 'HOLD'
                ORDER BY trade_date, symbol
                """
            ).fetchall()

        stats['hold_scanned'] = len(rows)

        def record_decision(
            symbol: str,
            entry_td: str,
            decision: str,
            reason: str,
            price: float,
            entry_price: float,
            highest_before: float,
            highest_after: float,
            stop_before: float,
            stop_after: float,
            qty: int,
            pnl_now: float,
            quote_source: str,
            signal_task_id: str,
            days_held: int = 0,
            max_gain_ratio: float = 0.0,
            drawdown_from_high: float = 0.0,
            risk_zone: str = 'NORMAL',
            action_intent: str = 'HOLD',
            sell_rule_id: str = '',
            strength_tier: str = '',
            qty_sold: int = 0,
            qty_after: int = 0,
            sell_price: float = 0.0,
        ) -> None:
            with DBGateway(self.db_path, read_only=False, logger=logger) as conn:
                self._record_decision(
                    conn, check_key, td, symbol, entry_td,
                    decision, reason, price, entry_price,
                    highest_before, highest_after, stop_before, stop_after,
                    qty, pnl_now, quote_source, signal_task_id,
                    days_held, max_gain_ratio, drawdown_from_high, risk_zone, action_intent,
                    sell_rule_id, strength_tier, qty_sold, qty_after, sell_price,
                )

        for row in rows:
            (
                symbol_raw,
                entry_td_raw,
                entry_raw,
                highest_raw,
                stop_raw,
                qty_raw,
                signal_task_raw,
                entry_score_raw,
                strategy_tag_raw,
                initial_qty_raw,
                realized_qty_raw,
                realized_amount_raw,
                realized_gross_amount_raw,
                realized_net_amount_raw,
                realized_commission_raw,
                realized_stamp_tax_raw,
                realized_transfer_fee_raw,
                realized_tax_total_raw,
                entry_total_cost_raw,
                sell_stage_raw,
                strength_tier_raw,
            ) = row
            symbol = str(symbol_raw or '').strip().upper()
            entry_td = _normalize_trade_date(entry_td_raw)
            entry_price = float(entry_raw or 0)
            highest_before = float(highest_raw or 0)
            stop_before = float(stop_raw or 0)
            qty = int(qty_raw or 0)
            signal_task_id = str(signal_task_raw or '').strip()
            strategy_tag = str(strategy_tag_raw or '').strip()
            initial_qty = int(initial_qty_raw or qty or 0)
            realized_qty = int(realized_qty_raw or 0)
            realized_amount = float(realized_amount_raw or 0)
            realized_gross_amount = float(realized_gross_amount_raw or realized_amount or 0)
            realized_net_amount = float(realized_net_amount_raw or realized_amount or 0)
            realized_commission = float(realized_commission_raw or 0)
            realized_stamp_tax = float(realized_stamp_tax_raw or 0)
            realized_transfer_fee = float(realized_transfer_fee_raw or 0)
            realized_tax_total = float(realized_tax_total_raw or 0)
            entry_total_cost = float(entry_total_cost_raw or (entry_price * initial_qty) or 0)
            sell_stage = str(sell_stage_raw or 'HOLD_FULL').strip().upper() or 'HOLD_FULL'
            strength_tier = str(strength_tier_raw or '').strip().upper()
            if strength_tier not in {'STRONG', 'NORMAL'}:
                strength_tier = classify_position_strength(float(entry_score_raw or 0), strategy_tag)

            if not symbol or entry_price <= 0 or qty <= 0:
                stats['skip_invalid'] += 1
                record_decision(
                    symbol or 'UNKNOWN', entry_td,
                    'SKIP_INVALID', 'invalid paper position', 0, entry_price,
                    highest_before, highest_before, stop_before, stop_before,
                    qty, 0.0, 'NONE', signal_task_id,
                )
                continue

            quote_event = self._fetch_realtime_quote_event(symbol)
            with DBGateway(self.db_path, read_only=False, logger=logger) as conn:
                self._record_quote_health(conn, check_key, td, symbol, entry_td, quote_event)

            if not quote_event.get('ok'):
                stats['no_realtime'] += 1
                if self._maybe_push_quote_alert(symbol, entry_td, td, quote_event, qty, entry_price, stop_before, now):
                    stats['quote_alerts'] += 1
                quote_status = str(quote_event.get('status') or 'QUOTE_UNAVAILABLE')
                record_decision(
                    symbol, entry_td,
                    'HOLD_NO_REALTIME', f'realtime quote unavailable: {quote_status}', 0, entry_price,
                    highest_before, highest_before, stop_before, stop_before,
                    qty, 0.0, str(quote_event.get('source') or 'NONE'), signal_task_id,
                )
                continue

            price = float(quote_event.get('price') or 0)
            quote_source = str(quote_event.get('source') or 'TUSHARE_REALTIME')
            if price <= 0:
                stats['skip_invalid'] += 1
                continue

            highest_after = max(highest_before, entry_price, price)
            stop_after = self._protection_line(entry_price, highest_after, stop_before, sell_stage)
            pnl_now = round((price - entry_price) / entry_price, 6)
            max_gain_ratio = self._ratio(highest_after, entry_price)
            drawdown_from_high = self._ratio(price, highest_after)
            days_held = self._trading_days_held(symbol, entry_td, td) or self._days_held(entry_td, td)
            risk_zone = self._risk_zone(pnl_now, max_gain_ratio, drawdown_from_high)
            action_intent = 'HOLD'
            decision = 'HOLD'
            reason = 'above protection line'
            sell_rule_id = ''
            sell_qty = 0
            sell_price = 0.0

            with DBGateway(self.db_path, read_only=False, logger=logger) as conn:
                plan = self._select_sell_plan(
                    conn=conn,
                    symbol=symbol,
                    trade_date=td,
                    position_trade_date=entry_td,
                    now=now,
                    price=price,
                    entry_price=entry_price,
                    highest_after=highest_after,
                    stop_after=stop_after,
                    qty=qty,
                    initial_qty=initial_qty,
                    sell_stage=sell_stage,
                    strength_tier=strength_tier,
                    pnl_now=pnl_now,
                    max_gain_ratio=max_gain_ratio,
                    drawdown_from_high=drawdown_from_high,
                    days_held=days_held,
                )
                stop_after = max(stop_after, float(plan.get('stop_after') or 0))

                if plan.get('decision') == 'LIMIT_UP_EXEMPT':
                    stats['held'] += 1
                    decision = 'LIMIT_UP_EXEMPT'
                    reason = str(plan.get('reason') or 'limit-up exemption')
                    action_intent = 'EXEMPT'
                    sell_rule_id = 'LIMIT_UP_EXEMPT'
                elif int(plan.get('qty_to_sell') or 0) > 0:
                    sell_qty = min(qty, int(plan.get('qty_to_sell') or 0))
                    blocked, block_reason = self._limit_down_sell_block(conn, symbol, td, price)
                    if blocked:
                        stats['held'] += 1
                        decision = 'LIMIT_DOWN_UNFILLED'
                        reason = block_reason[:300]
                        action_intent = 'UNFILLED'
                        sell_rule_id = 'LIMIT_DOWN_UNFILLED'
                        sell_qty = 0
                        sell_price = 0.0
                        qty_after = qty
                    else:
                        sell_rule_id = str(plan.get('rule_id') or 'SELL_RULE')[:64]
                        execution_profile = self._execution_profile(sell_rule_id, pnl_now, price, stop_after)
                        fill = calculate_shadow_fill(symbol, 'SELL', price, sell_qty, td, execution_profile=execution_profile)
                        sell_qty = int(fill.qty or sell_qty)
                        qty_after = max(0, qty - sell_qty)
                        final_sell = qty_after <= 0
                        reason = str(plan.get('reason') or sell_rule_id)[:260]
                        confirm_mode = str(plan.get('confirm_mode') or STOP_LOSS_CONFIRM_MODE or 'intraday')[:32]
                        reason = f'{reason} | confirm={confirm_mode}'[:300]
                        decision = sell_rule_id
                        action_intent = 'SELL'
                        sell_price = float(fill.price_shadow)
                        new_stage = str(plan.get('new_stage') or ('SOLD' if final_sell else sell_stage))[:32]
                        new_realized_qty = int(realized_qty + sell_qty)
                        new_realized_gross_amount = round(float(realized_gross_amount + fill.gross_amount), 2)
                        new_realized_amount = new_realized_gross_amount
                        new_realized_net_amount = round(float(realized_net_amount + fill.net_amount), 2)
                        new_realized_commission = round(float(realized_commission + fill.commission), 2)
                        new_realized_stamp_tax = round(float(realized_stamp_tax + fill.stamp_tax), 2)
                        new_realized_transfer_fee = round(float(realized_transfer_fee + fill.transfer_fee), 2)
                        new_realized_tax_total = round(float(realized_tax_total + fill.tax_total), 2)
                        entry_cost_sold = entry_total_cost * (new_realized_qty / max(1, initial_qty))
                        realized_net_pnl = round(new_realized_net_amount - entry_cost_sold, 2)
                        pnl_ratio = round(realized_net_pnl / entry_cost_sold, 6) if entry_cost_sold > 0 else 0.0
                        pnl_ratio_gross = round(((new_realized_gross_amount / max(1, new_realized_qty)) - entry_price) / entry_price, 6)
                        now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        trace_id = f'{signal_task_id or symbol}:{check_key}:{sell_rule_id}'
                        tag = (strategy_tag or 'PAPER_INTRADAY') + f'|{sell_rule_id}|profile={execution_profile}|confirm={confirm_mode}'
                        committed, liquidation_event = self._commit_sell_execution(
                            conn,
                            fill=fill,
                            trace_id=trace_id,
                            strategy_tag=tag,
                            trade_date=td,
                            position_values=[
                                highest_after,
                                stop_after,
                                qty_after,
                                new_realized_qty,
                                new_realized_amount,
                                new_realized_gross_amount,
                                new_realized_commission,
                                new_realized_stamp_tax,
                                new_realized_transfer_fee,
                                new_realized_tax_total,
                                new_realized_net_amount,
                                realized_net_pnl,
                                new_stage,
                                strength_tier,
                                sell_rule_id,
                                reason,
                                sell_price,
                                pnl_ratio,
                                pnl_ratio_gross,
                                PAPER_SOLD_STATUS if final_sell else 'HOLD',
                                final_sell,
                                td,
                                now_ts,
                                symbol,
                                entry_td,
                                sell_qty,
                            ],
                            liquidation={
                                'enabled': self._is_liquidation_rule(sell_rule_id),
                                'parent_key': f'{symbol}|{entry_td}|{sell_rule_id}',
                                'symbol': symbol,
                                'position_trade_date': entry_td,
                                'rule_id': sell_rule_id,
                                'execution_profile': execution_profile,
                                'initial_qty': qty,
                                'sell_qty': sell_qty,
                                'qty_after': qty_after,
                                'final_sell': final_sell,
                            },
                        )
                        if committed:
                            if final_sell:
                                stats['sold'] += 1
                            else:
                                stats['partial_sold'] += 1
                            sell_receipts.append((fill, trace_id, tag, symbol, pnl_ratio, final_sell))
                            sell_pushes.append({
                                'symbol': symbol,
                                'entry_price': entry_price,
                                'price': price,
                                'sell_price': sell_price,
                                'qty': sell_qty,
                                'qty_after': qty_after,
                                'pnl_ratio': pnl_ratio,
                                'stop': stop_after,
                                'trade_date': td,
                                'entry_trade_date': entry_td,
                                'quote_source': quote_source,
                                'score': float(entry_score_raw or 0),
                                'sell_rule_id': sell_rule_id,
                                'reason': reason,
                                'strength_tier': strength_tier,
                                'final_sell': final_sell,
                                'gross_amount': fill.gross_amount,
                                'commission': fill.commission,
                                'stamp_tax': fill.stamp_tax,
                                'transfer_fee': fill.transfer_fee,
                                'tax_total': fill.tax_total,
                                'net_amount': fill.net_amount,
                                'realized_net_pnl': realized_net_pnl,
                                'confirm_mode': confirm_mode,
                                'execution_profile': execution_profile,
                                'liquidation_event': liquidation_event,
                                'pricing_mode': fill.pricing_mode,
                                'capacity_capped': bool(getattr(fill.breakdown, 'is_capacity_capped', False)),
                                'slippage_cost': fill.slippage_cost,
                            })
                        else:
                            stats['skip_invalid'] += 1
                            decision = 'SELL_ATOMIC_COMMIT_FAILED'
                            reason = 'position and ledger transaction rolled back'
                            action_intent = 'HOLD'
                            sell_qty = 0
                else:
                    planned_decision = str(plan.get('decision') or '').strip()
                    if planned_decision and planned_decision != 'HOLD':
                        stats['held'] += 1
                        decision = planned_decision[:64]
                        reason = str(plan.get('reason') or planned_decision)[:300]
                        action_intent = 'WATCH_RISK' if planned_decision == 'STOP_LOSS_INTRADAY_WARN' else 'HOLD'
                        sell_rule_id = str(plan.get('rule_id') or planned_decision)[:64]
                    planned_stage = str(plan.get('new_stage') or '').strip().upper()
                    if price > highest_before or stop_after > stop_before or (planned_stage and planned_stage != sell_stage):
                        now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        next_stage = planned_stage or sell_stage
                        if max_gain_ratio >= SECOND_TAKE_PROFIT_RATIO and sell_stage in {'TAKE_1_DONE', 'CORE_PROFIT_LOCKED', 'RUNNER_LEFT'}:
                            next_stage = 'PROFIT_LOCK'
                        conn.execute(
                            """
                            UPDATE fact_paper_positions
                            SET highest_price = ?,
                                dynamic_stop_price = ?,
                                sell_stage = ?,
                                strength_tier = ?,
                                updated_at = CAST(? AS TIMESTAMP)
                            WHERE symbol = ?
                              AND CAST(trade_date AS DATE) = CAST(? AS DATE)
                              AND UPPER(COALESCE(status, '')) = 'HOLD'
                            """,
                            [highest_after, stop_after, next_stage, strength_tier, now_ts, symbol, entry_td],
                        )
                        stats['raised_stop'] += 1
                        decision = 'RAISE_STOP'
                        reason = 'protection line raised'
                        action_intent = 'RAISE_STOP'
                    if decision == 'HOLD':
                        stats['held'] += 1
                        if max_gain_ratio >= TAKE_PROFIT_WATCH_RATIO:
                            decision = 'TAKE_PROFIT_WATCH'
                            reason = 'strong unrealized gain; trailing stop active'
                            action_intent = 'WATCH_PROFIT'
                        elif risk_zone == 'DANGER':
                            decision = 'DANGER_HOLD'
                            reason = 'loss near risk boundary; waiting stop confirmation'
                            action_intent = 'WATCH_RISK'

                self._record_decision(
                    conn, check_key, td, symbol, entry_td,
                    decision, reason, price, entry_price,
                    highest_before, highest_after, stop_before, stop_after,
                    qty, pnl_now, quote_source, signal_task_id,
                    days_held, max_gain_ratio, drawdown_from_high, risk_zone, action_intent,
                    sell_rule_id, strength_tier, sell_qty, max(0, qty - sell_qty), sell_price,
                )

        for fill, _trace_id, _tag, _symbol, _pnl_ratio, final_sell in sell_receipts:
            self._update_shadow_metrics_sell(td, fill.net_amount, position_closed=final_sell)

        for payload in sell_pushes:
            if str(payload.get('liquidation_event') or ''):
                self._push_shadow_liquidation(payload)
            elif not self._is_liquidation_rule(str(payload.get('sell_rule_id') or '')):
                self._push_shadow_sell(payload)

        logger.info(f'[intraday-paper] summary: {stats}')
        return stats

    def _commit_sell_execution(
        self,
        conn,
        *,
        fill,
        trace_id: str,
        strategy_tag: str,
        trade_date: str,
        position_values,
        liquidation,
    ):
        """Atomically update one position and persist its SELL receipt."""
        conn.execute('BEGIN TRANSACTION')
        try:
            updated = conn.execute(
                """
                UPDATE fact_paper_positions
                SET highest_price = ?,
                    dynamic_stop_price = ?,
                    qty = ?,
                    realized_qty = ?,
                    realized_amount = ?,
                    realized_gross_amount = ?,
                    realized_commission = ?,
                    realized_stamp_tax = ?,
                    realized_transfer_fee = ?,
                    realized_tax_total = ?,
                    realized_net_amount = ?,
                    realized_net_pnl = ?,
                    sell_stage = ?,
                    strength_tier = ?,
                    last_sell_rule_id = ?,
                    last_sell_reason = ?,
                    exit_price = ?,
                    pnl_ratio = ?,
                    pnl_ratio_gross = ?,
                    status = ?,
                    exit_trade_date = CASE WHEN ? THEN CAST(? AS DATE) ELSE exit_trade_date END,
                    updated_at = CAST(? AS TIMESTAMP)
                WHERE symbol = ?
                  AND CAST(trade_date AS DATE) = CAST(? AS DATE)
                  AND UPPER(COALESCE(status, '')) = 'HOLD'
                  AND COALESCE(qty, 0) >= ?
                RETURNING symbol
                """,
                position_values,
            ).fetchone()
            if not updated:
                conn.execute('ROLLBACK')
                return False, ''

            if not persist_fill(
                fill,
                trace_id=trace_id,
                tide_mode='Golden',
                strategy_tag=strategy_tag,
                trade_date=trade_date,
                conn=conn,
            ):
                conn.execute('ROLLBACK')
                return False, ''

            liquidation_event = ''
            if liquidation.get('enabled'):
                liquidation_event = self._register_liquidation_event(
                    conn=conn,
                    parent_key=liquidation['parent_key'],
                    trade_date=trade_date,
                    symbol=liquidation['symbol'],
                    position_trade_date=liquidation['position_trade_date'],
                    rule_id=liquidation['rule_id'],
                    execution_profile=liquidation['execution_profile'],
                    initial_qty=liquidation['initial_qty'],
                    sell_qty=liquidation['sell_qty'],
                    qty_after=liquidation['qty_after'],
                    final_sell=liquidation['final_sell'],
                )

            conn.execute('COMMIT')
            return True, liquidation_event
        except Exception as exc:
            try:
                conn.execute('ROLLBACK')
            except Exception:
                pass
            logger.exception(
                '[intraday-paper] SELL transaction rolled back symbol=%s trace=%s: %s',
                getattr(fill, 'symbol', ''), trace_id, exc,
            )
            return False, ''

    def _ensure_decision_table(self, conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS fact_shadow_intraday_decisions (
                idempotency_key VARCHAR PRIMARY KEY,
                check_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                trade_date DATE,
                symbol VARCHAR,
                position_trade_date DATE,
                decision VARCHAR,
                reason VARCHAR,
                price DOUBLE,
                entry_price DOUBLE,
                highest_before DOUBLE,
                highest_after DOUBLE,
                stop_before DOUBLE,
                stop_after DOUBLE,
                qty INTEGER,
                pnl_ratio DOUBLE,
                quote_source VARCHAR,
                signal_task_id VARCHAR,
                days_held INTEGER DEFAULT 0,
                max_gain_ratio DOUBLE DEFAULT 0,
                drawdown_from_high DOUBLE DEFAULT 0,
                risk_zone VARCHAR DEFAULT 'NORMAL',
                action_intent VARCHAR DEFAULT 'HOLD',
                sell_rule_id VARCHAR DEFAULT '',
                strength_tier VARCHAR DEFAULT '',
                qty_sold INTEGER DEFAULT 0,
                qty_after INTEGER DEFAULT 0,
                sell_price DOUBLE DEFAULT 0
            )
            """
        )
        self._patch_decision_columns(conn)
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_shadow_intraday_symbol_time
            ON fact_shadow_intraday_decisions(symbol, check_time)
            """
        )

    def _record_decision(
        self,
        conn,
        check_key: str,
        trade_date: str,
        symbol: str,
        position_trade_date: str,
        decision: str,
        reason: str,
        price: float,
        entry_price: float,
        highest_before: float,
        highest_after: float,
        stop_before: float,
        stop_after: float,
        qty: int,
        pnl_ratio: float,
        quote_source: str,
        signal_task_id: str,
        days_held: int = 0,
        max_gain_ratio: float = 0.0,
        drawdown_from_high: float = 0.0,
        risk_zone: str = 'NORMAL',
        action_intent: str = 'HOLD',
        sell_rule_id: str = '',
        strength_tier: str = '',
        qty_sold: int = 0,
        qty_after: int = 0,
        sell_price: float = 0.0,
    ) -> None:
        key = f'{check_key}|{symbol}|{position_trade_date}'
        now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn.execute(
            """
            INSERT INTO fact_shadow_intraday_decisions (
                idempotency_key, check_time, trade_date, symbol, position_trade_date,
                decision, reason, price, entry_price, highest_before, highest_after,
                stop_before, stop_after, qty, pnl_ratio, quote_source, signal_task_id,
                days_held, max_gain_ratio, drawdown_from_high, risk_zone, action_intent,
                sell_rule_id, strength_tier, qty_sold, qty_after, sell_price
            )
            VALUES (?, CAST(? AS TIMESTAMP), CAST(? AS DATE), ?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (idempotency_key) DO UPDATE SET
                check_time = EXCLUDED.check_time,
                decision = EXCLUDED.decision,
                reason = EXCLUDED.reason,
                price = EXCLUDED.price,
                highest_after = EXCLUDED.highest_after,
                stop_after = EXCLUDED.stop_after,
                pnl_ratio = EXCLUDED.pnl_ratio,
                quote_source = EXCLUDED.quote_source,
                days_held = EXCLUDED.days_held,
                max_gain_ratio = EXCLUDED.max_gain_ratio,
                drawdown_from_high = EXCLUDED.drawdown_from_high,
                risk_zone = EXCLUDED.risk_zone,
                action_intent = EXCLUDED.action_intent,
                sell_rule_id = EXCLUDED.sell_rule_id,
                strength_tier = EXCLUDED.strength_tier,
                qty_sold = EXCLUDED.qty_sold,
                qty_after = EXCLUDED.qty_after,
                sell_price = EXCLUDED.sell_price
            """,
            [
                key,
                now_ts,
                trade_date,
                symbol,
                position_trade_date,
                decision,
                reason[:300],
                float(price or 0),
                float(entry_price or 0),
                float(highest_before or 0),
                float(highest_after or 0),
                float(stop_before or 0),
                float(stop_after or 0),
                int(qty or 0),
                float(pnl_ratio or 0),
                str(quote_source or '')[:64],
                str(signal_task_id or '')[:128],
                int(days_held or 0),
                float(max_gain_ratio or 0),
                float(drawdown_from_high or 0),
                str(risk_zone or 'NORMAL')[:32],
                str(action_intent or 'HOLD')[:32],
                str(sell_rule_id or '')[:64],
                str(strength_tier or '')[:16],
                int(qty_sold or 0),
                int(qty_after or 0),
                float(sell_price or 0),
            ],
        )

    def _patch_decision_columns(self, conn) -> None:
        rows = conn.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'fact_shadow_intraday_decisions'
            """
        ).fetchall()
        existing = {str(r[0]).lower() for r in rows}
        required = {
            'days_held': 'INTEGER DEFAULT 0',
            'max_gain_ratio': 'DOUBLE DEFAULT 0',
            'drawdown_from_high': 'DOUBLE DEFAULT 0',
            'risk_zone': "VARCHAR DEFAULT 'NORMAL'",
            'action_intent': "VARCHAR DEFAULT 'HOLD'",
            'sell_rule_id': "VARCHAR DEFAULT ''",
            'strength_tier': "VARCHAR DEFAULT ''",
            'qty_sold': 'INTEGER DEFAULT 0',
            'qty_after': 'INTEGER DEFAULT 0',
            'sell_price': 'DOUBLE DEFAULT 0',
        }
        for col, ddl in required.items():
            if col not in existing:
                conn.execute(f'ALTER TABLE fact_shadow_intraday_decisions ADD COLUMN {col} {ddl}')
                logger.info(f'[intraday-paper] patch decision schema add column: {col}')

    def _ensure_quote_health_tables(self, conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS fact_shadow_quote_health (
                idempotency_key VARCHAR PRIMARY KEY,
                event_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                trade_date DATE,
                symbol VARCHAR,
                position_trade_date DATE,
                source VARCHAR DEFAULT '',
                status VARCHAR DEFAULT '',
                status_code VARCHAR DEFAULT '',
                price DOUBLE DEFAULT 0,
                reason VARCHAR DEFAULT '',
                elapsed_ms DOUBLE DEFAULT 0,
                raw_snapshot_json VARCHAR DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_shadow_quote_health_symbol_time
            ON fact_shadow_quote_health(symbol, event_time)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS fact_shadow_quote_alerts (
                alert_key VARCHAR PRIMARY KEY,
                alert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                trade_date DATE,
                symbol VARCHAR,
                position_trade_date DATE,
                alert_type VARCHAR,
                fail_count INTEGER DEFAULT 0,
                latest_status VARCHAR DEFAULT '',
                latest_reason VARCHAR DEFAULT '',
                pushed INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def _ensure_liquidation_event_table(self, conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS fact_shadow_liquidation_events (
                parent_key VARCHAR PRIMARY KEY,
                trade_date DATE,
                symbol VARCHAR,
                position_trade_date DATE,
                rule_id VARCHAR DEFAULT '',
                execution_profile VARCHAR DEFAULT '',
                status VARCHAR DEFAULT '',
                initial_qty INTEGER DEFAULT 0,
                sold_qty INTEGER DEFAULT 0,
                qty_after INTEGER DEFAULT 0,
                child_count INTEGER DEFAULT 0,
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    @staticmethod
    def _is_liquidation_rule(rule_id: str) -> bool:
        return str(rule_id or '').upper() in {
            'WRONG_PICK_STOP',
            'TRAILING_RUNNER_STOP',
            'TIME_EFFICIENCY_EXIT',
            'PROFIT_LOCK_CLEAR',
            'PROFIT_LOCK_FLOOR_EXIT',
            'PROFIT_LOCK_GAP_EXIT',
            'PROFIT_LOCK_TRIM_SMALL_CLEAR',
            'TWO_LIMIT_UP_FULL_EXIT',
            'THESIS_CRITICAL_RISK_EXIT',
        }

    @staticmethod
    def _execution_profile(rule_id: str, pnl_now: float, price: float, stop_after: float) -> str:
        rule = str(rule_id or '').upper()
        if rule.startswith('TAKE_PROFIT') or rule in {'PROFIT_LOCK_TRIM', 'TWO_LIMIT_UP_FULL_EXIT'}:
            return 'PROFIT_TAKE'
        if rule in {
            'WRONG_PICK_STOP',
            'TRAILING_RUNNER_STOP',
            'TIME_EFFICIENCY_EXIT',
            'PROFIT_LOCK_CLEAR',
            'PROFIT_LOCK_FLOOR_EXIT',
            'PROFIT_LOCK_GAP_EXIT',
            'PROFIT_LOCK_TRIM_SMALL_CLEAR',
            'THESIS_CRITICAL_RISK_EXIT',
        }:
            if pnl_now <= OPENING_EXTREME_STOP_RATIO:
                return 'PANIC_STOP'
            if stop_after > 0 and price <= stop_after * 0.98:
                return 'PANIC_STOP'
            return 'PROTECTIVE_STOP'
        return 'STANDARD'

    @staticmethod
    def _latest_critical_thesis_exit(conn, symbol: str, position_trade_date: str, trade_date: str):
        try:
            exists = conn.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_name='fact_shadow_thesis_reviews' LIMIT 1"
            ).fetchone()
            if not exists:
                return None
            row = conn.execute(
                """
                SELECT CAST(trade_date AS VARCHAR), thesis_state, management_action,
                       thesis_reason, COALESCE(observer_only, TRUE)
                FROM fact_shadow_thesis_reviews
                WHERE symbol = ?
                  AND CAST(position_trade_date AS DATE) = CAST(? AS DATE)
                  AND CAST(trade_date AS DATE) < CAST(? AS DATE)
                ORDER BY trade_date DESC, updated_at DESC
                LIMIT 1
                """,
                [symbol, position_trade_date, trade_date],
            ).fetchone()
            if not row:
                return None
            review_date, state, action, reason, _observer_only = row
            if (
                str(state or '').upper() == 'THESIS_INVALIDATED'
                and str(action or '').upper() == 'EXIT_NEXT_SESSION'
                and str(reason or '').lower() == 'critical_news_or_regulatory_risk'
            ):
                return {
                    'review_date': str(review_date or '')[:10],
                    'thesis_state': str(state),
                    'management_action': str(action),
                    'thesis_reason': str(reason),
                }
        except Exception as exc:
            logger.warning('[intraday-paper] thesis exit review unavailable %s: %s', symbol, exc)
        return None

    def _register_liquidation_event(
        self,
        conn,
        *,
        parent_key: str,
        trade_date: str,
        symbol: str,
        position_trade_date: str,
        rule_id: str,
        execution_profile: str,
        initial_qty: int,
        sell_qty: int,
        qty_after: int,
        final_sell: bool,
    ) -> str:
        self._ensure_liquidation_event_table(conn)
        now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        existing = conn.execute(
            "SELECT status FROM fact_shadow_liquidation_events WHERE parent_key = ?",
            [parent_key],
        ).fetchone()
        status = 'COMPLETED' if final_sell else 'IN_PROGRESS'
        if existing is None:
            conn.execute(
                """
                INSERT INTO fact_shadow_liquidation_events (
                    parent_key, trade_date, symbol, position_trade_date, rule_id,
                    execution_profile, status, initial_qty, sold_qty, qty_after,
                    child_count, started_at, completed_at, updated_at
                )
                VALUES (?, CAST(? AS DATE), ?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, 1,
                        CAST(? AS TIMESTAMP), CASE WHEN ? THEN CAST(? AS TIMESTAMP) ELSE NULL END, CAST(? AS TIMESTAMP))
                """,
                [
                    parent_key,
                    trade_date,
                    symbol,
                    position_trade_date,
                    rule_id,
                    execution_profile,
                    status,
                    int(initial_qty or 0),
                    int(sell_qty or 0),
                    int(qty_after or 0),
                    now_ts,
                    final_sell,
                    now_ts,
                    now_ts,
                ],
            )
            return 'COMPLETED' if final_sell else 'STARTED'

        prev_status = str(existing[0] or '').upper()
        conn.execute(
            """
            UPDATE fact_shadow_liquidation_events
            SET status = ?,
                execution_profile = ?,
                sold_qty = COALESCE(sold_qty, 0) + ?,
                qty_after = ?,
                child_count = COALESCE(child_count, 0) + 1,
                completed_at = CASE WHEN ? THEN CAST(? AS TIMESTAMP) ELSE completed_at END,
                updated_at = CAST(? AS TIMESTAMP)
            WHERE parent_key = ?
            """,
            [
                status,
                execution_profile,
                int(sell_qty or 0),
                int(qty_after or 0),
                final_sell,
                now_ts,
                now_ts,
                parent_key,
            ],
        )
        if final_sell and prev_status != 'COMPLETED':
            return 'COMPLETED'
        return ''

    def _record_quote_health(
        self,
        conn,
        check_key: str,
        trade_date: str,
        symbol: str,
        position_trade_date: str,
        quote_event: Dict[str, object],
    ) -> None:
        key = f'{check_key}|{symbol}|{position_trade_date}|QUOTE'
        now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn.execute(
            """
            INSERT INTO fact_shadow_quote_health (
                idempotency_key, event_time, trade_date, symbol, position_trade_date,
                source, status, status_code, price, reason, elapsed_ms,
                raw_snapshot_json, created_at
            )
            VALUES (?, CAST(? AS TIMESTAMP), CAST(? AS DATE), ?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, CAST(? AS TIMESTAMP))
            ON CONFLICT (idempotency_key) DO UPDATE SET
                event_time = EXCLUDED.event_time,
                source = EXCLUDED.source,
                status = EXCLUDED.status,
                status_code = EXCLUDED.status_code,
                price = EXCLUDED.price,
                reason = EXCLUDED.reason,
                elapsed_ms = EXCLUDED.elapsed_ms,
                raw_snapshot_json = EXCLUDED.raw_snapshot_json
            """,
            [
                key,
                now_ts,
                trade_date,
                symbol,
                position_trade_date,
                str(quote_event.get('source') or '')[:64],
                str(quote_event.get('status') or '')[:64],
                str(quote_event.get('status_code') or '')[:32],
                float(quote_event.get('price') or 0),
                str(quote_event.get('reason') or '')[:300],
                float(quote_event.get('elapsed_ms') or 0),
                str(quote_event.get('raw_snapshot_json') or '')[:QUOTE_RAW_SNAPSHOT_LIMIT],
                now_ts,
            ],
        )

    def _maybe_push_quote_alert(
        self,
        symbol: str,
        position_trade_date: str,
        trade_date: str,
        quote_event: Dict[str, object],
        qty: int,
        entry_price: float,
        stop_price: float,
        now: datetime,
    ) -> bool:
        if now.hour * 100 + now.minute < 930:
            return False
        try:
            alert_key = ''
            fail_count = 0
            latest_status = str(quote_event.get('status') or 'QUOTE_UNAVAILABLE')[:64]
            latest_reason = str(quote_event.get('reason') or '')[:300]
            with DBGateway(self.db_path, read_only=False, logger=logger) as conn:
                self._ensure_quote_health_tables(conn)
                start_ts = f'{trade_date} 09:30:00'
                row = conn.execute(
                    """
                    SELECT MAX(event_time)
                    FROM fact_shadow_quote_health
                    WHERE trade_date = CAST(? AS DATE)
                      AND symbol = ?
                      AND position_trade_date = CAST(? AS DATE)
                      AND event_time >= CAST(? AS TIMESTAMP)
                      AND status = 'QUOTE_OK'
                    """,
                    [trade_date, symbol, position_trade_date, start_ts],
                ).fetchone()
                last_ok = row[0] if row and row[0] else None
                window_start = str(last_ok) if last_ok else start_ts
                fail_row = conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM fact_shadow_quote_health
                    WHERE trade_date = CAST(? AS DATE)
                      AND symbol = ?
                      AND position_trade_date = CAST(? AS DATE)
                      AND event_time >= CAST(? AS TIMESTAMP)
                      AND status <> 'QUOTE_OK'
                    """,
                    [trade_date, symbol, position_trade_date, window_start],
                ).fetchone()
                fail_count = int(fail_row[0] or 0) if fail_row else 0
                if fail_count < max(1, QUOTE_ALERT_AFTER_0930_FAILURES):
                    return False

                alert_type = 'QUOTE_MISSING_INTRADAY'
                alert_key = f'{trade_date}|{symbol}|{position_trade_date}|{alert_type}'
                exists = conn.execute(
                    "SELECT 1 FROM fact_shadow_quote_alerts WHERE alert_key = ?",
                    [alert_key],
                ).fetchone()
                if exists:
                    return False

                now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                conn.execute(
                    """
                    INSERT INTO fact_shadow_quote_alerts (
                        alert_key, alert_time, trade_date, symbol, position_trade_date,
                        alert_type, fail_count, latest_status, latest_reason, pushed, created_at
                    )
                    VALUES (?, CAST(? AS TIMESTAMP), CAST(? AS DATE), ?, CAST(? AS DATE), ?, ?, ?, ?, 0, CAST(? AS TIMESTAMP))
                    """,
                    [
                        alert_key,
                        now_ts,
                        trade_date,
                        symbol,
                        position_trade_date,
                        alert_type,
                        fail_count,
                        latest_status,
                        latest_reason,
                        now_ts,
                    ],
                )
            pushed = self._push_quote_alert(
                symbol=symbol,
                trade_date=trade_date,
                position_trade_date=position_trade_date,
                fail_count=fail_count,
                latest_status=latest_status,
                latest_reason=latest_reason,
                qty=qty,
                entry_price=entry_price,
                stop_price=stop_price,
            )
            if alert_key:
                with DBGateway(self.db_path, read_only=False, logger=logger) as conn:
                    self._ensure_quote_health_tables(conn)
                    conn.execute(
                        "UPDATE fact_shadow_quote_alerts SET pushed = ? WHERE alert_key = ?",
                        [1 if pushed else 0, alert_key],
                    )
            return pushed
        except Exception as exc:
            logger.warning(f'[intraday-paper] quote alert failed: {symbol} | {exc}')
            return False

    @staticmethod
    def _ratio(numerator: float, denominator: float) -> float:
        if denominator <= 0:
            return 0.0
        return round((float(numerator or 0) - denominator) / denominator, 6)

    @staticmethod
    def _round_lot(qty: float) -> int:
        return max(0, int(float(qty or 0) // 100) * 100)

    def _sell_qty_to_target(self, qty: int, target_remaining: int) -> int:
        sell_qty = max(0, int(qty or 0) - max(0, int(target_remaining or 0)))
        sell_qty = self._round_lot(sell_qty)
        if sell_qty <= 0 and qty >= 100 and target_remaining < qty:
            sell_qty = min(qty, 100)
        return min(qty, sell_qty)

    def _first_take_qty(self, qty: int, initial_qty: int, strength_tier: str) -> int:
        base = max(int(initial_qty or 0), int(qty or 0))
        ratio = 1 / 3 if strength_tier == 'STRONG' else 1 / 2
        return min(qty, max(0, self._round_lot(base * ratio)))

    def _runner_target_qty(self, initial_qty: int, qty: int) -> int:
        base = max(int(initial_qty or 0), int(qty or 0))
        target = self._round_lot(base * max(0.0, min(PROFIT_LOCK_RUNNER_MAX_PCT, 1.0)))
        return min(qty, target)

    def _runner_trim_qty(self, qty: int) -> Tuple[int, bool]:
        qty_i = max(0, int(qty or 0))
        if qty_i <= 0:
            return 0, False
        if qty_i <= 100:
            return qty_i, True
        trim = self._round_lot(qty_i / 2)
        if trim <= 0:
            return qty_i, True
        return min(qty_i, trim), False

    @staticmethod
    def _profit_floor_price(entry_price: float) -> float:
        if entry_price <= 0:
            return 0.0
        return round(entry_price * (1 + PROFIT_LOCK_FLOOR_RATIO), 4)

    def _protection_line(self, entry_price: float, highest_price: float, stop_before: float, sell_stage: str) -> float:
        stage = str(sell_stage or '').upper()
        if stage in PROFIT_LOCK_STAGES:
            return round(max(self._profit_floor_price(entry_price), stop_before or 0), 4)
        if stage in {'RUNNER_LEFT', 'CORE_PROFIT_LOCKED'}:
            return round(max(entry_price, highest_price * (1 - RUNNER_TRAIL_DRAWDOWN), stop_before or 0), 4)
        if stage in {'TAKE_1_DONE', 'TAKE_PROFIT_8_NORMAL', 'TAKE_PROFIT_8_STRONG'}:
            return round(max(entry_price, stop_before or 0), 4)
        return round(max(entry_price * PAPER_INITIAL_STOP_MULTIPLIER, stop_before or 0), 4)

    @staticmethod
    def _is_opening_window(now: datetime) -> bool:
        hhmm = now.hour * 100 + now.minute
        return 930 <= hhmm < 950

    @staticmethod
    def _is_close_confirm_window(now: datetime) -> bool:
        hhmm = now.hour * 100 + now.minute
        return hhmm >= 1445

    def _needs_close_confirm(self, now: datetime, rule_id: str) -> bool:
        if STOP_LOSS_CONFIRM_MODE != 'close_confirm':
            return False
        if self._is_close_confirm_window(now):
            return False
        return str(rule_id or '').upper() in {'WRONG_PICK_STOP', 'TRAILING_RUNNER_STOP'}

    def _trading_days_held(self, symbol: str, entry_trade_date: str, trade_date: str) -> int:
        try:
            with DBGateway(self.db_path, read_only=True, logger=logger) as conn:
                row = conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM fact_daily
                    WHERE symbol = ?
                      AND CAST(trade_date AS DATE) > CAST(? AS DATE)
                      AND CAST(trade_date AS DATE) <= CAST(? AS DATE)
                    """,
                    [symbol, entry_trade_date, trade_date],
                ).fetchone()
            return int(row[0] or 0) if row else 0
        except Exception:
            return 0

    def _previous_close(self, conn, symbol: str, trade_date: str) -> float:
        try:
            row = conn.execute(
                """
                SELECT close
                FROM fact_daily
                WHERE symbol = ?
                  AND CAST(trade_date AS DATE) < CAST(? AS DATE)
                  AND COALESCE(close, 0) > 0
                ORDER BY trade_date DESC
                LIMIT 1
                """,
                [symbol, trade_date],
            ).fetchone()
            return float(row[0] or 0) if row else 0.0
        except Exception:
            return 0.0

    def _is_limit_up_like(self, conn, symbol: str, trade_date: str, price: float) -> bool:
        prev_close = self._previous_close(conn, symbol, trade_date)
        if prev_close <= 0 or price <= 0:
            return False
        pct = price / prev_close - 1
        sym = str(symbol or '').upper()
        if sym.endswith('.BJ') or sym.startswith(('8', '9')):
            threshold = 0.29
        elif sym.startswith(('300', '301', '688')):
            threshold = 0.195
        else:
            threshold = 0.098
        return pct >= threshold

    def _current_limit_up_streak(self, conn, symbol: str, trade_date: str, price: float) -> int:
        if not self._is_limit_up_like(conn, symbol, trade_date, price):
            return 0
        threshold = limit_pct_for_symbol(symbol) * 0.98
        streak = 1
        try:
            rows = conn.execute(
                """
                SELECT COALESCE(close, 0), COALESCE(pre_close, 0), COALESCE(pct_chg, 0)
                FROM fact_daily
                WHERE symbol = ?
                  AND CAST(trade_date AS DATE) < CAST(? AS DATE)
                  AND COALESCE(close, 0) > 0
                ORDER BY trade_date DESC
                LIMIT 10
                """,
                [symbol, trade_date],
            ).fetchall()
            for close_raw, pre_raw, pct_raw in rows:
                close = float(close_raw or 0)
                pre_close = float(pre_raw or 0)
                pct = (close / pre_close - 1.0) if pre_close > 0 else float(pct_raw or 0) / 100.0
                if pct >= threshold:
                    streak += 1
                else:
                    break
        except Exception as exc:
            logger.debug('[intraday-paper] limit-up streak unavailable: %s | %s', symbol, exc)
        return streak

    def _load_runner_context(self, conn, symbol: str, trade_date: str, position_trade_date: str) -> Dict[str, object]:
        try:
            row = conn.execute(
                """
                SELECT COALESCE(runner_gate, 'DENY'),
                       COALESCE(runner_gate_reason, ''),
                       COALESCE(context_quality, 'UNAVAILABLE'),
                       COALESCE(market_gate, 'UNKNOWN'),
                       COALESCE(news_risk_level, 'UNAVAILABLE'),
                       COALESCE(regulatory_risk_level, 'UNAVAILABLE'),
                       COALESCE(liquidity_ok, FALSE),
                       COALESCE(volume_expansion_ok, FALSE),
                       COALESCE(limit_up_streak, 0),
                       CAST(trade_date AS VARCHAR)
                FROM fact_shadow_position_contexts
                WHERE symbol = ?
                  AND CAST(position_trade_date AS DATE) = CAST(? AS DATE)
                  AND CAST(trade_date AS DATE) < CAST(? AS DATE)
                ORDER BY trade_date DESC
                LIMIT 1
                """,
                [symbol, position_trade_date, trade_date],
            ).fetchone()
        except Exception as exc:
            logger.debug('[intraday-paper] runner context unavailable: %s | %s', symbol, exc)
            row = None
        if not row:
            return {
                'runner_gate': 'DENY',
                'runner_gate_reason': 'context_missing',
                'context_quality': 'UNAVAILABLE',
                'context_trade_date': '',
            }
        return {
            'runner_gate': str(row[0] or 'DENY').upper(),
            'runner_gate_reason': str(row[1] or ''),
            'context_quality': str(row[2] or 'UNAVAILABLE'),
            'market_gate': str(row[3] or 'UNKNOWN'),
            'news_risk_level': str(row[4] or 'UNAVAILABLE'),
            'regulatory_risk_level': str(row[5] or 'UNAVAILABLE'),
            'liquidity_ok': bool(row[6]),
            'volume_expansion_ok': bool(row[7]),
            'limit_up_streak': int(row[8] or 0),
            'context_trade_date': str(row[9] or '')[:10],
        }

    @staticmethod
    def _runner_context_allows(context: Dict[str, object]) -> bool:
        return str((context or {}).get('runner_gate') or '').upper() == 'ALLOW'

    def _fetch_realtime_ohlc(self, symbol: str) -> Dict[str, float]:
        try:
            import tushare as ts
            df = ts.realtime_quote(ts_code=symbol)
            if df is None or len(df) <= 0:
                return {}
            row = df.iloc[0]
            return {
                'open': self._first_float(row, ['OPEN', 'open']),
                'high': self._first_float(row, ['HIGH', 'high']),
                'low': self._first_float(row, ['LOW', 'low']),
                'price': self._first_float(row, ['PRICE', 'price', 'LAST', 'last']),
            }
        except Exception as exc:
            logger.debug(f'[intraday-paper] realtime ohlc failed: {symbol} | {exc}')
            return {}

    def _limit_down_sell_block(self, conn, symbol: str, trade_date: str, price: float) -> Tuple[bool, str]:
        pre_close = previous_close(conn, symbol, trade_date)
        if pre_close <= 0:
            return False, ''
        limit_info = fetch_limit_info(symbol, trade_date, pre_close)
        down_limit = float(limit_info.down_limit or 0)
        if not is_price_near_down_limit(price, down_limit):
            return False, ''
        eps = max(0.01, down_limit * 0.001)
        try:
            row = conn.execute(
                """
                SELECT open, high, low, close
                FROM fact_daily
                WHERE symbol = ?
                  AND CAST(trade_date AS DATE) = CAST(? AS DATE)
                LIMIT 1
                """,
                [symbol, trade_date],
            ).fetchone()
            if row:
                open_px, high_px, low_px, close_px = [float(v or 0) for v in row]
                if max(open_px, high_px, close_px) > down_limit + eps:
                    return False, ''
                if all(abs(v - down_limit) <= eps for v in (open_px, high_px, low_px, close_px) if v > 0):
                    return True, f'limit-down one-price locked: price={price:.4f} down_limit={down_limit:.4f} source={limit_info.source}'
        except Exception as exc:
            logger.debug(f'[intraday-paper] daily limit-down evidence failed: {symbol} | {exc}')

        snap = self._fetch_realtime_ohlc(symbol)
        if snap and max(float(snap.get('open') or 0), float(snap.get('high') or 0), float(snap.get('price') or 0)) > down_limit + eps:
            return False, ''
        return True, f'limit-down sell deferred: no opened evidence price={price:.4f} down_limit={down_limit:.4f} source={limit_info.source}'

    def _select_sell_plan(
        self,
        *,
        conn,
        symbol: str,
        trade_date: str,
        position_trade_date: str,
        now: datetime,
        price: float,
        entry_price: float,
        highest_after: float,
        stop_after: float,
        qty: int,
        initial_qty: int,
        sell_stage: str,
        strength_tier: str,
        pnl_now: float,
        max_gain_ratio: float,
        drawdown_from_high: float,
        days_held: int,
    ) -> Dict[str, object]:
        stage = str(sell_stage or 'HOLD_FULL').upper()
        tier = str(strength_tier or 'NORMAL').upper()
        opening = self._is_opening_window(now)
        thesis_exit = self._latest_critical_thesis_exit(
            conn, symbol, position_trade_date, trade_date
        ) if conn is not None else None
        if thesis_exit:
            return {
                'decision': 'THESIS_CRITICAL_RISK_EXIT',
                'rule_id': 'THESIS_CRITICAL_RISK_EXIT',
                'reason': (
                    f"review_date={thesis_exit['review_date']} "
                    f"reason={thesis_exit['thesis_reason']}"
                ),
                'qty_to_sell': qty,
                'stop_after': stop_after,
                'new_stage': 'SOLD',
                'confirm_mode': 'prior_eod_thesis_review',
            }
        limit_up_streak = self._current_limit_up_streak(conn, symbol, trade_date, price) if conn is not None else 0
        if limit_up_streak >= 2 and pnl_now >= FIRST_TAKE_PROFIT_RATIO:
            return {
                'decision': 'TWO_LIMIT_UP_FULL_EXIT',
                'rule_id': 'TWO_LIMIT_UP_FULL_EXIT',
                'reason': f'limit-up streak={limit_up_streak}; default full exit after two limit-up days',
                'qty_to_sell': qty,
                'stop_after': max(stop_after, self._profit_floor_price(entry_price)),
                'new_stage': 'SOLD',
                'confirm_mode': 'intraday',
            }

        if opening and pnl_now > OPENING_EXTREME_STOP_RATIO and pnl_now < FIRST_TAKE_PROFIT_RATIO:
            return {
                'decision': 'OPENING_WAIT',
                'rule_id': 'OPENING_WAIT',
                'reason': 'opening wait window',
                'qty_to_sell': 0,
                'stop_after': stop_after,
                'new_stage': stage,
            }

        if pnl_now <= (OPENING_EXTREME_STOP_RATIO if opening else WRONG_PICK_STOP_RATIO):
            if self._needs_close_confirm(now, 'WRONG_PICK_STOP'):
                return {
                    'decision': 'STOP_LOSS_INTRADAY_WARN',
                    'rule_id': 'STOP_LOSS_INTRADAY_WARN',
                    'reason': f'pnl {pnl_now:.2%} touched wrong-pick stop; close_confirm pending',
                    'qty_to_sell': 0,
                    'stop_after': stop_after,
                    'new_stage': stage,
                    'confirm_mode': STOP_LOSS_CONFIRM_MODE,
                }
            return {
                'decision': 'WRONG_PICK_STOP',
                'rule_id': 'WRONG_PICK_STOP',
                'reason': f'pnl {pnl_now:.2%} breached wrong-pick stop',
                'qty_to_sell': qty,
                'stop_after': stop_after,
                'new_stage': 'SOLD',
                'confirm_mode': STOP_LOSS_CONFIRM_MODE,
            }

        if stage in (PROFIT_LOCK_STAGES | {'RUNNER_LEFT', 'CORE_PROFIT_LOCKED'}):
            lock_stop = round(max(self._profit_floor_price(entry_price), stop_after), 4)
            if opening and price <= lock_stop:
                return {
                    'decision': 'PROFIT_LOCK_GAP_EXIT',
                    'rule_id': 'PROFIT_LOCK_GAP_EXIT',
                    'reason': f'opening price {price:.4f} below profit-lock floor {lock_stop:.4f}',
                    'qty_to_sell': qty,
                    'stop_after': lock_stop,
                    'new_stage': 'SOLD',
                    'confirm_mode': 'intraday',
                }
            if price <= lock_stop:
                return {
                    'decision': 'PROFIT_LOCK_FLOOR_EXIT',
                    'rule_id': 'PROFIT_LOCK_FLOOR_EXIT',
                    'reason': f'price {price:.4f} <= profit-lock floor {lock_stop:.4f}',
                    'qty_to_sell': qty,
                    'stop_after': lock_stop,
                    'new_stage': 'SOLD',
                    'confirm_mode': 'intraday',
                }
            if drawdown_from_high <= -PROFIT_LOCK_CLEAR_DRAWDOWN:
                return {
                    'decision': 'PROFIT_LOCK_CLEAR',
                    'rule_id': 'PROFIT_LOCK_CLEAR',
                    'reason': f'drawdown {drawdown_from_high:.2%} breached profit-lock clear line',
                    'qty_to_sell': qty,
                    'stop_after': lock_stop,
                    'new_stage': 'SOLD',
                    'confirm_mode': 'intraday',
                }
            if stage != 'PROFIT_LOCK_TRIMMED' and drawdown_from_high <= -PROFIT_LOCK_TRIM_DRAWDOWN:
                trim_qty, clear_all = self._runner_trim_qty(qty)
                if trim_qty > 0:
                    rule_id = 'PROFIT_LOCK_TRIM_SMALL_CLEAR' if clear_all else 'PROFIT_LOCK_TRIM'
                    return {
                        'decision': rule_id,
                        'rule_id': rule_id,
                        'reason': f'drawdown {drawdown_from_high:.2%} reached profit-lock trim line',
                        'qty_to_sell': trim_qty,
                        'stop_after': lock_stop,
                        'new_stage': 'SOLD' if clear_all else 'PROFIT_LOCK_TRIMMED',
                        'confirm_mode': 'intraday',
                    }
            stop_after = lock_stop
            if stage in {'RUNNER_LEFT', 'CORE_PROFIT_LOCKED'}:
                return {
                    'decision': 'PROFIT_LOCK_ARMED',
                    'rule_id': 'PROFIT_LOCK_ARMED',
                    'reason': 'legacy runner migrated to profit-lock floor',
                    'qty_to_sell': 0,
                    'stop_after': lock_stop,
                    'new_stage': 'PROFIT_LOCK',
                }

        if max_gain_ratio >= SECOND_TAKE_PROFIT_RATIO and stage not in (PROFIT_LOCK_STAGES | {'RUNNER_LEFT', 'CORE_PROFIT_LOCKED'}):
            lock_stop = round(max(self._profit_floor_price(entry_price), stop_after), 4)
            if tier != 'STRONG':
                return {
                    'decision': 'TAKE_PROFIT_12_FULL',
                    'rule_id': 'TAKE_PROFIT_12_FULL',
                    'reason': f'max gain {max_gain_ratio:.2%} reached 12% target; NORMAL tier exits runner',
                    'qty_to_sell': qty,
                    'stop_after': lock_stop,
                    'new_stage': 'SOLD',
                    'confirm_mode': 'intraday',
                }
            runner_context = (
                self._load_runner_context(conn, symbol, trade_date, position_trade_date)
                if conn is not None else
                {'runner_gate': 'DENY', 'runner_gate_reason': 'context_missing'}
            )
            if not self._runner_context_allows(runner_context):
                reason = str(runner_context.get('runner_gate_reason') or 'context_denied')
                context_date = str(runner_context.get('context_trade_date') or '')
                suffix = f' context_date={context_date}' if context_date else ''
                return {
                    'decision': 'TAKE_PROFIT_12_FULL',
                    'rule_id': 'TAKE_PROFIT_12_FULL',
                    'reason': f'max gain {max_gain_ratio:.2%} reached 12% target; runner denied by EOD context: {reason}{suffix}',
                    'qty_to_sell': qty,
                    'stop_after': lock_stop,
                    'new_stage': 'SOLD',
                    'confirm_mode': 'intraday',
                }
            target = self._runner_target_qty(initial_qty, qty)
            if target <= 0:
                return {
                    'decision': 'TAKE_PROFIT_12_FULL',
                    'rule_id': 'TAKE_PROFIT_12_FULL',
                    'reason': f'max gain {max_gain_ratio:.2%} reached 12% target; runner target rounds to zero',
                    'qty_to_sell': qty,
                    'stop_after': lock_stop,
                    'new_stage': 'SOLD',
                    'confirm_mode': 'intraday',
                }
            qty_to_sell = self._sell_qty_to_target(qty, target)
            if qty_to_sell > 0:
                return {
                    'decision': 'TAKE_PROFIT_12_LOCK',
                    'rule_id': 'TAKE_PROFIT_12_LOCK',
                    'reason': f'max gain {max_gain_ratio:.2%} reached 12% target; STRONG runner capped at {PROFIT_LOCK_RUNNER_MAX_PCT:.0%}',
                    'qty_to_sell': qty_to_sell,
                    'stop_after': lock_stop,
                    'new_stage': 'PROFIT_LOCK',
                    'confirm_mode': 'intraday',
                }
            return {
                'decision': 'PROFIT_LOCK_ARMED',
                'rule_id': 'PROFIT_LOCK_ARMED',
                'reason': f'max gain {max_gain_ratio:.2%} reached 12% target; runner already within cap',
                'qty_to_sell': 0,
                'stop_after': lock_stop,
                'new_stage': 'PROFIT_LOCK',
            }

        if pnl_now >= FIRST_TAKE_PROFIT_RATIO and stage == 'HOLD_FULL':
            qty_to_sell = self._first_take_qty(qty, initial_qty, tier)
            rule_id = 'TAKE_PROFIT_8_STRONG' if tier == 'STRONG' else 'TAKE_PROFIT_8_NORMAL'
            if qty_to_sell > 0:
                return {
                    'decision': rule_id,
                    'rule_id': rule_id,
                    'reason': f'pnl {pnl_now:.2%} reached first take-profit',
                    'qty_to_sell': qty_to_sell,
                    'stop_after': round(max(entry_price, stop_after), 4),
                    'new_stage': 'TAKE_1_DONE',
                }

        time_limit = TIME_EXIT_DAYS_STRONG if tier == 'STRONG' else TIME_EXIT_DAYS_NORMAL
        if days_held >= time_limit and max_gain_ratio < TIME_EXIT_MIN_GAIN:
            return {
                'decision': 'TIME_EFFICIENCY_EXIT',
                'rule_id': 'TIME_EFFICIENCY_EXIT',
                'reason': f'{days_held} trading days held, max gain {max_gain_ratio:.2%} below 5%',
                'qty_to_sell': qty,
                'stop_after': stop_after,
                'new_stage': 'SOLD',
            }

        if price <= stop_after:
            rule_id = 'TRAILING_RUNNER_STOP' if stage != 'HOLD_FULL' else 'WRONG_PICK_STOP'
            if self._needs_close_confirm(now, rule_id):
                return {
                    'decision': 'STOP_LOSS_INTRADAY_WARN',
                    'rule_id': 'STOP_LOSS_INTRADAY_WARN',
                    'reason': f'price {price:.4f} touched protection line {stop_after:.4f}; close_confirm pending',
                    'qty_to_sell': 0,
                    'stop_after': stop_after,
                    'new_stage': stage,
                    'confirm_mode': STOP_LOSS_CONFIRM_MODE,
                }
            return {
                'decision': rule_id,
                'rule_id': rule_id,
                'reason': f'price {price:.4f} <= protection line {stop_after:.4f}',
                'qty_to_sell': qty,
                'stop_after': stop_after,
                'new_stage': 'SOLD',
                'confirm_mode': STOP_LOSS_CONFIRM_MODE,
            }

        return {
            'decision': 'HOLD',
            'rule_id': '',
            'reason': 'hold above protection line',
            'qty_to_sell': 0,
            'stop_after': stop_after,
            'new_stage': stage,
        }

    @staticmethod
    def _days_held(entry_trade_date: str, trade_date: str) -> int:
        try:
            start = datetime.strptime(str(entry_trade_date)[:10], '%Y-%m-%d').date()
            end = datetime.strptime(str(trade_date)[:10], '%Y-%m-%d').date()
            return max(0, (end - start).days)
        except Exception:
            return 0

    @staticmethod
    def _risk_zone(pnl_ratio: float, max_gain_ratio: float, drawdown_from_high: float) -> str:
        if pnl_ratio <= DANGER_LOSS_RATIO:
            return 'DANGER'
        if max_gain_ratio >= TAKE_PROFIT_WATCH_RATIO:
            return 'PROFIT_WATCH'
        if drawdown_from_high <= -0.04:
            return 'PULLBACK'
        return 'NORMAL'

    def _fetch_realtime_quote(self, symbol: str) -> Optional[Tuple[float, str]]:
        event = self._fetch_realtime_quote_event(symbol)
        if event.get('ok'):
            return float(event.get('price') or 0), str(event.get('source') or 'TUSHARE_REALTIME')
        return None

    def _fetch_realtime_quote_event(self, symbol: str) -> Dict[str, object]:
        started = time.perf_counter()
        event: Dict[str, object] = {
            'ok': False,
            'source': 'TUSHARE_REALTIME',
            'status': 'QUOTE_UNAVAILABLE',
            'status_code': 'N/A',
            'price': 0.0,
            'reason': '',
            'elapsed_ms': 0.0,
            'raw_snapshot_json': '',
        }
        try:
            import tushare as ts
            df = ts.realtime_quote(ts_code=symbol)
            event['raw_snapshot_json'] = self._snapshot_realtime_response(df)
            if df is None:
                event['status'] = 'QUOTE_EMPTY_RESPONSE'
                event['reason'] = 'tushare returned None'
            elif len(df) <= 0:
                event['status'] = 'QUOTE_EMPTY_RESPONSE'
                event['reason'] = 'tushare returned empty DataFrame'
            else:
                row = df.iloc[0]
                price = self._first_float(row, ['PRICE', 'price', 'LAST', 'last'])
                quote_date = row.get('DATE', row.get('date', ''))
                quote_time = row.get('TIME', row.get('time', ''))
                fresh, status, reason, age_seconds = validate_realtime_quote_time(
                    quote_date,
                    quote_time,
                )
                if not fresh:
                    event['status'] = status
                    event['reason'] = reason
                elif price and price > 0:
                    event['ok'] = True
                    event['status'] = 'QUOTE_OK'
                    event['price'] = price
                    event['reason'] = f'price parsed; quote age {max(0.0, age_seconds or 0):.0f}s'
                else:
                    event['status'] = 'QUOTE_MISSING_PRICE'
                    event['reason'] = 'no positive PRICE/LAST field in tushare response'
        except Exception as exc:
            event['status'] = 'QUOTE_API_EXCEPTION'
            event['reason'] = f'{type(exc).__name__}: {exc}'[:300]
            event['raw_snapshot_json'] = json.dumps(
                {'kind': 'exception', 'exception_type': type(exc).__name__, 'message': str(exc)[:1000]},
                ensure_ascii=False,
            )[:QUOTE_RAW_SNAPSHOT_LIMIT]
            logger.warning(f'[intraday-paper] realtime quote failed: {symbol} | {exc}')
        finally:
            event['elapsed_ms'] = round((time.perf_counter() - started) * 1000, 2)
        return event

    @staticmethod
    def _json_scalar(value):
        try:
            if hasattr(value, 'item'):
                value = value.item()
        except Exception:
            pass
        if value is None:
            return None
        if isinstance(value, (str, int, float, bool)):
            return value
        return str(value)

    def _snapshot_realtime_response(self, df) -> str:
        try:
            if df is None:
                payload = {'kind': 'None', 'rows': 0, 'columns': []}
            else:
                columns = [str(c) for c in list(getattr(df, 'columns', []))]
                payload = {
                    'kind': type(df).__name__,
                    'rows': int(len(df)),
                    'columns': columns,
                }
                if len(df) > 0:
                    row = df.iloc[0]
                    payload['first_row'] = {str(k): self._json_scalar(row.get(k)) for k in columns}
            return json.dumps(payload, ensure_ascii=False, default=str)[:QUOTE_RAW_SNAPSHOT_LIMIT]
        except Exception as exc:
            return json.dumps(
                {'kind': 'snapshot_error', 'exception_type': type(exc).__name__, 'message': str(exc)[:1000]},
                ensure_ascii=False,
            )[:QUOTE_RAW_SNAPSHOT_LIMIT]

    @staticmethod
    def _first_float(row, keys: List[str]) -> float:
        for key in keys:
            try:
                val = row.get(key, None)
                if val is None or val == '':
                    continue
                num = float(val)
                if num > 0:
                    return num
            except Exception:
                continue
        return 0.0

    def _update_shadow_metrics_sell(self, trade_date: str, proceeds: float, position_closed: bool = True) -> None:
        try:
            with DBGateway(self.db_path, read_only=False, logger=logger) as conn:
                rebuild_shadow_metrics(conn, through_date=trade_date)
        except Exception as exc:
            logger.warning(f'[intraday-paper] metrics sell update failed: {exc}')

    def _lookup_stock_name(self, symbol: str) -> str:
        sym = str(symbol or '').strip().upper()
        if not sym:
            return ''
        try:
            with DBGateway(self.db_path, read_only=True, logger=logger) as conn:
                row = conn.execute(
                    "SELECT name FROM fact_stock_basic WHERE symbol = ? LIMIT 1",
                    [sym],
                ).fetchone()
            if row and row[0]:
                return str(row[0]).strip()
        except Exception as exc:
            logger.debug(f'[intraday-paper] stock name lookup failed: {sym} | {exc}')
        return sym

    def _push_quote_alert(
        self,
        *,
        symbol: str,
        trade_date: str,
        position_trade_date: str,
        fail_count: int,
        latest_status: str,
        latest_reason: str,
        qty: int,
        entry_price: float,
        stop_price: float,
    ) -> bool:
        if Config is None:
            return False
        token = str(getattr(Config, 'PUSHPLUS_TOKEN', '') or '').strip()
        if not token:
            logger.warning('[intraday-paper] quote alert skipped: PUSHPLUS_TOKEN empty')
            return False
        name = self._lookup_stock_name(symbol)
        display = f'{name} {symbol}' if name and name != symbol else symbol
        title = f'持仓风控行情缺失 | {display}'
        content = '\n'.join([
            '[持仓风控行情缺失]',
            f'标的：{display}',
            f'交易日：{trade_date}',
            f'建仓日：{position_trade_date}',
            f'连续缺价次数：{fail_count}',
            f'最新状态：{self._quote_status_text(latest_status)}',
            f'原因：{self._quote_reason_text(latest_reason)}',
            f'持仓数量：{int(qty or 0):,} 股',
            f'建仓价：{float(entry_price or 0):.4f}',
            f'保护线：{float(stop_price or 0):.4f}',
            '',
            '处理：本次不使用可疑价格触发卖出；系统已落档原始行情快照，等待后续可信报价或盘后复盘标记。',
        ])
        try:
            resp = requests.post(
                str(getattr(Config, 'PUSHPLUS_URL', 'https://www.pushplus.plus/send')),
                json={'token': token, 'title': title, 'content': content[:2000], 'template': 'txt'},
                timeout=int(getattr(Config, 'PUSHPLUS_TIMEOUT', 10) or 10),
            )
            ok = resp.status_code == 200 and resp.json().get('code') == 200
            if ok:
                logger.info(f'[intraday-paper] quote alert push sent: {symbol}')
                return True
            logger.warning(f'[intraday-paper] quote alert push failed: {symbol} | {resp.text[:300]}')
        except Exception as exc:
            logger.warning(f'[intraday-paper] quote alert push exception: {symbol} | {exc}')
        return False

    def _push_shadow_liquidation(self, payload: Dict[str, object]) -> bool:
        if Config is None:
            return False
        token = str(getattr(Config, 'PUSHPLUS_TOKEN', '') or '').strip()
        if not token:
            return False
        symbol = str(payload.get('symbol') or '').strip().upper()
        name = self._lookup_stock_name(symbol)
        display = f'{name} {symbol}' if name and name != symbol else symbol
        event = str(payload.get('liquidation_event') or '').upper()
        event_text = '清算完成' if event == 'COMPLETED' else '清算开始'
        rule_id = str(payload.get('sell_rule_id') or '').strip() or 'SELL_RULE'
        qty_after = int(payload.get('qty_after', 0) or 0)
        action_text = self._sell_action_text(rule_id)
        execution_text = self._execution_text(rule_id)
        title = f'烛龙模拟盘{event_text} | {display}'
        pnl = float(payload.get('pnl_ratio') or 0)
        content = '\n'.join([
            f'[模拟盘{event_text}]',
            f'标的：{display}',
            f'交易日：{payload.get("trade_date", "")}',
            f'建仓日：{payload.get("entry_trade_date", "")}',
            f'触发动作：{action_text}',
            f'执行口径：{execution_text}',
            f'本次卖出：{int(payload.get("qty", 0) or 0):,} 股',
            f'剩余数量：{qty_after:,} 股',
            f'实时价：{float(payload.get("price") or 0):.4f}',
            f'模拟成交价：{float(payload.get("sell_price") or 0):.4f}',
            f'保护线：{float(payload.get("stop") or 0):.4f}',
            f'净盈亏比例：{pnl:.2%}',
            f'净盈亏：{float(payload.get("realized_net_pnl") or 0):,.2f}',
            f'税费合计：{float(payload.get("tax_total") or 0):,.2f}',
            f'冲击成本：{float(payload.get("slippage_cost") or 0):,.2f}',
            f'流动性限制：{"有，已按可成交量收缩" if payload.get("capacity_capped") else "无"}',
            f'行情来源：{self._quote_source_text(str(payload.get("quote_source") or ""))}',
            '',
            '说明：这是影子模拟盘的纪律执行记录，不代表真实交易指令。',
        ])
        try:
            resp = requests.post(
                str(getattr(Config, 'PUSHPLUS_URL', 'https://www.pushplus.plus/send')),
                json={'token': token, 'title': title, 'content': content[:2000], 'template': 'txt'},
                timeout=int(getattr(Config, 'PUSHPLUS_TIMEOUT', 10) or 10),
            )
            ok = resp.status_code == 200 and resp.json().get('code') == 200
            if ok:
                logger.info(f'[intraday-paper] shadow liquidation push sent: {symbol} {event}')
                return True
            logger.warning(f'[intraday-paper] shadow liquidation push failed: {symbol} | {resp.text[:300]}')
        except Exception as exc:
            logger.warning(f'[intraday-paper] shadow liquidation push exception: {symbol} | {exc}')
        return False

    def _push_shadow_sell(self, payload: Dict[str, object]) -> bool:
        if Config is None:
            return False
        token = str(getattr(Config, 'PUSHPLUS_TOKEN', '') or '').strip()
        if not token:
            return False
        symbol = str(payload.get('symbol') or '').strip().upper()
        name = self._lookup_stock_name(symbol)
        display = f'{name} {symbol}' if name and name != symbol else symbol
        title = f'\u70db\u9f99\u6a21\u62df\u76d8\u5356\u51fa | {display}'
        pnl = float(payload.get('pnl_ratio') or 0)
        rule_id = str(payload.get('sell_rule_id') or '').strip() or 'SELL_RULE'
        strength_tier = str(payload.get('strength_tier') or 'NORMAL').strip().upper()
        final_sell = bool(payload.get('final_sell'))
        qty_after = int(payload.get('qty_after', 0) or 0)
        stage_text = '\u6e05\u4ed3' if final_sell else '\u5206\u6279\u5356\u51fa'
        tier_text = '\u5f3a\u52bf\u7968' if strength_tier == 'STRONG' else '\u666e\u901a\u7968'
        action_text = self._sell_action_text(rule_id)
        reason_text = self._push_reason_text(rule_id, str(payload.get('reason') or ''))
        content = '\n'.join([
            '[\u6a21\u62df\u76d8\u5356\u51fa]',
            f'\u6807\u7684\uff1a{display}',
            f'\u89e6\u53d1\u4ea4\u6613\u65e5\uff1a{payload.get("trade_date", "")}',
            f'\u5efa\u4ed3\u4ea4\u6613\u65e5\uff1a{payload.get("entry_trade_date", "")}',
            f'\u5356\u51fa\u7c7b\u578b\uff1a{stage_text}',
            f'\u6301\u4ed3\u5206\u7c7b\uff1a{tier_text}',
            f'\u89e6\u53d1\u52a8\u4f5c\uff1a{action_text}',
            f'\u539f\u56e0\uff1a{reason_text}',
            f'\u5356\u51fa\u6570\u91cf\uff1a{int(payload.get("qty", 0) or 0):,} \u80a1',
            f'\u5269\u4f59\u6570\u91cf\uff1a{qty_after:,} \u80a1',
            f'\u5efa\u4ed3\u4ef7\uff1a{float(payload.get("entry_price") or 0):.4f}',
            f'\u5b9e\u65f6\u4ef7\uff1a{float(payload.get("price") or 0):.4f}',
            f'\u6a21\u62df\u5356\u51fa\u4ef7\uff1a{float(payload.get("sell_price") or 0):.4f}',
            f'\u5356\u51fa\u6210\u4ea4\u989d\uff1a{float(payload.get("gross_amount") or 0):,.2f}',
            f'\u4f63\u91d1\uff1a{float(payload.get("commission") or 0):,.2f}',
            f'\u5370\u82b1\u7a0e\uff1a{float(payload.get("stamp_tax") or 0):,.2f}',
            f'\u8fc7\u6237\u8d39\uff1a{float(payload.get("transfer_fee") or 0):,.2f}',
            f'\u7a0e\u8d39\u5408\u8ba1\uff1a{float(payload.get("tax_total") or 0):,.2f}',
            f'\u5356\u51fa\u51c0\u5165\u8d26\uff1a{float(payload.get("net_amount") or 0):,.2f}',
            f'\u5f53\u524d\u4fdd\u62a4\u7ebf\uff1a{float(payload.get("stop") or 0):.4f}',
            f'\u76c8\u4e8f\u6bd4\u4f8b\uff1a{pnl:.2%}',
            f'\u51c0\u76c8\u4e8f\uff1a{float(payload.get("realized_net_pnl") or 0):,.2f}',
            f'\u884c\u60c5\u6765\u6e90\uff1a{self._quote_source_text(str(payload.get("quote_source") or ""))}',
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
                logger.info(f'[intraday-paper] shadow SELL push sent: {symbol}')
                return True
            logger.warning(f'[intraday-paper] shadow SELL push failed: {symbol} | {resp.text[:300]}')
        except Exception as exc:
            logger.warning(f'[intraday-paper] shadow SELL push exception: {symbol} | {exc}')
        return False

    @staticmethod
    def _sell_action_text(rule_id: str) -> str:
        rule = str(rule_id or '').upper()
        mapping = {
            'TAKE_PROFIT_8_NORMAL': '达到第一止盈线，普通持仓先卖出一半',
            'TAKE_PROFIT_8_STRONG': '达到第一止盈线，强势持仓先兑现一部分',
            'TAKE_PROFIT_12': '达到第二止盈线，收缩到观察仓',
            'TAKE_PROFIT_12_FULL': '达到第二止盈线，落袋清仓',
            'TAKE_PROFIT_12_LOCK': '达到第二止盈线，仅保留小观察仓',
            'PROFIT_LOCK_TRIM': '利润保护触发，观察仓先减半',
            'PROFIT_LOCK_CLEAR': '利润保护触发，观察仓清仓',
            'PROFIT_LOCK_FLOOR_EXIT': '跌回利润保护线，观察仓清仓',
            'PROFIT_LOCK_GAP_EXIT': '开盘低于利润保护线，优先退出',
            'PROFIT_LOCK_TRIM_SMALL_CLEAR': '观察仓过小，利润保护触发后直接清仓',
            'TWO_LIMIT_UP_FULL_EXIT': '连续涨停后按纪律全部落袋',
            'WRONG_PICK_STOP': '跌破防守线，止损退出',
            'TRAILING_RUNNER_STOP': '跌破移动保护线，退出剩余仓位',
            'TIME_EFFICIENCY_EXIT': '持仓时间过长且利润不足，退出',
            'LIMIT_DOWN_UNFILLED': '接近跌停，暂无法按计划卖出',
            'THESIS_CRITICAL_RISK_EXIT': '盘后确认重大新闻或监管风险，次日退出',
        }
        return mapping.get(rule, '触发卖出纪律')

    @staticmethod
    def _execution_text(rule_id: str) -> str:
        rule = str(rule_id or '').upper()
        if rule.startswith('TAKE_PROFIT') or rule in {'PROFIT_LOCK_TRIM', 'TWO_LIMIT_UP_FULL_EXIT'}:
            return '主动止盈，按较温和的成交影响估算'
        if rule.startswith('PROFIT_LOCK'):
            return '利润保护，按防守性成交影响估算'
        if rule in {'WRONG_PICK_STOP', 'TRAILING_RUNNER_STOP', 'TIME_EFFICIENCY_EXIT'}:
            return '风险退出，按防守性成交影响估算'
        if rule == 'THESIS_CRITICAL_RISK_EXIT':
            return '重大信息风险退出，按防守性成交影响估算'
        return '常规影子成交估算'

    @staticmethod
    def _quote_source_text(source: str) -> str:
        src = str(source or '').upper()
        if 'TUSHARE' in src and 'REALTIME' in src:
            return 'Tushare 实时行情'
        if 'DAILY' in src:
            return '日线行情'
        if not src:
            return '未记录'
        return '行情接口'

    @staticmethod
    def _quote_status_text(status: str) -> str:
        raw = str(status or '').upper()
        mapping = {
            'QUOTE_UNAVAILABLE': '未取得可用实时价',
            'QUOTE_EMPTY_RESPONSE': '行情接口返回为空',
            'QUOTE_INVALID_PRICE': '行情价格无效',
            'QUOTE_TIME_INVALID': '行情时间不可信',
            'QUOTE_STALE': '行情时间滞后',
            'QUOTE_OK': '行情正常',
        }
        return mapping.get(raw, '行情暂不可用')

    @staticmethod
    def _quote_reason_text(reason: str) -> str:
        raw = str(reason or '').strip()
        if not raw:
            return '未返回可用实时价'
        lowered = raw.lower()
        if 'empty' in lowered or 'none' in lowered:
            return '行情接口没有返回可用数据'
        if 'invalid' in lowered:
            return '行情价格或时间校验未通过'
        if 'timeout' in lowered:
            return '行情接口响应超时'
        return raw[:120]

    @classmethod
    def _push_reason_text(cls, rule_id: str, raw_reason: str) -> str:
        rule = str(rule_id or '').upper()
        raw = str(raw_reason or '')
        if 'runner denied by EOD context' in raw:
            return '盘后条件未全部满足，不保留高风险观察仓'
        if 'NORMAL tier exits runner' in raw:
            return '普通持仓达到第二止盈线，按纪律不保留观察仓'
        if 'STRONG runner capped' in raw:
            return '强势持仓达到第二止盈线，仅保留最多 10% 观察仓'
        if 'limit-up streak' in raw:
            return '已达到连续涨停落袋条件'
        if 'drawdown' in raw and 'trim' in raw:
            return '从高点回撤达到减仓线，先收回部分利润'
        if 'drawdown' in raw:
            return '从高点回撤达到清仓线，保护已实现利润'
        if 'profit-lock floor' in raw:
            return '价格触及利润保护线，退出剩余仓位'
        if rule:
            return cls._sell_action_text(rule)
        return '触发影子盘卖出纪律'


if __name__ == '__main__':
    import argparse
    logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
    parser = argparse.ArgumentParser(description='Zhulong shadow intraday paper-position manager')
    parser.add_argument('--date', type=str, default=None)
    args = parser.parse_args()
    print(ShadowIntradayManager().run_once(args.date))
