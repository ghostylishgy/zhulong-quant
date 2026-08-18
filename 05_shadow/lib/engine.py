#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
05_shadow/lib/engine.py
Shadow slippage engine with DBGateway contract.
"""

from __future__ import annotations

import json
import logging
import os
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple

try:
    from .db_contract import DB_PATH, DBGateway
    from .costs import calculate_trade_cost
    from .shadow_config import load_shadow_rules
except Exception:
    from db_contract import DB_PATH, DBGateway
    from costs import calculate_trade_cost
    from shadow_config import load_shadow_rules

logger = logging.getLogger('shadow.engine')

BASE_DIR = Path('/root/quant_project')
RULES_PATH = BASE_DIR / '05_shadow' / 'config' / 'rules.yaml'

FALLBACK_SLIPPAGE_PCT = 0.003
PAPER_DYNAMIC_STOP_MULTIPLIER = 0.92
PAPER_INITIAL_STOP_MULTIPLIER = 0.95
PAPER_HOLD_STATUS = 'HOLD'
PAPER_SOLD_STATUS = 'SOLD'
PAPER_STRENGTH_STRONG_SCORE = 70.0
FORCE_LIQUIDATION_PENALTY_PCT = 0.015
FORCE_LIQUIDATION_GLOBAL_KEY_SUFFIX = 'MELTDOWN_ALL'
FORCE_LIQUIDATION_SINGLE_KEY_SUFFIX = 'FORCE_SELL'


@dataclass
class SlippageBreakdown:
    vol_part_pct: float = 0.0
    size_part_pct: float = 0.0
    total_pct: float = 0.0
    is_fallback: bool = False
    is_capacity_capped: bool = False
    original_qty: int = 0
    adjusted_qty: int = 0
    sigma: float = 0.0
    adv20: float = 0.0
    avg_vol_5d: float = 0.0


@dataclass
class ShadowFill:
    symbol: str
    action: str
    price_logical: float
    price_shadow: float
    qty: int
    slippage_cost: float
    breakdown: SlippageBreakdown
    pricing_mode: str = 'DYNAMIC'
    timestamp: str = ''
    gross_amount: float = 0.0
    commission: float = 0.0
    stamp_tax: float = 0.0
    transfer_fee: float = 0.0
    tax_total: float = 0.0
    net_amount: float = 0.0

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if self.gross_amount <= 0 and self.price_shadow > 0 and self.qty > 0:
            cost = calculate_trade_cost(self.action, price=self.price_shadow, qty=self.qty)
            self.gross_amount = cost.gross_amount
            self.commission = cost.commission
            self.stamp_tax = cost.stamp_tax
            self.transfer_fee = cost.transfer_fee
            self.tax_total = cost.tax_total
            self.net_amount = cost.net_amount


def _load_rules() -> dict:
    return load_shadow_rules()


def _round_sell_lot(qty: int, original_qty: int) -> int:
    qty_i = max(0, int(qty or 0))
    original_i = max(0, int(original_qty or 0))
    if qty_i >= original_i:
        return original_i
    rounded = (qty_i // 100) * 100
    if rounded <= 0 and qty_i > 0 and original_i >= 100:
        rounded = min(100, original_i)
    return min(original_i, rounded)


def classify_position_strength(entry_score: float = 0.0, strategy_tag: str = '') -> str:
    """Lock a simple strength tier at entry; later logic may only downgrade it."""
    tag = str(strategy_tag or '').upper()
    risk_terms = ('DISTRIBUTION', 'EXHAUST', 'VETO', 'RETREAT', 'DIVERGENCE', '出货', '退潮', '透支', '分歧')
    if any(term in tag for term in risk_terms):
        return 'NORMAL'
    return 'STRONG' if float(entry_score or 0) >= PAPER_STRENGTH_STRONG_SCORE else 'NORMAL'


def _fetch_volatility_data(symbol: str) -> Tuple[float, float, float]:
    """
    Pull sigma and volume metrics from fact_daily.
    Returns: (sigma_5d, adv20, avg_vol_5d)
    """
    with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
        sigma_row = conn.execute(
            """
            WITH recent AS (
                SELECT pct_chg
                FROM fact_daily
                WHERE symbol = ? AND close > 0
                ORDER BY trade_date DESC
                LIMIT 5
            )
            SELECT STDDEV_SAMP(pct_chg) AS sigma,
                   COUNT(*) AS cnt
            FROM recent
            """,
            [symbol],
        ).fetchone()

        vol_row = conn.execute(
            """
            WITH vol_window AS (
                SELECT vol,
                       ROW_NUMBER() OVER (ORDER BY trade_date DESC) AS rn
                FROM fact_daily
                WHERE symbol = ? AND vol > 0
                ORDER BY trade_date DESC
                LIMIT 20
            )
            SELECT
                AVG(vol) AS adv20,
                AVG(CASE WHEN rn <= 5 THEN vol END) AS avg_vol_5d,
                COUNT(*) AS cnt
            FROM vol_window
            """,
            [symbol],
        ).fetchone()

        if not sigma_row or not vol_row or sigma_row[1] < 3 or vol_row[2] < 5:
            raise ValueError(
                f'{symbol} history not enough: sigma_days={sigma_row[1] if sigma_row else 0}, '
                f'vol_days={vol_row[2] if vol_row else 0}'
            )

        sigma = float(sigma_row[0] or 0) / 100.0
        adv20 = float(vol_row[0] or 1)
        avg_vol_5d = float(vol_row[1] or 1)
        return sigma, adv20, avg_vol_5d


def calculate_shadow_fill(
    symbol: str,
    action: str,
    price_logical: float,
    qty: int,
    trade_date: str | None = None,
    execution_profile: str = 'STANDARD',
) -> ShadowFill:
    rules = _load_rules()
    alpha = float(rules.get('slippage', {}).get('alpha', 0.1))
    beta = float(rules.get('slippage', {}).get('beta', 0.05))
    liq_rules = dict(rules.get('liquidation') or {})
    profile = str(execution_profile or 'STANDARD').upper()

    breakdown = SlippageBreakdown(original_qty=qty, adjusted_qty=qty)
    pricing_mode = 'DYNAMIC'

    try:
        sigma, adv20, avg_vol_5d = _fetch_volatility_data(symbol)
        breakdown.sigma = sigma
        breakdown.adv20 = adv20
        breakdown.avg_vol_5d = avg_vol_5d

        participation = 0.10
        impact_multiplier = 1.0
        if action.upper() == 'SELL':
            if profile == 'PANIC_STOP':
                participation = float(liq_rules.get('panic_stop_participation', 0.50) or 0.50)
                impact_multiplier = float(liq_rules.get('panic_impact_multiplier', 2.5) or 2.5)
                pricing_mode = 'PANIC_STOP'
            elif profile == 'PROTECTIVE_STOP':
                participation = float(liq_rules.get('protective_stop_participation', 0.30) or 0.30)
                impact_multiplier = float(liq_rules.get('protective_impact_multiplier', 1.5) or 1.5)
                pricing_mode = 'PROTECTIVE_STOP'
            elif profile == 'PROFIT_TAKE':
                participation = float(liq_rules.get('profit_take_participation', 0.10) or 0.10)
                pricing_mode = 'PROFIT_TAKE'
        participation = max(0.01, min(1.0, participation))
        small_full_qty = int(liq_rules.get('small_order_full_qty', 5000) or 5000)

        capacity_limit = int(avg_vol_5d * participation)
        if action.upper() == 'SELL' and profile in {'PROTECTIVE_STOP', 'PANIC_STOP'} and qty <= small_full_qty:
            capacity_limit = qty
        if qty > capacity_limit and capacity_limit > 0:
            logger.warning(
                f'[capacity] {symbol} {profile} qty {qty} > {participation:.0%} x avg_vol_5d({avg_vol_5d:.0f})={capacity_limit}, capping'
            )
            breakdown.is_capacity_capped = True
            if action.upper() == 'SELL':
                qty = _round_sell_lot(capacity_limit, qty)
            else:
                qty = capacity_limit
            breakdown.adjusted_qty = qty

        vol_part = alpha * abs(sigma)
        size_part = beta * (qty / adv20) * impact_multiplier if adv20 > 0 else 0.0
        total_slippage_pct = vol_part + size_part

        breakdown.vol_part_pct = vol_part * 100
        breakdown.size_part_pct = size_part * 100
        breakdown.total_pct = total_slippage_pct * 100

    except Exception as exc:
        logger.warning(f'[fallback] {symbol} slippage fallback: {exc}')
        total_slippage_pct = FALLBACK_SLIPPAGE_PCT
        breakdown.is_fallback = True
        breakdown.vol_part_pct = FALLBACK_SLIPPAGE_PCT * 100
        breakdown.size_part_pct = 0.0
        breakdown.total_pct = FALLBACK_SLIPPAGE_PCT * 100
        pricing_mode = 'FALLBACK_PRICING'

    direction = 1 if action.upper() == 'BUY' else -1
    price_shadow = price_logical * (1 + direction * total_slippage_pct)
    price_shadow = round(price_shadow, 4)
    slippage_cost = abs(price_shadow - price_logical) * qty
    cost = calculate_trade_cost(action, price=price_shadow, qty=qty)

    fill = ShadowFill(
        symbol=symbol,
        action=action.upper(),
        price_logical=price_logical,
        price_shadow=price_shadow,
        qty=qty,
        slippage_cost=round(slippage_cost, 2),
        breakdown=breakdown,
        pricing_mode=pricing_mode,
        gross_amount=cost.gross_amount,
        commission=cost.commission,
        stamp_tax=cost.stamp_tax,
        transfer_fee=cost.transfer_fee,
        tax_total=cost.tax_total,
        net_amount=cost.net_amount,
    )

    logger.info(
        f'[fill] {fill.action} {fill.symbol} x{fill.qty} '
        f'logical={fill.price_logical:.2f} -> shadow={fill.price_shadow:.4f} '
        f'slip={breakdown.total_pct:.4f}% slip_cost={fill.slippage_cost:.2f} '
        f'fees={fill.tax_total:.2f} net={fill.net_amount:.2f} mode={pricing_mode}'
    )
    return fill


def _ensure_table_columns(conn, table_name: str, required: Dict[str, str]) -> None:
    rows = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchall()
    existing = {str(r[0]).lower() for r in rows}
    for col, ddl in required.items():
        if col.lower() not in existing:
            conn.execute(f'ALTER TABLE {table_name} ADD COLUMN {col} {ddl}')
            logger.info('[schema] patch column added: %s.%s', table_name, col)


def _ensure_shadow_ledger(conn) -> None:
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
            pricing_mode VARCHAR,
            gross_amount DOUBLE DEFAULT 0,
            commission DOUBLE DEFAULT 0,
            stamp_tax DOUBLE DEFAULT 0,
            transfer_fee DOUBLE DEFAULT 0,
            tax_total DOUBLE DEFAULT 0,
            net_amount DOUBLE DEFAULT 0
        )
        """
    )
    _ensure_table_columns(
        conn,
        'fact_shadow_ledger',
        {
            'gross_amount': 'DOUBLE DEFAULT 0',
            'commission': 'DOUBLE DEFAULT 0',
            'stamp_tax': 'DOUBLE DEFAULT 0',
            'transfer_fee': 'DOUBLE DEFAULT 0',
            'tax_total': 'DOUBLE DEFAULT 0',
            'net_amount': 'DOUBLE DEFAULT 0',
        },
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



def _normalize_trade_date(trade_date: str | None) -> str:
    text = str(trade_date or '').strip()
    if not text:
        return datetime.now().strftime('%Y-%m-%d')
    if len(text) == 8 and text.isdigit():
        return f'{text[0:4]}-{text[4:6]}-{text[6:8]}'
    if len(text) >= 10 and text[4] == '-' and text[7] == '-':
        return text[:10]
    raise ValueError(f'Unsupported trade_date format: {trade_date}')


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


def _ensure_paper_positions(conn) -> None:
    """
    Create or patch the paper-trading ledger table in-place.
    Required contract fields:
    symbol, trade_date, entry_price, highest_price, dynamic_stop_price,
    status, exit_price, pnl_ratio
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_paper_positions (
            symbol VARCHAR NOT NULL,
            trade_date DATE NOT NULL,
            entry_price DOUBLE,
            highest_price DOUBLE,
            dynamic_stop_price DOUBLE,
            status VARCHAR DEFAULT 'HOLD',
            exit_price DOUBLE,
            pnl_ratio DOUBLE,
            qty INTEGER DEFAULT 0,
            signal_task_id VARCHAR DEFAULT '',
            entry_score DOUBLE DEFAULT 0,
            source VARCHAR DEFAULT 'L4_PASS',
            strategy_tag VARCHAR DEFAULT '',
            entry_tide_gate VARCHAR DEFAULT '',
            entry_tide_ratio DOUBLE DEFAULT 0,
            entry_policy VARCHAR DEFAULT '',
            entry_gross_amount DOUBLE DEFAULT 0,
            entry_commission DOUBLE DEFAULT 0,
            entry_stamp_tax DOUBLE DEFAULT 0,
            entry_transfer_fee DOUBLE DEFAULT 0,
            entry_tax_total DOUBLE DEFAULT 0,
            entry_total_cost DOUBLE DEFAULT 0,
            initial_qty INTEGER DEFAULT 0,
            realized_qty INTEGER DEFAULT 0,
            realized_amount DOUBLE DEFAULT 0,
            realized_gross_amount DOUBLE DEFAULT 0,
            realized_commission DOUBLE DEFAULT 0,
            realized_stamp_tax DOUBLE DEFAULT 0,
            realized_transfer_fee DOUBLE DEFAULT 0,
            realized_tax_total DOUBLE DEFAULT 0,
            realized_net_amount DOUBLE DEFAULT 0,
            realized_net_pnl DOUBLE DEFAULT 0,
            pnl_ratio_gross DOUBLE,
            fee_backfill_source VARCHAR DEFAULT '',
            fee_backfilled_at TIMESTAMP,
            sell_stage VARCHAR DEFAULT 'HOLD_FULL',
            strength_tier VARCHAR DEFAULT 'NORMAL',
            reinforcement_count INTEGER DEFAULT 0,
            last_reinforced_date DATE,
            last_reinforced_score DOUBLE DEFAULT 0,
            reinforcement_reason VARCHAR DEFAULT '',
            reinforcement_updated_at TIMESTAMP,
            last_sell_rule_id VARCHAR DEFAULT '',
            last_sell_reason VARCHAR DEFAULT '',
            exit_trade_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (symbol, trade_date)
        )
        """
    )

    required_columns = {
        'symbol': 'VARCHAR',
        'trade_date': 'DATE',
        'entry_price': 'DOUBLE',
        'highest_price': 'DOUBLE',
        'dynamic_stop_price': 'DOUBLE',
        'status': "VARCHAR DEFAULT 'HOLD'",
        'exit_price': 'DOUBLE',
        'pnl_ratio': 'DOUBLE',
        'qty': 'INTEGER DEFAULT 0',
        'signal_task_id': "VARCHAR DEFAULT ''",
        'entry_score': 'DOUBLE DEFAULT 0',
        'source': "VARCHAR DEFAULT 'L4_PASS'",
        'strategy_tag': "VARCHAR DEFAULT ''",
        'entry_tide_gate': "VARCHAR DEFAULT ''",
        'entry_tide_ratio': 'DOUBLE DEFAULT 0',
        'entry_policy': "VARCHAR DEFAULT ''",
        'entry_gross_amount': 'DOUBLE DEFAULT 0',
        'entry_commission': 'DOUBLE DEFAULT 0',
        'entry_stamp_tax': 'DOUBLE DEFAULT 0',
        'entry_transfer_fee': 'DOUBLE DEFAULT 0',
        'entry_tax_total': 'DOUBLE DEFAULT 0',
        'entry_total_cost': 'DOUBLE DEFAULT 0',
        'initial_qty': 'INTEGER DEFAULT 0',
        'realized_qty': 'INTEGER DEFAULT 0',
        'realized_amount': 'DOUBLE DEFAULT 0',
        'realized_gross_amount': 'DOUBLE DEFAULT 0',
        'realized_commission': 'DOUBLE DEFAULT 0',
        'realized_stamp_tax': 'DOUBLE DEFAULT 0',
        'realized_transfer_fee': 'DOUBLE DEFAULT 0',
        'realized_tax_total': 'DOUBLE DEFAULT 0',
        'realized_net_amount': 'DOUBLE DEFAULT 0',
        'realized_net_pnl': 'DOUBLE DEFAULT 0',
        'pnl_ratio_gross': 'DOUBLE',
        'fee_backfill_source': "VARCHAR DEFAULT ''",
        'fee_backfilled_at': 'TIMESTAMP',
        'sell_stage': "VARCHAR DEFAULT 'HOLD_FULL'",
        'strength_tier': "VARCHAR DEFAULT 'NORMAL'",
        'reinforcement_count': 'INTEGER DEFAULT 0',
        'last_reinforced_date': 'DATE',
        'last_reinforced_score': 'DOUBLE DEFAULT 0',
        'reinforcement_reason': "VARCHAR DEFAULT ''",
        'reinforcement_updated_at': 'TIMESTAMP',
        'last_sell_rule_id': "VARCHAR DEFAULT ''",
        'last_sell_reason': "VARCHAR DEFAULT ''",
        'exit_trade_date': 'DATE',
        'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
        'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
    }

    rows = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'fact_paper_positions'
        """
    ).fetchall()
    existing = {str(r[0]).lower() for r in rows}

    for col, ddl in required_columns.items():
        if col not in existing:
            conn.execute(f'ALTER TABLE fact_paper_positions ADD COLUMN {col} {ddl}')
            logger.info(f'[paper] patch schema add column: {col}')

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_paper_positions_status_trade_date
        ON fact_paper_positions(status, trade_date)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_paper_positions_symbol_trade_date
        ON fact_paper_positions(symbol, trade_date)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_paper_positions_signal_task
        ON fact_paper_positions(signal_task_id)
        """
    )

    conn.execute(
        """
        UPDATE fact_paper_positions
        SET status = 'HOLD'
        WHERE status IS NULL OR TRIM(status) = ''
        """
    )
    conn.execute(
        """
        UPDATE fact_paper_positions
        SET highest_price = entry_price
        WHERE highest_price IS NULL AND entry_price IS NOT NULL
        """
    )
    conn.execute(
        """
        UPDATE fact_paper_positions
        SET dynamic_stop_price = ROUND(COALESCE(highest_price, entry_price, 0) * ?, 4)
        WHERE dynamic_stop_price IS NULL
          AND COALESCE(highest_price, entry_price, 0) > 0
        """,
        [PAPER_DYNAMIC_STOP_MULTIPLIER],
    )
    conn.execute(
        """
        UPDATE fact_paper_positions
        SET initial_qty = COALESCE(NULLIF(initial_qty, 0), qty, 0),
            realized_qty = COALESCE(realized_qty, 0),
            realized_amount = COALESCE(realized_amount, 0),
            realized_gross_amount = COALESCE(realized_gross_amount, realized_amount, 0),
            realized_commission = COALESCE(realized_commission, 0),
            realized_stamp_tax = COALESCE(realized_stamp_tax, 0),
            realized_transfer_fee = COALESCE(realized_transfer_fee, 0),
            realized_tax_total = COALESCE(realized_tax_total, 0),
            realized_net_amount = COALESCE(realized_net_amount, realized_amount, 0),
            realized_net_pnl = COALESCE(realized_net_pnl, 0),
            entry_gross_amount = COALESCE(NULLIF(entry_gross_amount, 0), entry_price * COALESCE(NULLIF(initial_qty, 0), qty, 0), 0),
            entry_total_cost = COALESCE(NULLIF(entry_total_cost, 0), entry_price * COALESCE(NULLIF(initial_qty, 0), qty, 0), 0),
            sell_stage = COALESCE(NULLIF(TRIM(sell_stage), ''), 'HOLD_FULL'),
            reinforcement_count = COALESCE(reinforcement_count, 0),
            last_reinforced_score = COALESCE(last_reinforced_score, 0),
            reinforcement_reason = COALESCE(reinforcement_reason, ''),
            strength_tier = CASE
                WHEN strength_tier IS NULL
                  OR TRIM(strength_tier) = ''
                  OR (UPPER(TRIM(strength_tier)) = 'NORMAL' AND COALESCE(entry_score, 0) >= ?)
                THEN CASE
                    WHEN COALESCE(entry_score, 0) >= ?
                     AND UPPER(COALESCE(strategy_tag, '')) NOT LIKE '%DISTRIBUTION%'
                     AND UPPER(COALESCE(strategy_tag, '')) NOT LIKE '%EXHAUST%'
                     AND UPPER(COALESCE(strategy_tag, '')) NOT LIKE '%VETO%'
                     AND UPPER(COALESCE(strategy_tag, '')) NOT LIKE '%RETREAT%'
                     AND UPPER(COALESCE(strategy_tag, '')) NOT LIKE '%DIVERGENCE%'
                    THEN 'STRONG'
                    ELSE 'NORMAL'
                END
                ELSE UPPER(TRIM(strength_tier))
            END
        WHERE COALESCE(initial_qty, 0) = 0
           OR realized_qty IS NULL
           OR realized_amount IS NULL
           OR realized_gross_amount IS NULL
           OR realized_commission IS NULL
           OR realized_stamp_tax IS NULL
           OR realized_transfer_fee IS NULL
           OR realized_tax_total IS NULL
           OR realized_net_amount IS NULL
           OR realized_net_pnl IS NULL
           OR entry_gross_amount IS NULL
           OR entry_total_cost IS NULL
           OR sell_stage IS NULL
           OR TRIM(sell_stage) = ''
           OR reinforcement_count IS NULL
           OR last_reinforced_score IS NULL
           OR reinforcement_reason IS NULL
           OR strength_tier IS NULL
           OR TRIM(strength_tier) = ''
           OR (UPPER(TRIM(strength_tier)) = 'NORMAL' AND COALESCE(entry_score, 0) >= ?)
        """,
        [PAPER_STRENGTH_STRONG_SCORE, PAPER_STRENGTH_STRONG_SCORE, PAPER_STRENGTH_STRONG_SCORE],
    )


def _ensure_force_liquidation_guard(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_paper_liquidation_guard (
            idempotency_key VARCHAR PRIMARY KEY,
            trade_date DATE NOT NULL,
            symbol VARCHAR,
            run_id VARCHAR,
            trigger_reason VARCHAR,
            sell_basis_price DOUBLE,
            sell_price DOUBLE,
            note VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_paper_liq_guard_trade_date
        ON fact_paper_liquidation_guard(trade_date, symbol)
        """
    )


def has_open_paper_position(symbol: str) -> bool:
    sym = str(symbol or '').strip().upper()
    if not sym:
        return False
    try:
        with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
            _ensure_paper_positions(conn)
            row = conn.execute(
                """
                SELECT 1
                FROM fact_paper_positions
                WHERE symbol = ?
                  AND UPPER(COALESCE(status, '')) = 'HOLD'
                LIMIT 1
                """,
                [sym],
            ).fetchone()
            return bool(row)
    except Exception as exc:
        logger.warning(f'[paper] open-position check failed: {sym} | {exc}')
        return False


def open_paper_position_from_fill(
    fill: ShadowFill,
    *,
    signal_task_id: str = '',
    entry_score: float = 0.0,
    source: str = 'L4_PASS',
    strategy_tag: str = '',
    entry_tide_gate: str = '',
    entry_tide_ratio: float = 0.0,
    entry_policy: str = '',
    trade_date: str | None = None,
    conn=None,
) -> bool:
    """Open one idempotent paper position from a shadow BUY fill."""
    if fill.action.upper() != 'BUY':
        logger.warning(f'[paper] open skipped non-BUY fill: {fill.symbol} {fill.action}')
        return False
    td = _normalize_trade_date(trade_date or fill.timestamp[:10])
    symbol = str(fill.symbol or '').strip().upper()
    task_id = str(signal_task_id or '').strip()
    if not symbol or fill.price_shadow <= 0 or fill.qty <= 0:
        logger.warning(f'[paper] open skipped invalid fill: {symbol} price={fill.price_shadow} qty={fill.qty}')
        return False

    connection_context = nullcontext(conn) if conn is not None else DBGateway(
        DB_PATH, read_only=False, logger=logger
    )
    with connection_context as conn:
        _ensure_paper_positions(conn)
        if task_id:
            dup_task = conn.execute(
                """
                SELECT 1
                FROM fact_paper_positions
                WHERE signal_task_id = ?
                LIMIT 1
                """,
                [task_id],
            ).fetchone()
            if dup_task:
                logger.info(f'[paper] duplicate signal ignored: {task_id}')
                return False

        existing_hold = conn.execute(
            """
            SELECT 1
            FROM fact_paper_positions
            WHERE symbol = ?
              AND UPPER(COALESCE(status, '')) = 'HOLD'
            LIMIT 1
            """,
            [symbol],
        ).fetchone()
        if existing_hold:
            logger.info(f'[paper] existing HOLD position, skip new BUY: {symbol}')
            return False

        existing_same_day = conn.execute(
            """
            SELECT 1
            FROM fact_paper_positions
            WHERE symbol = ?
              AND CAST(trade_date AS DATE) = CAST(? AS DATE)
            LIMIT 1
            """,
            [symbol, td],
        ).fetchone()
        if existing_same_day:
            logger.info(f'[paper] same-day position exists, skip: {symbol} {td}')
            return False

        entry_price = float(fill.price_shadow)
        dynamic_stop = round(entry_price * PAPER_INITIAL_STOP_MULTIPLIER, 4)
        strength_tier = classify_position_strength(entry_score, strategy_tag)
        now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn.execute(
            """
            INSERT INTO fact_paper_positions (
                symbol, trade_date, entry_price, highest_price, dynamic_stop_price,
                status, exit_price, pnl_ratio, qty, signal_task_id, entry_score,
                source, strategy_tag, entry_tide_gate, entry_tide_ratio, entry_policy,
                entry_gross_amount, entry_commission, entry_stamp_tax, entry_transfer_fee,
                entry_tax_total, entry_total_cost,
                initial_qty, realized_qty, realized_amount, realized_gross_amount,
                realized_commission, realized_stamp_tax, realized_transfer_fee,
                realized_tax_total, realized_net_amount, realized_net_pnl,
                sell_stage, strength_tier, last_sell_rule_id, last_sell_reason,
                exit_trade_date, created_at, updated_at
            )
            VALUES (?, CAST(? AS DATE), ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?,
                    ?, 0, 0, 0,
                    0, 0, 0,
                    0, 0, 0,
                    'HOLD_FULL', ?, '', '', NULL,
                    CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP))
            """,
            [
                symbol,
                td,
                entry_price,
                entry_price,
                dynamic_stop,
                PAPER_HOLD_STATUS,
                int(fill.qty),
                task_id,
                float(entry_score or 0),
                str(source or 'L4_PASS')[:64],
                str(strategy_tag or '')[:200],
                str(entry_tide_gate or '')[:32],
                float(entry_tide_ratio or 0),
                str(entry_policy or '')[:64],
                fill.gross_amount,
                fill.commission,
                fill.stamp_tax,
                fill.transfer_fee,
                fill.tax_total,
                fill.net_amount,
                int(fill.qty),
                strength_tier,
                now_ts,
                now_ts,
            ],
        )
    logger.info(
        f'[paper] BUY {symbol} | trade_date={td} qty={fill.qty} entry={entry_price:.4f} '
        f'stop={dynamic_stop:.4f} score={float(entry_score or 0):.1f} tier={strength_tier}'
    )
    return True


def capture_core_attack_positions(trade_date: str | None = None) -> int:
    """
    Nightly buy-capture:
    ingest fresh [CORE_ATTACK] symbols from fact_macro_micro_overlay
    and open paper positions at daily close price.
    """
    td = _normalize_trade_date(trade_date)
    inserted = 0
    skipped_price = 0
    skipped_hold = 0
    skipped_dup = 0
    core_attack_cn = ''.join([chr(0x6838), chr(0x5fc3), chr(0x51fa), chr(0x51fb)])

    with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
        _ensure_paper_positions(conn)
        if not _table_exists(conn, 'fact_macro_micro_overlay'):
            logger.warning('[paper] fact_macro_micro_overlay missing, buy-capture skipped')
            return 0
        if not _table_exists(conn, 'fact_daily'):
            logger.warning('[paper] fact_daily missing, buy-capture skipped')
            return 0

        rows = conn.execute(
            """
            WITH candidates AS (
                SELECT DISTINCT
                    TRIM(symbol) AS symbol,
                    CAST(trade_date AS DATE) AS trade_date
                FROM fact_macro_micro_overlay
                WHERE CAST(trade_date AS DATE) = CAST(? AS DATE)
                  AND (
                        COALESCE(signal_label, '') LIKE ?
                        OR UPPER(COALESCE(signal_label, '')) LIKE ?
                  )
            )
            SELECT
                c.symbol,
                CAST(c.trade_date AS VARCHAR) AS trade_date,
                COALESCE(d.close, 0) AS entry_price
            FROM candidates c
            LEFT JOIN fact_daily d
              ON d.symbol = c.symbol
             AND CAST(d.trade_date AS DATE) = c.trade_date
            ORDER BY c.symbol
            """,
            [td, f'%{core_attack_cn}%', '%CORE_ATTACK%'],
        ).fetchall()

        for symbol_raw, sig_td, entry_raw in rows:
            symbol = str(symbol_raw or '').strip().upper()
            entry_price = float(entry_raw or 0)
            if not symbol:
                continue
            if entry_price <= 0:
                skipped_price += 1
                logger.warning(f'[paper] skip buy-capture {symbol}: invalid close={entry_price}')
                continue

            row_hold = conn.execute(
                """
                SELECT 1
                FROM fact_paper_positions
                WHERE symbol = ?
                  AND UPPER(COALESCE(status, '')) = 'HOLD'
                LIMIT 1
                """,
                [symbol],
            ).fetchone()
            if row_hold:
                skipped_hold += 1
                continue

            row_dup = conn.execute(
                """
                SELECT 1
                FROM fact_paper_positions
                WHERE symbol = ?
                  AND CAST(trade_date AS DATE) = CAST(? AS DATE)
                LIMIT 1
                """,
                [symbol, sig_td],
            ).fetchone()
            if row_dup:
                skipped_dup += 1
                continue

            dynamic_stop = round(entry_price * PAPER_DYNAMIC_STOP_MULTIPLIER, 4)
            now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            conn.execute(
                """
                INSERT INTO fact_paper_positions (
                    symbol,
                    trade_date,
                    entry_price,
                    highest_price,
                    dynamic_stop_price,
                    status,
                    exit_price,
                    pnl_ratio,
                    qty,
                    signal_task_id,
                    entry_score,
                    source,
                    strategy_tag,
                    exit_trade_date,
                    created_at,
                    updated_at
                )
                VALUES (
                    ?,
                    CAST(? AS DATE),
                    ?,
                    ?,
                    ?,
                    ?,
                    NULL,
                    NULL,
                    0,
                    '',
                    0,
                    'CORE_ATTACK',
                    'CORE_ATTACK',
                    NULL,
                    CAST(? AS TIMESTAMP),
                    CAST(? AS TIMESTAMP)
                )
                """,
                [symbol, sig_td, entry_price, entry_price, dynamic_stop, PAPER_HOLD_STATUS, now_ts, now_ts],
            )
            inserted += 1
            logger.info(
                f'[paper] BUY {symbol} | trade_date={sig_td} entry={entry_price:.4f} '
                f'highest={entry_price:.4f} stop={dynamic_stop:.4f}'
            )

    logger.info(
        f'[paper] buy-capture done | trade_date={td} '
        f'inserted={inserted} skip_price={skipped_price} '
        f'skip_hold={skipped_hold} skip_dup={skipped_dup}'
    )
    return inserted



def ensure_position_reviews_table(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_shadow_position_reviews (
            review_id VARCHAR PRIMARY KEY,
            trade_date DATE NOT NULL,
            symbol VARCHAR NOT NULL,
            position_trade_date DATE NOT NULL,
            position_status VARCHAR DEFAULT '',
            hold_days INTEGER DEFAULT 0,
            entry_price DOUBLE DEFAULT 0,
            close_price DOUBLE DEFAULT 0,
            highest_price DOUBLE DEFAULT 0,
            dynamic_stop_price DOUBLE DEFAULT 0,
            realized_pnl DOUBLE DEFAULT NULL,
            unrealized_pnl DOUBLE DEFAULT 0,
            max_gain_ratio DOUBLE DEFAULT 0,
            max_drawdown_ratio DOUBLE DEFAULT 0,
            decision_state VARCHAR DEFAULT 'ACTIVE_HOLD',
            quality_label VARCHAR DEFAULT 'PENDING',
            evidence_json VARCHAR DEFAULT '{}',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    _patch_position_review_columns(conn)
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_shadow_position_reviews_symbol_date
        ON fact_shadow_position_reviews(symbol, trade_date)
        """
    )


def _patch_position_review_columns(conn) -> None:
    rows = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'fact_shadow_position_reviews'
        """
    ).fetchall()
    existing = {str(r[0]).lower() for r in rows}
    required = {
        'review_id': 'VARCHAR',
        'trade_date': 'DATE',
        'symbol': 'VARCHAR',
        'position_trade_date': 'DATE',
        'position_status': "VARCHAR DEFAULT ''",
        'hold_days': 'INTEGER DEFAULT 0',
        'entry_price': 'DOUBLE DEFAULT 0',
        'close_price': 'DOUBLE DEFAULT 0',
        'highest_price': 'DOUBLE DEFAULT 0',
        'dynamic_stop_price': 'DOUBLE DEFAULT 0',
        'realized_pnl': 'DOUBLE DEFAULT NULL',
        'unrealized_pnl': 'DOUBLE DEFAULT 0',
        'max_gain_ratio': 'DOUBLE DEFAULT 0',
        'max_drawdown_ratio': 'DOUBLE DEFAULT 0',
        'decision_state': "VARCHAR DEFAULT 'ACTIVE_HOLD'",
        'quality_label': "VARCHAR DEFAULT 'PENDING'",
        'evidence_json': "VARCHAR DEFAULT '{}'",
        'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
        'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
    }
    for col, ddl in required.items():
        if col not in existing:
            conn.execute(f'ALTER TABLE fact_shadow_position_reviews ADD COLUMN {col} {ddl}')
            logger.info(f'[paper] patch position review schema add column: {col}')


def _review_decision_state(
    status: str,
    realized_pnl: float | None,
    unrealized_pnl: float,
    max_gain_ratio: float,
    max_drawdown_ratio: float,
    close_price: float,
    dynamic_stop: float,
    hold_days: int,
) -> tuple[str, str]:
    status_u = str(status or '').upper()
    if status_u == PAPER_SOLD_STATUS:
        pnl = float(realized_pnl if realized_pnl is not None else unrealized_pnl)
        if pnl > 0:
            return 'CLOSED_PROFIT', 'SELL_PROTECTED_PROFIT'
        return 'CLOSED_LOSS', 'STOP_LOSS_CONFIRMED'
    if dynamic_stop > 0 and close_price > 0 and close_price <= dynamic_stop * 1.015:
        return 'STOP_NEAR', 'RISK_NEEDS_WATCH'
    if max_gain_ratio >= 0.06 and unrealized_pnl > 0:
        return 'PROFIT_PROTECTED', 'WIN_RUNNING'
    if max_drawdown_ratio <= -0.05 or unrealized_pnl <= -0.05:
        return 'DANGER_WATCH', 'RISK_NEEDS_WATCH'
    if max_drawdown_ratio <= -0.04 or unrealized_pnl <= -0.03:
        return 'PULLBACK_WATCH', 'RISK_NEEDS_WATCH'
    if hold_days >= 10 and unrealized_pnl < 0.01:
        return 'STALE_WEAK', 'TIME_DECAY_WEAK'
    if unrealized_pnl > 0:
        return 'ACTIVE_HOLD', 'BUY_VALIDATED_RUNNING'
    if unrealized_pnl <= -0.02:
        return 'ACTIVE_HOLD', 'BUY_WEAK_RUNNING'
    return 'ACTIVE_HOLD', 'PENDING'


def _quote_unavailable_breach(
    conn,
    symbol: str,
    position_trade_date: str,
    trade_date: str,
    close_price: float,
    dynamic_stop: float,
) -> Dict[str, object]:
    if close_price <= 0 or dynamic_stop <= 0 or close_price > dynamic_stop:
        return {}
    if not _table_exists(conn, 'fact_shadow_quote_health'):
        return {}
    start_ts = f'{trade_date} 09:30:00'
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS total_calls,
            SUM(CASE WHEN status = 'QUOTE_OK' THEN 1 ELSE 0 END) AS ok_calls,
            SUM(CASE WHEN status <> 'QUOTE_OK' THEN 1 ELSE 0 END) AS fail_calls,
            MIN(event_time) AS first_event,
            MAX(event_time) AS last_event,
            string_agg(DISTINCT status, ',') AS statuses
        FROM fact_shadow_quote_health
        WHERE trade_date = CAST(? AS DATE)
          AND symbol = ?
          AND position_trade_date = CAST(? AS DATE)
          AND event_time >= CAST(? AS TIMESTAMP)
        """,
        [trade_date, symbol, position_trade_date, start_ts],
    ).fetchone()
    if not row:
        return {}
    total_calls = int(row[0] or 0)
    ok_calls = int(row[1] or 0)
    fail_calls = int(row[2] or 0)
    if total_calls <= 0 or ok_calls > 0 or fail_calls < 3:
        return {}
    return {
        'state': 'SELL_DEFERRED_DAILY_BREACH',
        'quality_label': 'QUOTE_UNAVAILABLE_RISK',
        'reason': 'daily close breached protection line while intraday quote was unavailable',
        'total_calls_after_0930': total_calls,
        'failed_calls_after_0930': fail_calls,
        'statuses': str(row[5] or ''),
        'first_event': str(row[3] or ''),
        'last_event': str(row[4] or ''),
        'close_price': round(float(close_price), 4),
        'dynamic_stop_price': round(float(dynamic_stop), 4),
    }


def review_paper_positions(trade_date: str | None = None) -> Dict[str, int | str]:
    """Create one daily review snapshot for every paper position with available market data."""
    td = _normalize_trade_date(trade_date)
    stats: Dict[str, int | str] = {
        'trade_date': td,
        'positions_scanned': 0,
        'reviews_upserted': 0,
        'skip_no_tables': 0,
        'skip_missing_daily': 0,
        'quote_unavailable_risk': 0,
    }
    with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
        _ensure_paper_positions(conn)
        ensure_position_reviews_table(conn)
        if not _table_exists(conn, 'fact_daily'):
            stats['skip_no_tables'] = 1
            return stats

        positions = conn.execute(
            """
            SELECT
                symbol,
                CAST(trade_date AS VARCHAR) AS position_trade_date,
                COALESCE(entry_price, 0) AS entry_price,
                COALESCE(highest_price, entry_price, 0) AS highest_price,
                COALESCE(dynamic_stop_price, 0) AS dynamic_stop_price,
                COALESCE(status, '') AS status,
                COALESCE(exit_price, 0) AS exit_price,
                COALESCE(pnl_ratio, NULL) AS realized_pnl,
                CAST(exit_trade_date AS VARCHAR) AS exit_trade_date,
                COALESCE(qty, 0) AS qty,
                COALESCE(signal_task_id, '') AS signal_task_id,
                COALESCE(entry_score, 0) AS entry_score,
                COALESCE(source, '') AS source,
                COALESCE(strategy_tag, '') AS strategy_tag
            FROM fact_paper_positions p
            WHERE CAST(p.trade_date AS DATE) <= CAST(? AS DATE)
              AND (
                  UPPER(COALESCE(NULLIF(p.status, ''), 'HOLD')) = ?
                  OR (
                      UPPER(COALESCE(p.status, '')) = ?
                      AND p.exit_trade_date IS NOT NULL
                      AND CAST(p.exit_trade_date AS DATE) <= CAST(? AS DATE)
                      AND NOT EXISTS (
                          SELECT 1
                          FROM fact_shadow_position_reviews r
                          WHERE r.review_id = p.symbol || '|' || CAST(CAST(p.trade_date AS DATE) AS VARCHAR) || '|FINAL'
                      )
                  )
              )
            ORDER BY p.trade_date, p.symbol
            """,
            [td, PAPER_HOLD_STATUS, PAPER_SOLD_STATUS, td],
        ).fetchall()
        stats['positions_scanned'] = len(positions)

        for row in positions:
            (
                symbol_raw,
                pos_td_raw,
                entry_raw,
                highest_raw,
                stop_raw,
                status_raw,
                exit_raw,
                realized_raw,
                exit_td_raw,
                qty_raw,
                signal_task_raw,
                entry_score_raw,
                source_raw,
                strategy_tag_raw,
            ) = row
            symbol = str(symbol_raw or '').strip().upper()
            pos_td = _normalize_trade_date(pos_td_raw)
            entry_price = float(entry_raw or 0)
            if not symbol or entry_price <= 0:
                continue

            status = str(status_raw or '').strip().upper() or PAPER_HOLD_STATUS
            is_sold = status == PAPER_SOLD_STATUS
            exit_td = _normalize_trade_date(exit_td_raw) if str(exit_td_raw or '').strip() else ''
            effective_td = exit_td if is_sold and exit_td else td
            review_id = f'{symbol}|{pos_td}|FINAL' if is_sold else f'{symbol}|{pos_td}|{td}'

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
                [symbol, pos_td, effective_td],
            ).fetchall()
            if not daily_rows:
                stats['skip_missing_daily'] += 1
                continue

            close_price = float(daily_rows[-1][1] or 0)
            exit_price = float(exit_raw or 0)
            if is_sold and exit_price > 0:
                close_price = exit_price
            max_high = max(float(r[2] or 0) for r in daily_rows)
            min_low = min(float(r[3] or 0) for r in daily_rows)
            highest_price = max(entry_price, max_high) if is_sold else max(float(highest_raw or 0), entry_price, max_high)
            dynamic_stop = float(stop_raw or 0)
            if dynamic_stop <= 0:
                dynamic_stop = round(highest_price * PAPER_DYNAMIC_STOP_MULTIPLIER, 4)
            realized_pnl = None if realized_raw is None else float(realized_raw)
            calc_pnl = round((close_price - entry_price) / entry_price, 6)
            unrealized_pnl = float(realized_pnl) if is_sold and realized_pnl is not None else calc_pnl
            max_gain_ratio = round((highest_price - entry_price) / entry_price, 6)
            max_drawdown_ratio = round((min_low - entry_price) / entry_price, 6)
            try:
                start = datetime.strptime(pos_td[:10], '%Y-%m-%d').date()
                end = datetime.strptime(effective_td[:10], '%Y-%m-%d').date()
                hold_days = max(0, (end - start).days)
            except Exception:
                hold_days = len(daily_rows) - 1
            decision_state, quality_label = _review_decision_state(
                status,
                realized_pnl,
                unrealized_pnl,
                max_gain_ratio,
                max_drawdown_ratio,
                close_price,
                dynamic_stop,
                hold_days,
            )
            quote_risk = {}
            if not is_sold:
                quote_risk = _quote_unavailable_breach(conn, symbol, pos_td, td, close_price, dynamic_stop)
                if quote_risk:
                    decision_state = str(quote_risk.get('state') or decision_state)
                    quality_label = str(quote_risk.get('quality_label') or quality_label)
                    stats['quote_unavailable_risk'] = int(stats.get('quote_unavailable_risk', 0) or 0) + 1
            evidence = {
                'qty': int(qty_raw or 0),
                'signal_task_id': str(signal_task_raw or ''),
                'entry_score': float(entry_score_raw or 0),
                'source': str(source_raw or ''),
                'strategy_tag': str(strategy_tag_raw or ''),
                'exit_price': float(exit_raw or 0),
                'exit_trade_date': str(exit_td_raw or ''),
                'review_scope': 'FINAL' if is_sold else 'DAILY',
                'market_window_end': effective_td,
                'daily_points': len(daily_rows),
            }
            if quote_risk:
                evidence['quote_unavailable_risk'] = quote_risk
            now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            conn.execute(
                """
                INSERT INTO fact_shadow_position_reviews (
                    review_id, trade_date, symbol, position_trade_date, position_status,
                    hold_days, entry_price, close_price, highest_price, dynamic_stop_price,
                    realized_pnl, unrealized_pnl, max_gain_ratio, max_drawdown_ratio,
                    decision_state, quality_label, evidence_json, created_at, updated_at
                )
                VALUES (?, CAST(? AS DATE), ?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP))
                ON CONFLICT (review_id) DO UPDATE SET
                    position_status = EXCLUDED.position_status,
                    hold_days = EXCLUDED.hold_days,
                    close_price = EXCLUDED.close_price,
                    highest_price = EXCLUDED.highest_price,
                    dynamic_stop_price = EXCLUDED.dynamic_stop_price,
                    realized_pnl = EXCLUDED.realized_pnl,
                    unrealized_pnl = EXCLUDED.unrealized_pnl,
                    max_gain_ratio = EXCLUDED.max_gain_ratio,
                    max_drawdown_ratio = EXCLUDED.max_drawdown_ratio,
                    decision_state = EXCLUDED.decision_state,
                    quality_label = EXCLUDED.quality_label,
                    evidence_json = EXCLUDED.evidence_json,
                    updated_at = EXCLUDED.updated_at
                """,
                [
                    review_id,
                    effective_td,
                    symbol,
                    pos_td,
                    status,
                    hold_days,
                    entry_price,
                    close_price,
                    highest_price,
                    dynamic_stop,
                    realized_pnl,
                    unrealized_pnl,
                    max_gain_ratio,
                    max_drawdown_ratio,
                    decision_state,
                    quality_label,
                    json.dumps(evidence, ensure_ascii=False, default=str)[:2000],
                    now_ts,
                    now_ts,
                ],
            )
            stats['reviews_upserted'] += 1
    logger.info(f'[paper] position review summary: {stats}')
    return stats

def settle_trailing_stop_positions(trade_date: str | None = None) -> Dict[str, int | str]:
    """Legacy daily stop executor.

    V1 exit strategy is owned by ShadowIntradayManager because sell execution
    needs realtime quotes and staged exits. Nightly audit must not synthesize
    a same-day stop fill from daily OHLC. Keep this path behind an explicit
    emergency flag for backward-compatible manual recovery only.
    """
    td = _normalize_trade_date(trade_date)
    current_check_date = td
    stats = {
        'trade_date': td,
        'hold_scanned': 0,
        'highest_raised': 0,
        'sold': 0,
        'hold_updated': 0,
        'skip_invalid_kline': 0,
        'skip_entry_trade_date': 0,
        'disabled': 0,
    }
    if str(os.getenv('SHADOW_DAILY_STOP_EXECUTION_ENABLED', '0')).strip() != '1':
        stats['disabled'] = 1
        logger.info(
            '[paper] daily trailing-stop settlement disabled; '
            'intraday manager owns paper sell execution'
        )
        return stats
    with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
        _ensure_paper_positions(conn)
        if not _table_exists(conn, 'fact_daily'):
            logger.warning('[paper] fact_daily missing, trailing-stop settlement skipped')
            return stats

        rows = conn.execute(
            """
            SELECT
                p.symbol,
                CAST(p.trade_date AS VARCHAR) AS entry_trade_date,
                COALESCE(p.entry_price, 0) AS entry_price,
                COALESCE(p.highest_price, p.entry_price, 0) AS highest_price,
                COALESCE(
                    p.dynamic_stop_price,
                    COALESCE(p.highest_price, p.entry_price, 0) * ?
                ) AS dynamic_stop_price,
                COALESCE(d.high, 0) AS day_high,
                COALESCE(d.low, 0) AS day_low,
                COALESCE(p.qty, 0) AS qty,
                COALESCE(p.signal_task_id, '') AS signal_task_id,
                COALESCE(p.strategy_tag, '') AS strategy_tag
            FROM fact_paper_positions p
            JOIN fact_daily d
              ON d.symbol = p.symbol
             AND CAST(d.trade_date AS DATE) = CAST(? AS DATE)
            WHERE UPPER(COALESCE(p.status, '')) = 'HOLD'
            ORDER BY p.symbol, p.trade_date
            """,
            [PAPER_DYNAMIC_STOP_MULTIPLIER, td],
        ).fetchall()

        for symbol_raw, entry_td, entry_raw, highest_raw, stop_raw, day_high_raw, day_low_raw, qty_raw, signal_task_raw, strategy_tag_raw in rows:
            stats['hold_scanned'] += 1
            entry_trade_date = _normalize_trade_date(entry_td)
            if entry_trade_date >= current_check_date:
                stats['skip_entry_trade_date'] += 1
                continue
            symbol = str(symbol_raw or '').strip().upper()
            entry_price = float(entry_raw or 0)
            highest_price = float(highest_raw or 0)
            dynamic_stop = float(stop_raw or 0)
            day_high = float(day_high_raw or 0)
            day_low = float(day_low_raw or 0)
            qty = int(qty_raw or 0)
            signal_task_id = str(signal_task_raw or '').strip()
            strategy_tag = str(strategy_tag_raw or '').strip()

            if entry_price <= 0 or day_high <= 0 or day_low <= 0:
                stats['skip_invalid_kline'] += 1
                continue

            highest_price = max(highest_price, entry_price)
            if dynamic_stop <= 0:
                dynamic_stop = round(highest_price * PAPER_DYNAMIC_STOP_MULTIPLIER, 4)

            changed = False
            if day_high > highest_price:
                highest_price = day_high
                dynamic_stop = round(highest_price * PAPER_DYNAMIC_STOP_MULTIPLIER, 4)
                stats['highest_raised'] += 1
                changed = True

            # Strategy contract: trailing-stop check uses the latest dynamic_stop after high update.
            if day_low <= dynamic_stop:
                if qty <= 0:
                    stats['skip_invalid_kline'] += 1
                    logger.warning('[paper] daily SELL skipped invalid qty: %s qty=%s', symbol, qty)
                    continue
                exit_price = round(dynamic_stop, 4)
                pnl_ratio = round((exit_price - entry_price) / entry_price, 6)
                now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                sell_fill = ShadowFill(
                    symbol=symbol,
                    action='SELL',
                    price_logical=exit_price,
                    price_shadow=exit_price,
                    qty=qty,
                    slippage_cost=0.0,
                    breakdown=SlippageBreakdown(original_qty=qty, adjusted_qty=qty),
                    pricing_mode='PAPER_STOP',
                )
                trace_id = f'{signal_task_id or entry_td}:{td}:SELL'
                sell_tag = (strategy_tag or 'PAPER_TRAILING_STOP') + '|SELL'
                conn.execute('BEGIN TRANSACTION')
                try:
                    updated = conn.execute(
                        """
                        UPDATE fact_paper_positions
                        SET highest_price = ?,
                            dynamic_stop_price = ?,
                            status = ?,
                            exit_price = ?,
                            pnl_ratio = ?,
                            exit_trade_date = CAST(? AS DATE),
                            updated_at = CAST(? AS TIMESTAMP)
                        WHERE symbol = ?
                          AND CAST(trade_date AS DATE) = CAST(? AS DATE)
                          AND UPPER(COALESCE(status, '')) = 'HOLD'
                        RETURNING symbol
                        """,
                        [
                            highest_price,
                            dynamic_stop,
                            PAPER_SOLD_STATUS,
                            exit_price,
                            pnl_ratio,
                            td,
                            now_ts,
                            symbol,
                            entry_td,
                        ],
                    ).fetchone()
                    if not updated or not persist_fill(
                            sell_fill,
                            trace_id=trace_id,
                            tide_mode='Golden',
                            strategy_tag=sell_tag,
                            trade_date=td,
                            conn=conn,
                        ):
                        conn.execute('ROLLBACK')
                        logger.warning('[paper] daily SELL rolled back: %s %s', symbol, td)
                        continue
                    conn.execute('COMMIT')
                except Exception:
                    try:
                        conn.execute('ROLLBACK')
                    except Exception:
                        pass
                    logger.exception('[paper] daily SELL transaction failed: %s %s', symbol, td)
                    continue
                stats['sold'] += 1
                logger.info(
                    f'[paper] SELL {symbol} | entry={entry_price:.4f} stop={exit_price:.4f} '
                    f'pnl={pnl_ratio:.4%} eval_date={td}'
                )
                continue

            if changed or abs(dynamic_stop - float(stop_raw or 0)) > 1e-12:
                now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                conn.execute(
                    """
                    UPDATE fact_paper_positions
                    SET highest_price = ?,
                        dynamic_stop_price = ?,
                        updated_at = CAST(? AS TIMESTAMP)
                    WHERE symbol = ?
                      AND CAST(trade_date AS DATE) = CAST(? AS DATE)
                      AND UPPER(COALESCE(status, '')) = 'HOLD'
                    """,
                    [highest_price, dynamic_stop, now_ts, symbol, entry_td],
                )
                stats['hold_updated'] += 1

    logger.info(
        f'[paper] trailing-stop settlement | trade_date={td} '
        f'scanned={stats["hold_scanned"]} raised={stats["highest_raised"]} '
        f'sold={stats["sold"]} hold_updated={stats["hold_updated"]} '
        f'skip_entry_trade_date={stats["skip_entry_trade_date"]} '
        f'skip_invalid_kline={stats["skip_invalid_kline"]}'
    )
    return stats


def force_liquidate_positions(run_id: str, trigger_reason: str = "KILL_SWITCH") -> Dict[str, int | str]:
    """
    Force-liquidate all HOLD paper positions with pessimistic slippage.

    Idempotency keys:
      - global: {trade_date}_MELTDOWN_ALL
      - per symbol: {trade_date}_{symbol}_FORCE_SELL
    """
    td = _normalize_trade_date(None)
    run_tag = str(run_id or '').strip() or f'force_liq_{datetime.now().strftime("%H%M%S")}'
    reason = str(trigger_reason or 'KILL_SWITCH').strip().upper()
    global_key = f'{td}_{FORCE_LIQUIDATION_GLOBAL_KEY_SUFFIX}'

    stats: Dict[str, int | str] = {
        'trade_date': td,
        'run_id': run_tag,
        'trigger_reason': reason,
        'hold_scanned': 0,
        'sold': 0,
        'duplicate_blocked': 0,
        'duplicate_symbol_skipped': 0,
        'skip_invalid_price': 0,
    }

    with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
        _ensure_paper_positions(conn)
        _ensure_shadow_ledger(conn)
        _ensure_force_liquidation_guard(conn)

        conn.execute('BEGIN TRANSACTION')
        try:
            now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            inserted_global = conn.execute(
                """
                INSERT INTO fact_paper_liquidation_guard (
                    idempotency_key,
                    trade_date,
                    symbol,
                    run_id,
                    trigger_reason,
                    note,
                    created_at
                )
                VALUES (?, CAST(? AS DATE), NULL, ?, ?, ?, CAST(? AS TIMESTAMP))
                ON CONFLICT (idempotency_key) DO NOTHING
                RETURNING idempotency_key
                """,
                [global_key, td, run_tag, reason, 'GLOBAL_PENDING', now_ts],
            ).fetchone()

            if not inserted_global:
                conn.execute('ROLLBACK')
                stats['duplicate_blocked'] = 1
                logger.warning(
                    f"[paper] force liquidation duplicate blocked | key={global_key} run_id={run_tag}"
                )
                return stats

            if _table_exists(conn, 'fact_daily'):
                hold_rows = conn.execute(
                    """
                    WITH latest_close AS (
                        SELECT
                            symbol,
                            close,
                            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date DESC) AS rn
                        FROM fact_daily
                        WHERE CAST(trade_date AS DATE) <= CAST(? AS DATE)
                          AND COALESCE(close, 0) > 0
                    )
                    SELECT
                        p.symbol,
                        CAST(p.trade_date AS VARCHAR) AS trade_date,
                        COALESCE(p.entry_price, 0) AS entry_price,
                        COALESCE(l.close, 0) AS market_price,
                        COALESCE(p.qty, 0) AS qty,
                        COALESCE(NULLIF(p.initial_qty, 0), p.qty, 0) AS initial_qty,
                        COALESCE(NULLIF(p.entry_total_cost, 0), p.entry_price * COALESCE(NULLIF(p.initial_qty, 0), p.qty, 0), 0) AS entry_total_cost,
                        COALESCE(p.strategy_tag, '') AS strategy_tag
                    FROM fact_paper_positions p
                    LEFT JOIN latest_close l
                      ON l.symbol = p.symbol
                     AND l.rn = 1
                    WHERE p.status = 'HOLD'
                    ORDER BY p.symbol, p.trade_date
                    """,
                    [td],
                ).fetchall()
            else:
                hold_rows = conn.execute(
                    """
                    SELECT
                        symbol,
                        CAST(trade_date AS VARCHAR) AS trade_date,
                        COALESCE(entry_price, 0) AS entry_price,
                        0 AS market_price,
                        COALESCE(qty, 0) AS qty,
                        COALESCE(NULLIF(initial_qty, 0), qty, 0) AS initial_qty,
                        COALESCE(NULLIF(entry_total_cost, 0), entry_price * COALESCE(NULLIF(initial_qty, 0), qty, 0), 0) AS entry_total_cost,
                        COALESCE(strategy_tag, '') AS strategy_tag
                    FROM fact_paper_positions
                    WHERE status = 'HOLD'
                    ORDER BY symbol, trade_date
                    """
                ).fetchall()

            stats['hold_scanned'] = len(hold_rows)

            for symbol_raw, entry_td_raw, entry_raw, market_raw, qty_raw, initial_qty_raw, entry_total_raw, strategy_tag_raw in hold_rows:
                symbol = str(symbol_raw or '').strip().upper()
                if not symbol:
                    continue
                entry_td = _normalize_trade_date(entry_td_raw)
                entry_price = float(entry_raw or 0)
                market_price = float(market_raw or 0)
                qty = int(qty_raw or 0)
                initial_qty = int(initial_qty_raw or qty or 0)
                entry_total_cost = float(entry_total_raw or (entry_price * initial_qty) or 0)
                strategy_tag = str(strategy_tag_raw or 'PAPER_FORCE_LIQUIDATION')
                symbol_key = f'{td}_{symbol}_{FORCE_LIQUIDATION_SINGLE_KEY_SUFFIX}'
                now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                inserted_symbol = conn.execute(
                    """
                    INSERT INTO fact_paper_liquidation_guard (
                        idempotency_key,
                        trade_date,
                        symbol,
                        run_id,
                        trigger_reason,
                        note,
                        created_at
                    )
                    VALUES (?, CAST(? AS DATE), ?, ?, ?, ?, CAST(? AS TIMESTAMP))
                    ON CONFLICT (idempotency_key) DO NOTHING
                    RETURNING idempotency_key
                    """,
                    [symbol_key, td, symbol, run_tag, reason, 'PENDING', now_ts],
                ).fetchone()

                if not inserted_symbol:
                    stats['duplicate_symbol_skipped'] += 1
                    continue

                candidates = [px for px in (market_price, entry_price) if px and px > 0]
                if not candidates:
                    stats['skip_invalid_price'] += 1
                    conn.execute(
                        """
                        UPDATE fact_paper_liquidation_guard
                        SET note = 'SKIP_INVALID_PRICE'
                        WHERE idempotency_key = ?
                        """,
                        [symbol_key],
                    )
                    continue

                basis_price = min(candidates)
                sell_price = round(basis_price * (1.0 - FORCE_LIQUIDATION_PENALTY_PCT), 4)
                sell_cost = calculate_trade_cost('SELL', price=sell_price, qty=qty)
                entry_cost_sold = entry_total_cost * (qty / max(1, initial_qty)) if qty > 0 else 0.0
                realized_net_pnl = round(sell_cost.net_amount - entry_cost_sold, 2) if entry_cost_sold > 0 else 0.0
                pnl_ratio_net = round(realized_net_pnl / entry_cost_sold, 6) if entry_cost_sold > 0 else None
                pnl_ratio_gross = round((sell_cost.gross_amount - (entry_price * qty)) / (entry_price * qty), 6) if entry_price > 0 and qty > 0 else None
                now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                updated = conn.execute(
                    """
                    UPDATE fact_paper_positions
                    SET status = 'SOLD',
                        qty = 0,
                        realized_qty = ?,
                        realized_amount = ?,
                        realized_gross_amount = ?,
                        realized_commission = ?,
                        realized_stamp_tax = ?,
                        realized_transfer_fee = ?,
                        realized_tax_total = ?,
                        realized_net_amount = ?,
                        realized_net_pnl = ?,
                        exit_price = ?,
                        pnl_ratio = ?,
                        pnl_ratio_gross = ?,
                        last_sell_rule_id = 'FORCE_LIQUIDATION',
                        last_sell_reason = ?,
                        exit_trade_date = CAST(? AS DATE),
                        updated_at = CAST(? AS TIMESTAMP)
                    WHERE symbol = ?
                      AND CAST(trade_date AS DATE) = CAST(? AS DATE)
                      AND status = 'HOLD'
                    RETURNING symbol
                    """,
                    [
                        qty,
                        sell_cost.gross_amount,
                        sell_cost.gross_amount,
                        sell_cost.commission,
                        sell_cost.stamp_tax,
                        sell_cost.transfer_fee,
                        sell_cost.tax_total,
                        sell_cost.net_amount,
                        realized_net_pnl,
                        sell_price,
                        pnl_ratio_net,
                        pnl_ratio_gross,
                        reason,
                        td,
                        now_ts,
                        symbol,
                        entry_td,
                    ],
                ).fetchone()

                if not updated:
                    stats['duplicate_symbol_skipped'] += 1
                    conn.execute(
                        """
                        UPDATE fact_paper_liquidation_guard
                        SET sell_basis_price = ?,
                            sell_price = ?,
                            note = 'SKIP_ALREADY_SOLD'
                        WHERE idempotency_key = ?
                        """,
                        [basis_price, sell_price, symbol_key],
                    )
                    continue

                stats['sold'] += 1
                conn.execute(
                    """
                    INSERT INTO fact_shadow_ledger (
                        timestamp, trade_date, trace_id, symbol, action,
                        price_logical, price_shadow, qty, tide_mode, strategy_tag,
                        slippage_cost, pricing_mode, gross_amount, commission, stamp_tax,
                        transfer_fee, tax_total, net_amount
                    )
                    VALUES (CAST(? AS TIMESTAMP), CAST(? AS DATE), ?, ?, 'SELL', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        now_ts,
                        td,
                        symbol_key,
                        symbol,
                        basis_price,
                        sell_price,
                        qty,
                        'Force',
                        f'{strategy_tag}|FORCE_LIQUIDATION',
                        round(abs(sell_price - basis_price) * qty, 2),
                        'FORCE_LIQUIDATION',
                        sell_cost.gross_amount,
                        sell_cost.commission,
                        sell_cost.stamp_tax,
                        sell_cost.transfer_fee,
                        sell_cost.tax_total,
                        sell_cost.net_amount,
                    ],
                )
                conn.execute(
                    """
                    UPDATE fact_paper_liquidation_guard
                    SET sell_basis_price = ?,
                        sell_price = ?,
                        note = ?
                    WHERE idempotency_key = ?
                    """,
                    [
                        basis_price,
                        sell_price,
                        '\u5df2\u6267\u884c -1.5% \u60b2\u89c2\u6ed1\u70b9\u60e9\u7f5a',
                        symbol_key,
                    ],
                )
                logger.critical(
                    f'[paper] FORCE_SELL {symbol} | basis={basis_price:.4f} -> sell={sell_price:.4f} '
                    f'| reason={reason} | \u5df2\u6267\u884c -1.5% \u60b2\u89c2\u6ed1\u70b9\u60e9\u7f5a'
                )

            conn.execute(
                """
                UPDATE fact_paper_liquidation_guard
                SET note = ?
                WHERE idempotency_key = ?
                """,
                [
                    f'GLOBAL_DONE sold={stats["sold"]} duplicate_symbol={stats["duplicate_symbol_skipped"]}',
                    global_key,
                ],
            )
            conn.execute('COMMIT')
        except Exception:
            conn.execute('ROLLBACK')
            raise

    logger.info(
        f'[paper] force liquidation summary | date={td} run_id={run_tag} reason={reason} '
        f'scanned={stats["hold_scanned"]} sold={stats["sold"]} '
        f'dup_global={stats["duplicate_blocked"]} dup_symbol={stats["duplicate_symbol_skipped"]} '
        f'skip_invalid_price={stats["skip_invalid_price"]}'
    )
    return stats


def run_paper_trading_cycle(trade_date: str | None = None) -> Dict[str, int | str]:
    td = _normalize_trade_date(trade_date)
    inserted = capture_core_attack_positions(td)
    settled = settle_trailing_stop_positions(td)
    reviewed = review_paper_positions(td)
    result = dict(settled)
    result['buy_inserted'] = int(inserted)
    result['reviews_upserted'] = int(reviewed.get('reviews_upserted', 0) or 0)
    return result

def persist_fill(
    fill: ShadowFill,
    trace_id: str = '',
    tide_mode: str = 'Standard',
    strategy_tag: str = '',
    trade_date: str | None = None,
    conn=None,
) -> bool:
    """Persist simulated receipt into fact_shadow_ledger (RW via DBGateway)."""
    td = trade_date or fill.timestamp[:10]
    tag = strategy_tag or ''
    trace = (trace_id or '').strip()

    # trace_id is primary dedup key; fallback to business fingerprint to avoid duplicate writes
    dedup_key = trace if trace else (
        f'{td}|{fill.symbol}|{fill.action}|{fill.qty}|{fill.price_shadow:.4f}|{tag}|{tide_mode}'
    )

    def _write(active_conn) -> bool:
        _ensure_shadow_ledger(active_conn)

        dedup_row = active_conn.execute(
            """
            INSERT INTO shadow_dedup_guard (dedup_key, trace_id, symbol, action, trade_date)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (dedup_key) DO NOTHING
            RETURNING dedup_key
            """,
            [dedup_key, trace, fill.symbol, fill.action, td],
        ).fetchone()

        if not dedup_row:
            logger.warning(f'[ledger] duplicate blocked: {fill.symbol} dedup={dedup_key}')
            return False

        active_conn.execute(
            """
            INSERT INTO fact_shadow_ledger
            (timestamp, trade_date, trace_id, symbol, action,
             price_logical, price_shadow, qty, tide_mode,
             strategy_tag, slippage_cost, pricing_mode,
             gross_amount, commission, stamp_tax, transfer_fee, tax_total, net_amount)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                fill.timestamp,
                td,
                trace,
                fill.symbol,
                fill.action,
                fill.price_logical,
                fill.price_shadow,
                fill.qty,
                tide_mode,
                tag,
                fill.slippage_cost,
                fill.pricing_mode,
                fill.gross_amount,
                fill.commission,
                fill.stamp_tax,
                fill.transfer_fee,
                fill.tax_total,
                fill.net_amount,
            ],
        )
        return True

    try:
        if conn is not None:
            written = _write(conn)
        else:
            with DBGateway(DB_PATH, read_only=False, logger=logger) as owned_conn:
                owned_conn.execute('BEGIN TRANSACTION')
                try:
                    written = _write(owned_conn)
                    if written:
                        owned_conn.execute('COMMIT')
                    else:
                        owned_conn.execute('ROLLBACK')
                except Exception:
                    owned_conn.execute('ROLLBACK')
                    raise

        if written:
            logger.info(f'[ledger] fact_shadow_ledger write OK: {fill.symbol} trace={trace}')
        return written
    except Exception as exc:
        logger.warning(f'[ledger] fact_shadow_ledger write failed (non-blocking): {fill.symbol} | {exc}')
        return False

def preheat_adv20_cache() -> int:
    """Precompute ADV20 cache table for faster intraday simulation."""
    with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
        conn.execute('DROP TABLE IF EXISTS _shadow_adv20_cache')
        conn.execute(
            """
            CREATE TABLE _shadow_adv20_cache AS
            WITH ranked AS (
                SELECT symbol, vol, trade_date,
                       ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date DESC) AS rn
                FROM fact_daily
                WHERE vol > 0
            )
            SELECT
                symbol,
                AVG(vol) AS adv20,
                AVG(CASE WHEN rn <= 5 THEN vol END) AS avg_vol_5d,
                COUNT(*) AS data_days
            FROM ranked
            WHERE rn <= 20
            GROUP BY symbol
            HAVING COUNT(*) >= 10
            """
        )
        cnt = conn.execute('SELECT COUNT(*) FROM _shadow_adv20_cache').fetchone()[0]
        logger.info(f'[preheat] ADV20 cache built for {cnt} symbols')
        return int(cnt)
