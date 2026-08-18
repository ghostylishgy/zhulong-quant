#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Shared Shadow configuration loader.

This is the single source for Shadow execution thresholds and trade-cost
parameters. Callers should not hardcode stop-loss or fee values.
"""

from __future__ import annotations

import logging
import os
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict

import yaml

logger = logging.getLogger('shadow.config')

BASE_DIR = Path('/root/quant_project')
RULES_PATH = BASE_DIR / '05_shadow' / 'config' / 'rules.yaml'

DEFAULT_RULES: Dict[str, Any] = {
    'cooldown': {'max_daily_trades': 6, 'same_symbol_min_interval_days': 3},
    'position': {
        'initial_capital': 1_000_000.0,
        'max_positions': 4,
        'max_single_pct': 0.35,
        'min_single_pct': 0.10,
        'trailing_stop_pct': 0.03,
    },
    'sell_rules': {
        'danger_loss_ratio': -0.05,
        'first_take_profit_ratio': 0.08,
        'second_take_profit_ratio': 0.12,
        'profit_lock_floor_ratio': 0.08,
        'profit_lock_trim_drawdown': 0.035,
        'profit_lock_clear_drawdown': 0.05,
        'profit_lock_runner_max_pct': 0.10,
        'wrong_pick_stop_ratio': -0.05,
        'opening_extreme_stop_ratio': -0.07,
        'stop_loss_confirm_mode': 'intraday',
    },
    'quote_health': {
        'alert_after_0930_failures': 3,
        'raw_snapshot_limit': 3000,
    },
    'liquidation': {
        'profit_take_participation': 0.10,
        'protective_stop_participation': 0.30,
        'panic_stop_participation': 0.50,
        'small_order_full_qty': 5000,
        'protective_impact_multiplier': 1.5,
        'panic_impact_multiplier': 2.5,
        'target_minutes': 30,
    },
    'trade_costs': {
        'enabled': True,
        'commission_rate': 0.00025,
        'commission_min': 5.0,
        'stamp_tax_rate_sell': 0.0005,
        'transfer_fee_rate': 0.00001,
    },
    'slippage': {
        'alpha': 0.1,
        'beta': 0.05,
    },
}


def _merge_dict(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged = deepcopy(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_dict(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_shadow_rules() -> Dict[str, Any]:
    try:
        with open(RULES_PATH, 'r', encoding='utf-8') as f:
            loaded = yaml.safe_load(f) or {}
        rules = _merge_dict(DEFAULT_RULES, loaded)
    except Exception as exc:
        logger.warning('rules.yaml load failed: %s, using defaults', exc)
        rules = deepcopy(DEFAULT_RULES)

    mode_override = str(os.getenv('STOP_LOSS_CONFIRM_MODE', '') or '').strip().lower()
    if mode_override:
        rules.setdefault('sell_rules', {})['stop_loss_confirm_mode'] = mode_override
    return rules


def trade_cost_rules() -> Dict[str, Any]:
    return dict(load_shadow_rules().get('trade_costs') or {})


def sell_rules() -> Dict[str, Any]:
    return dict(load_shadow_rules().get('sell_rules') or {})


def position_rules() -> Dict[str, Any]:
    return dict(load_shadow_rules().get('position') or {})


def quote_health_rules() -> Dict[str, Any]:
    return dict(load_shadow_rules().get('quote_health') or {})


def liquidation_rules() -> Dict[str, Any]:
    return dict(load_shadow_rules().get('liquidation') or {})
