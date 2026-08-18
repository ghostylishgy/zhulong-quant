#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Trade-cost accounting for Shadow paper fills.

All fees are calculated per execution. This preserves the non-linear effect of
the minimum commission on partial exits and small batches.
"""

from __future__ import annotations

from dataclasses import dataclass

try:
    from .shadow_config import trade_cost_rules
except Exception:
    from shadow_config import trade_cost_rules


@dataclass(frozen=True)
class TradeCost:
    gross_amount: float = 0.0
    commission: float = 0.0
    stamp_tax: float = 0.0
    transfer_fee: float = 0.0
    tax_total: float = 0.0
    net_amount: float = 0.0


def calculate_trade_cost(action: str, price: float = 0.0, qty: int = 0, gross_amount: float | None = None) -> TradeCost:
    action_u = str(action or '').strip().upper()
    gross = float(gross_amount) if gross_amount is not None else float(price or 0) * int(qty or 0)
    gross = round(max(0.0, gross), 2)
    if gross <= 0:
        return TradeCost()

    cfg = trade_cost_rules()
    if not bool(cfg.get('enabled', True)):
        return TradeCost(gross_amount=gross, net_amount=gross)

    commission_rate = float(cfg.get('commission_rate', 0.00025) or 0.0)
    commission_min = float(cfg.get('commission_min', 5.0) or 0.0)
    stamp_rate = float(cfg.get('stamp_tax_rate_sell', 0.0005) or 0.0)
    transfer_rate = float(cfg.get('transfer_fee_rate', 0.00001) or 0.0)

    commission = max(round(gross * commission_rate, 2), round(commission_min, 2))
    stamp_tax = round(gross * stamp_rate, 2) if action_u == 'SELL' else 0.0
    transfer_fee = round(gross * transfer_rate, 2)
    tax_total = round(commission + stamp_tax + transfer_fee, 2)
    if action_u == 'SELL':
        net_amount = round(gross - tax_total, 2)
    else:
        net_amount = round(gross + tax_total, 2)
    return TradeCost(
        gross_amount=gross,
        commission=commission,
        stamp_tax=stamp_tax,
        transfer_fee=transfer_fee,
        tax_total=tax_total,
        net_amount=net_amount,
    )


def max_affordable_buy_qty(price: float, allocated_cash: float, lot_size: int = 100) -> int:
    """Return max round-lot BUY qty whose gross + fees fits allocated_cash."""
    if price <= 0 or allocated_cash <= 0:
        return 0
    qty = int(allocated_cash / price) // lot_size * lot_size
    while qty >= lot_size:
        if calculate_trade_cost('BUY', price=price, qty=qty).net_amount <= allocated_cash:
            return qty
        qty -= lot_size
    return 0
