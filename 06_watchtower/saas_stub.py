#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
06_watchtower/saas_stub.py
SaaS ghost hooks (stub only):
- Auth stub: resolve current user from headers/token.
- Billing stub: token reserve/consume mock with debug logs.
"""

from __future__ import annotations

import logging
from typing import Dict, Optional

from fastapi import Header

logger = logging.getLogger('watchtower.saas_stub')


def _extract_user_from_auth(authorization: Optional[str]) -> str:
    text = str(authorization or '').strip()
    if not text:
        return ''
    if text.lower().startswith('bearer '):
        return text[7:].strip()
    return text


def get_current_user(
    x_touchstone_user: Optional[str] = Header(default=None, alias='X-Touchstone-User'),
    x_user_id: Optional[str] = Header(default=None, alias='X-User-Id'),
    authorization: Optional[str] = Header(default=None, alias='Authorization'),
) -> Dict[str, str]:
    """
    Auth stub:
    - Prefer user id from headers/token.
    - Fallback to local admin when missing.
    """
    user_id = (
        str(x_touchstone_user or '').strip()
        or str(x_user_id or '').strip()
        or _extract_user_from_auth(authorization)
    )
    if not user_id:
        return {'user_id': 'local_admin', 'plan': 'premium'}
    return {'user_id': user_id, 'plan': 'premium'}


class TokenLedgerMock:
    """
    Billing stub:
    - Reserve/consume always return True.
    - Print debug logs for future token-ledger replacement.
    """

    def reserve(self, user_id: str, token_amount: int) -> bool:
        amount = int(token_amount or 0)
        logger.debug('[SaaS Stub] user=%s reserve=%s', user_id, amount)
        print(f'[SaaS Stub] 模拟扣除 Token: {amount}')
        return True

    def consume(self, user_id: str = '', token_amount: int = 0) -> bool:
        amount = int(token_amount or 0)
        logger.debug('[SaaS Stub] user=%s consume=%s', user_id, amount)
        print(f'[SaaS Stub] 模拟扣除 Token: {amount}')
        return True
