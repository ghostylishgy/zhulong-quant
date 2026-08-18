#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
05_shadow/lib/news_entry_gate.py
Read-only news-observation gate before Shadow T+1 entry.

This module does not modify L4 verdicts, nexus_audits scores, RAG, or Shadow
state. Callers decide how to persist a blocked Shadow signal.
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import requests

try:
    from .db_contract import DBGateway, DB_PATH
except Exception:
    from db_contract import DBGateway, DB_PATH

BASE_DIR = Path('/root/quant_project')
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from config.settings import Config, ensure_syspath  # noqa: E402

ensure_syspath()

logger = logging.getLogger('shadow.news_entry_gate')

NEWS_REASON_RISK_CN = (
    "\u65b0\u95fb\u98ce\u9669\u62e6\u622a\uff1a{symbol} "
    "\u56e0\u516c\u544a/ST/\u76d1\u7ba1\u98ce\u9669\uff0c\u5df2\u8df3\u8fc7\u5f71\u5b50\u76d8\u4e70\u5165\u8ba1\u5212\u3002"
)
NEWS_REASON_CAUTION_CN = (
    "\u65b0\u95fb\u8c28\u614e\u62e6\u622a\uff1a{symbol} "
    "\u65b0\u95fb\u89c2\u5bdf\u63d0\u793a\u98ce\u9669\uff0cPASS \u4fe1\u53f7\u5df2\u964d\u4e3a\u89c2\u5bdf\uff0c\u8df3\u8fc7\u5f71\u5b50\u76d8\u4e70\u5165\u8ba1\u5212\u3002"
)
NEWS_REASON_UNAVAILABLE_CN = (
    "\u65b0\u95fb\u68c0\u67e5\u672a\u5b8c\u6210\uff1a{symbol} "
    "\u4e70\u5165\u524d\u65b0\u95fb\u89c2\u5bdf\u672a\u5b8c\u6210\u6216\u4e0d\u53ef\u7528\uff0c\u5df2\u6309 fail-closed \u8df3\u8fc7\u5f71\u5b50\u76d8\u4e70\u5165\u8ba1\u5212\u3002"
)
NEWS_PUSH_TITLE_CN = "\u70db\u9f99\u5f71\u5b50\u76d8\u65b0\u95fb\u98ce\u9669\u62e6\u622a"

CRITICAL_STATUSES = {'NEWS_CRITICAL_CANDIDATE'}
CRITICAL_GATES = {'WOULD_VETO'}
CRITICAL_LEVELS = {'CRITICAL', 'CRITICAL_CANDIDATE'}
CAUTION_STATUSES = {'NEWS_CAUTION'}
CAUTION_GATES = {'WOULD_CAP_HOLD'}
CAUTION_LEVELS = {'CAUTION', 'CAUTION_CANDIDATE', 'HIGH'}
ALLOW_STATUSES = {'NEWS_CLEAR', 'NEWS_SIGNAL'}
UNAVAILABLE_STATUSES = {
    '',
    'NEWS_QUEUED',
    'NEWS_UNAVAILABLE',
    'NEWS_PENDING',
    'NEWS_PARTIAL',
    'NEWS_FAILED',
    'NEWS_ERROR',
}

NEWS_FIELD_COLUMNS = [
    'task_id',
    'run_id',
    'symbol',
    'name',
    'trade_date',
    'l4_final_verdict',
    'final_score',
    'l4_news_status',
    'l4_news_gate',
    'l4_news_risk_level',
    'l4_news_risk_score',
    'l4_news_summary',
    'l4_news_evidence',
    'l4_news_as_of',
    'l4_news_checked_at',
    'l4_news_policy',
    'l4_news_prompt_injected',
    'l4_news_gate_applied',
]


def _upper(value: Any) -> str:
    return str(value or '').strip().upper()


def _safe_float(value: Any) -> float:
    try:
        if value is None or value == '':
            return 0.0
        return float(value)
    except Exception:
        return 0.0


def _safe_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or '').strip().lower()
    return text in {'1', 'true', 'yes', 'y'}


def _shorten(text: Any, limit: int = 2500) -> str:
    value = str(text or '')
    if len(value) <= limit:
        return value
    return value[:limit] + '...<truncated>'


def _extract_negative_tags(raw_evidence: Any) -> list[str]:
    if not raw_evidence:
        return []
    try:
        data = json.loads(raw_evidence) if isinstance(raw_evidence, str) else raw_evidence
    except Exception:
        return []
    tags: list[str] = []
    if isinstance(data, dict):
        for key in ('negative_tags', 'risk_tags', 'tags'):
            raw_tags = data.get(key)
            if isinstance(raw_tags, list):
                tags.extend(str(x) for x in raw_tags if x)
        items = data.get('items') or data.get('evidence') or []
        if isinstance(items, list):
            for item in items:
                if isinstance(item, dict):
                    raw_tags = item.get('negative_tags') or item.get('risk_tags') or item.get('tags') or []
                    if isinstance(raw_tags, list):
                        tags.extend(str(x) for x in raw_tags if x)
    return sorted(set(tags))[:20]


def fetch_news_gate_context(signal: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Load the news observation fields for a Shadow candidate from nexus_audits."""
    task_id = str(signal.get('task_id') or '').strip()
    symbol = str(signal.get('symbol') or '').strip().upper()
    trade_date = str(signal.get('trade_date') or signal.get('signal_trade_date') or '')[:10]
    if not task_id and not symbol:
        return None

    select_sql = ', '.join(NEWS_FIELD_COLUMNS)
    clauses = []
    params: list[Any] = []
    if task_id:
        clauses.append('task_id = ?')
        params.append(task_id)
    if symbol and trade_date:
        clauses.append('(symbol = ? AND CAST(trade_date AS DATE) = CAST(? AS DATE))')
        params.extend([symbol, trade_date])
    elif symbol:
        clauses.append('symbol = ?')
        params.append(symbol)
    if not clauses:
        return None

    try:
        with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
            row = conn.execute(
                f"""
                SELECT {select_sql}
                FROM nexus_audits
                WHERE {' OR '.join(clauses)}
                ORDER BY
                    CASE WHEN task_id = ? THEN 0 ELSE 1 END,
                    created_at DESC,
                    task_id DESC
                LIMIT 1
                """,
                params + [task_id],
            ).fetchone()
        if not row:
            return None
        return {NEWS_FIELD_COLUMNS[i]: row[i] for i in range(len(NEWS_FIELD_COLUMNS))}
    except Exception as exc:
        logger.error('[NewsGate] failed to load news gate context | task=%s symbol=%s err=%s', task_id, symbol, exc, exc_info=True)
        return None


def evaluate_news_fields(fields: Optional[Dict[str, Any]], *, symbol: str = '') -> Dict[str, Any]:
    """Pure policy evaluation for the Shadow pre-buy news gate."""
    data = dict(fields or {})
    symbol = str(symbol or data.get('symbol') or '').strip().upper()
    status = _upper(data.get('l4_news_status'))
    gate = _upper(data.get('l4_news_gate'))
    risk_level = _upper(data.get('l4_news_risk_level'))
    risk_score = _safe_float(data.get('l4_news_risk_score'))
    verdict = _upper(data.get('l4_final_verdict'))
    evidence_raw = data.get('l4_news_evidence')
    negative_tags = _extract_negative_tags(evidence_raw)

    evidence = {
        'gate_context': 'shadow_t1_pre_buy_news_gate',
        'task_id': str(data.get('task_id') or ''),
        'run_id': str(data.get('run_id') or ''),
        'symbol': symbol,
        'trade_date': str(data.get('trade_date') or '')[:10],
        'original_l4_verdict': verdict,
        'original_final_score': _safe_float(data.get('final_score')),
        'l4_news_status': status,
        'l4_news_gate': gate,
        'l4_news_risk_level': risk_level,
        'l4_news_risk_score': risk_score,
        'l4_news_summary': str(data.get('l4_news_summary') or ''),
        'l4_news_as_of': str(data.get('l4_news_as_of') or data.get('l4_news_checked_at') or ''),
        'l4_news_policy': str(data.get('l4_news_policy') or ''),
        'l4_news_prompt_injected': _safe_bool(data.get('l4_news_prompt_injected')),
        'l4_news_gate_applied': _safe_bool(data.get('l4_news_gate_applied')),
        'l4_news_negative_tags': negative_tags,
        'l4_news_evidence_preview': _shorten(evidence_raw),
    }

    if status in CRITICAL_STATUSES or gate in CRITICAL_GATES or risk_level in CRITICAL_LEVELS:
        reason = 'SKIPPED_NEWS_RISK'
        reason_cn = NEWS_REASON_RISK_CN.format(symbol=symbol or 'UNKNOWN')
        return {
            'allow': False,
            'reason': reason,
            'reason_cn': reason_cn,
            'news_label': 'NEWS_CRITICAL',
            'evidence': {**evidence, 'news_gate_decision': reason, 'news_reason_cn': reason_cn},
        }

    if status in CAUTION_STATUSES or gate in CAUTION_GATES or risk_level in CAUTION_LEVELS:
        reason = 'SKIPPED_NEWS_CAUTION'
        reason_cn = NEWS_REASON_CAUTION_CN.format(symbol=symbol or 'UNKNOWN')
        return {
            'allow': False,
            'reason': reason,
            'reason_cn': reason_cn,
            'news_label': 'NEWS_CAUTION',
            'evidence': {**evidence, 'news_gate_decision': reason, 'news_reason_cn': reason_cn},
        }

    if status in ALLOW_STATUSES:
        reason = status or 'NEWS_CLEAR'
        return {
            'allow': True,
            'reason': reason,
            'reason_cn': '',
            'news_label': status,
            'evidence': {**evidence, 'news_gate_decision': 'ALLOW_' + reason},
        }

    if status in UNAVAILABLE_STATUSES or not status:
        reason = 'SKIPPED_NEWS_UNAVAILABLE'
    else:
        reason = 'SKIPPED_NEWS_UNAVAILABLE'
    reason_cn = NEWS_REASON_UNAVAILABLE_CN.format(symbol=symbol or 'UNKNOWN')
    return {
        'allow': False,
        'reason': reason,
        'reason_cn': reason_cn,
        'news_label': 'NEWS_UNAVAILABLE',
        'evidence': {**evidence, 'news_gate_decision': reason, 'news_reason_cn': reason_cn},
    }


def evaluate_shadow_news_entry_gate(signal: Dict[str, Any]) -> Dict[str, Any]:
    """Read nexus_audits and evaluate whether a Shadow entry may proceed."""
    context = fetch_news_gate_context(signal)
    if context is None:
        context = {
            'task_id': signal.get('task_id'),
            'run_id': signal.get('run_id'),
            'symbol': signal.get('symbol'),
            'name': signal.get('name'),
            'trade_date': signal.get('trade_date') or signal.get('signal_trade_date'),
            'l4_final_verdict': signal.get('l4_final_verdict') or signal.get('final_verdict') or 'PASS',
            'final_score': signal.get('final_score'),
            'l4_news_status': '',
            'l4_news_gate': '',
            'l4_news_risk_level': '',
            'l4_news_risk_score': 0,
            'l4_news_summary': 'nexus_audits news context not found',
            'l4_news_evidence': '',
            'l4_news_as_of': '',
            'l4_news_policy': '',
            'l4_news_prompt_injected': False,
            'l4_news_gate_applied': False,
        }
    return evaluate_news_fields(context, symbol=str(signal.get('symbol') or context.get('symbol') or ''))


def push_shadow_news_gate_skip(signal: Dict[str, Any], decision: Dict[str, Any], *, stage: str) -> None:
    """Send a concise Chinese push for a blocked Shadow entry, if PushPlus is configured."""
    if decision.get('allow'):
        return
    token = getattr(Config, 'PUSHPLUS_TOKEN', '') or getattr(Config, 'PUSH_PLUS_TOKEN', '') or ''
    if not token:
        return
    symbol = str(signal.get('symbol') or decision.get('evidence', {}).get('symbol') or '').strip().upper()
    name = str(signal.get('name') or '')
    reason_cn = str(decision.get('reason_cn') or NEWS_REASON_UNAVAILABLE_CN.format(symbol=symbol or 'UNKNOWN'))
    evidence = decision.get('evidence') or {}
    title = f'{NEWS_PUSH_TITLE_CN} | {symbol}' if symbol else NEWS_PUSH_TITLE_CN
    content = '\n'.join(
        [
            reason_cn,
            f'\u6807\u7684\uff1a{symbol} {name}'.strip(),
            f'\u9636\u6bb5\uff1a{stage}',
            f"\u65b0\u95fb\u72b6\u6001\uff1a{evidence.get('l4_news_status', '')} / {evidence.get('l4_news_gate', '')}",
            f"\u98ce\u9669\u7b49\u7ea7\uff1a{evidence.get('l4_news_risk_level', '')} score={evidence.get('l4_news_risk_score', '')}",
            f"as_of\uff1a{evidence.get('l4_news_as_of', '')}",
            f"\u6458\u8981\uff1a{evidence.get('l4_news_summary', '')}",
            '\u672c\u6b21\u4ec5\u62e6\u622a\u5f71\u5b50\u76d8\u4e70\u5165\u8ba1\u5212\uff0c\u4e0d\u4fee\u6539 L4 verdict\u3002',
        ]
    )
    try:
        requests.post(
            str(getattr(Config, 'PUSHPLUS_URL', 'https://www.pushplus.plus/send')),
            json={'token': token, 'title': title, 'content': content[:2000], 'template': 'txt'},
            timeout=int(getattr(Config, 'PUSHPLUS_TIMEOUT', 10) or 10),
        )
    except Exception as exc:
        logger.warning('[NewsGate] push failed | %s %s', symbol, exc)
