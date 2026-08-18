#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
02_brain/macro_v2.py
Whale Tracker pre-research module.

Scope:
- Define a standard report-ingestion interface.
- Hold valuation percentile snapshots.
- Produce a preliminary high/low-level classification signal.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Protocol


class ReportProvider(Protocol):
    """External report data source contract."""

    def fetch_reports(self, symbol: str, trade_date: str) -> List[Dict[str, Any]]:
        ...


class ValuationProvider(Protocol):
    """External valuation percentile source contract."""

    def fetch_percentile(self, symbol: str, trade_date: str) -> Optional[float]:
        ...


@dataclass
class ResearchReport:
    source: str
    title: str
    published_at: str
    content: str
    url: str = ''

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> 'ResearchReport':
        return cls(
            source=str(payload.get('source') or 'unknown'),
            title=str(payload.get('title') or '').strip(),
            published_at=str(payload.get('published_at') or '').strip(),
            content=str(payload.get('content') or '').strip(),
            url=str(payload.get('url') or '').strip(),
        )


@dataclass
class WhaleSignal:
    symbol: str
    trade_date: str
    valuation_percentile: Optional[float]
    level: str
    confidence: float
    reason: str
    report_count: int


class WhaleTracker:
    """
    Preliminary whale-tracking engine.

    Notes:
    - This is an interface-first scaffold.
    - Final alpha logic can extend this class without breaking callers.
    """

    def __init__(
        self,
        *,
        high_cutoff: float = 80.0,
        low_cutoff: float = 20.0,
        min_report_count: int = 1,
    ) -> None:
        self.high_cutoff = float(high_cutoff)
        self.low_cutoff = float(low_cutoff)
        self.min_report_count = max(1, int(min_report_count))
        self._reports_by_symbol: Dict[str, List[ResearchReport]] = {}
        self._valuation_by_symbol: Dict[str, float] = {}
        self._valuation_asof: Dict[str, str] = {}

    @staticmethod
    def _norm_symbol(symbol: str) -> str:
        return str(symbol or '').strip().upper()

    @staticmethod
    def _clamp_percentile(percentile: Optional[float]) -> Optional[float]:
        if percentile is None:
            return None
        value = float(percentile)
        if value < 0:
            return 0.0
        if value > 100:
            return 100.0
        return value

    def ingest_reports(self, symbol: str, reports: Iterable[Dict[str, Any]]) -> int:
        """Standard ingestion interface for research reports."""
        sym = self._norm_symbol(symbol)
        normalized: List[ResearchReport] = []
        for item in reports:
            rep = ResearchReport.from_dict(item)
            if rep.title or rep.content:
                normalized.append(rep)
        self._reports_by_symbol[sym] = normalized
        return len(normalized)

    def set_valuation_percentile(self, symbol: str, percentile: Optional[float], trade_date: str) -> None:
        """Set valuation percentile snapshot (0~100)."""
        sym = self._norm_symbol(symbol)
        value = self._clamp_percentile(percentile)
        if value is None:
            self._valuation_by_symbol.pop(sym, None)
            self._valuation_asof.pop(sym, None)
            return
        self._valuation_by_symbol[sym] = value
        self._valuation_asof[sym] = str(trade_date or '').strip() or datetime.now().strftime('%Y-%m-%d')

    def sync_from_providers(
        self,
        *,
        symbol: str,
        trade_date: str,
        report_provider: ReportProvider,
        valuation_provider: Optional[ValuationProvider] = None,
    ) -> WhaleSignal:
        """One-step provider sync for upstream orchestration."""
        reports = report_provider.fetch_reports(symbol=symbol, trade_date=trade_date)
        self.ingest_reports(symbol, reports)
        if valuation_provider is not None:
            self.set_valuation_percentile(
                symbol,
                valuation_provider.fetch_percentile(symbol=symbol, trade_date=trade_date),
                trade_date,
            )
        return self.evaluate_level(symbol=symbol, trade_date=trade_date)

    def evaluate_level(self, *, symbol: str, trade_date: str) -> WhaleSignal:
        """Core high/low-level decision based on valuation percentile."""
        sym = self._norm_symbol(symbol)
        percentile = self._valuation_by_symbol.get(sym)
        reports = self._reports_by_symbol.get(sym, [])
        report_count = len(reports)

        if percentile is None:
            return WhaleSignal(
                symbol=sym,
                trade_date=str(trade_date),
                valuation_percentile=None,
                level='UNKNOWN',
                confidence=0.0,
                reason='valuation percentile missing',
                report_count=report_count,
            )

        if percentile >= self.high_cutoff:
            level = 'HIGH_ZONE'
            reason = f'valuation_percentile={percentile:.1f} >= high_cutoff={self.high_cutoff:.1f}'
        elif percentile <= self.low_cutoff:
            level = 'LOW_ZONE'
            reason = f'valuation_percentile={percentile:.1f} <= low_cutoff={self.low_cutoff:.1f}'
        else:
            level = 'MID_ZONE'
            reason = f'valuation_percentile={percentile:.1f} between [{self.low_cutoff:.1f}, {self.high_cutoff:.1f}]'

        coverage = min(1.0, report_count / float(self.min_report_count))
        distance = abs(percentile - 50.0) / 50.0
        confidence = min(0.99, round(0.35 + 0.45 * distance + 0.20 * coverage, 3))
        if report_count < self.min_report_count:
            reason += f'; reports_insufficient={report_count}/{self.min_report_count}'

        return WhaleSignal(
            symbol=sym,
            trade_date=str(trade_date),
            valuation_percentile=percentile,
            level=level,
            confidence=confidence,
            reason=reason,
            report_count=report_count,
        )

    def build_payload(self, *, symbol: str, trade_date: str, report_limit: int = 5) -> Dict[str, Any]:
        """Unified output payload for downstream strategy/risk modules."""
        signal = self.evaluate_level(symbol=symbol, trade_date=trade_date)
        sym = self._norm_symbol(symbol)
        reports = self._reports_by_symbol.get(sym, [])[: max(1, int(report_limit))]
        return {
            'symbol': signal.symbol,
            'trade_date': signal.trade_date,
            'valuation': {
                'percentile': signal.valuation_percentile,
                'asof': self._valuation_asof.get(sym, ''),
                'high_cutoff': self.high_cutoff,
                'low_cutoff': self.low_cutoff,
                'level': signal.level,
                'confidence': signal.confidence,
                'reason': signal.reason,
            },
            'reports': [
                {
                    'source': r.source,
                    'title': r.title,
                    'published_at': r.published_at,
                    'url': r.url,
                    'content': r.content,
                }
                for r in reports
            ],
            'report_count': signal.report_count,
        }


__all__ = [
    'ReportProvider',
    'ValuationProvider',
    'ResearchReport',
    'WhaleSignal',
    'WhaleTracker',
]
