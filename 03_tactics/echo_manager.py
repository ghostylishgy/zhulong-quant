#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
03_tactics/echo_manager.py
Echo System - Micro-Timing Repair Mechanism

Architecture:
    L4 REJECT (Technical) -> incubate() -> INCUBATING
    Physics Sensor check  -> READY / EXPIRED
    Tide + Sector check   -> SUSPEND / awaken()
    Awaken -> L4 re-audit with Shadow Bonus +0.05

States: INCUBATING -> READY -> AWAKENED / SUSPEND / EXPIRED

RED LINES:
    - Fundamental Risk rejects are NEVER incubated
    - Only OVERBOUGHT / HIGH_FRICTION qualify
"""

import hashlib
import logging
import pandas as pd
from datetime import datetime
from dataclasses import dataclass
from typing import Optional, List, Dict, Any

import urllib.request
import json as _json
import runpy
import sys
from pathlib import Path

TACTICS_DIR = Path(__file__).resolve().parent
if str(TACTICS_DIR) not in sys.path:
    sys.path.insert(0, str(TACTICS_DIR))

from db_contract import PROJECT_ROOT, DB_PATH, DBGateway

logger = logging.getLogger('zhulong.echo_manager')


# ???????????????????????????????????????????????????????????????
# Node-103 ??? RAG ???? (Qdrant Vector Search)
# ???????????????????????????????????????????????????????????????

QDRANT_HOST = '192.0.2.30'
QDRANT_PORT = 6333
QDRANT_COLLECTION = 'zhulong_memory'
QDRANT_TIMEOUT = 3  # ?, ???????

_qdrant_logger = logging.getLogger('zhulong.echo.qdrant')


def qdrant_search(query_vector: list, top_k: int = 5, score_threshold: float = 0.6) -> list:
    """
    ? Node-103 Qdrant ?????????

    Returns:
        [{'id': str, 'score': float, 'payload': dict}, ...]
    """
    url = f'http://{QDRANT_HOST}:{QDRANT_PORT}/collections/{QDRANT_COLLECTION}/points/search'

    payload = _json.dumps(
        {
            'vector': query_vector,
            'limit': top_k,
            'score_threshold': score_threshold,
            'with_payload': True,
        }
    ).encode('utf-8')

    req = urllib.request.Request(
        url,
        data=payload,
        headers={'Content-Type': 'application/json'},
        method='POST',
    )

    try:
        with urllib.request.urlopen(req, timeout=QDRANT_TIMEOUT) as resp:
            data = _json.loads(resp.read().decode('utf-8'))
            results = data.get('result', [])
            _qdrant_logger.info(
                f'[Qdrant] ????: {len(results)} ? (top_score={results[0]["score"]:.3f})'
                if results
                else '[Qdrant] ????: 0 ?'
            )
            return results
    except urllib.error.URLError as e:
        _qdrant_logger.warning(f'[Qdrant] Node-103 ????: {e} ? ????')
        return []
    except Exception as e:
        _qdrant_logger.warning(f'[Qdrant] ????: {e} ? ????')
        return []


def qdrant_upsert(point_id: str, vector: list, payload: dict) -> bool:
    """? Node-103 ??/??????"""
    url = f'http://{QDRANT_HOST}:{QDRANT_PORT}/collections/{QDRANT_COLLECTION}/points'

    body = _json.dumps(
        {
            'points': [
                {
                    'id': int(hashlib.sha256(str(point_id).encode('utf-8')).hexdigest()[:16], 16) % (2**63),
                    'vector': vector,
                    'payload': {**payload, 'source_id': point_id},
                }
            ]
        }
    ).encode('utf-8')

    req = urllib.request.Request(
        url,
        data=body,
        headers={'Content-Type': 'application/json'},
        method='PUT',
    )

    try:
        with urllib.request.urlopen(req, timeout=QDRANT_TIMEOUT):
            _qdrant_logger.info(f'[Qdrant] ????: {point_id}')
            return True
    except Exception as e:
        _qdrant_logger.warning(f'[Qdrant] ????: {e}')
        return False


def qdrant_health_check() -> dict:
    """?? Node-103 Qdrant ??????"""
    url = f'http://{QDRANT_HOST}:{QDRANT_PORT}/collections/{QDRANT_COLLECTION}'
    try:
        req = urllib.request.Request(url, method='GET')
        with urllib.request.urlopen(req, timeout=QDRANT_TIMEOUT) as resp:
            data = _json.loads(resp.read().decode('utf-8'))
            result = data.get('result', {})
            return {
                'online': True,
                'status': result.get('status', 'unknown'),
                'vectors_count': result.get('vectors_count', 0),
                'host': f'{QDRANT_HOST}:{QDRANT_PORT}',
            }
    except Exception as e:
        return {
            'online': False,
            'status': str(e),
            'vectors_count': 0,
            'host': f'{QDRANT_HOST}:{QDRANT_PORT}',
        }


# Shadow Bonus injected into L4 re-audit
SHADOW_BONUS = 0.05

# Technical reject reasons that qualify for echo incubation
ECHO_ELIGIBLE_REASONS = {'OVERBOUGHT', 'HIGH_FRICTION'}

# Fundamental reasons that are STRICTLY FORBIDDEN from echo
ECHO_FORBIDDEN_KEYWORDS = {
    'fundamental',
    'fraud',
    'delisting',
    'st_risk',
    'debt_crisis',
    'regulatory',
    'governance',
}

# TTL initial value (T+2 decay)
DEFAULT_TTL = 2


@dataclass
class EchoEntry:
    """Single echo log entry"""

    fingerprint: str
    symbol: str
    trade_date: str
    current_state: str = 'INCUBATING'
    ttl_counter: int = DEFAULT_TTL
    veto_reason: str = ''
    price_at_entry: float = 0.0
    vol_ma5_at_entry: float = 0.0
    original_score: int = 0
    industry: str = ''
    created_at: str = ''
    updated_at: str = ''


@dataclass
class PhysicsCheckResult:
    """Result of the physics sensor check"""

    passed: bool = False
    price_in_band: bool = False
    bias_ok: bool = False
    vol_contraction_ok: bool = False
    current_close: float = 0.0
    ma5: float = 0.0
    bias5: float = 0.0
    vol_ratio: float = 0.0
    reason: str = ''


class EchoManager:
    """
    Echo System Controller

    Lifecycle:
        1. incubate()  - L4 REJECT (technical only) -> create INCUBATING entry
        2. scan_ready() - Physics sensor -> promote INCUBATING -> READY
        3. attempt_awaken() - Tide check + L4 re-audit with shadow bonus
        4. decay_ttl() - Daily T+2 countdown -> EXPIRED
    """

    def __init__(self):
        self._ensure_table()

    def _ensure_table(self):
        """Initialize fact_echo_logs table in DuckDB"""
        with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS fact_echo_logs (
                    fingerprint      VARCHAR PRIMARY KEY,
                    symbol           VARCHAR NOT NULL,
                    trade_date       DATE NOT NULL,
                    current_state    VARCHAR DEFAULT 'INCUBATING',
                    ttl_counter      INTEGER DEFAULT 2,
                    veto_reason      VARCHAR,
                    price_at_entry   DOUBLE,
                    vol_ma5_at_entry DOUBLE,
                    original_score   INTEGER DEFAULT 0,
                    industry         VARCHAR DEFAULT '',
                    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_echo_symbol
                ON fact_echo_logs(symbol)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_echo_state
                ON fact_echo_logs(current_state)
                """
            )
        logger.info('fact_echo_logs table ready')

    @staticmethod
    def _make_fingerprint(symbol: str, trade_date: str, reason: str) -> str:
        raw = f'{symbol}_{trade_date}_{reason}'
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    @staticmethod
    def is_echo_eligible(veto_reason: str) -> bool:
        """RED LINE: fundamental risk is NEVER eligible."""
        reason_upper = (veto_reason or '').upper()

        for forbidden in ECHO_FORBIDDEN_KEYWORDS:
            if forbidden.upper() in reason_upper:
                return False

        for eligible in ECHO_ELIGIBLE_REASONS:
            if eligible in reason_upper:
                return True

        if 'BLUE VETO' in reason_upper:
            for forbidden in ECHO_FORBIDDEN_KEYWORDS:
                if forbidden.upper() in reason_upper:
                    return False
            if any(kw in reason_upper for kw in ['SCORE', 'OVERHEAT', 'OVERBOUGHT', 'FRICTION']):
                return True

        return False

    def incubate(
        self,
        symbol: str,
        trade_date: str,
        veto_reason: str,
        price: float = 0.0,
        vol_ma5: float = 0.0,
        original_score: int = 0,
        industry: str = '',
    ) -> Optional[str]:
        """Incubate a technically-rejected symbol."""
        if not self.is_echo_eligible(veto_reason):
            logger.info(f"ECHO REJECT: {symbol} reason='{veto_reason}' is fundamental, NOT eligible")
            return None

        fingerprint = self._make_fingerprint(symbol, trade_date, veto_reason)
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
            existing = conn.execute(
                """
                SELECT fingerprint, current_state
                FROM fact_echo_logs
                WHERE symbol = ?
                  AND current_state IN ('INCUBATING', 'READY')
                """,
                [symbol],
            ).fetchall()

            if existing:
                logger.info(f'ECHO DEDUP: {symbol} already has active entry {existing[0][0]}, skipping')
                return None

            conn.execute(
                """
                INSERT INTO fact_echo_logs (
                    fingerprint, symbol, trade_date, current_state, ttl_counter,
                    veto_reason, price_at_entry, vol_ma5_at_entry, original_score,
                    industry, created_at, updated_at
                )
                VALUES (?, ?, CAST(? AS DATE), 'INCUBATING', ?, ?, ?, ?, ?, ?, CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP))
                """,
                [
                    fingerprint,
                    symbol,
                    trade_date,
                    DEFAULT_TTL,
                    veto_reason,
                    price,
                    vol_ma5,
                    original_score,
                    industry,
                    now,
                    now,
                ],
            )

        logger.info(f"ECHO INCUBATE: {symbol} fp={fingerprint} reason='{veto_reason}' TTL={DEFAULT_TTL}")
        return fingerprint

    def check_physics(self, symbol: str, trade_date: str = None) -> PhysicsCheckResult:
        """
        Physics Three Laws Sensor:
            1. Price Correction: MA5 <= P <= MA5 * 1.02
            2. Bias5 <= 2%
            3. Volume Contraction: 0.2 <= Vol/Vol_ma5 < 0.6
        """
        result = PhysicsCheckResult()

        with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
            if not trade_date:
                td = conn.execute('SELECT MAX(trade_date) FROM fact_daily').fetchone()[0]
                trade_date = td.strftime('%Y-%m-%d') if hasattr(td, 'strftime') else str(td)

            df = conn.execute(
                """
                SELECT trade_date, close, vol AS volume
                FROM fact_daily
                WHERE symbol = ?
                  AND trade_date <= CAST(? AS DATE)
                ORDER BY trade_date DESC
                LIMIT 10
                """,
                [symbol, trade_date],
            ).df()

        if len(df) < 5:
            result.reason = f'Insufficient data: {len(df)} days'
            return result

        df = df.sort_values('trade_date').reset_index(drop=True)
        latest = df.iloc[-1]

        ma5 = df['close'].tail(5).mean()
        vol_ma5 = df['volume'].tail(5).mean()
        current_close = float(latest['close'])
        current_vol = float(latest['volume'])

        result.current_close = current_close
        result.ma5 = ma5

        result.price_in_band = ma5 <= current_close <= ma5 * 1.02

        bias5 = (current_close - ma5) / ma5 if ma5 > 0 else 999
        result.bias5 = bias5
        result.bias_ok = abs(bias5) <= 0.02

        vol_ratio = current_vol / vol_ma5 if vol_ma5 > 0 else 999
        result.vol_ratio = vol_ratio
        result.vol_contraction_ok = 0.2 <= vol_ratio < 0.6

        if vol_ratio >= 1.0 and current_close < df.iloc[-2]['close']:
            result.reason = f'HARD_KILL: volume expansion ({vol_ratio:.2f}x) + price drop'
            result.passed = False
            return result

        result.passed = result.price_in_band and result.bias_ok and result.vol_contraction_ok
        if not result.passed:
            failures = []
            if not result.price_in_band:
                failures.append(f'price={current_close:.2f} vs MA5={ma5:.2f}')
            if not result.bias_ok:
                failures.append(f'bias5={bias5*100:.2f}%')
            if not result.vol_contraction_ok:
                failures.append(f'vol_ratio={vol_ratio:.2f}')
            result.reason = 'PHYSICS_FAIL: ' + ', '.join(failures)
        else:
            result.reason = 'PHYSICS_PASS: all three laws satisfied'

        return result

    def is_sector_melting(self, symbol: str, trade_date: str = None) -> bool:
        """Sector resonance meltdown: if the industry dropped > 3% today, SUSPEND."""
        with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
            if not trade_date:
                td = conn.execute('SELECT MAX(trade_date) FROM fact_daily').fetchone()[0]
                trade_date = td.strftime('%Y-%m-%d') if hasattr(td, 'strftime') else str(td)

            industry_row = conn.execute(
                'SELECT industry FROM fact_stock_basic WHERE symbol = ?',
                [symbol],
            ).fetchone()
            if not industry_row or not industry_row[0]:
                return False

            industry = industry_row[0]
            avg_pct = conn.execute(
                """
                SELECT AVG(fd.pct_chg)
                FROM fact_daily fd
                JOIN fact_stock_basic fb ON fd.symbol = fb.symbol
                WHERE fb.industry = ?
                  AND fd.trade_date = CAST(? AS DATE)
                """,
                [industry, trade_date],
            ).fetchone()[0]

        if avg_pct is not None and avg_pct < -3.0:
            logger.warning(f'SECTOR MELTDOWN: {symbol} industry={industry} avg_pct={avg_pct:.2f}%')
            return True
        return False

    def scan_ready(self, trade_date: str = None) -> List[EchoEntry]:
        """Scan all INCUBATING entries and promote qualifying rows to READY."""
        promoted: List[EchoEntry] = []
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        entries_sql = """
                SELECT fingerprint, symbol, trade_date, current_state,
                       ttl_counter, veto_reason, price_at_entry,
                       vol_ma5_at_entry, original_score, industry
                FROM fact_echo_logs
                WHERE current_state = 'INCUBATING'
                """

        with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
            entries = conn.execute(entries_sql).fetchall()
        if not entries:
            return promoted

        with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
            entries = conn.execute(entries_sql).fetchall()

            for row in entries:
                entry = EchoEntry(
                    fingerprint=row[0],
                    symbol=row[1],
                    trade_date=str(row[2]),
                    current_state=row[3],
                    ttl_counter=row[4],
                    veto_reason=row[5] or '',
                    price_at_entry=row[6] or 0,
                    vol_ma5_at_entry=row[7] or 0,
                    original_score=row[8] or 0,
                    industry=row[9] or '',
                )

                physics = self.check_physics(entry.symbol, trade_date)
                if physics.passed:
                    if self.is_sector_melting(entry.symbol, trade_date):
                        conn.execute(
                            """
                            UPDATE fact_echo_logs
                            SET current_state = 'SUSPEND', updated_at = CAST(? AS TIMESTAMP)
                            WHERE fingerprint = ?
                            """,
                            [now, entry.fingerprint],
                        )
                        logger.info(f'ECHO SUSPEND (sector): {entry.symbol} fp={entry.fingerprint}')
                        continue

                    conn.execute(
                        """
                        UPDATE fact_echo_logs
                        SET current_state = 'READY', updated_at = CAST(? AS TIMESTAMP)
                        WHERE fingerprint = ?
                        """,
                        [now, entry.fingerprint],
                    )
                    entry.current_state = 'READY'
                    promoted.append(entry)
                    logger.info(
                        f'ECHO READY: {entry.symbol} fp={entry.fingerprint} '
                        f'physics=PASS (close={physics.current_close:.2f} '
                        f'bias={physics.bias5*100:.1f}% vol={physics.vol_ratio:.2f})'
                    )
                else:
                    logger.debug(f'ECHO WAIT: {entry.symbol} {physics.reason}')

        return promoted

    def attempt_awaken(self, trade_date: str = None) -> List[Dict[str, Any]]:
        """Attempt to awaken READY entries and return L4 re-audit payloads."""
        tide_risk_gate = 'AGGRESSIVE'
        try:
            _loader_ns = runpy.run_path(str(PROJECT_ROOT / '04_governance' / 'lib' / 'core' / 'module_loader.py'))
            _tide_mod = _loader_ns['load_module_from_path'](
                'tide_sensor_echo_manager',
                PROJECT_ROOT / '04_governance' / 'lib' / 'tide_sensor.py',
            )
            sensor = _tide_mod.get_sensor()
            tide_state = sensor.get_risk_gate(trade_date)
            tide_risk_gate = tide_state.risk_gate
        except Exception as e:
            logger.warning(f'Tide check unavailable for awakening: {e}')

        awaken_items: List[Dict[str, Any]] = []
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        entries_sql = """
                SELECT fingerprint, symbol, trade_date, veto_reason, original_score, industry
                FROM fact_echo_logs
                WHERE current_state = 'READY'
                """

        with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
            entries = conn.execute(entries_sql).fetchall()
        if not entries:
            return awaken_items

        with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
            entries = conn.execute(entries_sql).fetchall()

            for row in entries:
                fp, symbol, td, reason, orig_score, _industry = row

                if tide_risk_gate == 'FORCE_NO_EDGE':
                    conn.execute(
                        """
                        UPDATE fact_echo_logs
                        SET current_state = 'SUSPEND', updated_at = CAST(? AS TIMESTAMP)
                        WHERE fingerprint = ?
                        """,
                        [now, fp],
                    )
                    logger.info(f'ECHO SUSPEND (tide): {symbol} fp={fp}')
                    continue

                prompt_context = (
                    f'[ECHO AWAKENING] {symbol} was rejected yesterday due to: {reason}. '
                    f'Original score: {orig_score}. '
                    'After T+1 cooling, physics sensor confirms: '
                    'price returned to MA5 band, bias < 2%, volume contracted. '
                    'Re-evaluate with focus on IMPROVED risk/reward ratio. '
                    f'Shadow bonus: +{SHADOW_BONUS} applied to final score.'
                )

                awaken_items.append(
                    {
                        'symbol': symbol,
                        'fingerprint': fp,
                        'shadow_bonus': SHADOW_BONUS,
                        'prompt_context': prompt_context,
                        'original_score': orig_score,
                        'trade_date': str(td),
                        'veto_reason': reason or '',
                    }
                )

                conn.execute(
                    """
                    UPDATE fact_echo_logs
                    SET current_state = 'AWAKENED', updated_at = CAST(? AS TIMESTAMP)
                    WHERE fingerprint = ?
                    """,
                    [now, fp],
                )
                try:
                    from tactic_signal_bus import record_tactic_signal
                    record_tactic_signal(
                        source='ECHO',
                        symbol=symbol,
                        signal_type='ECHO_AWAKENED',
                        verdict='AWAKENED',
                        score=float(orig_score or 0) + SHADOW_BONUS * 100,
                        price=0.0,
                        reason=reason or '',
                        evidence={
                            'fingerprint': fp,
                            'original_score': orig_score,
                            'shadow_bonus': SHADOW_BONUS,
                            'trade_date': str(td),
                            'prompt_context': prompt_context,
                        },
                        trade_date=str(td),
                        ttl_minutes=180,
                    )
                except Exception as exc:
                    logger.warning(f'ECHO signal bus write failed: {symbol} fp={fp} | {exc}')
                logger.info(f'ECHO AWAKEN: {symbol} fp={fp} bonus={SHADOW_BONUS}')

        return awaken_items

    def decay_ttl(self) -> Dict[str, int]:
        """Daily TTL countdown. ttl_counter -= 1; 0 -> EXPIRED."""
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        with DBGateway(DB_PATH, read_only=False, logger=logger) as conn:
            conn.execute(
                """
                UPDATE fact_echo_logs
                SET ttl_counter = ttl_counter - 1,
                    updated_at = CAST(? AS TIMESTAMP)
                WHERE current_state IN ('INCUBATING', 'READY')
                  AND ttl_counter > 0
                """,
                [now],
            )

            expired = conn.execute(
                """
                UPDATE fact_echo_logs
                SET current_state = 'EXPIRED',
                    updated_at = CAST(? AS TIMESTAMP)
                WHERE current_state IN ('INCUBATING', 'READY')
                  AND ttl_counter <= 0
                RETURNING fingerprint, symbol
                """,
                [now],
            ).fetchall()

            active = conn.execute(
                """
                SELECT COUNT(*)
                FROM fact_echo_logs
                WHERE current_state IN ('INCUBATING', 'READY')
                """
            ).fetchone()[0]

        for fp, sym in expired:
            logger.info(f'ECHO EXPIRED: {sym} fp={fp}')

        stats = {'expired': len(expired), 'active_remaining': active}
        logger.info(f"ECHO DECAY: expired={stats['expired']} active={stats['active_remaining']}")
        return stats

    def get_status(self) -> Dict[str, Any]:
        """Get echo system status summary"""
        with DBGateway(DB_PATH, read_only=True, logger=logger) as conn:
            counts = conn.execute(
                """
                SELECT current_state, COUNT(*) as cnt
                FROM fact_echo_logs
                GROUP BY current_state
                """
            ).fetchall()

            recent = conn.execute(
                """
                SELECT symbol, current_state, ttl_counter, veto_reason
                FROM fact_echo_logs
                ORDER BY updated_at DESC
                LIMIT 10
                """
            ).fetchall()

        return {
            'states': {row[0]: row[1] for row in counts},
            'recent': [
                {'symbol': r[0], 'state': r[1], 'ttl': r[2], 'reason': r[3]} for r in recent
            ],
        }


_echo_instance = None


def get_echo_manager() -> EchoManager:
    global _echo_instance
    if _echo_instance is None:
        _echo_instance = EchoManager()
    return _echo_instance


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')

    em = get_echo_manager()

    print('=' * 60)
    print('  ECHO SYSTEM SELF-TEST')
    print('=' * 60)

    print('\n--- Eligibility Tests ---')
    tests = [
        ('OVERBOUGHT', True),
        ('HIGH_FRICTION', True),
        ('BLUE VETO: overbought signal', True),
        ('fundamental risk: debt crisis', False),
        ('score too low (35)', False),
        ('BLUE VETO: fraud detected', False),
    ]
    for reason, expected in tests:
        result = EchoManager.is_echo_eligible(reason)
        status = 'PASS' if result == expected else 'FAIL'
        print(f"  [{status}] '{reason}' -> eligible={result} (expected={expected})")

    print('\n--- Incubation Test ---')
    fp = em.incubate(
        '000001.SZ',
        '2026-02-12',
        'OVERBOUGHT',
        price=15.5,
        vol_ma5=50000000,
        original_score=62,
    )
    print(f'  Fingerprint: {fp}')

    print('\n--- Physics Check ---')
    physics = em.check_physics('000001.SZ')
    print(f'  Passed: {physics.passed}')
    print(f'  Price band: {physics.price_in_band} (close={physics.current_close:.2f} MA5={physics.ma5:.2f})')
    print(f'  Bias5: {physics.bias_ok} ({physics.bias5*100:.2f}%)')
    print(f'  Vol contraction: {physics.vol_contraction_ok} (ratio={physics.vol_ratio:.2f})')
    print(f'  Reason: {physics.reason}')

    print('\n--- Status ---')
    status = em.get_status()
    print(f"  States: {status['states']}")
    for r in status['recent']:
        print(f"  {r['symbol']} | {r['state']} | TTL={r['ttl']} | {r['reason']}")
