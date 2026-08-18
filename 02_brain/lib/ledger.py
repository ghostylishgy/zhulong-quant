#!/usr/bin/env python3
import json
import logging
import sys
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from typing import Dict

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = str(PROJECT_ROOT / 'storage' / 'database' / 'zhulong.duckdb')

_CORE_DIR = PROJECT_ROOT / '04_governance' / 'lib' / 'core'
if str(_CORE_DIR) not in sys.path:
    sys.path.append(str(_CORE_DIR))
from module_loader import load_attr_from_path

DBGateway = load_attr_from_path(
    'zhulong_db_gateway',
    PROJECT_ROOT / '01_engine' / 'lib' / 'db_gateway.py',
    'DBGateway',
)

logger = logging.getLogger('zhulong.rag.ledger')


@dataclass
class GameOperator:
    trigger: str = ''
    counterparty: str = ''
    risk_flag: str = ''
    verdict: str = ''
    key_numbers: Dict[str, float] = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)


@dataclass
class LogicFingerprint:
    ts_code: str
    trade_date: date
    stock_name: str = ''
    event_type: str = ''
    game_operator: GameOperator = field(default_factory=GameOperator)
    logic_score: int = 0
    gamma_score: float = 5.0
    zeta_score: float = 5.0
    game_signal: str = 'NEUTRAL'
    seat_profile: str = 'UNKNOWN'
    credibility_score: float = 1.0
    raw_evidence: str = ''


class LedgerManager:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path

    def archive(self, fp: LogicFingerprint) -> int:
        with DBGateway(self.db_path, read_only=False, logger=logger) as conn:
            c = conn.cursor()
            c.execute(
                'INSERT INTO logic_ledger (ts_code, trade_date, stock_name, event_type, game_operator, logic_score, gamma_score, zeta_score, game_signal, seat_profile, credibility_score, raw_evidence) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                (
                    fp.ts_code,
                    fp.trade_date.isoformat(),
                    fp.stock_name,
                    fp.event_type,
                    fp.game_operator.to_json(),
                    fp.logic_score,
                    fp.gamma_score,
                    fp.zeta_score,
                    fp.game_signal,
                    fp.seat_profile,
                    fp.credibility_score,
                    fp.raw_evidence,
                ),
            )
            lid = c.lastrowid
            conn.commit()
            return lid

    def count(self) -> int:
        with DBGateway(self.db_path, read_only=True, logger=logger) as conn:
            c = conn.cursor()
            c.execute('SELECT COUNT(*) FROM logic_ledger')
            n = c.fetchone()[0]
            return n


def extract_operator(signal: str, logic: int, zeta: float) -> GameOperator:
    op = GameOperator()
    if signal == 'EXIT':
        op.trigger = 'LOGIC_FALSIFIED'
        op.counterparty = 'INST_EXITING'
        op.risk_flag = 'SEVERE_DIVERGENCE'
        op.verdict = 'FORCED_EXIT'
    elif signal == 'DIV_TRAP':
        op.trigger = 'HIGH_LOGIC_LOW_ZETA'
        op.counterparty = 'WEAK_CAPITAL'
        op.risk_flag = 'DIVERGENCE_WARNING'
        op.verdict = 'CAUTION'
    else:
        op.trigger = 'NEUTRAL'
        op.verdict = 'HOLD'
    op.key_numbers = {'logic': logic, 'zeta': zeta, 'div': logic / 10 - zeta}
    return op


if __name__ == '__main__':
    mgr = LedgerManager()
    print('Count:', mgr.count())
