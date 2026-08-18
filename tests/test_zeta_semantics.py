#!/usr/bin/env python3

import importlib.util
import os
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEST_LOG_DIR = Path(tempfile.gettempdir()) / f"zhulong_zeta_test_logs_{os.getpid()}"
TEST_LOG_DIR.mkdir(parents=True, exist_ok=True)
_ENV_KEYS = ("ZHULONG_NEXUS_LOG_PATH", "ZHULONG_GOVERNANCE_LOG_PATH")
_ENV_BEFORE = {key: os.environ.get(key) for key in _ENV_KEYS}
os.environ.setdefault("ZHULONG_NEXUS_LOG_PATH", str(TEST_LOG_DIR / "nexus.log"))
os.environ.setdefault("ZHULONG_GOVERNANCE_LOG_PATH", str(TEST_LOG_DIR / "governance.log"))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.settings import Config

_ORIGINAL_LOG_DIR = Config.LOG_DIR
Config.LOG_DIR = TEST_LOG_DIR

from zeta.zeta_auditor_v240 import GameSignal, ZetaAuditor
from zeta.zeta_collector_v240 import ZetaData

MODULE_PATH = ROOT / "02_brain" / "decision_engine.py"
SPEC = importlib.util.spec_from_file_location("zhulong_test_zeta_semantics", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)
Config.LOG_DIR = _ORIGINAL_LOG_DIR
for _key, _value in _ENV_BEFORE.items():
    if _value is None:
        os.environ.pop(_key, None)
    else:
        os.environ[_key] = _value


def zeta_data(*, lhb_net=0.0, inst_buy=0, hot_money=0, margin_delta=0.0):
    return ZetaData(
        ts_code="000001.SZ",
        trade_date=date(2026, 7, 31),
        lhb_net=lhb_net,
        inst_buy=inst_buy,
        hot_money=hot_money,
        margin_delta=margin_delta,
    )


class ZetaSemanticTest(unittest.TestCase):
    def test_lhb_strong_direction_uses_yuan_threshold(self):
        auditor = ZetaAuditor()
        small = auditor.audit(zeta_data(lhb_net=5_000), logic_score=50)
        strong_buy = auditor.audit(zeta_data(lhb_net=50_000_001), logic_score=50)
        strong_sell = auditor.audit(zeta_data(lhb_net=-50_000_001), logic_score=50)

        self.assertEqual(small.game_signal, GameSignal.NEUTRAL)
        self.assertEqual(strong_buy.game_signal, GameSignal.BUY_PRESSURE)
        self.assertEqual(strong_sell.game_signal, GameSignal.SELL_PRESSURE)

    def test_neutral_or_sell_direction_never_gets_bonus(self):
        neutral = {
            "zeta_score": 5.0,
            "game_signal": "NEUTRAL",
            "inst_flow": "NEUTRAL",
            "margin_trend": "UNKNOWN",
        }
        sell = {
            "zeta_score": 7.0,
            "game_signal": "SELL_PRESSURE",
            "inst_flow": "OUTFLOW",
            "margin_trend": "INFLOW",
        }

        self.assertEqual(MODULE._zeta_directional_bonus(neutral)[0], 0)
        self.assertEqual(MODULE._zeta_directional_bonus(sell)[0], 0)

    def test_buy_bonus_requires_non_hot_money_confirmation(self):
        unconfirmed = {
            "zeta_score": 7.0,
            "game_signal": "BUY_PRESSURE",
            "inst_flow": "NEUTRAL",
            "margin_trend": "UNKNOWN",
            "hot_money_flow": "INFLOW",
        }
        confirmed = dict(unconfirmed, inst_flow="INFLOW")
        contradicted = dict(confirmed, margin_trend="OUTFLOW")

        self.assertEqual(MODULE._zeta_directional_bonus(unconfirmed)[0], 0)
        self.assertEqual(MODULE._zeta_directional_bonus(confirmed)[0], 5)
        self.assertEqual(MODULE._zeta_directional_bonus(contradicted)[0], 0)


if __name__ == "__main__":
    unittest.main()
