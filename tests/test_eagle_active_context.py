#!/usr/bin/env python3

import json
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from eagle_active_context import build_context_manifest, write_artifacts


def trading_dates(count=15):
    current = date(2026, 7, 20)
    result = []
    while len(result) < count:
        if current.weekday() < 5:
            result.append(current.isoformat())
        current += timedelta(days=1)
    return result


DATES = trading_dates()
TRADE_DATE = DATES[-1]


def daily_rows(symbol, industry, *, start=10.0, step=0.08, rps_start=55.0):
    rows = []
    prior = start
    for index, trade_date in enumerate(DATES):
        close = start + step * index
        pct_chg = (close / prior - 1.0) * 100.0 if index else 0.0
        rows.append({
            "trade_date": trade_date,
            "symbol": symbol,
            "name": symbol,
            "industry": industry,
            "market": "主板",
            "is_st": False,
            "open": close - 0.04,
            "high": close + 0.04,
            "low": close - 0.08,
            "close": close,
            "pre_close": prior,
            "pct_chg": pct_chg,
            "vol": 1000 + index * 45,
            "amount": 100000 + index * 5000,
            "turnover_rate": 1.0 + index * 0.03,
            "ma20": close - 0.12,
            "vol_ma5": 1000 + index * 30,
            "rps_10": rps_start + index,
        })
        prior = close
    return rows


def raw_candidate(symbol, industry="Power", **overrides):
    candidate = {
        "candidate_id": f"EAGLE-{TRADE_DATE.replace('-', '')}-{symbol}",
        "symbol": symbol,
        "trade_date": TRADE_DATE,
        "industry": industry,
        "market": "主板",
        "eligibility": "OBSERVE_ONLY_A_SHARE",
        "origin_type": "SECTOR_RESONANCE",
        "primary_morphology": "TREND_CONTINUATION",
        "candidate_status": "PERSISTENT_ACTIVITY",
        "active_window_count": 5,
        "active_span_minutes": 60,
        "intraday_peak_drawdown_pct": -0.5,
        "data_quality": "COMPLETE",
        "observation_only": True,
        "no_trade_signal": True,
    }
    candidate.update(overrides)
    return candidate


def raw_manifest(candidates):
    return {
        "schema_version": "eagle_active_path_v0.4",
        "rule_version": "eagle_active_rules_v0.4",
        "trade_date": TRADE_DATE,
        "data_quality": "COMPLETE",
        "observation_only": True,
        "no_trade_signal": True,
        "candidates": candidates,
    }


def market_fixture():
    rows = []
    for number in range(1, 5):
        rows.extend(daily_rows(f"00000{number}.SZ", "Power", step=0.08 + number * 0.005))
    for number in range(5, 11):
        rows.extend(daily_rows(f"0000{number}.SZ", "Other", step=-0.005, rps_start=45))
    return rows


class EagleActiveContextTest(unittest.TestCase):
    def test_persistent_constructive_sector_path_becomes_audit_ready_observer(self):
        source = raw_manifest([
            raw_candidate("000001.SZ"),
            raw_candidate("000002.SZ"),
        ])
        manifest = build_context_manifest(
            source,
            market_fixture(),
            generated_at="2026-08-07 20:50:00",
        )
        row = next(item for item in manifest["candidates"] if item["symbol"] == "000001.SZ")
        self.assertEqual(row["route"], "AUDIT_READY_OBSERVER")
        self.assertEqual(row["context_archetype"], "SECTOR_LED_MOMENTUM")
        self.assertIn("THREE_DAY_CONSTRUCTIVE_PATH", row["positive_evidence"])
        self.assertIn("POST_CLOSE_SECTOR_SUPPORT", row["positive_evidence"])
        self.assertTrue(row["observation_only"])
        self.assertFalse(row["generate_task"])

    def test_single_window_pulse_is_rejected_as_noise(self):
        source = raw_manifest([
            raw_candidate(
                "000001.SZ",
                candidate_status="PULSE_ONLY",
                primary_morphology="UNSTABLE_SPIKE",
                active_window_count=1,
                active_span_minutes=0,
            )
        ])
        manifest = build_context_manifest(source, market_fixture())
        row = manifest["candidates"][0]
        self.assertEqual(row["route"], "REJECTED_NOISE")
        self.assertEqual(row["context_archetype"], "PULSE_NOISE")

    def test_st_and_bj_never_become_audit_ready(self):
        source = raw_manifest([
            raw_candidate("000001.SZ", eligibility="OBSERVE_ONLY_ST"),
            raw_candidate("000002.SZ", eligibility="OBSERVE_ONLY_BJ", market="北交所"),
        ])
        manifest = build_context_manifest(source, market_fixture())
        self.assertTrue(all(row["route"] == "WATCH_ONLY" for row in manifest["candidates"]))

    def test_late_climax_is_rejected_even_with_constructive_history(self):
        source = raw_manifest([
            raw_candidate(
                "000001.SZ",
                primary_morphology="LATE_CLIMAX",
                intraday_peak_drawdown_pct=-3.0,
            ),
            raw_candidate("000002.SZ"),
        ])
        manifest = build_context_manifest(source, market_fixture())
        row = next(item for item in manifest["candidates"] if item["symbol"] == "000001.SZ")
        self.assertEqual(row["route"], "WATCH_ONLY")
        self.assertEqual(row["context_archetype"], "LATE_CLIMAX_RISK")

    def test_already_overextended_three_day_path_is_not_promoted(self):
        rows = market_fixture()
        for row in rows:
            if row["symbol"] == "000001.SZ" and row["trade_date"] in DATES[-3:]:
                row["close"] *= 1.12
                row["open"] = row["close"] - 0.04
                row["high"] = row["close"] + 0.04
                row["low"] = row["close"] - 0.08
                row["pct_chg"] = 6.0
        source = raw_manifest([raw_candidate("000001.SZ"), raw_candidate("000002.SZ")])
        manifest = build_context_manifest(source, rows)
        row = next(item for item in manifest["candidates"] if item["symbol"] == "000001.SZ")
        self.assertEqual(row["route"], "WATCH_ONLY")
        self.assertIn("THREE_DAY_OVEREXTENDED", row["risk_evidence"])

    def test_missing_current_daily_history_fails_closed_to_watch_only(self):
        source = raw_manifest([raw_candidate("300999.SZ", industry="Unknown")])
        manifest = build_context_manifest(source, market_fixture())
        row = manifest["candidates"][0]
        self.assertEqual(row["route"], "WATCH_ONLY")
        self.assertIn("DAILY_HISTORY", row["missing_evidence"])

    def test_zero_audit_ready_is_a_valid_manifest(self):
        source = raw_manifest([
            raw_candidate(
                "000001.SZ",
                candidate_status="PULSE_ONLY",
                primary_morphology="UNSTABLE_SPIKE",
                active_window_count=1,
                active_span_minutes=0,
            )
        ])
        manifest = build_context_manifest(source, market_fixture())
        self.assertEqual(manifest["route_counts"].get("AUDIT_READY_OBSERVER", 0), 0)
        self.assertTrue(manifest["no_trade_signal"])

    def test_artifacts_are_atomic_and_keep_source_identity(self):
        source = raw_manifest([raw_candidate("000001.SZ"), raw_candidate("000002.SZ")])
        manifest = build_context_manifest(
            source,
            market_fixture(),
            generated_at="2026-08-07 20:50:00",
        )
        with tempfile.TemporaryDirectory() as temp:
            paths = write_artifacts(manifest, temp)
            saved = json.loads(Path(paths["manifest_path"]).read_text(encoding="utf-8"))
            preview = Path(paths["preview_path"]).read_text(encoding="utf-8")
        self.assertEqual(saved["source_manifest_sha256"], manifest["source_manifest_sha256"])
        self.assertIn("AUDIT_READY_OBSERVER", preview)
        self.assertIn("does not create validation tasks", preview)

    def test_unsafe_source_manifest_is_rejected(self):
        source = raw_manifest([raw_candidate("000001.SZ")])
        source["no_trade_signal"] = False
        with self.assertRaisesRegex(ValueError, "UNSAFE_SOURCE_MANIFEST"):
            build_context_manifest(source, market_fixture())

    def test_partial_source_quality_cannot_create_audit_ready_candidate(self):
        source = raw_manifest([raw_candidate("000001.SZ"), raw_candidate("000002.SZ")])
        source["data_quality"] = "PARTIAL_LOW_COVERAGE"
        manifest = build_context_manifest(source, market_fixture())
        self.assertTrue(all(row["route"] == "WATCH_ONLY" for row in manifest["candidates"]))
        self.assertTrue(
            all("SOURCE_MANIFEST_QUALITY" in row["missing_evidence"] for row in manifest["candidates"])
        )


if __name__ == "__main__":
    unittest.main()
