import json
import tempfile
import unittest
from pathlib import Path

from eagle_active_path import EagleActivePathObserver, build_manifest, derive_manifest_candidates


def quote(symbol, industry, quote_time, price, volume, amount, *, pct=2.5, source="tushare_realtime", is_st=False):
    return {
        "symbol": symbol,
        "industry": industry,
        "market": "主板",
        "is_st": is_st,
        "quote_date": "2026-08-03",
        "quote_time": quote_time,
        "quote_source": source,
        "price": price,
        "pct_chg": pct,
        "volume": volume,
        "amount": amount,
    }


def scan(clock, quotes, *, universe_count=None, realtime_count=None):
    return {
        "scan_time": f"2026-08-03 {clock}",
        "quotes": quotes,
        "quote_stats": {
            "universe_count": universe_count if universe_count is not None else len(quotes),
            "realtime_count": realtime_count if realtime_count is not None else len(quotes),
        },
    }


class EagleActivePathTest(unittest.TestCase):
    def test_real_cumulative_values_become_deltas(self):
        scans = [
            scan("09:30:00", [
                quote("000001.SZ", "Power", "09:30:00", 10, 100, 1000),
                quote("000002.SZ", "Power", "09:30:00", 10, 100, 1000),
            ]),
            scan("09:40:00", [
                quote("000001.SZ", "Power", "09:40:00", 10.5, 300, 4000),
                quote("000002.SZ", "Power", "09:40:00", 10.1, 110, 1100),
            ]),
        ]
        candidates = derive_manifest_candidates(scans, "2026-08-03")
        row = next(item for item in candidates if item["symbol"] == "000001.SZ")
        self.assertEqual(row["active_window_count"], 1)
        self.assertEqual(row["candidate_status"], "PULSE_ONLY")
        self.assertEqual(row["primary_morphology"], "UNSTABLE_SPIKE")

    def test_persistent_sector_resonance_is_separate_from_morphology(self):
        scans = []
        for clock, base in (("09:30:00", 100), ("09:40:00", 300), ("09:50:00", 500), ("10:00:00", 700)):
            scans.append(scan(clock, [
                quote("000001.SZ", "Power", clock, 10 + base / 1000, base, base * 10),
                quote("000002.SZ", "Power", clock, 10 + base / 1100, base, base * 10),
                quote("000003.SZ", "Power", clock, 10 + base / 1200, base, base * 10),
                quote("000004.SZ", "Other", clock, 10.0, 100 + base, 1000 + base),
            ]))
        candidates = derive_manifest_candidates(scans, "2026-08-03")
        row = next(item for item in candidates if item["symbol"] == "000001.SZ")
        self.assertEqual(row["origin_type"], "SECTOR_RESONANCE")
        self.assertEqual(row["primary_morphology"], "TREND_CONTINUATION")
        self.assertIn("PERSISTENT_ACTIVITY", row["morphology_flags"])

    def test_counter_reset_never_becomes_active_evidence(self):
        scans = [
            scan("09:30:00", [quote("000001.SZ", "Power", "09:30:00", 10, 500, 5000)]),
            scan("09:40:00", [quote("000001.SZ", "Power", "09:40:00", 10.2, 100, 1000)]),
        ]
        manifest = build_manifest(scans, "2026-08-03")
        self.assertEqual(manifest["candidate_count"], 0)

    def test_invalid_source_and_wrong_date_are_fail_closed(self):
        scans = [
            scan("09:30:00", [
                quote("000001.SZ", "Power", "09:30:00", 10, 100, 1000, source="fact_daily"),
                {**quote("000002.SZ", "Power", "09:30:00", 10, 100, 1000), "quote_date": "2026-08-02"},
            ])
        ]
        manifest = build_manifest(scans, "2026-08-03")
        self.assertEqual(manifest["candidate_count"], 0)
        self.assertEqual(manifest["data_quality"], "PARTIAL_NO_VALID_SCANS")

    def test_artifacts_are_recoverable_and_duplicate_scan_is_ignored(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            observer = EagleActivePathObserver("2026-08-03", root)
            payload = [quote("000001.SZ", "Power", "09:30:00", 10, 100, 1000)]
            first = observer.ingest_scan(payload, scan_time="09:30:00")
            second = observer.ingest_scan(payload, scan_time="09:30:00")
            self.assertTrue(first["recorded"])
            self.assertFalse(second["recorded"])
            manifest = observer.finalize(generated_at="2026-08-03 15:05:00")
            self.assertTrue((root / "manifests/eagle_candidates_2026-08-03.json").exists())
            self.assertTrue((root / "previews/eagle_preview_2026-08-03.md").exists())
            self.assertEqual(manifest["scan_count"], 1)

            recovered = EagleActivePathObserver("2026-08-03", root)
            recovered_manifest = recovered.finalize(generated_at="2026-08-03 15:06:00")
            self.assertEqual(recovered_manifest["scan_count"], 1)
            saved = json.loads((root / "manifests/eagle_candidates_2026-08-03.json").read_text(encoding="utf-8"))
            self.assertTrue(saved["observation_only"])
            self.assertTrue(saved["no_trade_signal"])

    def test_missing_symbol_window_does_not_bridge_cumulative_delta(self):
        scans = [
            scan("09:30:00", [
                quote("000001.SZ", "Power", "09:30:00", 10, 100, 1000),
                quote("000002.SZ", "Power", "09:30:00", 10, 100, 1000),
            ]),
            scan("09:40:00", [
                quote("000002.SZ", "Power", "09:40:00", 10.1, 200, 2000),
            ]),
            scan("09:50:00", [
                quote("000001.SZ", "Power", "09:50:00", 10.5, 300, 4000),
                quote("000002.SZ", "Power", "09:50:00", 10.2, 300, 3000),
            ]),
        ]
        candidates = derive_manifest_candidates(scans, "2026-08-03")
        self.assertFalse(any(item["symbol"] == "000001.SZ" for item in candidates))
        manifest = build_manifest(scans, "2026-08-03")
        self.assertEqual(manifest["data_quality"], "PARTIAL_WINDOW_GAP")
        self.assertGreaterEqual(manifest["window_gap_count"], 1)

    def test_low_coverage_cannot_create_single_stock_candidate(self):
        scans = [
            scan(
                "09:30:00",
                [quote("000001.SZ", "Power", "09:30:00", 10, 100, 1000)],
                universe_count=1200,
                realtime_count=1,
            ),
            scan(
                "09:40:00",
                [quote("000001.SZ", "Power", "09:40:00", 10.5, 300, 4000)],
                universe_count=1200,
                realtime_count=1,
            ),
        ]
        manifest = build_manifest(scans, "2026-08-03")
        self.assertEqual(manifest["candidate_count"], 0)
        self.assertEqual(manifest["data_quality"], "PARTIAL_LOW_COVERAGE")
        self.assertEqual(manifest["coverage"]["low_coverage_scan_count"], 1)

    def test_full_coverage_without_percentile_sample_is_not_complete(self):
        first_quotes = [
            quote(f"{index:06d}.SZ", "Power", "09:30:00", 10, 100, 1000)
            for index in range(20)
        ]
        second_quotes = [
            quote(f"{index:06d}.SZ", "Power", "09:40:00", 10.5, 300, 4000)
            for index in range(19)
        ]
        scans = [
            scan("09:30:00", first_quotes, universe_count=20),
            scan("09:40:00", second_quotes, universe_count=20),
        ]
        manifest = build_manifest(scans, "2026-08-03")
        self.assertEqual(manifest["candidate_count"], 0)
        self.assertEqual(manifest["data_quality"], "PARTIAL_INSUFFICIENT_SAMPLE")
        self.assertEqual(manifest["insufficient_sample_scan_count"], 1)

    def test_open_and_lunch_session_baselines_do_not_poison_the_day(self):
        def quotes_for(clock, count, volume, amount):
            return [
                quote(
                    f"{index:06d}.SZ",
                    "Power",
                    clock,
                    10.0 + index / 10000,
                    volume,
                    amount,
                )
                for index in range(count)
            ]

        scans = [
            scan("09:30:00", quotes_for("09:30:00", 70, 100, 1000), universe_count=100),
            scan("09:40:00", quotes_for("09:40:00", 100, 300, 3000), universe_count=100),
            scan("09:50:00", quotes_for("09:50:00", 100, 500, 5000), universe_count=100),
            scan("13:00:00", quotes_for("13:00:00", 50, 500, 5000), universe_count=100),
            scan("13:10:00", quotes_for("13:10:00", 100, 700, 7000), universe_count=100),
            scan("13:20:00", quotes_for("13:20:00", 100, 900, 9000), universe_count=100),
        ]

        manifest = build_manifest(scans, "2026-08-03")

        self.assertEqual(manifest["data_quality"], "COMPLETE")
        self.assertEqual(manifest["session_baseline_scan_count"], 2)
        self.assertEqual(manifest["session_warmup_scan_count"], 2)
        self.assertEqual(manifest["insufficient_delta_coverage_scan_count"], 0)
        self.assertEqual(manifest["coverage"]["low_coverage_scan_count"], 0)
        self.assertEqual(manifest["window_gap_count"], 0)
        self.assertGreater(manifest["candidate_count"], 0)
        self.assertTrue(all(row["data_quality"] == "COMPLETE" for row in manifest["candidates"]))
        warmups = [quality for quality in manifest["scan_quality"] if quality["session_warmup"]]
        self.assertTrue(all(not quality["window_eligible"] for quality in warmups))

    def test_latest_observation_drives_drawdown_and_late_climax(self):
        from eagle_active_path import _primary_morphology

        points = [
            {"quote_time": "10:10:00", "price": 11.0, "active_window": True},
            {"quote_time": "14:50:00", "price": 9.8, "active_window": False},
        ]
        primary, flags = _primary_morphology(points)
        self.assertIn("LATE_SESSION_OBSERVATION", flags)
        self.assertIn("PRICE_RETRACE_AFTER_PEAK", flags)
        self.assertEqual(primary, "LATE_CLIMAX")

    def test_unknown_symbol_is_not_labeled_a_share(self):
        point = quote("XYZ", "Power", "09:30:00", 10, 100, 1000)
        point["list_date"] = "2026-08-01"
        scans = [
            scan("09:30:00", [point]),
            scan("09:40:00", [{**point, "quote_time": "09:40:00", "volume": 300, "amount": 4000}]),
        ]
        candidates = derive_manifest_candidates(scans, "2026-08-03")
        row = next(item for item in candidates if item["symbol"] == "XYZ")
        self.assertEqual(row["eligibility"], "OBSERVE_ONLY_UNSUPPORTED_MARKET")
        self.assertEqual(row["listing_age_days"], 2)

    def test_missing_market_metadata_is_not_assumed_a_share(self):
        point = quote("000001.SZ", "Power", "09:30:00", 10, 100, 1000)
        point["market"] = ""
        scans = [
            scan("09:30:00", [point]),
            scan("09:40:00", [{**point, "quote_time": "09:40:00", "volume": 300, "amount": 4000}]),
        ]
        row = next(item for item in derive_manifest_candidates(scans, "2026-08-03") if item["symbol"] == "000001.SZ")
        self.assertEqual(row["eligibility"], "OBSERVE_ONLY_UNSUPPORTED_MARKET")


if __name__ == "__main__":
    unittest.main()
