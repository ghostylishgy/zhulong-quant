from datetime import datetime, timezone
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from us_radar.evidence import extract_dry_run_signal
from us_radar.form4 import parse_form4_xml
from us_radar.industry import expand_event_targets
from us_radar.market_data import MarketDataClient, PricePoint, _event_base_date, _return_from_prices, median_return
from us_radar.cli import _apply_us_api_budget, _error_update, cmd_fetch_sec
from us_radar.push import DISCLAIMER, build_daily_push
from us_radar.quality import classify_event
from us_radar.schema import stable_event_id
from us_radar.sec_edgar import parse_atom_feed
from us_radar.sec_daily_index import parse_daily_index, reconcile_watchlist
from us_radar.storage import EventStore
from us_radar.validation import (
    build_validation_task,
    classify_direction,
    classify_quality_gate,
    classify_signal,
    classify_window,
    event_direction_from_quality,
)


SAMPLE_ATOM = """<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<feed xmlns=\"http://www.w3.org/2005/Atom\">
  <entry>
    <title>8-K - NVIDIA CORP (0001045810) (Filer)</title>
    <category term="8-K" />
    <updated>2026-05-15T21:15:00-04:00</updated>
    <link href=\"https://www.sec.gov/Archives/edgar/data/1045810/000104581026000123/0001045810-26-000123-index.htm\" />
    <summary>Accession Number: 0001045810-26-000123; new partnership and capacity expansion</summary>
  </entry>
  <entry>
    <title>4 - Statement of changes in beneficial ownership of securities</title>
    <category term="4" />
    <updated>2026-05-15T20:15:00-04:00</updated>
    <link href=\"https://www.sec.gov/Archives/edgar/data/2488/000000248826000111/0000002488-26-000111-index.htm\" />
    <summary>Accession Number: 0000002488-26-000111</summary>
  </entry>
</feed>
"""

SAMPLE_FORM4 = """<?xml version=\"1.0\"?>
<ownershipDocument>
  <issuer><issuerTradingSymbol>NVDA</issuerTradingSymbol></issuer>
  <reportingOwner>
    <reportingOwnerId><rptOwnerName>Test Insider</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship><isDirector>1</isDirector><isOfficer>0</isOfficer></reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <transactionDate><value>2026-05-14</value></transactionDate>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>100</value></transactionShares>
        <transactionPricePerShare><value>10.5</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>
"""

SAMPLE_GRAPH = {
    "themes": [
        {
            "name": "ai_compute",
            "event_signals": ["财报capex指引"],
            "targets": [
                {"market": "US", "ticker": "NVDA", "name": "NVIDIA", "role": "primary", "transmission_type": "source", "event_signals": ["新品发布"], "link_logic": "AI算力源事件"},
                {"market": "CN", "ticker": "300308.SZ", "name": "中际旭创", "role": "transmission", "transmission_type": "true_business", "link_logic": "光模块核心供应商"},
                {"market": "CN", "ticker": "002230.SZ", "name": "科大讯飞", "role": "transmission", "transmission_type": "sentiment", "link_logic": "AI应用层情绪传导"},
            ],
        }
    ]
}


class SecEdgarParseTests(unittest.TestCase):
    def test_parse_atom_feed_extracts_events(self):
        events = parse_atom_feed(SAMPLE_ATOM, form_type="8-K", ticker="NVDA", cik="0001045810")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event_type, "8-K")
        self.assertEqual(events[0].ticker, "NVDA")
        self.assertEqual(events[0].accession, "0001045810-26-000123")
        self.assertEqual(events[0].event_time, "2026-05-16T01:15:00+00:00")

    def test_parse_atom_feed_filters_prefix_forms_and_accepts_amendments(self):
        feed = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry><title>4 - Insider</title><category term="4"/><updated>2026-05-15T20:15:00Z</updated><link href="https://example/1"/></entry>
  <entry><title>4/A - Insider amendment</title><category term="4/A"/><updated>2026-05-15T20:16:00Z</updated><link href="https://example/2"/></entry>
  <entry><title>424B5 - Prospectus</title><category term="424B5"/><updated>2026-05-15T20:17:00Z</updated><link href="https://example/3"/></entry>
</feed>"""
        events = parse_atom_feed(feed, form_type="4", ticker="NVDA")
        self.assertEqual([event.title for event in events], ["4 - Insider", "4/A - Insider amendment"])
        self.assertTrue(all(event.event_type == "4" for event in events))

    def test_parse_atom_feed_fails_closed_without_category(self):
        feed = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry><title>4 - Missing category</title><updated>2026-05-15T20:15:00Z</updated><link href="https://example/1"/></entry>
</feed>"""
        self.assertEqual(parse_atom_feed(feed, form_type="4"), [])

    def test_stable_event_id_prefers_accession(self):
        a = stable_event_id("sec", "8-K", "0001045810-26-000123", "url-a", "title-a", "time-a")
        b = stable_event_id("sec", "8-K", "0001045810-26-000123", "url-b", "title-b", "time-b")
        self.assertEqual(a, b)


class SecDailyIndexTests(unittest.TestCase):
    def test_parse_and_reconcile_daily_index_uses_exact_form_families(self):
        payload = """Daily Index of EDGAR Dissemination Feed
CIK|Company Name|Form Type|Date Filed|File Name
--------------------------------------------------------------------------------
1045810|NVIDIA CORP|8-K|2026-07-27|edgar/data/1045810/000104581026000321/nvda.htm
1045810|NVIDIA CORP|8-K/A|2026-07-27|edgar/data/1045810/000104581026000322/amend.htm
1045810|NVIDIA CORP|424B5|2026-07-27|edgar/data/1045810/000104581026000323/prospectus.htm
2488|ADVANCED MICRO DEVICES INC|4|2026-07-27|edgar/data/2488/000000248826000111/form4.xml
999999|UNWATCHED INC|8-K|2026-07-27|edgar/data/999999/000099999926000001/event.htm
"""
        entries = parse_daily_index(payload)
        self.assertEqual(len(entries), 5)
        self.assertEqual(entries[0].accession, "0001045810-26-000321")

        result = reconcile_watchlist(
            entries=entries,
            watchlist_ciks={"1045810", "0000002488"},
            requested_forms={"8-K", "4"},
            known_accessions={"0001045810-26-000321"},
        )
        self.assertEqual(result.total_index_rows, 5)
        self.assertEqual(result.watched_rows, 3)
        self.assertEqual(result.known_rows, 1)
        self.assertEqual(
            [row.form_type for row in result.missing_rows],
            ["8-K/A", "4"],
        )

    def test_daily_index_parser_skips_headers_and_malformed_rows(self):
        entries = parse_daily_index(
            "CIK|Company Name|Form Type|Date Filed|File Name\n"
            "not-a-cik|BAD|4|2026-07-27|bad.txt\n"
            "1045810|NVIDIA CORP|8-K|2026-07-27|edgar/data/1045810/0001045810-26-000321.txt\n"
        )
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].accession, "0001045810-26-000321")


class EventStoreTests(unittest.TestCase):
    def test_upsert_is_idempotent(self):
        events = parse_atom_feed(SAMPLE_ATOM, form_type="8-K")
        with tempfile.TemporaryDirectory() as tmpdir:
            store = EventStore(Path(tmpdir) / "us_radar.sqlite3")
            store.initialize()
            self.assertEqual(store.upsert_events(events), 1)
            self.assertEqual(store.upsert_events(events), 0)
            self.assertEqual(
                store.known_event_accessions(),
                {"0001045810-26-000123"},
            )

    def test_recent_events(self):
        events = parse_atom_feed(SAMPLE_ATOM, form_type="8-K")
        with tempfile.TemporaryDirectory() as tmpdir:
            store = EventStore(Path(tmpdir) / "us_radar.sqlite3")
            store.initialize()
            store.upsert_events(events)
            recent = store.recent_events(datetime(2026, 5, 15, tzinfo=timezone.utc))
            self.assertEqual(len(recent), 1)

    def test_evidence_signal_is_idempotent(self):
        events = parse_atom_feed(SAMPLE_ATOM, form_type="8-K", ticker="NVDA")
        signal = extract_dry_run_signal(events[0])
        with tempfile.TemporaryDirectory() as tmpdir:
            store = EventStore(Path(tmpdir) / "us_radar.sqlite3")
            store.initialize()
            store.upsert_events(events)
            self.assertEqual(store.insert_signal(signal), 1)
            self.assertEqual(store.insert_signal(signal), 0)

    def test_signal_targets_keep_transmission_types_and_validation_tasks(self):
        events = parse_atom_feed(SAMPLE_ATOM, form_type="8-K", ticker="NVDA")
        targets = expand_event_targets(events[0], SAMPLE_GRAPH)
        with tempfile.TemporaryDirectory() as tmpdir:
            store = EventStore(Path(tmpdir) / "us_radar.sqlite3")
            store.initialize()
            store.upsert_events(events)
            self.assertEqual(store.upsert_signal_targets(targets), 3)
            rows = store.pending_validation_targets([1, 3], 10)
            inserted = 0
            for row in rows:
                inserted += store.insert_validation_task(build_validation_task(row, 1, "QQQ" if row["target_market"] == "US" else "000300.SH"))
            self.assertEqual(inserted, 3)

    def test_pending_targets_fill_missing_horizons_idempotently(self):
        events = parse_atom_feed(SAMPLE_ATOM, form_type="8-K", ticker="NVDA")
        targets = expand_event_targets(events[0], SAMPLE_GRAPH)
        with tempfile.TemporaryDirectory() as tmpdir:
            store = EventStore(Path(tmpdir) / "us_radar.sqlite3")
            store.initialize()
            store.upsert_events(events)
            store.upsert_signal_targets(targets)
            rows = store.pending_validation_targets([1, 3], 10)
            for row in rows:
                benchmark = "QQQ" if row["target_market"] == "US" else "000300.SH"
                store.insert_validation_task(build_validation_task(row, 1, benchmark))

            incomplete = store.pending_validation_targets([1, 3], 10)
            self.assertEqual(len(incomplete), 3)
            inserted = 0
            for row in incomplete:
                benchmark = "QQQ" if row["target_market"] == "US" else "000300.SH"
                for horizon in (1, 3):
                    inserted += store.insert_validation_task(build_validation_task(row, horizon, benchmark))
            self.assertEqual(inserted, 3)
            self.assertEqual(store.pending_validation_targets([1, 3], 10), [])

    def test_transient_market_data_states_are_retried(self):
        events = parse_atom_feed(SAMPLE_ATOM, form_type="8-K", ticker="NVDA")
        targets = expand_event_targets(events[0], SAMPLE_GRAPH)
        with tempfile.TemporaryDirectory() as tmpdir:
            store = EventStore(Path(tmpdir) / "us_radar.sqlite3")
            store.initialize()
            store.upsert_events(events)
            store.upsert_signal_targets(targets)
            row = store.pending_validation_targets([1], 1)[0]
            benchmark = "QQQ" if row["target_market"] == "US" else "000300.SH"
            store.insert_validation_task(build_validation_task(row, 1, benchmark))

            for quality in ("RATE_LIMITED", "PROVIDER_ERROR", "NO_PRICE_DATA", "NO_BASE_PRICE", "BENCHMARK_MISSING"):
                with store.connect() as conn:
                    conn.execute(
                        "UPDATE signal_validation_results SET data_quality = ? WHERE target_id = ?",
                        (quality, row["target_id"]),
                    )
                self.assertEqual(len(store.validation_work_items(10)), 1, quality)

            with store.connect() as conn:
                conn.execute(
                    "UPDATE signal_validation_results SET data_quality = 'DATA_OK' WHERE target_id = ?",
                    (row["target_id"],),
                )
            self.assertEqual(store.validation_work_items(10), [])
            self.assertEqual(len(store.validation_work_items(10, include_complete=True)), 1)

    def test_form4_and_quality(self):
        events = parse_atom_feed(SAMPLE_ATOM, form_type="4", ticker="NVDA")
        txs = parse_form4_xml(events[0], SAMPLE_FORM4)
        with tempfile.TemporaryDirectory() as tmpdir:
            store = EventStore(Path(tmpdir) / "us_radar.sqlite3")
            store.initialize()
            store.upsert_events(events)
            self.assertEqual(store.insert_form4_transactions(txs), 1)
            quality = classify_event(events[0], store.form4_open_market_count(events[0].event_id))
            self.assertEqual(quality.quality_class, "form4_open_market")
            self.assertEqual(quality.transmission_window, "3-10个交易日")
            self.assertEqual(store.upsert_event_quality(quality), 1)

    def test_form4_parsed_non_open_market_has_explicit_quality(self):
        event = parse_atom_feed(SAMPLE_ATOM, form_type="4", ticker="NVDA")[0]
        quality = classify_event(event, form4_open_market_count=0, form4_transaction_count=2)
        self.assertEqual(quality.quality_class, "form4_non_open_market")
        self.assertEqual(quality.transmission_window, "3-10个交易日")

    def test_fetch_sec_returns_nonzero_only_when_every_company_request_fails(self):
        settings = types.SimpleNamespace(sec={"default_limit": 40})
        watchlist = [
            types.SimpleNamespace(cik="1", ticker="ONE"),
            types.SimpleNamespace(cik="2", ticker="TWO"),
        ]

        class FakeStore:
            def initialize(self):
                return None

            def upsert_events(self, events):
                return len(events)

        class FailingClient:
            def fetch_company_filings(self, *args, **kwargs):
                raise RuntimeError("network down")

        args = types.SimpleNamespace(limit=1, all_companies=False, form_type="8-K")
        with patch("us_radar.cli.load_settings", return_value=settings), \
             patch("us_radar.cli.load_watchlist", return_value=watchlist), \
             patch("us_radar.cli.EventStore.from_settings", return_value=FakeStore()), \
             patch("us_radar.cli.SecEdgarClient.from_settings", return_value=FailingClient()), \
             patch("us_radar.cli.time.sleep"):
            self.assertEqual(cmd_fetch_sec(args, "/tmp/unit"), 1)

        class PartiallyHealthyClient:
            def fetch_company_filings(self, cik, *args, **kwargs):
                if cik == "1":
                    raise RuntimeError("one request failed")
                return []

        with patch("us_radar.cli.load_settings", return_value=settings), \
             patch("us_radar.cli.load_watchlist", return_value=watchlist), \
             patch("us_radar.cli.EventStore.from_settings", return_value=FakeStore()), \
             patch("us_radar.cli.SecEdgarClient.from_settings", return_value=PartiallyHealthyClient()), \
             patch("us_radar.cli.time.sleep"):
            self.assertEqual(cmd_fetch_sec(args, "/tmp/unit"), 0)

    def test_sidecar_scripts_capture_stderr_in_their_own_logs(self):
        scripts_dir = Path(__file__).resolve().parents[1] / "scripts"
        for name in ("run_once.sh", "validate_once.sh", "push_daily.sh"):
            text = (scripts_dir / name).read_text(encoding="utf-8")
            self.assertIn("} 2>&1 | tee -a", text, name)

    def test_event_chain_summary_uses_median_instead_of_row_count(self):
        event = parse_atom_feed(SAMPLE_ATOM, form_type="8-K", ticker="NVDA")[0]
        targets = expand_event_targets(event, SAMPLE_GRAPH)
        with tempfile.TemporaryDirectory() as tmpdir:
            store = EventStore(Path(tmpdir) / "us_radar.sqlite3")
            store.initialize()
            store.upsert_events([event])
            store.upsert_signal_targets(targets)
            now = datetime.now(timezone.utc).isoformat()
            with store.connect() as conn:
                conn.execute("UPDATE signal_targets SET transmission_type='true_business' WHERE target_market='CN'")
                cn_targets = conn.execute(
                    "SELECT target_id, target_ticker, transmission_type FROM signal_targets WHERE target_market='CN' ORDER BY target_ticker"
                ).fetchall()
                for index, target in enumerate(cn_targets):
                    excess = 0.04 if index == 0 else -0.01
                    label = "single_event_signal" if abs(excess) > 0.02 else "below_threshold"
                    conn.execute(
                        """
                        INSERT INTO signal_validation_results (
                            validation_id, target_id, event_id, target_market,
                            target_ticker, horizon_days, data_quality,
                            measured_at, created_at, primary_excess, signal_label,
                            direction_label, event_direction, direction_source,
                            direction_confidence, quality_gate,
                            is_effective_sample, validation_rule_version
                        ) VALUES (?, ?, ?, 'CN', ?, 1, 'DATA_OK', ?, ?, ?, ?,
                                  'positive_transmission', 'bullish',
                                  'evidence_heuristic', 0.6, 'effective_sample', 1,
                                  'validation_v3_test')
                        """,
                        (f"v-{index}", target["target_id"], event.event_id, target["target_ticker"], now, now, excess, label),
                    )
            store.initialize()
            with store.connect() as conn:
                migrated_types = conn.execute(
                    "SELECT DISTINCT transmission_type FROM signal_validation_results"
                ).fetchall()
            self.assertEqual([row[0] for row in migrated_types], ["true_business"])
            self.assertEqual(store.refresh_event_chain_summaries(), 1)
            with store.connect() as conn:
                rows = conn.execute(
                    "SELECT transmission_type, effective_targets, significant_targets, median_primary_excess, signal_label FROM event_chain_validation_results ORDER BY transmission_type"
                ).fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["effective_targets"], 2)
            self.assertEqual(rows[0]["significant_targets"], 1)
            self.assertAlmostEqual(rows[0]["median_primary_excess"], 0.015)
            self.assertEqual(rows[0]["signal_label"], "below_threshold")

    def test_reset_derived_keeps_events(self):
        events = parse_atom_feed(SAMPLE_ATOM, form_type="8-K")
        with tempfile.TemporaryDirectory() as tmpdir:
            store = EventStore(Path(tmpdir) / "us_radar.sqlite3")
            store.initialize()
            store.upsert_events(events)
            store.upsert_signal_targets(expand_event_targets(events[0], SAMPLE_GRAPH))
            deleted = store.reset_derived_data()
            self.assertGreaterEqual(deleted["signal_targets"], 1)
            self.assertEqual(len(store.recent_events(datetime(2026, 5, 15, tzinfo=timezone.utc))), 1)

    def test_daily_push_handles_empty_sidecar_database(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            store = EventStore(root / "us_radar.sqlite3")
            store.initialize()
            message = build_daily_push(store, root, hours=24)
            self.assertIn("【美股雷达旁路·研究】", message.title)
            self.assertIn("暂无 DATA_OK 且达阈值的有效样本", message.content)
            self.assertIn(DISCLAIMER, message.content)


    def test_daily_push_top5_excludes_low_quality_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            store = EventStore(root / "us_radar.sqlite3")
            store.initialize()
            now = datetime.now(timezone.utc).isoformat()
            with store.connect() as conn:
                conn.execute(
                    """
                    INSERT INTO us_events (
                        event_id, source, event_type, ticker, company, cik,
                        accession, event_time, title, url, summary, raw_payload, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("event-good", "sec", "8-K", "NVDA", "NVIDIA", "0001045810", "acc-good", now, "good", "", "", "{}", now),
                )
                conn.execute(
                    """
                    INSERT INTO us_events (
                        event_id, source, event_type, ticker, company, cik,
                        accession, event_time, title, url, summary, raw_payload, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("event-stale", "sec", "8-K", "AMD", "AMD", "0000002488", "acc-stale", "2026-05-15T00:00:00+00:00", "stale", "", "", "{}", now),
                )
                for target_id, event_id, ticker in [
                    ("target-good", "event-good", "GOOD.CN"),
                    ("target-bad", "event-good", "BAD.CN"),
                    ("target-stale", "event-stale", "STALE.CN"),
                ]:
                    conn.execute(
                        """
                        INSERT INTO signal_targets (
                            target_id, event_id, target_market, target_ticker,
                            target_name, target_role, theme, link_reason, confidence,
                            transmission_type, enabled_for_research, enabled_for_trading, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0, ?)
                        """,
                        (target_id, event_id, "CN", ticker, ticker, "transmission", "ai_compute", "unit", 0.9, "true_business", now),
                    )
                for validation_id, target_id, event_id, ticker, quality, effective, gate, excess in [
                    ("validation-good", "target-good", "event-good", "GOOD.CN", "DATA_OK", 1, "effective_sample", 0.08),
                    ("validation-bad", "target-bad", "event-good", "BAD.CN", "PROVIDER_ERROR", 0, "excluded_quality", 0.50),
                    ("validation-stale", "target-stale", "event-stale", "STALE.CN", "DATA_OK", 1, "effective_sample", 0.90),
                ]:
                    conn.execute(
                        """
                        INSERT INTO signal_validation_results (
                            validation_id, target_id, event_id, target_market, target_ticker,
                            horizon_days, benchmark, target_return, benchmark_return,
                            excess_return, data_quality, measured_at, created_at,
                            primary_excess, signal_label, direction_label, direction_source,
                            quality_gate, is_effective_sample, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            validation_id, target_id, event_id, "CN", ticker, 3,
                            "000300.SH", 0.1, 0.02, excess, quality, now, now,
                            excess, "short_window_signal", "positive_transmission",
                            "evidence_heuristic", gate, effective, now,
                        ),
                    )
            message = build_daily_push(store, root, hours=24, limit=5)
            self.assertIn("GOOD.CN", message.content)
            self.assertNotIn("BAD.CN", message.content)
            self.assertNotIn("STALE.CN", message.content)


class MarketDataValidationTests(unittest.TestCase):
    def test_return_from_prices_uses_trading_days_after_event(self):
        prices = [
            PricePoint(datetime(2026, 5, 15).date(), 100.0),
            PricePoint(datetime(2026, 5, 18).date(), 101.0),
            PricePoint(datetime(2026, 5, 19).date(), 103.0),
            PricePoint(datetime(2026, 5, 20).date(), 104.0),
        ]
        result = _return_from_prices("TEST", "US", 3, datetime(2026, 5, 15).date(), prices, "unit")
        self.assertEqual(result.base_date, "2026-05-15")
        self.assertEqual(result.horizon_date, "2026-05-20")
        self.assertAlmostEqual(result.return_pct, (104.0 / 100.0) - 1)

    def test_return_from_prices_waits_when_forward_window_not_available(self):
        prices = [PricePoint(datetime(2026, 5, 15).date(), 100.0)]
        result = _return_from_prices("TEST", "US", 1, datetime(2026, 5, 15).date(), prices, "unit")
        self.assertEqual(result.data_quality, "INSUFFICIENT_FORWARD_DATA")
        self.assertEqual(result.base_date, "2026-05-15")
        self.assertIsNone(result.horizon_date)

    def test_us_event_base_date_uses_previous_close_before_market_close(self):
        self.assertEqual(_event_base_date("2026-05-15T15:00:00-04:00", "US").isoformat(), "2026-05-14")
        self.assertEqual(_event_base_date("2026-05-15T16:30:00-04:00", "US").isoformat(), "2026-05-15")
        self.assertEqual(_event_base_date("2026-05-17T12:00:00-04:00", "US").isoformat(), "2026-05-15")
        self.assertEqual(_event_base_date("2026-05-15T15:00:00-04:00", "CN").isoformat(), "2026-05-15")

    def test_cn_event_base_date_uses_last_close_before_shanghai_session(self):
        self.assertEqual(_event_base_date("2026-07-15T00:27:57+00:00", "CN").isoformat(), "2026-07-14")
        self.assertEqual(_event_base_date("2026-07-15T10:17:07+00:00", "CN").isoformat(), "2026-07-15")
        self.assertEqual(_event_base_date("2026-07-19T02:00:00+00:00", "CN").isoformat(), "2026-07-17")

    def test_cn_return_excludes_unadjusted_corporate_action_window(self):
        prices = [
            PricePoint(datetime(2026, 7, 22).date(), 100.0, 100.0),
            PricePoint(datetime(2026, 7, 23).date(), 101.0, 100.0),
            PricePoint(datetime(2026, 7, 24).date(), 99.0, 98.0),
        ]
        result = _return_from_prices("TEST.CN", "CN", 2, datetime(2026, 7, 22).date(), prices, "unit")
        self.assertEqual(result.data_quality, "CORPORATE_ACTION_UNADJUSTED")
        self.assertIsNone(result.return_pct)
        self.assertEqual(result.horizon_date, "2026-07-24")

    def test_cn_return_accepts_continuous_raw_window(self):
        prices = [
            PricePoint(datetime(2026, 7, 22).date(), 100.0, 99.0),
            PricePoint(datetime(2026, 7, 23).date(), 101.0, 100.0),
            PricePoint(datetime(2026, 7, 24).date(), 102.0, 101.0),
        ]
        result = _return_from_prices("TEST.CN", "CN", 2, datetime(2026, 7, 22).date(), prices, "unit")
        self.assertEqual(result.data_quality, "DATA_OK")
        self.assertAlmostEqual(result.return_pct, 0.02)

    def test_validation_labels_follow_frozen_thresholds(self):
        self.assertEqual(classify_signal(0.021, 1), "single_event_signal")
        self.assertEqual(classify_signal(-0.031, 3), "short_window_signal")
        self.assertEqual(classify_signal(0.049, 5), "below_threshold")
        self.assertEqual(classify_direction("bullish", 0.021, 1), "positive_transmission")
        self.assertEqual(classify_direction("bullish", 0.019, 1), "no_significant_move")
        self.assertEqual(classify_direction("bullish", -0.021, 1), "reverse_to_signal")
        self.assertEqual(classify_window("4", 3), "main_window")
        self.assertEqual(median_return([0.03, 0.01, 0.02]), 0.02)

    def test_quality_gate_requires_clean_completed_sample(self):
        self.assertEqual(classify_quality_gate("DATA_OK", 0.021), ("effective_sample", 1))
        self.assertEqual(classify_quality_gate("INSUFFICIENT_FORWARD_DATA", None), ("pending_validation", 0))
        self.assertEqual(classify_quality_gate("PROVIDER_ERROR", 0.021), ("pending_validation", 0))
        self.assertEqual(classify_quality_gate("BAD_BASE_PRICE", None), ("excluded_quality", 0))

    def test_event_direction_records_source(self):
        self.assertEqual(
            event_direction_from_quality({"quality_class": "form4_open_market", "net_form4_direction": "bearish"}),
            ("bearish", "form4_transaction", 0.95),
        )
        self.assertEqual(
            event_direction_from_quality({"event_type": "8-K", "quality_score": 0.68, "evidence_output_json": '{"signal_direction": "bullish", "confidence": 0.7}'}),
            ("bullish", "evidence_heuristic", 0.68),
        )
        self.assertEqual(
            event_direction_from_quality({"event_type": "8-K", "evidence_output_json": '{"signal_direction": "neutral", "confidence": 0.35}'}),
            ("unknown", "evidence_neutral", 0.35),
        )
        self.assertEqual(
            event_direction_from_quality({"quality_class": "form4_non_open_market", "event_type": "4"}),
            ("unknown", "form4_non_open_market", 0.0),
        )
        self.assertEqual(
            event_direction_from_quality(
                {
                    "event_type": "8-K",
                    "quality_class": "8k_high_signal",
                    "event_summary": "Company announced a new customer",
                    "evidence_output_json": '{"signal_direction": "neutral", "confidence": 0.35}',
                }
            ),
            ("bullish", "8k_text_heuristic", 0.45),
        )
        conflicted = event_direction_from_quality(
            {
                "event_type": "8-K",
                "event_summary": "New customer contract but lowered guidance",
                "evidence_output_json": '{"signal_direction": "neutral", "confidence": 0.35}',
            }
        )
        self.assertEqual(conflicted, ("unknown", "evidence_neutral", 0.35))

    def test_us_api_budget_keeps_cn_and_limits_unique_us_symbols(self):
        rows = [
            {"target_market": "CN", "target_ticker": "300308.SZ"},
            {"target_market": "US", "target_ticker": "QQQ"},
            {"target_market": "US", "target_ticker": "QQQ"},
            {"target_market": "US", "target_ticker": "NVDA"},
        ]
        kept, skipped = _apply_us_api_budget(rows, 1)
        self.assertEqual(skipped, 1)
        self.assertEqual([row["target_ticker"] for row in kept], ["300308.SZ", "QQQ", "QQQ"])

    def test_cn_missing_price_database_is_provider_error(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = MarketDataClient(Path(tmpdir))
            result = client.get_return("CN", "000001.SZ", "2026-05-15T00:00:00+00:00", 1)
            self.assertEqual(result.data_quality, "PROVIDER_ERROR")

    def test_cn_snapshot_is_required_by_default_even_when_live_db_exists(self):
        try:
            import duckdb
        except ImportError:
            self.skipTest("duckdb not installed")
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            live_db = root / "storage" / "database" / "zhulong.duckdb"
            live_db.parent.mkdir(parents=True)
            conn = duckdb.connect(str(live_db))
            conn.execute("CREATE TABLE fact_daily (symbol VARCHAR, trade_date DATE, close DOUBLE, pre_close DOUBLE)")
            conn.execute("INSERT INTO fact_daily VALUES ('000001.SZ', '2026-05-14', 100, 99), ('000001.SZ', '2026-05-15', 102, 100)")
            conn.close()

            client = MarketDataClient(root)
            self.assertEqual(client.zhulong_db.name, "zhulong_api_readonly.duckdb")
            result = client.get_return("CN", "000001.SZ", "2026-05-15T00:00:00+00:00", 1)
            self.assertEqual(result.data_quality, "PROVIDER_ERROR")

    def test_cn_live_readonly_fallback_requires_explicit_opt_out(self):
        try:
            import duckdb
        except ImportError:
            self.skipTest("duckdb not installed")
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            module_dir = root / "10_us_radar"
            module_dir.mkdir(parents=True)
            (module_dir / ".env.local").write_text("US_RADAR_REQUIRE_SNAPSHOT=0\n", encoding="utf-8")
            live_db = root / "storage" / "database" / "zhulong.duckdb"
            live_db.parent.mkdir(parents=True)
            conn = duckdb.connect(str(live_db))
            conn.execute("CREATE TABLE fact_daily (symbol VARCHAR, trade_date DATE, close DOUBLE, pre_close DOUBLE)")
            conn.execute("INSERT INTO fact_daily VALUES ('000001.SZ', '2026-05-14', 100, 99), ('000001.SZ', '2026-05-15', 102, 100)")
            conn.close()

            client = MarketDataClient(root)
            self.assertEqual(client.zhulong_db, live_db)
            result = client.get_return("CN", "000001.SZ", "2026-05-15T00:00:00+00:00", 1)
            self.assertEqual(result.data_quality, "DATA_OK")
            self.assertAlmostEqual(result.return_pct, 0.02)

    def test_cn_empty_price_query_is_no_price_data(self):
        try:
            import duckdb
        except ImportError:
            self.skipTest("duckdb not installed")
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            db_path = root / "storage" / "database" / "zhulong_api_readonly.duckdb"
            db_path.parent.mkdir(parents=True)
            conn = duckdb.connect(str(db_path))
            conn.execute("CREATE TABLE fact_daily (symbol VARCHAR, trade_date DATE, close DOUBLE)")
            conn.close()
            client = MarketDataClient(root)
            result = client.get_return("CN", "000001.SZ", "2026-05-15T00:00:00+00:00", 1)
            self.assertEqual(result.data_quality, "NO_PRICE_DATA")

    def test_yfinance_provider_return_uses_close_prices(self):
        try:
            import pandas as pd
        except ImportError:
            self.skipTest("pandas not installed")
        fake_yfinance = types.SimpleNamespace(
            download=lambda *args, **kwargs: pd.DataFrame(
                {"Close": [100.0, 103.0]},
                index=pd.to_datetime(["2026-05-15", "2026-05-18"]),
            )
        )
        previous = sys.modules.get("yfinance")
        sys.modules["yfinance"] = fake_yfinance
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                client = MarketDataClient(Path(tmpdir))
                result = client.provider_return("US", "QQQ", "2026-05-15T16:30:00-04:00", 1, "yfinance")
            self.assertEqual(result.provider, "yfinance.download")
            self.assertEqual(result.data_quality, "DATA_OK")
            self.assertAlmostEqual(result.return_pct, 0.03)
        finally:
            if previous is None:
                sys.modules.pop("yfinance", None)
            else:
                sys.modules["yfinance"] = previous

    def test_us_prices_can_skip_fmp_when_disabled(self):
        try:
            import pandas as pd
        except ImportError:
            self.skipTest("pandas not installed")
        fake_yfinance = types.SimpleNamespace(
            download=lambda *args, **kwargs: pd.DataFrame(
                {"Close": [100.0, 102.0]},
                index=pd.to_datetime(["2026-05-15", "2026-05-18"]),
            )
        )
        previous = sys.modules.get("yfinance")
        sys.modules["yfinance"] = fake_yfinance
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                (root / "10_us_radar").mkdir()
                (root / "10_us_radar" / ".env.local").write_text("US_RADAR_DISABLE_FMP=1\n", encoding="utf-8")
                client = MarketDataClient(root)
                client._fmp_prices = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("FMP should be skipped"))
                result = client.get_return("US", "QQQ", "2026-05-15T16:30:00-04:00", 1)
            self.assertEqual(result.provider, "yfinance.download")
            self.assertEqual(result.data_quality, "DATA_OK")
            self.assertAlmostEqual(result.return_pct, 0.02)
        finally:
            if previous is None:
                sys.modules.pop("yfinance", None)
            else:
                sys.modules["yfinance"] = previous

    def test_us_prices_fall_back_to_twelve_after_alpha(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            module_dir = root / "10_us_radar"
            module_dir.mkdir()
            (module_dir / ".env.local").write_text("US_RADAR_DISABLE_FMP=1\nUS_RADAR_TWELVE_DATA_API_KEY=unit\n", encoding="utf-8")
            client = MarketDataClient(root)
            client._yfinance_prices = lambda *args: []
            client._alpha_vantage_prices = lambda *args: []
            client._twelve_data_prices = lambda *args: [
                PricePoint(datetime(2026, 5, 15).date(), 100.0),
                PricePoint(datetime(2026, 5, 18).date(), 102.0),
            ]
            result = client.get_return("US", "QQQ", "2026-05-15T16:30:00-04:00", 1)
        self.assertEqual(result.provider, "twelve_data.time_series")
        self.assertEqual(result.data_quality, "DATA_OK")

    def test_twelve_data_cache_avoids_duplicate_api_call(self):
        payload = {
            "status": "ok",
            "values": [
                {"datetime": "2026-05-18", "close": "102.0"},
                {"datetime": "2026-05-15", "close": "100.0"},
            ],
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            module_dir = root / "10_us_radar"
            module_dir.mkdir()
            (module_dir / ".env.local").write_text("US_RADAR_TWELVE_DATA_API_KEY=unit\n", encoding="utf-8")
            client = MarketDataClient(root)
            with patch("us_radar.market_data._get_json", return_value=payload) as request:
                first = client.provider_return("US", "QQQ", "2026-05-15T16:30:00-04:00", 1, "twelve")
                second = client.provider_return("US", "QQQ", "2026-05-15T16:30:00-04:00", 1, "twelve")
        self.assertEqual(request.call_count, 1)
        self.assertEqual(first.data_quality, "DATA_OK")
        self.assertEqual(second.data_quality, "DATA_OK")

    def test_twelve_data_budget_is_hard_capped_at_seven(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            module_dir = root / "10_us_radar"
            module_dir.mkdir()
            (module_dir / ".env.local").write_text("US_RADAR_TWELVE_MAX_CALLS_PER_MINUTE=99\n", encoding="utf-8")
            client = MarketDataClient(root)
            self.assertTrue(all(client._take_twelve_budget() for _ in range(7)))
            self.assertFalse(client._take_twelve_budget())

    def test_alpaca_provider_return_uses_adjusted_sip_bars(self):
        payload = {
            "bars": [
                {"t": "2026-05-15T04:00:00Z", "c": 100.0},
                {"t": "2026-05-18T04:00:00Z", "c": 102.5},
            ]
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            module_dir = root / "10_us_radar"
            module_dir.mkdir()
            (module_dir / ".env.local").write_text(
                "US_RADAR_ALPACA_API_KEY=unit\nUS_RADAR_ALPACA_SECRET_KEY=secret\n",
                encoding="utf-8",
            )
            client = MarketDataClient(root)
            with patch("us_radar.market_data._get_json", return_value=payload) as request:
                result = client.provider_return("US", "SPY", "2026-05-15T16:30:00-04:00", 1, "alpaca")
        self.assertEqual(result.provider, "alpaca.sip.adjusted")
        self.assertEqual(result.data_quality, "DATA_OK")
        self.assertAlmostEqual(result.return_pct, 0.025)
        self.assertIn("adjustment=all", request.call_args.args[0])
        self.assertIn("feed=sip", request.call_args.args[0])
        self.assertEqual(request.call_args.kwargs["headers"]["APCA-API-KEY-ID"], "unit")

    def test_massive_provider_return_uses_adjusted_aggregates(self):
        payload = {
            "status": "OK",
            "results": [
                {"t": int(datetime(2026, 5, 15, 14, 30, tzinfo=timezone.utc).timestamp() * 1000), "c": 100.0},
                {"t": int(datetime(2026, 5, 18, 14, 30, tzinfo=timezone.utc).timestamp() * 1000), "c": 103.0},
            ],
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            module_dir = root / "10_us_radar"
            module_dir.mkdir()
            (module_dir / ".env.local").write_text("US_RADAR_MASSIVE_API_KEY=unit\n", encoding="utf-8")
            client = MarketDataClient(root)
            with patch("us_radar.market_data._get_json", return_value=payload) as request:
                result = client.provider_return("US", "SPY", "2026-05-15T16:30:00-04:00", 1, "massive")
        self.assertEqual(result.provider, "massive.aggs.adjusted")
        self.assertEqual(result.data_quality, "DATA_OK")
        self.assertAlmostEqual(result.return_pct, 0.03)
        self.assertIn("adjusted=true", request.call_args.args[0])

    def test_yfinance_provider_return_handles_multiindex_close(self):
        try:
            import pandas as pd
        except ImportError:
            self.skipTest("pandas not installed")
        columns = pd.MultiIndex.from_tuples([("Close", "QQQ")])
        fake_yfinance = types.SimpleNamespace(
            download=lambda *args, **kwargs: pd.DataFrame(
                [[100.0], [104.0]],
                index=pd.to_datetime(["2026-05-15", "2026-05-18"]),
                columns=columns,
            )
        )
        previous = sys.modules.get("yfinance")
        sys.modules["yfinance"] = fake_yfinance
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                client = MarketDataClient(Path(tmpdir))
                result = client.provider_return("US", "QQQ", "2026-05-15T16:30:00-04:00", 1, "yfinance")
            self.assertEqual(result.data_quality, "DATA_OK")
            self.assertAlmostEqual(result.return_pct, 0.04)
        finally:
            if previous is None:
                sys.modules.pop("yfinance", None)
            else:
                sys.modules["yfinance"] = previous

    def test_error_update_marks_single_row_provider_error(self):
        row = {
            "validation_id": "validation-error",
            "horizon_days": 1,
            "target_market": "CN",
            "target_ticker": "000001.SZ",
            "benchmark": "000300.SH",
            "event_type": "8-K",
        }
        update = _error_update(row, RuntimeError("unit"))
        self.assertEqual(update.data_quality, "PROVIDER_ERROR")
        self.assertEqual(update.price_provider, "exception:RuntimeError")
        self.assertEqual(update.is_effective_sample, 0)


if __name__ == "__main__":
    unittest.main()
