import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
AUDIT_PATH = ROOT / "tools" / "audit_shadow_corporate_actions.py"
TAX_PATH = ROOT / "tools" / "shadow_dividend_tax_recovery.py"


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


AUDIT = load_module("bl018_audit_for_tax", AUDIT_PATH)
TAX = load_module("bl018_tax_recovery", TAX_PATH)


class DividendTaxRecoveryTest(unittest.TestCase):
    def setUp(self):
        positions = [
            AUDIT.Position(
                "000001.SZ", "2025-01-10", "task-old", 100, 0, 100, "2026-02-01"
            ),
            AUDIT.Position(
                "000001.SZ", "2026-01-01", "task-new", 200, 0, 200, "2026-03-02"
            ),
        ]
        sales = [
            AUDIT.Sale("000001.SZ", "2025-01-10", "2026-02-01", "sale-1", 100),
            AUDIT.Sale("000001.SZ", "2026-01-01", "2026-02-01", "sale-2", 50),
            AUDIT.Sale("000001.SZ", "2026-01-01", "2026-03-02", "sale-3", 150),
        ]
        event = {
            "ts_code": "000001.SZ",
            "end_date": "20251231",
            "ann_date": "20260105",
            "div_proc": "实施",
            "cash_div": 1.0,
            "cash_div_tax": 1.0,
            "stk_div": 0.0,
            "stk_bo_rate": 0.0,
            "stk_co_rate": 0.0,
            "record_date": "20260115",
            "ex_date": "20260116",
            "pay_date": "20260120",
            "div_listdate": "",
        }
        self.event = event
        prices = [
            {
                "symbol": "000001.SZ",
                "trade_date": "2026-01-15",
                "close": 100.0,
                "pre_close": 99.0,
            },
            {
                "symbol": "000001.SZ",
                "trade_date": "2026-01-16",
                "close": 99.5,
                "pre_close": 99.0,
            },
        ]
        self.prices = prices
        self.audit = AUDIT.build_audit(
            positions, sales, [event], "2026-03-03", price_rows=prices
        )
        self.assertEqual(self.audit["quality_status"], "DATA_OK")
        self.preview = TAX.build_tax_preview(self.audit)
        self.review = {
            "schema_version": TAX.REVIEW_SCHEMA,
            "source_tax_preview_sha256": self.preview["payload_sha256"],
            "decision": TAX.APPROVE_DECISION,
            "reviewed_by": "tester",
            "reviewed_at": "2026-07-30T14:00:00+00:00",
            "notes": "fixture approval",
        }
        self.review["review_manifest_sha256"] = TAX.canonical_sha(self.review)
        self.tmp = tempfile.TemporaryDirectory()
        self.conn = duckdb.connect(str(Path(self.tmp.name) / "tax.duckdb"))
        TAX.ensure_tax_ledger_schema(self.conn)

    def tearDown(self):
        self.conn.close()
        self.tmp.cleanup()

    def records(self):
        return TAX.build_tax_records(self.preview, self.review)

    def test_account_fifo_spans_three_tax_buckets(self):
        allocations = self.preview["allocations"]
        self.assertEqual([item["allocated_qty"] for item in allocations], [100, 50, 150])
        self.assertEqual(
            [item["holding_period_bucket"] for item in allocations],
            ["GT_1_YEAR", "LE_1_MONTH", "GT_1_MONTH_LE_1_YEAR"],
        )
        self.assertEqual([item["tax_delta"] for item in allocations], [0.0, -10.0, -15.0])
        summary = self.preview["event_summaries"][0]
        self.assertEqual(summary["entitled_qty"], 300)
        self.assertEqual(summary["remaining_entitled_qty"], 0)
        self.assertEqual(summary["tax_recovered_to_date"], -25.0)
        self.assertEqual(summary["cash_receivable_net_final"], 275.0)
        self.assertEqual(summary["tax_settlement_status"], "SETTLED")

    def test_calendar_month_and_year_boundaries_are_inclusive(self):
        self.assertEqual(
            TAX.holding_period_terms("2026-01-31", "2026-02-28")[
                "holding_period_bucket"
            ],
            "LE_1_MONTH",
        )
        self.assertEqual(
            TAX.holding_period_terms("2026-01-31", "2026-03-01")[
                "holding_period_bucket"
            ],
            "GT_1_MONTH_LE_1_YEAR",
        )
        self.assertEqual(
            TAX.holding_period_terms("2026-01-31", "2027-01-31")[
                "holding_period_bucket"
            ],
            "GT_1_MONTH_LE_1_YEAR",
        )
        self.assertEqual(
            TAX.holding_period_terms("2026-01-31", "2027-02-01")[
                "holding_period_bucket"
            ],
            "GT_1_YEAR",
        )

    def test_partial_sale_keeps_final_net_cash_unknown(self):
        positions = [
            AUDIT.Position(
                "000001.SZ", "2025-01-10", "task-old", 100, 0, 100, "2026-02-01"
            ),
            AUDIT.Position(
                "000001.SZ", "2026-01-01", "task-new", 200, 200, 0, ""
            ),
        ]
        sales = [
            AUDIT.Sale("000001.SZ", "2025-01-10", "2026-02-01", "sale-1", 100)
        ]
        partial_audit = AUDIT.build_audit(
            positions,
            sales,
            [self.event],
            "2026-03-03",
            price_rows=self.prices,
        )
        preview = TAX.build_tax_preview(partial_audit)
        summary = preview["event_summaries"][0]
        self.assertEqual(summary["allocated_sold_qty"], 100)
        self.assertEqual(summary["remaining_entitled_qty"], 200)
        self.assertIsNone(summary["cash_receivable_net_final"])
        self.assertEqual(summary["tax_settlement_status"], "PARTIALLY_SETTLED")

    def test_sale_evidence_sha_is_mandatory(self):
        tampered = dict(self.audit)
        tampered["sale_evidence"] = list(self.audit["sale_evidence"][:-1])
        tampered["payload_sha256"] = TAX.canonical_sha(
            {
                key: value
                for key, value in tampered.items()
                if key not in {"generated_at", "payload_sha256"}
            }
        )
        with self.assertRaisesRegex(TAX.TaxContractError, "sale evidence sha"):
            TAX.build_tax_preview(tampered)

    def test_rehashed_but_incomplete_sale_evidence_is_rejected(self):
        tampered = dict(self.audit)
        tampered["sale_evidence"] = list(self.audit["sale_evidence"][:-1])
        tampered["sale_evidence_sha256"] = TAX.canonical_sha(
            {"sales": tampered["sale_evidence"]}
        )
        tampered["payload_sha256"] = TAX.canonical_sha(
            {
                key: value
                for key, value in tampered.items()
                if key not in {"generated_at", "payload_sha256"}
            }
        )
        with self.assertRaisesRegex(TAX.TaxContractError, "realized lot quantity"):
            TAX.build_tax_preview(tampered)

    def test_conflicting_duplicate_sale_is_rejected(self):
        sales = list(self.audit["sale_evidence"])
        conflict = dict(sales[0])
        conflict["qty_sold"] = 999
        sales.append(conflict)
        tampered = dict(self.audit)
        tampered["sale_evidence"] = sales
        tampered["sale_evidence_sha256"] = TAX.canonical_sha({"sales": sales})
        tampered["payload_sha256"] = TAX.canonical_sha(
            {
                key: value
                for key, value in tampered.items()
                if key not in {"generated_at", "payload_sha256"}
            }
        )
        with self.assertRaisesRegex(TAX.TaxContractError, "conflicting content"):
            TAX.build_tax_preview(tampered)

    def test_same_day_buy_and_sell_requires_daily_net_contract(self):
        tampered = dict(self.audit)
        positions = list(self.audit["position_evidence"])
        positions.append(
            {
                "symbol": "000001.SZ",
                "position_trade_date": "2026-02-01",
                "signal_task_id": "same-day-buy",
                "initial_qty": 100,
                "current_qty": 100,
                "realized_qty": 0,
                "exit_trade_date": "",
            }
        )
        positions.sort(
            key=lambda item: (
                item["position_trade_date"], item["symbol"], item["signal_task_id"]
            )
        )
        tampered["position_evidence"] = positions
        tampered["position_evidence_sha256"] = TAX.canonical_sha(
            {"positions": positions}
        )
        tampered["payload_sha256"] = TAX.canonical_sha(
            {
                key: value
                for key, value in tampered.items()
                if key not in {"generated_at", "payload_sha256"}
            }
        )
        with self.assertRaisesRegex(TAX.TaxContractError, "same-day account netting"):
            TAX.build_tax_preview(tampered)

    def test_unrelated_symbol_same_day_activity_does_not_block(self):
        tampered = dict(self.audit)
        positions = list(self.audit["position_evidence"])
        positions.append(
            {
                "symbol": "600000.SH",
                "position_trade_date": "2026-02-01",
                "signal_task_id": "unrelated-buy",
                "initial_qty": 100,
                "current_qty": 0,
                "realized_qty": 100,
                "exit_trade_date": "2026-02-01",
            }
        )
        positions.sort(
            key=lambda item: (
                item["position_trade_date"], item["symbol"], item["signal_task_id"]
            )
        )
        sales = list(self.audit["sale_evidence"])
        sales.append(
            {
                "symbol": "600000.SH",
                "position_trade_date": "2026-02-01",
                "trade_date": "2026-02-01",
                "idempotency_key": "unrelated-sale",
                "qty_sold": 100,
            }
        )
        sales.sort(
            key=lambda item: (
                item["trade_date"], item["symbol"], item["idempotency_key"]
            )
        )
        tampered["position_evidence"] = positions
        tampered["position_evidence_sha256"] = TAX.canonical_sha(
            {"positions": positions}
        )
        tampered["sale_evidence"] = sales
        tampered["sale_evidence_sha256"] = TAX.canonical_sha({"sales": sales})
        tampered["payload_sha256"] = TAX.canonical_sha(
            {
                key: value
                for key, value in tampered.items()
                if key not in {"generated_at", "payload_sha256"}
            }
        )
        preview = TAX.build_tax_preview(tampered)
        self.assertEqual(preview["allocation_count"], 3)

    def test_stock_or_mixed_event_fails_closed(self):
        audit = dict(self.audit)
        events = [dict(self.audit["events"][0])]
        events[0]["event_type"] = "CASH_AND_STOCK"
        events[0]["stock_div_per_share"] = 0.1
        events[0]["stock_bonus_rate"] = 0.1
        audit["events"] = events
        audit["payload_sha256"] = TAX.canonical_sha(
            {
                key: value
                for key, value in audit.items()
                if key not in {"generated_at", "payload_sha256"}
            }
        )
        with self.assertRaisesRegex(TAX.TaxContractError, "pure cash"):
            TAX.build_tax_preview(audit)

    def test_exact_reapplication_is_idempotent(self):
        record = self.records()[0]
        self.assertEqual(TAX.append_tax_record(self.conn, record), "APPLIED")
        self.assertEqual(TAX.append_tax_record(self.conn, record), "ALREADY_APPLIED")
        self.assertEqual(
            self.conn.execute(f"SELECT COUNT(*) FROM {TAX.TABLE_NAME}").fetchone()[0],
            1,
        )

    def test_reviewed_batch_is_atomic_on_late_conflict(self):
        records = self.records()
        conflict = dict(records[-1])
        conflict["reviewed_by"] = "other"
        conflict["record_sha256"] = TAX.tax_record_sha(conflict)
        self.assertEqual(TAX.append_tax_record(self.conn, conflict), "APPLIED")

        with self.assertRaises(TAX.TaxApplicationConflict):
            TAX.append_reviewed_tax_batch(self.conn, self.preview, self.review)

        stored = self.conn.execute(
            f"SELECT tax_application_id, record_sha256 FROM {TAX.TABLE_NAME}"
        ).fetchall()
        self.assertEqual(
            stored,
            [(conflict["tax_application_id"], conflict["record_sha256"])],
        )

    def test_same_application_id_with_changed_content_is_rejected(self):
        record = self.records()[0]
        TAX.append_tax_record(self.conn, record)
        changed = dict(record)
        changed["reviewed_by"] = "other"
        changed["record_sha256"] = TAX.tax_record_sha(changed)
        with self.assertRaises(TAX.TaxApplicationConflict):
            TAX.append_tax_record(self.conn, changed)

    def test_event_revision_requires_reviewed_correction(self):
        record = self.records()[0]
        TAX.append_tax_record(self.conn, record)
        revised = dict(record)
        revised["event_sha256"] = "b" * 64
        revised["tax_application_id"] = TAX.tax_application_id(
            revised["event_sha256"], revised["lot_id"], revised["sale_idempotency_key"]
        )
        revised["record_sha256"] = TAX.tax_record_sha(revised)
        with self.assertRaises(TAX.TaxEventRevisionConflict):
            TAX.append_tax_record(self.conn, revised)

    def test_review_must_bind_exact_preview(self):
        review = dict(self.review)
        review["source_tax_preview_sha256"] = "c" * 64
        review["review_manifest_sha256"] = TAX.canonical_sha(
            {
                key: value
                for key, value in review.items()
                if key != "review_manifest_sha256"
            }
        )
        with self.assertRaisesRegex(TAX.TaxContractError, "does not bind"):
            TAX.build_tax_records(self.preview, review)

    def test_sale_after_source_as_of_is_rejected(self):
        tampered = dict(self.audit)
        sales = [dict(item) for item in self.audit["sale_evidence"]]
        sales[-1]["trade_date"] = "2026-03-04"
        sales.sort(
            key=lambda item: (
                item["trade_date"], item["symbol"], item["idempotency_key"]
            )
        )
        tampered["sale_evidence"] = sales
        tampered["sale_evidence_sha256"] = TAX.canonical_sha({"sales": sales})
        tampered["payload_sha256"] = TAX.canonical_sha(
            {
                key: value
                for key, value in tampered.items()
                if key not in {"generated_at", "payload_sha256"}
            }
        )
        with self.assertRaisesRegex(TAX.TaxContractError, "exceeds source_as_of"):
            TAX.build_tax_preview(tampered)


if __name__ == "__main__":
    unittest.main()
