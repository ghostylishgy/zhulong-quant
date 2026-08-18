import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
AUDIT_PATH = ROOT / "tools" / "audit_shadow_corporate_actions.py"
LEDGER_PATH = ROOT / "tools" / "shadow_corporate_action_ledger.py"


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


AUDIT = load_module("bl018_audit_for_ledger", AUDIT_PATH)
LEDGER = load_module("bl018_ledger", LEDGER_PATH)


class CorporateActionLedgerTest(unittest.TestCase):
    def setUp(self):
        self.position = AUDIT.Position(
            "688809.SH", "2026-06-11", "task-1", 500, 0, 500, "2026-06-26"
        )
        sales = [
            AUDIT.Sale("688809.SH", "2026-06-11", "2026-06-12", "s1", 200),
            AUDIT.Sale("688809.SH", "2026-06-11", "2026-06-24", "s2", 200),
            AUDIT.Sale("688809.SH", "2026-06-11", "2026-06-26", "s3", 100),
        ]
        event = {
            "ts_code": "688809.SH",
            "end_date": "20251231",
            "ann_date": "20260520",
            "div_proc": "实施",
            "cash_div": 1.0,
            "cash_div_tax": 1.0,
            "stk_div": 0.2,
            "record_date": "20260618",
            "ex_date": "20260622",
            "pay_date": "20260624",
            "div_listdate": "20260625",
        }
        prices = [
            {"symbol": "688809.SH", "trade_date": "2026-06-18", "close": 626.0, "pre_close": 620.0},
            {"symbol": "688809.SH", "trade_date": "2026-06-22", "close": 521.0, "pre_close": 520.833333},
        ]
        self.audit = AUDIT.build_audit(
            [self.position], sales, [event], "2026-07-30", price_rows=prices
        )
        self.review = {
            "schema_version": LEDGER.REVIEW_SCHEMA,
            "source_audit_sha256": self.audit["payload_sha256"],
            "decision": "APPROVE_LEDGER_DRY_RUN",
            "reviewed_by": "tester",
            "reviewed_at": "2026-07-30T13:00:00+00:00",
            "notes": "fixture approval",
        }
        self.review["review_manifest_sha256"] = LEDGER.canonical_sha(self.review)
        self.tmp = tempfile.TemporaryDirectory()
        self.conn = duckdb.connect(str(Path(self.tmp.name) / "ledger.duckdb"))
        LEDGER.ensure_ledger_schema(self.conn)

    def tearDown(self):
        self.conn.close()
        self.tmp.cleanup()

    def records(self):
        return LEDGER.build_application_records(self.audit, self.review)

    def test_builds_stage_specific_cash_and_share_deltas(self):
        records = {record["stage"]: record for record in self.records()}
        self.assertEqual(set(records), LEDGER.ALLOWED_STAGES)
        self.assertEqual(records["RIGHTS_FROZEN"]["rights_frozen_qty"], 300)
        self.assertEqual(records["RECEIVABLE_CREATED"]["cash_receivable_delta_gross"], 300.0)
        self.assertEqual(records["RECEIVABLE_CREATED"]["pending_share_delta"], 60.0)
        self.assertEqual(records["CASH_PAYABLE"]["cash_balance_delta_gross"], 300.0)
        self.assertEqual(records["STOCK_AVAILABLE"]["pending_share_delta"], -60.0)
        self.assertEqual(records["STOCK_AVAILABLE"]["available_share_delta"], 60.0)
        self.assertIsNone(records["CASH_PAYABLE"]["tax_delta"])

    def test_exact_reapplication_is_idempotent(self):
        record = self.records()[0]
        self.assertEqual(LEDGER.append_application(self.conn, record), "APPLIED")
        self.assertEqual(LEDGER.append_application(self.conn, record), "ALREADY_APPLIED")
        self.assertEqual(
            self.conn.execute(
                f"SELECT COUNT(*) FROM {LEDGER.TABLE_NAME}"
            ).fetchone()[0],
            1,
        )

    def test_reviewed_batch_is_atomic_on_late_conflict(self):
        records = self.records()
        conflict = dict(next(item for item in records if item["stage"] == "CASH_PAYABLE"))
        conflict["reviewed_by"] = "other"
        conflict["record_sha256"] = LEDGER.application_record_sha(conflict)
        self.assertEqual(LEDGER.append_application(self.conn, conflict), "APPLIED")

        with self.assertRaises(LEDGER.ApplicationConflict):
            LEDGER.append_reviewed_batch(self.conn, self.audit, self.review)

        stored = self.conn.execute(
            f"SELECT application_id, record_sha256 FROM {LEDGER.TABLE_NAME}"
        ).fetchall()
        self.assertEqual(stored, [(conflict["application_id"], conflict["record_sha256"])])

    def test_application_id_must_bind_event_lot_and_stage(self):
        record = self.records()[0]
        record["application_id"] = "d" * 64
        record["record_sha256"] = LEDGER.application_record_sha(record)
        with self.assertRaisesRegex(LEDGER.LedgerContractError, "does not bind"):
            LEDGER.validate_application_record(record)

    def test_same_application_id_with_changed_content_is_rejected(self):
        record = self.records()[0]
        LEDGER.append_application(self.conn, record)
        changed = dict(record)
        changed["reviewed_by"] = "other"
        changed["record_sha256"] = LEDGER.application_record_sha(changed)
        with self.assertRaises(LEDGER.ApplicationConflict):
            LEDGER.append_application(self.conn, changed)

    def test_event_revision_requires_reviewed_correction_path(self):
        record = self.records()[0]
        LEDGER.append_application(self.conn, record)
        revised = dict(record)
        revised["event_sha256"] = "b" * 64
        revised["application_id"] = LEDGER.canonical_sha(
            {
                "event_sha256": revised["event_sha256"],
                "position_trade_date": revised["position_trade_date"],
                "stage": revised["stage"],
            }
        )
        revised["record_sha256"] = LEDGER.application_record_sha(revised)
        with self.assertRaises(LEDGER.EventRevisionConflict):
            LEDGER.append_application(self.conn, revised)

    def test_tampered_audit_is_rejected(self):
        tampered = dict(self.audit)
        tampered["impacts_found"] = 99
        with self.assertRaisesRegex(LEDGER.LedgerContractError, "audit payload sha"):
            LEDGER.build_application_records(tampered, self.review)

    def test_review_must_bind_exact_audit(self):
        review = dict(self.review)
        review["source_audit_sha256"] = "c" * 64
        review["review_manifest_sha256"] = LEDGER.canonical_sha(
            {key: value for key, value in review.items() if key != "review_manifest_sha256"}
        )
        with self.assertRaisesRegex(LEDGER.LedgerContractError, "does not bind"):
            LEDGER.build_application_records(self.audit, review)

    def test_pending_fifo_tax_cannot_carry_tax_delta(self):
        record = next(
            item for item in self.records() if item["stage"] == "CASH_PAYABLE"
        )
        record["tax_delta"] = -30.0
        record["record_sha256"] = LEDGER.application_record_sha(record)
        with self.assertRaisesRegex(LEDGER.LedgerContractError, "pending FIFO"):
            LEDGER.validate_application_record(record)

    def test_missing_price_anchor_blocks_ledger_plan(self):
        impact = self.audit["impacts"][0]
        impact["price_anchor_evidence"] = {"status": "UNAVAILABLE"}
        self.audit["payload_sha256"] = AUDIT.canonical_sha(
            {
                key: value
                for key, value in self.audit.items()
                if key not in {"generated_at", "payload_sha256"}
            }
        )
        self.review["source_audit_sha256"] = self.audit["payload_sha256"]
        self.review["review_manifest_sha256"] = LEDGER.canonical_sha(
            {
                key: value
                for key, value in self.review.items()
                if key != "review_manifest_sha256"
            }
        )
        with self.assertRaisesRegex(LEDGER.LedgerContractError, "price anchor"):
            self.records()


if __name__ == "__main__":
    unittest.main()
