import importlib.util
import sys
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "tools" / "audit_shadow_corporate_actions.py"
SPEC = importlib.util.spec_from_file_location("bl018_audit", MODULE_PATH)
MOD = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules[SPEC.name] = MOD
SPEC.loader.exec_module(MOD)


def event(**overrides):
    row = {
        "ts_code": "688809.SH",
        "end_date": "20251231",
        "ann_date": "20260520",
        "div_proc": "实施",
        "cash_div": 1.0,
        "cash_div_tax": 1.0,
        "stk_div": 0.0,
        "record_date": "20260618",
        "ex_date": "20260622",
        "pay_date": "20260624",
        "div_listdate": None,
    }
    row.update(overrides)
    return row


class CorporateActionAuditTest(unittest.TestCase):
    def setUp(self):
        self.position = MOD.Position(
            symbol="688809.SH",
            position_trade_date="2026-06-11",
            signal_task_id="task-1",
            initial_qty=500,
            current_qty=0,
            realized_qty=500,
            exit_trade_date="2026-06-26",
        )
        self.sales = [
            MOD.Sale("688809.SH", "2026-06-11", "2026-06-12", "s1", 200),
            MOD.Sale("688809.SH", "2026-06-11", "2026-06-24", "s2", 200),
            MOD.Sale("688809.SH", "2026-06-11", "2026-06-26", "s3", 100),
        ]

    def test_partial_sale_before_record_freezes_remaining_quantity(self):
        report = MOD.build_audit(
            [self.position], self.sales, [event()], "2026-07-28"
        )
        impact = report["impacts"][0]
        self.assertEqual(impact["rights_frozen_qty"], 300)
        self.assertEqual(impact["sold_through_record_date"], 200)
        self.assertEqual(impact["cash_receivable_gross"], 300.0)
        self.assertEqual(report["quality_status"], "DATA_OK")

    def test_sale_on_record_date_reduces_entitlement(self):
        sales = [MOD.Sale("688809.SH", "2026-06-11", "2026-06-18", "s1", 200)]
        qty, sold = MOD.quantity_at_record_date(self.position, sales, "2026-06-18")
        self.assertEqual((qty, sold), (300, 200))

    def test_pay_date_is_not_collapsed_into_ex_date(self):
        report = MOD.build_audit(
            [self.position], self.sales, [event()], "2026-07-28"
        )
        impact = report["impacts"][0]
        self.assertEqual(impact["ex_date"], "2026-06-22")
        self.assertEqual(impact["pay_date"], "2026-06-24")
        self.assertEqual(impact["stages"]["ex_date"], "RECEIVABLE_CREATED")
        self.assertEqual(impact["stages"]["pay_date"], "CASH_PAYABLE")

    def test_stock_dividend_remains_pending_until_list_date(self):
        stock_event = event(
            cash_div=0.0,
            cash_div_tax=0.0,
            stk_div=0.2,
            div_listdate="20260625",
        )
        report = MOD.build_audit(
            [self.position], self.sales, [stock_event], "2026-07-28"
        )
        impact = report["impacts"][0]
        self.assertEqual(impact["pending_stock_qty_raw"], 60.0)
        self.assertEqual(impact["div_listdate"], "2026-06-25")
        self.assertEqual(impact["stages"]["div_listdate"], "STOCK_AVAILABLE")

    def test_multiple_entry_lots_remain_separate_and_aggregate(self):
        second_lot = MOD.Position(
            symbol="688809.SH",
            position_trade_date="2026-06-01",
            signal_task_id="task-2",
            initial_qty=400,
            current_qty=400,
            realized_qty=0,
            exit_trade_date="",
        )
        report = MOD.build_audit(
            [self.position, second_lot], self.sales, [event()], "2026-07-28"
        )
        self.assertEqual(len(report["impacts"]), 2)
        total = report["event_impact_totals"][0]
        self.assertEqual(total["position_lot_count"], 2)
        self.assertEqual(total["rights_frozen_qty_total"], 700)
        self.assertEqual(total["cash_receivable_gross_total"], 700.0)
        keys = {
            stage["application_key"]
            for impact in report["impacts"]
            for stage in impact["stage_application_preview"]
        }
        self.assertEqual(len(keys), report["application_preview_count"])

    def test_cash_and_stock_event_preserves_both_receivables(self):
        combined = event(stk_div=0.2, div_listdate="20260625")
        report = MOD.build_audit(
            [self.position], self.sales, [combined], "2026-07-28"
        )
        impact = report["impacts"][0]
        self.assertEqual(report["events"][0]["event_type"], "CASH_AND_STOCK")
        self.assertEqual(impact["cash_receivable_gross"], 300.0)
        self.assertEqual(impact["pending_stock_qty_raw"], 60.0)
        self.assertIsNone(impact["cash_receivable_net"])
        self.assertEqual(impact["dividend_tax_status"], "PENDING_FIFO_SALE_LEDGER")
        self.assertEqual(
            {item["stage"] for item in impact["stage_application_preview"]},
            {"RIGHTS_FROZEN", "RECEIVABLE_CREATED", "CASH_PAYABLE", "STOCK_AVAILABLE"},
        )

    def test_price_anchor_is_cross_check_evidence_not_execution_price(self):
        price_rows = [
            {
                "symbol": "688809.SH",
                "trade_date": "2026-06-19",
                "close": 10.0,
                "pre_close": 9.8,
            },
            {
                "symbol": "688809.SH",
                "trade_date": "2026-06-22",
                "close": 9.5,
                "pre_close": 9.0,
            },
        ]
        report = MOD.build_audit(
            [self.position], self.sales, [event()], "2026-07-28", price_rows
        )
        evidence = report["impacts"][0]["price_anchor_evidence"]
        self.assertEqual(evidence["status"], "EVIDENCE_AVAILABLE")
        self.assertEqual(evidence["theoretical_reference"], 9.0)
        self.assertEqual(evidence["absolute_delta"], 0.0)
        self.assertEqual(evidence["authority"], "CROSS_CHECK_ONLY")

    def test_application_key_binds_event_lot_and_stage(self):
        first = MOD.stage_application_key("a" * 64, "2026-06-11", "RIGHTS_FROZEN")
        repeated = MOD.stage_application_key("a" * 64, "2026-06-11", "RIGHTS_FROZEN")
        other_stage = MOD.stage_application_key("a" * 64, "2026-06-11", "CASH_PAYABLE")
        other_lot = MOD.stage_application_key("a" * 64, "2026-06-12", "RIGHTS_FROZEN")
        self.assertEqual(first, repeated)
        self.assertEqual(len({first, other_stage, other_lot}), 3)

    def test_duplicate_rows_dedupe_by_content_hash(self):
        events, warnings = MOD.dedupe_events([event(), event()], "2026-07-28")
        self.assertEqual(len(events), 1)
        self.assertEqual(warnings, [])

    def test_changed_content_for_same_identity_marks_revision(self):
        rows = [event(cash_div=1.0), event(cash_div=1.2)]
        events, warnings = MOD.dedupe_events(
            rows, "2026-07-28"
        )
        self.assertEqual(len(events), 2)
        self.assertTrue(warnings[0].startswith("EVENT_REVISION_DETECTED:"))
        report = MOD.build_audit([self.position], self.sales, rows, "2026-07-28")
        self.assertEqual(report["quality_status"], "EVENT_REVISION_DETECTED")
        self.assertEqual(report["impacts"], [])
        self.assertTrue(
            any(item.startswith("AMBIGUOUS_EVENT_SKIPPED:") for item in report["warnings"])
        )

    def test_fetch_date_does_not_change_event_content_sha(self):
        first = MOD.normalize_event(event(source="fixture-a"), "2026-07-28")
        second = MOD.normalize_event(event(source="fixture-b"), "2026-07-29")
        self.assertEqual(first["event_identity"], second["event_identity"])
        self.assertEqual(first["event_sha256"], second["event_sha256"])

    def test_ex_date_revision_keeps_identity_and_changes_sha(self):
        first = MOD.normalize_event(event(ex_date="20260622"), "2026-07-28")
        second = MOD.normalize_event(event(ex_date="20260623"), "2026-07-28")
        self.assertEqual(first["event_identity"], second["event_identity"])
        self.assertNotEqual(first["event_sha256"], second["event_sha256"])

    def test_missing_sell_history_is_partial_not_guessed(self):
        report = MOD.build_audit(
            [self.position], self.sales[:1], [event()], "2026-07-28"
        )
        self.assertEqual(report["quality_status"], "PARTIAL_HISTORY")
        self.assertFalse(report["impacts"][0]["history_complete"])

    def test_proposal_row_is_not_an_implemented_event(self):
        report = MOD.build_audit(
            [self.position], self.sales, [event(div_proc="预案")], "2026-07-28"
        )
        self.assertEqual(report["quality_status"], "VALID_EMPTY")
        self.assertEqual(report["impacts"], [])

    def test_position_closed_before_record_date_has_no_impact(self):
        closed = MOD.Position(
            symbol="688809.SH",
            position_trade_date="2026-06-11",
            signal_task_id="task-closed",
            initial_qty=500,
            current_qty=0,
            realized_qty=500,
            exit_trade_date="2026-06-17",
        )
        report = MOD.build_audit([closed], [], [event()], "2026-07-28")
        self.assertEqual(report["quality_status"], "VALID_EMPTY")
        self.assertEqual(report["warnings"], [])


if __name__ == "__main__":
    unittest.main()
