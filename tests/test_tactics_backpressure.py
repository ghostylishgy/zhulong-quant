#!/usr/bin/env python3

import importlib.util
import sys
import threading
import types
import unittest
from pathlib import Path

from tests import _test_log_isolation  # noqa: F401

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "03_tactics" / "tactics_bridge.py"
spec = importlib.util.spec_from_file_location("zhulong_test_tactics_backpressure", MODULE_PATH)
MOD = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = MOD
spec.loader.exec_module(MOD)


class TacticsBackpressureTest(unittest.TestCase):
    def test_eagle_persistence_does_not_use_bounded_tactics_slots(self):
        global_started = threading.Event()
        global_release = threading.Event()
        eagle_started = threading.Event()
        eagle_release = threading.Event()

        def global_blocked():
            global_started.set()
            global_release.wait(timeout=2)

        def eagle_blocked():
            eagle_started.set()
            eagle_release.wait(timeout=2)
            return "persisted"

        global_future = MOD.fire_and_forget("GlobalBlocked", global_blocked)
        self.assertIsNotNone(global_future)
        self.assertTrue(global_started.wait(timeout=1))

        eagle_future = MOD._submit_eagle_persistence(eagle_blocked)
        self.assertIsNotNone(eagle_future)
        self.assertTrue(eagle_started.wait(timeout=1))

        eagle_running = MOD.eagle_persistence_snapshot()
        self.assertGreaterEqual(eagle_running["inflight"], 1)
        self.assertGreaterEqual(eagle_running["active"], 1)

        eagle_release.set()
        global_release.set()
        self.assertEqual(eagle_future.result(timeout=2), "persisted")
        global_future.result(timeout=2)

    def test_eagle_persistence_queues_within_capacity(self):
        started = threading.Event()
        release = threading.Event()

        def blocked():
            started.set()
            release.wait(timeout=2)
            return "first"

        first = MOD._submit_eagle_persistence(blocked)
        self.assertIsNotNone(first)
        self.assertTrue(started.wait(timeout=1))
        second = MOD._submit_eagle_persistence(lambda: "second")
        self.assertIsNotNone(second)

        queued = MOD.eagle_persistence_snapshot()
        self.assertGreaterEqual(queued["inflight"], 2)
        self.assertGreaterEqual(queued["queued"], 1)
        self.assertEqual(queued["capacity"], MOD._EAGLE_PERSISTENCE_CAPACITY)

        release.set()
        self.assertEqual(first.result(timeout=2), "first")
        self.assertEqual(second.result(timeout=2), "second")
        completed = MOD.eagle_persistence_snapshot()
        self.assertEqual(completed["inflight"], 0)
        self.assertEqual(completed["active"], 0)
        self.assertGreaterEqual(completed["completed"], 2)
        self.assertEqual(completed["submit_failed"], 0)

    def test_eagle_persistence_rejects_explicitly_at_capacity(self):
        old_capacity = MOD._EAGLE_PERSISTENCE_CAPACITY
        old_timeout = MOD._EAGLE_PERSISTENCE_SUBMIT_TIMEOUT_MS
        old_slots = MOD._eagle_persistence_slots
        started = threading.Event()
        release = threading.Event()
        before = MOD.eagle_persistence_snapshot()["rejected"]
        try:
            MOD._EAGLE_PERSISTENCE_CAPACITY = 1
            MOD._EAGLE_PERSISTENCE_SUBMIT_TIMEOUT_MS = 10
            MOD._eagle_persistence_slots = threading.BoundedSemaphore(1)

            def blocked():
                started.set()
                release.wait(timeout=2)
                return "first"

            first = MOD._submit_eagle_persistence(blocked)
            self.assertIsNotNone(first)
            self.assertTrue(started.wait(timeout=1))
            self.assertIsNone(MOD._submit_eagle_persistence(lambda: "overflow"))
            self.assertEqual(
                MOD.eagle_persistence_snapshot()["rejected"], before + 1
            )
            release.set()
            self.assertEqual(first.result(timeout=2), "first")
        finally:
            release.set()
            MOD._EAGLE_PERSISTENCE_CAPACITY = old_capacity
            MOD._EAGLE_PERSISTENCE_SUBMIT_TIMEOUT_MS = old_timeout
            MOD._eagle_persistence_slots = old_slots

    def test_eagle_observer_sides_retry_independently(self):
        old_attempts = MOD._EAGLE_PERSISTENCE_RETRY_ATTEMPTS
        old_delay = MOD._EAGLE_PERSISTENCE_RETRY_DELAY_MS
        old_signal = sys.modules.get("tactic_signal_bus")
        old_protocol = sys.modules.get("protocol_observer")
        signal_calls = []
        protocol_calls = []
        try:
            MOD._EAGLE_PERSISTENCE_RETRY_ATTEMPTS = 3
            MOD._EAGLE_PERSISTENCE_RETRY_DELAY_MS = 1
            signal_module = types.ModuleType("tactic_signal_bus")
            protocol_module = types.ModuleType("protocol_observer")

            def record_signal(**kwargs):
                signal_calls.append(kwargs)
                return len(signal_calls) >= 2

            def record_protocol(**kwargs):
                protocol_calls.append(kwargs)
                return True

            signal_module.record_tactic_signal = record_signal
            protocol_module.PROTOCOL_EAGLE = "EAGLE"
            protocol_module.TRIGGER_EAGLE_PULSE = "EAGLE_PULSE_AUDIT"
            protocol_module.record_protocol_event = record_protocol
            sys.modules["tactic_signal_bus"] = signal_module
            sys.modules["protocol_observer"] = protocol_module
            candidate = types.SimpleNamespace(
                symbol="000001.SZ", price=10.0, trigger_time="10:00:00",
                pct_chg=3.0, v_ratio=1.5, amount=1000.0,
                quote_source="test", quote_date="2026-07-18",
                quote_time="10:00:00", universe_source_date="2026-07-17",
                historical_pct_chg=0.0, t3_status="HEALTHY",
                t3_decay_rate=0.1, t5_verdict="PASS", t5_score=80,
            )
            future = MOD._queue_eagle_signal(candidate)
            self.assertIsNotNone(future)
            result = future.result(timeout=2)
            self.assertTrue(result["ok"])
            self.assertEqual(result["signal_bus_retries"], 1)
            self.assertEqual(result["protocol_observer_retries"], 0)
            self.assertEqual(len(signal_calls), 2)
            self.assertEqual(len(protocol_calls), 1)
        finally:
            MOD._EAGLE_PERSISTENCE_RETRY_ATTEMPTS = old_attempts
            MOD._EAGLE_PERSISTENCE_RETRY_DELAY_MS = old_delay
            if old_signal is None:
                sys.modules.pop("tactic_signal_bus", None)
            else:
                sys.modules["tactic_signal_bus"] = old_signal
            if old_protocol is None:
                sys.modules.pop("protocol_observer", None)
            else:
                sys.modules["protocol_observer"] = old_protocol

    def test_eagle_persistence_executor_can_be_drained_and_recreated(self):
        old_executor = MOD._eagle_persistence_executor
        before = MOD._submit_eagle_persistence(lambda: "before")
        self.assertEqual(before.result(timeout=2), "before")
        MOD._recycle_eagle_persistence_executor()
        self.assertIsNot(MOD._eagle_persistence_executor, old_executor)
        after = MOD._submit_eagle_persistence(lambda: "after")
        self.assertEqual(after.result(timeout=2), "after")

    def test_eagle_partial_failure_is_explicitly_counted(self):
        old_attempts = MOD._EAGLE_PERSISTENCE_RETRY_ATTEMPTS
        old_delay = MOD._EAGLE_PERSISTENCE_RETRY_DELAY_MS
        old_signal = sys.modules.get("tactic_signal_bus")
        old_protocol = sys.modules.get("protocol_observer")
        before = MOD.eagle_persistence_snapshot()["partial_failures"]
        try:
            MOD._EAGLE_PERSISTENCE_RETRY_ATTEMPTS = 2
            MOD._EAGLE_PERSISTENCE_RETRY_DELAY_MS = 1
            signal_module = types.ModuleType("tactic_signal_bus")
            signal_module.record_tactic_signal = lambda **kwargs: False
            protocol_module = types.ModuleType("protocol_observer")
            protocol_module.PROTOCOL_EAGLE = "EAGLE"
            protocol_module.TRIGGER_EAGLE_PULSE = "EAGLE_PULSE_AUDIT"
            protocol_module.record_protocol_event = lambda **kwargs: True
            sys.modules["tactic_signal_bus"] = signal_module
            sys.modules["protocol_observer"] = protocol_module
            candidate = types.SimpleNamespace(
                symbol="000002.SZ", price=9.0, trigger_time="10:01:00",
                t3_status="HEALTHY", t3_decay_rate=0.1,
                t5_verdict="PASS", t5_score=80,
            )
            result = MOD._queue_eagle_signal(candidate).result(timeout=2)
            self.assertFalse(result["ok"])
            self.assertFalse(result["signal_bus_ok"])
            self.assertTrue(result["protocol_observer_ok"])
            self.assertEqual(
                MOD.eagle_persistence_snapshot()["partial_failures"],
                before + 1,
            )
        finally:
            MOD._EAGLE_PERSISTENCE_RETRY_ATTEMPTS = old_attempts
            MOD._EAGLE_PERSISTENCE_RETRY_DELAY_MS = old_delay
            if old_signal is None:
                sys.modules.pop("tactic_signal_bus", None)
            else:
                sys.modules["tactic_signal_bus"] = old_signal
            if old_protocol is None:
                sys.modules.pop("protocol_observer", None)
            else:
                sys.modules["protocol_observer"] = old_protocol

    def test_eagle_persistence_tracks_failed_write_result(self):
        before = MOD.eagle_persistence_snapshot()["failed"]
        future = MOD._submit_eagle_persistence(
            lambda: {"ok": False, "signal_bus_ok": False}
        )
        self.assertIsNotNone(future)
        self.assertFalse(future.result(timeout=2)["ok"])
        after = MOD.eagle_persistence_snapshot()["failed"]
        self.assertEqual(after, before + 1)

    def test_snapshot_tracks_inflight_and_completion(self):
        started = threading.Event()
        release = threading.Event()

        def blocked():
            started.set()
            release.wait(timeout=2)
            return "done"

        future = MOD.fire_and_forget("TelemetryTest", blocked)
        self.assertIsNotNone(future)
        self.assertTrue(started.wait(timeout=1))
        running = MOD.backpressure_snapshot()
        self.assertGreaterEqual(running["inflight"], 1)
        self.assertGreaterEqual(running["active"], 1)
        self.assertEqual(running["capacity"], 5)

        release.set()
        self.assertEqual(future.result(timeout=2), "done")
        completed = MOD.backpressure_snapshot()
        self.assertEqual(completed["inflight"], 0)
        self.assertEqual(completed["active"], 0)
        self.assertGreaterEqual(completed["completed"], 1)


if __name__ == "__main__":
    unittest.main()
