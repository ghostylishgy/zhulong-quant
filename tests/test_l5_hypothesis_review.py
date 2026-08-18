#!/usr/bin/env python3
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("zhulong_test_l5_review", ROOT / "tools/review_l5_hypotheses.py")
MOD = importlib.util.module_from_spec(spec); sys.modules[spec.name] = MOD; spec.loader.exec_module(MOD)


class L5HypothesisReviewTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.root = Path(self.tmp.name); self.packet = self.root / "packet.json"
        self.hypothesis = {"hypothesis_id": "H1", "observation": "x", "affected_archetypes": ["TREND_INITIATION"],
                           "supporting_counts": {}, "proposed_test": "offline", "failure_condition": "no edge",
                           "overfitting_risk": "high", "status": "HUMAN_REVIEW_REQUIRED"}
        self.packet.write_text(json.dumps({"schema_version": MOD.PACKET_VERSION, "no_trade_signal": True,
            "sample_gate": {"offline_replay_ready_archetypes": ["TREND_INITIATION"]},
            "llm": {"result": {"hypotheses": [self.hypothesis]}}}), encoding="utf-8")

    def tearDown(self): self.tmp.cleanup()

    def test_hash_bound_approval_does_not_run_replay(self):
        manifest = MOD.initialize(self.packet); manifest.update({"reviewer": "codex-test", "reviewed_at": "2026-07-12T10:00:00+08:00"})
        manifest["rows"][0].update({"decision": "APPROVE_FOR_OFFLINE_REPLAY", "review_note": "test separately"})
        path = self.root / "manifest.json"; path.write_text(json.dumps(manifest), encoding="utf-8")
        result = MOD.apply_review(self.packet, path)
        self.assertEqual(result["approved_count"], 1); self.assertFalse(result["run_replay"]); self.assertFalse(result["auto_promote"])

    def test_modified_hypothesis_fails_closed(self):
        manifest = MOD.initialize(self.packet); manifest.update({"reviewer": "x", "reviewed_at": "2026-07-12T10:00:00+08:00"})
        manifest["rows"][0].update({"hypothesis_sha256": "bad", "decision": "REJECT", "review_note": "bad"})
        path = self.root / "manifest.json"; path.write_text(json.dumps(manifest), encoding="utf-8")
        with self.assertRaises(ValueError): MOD.apply_review(self.packet, path)


if __name__ == "__main__": unittest.main()
