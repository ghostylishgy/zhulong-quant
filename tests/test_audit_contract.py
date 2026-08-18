import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PATH = ROOT / "02_brain/lib/audit_contract.py"
SPEC = importlib.util.spec_from_file_location("zhulong_test_audit_contract", PATH)
MOD = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MOD
SPEC.loader.exec_module(MOD)


class AuditContractTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        for index, relative in enumerate(MOD.CONTRACT_FILES):
            path = self.root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f"fixture-{index}\n", encoding="utf-8")

    def tearDown(self):
        self.tempdir.cleanup()

    def test_same_files_and_switches_are_deterministic(self):
        environment = {"AUDIT_PROFILE": "balanced", "L3_MODEL_ENABLED": "0"}
        first = MOD.build_manifest(self.root, environment)
        second = MOD.build_manifest(self.root, environment)

        self.assertEqual(first, second)
        self.assertTrue(MOD.verify_manifest(first))
        self.assertFalse(first["run_identity_bound"])
        self.assertEqual(
            first["binding_status"],
            "MANIFEST_ONLY_NOT_YET_BOUND_TO_AUDIT_RUN",
        )

    def test_effective_top_level_settings_and_rag_contract_are_bound(self):
        self.assertIn("config/settings.py", MOD.CONTRACT_FILES)
        self.assertIn("config/rag_contract.py", MOD.CONTRACT_FILES)
        self.assertIn("02_brain/lib/local_model_microtasks.py", MOD.CONTRACT_FILES)
        manifest = MOD.build_manifest(self.root, {})
        self.assertIn("config/settings.py", manifest["files"])
        self.assertIn("config/rag_contract.py", manifest["files"])
        self.assertIn("02_brain/lib/local_model_microtasks.py", manifest["files"])

    def test_local_model_observer_authority_switches_are_bound(self):
        for key in (
            "L2_MODEL_ENABLED",
            "L3_MODEL_ENABLED",
            "L3_MODEL_AUTHORITY_ENABLED",
            "L2_OBSERVER_NUM_PREDICT",
            "L3_OBSERVER_NUM_PREDICT",
        ):
            self.assertIn(key, MOD.RUNTIME_SWITCHES)
        observer = MOD.build_manifest(
            self.root,
            {"L3_MODEL_ENABLED": "1", "L3_MODEL_AUTHORITY_ENABLED": "0"},
        )
        authority = MOD.build_manifest(
            self.root,
            {"L3_MODEL_ENABLED": "1", "L3_MODEL_AUTHORITY_ENABLED": "1"},
        )
        self.assertNotEqual(observer["contract_sha256"], authority["contract_sha256"])

    def test_file_change_changes_contract_identity(self):
        first = MOD.build_manifest(self.root, {})
        target = self.root / MOD.CONTRACT_FILES[0]
        target.write_text("changed\n", encoding="utf-8")
        second = MOD.build_manifest(self.root, {})

        self.assertNotEqual(first["contract_sha256"], second["contract_sha256"])

    def test_runtime_switch_change_changes_contract_identity(self):
        first = MOD.build_manifest(self.root, {"L4_NEWS_POLICY": "OBSERVE_ONLY"})
        second = MOD.build_manifest(self.root, {"L4_NEWS_POLICY": "COURT_CONTEXT"})

        self.assertNotEqual(first["contract_sha256"], second["contract_sha256"])

    def test_secrets_are_never_serialized(self):
        manifest = MOD.build_manifest(
            self.root,
            {
                "AUDIT_PROFILE": "balanced",
                "DEEPSEEK_API_KEY": "super-secret-key",
                "PUSHPLUS_TOKEN": "super-secret-token",
            },
        )
        rendered = json.dumps(manifest, sort_keys=True)

        self.assertNotIn("super-secret", rendered)
        self.assertNotIn("DEEPSEEK_API_KEY", rendered)
        self.assertNotIn("PUSHPLUS_TOKEN", rendered)

    def test_missing_contract_file_fails_closed(self):
        (self.root / MOD.CONTRACT_FILES[-1]).unlink()
        with self.assertRaisesRegex(MOD.AuditContractError, "missing"):
            MOD.build_manifest(self.root, {})

    def test_tampered_manifest_fails_verification(self):
        manifest = MOD.build_manifest(self.root, {})
        manifest["runtime_switches"]["L3_MODEL_ENABLED"] = "1"
        self.assertFalse(MOD.verify_manifest(manifest))

    def test_binding_claim_cannot_be_tampered(self):
        manifest = MOD.build_manifest(self.root, {})
        manifest["run_identity_bound"] = True
        manifest["binding_status"] = "BOUND"
        self.assertFalse(MOD.verify_manifest(manifest))

    def test_recomputed_incomplete_allowlist_is_rejected(self):
        manifest = MOD.build_manifest(self.root, {})
        manifest["files"].pop(MOD.CONTRACT_FILES[-1])
        identity = {
            key: manifest[key]
            for key in (
                "schema_version",
                "contract_scope",
                "files",
                "runtime_switches",
                "secret_free",
                "run_identity_bound",
                "binding_status",
            )
        }
        manifest["contract_sha256"] = MOD.canonical_sha256(identity)
        self.assertFalse(MOD.verify_manifest(manifest))

    def test_bound_artifact_is_deterministic_and_verifiable(self):
        first = MOD.build_bound_artifact(
            self.root,
            "2026-07-30",
            "a1b2c3d4",
            "2026-07-30T20:59:59+08:00",
            {"L3_MODEL_ENABLED": "0"},
        )
        second = MOD.build_bound_artifact(
            self.root,
            "2026-07-30",
            "a1b2c3d4",
            "2026-07-30T20:59:59+08:00",
            {"L3_MODEL_ENABLED": "0"},
        )
        self.assertEqual(first, second)
        self.assertTrue(MOD.verify_bound_artifact(first))
        self.assertEqual(first["binding"]["binding_status"], "BOUND_AT_AUDIT_START")

    def test_binding_identity_changes_with_run_scope(self):
        manifest = MOD.build_manifest(self.root, {})
        first = MOD.bind_manifest(
            manifest, "2026-07-30", "a1b2c3d4", "2026-07-30T21:00:00+08:00"
        )
        second = MOD.bind_manifest(
            manifest, "2026-07-30", "e5f6a7b8", "2026-07-30T21:00:00+08:00"
        )
        self.assertNotEqual(first["binding_sha256"], second["binding_sha256"])

    def test_binding_rejects_invalid_or_naive_scope(self):
        manifest = MOD.build_manifest(self.root, {})
        with self.assertRaises(MOD.AuditContractError):
            MOD.bind_manifest(manifest, "20260730", "a1b2c3d4", "2026-07-30T21:00:00+08:00")
        with self.assertRaises(MOD.AuditContractError):
            MOD.bind_manifest(manifest, "2026-07-30", "not-a-run", "2026-07-30T21:00:00+08:00")
        with self.assertRaises(MOD.AuditContractError):
            MOD.bind_manifest(manifest, "2026-07-30", "a1b2c3d4", "2026-07-30T21:00:00")

    def test_binding_tamper_is_rejected(self):
        artifact = MOD.build_bound_artifact(
            self.root,
            "2026-07-30",
            "a1b2c3d4",
            "2026-07-30T21:00:00+08:00",
            {},
        )
        artifact["binding"]["evidence_as_of"] = "2026-07-30T20:00:00+08:00"
        self.assertFalse(MOD.verify_bound_artifact(artifact))


if __name__ == "__main__":
    unittest.main()
