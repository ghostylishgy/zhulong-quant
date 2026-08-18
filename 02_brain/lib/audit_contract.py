"""Deterministic, secret-free audit contract fingerprint for BL-021."""

from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping


SCHEMA_VERSION = "zhulong_audit_contract_v0.1"
CONTRACT_SCOPE = "L1_TO_L4_DECISION_BEHAVIOR"
SECRET_FREE = True
RUN_IDENTITY_BOUND = False
BINDING_STATUS = "MANIFEST_ONLY_NOT_YET_BOUND_TO_AUDIT_RUN"
BINDING_SCHEMA_VERSION = "zhulong_audit_run_contract_binding_v0.1"
BOUND_STATUS = "BOUND_AT_AUDIT_START"
ARTIFACT_SCHEMA_VERSION = "zhulong_audit_contract_artifact_v0.1"

# These files can alter candidate admission, evidence interpretation, prompts,
# model parsing, or the final L4 verdict. Storage and notification code is out
# of scope because it cannot legitimately change the verdict contract.
CONTRACT_FILES = (
    "02_brain/lib/audit_contract.py",
    "02_brain/decision_engine.py",
    "02_brain/lib/local_model_microtasks.py",
    "02_brain/models/fin_auditor_observer.Modelfile",
    "02_brain/zpe2_optimizer.py",
    "02_brain/lib/compute_gateway.py",
    "02_brain/lib/news_verifier.py",
    "02_brain/lib/rag_pipeline.py",
    "02_brain/lib/trend_hunter.py",
    "02_brain/lib/zeta_auditor.py",
    "02_brain/lib/zeta_collector.py",
    "config/rag_contract.py",
    "config/settings.py",
    "04_governance/config/settings.py",
    "04_governance/lib/core/llm_parser.py",
    "04_governance/lib/tide_sensor.py",
)

# Fixed allowlist only. Credentials, endpoints, notification settings, and
# run-specific identity/as-of values must never enter the manifest.
RUNTIME_SWITCHES = (
    "AUDIT_PROFILE",
    "ZHULONG_AUDIT_PROFILE",
    "AUDIT_STRATEGY",
    "AUDIT_L1_CANDIDATE_LIMIT",
    "AUDIT_L2_TOP_N_INTRADAY",
    "AUDIT_L2_TOP_N_CLOSE",
    "AUDIT_L2_PASS_THRESHOLD",
    "AUDIT_L2_WATCH_THRESHOLD",
    "AUDIT_L4_PASS_THRESHOLD",
    "AUDIT_L4_WATCH_THRESHOLD",
    "AUDIT_ZETA_DIV_SOFT",
    "AUDIT_ZETA_DIV_HARD",
    "AUDIT_CYCLE_BUDGET_SEC",
    "AUDIT_NEXT_STAGE_RESERVE_SEC",
    "L2_MODEL_ENABLED",
    "L3_MODEL_ENABLED",
    "L3_MODEL_AUTHORITY_ENABLED",
    "L3_MODEL_OVERRIDE",
    "L2_LFM_PARSE_FAIL_STREAK",
    "L2_CORE_NUM_PREDICT",
    "L2_OBSERVER_NUM_PREDICT",
    "L2_SECONDARY_NUM_PREDICT",
    "L2_THINKING_USE_SCHEMA",
    "L3_OBSERVER_NUM_PREDICT",
    "L4_NOTARY_STRICT",
    "L4_NOTARY_VETO_HARD",
    "L4_NEWS_ENABLED",
    "L4_NEWS_POLICY",
    "L4_NEWS_TIMEOUT_SECONDS",
    "ZHULONG_LLM_TIMEOUT_MULTIPLIER",
)

_SENSITIVE_KEY_PARTS = ("KEY", "TOKEN", "SECRET", "PASSWORD", "COOKIE")


class AuditContractError(RuntimeError):
    """Raised when an exact audit contract cannot be constructed or verified."""


def canonical_sha256(value: Any) -> str:
    raw = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_runtime_switches(environment: Mapping[str, str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for key in RUNTIME_SWITCHES:
        if any(part in key for part in _SENSITIVE_KEY_PARTS):
            raise AuditContractError(f"sensitive key entered runtime allowlist: {key}")
        raw = environment.get(key)
        result[key] = "<UNSET>" if raw is None or str(raw).strip() == "" else str(raw).strip()
    return result


def build_manifest(
    project_root: Path,
    environment: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    file_records: dict[str, dict[str, Any]] = {}
    missing: list[str] = []
    for relative in CONTRACT_FILES:
        path = (root / relative).resolve()
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise AuditContractError(f"contract path escapes project root: {relative}") from exc
        if not path.is_file():
            missing.append(relative)
            continue
        file_records[relative] = {
            "sha256": file_sha256(path),
            "size_bytes": path.stat().st_size,
        }
    if missing:
        raise AuditContractError(f"contract file(s) missing: {', '.join(missing)}")

    runtime = _safe_runtime_switches(environment if environment is not None else os.environ)
    identity = {
        "schema_version": SCHEMA_VERSION,
        "contract_scope": CONTRACT_SCOPE,
        "files": file_records,
        "runtime_switches": runtime,
        "secret_free": SECRET_FREE,
        "run_identity_bound": RUN_IDENTITY_BOUND,
        "binding_status": BINDING_STATUS,
    }
    return {
        **identity,
        "contract_sha256": canonical_sha256(identity),
    }


def verify_manifest(manifest: Mapping[str, Any]) -> bool:
    if str(manifest.get("schema_version") or "") != SCHEMA_VERSION:
        return False
    if str(manifest.get("contract_scope") or "") != CONTRACT_SCOPE:
        return False
    if manifest.get("secret_free") is not SECRET_FREE:
        return False
    if manifest.get("run_identity_bound") is not RUN_IDENTITY_BOUND:
        return False
    if str(manifest.get("binding_status") or "") != BINDING_STATUS:
        return False
    files = manifest.get("files")
    runtime_switches = manifest.get("runtime_switches")
    if not isinstance(files, Mapping) or set(files) != set(CONTRACT_FILES):
        return False
    if not isinstance(runtime_switches, Mapping) or set(runtime_switches) != set(RUNTIME_SWITCHES):
        return False
    identity = {
        "schema_version": manifest.get("schema_version"),
        "contract_scope": manifest.get("contract_scope"),
        "files": files,
        "runtime_switches": runtime_switches,
        "secret_free": manifest.get("secret_free"),
        "run_identity_bound": manifest.get("run_identity_bound"),
        "binding_status": manifest.get("binding_status"),
    }
    return bool(manifest.get("contract_sha256")) and (
        str(manifest.get("contract_sha256")) == canonical_sha256(identity)
    )


def _normalize_trade_date(value: str) -> str:
    raw = str(value or "").strip()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        raise AuditContractError(f"invalid binding trade_date: {raw!r}")
    try:
        return date.fromisoformat(raw).isoformat()
    except ValueError as exc:
        raise AuditContractError(f"invalid binding trade_date: {raw!r}") from exc


def _normalize_run_id(value: str) -> str:
    raw = str(value or "").strip().lower()
    if not re.fullmatch(r"[0-9a-f]{8}", raw):
        raise AuditContractError(f"invalid binding run_id: {raw!r}")
    return raw


def _normalize_evidence_as_of(value: str) -> str:
    raw = str(value or "").strip()
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise AuditContractError(f"invalid binding evidence_as_of: {raw!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise AuditContractError("binding evidence_as_of must include a timezone")
    return parsed.isoformat()


def bind_manifest(
    manifest: Mapping[str, Any],
    trade_date: str,
    run_id: str,
    evidence_as_of: str,
) -> dict[str, Any]:
    if not verify_manifest(manifest):
        raise AuditContractError("cannot bind an invalid audit contract manifest")
    identity = {
        "schema_version": BINDING_SCHEMA_VERSION,
        "binding_status": BOUND_STATUS,
        "contract_schema_version": SCHEMA_VERSION,
        "contract_sha256": str(manifest["contract_sha256"]),
        "trade_date": _normalize_trade_date(trade_date),
        "run_id": _normalize_run_id(run_id),
        "evidence_as_of": _normalize_evidence_as_of(evidence_as_of),
    }
    return {**identity, "binding_sha256": canonical_sha256(identity)}


def verify_binding(
    binding: Mapping[str, Any],
    manifest: Mapping[str, Any] | None = None,
) -> bool:
    if str(binding.get("schema_version") or "") != BINDING_SCHEMA_VERSION:
        return False
    if str(binding.get("binding_status") or "") != BOUND_STATUS:
        return False
    if str(binding.get("contract_schema_version") or "") != SCHEMA_VERSION:
        return False
    try:
        trade_date = _normalize_trade_date(str(binding.get("trade_date") or ""))
        run_id = _normalize_run_id(str(binding.get("run_id") or ""))
        evidence_as_of = _normalize_evidence_as_of(str(binding.get("evidence_as_of") or ""))
    except AuditContractError:
        return False
    identity = {
        "schema_version": binding.get("schema_version"),
        "binding_status": binding.get("binding_status"),
        "contract_schema_version": binding.get("contract_schema_version"),
        "contract_sha256": str(binding.get("contract_sha256") or ""),
        "trade_date": trade_date,
        "run_id": run_id,
        "evidence_as_of": evidence_as_of,
    }
    if str(binding.get("binding_sha256") or "") != canonical_sha256(identity):
        return False
    if manifest is not None:
        if not verify_manifest(manifest):
            return False
        if str(manifest.get("contract_sha256") or "") != identity["contract_sha256"]:
            return False
    return bool(re.fullmatch(r"[0-9a-f]{64}", identity["contract_sha256"]))


def build_bound_artifact(
    project_root: Path,
    trade_date: str,
    run_id: str,
    evidence_as_of: str,
    environment: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    manifest = build_manifest(project_root, environment)
    binding = bind_manifest(manifest, trade_date, run_id, evidence_as_of)
    return {
        "artifact_schema_version": ARTIFACT_SCHEMA_VERSION,
        "manifest": manifest,
        "binding": binding,
    }


def verify_bound_artifact(artifact: Mapping[str, Any]) -> bool:
    if str(artifact.get("artifact_schema_version") or "") != ARTIFACT_SCHEMA_VERSION:
        return False
    manifest = artifact.get("manifest")
    binding = artifact.get("binding")
    if not isinstance(manifest, Mapping) or not isinstance(binding, Mapping):
        return False
    return verify_binding(binding, manifest)
