#!/usr/bin/env python3
"""Build or verify a secret-free BL-021 audit contract manifest."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "02_brain/lib/audit_contract.py"


def _load_contract_module():
    spec = importlib.util.spec_from_file_location("zhulong_audit_contract", MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"audit contract module unavailable: {MODULE_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


CONTRACT = _load_contract_module()


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temp_path.write_text(text, encoding="utf-8")
    os.replace(temp_path, path)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the BL-021 audit contract fingerprint")
    parser.add_argument("--project-root", default=str(ROOT))
    parser.add_argument("--output", default="")
    parser.add_argument("--verify", default="")
    parser.add_argument("--bind-trade-date", default="")
    parser.add_argument("--bind-run-id", default="")
    parser.add_argument("--evidence-as-of", default="")
    args = parser.parse_args()

    if args.verify:
        path = Path(args.verify)
        payload = json.loads(path.read_text(encoding="utf-8"))
        if "artifact_schema_version" in payload:
            valid = CONTRACT.verify_bound_artifact(payload)
        else:
            valid = CONTRACT.verify_manifest(payload)
        print(json.dumps({"manifest": str(path), "valid": valid}, indent=2))
        return 0 if valid else 2

    bind_values = (args.bind_trade_date, args.bind_run_id, args.evidence_as_of)
    if any(bind_values) and not all(bind_values):
        parser.error(
            "--bind-trade-date, --bind-run-id and --evidence-as-of must be supplied together"
        )
    if all(bind_values):
        payload = CONTRACT.build_bound_artifact(
            Path(args.project_root),
            args.bind_trade_date,
            args.bind_run_id,
            args.evidence_as_of,
        )
    else:
        payload = CONTRACT.build_manifest(Path(args.project_root))
    text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        _atomic_write(Path(args.output), text)
    else:
        print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
