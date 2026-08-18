#!/usr/bin/env python3
"""Rebuild Eagle Active Path manifests from immutable realtime JSONL windows."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TACTICS_DIR = PROJECT_ROOT / "03_tactics"
if str(TACTICS_DIR) not in sys.path:
    sys.path.insert(0, str(TACTICS_DIR))

from eagle_active_path import RULE_VERSION, build_manifest, render_preview
from eagle_active_store import persist_manifest


REPORT_ROOT = PROJECT_ROOT / "storage" / "reports" / "eagle_active_path"
WINDOW_DIR = REPORT_ROOT / "windows"
MANIFEST_DIR = REPORT_ROOT / "manifests"
PREVIEW_DIR = REPORT_ROOT / "previews"
HISTORY_DIR = REPORT_ROOT / "history"
BACKFILL_DIR = REPORT_ROOT / "backfills"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise


def _load_scans(path: Path) -> List[Dict[str, Any]]:
    scans: List[Dict[str, Any]] = []
    for line_number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        text = raw.strip()
        if not text:
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSONL {path}:{line_number}: {exc}") from exc
        if not isinstance(payload, dict):
            raise ValueError(f"invalid scan payload {path}:{line_number}")
        scans.append(payload)
    return scans


def _old_manifest_metadata(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"exists": False, "sha256": "", "rule_version": ""}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        "exists": True,
        "sha256": _sha256(path),
        "rule_version": str(payload.get("rule_version") or "UNKNOWN"),
        "data_quality": str(payload.get("data_quality") or ""),
        "candidate_count": int(payload.get("candidate_count") or 0),
    }


def _archive_artifact(path: Path, trade_date: str, rule_version: str, sha256: str) -> str:
    if not path.exists():
        return ""
    safe_rule = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in rule_version)
    destination = HISTORY_DIR / (
        f"{path.stem}.{safe_rule}.{sha256[:12]}{path.suffix}"
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    if not destination.exists():
        shutil.copy2(path, destination)
    return str(destination)


def rebuild_day(trade_date: str, *, apply: bool) -> Dict[str, Any]:
    window_path = WINDOW_DIR / f"eagle_windows_{trade_date}.jsonl"
    manifest_path = MANIFEST_DIR / f"eagle_candidates_{trade_date}.json"
    preview_path = PREVIEW_DIR / f"eagle_preview_{trade_date}.md"
    if not window_path.exists():
        return {"trade_date": trade_date, "status": "WINDOW_FILE_MISSING"}

    old_meta = _old_manifest_metadata(manifest_path)
    window_sha = _sha256(window_path)
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    manifest = build_manifest(_load_scans(window_path), trade_date, generated_at=generated_at)
    manifest["window_file"] = str(window_path)
    manifest["reprocessing"] = {
        "source_window_sha256": window_sha,
        "source_manifest_sha256": old_meta.get("sha256", ""),
        "source_rule_version": old_meta.get("rule_version", ""),
        "reprocessed_at": generated_at,
        "backfill_tool": "tools/backfill_eagle_active_path.py",
    }

    result: Dict[str, Any] = {
        "trade_date": trade_date,
        "status": "DRY_RUN" if not apply else "APPLIED",
        "window_file": str(window_path),
        "window_sha256": window_sha,
        "scan_count": int(manifest.get("scan_count") or 0),
        "old_rule_version": old_meta.get("rule_version", ""),
        "old_data_quality": old_meta.get("data_quality", ""),
        "old_candidate_count": old_meta.get("candidate_count", 0),
        "new_rule_version": str(manifest.get("rule_version") or ""),
        "new_data_quality": str(manifest.get("data_quality") or ""),
        "new_candidate_count": int(manifest.get("candidate_count") or 0),
        "diagnostic_candidate_count": int(manifest.get("diagnostic_candidate_count") or 0),
        "session_baseline_scan_count": int(manifest.get("session_baseline_scan_count") or 0),
        "session_warmup_scan_count": int(manifest.get("session_warmup_scan_count") or 0),
        "window_gap_count": int(manifest.get("window_gap_count") or 0),
        "insufficient_delta_coverage_scan_count": int(
            manifest.get("insufficient_delta_coverage_scan_count") or 0
        ),
        "low_coverage_scan_count": int((manifest.get("coverage") or {}).get("low_coverage_scan_count") or 0),
    }
    if not apply:
        return result

    archived_manifest = _archive_artifact(
        manifest_path,
        trade_date,
        str(old_meta.get("rule_version") or "UNKNOWN"),
        str(old_meta.get("sha256") or "absent"),
    )
    archived_preview = ""
    if preview_path.exists():
        archived_preview = _archive_artifact(
            preview_path,
            trade_date,
            str(old_meta.get("rule_version") or "UNKNOWN"),
            _sha256(preview_path),
        )
    persistence = persist_manifest(manifest)
    _atomic_write(manifest_path, json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    _atomic_write(preview_path, render_preview(manifest))
    result.update({
        "manifest_path": str(manifest_path),
        "manifest_file_sha256": _sha256(manifest_path),
        "archived_manifest": archived_manifest,
        "archived_preview": archived_preview,
        "persistence": persistence,
    })
    return result


def _date_range(start: str, end: str) -> List[str]:
    start_date = date.fromisoformat(start)
    end_date = date.fromisoformat(end)
    if end_date < start_date:
        raise ValueError("end date precedes start date")
    return [
        p.stem.replace("eagle_windows_", "")
        for p in sorted(WINDOW_DIR.glob("eagle_windows_*.jsonl"))
        if start_date <= date.fromisoformat(p.stem.replace("eagle_windows_", "")) <= end_date
    ]


def _render_report(payload: Dict[str, Any]) -> str:
    lines = [
        "# Eagle Active Path Backfill Report",
        "",
        f"- mode: {payload['mode']}",
        f"- rule_version: {payload['rule_version']}",
        f"- generated_at: {payload['generated_at']}",
        "- observation_only: true",
        "- execution_enabled: false",
        "",
        "| Trade date | Old quality | New quality | Old candidates | New candidates | DB upserted |",
        "|---|---|---|---:|---:|---:|",
    ]
    for row in payload["results"]:
        persistence = row.get("persistence") or {}
        lines.append(
            f"| {row.get('trade_date', '')} | {row.get('old_data_quality', '')} | "
            f"{row.get('new_data_quality', '')} | {row.get('old_candidate_count', 0)} | "
            f"{row.get('new_candidate_count', 0)} | {persistence.get('upserted', 0)} |"
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--apply", action="store_true", help="write rebuilt artifacts and observation rows")
    args = parser.parse_args()

    dates = _date_range(args.start_date, args.end_date)
    payload = {
        "mode": "apply" if args.apply else "dry_run",
        "rule_version": RULE_VERSION,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "observation_only": True,
        "execution_enabled": False,
        "results": [rebuild_day(trade_date, apply=args.apply) for trade_date in dates],
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    if args.apply:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_path = BACKFILL_DIR / f"eagle_active_backfill_{stamp}.json"
        md_path = BACKFILL_DIR / f"eagle_active_backfill_{stamp}.md"
        _atomic_write(json_path, json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
        _atomic_write(md_path, _render_report(payload))
        print(f"report_json={json_path}")
        print(f"report_md={md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
