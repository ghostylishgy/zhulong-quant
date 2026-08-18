#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MODULE_DIR="$ROOT_DIR/10_us_radar"
LOG_DIR="$MODULE_DIR/logs"
PYTHONPATH_DIR="$MODULE_DIR/src"
ENV_FILE="$MODULE_DIR/.env.local"
LOCK_FILE="/tmp/us_radar_run_once.lock"

mkdir -p "$LOG_DIR" "$MODULE_DIR/data" "$MODULE_DIR/reports"
export PYTHONPATH="$PYTHONPATH_DIR${PYTHONPATH:+:$PYTHONPATH}"
PYTHON_BIN="$MODULE_DIR/.venv/bin/python"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="python3"
fi

if command -v flock >/dev/null 2>&1; then
  exec 9>"$LOCK_FILE"
  if ! flock -n 9; then
    echo "[$(date -Is)] us_radar run_once already running, skip"
    exit 0
  fi
fi

apply_proxy_env() {
  if [[ -n "${US_RADAR_HTTP_PROXY:-}" ]]; then
    export HTTP_PROXY="$US_RADAR_HTTP_PROXY"
    export http_proxy="$US_RADAR_HTTP_PROXY"
  fi
  if [[ -n "${US_RADAR_HTTPS_PROXY:-}" ]]; then
    export HTTPS_PROXY="$US_RADAR_HTTPS_PROXY"
    export https_proxy="$US_RADAR_HTTPS_PROXY"
  fi
  if [[ -n "${US_RADAR_ALL_PROXY:-}" ]]; then
    export ALL_PROXY="$US_RADAR_ALL_PROXY"
    export all_proxy="$US_RADAR_ALL_PROXY"
  fi
  if [[ -n "${US_RADAR_NO_PROXY:-}" ]]; then
    export NO_PROXY="$US_RADAR_NO_PROXY"
    export no_proxy="$US_RADAR_NO_PROXY"
  fi
}

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

apply_proxy_env

{
  echo "[$(date -Is)] us_radar run_once start"
  "$PYTHON_BIN" -m us_radar.cli init-db
  "$PYTHON_BIN" -m us_radar.cli init-prompts
  if [[ "${US_RADAR_SKIP_NETWORK:-0}" == "1" ]]; then
    echo "[$(date -Is)] US_RADAR_SKIP_NETWORK=1, skipping SEC fetch"
  else
    sec_fetch_failures=0
    "$PYTHON_BIN" -m us_radar.cli fetch-sec --form-type "8-K" --limit "${US_RADAR_SEC_LIMIT:-25}" || { sec_fetch_failures=$((sec_fetch_failures + 1)); echo "[$(date -Is)] fetch-sec 8-K failed, continuing in degraded mode"; }
    "$PYTHON_BIN" -m us_radar.cli fetch-sec --form-type "4" --limit "${US_RADAR_SEC_LIMIT:-25}" || { sec_fetch_failures=$((sec_fetch_failures + 1)); echo "[$(date -Is)] fetch-sec Form 4 failed, continuing in degraded mode"; }
    "$PYTHON_BIN" -m us_radar.cli enrich-form4 --hours "${US_RADAR_REPORT_HOURS:-72}" --limit "${US_RADAR_FORM4_LIMIT:-10}" || echo "[$(date -Is)] enrich-form4 failed, continuing"
    if (( sec_fetch_failures >= 2 )); then
      echo "[$(date -Is)] all SEC fetch streams failed"
      exit 1
    fi
  fi
  "$PYTHON_BIN" -m us_radar.cli classify-events --hours "${US_RADAR_REPORT_HOURS:-72}"
  "$PYTHON_BIN" -m us_radar.cli extract-evidence --hours "${US_RADAR_REPORT_HOURS:-72}"
  "$PYTHON_BIN" -m us_radar.cli expand-targets --hours "${US_RADAR_REPORT_HOURS:-72}"
  "$PYTHON_BIN" -m us_radar.cli prepare-validation
  "$PYTHON_BIN" -m us_radar.cli validate-returns --limit "${US_RADAR_VALIDATE_RETURNS_LIMIT:-500}" --max-us-symbols "${US_RADAR_MAX_US_API_SYMBOLS:-4}"
  "$PYTHON_BIN" -m us_radar.cli report --hours "${US_RADAR_REPORT_HOURS:-72}"
  echo "[$(date -Is)] us_radar run_once done"
} 2>&1 | tee -a "$LOG_DIR/run_once.log"
