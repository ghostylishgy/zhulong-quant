#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
export PYTHONPATH="${ROOT_DIR}:${PYTHONPATH:-}"

mkdir -p "${ROOT_DIR}/logs"
mkdir -p "${ROOT_DIR}/storage/logs"

echo "[boot_fix] ROOT_DIR=${ROOT_DIR}"
echo "[boot_fix] PYTHONPATH=${PYTHONPATH}"

# Intel/OpenVINO runtime bootstrap (best-effort)
if [ -f "/opt/intel/openvino/setupvars.sh" ]; then
  # shellcheck disable=SC1091
  source /opt/intel/openvino/setupvars.sh
  echo "[boot_fix] sourced /opt/intel/openvino/setupvars.sh"
elif [ -f "/opt/intel/openvino_2025/setupvars.sh" ]; then
  # shellcheck disable=SC1091
  source /opt/intel/openvino_2025/setupvars.sh
  echo "[boot_fix] sourced /opt/intel/openvino_2025/setupvars.sh"
else
  echo "[boot_fix] INFO: OpenVINO setupvars.sh not found, continue with system runtime"
fi

export ONEAPI_DEVICE_SELECTOR="${ONEAPI_DEVICE_SELECTOR:-level_zero:gpu}"
export OCL_ICD_VENDORS="${OCL_ICD_VENDORS:-/etc/OpenCL/vendors}"

if [ -d /dev/dri ]; then
  echo "[boot_fix] /dev/dri present: $(ls -1 /dev/dri | tr '\n' ' ')"
else
  echo "[boot_fix] WARN: /dev/dri missing, iGPU device is not visible to current node"
fi

if command -v clinfo >/dev/null 2>&1; then
  CL_LINE=$(clinfo 2>/dev/null | grep -E "Intel\(R\).*Graphics|Intel\(R\) UHD Graphics|0x46d1" | head -n 1 || true)
  if [ -n "$CL_LINE" ]; then
    echo "[boot_fix] OpenCL Intel device detected: $CL_LINE"
  else
    echo "[boot_fix] WARN: Intel OpenCL device not detected in clinfo"
  fi
fi

if [ ! -f "${ROOT_DIR}/.env" ]; then
  echo "[boot_fix] WARN: .env missing at ${ROOT_DIR}/.env"
fi

if [ ! -f "${ROOT_DIR}/storage/database/zhulong.duckdb" ]; then
  echo "[boot_fix] WARN: DuckDB missing at ${ROOT_DIR}/storage/database/zhulong.duckdb"
fi

echo "[boot_fix] done"
