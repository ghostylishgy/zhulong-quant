#!/bin/sh
set -eu

cd /root/quant_project
mkdir -p logs

LOCK_FILE="/tmp/zhulong-launch.lock"
PID_FILE="/tmp/zhulong-daemon.pid"

if ! command -v flock >/dev/null 2>&1; then
  echo "[launch] flock not found. Please install util-linux first."
  exit 2
fi

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "[launch] another launcher is running, skip."
  exit 0
fi

# Explicit restart only when requested:
#   sh start_daemon.sh --restart
if [ "${1:-}" = "--restart" ]; then
  pkill -f "python3( -u)? zhulong_daemon.py" || true
  sleep 1
fi

RUNNING_PID=$(pgrep -f "[p]ython3( -u)? zhulong_daemon.py" | head -n1 || true)
if [ -n "${RUNNING_PID}" ]; then
  echo "[launch] daemon already running, pid=${RUNNING_PID}"
  exit 0
fi

# Release lock before spawn to prevent fd inheritance by daemon process.
flock -u 9
exec 9>&-

nohup python3 -u zhulong_daemon.py >> logs/daemon.log 2>&1 &
NEW_PID=$!
echo "${NEW_PID}" > "$PID_FILE"
echo "[launch] daemon started, pid=${NEW_PID}"
