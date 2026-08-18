#!/bin/bash
set -eu
cd /root/quant_project

LOG_ONESHOT="logs/oneshot_430.log"
LOG_DAEMON="logs/daemon.log"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] ONE-SHOT 4.30 审计启动" >> "$LOG_ONESHOT"

# 运行 ONE-SHOT 审计 (TRADE_DATE 触发 harvest+audit 后自动退出)
TRADE_DATE=2026-04-30 python3 -u zhulong_daemon.py >> "$LOG_ONESHOT" 2>&1
EXIT_CODE=$?

echo "[$(date '+%Y-%m-%d %H:%M:%S')] ONE-SHOT 完成, exit=$EXIT_CODE" >> "$LOG_ONESHOT"

# ONE-SHOT 结束后重拉常驻 daemon
sleep 2
sh start_daemon.sh >> "$LOG_DAEMON" 2>&1
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 常驻 daemon 已重新拉起" >> "$LOG_ONESHOT"
