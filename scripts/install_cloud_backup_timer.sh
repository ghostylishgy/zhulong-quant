#!/usr/bin/env bash
set -euo pipefail
ROOT=/root/quant_project
install -m 0644 "$ROOT/systemd/zhulong-cloud-backup.service" /etc/systemd/system/zhulong-cloud-backup.service
install -m 0644 "$ROOT/systemd/zhulong-cloud-backup.timer" /etc/systemd/system/zhulong-cloud-backup.timer
systemctl daemon-reload
systemctl enable zhulong-cloud-backup.timer
echo "Installed zhulong-cloud-backup.timer. Start it with: systemctl start zhulong-cloud-backup.timer"
