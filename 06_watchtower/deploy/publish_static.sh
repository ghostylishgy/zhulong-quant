#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/root/quant_project}"
WEB_DIR="$PROJECT_ROOT/06_watchtower/zhulong_web"
PUBLIC_ROOT="${PUBLIC_ROOT:-/var/www/zhulong-watchtower}"
NGINX_CONF_SRC="$PROJECT_ROOT/06_watchtower/deploy/nginx.zhulong-watchtower.conf"
NGINX_CONF_DST="/etc/nginx/sites-available/zhulong-watchtower.conf"

cd "$WEB_DIR"
npm run build

install -d -m 0755 "$PUBLIC_ROOT"
rsync -a --delete "$WEB_DIR/dist/" "$PUBLIC_ROOT/"
chown -R www-data:www-data "$PUBLIC_ROOT"

if [ -f "$NGINX_CONF_SRC" ]; then
  install -m 0644 "$NGINX_CONF_SRC" "$NGINX_CONF_DST"
  ln -sfn "$NGINX_CONF_DST" /etc/nginx/sites-enabled/zhulong-watchtower.conf
fi

nginx -t
systemctl reload nginx.service
printf 'Published Watchtower static site to %s\n' "$PUBLIC_ROOT"
