#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   sudo SERVER_NAME=example.com APP_PORT=8501 bash 03_Scripts/deploy/nginx/install_jato_nginx.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
NGINX_TEMPLATE="$ROOT_DIR/03_Scripts/deploy/nginx/jato_dashboard.conf.example"
TARGET_CONF="/etc/nginx/sites-available/jato_dashboard.conf"
ENABLED_CONF="/etc/nginx/sites-enabled/jato_dashboard.conf"

SERVER_NAME="${SERVER_NAME:-_}"
APP_PORT="${APP_PORT:-8501}"

if [[ ! -f "$NGINX_TEMPLATE" ]]; then
  echo "[ERROR] Nginx template not found: $NGINX_TEMPLATE"
  exit 1
fi

echo "[INFO] Install dependencies"
apt-get update -y
apt-get install -y nginx

echo "[INFO] Render nginx config (server_name=$SERVER_NAME, app_port=$APP_PORT)"
sed -e "s/server_name _;/server_name $SERVER_NAME;/g" \
    -e "s/server 127.0.0.1:8501/server 127.0.0.1:${APP_PORT}/g" \
    "$NGINX_TEMPLATE" > "$TARGET_CONF"

ln -sf "$TARGET_CONF" "$ENABLED_CONF"
rm -f /etc/nginx/sites-enabled/default

echo "[INFO] Validate and reload nginx"
nginx -t
systemctl enable nginx
systemctl restart nginx

echo "[INFO] Nginx installed. Health check:"
curl -fsS "http://127.0.0.1/healthz" && echo
