#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   sudo SERVER_NAME=example.com BACKEND_PORT=8000 FRONTEND_ROOT=/opt/JATO_Analysis_System/06_AppPlatform/frontend/dist \
#     bash 03_Scripts/deploy/nginx/install_jato_fullstack_nginx.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
NGINX_TEMPLATE="$ROOT_DIR/03_Scripts/deploy/nginx/jato_fullstack.conf.example"
TARGET_CONF="/etc/nginx/sites-available/jato_fullstack.conf"
ENABLED_CONF="/etc/nginx/sites-enabled/jato_fullstack.conf"

SERVER_NAME="${SERVER_NAME:-_}"
BACKEND_PORT="${BACKEND_PORT:-8000}"
FRONTEND_ROOT="${FRONTEND_ROOT:-/opt/JATO_Analysis_System/06_AppPlatform/frontend/dist}"
ALLOW_CERTBOT_OVERWRITE="${ALLOW_CERTBOT_OVERWRITE:-false}"

allow_certbot_overwrite=false
case "${ALLOW_CERTBOT_OVERWRITE,,}" in
  1|true|yes|on) allow_certbot_overwrite=true ;;
esac

if [[ ! -f "$NGINX_TEMPLATE" ]]; then
  echo "[ERROR] Nginx template not found: $NGINX_TEMPLATE"
  exit 1
fi

echo "[INFO] Install nginx"
apt-get update -y
apt-get install -y nginx

if [[ -f "$TARGET_CONF" ]] && grep -qi 'managed by Certbot' "$TARGET_CONF" && [[ "$allow_certbot_overwrite" != "true" ]]; then
  echo "[WARN] Existing nginx config is managed by Certbot; skipping overwrite to preserve HTTPS."
  echo "[INFO] Set ALLOW_CERTBOT_OVERWRITE=true only if you intentionally want to replace the cert-managed config."
  nginx -t
  systemctl enable nginx
  systemctl restart nginx
  curl -fsS http://127.0.0.1/healthz && echo
  exit 0
fi

echo "[INFO] Render nginx config"
sed \
  -e "s|__SERVER_NAME__|$SERVER_NAME|g" \
  -e "s|__BACKEND_PORT__|$BACKEND_PORT|g" \
  -e "s|__FRONTEND_ROOT__|$FRONTEND_ROOT|g" \
  "$NGINX_TEMPLATE" > "$TARGET_CONF"

ln -sf "$TARGET_CONF" "$ENABLED_CONF"
rm -f /etc/nginx/sites-enabled/default

echo "[INFO] Validate and restart nginx"
nginx -t
systemctl enable nginx
systemctl restart nginx

echo "[INFO] Nginx installed. Health check:"
curl -fsS http://127.0.0.1/healthz && echo