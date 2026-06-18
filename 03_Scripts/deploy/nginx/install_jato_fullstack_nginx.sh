#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   sudo SERVER_NAME=example.com BACKEND_PORT=8000 FRONTEND_ROOT=/opt/JATO_Analysis_System-main/06_AppPlatform/frontend/dist \
#     bash 03_Scripts/deploy/nginx/install_jato_fullstack_nginx.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
NGINX_TEMPLATE="$ROOT_DIR/03_Scripts/deploy/nginx/jato_fullstack.conf.example"
TARGET_CONF="/etc/nginx/sites-available/jato_fullstack.conf"
ENABLED_CONF="/etc/nginx/sites-enabled/jato_fullstack.conf"

SERVER_NAME="${SERVER_NAME:-_}"
BACKEND_PORT="${BACKEND_PORT:-8000}"
ALLOW_CERTBOT_OVERWRITE="${ALLOW_CERTBOT_OVERWRITE:-false}"

resolve_frontend_root() {
  if [[ -n "${FRONTEND_ROOT:-}" ]]; then
    printf '%s\n' "$FRONTEND_ROOT"
    return 0
  fi

  local candidate=""
  for candidate in \
    /opt/JATO_Analysis_System-main/06_AppPlatform/frontend/dist \
    /opt/JATO_Analysis_System/06_AppPlatform/frontend/dist
  do
    if [[ -d "$candidate" ]]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done

  printf '%s\n' /opt/JATO_Analysis_System-main/06_AppPlatform/frontend/dist
}

FRONTEND_ROOT="$(resolve_frontend_root)"

allow_certbot_overwrite=false
case "${ALLOW_CERTBOT_OVERWRITE,,}" in
  1|true|yes|on) allow_certbot_overwrite=true ;;
esac

patch_certbot_managed_api_cache_control() {
  local target="$1"

  if [[ ! -f "$target" ]]; then
    return 0
  fi

  python3 - "$target" <<'PY'
import re
import sys
from datetime import datetime
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
pattern = re.compile(
    r'(location\s+\^~\s+/v1/\s*\{(?:(?!\n\s*location\s).)*?)'
    r'\n\s*add_header\s+Cache-Control\s+"no-store"\s+always;\n',
    re.S,
)
next_text, count = pattern.subn(
    r'\1\n        # Let FastAPI set Cache-Control so cacheable JSON endpoints keep their headers.\n',
    text,
)
if count:
    backup_dir = Path("/etc/nginx/jato-backups")
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup = backup_dir / f"{path.name}.pre-api-cache-control-{datetime.now():%Y%m%dT%H%M%S}.bak"
    backup.write_text(text, encoding="utf-8")
    path.write_text(next_text, encoding="utf-8")
    print(f"[INFO] Removed {count} proxy-level no-store line(s) from /v1/ in {path}; backup={backup}")
else:
    print(f"[INFO] No /v1/ proxy no-store line found in {path}")
PY
}

if [[ ! -f "$NGINX_TEMPLATE" ]]; then
  echo "[ERROR] Nginx template not found: $NGINX_TEMPLATE"
  exit 1
fi

echo "[INFO] Install nginx"
apt-get update -y
apt-get install -y nginx

if [[ -f "$TARGET_CONF" ]] && grep -qi 'managed by Certbot' "$TARGET_CONF" && [[ "$allow_certbot_overwrite" != "true" ]]; then
  echo "[WARN] Existing nginx config is managed by Certbot; skipping full overwrite to preserve HTTPS."
  echo "[INFO] Applying safe /v1/ Cache-Control patch in the existing config."
  patch_certbot_managed_api_cache_control "$TARGET_CONF"
  patch_certbot_managed_api_cache_control "$ENABLED_CONF"
  echo "[INFO] Set ALLOW_CERTBOT_OVERWRITE=true only if you intentionally want to replace the cert-managed config."
  nginx -t
  systemctl enable nginx
  systemctl reload nginx || systemctl restart nginx
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

patch_certbot_managed_api_cache_control "$TARGET_CONF"
patch_certbot_managed_api_cache_control "$ENABLED_CONF"

echo "[INFO] Validate and restart nginx"
nginx -t
systemctl enable nginx
systemctl restart nginx

echo "[INFO] Nginx installed. Health check:"
curl -fsS http://127.0.0.1/healthz && echo
