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

patch_static_route_metadata_headers() {
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
changed = False

insert_block = r'''
    location = /build-meta.json {
        try_files $uri =404;
        add_header Cache-Control "no-cache, no-store, must-revalidate" always;
        add_header Pragma "no-cache" always;
        add_header Expires "0" always;
        add_header Access-Control-Allow-Origin "*" always;
        add_header Timing-Allow-Origin "*" always;
    }

    location = /route-probe.txt {
        try_files $uri =404;
        add_header Cache-Control "no-cache, no-store, must-revalidate" always;
        add_header Pragma "no-cache" always;
        add_header Expires "0" always;
        add_header Access-Control-Allow-Origin "*" always;
        add_header Timing-Allow-Origin "*" always;
    }

'''

if "location = /build-meta.json" not in text or "location = /route-probe.txt" not in text:
    marker = re.search(r"\n\s*location\s+\^~\s+/assets/\s*\{", text)
    if not marker:
        marker = re.search(r"\n\s*location\s+=\s+/index\.html\s*\{", text)
    if marker:
        text = text[:marker.start() + 1] + insert_block + text[marker.start() + 1:]
        changed = True

def ensure_header(location_pattern: str, header_line: str) -> None:
    global text, changed
    pattern = re.compile(rf"({location_pattern}\s*\{{(?:(?!\n\s*location\s).)*?)(\n\s*\}})", re.S)
    match = pattern.search(text)
    if not match or header_line in match.group(1):
        return
    replacement = match.group(1) + f"\n        {header_line}" + match.group(2)
    text = text[:match.start()] + replacement + text[match.end():]
    changed = True

for location_pattern in [r"location\s+=\s+/build-meta\.json", r"location\s+=\s+/route-probe\.txt"]:
    ensure_header(location_pattern, 'add_header Access-Control-Allow-Origin "*" always;')
    ensure_header(location_pattern, 'add_header Timing-Allow-Origin "*" always;')

if changed:
    backup_dir = Path("/etc/nginx/jato-backups")
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup = backup_dir / f"{path.name}.pre-route-metadata-{datetime.now():%Y%m%dT%H%M%S}.bak"
    backup.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    path.write_text(text, encoding="utf-8")
    print(f"[INFO] Patched route metadata headers in {path}; backup={backup}")
else:
    print(f"[INFO] Route metadata headers already present in {path}")
PY
}

patch_all_jato_nginx_configs() {
  local seen=""
  local target=""
  local candidates=("$TARGET_CONF" "$ENABLED_CONF")

  while IFS= read -r target; do
    candidates+=("$target")
  done < <(
    grep -rlE 'jato_fullstack_api|JATO_Analysis_System-main/06_AppPlatform/frontend/dist|JATO_Analysis_System/06_AppPlatform/frontend/dist' \
      /etc/nginx/sites-available /etc/nginx/sites-enabled /etc/nginx/conf.d 2>/dev/null || true
  )

  for target in "${candidates[@]}"; do
    if [[ -z "$target" || ! -f "$target" ]]; then
      continue
    fi
    case "|$seen|" in
      *"|$target|"*) continue ;;
    esac
    seen="${seen}|$target"
    patch_certbot_managed_api_cache_control "$target"
    patch_static_route_metadata_headers "$target"
  done
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
  echo "[INFO] Applying safe nginx patches in existing JATO configs."
  patch_all_jato_nginx_configs
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

patch_all_jato_nginx_configs

echo "[INFO] Validate and restart nginx"
nginx -t
systemctl enable nginx
systemctl restart nginx

echo "[INFO] Nginx installed. Health check:"
curl -fsS http://127.0.0.1/healthz && echo
