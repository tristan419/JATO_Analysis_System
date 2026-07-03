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

def find_matching_brace(source: str, open_index: int) -> int:
    depth = 0
    for index in range(open_index, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return index
    return -1

def iter_server_spans(source: str):
    for match in re.finditer(r"\bserver\s*\{", source):
        open_index = source.find("{", match.start())
        close_index = find_matching_brace(source, open_index)
        if close_index != -1:
            yield match.start(), close_index + 1

build_meta_location = r'''
    location = /build-meta.json {
        try_files $uri =404;
        add_header Cache-Control "no-cache, no-store, must-revalidate" always;
        add_header Pragma "no-cache" always;
        add_header Expires "0" always;
        add_header Access-Control-Allow-Origin "*" always;
        add_header Timing-Allow-Origin "*" always;
    }

'''

route_probe_location = r'''
    location = /route-probe.txt {
        try_files $uri =404;
        add_header Cache-Control "no-cache, no-store, must-revalidate" always;
        add_header Pragma "no-cache" always;
        add_header Expires "0" always;
        add_header Access-Control-Allow-Origin "*" always;
        add_header Timing-Allow-Origin "*" always;
    }

'''

metadata_headers = [
    'add_header Cache-Control "no-cache, no-store, must-revalidate" always;',
    'add_header Pragma "no-cache" always;',
    'add_header Expires "0" always;',
    'add_header Access-Control-Allow-Origin "*" always;',
    'add_header Timing-Allow-Origin "*" always;',
]

def serves_frontend(block: str) -> bool:
    if re.search(r"\n\s*return\s+30[18]\s+", block):
        return False
    if not re.search(r"\n\s*root\s+", block):
        return False
    return (
        "jato_fullstack_api" in block
        or "try_files $uri $uri/ /index.html" in block
        or re.search(r"\n\s*location\s+\^~\s+/assets/\s*\{", block)
    )

def insert_missing_locations(block: str) -> str:
    pieces = []
    if "location = /build-meta.json" not in block:
        pieces.append(build_meta_location)
    if "location = /route-probe.txt" not in block:
        pieces.append(route_probe_location)
    if not pieces:
        return block

    insert_block = "".join(pieces)
    marker = re.search(r"\n\s*location\s+\^~\s+/assets/\s*\{", block)
    if not marker:
        marker = re.search(r"\n\s*location\s+=\s+/index\.html\s*\{", block)
    if not marker:
        marker = re.search(r"\n\s*location\s+/\s*\{", block)
    if marker:
        return block[:marker.start() + 1] + insert_block + block[marker.start() + 1:]

    close_index = block.rfind("}")
    if close_index == -1:
        return block
    return block[:close_index] + insert_block + block[close_index:]

def ensure_location_headers(block: str, location_pattern: str) -> str:
    pattern = re.compile(rf"({location_pattern}\s*\{{(?:(?!\n\s*location\s).)*?)(\s*\}})", re.S)

    def repl(match: re.Match[str]) -> str:
        body = match.group(1)
        for header in metadata_headers:
            if header not in body:
                body += f"\n        {header}"
        return body + match.group(2)

    return pattern.sub(repl, block)

parts = []
last = 0
for start, end in iter_server_spans(text):
    block = text[start:end]
    next_block = block
    if serves_frontend(block):
        next_block = insert_missing_locations(next_block)
        next_block = ensure_location_headers(next_block, r"location\s+=\s+/build-meta\.json")
        next_block = ensure_location_headers(next_block, r"location\s+=\s+/route-probe\.txt")
    if next_block != block:
        changed = True
    parts.append(text[last:start])
    parts.append(next_block)
    last = end
parts.append(text[last:])
if parts:
    text = "".join(parts)

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
