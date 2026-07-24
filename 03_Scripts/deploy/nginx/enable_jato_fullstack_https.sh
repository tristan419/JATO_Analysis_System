#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
NGINX_INSTALL_SCRIPT="${NGINX_INSTALL_SCRIPT:-$ROOT_DIR/03_Scripts/deploy/nginx/install_jato_fullstack_nginx.sh}"
PRODUCTION_MUTATION_LOCK_LIB="${PRODUCTION_MUTATION_LOCK_LIB:-$ROOT_DIR/03_Scripts/deploy/lib/production_mutation_lock.sh}"

SERVER_NAME="${SERVER_NAME:-}"
BACKEND_PORT="${BACKEND_PORT:-}"
FRONTEND_ROOT="${FRONTEND_ROOT:-}"
CERTBOT_EMAIL="${CERTBOT_EMAIL:-}"
CERTBOT_STAGING="${CERTBOT_STAGING:-false}"
CERTBOT_RENEW_DRY_RUN="${CERTBOT_RENEW_DRY_RUN:-false}"
BLUEGREEN_STATE_ROOT="${BLUEGREEN_STATE_ROOT:-/var/lib/jato-release}"
ACTIVE_SLOT_FILE="${ACTIVE_SLOT_FILE:-$BLUEGREEN_STATE_ROOT/active-slot}"
DEPLOYMENT_MARKER="${DEPLOYMENT_MARKER:-$BLUEGREEN_STATE_ROOT/deployment-maintenance}"
ACTIVE_RELEASE_LINK="${ACTIVE_RELEASE_LINK:-/opt/jato/active}"
SLOTS_ROOT="${SLOTS_ROOT:-/opt/jato/slots}"
RELEASES_ROOT="${RELEASES_ROOT:-/opt/jato/releases}"
ACTIVE_RELEASE_CONF="${ACTIVE_RELEASE_CONF:-/etc/jato-fullstack/nginx/active-release.conf}"
NGINX_SITE_CONF="${NGINX_SITE_CONF:-/etc/nginx/sites-available/jato_fullstack.conf}"
BLUEGREEN_ACTIVE=false
EXPECTED_ACTIVE_SLOT=""
EXPECTED_ACTIVE_ROOT=""

is_truthy() {
  case "$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

if [[ ! -f "$PRODUCTION_MUTATION_LOCK_LIB" \
  || -L "$PRODUCTION_MUTATION_LOCK_LIB" ]]; then
  echo "[ERROR] Production mutation lock helper is missing or unsafe" >&2
  exit 1
fi
# shellcheck disable=SC1090
source "$PRODUCTION_MUTATION_LOCK_LIB"

fsync_managed_nginx_file() {
  local file_path="$1"
  if [[ ! -f "$file_path" || -L "$file_path" ]]; then
    echo "[ERROR] Managed Nginx file is missing or unsafe: $file_path" >&2
    return 1
  fi
  python3 -B - "$file_path" <<'PY'
import os
import stat
import sys

path = sys.argv[1]
flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
descriptor = os.open(path, flags)
try:
    if not stat.S_ISREG(os.fstat(descriptor).st_mode):
        raise SystemExit(f"[ERROR] managed Nginx path is not a regular file: {path}")
    os.fsync(descriptor)
finally:
    os.close(descriptor)

parent_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
parent = os.open(os.path.dirname(path), parent_flags)
try:
    os.fsync(parent)
finally:
    os.close(parent)
PY
}

read_bluegreen_contract() {
  python3 - \
    "$ACTIVE_SLOT_FILE" \
    "$DEPLOYMENT_MARKER" \
    "$ACTIVE_RELEASE_LINK" \
    "$SLOTS_ROOT" \
    "$RELEASES_ROOT" \
    "$ACTIVE_RELEASE_CONF" \
    "$NGINX_SITE_CONF" <<'PY'
from pathlib import Path
import re
import sys

(
    slot_file,
    marker,
    active_link,
    slots_root,
    releases_root,
    active_conf,
    site_conf,
) = map(Path, sys.argv[1:])
if marker.exists() or marker.is_symlink():
    raise SystemExit("[ERROR] deployment maintenance fence is active; TLS changes are blocked")
if not slot_file.is_file() or slot_file.is_symlink():
    raise SystemExit("[ERROR] durable active-slot must be a regular non-symlink file")
slot_text = slot_file.read_text(encoding="utf-8")
if slot_text.strip() not in {"8000", "8001"} or len(slot_text.split()) != 1:
    raise SystemExit("[ERROR] durable active-slot is malformed")
slot = slot_text.strip()
slot_link = slots_root / slot / "current"
if not active_link.is_symlink() or not slot_link.is_symlink():
    raise SystemExit("[ERROR] blue/green active and slot/current paths must be symlinks")
try:
    active_root = active_link.resolve(strict=True)
    slot_root = slot_link.resolve(strict=True)
    releases = releases_root.resolve(strict=True)
except OSError as exc:
    raise SystemExit(f"[ERROR] blue/green release symlink resolution failed: {exc}") from exc
if active_root != slot_root:
    raise SystemExit("[ERROR] active release link does not match durable active-slot")
try:
    active_root.relative_to(releases)
except ValueError as exc:
    raise SystemExit("[ERROR] active release escaped the immutable releases root") from exc
frontend = active_root / "06_AppPlatform/frontend/dist"
if not frontend.is_dir() or frontend.is_symlink():
    raise SystemExit("[ERROR] active frontend dist is missing or unsafe")
if not active_conf.is_file() or active_conf.is_symlink():
    raise SystemExit("[ERROR] active Nginx release include is missing or unsafe")
active_text = active_conf.read_text(encoding="utf-8")
if not re.search(
    rf"(?m)^[ \t]*server[ \t]+127\.0\.0\.1:{slot}[ \t]+",
    active_text,
):
    raise SystemExit("[ERROR] active Nginx upstream does not match durable active-slot")
if f'default "{frontend}";' not in active_text:
    raise SystemExit("[ERROR] active Nginx frontend does not match the active release")
if "listen 127.0.0.1:18000;" not in active_text:
    raise SystemExit("[ERROR] stable loopback API is missing from active Nginx release")
if not site_conf.is_file() or site_conf.is_symlink():
    raise SystemExit("[ERROR] JATO Nginx site is missing or unsafe")
site_text = site_conf.read_text(encoding="utf-8")
required = (
    "include /etc/jato-fullstack/nginx/active-release.conf;",
    "root $jato_frontend_root;",
    "if (-f /var/lib/jato-release/deployment-maintenance)",
)
missing = [entry for entry in required if entry not in site_text]
if missing:
    raise SystemExit(f"[ERROR] JATO Nginx site lost its blue/green contract: {missing}")
if re.search(r"(?m)^[ \t]*upstream[ \t]+jato_fullstack_api[ \t]*\{", site_text):
    raise SystemExit("[ERROR] JATO Nginx site must not own a fixed backend upstream")
print(f"{slot}\t{active_root}\t{frontend}")
PY
}

detect_bluegreen_state() {
  local contract=""
  local active_frontend=""
  if [[ ! -e "$ACTIVE_SLOT_FILE" && ! -L "$ACTIVE_SLOT_FILE" \
    && ! -e "$ACTIVE_RELEASE_LINK" && ! -L "$ACTIVE_RELEASE_LINK" ]]; then
    return 1
  fi
  contract="$(read_bluegreen_contract)"
  IFS=$'\t' read -r EXPECTED_ACTIVE_SLOT EXPECTED_ACTIVE_ROOT active_frontend \
    <<< "$contract"
  if [[ -z "$EXPECTED_ACTIVE_SLOT" || -z "$EXPECTED_ACTIVE_ROOT" \
    || -z "$active_frontend" ]]; then
    echo "[ERROR] Failed to parse the blue/green active contract" >&2
    return 2
  fi
  BACKEND_PORT="$EXPECTED_ACTIVE_SLOT"
  FRONTEND_ROOT="$active_frontend"
  BLUEGREEN_ACTIVE=true
}

if [[ -z "$SERVER_NAME" || "$SERVER_NAME" == "_" ]]; then
  echo "[ERROR] SERVER_NAME must contain one or more public domains before enabling HTTPS"
  exit 1
fi

read -r -a DOMAINS <<<"$SERVER_NAME"
if [[ "${#DOMAINS[@]}" -eq 0 ]]; then
  echo "[ERROR] Failed to parse domains from SERVER_NAME=$SERVER_NAME"
  exit 1
fi

PRIMARY_DOMAIN="${DOMAINS[0]}"
CERT_PATH="/etc/letsencrypt/live/${PRIMARY_DOMAIN}/fullchain.pem"

jato_acquire_production_mutation_lock

if detect_bluegreen_state; then
  echo "[INFO] Verified blue/green active slot $EXPECTED_ACTIVE_SLOT; preserving active-release.conf"
else
  detect_rc=$?
  if [[ "$detect_rc" -ne 1 ]]; then
    exit "$detect_rc"
  fi
  if [[ -z "$BACKEND_PORT" || -z "$FRONTEND_ROOT" ]]; then
    echo "[ERROR] Initial non-blue/green TLS setup requires explicit BACKEND_PORT and FRONTEND_ROOT" >&2
    exit 1
  fi
  echo "[INFO] Ensure the explicitly selected initial nginx config exists for: $SERVER_NAME"
  SERVER_NAME="$SERVER_NAME" BACKEND_PORT="$BACKEND_PORT" FRONTEND_ROOT="$FRONTEND_ROOT" \
    bash "$NGINX_INSTALL_SCRIPT"
fi

echo "[INFO] Install certbot"
apt-get update -y
apt-get install -y certbot python3-certbot-nginx

if command -v ufw >/dev/null 2>&1 && ufw status 2>/dev/null | grep -qi 'Status: active'; then
  ufw allow 'Nginx Full' >/dev/null 2>&1 || true
fi

certbot_args=(
  --nginx
  --non-interactive
  --agree-tos
  --redirect
  --keep-until-expiring
  --expand
  --cert-name "$PRIMARY_DOMAIN"
)

if [[ -n "$CERTBOT_EMAIL" ]]; then
  certbot_args+=(--email "$CERTBOT_EMAIL")
else
  echo "[WARN] CERTBOT_EMAIL not provided; registering certbot without a contact email"
  certbot_args+=(--register-unsafely-without-email)
fi

if is_truthy "$CERTBOT_STAGING"; then
  certbot_args+=(--staging)
fi

for domain in "${DOMAINS[@]}"; do
  certbot_args+=(-d "$domain")
done

certbot_succeeded=false
if certbot "${certbot_args[@]}"; then
  certbot_succeeded=true
elif [[ -f "$CERT_PATH" ]]; then
  echo "[WARN] Certbot returned non-zero, but an existing certificate is present; validating current HTTPS before continuing"
else
  exit 1
fi

if [[ "$BLUEGREEN_ACTIVE" == "true" ]]; then
  contract="$(read_bluegreen_contract)"
  IFS=$'\t' read -r current_slot current_root current_frontend <<< "$contract"
  if [[ "$current_slot" != "$EXPECTED_ACTIVE_SLOT" \
    || "$current_root" != "$EXPECTED_ACTIVE_ROOT" \
    || "$current_frontend" != "$FRONTEND_ROOT" ]]; then
    echo "[ERROR] Active release changed during TLS setup; refusing an unsafe reload" >&2
    exit 1
  fi
  echo "[INFO] TLS updated without rewriting the blue/green active release include"
else
  echo "[INFO] Re-apply the explicitly selected initial nginx contract after certbot"
  SERVER_NAME="$SERVER_NAME" BACKEND_PORT="$BACKEND_PORT" FRONTEND_ROOT="$FRONTEND_ROOT" \
    bash "$NGINX_INSTALL_SCRIPT"
fi

fsync_managed_nginx_file "$ACTIVE_RELEASE_CONF"
fsync_managed_nginx_file "$NGINX_SITE_CONF"

echo "[INFO] Validate nginx and local HTTPS"
nginx -t
systemctl reload nginx
for domain in "${DOMAINS[@]}"; do
  curl --fail --silent --show-error \
    --resolve "${domain}:443:127.0.0.1" \
    "https://${domain}/healthz" >/dev/null
done

if systemctl list-unit-files 2>/dev/null | grep -q '^certbot.timer'; then
  systemctl enable --now certbot.timer >/dev/null 2>&1 || true
fi

if is_truthy "$CERTBOT_RENEW_DRY_RUN"; then
  echo "[INFO] Running certbot renew dry-run"
  certbot renew --dry-run
fi

if [[ "$certbot_succeeded" == "true" ]]; then
  echo "[INFO] HTTPS ready for $SERVER_NAME"
else
  echo "[WARN] Reused the existing certificate after a failed certbot attempt; HTTPS still validated for $SERVER_NAME"
fi
