#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
NGINX_INSTALL_SCRIPT="$ROOT_DIR/03_Scripts/deploy/nginx/install_jato_fullstack_nginx.sh"

SERVER_NAME="${SERVER_NAME:-}"
BACKEND_PORT="${BACKEND_PORT:-8000}"
FRONTEND_ROOT="${FRONTEND_ROOT:-/opt/JATO_Analysis_System/06_AppPlatform/frontend/dist}"
CERTBOT_EMAIL="${CERTBOT_EMAIL:-}"
CERTBOT_STAGING="${CERTBOT_STAGING:-false}"
CERTBOT_RENEW_DRY_RUN="${CERTBOT_RENEW_DRY_RUN:-false}"

is_truthy() {
  case "${1,,}" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
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
LOCAL_HEALTHCHECK_URL="https://${PRIMARY_DOMAIN}/healthz"
CERT_PATH="/etc/letsencrypt/live/${PRIMARY_DOMAIN}/fullchain.pem"

echo "[INFO] Ensure base nginx config exists for: $SERVER_NAME"
SERVER_NAME="$SERVER_NAME" BACKEND_PORT="$BACKEND_PORT" FRONTEND_ROOT="$FRONTEND_ROOT" \
  bash "$NGINX_INSTALL_SCRIPT"

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

echo "[INFO] Validate nginx and local HTTPS"
nginx -t
systemctl reload nginx
curl --fail --silent --show-error --resolve "${PRIMARY_DOMAIN}:443:127.0.0.1" "$LOCAL_HEALTHCHECK_URL" >/dev/null

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
