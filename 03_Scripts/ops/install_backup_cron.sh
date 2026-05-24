#!/usr/bin/env bash
set -Eeuo pipefail

REPO_DIR="${REPO_DIR:-/opt/JATO_Analysis_System-main}"
BACKEND_ENV_FILE="${BACKEND_ENV_FILE:-/etc/jato-fullstack/backend.env}"
BACKUP_SCRIPT="$REPO_DIR/03_Scripts/ops/backup_production_data.sh"
CRON_SCHEDULE="${BACKUP_CRON_SCHEDULE:-0 2 * * *}"
CRON_USER="${BACKUP_CRON_USER:-root}"
CRON_LINE="$CRON_SCHEDULE REPO_DIR=$REPO_DIR BACKEND_ENV_FILE=$BACKEND_ENV_FILE $BACKUP_SCRIPT >> /var/log/jato-backup.log 2>&1"

if [[ ! -f "$BACKUP_SCRIPT" ]]; then
  echo "[ERROR] Backup script missing: $BACKUP_SCRIPT" >&2
  exit 1
fi

if [[ "$(id -u)" -ne 0 ]]; then
  echo "[ERROR] Run this script with sudo/root so it can install the production cron." >&2
  exit 1
fi

tmp="$(mktemp)"
crontab -u "$CRON_USER" -l 2>/dev/null | grep -v 'backup_production_data.sh' > "$tmp" || true
printf '%s\n' "$CRON_LINE" >> "$tmp"
crontab -u "$CRON_USER" "$tmp"
rm -f "$tmp"

echo "[INFO] Installed backup cron for $CRON_USER:"
echo "$CRON_LINE"
