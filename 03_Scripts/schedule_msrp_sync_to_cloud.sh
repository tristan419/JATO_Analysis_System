#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="${REPO_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
MAIN_SCRIPT="$SCRIPT_DIR/sync_msrp_db_to_cloud.sh"
VENV_ACTIVATE="$REPO_DIR/.venv/bin/activate"
LOG_DIR="${JATO_MSRP_SYNC_LOG_DIR:-$SCRIPT_DIR/logs}"
LOCK_FILE="${JATO_MSRP_SYNC_LOCK_FILE:-/tmp/jato-msrp-db-sync.lock}"
TIMESTAMP="$(date '+%Y%m%d-%H%M%S')"
LOG_FILE="$LOG_DIR/msrp-db-sync-$TIMESTAMP.log"
LATEST_LOG_LINK="$LOG_DIR/msrp-db-sync-latest.log"
FAILURE_SUMMARY_FILE="$LOG_DIR/msrp-db-sync-last-failure.txt"
JOB_STARTED_AT="$(date '+%Y-%m-%d %H:%M:%S %z')"
HOST_NAME="$(scutil --get LocalHostName 2>/dev/null || hostname -s 2>/dev/null || hostname 2>/dev/null || echo unknown-host)"
ALERT_EMAIL="${JATO_MSRP_ALERT_EMAIL:-}"
ALERT_NOTIFY="${JATO_MSRP_ALERT_NOTIFY:-true}"
ALERT_MAIL_BIN="${JATO_MSRP_ALERT_MAIL_BIN:-mail}"
ALERT_TAIL_LINES="${JATO_MSRP_ALERT_TAIL_LINES:-40}"

export PATH="/opt/homebrew/opt/postgresql@16/bin:/opt/homebrew/opt/libpq/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}"

is_truthy() {
  local normalized=""
  normalized="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')"
  case "$normalized" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

write_failure_summary() {
  local exit_code="$1"
  {
    printf 'JATO MSRP DB sync failed\n'
    printf 'Host: %s\n' "$HOST_NAME"
    printf 'Started: %s\n' "$JOB_STARTED_AT"
    printf 'Ended: %s\n' "$(date '+%Y-%m-%d %H:%M:%S %z')"
    printf 'Exit code: %s\n' "$exit_code"
    printf 'Repo: %s\n' "$REPO_DIR"
    printf 'Sync script: %s\n' "$MAIN_SCRIPT"
    printf 'Log file: %s\n' "$LOG_FILE"
    if [[ -n "$ALERT_EMAIL" ]]; then
      printf 'Alert email: %s\n' "$ALERT_EMAIL"
    fi
    printf '\nRecent log tail (%s lines):\n' "$ALERT_TAIL_LINES"
    tail -n "$ALERT_TAIL_LINES" "$LOG_FILE" 2>/dev/null || true
  } > "$FAILURE_SUMMARY_FILE"
  echo "[ERROR] Failure summary written: $FAILURE_SUMMARY_FILE"
}

send_failure_email() {
  local mail_bin_path=""
  local subject=""

  if [[ -z "$ALERT_EMAIL" ]]; then
    echo "[WARN] JATO_MSRP_ALERT_EMAIL is empty; skipping failure email"
    return 0
  fi

  mail_bin_path="$(command -v "$ALERT_MAIL_BIN" 2>/dev/null || true)"
  if [[ -z "$mail_bin_path" ]]; then
    echo "[WARN] Alert mail binary not found: $ALERT_MAIL_BIN"
    return 1
  fi

  subject="[JATO MSRP Sync Failed] $HOST_NAME $(date '+%Y-%m-%d %H:%M:%S')"
  if "$mail_bin_path" -s "$subject" "$ALERT_EMAIL" < "$FAILURE_SUMMARY_FILE"; then
    echo "[INFO] Failure alert email submitted to $ALERT_EMAIL via $mail_bin_path"
    return 0
  fi

  echo "[WARN] Failed to submit failure alert email via $mail_bin_path"
  return 1
}

send_failure_notification() {
  if ! is_truthy "$ALERT_NOTIFY"; then
    return 0
  fi

  if ! command -v osascript >/dev/null 2>&1; then
    return 0
  fi

  osascript - "$HOST_NAME" "$FAILURE_SUMMARY_FILE" <<'APPLESCRIPT' >/dev/null 2>&1 || true
on run argv
  set hostName to item 1 of argv
  set summaryPath to item 2 of argv
  display notification "失败摘要已写入: " & summaryPath with title "JATO MSRP Sync Failed" subtitle hostName sound name "Frog"
end run
APPLESCRIPT
}

on_exit() {
  local exit_code="$1"

  if [[ "$exit_code" -eq 0 ]]; then
    rm -f "$FAILURE_SUMMARY_FILE"
    return 0
  fi

  write_failure_summary "$exit_code"
  send_failure_email || true
  send_failure_notification || true
}

mkdir -p "$LOG_DIR"
exec > >(tee -a "$LOG_FILE") 2>&1
trap 'on_exit $?' EXIT

if command -v flock >/dev/null 2>&1; then
  exec 9>"$LOCK_FILE"
  if ! flock -n 9; then
    echo "[ERROR] Another MSRP DB sync job already holds lock $LOCK_FILE"
    exit 1
  fi
fi

if [[ -f "$VENV_ACTIVATE" ]]; then
  # shellcheck disable=SC1090
  source "$VENV_ACTIVATE"
fi

if [[ ! -f "$MAIN_SCRIPT" ]]; then
  echo "[ERROR] Sync script not found: $MAIN_SCRIPT"
  exit 1
fi

ln -sfn "$LOG_FILE" "$LATEST_LOG_LINK"

echo "[INFO] MSRP scheduled sync runner"
echo "[INFO] Repo: $REPO_DIR"
echo "[INFO] Sync script: $MAIN_SCRIPT"
echo "[INFO] Log file: $LOG_FILE"
echo "[INFO] Lock file: $LOCK_FILE"
echo "[INFO] SSH alias: ${SSH_ALIAS:-tencent-cloud}"
echo "[INFO] Alert email: ${ALERT_EMAIL:-<disabled>}"

cd "$REPO_DIR"
/bin/bash "$MAIN_SCRIPT"

echo "[INFO] MSRP scheduled sync finished successfully"