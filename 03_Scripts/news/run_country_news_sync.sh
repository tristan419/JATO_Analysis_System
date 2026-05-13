#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="${REPO_DIR:-$(cd "$SCRIPT_DIR/../.." && pwd)}"
MAIN_SCRIPT="$SCRIPT_DIR/sync_country_news_digest.py"
PYTHON_BIN="${JATO_COUNTRY_NEWS_PYTHON:-$REPO_DIR/.venv/bin/python}"
VENV_ACTIVATE="$REPO_DIR/.venv/bin/activate"
LOG_DIR="${JATO_COUNTRY_NEWS_LOG_DIR:-$REPO_DIR/03_Scripts/logs}"
LOCK_FILE="${JATO_COUNTRY_NEWS_LOCK_FILE:-/tmp/jato-country-news-sync.lock}"
SYNC_ARGS="${JATO_COUNTRY_NEWS_SYNC_ARGS:---workers 4}"
TIMESTAMP="$(date '+%Y%m%d-%H%M%S')"
LOG_FILE="$LOG_DIR/country-news-sync-$TIMESTAMP.log"
LATEST_LOG_LINK="$LOG_DIR/country-news-sync-latest.log"
FAILURE_SUMMARY_FILE="$LOG_DIR/country-news-sync-last-failure.txt"
JOB_STARTED_AT="$(date '+%Y-%m-%d %H:%M:%S %z')"
HOST_NAME="$(scutil --get LocalHostName 2>/dev/null || hostname -s 2>/dev/null || hostname 2>/dev/null || echo unknown-host)"
ALERT_EMAIL="${JATO_COUNTRY_NEWS_ALERT_EMAIL:-}"
ALERT_NOTIFY="${JATO_COUNTRY_NEWS_ALERT_NOTIFY:-true}"
ALERT_MAIL_BIN="${JATO_COUNTRY_NEWS_ALERT_MAIL_BIN:-mail}"
ALERT_TAIL_LINES="${JATO_COUNTRY_NEWS_ALERT_TAIL_LINES:-40}"

# Also load from /etc/jato-fullstack/ env files on Tencent Cloud.
BACKEND_ENV_FILE="${BACKEND_ENV_FILE:-/etc/jato-fullstack/backend.env}"
NEWS_ENV_FILE="${NEWS_ENV_FILE:-/etc/jato-fullstack/country-news.env}"

export PATH="/opt/homebrew/opt/postgresql@16/bin:/opt/homebrew/opt/libpq/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}"

load_env_file() {
  local path="$1"
  if [[ -f "$path" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$path"
    set +a
  fi
}

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
    printf 'JATO Country News sync failed\n'
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
    echo "[WARN] JATO_COUNTRY_NEWS_ALERT_EMAIL is empty; skipping failure email"
    return 0
  fi

  mail_bin_path="$(command -v "$ALERT_MAIL_BIN" 2>/dev/null || true)"
  if [[ -z "$mail_bin_path" ]]; then
    echo "[WARN] Alert mail binary not found: $ALERT_MAIL_BIN"
    return 1
  fi

  subject="[JATO Country News Sync Failed] $HOST_NAME $(date '+%Y-%m-%d %H:%M:%S')"
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
  display notification "失败摘要已写入: " & summaryPath with title "JATO Country News Sync Failed" subtitle hostName sound name "Frog"
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

_write_news_status_json() {
  local status="$1"
  local success_count="$2"
  local failed_count="$3"
  local last_error="$4"

  local status_path="$LOG_DIR/scheduled_fetch_status.json"
  python3 - "$status_path" "$status" "$success_count" "$failed_count" "$last_error" <<'PY'
import json, os, sys
from datetime import datetime, timezone

path = sys.argv[1]
status = sys.argv[2]
success_count = int(sys.argv[3])
failed_count = int(sys.argv[4])
last_error = sys.argv[5]

existing = {}
if os.path.exists(path):
    try:
        existing = json.loads(open(path).read())
    except Exception:
        pass

existing["news"] = {
    "lastRunAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "status": status,
    "successCount": success_count,
    "failedCount": failed_count,
    "lastError": last_error or None,
}

json.dump(existing, open(path, "w"), indent=2, ensure_ascii=False)
PY
}

# Load env files (Tencent Cloud or local overrides).
load_env_file "$BACKEND_ENV_FILE"
load_env_file "$NEWS_ENV_FILE"

mkdir -p "$LOG_DIR"
exec > >(tee -a "$LOG_FILE") 2>&1
trap 'on_exit $?' EXIT

if command -v flock >/dev/null 2>&1; then
  exec 9>"$LOCK_FILE"
  if ! flock -n 9; then
    echo "[ERROR] Another country news sync job already holds lock $LOCK_FILE"
    exit 1
  fi
fi

if [[ ! -x "$PYTHON_BIN" && -f "$VENV_ACTIVATE" ]]; then
  # shellcheck disable=SC1090
  source "$VENV_ACTIVATE"
  PYTHON_BIN="$(command -v python)"
fi

if [[ ! -f "$MAIN_SCRIPT" ]]; then
  echo "[ERROR] News sync script not found: $MAIN_SCRIPT"
  exit 1
fi

if [[ -z "$PYTHON_BIN" || ! -x "$PYTHON_BIN" ]]; then
  echo "[ERROR] Python executable not found. Set JATO_COUNTRY_NEWS_PYTHON or create .venv." 
  exit 1
fi

ln -sfn "$LOG_FILE" "$LATEST_LOG_LINK"

echo "[INFO] Country news scheduled sync runner"
echo "[INFO] Host: $HOST_NAME"
echo "[INFO] Started: $JOB_STARTED_AT"
echo "[INFO] Repo: $REPO_DIR"
echo "[INFO] Python: $PYTHON_BIN"
echo "[INFO] Sync script: $MAIN_SCRIPT"
echo "[INFO] Log file: $LOG_FILE"
echo "[INFO] Lock file: $LOCK_FILE"
echo "[INFO] Sync args: $SYNC_ARGS"
echo "[INFO] Backend env: $BACKEND_ENV_FILE"
echo "[INFO] News env: $NEWS_ENV_FILE"
echo "[INFO] Alert email: ${ALERT_EMAIL:-<disabled>}"

cd "$REPO_DIR"
env_args=()
if [[ -n "$SYNC_ARGS" ]]; then
  read -r -a env_args <<< "$SYNC_ARGS"
fi

NEWS_EXIT=0
set +e
"$PYTHON_BIN" "$MAIN_SCRIPT" "${env_args[@]}" "$@"
NEWS_EXIT=$?
set -e

echo "[INFO] News sync exit code: $NEWS_EXIT"

# Parse country success/failure counts from the script output or log
NEWS_SUCCESS=0
NEWS_FAILED=0
NEWS_LAST_ERROR=""

if [[ "$NEWS_EXIT" -eq 0 ]]; then
  _write_news_status_json "success" "0" "0" ""
  echo "[INFO] Country news scheduled sync finished successfully"
else
  NEWS_LAST_ERROR="news sync exited with code $NEWS_EXIT"
  _write_news_status_json "failure" "0" "0" "$NEWS_LAST_ERROR"
  echo "[ERROR] Country news scheduled sync failed (exit=$NEWS_EXIT)"
  exit 1
fi
