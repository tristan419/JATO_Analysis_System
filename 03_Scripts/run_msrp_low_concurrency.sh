#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="${REPO_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
BACKEND_ENV_FILE="${BACKEND_ENV_FILE:-/etc/jato-fullstack/backend.env}"
MSRP_ENV_FILE="${MSRP_ENV_FILE:-/etc/jato-fullstack/msrp.env}"

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
  case "${1,,}" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

resolve_countries() {
  case "$1" in
    batch_a|a) printf '%s\n' se fi no dk hu hr at cz de fr it pl ;;
    1) printf '%s\n' se hr ;;
    2) printf '%s\n' hu no at cz ch ;;
    all) printf '%s\n' se hr hu no at cz ch de fr it pl ;;
    *) tr ',' '\n' <<<"$1" | sed 's/^ *//;s/ *$//' | sed '/^$/d' ;;
  esac
}

load_env_file "$BACKEND_ENV_FILE"
load_env_file "$MSRP_ENV_FILE"

MODE="${JATO_MSRP_MODE:-dryrun}"
COUNTRIES_RAW="${JATO_MSRP_COUNTRIES:-batch_a}"
PAUSE_SECONDS="${JATO_MSRP_PAUSE_SECONDS:-20}"
STOP_ON_FAILURE="${JATO_MSRP_STOP_ON_FAILURE:-true}"
PYTHON_BIN="${JATO_MSRP_PYTHON:-$REPO_DIR/.venv/bin/python}"
LOG_DIR="${JATO_MSRP_LOG_DIR:-$REPO_DIR/03_Scripts/logs}"
LOCK_FILE="${JATO_MSRP_LOCK_FILE:-/tmp/jato-msrp-low-concurrency.lock}"
BACKEND_PORT="${BACKEND_PORT:-8000}"
export JATO_API_BASE="${JATO_API_BASE:-http://127.0.0.1:${BACKEND_PORT}/v1}"
export JATO_STRICT_EXIT="${JATO_STRICT_EXIT:-true}"
export APP_USER_NAME="${APP_USER_NAME:-msrp-cron}"
AUTO_REVIEW="${JATO_MSRP_AUTO_REVIEW:-true}"
AUTO_MATERIALIZE="${JATO_MSRP_AUTO_MATERIALIZE:-true}"
AUTO_REVIEW_LIMIT="${JATO_MSRP_AUTO_REVIEW_LIMIT:-500}"
MATERIALIZE_LIMIT="${JATO_MSRP_MATERIALIZE_LIMIT:-500}"
AUTO_REVIEW_DECIDED_BY="${JATO_AUTO_REVIEW_DECIDED_BY:-${APP_USER_NAME:-msrp-cron}}"
export NVAPI_KEY="${NVAPI_KEY:-${NVIDIA_API_KEY:-}}"

case "$MODE" in
  dryrun) TARGET_SCRIPT="$SCRIPT_DIR/batch_dryrun.py" ;;
  ingest) TARGET_SCRIPT="$SCRIPT_DIR/batch_ingest.py" ;;
  *)
    echo "[ERROR] Unsupported JATO_MSRP_MODE=$MODE. Use dryrun or ingest." >&2
    exit 1
    ;;
esac

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "[ERROR] Python executable not found: $PYTHON_BIN" >&2
  exit 1
fi

mkdir -p "$LOG_DIR"
TIMESTAMP="$(date '+%Y%m%d-%H%M%S')"
LOG_FILE="$LOG_DIR/msrp-${MODE}-${TIMESTAMP}.log"
exec > >(tee -a "$LOG_FILE") 2>&1

if command -v flock >/dev/null 2>&1; then
  exec 9>"$LOCK_FILE"
  if ! flock -n 9; then
    echo "[ERROR] Another MSRP run already holds lock $LOCK_FILE"
    exit 1
  fi
fi

mapfile -t COUNTRIES < <(resolve_countries "$COUNTRIES_RAW")
if [[ "${#COUNTRIES[@]}" -eq 0 ]]; then
  echo "[ERROR] No countries resolved from JATO_MSRP_COUNTRIES=$COUNTRIES_RAW" >&2
  exit 1
fi

CONCURRENCY="${JATO_MSRP_CONCURRENCY:-2}"

echo "[INFO] MSRP low-concurrency runner"
echo "[INFO] Repo: $REPO_DIR"
echo "[INFO] Mode: $MODE"
echo "[INFO] Countries: ${COUNTRIES[*]}"
echo "[INFO] Concurrency: $CONCURRENCY (JATO_MSRP_CONCURRENCY)"
echo "[INFO] Backend env: $BACKEND_ENV_FILE"
echo "[INFO] MSRP env: $MSRP_ENV_FILE"
echo "[INFO] API base: $JATO_API_BASE"
echo "[INFO] Log file: $LOG_FILE"
echo "[INFO] Auto review: $AUTO_REVIEW"
echo "[INFO] Auto materialize: $AUTO_MATERIALIZE"

total="${#COUNTRIES[@]}"
active=0
country_idx=0
failures=0
declare -a pids=()
declare -A pid_country=()

while (( country_idx < total || active > 0 )); do
  # Start new jobs while under concurrency limit
  while (( active < CONCURRENCY && country_idx < total )); do
    country="${COUNTRIES[$country_idx]}"
    country_log="${LOG_DIR}/msrp-${MODE}-${country}-$(date +%Y%m%d-%H%M%S).log"
    echo "[RUN] $((country_idx + 1))/$total country=$country mode=$MODE (parallel slot $((active + 1))/$CONCURRENCY)"

    extra_args=()
    if [[ "$MODE" == "ingest" ]]; then
      if is_truthy "$AUTO_REVIEW"; then
        extra_args+=(--auto-review --decided-by "$AUTO_REVIEW_DECIDED_BY" --auto-review-limit "$AUTO_REVIEW_LIMIT")
      fi
      if is_truthy "$AUTO_MATERIALIZE"; then
        extra_args+=(--materialize --materialize-limit "$MATERIALIZE_LIMIT")
      fi
    fi

    (
      if "$PYTHON_BIN" "$TARGET_SCRIPT" "$country" "${extra_args[@]}"; then
        echo "[OK] country=$country"
        exit 0
      else
        echo "[FAIL] country=$country"
        exit 1
      fi
    ) > "$country_log" 2>&1 &

    pids+=($!)
    pid_country[$!]="$country"
    active=$((active + 1))
    country_idx=$((country_idx + 1))
  done

  # Wait for any child to finish
  if (( active > 0 )); then
    if wait -n "${pids[@]}" 2>/dev/null; then
      finished_pid=$!
    else
      # Find which pid finished
      for pid in "${pids[@]}"; do
        if ! kill -0 "$pid" 2>/dev/null; then
          finished_pid=$pid
          break
        fi
      done
    fi

    # Find and remove finished pid
    new_pids=()
    for pid in "${pids[@]}"; do
      if [[ "$pid" == "$finished_pid" ]] || ! kill -0 "$pid" 2>/dev/null; then
        wait "$pid" 2>/dev/null && true
        rc=$?
        finished_country="${pid_country[$pid]:-?}"
        if [[ $rc -ne 0 ]]; then
          failures=$((failures + 1))
          if is_truthy "$STOP_ON_FAILURE"; then
            echo "[INFO] Stopping because JATO_MSRP_STOP_ON_FAILURE=true"
            # Kill remaining children
            for rpid in "${new_pids[@]}"; do
              kill "$rpid" 2>/dev/null || true
            done
            wait 2>/dev/null
            exit 1
          fi
        fi
        active=$((active - 1))
      else
        new_pids+=("$pid")
      fi
    done
    pids=("${new_pids[@]}")
  fi
done

# Collect per-country logs into main log
for country in "${COUNTRIES[@]}"; do
  for clog in "$LOG_DIR"/msrp-${MODE}-${country}-*.log; do
    if [[ -f "$clog" ]]; then
      cat "$clog" >> "$LOG_FILE"
    fi
  done
done

if (( failures > 0 )); then
  echo "[WARN] Completed with $failures failed country runs"
  exit 1
fi

echo "[INFO] Completed successfully (parallel ×${CONCURRENCY})"
