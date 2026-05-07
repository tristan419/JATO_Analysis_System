#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="${REPO_DIR:-$(cd "$SCRIPT_DIR/../.." && pwd)}"
PYTHON_BIN="${JATO_VOC_PYTHON:-$REPO_DIR/.venv/bin/python}"
VENV_ACTIVATE="$REPO_DIR/.venv/bin/activate"
BACKEND_ENV_FILE="${BACKEND_ENV_FILE:-/etc/jato-fullstack/backend.env}"
VOC_ENV_FILE="${VOC_ENV_FILE:-/etc/jato-fullstack/voc.env}"
LOG_DIR="${JATO_VOC_LOG_DIR:-$REPO_DIR/03_Scripts/logs}"
LOCK_FILE="${JATO_VOC_LOCK_FILE:-/tmp/jato-voc-forum-sync.lock}"
OUTPUT_ROOT="${JATO_VOC_OUTPUT_ROOT:-04_Processed_data/voc}"
RAW_SUMMARY_PATH="${JATO_VOC_RAW_SUMMARY_PATH:-$LOG_DIR/voc-raw-latest.json}"
ENRICHED_SUMMARY_PATH="${JATO_VOC_ENRICHED_SUMMARY_PATH:-$LOG_DIR/voc-enriched-latest.json}"
TIMESTAMP="$(date '+%Y%m%d-%H%M%S')"
LOG_FILE="$LOG_DIR/voc-forum-sync-$TIMESTAMP.log"
LATEST_LOG_LINK="$LOG_DIR/voc-forum-sync-latest.log"
JOB_STARTED_AT="$(date '+%Y-%m-%d %H:%M:%S %z')"
HOST_NAME="$(hostname -s 2>/dev/null || hostname 2>/dev/null || echo unknown-host)"

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
  local normalized="${1,,}"
  case "$normalized" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

resolve_python_bin() {
  if [[ -x "$PYTHON_BIN" ]]; then
    return 0
  fi
  if [[ -f "$VENV_ACTIVATE" ]]; then
    # shellcheck disable=SC1090
    source "$VENV_ACTIVATE"
    PYTHON_BIN="$(command -v python)"
  fi
}

normalize_csv_words() {
  tr ',' ' ' <<<"$1" | xargs
}

resolve_output_root() {
  if [[ "$OUTPUT_ROOT" = /* ]]; then
    printf '%s\n' "$OUTPUT_ROOT"
  else
    printf '%s\n' "$REPO_DIR/$OUTPUT_ROOT"
  fi
}

run_voc_store_sync() {
  local output_root_abs="$1"
  local countries_raw="$2"

  PYTHONPATH="$REPO_DIR/06_AppPlatform/backend:${PYTHONPATH:-}" \
  REPO_DIR="$REPO_DIR" \
  JATO_VOC_COUNTRIES="$countries_raw" \
  JATO_VOC_OUTPUT_ROOT="$output_root_abs" \
  "$PYTHON_BIN" - <<'PY'
from pathlib import Path
import json
import os
import sys

repo_dir = Path(os.environ["REPO_DIR"])
sys.path.insert(0, str(repo_dir / "06_AppPlatform/backend"))

from app.services.voc_staging_service import sync_voc_raw_to_store

countries_raw = os.environ.get("JATO_VOC_COUNTRIES", "")
country_filter = {
    token.strip().upper()
    for token in countries_raw.replace(",", " ").split()
    if token.strip()
}
payload = sync_voc_raw_to_store(
    country_filter=country_filter or None,
    root=Path(os.environ["JATO_VOC_OUTPUT_ROOT"]),
)
print(json.dumps(payload, ensure_ascii=False, indent=2))
PY
}

load_env_file "$BACKEND_ENV_FILE"
load_env_file "$VOC_ENV_FILE"

JATO_VOC_BATCH_FILES="${JATO_VOC_BATCH_FILES:-07_ScrapingToolkit/voc_sources/batch_a.yaml}"
JATO_VOC_COUNTRIES="${JATO_VOC_COUNTRIES:-SE,FI,NO,DK,AT,CZ,HR,HU}"
JATO_VOC_MAX_LINKS_PER_SOURCE="${JATO_VOC_MAX_LINKS_PER_SOURCE:-4}"
JATO_VOC_TIMEOUT_SECONDS="${JATO_VOC_TIMEOUT_SECONDS:-20}"
JATO_VOC_SYNC_TO_STORE="${JATO_VOC_SYNC_TO_STORE:-true}"
JATO_VOC_ENRICH="${JATO_VOC_ENRICH:-true}"

mkdir -p "$LOG_DIR"
exec > >(tee -a "$LOG_FILE") 2>&1

if command -v flock >/dev/null 2>&1; then
  exec 9>"$LOCK_FILE"
  if ! flock -n 9; then
    echo "[ERROR] Another VOC sync job already holds lock $LOCK_FILE"
    exit 1
  fi
fi

resolve_python_bin
if [[ -z "$PYTHON_BIN" || ! -x "$PYTHON_BIN" ]]; then
  echo "[ERROR] Python executable not found. Set JATO_VOC_PYTHON or create .venv."
  exit 1
fi

OUTPUT_ROOT_ABS="$(resolve_output_root)"
COUNTRIES_NORMALIZED="$(normalize_csv_words "$JATO_VOC_COUNTRIES")"
IFS=' ' read -r -a VOC_BATCH_FILE_ARGS <<<"$(normalize_csv_words "$JATO_VOC_BATCH_FILES")"
IFS=' ' read -r -a VOC_COUNTRY_ARGS <<<"$COUNTRIES_NORMALIZED"

if command -v jato-voc-fetch >/dev/null 2>&1; then
  VOC_FETCH_CMD=(jato-voc-fetch)
else
  VOC_FETCH_CMD=("$PYTHON_BIN" "$REPO_DIR/07_ScrapingToolkit/jato_scraper/voc_fetcher.py")
fi

if command -v jato-voc-enrich >/dev/null 2>&1; then
  VOC_ENRICH_CMD=(jato-voc-enrich)
else
  VOC_ENRICH_CMD=("$PYTHON_BIN" "$REPO_DIR/07_ScrapingToolkit/jato_scraper/voc_enricher.py")
fi

VOC_FETCH_RUN=("${VOC_FETCH_CMD[@]}" --batch-files "${VOC_BATCH_FILE_ARGS[@]}")
VOC_ENRICH_RUN=("${VOC_ENRICH_CMD[@]}")

if [[ "${#VOC_COUNTRY_ARGS[@]}" -gt 0 ]]; then
  VOC_FETCH_RUN+=(--countries "${VOC_COUNTRY_ARGS[@]}")
  VOC_ENRICH_RUN+=(--countries "${VOC_COUNTRY_ARGS[@]}")
fi

VOC_FETCH_RUN+=(
  --output-root "$OUTPUT_ROOT_ABS"
  --max-links-per-source "$JATO_VOC_MAX_LINKS_PER_SOURCE"
  --timeout-seconds "$JATO_VOC_TIMEOUT_SECONDS"
  --output "$RAW_SUMMARY_PATH"
)
VOC_ENRICH_RUN+=(
  --output-root "$OUTPUT_ROOT_ABS"
  --output "$ENRICHED_SUMMARY_PATH"
)

ln -sfn "$LOG_FILE" "$LATEST_LOG_LINK"
mkdir -p "$(dirname "$RAW_SUMMARY_PATH")" "$(dirname "$ENRICHED_SUMMARY_PATH")" "$OUTPUT_ROOT_ABS"

echo "[INFO] VOC forum scheduled sync runner"
echo "[INFO] Host: $HOST_NAME"
echo "[INFO] Started: $JOB_STARTED_AT"
echo "[INFO] Repo: $REPO_DIR"
echo "[INFO] Python: $PYTHON_BIN"
echo "[INFO] Fetch command: ${VOC_FETCH_CMD[*]}"
echo "[INFO] Enrich command: ${VOC_ENRICH_CMD[*]}"
echo "[INFO] Batch files: ${VOC_BATCH_FILE_ARGS[*]}"
echo "[INFO] Countries: ${VOC_COUNTRY_ARGS[*]}"
echo "[INFO] Output root: $OUTPUT_ROOT_ABS"
echo "[INFO] Sync raw to store: $JATO_VOC_SYNC_TO_STORE"
echo "[INFO] Enrich: $JATO_VOC_ENRICH"
echo "[INFO] Log file: $LOG_FILE"

cd "$REPO_DIR"
"${VOC_FETCH_RUN[@]}"

if is_truthy "$JATO_VOC_SYNC_TO_STORE"; then
  echo "[INFO] Sync raw VOC artifacts to PostgreSQL staging"
  run_voc_store_sync "$OUTPUT_ROOT_ABS" "$JATO_VOC_COUNTRIES"
else
  echo "[INFO] Skip raw-to-store sync because JATO_VOC_SYNC_TO_STORE=$JATO_VOC_SYNC_TO_STORE"
fi

if is_truthy "$JATO_VOC_ENRICH"; then
  echo "[INFO] Build VOC enriched signals and deck artifacts"
  "${VOC_ENRICH_RUN[@]}"
else
  echo "[INFO] Skip VOC enrichment because JATO_VOC_ENRICH=$JATO_VOC_ENRICH"
fi

echo "[INFO] VOC forum scheduled sync finished successfully"