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
  case "$1" in
    1|true|TRUE|True|yes|YES|Yes|on|ON|On) return 0 ;;
    *) return 1 ;;
  esac
}

_write_msrp_status() {
  local pipeline="$1"
  local status="$2"
  local reason="$3"
  local metadata_json="${4:-}"
  local status_path="$REPO_DIR/03_Scripts/logs/scheduled_fetch_status.json"
  python3 -c "
import json,os
from datetime import datetime,timezone
p=os.path.expanduser('$status_path')
d=json.loads(open(p).read()) if os.path.exists(p) else {}
d['$pipeline']={
    'lastRunAt': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    'status': '$status',
    'reason': '$reason',
}
with open(p,'w') as f: f.write(json.dumps(d,indent=2)+chr(10))
" 2>/dev/null || true
  writer_cmd=(python3 "$REPO_DIR/03_Scripts/hermes/pipeline_status_writer.py" "$pipeline" \
    --status "$status" \
    --source "03_Scripts/run_msrp_low_concurrency.sh" \
    --message "$reason" \
    --artifact-ref "03_Scripts/logs/scheduled_fetch_status.json" \
    --repo-root "$REPO_DIR")
  if [[ -n "$metadata_json" ]]; then
    writer_cmd+=(--metadata-json "$metadata_json")
  fi
  "${writer_cmd[@]}" 2>/dev/null || true
  echo "[status] $pipeline=$status written to $status_path"
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
STOP_ON_FAILURE="${JATO_MSRP_STOP_ON_FAILURE:-false}"
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
AUTO_REVIEW_MIN_SCORE="${JATO_MSRP_AUTO_REVIEW_MIN_SCORE:-70}"
MATERIALIZE_LIMIT="${JATO_MSRP_MATERIALIZE_LIMIT:-500}"
AUTO_REVIEW_DECIDED_BY="${JATO_AUTO_REVIEW_DECIDED_BY:-${APP_USER_NAME:-msrp-cron}}"
REFRESH_CURRENT_SNAPSHOT="${JATO_MSRP_REFRESH_CURRENT_SNAPSHOT:-true}"
CURRENT_SNAPSHOT_LIMIT="${JATO_MSRP_CURRENT_SNAPSHOT_LIMIT:-500}"
CURRENT_SNAPSHOT_THRESHOLD_PCT="${JATO_MSRP_PRICE_ALERT_THRESHOLD_PCT:-3}"
CURRENT_SNAPSHOT_TIMEOUT_SECONDS="${JATO_MSRP_CURRENT_SNAPSHOT_TIMEOUT_SECONDS:-30}"
REFRESH_PRICE_ALERT_REVIEW_QUEUE="${JATO_MSRP_REFRESH_PRICE_ALERT_REVIEW_QUEUE:-true}"
REFRESH_READINESS_AUDIT="${JATO_MSRP_REFRESH_READINESS_AUDIT:-true}"
READINESS_AUDIT_TIMEOUT_SECONDS="${JATO_MSRP_READINESS_AUDIT_TIMEOUT_SECONDS:-30}"
COUNTRY_TIMEOUT_SECONDS="${JATO_MSRP_COUNTRY_TIMEOUT_SECONDS:-3600}"
export NVAPI_KEY="${NVAPI_KEY:-${NVIDIA_API_KEY:-}}"

if ! [[ "$COUNTRY_TIMEOUT_SECONDS" =~ ^[0-9]+$ ]]; then
  echo "[WARN] Invalid JATO_MSRP_COUNTRY_TIMEOUT_SECONDS=$COUNTRY_TIMEOUT_SECONDS; disabling per-country timeout"
  COUNTRY_TIMEOUT_SECONDS=0
fi
TIMEOUT_BIN="$(command -v timeout || true)"
if [[ "$COUNTRY_TIMEOUT_SECONDS" -gt 0 && -z "$TIMEOUT_BIN" ]]; then
  echo "[WARN] timeout command not found; per-country timeout disabled on this host"
fi

# ── Dryrun-to-ingest gate ──────────────────────────────────────────
MIN_DRYRUN_PASS_PCT="${JATO_MSRP_MIN_DRYRUN_PASS_PCT:-70}"
MSRP_ARTIFACT_DIR="$REPO_DIR/03_Scripts/diagnostics/artifacts"
DRYRUN_REPORT="$MSRP_ARTIFACT_DIR/dryrun_report.json"
SOURCE_REPAIR_BACKLOG_ARTIFACT="$MSRP_ARTIFACT_DIR/msrp_source_repair_backlog.json"
SOURCE_REFERENCE_EVIDENCE_ARTIFACT="$MSRP_ARTIFACT_DIR/msrp_source_reference_evidence.json"
SOURCE_REPAIR_BACKLOG_SCRIPT="$SCRIPT_DIR/msrp_source_repair_backlog.py"
SOURCE_REFERENCE_EVIDENCE_SCRIPT="$SCRIPT_DIR/msrp_reference_evidence.py"
SOURCE_REVIEW_QUEUE_SCRIPT="$SCRIPT_DIR/msrp_source_review_queue.py"
REFRESH_SOURCE_GOVERNANCE="${JATO_MSRP_REFRESH_SOURCE_GOVERNANCE:-true}"
REFRESH_SOURCE_REFERENCE_EVIDENCE="${JATO_MSRP_REFRESH_SOURCE_REFERENCE_EVIDENCE:-true}"
SOURCE_REFERENCE_PAGE_SIZE="${JATO_MSRP_SOURCE_REFERENCE_PAGE_SIZE:-1000}"
SOURCE_REFERENCE_MAX_PAGES="${JATO_MSRP_SOURCE_REFERENCE_MAX_PAGES:-2}"

if [[ "$MODE" == "ingest" ]]; then
  GATE_SKIP=false
  if [[ ! -f "$DRYRUN_REPORT" ]]; then
    echo "[GATE] No dryrun report found at $DRYRUN_REPORT — skipping ingest."
    GATE_SKIP=true
  else
    PASS_PCT=$(python3 -c "
import json
with open('$DRYRUN_REPORT') as f:
    r = json.load(f)
s = r.get('summary') or r
if 'passPct' in s:
    pct = float(s.get('passPct') or 0)
else:
    total = s.get('total', 0)
    ok = s.get('pass', 0)
    pct = (ok / total * 100) if total > 0 else 0
print(f'{pct:.0f}')
" 2>/dev/null || echo "0")
    echo "[GATE] Latest dryrun pass rate: ${PASS_PCT}% (threshold: ${MIN_DRYRUN_PASS_PCT}%)"
    if python3 -c "exit(0 if int('${PASS_PCT:-0}') < int('$MIN_DRYRUN_PASS_PCT') else 1)" 2>/dev/null; then
      echo "[GATE] Pass rate ${PASS_PCT}% < ${MIN_DRYRUN_PASS_PCT}% — skipping ingest."
      GATE_SKIP=true
    fi
  fi

  if $GATE_SKIP; then
    _write_msrp_status "msrp_ingest" "skipped" "Dryrun pass rate ${PASS_PCT:-0}% below threshold ${MIN_DRYRUN_PASS_PCT}%"
    exit 0
  fi
fi

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

# ── Phase 2: Per-batch run directory ───────────────────────────────
TIMESTAMP="$(date -u '+%Y%m%d-%H%M%S')"
RUN_ID="msrp-${MODE}-${TIMESTAMP}"
RUN_DIR="$LOG_DIR/$RUN_ID"
COUNTRY_ARTIFACT_DIR="$RUN_DIR/countries"
COUNTRY_DONE_DIR="$RUN_DIR/.country_done"
mkdir -p "$RUN_DIR" "$COUNTRY_ARTIFACT_DIR" "$COUNTRY_DONE_DIR"

LOG_FILE="$RUN_DIR/run.log"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "[INFO] Run ID: $RUN_ID"
echo "[INFO] Run dir: $RUN_DIR"

if command -v flock >/dev/null 2>&1; then
  exec 9>"$LOCK_FILE"
  if ! flock -n 9; then
    echo "[ERROR] Another MSRP run already holds lock $LOCK_FILE"
    exit 1
  fi
fi

COUNTRIES=()
while IFS= read -r country; do
  COUNTRIES+=("$country")
done < <(resolve_countries "$COUNTRIES_RAW")
if [[ "${#COUNTRIES[@]}" -eq 0 ]]; then
  echo "[ERROR] No countries resolved from JATO_MSRP_COUNTRIES=$COUNTRIES_RAW" >&2
  exit 1
fi

REQUESTED_CONCURRENCY="${JATO_MSRP_CONCURRENCY:-2}"
CONCURRENCY="$REQUESTED_CONCURRENCY"
if ! [[ "$CONCURRENCY" =~ ^[0-9]+$ ]] || [[ "$CONCURRENCY" -lt 1 ]]; then
  echo "[WARN] Invalid JATO_MSRP_CONCURRENCY=$REQUESTED_CONCURRENCY; falling back to 1"
  CONCURRENCY=1
fi
MAX_DRYRUN_CONCURRENCY="${JATO_MSRP_MAX_DRYRUN_CONCURRENCY:-2}"
if ! [[ "$MAX_DRYRUN_CONCURRENCY" =~ ^[0-9]+$ ]] || [[ "$MAX_DRYRUN_CONCURRENCY" -lt 1 ]]; then
  echo "[WARN] Invalid JATO_MSRP_MAX_DRYRUN_CONCURRENCY=$MAX_DRYRUN_CONCURRENCY; using 2"
  MAX_DRYRUN_CONCURRENCY=2
fi
ALLOW_HIGH_CONCURRENCY="${JATO_MSRP_ALLOW_HIGH_CONCURRENCY:-false}"
if [[ "$MODE" == "dryrun" ]] && ! is_truthy "$ALLOW_HIGH_CONCURRENCY" && [[ "$CONCURRENCY" -gt "$MAX_DRYRUN_CONCURRENCY" ]]; then
  echo "[WARN] Dryrun concurrency requested=$REQUESTED_CONCURRENCY capped to $MAX_DRYRUN_CONCURRENCY"
  echo "[WARN] Set JATO_MSRP_ALLOW_HIGH_CONCURRENCY=true to override after pass rates are stable."
  CONCURRENCY="$MAX_DRYRUN_CONCURRENCY"
fi

echo "[INFO] MSRP low-concurrency runner"
echo "[INFO] Repo: $REPO_DIR"
echo "[INFO] Mode: $MODE"
echo "[INFO] Countries: ${COUNTRIES[*]}"
echo "[INFO] Requested concurrency: $REQUESTED_CONCURRENCY"
echo "[INFO] Concurrency: $CONCURRENCY"
echo "[INFO] Max dryrun concurrency: $MAX_DRYRUN_CONCURRENCY"
echo "[INFO] Allow high concurrency: $ALLOW_HIGH_CONCURRENCY"
echo "[INFO] Backend env: $BACKEND_ENV_FILE"
echo "[INFO] MSRP env: $MSRP_ENV_FILE"
echo "[INFO] API base: $JATO_API_BASE"
echo "[INFO] Log file: $LOG_FILE"
echo "[INFO] Auto review: $AUTO_REVIEW"
echo "[INFO] Auto materialize: $AUTO_MATERIALIZE"
echo "[INFO] Auto review min score: $AUTO_REVIEW_MIN_SCORE"
echo "[INFO] Refresh source governance artifacts: $REFRESH_SOURCE_GOVERNANCE"
echo "[INFO] Refresh source reference evidence: $REFRESH_SOURCE_REFERENCE_EVIDENCE"
echo "[INFO] Refresh current price snapshot: $REFRESH_CURRENT_SNAPSHOT"
echo "[INFO] Refresh MSRP readiness audit: $REFRESH_READINESS_AUDIT"
echo "[INFO] Country timeout seconds: $COUNTRY_TIMEOUT_SECONDS"

MSRP_RUNTIME_METADATA="$(
  MSRP_MODE="$MODE" \
  MSRP_COUNTRIES_RAW="$COUNTRIES_RAW" \
  MSRP_COUNTRIES="${COUNTRIES[*]}" \
  MSRP_REQUESTED_CONCURRENCY="$REQUESTED_CONCURRENCY" \
  MSRP_EFFECTIVE_CONCURRENCY="$CONCURRENCY" \
  MSRP_MAX_DRYRUN_CONCURRENCY="$MAX_DRYRUN_CONCURRENCY" \
  MSRP_ALLOW_HIGH_CONCURRENCY="$ALLOW_HIGH_CONCURRENCY" \
  MSRP_STOP_ON_FAILURE="$STOP_ON_FAILURE" \
  MSRP_COUNTRY_TIMEOUT_SECONDS="$COUNTRY_TIMEOUT_SECONDS" \
  python3 -c '
import json
import os

def truthy(value):
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}

def to_int(value):
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0

payload = {
    "mode": os.environ.get("MSRP_MODE", ""),
    "countriesRaw": os.environ.get("MSRP_COUNTRIES_RAW", ""),
    "countries": [item for item in os.environ.get("MSRP_COUNTRIES", "").split() if item],
    "requestedConcurrency": to_int(os.environ.get("MSRP_REQUESTED_CONCURRENCY")),
    "effectiveConcurrency": to_int(os.environ.get("MSRP_EFFECTIVE_CONCURRENCY")),
    "maxDryrunConcurrency": to_int(os.environ.get("MSRP_MAX_DRYRUN_CONCURRENCY")),
    "allowHighConcurrency": truthy(os.environ.get("MSRP_ALLOW_HIGH_CONCURRENCY")),
    "stopOnFailure": truthy(os.environ.get("MSRP_STOP_ON_FAILURE")),
    "countryTimeoutSeconds": to_int(os.environ.get("MSRP_COUNTRY_TIMEOUT_SECONDS")),
    "proxyConfigured": any(
        os.environ.get(name)
        for name in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY", "all_proxy", "ALL_PROXY")
    ),
}
print(json.dumps(payload, separators=(",", ":")))
'
)"

PIPELINE_ID="msrp_${MODE}"
_write_msrp_status "$PIPELINE_ID" "running" "run $RUN_ID started countries=${#COUNTRIES[@]} concurrency=$CONCURRENCY requested_concurrency=$REQUESTED_CONCURRENCY" "$MSRP_RUNTIME_METADATA"

total="${#COUNTRIES[@]}"
active=0
country_idx=0
failures=0
declare -a pids=()
declare -a pid_countries=()

wait_for_finished_country() {
  local idx
  local status_file
  while true; do
    for idx in "${!pid_countries[@]}"; do
      status_file="$COUNTRY_DONE_DIR/${pid_countries[$idx]}.status"
      if [[ -f "$status_file" ]]; then
        echo "$idx"
        return 0
      fi
    done
    sleep 1
  done
}

remove_country_pid_at() {
  local idx="$1"
  unset "pids[$idx]"
  unset "pid_countries[$idx]"
  if ((${#pids[@]} > 0)); then
    pids=("${pids[@]}")
    pid_countries=("${pid_countries[@]}")
  else
    pids=()
    pid_countries=()
  fi
}

while (( country_idx < total || active > 0 )); do
  while (( active < CONCURRENCY && country_idx < total )); do
    country="${COUNTRIES[$country_idx]}"
    # Phase 2: Country log goes into run dir (no glob that picks up historical)
    country_log="$RUN_DIR/${country}.log"
    country_artifact="$COUNTRY_ARTIFACT_DIR/${country}.json"
    country_status_file="$COUNTRY_DONE_DIR/${country}.status"
    rm -f "$country_status_file"
    echo "[RUN] $((country_idx + 1))/$total country=$country mode=$MODE (parallel slot $((active + 1))/$CONCURRENCY)"

    extra_args=()
    if [[ "$MODE" == "ingest" ]]; then
      if is_truthy "$AUTO_REVIEW"; then
        extra_args+=(--auto-review --decided-by "$AUTO_REVIEW_DECIDED_BY" --auto-review-limit "$AUTO_REVIEW_LIMIT" --auto-review-min-score "$AUTO_REVIEW_MIN_SCORE")
      fi
      if is_truthy "$AUTO_MATERIALIZE"; then
        extra_args+=(--materialize --materialize-limit "$MATERIALIZE_LIMIT")
      fi
    fi

    (
      trap 'status_rc=$?; printf "%s\n" "$status_rc" > "$country_status_file"' EXIT
      # Phase 3: Export child-run context so batch_dryrun knows not to write global status
      export JATO_MSRP_RUN_ID="$RUN_ID"
      export JATO_MSRP_RUN_DIR="$RUN_DIR"
      export JATO_MSRP_CHILD_RUN="true"

      cmd=("$PYTHON_BIN" "$TARGET_SCRIPT" "$country")
      if [[ "${#extra_args[@]}" -gt 0 ]]; then
        cmd+=("${extra_args[@]}")
      fi
      run_cmd=("${cmd[@]}")
      if [[ "$COUNTRY_TIMEOUT_SECONDS" -gt 0 && -n "$TIMEOUT_BIN" ]]; then
        run_cmd=("$TIMEOUT_BIN" "${COUNTRY_TIMEOUT_SECONDS}s" "${cmd[@]}")
      fi
      if "${run_cmd[@]}"; then
        echo "[OK] country=$country"
        exit 0
      else
        rc=$?
        if [[ "$rc" -eq 124 ]]; then
          echo "[TIMEOUT] country=$country exceeded ${COUNTRY_TIMEOUT_SECONDS}s"
        fi
        echo "[FAIL] country=$country"
        exit "$rc"
      fi
    ) > "$country_log" 2>&1 &

    pids+=($!)
    pid_countries+=("$country")
    active=$((active + 1))
    country_idx=$((country_idx + 1))
  done

  if (( active > 0 )); then
    finished_index="$(wait_for_finished_country)"
    pid="${pids[$finished_index]}"
    finished_country="${pid_countries[$finished_index]:-?}"
    country_status_file="$COUNTRY_DONE_DIR/${finished_country}.status"
    rc="$(cat "$country_status_file" 2>/dev/null || echo 1)"
    wait "$pid" 2>/dev/null || true
    rm -f "$country_status_file"
    echo "[DONE] country=$finished_country rc=$rc"
    if [[ $rc -ne 0 ]]; then
      failures=$((failures + 1))
      if is_truthy "$STOP_ON_FAILURE"; then
        echo "[INFO] Stopping because JATO_MSRP_STOP_ON_FAILURE=true"
        for idx in "${!pids[@]}"; do
          if [[ "$idx" == "$finished_index" ]]; then
            continue
          fi
          rpid="${pids[$idx]}"
          kill "$rpid" 2>/dev/null || true
        done
        wait 2>/dev/null || true
        exit 1
      fi
    fi
    remove_country_pid_at "$finished_index"
    active=$((active - 1))
  fi
done

# Phase 2: Collect per-country logs from run dir only (not historical)
for country in "${COUNTRIES[@]}"; do
  clog="$RUN_DIR/${country}.log"
  if [[ -f "$clog" ]]; then
    cat "$clog" >> "$LOG_FILE"
  fi
done

# Phase 4+5: Run aggregator to produce v3 report + write global status
echo "[INFO] Running aggregator..."
AGGR_SCRIPT="$SCRIPT_DIR/msrp_dryrun_aggregate.py"
if [[ -f "$AGGR_SCRIPT" ]] && [[ "$MODE" == "dryrun" ]]; then
  if "$PYTHON_BIN" "$AGGR_SCRIPT" \
    --run-dir "$RUN_DIR" \
    --expected-countries "${COUNTRIES[*]}" \
    --out-latest "$REPO_DIR/03_Scripts/diagnostics/artifacts/dryrun_report.json" 2>&1; then
    echo "[INFO] Aggregation complete"
    if is_truthy "$REFRESH_SOURCE_GOVERNANCE"; then
      echo "[INFO] Refreshing MSRP source governance artifacts..."
      if [[ -f "$SOURCE_REPAIR_BACKLOG_SCRIPT" ]]; then
        if "$PYTHON_BIN" "$SOURCE_REPAIR_BACKLOG_SCRIPT" \
          --dryrun-artifact "$DRYRUN_REPORT" \
          --out-dir "$MSRP_ARTIFACT_DIR" 2>&1; then
          echo "[INFO] MSRP source repair backlog refreshed"
        else
          echo "[WARN] MSRP source repair backlog refresh failed (non-fatal)"
        fi
      else
        echo "[WARN] MSRP source repair backlog script not found: $SOURCE_REPAIR_BACKLOG_SCRIPT"
      fi

      if is_truthy "$REFRESH_SOURCE_REFERENCE_EVIDENCE"; then
        if [[ -f "$SOURCE_REFERENCE_EVIDENCE_SCRIPT" && -f "$SOURCE_REPAIR_BACKLOG_ARTIFACT" ]]; then
          if "$PYTHON_BIN" "$SOURCE_REFERENCE_EVIDENCE_SCRIPT" \
            --backlog "$SOURCE_REPAIR_BACKLOG_ARTIFACT" \
            --out-dir "$MSRP_ARTIFACT_DIR" \
            --page-size "$SOURCE_REFERENCE_PAGE_SIZE" \
            --max-pages "$SOURCE_REFERENCE_MAX_PAGES" 2>&1; then
            echo "[INFO] MSRP source reference evidence refreshed"
          else
            echo "[WARN] MSRP source reference evidence refresh failed (non-fatal)"
          fi
        else
          echo "[WARN] MSRP source reference evidence prerequisites missing"
        fi
      else
        echo "[INFO] MSRP source reference evidence skipped"
      fi

      if [[ -f "$SOURCE_REVIEW_QUEUE_SCRIPT" && -f "$SOURCE_REPAIR_BACKLOG_ARTIFACT" ]]; then
        if "$PYTHON_BIN" "$SOURCE_REVIEW_QUEUE_SCRIPT" \
          --backlog "$SOURCE_REPAIR_BACKLOG_ARTIFACT" \
          --reference "$SOURCE_REFERENCE_EVIDENCE_ARTIFACT" \
          --out-dir "$MSRP_ARTIFACT_DIR" 2>&1; then
          echo "[INFO] MSRP source review queue refreshed"
        else
          echo "[WARN] MSRP source review queue refresh failed (non-fatal)"
        fi
      else
        echo "[WARN] MSRP source review queue prerequisites missing"
      fi
    else
      echo "[INFO] MSRP source governance artifacts skipped"
    fi
    HERMES_MSRP_SCRIPT="$SCRIPT_DIR/hermes/hermes_msrp_country_progress.py"
    if [[ -f "$HERMES_MSRP_SCRIPT" ]]; then
      if "$PYTHON_BIN" "$HERMES_MSRP_SCRIPT" --out-dir "$REPO_DIR/hermes/reports" 2>&1; then
        echo "[INFO] Hermes MSRP country progress refreshed"
      else
        echo "[WARN] Hermes MSRP country progress refresh failed (non-fatal)"
      fi
    fi
  else
    echo "[WARN] Aggregation failed (non-fatal)"
  fi
fi

# Phase 5: Write global status once (only if aggregator already ran)
if [[ "$MODE" == "dryrun" ]]; then
  AGGR_REPORT="$REPO_DIR/03_Scripts/diagnostics/artifacts/dryrun_report.json"
  if [[ -f "$AGGR_REPORT" ]]; then
    MSRP_RUNTIME_METADATA_JSON="$MSRP_RUNTIME_METADATA" python3 -c "
import json
import os
from datetime import datetime, timezone
r = json.load(open('$AGGR_REPORT'))
s = r.get('summary', {})
runtime_metadata = json.loads(os.environ.get('MSRP_RUNTIME_METADATA_JSON') or '{}')
status_path = '$REPO_DIR/03_Scripts/logs/scheduled_fetch_status.json'
existing = json.load(open(status_path)) if __import__('os').path.exists(status_path) else {}
existing['msrp_dryrun'] = {
    'lastRunAt': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    'status': s.get('status', 'unknown'),
    'runId': r.get('runId', ''),
    'artifactPath': '03_Scripts/diagnostics/artifacts/dryrun_report.json',
    'historyIndexPath': '03_Scripts/diagnostics/artifacts/dryrun_runs_index.json',
    'runArtifactPath': f\"03_Scripts/diagnostics/artifacts/dryrun_report_{r.get('runId', '')}.json\" if r.get('runId') else '',
    'schemaVersion': 'msrp_dryrun_report_v3',
    'runtimeMetadata': runtime_metadata,
}
with open(status_path, 'w') as f:
    f.write(json.dumps(existing, indent=2) + chr(10))
status_dir = '$REPO_DIR/hermes/reports/pipeline_status'
__import__('os').makedirs(status_dir, exist_ok=True)
status_record = {
    'pipelineId': 'msrp_dryrun',
    'status': s.get('status', 'unknown'),
    'lastRunAt': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    'finishedAt': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    'exitCode': 0,
    'durationSeconds': 0,
    'recordsProcessed': s.get('total', 0),
    'failedCount': (s.get('total', 0) or 0) - (s.get('pass', 0) or 0),
    'warningCount': len(r.get('missingCountries', [])),
    'artifactRefs': [
        '03_Scripts/diagnostics/artifacts/dryrun_report.json',
        '03_Scripts/diagnostics/artifacts/dryrun_runs_index.json',
        '03_Scripts/diagnostics/artifacts/msrp_source_repair_backlog.json',
        '03_Scripts/diagnostics/artifacts/msrp_source_repair_backlog.md',
        '03_Scripts/diagnostics/artifacts/msrp_source_reference_evidence.json',
        '03_Scripts/diagnostics/artifacts/msrp_source_reference_evidence.md',
        '03_Scripts/diagnostics/artifacts/msrp_source_review_queue.json',
        '03_Scripts/diagnostics/artifacts/msrp_source_review_queue.md',
        '03_Scripts/diagnostics/artifacts/msrp_price_alert_review_queue.json',
        '03_Scripts/diagnostics/artifacts/msrp_price_alert_review_queue.md',
        'hermes/reports/msrp_country_progress.json',
        'hermes/reports/msrp_country_progress.md',
    ],
    'source': '03_Scripts/run_msrp_low_concurrency.sh',
    'message': 'aggregated dryrun report',
    'metadata': runtime_metadata,
    'runId': r.get('runId', ''),
    'countryCount': len(r.get('expectedCountries', [])),
    'observedCountryCount': len(r.get('observedCountries', [])),
    'missingCountryCount': len(r.get('missingCountries', [])),
}
with open(status_dir + '/msrp_dryrun.json', 'w') as f:
    f.write(json.dumps(status_record, indent=2) + chr(10))
print('[status] msrp_dryrun written')
" 2>&1 || true
  fi
fi

CURRENT_SNAPSHOT_SCRIPT="$SCRIPT_DIR/hermes/hermes_msrp_current_price_snapshot.py"
if is_truthy "$REFRESH_CURRENT_SNAPSHOT" && [[ -f "$CURRENT_SNAPSHOT_SCRIPT" ]]; then
  echo "[INFO] Refreshing Hermes MSRP current price snapshot..."
  if "$PYTHON_BIN" "$CURRENT_SNAPSHOT_SCRIPT" \
    --api-base "$JATO_API_BASE" \
    --out-dir "$REPO_DIR/hermes/reports" \
    --limit "$CURRENT_SNAPSHOT_LIMIT" \
    --threshold-pct "$CURRENT_SNAPSHOT_THRESHOLD_PCT" \
    --timeout-seconds "$CURRENT_SNAPSHOT_TIMEOUT_SECONDS" 2>&1; then
    echo "[INFO] Hermes MSRP current price snapshot refreshed"
  else
    echo "[WARN] Hermes MSRP current price snapshot refresh failed (non-fatal)"
  fi
fi

PRICE_ALERT_REVIEW_QUEUE_SCRIPT="$SCRIPT_DIR/msrp_price_alert_review_queue.py"
CURRENT_SNAPSHOT_ARTIFACT="$REPO_DIR/hermes/reports/msrp_current_price_snapshot.json"
if is_truthy "$REFRESH_PRICE_ALERT_REVIEW_QUEUE" && [[ -f "$PRICE_ALERT_REVIEW_QUEUE_SCRIPT" ]]; then
  if [[ -f "$CURRENT_SNAPSHOT_ARTIFACT" ]]; then
    echo "[INFO] Refreshing MSRP price alert review queue..."
    if "$PYTHON_BIN" "$PRICE_ALERT_REVIEW_QUEUE_SCRIPT" \
      --snapshot "$CURRENT_SNAPSHOT_ARTIFACT" \
      --out-dir "$MSRP_ARTIFACT_DIR" 2>&1; then
      echo "[INFO] MSRP price alert review queue refreshed"
    else
      echo "[WARN] MSRP price alert review queue refresh failed (non-fatal)"
    fi
  else
    echo "[WARN] MSRP price alert review queue skipped; current snapshot artifact missing"
  fi
fi

READINESS_AUDIT_SCRIPT="$SCRIPT_DIR/diagnostics/msrp_readiness_audit.py"
if is_truthy "$REFRESH_READINESS_AUDIT" && [[ -f "$READINESS_AUDIT_SCRIPT" ]]; then
  echo "[INFO] Refreshing MSRP readiness audit..."
  if "$PYTHON_BIN" "$READINESS_AUDIT_SCRIPT" \
    --api-base "$JATO_API_BASE" \
    --out-dir "$REPO_DIR/hermes/reports" \
    --write-status \
    --timeout-seconds "$READINESS_AUDIT_TIMEOUT_SECONDS" 2>&1; then
    echo "[INFO] MSRP readiness audit refreshed"
  else
    echo "[WARN] MSRP readiness audit refresh failed (non-fatal)"
  fi
fi

if (( failures > 0 )); then
  echo "[WARN] Completed with $failures failed country runs"
  exit 1
fi

echo "[INFO] Completed successfully (run dir: $RUN_DIR)"
