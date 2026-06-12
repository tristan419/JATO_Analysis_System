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
  python3 "$REPO_DIR/03_Scripts/hermes/pipeline_status_writer.py" "$pipeline" \
    --status "$status" \
    --source "03_Scripts/run_msrp_low_concurrency.sh" \
    --message "$reason" \
    --artifact-ref "03_Scripts/logs/scheduled_fetch_status.json" \
    --repo-root "$REPO_DIR" 2>/dev/null || true
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
MATERIALIZE_LIMIT="${JATO_MSRP_MATERIALIZE_LIMIT:-500}"
AUTO_REVIEW_DECIDED_BY="${JATO_AUTO_REVIEW_DECIDED_BY:-${APP_USER_NAME:-msrp-cron}}"
REFRESH_CURRENT_SNAPSHOT="${JATO_MSRP_REFRESH_CURRENT_SNAPSHOT:-true}"
CURRENT_SNAPSHOT_LIMIT="${JATO_MSRP_CURRENT_SNAPSHOT_LIMIT:-500}"
CURRENT_SNAPSHOT_THRESHOLD_PCT="${JATO_MSRP_PRICE_ALERT_THRESHOLD_PCT:-3}"
CURRENT_SNAPSHOT_TIMEOUT_SECONDS="${JATO_MSRP_CURRENT_SNAPSHOT_TIMEOUT_SECONDS:-30}"
REFRESH_READINESS_AUDIT="${JATO_MSRP_REFRESH_READINESS_AUDIT:-true}"
READINESS_AUDIT_TIMEOUT_SECONDS="${JATO_MSRP_READINESS_AUDIT_TIMEOUT_SECONDS:-30}"
export NVAPI_KEY="${NVAPI_KEY:-${NVIDIA_API_KEY:-}}"

# ── Dryrun-to-ingest gate ──────────────────────────────────────────
MIN_DRYRUN_PASS_PCT="${JATO_MSRP_MIN_DRYRUN_PASS_PCT:-70}"
DRYRUN_REPORT="$REPO_DIR/03_Scripts/diagnostics/artifacts/dryrun_report.json"

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
mkdir -p "$RUN_DIR" "$COUNTRY_ARTIFACT_DIR"

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

CONCURRENCY="${JATO_MSRP_CONCURRENCY:-2}"

echo "[INFO] MSRP low-concurrency runner"
echo "[INFO] Repo: $REPO_DIR"
echo "[INFO] Mode: $MODE"
echo "[INFO] Countries: ${COUNTRIES[*]}"
echo "[INFO] Concurrency: $CONCURRENCY"
echo "[INFO] Backend env: $BACKEND_ENV_FILE"
echo "[INFO] MSRP env: $MSRP_ENV_FILE"
echo "[INFO] API base: $JATO_API_BASE"
echo "[INFO] Log file: $LOG_FILE"
echo "[INFO] Auto review: $AUTO_REVIEW"
echo "[INFO] Auto materialize: $AUTO_MATERIALIZE"
echo "[INFO] Refresh current price snapshot: $REFRESH_CURRENT_SNAPSHOT"
echo "[INFO] Refresh MSRP readiness audit: $REFRESH_READINESS_AUDIT"

total="${#COUNTRIES[@]}"
active=0
country_idx=0
failures=0
declare -a pids=()
declare -a pid_countries=()

while (( country_idx < total || active > 0 )); do
  while (( active < CONCURRENCY && country_idx < total )); do
    country="${COUNTRIES[$country_idx]}"
    # Phase 2: Country log goes into run dir (no glob that picks up historical)
    country_log="$RUN_DIR/${country}.log"
    country_artifact="$COUNTRY_ARTIFACT_DIR/${country}.json"
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
      # Phase 3: Export child-run context so batch_dryrun knows not to write global status
      export JATO_MSRP_RUN_ID="$RUN_ID"
      export JATO_MSRP_RUN_DIR="$RUN_DIR"
      export JATO_MSRP_CHILD_RUN="true"

      cmd=("$PYTHON_BIN" "$TARGET_SCRIPT" "$country")
      if [[ "${#extra_args[@]}" -gt 0 ]]; then
        cmd+=("${extra_args[@]}")
      fi
      if "${cmd[@]}"; then
        echo "[OK] country=$country"
        exit 0
      else
        echo "[FAIL] country=$country"
        exit 1
      fi
    ) > "$country_log" 2>&1 &

    pids+=($!)
    pid_countries+=("$country")
    active=$((active + 1))
    country_idx=$((country_idx + 1))
  done

  if (( active > 0 )); then
    pid="${pids[0]}"
    finished_country="${pid_countries[0]:-?}"
    rc=0
    wait "$pid" 2>/dev/null || rc=$?
    if [[ $rc -ne 0 ]]; then
      failures=$((failures + 1))
      if is_truthy "$STOP_ON_FAILURE"; then
        echo "[INFO] Stopping because JATO_MSRP_STOP_ON_FAILURE=true"
        for rpid in "${pids[@]:1}"; do
          kill "$rpid" 2>/dev/null || true
        done
        wait 2>/dev/null || true
        exit 1
      fi
    fi
    pids=("${pids[@]:1}")
    pid_countries=("${pid_countries[@]:1}")
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
    python3 -c "
import json
from datetime import datetime, timezone
r = json.load(open('$AGGR_REPORT'))
s = r.get('summary', {})
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
    'artifactRefs': ['03_Scripts/diagnostics/artifacts/dryrun_report.json'],
    'source': '03_Scripts/run_msrp_low_concurrency.sh',
    'message': 'aggregated dryrun report',
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
