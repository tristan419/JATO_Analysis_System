#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="${REPO_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
RUNNER="${JATO_MSRP_RUNNER:-$SCRIPT_DIR/run_msrp_low_concurrency.sh}"
CONFIG_SOURCE_SYNC="${JATO_CONFIG_SOURCE_SYNC:-$SCRIPT_DIR/engineering_config_source_sync.py}"
UNIFIED_READINESS_AUDIT="${JATO_UNIFIED_READINESS_AUDIT:-$SCRIPT_DIR/diagnostics/unified_scraping_readiness_audit.py}"
GOAL_COMPLETION_AUDIT="${JATO_GOAL_COMPLETION_AUDIT:-$SCRIPT_DIR/diagnostics/goal_completion_audit.py}"
PYTHON_BIN="${JATO_MSRP_PYTHON:-$REPO_DIR/.venv/bin/python}"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="$(command -v python3 || true)"
fi

COUNTRIES_RAW="${JATO_MSRP_PIPELINE_COUNTRIES:-${JATO_MSRP_COUNTRIES:-batch_a}}"
CONFIG_SOURCE_SYNC_COUNTRIES="${JATO_CONFIG_SOURCE_SYNC_COUNTRIES:-se,fi,no,dk}"
CONFIG_SOURCE_SYNC_SAMPLE_PER_COUNTRY="${JATO_CONFIG_SOURCE_SYNC_SAMPLE_PER_COUNTRY:-1}"
UNIFIED_READINESS_SAMPLE_PER_KIND="${JATO_UNIFIED_READINESS_SAMPLE_PER_KIND:-1}"
UNIFIED_READINESS_ARTIFACT_ROOT="${JATO_UNIFIED_READINESS_ARTIFACT_ROOT:-$REPO_DIR/03_Scripts/diagnostics/artifacts/unified_scraping_stage_smoke}"
MIN_DRYRUN_PASS_PCT="${JATO_MSRP_MIN_DRYRUN_PASS_PCT:-70}"
DRYRUN_REPORT="$REPO_DIR/03_Scripts/diagnostics/artifacts/dryrun_report.json"
CONFIG_SOURCE_SYNC_STATUS="$REPO_DIR/hermes/reports/pipeline_status/engineering_config_source_sync.json"
UNIFIED_READINESS_STATUS="$REPO_DIR/hermes/reports/pipeline_status/unified_scraping_readiness.json"
GOAL_COMPLETION_STATUS="$REPO_DIR/hermes/reports/pipeline_status/goal_completion_audit.json"
STARTED_AT="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

is_truthy() {
  case "$1" in
    1|true|TRUE|True|yes|YES|Yes|on|ON|On) return 0 ;;
    *) return 1 ;;
  esac
}

write_pipeline_status() {
  local status="$1"
  local message="$2"
  local exit_code="$3"
  local phase="$4"
  MSRP_PIPELINE_STATUS="$status" \
  MSRP_PIPELINE_MESSAGE="$message" \
  MSRP_PIPELINE_EXIT_CODE="$exit_code" \
  MSRP_PIPELINE_PHASE="$phase" \
  MSRP_PIPELINE_STARTED_AT="$STARTED_AT" \
  MSRP_PIPELINE_COUNTRIES_RAW="$COUNTRIES_RAW" \
  MSRP_PIPELINE_DRYRUN_REPORT="$DRYRUN_REPORT" \
  MSRP_PIPELINE_REPO_DIR="$REPO_DIR" \
  "$PYTHON_BIN" -c '
import json
import os
from datetime import datetime, timezone
from pathlib import Path

repo = Path(os.environ["MSRP_PIPELINE_REPO_DIR"])
status = os.environ["MSRP_PIPELINE_STATUS"]
message = os.environ["MSRP_PIPELINE_MESSAGE"]
exit_code = int(os.environ["MSRP_PIPELINE_EXIT_CODE"])
phase = os.environ["MSRP_PIPELINE_PHASE"]
started_at = os.environ["MSRP_PIPELINE_STARTED_AT"]
finished_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
countries_raw = os.environ["MSRP_PIPELINE_COUNTRIES_RAW"]
dryrun_report = Path(os.environ["MSRP_PIPELINE_DRYRUN_REPORT"])
artifact_refs = [
    "03_Scripts/diagnostics/artifacts/dryrun_report.json",
    "03_Scripts/diagnostics/artifacts/dryrun_runs_index.json",
    "03_Scripts/logs/scheduled_fetch_status.json",
    "hermes/reports/engineering_config_source_sync.json",
    "hermes/reports/engineering_config_source_sync.md",
    "hermes/reports/pipeline_status/engineering_config_source_sync.json",
    "hermes/reports/msrp_current_price_snapshot.json",
    "hermes/reports/msrp_readiness_audit.json",
    "hermes/reports/pipeline_status/msrp_readiness_audit.json",
    "hermes/reports/unified_scraping_readiness.json",
    "hermes/reports/unified_scraping_readiness.md",
    "hermes/reports/pipeline_status/unified_scraping_readiness.json",
    "hermes/reports/goal_completion_audit.json",
    "hermes/reports/goal_completion_audit.md",
    "hermes/reports/pipeline_status/goal_completion_audit.json",
]

status_path = repo / "03_Scripts" / "logs" / "scheduled_fetch_status.json"
status_path.parent.mkdir(parents=True, exist_ok=True)
try:
    existing = json.loads(status_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    existing = {}
existing["msrp_pipeline"] = {
    "lastRunAt": finished_at,
    "status": status,
    "phase": phase,
    "reason": message,
    "countriesRaw": countries_raw,
    "dryrunReportPath": "03_Scripts/diagnostics/artifacts/dryrun_report.json",
}
status_path.write_text(json.dumps(existing, indent=2) + "\n", encoding="utf-8")

report_payload = {}
try:
    report_payload = json.loads(dryrun_report.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    report_payload = {}
summary = report_payload.get("summary") if isinstance(report_payload.get("summary"), dict) else {}
config_status_path = repo / "hermes" / "reports" / "pipeline_status" / "engineering_config_source_sync.json"
unified_status_path = repo / "hermes" / "reports" / "pipeline_status" / "unified_scraping_readiness.json"
goal_status_path = repo / "hermes" / "reports" / "pipeline_status" / "goal_completion_audit.json"

def _read_json(path):
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}

config_payload = _read_json(config_status_path)
unified_payload = _read_json(unified_status_path)
goal_payload = _read_json(goal_status_path)
pipeline_status = "failed" if status in {"failed", "failure", "error"} else status
record = {
    "pipelineId": "msrp_pipeline",
    "status": pipeline_status,
    "lastRunAt": finished_at,
    "startedAt": started_at,
    "finishedAt": finished_at,
    "exitCode": exit_code,
    "durationSeconds": 0,
    "recordsProcessed": int(summary.get("total") or 0),
    "failedCount": int((summary.get("total") or 0) - (summary.get("pass") or 0)),
    "warningCount": len(report_payload.get("missingCountries") or []),
    "artifactRefs": artifact_refs,
    "source": "03_Scripts/run_msrp_pipeline.sh",
    "message": message,
    "metadata": {
        "phase": phase,
        "countriesRaw": countries_raw,
        "dryrunRunId": report_payload.get("runId"),
        "dryrunGateStatus": summary.get("gateStatus"),
        "dryrunPassPct": summary.get("passPct"),
        "dryrunGateThreshold": summary.get("gateThreshold"),
        "configSourceSyncStatus": config_payload.get("status"),
        "configSourceSyncPipelineId": config_payload.get("pipelineId"),
        "unifiedReadinessStatus": unified_payload.get("status"),
        "unifiedReadinessPipelineId": unified_payload.get("pipelineId"),
        "unifiedReadinessReadinessStatus": unified_payload.get("readinessStatus"),
        "goalCompletionStatus": goal_payload.get("status"),
        "goalCompletionPipelineId": goal_payload.get("pipelineId"),
        "goalCompletionState": goal_payload.get("goalCompletionStatus"),
        "goalLocalP0Ready": goal_payload.get("localP0Ready"),
    },
}
status_dir = repo / "hermes" / "reports" / "pipeline_status"
status_dir.mkdir(parents=True, exist_ok=True)
(status_dir / "msrp_pipeline.json").write_text(
    json.dumps(record, ensure_ascii=False, indent=2) + "\n",
    encoding="utf-8",
)
'
}

read_dryrun_gate() {
  "$PYTHON_BIN" - "$DRYRUN_REPORT" "$MIN_DRYRUN_PASS_PCT" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
default_threshold = int(float(sys.argv[2]))
payload = json.loads(path.read_text(encoding="utf-8"))
summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
threshold = int(float(summary.get("gateThreshold") or default_threshold))
pass_pct = float(summary.get("passPct") or 0)
gate_status = str(summary.get("gateStatus") or "").strip().lower()
if not gate_status:
    gate_status = "allowed" if pass_pct >= threshold else "blocked"
print("|".join([
    gate_status,
    f"{pass_pct:.1f}",
    str(threshold),
    str(payload.get("runId") or ""),
]))
PY
}

run_config_source_sync() {
  if is_truthy "${JATO_MSRP_PIPELINE_SKIP_CONFIG_SYNC:-false}"; then
    echo "[PIPELINE] Phase 1/4: official config source sync skipped by JATO_MSRP_PIPELINE_SKIP_CONFIG_SYNC"
    return 0
  fi
  if [[ ! -f "$CONFIG_SOURCE_SYNC" ]]; then
    echo "[ERROR] Config source sync script not found: $CONFIG_SOURCE_SYNC" >&2
    return 1
  fi

  local -a stage_args=()
  if is_truthy "${JATO_CONFIG_SOURCE_SYNC_STAGE_SMOKE:-true}"; then
    stage_args+=(
      --artifact-root
      "$REPO_DIR/03_Scripts/diagnostics/artifacts/engineering_config_source_sync_stage"
    )
  else
    stage_args+=(--no-stage-smoke)
  fi

  echo "[PIPELINE] Phase 1/4: official config source sync"
  "$PYTHON_BIN" "$CONFIG_SOURCE_SYNC" \
    --repo-root "$REPO_DIR" \
    --required-countries "$CONFIG_SOURCE_SYNC_COUNTRIES" \
    --sample-per-country "$CONFIG_SOURCE_SYNC_SAMPLE_PER_COUNTRY" \
    --out-dir "$REPO_DIR/hermes/reports" \
    --write-status \
    --strict \
    "${stage_args[@]}" >/dev/null
}

run_post_pipeline_audits() {
  if ! is_truthy "${JATO_MSRP_PIPELINE_RUN_POST_AUDITS:-true}"; then
    echo "[PIPELINE] Phase 4/4: post-ingest audits skipped by JATO_MSRP_PIPELINE_RUN_POST_AUDITS"
    return 0
  fi

  echo "[PIPELINE] Phase 4/4: readiness and goal audits"
  if is_truthy "${JATO_MSRP_PIPELINE_REFRESH_UNIFIED_READINESS:-true}"; then
    if [[ ! -f "$UNIFIED_READINESS_AUDIT" ]]; then
      echo "[ERROR] Unified scraping readiness audit not found: $UNIFIED_READINESS_AUDIT" >&2
      return 1
    fi
    if ! "$PYTHON_BIN" "$UNIFIED_READINESS_AUDIT" \
      --repo-root "$REPO_DIR" \
      --artifact-root "$UNIFIED_READINESS_ARTIFACT_ROOT" \
      --out-dir "$REPO_DIR/hermes/reports" \
      --write-status \
      --sample-per-kind "$UNIFIED_READINESS_SAMPLE_PER_KIND" \
      --strict >/dev/null; then
      return 1
    fi
  else
    echo "[PIPELINE] Unified scraping readiness audit skipped by JATO_MSRP_PIPELINE_REFRESH_UNIFIED_READINESS"
  fi

  if is_truthy "${JATO_MSRP_PIPELINE_REFRESH_GOAL_AUDIT:-true}"; then
    if [[ ! -f "$GOAL_COMPLETION_AUDIT" ]]; then
      echo "[ERROR] Goal completion audit not found: $GOAL_COMPLETION_AUDIT" >&2
      return 1
    fi
    local -a goal_args=(
      --repo-root "$REPO_DIR"
      --out-dir "$REPO_DIR/hermes/reports"
      --write-status
    )
    if [[ -n "${JATO_GOAL_COMPLETION_REMOTE_API_BASE:-}" ]]; then
      goal_args+=(--remote-api-base "$JATO_GOAL_COMPLETION_REMOTE_API_BASE")
    fi
    if [[ -n "${JATO_GOAL_COMPLETION_REMOTE_RESOLVE_IP:-}" ]]; then
      goal_args+=(--remote-resolve-ip "$JATO_GOAL_COMPLETION_REMOTE_RESOLVE_IP")
    fi
    if [[ -n "${JATO_GOAL_COMPLETION_TIMEOUT_SECONDS:-}" ]]; then
      goal_args+=(--timeout-seconds "$JATO_GOAL_COMPLETION_TIMEOUT_SECONDS")
    fi
    if ! "$PYTHON_BIN" "$GOAL_COMPLETION_AUDIT" "${goal_args[@]}" >/dev/null; then
      return 1
    fi
  else
    echo "[PIPELINE] Goal completion audit skipped by JATO_MSRP_PIPELINE_REFRESH_GOAL_AUDIT"
  fi
}

if [[ -z "$PYTHON_BIN" || ! -x "$PYTHON_BIN" ]]; then
  echo "[ERROR] Python executable not found." >&2
  exit 1
fi
if [[ ! -x "$RUNNER" ]]; then
  echo "[ERROR] MSRP runner not executable: $RUNNER" >&2
  exit 1
fi

echo "[INFO] MSRP full pipeline"
echo "[INFO] Repo: $REPO_DIR"
echo "[INFO] Runner: $RUNNER"
echo "[INFO] Config source sync: $CONFIG_SOURCE_SYNC"
echo "[INFO] Unified readiness audit: $UNIFIED_READINESS_AUDIT"
echo "[INFO] Goal completion audit: $GOAL_COMPLETION_AUDIT"
echo "[INFO] Countries: $COUNTRIES_RAW"
echo "[INFO] Dryrun gate threshold: $MIN_DRYRUN_PASS_PCT%"

write_pipeline_status "running" "MSRP full pipeline started" 0 "started"

if ! run_config_source_sync; then
  write_pipeline_status "failed" "Official config source sync failed" 1 "config_source_sync"
  exit 1
fi

if ! is_truthy "${JATO_MSRP_PIPELINE_SKIP_DRYRUN:-false}"; then
  echo "[PIPELINE] Phase 2/4: dryrun"
  if ! JATO_MSRP_MODE=dryrun JATO_MSRP_COUNTRIES="$COUNTRIES_RAW" "$RUNNER"; then
    write_pipeline_status "failed" "Dryrun phase failed" 1 "dryrun"
    exit 1
  fi
else
  echo "[PIPELINE] Phase 2/4: dryrun skipped by JATO_MSRP_PIPELINE_SKIP_DRYRUN"
fi

if [[ ! -f "$DRYRUN_REPORT" ]]; then
  write_pipeline_status "failed" "Dryrun report missing after dryrun phase" 1 "gate"
  echo "[ERROR] Dryrun report missing: $DRYRUN_REPORT" >&2
  exit 1
fi

IFS='|' read -r gate_status pass_pct gate_threshold dryrun_run_id < <(read_dryrun_gate)
echo "[PIPELINE] Gate:"
echo "  status=${gate_status:-unknown}"
echo "  passPct=${pass_pct:-0}%"
echo "  threshold=${gate_threshold:-$MIN_DRYRUN_PASS_PCT}%"
echo "  run=${dryrun_run_id:-unknown}"

if [[ "$gate_status" != "allowed" ]]; then
  write_pipeline_status "skipped" "Ingest skipped by dryrun gate" 0 "gate"
  echo "[PIPELINE] Ingest skipped by dryrun gate."
  exit 0
fi

if is_truthy "${JATO_MSRP_PIPELINE_SKIP_INGEST:-false}"; then
  write_pipeline_status "skipped" "Ingest skipped by JATO_MSRP_PIPELINE_SKIP_INGEST" 0 "ingest"
  echo "[PIPELINE] Ingest skipped by JATO_MSRP_PIPELINE_SKIP_INGEST."
  exit 0
fi

echo "[PIPELINE] Phase 3/4: ingest"
if ! JATO_MSRP_MODE=ingest JATO_MSRP_COUNTRIES="$COUNTRIES_RAW" "$RUNNER"; then
  write_pipeline_status "failed" "Ingest phase failed" 1 "ingest"
  exit 1
fi

if ! run_post_pipeline_audits; then
  write_pipeline_status "failed" "Post-ingest readiness audit failed" 1 "post_audit"
  exit 1
fi

write_pipeline_status "success" "MSRP full pipeline completed" 0 "completed"
echo "[INFO] MSRP full pipeline completed"
