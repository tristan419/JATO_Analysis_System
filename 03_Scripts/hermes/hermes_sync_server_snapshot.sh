#!/usr/bin/env bash
# Hermes Phase 6.5 — Server Artifact Sync
#
# Pull runtime reports and lightweight production snapshots from server
# to local for debugging. Safe: no secrets, no large raw data, no code overwrite.
#
# Usage:
#   bash 03_Scripts/hermes/hermes_sync_server_snapshot.sh
#   bash 03_Scripts/hermes/hermes_sync_server_snapshot.sh --host 1.2.3.4 --user ubuntu
#   bash 03_Scripts/hermes/hermes_sync_server_snapshot.sh --include-optional
#
# Direction: server → local (read-only pull)
set -euo pipefail

SSH_HOST="${SSH_HOST:-}"
SSH_USER="${SSH_USER:-ubuntu}"
SSH_PORT="${SSH_PORT:-22}"
SSH_KEY="${SSH_KEY:-}"
SSH_ALIAS="${SSH_ALIAS:-}"
REMOTE_ROOT="${REMOTE_ROOT:-/opt/JATO_Analysis_System-main}"
LOCAL_ROOT="${LOCAL_ROOT:-.hermes_server_snapshot}"
INCLUDE_OPTIONAL="${INCLUDE_OPTIONAL:-false}"

usage() {
  cat <<'EOF'
Usage: hermes_sync_server_snapshot.sh [options]

Options:
  --host HOST          SSH host (default: $SSH_HOST)
  --user USER          SSH user (default: ubuntu)
  --port PORT          SSH port (default: 22)
  --remote-root DIR    Server repo root (default: /opt/JATO_Analysis_System-main)
  --local-root DIR     Local snapshot dir (default: .hermes_server_snapshot)
  --include-optional   Also sync lightweight deck/summary artifacts
  --help               Show this message
EOF
  exit 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host) SSH_HOST="$2"; shift 2 ;;
    --user) SSH_USER="$2"; shift 2 ;;
    --port) SSH_PORT="$2"; shift 2 ;;
    --remote-root) REMOTE_ROOT="$2"; shift 2 ;;
    --local-root) LOCAL_ROOT="$2"; shift 2 ;;
    --include-optional) INCLUDE_OPTIONAL=true; shift ;;
    --help) usage ;;
    *) echo "Unknown: $1"; usage ;;
  esac
done

if [[ -z "$SSH_HOST" && -z "$SSH_ALIAS" ]]; then
  echo "[ERROR] SSH_HOST or SSH_ALIAS is required."
  exit 1
fi

SSH_CMD="ssh -o ConnectTimeout=10"
SSH_SCP="scp -q"
if [[ -n "$SSH_KEY" ]]; then
  SSH_CMD="$SSH_CMD -i $SSH_KEY"
  SSH_SCP="$SSH_SCP -i $SSH_KEY"
fi
if [[ -n "$SSH_ALIAS" ]]; then
  SSH_TARGET="$SSH_ALIAS"
  SSH_CMD="$SSH_CMD -p 22"
else
  SSH_TARGET="$SSH_TARGET"
  SSH_CMD="$SSH_CMD -p $SSH_PORT"
  SSH_SCP="$SSH_SCP -P $SSH_PORT"
fi

echo "══════════════════════════════════════════════"
echo " Hermes Server Artifact Sync"
echo "══════════════════════════════════════════════"
echo " Server:  $SSH_TARGET:$SSH_PORT"
echo " Remote:  $REMOTE_ROOT"
echo " Local:   $LOCAL_ROOT"
echo " Optional: $INCLUDE_OPTIONAL"
echo ""

# ── Test SSH ────────────────────────────────────────────────────
echo "[1/5] Testing SSH connection..."
if ! $SSH_CMD "$SSH_TARGET" "hostname" >/dev/null 2>&1; then
  echo "[ERROR] SSH connection failed to $SSH_TARGET:$SSH_PORT"
  exit 1
fi
echo "       Connected."

# ── Create local dir ────────────────────────────────────────────
mkdir -p "$LOCAL_ROOT"/{reports,logs,artifacts}

# ── Core: Hermes reports (always sync) ──────────────────────────
echo "[2/5] Syncing Hermes reports..."
CORE_PATHS=(
  "hermes/reports/pipeline_health.json"
  "hermes/reports/hermes_pipeline_audit_report.md"
  "hermes/reports/source_quality_report.json"
  "hermes/reports/source_quality_report.md"
  "hermes/reports/cost_report.json"
  "hermes/reports/cost_report.md"
  "hermes/reports/hermes_code_audit_report.json"
  "hermes/reports/hermes_code_audit_report.md"
  "hermes/reports/hermes_intake_report.json"
  "hermes/reports/hermes_intake_report.md"
  "hermes/reports/answer_audit_report.md"
  "hermes/reports/evidence_ledger_report.md"
  "hermes/reports/pipeline_health.json"
  "hermes/evidence_ledger.jsonl"
  "hermes/answer_audit.jsonl"
  "03_Scripts/logs/scheduled_fetch_status.json"
)

for path in "${CORE_PATHS[@]}"; do
  remote="$REMOTE_ROOT/$path"
  local_dir="$LOCAL_ROOT/$(dirname "$path")"
  mkdir -p "$local_dir"
  $SSH_CMD "$SSH_TARGET" "test -f $remote" 2>/dev/null && {
    scp -P "$SSH_PORT" -q "$SSH_TARGET:$remote" "$local_dir/" && echo "       $path"
  } || true
done

# ── Core: Hermes registry (always sync) ─────────────────────────
echo "[3/5] Syncing Hermes registries..."
for f in source_registry pipeline_registry feature_registry prompt_registry artifact_registry governance_gaps proposal_registry model_pricing; do
  remote="$REMOTE_ROOT/hermes/${f}.yaml"
  local_dir="$LOCAL_ROOT/hermes"
  mkdir -p "$local_dir"
  $SSH_CMD "$SSH_TARGET" "test -f $remote" 2>/dev/null && {
    scp -P "$SSH_PORT" -q "$SSH_TARGET:$remote" "$local_dir/" && echo "       hermes/${f}.yaml"
  } || true
done

# ── Optional: lightweight artifacts ──────────────────────────────
if $INCLUDE_OPTIONAL; then
  echo "[4/5] Syncing optional lightweight artifacts..."
  OPT_PATTERNS=(
    "04_Processed_data/voc/*/deck/*.json"
    "04_Processed_data/voc/*/enriched/*.json"
  )
  for pattern in "${OPT_PATTERNS[@]}"; do
    remote="$REMOTE_ROOT/$pattern"
    local_dir="$LOCAL_ROOT/$(dirname "$pattern")"
    mkdir -p "$local_dir"
    $SSH_CMD "$SSH_TARGET" "ls $remote" 2>/dev/null | while read -r f; do
      scp -P "$SSH_PORT" -q "$SSH_TARGET:$f" "$local_dir/" 2>/dev/null || true
    done
  done
else
  echo "[4/5] Skipping optional artifacts (use --include-optional to sync)"
fi

# ── Generate summary ─────────────────────────────────────────────
echo "[5/5] Generating snapshot summary..."
python3 "$(dirname "$0")/hermes_sync_server_snapshot.py" \
  --snapshot-dir "$LOCAL_ROOT" \
  --out "$LOCAL_ROOT/SNAPSHOT_SUMMARY.md" \
  --json-out "$LOCAL_ROOT/SNAPSHOT_SUMMARY.json" 2>/dev/null || {
  echo "       Summary generator skipped (Python not available or snapshot empty)"
}

echo ""
echo "══════════════════════════════════════════════"
echo " Sync complete."
echo " Snapshot: $LOCAL_ROOT"
echo " Summary:  $LOCAL_ROOT/SNAPSHOT_SUMMARY.md"
echo "══════════════════════════════════════════════"
echo ""
echo " Next: cat $LOCAL_ROOT/SNAPSHOT_SUMMARY.md"
echo "       python3 03_Scripts/hermes/hermes_cost_report.py (with synced data)"
