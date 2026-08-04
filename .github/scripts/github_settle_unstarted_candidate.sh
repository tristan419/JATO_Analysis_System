#!/usr/bin/env bash
set -Eeuo pipefail

: "${AUTH_MODE:?AUTH_MODE is required}"
: "${SETTLEMENT_PLAN:?SETTLEMENT_PLAN is required}"
: "${SETTLEMENT_OUTPUT:?SETTLEMENT_OUTPUT is required}"
: "${SSH_HOST:?SSH_HOST is required}"
: "${SSH_USER:?SSH_USER is required}"

if [[ "$AUTH_MODE" != "key" && "$AUTH_MODE" != "password" ]]; then
  echo "[ERROR] Unsupported settlement SSH authentication mode" >&2
  exit 1
fi
if [[ -L "$SETTLEMENT_PLAN" || ! -f "$SETTLEMENT_PLAN" ]]; then
  echo "[ERROR] Settlement plan is missing or unsafe" >&2
  exit 1
fi
if [[ -L "$SETTLEMENT_OUTPUT" ]]; then
  echo "[ERROR] Settlement output path is unsafe" >&2
  exit 1
fi

helper=".github/scripts/settle_unstarted_candidate.py"
checkpoint_module="03_Scripts/deploy/release_checkpoint.py"
for source in "$helper" "$checkpoint_module"; do
  if [[ -L "$source" || ! -f "$source" ]]; then
    echo "[ERROR] Settlement control source is missing or unsafe: $source" >&2
    exit 1
  fi
done

SSH_PORT="${SSH_PORT:-22}"
ssh_options=(
  -T -p "$SSH_PORT"
  -o StrictHostKeyChecking=yes
  -o UserKnownHostsFile="$HOME/.ssh/known_hosts"
  -o ConnectTimeout=20
  -o ConnectionAttempts=2
  -o ServerAliveInterval=10
  -o ServerAliveCountMax=2
)
if [[ "$AUTH_MODE" == "key" ]]; then
  : "${SSH_PRIVATE_KEY:?SSH_PRIVATE_KEY is required for key authentication}"
  install -d -m 700 "$HOME/.ssh"
  printf '%s\n' "$SSH_PRIVATE_KEY" > "$HOME/.ssh/github_actions_tencent_key"
  chmod 600 "$HOME/.ssh/github_actions_tencent_key"
  ssh_command=(
    ssh -o BatchMode=yes -i "$HOME/.ssh/github_actions_tencent_key"
  )
else
  : "${SSH_PASSWORD:?SSH_PASSWORD is required for password authentication}"
  command -v sshpass >/dev/null
  export SSHPASS="$SSH_PASSWORD"
  ssh_command=(
    sshpass -e ssh
    -o PreferredAuthentications=password
    -o PubkeyAuthentication=no
  )
fi

payload="$(mktemp "${RUNNER_TEMP:-/tmp}/unstarted-candidate-settlement.XXXXXX")"
umask 077
chmod 600 "$payload"
trap 'rm -f "$payload"' EXIT

write_export() {
  local name="$1"
  local value="$2"
  printf 'export %s=%q\n' "$name" "$value" >> "$payload"
}

write_export SETTLEMENT_HELPER_B64 "$(base64 -w 0 "$helper")"
write_export SETTLEMENT_HELPER_SHA256 "$(sha256sum "$helper" | awk '{print $1}')"
write_export CHECKPOINT_MODULE_B64 "$(base64 -w 0 "$checkpoint_module")"
write_export CHECKPOINT_MODULE_SHA256 \
  "$(sha256sum "$checkpoint_module" | awk '{print $1}')"
write_export SETTLEMENT_PLAN_B64 "$(base64 -w 0 "$SETTLEMENT_PLAN")"
write_export SETTLEMENT_PLAN_SHA256 \
  "$(sha256sum "$SETTLEMENT_PLAN" | awk '{print $1}')"

cat >> "$payload" <<'REMOTE'
set -Eeuo pipefail
work="$(mktemp -d)"
cleanup() {
  rm -rf "$work"
}
trap cleanup EXIT
umask 077
printf '%s' "$SETTLEMENT_HELPER_B64" | base64 -d > "$work/settle_unstarted_candidate.py"
printf '%s' "$CHECKPOINT_MODULE_B64" | base64 -d > "$work/release_checkpoint.py"
printf '%s' "$SETTLEMENT_PLAN_B64" | base64 -d > "$work/plan.json"
chmod 600 "$work"/*.py "$work/plan.json"
test "$(sha256sum "$work/settle_unstarted_candidate.py" | awk '{print $1}')" \
  = "$SETTLEMENT_HELPER_SHA256"
test "$(sha256sum "$work/release_checkpoint.py" | awk '{print $1}')" \
  = "$CHECKPOINT_MODULE_SHA256"
test "$(sha256sum "$work/plan.json" | awk '{print $1}')" \
  = "$SETTLEMENT_PLAN_SHA256"
sudo -n env PYTHONPATH="$work" python3 -B "$work/settle_unstarted_candidate.py" \
  --plan "$work/plan.json" \
  --mode apply > "$work/result.json"
cat "$work/result.json"
REMOTE

timeout "${SETTLEMENT_TIMEOUT:-900}s" \
  "${ssh_command[@]}" "${ssh_options[@]}" \
  "$SSH_USER@$SSH_HOST" "umask 077; exec bash -s" \
  < "$payload" > "$SETTLEMENT_OUTPUT"

python3 - "$SETTLEMENT_OUTPUT" <<'PY'
import json
from pathlib import Path
import sys

path = Path(sys.argv[1])
payload = json.loads(path.read_text(encoding="utf-8"))
if (
    not isinstance(payload, dict)
    or payload.get("decision") != "candidate_prepare_aborted"
    or payload.get("mutationPerformed") is not True
    or payload.get("active", {}).get("changed") is not False
    or payload.get("databaseChanged") is not False
    or payload.get("trafficChanged") is not False
    or payload.get("jatoDataChanged") is not False
):
    raise SystemExit("unstarted Candidate settlement result is incomplete")
PY
chmod 600 "$SETTLEMENT_OUTPUT"
