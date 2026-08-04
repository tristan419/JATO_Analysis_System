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

helper=".github/scripts/settle_failed_candidate_preimage.py"
common_helper=".github/scripts/settle_unstarted_candidate.py"
checkpoint_module="03_Scripts/deploy/release_checkpoint.py"
preimage_module="03_Scripts/deploy/candidate_runtime_preimage.py"
for source in \
  "$SETTLEMENT_PLAN" "$helper" "$common_helper" \
  "$checkpoint_module" "$preimage_module"; do
  if [[ -L "$source" || ! -f "$source" ]]; then
    echo "[ERROR] Settlement control source is missing or unsafe: $source" >&2
    exit 1
  fi
done
if [[ -L "$SETTLEMENT_OUTPUT" ]]; then
  echo "[ERROR] Settlement output path is unsafe" >&2
  exit 1
fi

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
  ssh_command=(ssh -o BatchMode=yes -i "$HOME/.ssh/github_actions_tencent_key")
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

payload="$(mktemp "${RUNNER_TEMP:-/tmp}/failed-candidate-settlement.XXXXXX")"
umask 077
chmod 600 "$payload"
trap 'rm -f "$payload"' EXIT

write_payload() {
  local name="$1"
  local source="$2"
  printf '%s=%q\n' "${name}_B64" "$(base64 -w 0 "$source")" >> "$payload"
  printf '%s=%q\n' "${name}_SHA256" \
    "$(sha256sum "$source" | awk '{print $1}')" >> "$payload"
}

write_payload SETTLEMENT_HELPER "$helper"
write_payload COMMON_HELPER "$common_helper"
write_payload CHECKPOINT_MODULE "$checkpoint_module"
write_payload PREIMAGE_MODULE "$preimage_module"
write_payload SETTLEMENT_PLAN "$SETTLEMENT_PLAN"

cat >> "$payload" <<'REMOTE'
set -Eeuo pipefail
work="$(mktemp -d)"
cleanup() {
  rm -rf "$work"
}
trap cleanup EXIT
umask 077
decode() {
  local content="$1"
  local expected="$2"
  local target="$3"
  printf '%s' "$content" | base64 -d > "$target"
  test "$(sha256sum "$target" | awk '{print $1}')" = "$expected"
}
decode "$SETTLEMENT_HELPER_B64" "$SETTLEMENT_HELPER_SHA256" \
  "$work/settle_failed_candidate_preimage.py"
decode "$COMMON_HELPER_B64" "$COMMON_HELPER_SHA256" \
  "$work/settle_unstarted_candidate.py"
decode "$CHECKPOINT_MODULE_B64" "$CHECKPOINT_MODULE_SHA256" \
  "$work/release_checkpoint.py"
decode "$PREIMAGE_MODULE_B64" "$PREIMAGE_MODULE_SHA256" \
  "$work/candidate_runtime_preimage.py"
decode "$SETTLEMENT_PLAN_B64" "$SETTLEMENT_PLAN_SHA256" "$work/plan.json"
chmod 600 "$work"/*.py "$work/plan.json"
sudo -n env PYTHONPATH="$work" python3 -B \
  "$work/settle_failed_candidate_preimage.py" \
  --plan "$work/plan.json" --mode check > "$work/check.json"
sudo -n env PYTHONPATH="$work" python3 -B \
  "$work/settle_failed_candidate_preimage.py" \
  --plan "$work/plan.json" --mode apply > "$work/result.json"
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

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
if (
    not isinstance(payload, dict)
    or payload.get("decision") != "candidate_prepare_aborted"
    or payload.get("mutationPerformed") is not True
    or payload.get("active", {}).get("changed") is not False
    or payload.get("candidate", {}).get("restored") is not True
    or payload.get("databaseChanged") is not False
    or payload.get("trafficChanged") is not False
    or payload.get("jatoDataChanged") is not False
):
    raise SystemExit("failed Candidate settlement result is incomplete")
PY
chmod 600 "$SETTLEMENT_OUTPUT"
