#!/usr/bin/env bash
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
RUNNER_SCRIPT="$REPO_DIR/03_Scripts/ops/schedule_msrp_sync_to_cloud.sh"

export PATH="/opt/homebrew/opt/postgresql@16/bin:/opt/homebrew/opt/libpq/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}"

cd "$REPO_DIR"
clear

printf '══════════════════════════════════════════════════\n'
printf '  MSRP PostgreSQL 双击同步到腾讯云\n'
printf '══════════════════════════════════════════════════\n\n'

/bin/bash "$RUNNER_SCRIPT"
status=$?

printf '\n'
if [[ $status -eq 0 ]]; then
  printf '同步完成。\n'
else
  printf '同步失败，退出码: %s\n' "$status"
fi

read -r -p '按回车关闭窗口...'
exit "$status"
