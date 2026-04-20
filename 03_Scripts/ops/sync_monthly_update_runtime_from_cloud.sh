#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

SSH_ALIAS="${SSH_ALIAS:-tencent-cloud}"
CLOUD_HOST="${CLOUD_HOST:-150.158.141.14}"
CLOUD_USER="${CLOUD_USER:-root}"
CLOUD_REPO="${CLOUD_REPO:-/opt/JATO_Analysis_System-main}"
REMOTE_ARCHIVE="${REMOTE_ARCHIVE:-/tmp/jato-monthly-runtime-sync.tar.gz}"

BACKUP_ROOT="${PROJECT_ROOT}/.runtime_sync_backups/monthly-update-$(date +%Y%m%d-%H%M%S)"
TMP_DIR="$(mktemp -d)"
LOCAL_ARCHIVE="${TMP_DIR}/jato-monthly-runtime-sync.tar.gz"

RUNTIME_PATHS=(
  "01_RAW_DATA/baseline"
  "01_RAW_DATA/patches"
  "01_RAW_DATA/historyDataArchive/baseline"
  "01_RAW_DATA/historyDataArchive/patches"
  "04_Processed_data/jato_full_archive.parquet"
  "04_Processed_data/manifest.json"
  "04_Processed_data/partitioned_dataset_v1"
  "04_Processed_data/refresh_job_report.json"
  "04_Processed_data/dataset_fingerprint.json"
)

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

echo "== JATO monthly runtime sync: cloud -> local =="
echo "Cloud repo: $CLOUD_REPO"
echo "Local repo: $PROJECT_ROOT"

if ssh -o ConnectTimeout=10 -o BatchMode=yes "$SSH_ALIAS" "echo ok" >/dev/null 2>&1; then
  echo "SSH ready via alias: $SSH_ALIAS"
else
  if ssh -o ConnectTimeout=10 -o BatchMode=yes -i ~/.ssh/tencent_lh.pem "$CLOUD_USER@$CLOUD_HOST" "echo ok" >/dev/null 2>&1; then
    SSH_ALIAS="$CLOUD_USER@$CLOUD_HOST"
    echo "SSH ready via direct host: $SSH_ALIAS"
  else
    echo "ERROR: cannot connect to Tencent Cloud via SSH." >&2
    exit 1
  fi
fi

echo "Packing remote runtime snapshot..."
ssh -o ConnectTimeout=30 "$SSH_ALIAS" CLOUD_REPO="$CLOUD_REPO" REMOTE_ARCHIVE="$REMOTE_ARCHIVE" "bash -s" <<'REMOTE_SCRIPT'
set -Eeuo pipefail

runtime_paths=(
  "01_RAW_DATA/baseline"
  "01_RAW_DATA/patches"
  "01_RAW_DATA/historyDataArchive/baseline"
  "01_RAW_DATA/historyDataArchive/patches"
  "04_Processed_data/jato_full_archive.parquet"
  "04_Processed_data/manifest.json"
  "04_Processed_data/partitioned_dataset_v1"
  "04_Processed_data/refresh_job_report.json"
  "04_Processed_data/dataset_fingerprint.json"
)

existing_paths=()
for rel in "${runtime_paths[@]}"; do
  if [[ -e "${CLOUD_REPO}/${rel}" ]]; then
    existing_paths+=("$rel")
  fi
done

if [[ ${#existing_paths[@]} -eq 0 ]]; then
  echo "No monthly runtime paths found on cloud." >&2
  exit 1
fi

rm -f "$REMOTE_ARCHIVE"
tar czf "$REMOTE_ARCHIVE" -C "$CLOUD_REPO" "${existing_paths[@]}"
printf 'Remote snapshot contains:\n'
printf '  - %s\n' "${existing_paths[@]}"
REMOTE_SCRIPT

echo "Downloading runtime snapshot..."
scp -o ConnectTimeout=30 -o ServerAliveInterval=15 "$SSH_ALIAS:$REMOTE_ARCHIVE" "$LOCAL_ARCHIVE"
ssh -o ConnectTimeout=30 "$SSH_ALIAS" "rm -f '$REMOTE_ARCHIVE'"

echo "Backing up current local runtime to: $BACKUP_ROOT"
mkdir -p "$BACKUP_ROOT"
for rel in "${RUNTIME_PATHS[@]}"; do
  src="${PROJECT_ROOT}/${rel}"
  if [[ ! -e "$src" ]]; then
    continue
  fi
  mkdir -p "$(dirname "${BACKUP_ROOT}/${rel}")"
  cp -R "$src" "${BACKUP_ROOT}/${rel}"
done

echo "Replacing local runtime files..."
for rel in "${RUNTIME_PATHS[@]}"; do
  target="${PROJECT_ROOT}/${rel}"
  if [[ -e "$target" ]]; then
    rm -rf "$target"
  fi
done

tar xzf "$LOCAL_ARCHIVE" -C "$PROJECT_ROOT"

echo "Sync completed."
echo "Backup saved at: $BACKUP_ROOT"
echo "Current local snapshot:"
for rel in "${RUNTIME_PATHS[@]}"; do
  target="${PROJECT_ROOT}/${rel}"
  if [[ -e "$target" ]]; then
    echo "  - $rel"
  fi
done
