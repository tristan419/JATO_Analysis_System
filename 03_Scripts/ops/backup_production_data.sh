#!/usr/bin/env bash
set -Eeuo pipefail

REPO_DIR="${REPO_DIR:-/opt/JATO_Analysis_System-main}"
BACKEND_ENV_FILE="${BACKEND_ENV_FILE:-/etc/jato-fullstack/backend.env}"
BACKUP_ROOT="${BACKUP_ROOT:-/opt/backups/jato}"
RETENTION_DAYS="${BACKUP_RETENTION_DAYS:-30}"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"

PG_BACKUP_DIR="$BACKUP_ROOT/pg"
OPS_BACKUP_DIR="$BACKUP_ROOT/ops"
MANIFEST_DIR="$BACKUP_ROOT/manifests"

log() {
  printf '[backup] %s\n' "$1"
}

load_backend_env() {
  if [[ ! -f "$BACKEND_ENV_FILE" ]]; then
    log "backend env file missing: $BACKEND_ENV_FILE"
    return 1
  fi

  set -a
  # shellcheck disable=SC1090
  source "$BACKEND_ENV_FILE"
  set +a
}

database_url() {
  printf '%s\n' "${APP_DATABASE_URL:-${DATABASE_URL:-}}"
}

normalize_pg_dump_url() {
  local raw_url="$1"
  raw_url="${raw_url/postgresql+psycopg2:\/\//postgresql://}"
  raw_url="${raw_url/postgresql+psycopg:\/\//postgresql://}"
  raw_url="${raw_url/postgresql+asyncpg:\/\//postgresql://}"
  printf '%s\n' "$raw_url"
}

main() {
  mkdir -p "$PG_BACKUP_DIR" "$OPS_BACKUP_DIR" "$MANIFEST_DIR"

  load_backend_env
  local db_url
  db_url="$(database_url)"
  if [[ -z "$db_url" ]]; then
    log "APP_DATABASE_URL/DATABASE_URL is empty; skipping postgres backup"
  elif ! command -v pg_dump >/dev/null 2>&1; then
    log "pg_dump not found; skipping postgres backup"
  else
    db_url="$(normalize_pg_dump_url "$db_url")"
    local pg_out="$PG_BACKUP_DIR/jato-${TIMESTAMP}.dump"
    log "dumping PostgreSQL schemas to $pg_out"
    pg_dump "$db_url" \
      --schema=auth \
      --schema=ordering \
      --schema=public \
      -Fc \
      -f "$pg_out"
  fi

  local ops_root="$REPO_DIR/04_Processed_data/ops"
  if [[ -d "$ops_root" ]]; then
    local ops_out="$OPS_BACKUP_DIR/ops-${TIMESTAMP}.tar.gz"
    log "archiving runtime ops artifacts to $ops_out"
    tar czf "$ops_out" \
      -C "$ops_root" \
      --ignore-failed-read \
      coc_match order_genius_uploads 2>/dev/null || true
  else
    log "ops root missing: $ops_root"
  fi

  cat > "$MANIFEST_DIR/backup-${TIMESTAMP}.json" <<JSON
{
  "createdAt": "$(date -Iseconds)",
  "repoDir": "$REPO_DIR",
  "backendEnvFile": "$BACKEND_ENV_FILE",
  "pgBackupDir": "$PG_BACKUP_DIR",
  "opsBackupDir": "$OPS_BACKUP_DIR",
  "retentionDays": $RETENTION_DAYS
}
JSON

  find "$PG_BACKUP_DIR" -name 'jato-*.dump' -mtime +"$RETENTION_DAYS" -delete 2>/dev/null || true
  find "$OPS_BACKUP_DIR" -name 'ops-*.tar.gz' -mtime +"$RETENTION_DAYS" -delete 2>/dev/null || true
  find "$MANIFEST_DIR" -name 'backup-*.json' -mtime +"$RETENTION_DAYS" -delete 2>/dev/null || true
  log "backup completed"
}

main "$@"
