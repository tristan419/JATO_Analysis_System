#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

REPO_DIR="${REPO_DIR:-/opt/JATO_Analysis_System-main}"
BACKEND_ENV_FILE="${BACKEND_ENV_FILE:-/etc/jato-fullstack/backend.env}"
BACKUP_ROOT="${BACKUP_ROOT:-/opt/backups/jato}"
RETENTION_DAYS="${BACKUP_RETENTION_DAYS:-30}"
REQUIRE_DATABASE_BACKUP="${REQUIRE_DATABASE_BACKUP:-false}"
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

is_truthy() {
  case "$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

fsync_file_and_parent() {
  python3 - "$1" <<'PY'
import os
from pathlib import Path
import sys

path = Path(sys.argv[1])
file_descriptor = os.open(path, os.O_RDONLY)
try:
    os.fsync(file_descriptor)
finally:
    os.close(file_descriptor)
directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
directory_descriptor = os.open(path.parent, directory_flags)
try:
    os.fsync(directory_descriptor)
finally:
    os.close(directory_descriptor)
PY
}

main() {
  local db_url=""
  local database_enabled="false"
  local database_required="false"
  local database_status="skipped"
  local pg_out=""
  local pg_temp=""
  local pg_bytes=0
  local pg_sha256=""
  local ops_out=""
  local manifest_path="$MANIFEST_DIR/backup-${TIMESTAMP}.json"

  mkdir -p "$PG_BACKUP_DIR" "$OPS_BACKUP_DIR" "$MANIFEST_DIR"
  chmod 700 "$BACKUP_ROOT" "$PG_BACKUP_DIR" "$OPS_BACKUP_DIR" "$MANIFEST_DIR"

  if is_truthy "$REQUIRE_DATABASE_BACKUP"; then
    database_required="true"
  fi
  if load_backend_env; then
    if is_truthy "${APP_DATABASE_ENABLED:-false}"; then
      database_enabled="true"
      database_required="true"
    fi
    db_url="$(database_url)"
  elif [[ "$database_required" == "true" ]]; then
    log "required database backup cannot load backend env"
    return 1
  fi

  if [[ -z "$db_url" ]]; then
    if [[ "$database_required" == "true" ]]; then
      log "required APP_DATABASE_URL/DATABASE_URL is empty"
      return 1
    fi
    log "database URL is empty; postgres backup not required"
  elif ! command -v pg_dump >/dev/null 2>&1; then
    if [[ "$database_required" == "true" ]]; then
      log "required pg_dump command is not available"
      return 1
    fi
    log "pg_dump not found; optional postgres backup skipped"
  else
    db_url="$(normalize_pg_dump_url "$db_url")"
    pg_out="$PG_BACKUP_DIR/jato-${TIMESTAMP}.dump"
    pg_temp="${pg_out}.partial.$$"
    trap 'if [[ -n "${pg_temp:-}" ]]; then rm -f "$pg_temp"; fi' EXIT
    log "dumping PostgreSQL schemas to $pg_out"
    pg_dump "$db_url" \
      --schema=auth \
      --schema=ordering \
      --schema=public \
      -Fc \
      -f "$pg_temp"
    if [[ ! -s "$pg_temp" ]]; then
      log "postgres backup is empty; refusing to mark backup successful"
      return 1
    fi
    pg_bytes="$(wc -c < "$pg_temp" | tr -d '[:space:]')"
    pg_sha256="$(sha256sum "$pg_temp" | awk '{print $1}')"
    chmod 600 "$pg_temp"
    fsync_file_and_parent "$pg_temp"
    mv "$pg_temp" "$pg_out"
    fsync_file_and_parent "$pg_out"
    pg_temp=""
    database_status="completed"
  fi

  local ops_root="$REPO_DIR/04_Processed_data/ops"
  if [[ -d "$ops_root" ]]; then
    ops_out="$OPS_BACKUP_DIR/ops-${TIMESTAMP}.tar.gz"
    log "archiving runtime ops artifacts to $ops_out"
    if tar czf "$ops_out" \
      -C "$ops_root" \
      --ignore-failed-read \
      coc_match order_genius_uploads 2>/dev/null; then
      chmod 600 "$ops_out"
      fsync_file_and_parent "$ops_out"
    else
      log "optional runtime ops backup failed; omitting it from the manifest"
      rm -f "$ops_out"
      ops_out=""
    fi
  else
    log "ops root missing: $ops_root"
  fi

  python3 - "$manifest_path" "$REPO_DIR" "$BACKEND_ENV_FILE" \
    "$RETENTION_DAYS" "$database_enabled" "$database_required" \
    "$database_status" "$pg_out" "$pg_bytes" "$pg_sha256" "$ops_out" <<'PY'
import datetime as dt
import json
import os
from pathlib import Path
import sys
import tempfile

(
    manifest_path,
    repo_dir,
    backend_env_file,
    retention_days,
    database_enabled,
    database_required,
    database_status,
    dump_path,
    dump_bytes,
    dump_sha256,
    ops_path,
) = sys.argv[1:]
payload = {
    "createdAt": dt.datetime.now(dt.timezone.utc).isoformat(),
    "repoDir": repo_dir,
    "backendEnvFile": backend_env_file,
    "retentionDays": int(retention_days),
    "database": {
        "enabled": database_enabled == "true",
        "required": database_required == "true",
        "status": database_status,
        "dumpPath": dump_path or None,
        "dumpBytes": int(dump_bytes),
        "dumpSha256": dump_sha256 or None,
    },
    "opsArchivePath": ops_path or None,
}
path = Path(manifest_path)
descriptor, temporary_name = tempfile.mkstemp(
    prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
)
temporary = Path(temporary_name)
try:
    os.fchmod(descriptor, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    os.chmod(path, 0o600)
    directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    directory_fd = os.open(path.parent, directory_flags)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
finally:
    try:
        os.close(descriptor)
    except OSError:
        pass
    try:
        temporary.unlink()
    except FileNotFoundError:
        pass
PY

  if [[ "$database_required" == "true" && "$database_status" != "completed" ]]; then
    log "required database backup was not completed"
    return 1
  fi

  find "$PG_BACKUP_DIR" -name 'jato-*.dump' -mtime +"$RETENTION_DAYS" -delete 2>/dev/null || true
  find "$OPS_BACKUP_DIR" -name 'ops-*.tar.gz' -mtime +"$RETENTION_DAYS" -delete 2>/dev/null || true
  find "$MANIFEST_DIR" -name 'backup-*.json' -mtime +"$RETENTION_DAYS" -delete 2>/dev/null || true
  local manifest_bytes
  local manifest_sha256
  manifest_bytes="$(wc -c < "$manifest_path" | tr -d '[:space:]')"
  manifest_sha256="$(sha256sum "$manifest_path" | awk '{print $1}')"
  log "backup completed; manifest=$manifest_path manifestBytes=$manifest_bytes manifestSha256=$manifest_sha256"
}

main "$@"
