#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

REPO_DIR="${REPO_DIR:-/opt/JATO_Analysis_System-main}"
BACKEND_ENV_FILE="${BACKEND_ENV_FILE:-/etc/jato-fullstack/backend.env}"
BACKUP_ROOT="${BACKUP_ROOT:-/opt/backups/jato}"
RETENTION_DAYS="${BACKUP_RETENTION_DAYS:-30}"
REQUIRE_DATABASE_BACKUP="${REQUIRE_DATABASE_BACKUP:-false}"
TIMESTAMP="${BACKUP_TIMESTAMP:-$(date +%Y%m%d-%H%M%S)}"
PYTHON_BIN="${PYTHON_BIN:-$REPO_DIR/.venv/bin/python}"
INTEGRITY_SCRIPT="${MSRP_EVIDENCE_INTEGRITY_SCRIPT:-$REPO_DIR/03_Scripts/ops/msrp_evidence_integrity.py}"
RELEASE_PATHS_LIB="${MSRP_RELEASE_PATHS_LIB:-$REPO_DIR/03_Scripts/deploy/lib/release_paths.sh}"

PG_BACKUP_DIR="$BACKUP_ROOT/pg"
OPS_BACKUP_DIR="$BACKUP_ROOT/ops"
EVIDENCE_BACKUP_DIR="$BACKUP_ROOT/msrp-evidence"
INTEGRITY_REPORT_DIR="$BACKUP_ROOT/integrity"
MANIFEST_DIR="$BACKUP_ROOT/manifests"

TEMPORARY_PATHS=()

log() {
  printf '[backup] %s\n' "$1"
}

cleanup_temporary_paths() {
  local path=""
  for path in "${TEMPORARY_PATHS[@]-}"; do
    rm -rf "$path" 2>/dev/null || true
  done
}

trap cleanup_temporary_paths EXIT

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

sha256_file() {
  local path="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$path" | awk '{print $1}'
  else
    shasum -a 256 "$path" | awk '{print $1}'
  fi
}

file_size() {
  local path="$1"
  stat -c %s "$path" 2>/dev/null || stat -f %z "$path"
}

require_command() {
  local command_name="$1"
  if ! command -v "$command_name" >/dev/null 2>&1; then
    log "required command not found: $command_name"
    return 1
  fi
}

write_manifest() {
  local manifest_path="$1"
  local database_enabled="$2"
  local database_required="$3"
  local database_status="$4"
  local pg_path="$5"
  local pg_hash="$6"
  local pg_bytes="$7"
  local evidence_path="$8"
  local evidence_hash="$9"
  local evidence_archive_bytes="${10}"
  local integrity_path="${11}"
  local integrity_hash="${12}"
  local integrity_bytes="${13}"
  local object_count="${14}"
  local object_bytes="${15}"
  local ops_path="${16}"
  local ops_hash="${17}"
  local ops_bytes="${18}"

  MANIFEST_PATH="$manifest_path" \
  BACKUP_CREATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  BACKUP_REPO_DIR="$REPO_DIR" \
  BACKUP_ENV_FILE="$BACKEND_ENV_FILE" \
  BACKUP_RETENTION_DAYS="$RETENTION_DAYS" \
  BACKUP_TIMESTAMP_VALUE="$TIMESTAMP" \
  BACKUP_DATABASE_ENABLED="$database_enabled" \
  BACKUP_DATABASE_REQUIRED="$database_required" \
  BACKUP_DATABASE_STATUS="$database_status" \
  BACKUP_PG_PATH="$pg_path" \
  BACKUP_PG_HASH="$pg_hash" \
  BACKUP_PG_BYTES="$pg_bytes" \
  BACKUP_EVIDENCE_PATH="$evidence_path" \
  BACKUP_EVIDENCE_HASH="$evidence_hash" \
  BACKUP_EVIDENCE_ARCHIVE_BYTES="$evidence_archive_bytes" \
  BACKUP_INTEGRITY_PATH="$integrity_path" \
  BACKUP_INTEGRITY_HASH="$integrity_hash" \
  BACKUP_INTEGRITY_BYTES="$integrity_bytes" \
  BACKUP_OBJECT_COUNT="$object_count" \
  BACKUP_OBJECT_BYTES="$object_bytes" \
  BACKUP_OPS_PATH="$ops_path" \
  BACKUP_OPS_HASH="$ops_hash" \
  BACKUP_OPS_BYTES="$ops_bytes" \
  "$PYTHON_BIN" - <<'PY'
import json
import os
from pathlib import Path
import tempfile


def artifact(path_key: str, hash_key: str, bytes_key: str) -> dict[str, object] | None:
    path = os.environ.get(path_key, "")
    if not path:
        return None
    return {
        "artifactPath": path,
        "sha256": os.environ[hash_key],
        "sizeBytes": int(os.environ[bytes_key]),
    }


database_artifact = artifact("BACKUP_PG_PATH", "BACKUP_PG_HASH", "BACKUP_PG_BYTES")
evidence_artifact = artifact(
    "BACKUP_EVIDENCE_PATH",
    "BACKUP_EVIDENCE_HASH",
    "BACKUP_EVIDENCE_ARCHIVE_BYTES",
)
integrity_artifact = artifact(
    "BACKUP_INTEGRITY_PATH",
    "BACKUP_INTEGRITY_HASH",
    "BACKUP_INTEGRITY_BYTES",
)
ops_artifact = artifact("BACKUP_OPS_PATH", "BACKUP_OPS_HASH", "BACKUP_OPS_BYTES")
database = {
    "enabled": os.environ["BACKUP_DATABASE_ENABLED"] == "true",
    "required": os.environ["BACKUP_DATABASE_REQUIRED"] == "true",
    "status": os.environ["BACKUP_DATABASE_STATUS"],
    "dumpPath": os.environ["BACKUP_PG_PATH"] or None,
    "dumpBytes": int(os.environ["BACKUP_PG_BYTES"]),
    "dumpSha256": os.environ["BACKUP_PG_HASH"] or None,
    "format": "postgresql_custom",
    "schemas": ["auth", "ordering", "public", "msrp"],
}
if database_artifact:
    database.update(database_artifact)

payload = {
    "schemaVersion": "jato_production_backup_v2",
    "backupTimestamp": os.environ["BACKUP_TIMESTAMP_VALUE"],
    "createdAt": os.environ["BACKUP_CREATED_AT"],
    "repoDir": os.environ["BACKUP_REPO_DIR"],
    "backendEnvFile": os.environ["BACKUP_ENV_FILE"],
    "retentionDays": int(os.environ["BACKUP_RETENTION_DAYS"]),
    "database": database,
    "evidenceObjects": (
        {
            **evidence_artifact,
            "objectCount": int(os.environ["BACKUP_OBJECT_COUNT"]),
            "totalObjectBytes": int(os.environ["BACKUP_OBJECT_BYTES"]),
            "contentAddressed": True,
        }
        if evidence_artifact
        else None
    ),
    "integrityReport": integrity_artifact,
    "runtimeOps": ops_artifact,
    "opsArchivePath": os.environ["BACKUP_OPS_PATH"] or None,
}
manifest_path = Path(os.environ["MANIFEST_PATH"])
descriptor, temporary_name = tempfile.mkstemp(
    prefix=f".{manifest_path.name}.", suffix=".tmp", dir=manifest_path.parent
)
temporary = Path(temporary_name)
try:
    os.fchmod(descriptor, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, manifest_path)
    os.chmod(manifest_path, 0o600)
    directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    directory_fd = os.open(manifest_path.parent, directory_flags)
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
}

main() {
  local db_url=""
  local database_enabled="false"
  local database_required="false"
  local database_status="skipped"
  local pg_out=""
  local pg_temp=""
  local pg_bytes="0"
  local pg_sha256=""
  local evidence_root=""
  local evidence_out=""
  local evidence_temp=""
  local evidence_sha256=""
  local evidence_bytes="0"
  local integrity_out=""
  local integrity_temp=""
  local integrity_sha256=""
  local integrity_bytes="0"
  local object_count="0"
  local object_bytes="0"
  local object_list=""
  local ops_out=""
  local ops_temp=""
  local ops_sha256=""
  local ops_bytes="0"
  local ops_path=""
  local manifest_path="$MANIFEST_DIR/backup-${TIMESTAMP}.json"

  mkdir -p \
    "$PG_BACKUP_DIR" \
    "$OPS_BACKUP_DIR" \
    "$EVIDENCE_BACKUP_DIR" \
    "$INTEGRITY_REPORT_DIR" \
    "$MANIFEST_DIR"
  chmod 700 \
    "$BACKUP_ROOT" \
    "$PG_BACKUP_DIR" \
    "$OPS_BACKUP_DIR" \
    "$EVIDENCE_BACKUP_DIR" \
    "$INTEGRITY_REPORT_DIR" \
    "$MANIFEST_DIR"

  if [[ ! -x "$PYTHON_BIN" ]]; then
    PYTHON_BIN="$(command -v python3)"
  fi
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
    log "database URL is empty; postgres and MSRP evidence backups are not required"
  elif ! command -v pg_dump >/dev/null 2>&1; then
    if [[ "$database_required" == "true" ]]; then
      log "required pg_dump command is not available"
      return 1
    fi
    log "pg_dump not found; optional postgres and MSRP evidence backups skipped"
  else
    require_command tar
    if [[ ! -f "$INTEGRITY_SCRIPT" ]]; then
      log "MSRP evidence integrity command missing: $INTEGRITY_SCRIPT"
      return 1
    fi
    if [[ ! -f "$RELEASE_PATHS_LIB" ]]; then
      log "release path helper missing: $RELEASE_PATHS_LIB"
      return 1
    fi
    # shellcheck disable=SC1090
    source "$RELEASE_PATHS_LIB"
    evidence_root="$(
      resolve_msrp_evidence_root \
        "${APP_PROJECT_ROOT:-$REPO_DIR}" \
        "${MSRP_GOVERNANCE_EVIDENCE_ROOT:-}"
    )"
    assert_path_outside_release_roots \
      "$REPO_DIR" \
      "$evidence_root" \
      03_Scripts 06_AppPlatform 07_ScrapingToolkit hermes

    db_url="$(normalize_pg_dump_url "$db_url")"
    pg_out="$PG_BACKUP_DIR/jato-${TIMESTAMP}.dump"
    evidence_out="$EVIDENCE_BACKUP_DIR/msrp-evidence-${TIMESTAMP}.tar.gz"
    integrity_out="$INTEGRITY_REPORT_DIR/msrp-evidence-${TIMESTAMP}.json"
    pg_temp="${pg_out}.partial.$$"
    evidence_temp="${evidence_out}.partial.$$"
    integrity_temp="${integrity_out}.partial.$$"
    object_list="$(mktemp "$BACKUP_ROOT/.msrp-evidence-objects.XXXXXX")"
    TEMPORARY_PATHS+=("$pg_temp" "$evidence_temp" "$integrity_temp" "$object_list")

    log "dumping PostgreSQL schemas, including msrp, to $pg_out"
    pg_dump "$db_url" \
      --schema=auth \
      --schema=ordering \
      --schema=public \
      --schema=msrp \
      -Fc \
      -f "$pg_temp"
    if [[ ! -s "$pg_temp" ]]; then
      log "postgres backup is empty; refusing to mark backup successful"
      return 1
    fi

    log "checking DB Evidence Asset rows against $evidence_root"
    "$PYTHON_BIN" "$INTEGRITY_SCRIPT" \
      --evidence-root "$evidence_root" \
      --output "$integrity_temp" \
      --object-list-output "$object_list"

    log "archiving verified content-addressed evidence objects to $evidence_out"
    if [[ -s "$object_list" ]]; then
      COPYFILE_DISABLE=1 tar czf "$evidence_temp" -C "$evidence_root" -T "$object_list"
    else
      COPYFILE_DISABLE=1 tar czf "$evidence_temp" -T "$object_list"
    fi

    read -r object_count object_bytes < <(
      "$PYTHON_BIN" - "$integrity_temp" <<'PY'
import json
import sys
from pathlib import Path

summary = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))["summary"]
print(summary["healthyObjectCount"], summary["verifiedObjectBytes"])
PY
    )

    pg_bytes="$(file_size "$pg_temp")"
    pg_sha256="$(sha256_file "$pg_temp")"
    evidence_bytes="$(file_size "$evidence_temp")"
    evidence_sha256="$(sha256_file "$evidence_temp")"
    integrity_bytes="$(file_size "$integrity_temp")"
    integrity_sha256="$(sha256_file "$integrity_temp")"

    for pending_path in "$pg_temp" "$evidence_temp" "$integrity_temp"; do
      chmod 600 "$pending_path"
      fsync_file_and_parent "$pending_path"
    done
    mv "$pg_temp" "$pg_out"
    mv "$evidence_temp" "$evidence_out"
    mv "$integrity_temp" "$integrity_out"
    fsync_file_and_parent "$pg_out"
    fsync_file_and_parent "$evidence_out"
    fsync_file_and_parent "$integrity_out"
    pg_temp=""
    evidence_temp=""
    integrity_temp=""
    database_status="completed"
  fi

  local ops_root="$REPO_DIR/04_Processed_data/ops"
  local ops_entries=()
  local ops_entry=""
  for ops_entry in coc_match order_genius_uploads; do
    if [[ -e "$ops_root/$ops_entry" ]]; then
      ops_entries+=("$ops_entry")
    fi
  done
  if [[ "${#ops_entries[@]}" -gt 0 ]]; then
    ops_out="$OPS_BACKUP_DIR/ops-${TIMESTAMP}.tar.gz"
    ops_temp="${ops_out}.partial.$$"
    TEMPORARY_PATHS+=("$ops_temp")
    log "archiving runtime ops artifacts to $ops_out"
    COPYFILE_DISABLE=1 tar czf "$ops_temp" -C "$ops_root" "${ops_entries[@]}"
    ops_bytes="$(file_size "$ops_temp")"
    ops_sha256="$(sha256_file "$ops_temp")"
    chmod 600 "$ops_temp"
    fsync_file_and_parent "$ops_temp"
    mv "$ops_temp" "$ops_out"
    fsync_file_and_parent "$ops_out"
    ops_temp=""
    ops_path="$ops_out"
  else
    log "no optional coc_match/order_genius_uploads artifacts found"
  fi

  if [[ "$database_required" == "true" && "$database_status" != "completed" ]]; then
    log "required database backup was not completed"
    return 1
  fi

  write_manifest \
    "$manifest_path" \
    "$database_enabled" "$database_required" "$database_status" \
    "$pg_out" "$pg_sha256" "$pg_bytes" \
    "$evidence_out" "$evidence_sha256" "$evidence_bytes" \
    "$integrity_out" "$integrity_sha256" "$integrity_bytes" \
    "$object_count" "$object_bytes" \
    "$ops_path" "$ops_sha256" "$ops_bytes"

  find "$PG_BACKUP_DIR" -name 'jato-*.dump' -mtime +"$RETENTION_DAYS" -delete 2>/dev/null || true
  find "$OPS_BACKUP_DIR" -name 'ops-*.tar.gz' -mtime +"$RETENTION_DAYS" -delete 2>/dev/null || true
  find "$EVIDENCE_BACKUP_DIR" -name 'msrp-evidence-*.tar.gz' -mtime +"$RETENTION_DAYS" -delete 2>/dev/null || true
  find "$INTEGRITY_REPORT_DIR" -name 'msrp-evidence-*.json' -mtime +"$RETENTION_DAYS" -delete 2>/dev/null || true
  find "$MANIFEST_DIR" -name 'backup-*.json' -mtime +"$RETENTION_DAYS" -delete 2>/dev/null || true

  local manifest_bytes
  local manifest_sha256
  manifest_bytes="$(file_size "$manifest_path")"
  manifest_sha256="$(sha256_file "$manifest_path")"
  log "backup completed; manifest=$manifest_path manifestBytes=$manifest_bytes manifestSha256=$manifest_sha256"
}

main "$@"
