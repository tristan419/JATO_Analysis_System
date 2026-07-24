#!/usr/bin/env bash
set -Eeuo pipefail

# Install or safely migrate the stable JATO site configuration.
#
# The public backend port and frontend root are deliberately kept together in
# one generated include:
#   /etc/jato-fullstack/nginx/active-release.conf
#
# The blue/green release controller replaces that file atomically after the
# candidate passes readiness. This installer owns only the initial install and
# the one-time migration of an existing Certbot-managed site.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
NGINX_TEMPLATE="${NGINX_TEMPLATE:-$ROOT_DIR/03_Scripts/deploy/nginx/jato_fullstack.conf.example}"
PRODUCTION_MUTATION_LOCK_LIB="${PRODUCTION_MUTATION_LOCK_LIB:-$ROOT_DIR/03_Scripts/deploy/lib/production_mutation_lock.sh}"
NGINX_ETC_DIR="${NGINX_ETC_DIR:-/etc/nginx}"
JATO_ETC_DIR="${JATO_ETC_DIR:-/etc/jato-fullstack}"
TARGET_CONF="${TARGET_CONF:-$NGINX_ETC_DIR/sites-available/jato_fullstack.conf}"
ENABLED_CONF="${ENABLED_CONF:-$NGINX_ETC_DIR/sites-enabled/jato_fullstack.conf}"
DEFAULT_ENABLED_CONF="${DEFAULT_ENABLED_CONF:-$NGINX_ETC_DIR/sites-enabled/default}"
ACTIVE_RELEASE_CONF="${ACTIVE_RELEASE_CONF:-$JATO_ETC_DIR/nginx/active-release.conf}"
BACKUP_DIR="${BACKUP_DIR:-$NGINX_ETC_DIR/jato-backups}"
NGINX_PREIMAGE_DIR="${NGINX_PREIMAGE_DIR:-}"
INSTALL_MODE="${1:-install}"

SERVER_NAME="${SERVER_NAME:-_}"
BACKEND_PORT="${BACKEND_PORT:-8000}"
FRONTEND_ROOT="${FRONTEND_ROOT:-/opt/jato/slots/8000/current/06_AppPlatform/frontend/dist}"
SKIP_PACKAGE_INSTALL="${SKIP_PACKAGE_INSTALL:-false}"
SKIP_HEALTH_CHECK="${SKIP_HEALTH_CHECK:-false}"
NGINX_BIN="${NGINX_BIN:-nginx}"
SYSTEMCTL_BIN="${SYSTEMCTL_BIN:-systemctl}"
APT_GET_BIN="${APT_GET_BIN:-apt-get}"
CURL_BIN="${CURL_BIN:-curl}"

WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/jato-nginx-install.XXXXXX")"
SITE_CANDIDATE="$WORK_DIR/jato_fullstack.conf"
ACTIVE_CANDIDATE="$WORK_DIR/active-release.conf"
TARGET_SNAPSHOT="$WORK_DIR/target.original"
ACTIVE_SNAPSHOT="$WORK_DIR/active.original"
DEFAULT_SNAPSHOT="$WORK_DIR/default.original"
TARGET_EXISTED=false
ACTIVE_EXISTED=false
ENABLED_EXISTED=false
ENABLED_TARGET=""
DEFAULT_EXISTED=false
DEFAULT_WAS_SYMLINK=false
DEFAULT_TARGET=""
CERTBOT_MIGRATION=false
MUTATION_STARTED=false
COMPLETED=false
PREIMAGE_STAGING_DIR=""

is_truthy() {
  case "$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

fail() {
  echo "[ERROR] $*" >&2
  return 1
}

if [[ ! -f "$PRODUCTION_MUTATION_LOCK_LIB" \
  || -L "$PRODUCTION_MUTATION_LOCK_LIB" ]]; then
  fail "Production mutation lock helper is missing or unsafe"
  exit 1
fi
# shellcheck disable=SC1090
source "$PRODUCTION_MUTATION_LOCK_LIB"

validate_inputs() {
  if [[ ! "$BACKEND_PORT" =~ ^[0-9]+$ ]] \
    || (( BACKEND_PORT < 1 || BACKEND_PORT > 65535 )); then
    fail "BACKEND_PORT must be an integer between 1 and 65535"
  fi
  if [[ "$FRONTEND_ROOT" != /* ]] \
    || [[ "$FRONTEND_ROOT" == *$'\n'* ]] \
    || [[ "$FRONTEND_ROOT" == *$'\r'* ]] \
    || [[ "$FRONTEND_ROOT" == *'\'* ]] \
    || [[ "$FRONTEND_ROOT" == *'$'* ]] \
    || [[ "$FRONTEND_ROOT" == *';'* ]] \
    || [[ "$FRONTEND_ROOT" == *'{'* ]] \
    || [[ "$FRONTEND_ROOT" == *'}'* ]] \
    || [[ "$FRONTEND_ROOT" == *'"'* ]]; then
    fail "FRONTEND_ROOT must be a safe absolute path"
  fi
  if [[ ! "$SERVER_NAME" =~ ^(_|[A-Za-z0-9*.-]+)([[:space:]]+(_|[A-Za-z0-9*.-]+))*$ ]]; then
    fail "SERVER_NAME must contain only domain names (or _)"
  fi
  if [[ ! -f "$NGINX_TEMPLATE" || -L "$NGINX_TEMPLATE" ]]; then
    fail "Nginx template is missing or unsafe: $NGINX_TEMPLATE"
  fi
  if [[ -L "$TARGET_CONF" ]] \
    || [[ -e "$TARGET_CONF" && ! -f "$TARGET_CONF" ]]; then
    fail "Target nginx configuration must be a regular file: $TARGET_CONF"
  fi
  if [[ -L "$ACTIVE_RELEASE_CONF" ]] \
    || [[ -e "$ACTIVE_RELEASE_CONF" && ! -f "$ACTIVE_RELEASE_CONF" ]]; then
    fail "Active release include must be a regular file: $ACTIVE_RELEASE_CONF"
  fi
  if [[ -e "$ENABLED_CONF" && ! -L "$ENABLED_CONF" ]]; then
    fail "Enabled JATO site must be a symlink: $ENABLED_CONF"
  fi
  if [[ -e "$DEFAULT_ENABLED_CONF" \
    && ! -f "$DEFAULT_ENABLED_CONF" \
    && ! -L "$DEFAULT_ENABLED_CONF" ]]; then
    fail "Default nginx site must be a regular file or symlink"
  fi
}

render_active_release_candidate() {
  python3 - "$ACTIVE_CANDIDATE" "$BACKEND_PORT" "$FRONTEND_ROOT" <<'PY'
from pathlib import Path
import sys

output_path = Path(sys.argv[1])
port = int(sys.argv[2])
frontend_root = sys.argv[3]
payload = f"""# Managed by the JATO blue/green release controller.
# Backend and frontend must always move together in this one file.
upstream jato_fullstack_api {{
    server 127.0.0.1:{port} max_fails=3 fail_timeout=30s;
    keepalive 32;
}}

map $host $jato_frontend_root {{
    default \"{frontend_root}\";
}}

# Stable loopback entry for host-side consumers such as MSRP schedulers.
# It follows the same upstream switch as the public site and is never exposed externally.
server {{
    listen 127.0.0.1:18000;
    server_name _;

    location ^~ /v1/msrp/monthly-update {{
        if (-f /var/lib/jato-release/deployment-maintenance) {{
            return 423;
        }}
        proxy_pass http://jato_fullstack_api;
        proxy_http_version 1.1;
        proxy_buffering off;
        proxy_read_timeout 3600s;
        proxy_send_timeout 3600s;
        add_header Cache-Control "no-store" always;
    }}

    location / {{
        proxy_pass http://jato_fullstack_api;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto http;
        proxy_connect_timeout 10s;
        proxy_send_timeout 600s;
        proxy_read_timeout 600s;
        proxy_buffering off;
    }}
}}
"""
output_path.write_text(payload, encoding="utf-8")
PY
}

render_fresh_site_candidate() {
  python3 - "$NGINX_TEMPLATE" "$SITE_CANDIDATE" "$SERVER_NAME" <<'PY'
from pathlib import Path
import sys

template_path = Path(sys.argv[1])
output_path = Path(sys.argv[2])
server_name = sys.argv[3]
text = template_path.read_text(encoding="utf-8")
if text.count("__SERVER_NAME__") != 1:
    raise SystemExit("[ERROR] Nginx template must contain exactly one __SERVER_NAME__ placeholder")
text = text.replace("__SERVER_NAME__", server_name)
if "__BACKEND_PORT__" in text or "__FRONTEND_ROOT__" in text:
    raise SystemExit("[ERROR] Backend and frontend release values must not live in the site template")
required = (
    "include /etc/jato-fullstack/nginx/active-release.conf;",
    "root $jato_frontend_root;",
    "location = /readyz",
    "location ^~ /v1/msrp/monthly-update",
    "if (-f /var/lib/jato-release/deployment-maintenance)",
)
missing = [value for value in required if value not in text]
if missing:
    raise SystemExit(f"[ERROR] Nginx template is missing blue/green contract entries: {missing}")
output_path.write_text(text, encoding="utf-8")
PY
}

migrate_certbot_site_candidate() {
  python3 - "$TARGET_CONF" "$SITE_CANDIDATE" <<'PY'
from __future__ import annotations

import re
import sys
from pathlib import Path

source_path = Path(sys.argv[1])
output_path = Path(sys.argv[2])
text = source_path.read_text(encoding="utf-8")
include_line = "include /etc/jato-fullstack/nginx/active-release.conf;"

if "managed by Certbot" not in text:
    raise SystemExit("[ERROR] Refusing Certbot migration without its management marker")


def matching_brace(source: str, open_index: int) -> int:
    depth = 0
    for index in range(open_index, len(source)):
        character = source[index]
        if character == "{":
            depth += 1
        elif character == "}":
            depth -= 1
            if depth == 0:
                return index
    return -1


def named_block_spans(source: str, pattern: str) -> list[tuple[int, int]]:
    spans: list[tuple[int, int]] = []
    for match in re.finditer(pattern, source):
        open_index = source.find("{", match.start(), match.end())
        close_index = matching_brace(source, open_index)
        if open_index < 0 or close_index < 0:
            raise SystemExit("[ERROR] Refusing to migrate malformed nginx block structure")
        spans.append((match.start(), close_index + 1))
    return spans


upstream_spans = named_block_spans(
    text,
    r"(?m)^[ \t]*upstream[ \t]+jato_fullstack_api[ \t]*\{",
)
include_count = text.count(include_line)
if include_count > 1 or len(upstream_spans) > 1:
    raise SystemExit("[ERROR] Refusing ambiguous JATO upstream/include migration")
if include_count == 0:
    if len(upstream_spans) != 1:
        raise SystemExit("[ERROR] Existing Certbot config has no unique JATO upstream")
    start, end = upstream_spans[0]
    text = text[:start] + include_line + "\n" + text[end:].lstrip("\n")
elif upstream_spans:
    raise SystemExit("[ERROR] Existing Certbot config defines both the active include and upstream")


def add_ready_location(block: str) -> str:
    if re.search(r"(?m)^[ \t]*location[ \t]+=[ \t]+/readyz[ \t]*\{", block):
        return block
    health_match = re.search(
        r"(?m)^[ \t]*location[ \t]+=[ \t]+/healthz[ \t]*\{",
        block,
    )
    if health_match is None:
        raise SystemExit("[ERROR] JATO Certbot server is missing the /healthz location")
    open_index = block.find("{", health_match.start(), health_match.end())
    close_index = matching_brace(block, open_index)
    if close_index < 0:
        raise SystemExit("[ERROR] JATO /healthz location is malformed")
    indent = re.match(r"[ \t]*", health_match.group(0)).group(0)
    ready_block = (
        f"\n\n{indent}location = /readyz {{\n"
        f"{indent}    proxy_pass http://jato_fullstack_api/readyz;\n"
        f"{indent}    proxy_http_version 1.1;\n"
        f"{indent}    access_log off;\n"
        f'{indent}    add_header Cache-Control "no-store" always;\n'
        f"{indent}}}"
    )
    return block[: close_index + 1] + ready_block + block[close_index + 1 :]


def add_monthly_deployment_gate(block: str) -> str:
    if re.search(
        r"(?m)^[ \t]*location[ \t]+\^~[ \t]+/v1/msrp/monthly-update[ \t]*\{",
        block,
    ):
        durable_gate = "if (-f /var/lib/jato-release/deployment-maintenance)"
        legacy_gate = "if (-f /run/jato/deployment-maintenance)"
        if durable_gate in block:
            return block
        if legacy_gate in block:
            return block.replace(legacy_gate, durable_gate)
        raise SystemExit("[ERROR] Existing monthly route lacks the deployment marker gate")
    api_match = re.search(
        r"(?m)^[ \t]*location[ \t]+\^~[ \t]+/v1/[ \t]*\{",
        block,
    )
    if api_match is None:
        raise SystemExit("[ERROR] JATO Certbot server is missing the /v1/ location")
    indent = re.match(r"[ \t]*", api_match.group(0)).group(0)
    gate = (
        f"{indent}location ^~ /v1/msrp/monthly-update {{\n"
        f"{indent}    if (-f /var/lib/jato-release/deployment-maintenance) {{\n"
        f"{indent}        return 423;\n"
        f"{indent}    }}\n"
        f"{indent}    proxy_pass http://jato_fullstack_api;\n"
        f"{indent}    proxy_http_version 1.1;\n"
        f"{indent}    proxy_buffering off;\n"
        f"{indent}    proxy_read_timeout 3600s;\n"
        f"{indent}    proxy_send_timeout 3600s;\n"
        f'{indent}    add_header Cache-Control "no-store" always;\n'
        f"{indent}}}\n\n"
    )
    return block[: api_match.start()] + gate + block[api_match.start() :]


server_spans = named_block_spans(text, r"(?m)^[ \t]*server[ \t]*\{")
parts: list[str] = []
cursor = 0
jato_server_count = 0
for start, end in server_spans:
    block = text[start:end]
    parts.append(text[cursor:start])
    next_block = block
    if "proxy_pass http://jato_fullstack_api" in block:
        jato_server_count += 1
        if not re.search(r"(?m)^[ \t]*root[ \t]+[^;]+;", block):
            raise SystemExit("[ERROR] JATO Certbot server has no frontend root")
        next_block = re.sub(
            r"(?m)^([ \t]*root[ \t]+)[^;]+;",
            r"\1$jato_frontend_root;",
            next_block,
        )
        next_block = add_ready_location(next_block)
        next_block = add_monthly_deployment_gate(next_block)
    parts.append(next_block)
    cursor = end
parts.append(text[cursor:])
if jato_server_count < 1:
    raise SystemExit("[ERROR] Certbot config does not contain a JATO proxy server")

migrated = "".join(parts)
if "ssl_certificate" not in migrated or "managed by Certbot" not in migrated:
    raise SystemExit("[ERROR] Certbot TLS directives were not preserved")
if migrated.count(include_line) != 1:
    raise SystemExit("[ERROR] Migrated config must contain exactly one active release include")
if re.search(
    r"(?m)^[ \t]*upstream[ \t]+jato_fullstack_api[ \t]*\{",
    migrated,
):
    raise SystemExit("[ERROR] Migrated site still owns the JATO upstream")
output_path.write_text(migrated, encoding="utf-8")
PY
}

snapshot_existing_state() {
  if [[ -f "$TARGET_CONF" ]]; then
    cp -p "$TARGET_CONF" "$TARGET_SNAPSHOT"
    TARGET_EXISTED=true
  fi
  if [[ -f "$ACTIVE_RELEASE_CONF" ]]; then
    cp -p "$ACTIVE_RELEASE_CONF" "$ACTIVE_SNAPSHOT"
    ACTIVE_EXISTED=true
  fi
  if [[ -L "$ENABLED_CONF" ]]; then
    ENABLED_EXISTED=true
    ENABLED_TARGET="$(readlink "$ENABLED_CONF")"
  fi
  if [[ -L "$DEFAULT_ENABLED_CONF" ]]; then
    DEFAULT_EXISTED=true
    DEFAULT_WAS_SYMLINK=true
    DEFAULT_TARGET="$(readlink "$DEFAULT_ENABLED_CONF")"
  elif [[ -f "$DEFAULT_ENABLED_CONF" ]]; then
    DEFAULT_EXISTED=true
    cp -p "$DEFAULT_ENABLED_CONF" "$DEFAULT_SNAPSHOT"
  fi
}

atomic_install() {
  local source_path="$1"
  local target_path="$2"
  local target_dir=""
  local target_name=""
  local temp_path=""

  target_dir="$(dirname "$target_path")"
  target_name="$(basename "$target_path")"
  mkdir -p "$target_dir"
  temp_path="$(mktemp "$target_dir/.${target_name}.XXXXXX")"
  install -m 0644 "$source_path" "$temp_path"
  fsync_regular_file "$temp_path"
  python3 -B - "$temp_path" "$target_path" <<'PY'
import os
import sys

os.replace(sys.argv[1], sys.argv[2])
PY
  fsync_regular_file "$target_path"
  fsync_directory "$target_dir"
}

fsync_regular_file() {
  local file_path="$1"
  python3 -B - "$file_path" <<'PY'
import os
import stat
import sys

path = sys.argv[1]
flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
descriptor = os.open(path, flags)
try:
    mode = os.fstat(descriptor).st_mode
    if not stat.S_ISREG(mode):
        raise SystemExit(f"[ERROR] durable nginx target is not a regular file: {path}")
    os.fsync(descriptor)
finally:
    os.close(descriptor)
PY
}

fsync_directory() {
  local directory="$1"
  python3 -B - "$directory" <<'PY'
import os
import sys

directory = sys.argv[1]
flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
descriptor = os.open(directory, flags)
try:
    os.fsync(descriptor)
finally:
    os.close(descriptor)
PY
}

atomic_symlink() {
  local source_path="$1"
  local target_path="$2"
  local target_dir=""
  local target_name=""
  local temp_path=""

  target_dir="$(dirname "$target_path")"
  target_name="$(basename "$target_path")"
  mkdir -p "$target_dir"
  temp_path="$target_dir/.${target_name}.link.$$"
  rm -f "$temp_path"
  ln -s "$source_path" "$temp_path"
  python3 -B - "$temp_path" "$target_path" <<'PY'
import os
import sys

os.replace(sys.argv[1], sys.argv[2])
PY
  fsync_directory "$target_dir"
}

durable_remove() {
  local target_path="$1"
  local target_dir=""
  target_dir="$(dirname "$target_path")"
  if [[ -e "$target_path" || -L "$target_path" ]]; then
    rm -f "$target_path"
    fsync_directory "$target_dir"
  fi
}

persist_durable_preimage() {
  local preimage_parent=""
  local preimage_name=""
  local staging_dir=""
  if [[ -z "$NGINX_PREIMAGE_DIR" ]]; then
    return 0
  fi
  if [[ "$NGINX_PREIMAGE_DIR" != /* ]] \
    || [[ -L "$NGINX_PREIMAGE_DIR" ]] \
    || [[ -e "$NGINX_PREIMAGE_DIR" ]]; then
    fail "NGINX_PREIMAGE_DIR must be a new absolute non-symlink path"
    return 1
  fi
  preimage_parent="$(dirname "$NGINX_PREIMAGE_DIR")"
  preimage_name="$(basename "$NGINX_PREIMAGE_DIR")"
  if [[ -L "$preimage_parent" ]] \
    || [[ -e "$preimage_parent" && ! -d "$preimage_parent" ]]; then
    fail "NGINX_PREIMAGE_DIR parent is unsafe"
    return 1
  fi
  mkdir -p "$preimage_parent"
  staging_dir="$(mktemp -d "$preimage_parent/.${preimage_name}.XXXXXX")"
  PREIMAGE_STAGING_DIR="$staging_dir"
  chmod 0700 "$staging_dir"

  if [[ "$TARGET_EXISTED" == "true" ]]; then
    cp -p "$TARGET_SNAPSHOT" "$staging_dir/target.conf"
    fsync_regular_file "$staging_dir/target.conf"
  fi
  if [[ "$ACTIVE_EXISTED" == "true" ]]; then
    cp -p "$ACTIVE_SNAPSHOT" "$staging_dir/active-release.conf"
    fsync_regular_file "$staging_dir/active-release.conf"
  fi
  if [[ "$DEFAULT_EXISTED" == "true" && "$DEFAULT_WAS_SYMLINK" != "true" ]]; then
    cp -p "$DEFAULT_SNAPSHOT" "$staging_dir/default.conf"
    fsync_regular_file "$staging_dir/default.conf"
  fi
  python3 -B - \
    "$staging_dir/manifest.json" \
    "$TARGET_CONF" "$TARGET_EXISTED" \
    "$ACTIVE_RELEASE_CONF" "$ACTIVE_EXISTED" \
    "$ENABLED_CONF" "$ENABLED_EXISTED" "$ENABLED_TARGET" \
    "$DEFAULT_ENABLED_CONF" "$DEFAULT_EXISTED" "$DEFAULT_WAS_SYMLINK" \
    "$DEFAULT_TARGET" <<'PY'
import json
import os
from pathlib import Path
import sys

(
    manifest_path,
    target_path,
    target_existed,
    active_path,
    active_existed,
    enabled_path,
    enabled_existed,
    enabled_target,
    default_path,
    default_existed,
    default_was_symlink,
    default_target,
) = sys.argv[1:]
payload = {
    "version": 1,
    "target": {
        "path": target_path,
        "existed": target_existed == "true",
        "kind": "file",
        "backup": "target.conf",
    },
    "active": {
        "path": active_path,
        "existed": active_existed == "true",
        "kind": "file",
        "backup": "active-release.conf",
    },
    "enabled": {
        "path": enabled_path,
        "existed": enabled_existed == "true",
        "kind": "symlink",
        "target": enabled_target,
    },
    "default": {
        "path": default_path,
        "existed": default_existed == "true",
        "kind": "symlink" if default_was_symlink == "true" else "file",
        "target": default_target,
        "backup": "default.conf",
    },
}
path = Path(manifest_path)
def file_mode(path_text: str, existed: bool) -> int:
    if not existed:
        return 0o644
    return Path(path_text).stat().st_mode & 0o777

payload["target"]["mode"] = file_mode(target_path, target_existed == "true")
payload["active"]["mode"] = file_mode(active_path, active_existed == "true")
if default_existed == "true" and default_was_symlink != "true":
    payload["default"]["mode"] = file_mode(default_path, True)
with path.open("x", encoding="utf-8") as handle:
    json.dump(payload, handle, ensure_ascii=False, sort_keys=True)
    handle.write("\n")
    handle.flush()
    os.fsync(handle.fileno())
os.chmod(path, 0o600)
PY
  fsync_directory "$staging_dir"
  python3 -B - "$staging_dir" "$NGINX_PREIMAGE_DIR" <<'PY'
import os
import sys

os.replace(sys.argv[1], sys.argv[2])
parent = os.open(
    os.path.dirname(sys.argv[2]),
    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
)
try:
    os.fsync(parent)
finally:
    os.close(parent)
PY
  echo "[INFO] Preserved durable pre-switch nginx preimage: $NGINX_PREIMAGE_DIR"
  PREIMAGE_STAGING_DIR=""
}

restore_durable_preimage() {
  if [[ -z "$NGINX_PREIMAGE_DIR" ]] \
    || [[ "$NGINX_PREIMAGE_DIR" != /* ]] \
    || [[ -L "$NGINX_PREIMAGE_DIR" ]] \
    || [[ ! -d "$NGINX_PREIMAGE_DIR" ]]; then
    fail "restore-preimage requires a safe durable NGINX_PREIMAGE_DIR"
    return 1
  fi
  python3 -B - \
    "$NGINX_PREIMAGE_DIR" \
    "$TARGET_CONF" "$ACTIVE_RELEASE_CONF" "$ENABLED_CONF" \
    "$DEFAULT_ENABLED_CONF" <<'PY'
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import stat
import sys
import tempfile

preimage = Path(sys.argv[1])
expected_paths = {
    "target": sys.argv[2],
    "active": sys.argv[3],
    "enabled": sys.argv[4],
    "default": sys.argv[5],
}
manifest_path = preimage / "manifest.json"
if manifest_path.is_symlink() or not manifest_path.is_file():
    raise SystemExit("[ERROR] durable nginx preimage manifest is missing or unsafe")
manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
if manifest.get("version") != 1:
    raise SystemExit("[ERROR] durable nginx preimage version is unsupported")


def fsync_directory(directory: Path) -> None:
    descriptor = os.open(
        directory,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def remove_path(target: Path) -> None:
    if target.is_symlink() or target.exists():
        if target.is_dir() and not target.is_symlink():
            raise SystemExit(f"[ERROR] refusing to replace directory: {target}")
        target.unlink()
        fsync_directory(target.parent)


def restore_file(entry: dict[str, object], target: Path) -> None:
    backup = preimage / str(entry["backup"])
    if backup.is_symlink() or not backup.is_file():
        raise SystemExit(f"[ERROR] durable nginx preimage file is unsafe: {backup}")
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.restore.",
        dir=target.parent,
    )
    temporary = Path(temporary_name)
    try:
        with backup.open("rb") as source, os.fdopen(descriptor, "wb") as destination:
            shutil.copyfileobj(source, destination)
            destination.flush()
            os.fsync(destination.fileno())
        os.chmod(temporary, int(entry.get("mode", 0o644)))
        os.replace(temporary, target)
        target_descriptor = os.open(
            target,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            if not stat.S_ISREG(os.fstat(target_descriptor).st_mode):
                raise SystemExit(f"[ERROR] restored nginx target is not regular: {target}")
            os.fsync(target_descriptor)
        finally:
            os.close(target_descriptor)
        fsync_directory(target.parent)
    finally:
        if temporary.exists() or temporary.is_symlink():
            temporary.unlink()


def restore_symlink(entry: dict[str, object], target: Path) -> None:
    link_target = str(entry.get("target") or "")
    if not link_target:
        raise SystemExit(f"[ERROR] durable nginx symlink target is empty: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.parent / f".{target.name}.restore.{os.getpid()}"
    remove_path(temporary)
    temporary.symlink_to(link_target)
    os.replace(temporary, target)
    fsync_directory(target.parent)


for name, expected_path in expected_paths.items():
    entry = manifest.get(name)
    if not isinstance(entry, dict) or entry.get("path") != expected_path:
        raise SystemExit(f"[ERROR] durable nginx preimage path mismatch: {name}")
    target = Path(expected_path)
    if not bool(entry.get("existed")):
        remove_path(target)
    elif entry.get("kind") == "file":
        restore_file(entry, target)
    elif entry.get("kind") == "symlink":
        restore_symlink(entry, target)
    else:
        raise SystemExit(f"[ERROR] durable nginx preimage kind is invalid: {name}")
PY
}

restore_original_state() {
  set +e
  if [[ "$TARGET_EXISTED" == "true" ]]; then
    atomic_install "$TARGET_SNAPSHOT" "$TARGET_CONF"
  else
    durable_remove "$TARGET_CONF"
  fi
  if [[ "$ACTIVE_EXISTED" == "true" ]]; then
    atomic_install "$ACTIVE_SNAPSHOT" "$ACTIVE_RELEASE_CONF"
  else
    durable_remove "$ACTIVE_RELEASE_CONF"
  fi
  if [[ "$ENABLED_EXISTED" == "true" ]]; then
    atomic_symlink "$ENABLED_TARGET" "$ENABLED_CONF"
  else
    durable_remove "$ENABLED_CONF"
  fi
  if [[ "$DEFAULT_EXISTED" == "true" ]]; then
    if [[ "$DEFAULT_WAS_SYMLINK" == "true" ]]; then
      atomic_symlink "$DEFAULT_TARGET" "$DEFAULT_ENABLED_CONF"
    else
      atomic_install "$DEFAULT_SNAPSHOT" "$DEFAULT_ENABLED_CONF"
    fi
  else
    durable_remove "$DEFAULT_ENABLED_CONF"
  fi
  set -e
}

on_exit() {
  local rc=$?
  if [[ "$rc" -ne 0 && "$MUTATION_STARTED" == "true" && "$COMPLETED" != "true" ]]; then
    echo "[WARN] Restoring the previous nginx configuration" >&2
    restore_original_state
    "$NGINX_BIN" -t >/dev/null 2>&1 || true
    if "$SYSTEMCTL_BIN" is-active --quiet nginx >/dev/null 2>&1; then
      "$SYSTEMCTL_BIN" reload nginx >/dev/null 2>&1 || true
    fi
  fi
  if [[ -n "$PREIMAGE_STAGING_DIR" ]]; then
    rm -rf "$PREIMAGE_STAGING_DIR"
  fi
  rm -rf "$WORK_DIR"
  exit "$rc"
}
trap on_exit EXIT

jato_acquire_production_mutation_lock
case "$INSTALL_MODE" in
  install) ;;
  restore-preimage)
    restore_durable_preimage
    "$NGINX_BIN" -t
    if "$SYSTEMCTL_BIN" is-active --quiet nginx; then
      "$SYSTEMCTL_BIN" reload nginx
    else
      "$SYSTEMCTL_BIN" start nginx
    fi
    COMPLETED=true
    echo "[INFO] Restored the exact durable pre-switch nginx preimage"
    exit 0
    ;;
  *)
    fail "Unsupported nginx installer mode: $INSTALL_MODE"
    exit 1
    ;;
esac
validate_inputs

if ! is_truthy "$SKIP_PACKAGE_INSTALL"; then
  echo "[INFO] Install nginx"
  "$APT_GET_BIN" update -y
  "$APT_GET_BIN" install -y nginx
fi

mkdir -p "$(dirname "$TARGET_CONF")" "$(dirname "$ENABLED_CONF")" \
  "$(dirname "$ACTIVE_RELEASE_CONF")" "$BACKUP_DIR"

render_active_release_candidate
if [[ -f "$TARGET_CONF" ]] && grep -qi 'managed by Certbot' "$TARGET_CONF"; then
  CERTBOT_MIGRATION=true
  echo "[INFO] Safely migrate the existing Certbot-managed JATO site"
  migrate_certbot_site_candidate
else
  echo "[INFO] Render the stable JATO site"
  render_fresh_site_candidate
fi

snapshot_existing_state
persist_durable_preimage
if [[ "$TARGET_EXISTED" == "true" ]]; then
  backup_name="$(basename "$TARGET_CONF").pre-bluegreen-$(date -u +%Y%m%dT%H%M%SZ).$$.bak"
  cp -p "$TARGET_CONF" "$BACKUP_DIR/$backup_name"
  echo "[INFO] Preserved nginx backup: $BACKUP_DIR/$backup_name"
fi

MUTATION_STARTED=true
atomic_install "$ACTIVE_CANDIDATE" "$ACTIVE_RELEASE_CONF"
atomic_install "$SITE_CANDIDATE" "$TARGET_CONF"
atomic_symlink "$TARGET_CONF" "$ENABLED_CONF"
if [[ "$CERTBOT_MIGRATION" != "true" ]]; then
  durable_remove "$DEFAULT_ENABLED_CONF"
fi

echo "[INFO] Validate nginx before reload"
"$NGINX_BIN" -t
"$SYSTEMCTL_BIN" enable nginx >/dev/null
if "$SYSTEMCTL_BIN" is-active --quiet nginx; then
  "$SYSTEMCTL_BIN" reload nginx
else
  "$SYSTEMCTL_BIN" start nginx
fi

if ! is_truthy "$SKIP_HEALTH_CHECK"; then
  "$CURL_BIN" --fail --silent --show-error --max-time 20 \
    http://127.0.0.1/healthz >/dev/null
  "$CURL_BIN" --fail --silent --show-error --max-time 20 \
    http://127.0.0.1/readyz >/dev/null
fi

COMPLETED=true
echo "[INFO] JATO nginx blue/green entrypoint is ready"
