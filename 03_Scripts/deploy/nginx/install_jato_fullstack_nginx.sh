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
ENABLED_SNAPSHOT="$WORK_DIR/enabled.original"
DEFAULT_SNAPSHOT="$WORK_DIR/default.original"
TARGET_EXISTED=false
TARGET_MUTATION_STARTED=false
ACTIVE_EXISTED=false
ACTIVE_MUTATION_STARTED=false
ENABLED_EXISTED=false
ENABLED_WAS_SYMLINK=false
ENABLED_WAS_REGULAR=false
ENABLED_TARGET=""
ENABLED_REGULAR_SHA256=""
ENABLED_MUTATION_STARTED=false
ENABLED_EXTERNAL_DRIFT=false
ENABLED_ADOPTION_OWNER="$WORK_DIR/enabled-adoption-owner.json"
DEFAULT_EXISTED=false
DEFAULT_WAS_SYMLINK=false
DEFAULT_TARGET=""
DEFAULT_MUTATION_STARTED=false
CERTBOT_MIGRATION=false
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

inspect_regular_enabled_adoption() {
  local mode="$1"
  local adoption_owner_path="${2:-}"
  python3 -B - \
    "$mode" "$ENABLED_CONF" "$TARGET_CONF" \
    "$ENABLED_SNAPSHOT" "$TARGET_SNAPSHOT" \
    "$ENABLED_REGULAR_SHA256" "$adoption_owner_path" <<'PY'
from __future__ import annotations

import ctypes
import hashlib
import json
import os
import platform
import secrets
import stat
import sys

MAX_ADOPTION_BYTES = 4 * 1024 * 1024

(
    mode,
    enabled_path,
    canonical_path,
    enabled_snapshot_path,
    canonical_snapshot_path,
    expected_sha256,
    adoption_owner_path,
) = sys.argv[1:]


def read_regular(path: str) -> tuple[bytes, int]:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise SystemExit(
            f"[ERROR] nginx adoption input is unreadable or unsafe: "
            f"path={path} reason={exc}"
        ) from exc
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise SystemExit(
                f"[ERROR] nginx adoption input is not a regular file: path={path}"
            )
        if metadata.st_size > MAX_ADOPTION_BYTES:
            raise SystemExit(
                f"[ERROR] nginx adoption input exceeds {MAX_ADOPTION_BYTES} bytes: "
                f"path={path} bytes={metadata.st_size}"
            )
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if total > MAX_ADOPTION_BYTES:
                raise SystemExit(
                    f"[ERROR] nginx adoption input grew beyond "
                    f"{MAX_ADOPTION_BYTES} bytes while reading: path={path}"
                )
            chunks.append(chunk)
        return b"".join(chunks), stat.S_IMODE(metadata.st_mode)
    finally:
        os.close(descriptor)


def digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def fsync_directory(directory: str) -> None:
    descriptor = os.open(
        directory,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def exchange_paths(left: str, right: str) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    encoded_left = os.fsencode(left)
    encoded_right = os.fsencode(right)
    if sys.platform.startswith("linux"):
        renameat2 = getattr(libc, "renameat2", None)
        if renameat2 is None:
            raise OSError("libc renameat2 is unavailable")
        renameat2.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        renameat2.restype = ctypes.c_int
        result = renameat2(-100, encoded_left, -100, encoded_right, 2)
    elif sys.platform == "darwin":
        renamex_np = getattr(libc, "renamex_np", None)
        if renamex_np is None:
            raise OSError("libc renamex_np is unavailable")
        renamex_np.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
        renamex_np.restype = ctypes.c_int
        result = renamex_np(encoded_left, encoded_right, 2)
    else:
        raise OSError(
            f"atomic path exchange is unsupported on {platform.system()}"
        )
    if result != 0:
        error_number = ctypes.get_errno()
        raise OSError(
            error_number,
            os.strerror(error_number),
            f"{left} <-> {right}",
        )


def path_identity(path: str) -> tuple[int, int] | None:
    try:
        metadata = os.lstat(path)
    except FileNotFoundError:
        return None
    return metadata.st_dev, metadata.st_ino


class AdoptionRefused(Exception):
    pass


def exchange_adopt() -> None:
    if not adoption_owner_path:
        raise SystemExit("[ERROR] nginx adoption owner path is missing")
    enabled_snapshot, enabled_snapshot_mode = read_regular(
        enabled_snapshot_path
    )
    if digest(enabled_snapshot) != expected_sha256:
        raise SystemExit(
            "[ERROR] nginx adoption snapshot no longer matches its bound digest"
        )

    enabled_directory = os.path.dirname(enabled_path)
    temporary_path = ""
    symlink_identity: tuple[int, int] | None = None
    exchanged = False
    committed = False

    def is_owned_symlink(path: str) -> bool:
        if symlink_identity is None:
            return False
        try:
            metadata = os.lstat(path)
            return (
                stat.S_ISLNK(metadata.st_mode)
                and (metadata.st_dev, metadata.st_ino) == symlink_identity
                and os.readlink(path) == canonical_path
            )
        except (FileNotFoundError, OSError):
            return False

    def remove_owner_marker() -> None:
        try:
            os.unlink(adoption_owner_path)
        except FileNotFoundError:
            return
        fsync_directory(os.path.dirname(adoption_owner_path))

    def restore_failed_exchange() -> bool:
        if not temporary_path or not os.path.lexists(temporary_path):
            print(
                "[ERROR] nginx adoption rollback quarantine is missing; "
                "refusing further mutation",
                file=sys.stderr,
            )
            return False
        if not is_owned_symlink(enabled_path):
            print(
                "[ERROR] enabled nginx path changed after atomic exchange; "
                f"preserved displaced object at {temporary_path}",
                file=sys.stderr,
            )
            return False
        exchange_paths(temporary_path, enabled_path)
        if is_owned_symlink(temporary_path):
            os.unlink(temporary_path)
            fsync_directory(enabled_directory)
            remove_owner_marker()
            return True

        # A writer raced the identity check. Put its object back instead of
        # taking ownership of a path this installer no longer controls.
        exchange_paths(temporary_path, enabled_path)
        fsync_directory(enabled_directory)
        print(
            "[ERROR] enabled nginx path raced atomic rollback; external path "
            f"was restored and displaced object remains at {temporary_path}",
            file=sys.stderr,
        )
        return False

    try:
        for _attempt in range(32):
            candidate = os.path.join(
                enabled_directory,
                f".{os.path.basename(enabled_path)}.adopt."
                f"{os.getpid()}.{secrets.token_hex(12)}",
            )
            try:
                os.symlink(canonical_path, candidate)
            except FileExistsError:
                continue
            temporary_path = candidate
            break
        if not temporary_path:
            raise OSError("could not allocate a private nginx adoption path")

        symlink_identity = path_identity(temporary_path)
        if symlink_identity is None:
            raise OSError("nginx adoption symlink disappeared before exchange")
        owner_payload = json.dumps(
            {
                "version": 1,
                "device": symlink_identity[0],
                "inode": symlink_identity[1],
                "target": canonical_path,
            },
            sort_keys=True,
        ).encode("utf-8")
        owner_descriptor = os.open(
            adoption_owner_path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        try:
            offset = 0
            while offset < len(owner_payload):
                offset += os.write(owner_descriptor, owner_payload[offset:])
            os.fsync(owner_descriptor)
        finally:
            os.close(owner_descriptor)
        fsync_directory(os.path.dirname(adoption_owner_path))

        exchange_paths(temporary_path, enabled_path)
        exchanged = True
        fsync_directory(enabled_directory)
        try:
            displaced, displaced_mode = read_regular(temporary_path)
        except SystemExit as exc:
            print(str(exc), file=sys.stderr)
            raise AdoptionRefused(
                "atomic exchange displaced an unsafe enabled nginx path"
            ) from exc
        if (
            displaced != enabled_snapshot
            or digest(displaced) != expected_sha256
            or displaced_mode != enabled_snapshot_mode
        ):
            print(
                "[ERROR] enabled nginx adoption file changed at atomic "
                "exchange; refusing to overwrite it",
                file=sys.stderr,
            )
            print(
                f"[ERROR] enabled_exchange_sha256={digest(displaced)}",
                file=sys.stderr,
            )
            print(
                f"[ERROR] enabled_snapshot_sha256={digest(enabled_snapshot)}",
                file=sys.stderr,
            )
            print(
                f"[ERROR] enabled_exchange_mode={displaced_mode:04o} "
                f"enabled_snapshot_mode={enabled_snapshot_mode:04o}",
                file=sys.stderr,
            )
            print("[ERROR] difference_content=redacted", file=sys.stderr)
            raise AdoptionRefused("enabled nginx adoption input drifted")

        os.unlink(temporary_path)
        fsync_directory(enabled_directory)
        committed = True
        print(expected_sha256)
    except AdoptionRefused as exc:
        if exchanged and restore_failed_exchange():
            raise SystemExit(75) from exc
        raise SystemExit(76) from exc
    except BaseException as exc:
        if exchanged and not committed:
            restored = restore_failed_exchange()
            raise SystemExit(76 if not restored else 75) from exc
        if temporary_path and is_owned_symlink(temporary_path):
            os.unlink(temporary_path)
            fsync_directory(enabled_directory)
        remove_owner_marker()
        raise


def exchange_restore() -> None:
    if not adoption_owner_path:
        raise SystemExit("[ERROR] nginx adoption owner path is missing")
    snapshot, snapshot_mode = read_regular(enabled_snapshot_path)
    if digest(snapshot) != expected_sha256:
        raise SystemExit(
            "[ERROR] nginx rollback snapshot no longer matches its bound digest"
        )

    owner_flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    owner_descriptor = os.open(adoption_owner_path, owner_flags)
    try:
        owner_metadata = os.fstat(owner_descriptor)
        if not stat.S_ISREG(owner_metadata.st_mode) or owner_metadata.st_size > 4096:
            raise SystemExit("[ERROR] nginx adoption ownership proof is unsafe")
        owner = json.loads(os.read(owner_descriptor, 4097).decode("utf-8"))
    finally:
        os.close(owner_descriptor)
    if owner.get("version") != 1 or owner.get("target") != canonical_path:
        raise SystemExit("[ERROR] nginx adoption ownership proof is invalid")

    enabled_directory = os.path.dirname(enabled_path)
    temporary_path = ""
    restore_identity: tuple[int, int] | None = None
    exchanged = False
    committed = False

    def is_restore_file(path: str) -> bool:
        if restore_identity is None:
            return False
        try:
            metadata = os.lstat(path)
        except FileNotFoundError:
            return False
        return (
            stat.S_ISREG(metadata.st_mode)
            and (metadata.st_dev, metadata.st_ino) == restore_identity
        )

    def is_owned_symlink(path: str) -> bool:
        try:
            metadata = os.lstat(path)
            return (
                stat.S_ISLNK(metadata.st_mode)
                and metadata.st_dev == owner.get("device")
                and metadata.st_ino == owner.get("inode")
                and os.readlink(path) == canonical_path
            )
        except (FileNotFoundError, OSError):
            return False

    def remove_owner_marker() -> None:
        try:
            os.unlink(adoption_owner_path)
        except FileNotFoundError:
            return
        fsync_directory(os.path.dirname(adoption_owner_path))

    def remove_restore_file() -> None:
        if temporary_path and is_restore_file(temporary_path):
            os.unlink(temporary_path)
            fsync_directory(enabled_directory)

    def put_displaced_path_back() -> bool:
        if not temporary_path or not os.path.lexists(temporary_path):
            print(
                "[ERROR] nginx rollback displaced-path quarantine is missing",
                file=sys.stderr,
            )
            return False
        if not is_restore_file(enabled_path):
            print(
                "[ERROR] enabled nginx path changed during atomic rollback; "
                f"preserved displaced object at {temporary_path}",
                file=sys.stderr,
            )
            return False
        exchange_paths(temporary_path, enabled_path)
        if is_restore_file(temporary_path):
            remove_restore_file()
            remove_owner_marker()
            return True

        # A writer raced the ownership check. Exchange once more so its path
        # remains authoritative; preserve the displaced object for recovery.
        exchange_paths(temporary_path, enabled_path)
        fsync_directory(enabled_directory)
        print(
            "[ERROR] enabled nginx path raced atomic rollback; external path "
            f"was restored and displaced object remains at {temporary_path}",
            file=sys.stderr,
        )
        return False

    try:
        for _attempt in range(32):
            candidate = os.path.join(
                enabled_directory,
                f".{os.path.basename(enabled_path)}.restore."
                f"{os.getpid()}.{secrets.token_hex(12)}",
            )
            try:
                restore_descriptor = os.open(
                    candidate,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    snapshot_mode,
                )
            except FileExistsError:
                continue
            temporary_path = candidate
            try:
                offset = 0
                while offset < len(snapshot):
                    offset += os.write(restore_descriptor, snapshot[offset:])
                os.fchmod(restore_descriptor, snapshot_mode)
                os.fsync(restore_descriptor)
            finally:
                os.close(restore_descriptor)
            break
        if not temporary_path:
            raise OSError("could not allocate a private nginx rollback path")

        restore_identity = path_identity(temporary_path)
        if restore_identity is None:
            raise OSError("nginx rollback file disappeared before exchange")
        exchange_paths(temporary_path, enabled_path)
        exchanged = True
        fsync_directory(enabled_directory)
        if not is_owned_symlink(temporary_path):
            print(
                "[ERROR] enabled nginx path is no longer the symlink owned "
                "by this installer; refusing rollback overwrite",
                file=sys.stderr,
            )
            if put_displaced_path_back():
                raise SystemExit(75)
            raise SystemExit(76)

        os.unlink(temporary_path)
        fsync_directory(enabled_directory)
        remove_owner_marker()
        committed = True
        print(expected_sha256)
    except SystemExit:
        raise
    except BaseException as exc:
        if exchanged and not committed:
            restored = put_displaced_path_back()
            raise SystemExit(76 if not restored else 75) from exc
        remove_restore_file()
        raise


if mode == "exchange-adopt":
    exchange_adopt()
    raise SystemExit(0)
if mode == "exchange-restore":
    exchange_restore()
    raise SystemExit(0)


def report_mismatch(enabled: bytes, canonical: bytes) -> None:
    print(
        "[ERROR] Enabled nginx site differs from canonical target; "
        "refusing automatic symlink adoption",
        file=sys.stderr,
    )
    print(f"[ERROR] enabled_path={enabled_path}", file=sys.stderr)
    print(f"[ERROR] canonical_path={canonical_path}", file=sys.stderr)
    print(f"[ERROR] enabled_sha256={digest(enabled)}", file=sys.stderr)
    print(f"[ERROR] canonical_sha256={digest(canonical)}", file=sys.stderr)
    print(
        f"[ERROR] enabled_bytes={len(enabled)} canonical_bytes={len(canonical)}",
        file=sys.stderr,
    )
    common = min(len(canonical), len(enabled))
    first_byte = next(
        (index for index in range(common) if canonical[index] != enabled[index]),
        common,
    )
    first_line = canonical[:first_byte].count(b"\n") + 1
    canonical_line_count = canonical.count(b"\n") + 1
    enabled_line_count = enabled.count(b"\n") + 1
    print(
        f"[ERROR] first_difference_byte={first_byte} "
        f"first_difference_line={first_line}",
        file=sys.stderr,
    )
    print(
        f"[ERROR] canonical_lines={canonical_line_count} "
        f"enabled_lines={enabled_line_count}",
        file=sys.stderr,
    )
    print(
        "[ERROR] difference_content=redacted; compare the reported paths "
        "offline using the bound SHA-256 values",
        file=sys.stderr,
    )


enabled, enabled_mode = read_regular(enabled_path)

if mode == "validate":
    canonical, _canonical_mode = read_regular(canonical_path)
    if enabled != canonical:
        report_mismatch(enabled, canonical)
        raise SystemExit(1)
    print(digest(enabled))
    raise SystemExit(0)

if len(expected_sha256) != 64:
    raise SystemExit("[ERROR] nginx adoption validation digest is missing or invalid")

enabled_snapshot, enabled_snapshot_mode = read_regular(enabled_snapshot_path)
if mode == "revalidate-enabled":
    if (
        digest(enabled) != expected_sha256
        or enabled != enabled_snapshot
        or enabled_mode != enabled_snapshot_mode
    ):
        print(
            "[ERROR] enabled nginx adoption file changed immediately before "
            "symlink replacement; refusing to overwrite it",
            file=sys.stderr,
        )
        print(f"[ERROR] enabled_current_sha256={digest(enabled)}", file=sys.stderr)
        print(
            f"[ERROR] enabled_snapshot_sha256={digest(enabled_snapshot)}",
            file=sys.stderr,
        )
        print(
            f"[ERROR] enabled_mode={enabled_mode:04o} "
            f"enabled_snapshot_mode={enabled_snapshot_mode:04o}",
            file=sys.stderr,
        )
        print(f"[ERROR] expected_sha256={expected_sha256}", file=sys.stderr)
        raise SystemExit(1)
    print(expected_sha256)
    raise SystemExit(0)

if mode != "revalidate":
    raise SystemExit(f"[ERROR] unsupported nginx adoption inspection mode: {mode}")

canonical, canonical_mode = read_regular(canonical_path)
canonical_snapshot, canonical_snapshot_mode = read_regular(canonical_snapshot_path)
payloads = {
    "enabled_current": enabled,
    "canonical_current": canonical,
    "enabled_snapshot": enabled_snapshot,
    "canonical_snapshot": canonical_snapshot,
}
digests = {name: digest(payload) for name, payload in payloads.items()}
if any(value != expected_sha256 for value in digests.values()) \
        or len(set(payloads.values())) != 1 \
        or enabled_mode != enabled_snapshot_mode \
        or canonical_mode != canonical_snapshot_mode:
    print(
        "[ERROR] nginx adoption inputs changed after validation; "
        "refusing mutation",
        file=sys.stderr,
    )
    for name, value in digests.items():
        print(f"[ERROR] {name}_sha256={value}", file=sys.stderr)
    print(
        f"[ERROR] enabled_mode={enabled_mode:04o} "
        f"enabled_snapshot_mode={enabled_snapshot_mode:04o}",
        file=sys.stderr,
    )
    print(
        f"[ERROR] canonical_mode={canonical_mode:04o} "
        f"canonical_snapshot_mode={canonical_snapshot_mode:04o}",
        file=sys.stderr,
    )
    print(f"[ERROR] expected_sha256={expected_sha256}", file=sys.stderr)
    raise SystemExit(1)
print(expected_sha256)
PY
}

verify_regular_enabled_preimage_unchanged() {
  local verified_sha256=""
  if [[ "$ENABLED_WAS_REGULAR" != "true" ]]; then
    return 0
  fi
  verified_sha256="$(inspect_regular_enabled_adoption revalidate)" || return 1
  if [[ "$verified_sha256" != "$ENABLED_REGULAR_SHA256" ]]; then
    fail "nginx adoption revalidation returned an unexpected digest"
    return 1
  fi
  echo "[INFO] Revalidated identical regular enabled/canonical nginx files: sha256=$verified_sha256"
}

adopt_enabled_site() {
  local adopted_sha256=""
  local adoption_rc=0
  if [[ "$ENABLED_WAS_REGULAR" != "true" ]]; then
    atomic_symlink \
      "$TARGET_CONF" "$ENABLED_CONF" ENABLED_MUTATION_STARTED
    return 0
  fi

  # The owner marker binds rollback to the exact symlink inode prepared by the
  # atomic exchange. Setting the flag first is safe: rollback will never touch
  # an enabled path that does not match this ownership proof.
  ENABLED_MUTATION_STARTED=true
  if adopted_sha256="$(
    inspect_regular_enabled_adoption exchange-adopt "$ENABLED_ADOPTION_OWNER"
  )"; then
    if [[ "$adopted_sha256" != "$ENABLED_REGULAR_SHA256" ]]; then
      fail "atomic nginx adoption returned an unexpected digest"
      return 1
    fi
    echo "[INFO] Atomically adopted identical regular enabled-site: sha256=$adopted_sha256"
    return 0
  else
    adoption_rc=$?
  fi
  ENABLED_EXTERNAL_DRIFT=true
  if [[ "$adoption_rc" -eq 75 ]]; then
    ENABLED_MUTATION_STARTED=false
  fi
  return "$adoption_rc"
}

restore_owned_regular_enabled_adoption() {
  local restored_sha256=""
  local restore_rc=0
  if restored_sha256="$(
    inspect_regular_enabled_adoption exchange-restore "$ENABLED_ADOPTION_OWNER"
  )"; then
    if [[ "$restored_sha256" != "$ENABLED_REGULAR_SHA256" ]]; then
      fail "atomic nginx adoption rollback returned an unexpected digest"
      return 1
    fi
    return 0
  else
    restore_rc=$?
  fi
  ENABLED_EXTERNAL_DRIFT=true
  return "$restore_rc"
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
    if [[ ! -f "$ENABLED_CONF" ]]; then
      fail "Enabled JATO site must be a regular file or symlink: $ENABLED_CONF"
    fi
    if [[ ! -f "$TARGET_CONF" ]]; then
      fail "Regular enabled JATO site requires a canonical target file: $TARGET_CONF"
    fi
    ENABLED_REGULAR_SHA256="$(inspect_regular_enabled_adoption validate)" \
      || return 1
    ENABLED_WAS_REGULAR=true
    echo "[INFO] Approved one-time regular enabled-site adoption: sha256=$ENABLED_REGULAR_SHA256"
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
    ENABLED_WAS_SYMLINK=true
    ENABLED_TARGET="$(readlink "$ENABLED_CONF")"
  elif [[ -f "$ENABLED_CONF" ]]; then
    ENABLED_EXISTED=true
    ENABLED_WAS_REGULAR=true
    cp -p "$ENABLED_CONF" "$ENABLED_SNAPSHOT"
  elif [[ -e "$ENABLED_CONF" ]]; then
    fail "Enabled JATO site changed to an unsupported file type"
    return 1
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

atomic_install_with_mode() {
  local source_path="$1"
  local target_path="$2"
  local install_mode="$3"
  local mutation_flag="${4:-}"
  local target_dir=""
  local target_name=""
  local temp_path=""

  if [[ ! "$install_mode" =~ ^0?[0-7]{3}$ ]]; then
    fail "atomic install mode is invalid: $install_mode"
    return 1
  fi
  target_dir="$(dirname "$target_path")"
  target_name="$(basename "$target_path")"
  mkdir -p "$target_dir"
  temp_path="$(mktemp "$target_dir/.${target_name}.XXXXXX")"
  if ! install -m "$install_mode" "$source_path" "$temp_path"; then
    rm -f "$temp_path"
    return 1
  fi
  if ! fsync_regular_file "$temp_path"; then
    rm -f "$temp_path"
    return 1
  fi
  if ! python3 -B - "$temp_path" "$target_path" <<'PY'
import os
import sys

os.replace(sys.argv[1], sys.argv[2])
PY
  then
    rm -f "$temp_path"
    return 1
  fi
  if [[ -n "$mutation_flag" ]]; then
    printf -v "$mutation_flag" '%s' true
  fi
  fsync_regular_file "$target_path"
  fsync_directory "$target_dir"
}

atomic_install() {
  atomic_install_with_mode "$1" "$2" 0644 "${3:-}"
}

atomic_restore_file() {
  local source_path="$1"
  local target_path="$2"
  local source_mode=""
  source_mode="$(python3 -B - "$source_path" <<'PY'
import os
import stat
import sys

path = sys.argv[1]
flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
descriptor = os.open(path, flags)
try:
    metadata = os.fstat(descriptor)
    if not stat.S_ISREG(metadata.st_mode):
        raise SystemExit(f"[ERROR] rollback source is not a regular file: {path}")
    print(f"{stat.S_IMODE(metadata.st_mode):04o}")
finally:
    os.close(descriptor)
PY
)" || return 1
  atomic_install_with_mode "$source_path" "$target_path" "$source_mode"
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
  local mutation_flag="${3:-}"
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
  if [[ -n "$mutation_flag" ]]; then
    printf -v "$mutation_flag" '%s' true
  fi
  fsync_directory "$target_dir"
}

durable_remove() {
  local target_path="$1"
  local mutation_flag="${2:-}"
  local target_dir=""
  target_dir="$(dirname "$target_path")"
  if [[ -e "$target_path" || -L "$target_path" ]]; then
    rm -f "$target_path"
    if [[ -n "$mutation_flag" ]]; then
      printf -v "$mutation_flag" '%s' true
    fi
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
  if [[ "$ENABLED_EXISTED" == "true" \
    && "$ENABLED_WAS_SYMLINK" != "true" ]]; then
    cp -p "$ENABLED_SNAPSHOT" "$staging_dir/enabled.conf"
    fsync_regular_file "$staging_dir/enabled.conf"
  fi
  if [[ "$DEFAULT_EXISTED" == "true" && "$DEFAULT_WAS_SYMLINK" != "true" ]]; then
    cp -p "$DEFAULT_SNAPSHOT" "$staging_dir/default.conf"
    fsync_regular_file "$staging_dir/default.conf"
  fi
  python3 -B - \
    "$staging_dir/manifest.json" \
    "$TARGET_CONF" "$TARGET_EXISTED" \
    "$ACTIVE_RELEASE_CONF" "$ACTIVE_EXISTED" \
    "$ENABLED_CONF" "$ENABLED_EXISTED" "$ENABLED_WAS_SYMLINK" \
    "$ENABLED_TARGET" \
    "$DEFAULT_ENABLED_CONF" "$DEFAULT_EXISTED" "$DEFAULT_WAS_SYMLINK" \
    "$DEFAULT_TARGET" <<'PY'
import json
import os
from pathlib import Path
import stat
import sys

(
    manifest_path,
    target_path,
    target_existed,
    active_path,
    active_existed,
    enabled_path,
    enabled_existed,
    enabled_was_symlink,
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
        "kind": "symlink" if enabled_was_symlink == "true" else "file",
        "target": enabled_target,
        "backup": "enabled.conf",
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
def snapshot_mode(backup_name: str, existed: bool) -> int:
    if not existed:
        return 0o644
    backup = path.parent / backup_name
    metadata = backup.lstat()
    if backup.is_symlink() or not stat.S_ISREG(metadata.st_mode):
        raise SystemExit(
            f"[ERROR] durable nginx preimage snapshot is unsafe: {backup}"
        )
    return metadata.st_mode & 0o777

payload["target"]["mode"] = snapshot_mode(
    "target.conf", target_existed == "true"
)
payload["active"]["mode"] = snapshot_mode(
    "active-release.conf", active_existed == "true"
)
if enabled_existed == "true" and enabled_was_symlink != "true":
    payload["enabled"]["mode"] = snapshot_mode("enabled.conf", True)
if default_existed == "true" and default_was_symlink != "true":
    payload["default"]["mode"] = snapshot_mode("default.conf", True)
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

verify_original_state() {
  python3 -B - \
    "$ENABLED_EXTERNAL_DRIFT" "$ENABLED_CONF" \
    "$TARGET_CONF" "$TARGET_EXISTED" "false" "" "$TARGET_SNAPSHOT" \
    "$TARGET_MUTATION_STARTED" \
    "$ACTIVE_RELEASE_CONF" "$ACTIVE_EXISTED" "false" "" "$ACTIVE_SNAPSHOT" \
    "$ACTIVE_MUTATION_STARTED" \
    "$ENABLED_CONF" "$ENABLED_EXISTED" "$ENABLED_WAS_SYMLINK" \
    "$ENABLED_TARGET" "$ENABLED_SNAPSHOT" "$ENABLED_MUTATION_STARTED" \
    "$DEFAULT_ENABLED_CONF" "$DEFAULT_EXISTED" "$DEFAULT_WAS_SYMLINK" \
    "$DEFAULT_TARGET" "$DEFAULT_SNAPSHOT" "$DEFAULT_MUTATION_STARTED" <<'PY'
from __future__ import annotations

import os
import stat
import sys


def read_regular(path: str) -> tuple[bytes, int]:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise SystemExit(
                f"[ERROR] restored nginx path is not regular: {path}"
            )
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        return b"".join(chunks), stat.S_IMODE(metadata.st_mode)
    finally:
        os.close(descriptor)


external_enabled_drift = sys.argv[1] == "true"
enabled_path = sys.argv[2]
arguments = sys.argv[3:]
if len(arguments) != 24:
    raise SystemExit("[ERROR] rollback verifier received an invalid contract")
for index in range(0, len(arguments), 6):
    path, existed, was_symlink, link_target, snapshot, mutated = (
        arguments[index:index + 6]
    )
    if mutated != "true":
        continue
    if external_enabled_drift and path == enabled_path:
        continue
    present = os.path.lexists(path)
    if existed != "true":
        if present:
            raise SystemExit(
                f"[ERROR] rollback left an unexpected nginx path: {path}"
            )
        continue
    if not present:
        raise SystemExit(f"[ERROR] rollback did not restore nginx path: {path}")
    metadata = os.lstat(path)
    if was_symlink == "true":
        if not stat.S_ISLNK(metadata.st_mode) or os.readlink(path) != link_target:
            raise SystemExit(
                f"[ERROR] rollback did not restore exact nginx symlink: {path}"
            )
        continue
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise SystemExit(
            f"[ERROR] rollback did not restore regular nginx file: {path}"
        )
    current_payload, current_mode = read_regular(path)
    snapshot_payload, snapshot_mode = read_regular(snapshot)
    if current_payload != snapshot_payload or current_mode != snapshot_mode:
        raise SystemExit(
            f"[ERROR] rollback did not restore exact nginx bytes/mode: {path}"
        )
PY
}

restore_original_state() {
  local restore_rc=0
  set +e
  if [[ "$TARGET_MUTATION_STARTED" == "true" ]]; then
    if [[ "$TARGET_EXISTED" == "true" ]]; then
      atomic_restore_file "$TARGET_SNAPSHOT" "$TARGET_CONF" || restore_rc=1
    else
      durable_remove "$TARGET_CONF" || restore_rc=1
    fi
  fi
  if [[ "$ACTIVE_MUTATION_STARTED" == "true" ]]; then
    if [[ "$ACTIVE_EXISTED" == "true" ]]; then
      atomic_restore_file "$ACTIVE_SNAPSHOT" "$ACTIVE_RELEASE_CONF" || restore_rc=1
    else
      durable_remove "$ACTIVE_RELEASE_CONF" || restore_rc=1
    fi
  fi
  if [[ "$ENABLED_MUTATION_STARTED" == "true" ]]; then
    if [[ "$ENABLED_WAS_REGULAR" == "true" ]]; then
      restore_owned_regular_enabled_adoption || restore_rc=1
    else
      if [[ "$ENABLED_EXISTED" == "true" ]]; then
        if [[ "$ENABLED_WAS_SYMLINK" == "true" ]]; then
          atomic_symlink "$ENABLED_TARGET" "$ENABLED_CONF" || restore_rc=1
        else
          atomic_restore_file "$ENABLED_SNAPSHOT" "$ENABLED_CONF" || restore_rc=1
        fi
      else
        durable_remove "$ENABLED_CONF" || restore_rc=1
      fi
    fi
  fi
  if [[ "$DEFAULT_MUTATION_STARTED" == "true" ]]; then
    if [[ "$DEFAULT_EXISTED" == "true" ]]; then
      if [[ "$DEFAULT_WAS_SYMLINK" == "true" ]]; then
        atomic_symlink "$DEFAULT_TARGET" "$DEFAULT_ENABLED_CONF" || restore_rc=1
      else
        atomic_restore_file "$DEFAULT_SNAPSHOT" "$DEFAULT_ENABLED_CONF" \
          || restore_rc=1
      fi
    else
      durable_remove "$DEFAULT_ENABLED_CONF" || restore_rc=1
    fi
  fi
  if [[ "$restore_rc" -eq 0 ]]; then
    verify_original_state || restore_rc=1
  fi
  set -e
  if [[ "$restore_rc" -ne 0 ]]; then
    fail "nginx rollback could not restore the exact original bytes, modes, and path types"
    return 1
  fi
  if [[ "$ENABLED_EXTERNAL_DRIFT" == "true" ]]; then
    fail "enabled nginx file changed outside this installer and was intentionally not overwritten during rollback"
    return 1
  fi
}

on_exit() {
  local rc=$?
  local owned_mutation=false
  if [[ "$TARGET_MUTATION_STARTED" == "true" \
    || "$ACTIVE_MUTATION_STARTED" == "true" \
    || "$ENABLED_MUTATION_STARTED" == "true" \
    || "$DEFAULT_MUTATION_STARTED" == "true" ]]; then
    owned_mutation=true
  fi
  if [[ "$rc" -ne 0 \
    && "$owned_mutation" == "true" \
    && "$COMPLETED" != "true" ]]; then
    echo "[WARN] Restoring the previous nginx configuration" >&2
    if restore_original_state; then
      "$NGINX_BIN" -t >/dev/null 2>&1 || true
      if "$SYSTEMCTL_BIN" is-active --quiet nginx >/dev/null 2>&1; then
        "$SYSTEMCTL_BIN" reload nginx >/dev/null 2>&1 || true
      fi
    else
      echo "[ERROR] Automatic nginx rollback failed closed; use the durable preimage before any retry: ${NGINX_PREIMAGE_DIR:-not-configured}" >&2
      rc=90
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
verify_regular_enabled_preimage_unchanged
persist_durable_preimage
if [[ "$TARGET_EXISTED" == "true" ]]; then
  backup_name="$(basename "$TARGET_CONF").pre-bluegreen-$(date -u +%Y%m%dT%H%M%SZ).$$.bak"
  cp -p "$TARGET_CONF" "$BACKUP_DIR/$backup_name"
  echo "[INFO] Preserved nginx backup: $BACKUP_DIR/$backup_name"
fi
if [[ "$ENABLED_EXISTED" == "true" \
  && "$ENABLED_WAS_SYMLINK" != "true" ]]; then
  enabled_backup_name="$(basename "$ENABLED_CONF").pre-bluegreen-enabled-$(date -u +%Y%m%dT%H%M%SZ).$$.bak"
  cp -p "$ENABLED_SNAPSHOT" "$BACKUP_DIR/$enabled_backup_name"
  echo "[INFO] Preserved enabled-site backup: $BACKUP_DIR/$enabled_backup_name"
fi

verify_regular_enabled_preimage_unchanged
atomic_install \
  "$ACTIVE_CANDIDATE" "$ACTIVE_RELEASE_CONF" ACTIVE_MUTATION_STARTED
atomic_install "$SITE_CANDIDATE" "$TARGET_CONF" TARGET_MUTATION_STARTED
adopt_enabled_site
if [[ "$CERTBOT_MIGRATION" != "true" ]]; then
  durable_remove "$DEFAULT_ENABLED_CONF" DEFAULT_MUTATION_STARTED
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
