#!/usr/bin/env python3
"""Content-addressed V2 release identity, pointers, and garbage collection."""

from __future__ import annotations

from dataclasses import dataclass
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import sys
import tarfile
import tempfile
from typing import Any, Literal


DEPLOY_DIR = Path(__file__).resolve().parent
if str(DEPLOY_DIR) not in sys.path:
    sys.path.insert(0, str(DEPLOY_DIR))

from validate_release_archive import (  # noqa: E402
    ArchiveValidationError,
    validate_archive,
)


SHA40_PATTERN = re.compile(r"^[0-9a-f]{40}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
REPOSITORY_PATTERN = re.compile(
    r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$",
)
SAFE_TEXT_PATTERN = re.compile(r"^[A-Za-z0-9._:@/+\-=]+$")
MAX_MANIFEST_BYTES = 256 * 1024
SLOTS = ("8000", "8001")
POINTER_KINDS = ("current", "previous")
ARCHIVE_CACHE_NAME_PATTERN = re.compile(
    r"^(?P<digest>[0-9a-f]{64})\.tar\.gz(?P<sidecar>\.partial|\.sha256|\.lock)?$"
)

Slot = Literal["8000", "8001"]
PointerKind = Literal["current", "previous"]


class ReleaseStoreError(RuntimeError):
    """One fail-closed release-store contract violation."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, order=True)
class ReleaseIdentity:
    """Stable identity of one immutable complete release archive."""

    commit_sha: str
    archive_sha256: str

    def __post_init__(self) -> None:
        if not SHA40_PATTERN.fullmatch(self.commit_sha):
            raise ReleaseStoreError(
                "commit_sha_invalid",
                "release commit must be one full lowercase Git SHA",
            )
        if not SHA256_PATTERN.fullmatch(self.archive_sha256):
            raise ReleaseStoreError(
                "archive_sha256_invalid",
                "release archive digest must be one lowercase SHA-256",
            )

    def relative_path(self) -> Path:
        return Path(self.commit_sha) / self.archive_sha256


@dataclass(frozen=True)
class ReleaseManifest:
    """Canonical, retry-stable sidecar for one complete release."""

    repository: str
    identity: ReleaseIdentity
    archive_bytes: int
    frontend_artifact_identity: str
    frontend_artifact_checksum: str
    frontend_build_id: str
    build_metadata_sha256: str

    def __post_init__(self) -> None:
        if not REPOSITORY_PATTERN.fullmatch(self.repository):
            raise ReleaseStoreError(
                "repository_invalid",
                "repository must use owner/name format",
            )
        if isinstance(self.archive_bytes, bool) or self.archive_bytes <= 0:
            raise ReleaseStoreError(
                "archive_bytes_invalid",
                "archive byte count must be a positive integer",
            )
        _validate_safe_text(
            self.frontend_artifact_identity,
            field="frontend.artifactIdentity",
        )
        _validate_sha256(
            self.frontend_artifact_checksum,
            field="frontend.artifactChecksum",
        )
        _validate_sha256(
            self.frontend_build_id,
            field="frontend.buildId",
        )
        _validate_sha256(
            self.build_metadata_sha256,
            field="buildMetadataSha256",
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schemaVersion": 1,
            "repository": self.repository,
            "commitSha": self.identity.commit_sha,
            "archive": {
                "bytes": self.archive_bytes,
                "sha256": self.identity.archive_sha256,
            },
            "frontend": {
                "artifactIdentity": self.frontend_artifact_identity,
                "artifactChecksum": self.frontend_artifact_checksum,
                "buildId": self.frontend_build_id,
            },
            "buildMetadataSha256": self.build_metadata_sha256,
        }


@dataclass(frozen=True)
class ReleaseLayout:
    """The only authoritative release and pointer namespace."""

    release_root: Path = Path("/opt/jato/releases")
    slots_root: Path = Path("/opt/jato/slots")
    staging_root: Path = Path("/opt/jato/staging")
    expected_owner_uid: int = 0

    def release_path(self, identity: ReleaseIdentity) -> Path:
        return self.release_root / identity.relative_path()

    def pointer_path(self, slot: Slot, kind: PointerKind) -> Path:
        if slot not in SLOTS:
            raise ReleaseStoreError("slot_invalid", f"unsupported fixed slot: {slot}")
        if kind not in POINTER_KINDS:
            raise ReleaseStoreError(
                "pointer_kind_invalid",
                f"unsupported pointer kind: {kind}",
            )
        return self.slots_root / slot / kind


@dataclass(frozen=True)
class PointerPair:
    current: ReleaseIdentity | None
    previous: ReleaseIdentity | None


@dataclass(frozen=True)
class StoreScan:
    releases: tuple[ReleaseIdentity, ...]
    preserved_unmanaged: tuple[ReleaseIdentity, ...]
    diagnostics: tuple[dict[str, str], ...]


@dataclass(frozen=True)
class GcPlan:
    protected: tuple[ReleaseIdentity, ...]
    removable: tuple[ReleaseIdentity, ...]
    diagnostics: tuple[dict[str, str], ...]


@dataclass(frozen=True)
class ArchiveCacheGcResult:
    """One bounded pass over the transport archive cache."""

    protected: tuple[ReleaseIdentity, ...]
    removed: tuple[ReleaseIdentity, ...]
    removed_paths: tuple[str, ...]
    diagnostics: tuple[dict[str, str], ...]


def _validate_sha256(value: str, *, field: str) -> None:
    if not SHA256_PATTERN.fullmatch(value):
        raise ReleaseStoreError(
            "manifest_field_invalid",
            f"{field} must be one lowercase SHA-256",
        )


def _validate_safe_text(value: str, *, field: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 512
        or not SAFE_TEXT_PATTERN.fullmatch(value)
    ):
        raise ReleaseStoreError(
            "manifest_field_invalid",
            f"{field} contains unsupported characters or length",
        )


def canonical_manifest_bytes(manifest: ReleaseManifest) -> bytes:
    """Serialize a manifest into its only accepted byte representation."""

    return (
        json.dumps(
            manifest.to_dict(),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def manifest_sha256(manifest_bytes: bytes) -> str:
    return hashlib.sha256(manifest_bytes).hexdigest()


def _required_exact_keys(
    payload: dict[str, Any],
    expected: set[str],
    *,
    field: str,
) -> None:
    actual = set(payload)
    if actual != expected:
        missing = sorted(expected - actual)
        unexpected = sorted(actual - expected)
        raise ReleaseStoreError(
            "manifest_shape_invalid",
            f"{field} keys differ: missing={missing} unexpected={unexpected}",
        )


def _required_mapping(payload: dict[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise ReleaseStoreError(
            "manifest_shape_invalid",
            f"{key} must be one JSON object",
        )
    return value


def parse_manifest_bytes(
    payload: bytes,
    *,
    expected_sha256: str | None = None,
) -> ReleaseManifest:
    """Parse and validate one canonical sidecar without execution metadata."""

    if not payload or len(payload) > MAX_MANIFEST_BYTES:
        raise ReleaseStoreError(
            "manifest_size_invalid",
            "release manifest size is outside its bounded range",
        )
    if expected_sha256 is not None:
        _validate_sha256(expected_sha256, field="manifestSha256")
        actual_sha256 = manifest_sha256(payload)
        if actual_sha256 != expected_sha256:
            raise ReleaseStoreError(
                "manifest_sha256_mismatch",
                "release manifest SHA-256 does not match the expected digest",
            )
    try:
        parsed = json.loads(payload.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ReleaseStoreError(
            "manifest_json_invalid",
            "release manifest is not valid UTF-8 JSON",
        ) from exc
    if not isinstance(parsed, dict):
        raise ReleaseStoreError(
            "manifest_shape_invalid",
            "release manifest must be one JSON object",
        )
    _required_exact_keys(
        parsed,
        {
            "schemaVersion",
            "repository",
            "commitSha",
            "archive",
            "frontend",
            "buildMetadataSha256",
        },
        field="manifest",
    )
    if parsed["schemaVersion"] != 1:
        raise ReleaseStoreError(
            "manifest_schema_unsupported",
            "release manifest schemaVersion must be 1",
        )
    archive = _required_mapping(parsed, "archive")
    frontend = _required_mapping(parsed, "frontend")
    _required_exact_keys(archive, {"bytes", "sha256"}, field="archive")
    _required_exact_keys(
        frontend,
        {"artifactIdentity", "artifactChecksum", "buildId"},
        field="frontend",
    )
    if isinstance(archive["bytes"], bool) or not isinstance(archive["bytes"], int):
        raise ReleaseStoreError(
            "archive_bytes_invalid",
            "archive.bytes must be a positive integer",
        )
    try:
        manifest = ReleaseManifest(
            repository=parsed["repository"],
            identity=ReleaseIdentity(
                commit_sha=parsed["commitSha"],
                archive_sha256=archive["sha256"],
            ),
            archive_bytes=archive["bytes"],
            frontend_artifact_identity=frontend["artifactIdentity"],
            frontend_artifact_checksum=frontend["artifactChecksum"],
            frontend_build_id=frontend["buildId"],
            build_metadata_sha256=parsed["buildMetadataSha256"],
        )
    except TypeError as exc:
        raise ReleaseStoreError(
            "manifest_field_invalid",
            "release manifest contains a field with an invalid type",
        ) from exc
    if payload != canonical_manifest_bytes(manifest):
        raise ReleaseStoreError(
            "manifest_not_canonical",
            "release manifest bytes are not in canonical representation",
        )
    return manifest


def read_manifest_file(
    path: Path,
    *,
    expected_sha256: str,
) -> ReleaseManifest:
    """Read a safe regular sidecar and verify its byte digest."""

    try:
        metadata = path.lstat()
    except OSError as exc:
        raise ReleaseStoreError(
            "manifest_unreadable",
            f"cannot inspect release manifest: {path}",
        ) from exc
    if not stat.S_ISREG(metadata.st_mode):
        raise ReleaseStoreError(
            "manifest_path_unsafe",
            "release manifest must be a non-symlink regular file",
        )
    if stat.S_IMODE(metadata.st_mode) & 0o022:
        raise ReleaseStoreError(
            "manifest_permissions_unsafe",
            "release manifest must not be group- or world-writable",
        )
    if metadata.st_size <= 0 or metadata.st_size > MAX_MANIFEST_BYTES:
        raise ReleaseStoreError(
            "manifest_size_invalid",
            "release manifest size is outside its bounded range",
        )
    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise ReleaseStoreError(
            "manifest_unreadable",
            f"cannot read release manifest: {path}",
        ) from exc
    return parse_manifest_bytes(payload, expected_sha256=expected_sha256)


def hash_regular_file(path: Path) -> tuple[int, str]:
    """Hash a stable regular file without following its final symlink."""

    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise ReleaseStoreError(
            "artifact_unreadable",
            f"cannot open artifact as a non-symlink regular file: {path}",
        ) from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_size <= 0:
            raise ReleaseStoreError(
                "artifact_path_unsafe",
                f"artifact must be one non-empty regular file: {path}",
            )
        digest = hashlib.sha256()
        total = 0
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            total += len(chunk)
        after = os.fstat(descriptor)
        identity_before = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        )
        identity_after = (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        )
        if identity_before != identity_after or total != before.st_size:
            raise ReleaseStoreError(
                "artifact_changed_during_hash",
                f"artifact changed while it was hashed: {path}",
            )
        return total, digest.hexdigest()
    except OSError as exc:
        raise ReleaseStoreError(
            "artifact_unreadable",
            f"cannot read artifact: {path}",
        ) from exc
    finally:
        os.close(descriptor)


def write_manifest_file(path: Path, manifest: ReleaseManifest) -> tuple[bool, str]:
    """Create one immutable canonical manifest without overwriting a conflict."""

    payload = canonical_manifest_bytes(manifest)
    digest = manifest_sha256(payload)
    _assert_real_directory(path.parent, code="manifest_parent_unsafe")
    try:
        existing = path.lstat()
    except FileNotFoundError:
        existing = None
    except OSError as exc:
        raise ReleaseStoreError(
            "manifest_unreadable",
            f"cannot inspect manifest output: {path}",
        ) from exc
    if existing is not None:
        if not stat.S_ISREG(existing.st_mode):
            raise ReleaseStoreError(
                "manifest_path_unsafe",
                "manifest output already exists and is not a regular file",
            )
        try:
            existing_payload = path.read_bytes()
        except OSError as exc:
            raise ReleaseStoreError(
                "manifest_unreadable",
                f"cannot read existing manifest output: {path}",
            ) from exc
        if existing_payload != payload:
            raise ReleaseStoreError(
                "manifest_identity_conflict",
                "manifest output already exists with different canonical bytes",
            )
        return False, digest

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        descriptor = -1
        try:
            os.link(temporary_name, path, follow_symlinks=False)
        except FileExistsError:
            try:
                existing_payload = path.read_bytes()
            except OSError as exc:
                raise ReleaseStoreError(
                    "manifest_identity_conflict",
                    "manifest output appeared but cannot be verified",
                ) from exc
            if existing_payload != payload:
                raise ReleaseStoreError(
                    "manifest_identity_conflict",
                    "manifest output appeared with different canonical bytes",
                )
            return False, digest
        _fsync_directory(path.parent)
        return True, digest
    except OSError as exc:
        raise ReleaseStoreError(
            "manifest_write_failed",
            f"cannot create immutable release manifest: {path}",
        ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass


def verify_archive_matches_manifest(
    manifest: ReleaseManifest,
    archive_path: Path,
) -> None:
    """Recompute archive bytes and digest and compare them with the sidecar."""

    archive_bytes, archive_sha256 = hash_regular_file(archive_path)
    if archive_bytes != manifest.archive_bytes:
        raise ReleaseStoreError(
            "archive_bytes_mismatch",
            "archive byte count does not match release manifest",
        )
    if archive_sha256 != manifest.identity.archive_sha256:
        raise ReleaseStoreError(
            "archive_sha256_mismatch",
            "archive SHA-256 does not match release manifest",
        )


def _assert_real_directory(path: Path, *, code: str) -> None:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise ReleaseStoreError(code, f"required directory is unavailable: {path}") from exc
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise ReleaseStoreError(code, f"required path is not a real directory: {path}")


def _assert_pointer_namespace(layout: ReleaseLayout, slot: Slot) -> None:
    for path in (layout.slots_root, layout.slots_root / slot):
        _assert_real_directory(path, code="pointer_parent_unsafe")
        metadata = path.lstat()
        if (
            metadata.st_uid != layout.expected_owner_uid
            or stat.S_IMODE(metadata.st_mode) & 0o022
        ):
            raise ReleaseStoreError(
                "pointer_parent_unsafe",
                f"pointer directory owner or mode is unsafe: {path}",
            )


def validate_release_directory(
    layout: ReleaseLayout,
    identity: ReleaseIdentity,
) -> Path:
    """Require the exact two-level content-addressed release directory."""

    _assert_real_directory(layout.release_root, code="release_root_unsafe")
    commit_root = layout.release_root / identity.commit_sha
    release_path = commit_root / identity.archive_sha256
    _assert_real_directory(commit_root, code="release_commit_root_unsafe")
    _assert_real_directory(release_path, code="release_directory_unsafe")
    try:
        if release_path.resolve(strict=True) != release_path:
            raise ReleaseStoreError(
                "release_directory_unsafe",
                "release directory canonical path differs from its expected path",
            )
    except OSError as exc:
        raise ReleaseStoreError(
            "release_directory_unsafe",
            "release directory cannot be resolved",
        ) from exc
    return release_path


def promote_staged_release(
    layout: ReleaseLayout,
    identity: ReleaseIdentity,
    staging: Path,
    *,
    expected_manifest_sha256: str,
) -> bool:
    """Atomically publish one already validated and built staging directory.

    Archive validation, extraction, dependency installation, and durable-data
    links happen before this boundary.  This function only verifies the V2
    manifest identity and performs the same-filesystem atomic rename.
    """

    _assert_real_directory(layout.release_root, code="release_root_unsafe")
    _assert_real_directory(staging.parent, code="staging_parent_unsafe")
    _assert_real_directory(staging, code="staging_directory_unsafe")
    try:
        staging_real = staging.resolve(strict=True)
        release_root_real = layout.release_root.resolve(strict=True)
    except OSError as exc:
        raise ReleaseStoreError(
            "staging_directory_unsafe",
            "staging or release root cannot be resolved",
        ) from exc
    if staging_real.is_relative_to(release_root_real):
        raise ReleaseStoreError(
            "staging_directory_unsafe",
            "staging must be outside the immutable release root",
        )
    manifest = read_manifest_file(
        staging / "release-v2-manifest.json",
        expected_sha256=expected_manifest_sha256,
    )
    if manifest.identity != identity:
        raise ReleaseStoreError(
            "release_identity_mismatch",
            "staging manifest identity differs from target",
        )
    commit_root = layout.release_root / identity.commit_sha
    target = layout.release_path(identity)
    try:
        commit_root.mkdir(mode=0o755)
    except FileExistsError:
        _assert_real_directory(commit_root, code="release_commit_root_unsafe")
    except OSError as exc:
        raise ReleaseStoreError(
            "release_commit_root_create_failed",
            f"cannot create release commit directory: {commit_root}",
        ) from exc
    try:
        target.lstat()
    except FileNotFoundError:
        try:
            os.replace(staging, target)
            _fsync_directory(commit_root)
        except OSError as exc:
            raise ReleaseStoreError(
                "release_promote_failed",
                "cannot atomically promote staged release",
            ) from exc
        validate_release_directory(layout, identity)
        return True
    except OSError as exc:
        raise ReleaseStoreError(
            "release_directory_unsafe",
            f"cannot inspect existing release directory: {target}",
        ) from exc
    validate_release_directory(layout, identity)
    existing = read_manifest_file(
        target / "release-v2-manifest.json",
        expected_sha256=expected_manifest_sha256,
    )
    if existing != manifest:
        raise ReleaseStoreError(
            "release_identity_conflict",
            "existing release manifest differs from staging",
        )
    return False


def identity_from_release_path(
    layout: ReleaseLayout,
    release_path: Path,
) -> ReleaseIdentity:
    """Parse only an exact release-root/commit/archive directory."""

    try:
        relative = release_path.relative_to(layout.release_root)
    except ValueError as exc:
        raise ReleaseStoreError(
            "pointer_target_outside_store",
            "pointer target is outside the release root",
        ) from exc
    if len(relative.parts) != 2:
        raise ReleaseStoreError(
            "pointer_target_shape_invalid",
            "pointer target must be release-root/commit/archive",
        )
    identity = ReleaseIdentity(relative.parts[0], relative.parts[1])
    validate_release_directory(layout, identity)
    return identity


def read_pointer(
    layout: ReleaseLayout,
    slot: Slot,
    kind: PointerKind,
) -> ReleaseIdentity | None:
    """Resolve one pointer without accepting regular-file or dangling aliases."""

    pointer = layout.pointer_path(slot, kind)
    _assert_pointer_namespace(layout, slot)
    try:
        metadata = pointer.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise ReleaseStoreError(
            "pointer_unreadable",
            f"cannot inspect release pointer: {pointer}",
        ) from exc
    if not stat.S_ISLNK(metadata.st_mode):
        raise ReleaseStoreError(
            "pointer_path_unsafe",
            f"release pointer must be a symlink: {pointer}",
        )
    if metadata.st_uid != layout.expected_owner_uid:
        raise ReleaseStoreError(
            "pointer_path_unsafe",
            f"release pointer owner is unsafe: {pointer}",
        )
    try:
        target = pointer.resolve(strict=True)
    except OSError as exc:
        raise ReleaseStoreError(
            "pointer_dangling",
            f"release pointer is dangling or unreadable: {pointer}",
        ) from exc
    identity = identity_from_release_path(layout, target)
    manifest_path = target / "release-v2-manifest.json"
    _, digest = hash_regular_file(manifest_path)
    manifest = read_manifest_file(manifest_path, expected_sha256=digest)
    if manifest.identity != identity:
        raise ReleaseStoreError(
            "pointer_manifest_identity_mismatch",
            "release pointer manifest identity differs from its target",
        )
    return identity


def read_pointer_pair(layout: ReleaseLayout, slot: Slot) -> PointerPair:
    return PointerPair(
        current=read_pointer(layout, slot, "current"),
        previous=read_pointer(layout, slot, "previous"),
    )


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_symlink(
    layout: ReleaseLayout,
    slot: Slot,
    kind: PointerKind,
    identity: ReleaseIdentity,
) -> None:
    """Atomically replace one pointer with a validated relative symlink."""

    target = validate_release_directory(layout, identity)
    pointer = layout.pointer_path(slot, kind)
    _assert_pointer_namespace(layout, slot)
    try:
        existing = pointer.lstat()
    except FileNotFoundError:
        existing = None
    except OSError as exc:
        raise ReleaseStoreError(
            "pointer_unreadable",
            f"cannot inspect release pointer: {pointer}",
        ) from exc
    if existing is not None and not stat.S_ISLNK(existing.st_mode):
        raise ReleaseStoreError(
            "pointer_path_unsafe",
            f"refusing to replace a non-symlink pointer path: {pointer}",
        )
    if existing is not None and existing.st_uid != layout.expected_owner_uid:
        raise ReleaseStoreError(
            "pointer_path_unsafe",
            f"refusing to replace a pointer owned by another uid: {pointer}",
        )
    relative_target = os.path.relpath(target, pointer.parent)
    temporary_name = ""
    try:
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{kind}.",
            suffix=".tmp",
            dir=pointer.parent,
        )
        os.close(descriptor)
        os.unlink(temporary_name)
        os.symlink(relative_target, temporary_name)
        os.replace(temporary_name, pointer)
        _fsync_directory(pointer.parent)
    except OSError as exc:
        raise ReleaseStoreError(
            "pointer_update_failed",
            f"cannot atomically update release pointer: {pointer}",
        ) from exc
    finally:
        if temporary_name:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass


def clear_pointer(
    layout: ReleaseLayout,
    slot: Slot,
    kind: PointerKind,
) -> bool:
    """Remove one pointer, refusing to delete any regular path."""

    pointer = layout.pointer_path(slot, kind)
    _assert_pointer_namespace(layout, slot)
    try:
        metadata = pointer.lstat()
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise ReleaseStoreError(
            "pointer_unreadable",
            f"cannot inspect release pointer: {pointer}",
        ) from exc
    if not stat.S_ISLNK(metadata.st_mode):
        raise ReleaseStoreError(
            "pointer_path_unsafe",
            f"refusing to remove a non-symlink pointer path: {pointer}",
        )
    if metadata.st_uid != layout.expected_owner_uid:
        raise ReleaseStoreError(
            "pointer_path_unsafe",
            f"refusing to remove a pointer owned by another uid: {pointer}",
        )
    try:
        pointer.unlink()
        _fsync_directory(pointer.parent)
    except OSError as exc:
        raise ReleaseStoreError(
            "pointer_clear_failed",
            f"cannot clear release pointer: {pointer}",
        ) from exc
    return True


def _read_release_manifest(
    layout: ReleaseLayout,
    identity: ReleaseIdentity,
) -> ReleaseManifest:
    """Read the canonical V2 manifest and bind it to its release directory."""

    release = validate_release_directory(layout, identity)
    manifest_path = release / "release-v2-manifest.json"
    _, digest = hash_regular_file(manifest_path)
    manifest = read_manifest_file(manifest_path, expected_sha256=digest)
    if manifest.identity != identity:
        raise ReleaseStoreError(
            "release_manifest_identity_mismatch",
            "release manifest identity differs from its directory",
        )
    return manifest


def scan_release_store(layout: ReleaseLayout) -> StoreScan:
    """Scan only release-root/commit/archive; never follow aliases."""

    _assert_real_directory(layout.release_root, code="release_root_unsafe")
    releases: list[ReleaseIdentity] = []
    preserved_unmanaged: list[ReleaseIdentity] = []
    diagnostics: list[dict[str, str]] = []
    for commit_path in sorted(layout.release_root.iterdir(), key=lambda path: path.name):
        try:
            commit_metadata = commit_path.lstat()
        except OSError as exc:
            diagnostics.append(
                {
                    "code": "release_entry_unreadable",
                    "path": str(commit_path),
                    "message": str(exc),
                }
            )
            continue
        if not SHA40_PATTERN.fullmatch(commit_path.name):
            diagnostics.append(
                {
                    "code": "unexpected_release_root_entry",
                    "path": str(commit_path),
                    "message": "entry is not a full lowercase commit directory",
                }
            )
            continue
        if not stat.S_ISDIR(commit_metadata.st_mode) or stat.S_ISLNK(commit_metadata.st_mode):
            diagnostics.append(
                {
                    "code": "unsafe_release_commit_entry",
                    "path": str(commit_path),
                    "message": "commit entry is not a real directory",
                }
            )
            continue
        for archive_path in sorted(commit_path.iterdir(), key=lambda path: path.name):
            try:
                archive_metadata = archive_path.lstat()
            except OSError as exc:
                diagnostics.append(
                    {
                        "code": "release_entry_unreadable",
                        "path": str(archive_path),
                        "message": str(exc),
                    }
                )
                continue
            if not SHA256_PATTERN.fullmatch(archive_path.name):
                diagnostics.append(
                    {
                        "code": "unexpected_release_commit_entry",
                        "path": str(archive_path),
                        "message": "entry is not a lowercase archive digest directory",
                    }
                )
                continue
            if not stat.S_ISDIR(archive_metadata.st_mode) or stat.S_ISLNK(archive_metadata.st_mode):
                diagnostics.append(
                    {
                        "code": "unsafe_release_archive_entry",
                        "path": str(archive_path),
                        "message": "archive entry is not a real directory",
                    }
                )
                continue
            identity = ReleaseIdentity(commit_path.name, archive_path.name)
            manifest_path = archive_path / "release-v2-manifest.json"
            try:
                manifest_path.lstat()
            except FileNotFoundError:
                preserved_unmanaged.append(identity)
                continue
            except OSError:
                diagnostics.append(
                    {
                        "code": "non_v2_release_entry",
                        "path": str(archive_path),
                        "message": "manifest_unreadable",
                    }
                )
                continue
            try:
                _read_release_manifest(layout, identity)
            except ReleaseStoreError as exc:
                diagnostic_code = (
                    exc.code
                    if exc.code == "release_manifest_identity_mismatch"
                    else "non_v2_release_entry"
                )
                diagnostics.append(
                    {
                        "code": diagnostic_code,
                        "path": str(archive_path),
                        "message": (
                            str(exc)
                            if diagnostic_code == exc.code
                            else exc.code
                        ),
                    }
                )
                continue
            releases.append(identity)
    return StoreScan(
        tuple(releases),
        tuple(preserved_unmanaged),
        tuple(diagnostics),
    )


def protected_release_identities(layout: ReleaseLayout) -> tuple[ReleaseIdentity, ...]:
    """Resolve the four and only four durable release references."""

    protected: set[ReleaseIdentity] = set()
    for slot in SLOTS:
        for kind in POINTER_KINDS:
            identity = read_pointer(layout, slot, kind)
            if identity is not None:
                protected.add(identity)
    return tuple(sorted(protected))


def _delete_release_directory(
    layout: ReleaseLayout,
    identity: ReleaseIdentity,
) -> None:
    """Delete one already selected exact release directory."""

    release = validate_release_directory(layout, identity)
    commit_root = release.parent
    try:
        shutil.rmtree(release)
        _fsync_directory(commit_root)
        if not any(commit_root.iterdir()):
            commit_root.rmdir()
            _fsync_directory(layout.release_root)
    except OSError as exc:
        raise ReleaseStoreError(
            "gc_delete_failed",
            f"cannot remove unreferenced release: {release}",
        ) from exc


def remove_if_unreferenced(
    layout: ReleaseLayout,
    identity: ReleaseIdentity,
) -> bool:
    """Remove one manifest-proven V2 release absent from all four pointers.

    The caller must hold the shared production release lock.  A missing target
    is an idempotent no-op; an unmanaged or damaged target fails closed.
    """

    if identity in protected_release_identities(layout):
        return False
    release = layout.release_path(identity)
    try:
        release.lstat()
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise ReleaseStoreError(
            "release_directory_unsafe",
            f"cannot inspect release directory: {release}",
        ) from exc
    _read_release_manifest(layout, identity)
    if identity in protected_release_identities(layout):
        return False
    _delete_release_directory(layout, identity)
    return True


def plan_garbage_collection(
    layout: ReleaseLayout,
    *,
    additional_protected: tuple[ReleaseIdentity, ...] = (),
) -> GcPlan:
    """Return a mark-and-sweep plan without deleting or moving anything."""

    scan = scan_release_store(layout)
    protected = set(protected_release_identities(layout))
    protected.update(additional_protected)
    removable = tuple(identity for identity in scan.releases if identity not in protected)
    return GcPlan(
        protected=tuple(sorted(protected)),
        removable=removable,
        diagnostics=scan.diagnostics,
    )


def collect_garbage(
    layout: ReleaseLayout,
    *,
    additional_protected: tuple[ReleaseIdentity, ...] = (),
) -> tuple[ReleaseIdentity, ...]:
    """Delete only releases absent from all four pointers.

    The caller must hold the shared production release lock.  Any unexpected
    store entry stops collection so a cleanup never guesses around drift.
    """

    plan = plan_garbage_collection(
        layout,
        additional_protected=additional_protected,
    )
    if plan.diagnostics:
        raise ReleaseStoreError(
            "gc_store_ambiguous",
            "release store contains unexpected entries; refusing collection",
        )
    removed: list[ReleaseIdentity] = []
    for identity in plan.removable:
        if identity in protected_release_identities(layout):
            continue
        _delete_release_directory(layout, identity)
        removed.append(identity)
    return tuple(removed)


def _cache_diagnostic(code: str, path: Path, message: str) -> dict[str, str]:
    return {"code": code, "path": str(path), "message": message}


def _inspect_cache_file(
    path: Path,
    *,
    diagnostics: list[dict[str, str]],
) -> bool:
    """Return true only for a real regular cache file without following it."""

    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return False
    except OSError as exc:
        diagnostics.append(
            _cache_diagnostic("archive_cache_entry_unreadable", path, str(exc))
        )
        return False
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        diagnostics.append(
            _cache_diagnostic(
                "archive_cache_entry_unsafe",
                path,
                "cache entry is not a real regular file",
            )
        )
        return False
    return True


def _acquire_archive_cache_lock(
    lock_path: Path,
    *,
    diagnostics: list[dict[str, str]],
) -> int | None:
    """Acquire one uploader lock without replacing or unlinking its inode."""

    if not _inspect_cache_file(lock_path, diagnostics=diagnostics):
        if not lock_path.exists() and not lock_path.is_symlink():
            diagnostics.append(
                _cache_diagnostic(
                    "archive_cache_lock_missing",
                    lock_path,
                    "cache data is preserved because its uploader lock is missing",
                )
            )
        return None
    flags = os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor: int | None = None
    try:
        descriptor = os.open(lock_path, flags)
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            diagnostics.append(
                _cache_diagnostic(
                    "archive_cache_lock_unsafe",
                    lock_path,
                    "uploader lock is not a regular file",
                )
            )
            os.close(descriptor)
            return None
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return descriptor
    except BlockingIOError:
        diagnostics.append(
            _cache_diagnostic(
                "archive_cache_lock_busy",
                lock_path,
                "cache identity is still in use by an uploader",
            )
        )
    except OSError as exc:
        diagnostics.append(
            _cache_diagnostic("archive_cache_lock_unreadable", lock_path, str(exc))
        )
    if descriptor is not None:
        os.close(descriptor)
    return None


def collect_archive_cache(
    layout: ReleaseLayout,
    cache_root: Path,
) -> ArchiveCacheGcResult:
    """Delete unreferenced transport bytes under exact identity paths only.

    Protection comes exclusively from the four V2 pointers.  Upload lock files
    are permanent coordination inodes: this function acquires them
    nonblocking and never unlinks them.  Unknown, malformed, or unsafe entries
    are preserved and surfaced as diagnostics instead of being guessed about.
    The caller must hold the shared production release lock.
    """

    protected = protected_release_identities(layout)
    try:
        root_metadata = cache_root.lstat()
    except FileNotFoundError:
        return ArchiveCacheGcResult(protected, (), (), ())
    except OSError as exc:
        raise ReleaseStoreError(
            "archive_cache_root_unreadable",
            f"cannot inspect archive cache root: {cache_root}",
        ) from exc
    if stat.S_ISLNK(root_metadata.st_mode) or not stat.S_ISDIR(root_metadata.st_mode):
        raise ReleaseStoreError(
            "archive_cache_root_unsafe",
            f"archive cache root is not a real directory: {cache_root}",
        )

    diagnostics: list[dict[str, str]] = []
    identities: set[ReleaseIdentity] = set()
    unsafe_identities: set[ReleaseIdentity] = set()
    try:
        commit_entries = sorted(cache_root.iterdir(), key=lambda path: path.name)
    except OSError as exc:
        raise ReleaseStoreError(
            "archive_cache_root_unreadable",
            f"cannot enumerate archive cache root: {cache_root}",
        ) from exc
    for commit_path in commit_entries:
        try:
            commit_metadata = commit_path.lstat()
        except OSError as exc:
            diagnostics.append(
                _cache_diagnostic("archive_cache_entry_unreadable", commit_path, str(exc))
            )
            continue
        if not SHA40_PATTERN.fullmatch(commit_path.name):
            diagnostics.append(
                _cache_diagnostic(
                    "archive_cache_path_unmanaged",
                    commit_path,
                    "entry is not an exact lowercase commit directory",
                )
            )
            continue
        if stat.S_ISLNK(commit_metadata.st_mode) or not stat.S_ISDIR(commit_metadata.st_mode):
            diagnostics.append(
                _cache_diagnostic(
                    "archive_cache_path_unsafe",
                    commit_path,
                    "commit entry is not a real directory",
                )
            )
            continue
        try:
            archive_entries = sorted(commit_path.iterdir(), key=lambda path: path.name)
        except OSError as exc:
            diagnostics.append(
                _cache_diagnostic("archive_cache_entry_unreadable", commit_path, str(exc))
            )
            continue
        for entry in archive_entries:
            match = ARCHIVE_CACHE_NAME_PATTERN.fullmatch(entry.name)
            if match is None:
                diagnostics.append(
                    _cache_diagnostic(
                        "archive_cache_path_unmanaged",
                        entry,
                        "entry is not an exact archive identity path",
                    )
                )
                continue
            identity = ReleaseIdentity(commit_path.name, match.group("digest"))
            identities.add(identity)
            if not _inspect_cache_file(entry, diagnostics=diagnostics):
                unsafe_identities.add(identity)

    protected_set = set(protected)
    removed_identities: list[ReleaseIdentity] = []
    removed_paths: list[str] = []
    for identity in sorted(identities):
        if identity in protected_set or identity in unsafe_identities:
            continue
        basename = f"{identity.archive_sha256}.tar.gz"
        identity_root = cache_root / identity.commit_sha
        data_paths = tuple(
            identity_root / f"{basename}{suffix}"
            for suffix in ("", ".partial", ".sha256")
        )
        if not any(path.exists() or path.is_symlink() for path in data_paths):
            continue
        lock_path = identity_root / f"{basename}.lock"
        descriptor = _acquire_archive_cache_lock(
            lock_path,
            diagnostics=diagnostics,
        )
        if descriptor is None:
            continue
        try:
            if identity in set(protected_release_identities(layout)):
                continue
            existing: list[Path] = []
            unsafe = False
            for path in data_paths:
                before = len(diagnostics)
                if _inspect_cache_file(path, diagnostics=diagnostics):
                    existing.append(path)
                elif len(diagnostics) != before:
                    unsafe = True
            if unsafe:
                continue
            removed_for_identity = False
            for path in existing:
                try:
                    path.unlink()
                except OSError as exc:
                    diagnostics.append(
                        _cache_diagnostic("archive_cache_delete_failed", path, str(exc))
                    )
                    continue
                removed_paths.append(str(path))
                removed_for_identity = True
            if removed_for_identity:
                try:
                    _fsync_directory(identity_root)
                except OSError as exc:
                    diagnostics.append(
                        _cache_diagnostic(
                            "archive_cache_fsync_failed",
                            identity_root,
                            str(exc),
                        )
                    )
                removed_identities.append(identity)
        finally:
            os.close(descriptor)

    return ArchiveCacheGcResult(
        protected=protected,
        removed=tuple(removed_identities),
        removed_paths=tuple(removed_paths),
        diagnostics=tuple(diagnostics),
    )
