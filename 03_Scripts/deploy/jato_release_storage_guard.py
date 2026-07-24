#!/usr/bin/env python3
"""Fail-closed host resource checks for Tencent blue/green releases.

Only immutable releases that can be proven settled and unreferenced are
eligible for garbage collection.  The helper deliberately keeps checkpoints,
runtime data, upload archives, and every ambiguous path out of scope.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
import errno
import json
import math
import os
from pathlib import Path
import re
import secrets
import shutil
import stat
import subprocess
import sys
import tempfile
import time

from jato_bluegreen_boot_reconcile import (
    ReconcileError,
    read_active_frontend_root,
    read_active_slot,
)
from release_checkpoint import (
    CheckpointError,
    DANGEROUS_RETRY_CLASSES,
    ReleaseIdentity,
    cross_release_is_settled,
    load_checkpoint,
)


GIB = 1024**3
COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
ARCHIVE_PATTERN = re.compile(r"^[0-9a-f]{64}$")
IDENTITY_FILE_NAME = ".jato-release-identity"
MAX_IDENTITY_BYTES = 512
QUARANTINE_PATTERN = re.compile(
    r"^\.gc-(?P<archive>[0-9a-f]{64})-(?P<nonce>[0-9a-f]{16})$"
)
QUARANTINE_MARKER_PATTERN = re.compile(
    r"^(?P<quarantine>\.gc-[0-9a-f]{64}-[0-9a-f]{16})\.marker\.json$"
)
QUARANTINE_MARKER_TEMP_PATTERN = re.compile(
    r"^\.gc-[0-9a-f]{64}-[0-9a-f]{16}\.marker\.json\."
    r"[A-Za-z0-9_-]{6,}\.tmp$"
)
MAX_MARKER_BYTES = 2048
MOUNTINFO_ESCAPE_PATTERN = re.compile(r"\\([0-7]{3})")


class StorageGuardError(RuntimeError):
    """Raised when resource state cannot be proven safe."""


class MissingReleaseIdentityError(StorageGuardError):
    """Raised when an immutable release identity file is absent."""


@dataclass(frozen=True)
class ReleaseEntry:
    path: Path
    commit_sha: str
    archive_sha256: str
    modified_ns: int
    device: int
    inode: int

    @property
    def release_id(self) -> str:
        return f"{self.commit_sha}/{self.archive_sha256}"


@dataclass(frozen=True)
class QuarantineEntry:
    release: ReleaseEntry
    quarantine_path: Path
    marker_path: Path
    original_path: Path
    renamed: bool


@dataclass(frozen=True)
class ReleaseInventory:
    releases: tuple[ReleaseEntry, ...]
    quarantines: tuple[QuarantineEntry, ...]
    orphan_markers: tuple[Path, ...]


@dataclass(frozen=True)
class CheckpointInventory:
    settled_release_ids: frozenset[str]
    protected_release_ids: frozenset[str]
    current_payload: Mapping[str, object]


@dataclass(frozen=True)
class FilesystemUsage:
    available_bytes: int
    total_bytes: int


@dataclass(frozen=True)
class StorageGuardReport:
    available_before_bytes: int
    available_after_bytes: int
    filesystem_total_bytes: int
    minimum_available_bytes: int
    minimum_available_percent: float
    protected_release_ids: tuple[str, ...]
    retained_unreferenced_release_ids: tuple[str, ...]
    pruned_release_ids: tuple[str, ...]
    resumed_quarantine_release_ids: tuple[str, ...]

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=True, sort_keys=True)


@dataclass(frozen=True)
class MemoryBudgetReport:
    total_bytes: int
    available_bytes: int
    active_bytes: int
    active_memory_high_bytes: int
    active_memory_max_bytes: int
    candidate_max_bytes: int
    os_reserve_bytes: int

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=True, sort_keys=True)


def _absolute(path: Path) -> Path:
    if not path.is_absolute():
        raise StorageGuardError(f"path must be absolute: {path}")
    return Path(os.path.abspath(path))


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _real_directory(path: Path, label: str) -> Path:
    path = _absolute(path)
    try:
        metadata = path.lstat()
    except FileNotFoundError as exc:
        raise StorageGuardError(f"{label} does not exist: {path}") from exc
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise StorageGuardError(f"{label} must be a real directory: {path}")
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise StorageGuardError(f"cannot resolve {label}: {path}") from exc
    if resolved != path:
        raise StorageGuardError(f"{label} traverses a symlink: {path} -> {resolved}")
    return path


def _ensure_real_directory(path: Path) -> Path:
    """Create a release root while rejecting symlinks in existing components."""

    path = _absolute(path)
    if path == Path("/"):
        raise StorageGuardError("release root must not be the filesystem root")
    missing: list[Path] = []
    current = path
    while True:
        try:
            metadata = current.lstat()
        except FileNotFoundError:
            missing.append(current)
            if current.parent == current:
                raise StorageGuardError(
                    f"cannot find an existing parent for release root: {path}"
                )
            current = current.parent
            continue
        if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
            raise StorageGuardError(
                f"release root ancestor must be a real directory: {current}"
            )
        break
    for directory in reversed(missing):
        try:
            directory.mkdir(mode=0o755)
        except FileExistsError:
            pass
        metadata = directory.lstat()
        if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
            raise StorageGuardError(
                f"created release root component is unsafe: {directory}"
            )
        _fsync_directory(directory.parent)
    return _real_directory(path, "release root")


def _read_small_regular_file(path: Path, *, limit: int, label: str) -> bytes:
    try:
        before = path.lstat()
    except FileNotFoundError as exc:
        raise StorageGuardError(f"{label} is missing: {path}") from exc
    if (
        not stat.S_ISREG(before.st_mode)
        or stat.S_ISLNK(before.st_mode)
        or before.st_size > limit
    ):
        raise StorageGuardError(
            f"{label} must be a small regular non-symlink file: {path}"
        )
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise StorageGuardError(f"{label} changed while being opened: {path}")
        chunks: list[bytes] = []
        remaining = limit + 1
        while remaining:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
    finally:
        os.close(descriptor)
    if len(raw) > limit:
        raise StorageGuardError(f"{label} exceeds {limit} bytes: {path}")
    return raw


def _expected_identity(commit_sha: str, archive_sha256: str) -> str:
    return f"commit={commit_sha} archive={archive_sha256}"


def _read_identity(path: Path) -> str:
    try:
        raw = _read_small_regular_file(
            path,
            limit=MAX_IDENTITY_BYTES,
            label="release identity",
        )
    except StorageGuardError as exc:
        if not path.exists() and not path.is_symlink():
            raise MissingReleaseIdentityError(str(exc)) from exc
        raise
    try:
        return raw.decode("utf-8").strip()
    except UnicodeDecodeError as exc:
        raise StorageGuardError(f"release identity is not UTF-8: {path}") from exc


def _write_quarantine_marker(
    marker_path: Path,
    *,
    release: ReleaseEntry,
    quarantine_name: str,
) -> None:
    payload = {
        "schemaVersion": 1,
        "releaseId": release.release_id,
        "commit": release.commit_sha,
        "archiveSha256": release.archive_sha256,
        "device": release.device,
        "inode": release.inode,
        "quarantineName": quarantine_name,
    }
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f"{marker_path.name}.",
        suffix=".tmp",
        dir=marker_path.parent,
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, separators=(",", ":"), sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        if marker_path.exists() or marker_path.is_symlink():
            raise StorageGuardError(
                f"quarantine marker unexpectedly exists: {marker_path}"
            )
        os.replace(temporary, marker_path)
        _fsync_directory(marker_path.parent)
    except BaseException:
        try:
            os.close(descriptor)
        except OSError:
            pass
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise


def _read_quarantine_marker(marker_path: Path) -> dict[str, object]:
    raw = _read_small_regular_file(
        marker_path,
        limit=MAX_MARKER_BYTES,
        label="quarantine marker",
    )
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise StorageGuardError(
            f"invalid quarantine marker JSON: {marker_path}"
        ) from exc
    expected_fields = {
        "schemaVersion",
        "releaseId",
        "commit",
        "archiveSha256",
        "device",
        "inode",
        "quarantineName",
    }
    if not isinstance(payload, dict) or set(payload) != expected_fields:
        raise StorageGuardError(
            f"quarantine marker fields are invalid: {marker_path}"
        )
    commit = payload["commit"]
    archive = payload["archiveSha256"]
    quarantine_name = payload["quarantineName"]
    quarantine_match = (
        QUARANTINE_PATTERN.fullmatch(quarantine_name)
        if isinstance(quarantine_name, str)
        else None
    )
    if (
        payload["schemaVersion"] != 1
        or not isinstance(commit, str)
        or not COMMIT_PATTERN.fullmatch(commit)
        or not isinstance(archive, str)
        or not ARCHIVE_PATTERN.fullmatch(archive)
        or payload["releaseId"] != f"{commit}/{archive}"
        or quarantine_match is None
        or quarantine_match.group("archive") != archive
        or not isinstance(payload["device"], int)
        or isinstance(payload["device"], bool)
        or payload["device"] < 0
        or not isinstance(payload["inode"], int)
        or isinstance(payload["inode"], bool)
        or payload["inode"] <= 0
    ):
        raise StorageGuardError(
            f"quarantine marker values are invalid: {marker_path}"
        )
    return payload


def _release_entry(
    path: Path,
    *,
    commit_sha: str,
    archive_sha256: str,
    root_device: int,
    allow_missing_identity: bool,
) -> ReleaseEntry:
    metadata = path.lstat()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_dev != root_device
    ):
        raise StorageGuardError(
            f"release must be a same-filesystem real directory: {path}"
        )
    try:
        identity = _read_identity(path / IDENTITY_FILE_NAME)
    except MissingReleaseIdentityError:
        if not allow_missing_identity:
            raise
    else:
        if identity != _expected_identity(commit_sha, archive_sha256):
            raise StorageGuardError(f"release identity mismatch: {path}")
    return ReleaseEntry(
        path=path,
        commit_sha=commit_sha,
        archive_sha256=archive_sha256,
        modified_ns=metadata.st_mtime_ns,
        device=metadata.st_dev,
        inode=metadata.st_ino,
    )


def _discover_releases(
    releases_root: Path,
    *,
    incomplete_allowed: frozenset[str],
) -> ReleaseInventory:
    root_device = releases_root.lstat().st_dev
    releases: list[ReleaseEntry] = []
    quarantines: list[QuarantineEntry] = []
    orphan_markers: list[Path] = []
    for commit_path in sorted(releases_root.iterdir(), key=lambda item: item.name):
        if not COMMIT_PATTERN.fullmatch(commit_path.name):
            raise StorageGuardError(
                f"unexpected entry under release root: {commit_path}"
            )
        commit_metadata = commit_path.lstat()
        if (
            not stat.S_ISDIR(commit_metadata.st_mode)
            or stat.S_ISLNK(commit_metadata.st_mode)
            or commit_metadata.st_dev != root_device
        ):
            raise StorageGuardError(
                f"release commit parent is unsafe: {commit_path}"
            )
        children = {child.name: child for child in commit_path.iterdir()}
        consumed: set[str] = set()
        for name, temporary_marker in sorted(children.items()):
            if QUARANTINE_MARKER_TEMP_PATTERN.fullmatch(name) is None:
                continue
            metadata = temporary_marker.lstat()
            if (
                not stat.S_ISREG(metadata.st_mode)
                or stat.S_ISLNK(metadata.st_mode)
                or metadata.st_dev != root_device
                or metadata.st_size > MAX_MARKER_BYTES
            ):
                raise StorageGuardError(
                    f"quarantine marker temporary is unsafe: {temporary_marker}"
                )
            orphan_markers.append(temporary_marker)
            consumed.add(name)
        for name, marker_path in sorted(children.items()):
            marker_match = QUARANTINE_MARKER_PATTERN.fullmatch(name)
            if marker_match is None:
                continue
            payload = _read_quarantine_marker(marker_path)
            quarantine_name = str(payload["quarantineName"])
            if payload["commit"] != commit_path.name:
                raise StorageGuardError(
                    f"quarantine marker commit does not match its parent: {marker_path}"
                )
            expected_marker_name = f"{quarantine_name}.marker.json"
            if name != expected_marker_name:
                raise StorageGuardError(
                    f"quarantine marker name is inconsistent: {marker_path}"
                )
            archive = str(payload["archiveSha256"])
            original_path = commit_path / archive
            quarantine_path = commit_path / quarantine_name
            if quarantine_path.exists() or quarantine_path.is_symlink():
                release = _release_entry(
                    quarantine_path,
                    commit_sha=commit_path.name,
                    archive_sha256=archive,
                    root_device=root_device,
                    allow_missing_identity=True,
                )
                renamed = True
                consumed.add(quarantine_name)
            elif original_path.exists() or original_path.is_symlink():
                release = _release_entry(
                    original_path,
                    commit_sha=commit_path.name,
                    archive_sha256=archive,
                    root_device=root_device,
                    allow_missing_identity=False,
                )
                renamed = False
                consumed.add(archive)
            else:
                orphan_markers.append(marker_path)
                consumed.add(name)
                continue
            if (release.device, release.inode) != (
                payload["device"],
                payload["inode"],
            ):
                raise StorageGuardError(
                    f"quarantine inode changed since it was planned: {marker_path}"
                )
            quarantines.append(
                QuarantineEntry(
                    release=release,
                    quarantine_path=quarantine_path,
                    marker_path=marker_path,
                    original_path=original_path,
                    renamed=renamed,
                )
            )
            consumed.add(name)

        for name, child in sorted(children.items()):
            if name in consumed:
                continue
            if QUARANTINE_PATTERN.fullmatch(name) is not None or name.startswith(
                ".gc-"
            ):
                raise StorageGuardError(
                    f"unrecognized or unbound quarantine entry: {child}"
                )
            if not ARCHIVE_PATTERN.fullmatch(name):
                raise StorageGuardError(
                    f"unexpected entry under release commit: {child}"
                )
            release_id = f"{commit_path.name}/{name}"
            releases.append(
                _release_entry(
                    child,
                    commit_sha=commit_path.name,
                    archive_sha256=name,
                    root_device=root_device,
                    allow_missing_identity=release_id in incomplete_allowed,
                )
            )
    release_ids = [entry.release_id for entry in releases]
    quarantine_ids = [entry.release.release_id for entry in quarantines]
    if len(set(release_ids + quarantine_ids)) != len(release_ids + quarantine_ids):
        raise StorageGuardError("one release identity appears more than once")
    return ReleaseInventory(
        releases=tuple(releases),
        quarantines=tuple(quarantines),
        orphan_markers=tuple(orphan_markers),
    )


def _read_checkpoint_inventory(
    checkpoints_root: Path,
    current_checkpoint: Path,
    *,
    expected_repository: str | None = None,
) -> CheckpointInventory:
    root = _real_directory(checkpoints_root, "checkpoints root")
    current_checkpoint = _absolute(current_checkpoint)
    settled: set[str] = set()
    protected: set[str] = set()
    current_payload: Mapping[str, object] | None = None
    seen: set[str] = set()
    repositories: set[str] = set()
    for commit_path in sorted(root.iterdir(), key=lambda item: item.name):
        _real_directory(commit_path, "checkpoint commit directory")
        if not COMMIT_PATTERN.fullmatch(commit_path.name):
            raise StorageGuardError(
                f"invalid checkpoint commit directory: {commit_path}"
            )
        for entry in sorted(commit_path.iterdir(), key=lambda item: item.name):
            metadata = entry.lstat()
            if not stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
                raise StorageGuardError(
                    f"checkpoint namespace entry is unsafe: {entry}"
                )
            if entry.name.endswith(".evidence.json"):
                archive = entry.name[: -len(".evidence.json")]
                if not ARCHIVE_PATTERN.fullmatch(archive):
                    raise StorageGuardError(
                        f"invalid release evidence filename: {entry}"
                    )
                continue
            if not entry.name.endswith(".json"):
                raise StorageGuardError(
                    f"unexpected checkpoint namespace entry: {entry}"
                )
            archive = entry.name[: -len(".json")]
            if not ARCHIVE_PATTERN.fullmatch(archive):
                raise StorageGuardError(f"invalid checkpoint filename: {entry}")
            try:
                payload = load_checkpoint(entry)
                identity = ReleaseIdentity.from_mapping(payload["identity"])
            except CheckpointError as exc:
                raise StorageGuardError(
                    f"invalid release checkpoint: {entry}: {exc}"
                ) from exc
            if identity.commit != commit_path.name or identity.archiveSha256 != archive:
                raise StorageGuardError(
                    f"checkpoint identity does not match its path: {entry}"
                )
            repositories.add(identity.repository)
            if (
                expected_repository is not None
                and identity.repository != expected_repository
            ):
                raise StorageGuardError(
                    f"checkpoint repository differs from the release namespace: "
                    f"{entry}"
                )
            if payload["retryClass"] in DANGEROUS_RETRY_CLASSES:
                raise StorageGuardError(
                    "checkpoint requires operator recovery before release GC: "
                    f"{entry}"
                )
            release_id = f"{identity.commit}/{identity.archiveSha256}"
            if release_id in seen:
                raise StorageGuardError(
                    f"duplicate checkpoint identity: {release_id}"
                )
            seen.add(release_id)
            if entry == current_checkpoint:
                current_payload = payload
            if cross_release_is_settled(payload):
                settled.add(release_id)
            else:
                protected.add(release_id)
    if current_payload is None:
        raise StorageGuardError(
            f"current release checkpoint is absent from its namespace: "
            f"{current_checkpoint}"
        )
    if len(repositories) != 1:
        raise StorageGuardError(
            "checkpoint namespace contains more than one repository identity"
        )
    return CheckpointInventory(
        settled_release_ids=frozenset(settled),
        protected_release_ids=frozenset(protected),
        current_payload=current_payload,
    )


def _release_id_from_path(
    releases_root: Path,
    path: Path,
    *,
    require_exists: bool,
) -> str | None:
    path = _absolute(path)
    try:
        normalized = path.resolve(strict=require_exists)
    except (OSError, RuntimeError) as exc:
        raise StorageGuardError(f"cannot resolve release reference: {path}") from exc
    try:
        relative = normalized.relative_to(releases_root)
    except ValueError:
        return None
    if len(relative.parts) < 2:
        raise StorageGuardError(
            f"reference inside release root does not identify a release: {path}"
        )
    commit, archive_name = relative.parts[:2]
    archive_match = ARCHIVE_PATTERN.fullmatch(archive_name)
    quarantine_match = QUARANTINE_PATTERN.fullmatch(archive_name)
    archive = (
        archive_name
        if archive_match is not None
        else quarantine_match.group("archive")
        if quarantine_match is not None
        else ""
    )
    if not COMMIT_PATTERN.fullmatch(commit) or not ARCHIVE_PATTERN.fullmatch(archive):
        raise StorageGuardError(
            f"reference inside release root has an invalid identity: {path}"
        )
    return f"{commit}/{archive}"


def _target_release_id(releases_root: Path, target_root: Path) -> str:
    target = _absolute(target_root)
    try:
        relative = target.relative_to(releases_root)
    except ValueError as exc:
        raise StorageGuardError(
            "target root must be below the configured release root"
        ) from exc
    if (
        len(relative.parts) != 2
        or not COMMIT_PATTERN.fullmatch(relative.parts[0])
        or not ARCHIVE_PATTERN.fullmatch(relative.parts[1])
    ):
        raise StorageGuardError(
            "target root must identify one content-addressed release"
        )
    return f"{relative.parts[0]}/{relative.parts[1]}"


def _decode_mountinfo_path(value: str) -> Path:
    return Path(
        MOUNTINFO_ESCAPE_PATTERN.sub(
            lambda match: chr(int(match.group(1), 8)),
            value,
        )
    )


def read_mount_points(mountinfo_path: Path = Path("/proc/self/mountinfo")) -> tuple[Path, ...]:
    raw = _read_small_regular_file(
        mountinfo_path,
        limit=16 * 1024 * 1024,
        label="mountinfo",
    )
    points: list[Path] = []
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise StorageGuardError("mountinfo is not UTF-8") from exc
    for line in text.splitlines():
        fields = line.split()
        if len(fields) < 10 or "-" not in fields:
            raise StorageGuardError("mountinfo contains a malformed record")
        mount_point = _decode_mountinfo_path(fields[4])
        if not mount_point.is_absolute():
            raise StorageGuardError("mountinfo contains a relative mount point")
        points.append(mount_point)
    return tuple(points)


def _assert_no_mount_boundary(
    entry: ReleaseEntry,
    mount_points: Iterable[Path],
    releases_root: Path,
) -> None:
    for mount_point in mount_points:
        normalized = _absolute(mount_point)
        try:
            relative_to_store = normalized.relative_to(releases_root)
        except ValueError:
            continue
        if not relative_to_store.parts:
            # A dedicated filesystem mounted exactly at the reviewed release
            # root is allowed; nested mounts and bind mounts are not.
            continue
        raise StorageGuardError(
            f"release store contains a nested mount or bind-mount boundary: "
            f"{normalized}"
        )
    for directory, child_directories, _files in os.walk(
        entry.path,
        topdown=True,
        followlinks=False,
    ):
        directory_path = Path(directory)
        metadata = directory_path.lstat()
        if metadata.st_dev != entry.device:
            raise StorageGuardError(
                f"release contains a filesystem boundary: {directory_path}"
            )
        safe_children: list[str] = []
        for child_name in child_directories:
            child = directory_path / child_name
            child_metadata = child.lstat()
            if stat.S_ISLNK(child_metadata.st_mode):
                continue
            if (
                not stat.S_ISDIR(child_metadata.st_mode)
                or child_metadata.st_dev != entry.device
            ):
                raise StorageGuardError(
                    f"release contains an unsafe directory entry: {child}"
                )
            safe_children.append(child_name)
        child_directories[:] = safe_children


def collect_process_release_ids(
    releases_root: Path,
    *,
    proc_root: Path = Path("/proc"),
) -> frozenset[str]:
    proc_root = _real_directory(proc_root, "proc root")
    protected: set[str] = set()
    for process in sorted(proc_root.iterdir(), key=lambda item: item.name):
        if not process.name.isdigit():
            continue
        for link_name in ("cwd", "exe", "root"):
            link = process / link_name
            try:
                raw_target = os.readlink(link)
            except FileNotFoundError:
                continue
            except PermissionError as exc:
                raise StorageGuardError(
                    f"cannot inspect process release reference: {link}"
                ) from exc
            except OSError as exc:
                if exc.errno in {errno.ENOENT, errno.ESRCH}:
                    continue
                raise StorageGuardError(
                    f"cannot inspect process release reference: {link}: {exc}"
                ) from exc
            if raw_target.endswith(" (deleted)"):
                raw_target = raw_target[: -len(" (deleted)")]
            target = Path(raw_target)
            if not target.is_absolute():
                target = process / target
            release_id = _release_id_from_path(
                releases_root,
                target,
                require_exists=False,
            )
            if release_id is not None:
                protected.add(release_id)
    return frozenset(protected)


def collect_release_references(
    *,
    releases_root: Path,
    protected_roots: Iterable[Path],
    nginx_active_release_conf: Path,
    expected_active_slot: str,
    expected_active_root: Path,
    proc_root: Path,
    allow_missing_nginx_legacy: bool = False,
) -> frozenset[str]:
    expected_active_root = _real_directory(
        expected_active_root,
        "expected active release root",
    )
    expected_frontend_root = (
        expected_active_root / "06_AppPlatform/frontend/dist"
    )
    _real_directory(expected_frontend_root, "expected active frontend root")
    protected: set[str] = set()
    for path in protected_roots:
        release_id = _release_id_from_path(
            releases_root,
            path,
            require_exists=True,
        )
        if release_id is not None:
            protected.add(release_id)
    nginx_missing = (
        not nginx_active_release_conf.exists()
        and not nginx_active_release_conf.is_symlink()
    )
    if nginx_missing and allow_missing_nginx_legacy:
        active_release_id = _release_id_from_path(
            releases_root,
            expected_active_root,
            require_exists=True,
        )
        process_references = collect_process_release_ids(
            releases_root,
            proc_root=proc_root,
        )
        if active_release_id is not None or protected or process_references:
            raise StorageGuardError(
                "missing Nginx release include is allowed only for an entirely "
                "legacy first deployment"
            )
        return frozenset()
    try:
        routed_slot = read_active_slot(nginx_active_release_conf)
        frontend_root = read_active_frontend_root(nginx_active_release_conf)
    except ReconcileError as exc:
        raise StorageGuardError(
            f"cannot prove the active Nginx release: {exc}"
        ) from exc
    if expected_active_slot not in {"8000", "8001"} or routed_slot != expected_active_slot:
        raise StorageGuardError(
            "Nginx active slot differs from the controller active slot"
        )
    try:
        routed_frontend = frontend_root.resolve(strict=True)
        expected_frontend = expected_frontend_root.resolve(strict=True)
    except OSError as exc:
        raise StorageGuardError(
            "cannot resolve the Nginx or controller frontend root"
        ) from exc
    if routed_frontend != expected_frontend:
        raise StorageGuardError(
            "Nginx frontend root differs from the controller active release"
        )
    frontend_release = _release_id_from_path(
        releases_root,
        frontend_root,
        require_exists=True,
    )
    if frontend_release is not None:
        protected.add(frontend_release)
    protected.update(
        collect_process_release_ids(releases_root, proc_root=proc_root)
    )
    return frozenset(protected)


def filesystem_usage(path: Path) -> FilesystemUsage:
    value = os.statvfs(path)
    available = value.f_bavail * value.f_frsize
    total = value.f_blocks * value.f_frsize
    if available < 0 or total <= 0 or available > total:
        raise StorageGuardError("filesystem returned invalid capacity values")
    return FilesystemUsage(available_bytes=available, total_bytes=total)


def _required_available_bytes(
    usage: FilesystemUsage,
    minimum_available_bytes: int,
    minimum_available_percent: float,
) -> int:
    if minimum_available_bytes <= 0:
        raise StorageGuardError("minimum available bytes must be positive")
    if (
        not math.isfinite(minimum_available_percent)
        or minimum_available_percent < 0
        or minimum_available_percent > 100
    ):
        raise StorageGuardError(
            "minimum available percent must be between 0 and 100"
        )
    percentage = math.ceil(
        usage.total_bytes * minimum_available_percent / 100
    )
    return max(minimum_available_bytes, percentage)


def _validate_release_unchanged(entry: ReleaseEntry) -> None:
    metadata = entry.path.lstat()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or (metadata.st_dev, metadata.st_ino) != (entry.device, entry.inode)
    ):
        raise StorageGuardError(
            f"release changed between validation and pruning: {entry.path}"
        )
    identity = _read_identity(entry.path / IDENTITY_FILE_NAME)
    if identity != _expected_identity(entry.commit_sha, entry.archive_sha256):
        raise StorageGuardError(
            f"release identity changed before pruning: {entry.path}"
        )


def _remove_quarantine_tree(
    quarantine: QuarantineEntry,
    releases_root: Path,
    mount_points: Iterable[Path],
) -> None:
    entry = quarantine.release
    try:
        metadata = entry.path.lstat()
    except FileNotFoundError as exc:
        raise StorageGuardError(
            f"quarantined release disappeared before deletion: {entry.path}"
        ) from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or (metadata.st_dev, metadata.st_ino) != (entry.device, entry.inode)
    ):
        raise StorageGuardError(
            f"quarantined release changed before deletion: {entry.path}"
        )
    marker = _read_quarantine_marker(quarantine.marker_path)
    if (
        marker["releaseId"] != entry.release_id
        or marker["device"] != entry.device
        or marker["inode"] != entry.inode
        or marker["quarantineName"] != quarantine.quarantine_path.name
    ):
        raise StorageGuardError(
            f"quarantine marker changed before deletion: {quarantine.marker_path}"
        )
    _assert_no_mount_boundary(entry, mount_points, releases_root)
    try:
        shutil.rmtree(entry.path)
    except OSError as exc:
        raise StorageGuardError(
            f"failed to prune quarantined release: {entry.path}: {exc}"
        ) from exc
    _fsync_directory(entry.path.parent)
    try:
        quarantine.marker_path.unlink()
    except FileNotFoundError as exc:
        raise StorageGuardError(
            f"quarantine marker disappeared during deletion: "
            f"{quarantine.marker_path}"
        ) from exc
    _fsync_directory(quarantine.marker_path.parent)
    try:
        entry.path.parent.rmdir()
    except OSError:
        return
    _fsync_directory(releases_root)


def _prepare_quarantine(entry: ReleaseEntry) -> QuarantineEntry:
    _validate_release_unchanged(entry)
    nonce = secrets.token_hex(8)
    quarantine_name = f".gc-{entry.archive_sha256}-{nonce}"
    quarantine_path = entry.path.parent / quarantine_name
    marker_path = entry.path.parent / f"{quarantine_name}.marker.json"
    _write_quarantine_marker(
        marker_path,
        release=entry,
        quarantine_name=quarantine_name,
    )
    try:
        os.rename(entry.path, quarantine_path)
        _fsync_directory(entry.path.parent)
    except BaseException:
        try:
            marker_path.unlink()
            _fsync_directory(marker_path.parent)
        except OSError:
            pass
        raise
    return QuarantineEntry(
        release=ReleaseEntry(
            path=quarantine_path,
            commit_sha=entry.commit_sha,
            archive_sha256=entry.archive_sha256,
            modified_ns=entry.modified_ns,
            device=entry.device,
            inode=entry.inode,
        ),
        quarantine_path=quarantine_path,
        marker_path=marker_path,
        original_path=entry.path,
        renamed=True,
    )


def _resume_planned_quarantine(quarantine: QuarantineEntry) -> QuarantineEntry:
    if quarantine.renamed:
        return quarantine
    _validate_release_unchanged(quarantine.release)
    os.rename(quarantine.original_path, quarantine.quarantine_path)
    _fsync_directory(quarantine.original_path.parent)
    return QuarantineEntry(
        release=ReleaseEntry(
            path=quarantine.quarantine_path,
            commit_sha=quarantine.release.commit_sha,
            archive_sha256=quarantine.release.archive_sha256,
            modified_ns=quarantine.release.modified_ns,
            device=quarantine.release.device,
            inode=quarantine.release.inode,
        ),
        quarantine_path=quarantine.quarantine_path,
        marker_path=quarantine.marker_path,
        original_path=quarantine.original_path,
        renamed=True,
    )


def _restore_quarantine(quarantine: QuarantineEntry) -> None:
    if not quarantine.renamed:
        return
    if quarantine.original_path.exists() or quarantine.original_path.is_symlink():
        raise StorageGuardError(
            f"cannot restore newly referenced quarantine: "
            f"{quarantine.original_path}"
        )
    os.rename(quarantine.quarantine_path, quarantine.original_path)
    _fsync_directory(quarantine.original_path.parent)
    quarantine.marker_path.unlink()
    _fsync_directory(quarantine.marker_path.parent)


def guard_release_storage(
    *,
    releases_root: Path,
    target_root: Path,
    protected_roots: Iterable[Path],
    checkpoints_root: Path,
    current_checkpoint: Path,
    expected_repository: str | None = None,
    nginx_active_release_conf: Path,
    expected_active_slot: str,
    expected_active_root: Path | None = None,
    minimum_available_bytes: int,
    minimum_available_percent: float = 0,
    keep_unreferenced: int = 3,
    normal_min_age_seconds: int = 14 * 24 * 60 * 60,
    emergency_min_age_seconds: int = 24 * 60 * 60,
    check_only: bool = False,
    allow_missing_nginx_legacy: bool = False,
    proc_root: Path = Path("/proc"),
    now_ns: int | None = None,
    filesystem_usage_provider: Callable[[Path], FilesystemUsage] | None = None,
    reference_provider: Callable[[], frozenset[str]] | None = None,
    mount_points_provider: Callable[[], Iterable[Path]] | None = None,
    quarantine_remover: Callable[
        [QuarantineEntry, Path, Iterable[Path]], None
    ]
    | None = None,
) -> StorageGuardReport:
    if keep_unreferenced < 0:
        raise StorageGuardError("unreferenced retention must not be negative")
    if normal_min_age_seconds < 0 or emergency_min_age_seconds < 0:
        raise StorageGuardError("release ages must not be negative")
    if emergency_min_age_seconds > normal_min_age_seconds:
        raise StorageGuardError(
            "emergency release age cannot exceed the normal release age"
        )
    root = _ensure_real_directory(releases_root)
    target_id = _target_release_id(root, target_root)
    checkpoints = _read_checkpoint_inventory(
        checkpoints_root,
        current_checkpoint,
        expected_repository=expected_repository,
    )
    current = checkpoints.current_payload
    current_identity = ReleaseIdentity.from_mapping(current["identity"])
    if target_id != f"{current_identity.commit}/{current_identity.archiveSha256}":
        raise StorageGuardError(
            "target release does not match the current checkpoint identity"
        )
    incomplete_allowed = frozenset()
    if (
        current["phase"] == "source_install_started"
        and current["status"] == "in_progress"
    ):
        incomplete_allowed = frozenset({target_id})
    inventory = _discover_releases(
        root,
        incomplete_allowed=incomplete_allowed,
    )
    if reference_provider is None and expected_active_root is None:
        raise StorageGuardError(
            "expected active root is required for automatic reference discovery"
        )
    automatic_active_root = expected_active_root
    usage_provider = filesystem_usage_provider or filesystem_usage
    def automatic_references() -> frozenset[str]:
        if automatic_active_root is None:  # Defensive; checked above.
            raise StorageGuardError("expected active root is unavailable")
        return collect_release_references(
            releases_root=root,
            protected_roots=protected_roots,
            nginx_active_release_conf=nginx_active_release_conf,
            expected_active_slot=expected_active_slot,
            expected_active_root=automatic_active_root,
            proc_root=proc_root,
            allow_missing_nginx_legacy=allow_missing_nginx_legacy,
        )
    references = reference_provider or automatic_references
    mount_provider = mount_points_provider or read_mount_points
    remover = quarantine_remover or _remove_quarantine_tree
    protected = set(references())
    protected.update(checkpoints.protected_release_ids)
    protected.add(target_id)
    available_before = usage_provider(root)
    required = _required_available_bytes(
        available_before,
        minimum_available_bytes,
        minimum_available_percent,
    )
    root_device = root.lstat().st_dev

    def refreshed_usage() -> FilesystemUsage:
        metadata = root.lstat()
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_dev != root_device
        ):
            raise StorageGuardError(
                "release filesystem changed during capacity enforcement"
            )
        usage = usage_provider(root)
        if usage.total_bytes != available_before.total_bytes:
            raise StorageGuardError(
                "release filesystem total capacity changed during GC"
            )
        refreshed_required = _required_available_bytes(
            usage,
            minimum_available_bytes,
            minimum_available_percent,
        )
        if refreshed_required != required:
            raise StorageGuardError(
                "release filesystem reserve changed during GC"
            )
        return usage
    if check_only:
        if inventory.quarantines or inventory.orphan_markers:
            raise StorageGuardError(
                "check-only reserve cannot proceed with pending GC quarantine"
            )
        if available_before.available_bytes < required:
            raise StorageGuardError(
                "insufficient release filesystem capacity: "
                f"available={available_before.available_bytes} required={required}"
            )
        retained_ids = tuple(
            sorted(
                entry.release_id
                for entry in inventory.releases
                if entry.release_id not in protected
            )
        )
        return StorageGuardReport(
            available_before_bytes=available_before.available_bytes,
            available_after_bytes=available_before.available_bytes,
            filesystem_total_bytes=available_before.total_bytes,
            minimum_available_bytes=required,
            minimum_available_percent=minimum_available_percent,
            protected_release_ids=tuple(sorted(protected)),
            retained_unreferenced_release_ids=retained_ids,
            pruned_release_ids=(),
            resumed_quarantine_release_ids=(),
        )

    for marker in inventory.orphan_markers:
        marker.unlink()
        _fsync_directory(marker.parent)

    resumed: list[str] = []
    pruned: list[str] = []
    for pending in inventory.quarantines:
        release_id = pending.release.release_id
        if release_id not in checkpoints.settled_release_ids:
            raise StorageGuardError(
                f"quarantined release lacks a settled checkpoint: {release_id}"
            )
        if release_id in protected or release_id in references():
            raise StorageGuardError(
                f"quarantined release became referenced: {release_id}"
            )
        resumed_entry = _resume_planned_quarantine(pending)
        refreshed = references()
        if release_id in refreshed:
            _restore_quarantine(resumed_entry)
            raise StorageGuardError(
                f"release became referenced after quarantine: {release_id}"
            )
        remover(resumed_entry, root, tuple(mount_provider()))
        resumed.append(release_id)
        pruned.append(release_id)

    eligible = [
        entry
        for entry in inventory.releases
        if entry.release_id not in protected
        and entry.release_id in checkpoints.settled_release_ids
    ]
    newest_first = sorted(
        eligible,
        key=lambda item: (item.modified_ns, item.release_id),
        reverse=True,
    )
    retained = list(newest_first[:keep_unreferenced])
    beyond_retention = list(newest_first[keep_unreferenced:])
    current_time_ns = time.time_ns() if now_ns is None else now_ns
    normal_cutoff = current_time_ns - normal_min_age_seconds * 1_000_000_000
    emergency_cutoff = (
        current_time_ns - emergency_min_age_seconds * 1_000_000_000
    )

    normal_queue = sorted(
        (
            entry
            for entry in beyond_retention
            if entry.modified_ns <= normal_cutoff
        ),
        key=lambda item: (item.modified_ns, item.release_id),
    )
    normal_ids = {entry.release_id for entry in normal_queue}

    def prune(entry: ReleaseEntry) -> None:
        nonlocal protected
        refreshed = set(references())
        refreshed.update(checkpoints.protected_release_ids)
        refreshed.add(target_id)
        if entry.release_id in refreshed:
            raise StorageGuardError(
                f"release became referenced before quarantine: {entry.release_id}"
            )
        _assert_no_mount_boundary(entry, tuple(mount_provider()), root)
        quarantine = _prepare_quarantine(entry)
        after_rename = set(references())
        after_rename.update(checkpoints.protected_release_ids)
        after_rename.add(target_id)
        if entry.release_id in after_rename:
            _restore_quarantine(quarantine)
            raise StorageGuardError(
                f"release became referenced after quarantine: {entry.release_id}"
            )
        remover(quarantine, root, tuple(mount_provider()))
        pruned.append(entry.release_id)
        protected = after_rename

    for entry in normal_queue:
        prune(entry)
        beyond_retention.remove(entry)

    usage_after = refreshed_usage()
    emergency_pool = sorted(
        (
            entry
            for entry in retained + beyond_retention
            if entry.release_id not in normal_ids
            and entry.modified_ns <= emergency_cutoff
        ),
        key=lambda item: (item.modified_ns, item.release_id),
    )
    while usage_after.available_bytes < required and emergency_pool:
        entry = emergency_pool.pop(0)
        prune(entry)
        if entry in retained:
            retained.remove(entry)
        if entry in beyond_retention:
            beyond_retention.remove(entry)
        usage_after = refreshed_usage()

    if usage_after.available_bytes < required:
        raise StorageGuardError(
            "insufficient release filesystem capacity after safe GC: "
            f"available={usage_after.available_bytes} required={required}"
        )
    pruned_ids = set(pruned)
    retained_ids = tuple(
        sorted(
            entry.release_id
            for entry in inventory.releases
            if entry.release_id not in protected
            and entry.release_id not in pruned_ids
        )
    )
    return StorageGuardReport(
        available_before_bytes=available_before.available_bytes,
        available_after_bytes=usage_after.available_bytes,
        filesystem_total_bytes=usage_after.total_bytes,
        minimum_available_bytes=required,
        minimum_available_percent=minimum_available_percent,
        protected_release_ids=tuple(sorted(protected)),
        retained_unreferenced_release_ids=retained_ids,
        pruned_release_ids=tuple(pruned),
        resumed_quarantine_release_ids=tuple(resumed),
    )


def validate_memory_budget(
    *,
    total_bytes: int,
    available_bytes: int,
    active_bytes: int,
    active_memory_high_bytes: int,
    active_memory_max_bytes: int,
    expected_active_memory_high_bytes: int,
    expected_active_memory_max_bytes: int,
    minimum_total_bytes: int,
    minimum_available_bytes: int,
    candidate_max_bytes: int,
    os_reserve_bytes: int,
) -> MemoryBudgetReport:
    values = {
        "total": total_bytes,
        "available": available_bytes,
        "active": active_bytes,
        "active MemoryHigh": active_memory_high_bytes,
        "active MemoryMax": active_memory_max_bytes,
        "expected active MemoryHigh": expected_active_memory_high_bytes,
        "expected active MemoryMax": expected_active_memory_max_bytes,
        "minimum total": minimum_total_bytes,
        "minimum available": minimum_available_bytes,
        "candidate max": candidate_max_bytes,
        "OS reserve": os_reserve_bytes,
    }
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value < 0
        for value in values.values()
    ):
        raise StorageGuardError("memory values must be non-negative integers")
    if (
        total_bytes <= 0
        or minimum_total_bytes <= 0
        or minimum_available_bytes <= 0
        or candidate_max_bytes <= 0
        or os_reserve_bytes <= 0
        or active_memory_high_bytes <= 0
        or active_memory_max_bytes <= 0
        or expected_active_memory_high_bytes <= 0
        or expected_active_memory_max_bytes <= 0
        or expected_active_memory_high_bytes
        > expected_active_memory_max_bytes
        or available_bytes > total_bytes
        or active_bytes > total_bytes
    ):
        raise StorageGuardError("memory budget values are invalid")
    if active_memory_high_bytes != expected_active_memory_high_bytes:
        raise StorageGuardError(
            "active slot MemoryHigh differs from the reviewed limit: "
            f"actual={active_memory_high_bytes} "
            f"expected={expected_active_memory_high_bytes}"
        )
    if active_memory_max_bytes != expected_active_memory_max_bytes:
        raise StorageGuardError(
            "active slot MemoryMax differs from the reviewed limit: "
            f"actual={active_memory_max_bytes} "
            f"expected={expected_active_memory_max_bytes}"
        )
    if total_bytes < minimum_total_bytes:
        raise StorageGuardError(
            f"host RAM is below the blue/green minimum: "
            f"total={total_bytes} required={minimum_total_bytes}"
        )
    if available_bytes < minimum_available_bytes:
        raise StorageGuardError(
            f"available RAM is below the candidate-start minimum: "
            f"available={available_bytes} required={minimum_available_bytes}"
        )
    if active_bytes + candidate_max_bytes > total_bytes - os_reserve_bytes:
        raise StorageGuardError(
            "active memory plus candidate cap would consume the OS reserve"
        )
    return MemoryBudgetReport(
        total_bytes=total_bytes,
        available_bytes=available_bytes,
        active_bytes=active_bytes,
        active_memory_high_bytes=active_memory_high_bytes,
        active_memory_max_bytes=active_memory_max_bytes,
        candidate_max_bytes=candidate_max_bytes,
        os_reserve_bytes=os_reserve_bytes,
    )


def _read_meminfo(path: Path) -> tuple[int, int]:
    raw = _read_small_regular_file(
        path,
        limit=128 * 1024,
        label="meminfo",
    )
    try:
        lines = raw.decode("utf-8").splitlines()
    except UnicodeDecodeError as exc:
        raise StorageGuardError("meminfo is not UTF-8") from exc
    values: dict[str, int] = {}
    for line in lines:
        if ":" not in line:
            raise StorageGuardError("meminfo contains a malformed record")
        key, raw_value = line.split(":", 1)
        tokens = raw_value.strip().split()
        if len(tokens) != 2 or tokens[1] != "kB" or not tokens[0].isdigit():
            continue
        values[key] = int(tokens[0]) * 1024
    if "MemTotal" not in values or "MemAvailable" not in values:
        raise StorageGuardError("MemTotal or MemAvailable is absent from meminfo")
    return values["MemTotal"], values["MemAvailable"]


def check_host_memory(
    *,
    active_service: str,
    meminfo_path: Path,
    expected_active_memory_high_bytes: int,
    expected_active_memory_max_bytes: int,
    minimum_total_bytes: int,
    minimum_available_bytes: int,
    candidate_max_bytes: int,
    os_reserve_bytes: int,
) -> MemoryBudgetReport:
    total, available = _read_meminfo(meminfo_path)

    def read_property(name: str) -> int:
        try:
            raw = subprocess.check_output(
                [
                    "systemctl",
                    "show",
                    active_service,
                    "-p",
                    name,
                    "--value",
                ],
                text=True,
            ).strip()
        except (OSError, subprocess.CalledProcessError) as exc:
            raise StorageGuardError(
                f"active slot {name} is unavailable"
            ) from exc
        if not raw.isdigit():
            raise StorageGuardError(f"active slot {name} is invalid")
        return int(raw)

    active = read_property("MemoryCurrent")
    active_memory_high = read_property("MemoryHigh")
    active_memory_max = read_property("MemoryMax")
    return validate_memory_budget(
        total_bytes=total,
        available_bytes=available,
        active_bytes=active,
        active_memory_high_bytes=active_memory_high,
        active_memory_max_bytes=active_memory_max,
        expected_active_memory_high_bytes=expected_active_memory_high_bytes,
        expected_active_memory_max_bytes=expected_active_memory_max_bytes,
        minimum_total_bytes=minimum_total_bytes,
        minimum_available_bytes=minimum_available_bytes,
        candidate_max_bytes=candidate_max_bytes,
        os_reserve_bytes=os_reserve_bytes,
    )


def _positive_int(value: str) -> int:
    if not value.isdigit() or int(value) <= 0:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return int(value)


def _nonnegative_int(value: str) -> int:
    if not value.isdigit():
        raise argparse.ArgumentTypeError("value must be a non-negative integer")
    return int(value)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Enforce fail-closed blue/green host resource reserves."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    storage = subparsers.add_parser("storage")
    storage.add_argument("--releases-root", type=Path, required=True)
    storage.add_argument("--target-root", type=Path, required=True)
    storage.add_argument("--protected-root", type=Path, action="append", default=[])
    storage.add_argument("--checkpoints-root", type=Path, required=True)
    storage.add_argument("--current-checkpoint", type=Path, required=True)
    storage.add_argument("--expected-repository", required=True)
    storage.add_argument("--nginx-active-release-conf", type=Path, required=True)
    storage.add_argument(
        "--expected-active-slot",
        choices=("8000", "8001"),
        required=True,
    )
    storage.add_argument("--expected-active-root", type=Path, required=True)
    storage.add_argument(
        "--minimum-available-bytes",
        type=_positive_int,
        required=True,
    )
    storage.add_argument(
        "--minimum-available-percent",
        type=float,
        default=0,
    )
    storage.add_argument("--keep-unreferenced", type=_nonnegative_int, default=3)
    storage.add_argument(
        "--normal-min-age-seconds",
        type=_nonnegative_int,
        default=14 * 24 * 60 * 60,
    )
    storage.add_argument(
        "--emergency-min-age-seconds",
        type=_nonnegative_int,
        default=24 * 60 * 60,
    )
    storage.add_argument("--proc-root", type=Path, default=Path("/proc"))
    storage.add_argument("--check-only", action="store_true")
    storage.add_argument(
        "--allow-missing-nginx-legacy",
        action="store_true",
    )

    memory = subparsers.add_parser("memory")
    memory.add_argument("--active-service", required=True)
    memory.add_argument(
        "--expected-active-memory-high-bytes",
        type=_positive_int,
        required=True,
    )
    memory.add_argument(
        "--expected-active-memory-max-bytes",
        type=_positive_int,
        required=True,
    )
    memory.add_argument(
        "--meminfo-path",
        type=Path,
        default=Path("/proc/meminfo"),
    )
    memory.add_argument("--minimum-total-bytes", type=_positive_int, required=True)
    memory.add_argument(
        "--minimum-available-bytes",
        type=_positive_int,
        required=True,
    )
    memory.add_argument("--candidate-max-bytes", type=_positive_int, required=True)
    memory.add_argument("--os-reserve-bytes", type=_positive_int, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "storage":
            report: StorageGuardReport | MemoryBudgetReport = guard_release_storage(
                releases_root=args.releases_root,
                target_root=args.target_root,
                protected_roots=args.protected_root,
                checkpoints_root=args.checkpoints_root,
                current_checkpoint=args.current_checkpoint,
                expected_repository=args.expected_repository,
                nginx_active_release_conf=args.nginx_active_release_conf,
                expected_active_slot=args.expected_active_slot,
                expected_active_root=args.expected_active_root,
                minimum_available_bytes=args.minimum_available_bytes,
                minimum_available_percent=args.minimum_available_percent,
                keep_unreferenced=args.keep_unreferenced,
                normal_min_age_seconds=args.normal_min_age_seconds,
                emergency_min_age_seconds=args.emergency_min_age_seconds,
                check_only=args.check_only,
                allow_missing_nginx_legacy=args.allow_missing_nginx_legacy,
                proc_root=args.proc_root,
            )
        elif args.command == "memory":
            report = check_host_memory(
                active_service=args.active_service,
                meminfo_path=args.meminfo_path,
                expected_active_memory_high_bytes=(
                    args.expected_active_memory_high_bytes
                ),
                expected_active_memory_max_bytes=(
                    args.expected_active_memory_max_bytes
                ),
                minimum_total_bytes=args.minimum_total_bytes,
                minimum_available_bytes=args.minimum_available_bytes,
                candidate_max_bytes=args.candidate_max_bytes,
                os_reserve_bytes=args.os_reserve_bytes,
            )
        else:  # pragma: no cover
            raise StorageGuardError(f"unsupported command: {args.command}")
    except (CheckpointError, OSError, ReconcileError, StorageGuardError) as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1
    print(report.to_json())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
