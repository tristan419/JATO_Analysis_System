#!/usr/bin/env python3
"""Capture and restore one inactive blue/green Candidate runtime preimage.

The production controller owns a small, fixed set of Candidate-only paths.
Before the first write to any of those paths it records their complete semantic
state in a durable, content-checked preimage.  A failed release restores that
state instead of assuming that every path was absent and deleting blindly.

This helper never starts, stops, enables, or disables a service.  The caller
must prove the Candidate is quiescent before capture/restore and must run
``systemctl daemon-reload`` after a restore.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import hashlib
import json
import os
import re
import shutil
import stat
import sys
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
MAX_MANIFEST_BYTES = 4 * 1024 * 1024
MAX_RETAINED_PREIMAGE_ARCHIVES = 8
MAX_PREIMAGE_STORAGE_BYTES = 2 * 1024 * 1024 * 1024
MAX_PREIMAGE_STORAGE_NODES = 100_000
PREIMAGE_FREE_RESERVE_BYTES = 512 * 1024 * 1024
GIT_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
SLOT_PATTERN = re.compile(r"^(8000|8001)$")
SANDBOX_STAGE_NAME = ".10-candidate-sandbox.conf.jato-candidate-installing"
BOOT_ID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)
PROCFS_BOOT_ID_FILE = Path("/proc/sys/kernel/random/boot_id")
MAX_BOOT_ID_BYTES = 128
ROLE_ARGUMENTS = (
    ("slot_link", "slot-link"),
    ("slot_link_stage", "slot-link-stage"),
    ("slot_env", "slot-env"),
    ("slot_env_stage", "slot-env-stage"),
    ("explicit_unit", "explicit-unit"),
    ("explicit_unit_stage", "explicit-unit-stage"),
    ("instance_dropins", "instance-dropins"),
    ("persistent_control_dropins", "persistent-control-dropins"),
    ("runtime_control_dropins", "runtime-control-dropins"),
    ("candidate_cache_link", "candidate-cache-link"),
    ("candidate_cache_private", "candidate-cache-private"),
)
ROLE_NAMES = tuple(role for role, _ in ROLE_ARGUMENTS)
MANIFEST_FIELDS = {
    "schemaVersion",
    "kind",
    "identity",
    "createdAt",
    "bootId",
    "paths",
    "authorizedLive",
}
IDENTITY_FIELDS = {"commit", "archiveSha256", "candidateSlot"}
PATH_FIELDS = {"role", "path", "state", "payload", "tree"}
SLOT_OWNER_FIELDS = {
    "schemaVersion",
    "kind",
    "identity",
    "preimage",
    "manifestSha256",
}
RESTORE_INTENT_FIELDS = {
    "schemaVersion",
    "kind",
    "identity",
    "preimage",
    "manifestSha256",
    "roles",
}
RESTORE_INTENT_MAX_BYTES = 16 * 1024


class PreimageError(ValueError):
    """The preimage or one of its Candidate-only paths is unsafe."""


def _fail(message: str) -> None:
    raise PreimageError(message)


def _normalized_absolute(raw: str, label: str) -> Path:
    if not raw or "\n" in raw or "\r" in raw:
        _fail(f"{label} must be a non-empty single-line path")
    path = Path(raw)
    if not path.is_absolute() or ".." in path.parts or str(path) != raw:
        _fail(f"{label} must be a normalized absolute path")
    return path


def _lstat_optional(path: Path) -> os.stat_result | None:
    try:
        return path.lstat()
    except FileNotFoundError:
        return None


def _assert_real_parent_chain(path: Path, label: str) -> None:
    current = Path(path.anchor)
    for part in path.parent.parts[1:]:
        current /= part
        metadata = _lstat_optional(current)
        if metadata is None:
            continue
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            _fail(f"{label} parent is not a real directory: {current}")


def _require_private_owner_directory(path: Path, label: str) -> os.stat_result:
    metadata = _lstat_optional(path)
    if (
        metadata is None
        or stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) & 0o022
    ):
        _fail(f"{label} must be a real owner-controlled directory: {path}")
    return metadata


def _create_private_child(parent: Path, name: str) -> Path:
    _require_private_owner_directory(parent, "preimage namespace parent")
    child = parent / name
    metadata = _lstat_optional(child)
    if metadata is None:
        child.mkdir(mode=0o700)
        _fsync_directory(parent)
        metadata = child.lstat()
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        _fail(f"preimage namespace is not private: {child}")
    return child


def _guard_preimage_storage(
    state_root: Path,
    cache_policy: Mapping[str, Any],
) -> None:
    required_free = PREIMAGE_FREE_RESERVE_BYTES + int(cache_policy["maxBytes"])
    if shutil.disk_usage(state_root).free < required_free:
        _fail("Candidate preimage filesystem lacks its reserved free space")
    namespace = state_root / "candidate-preimages"
    if _lstat_optional(namespace) is None:
        return
    _require_private_owner_directory(namespace, "Candidate preimage namespace")
    pending = [namespace]
    total_bytes = 0
    nodes = 0
    archives = 0
    while pending:
        current = pending.pop()
        with os.scandir(current) as children:
            for child in children:
                nodes += 1
                if nodes > MAX_PREIMAGE_STORAGE_NODES:
                    _fail("Candidate preimage storage exceeds its node limit")
                metadata = child.stat(follow_symlinks=False)
                child_path = Path(child.path)
                if stat.S_ISLNK(metadata.st_mode):
                    total_bytes += len(os.fsencode(os.readlink(child_path)))
                    if total_bytes > MAX_PREIMAGE_STORAGE_BYTES:
                        _fail("Candidate preimage storage exceeds its byte limit")
                elif stat.S_ISREG(metadata.st_mode):
                    total_bytes += metadata.st_size
                    if total_bytes > MAX_PREIMAGE_STORAGE_BYTES:
                        _fail("Candidate preimage storage exceeds its byte limit")
                elif stat.S_ISDIR(metadata.st_mode):
                    if os.path.ismount(child_path):
                        _fail("Candidate preimage storage contains a mount point")
                    pending.append(child_path)
                    if child_path.parent.parent == namespace:
                        archives += 1
                        if archives >= MAX_RETAINED_PREIMAGE_ARCHIVES:
                            _fail(
                                "Candidate preimage retention limit reached; "
                                "settled audit payloads require reviewed cleanup"
                            )
                else:
                    _fail("Candidate preimage storage contains a special object")


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_regular(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            _fail(f"durability target is not a regular file: {path}")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _sha256_file(path: Path) -> str:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    digest = hashlib.sha256()
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            _fail(f"preimage regular-file expectation changed: {path}")
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    ) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        _fail(f"file changed while hashing: {path}")
    return digest.hexdigest()


def _xattrs(path: Path) -> Mapping[str, str]:
    try:
        names = os.listxattr(path, follow_symlinks=False)
    except (AttributeError, NotImplementedError):
        return {}
    except OSError as exc:
        # Unsupported filesystems normally use ENOTSUP/EOPNOTSUPP.  Any other
        # error is meaningful because silently omitting xattrs would make the
        # preimage incomplete.
        if exc.errno in {getattr(os, "ENOTSUP", 95), 95}:
            return {}
        raise
    result: dict[str, str] = {}
    for name in sorted(names):
        value = os.getxattr(path, name, follow_symlinks=False)
        result[name] = base64.b64encode(value).decode("ascii")
    return result


def _node_record(path: Path, relative: str) -> Mapping[str, Any]:
    metadata = path.lstat()
    common: dict[str, Any] = {
        "path": relative,
        "mode": format(stat.S_IMODE(metadata.st_mode), "04o"),
        "uid": metadata.st_uid,
        "gid": metadata.st_gid,
        "mtimeNs": metadata.st_mtime_ns,
        "xattrs": _xattrs(path),
    }
    if stat.S_ISREG(metadata.st_mode):
        if metadata.st_nlink != 1:
            _fail(f"hard-linked Candidate file is unsupported: {path}")
        common.update(
            {
                "type": "file",
                "bytes": metadata.st_size,
                "sha256": _sha256_file(path),
            }
        )
        return common
    if stat.S_ISLNK(metadata.st_mode):
        target = os.readlink(path)
        common.update(
            {
                "type": "symlink",
                "target": target,
                "targetBytes": len(os.fsencode(target)),
            }
        )
        return common
    if stat.S_ISDIR(metadata.st_mode):
        if os.path.ismount(path):
            _fail(f"Candidate path must not be a mount point: {path}")
        common["type"] = "directory"
        return common
    _fail(f"special Candidate filesystem object is unsupported: {path}")


def _tree(path: Path) -> list[Mapping[str, Any]]:
    records = [_node_record(path, ".")]
    if records[0]["type"] != "directory":
        return records
    for root, directories, files in os.walk(path, topdown=True, followlinks=False):
        directories.sort()
        files.sort()
        root_path = Path(root)
        for name in list(directories):
            child = root_path / name
            record = _node_record(child, child.relative_to(path).as_posix())
            records.append(record)
            if record["type"] == "symlink":
                directories.remove(name)
        for name in files:
            child = root_path / name
            records.append(
                _node_record(child, child.relative_to(path).as_posix())
            )
    records.sort(key=lambda item: (str(item["path"]).count("/"), item["path"]))
    return records


def _bounded_cache_tree(
    path: Path,
    policy: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    """Return a cache tree without first materializing an unbounded walk."""

    max_nodes = int(policy["maxNodes"])
    max_bytes = int(policy["maxBytes"])
    records: list[Mapping[str, Any]] = []
    total_bytes = 0
    pending: list[tuple[Path, str]] = [(path, ".")]
    while pending:
        current, relative = pending.pop()
        metadata = current.lstat()
        if stat.S_ISLNK(metadata.st_mode):
            _fail("Candidate private cache must not contain symbolic links")
        if stat.S_ISREG(metadata.st_mode):
            if metadata.st_nlink != 1:
                _fail("Candidate private cache must not contain hard-linked files")
            total_bytes += metadata.st_size
            if total_bytes > max_bytes:
                _fail("Candidate private cache exceeds its byte limit")
        elif stat.S_ISDIR(metadata.st_mode):
            if os.path.ismount(current):
                _fail("Candidate private cache must not contain mount points")
        else:
            _fail("Candidate private cache contains an unsupported special object")
        if len(records) >= max_nodes:
            _fail("Candidate private cache exceeds its node limit")
        records.append(_node_record(current, relative))
        if stat.S_ISDIR(metadata.st_mode):
            with os.scandir(current) as entries:
                for entry in entries:
                    if len(records) + len(pending) >= max_nodes:
                        _fail("Candidate private cache exceeds its node limit")
                    child_relative = (
                        entry.name
                        if relative == "."
                        else f"{relative}/{entry.name}"
                    )
                    pending.append((Path(entry.path), child_relative))
    records.sort(key=lambda item: (str(item["path"]).count("/"), item["path"]))
    return records


def _role_tree(
    path: Path,
    role: str,
    policies: Mapping[str, Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    if role == "candidate_cache_private":
        return _bounded_cache_tree(path, policies[role])
    return _tree(path)


def _set_xattrs(path: Path, expected: Mapping[str, str]) -> None:
    try:
        actual = set(os.listxattr(path, follow_symlinks=False))
    except (AttributeError, NotImplementedError):
        if expected:
            _fail(f"filesystem cannot restore required xattrs: {path}")
        return
    except OSError as exc:
        if exc.errno in {getattr(os, "ENOTSUP", 95), 95} and not expected:
            return
        raise
    for name in actual - set(expected):
        os.removexattr(path, name, follow_symlinks=False)
    for name, encoded in expected.items():
        os.setxattr(
            path,
            name,
            base64.b64decode(encoded, validate=True),
            follow_symlinks=False,
        )


def _apply_metadata(path: Path, record: Mapping[str, Any]) -> None:
    is_symlink = record["type"] == "symlink"
    os.chown(
        path,
        int(record["uid"]),
        int(record["gid"]),
        follow_symlinks=False,
    )
    if not is_symlink:
        os.chmod(path, int(str(record["mode"]), 8), follow_symlinks=False)
    _set_xattrs(path, record["xattrs"])
    try:
        os.utime(
            path,
            ns=(int(record["mtimeNs"]), int(record["mtimeNs"])),
            follow_symlinks=False,
        )
    except (NotImplementedError, OSError):
        if not is_symlink:
            raise


def _copy_semantic(
    source: Path,
    target: Path,
    *,
    records: Sequence[Mapping[str, Any]] | None = None,
) -> None:
    records = list(records) if records is not None else _tree(source)
    root = records[0]
    if root["type"] == "file":
        shutil.copyfile(source, target, follow_symlinks=False)
        _apply_metadata(target, root)
        _fsync_regular(target)
        return
    if root["type"] == "symlink":
        os.symlink(os.readlink(source), target)
        _apply_metadata(target, root)
        return
    target.mkdir(mode=0o700)
    for record in records[1:]:
        destination = target / str(record["path"])
        source_child = source / str(record["path"])
        if record["type"] == "directory":
            destination.mkdir(mode=0o700)
        elif record["type"] == "symlink":
            os.symlink(os.readlink(source_child), destination)
            _apply_metadata(destination, record)
        else:
            shutil.copyfile(source_child, destination, follow_symlinks=False)
            _apply_metadata(destination, record)
            _fsync_regular(destination)
    for record in reversed(records):
        if record["type"] == "directory":
            destination = target if record["path"] == "." else target / str(record["path"])
            _apply_metadata(destination, record)
            _fsync_directory(destination)


def _remove_node(path: Path) -> None:
    metadata = _lstat_optional(path)
    if metadata is None:
        return
    if stat.S_ISDIR(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode):
        if os.path.ismount(path):
            _fail(f"refusing to remove mounted Candidate path: {path}")
        # Validate first so rmtree can never traverse an unexpected special
        # node or hard-linked regular file.
        _tree(path)
        shutil.rmtree(path)
    elif stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        path.unlink()
    else:
        _fail(f"refusing to remove special Candidate object: {path}")


def _write_manifest(path: Path, payload: Mapping[str, Any]) -> None:
    raw = (
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False).encode(
            "utf-8"
        )
        + b"\n"
    )
    if len(raw) > MAX_MANIFEST_BYTES:
        _fail("Candidate preimage manifest exceeds the safe size limit")
    path.write_bytes(raw)
    os.chmod(path, 0o600)
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def _identity(arguments: argparse.Namespace) -> Mapping[str, str]:
    if not GIT_SHA_PATTERN.fullmatch(arguments.commit):
        _fail("commit must be a full lowercase git SHA")
    if not SHA256_PATTERN.fullmatch(arguments.archive_sha256):
        _fail("archive SHA-256 must be lowercase and complete")
    if not SLOT_PATTERN.fullmatch(arguments.candidate_slot):
        _fail("Candidate slot must be 8000 or 8001")
    return {
        "commit": arguments.commit,
        "archiveSha256": arguments.archive_sha256,
        "candidateSlot": arguments.candidate_slot,
    }


def _boot_id(arguments: argparse.Namespace) -> str:
    path = _normalized_absolute(str(arguments.boot_id_file), "boot ID file")
    _assert_real_parent_chain(path, "boot ID file")
    metadata = path.lstat()
    procfs_virtual_file = (
        path == PROCFS_BOOT_ID_FILE
        and metadata.st_size == 0
        and metadata.st_uid == 0
        and metadata.st_gid == 0
        and not stat.S_IMODE(metadata.st_mode) & 0o022
    )
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_nlink != 1
        or (metadata.st_size <= 0 and not procfs_virtual_file)
        or metadata.st_size > MAX_BOOT_ID_BYTES
    ):
        _fail("boot ID file must be a small regular file")
    descriptor = os.open(
        path,
        os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
    )
    try:
        opened = os.fstat(descriptor)
        if (
            metadata.st_dev,
            metadata.st_ino,
            metadata.st_mode,
            metadata.st_nlink,
        ) != (
            opened.st_dev,
            opened.st_ino,
            opened.st_mode,
            opened.st_nlink,
        ):
            _fail("boot ID file changed while opening")
        raw = bytearray()
        while len(raw) <= MAX_BOOT_ID_BYTES:
            chunk = os.read(descriptor, MAX_BOOT_ID_BYTES + 1 - len(raw))
            if not chunk:
                break
            raw.extend(chunk)
        if not raw or len(raw) > MAX_BOOT_ID_BYTES:
            _fail("boot ID file must contain at most 128 bytes")
        closed = os.fstat(descriptor)
        if (
            opened.st_dev,
            opened.st_ino,
            opened.st_mode,
            opened.st_nlink,
        ) != (
            closed.st_dev,
            closed.st_ino,
            closed.st_mode,
            closed.st_nlink,
        ):
            _fail("boot ID file changed while reading")
    finally:
        os.close(descriptor)
    value = bytes(raw).decode("ascii").strip()
    if not BOOT_ID_PATTERN.fullmatch(value):
        _fail("boot ID file does not contain one canonical boot ID")
    return value


def _stable_regular_source(path: Path, label: str) -> Mapping[str, Any]:
    _assert_real_parent_chain(path, label)
    before = _lstat_optional(path)
    if (
        before is None
        or not stat.S_ISREG(before.st_mode)
        or stat.S_ISLNK(before.st_mode)
        or before.st_nlink != 1
    ):
        _fail(f"{label} must be one unlinked regular file")
    digest = _sha256_file(path)
    after = path.lstat()
    if (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    ) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        _fail(f"{label} changed while authorizing Candidate writes")
    return {"bytes": before.st_size, "sha256": digest}


def _authorized_live(arguments: argparse.Namespace) -> Mapping[str, Any]:
    target = str(arguments.post_slot_link_target or "")
    if not target or "\n" in target or "\r" in target or not target.startswith("/"):
        _fail("post slot-link target must be one absolute path")
    memory_high = arguments.post_memory_high_bytes
    memory_max = arguments.post_memory_max_bytes
    active_memory_high = arguments.post_active_memory_high_bytes
    active_memory_max = arguments.post_active_memory_max_bytes
    cpu_quota = arguments.post_cpu_quota_percent
    active_cpu_quota = arguments.post_active_cpu_quota_percent
    cache_max = arguments.candidate_cache_max_bytes
    for value, label in (
        (memory_high, "post memory-high bytes"),
        (memory_max, "post memory-max bytes"),
        (active_memory_high, "post active memory-high bytes"),
        (active_memory_max, "post active memory-max bytes"),
        (cpu_quota, "post CPU quota percent"),
        (active_cpu_quota, "post active CPU quota percent"),
        (cache_max, "Candidate cache byte limit"),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            _fail(f"{label} must be a positive integer")
    if (
        memory_high >= memory_max
        or active_memory_high >= active_memory_max
        or cpu_quota > 1000
        or active_cpu_quota > 1000
    ):
        _fail("Candidate resource authorization is invalid")
    unit = _stable_regular_source(arguments.post_unit_source, "post unit source")
    env = _stable_regular_source(arguments.post_env_source, "post env source")
    sandbox = _stable_regular_source(
        arguments.post_sandbox_source,
        "post sandbox source",
    )
    target_owner = {"uid": os.geteuid(), "gid": os.getegid()}
    return {
        "slot_link": {
            "kind": "symlink_target",
            "target": target,
            **target_owner,
        },
        "slot_link_stage": {
            "kind": "staging_symlink_or_absent",
            "allowAbsent": True,
            "target": target,
            **target_owner,
        },
        "slot_env": {
            "kind": "regular_file",
            "mode": "0600",
            **env,
            **target_owner,
        },
        "slot_env_stage": {
            "kind": "staging_regular_or_absent",
            "allowAbsent": True,
            "mode": "0600",
            **env,
            **target_owner,
        },
        "explicit_unit": {
            "kind": "regular_file_or_absent",
            "allowAbsent": True,
            "mode": "0644",
            **unit,
            **target_owner,
        },
        "explicit_unit_stage": {
            "kind": "staging_regular_or_absent",
            "allowAbsent": True,
            "mode": "0644",
            **unit,
            **target_owner,
        },
        "instance_dropins": {
            "kind": "single_file_directory_delta",
            "allowAbsentOrEmpty": True,
            "name": "10-candidate-sandbox.conf",
            "stageName": SANDBOX_STAGE_NAME,
            "mode": "0644",
            **sandbox,
            **target_owner,
        },
        "persistent_control_dropins": {
            "kind": "systemd_resource_directory_delta",
            "allowAbsentOrPartial": True,
            "memoryHighBytes": memory_high,
            "memoryMaxBytes": memory_max,
            "cpuQuotaPercent": cpu_quota,
            "activeMemoryHighBytes": active_memory_high,
            "activeMemoryMaxBytes": active_memory_max,
            "activeCpuQuotaPercent": active_cpu_quota,
            **target_owner,
        },
        "runtime_control_dropins": {
            "kind": "systemd_resource_directory_delta",
            "allowAbsentOrPartial": True,
            "memoryHighBytes": memory_high,
            "memoryMaxBytes": memory_max,
            "cpuQuotaPercent": cpu_quota,
            "activeMemoryHighBytes": active_memory_high,
            "activeMemoryMaxBytes": active_memory_max,
            "activeCpuQuotaPercent": active_cpu_quota,
            **target_owner,
        },
        "candidate_cache_link": {
            "kind": "symlink_destination_or_absent",
            "allowAbsent": True,
            "destination": str(arguments.candidate_cache_private),
            **target_owner,
        },
        "candidate_cache_private": {
            "kind": "bounded_cache_directory",
            "allowAbsent": True,
            "maxBytes": cache_max,
            "maxNodes": 4096,
        },
    }


def _role_paths(arguments: argparse.Namespace) -> Mapping[str, Path]:
    result = {
        role: _normalized_absolute(
            str(getattr(arguments, argument.replace("-", "_"))),
            role,
        )
        for role, argument in ROLE_ARGUMENTS
    }
    raw_paths = [str(path) for path in result.values()]
    if len(set(raw_paths)) != len(raw_paths):
        _fail("Candidate preimage paths must be unique")
    for first in result.values():
        _assert_real_parent_chain(first, "Candidate path")
        for second in result.values():
            if first == second:
                continue
            try:
                second.relative_to(first)
            except ValueError:
                continue
            _fail("Candidate preimage paths must not contain one another")
    return result


def _assert_install_staging_absent(role_paths: Mapping[str, Path]) -> None:
    for role in ("slot_link_stage", "slot_env_stage", "explicit_unit_stage"):
        if _lstat_optional(role_paths[role]) is not None:
            _fail(f"Candidate transaction staging remains: {role}")
    sandbox_stage = role_paths["instance_dropins"] / SANDBOX_STAGE_NAME
    if _lstat_optional(sandbox_stage) is not None:
        _fail("Candidate sandbox transaction staging remains")


def _preimage_path(arguments: argparse.Namespace, identity: Mapping[str, str]) -> Path:
    path = _normalized_absolute(str(arguments.preimage), "preimage")
    if (
        path.name != identity["archiveSha256"]
        or path.parent.name != identity["commit"]
        or path.parent.parent.name != "candidate-preimages"
    ):
        _fail("preimage path does not match the release identity namespace")
    _assert_real_parent_chain(path, "preimage")
    return path


def _manifest_sha256(preimage: Path) -> str:
    manifest = preimage / "manifest.json"
    metadata = manifest.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) & 0o077
        or metadata.st_size <= 0
        or metadata.st_size > MAX_MANIFEST_BYTES
    ):
        _fail("Candidate preimage manifest is unsafe for owner binding")
    return _sha256_file(manifest)


def _slot_owner_path(preimage: Path, slot: str, *, create: bool) -> Path:
    state_root = preimage.parents[2]
    _require_private_owner_directory(state_root, "Candidate preimage storage root")
    owner_root = state_root / "slot-owners"
    if create:
        _create_private_child(state_root, "slot-owners")
    else:
        _require_private_owner_directory(owner_root, "Candidate slot owner namespace")
    return owner_root / f"{slot}.json"


def _load_slot_owner(
    preimage: Path,
    slot: str,
    *,
    required: bool,
) -> tuple[Mapping[str, Any] | None, Path]:
    owner_path = _slot_owner_path(preimage, slot, create=not required)
    metadata = _lstat_optional(owner_path)
    if metadata is None:
        if required:
            _fail(f"Candidate slot {slot} has no outstanding preimage owner")
        return None, owner_path
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) & 0o077
        or metadata.st_size <= 0
        or metadata.st_size > 16 * 1024
    ):
        _fail(f"Candidate slot {slot} owner record is unsafe")
    raw = owner_path.read_bytes()
    try:
        owner = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise PreimageError(f"Candidate slot {slot} owner record is invalid") from exc
    if (
        not isinstance(owner, dict)
        or set(owner) != SLOT_OWNER_FIELDS
        or owner.get("schemaVersion") != SCHEMA_VERSION
        or owner.get("kind") != "candidate_runtime_preimage_owner"
        or not isinstance(owner.get("identity"), dict)
        or set(owner["identity"]) != IDENTITY_FIELDS
        or not GIT_SHA_PATTERN.fullmatch(
            str(owner["identity"].get("commit") or "")
        )
        or not SHA256_PATTERN.fullmatch(
            str(owner["identity"].get("archiveSha256") or "")
        )
        or not SLOT_PATTERN.fullmatch(
            str(owner["identity"].get("candidateSlot") or "")
        )
        or owner["identity"].get("candidateSlot") != slot
        or not SHA256_PATTERN.fullmatch(str(owner.get("manifestSha256") or ""))
    ):
        _fail(f"Candidate slot {slot} owner record contract is invalid")
    owner_preimage = _normalized_absolute(str(owner.get("preimage") or ""), "owner preimage")
    expected_preimage = (
        preimage.parents[2]
        / "candidate-preimages"
        / str(owner["identity"]["commit"])
        / str(owner["identity"]["archiveSha256"])
    )
    if owner_preimage != expected_preimage:
        _fail(f"Candidate slot {slot} owner path differs from its identity")
    return owner, owner_path


def _atomic_write_slot_owner(path: Path, payload: Mapping[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.new")
    temporary_metadata = _lstat_optional(temporary)
    if temporary_metadata is not None:
        if (
            not stat.S_ISREG(temporary_metadata.st_mode)
            or stat.S_ISLNK(temporary_metadata.st_mode)
            or temporary_metadata.st_uid != os.geteuid()
            or stat.S_IMODE(temporary_metadata.st_mode) & 0o077
        ):
            _fail("Candidate slot owner temporary is unsafe")
        temporary.unlink()
        _fsync_directory(path.parent)
    _write_manifest(temporary, payload)
    os.replace(temporary, path)
    _fsync_directory(path.parent)


def _bind_slot_owner(
    preimage: Path,
    identity: Mapping[str, str],
) -> None:
    owner, owner_path = _load_slot_owner(
        preimage,
        identity["candidateSlot"],
        required=False,
    )
    expected = {
        "schemaVersion": SCHEMA_VERSION,
        "kind": "candidate_runtime_preimage_owner",
        "identity": identity,
        "preimage": str(preimage),
        "manifestSha256": _manifest_sha256(preimage),
    }
    if owner is not None:
        if owner != expected:
            foreign = owner.get("identity", {})
            _fail(
                "Candidate slot has an outstanding preimage owned by "
                f"commit={foreign.get('commit', 'unknown')} "
                f"archive={foreign.get('archiveSha256', 'unknown')}"
            )
        return
    _atomic_write_slot_owner(owner_path, expected)


def _require_current_slot_owner(
    preimage: Path,
    identity: Mapping[str, str],
) -> Path:
    owner, owner_path = _load_slot_owner(
        preimage,
        identity["candidateSlot"],
        required=True,
    )
    assert owner is not None
    if (
        owner.get("identity") != identity
        or owner.get("preimage") != str(preimage)
        or owner.get("manifestSha256") != _manifest_sha256(preimage)
    ):
        foreign = owner.get("identity", {})
        _fail(
            "Candidate slot outstanding owner differs from this release: "
            f"commit={foreign.get('commit', 'unknown')}"
        )
    return owner_path


def _slot_owner_for_identity(
    preimage: Path,
    identity: Mapping[str, str],
) -> tuple[Mapping[str, Any] | None, Path]:
    owner, owner_path = _load_slot_owner(
        preimage,
        identity["candidateSlot"],
        required=False,
    )
    if owner is not None and (
        owner.get("identity") != identity
        or owner.get("preimage") != str(preimage)
    ):
        foreign = owner.get("identity", {})
        _fail(
            "Candidate slot has an outstanding preimage owned by "
            f"commit={foreign.get('commit', 'unknown')} "
            f"archive={foreign.get('archiveSha256', 'unknown')}"
        )
    return owner, owner_path


def _clear_current_slot_owner(
    preimage: Path,
    identity: Mapping[str, str],
) -> None:
    owner_path = _require_current_slot_owner(preimage, identity)
    owner_path.unlink()
    _fsync_directory(owner_path.parent)


def _restore_intent_paths(preimage: Path) -> tuple[Path, Path]:
    intent = preimage.parent / f".{preimage.name}.restore-intent.json"
    temporary = preimage.parent / f".{preimage.name}.restore-intent.json.new"
    return intent, temporary


def _restore_intent_payload(
    preimage: Path,
    manifest: Mapping[str, Any],
) -> Mapping[str, Any]:
    return {
        "schemaVersion": SCHEMA_VERSION,
        "kind": "candidate_runtime_restore_intent",
        "identity": manifest["identity"],
        "preimage": str(preimage),
        "manifestSha256": _manifest_sha256(preimage),
        "roles": list(ROLE_NAMES),
    }


def _safe_restore_intent_file(
    path: Path,
    label: str,
    *,
    allow_partial: bool = False,
) -> os.stat_result:
    metadata = path.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_uid != os.geteuid()
        or metadata.st_gid != os.getegid()
        or stat.S_IMODE(metadata.st_mode) & (0o022 if allow_partial else 0o077)
        or (not allow_partial and metadata.st_size <= 0)
        or metadata.st_size > RESTORE_INTENT_MAX_BYTES
    ):
        _fail(f"{label} is unsafe")
    return metadata


def _load_restore_intent(
    preimage: Path,
    manifest: Mapping[str, Any],
) -> Mapping[str, Any] | None:
    intent_path, temporary = _restore_intent_paths(preimage)
    intent_metadata = _lstat_optional(intent_path)
    temporary_metadata = _lstat_optional(temporary)
    if intent_metadata is None:
        if temporary_metadata is not None:
            _safe_restore_intent_file(
                temporary,
                "Candidate restore-intent temporary",
                allow_partial=True,
            )
        return None
    if temporary_metadata is not None:
        _fail("Candidate restore intent and temporary both exist")
    _safe_restore_intent_file(intent_path, "Candidate restore intent")
    try:
        raw = json.loads(intent_path.read_text(encoding="utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise PreimageError("Candidate restore intent is invalid JSON") from exc
    expected = _restore_intent_payload(preimage, manifest)
    if (
        not isinstance(raw, dict)
        or set(raw) != RESTORE_INTENT_FIELDS
        or raw != expected
    ):
        _fail("Candidate restore intent contract changed")
    return raw


def _publish_restore_intent(
    preimage: Path,
    manifest: Mapping[str, Any],
) -> None:
    intent_path, temporary = _restore_intent_paths(preimage)
    if _lstat_optional(intent_path) is not None:
        _load_restore_intent(preimage, manifest)
        return
    temporary_metadata = _lstat_optional(temporary)
    if temporary_metadata is not None:
        _safe_restore_intent_file(
            temporary,
            "Candidate restore-intent temporary",
            allow_partial=True,
        )
        temporary.unlink()
        _fsync_directory(temporary.parent)
    _write_manifest(temporary, _restore_intent_payload(preimage, manifest))
    os.replace(temporary, intent_path)
    _fsync_directory(intent_path.parent)
    if _load_restore_intent(preimage, manifest) is None:
        _fail("Candidate restore intent was not published")


def _remove_restore_intent(
    preimage: Path,
    manifest: Mapping[str, Any],
) -> None:
    intent_path, temporary = _restore_intent_paths(preimage)
    if _lstat_optional(temporary) is not None:
        _fail("Candidate restore-intent temporary remains after restore")
    if _load_restore_intent(preimage, manifest) is None:
        _fail("Candidate restore intent disappeared before settlement")
    intent_path.unlink()
    _fsync_directory(intent_path.parent)


def _assert_restore_intent_absent(preimage: Path) -> None:
    intent_path, temporary = _restore_intent_paths(preimage)
    if (
        _lstat_optional(intent_path) is not None
        or _lstat_optional(temporary) is not None
    ):
        _fail("Candidate restore intent must be settled first")


def _validate_authorization(raw: object) -> Mapping[str, Any]:
    if not isinstance(raw, dict) or set(raw) != set(ROLE_NAMES):
        _fail("Candidate live authorization role set is invalid")
    expected_fields = {
        "symlink_target": {"kind", "target", "uid", "gid"},
        "regular_file": {"kind", "mode", "bytes", "sha256", "uid", "gid"},
        "regular_file_or_absent": {
            "kind",
            "allowAbsent",
            "mode",
            "bytes",
            "sha256",
            "uid",
            "gid",
        },
        "single_file_directory_delta": {
            "kind",
            "allowAbsentOrEmpty",
            "name",
            "stageName",
            "mode",
            "bytes",
            "sha256",
            "uid",
            "gid",
        },
        "systemd_resource_directory_delta": {
            "kind",
            "allowAbsentOrPartial",
            "memoryHighBytes",
            "memoryMaxBytes",
            "cpuQuotaPercent",
            "activeMemoryHighBytes",
            "activeMemoryMaxBytes",
            "activeCpuQuotaPercent",
            "uid",
            "gid",
        },
        "bounded_cache_directory": {
            "kind",
            "allowAbsent",
            "maxBytes",
            "maxNodes",
        },
        "symlink_destination_or_absent": {
            "kind",
            "allowAbsent",
            "destination",
            "uid",
            "gid",
        },
        "staging_symlink_or_absent": {
            "kind",
            "allowAbsent",
            "target",
            "uid",
            "gid",
        },
        "staging_regular_or_absent": {
            "kind",
            "allowAbsent",
            "mode",
            "bytes",
            "sha256",
            "uid",
            "gid",
        },
    }
    for role, value in raw.items():
        if not isinstance(value, dict) or value.get("kind") not in expected_fields:
            _fail(f"Candidate live authorization is invalid: {role}")
        if set(value) != expected_fields[value["kind"]]:
            _fail(f"Candidate live authorization fields are not exact: {role}")
        if "sha256" in value and not SHA256_PATTERN.fullmatch(
            str(value.get("sha256") or "")
        ):
            _fail(f"Candidate live authorization digest is invalid: {role}")
    return raw


def _validate_record(record: object, label: str) -> Mapping[str, Any]:
    required_common = {"path", "mode", "uid", "gid", "mtimeNs", "xattrs", "type"}
    if not isinstance(record, dict) or not required_common.issubset(record):
        _fail(f"{label} record is malformed")
    kind = record.get("type")
    expected = set(required_common)
    if kind == "file":
        expected.update({"bytes", "sha256"})
        if not SHA256_PATTERN.fullmatch(str(record.get("sha256") or "")):
            _fail(f"{label} file digest is invalid")
    elif kind == "symlink":
        expected.update({"target", "targetBytes"})
    elif kind != "directory":
        _fail(f"{label} record type is unsupported")
    if set(record) != expected:
        _fail(f"{label} record fields are not exact")
    if not isinstance(record.get("xattrs"), dict):
        _fail(f"{label} xattrs are invalid")
    return record


def _load_preimage(
    preimage: Path,
    identity: Mapping[str, str],
    role_paths: Mapping[str, Path],
) -> Mapping[str, Any]:
    _require_private_owner_directory(
        preimage.parents[2],
        "Candidate preimage storage root",
    )
    _require_private_owner_directory(
        preimage.parent.parent,
        "Candidate preimage namespace",
    )
    _require_private_owner_directory(
        preimage.parent,
        "Candidate release preimage namespace",
    )
    metadata = _lstat_optional(preimage)
    if metadata is None:
        _fail(f"Candidate preimage does not exist: {preimage}")
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        _fail("Candidate preimage root is not a private real directory")
    manifest_path = preimage / "manifest.json"
    manifest_metadata = _lstat_optional(manifest_path)
    if (
        manifest_metadata is None
        or not stat.S_ISREG(manifest_metadata.st_mode)
        or stat.S_ISLNK(manifest_metadata.st_mode)
        or manifest_metadata.st_nlink != 1
        or manifest_metadata.st_uid != os.geteuid()
        or manifest_metadata.st_size <= 0
        or manifest_metadata.st_size > MAX_MANIFEST_BYTES
        or stat.S_IMODE(manifest_metadata.st_mode) & 0o077
    ):
        _fail("Candidate preimage manifest is missing or unsafe")
    raw = manifest_path.read_bytes()
    try:
        manifest = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise PreimageError("Candidate preimage manifest is invalid JSON") from exc
    if not isinstance(manifest, dict) or set(manifest) != MANIFEST_FIELDS:
        _fail("Candidate preimage manifest fields are not exact")
    if (
        manifest.get("schemaVersion") != SCHEMA_VERSION
        or manifest.get("kind") != "candidate_runtime_preimage"
        or manifest.get("identity") != identity
        or not BOOT_ID_PATTERN.fullmatch(str(manifest.get("bootId") or ""))
    ):
        _fail("Candidate preimage identity changed")
    policies = _validate_authorization(manifest.get("authorizedLive"))
    paths = manifest.get("paths")
    if not isinstance(paths, list) or len(paths) != len(ROLE_NAMES):
        _fail("Candidate preimage path list is incomplete")
    by_role: dict[str, Mapping[str, Any]] = {}
    for raw_entry in paths:
        if not isinstance(raw_entry, dict) or set(raw_entry) != PATH_FIELDS:
            _fail("Candidate preimage path entry fields are not exact")
        role = raw_entry.get("role")
        if role not in ROLE_NAMES or role in by_role:
            _fail("Candidate preimage role set is invalid")
        if raw_entry.get("path") != str(role_paths[role]):
            _fail(f"Candidate preimage path changed for role {role}")
        state = raw_entry.get("state")
        payload_name = raw_entry.get("payload")
        tree = raw_entry.get("tree")
        if state == "absent":
            if payload_name is not None or tree != []:
                _fail(f"absent Candidate role has payload: {role}")
        elif state == "present":
            if payload_name != role or not isinstance(tree, list) or not tree:
                _fail(f"present Candidate role lacks payload: {role}")
            validated_tree = [
                _validate_record(record, f"{role}[{index}]")
                for index, record in enumerate(tree)
            ]
            payload_path = preimage / "payload" / role
            if _role_tree(payload_path, role, policies) != validated_tree:
                _fail(f"Candidate preimage payload changed: {role}")
        else:
            _fail(f"Candidate preimage state is invalid: {role}")
        by_role[role] = raw_entry
    if set(by_role) != set(ROLE_NAMES):
        _fail("Candidate preimage role set is incomplete")
    return manifest


def capture(
    preimage: Path,
    identity: Mapping[str, str],
    role_paths: Mapping[str, Path],
    authorized_live: Mapping[str, Any],
    boot_id: str,
) -> Mapping[str, Any]:
    owner, _ = _slot_owner_for_identity(preimage, identity)
    _assert_install_staging_absent(role_paths)
    _assert_restore_intent_absent(preimage)
    if _lstat_optional(preimage) is not None:
        if owner is not None:
            _require_current_slot_owner(preimage, identity)
        manifest = _load_preimage(preimage, identity, role_paths)
        if manifest["authorizedLive"] != authorized_live:
            _fail("existing Candidate preimage has different live authorization")
        verify_live(manifest, role_paths, boot_id)
        if manifest["bootId"] == boot_id:
            _bind_slot_owner(preimage, identity)
            return {"decision": "reused", "preimage": str(preimage)}
        if owner is not None:
            _fail("armed Candidate preimage belongs to another boot")
        _remove_node(preimage)
        _fsync_directory(preimage.parent)
    if owner is not None:
        _fail("Candidate slot owner points to a missing current preimage")
    state_root = preimage.parents[2]
    _require_private_owner_directory(state_root, "blue/green state root")
    namespace = _create_private_child(state_root, "candidate-preimages")
    _guard_preimage_storage(
        state_root,
        authorized_live["candidate_cache_private"],
    )
    release_namespace = _create_private_child(namespace, identity["commit"])
    if release_namespace != preimage.parent:
        _fail("preimage namespace differs from release identity")
    temporary = preimage.parent / f".{preimage.name}.capture.new"
    temporary_metadata = _lstat_optional(temporary)
    if temporary_metadata is not None:
        if (
            stat.S_ISLNK(temporary_metadata.st_mode)
            or not stat.S_ISDIR(temporary_metadata.st_mode)
            or temporary_metadata.st_uid != os.geteuid()
            or stat.S_IMODE(temporary_metadata.st_mode) & 0o077
        ):
            _fail("stale Candidate preimage temporary is unsafe")
        _remove_node(temporary)
        _fsync_directory(preimage.parent)
    temporary.mkdir(mode=0o700)
    try:
        payload_root = temporary / "payload"
        payload_root.mkdir(mode=0o700)
        path_entries: list[Mapping[str, Any]] = []
        for role in ROLE_NAMES:
            source = role_paths[role]
            if _lstat_optional(source) is None:
                path_entries.append(
                    {
                        "role": role,
                        "path": str(source),
                        "state": "absent",
                        "payload": None,
                        "tree": [],
                    }
                )
                continue
            try:
                before_tree = _role_tree(source, role, authorized_live)
            except PreimageError as exc:
                if role == "candidate_cache_private":
                    raise PreimageError(
                        "existing Candidate cache exceeds the safe preimage contract"
                    ) from exc
                raise
            destination = payload_root / role
            _copy_semantic(source, destination, records=before_tree)
            if (
                _role_tree(source, role, authorized_live) != before_tree
                or _role_tree(destination, role, authorized_live) != before_tree
            ):
                _fail(f"Candidate path changed while capturing preimage: {role}")
            path_entries.append(
                {
                    "role": role,
                    "path": str(source),
                    "state": "present",
                    "payload": role,
                    "tree": before_tree,
                }
            )
        manifest = {
            "schemaVersion": SCHEMA_VERSION,
            "kind": "candidate_runtime_preimage",
            "identity": identity,
            "createdAt": dt.datetime.now(dt.timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "bootId": boot_id,
            "paths": path_entries,
            "authorizedLive": authorized_live,
        }
        verify_live(manifest, role_paths, boot_id)
        _write_manifest(temporary / "manifest.json", manifest)
        _fsync_directory(payload_root)
        _fsync_directory(temporary)
        os.replace(temporary, preimage)
        _fsync_directory(preimage.parent)
    finally:
        if _lstat_optional(temporary) is not None:
            shutil.rmtree(temporary)
    _load_preimage(preimage, identity, role_paths)
    _bind_slot_owner(preimage, identity)
    return {"decision": "captured", "preimage": str(preimage)}


def _effective_entry(
    manifest: Mapping[str, Any],
    role: str,
    entry: Mapping[str, Any],
    boot_id: str,
) -> Mapping[str, Any]:
    if role == "runtime_control_dropins" and manifest["bootId"] != boot_id:
        return {
            "role": role,
            "path": entry["path"],
            "state": "absent",
            "payload": None,
            "tree": [],
        }
    return entry


def verify_live(
    manifest: Mapping[str, Any],
    role_paths: Mapping[str, Path],
    boot_id: str,
) -> None:
    entries = {entry["role"]: entry for entry in manifest["paths"]}
    policies = manifest["authorizedLive"]
    for role in ROLE_NAMES:
        entry = _effective_entry(manifest, role, entries[role], boot_id)
        live = role_paths[role]
        metadata = _lstat_optional(live)
        if entry["state"] == "absent":
            if metadata is not None:
                _fail(f"Candidate path should be absent after restore: {role}")
            continue
        if metadata is None or _role_tree(live, role, policies) != entry["tree"]:
            _fail(f"Candidate path differs from captured preimage: {role}")


def _regular_matches_policy(path: Path, policy: Mapping[str, Any]) -> bool:
    metadata = _lstat_optional(path)
    if (
        metadata is None
        or not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_uid != policy["uid"]
        or metadata.st_gid != policy["gid"]
        or format(stat.S_IMODE(metadata.st_mode), "04o") != policy["mode"]
        or metadata.st_size != policy["bytes"]
    ):
        return False
    return _sha256_file(path) == policy["sha256"]


def _staging_regular_matches(path: Path, policy: Mapping[str, Any]) -> bool:
    metadata = _lstat_optional(path)
    if (
        metadata is None
        or not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_uid != policy["uid"]
        or metadata.st_gid != policy["gid"]
        or format(stat.S_IMODE(metadata.st_mode), "04o")
        not in {"0600", policy["mode"]}
        or metadata.st_size > policy["bytes"]
        or _xattrs(path)
    ):
        return False
    return bool(
        metadata.st_size < policy["bytes"]
        or _sha256_file(path) == policy["sha256"]
    )


def _directory_root_is_owned(path: Path, policy: Mapping[str, Any]) -> bool:
    metadata = _lstat_optional(path)
    return bool(
        metadata is not None
        and stat.S_ISDIR(metadata.st_mode)
        and not stat.S_ISLNK(metadata.st_mode)
        and not os.path.ismount(path)
        and metadata.st_uid == policy["uid"]
        and metadata.st_gid == policy["gid"]
        and not stat.S_IMODE(metadata.st_mode) & 0o022
    )


def _records_without_mtime(record: Mapping[str, Any]) -> Mapping[str, Any]:
    return {key: value for key, value in record.items() if key != "mtimeNs"}


def _subtree_records(
    records: Mapping[str, Mapping[str, Any]],
    root: str,
) -> Mapping[str, Mapping[str, Any]]:
    prefix = f"{root}/"
    return {
        path: record
        for path, record in records.items()
        if path == root or path.startswith(prefix)
    }


def _directory_delta_matches(
    path: Path,
    entry: Mapping[str, Any],
    policy: Mapping[str, Any],
    controlled_names: Sequence[str],
    post_matcher: Callable[[Path, str], bool],
    *,
    allow_controlled_deletion: bool = False,
) -> bool:
    metadata = _lstat_optional(path)
    if metadata is None:
        return entry["state"] == "absent"
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or os.path.ismount(path)
    ):
        return False
    actual_tree = _tree(path)
    actual = {str(record["path"]): record for record in actual_tree}
    baseline = {
        str(record["path"]): record
        for record in entry["tree"]
    }
    if entry["state"] == "present":
        baseline_root = baseline.get(".")
        actual_root = actual.get(".")
        if (
            baseline_root is None
            or actual_root is None
            or baseline_root.get("type") != "directory"
            or _records_without_mtime(actual_root)
            != _records_without_mtime(baseline_root)
        ):
            return False
    elif not _directory_root_is_owned(path, policy):
        return False

    controlled = set(controlled_names)
    for relative, baseline_record in baseline.items():
        if relative == "." or any(
            relative == name or relative.startswith(f"{name}/")
            for name in controlled
        ):
            continue
        if actual.get(relative) != baseline_record:
            return False
    for relative in actual:
        if relative == "." or relative in baseline:
            continue
        if relative not in controlled:
            return False

    for name in controlled_names:
        baseline_subtree = _subtree_records(baseline, name)
        actual_subtree = _subtree_records(actual, name)
        if actual_subtree == baseline_subtree:
            continue
        if not actual_subtree:
            if not baseline_subtree or allow_controlled_deletion:
                continue
            return False
        if set(actual_subtree) != {name} or not post_matcher(path / name, name):
            return False
    return True


def _single_file_directory_matches(
    path: Path,
    entry: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> bool:
    file_policy = {
        key: policy[key]
        for key in ("uid", "gid", "mode", "bytes", "sha256")
    }
    final_name = str(policy["name"])
    stage_name = str(policy["stageName"])
    return _directory_delta_matches(
        path,
        entry,
        policy,
        [final_name, stage_name],
        lambda candidate, name: (
            _regular_matches_policy(candidate, file_policy)
            if name == final_name
            else _staging_regular_matches(candidate, file_policy)
        ),
        allow_controlled_deletion=True,
    )


def _semantic_systemd_lines(path: Path) -> list[str]:
    metadata = path.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_size <= 0
        or metadata.st_size > 4096
    ):
        return []
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        return []
    return [
        line.strip()
        for line in text.splitlines()
        if line.strip() and not line.lstrip().startswith(("#", ";"))
    ]


def _resource_value_matches(name: str, value: str, policy: Mapping[str, Any]) -> bool:
    if name == "50-MemoryHigh.conf":
        expected_values = {
            int(policy["memoryHighBytes"]),
            int(policy["activeMemoryHighBytes"]),
        }
        accepted = {str(expected) for expected in expected_values}
        accepted.update(
            f"{expected // (1024**3)}G"
            for expected in expected_values
            if expected % (1024**3) == 0
        )
        return value in {f"MemoryHigh={item}" for item in accepted}
    if name == "50-MemoryMax.conf":
        expected_values = {
            int(policy["memoryMaxBytes"]),
            int(policy["activeMemoryMaxBytes"]),
        }
        accepted = {str(expected) for expected in expected_values}
        accepted.update(
            f"{expected // (1024**3)}G"
            for expected in expected_values
            if expected % (1024**3) == 0
        )
        return value in {f"MemoryMax={item}" for item in accepted}
    if name == "50-CPUQuota.conf":
        percents = {
            int(policy["cpuQuotaPercent"]),
            int(policy["activeCpuQuotaPercent"]),
        }
        accepted = {f"CPUQuota={percent}%" for percent in percents}
        for percent in percents:
            if percent % 100 == 0:
                accepted.add(f"CPUQuotaPerSecUSec={percent // 100}s")
        return value in accepted
    return False


def _resource_file_matches(
    path: Path,
    name: str,
    policy: Mapping[str, Any],
) -> bool:
    metadata = _lstat_optional(path)
    if (
        metadata is None
        or metadata.st_uid != policy["uid"]
        or metadata.st_gid != policy["gid"]
        or stat.S_IMODE(metadata.st_mode) != 0o644
    ):
        return False
    lines = _semantic_systemd_lines(path)
    return bool(
        len(lines) == 2
        and lines[0] == "[Service]"
        and _resource_value_matches(name, lines[1], policy)
    )


def _resource_directory_matches(
    path: Path,
    entry: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> bool:
    allowed = [
        "50-CPUQuota.conf",
        "50-MemoryHigh.conf",
        "50-MemoryMax.conf",
    ]
    return _directory_delta_matches(
        path,
        entry,
        policy,
        allowed,
        lambda candidate, name: _resource_file_matches(candidate, name, policy),
    )


def _bounded_cache_matches(path: Path, policy: Mapping[str, Any]) -> bool:
    metadata = _lstat_optional(path)
    if metadata is None:
        return policy["allowAbsent"] is True
    try:
        _bounded_cache_tree(path, policy)
    except (OSError, PreimageError):
        return False
    return True


def _symlink_reaches_destination(path: Path, destination: str) -> bool:
    target = os.readlink(path)
    if not target or "\n" in target or "\r" in target:
        return False
    lexical = Path(
        os.path.normpath(
            target if os.path.isabs(target) else str(path.parent / target)
        )
    )
    return lexical == Path(destination)


def _matches_authorized_post(
    path: Path,
    entry: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> bool:
    kind = policy["kind"]
    metadata = _lstat_optional(path)
    if kind == "symlink_target":
        return bool(
            metadata is not None
            and stat.S_ISLNK(metadata.st_mode)
            and metadata.st_uid == policy["uid"]
            and metadata.st_gid == policy["gid"]
            and os.readlink(path) == policy["target"]
        )
    if kind == "staging_symlink_or_absent":
        return bool(
            (metadata is None and policy["allowAbsent"] is True)
            or (
                metadata is not None
                and stat.S_ISLNK(metadata.st_mode)
                and metadata.st_uid == policy["uid"]
                and metadata.st_gid == policy["gid"]
                and os.readlink(path) == policy["target"]
            )
        )
    if kind == "staging_regular_or_absent":
        if metadata is None:
            return policy["allowAbsent"] is True
        return _staging_regular_matches(path, policy)
    if kind == "regular_file":
        return _regular_matches_policy(path, policy)
    if kind == "regular_file_or_absent":
        return bool(
            (metadata is None and policy["allowAbsent"] is True)
            or _regular_matches_policy(path, policy)
        )
    if kind == "single_file_directory_delta":
        return _single_file_directory_matches(path, entry, policy)
    if kind == "systemd_resource_directory_delta":
        return _resource_directory_matches(path, entry, policy)
    if kind == "bounded_cache_directory":
        return _bounded_cache_matches(path, policy)
    if kind == "symlink_destination_or_absent":
        return bool(
            (metadata is None and policy["allowAbsent"] is True)
            or (
                metadata is not None
                and stat.S_ISLNK(metadata.st_mode)
                and metadata.st_uid == policy["uid"]
                and metadata.st_gid == policy["gid"]
                and _symlink_reaches_destination(path, str(policy["destination"]))
            )
        )
    return False


def _matches_preimage_state(
    path: Path,
    role: str,
    entry: Mapping[str, Any],
    policies: Mapping[str, Mapping[str, Any]],
) -> bool:
    metadata = _lstat_optional(path)
    if entry["state"] == "absent":
        return metadata is None
    return bool(
        metadata is not None
        and _role_tree(path, role, policies) == entry["tree"]
    )


def _temporary_paths(target: Path, role: str, token: str) -> tuple[Path, Path]:
    return (
        target.parent / f".{target.name}.jato-{token}-{role}.new",
        target.parent / f".{target.name}.jato-{token}-{role}.old",
    )


def _regular_is_prefix_of(candidate: Path, source: Path) -> bool:
    candidate_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    candidate_flags |= getattr(os, "O_NOFOLLOW", 0)
    source_flags = candidate_flags
    candidate_descriptor = os.open(candidate, candidate_flags)
    source_descriptor = os.open(source, source_flags)
    try:
        candidate_before = os.fstat(candidate_descriptor)
        source_before = os.fstat(source_descriptor)
        if (
            not stat.S_ISREG(candidate_before.st_mode)
            or not stat.S_ISREG(source_before.st_mode)
            or candidate_before.st_nlink != 1
            or source_before.st_nlink != 1
            or candidate_before.st_size > source_before.st_size
        ):
            return False
        remaining = candidate_before.st_size
        while remaining:
            amount = min(1024 * 1024, remaining)
            if os.read(candidate_descriptor, amount) != os.read(
                source_descriptor,
                amount,
            ):
                return False
            remaining -= amount
        candidate_after = os.fstat(candidate_descriptor)
        source_after = os.fstat(source_descriptor)
        return bool(
            (
                candidate_before.st_dev,
                candidate_before.st_ino,
                candidate_before.st_size,
                candidate_before.st_mtime_ns,
            )
            == (
                candidate_after.st_dev,
                candidate_after.st_ino,
                candidate_after.st_size,
                candidate_after.st_mtime_ns,
            )
            and (
                source_before.st_dev,
                source_before.st_ino,
                source_before.st_size,
                source_before.st_mtime_ns,
            )
            == (
                source_after.st_dev,
                source_after.st_ino,
                source_after.st_size,
                source_after.st_mtime_ns,
            )
        )
    finally:
        os.close(candidate_descriptor)
        os.close(source_descriptor)


def _partial_node_metadata_matches(
    metadata: os.stat_result,
    path: Path,
    expected: Mapping[str, Any],
) -> bool:
    if metadata.st_uid not in {os.geteuid(), int(expected["uid"])}:
        return False
    if metadata.st_gid not in {os.getegid(), int(expected["gid"])}:
        return False
    if not stat.S_ISLNK(metadata.st_mode):
        mode = stat.S_IMODE(metadata.st_mode)
        expected_mode = int(str(expected["mode"]), 8)
        if mode != expected_mode and mode not in {0o600, 0o644, 0o700}:
            return False
    actual_xattrs = _xattrs(path)
    expected_xattrs = expected["xattrs"]
    return all(
        expected_xattrs.get(name) == value
        for name, value in actual_xattrs.items()
    )


def _safe_partial_restore_staging(
    path: Path,
    source: Path,
    entry: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> bool:
    metadata = _lstat_optional(path)
    if metadata is None:
        return True
    expected = {
        str(record["path"]): record
        for record in entry["tree"]
    }
    max_nodes = int(policy.get("maxNodes", len(expected)))
    max_bytes = int(
        policy.get(
            "maxBytes",
            sum(
                int(record.get("bytes", 0))
                for record in expected.values()
            ),
        )
    )
    nodes = 0
    total_bytes = 0
    pending: list[tuple[Path, str]] = [(path, ".")]
    while pending:
        current, relative = pending.pop()
        current_metadata = current.lstat()
        expected_record = expected.get(relative)
        nodes += 1
        if (
            nodes > max_nodes
            or expected_record is None
            or not _partial_node_metadata_matches(
                current_metadata,
                current,
                expected_record,
            )
        ):
            return False
        source_node = source if relative == "." else source / relative
        expected_type = expected_record["type"]
        if stat.S_ISREG(current_metadata.st_mode):
            if expected_type != "file" or current_metadata.st_nlink != 1:
                return False
            total_bytes += current_metadata.st_size
            if total_bytes > max_bytes or not _regular_is_prefix_of(
                current,
                source_node,
            ):
                return False
        elif stat.S_ISLNK(current_metadata.st_mode):
            if (
                expected_type != "symlink"
                or os.readlink(current) != expected_record["target"]
            ):
                return False
        elif stat.S_ISDIR(current_metadata.st_mode):
            if expected_type != "directory" or os.path.ismount(current):
                return False
            with os.scandir(current) as children:
                for child in children:
                    if nodes + len(pending) >= max_nodes:
                        return False
                    child_relative = (
                        child.name
                        if relative == "."
                        else f"{relative}/{child.name}"
                    )
                    pending.append((Path(child.path), child_relative))
        else:
            return False
    return True


def _preflight_restore(
    preimage: Path,
    manifest: Mapping[str, Any],
    role_paths: Mapping[str, Path],
    boot_id: str,
    *,
    intent_armed: bool,
) -> None:
    token = str(manifest["identity"]["commit"])[:12]
    entries = {entry["role"]: entry for entry in manifest["paths"]}
    policies = manifest["authorizedLive"]
    for role in ROLE_NAMES:
        entry = _effective_entry(manifest, role, entries[role], boot_id)
        target = role_paths[role]
        new_path, old_path = _temporary_paths(target, role, token)
        live_accepted = _matches_preimage_state(
            target,
            role,
            entry,
            policies,
        ) or _matches_authorized_post(target, entry, policies[role])
        if entry["state"] == "absent":
            if _lstat_optional(new_path) is not None or _lstat_optional(old_path) is not None:
                _fail(f"unexpected restore temporary for absent Candidate role: {role}")
            if not live_accepted:
                _fail(f"Candidate live object is not owned by this release: {role}")
            continue
        parent_metadata = _lstat_optional(target.parent)
        if (
            parent_metadata is None
            or stat.S_ISLNK(parent_metadata.st_mode)
            or not stat.S_ISDIR(parent_metadata.st_mode)
            or os.path.ismount(target.parent)
        ):
            _fail(f"Candidate restore parent is unavailable: {role}")
        new_exists = _lstat_optional(new_path) is not None
        old_exists = _lstat_optional(old_path) is not None
        new_accepted = not new_exists or _matches_preimage_state(
            new_path,
            role,
            entry,
            policies,
        ) or _safe_partial_restore_staging(
            new_path,
            preimage / "payload" / role,
            entry,
            policies[role],
        )
        old_accepted = not old_exists or _matches_preimage_state(
            old_path,
            role,
            entry,
            policies,
        ) or _matches_authorized_post(old_path, entry, policies[role])
        interrupted_gap = (
            _lstat_optional(target) is None and new_exists and old_exists
        )
        live_missing_with_intent = (
            intent_armed and _lstat_optional(target) is None
        )
        if (
            not new_accepted
            or not old_accepted
            or not (
                live_accepted
                or interrupted_gap
                or live_missing_with_intent
            )
        ):
            _fail(f"Candidate restore state is not authorized: {role}")


def _restore_present(
    source: Path,
    target: Path,
    role: str,
    token: str,
    entry: Mapping[str, Any],
    policies: Mapping[str, Mapping[str, Any]],
) -> None:
    _assert_real_parent_chain(target, "restore target")
    new_path, old_path = _temporary_paths(target, role, token)
    if _lstat_optional(new_path) is not None and not _matches_preimage_state(
        new_path,
        role,
        entry,
        policies,
    ):
        _remove_node(new_path)
        _fsync_directory(target.parent)
    if _lstat_optional(new_path) is None:
        _copy_semantic(source, new_path, records=entry["tree"])
        # The copied inode/tree is durable inside itself; the parent fsync
        # makes the replacement name durable before any old live object moves.
        _fsync_directory(target.parent)
    if not _matches_preimage_state(
        new_path,
        role,
        entry,
        policies,
    ):
        _fail(f"Candidate restore replacement is incomplete: {role}")
    if _lstat_optional(target) is not None:
        if _lstat_optional(old_path) is not None:
            _remove_node(old_path)
            _fsync_directory(target.parent)
        os.replace(target, old_path)
        _fsync_directory(target.parent)
    os.replace(new_path, target)
    _fsync_directory(target.parent)
    if _lstat_optional(old_path) is not None:
        _remove_node(old_path)
        _fsync_directory(target.parent)


def restore(
    preimage: Path,
    manifest: Mapping[str, Any],
    role_paths: Mapping[str, Path],
    boot_id: str,
) -> Mapping[str, Any]:
    token = str(manifest["identity"]["commit"])[:12]
    entries = {entry["role"]: entry for entry in manifest["paths"]}
    policies = manifest["authorizedLive"]
    intent_armed = _load_restore_intent(preimage, manifest) is not None
    _preflight_restore(
        preimage,
        manifest,
        role_paths,
        boot_id,
        intent_armed=intent_armed,
    )
    if not intent_armed:
        _publish_restore_intent(preimage, manifest)
    for role in ROLE_NAMES:
        entry = _effective_entry(manifest, role, entries[role], boot_id)
        target = role_paths[role]
        if entry["state"] == "absent":
            _remove_node(target)
            if target.parent.exists():
                _fsync_directory(target.parent)
        else:
            _restore_present(
                preimage / "payload" / role,
                target,
                role,
                token,
                entry,
                policies,
            )
    verify_live(manifest, role_paths, boot_id)
    _remove_restore_intent(preimage, manifest)
    return {"decision": "restored", "preimage": str(preimage)}


def discard(
    preimage: Path,
    identity: Mapping[str, str],
    role_paths: Mapping[str, Path],
) -> Mapping[str, Any]:
    tombstone = preimage.parent / f".{preimage.name}.discarding"
    owner, owner_path = _slot_owner_for_identity(preimage, identity)
    preimage_exists = _lstat_optional(preimage) is not None
    tombstone_metadata = _lstat_optional(tombstone)
    if owner is None:
        if preimage_exists or tombstone_metadata is not None:
            _fail("unowned Candidate preimage residue cannot be discarded")
        return {"decision": "already-discarded", "preimage": str(preimage)}
    _assert_install_staging_absent(role_paths)
    _assert_restore_intent_absent(preimage)
    if preimage_exists and tombstone_metadata is not None:
        _fail("Candidate preimage and discard tombstone both exist")
    if preimage_exists:
        _require_current_slot_owner(preimage, identity)
        _load_preimage(preimage, identity, role_paths)
        os.replace(preimage, tombstone)
        _fsync_directory(preimage.parent)
        tombstone_metadata = tombstone.lstat()
    if tombstone_metadata is not None:
        if (
            stat.S_ISLNK(tombstone_metadata.st_mode)
            or not stat.S_ISDIR(tombstone_metadata.st_mode)
            or tombstone_metadata.st_uid != os.geteuid()
            or stat.S_IMODE(tombstone_metadata.st_mode) & 0o077
        ):
            _fail("Candidate preimage discard tombstone is unsafe")
        _remove_node(tombstone)
        _fsync_directory(preimage.parent)
    replay_owner, replay_owner_path = _slot_owner_for_identity(preimage, identity)
    if replay_owner != owner or replay_owner_path != owner_path:
        _fail("Candidate slot owner changed during discard")
    owner_path.unlink()
    _fsync_directory(owner_path.parent)
    return {"decision": "discarded", "preimage": str(preimage)}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("capture", "restore", "verify-live", "discard"))
    parser.add_argument("--preimage", required=True, type=Path)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--archive-sha256", required=True)
    parser.add_argument("--candidate-slot", required=True)
    parser.add_argument(
        "--boot-id-file",
        type=Path,
        default=Path("/proc/sys/kernel/random/boot_id"),
    )
    for _, argument in ROLE_ARGUMENTS:
        parser.add_argument(f"--{argument}", required=True, type=Path)
    parser.add_argument("--post-slot-link-target")
    parser.add_argument("--post-env-source", type=Path)
    parser.add_argument("--post-unit-source", type=Path)
    parser.add_argument("--post-sandbox-source", type=Path)
    parser.add_argument("--post-memory-high-bytes", type=int)
    parser.add_argument("--post-memory-max-bytes", type=int)
    parser.add_argument("--post-cpu-quota-percent", type=int)
    parser.add_argument("--post-active-memory-high-bytes", type=int)
    parser.add_argument("--post-active-memory-max-bytes", type=int)
    parser.add_argument("--post-active-cpu-quota-percent", type=int)
    parser.add_argument(
        "--candidate-cache-max-bytes",
        type=int,
        default=512 * 1024 * 1024,
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        identity = _identity(arguments)
        boot_id = _boot_id(arguments)
        roles = _role_paths(arguments)
        preimage = _preimage_path(arguments, identity)
        if arguments.command == "capture":
            authorization = _authorized_live(arguments)
            result = capture(preimage, identity, roles, authorization, boot_id)
        elif arguments.command == "discard":
            result = discard(preimage, identity, roles)
        else:
            owner, _ = _slot_owner_for_identity(preimage, identity)
            if owner is not None:
                _require_current_slot_owner(preimage, identity)
            manifest = _load_preimage(preimage, identity, roles)
            if arguments.command == "restore":
                if owner is None:
                    _assert_restore_intent_absent(preimage)
                    verify_live(manifest, roles, boot_id)
                    result = {
                        "decision": "already-restored",
                        "preimage": str(preimage),
                    }
                else:
                    result = restore(preimage, manifest, roles, boot_id)
                    _clear_current_slot_owner(preimage, identity)
            else:
                _assert_restore_intent_absent(preimage)
                verify_live(manifest, roles, boot_id)
                result = {"decision": "verified", "preimage": str(preimage)}
    except (OSError, PreimageError, ValueError, TypeError) as exc:
        print(f"candidate runtime preimage error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
