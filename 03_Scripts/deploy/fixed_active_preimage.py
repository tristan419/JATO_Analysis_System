#!/usr/bin/env python3
"""Capture and restore the fixed Active runtime files for one approved release.

The helper owns no processes and never touches application data.  It only
captures the two fixed-Active symlinks plus the slot environment and stable
Nginx include.  Restore is permitted only when every live path still matches
either the captured preimage or the immutable target authorized at capture.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import secrets
import shutil
import stat
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any

GIT_SHA = re.compile(r"^[0-9a-f]{40}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
SLOT = re.compile(r"^(8000|8001)$")
MAX_FILE_BYTES = 4 * 1024 * 1024
MAX_MANIFEST_BYTES = 64 * 1024
SCHEMA_VERSION = 1
LEGACY_FINGERPRINT_SCHEMA_VERSION = 1
LEGACY_EXCLUDED_PREFIXES = (
    ".git",
    "01_RAW_DATA",
    "04_Processed_data",
    "03_Scripts/diagnostics/artifacts",
    "03_Scripts/logs",
    "hermes/deploy_failure_context.txt",
    "hermes/reports",
)
LEGACY_IGNORED_COMPONENTS = (
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    "__pycache__",
    "node_modules",
)
LEGACY_IGNORED_SUFFIXES = (".pyc", ".pyo")
CONTENT_RELEASE_PROOF = re.compile(
    r"^content-addressed:[0-9a-f]{40}:[0-9a-f]{64}:"
    r"[0-9a-f]{64}:[0-9a-f]{64}$"
)
LEGACY_RELEASE_PROOF = re.compile(
    r"^legacy-private-fingerprint:[0-9a-f]{40}$"
)


class PreimageError(ValueError):
    """The requested fixed-Active preimage operation is unsafe."""


def _fail(message: str) -> None:
    raise PreimageError(message)


def _absolute(raw: str, label: str) -> Path:
    path = Path(raw)
    if (
        not raw
        or "\n" in raw
        or "\r" in raw
        or not path.is_absolute()
        or ".." in path.parts
        or str(path) != raw
    ):
        _fail(f"{label} must be one normalized absolute path")
    return path


def _lstat(path: Path) -> os.stat_result | None:
    try:
        return path.lstat()
    except FileNotFoundError:
        return None


def _fsync_dir(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _real_directory(path: Path, label: str) -> os.stat_result:
    metadata = _lstat(path)
    if (
        metadata is None
        or stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISDIR(metadata.st_mode)
    ):
        _fail(f"{label} must be a real directory: {path}")
    return metadata


def _real_parent_chain(path: Path, label: str) -> None:
    current = Path(path.anchor)
    for part in path.parent.parts[1:]:
        current /= part
        metadata = _lstat(current)
        if metadata is None:
            continue
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            _fail(f"{label} has an unsafe parent: {current}")


def _private_directory(path: Path, label: str) -> None:
    metadata = _real_directory(path, label)
    if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) & 0o077:
        _fail(f"{label} must remain private and owner-controlled: {path}")


def _ensure_private_child(parent: Path, name: str) -> Path:
    child = parent / name
    metadata = _lstat(child)
    if metadata is None:
        child.mkdir(mode=0o700)
        _fsync_dir(parent)
    _private_directory(child, "fixed Active preimage namespace")
    return child


def _open_regular(path: Path, label: str) -> tuple[int, os.stat_result]:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    metadata = os.fstat(descriptor)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_size < 0
        or metadata.st_size > MAX_FILE_BYTES
    ):
        os.close(descriptor)
        _fail(f"{label} must be one bounded unlinked regular file")
    return descriptor, metadata


def _read_regular(path: Path, label: str) -> tuple[bytes, Mapping[str, Any]]:
    descriptor, before = _open_regular(path, label)
    digest = hashlib.sha256()
    chunks: list[bytes] = []
    try:
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
            digest.update(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    identity = (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    )
    if identity != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        _fail(f"{label} changed while it was read")
    return b"".join(chunks), {
        "uid": before.st_uid,
        "gid": before.st_gid,
        "mode": format(stat.S_IMODE(before.st_mode), "04o"),
        "bytes": before.st_size,
        "sha256": digest.hexdigest(),
    }


def _stable_stat_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _fingerprint_regular(path: Path, label: str) -> Mapping[str, Any]:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    digest = hashlib.sha256()
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            _fail(f"{label} must be a regular file")
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if _stable_stat_identity(before) != _stable_stat_identity(after):
        _fail(f"{label} changed while its legacy fingerprint was captured")
    return {
        "type": "file",
        "mode": stat.S_IMODE(before.st_mode),
        "uid": before.st_uid,
        "gid": before.st_gid,
        "bytes": before.st_size,
        "sha256": digest.hexdigest(),
    }


def _legacy_path_is_excluded(relative: str) -> bool:
    parts = PurePosixPath(relative).parts
    if any(component in LEGACY_IGNORED_COMPONENTS for component in parts):
        return True
    if relative.endswith(LEGACY_IGNORED_SUFFIXES):
        return True
    return any(
        relative == prefix or relative.startswith(f"{prefix}/")
        for prefix in LEGACY_EXCLUDED_PREFIXES
    )


def _legacy_tree_entries(root: Path) -> list[Mapping[str, Any]]:
    entries: list[Mapping[str, Any]] = []

    def visit(directory: Path) -> None:
        before = directory.lstat()
        if directory.is_symlink() or not stat.S_ISDIR(before.st_mode):
            _fail(f"legacy fingerprint directory is unsafe: {directory}")
        with os.scandir(directory) as scanner:
            children = sorted(scanner, key=lambda item: item.name)
        for child in children:
            path = Path(child.path)
            relative = path.relative_to(root).as_posix()
            if _legacy_path_is_excluded(relative):
                continue
            metadata = child.stat(follow_symlinks=False)
            common = {
                "path": relative,
                "mode": stat.S_IMODE(metadata.st_mode),
                "uid": metadata.st_uid,
                "gid": metadata.st_gid,
            }
            if stat.S_ISDIR(metadata.st_mode):
                entries.append({**common, "type": "directory"})
                visit(path)
            elif stat.S_ISREG(metadata.st_mode):
                entries.append(
                    {
                        "path": relative,
                        **_fingerprint_regular(
                            path,
                            f"legacy Active source file {relative}",
                        ),
                    }
                )
            elif stat.S_ISLNK(metadata.st_mode):
                target = os.readlink(path)
                after = path.lstat()
                if _stable_stat_identity(metadata) != _stable_stat_identity(after):
                    _fail(
                        "legacy Active source link changed while its fingerprint "
                        f"was captured: {relative}"
                    )
                entries.append({**common, "type": "symlink", "target": target})
            else:
                _fail(f"unsupported legacy Active source object: {relative}")
        after = directory.lstat()
        if _stable_stat_identity(before) != _stable_stat_identity(after):
            _fail(
                "legacy Active source directory changed while its fingerprint "
                f"was captured: {directory}"
            )

    visit(root)
    return entries


def _legacy_interpreter_fingerprint(root: Path) -> Mapping[str, Any]:
    entry = root / ".venv/bin/python"
    metadata = entry.lstat()
    if stat.S_ISLNK(metadata.st_mode):
        entry_fingerprint: Mapping[str, Any] = {
            "type": "symlink",
            "mode": stat.S_IMODE(metadata.st_mode),
            "uid": metadata.st_uid,
            "gid": metadata.st_gid,
            "target": os.readlink(entry),
        }
    elif stat.S_ISREG(metadata.st_mode):
        entry_fingerprint = _fingerprint_regular(
            entry,
            "legacy Active Python entry",
        )
    else:
        _fail("legacy Active Python entry is not a file or symlink")
    resolved = entry.resolve(strict=True)
    resolved_fingerprint = _fingerprint_regular(
        resolved,
        "legacy Active resolved Python executable",
    )
    return {
        "path": ".venv/bin/python",
        "entry": entry_fingerprint,
        "resolvedPath": str(resolved),
        "resolved": resolved_fingerprint,
    }


def _legacy_release_fingerprint(
    root: Path,
    previous_release_sha: str,
) -> Mapping[str, Any]:
    metadata = _real_directory(root, "legacy Active root")
    entries = _legacy_tree_entries(root)
    aggregate = hashlib.sha256()
    for entry in entries:
        aggregate.update(
            json.dumps(
                entry,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        aggregate.update(b"\n")
    return {
        "schemaVersion": LEGACY_FINGERPRINT_SCHEMA_VERSION,
        "algorithm": "sha256",
        "root": str(root),
        "rootIdentity": {
            "device": metadata.st_dev,
            "inode": metadata.st_ino,
            "mode": stat.S_IMODE(metadata.st_mode),
            "uid": metadata.st_uid,
            "gid": metadata.st_gid,
        },
        "previousReleaseSha": previous_release_sha,
        "excludedPrefixes": list(LEGACY_EXCLUDED_PREFIXES),
        "ignoredComponents": list(LEGACY_IGNORED_COMPONENTS),
        "ignoredSuffixes": list(LEGACY_IGNORED_SUFFIXES),
        "entryCount": len(entries),
        "treeSha256": aggregate.hexdigest(),
        "runtimeInterpreter": _legacy_interpreter_fingerprint(root),
    }


def _file_matches(path: Path, expected: Mapping[str, Any]) -> bool:
    try:
        _, actual = _read_regular(path, "fixed Active live file")
    except (OSError, PreimageError):
        return False
    return actual == expected


def _symlink_target(path: Path, label: str) -> str:
    metadata = _lstat(path)
    if metadata is None or not stat.S_ISLNK(metadata.st_mode):
        _fail(f"{label} must be a symlink")
    target = os.readlink(path)
    if not target or "\n" in target or "\r" in target:
        _fail(f"{label} has an invalid target")
    return target


def _resolved_target(path: Path, raw_target: str) -> Path:
    lexical = Path(raw_target)
    if not lexical.is_absolute():
        lexical = path.parent / lexical
    return Path(os.path.realpath(lexical))


def _atomic_file(path: Path, payload: bytes, metadata: Mapping[str, Any]) -> None:
    _real_parent_chain(path, "fixed Active restore target")
    _real_directory(path.parent, "fixed Active restore parent")
    temporary = path.parent / f".{path.name}.{secrets.token_hex(12)}.new"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(temporary, flags, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(payload)
            handle.flush()
            os.fchown(handle.fileno(), int(metadata["uid"]), int(metadata["gid"]))
            os.fchmod(handle.fileno(), int(str(metadata["mode"]), 8))
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_dir(path.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _atomic_symlink(path: Path, target: str) -> None:
    _real_parent_chain(path, "fixed Active symlink")
    _real_directory(path.parent, "fixed Active symlink parent")
    temporary = path.parent / f".{path.name}.{secrets.token_hex(12)}.new"
    os.symlink(target, temporary)
    try:
        os.replace(temporary, path)
        _fsync_dir(path.parent)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _identity(arguments: argparse.Namespace) -> Mapping[str, str]:
    if not GIT_SHA.fullmatch(arguments.commit):
        _fail("commit must be one full lowercase git SHA")
    if not SHA256.fullmatch(arguments.archive_sha256):
        _fail("archive SHA-256 must be complete and lowercase")
    if not SLOT.fullmatch(arguments.active_slot):
        _fail("active slot must be 8000 or 8001")
    if not GIT_SHA.fullmatch(arguments.previous_release_sha):
        _fail("previous release SHA must be complete and lowercase")
    if arguments.previous_release_sha == arguments.commit:
        _fail("previous and target release SHAs must differ")
    proof = arguments.previous_release_proof
    if not (
        CONTENT_RELEASE_PROOF.fullmatch(proof)
        or LEGACY_RELEASE_PROOF.fullmatch(proof)
    ):
        _fail("previous release proof is malformed")
    proof_previous_sha = proof.split(":", 2)[1]
    if proof_previous_sha != arguments.previous_release_sha:
        _fail("previous release proof does not match its SHA")
    return {
        "commit": arguments.commit,
        "archiveSha256": arguments.archive_sha256,
        "activeSlot": arguments.active_slot,
        "previousReleaseSha": arguments.previous_release_sha,
        "previousReleaseProof": proof,
    }


def _paths(arguments: argparse.Namespace) -> Mapping[str, Path]:
    state_root = _absolute(arguments.state_root, "state root")
    slots_root = _absolute(arguments.slots_root, "slots root")
    slot_env_root = _absolute(arguments.slot_env_root, "slot env root")
    result = {
        "stateRoot": state_root,
        "slotLink": _absolute(arguments.slot_link, "slot link"),
        "slotEnv": _absolute(arguments.slot_env, "slot env"),
        "activeReleaseLink": _absolute(
            arguments.active_release_link,
            "active release link",
        ),
        "nginxConf": _absolute(arguments.nginx_conf, "Nginx include"),
        "previousReleaseRoot": _absolute(
            arguments.previous_release_root,
            "previous release root",
        ),
        "legacyRoot": _absolute(arguments.legacy_root, "legacy root"),
        "targetReleaseRoot": _absolute(
            arguments.target_release_root,
            "target release root",
        ),
        "targetEnv": _absolute(arguments.target_env, "target env source"),
        "targetNginx": _absolute(arguments.target_nginx, "target Nginx source"),
    }
    expected_slot_link = slots_root / arguments.active_slot / "current"
    expected_slot_env = slot_env_root / f"{arguments.active_slot}.env"
    if result["slotLink"] != expected_slot_link:
        _fail("slot link is outside the selected fixed Active slot")
    if result["slotEnv"] != expected_slot_env:
        _fail("slot env is outside the selected fixed Active slot")
    preimage = (
        state_root
        / "active-update-preimages"
        / arguments.commit
        / arguments.archive_sha256
    )
    result["preimage"] = preimage
    for label, path in result.items():
        if label != "preimage":
            _real_parent_chain(path, label)
    return result


def _manifest_path(paths: Mapping[str, Path]) -> Path:
    return paths["preimage"] / "manifest.json"


def _load_manifest(
    paths: Mapping[str, Path],
    identity: Mapping[str, str],
) -> Mapping[str, Any]:
    preimage = paths["preimage"]
    _private_directory(preimage, "fixed Active preimage")
    raw, _ = _read_regular(_manifest_path(paths), "fixed Active manifest")
    if len(raw) > MAX_MANIFEST_BYTES:
        _fail("fixed Active manifest exceeds its size limit")
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise PreimageError("fixed Active manifest is invalid") from exc
    if (
        not isinstance(payload, dict)
        or payload.get("schemaVersion") != SCHEMA_VERSION
        or payload.get("kind") != "fixed_active_preimage"
        or payload.get("identity") != identity
    ):
        _fail("fixed Active manifest identity is invalid")
    if payload.get("paths") != {
        name: str(paths[name])
        for name in (
            "slotLink",
            "slotEnv",
            "activeReleaseLink",
            "nginxConf",
            "previousReleaseRoot",
            "legacyRoot",
            "targetReleaseRoot",
        )
    }:
        _fail("fixed Active manifest path binding is invalid")
    for role in ("slotEnv", "nginxConf"):
        expected = payload.get("preimage", {}).get(role)
        target = payload.get("authorizedPost", {}).get(role)
        if not isinstance(expected, dict) or not isinstance(target, dict):
            _fail("fixed Active manifest file policy is invalid")
        payload_path = preimage / f"{role}.payload"
        if not _file_matches(payload_path, expected):
            _fail(f"fixed Active preimage payload changed: {role}")
    _verify_legacy_fingerprint(paths, identity, payload)
    return payload


def _verify_legacy_fingerprint(
    paths: Mapping[str, Path],
    identity: Mapping[str, str],
    payload: Mapping[str, Any],
) -> None:
    expected = payload.get("legacyPreviousReleaseFingerprint")
    if paths["previousReleaseRoot"] == paths["legacyRoot"]:
        if not isinstance(expected, dict):
            _fail("legacy Active preimage fingerprint is missing")
        actual = _legacy_release_fingerprint(
            paths["previousReleaseRoot"],
            identity["previousReleaseSha"],
        )
        if actual != expected:
            _fail("legacy Active source/runtime fingerprint drifted")
    elif expected is not None:
        _fail("content-addressed previous Active has a legacy fingerprint")


def _write_manifest(path: Path, payload: Mapping[str, Any]) -> None:
    encoded = (
        json.dumps(payload, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    )
    if len(encoded) > MAX_MANIFEST_BYTES:
        _fail("fixed Active manifest exceeds its size limit")
    with path.open("xb") as handle:
        handle.write(encoded)
        handle.flush()
        os.fchmod(handle.fileno(), 0o600)
        os.fsync(handle.fileno())


def capture(
    paths: Mapping[str, Path],
    identity: Mapping[str, str],
) -> Mapping[str, str]:
    preimage = paths["preimage"]
    if _lstat(preimage) is not None:
        _load_manifest(paths, identity)
        return {"decision": "reused", "preimage": str(preimage)}
    _real_directory(paths["stateRoot"], "state root")
    namespace = paths["stateRoot"] / "active-update-preimages"
    if _lstat(namespace) is None:
        namespace.mkdir(mode=0o700)
        _fsync_dir(paths["stateRoot"])
    _private_directory(namespace, "fixed Active preimage namespace")
    commit_dir = _ensure_private_child(namespace, identity["commit"])
    temporary = commit_dir / f".{identity['archiveSha256']}.capture.new"
    if _lstat(temporary) is not None:
        _fail("fixed Active preimage capture temporary already exists")
    previous_root = paths["previousReleaseRoot"]
    target_root = paths["targetReleaseRoot"]
    _real_directory(previous_root, "previous release root")
    _real_directory(target_root, "target release root")
    slot_target = _symlink_target(paths["slotLink"], "Active slot link")
    active_target = _symlink_target(
        paths["activeReleaseLink"],
        "Active release link",
    )
    if (
        _resolved_target(paths["slotLink"], slot_target) != previous_root
        or _resolved_target(paths["activeReleaseLink"], active_target)
        != previous_root
    ):
        _fail("fixed Active links do not resolve to the proven previous release")
    old_env, old_env_meta = _read_regular(paths["slotEnv"], "Active slot env")
    old_nginx, old_nginx_meta = _read_regular(
        paths["nginxConf"],
        "Active Nginx include",
    )
    _, target_env_meta = _read_regular(paths["targetEnv"], "target Active env")
    _, target_nginx_meta = _read_regular(
        paths["targetNginx"],
        "target Active Nginx include",
    )
    legacy_fingerprint = None
    if previous_root == paths["legacyRoot"]:
        legacy_fingerprint = _legacy_release_fingerprint(
            previous_root,
            identity["previousReleaseSha"],
        )
    target_env_meta = {
        **target_env_meta,
        "uid": old_env_meta["uid"],
        "gid": old_env_meta["gid"],
        "mode": "0600",
    }
    target_nginx_meta = {
        **target_nginx_meta,
        "uid": old_nginx_meta["uid"],
        "gid": old_nginx_meta["gid"],
        "mode": "0644",
    }
    temporary.mkdir(mode=0o700)
    try:
        for role, payload, metadata in (
            ("slotEnv", old_env, old_env_meta),
            ("nginxConf", old_nginx, old_nginx_meta),
        ):
            target = temporary / f"{role}.payload"
            with target.open("xb") as handle:
                handle.write(payload)
                handle.flush()
                os.fchown(
                    handle.fileno(),
                    int(metadata["uid"]),
                    int(metadata["gid"]),
                )
                os.fchmod(handle.fileno(), int(str(metadata["mode"]), 8))
                os.fsync(handle.fileno())
        manifest = {
            "schemaVersion": SCHEMA_VERSION,
            "kind": "fixed_active_preimage",
            "identity": identity,
            "paths": {
                name: str(paths[name])
                for name in (
                    "slotLink",
                    "slotEnv",
                    "activeReleaseLink",
                    "nginxConf",
                    "previousReleaseRoot",
                    "legacyRoot",
                    "targetReleaseRoot",
                )
            },
            "preimage": {
                "slotLink": slot_target,
                "activeReleaseLink": active_target,
                "slotEnv": old_env_meta,
                "nginxConf": old_nginx_meta,
            },
            "authorizedPost": {
                "slotLink": str(target_root),
                "activeReleaseLink": str(target_root),
                "slotEnv": target_env_meta,
                "nginxConf": target_nginx_meta,
            },
            "legacyPreviousReleaseFingerprint": legacy_fingerprint,
        }
        _write_manifest(temporary / "manifest.json", manifest)
        _fsync_dir(temporary)
        os.replace(temporary, preimage)
        _fsync_dir(commit_dir)
    finally:
        if _lstat(temporary) is not None:
            shutil.rmtree(temporary)
    _load_manifest(paths, identity)
    return {"decision": "captured", "preimage": str(preimage)}


def _link_matches(path: Path, expected: str) -> bool:
    try:
        return _symlink_target(path, "fixed Active live link") == expected
    except (OSError, PreimageError):
        return False


def restore(
    paths: Mapping[str, Path],
    identity: Mapping[str, str],
) -> Mapping[str, str]:
    manifest = _load_manifest(paths, identity)
    before = manifest["preimage"]
    after = manifest["authorizedPost"]
    for role in ("slotLink", "activeReleaseLink"):
        if not (
            _link_matches(paths[role], str(before[role]))
            or _link_matches(paths[role], str(after[role]))
        ):
            _fail(f"refusing to overwrite drifted fixed Active link: {role}")
    for role in ("slotEnv", "nginxConf"):
        if not (
            _file_matches(paths[role], before[role])
            or _file_matches(paths[role], after[role])
        ):
            _fail(f"refusing to overwrite drifted fixed Active file: {role}")
    restored_env, restored_env_metadata = _read_regular(
        paths["preimage"] / "slotEnv.payload",
        "fixed Active env preimage payload",
    )
    restored_nginx, restored_nginx_metadata = _read_regular(
        paths["preimage"] / "nginxConf.payload",
        "fixed Active Nginx preimage payload",
    )
    if (
        restored_env_metadata != before["slotEnv"]
        or restored_nginx_metadata != before["nginxConf"]
    ):
        _fail("fixed Active preimage payload changed before restoration")
    _atomic_file(paths["slotEnv"], restored_env, before["slotEnv"])
    _atomic_symlink(paths["slotLink"], str(before["slotLink"]))
    _atomic_symlink(
        paths["activeReleaseLink"],
        str(before["activeReleaseLink"]),
    )
    _atomic_file(paths["nginxConf"], restored_nginx, before["nginxConf"])
    if (
        not _file_matches(paths["slotEnv"], before["slotEnv"])
        or not _file_matches(paths["nginxConf"], before["nginxConf"])
        or not _link_matches(paths["slotLink"], str(before["slotLink"]))
        or not _link_matches(
            paths["activeReleaseLink"],
            str(before["activeReleaseLink"]),
        )
    ):
        _fail("fixed Active restore verification failed")
    _verify_legacy_fingerprint(paths, identity, manifest)
    return {"decision": "restored", "preimage": str(paths["preimage"])}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("capture", "verify", "restore"))
    parser.add_argument("--state-root", required=True)
    parser.add_argument("--slots-root", required=True)
    parser.add_argument("--slot-env-root", required=True)
    parser.add_argument("--slot-link", required=True)
    parser.add_argument("--slot-env", required=True)
    parser.add_argument("--active-release-link", required=True)
    parser.add_argument("--nginx-conf", required=True)
    parser.add_argument("--previous-release-root", required=True)
    parser.add_argument("--legacy-root", required=True)
    parser.add_argument("--target-release-root", required=True)
    parser.add_argument("--target-env", required=True)
    parser.add_argument("--target-nginx", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--archive-sha256", required=True)
    parser.add_argument("--active-slot", required=True)
    parser.add_argument("--previous-release-sha", required=True)
    parser.add_argument("--previous-release-proof", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        identity = _identity(arguments)
        paths = _paths(arguments)
        if arguments.command == "capture":
            result = capture(paths, identity)
        elif arguments.command == "verify":
            _load_manifest(paths, identity)
            result = {"decision": "verified", "preimage": str(paths["preimage"])}
        else:
            result = restore(paths, identity)
    except (OSError, PreimageError) as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
