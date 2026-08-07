#!/usr/bin/env python3
"""Build and verify the immutable source portion of a blue/green release.

Mutable runtime paths are deliberately excluded and governed by separate
deployment controls. Every other directory, regular file, and symlink is
type/mode/content sealed so a persistent release directory cannot be trusted
only because it contains a forgeable identity text file.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import tempfile
from typing import Any, Iterator


SCHEMA_VERSION = 2
SOURCE_EXCLUDED_PREFIXES = (
    ".venv",
    ".jato-release-identity",
    ".jato-source-seal.json",
    ".jato-runtime-seal.json",
    "01_RAW_DATA",
    "04_Processed_data",
    "03_Scripts/diagnostics/artifacts",
    "03_Scripts/logs",
    "06_AppPlatform/frontend/dist",
    "hermes/deploy_release.json",
    "hermes/deploy_failure_context.txt",
    "hermes/reports",
)
RUNTIME_SCOPES = (
    ".venv",
    "06_AppPlatform/frontend/dist",
)
SOURCE_CRITICAL_FILES = (
    "03_Scripts/deploy/cleanup_toolkit_egg_info.py",
    "03_Scripts/deploy/fixed_active_preimage.py",
    "03_Scripts/deploy/jato_bluegreen_boot_reconcile.py",
    "03_Scripts/deploy/nginx/jato_candidate_preview.conf.example",
    "03_Scripts/deploy/tencent_bluegreen_release.sh",
    "03_Scripts/deploy/validate_release_archive.py",
    "03_Scripts/deploy/jato_quiescence_gate.py",
    "03_Scripts/deploy/release_checkpoint.py",
    "03_Scripts/deploy/systemd/jato-bluegreen-boot-reconcile.service",
    "03_Scripts/deploy/systemd/nginx-jato-bluegreen-boot-reconcile.conf",
    "03_Scripts/deploy/verify_release_source_seal.py",
    "03_Scripts/ops/deploy_fullstack_server.sh",
)
RUNTIME_CRITICAL_FILES = (
    ".venv/bin/python",
    "06_AppPlatform/frontend/dist/index.html",
    "06_AppPlatform/frontend/dist/build-meta.json",
    "06_AppPlatform/frontend/dist/release-provenance.json",
)
PROFILE_POLICIES = {
    "source": {
        "excludedPrefixes": SOURCE_EXCLUDED_PREFIXES,
        "criticalFiles": SOURCE_CRITICAL_FILES,
    },
    "runtime": {
        "excludedPrefixes": (),
        "criticalFiles": RUNTIME_CRITICAL_FILES,
        "scopes": RUNTIME_SCOPES,
    },
}


class SealError(ValueError):
    """Raised when a source tree or seal is unsafe."""


def _relative_name(root: Path, path: Path) -> str:
    relative = path.relative_to(root).as_posix()
    if relative in {"", "."}:
        raise SealError("release seal cannot contain the root entry")
    pure = PurePosixPath(relative)
    if pure.is_absolute() or ".." in pure.parts:
        raise SealError(f"unsafe release path: {relative}")
    return relative


def _excluded(
    relative: str,
    excluded_prefixes: tuple[str, ...],
    profile: str,
) -> bool:
    parts = PurePosixPath(relative).parts
    if "__pycache__" in parts:
        return True
    if profile == "source" and relative.endswith((".pyc", ".pyo")):
        return True
    return any(
        relative == prefix or relative.startswith(f"{prefix}/")
        for prefix in excluded_prefixes
    )


def _hash_regular_file(path: Path) -> str:
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    digest = hashlib.sha256()
    try:
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    finally:
        os.close(descriptor)
    return digest.hexdigest()


def _walk(
    root: Path,
    excluded_prefixes: tuple[str, ...],
    profile: str,
    initial_directories: tuple[Path, ...] | None = None,
) -> Iterator[dict[str, Any]]:
    pending = list(initial_directories or (root,))
    while pending:
        directory = pending.pop()
        with os.scandir(directory) as scanner:
            children = sorted(scanner, key=lambda item: item.name, reverse=True)
        for child in children:
            path = Path(child.path)
            relative = _relative_name(root, path)
            if _excluded(relative, excluded_prefixes, profile):
                continue
            metadata = child.stat(follow_symlinks=False)
            mode = stat.S_IMODE(metadata.st_mode)
            if stat.S_ISDIR(metadata.st_mode):
                pending.append(path)
                yield {"path": relative, "type": "directory", "mode": mode}
            elif stat.S_ISREG(metadata.st_mode):
                yield {
                    "path": relative,
                    "type": "file",
                    "mode": mode,
                    "bytes": metadata.st_size,
                    "sha256": _hash_regular_file(path),
                }
            elif stat.S_ISLNK(metadata.st_mode):
                yield {
                    "path": relative,
                    "type": "symlink",
                    "mode": mode,
                    "target": os.readlink(path),
                }
            else:
                raise SealError(
                    f"unsupported release source object: {relative}"
                )


def _runtime_interpreter(
    root: Path,
    recorded_runtime_root: Path | None = None,
) -> dict[str, Any]:
    interpreter = root / ".venv/bin/python"
    metadata = interpreter.lstat()
    if not (stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode)):
        raise SealError("runtime Python interpreter must be a file or symlink")
    resolved = interpreter.resolve(strict=True)
    resolved_metadata = resolved.stat()
    if not stat.S_ISREG(resolved_metadata.st_mode):
        raise SealError("runtime Python interpreter must resolve to a regular file")
    resolved_path = str(resolved)
    if recorded_runtime_root is not None:
        if (
            not recorded_runtime_root.is_absolute()
            or ".." in recorded_runtime_root.parts
        ):
            raise SealError("recorded runtime root must be one normalized absolute path")
        try:
            relative_interpreter = resolved.relative_to(root.resolve(strict=True))
        except ValueError as exc:
            raise SealError(
                "relocatable runtime seal requires an interpreter inside the release root"
            ) from exc
        resolved_path = str(recorded_runtime_root / relative_interpreter)
    return {
        "path": ".venv/bin/python",
        "entryType": "symlink" if interpreter.is_symlink() else "file",
        "resolvedPath": resolved_path,
        "resolvedMode": stat.S_IMODE(resolved_metadata.st_mode),
        "resolvedBytes": resolved_metadata.st_size,
        "resolvedSha256": _hash_regular_file(resolved),
    }


def _validated_runtime_identity(
    release_identity: dict[str, str] | None,
) -> dict[str, str]:
    if not isinstance(release_identity, dict):
        raise SealError("runtime seal requires an explicit release identity")
    commit = release_identity.get("commit", "")
    archive_sha256 = release_identity.get("archiveSha256", "")
    frontend_identity = release_identity.get("frontendIdentity", "")
    frontend_checksum = release_identity.get("frontendChecksum", "")
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise SealError("runtime seal commit identity is malformed")
    if not re.fullmatch(r"[0-9a-f]{64}", archive_sha256):
        raise SealError("runtime seal archive identity is malformed")
    if not frontend_identity or len(frontend_identity) > 2048:
        raise SealError("runtime seal frontend identity is malformed")
    if not re.fullmatch(r"[0-9a-f]{64}", frontend_checksum):
        raise SealError("runtime seal frontend checksum is malformed")
    return {
        "commit": commit,
        "archiveSha256": archive_sha256,
        "frontendIdentity": frontend_identity,
        "frontendChecksum": frontend_checksum,
    }


def build_manifest(
    root: Path,
    profile: str = "source",
    release_identity: dict[str, str] | None = None,
    recorded_runtime_root: Path | None = None,
) -> dict[str, Any]:
    try:
        root_metadata = root.lstat()
    except FileNotFoundError as exc:
        raise SealError(f"release source root does not exist: {root}") from exc
    if root.is_symlink() or not stat.S_ISDIR(root_metadata.st_mode):
        raise SealError(f"release source root must be a real directory: {root}")
    try:
        policy = PROFILE_POLICIES[profile]
    except KeyError as exc:
        raise SealError(f"unsupported release seal profile: {profile}") from exc
    excluded_prefixes = policy["excludedPrefixes"]
    critical_files = policy["criticalFiles"]
    scopes = tuple(policy.get("scopes", ()))
    scoped_directories: tuple[Path, ...] | None = None
    scope_entries: list[dict[str, Any]] = []
    if scopes:
        directories: list[Path] = []
        for scope in scopes:
            directory = root / scope
            metadata = directory.lstat()
            if directory.is_symlink() or not stat.S_ISDIR(metadata.st_mode):
                raise SealError(f"runtime seal scope must be a real directory: {scope}")
            directories.append(directory)
            scope_entries.append(
                {
                    "path": scope,
                    "type": "directory",
                    "mode": stat.S_IMODE(metadata.st_mode),
                }
            )
        scoped_directories = tuple(directories)
    entries = sorted(
        scope_entries
        + list(
            _walk(
                root,
                excluded_prefixes,
                profile,
                scoped_directories,
            )
        ),
        key=lambda item: item["path"],
    )
    by_path = {entry["path"]: entry for entry in entries}
    for critical in critical_files:
        entry = by_path.get(critical)
        allowed_types = (
            {"file", "symlink"}
            if profile == "runtime" and critical == ".venv/bin/python"
            else {"file"}
        )
        if (
            not isinstance(entry, dict)
            or entry.get("type") not in allowed_types
        ):
            raise SealError(f"critical release source file is missing: {critical}")
    manifest = {
        "schemaVersion": SCHEMA_VERSION,
        "profile": profile,
        "algorithm": "sha256",
        "excludedPrefixes": list(excluded_prefixes),
        "scopes": list(scopes),
        "ignoredPathComponents": ["__pycache__"],
        "criticalFiles": list(critical_files),
        "entries": entries,
    }
    if profile == "runtime":
        manifest["releaseIdentity"] = _validated_runtime_identity(
            release_identity
        )
        manifest["runtimeInterpreter"] = _runtime_interpreter(
            root,
            recorded_runtime_root,
        )
        source_seal = root / ".jato-source-seal.json"
        source_metadata = source_seal.lstat()
        if source_seal.is_symlink() or not stat.S_ISREG(source_metadata.st_mode):
            raise SealError("runtime seal requires the verified source seal")
        manifest["sourceSealSha256"] = _hash_regular_file(source_seal)
        site_packages = sorted(
            path
            for path in (root / ".venv/lib").glob("python*/site-packages")
            if path.is_dir() and not path.is_symlink()
        )
        if not site_packages:
            raise SealError("runtime seal requires Python site-packages")
        for package in ("fastapi", "uvicorn"):
            if not any((path / package).is_dir() for path in site_packages):
                raise SealError(
                    f"runtime seal requires installed package: {package}"
                )
    elif release_identity is not None:
        raise SealError("source seal must not contain a runtime release identity")
    elif recorded_runtime_root is not None:
        raise SealError("source seal must not define a recorded runtime root")
    return manifest


def _read_manifest(
    path: Path,
    profile: str,
    release_identity: dict[str, str] | None,
) -> dict[str, Any]:
    metadata = path.lstat()
    if path.is_symlink() or not stat.S_ISREG(metadata.st_mode):
        raise SealError(f"source seal must be a regular non-symlink file: {path}")
    if metadata.st_size <= 0 or metadata.st_size > 64 * 1024 * 1024:
        raise SealError("source seal has an invalid size")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise SealError("source seal root must be an object")
    if payload.get("schemaVersion") != SCHEMA_VERSION:
        raise SealError("source seal schema version is unsupported")
    if payload.get("profile") != profile:
        raise SealError("source seal profile does not match the requested policy")
    if payload.get("algorithm") != "sha256":
        raise SealError("source seal algorithm is unsupported")
    try:
        policy = PROFILE_POLICIES[profile]
    except KeyError as exc:
        raise SealError(f"unsupported release seal profile: {profile}") from exc
    if payload.get("excludedPrefixes") != list(policy["excludedPrefixes"]):
        raise SealError("source seal mutable-path exclusions do not match policy")
    if payload.get("scopes") != list(policy.get("scopes", ())):
        raise SealError("source seal scopes do not match policy")
    if payload.get("ignoredPathComponents") != ["__pycache__"]:
        raise SealError("source seal ignored path components do not match policy")
    if payload.get("criticalFiles") != list(policy["criticalFiles"]):
        raise SealError("source seal critical-file policy does not match")
    if not isinstance(payload.get("entries"), list):
        raise SealError("source seal entries must be a list")
    if profile == "runtime":
        if payload.get("releaseIdentity") != _validated_runtime_identity(
            release_identity
        ):
            raise SealError("runtime seal release identity does not match")
        if not isinstance(payload.get("runtimeInterpreter"), dict):
            raise SealError("runtime seal interpreter binding is missing")
        if not re.fullmatch(
            r"[0-9a-f]{64}",
            str(payload.get("sourceSealSha256", "")),
        ):
            raise SealError("runtime seal source binding is missing")
    elif "releaseIdentity" in payload or "runtimeInterpreter" in payload:
        raise SealError("source seal contains unexpected runtime identity fields")
    return payload


def _write_manifest(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
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
        parent = os.open(
            path.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            os.fsync(parent)
        finally:
            os.close(parent)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build")
    build.add_argument("--root", type=Path, required=True)
    build.add_argument("--output", type=Path, required=True)
    build.add_argument(
        "--profile",
        choices=sorted(PROFILE_POLICIES),
        default="source",
    )
    verify = subparsers.add_parser("verify")
    verify.add_argument("--root", type=Path, required=True)
    verify.add_argument("--manifest", type=Path, required=True)
    verify.add_argument(
        "--profile",
        choices=sorted(PROFILE_POLICIES),
        default="source",
    )
    for command in (build, verify):
        command.add_argument("--commit", default="")
        command.add_argument("--archive-sha256", default="")
        command.add_argument("--frontend-identity", default="")
        command.add_argument("--frontend-checksum", default="")
        command.add_argument("--recorded-runtime-root", type=Path)
    arguments = parser.parse_args()
    release_identity = None
    if arguments.profile == "runtime":
        release_identity = {
            "commit": arguments.commit,
            "archiveSha256": arguments.archive_sha256,
            "frontendIdentity": arguments.frontend_identity,
            "frontendChecksum": arguments.frontend_checksum,
        }

    try:
        if arguments.command == "build":
            _write_manifest(
                arguments.output,
                build_manifest(
                    arguments.root,
                    arguments.profile,
                    release_identity,
                    arguments.recorded_runtime_root,
                ),
            )
            return 0
        expected = _read_manifest(
            arguments.manifest,
            arguments.profile,
            release_identity,
        )
        actual = build_manifest(
            arguments.root,
            arguments.profile,
            release_identity,
            arguments.recorded_runtime_root,
        )
        if actual != expected:
            expected_entries = {
                item["path"]: item for item in expected["entries"]
            }
            actual_entries = {
                item["path"]: item for item in actual["entries"]
            }
            changed = sorted(
                path
                for path in set(expected_entries) | set(actual_entries)
                if expected_entries.get(path) != actual_entries.get(path)
            )
            preview = ", ".join(changed[:12])
            raise SealError(
                "persistent release source does not match the verified archive "
                f"seal; changed={preview or 'manifest-policy'}"
            )
        return 0
    except (OSError, UnicodeError, json.JSONDecodeError, SealError) as exc:
        parser.error(str(exc))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
