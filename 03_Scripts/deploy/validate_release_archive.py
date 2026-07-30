#!/usr/bin/env python3
"""Fail-closed validation for immutable JATO release archives.

The validator is deliberately usable before archive extraction. Production
receives this file in its trusted stdin control payload; feature canaries stage
the same file in their root-owned control bundle. Both paths additionally bind
the trusted copy to the identically named member inside the content-addressed
archive.
"""

from __future__ import annotations

import argparse
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
from typing import BinaryIO, Callable


SCHEMA_VERSION = 2
MAX_MEMBERS = 50_000
MAX_EXPANDED_BYTES = 2 * 1024 * 1024 * 1024
MAX_MEMBER_BYTES = 512 * 1024 * 1024
MAX_CONTROL_BYTES = 8 * 1024 * 1024
PRIVATE_PREFIXES = (
    "01_RAW_DATA",
    "03_Scripts/diagnostics/artifacts",
)
REQUIRED_PRIVATE_WORKBOOK = "01_RAW_DATA/VOC_Nordic_SUV_Users_100.xlsx"
REQUIRED_DIAGNOSTICS_PREFIX = "03_Scripts/diagnostics/artifacts/"
REQUIRED_DIAGNOSTICS_FILES = frozenset(
    {
        (
            "03_Scripts/diagnostics/artifacts/msrp_backfill/"
            "sweden_swiss_top30_suv/official_evidence_leads.json"
        ),
        (
            "03_Scripts/diagnostics/artifacts/msrp_backfill/"
            "sweden_swiss_top30_suv/"
            "top30_suv_price_movement_candidates.json"
        ),
    }
)
RESERVED_RUNTIME_MEMBERS = frozenset(
    {
        ".jato-canary-archive-validation.json",
    }
)
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class ArchiveValidationError(ValueError):
    """Raised when a release archive cannot be proven safe."""


def _normalized_member_name(raw_name: str) -> str:
    raw = PurePosixPath(raw_name)
    parts = tuple(part for part in raw.parts if part != ".")
    if raw.is_absolute() or not parts or ".." in parts:
        raise ArchiveValidationError(f"unsafe release archive path: {raw_name}")
    return PurePosixPath(*parts).as_posix()


def _is_private_member(relative: str) -> bool:
    return any(
        relative == prefix or relative.startswith(f"{prefix}/")
        for prefix in PRIVATE_PREFIXES
    )


def _hash_stream(stream: BinaryIO) -> str:
    digest = hashlib.sha256()
    while True:
        chunk = stream.read(1024 * 1024)
        if not chunk:
            return digest.hexdigest()
        digest.update(chunk)


def _hash_regular_file(path: Path) -> str:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise ArchiveValidationError(
            f"trusted control file is unavailable: {path}"
        ) from exc
    if path.is_symlink() or not stat.S_ISREG(metadata.st_mode):
        raise ArchiveValidationError(
            f"trusted control file must be a regular non-symlink: {path}"
        )
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise ArchiveValidationError(
            f"trusted control file cannot be opened safely: {path}"
        ) from exc
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or (opened.st_dev, opened.st_ino)
            != (metadata.st_dev, metadata.st_ino)
            or opened.st_size <= 0
            or opened.st_size > MAX_CONTROL_BYTES
        ):
            raise ArchiveValidationError(
                f"trusted control file identity or size is unsafe: {path}"
            )
        with os.fdopen(os.dup(descriptor), "rb") as handle:
            return _hash_stream(handle)
    finally:
        os.close(descriptor)


def _parse_trusted_controls(values: list[str]) -> dict[str, Path]:
    parsed: dict[str, Path] = {}
    for raw in values:
        relative, separator, local_name = raw.partition("=")
        if not separator or not relative or not local_name:
            raise ArchiveValidationError(
                "trusted control must use ARCHIVE_RELATIVE_PATH=LOCAL_ABSOLUTE_PATH"
            )
        normalized = _normalized_member_name(relative)
        local = Path(local_name)
        if not local.is_absolute() or normalized in parsed:
            raise ArchiveValidationError(
                f"trusted control mapping is unsafe or duplicate: {raw}"
            )
        parsed[normalized] = local
    return parsed


def _atomic_json(path: Path, payload: dict[str, object]) -> None:
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
        directory_descriptor = os.open(
            path.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def assert_materialization_headroom(
    receipt: dict[str, object],
    *,
    target: Path,
    materialization_copies: int,
    reserve_bytes: int,
    available_bytes: int | None = None,
) -> int:
    expanded_bytes = receipt.get("expandedBytes")
    if (
        isinstance(expanded_bytes, bool)
        or not isinstance(expanded_bytes, int)
        or expanded_bytes <= 0
        or materialization_copies <= 0
        or reserve_bytes < 0
    ):
        raise ArchiveValidationError(
            "release archive headroom inputs are malformed"
        )
    required_bytes = expanded_bytes * materialization_copies + reserve_bytes
    if available_bytes is None:
        try:
            available_bytes = shutil.disk_usage(target).free
        except OSError as exc:
            raise ArchiveValidationError(
                f"release target filesystem cannot be inspected: {target}"
            ) from exc
    if available_bytes < required_bytes:
        raise ArchiveValidationError(
            "release target filesystem lacks extraction/runtime headroom: "
            f"available={available_bytes}, required={required_bytes}"
        )
    return required_bytes


def evaluate_materialization_headroom(
    receipt: dict[str, object],
    *,
    requests: list[tuple[Path, int, int]],
    device_probe: Callable[[Path], int] | None = None,
    available_probe: Callable[[Path], int] | None = None,
) -> list[dict[str, object]]:
    if not requests:
        raise ArchiveValidationError(
            "release archive headroom plan must not be empty"
        )
    expanded_bytes = receipt.get("expandedBytes")
    if (
        isinstance(expanded_bytes, bool)
        or not isinstance(expanded_bytes, int)
        or expanded_bytes <= 0
    ):
        raise ArchiveValidationError(
            "release archive receipt lacks expanded bytes"
        )
    seen_targets: set[Path] = set()
    grouped: dict[int, dict[str, object]] = {}
    for target, copies, reserve_bytes in requests:
        if not target.is_absolute():
            raise ArchiveValidationError(
                f"release headroom target must be absolute: {target}"
            )
        normalized = Path(os.path.normpath(target))
        if normalized in seen_targets:
            raise ArchiveValidationError(
                f"duplicate release headroom target: {normalized}"
            )
        seen_targets.add(normalized)
        if copies <= 0 or reserve_bytes < 0:
            raise ArchiveValidationError(
                "release archive headroom inputs are malformed"
            )
        try:
            metadata = normalized.lstat()
        except OSError as exc:
            raise ArchiveValidationError(
                f"release headroom target is unavailable: {normalized}"
            ) from exc
        if normalized.is_symlink() or not stat.S_ISDIR(metadata.st_mode):
            raise ArchiveValidationError(
                f"release headroom target must be a real directory: {normalized}"
            )
        device = (
            device_probe(normalized)
            if device_probe is not None
            else metadata.st_dev
        )
        available = (
            available_probe(normalized)
            if available_probe is not None
            else shutil.disk_usage(normalized).free
        )
        if (
            isinstance(device, bool)
            or not isinstance(device, int)
            or device < 0
            or isinstance(available, bool)
            or not isinstance(available, int)
            or available < 0
        ):
            raise ArchiveValidationError(
                "release headroom filesystem evidence is malformed"
            )
        group = grouped.setdefault(
            device,
            {
                "device": device,
                "targets": [],
                "availableBytes": available,
                "materializationCopies": 0,
                "reserveBytes": 0,
            },
        )
        targets = group["targets"]
        assert isinstance(targets, list)
        targets.append(str(normalized))
        group["availableBytes"] = min(
            int(group["availableBytes"]),
            available,
        )
        group["materializationCopies"] = (
            int(group["materializationCopies"]) + copies
        )
        group["reserveBytes"] = max(
            int(group["reserveBytes"]),
            reserve_bytes,
        )

    checks: list[dict[str, object]] = []
    for device in sorted(grouped):
        group = grouped[device]
        targets = sorted(str(value) for value in group["targets"])
        copies = int(group["materializationCopies"])
        reserve_bytes = int(group["reserveBytes"])
        available = int(group["availableBytes"])
        required = expanded_bytes * copies + reserve_bytes
        if available < required:
            raise ArchiveValidationError(
                "release target filesystem lacks extraction/runtime headroom: "
                f"device={device}, available={available}, required={required}"
            )
        checks.append(
            {
                "target": targets[0],
                "targets": targets,
                "device": device,
                "availableBytes": available,
                "requiredBytes": required,
                "materializationCopies": copies,
                "reserveBytes": reserve_bytes,
            }
        )
    return checks


def inspect_root_sealed_bundle(
    *,
    archive_path: Path,
    helper_path: Path,
    sealed_root: Path,
    expected_archive_sha256: str,
    expected_archive_bytes: int,
    expected_helper_sha256: str,
    expected_group_id: int,
    anchor: Path = Path("/var/lib"),
    expected_owner_uid: int = 0,
) -> dict[str, object]:
    if (
        not archive_path.is_absolute()
        or not helper_path.is_absolute()
        or not sealed_root.is_absolute()
        or not anchor.is_absolute()
        or archive_path.parent != helper_path.parent
        or sealed_root.parent != anchor
        or archive_path.parent == sealed_root
    ):
        raise ArchiveValidationError(
            "root-sealed release bundle paths are malformed"
        )
    try:
        archive_path.relative_to(sealed_root)
        helper_path.relative_to(sealed_root)
    except ValueError as exc:
        raise ArchiveValidationError(
            "root-sealed release bundle escaped its trust root"
        ) from exc
    if (
        not SHA256_PATTERN.fullmatch(expected_archive_sha256)
        or not SHA256_PATTERN.fullmatch(expected_helper_sha256)
        or expected_archive_bytes <= 0
        or expected_group_id < 0
        or expected_owner_uid < 0
    ):
        raise ArchiveValidationError(
            "root-sealed release identity is malformed"
        )

    chain: list[Path] = []
    cursor = archive_path.parent
    while True:
        chain.append(cursor)
        if cursor == anchor:
            break
        if cursor.parent == cursor:
            raise ArchiveValidationError(
                "root-sealed release bundle lacks its trusted anchor"
            )
        cursor = cursor.parent
    chain.reverse()
    if sealed_root not in chain:
        raise ArchiveValidationError(
            "root-sealed release bundle lacks its reviewed trust root"
        )
    directory_evidence: list[dict[str, object]] = []
    for path in chain:
        try:
            metadata = path.lstat()
        except OSError as exc:
            raise ArchiveValidationError(
                f"root-sealed directory is unavailable: {path}"
            ) from exc
        mode = stat.S_IMODE(metadata.st_mode)
        if (
            path.is_symlink()
            or not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != expected_owner_uid
            or mode & 0o022
        ):
            raise ArchiveValidationError(
                f"root-sealed directory is mutable or unsafe: {path}"
            )
        directory_evidence.append(
            {
                "path": str(path),
                "device": metadata.st_dev,
                "inode": metadata.st_ino,
                "uid": metadata.st_uid,
                "gid": metadata.st_gid,
                "mode": f"{mode:04o}",
            }
        )

    file_evidence: dict[str, dict[str, object]] = {}
    expected_files = (
        (
            "archive",
            archive_path,
            0o440,
            expected_archive_sha256,
            expected_archive_bytes,
        ),
        (
            "helper",
            helper_path,
            0o550,
            expected_helper_sha256,
            None,
        ),
    )
    for label, path, expected_mode, expected_sha256, expected_bytes in expected_files:
        try:
            metadata = path.lstat()
        except OSError as exc:
            raise ArchiveValidationError(
                f"root-sealed {label} is unavailable: {path}"
            ) from exc
        mode = stat.S_IMODE(metadata.st_mode)
        if (
            path.is_symlink()
            or not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != expected_owner_uid
            or metadata.st_gid != expected_group_id
            or mode != expected_mode
            or (expected_bytes is not None and metadata.st_size != expected_bytes)
        ):
            raise ArchiveValidationError(
                f"root-sealed {label} identity is unsafe"
            )
        digest = (
            _hash_regular_file(path)
            if label == "helper"
            else expected_archive_sha256
        )
        if digest != expected_sha256:
            raise ArchiveValidationError(
                f"root-sealed {label} SHA-256 mismatch"
            )
        file_evidence[label] = {
            "path": str(path),
            "device": metadata.st_dev,
            "inode": metadata.st_ino,
            "uid": metadata.st_uid,
            "gid": metadata.st_gid,
            "mode": f"{mode:04o}",
            "bytes": metadata.st_size,
            "sha256": digest,
        }
    return {
        "root": str(sealed_root),
        "anchor": str(anchor),
        "directories": directory_evidence,
        "archive": file_evidence["archive"],
        "helper": file_evidence["helper"],
    }


def validate_archive(
    archive_path: Path,
    *,
    expected_sha256: str,
    expected_bytes: int,
    trusted_controls: dict[str, Path] | None = None,
) -> dict[str, object]:
    if not archive_path.is_absolute():
        raise ArchiveValidationError("release archive path must be absolute")
    if not SHA256_PATTERN.fullmatch(expected_sha256):
        raise ArchiveValidationError("expected release archive SHA-256 is malformed")
    if expected_bytes <= 0:
        raise ArchiveValidationError("expected release archive bytes must be positive")
    try:
        metadata = archive_path.lstat()
    except OSError as exc:
        raise ArchiveValidationError(
            f"release archive is unavailable: {archive_path}"
        ) from exc
    if archive_path.is_symlink() or not stat.S_ISREG(metadata.st_mode):
        raise ArchiveValidationError(
            "release archive must be a regular non-symlink file"
        )
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(archive_path, flags)
    except OSError as exc:
        raise ArchiveValidationError(
            "release archive cannot be opened safely"
        ) from exc
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or (opened.st_dev, opened.st_ino)
            != (metadata.st_dev, metadata.st_ino)
            or opened.st_size != expected_bytes
        ):
            raise ArchiveValidationError(
                "release archive identity or byte count changed"
            )
        os.lseek(descriptor, 0, os.SEEK_SET)
        with os.fdopen(os.dup(descriptor), "rb") as identity_stream:
            actual_sha256 = _hash_stream(identity_stream)
        if actual_sha256 != expected_sha256:
            raise ArchiveValidationError("release archive SHA-256 mismatch")

        controls = trusted_controls or {}
        trusted_hashes = {
            relative: _hash_regular_file(local)
            for relative, local in sorted(controls.items())
        }
        archive_control_hashes: dict[str, str] = {}
        member_count = 0
        expanded_bytes = 0
        root_directory_seen = False
        seen: set[str] = set()
        public_files = 0
        public_directories = 0
        private_files = 0
        private_directories = 0
        required_workbook_mode = ""
        diagnostics_file_modes: set[str] = set()
        diagnostics_directory_modes: set[str] = set()
        entry_types: dict[str, str] = {}
        private_file_entries: list[dict[str, object]] = []
        private_directory_entries: list[dict[str, object]] = []
        os.lseek(descriptor, 0, os.SEEK_SET)
        with os.fdopen(os.dup(descriptor), "rb") as archive_stream:
            with tarfile.open(fileobj=archive_stream, mode="r:gz") as archive:
                if archive.pax_headers:
                    raise ArchiveValidationError(
                        "release archive global PAX headers are unsupported"
                    )
                for member in archive:
                    member_count += 1
                    if member_count > MAX_MEMBERS:
                        raise ArchiveValidationError(
                            "release archive contains too many members"
                        )
                    if member.size < 0 or member.size > MAX_MEMBER_BYTES:
                        raise ArchiveValidationError(
                            f"release archive member size is unsafe: {member.name}"
                        )
                    expanded_bytes += member.size
                    if expanded_bytes > MAX_EXPANDED_BYTES:
                        raise ArchiveValidationError(
                            "release archive expands beyond 2 GiB"
                        )
                    if member.name in {".", "./"}:
                        if (
                            not member.isdir()
                            or root_directory_seen
                            or member.uid != 0
                            or member.gid != 0
                            or member.pax_headers
                            or getattr(member, "sparse", None) is not None
                            or stat.S_IMODE(member.mode) != 0o755
                        ):
                            raise ArchiveValidationError(
                                "release archive root must be one mode-0755 directory"
                            )
                        root_directory_seen = True
                        continue
                    normalized = _normalized_member_name(member.name)
                    if normalized in seen:
                        raise ArchiveValidationError(
                            f"duplicate release archive path: {normalized}"
                        )
                    seen.add(normalized)
                    if (
                        member.uid != 0
                        or member.gid != 0
                        or member.pax_headers
                        or getattr(member, "sparse", None) is not None
                    ):
                        raise ArchiveValidationError(
                            f"release archive ownership/extension is unsafe: {normalized}"
                        )
                    if normalized in RESERVED_RUNTIME_MEMBERS:
                        raise ArchiveValidationError(
                            f"reserved release runtime member is forbidden: {normalized}"
                        )
                    if member.issym() or member.islnk() or member.isdev():
                        raise ArchiveValidationError(
                            f"unsupported release archive member: {normalized}"
                        )
                    if not (member.isfile() or member.isdir()):
                        raise ArchiveValidationError(
                            f"unsupported release archive entry type: {normalized}"
                        )
                    entry_types[normalized] = (
                        "file" if member.isfile() else "directory"
                    )
                    private = _is_private_member(normalized)
                    mode = stat.S_IMODE(member.mode)
                    if private:
                        allowed_modes = (
                            {0o600, 0o711} if member.isfile() else {0o711}
                        )
                    else:
                        allowed_modes = (
                            {0o644, 0o755} if member.isfile() else {0o755}
                        )
                    if mode not in allowed_modes:
                        raise ArchiveValidationError(
                            f"unsafe release archive mode {mode:04o}: {normalized}"
                        )
                    if private and member.isfile():
                        private_files += 1
                        source = archive.extractfile(member)
                        if source is None:
                            raise ArchiveValidationError(
                                f"private archive member is unreadable: {normalized}"
                            )
                        with source:
                            private_digest = _hash_stream(source)
                        private_file_entries.append(
                            {
                                "path": normalized,
                                "mode": f"{mode:04o}",
                                "sha256": private_digest,
                                "bytes": member.size,
                            }
                        )
                    elif private:
                        private_directories += 1
                        private_directory_entries.append(
                            {
                                "path": normalized,
                                "mode": f"{mode:04o}",
                            }
                        )
                    elif member.isfile():
                        public_files += 1
                    else:
                        public_directories += 1
                    if normalized == REQUIRED_PRIVATE_WORKBOOK and member.isfile():
                        required_workbook_mode = f"{mode:04o}"
                    if normalized.startswith(REQUIRED_DIAGNOSTICS_PREFIX):
                        if member.isfile():
                            diagnostics_file_modes.add(f"{mode:04o}")
                        else:
                            diagnostics_directory_modes.add(f"{mode:04o}")
                    if normalized in trusted_hashes:
                        if member.size <= 0 or member.size > MAX_CONTROL_BYTES:
                            raise ArchiveValidationError(
                                f"archive control member size is unsafe: {normalized}"
                            )
                        source = archive.extractfile(member)
                        if source is None:
                            raise ArchiveValidationError(
                                f"archive control member is unreadable: {normalized}"
                            )
                        with source:
                            archive_control_hashes[normalized] = _hash_stream(source)
        if member_count == 0 or not root_directory_seen:
            raise ArchiveValidationError(
                "release archive is empty or lacks its normalized root"
            )
        for relative in sorted(entry_types):
            parts = PurePosixPath(relative).parts
            for depth in range(1, len(parts)):
                parent = PurePosixPath(*parts[:depth]).as_posix()
                if entry_types.get(parent) != "directory":
                    raise ArchiveValidationError(
                        "release archive member lacks an explicit directory "
                        "parent with validated owner/mode: "
                        f"{parent} -> {relative}"
                    )
        if archive_control_hashes != trusted_hashes:
            missing = sorted(set(trusted_hashes) - set(archive_control_hashes))
            changed = sorted(
                relative
                for relative in set(trusted_hashes) & set(archive_control_hashes)
                if trusted_hashes[relative] != archive_control_hashes[relative]
            )
            raise ArchiveValidationError(
                "trusted control provenance differs from immutable archive; "
                f"missing={missing}, changed={changed}"
            )
        if (
            required_workbook_mode != "0600"
            or diagnostics_file_modes != {"0600"}
            or diagnostics_directory_modes != {"0711"}
            or not REQUIRED_DIAGNOSTICS_FILES.issubset(
                {
                    str(item["path"])
                    for item in private_file_entries
                }
            )
        ):
            raise ArchiveValidationError(
                "release archive lacks exact private workbook/required "
                "diagnostics 0600-file and 0711-directory coverage"
            )
        os.lseek(descriptor, 0, os.SEEK_SET)
        with os.fdopen(os.dup(descriptor), "rb") as final_identity_stream:
            final_sha256 = _hash_stream(final_identity_stream)
        if final_sha256 != actual_sha256:
            raise ArchiveValidationError(
                "release archive content changed during validation"
            )
        final_metadata = os.fstat(descriptor)
        if (
            (final_metadata.st_dev, final_metadata.st_ino, final_metadata.st_size)
            != (opened.st_dev, opened.st_ino, opened.st_size)
        ):
            raise ArchiveValidationError(
                "release archive identity changed during validation"
            )
        return {
            "schemaVersion": SCHEMA_VERSION,
            "status": "validated",
            "archiveSha256": actual_sha256,
            "archiveBytes": opened.st_size,
            "memberCount": member_count,
            "expandedBytes": expanded_bytes,
            "rootMode": "0755",
            "modePolicy": {
                "publicFiles": ["0644", "0755"],
                "publicDirectories": ["0755"],
                "privatePrefixes": list(PRIVATE_PREFIXES),
                "privateFiles": ["0600", "0711"],
                "privateDirectories": ["0711"],
            },
            "memberClasses": {
                "publicFiles": public_files,
                "publicDirectories": public_directories,
                "privateFiles": private_files,
                "privateDirectories": private_directories,
            },
            "privateModeEvidence": {
                "requiredWorkbook": {
                    "path": REQUIRED_PRIVATE_WORKBOOK,
                    "type": "file",
                    "mode": required_workbook_mode,
                },
                "diagnosticsArtifacts": {
                    "prefix": REQUIRED_DIAGNOSTICS_PREFIX,
                    "fileModes": sorted(diagnostics_file_modes),
                    "directoryModes": sorted(diagnostics_directory_modes),
                },
            },
            "privateEntries": {
                "files": sorted(
                    private_file_entries,
                    key=lambda item: str(item["path"]),
                ),
                "directories": sorted(
                    private_directory_entries,
                    key=lambda item: str(item["path"]),
                ),
            },
            "trustedControls": trusted_hashes,
        }
    except (OSError, tarfile.TarError) as exc:
        raise ArchiveValidationError(
            f"release archive cannot be parsed safely: {exc}"
        ) from exc
    finally:
        os.close(descriptor)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--expected-bytes", type=int, required=True)
    parser.add_argument(
        "--trusted-control",
        action="append",
        default=[],
        metavar="ARCHIVE_RELATIVE_PATH=LOCAL_ABSOLUTE_PATH",
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--headroom-target",
        action="append",
        nargs=3,
        default=[],
        metavar=("PATH", "COPIES", "RESERVE_BYTES"),
    )
    parser.add_argument("--validation-run-id")
    parser.add_argument("--validation-run-attempt", type=int)
    parser.add_argument("--sealed-root", type=Path)
    parser.add_argument("--sealed-helper", type=Path)
    parser.add_argument("--expected-helper-sha256")
    parser.add_argument("--expected-sealed-group", type=int)
    arguments = parser.parse_args()
    try:
        receipt = validate_archive(
            arguments.archive,
            expected_sha256=arguments.expected_sha256,
            expected_bytes=arguments.expected_bytes,
            trusted_controls=_parse_trusted_controls(arguments.trusted_control),
        )
        attempt_values = (
            arguments.validation_run_id,
            arguments.validation_run_attempt,
        )
        if any(value is not None for value in attempt_values):
            if (
                not arguments.validation_run_id
                or arguments.validation_run_attempt is None
                or arguments.validation_run_attempt <= 0
            ):
                raise ArchiveValidationError(
                    "release validation attempt identity is incomplete"
                )
            receipt["validationAttempt"] = {
                "runId": arguments.validation_run_id,
                "runAttempt": arguments.validation_run_attempt,
            }
        sealed_values = (
            arguments.sealed_root,
            arguments.sealed_helper,
            arguments.expected_helper_sha256,
            arguments.expected_sealed_group,
        )
        if any(value is not None for value in sealed_values):
            if (
                arguments.sealed_root != Path("/var/lib/jato-sealed-inputs")
                or arguments.sealed_helper is None
                or arguments.expected_helper_sha256 is None
                or arguments.expected_sealed_group is None
            ):
                raise ArchiveValidationError(
                    "root-sealed release validation inputs are incomplete"
                )
            receipt["sealedInput"] = inspect_root_sealed_bundle(
                archive_path=arguments.archive,
                helper_path=arguments.sealed_helper,
                sealed_root=arguments.sealed_root,
                expected_archive_sha256=arguments.expected_sha256,
                expected_archive_bytes=arguments.expected_bytes,
                expected_helper_sha256=arguments.expected_helper_sha256,
                expected_group_id=arguments.expected_sealed_group,
            )
        if arguments.headroom_target:
            requests: list[tuple[Path, int, int]] = []
            for target_name, copies_raw, reserve_raw in arguments.headroom_target:
                try:
                    copies = int(copies_raw)
                    reserve_bytes = int(reserve_raw)
                except ValueError as exc:
                    raise ArchiveValidationError(
                        "release headroom copies/reserve must be integers"
                    ) from exc
                requests.append((Path(target_name), copies, reserve_bytes))
            receipt["headroomChecks"] = evaluate_materialization_headroom(
                receipt,
                requests=requests,
            )
        if arguments.output is not None:
            _atomic_json(arguments.output, receipt)
        print(json.dumps(receipt, sort_keys=True, separators=(",", ":")))
        return 0
    except (ArchiveValidationError, OSError, ValueError) as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
