"""Lightweight, read-only application readiness checks."""

from __future__ import annotations

import json
import os
import re
import struct
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

import pyarrow.parquet as pq

from app.core.config import (
    PARQUET_PATH,
    PARTITIONED_PATH,
    PROJECT_ROOT,
)
from app.services.hermes_deploy_status_service import (
    normalize_release_metadata,
)

_COMMIT_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_MAX_RELEASE_METADATA_BYTES = 256 * 1024
_MAX_DATASET_MANIFEST_BYTES = 2 * 1024 * 1024
_MAX_PARTITION_DIRECTORIES = 128
# Production currently has 264 partition files. Keep a fixed hard ceiling with
# bounded headroom instead of deriving an unbounded limit from the manifest.
_MAX_PARQUET_FILES = 512
_MAX_DATASET_ENTRIES = 4096
_MAX_PARQUET_FOOTER_METADATA_BYTES = 4 * 1024 * 1024
_PARQUET_MAGIC = b"PAR1"

ReleaseMetadataProvider = Callable[[], dict[str, Any]]
GitCommitProvider = Callable[[Path], str]


@dataclass(frozen=True)
class RuntimeReleaseIdentity:
    """Immutable identity of the code loaded by this backend process."""

    commit_sha: str
    provenance: str


@dataclass(frozen=True)
class ReadinessSettings:
    """Filesystem and provenance inputs used by readiness checks."""

    release_metadata_path: Path
    partitioned_path: Path
    parquet_path: Path
    allow_release_metadata_fallback: bool = False


def _canonical_commit_sha(value: Any) -> str:
    candidate = str(value or "").strip().lower()
    return candidate if _COMMIT_SHA_PATTERN.fullmatch(candidate) else ""


def _git_commit_sha(project_root: Path) -> str:
    if not (project_root / ".git").exists():
        return ""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(project_root),
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    if result.returncode != 0:
        return ""
    return _canonical_commit_sha(result.stdout)


def resolve_runtime_release_identity(
    *,
    environ: Mapping[str, str] | None = None,
    project_root: Path = PROJECT_ROOT,
    git_commit_provider: GitCommitProvider = _git_commit_sha,
) -> RuntimeReleaseIdentity:
    """Resolve runtime identity once; callers should retain the result."""
    active_environ = environ if environ is not None else os.environ
    for env_name in ("APP_RELEASE_SHA", "APP_GIT_SHA"):
        raw_sha = str(active_environ.get(env_name) or "").strip()
        if raw_sha:
            return RuntimeReleaseIdentity(
                commit_sha=_canonical_commit_sha(raw_sha),
                provenance=env_name,
            )

    return RuntimeReleaseIdentity(
        commit_sha=git_commit_provider(project_root),
        provenance="git_worktree",
    )


_FROZEN_RUNTIME_RELEASE = resolve_runtime_release_identity()


def default_readiness_settings() -> ReadinessSettings:
    return ReadinessSettings(
        release_metadata_path=PROJECT_ROOT / "hermes" / "deploy_release.json",
        partitioned_path=PARTITIONED_PATH,
        parquet_path=PARQUET_PATH,
        allow_release_metadata_fallback=False,
    )


def _ok(code: str, **metadata: Any) -> dict[str, Any]:
    return {"status": "ok", "code": code, **metadata}


def _failed(code: str) -> dict[str, str]:
    return {"status": "failed", "code": code}


def _is_commit_sha(value: Any) -> bool:
    return bool(_canonical_commit_sha(value))


def _read_bounded_json(path: Path, *, max_bytes: int) -> dict[str, Any] | None:
    try:
        size = path.stat().st_size
        if size <= 0 or size > max_bytes:
            return None
        with path.open("rb") as handle:
            payload = handle.read(max_bytes + 1)
        if len(payload) > max_bytes:
            return None
        parsed = json.loads(payload.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _check_release(
    settings: ReadinessSettings,
    *,
    fallback_provider: ReleaseMetadataProvider,
    runtime_release: RuntimeReleaseIdentity,
) -> tuple[dict[str, Any], dict[str, Any]]:
    release_path = settings.release_metadata_path
    release_metadata: dict[str, Any] = {}
    raw_release_metadata: dict[str, Any] = {}
    metadata_provenance = "deploy_release_file"

    raw_release_metadata = _read_bounded_json(
        release_path,
        max_bytes=_MAX_RELEASE_METADATA_BYTES,
    ) or {}
    if raw_release_metadata:
        release_metadata = normalize_release_metadata(raw_release_metadata)

    if (
        not release_metadata
        and settings.allow_release_metadata_fallback
    ):
        try:
            raw_release_metadata = fallback_provider()
        except Exception:
            raw_release_metadata = {}
        release_metadata = normalize_release_metadata(raw_release_metadata)
        metadata_provenance = str(
            release_metadata.get("source") or "fallback"
        )

    runtime_sha = _canonical_commit_sha(runtime_release.commit_sha)
    release = {
        "commitSha": runtime_sha,
        "provenance": runtime_release.provenance,
        "metadataProvenance": metadata_provenance,
    }
    if not _is_commit_sha(runtime_sha):
        return _failed("runtime_commit_unavailable"), release

    if not release_metadata:
        return _failed("release_metadata_unavailable"), release

    expected_sha = _canonical_commit_sha(
        raw_release_metadata.get("expectedCommitSha")
        or raw_release_metadata.get("commitSha")
        or ""
    )
    release["expectedCommitSha"] = expected_sha

    if not _is_commit_sha(expected_sha):
        return _failed("release_expected_commit_invalid"), release
    if runtime_sha != expected_sha:
        return _failed("runtime_release_mismatch"), release

    return (
        _ok(
            "release_metadata_valid",
            provenance=runtime_release.provenance,
            metadataProvenance=metadata_provenance,
        ),
        release,
    )


def _frozen_release_metadata(
    runtime_release: RuntimeReleaseIdentity,
) -> dict[str, Any]:
    return {
        "expectedCommitSha": runtime_release.commit_sha,
        "source": runtime_release.provenance,
    }


def _read_parquet_shape(path: Path) -> tuple[int, int] | None:
    parquet_file: pq.ParquetFile | None = None
    try:
        if path.is_symlink() or not path.is_file():
            return None
        file_size = path.stat().st_size
        if file_size < 12:
            return None
        with path.open("rb") as handle:
            header = handle.read(4)
            handle.seek(-8, 2)
            footer = handle.read(8)
        footer_length = struct.unpack("<I", footer[:4])[0]
        if (
            header != _PARQUET_MAGIC
            or footer[4:] != _PARQUET_MAGIC
            or footer_length <= 0
            or footer_length > _MAX_PARQUET_FOOTER_METADATA_BYTES
            or footer_length + 12 > file_size
        ):
            return None
        parquet_file = pq.ParquetFile(path)
        metadata = parquet_file.metadata
        return metadata.num_rows, metadata.num_columns
    except Exception:
        return None
    finally:
        if parquet_file is not None:
            try:
                parquet_file.close()
            except Exception:
                pass


def _safe_partition_directory(root: Path, raw_relative_path: Any) -> Path | None:
    raw_path = str(raw_relative_path or "").strip()
    if not raw_path:
        return None
    relative_path = Path(raw_path)
    if relative_path.is_absolute():
        return None
    try:
        resolved_root = root.resolve()
        candidate = (resolved_root / relative_path).resolve()
        candidate.relative_to(resolved_root)
    except (OSError, ValueError):
        return None
    if candidate == resolved_root:
        return None
    return candidate


def _parquet_files_within(
    root: Path,
) -> tuple[list[Path] | None, str | None]:
    try:
        resolved_root = root.resolve()
        resolved_files: list[Path] = []
        pending_directories = [resolved_root]
        entry_count = 0
        while pending_directories:
            current_directory = pending_directories.pop()
            with os.scandir(current_directory) as entries:
                for entry in entries:
                    entry_count += 1
                    if entry_count > _MAX_DATASET_ENTRIES:
                        return None, "dataset_entry_limit_exceeded"
                    if entry.is_symlink():
                        return None, "dataset_parquet_enumeration_failed"
                    if entry.is_dir(follow_symlinks=False):
                        pending_directories.append(Path(entry.path))
                        continue
                    if not entry.name.endswith(".parquet"):
                        continue
                    if not entry.is_file(follow_symlinks=False):
                        return None, "dataset_parquet_enumeration_failed"
                    if len(resolved_files) >= _MAX_PARQUET_FILES:
                        return None, "dataset_parquet_limit_exceeded"
                    resolved_candidate = Path(entry.path).resolve()
                    resolved_candidate.relative_to(resolved_root)
                    resolved_files.append(resolved_candidate)
        if len(set(resolved_files)) != len(resolved_files):
            return None, "dataset_parquet_enumeration_failed"
        return sorted(resolved_files), None
    except (OSError, ValueError):
        return None, "dataset_parquet_enumeration_failed"


def _check_partitioned_dataset(path: Path) -> dict[str, Any]:
    manifest = _read_bounded_json(
        path / "manifest.json",
        max_bytes=_MAX_DATASET_MANIFEST_BYTES,
    )
    if manifest is None:
        return _failed("dataset_manifest_unreadable")

    parquet_count = manifest.get("parquetFileCount")
    partition_directories = manifest.get("partitionDirectories")
    if (
        not isinstance(parquet_count, int)
        or isinstance(parquet_count, bool)
        or parquet_count <= 0
        or parquet_count > _MAX_PARQUET_FILES
        or not isinstance(partition_directories, list)
        or not partition_directories
        or len(partition_directories) > _MAX_PARTITION_DIRECTORIES
    ):
        if (
            isinstance(parquet_count, int)
            and not isinstance(parquet_count, bool)
            and parquet_count > _MAX_PARQUET_FILES
        ) or (
            isinstance(partition_directories, list)
            and len(partition_directories) > _MAX_PARTITION_DIRECTORIES
        ):
            return _failed("dataset_manifest_limit_exceeded")
        return _failed("dataset_manifest_invalid")

    resolved_partitions: list[Path] = []
    for raw_directory in partition_directories:
        partition = _safe_partition_directory(path, raw_directory)
        if partition is None or not partition.is_dir():
            return _failed("dataset_partition_missing")
        resolved_partitions.append(partition)

    if len(set(resolved_partitions)) != len(resolved_partitions):
        return _failed("dataset_manifest_invalid")

    parquet_files, enumeration_error = _parquet_files_within(path)
    if parquet_files is None:
        return _failed(
            enumeration_error or "dataset_parquet_enumeration_failed"
        )

    assigned_files: set[Path] = set()
    for partition in resolved_partitions:
        partition_files = [
            parquet_file
            for parquet_file in parquet_files
            if parquet_file.is_relative_to(partition)
        ]
        if not partition_files:
            return _failed("dataset_partition_empty")
        assigned_files.update(partition_files)
    if len(parquet_files) != parquet_count:
        return _failed("dataset_parquet_count_mismatch")
    if assigned_files != set(parquet_files):
        return _failed("dataset_unassigned_parquet")

    for parquet_file in parquet_files:
        shape = _read_parquet_shape(parquet_file)
        if shape is None:
            return _failed("dataset_parquet_unreadable")
        row_count, column_count = shape
        if row_count <= 0 or column_count <= 0:
            return _failed("dataset_parquet_empty")

    return _ok(
        "partitioned_dataset_readable",
        source="partitioned",
        partitionCount=len(resolved_partitions),
        parquetFileCount=parquet_count,
    )


def _check_dataset(settings: ReadinessSettings) -> dict[str, Any]:
    try:
        partitioned_exists = settings.partitioned_path.exists()
    except OSError:
        partitioned_exists = False

    if partitioned_exists:
        if not settings.partitioned_path.is_dir():
            return _failed("partitioned_dataset_invalid")
        return _check_partitioned_dataset(settings.partitioned_path)

    shape = _read_parquet_shape(settings.parquet_path)
    if shape is None:
        return _failed("active_dataset_unreadable")
    row_count, column_count = shape
    if row_count <= 0 or column_count <= 0:
        return _failed("active_dataset_empty")
    return _ok(
        "parquet_dataset_readable",
        source="parquet",
        parquetFileCount=1,
    )


def build_readiness_report(
    settings: ReadinessSettings | None = None,
    *,
    release_metadata_provider: ReleaseMetadataProvider | None = None,
    runtime_release: RuntimeReleaseIdentity | None = None,
) -> dict[str, Any]:
    """Build a non-mutating readiness report with safe public details."""
    active_settings = settings or default_readiness_settings()
    active_runtime_release = runtime_release or _FROZEN_RUNTIME_RELEASE
    active_release_provider = release_metadata_provider or (
        lambda: _frozen_release_metadata(active_runtime_release)
    )

    try:
        release_check, release = _check_release(
            active_settings,
            fallback_provider=active_release_provider,
            runtime_release=active_runtime_release,
        )
    except Exception:
        release_check = _failed("release_check_failed")
        release = {"commitSha": ""}

    try:
        dataset_check = _check_dataset(active_settings)
    except Exception:
        dataset_check = _failed("dataset_check_failed")

    checks = {
        "release": release_check,
        "activeDataset": dataset_check,
    }
    ready = all(check["status"] == "ok" for check in checks.values())
    failures = [
        {"check": check_name, "code": check["code"]}
        for check_name, check in checks.items()
        if check["status"] != "ok"
    ]
    return {
        "status": "ready" if ready else "not_ready",
        "release": release,
        "checks": checks,
        "failures": failures,
    }
