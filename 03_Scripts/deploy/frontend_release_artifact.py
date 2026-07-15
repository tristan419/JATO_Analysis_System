#!/usr/bin/env python3
"""Create, verify, materialize, and audit immutable frontend releases."""

from __future__ import annotations

import argparse
import copy
import datetime as dt
import gzip
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import sys
import tarfile
import tempfile
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


SCHEMA_VERSION = 1
PAYLOAD_NAME = "frontend-dist.tar.gz"
MANIFEST_NAME = "frontend-release.json"
PUBLIC_PROVENANCE_NAME = "release-provenance.json"
BUILD_META_NAME = "build-meta.json"
SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
FRONTEND_BUILD_ID_IGNORES = {BUILD_META_NAME, PUBLIC_PROVENANCE_NAME}


class ReleaseValidationError(ValueError):
    """Raised when an immutable frontend release violates its contract."""


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ReleaseValidationError(f"required JSON file is missing: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ReleaseValidationError(f"invalid JSON in {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ReleaseValidationError(f"JSON root must be an object: {path}")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _mapping(payload: dict[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise ReleaseValidationError(f"manifest field {key!r} must be an object")
    return value


def _required_string(payload: dict[str, Any], key: str, context: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ReleaseValidationError(f"{context}.{key} must be a non-empty string")
    return value.strip()


def _require_sha(value: str, context: str) -> str:
    normalized = value.strip().lower()
    if not SHA_PATTERN.fullmatch(normalized):
        raise ReleaseValidationError(f"{context} must be a 40-character git SHA")
    return normalized


def _require_sha256(value: str, context: str) -> str:
    normalized = value.strip().lower()
    if not SHA256_PATTERN.fullmatch(normalized):
        raise ReleaseValidationError(f"{context} must be a 64-character SHA256")
    return normalized


def _require_iso_timestamp(value: str, context: str) -> str:
    normalized = value.strip()
    try:
        parsed = dt.datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ReleaseValidationError(f"{context} must be an ISO8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ReleaseValidationError(f"{context} must include a timezone")
    return normalized


def _normalize_artifact_digest(value: str) -> str:
    normalized = value.strip().lower()
    if normalized.startswith("sha256:"):
        normalized = normalized.removeprefix("sha256:")
    return f"sha256:{_require_sha256(normalized, 'GitHub artifact digest')}"


def _require_github_artifact_id(value: str) -> str:
    normalized = value.strip()
    if not normalized.isdigit() or int(normalized) <= 0:
        raise ReleaseValidationError("GitHub artifact id must be a positive integer")
    return normalized


def sha256_file(path: Path) -> str:
    if not path.is_file():
        raise ReleaseValidationError(f"artifact payload is missing: {path}")
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def frontend_build_id(dist_dir: Path) -> str:
    if not dist_dir.is_dir():
        raise ReleaseValidationError(f"frontend dist directory is missing: {dist_dir}")
    digest = hashlib.sha256()
    files = sorted(path for path in dist_dir.rglob("*") if path.is_file())
    for path in files:
        relative = path.relative_to(dist_dir).as_posix()
        if relative in FRONTEND_BUILD_ID_IGNORES:
            continue
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def artifact_identity(
    repository: str,
    run_id: str,
    run_attempt: str,
    artifact_name: str,
) -> str:
    return (
        f"gha://{repository}/actions/runs/{run_id}/attempts/{run_attempt}"
        f"/artifacts/{artifact_name}"
    )


def _validate_dist(
    dist_dir: Path,
    *,
    expected_github_sha: str,
    expected_build_id: str | None = None,
    expected_node_version: str | None = None,
) -> dict[str, Any]:
    if not (dist_dir / "index.html").is_file():
        raise ReleaseValidationError("frontend dist is incomplete: index.html is missing")

    build_meta = _read_json(dist_dir / BUILD_META_NAME)
    commit = _require_sha(
        _required_string(build_meta, "commit", "build-meta"),
        "build-meta.commit",
    )
    deploy_commit = _require_sha(
        _required_string(build_meta, "deployCommit", "build-meta"),
        "build-meta.deployCommit",
    )
    if deploy_commit != expected_github_sha:
        raise ReleaseValidationError(
            "build-meta deployCommit mismatch: "
            f"{deploy_commit} != {expected_github_sha}"
        )

    built_at = _require_iso_timestamp(
        _required_string(build_meta, "builtAt", "build-meta"),
        "build-meta.builtAt",
    )
    node_version = _required_string(build_meta, "nodeVersion", "build-meta")
    if not re.fullmatch(r"v\d+\.\d+\.\d+", node_version):
        raise ReleaseValidationError("build-meta.nodeVersion must be an exact Node version")
    if expected_node_version and node_version != expected_node_version:
        raise ReleaseValidationError(
            f"Node version mismatch: {node_version} != {expected_node_version}"
        )

    recorded_build_id = _require_sha256(
        _required_string(build_meta, "frontendBuildId", "build-meta"),
        "build-meta.frontendBuildId",
    )
    calculated_build_id = frontend_build_id(dist_dir)
    if recorded_build_id != calculated_build_id:
        raise ReleaseValidationError(
            "frontend build id mismatch: "
            f"{recorded_build_id} != {calculated_build_id}"
        )
    if expected_build_id and recorded_build_id != expected_build_id:
        raise ReleaseValidationError(
            f"unexpected frontend build id: {recorded_build_id} != {expected_build_id}"
        )

    return {
        "appCommit": commit,
        "buildTimestamp": built_at,
        "frontendBuildId": recorded_build_id,
        "nodeVersion": node_version,
    }


def _normalized_tar_info(path: Path, archive_name: str) -> tarfile.TarInfo:
    info = tarfile.TarInfo(archive_name)
    info.mtime = 0
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    if path.is_dir():
        info.type = tarfile.DIRTYPE
        info.mode = 0o755
        return info
    if not path.is_file() or path.is_symlink():
        raise ReleaseValidationError(f"unsupported frontend dist entry: {path}")
    info.size = path.stat().st_size
    info.mode = 0o644
    return info


def _write_deterministic_payload(dist_dir: Path, payload_path: Path) -> None:
    with payload_path.open("wb") as raw_handle:
        with gzip.GzipFile(
            filename="",
            mode="wb",
            fileobj=raw_handle,
            mtime=0,
        ) as gzip_handle:
            with tarfile.open(fileobj=gzip_handle, mode="w") as archive:
                root_info = _normalized_tar_info(dist_dir, "dist")
                archive.addfile(root_info)
                for path in sorted(dist_dir.rglob("*")):
                    relative = path.relative_to(dist_dir).as_posix()
                    archive_name = f"dist/{relative}"
                    info = _normalized_tar_info(path, archive_name)
                    if path.is_file():
                        with path.open("rb") as source:
                            archive.addfile(info, source)
                    else:
                        archive.addfile(info)


def create_release(
    *,
    dist_dir: Path,
    release_dir: Path,
    github_sha: str,
    artifact_name: str,
    repository: str,
    workflow: str,
    run_id: str,
    run_attempt: str,
) -> dict[str, Any]:
    normalized_sha = _require_sha(github_sha, "github sha")
    expected_name = f"frontend-dist-{normalized_sha}"
    if artifact_name != expected_name:
        raise ReleaseValidationError(
            f"artifact name must be {expected_name!r}; found {artifact_name!r}"
        )
    for value, context in (
        (repository, "repository"),
        (workflow, "workflow"),
        (run_id, "run id"),
        (run_attempt, "run attempt"),
    ):
        if not value.strip():
            raise ReleaseValidationError(f"{context} is required")

    dist_metadata = _validate_dist(
        dist_dir,
        expected_github_sha=normalized_sha,
    )
    if release_dir.exists() and any(release_dir.iterdir()):
        raise ReleaseValidationError(
            f"release directory must be empty before creation: {release_dir}"
        )
    release_dir.mkdir(parents=True, exist_ok=True)
    payload_path = release_dir / PAYLOAD_NAME
    _write_deterministic_payload(dist_dir, payload_path)
    payload_checksum = sha256_file(payload_path)
    identity = artifact_identity(repository, run_id, run_attempt, artifact_name)

    manifest: dict[str, Any] = {
        "schemaVersion": SCHEMA_VERSION,
        "release": {
            "releaseId": f"{run_id}-{run_attempt}",
            "environment": "production",
            "repository": repository,
            "workflow": workflow,
            "workflowRunId": run_id,
            "workflowRunAttempt": run_attempt,
            "buildTimestamp": dist_metadata["buildTimestamp"],
        },
        "source": {
            "githubSha": normalized_sha,
            "appCommit": dist_metadata["appCommit"],
            "deployCommit": normalized_sha,
            "deployCommitSemantics": (
                "GitHub workflow source revision; appCommit may point through "
                "Hermes-only metadata commits"
            ),
        },
        "artifact": {
            "name": artifact_name,
            "id": identity,
            "payload": PAYLOAD_NAME,
            "payloadBytes": payload_path.stat().st_size,
            "checksumAlgorithm": "sha256",
            "checksum": payload_checksum,
        },
        "frontend": {
            "buildMetaPath": BUILD_META_NAME,
            "buildId": dist_metadata["frontendBuildId"],
            "buildIdSemantics": (
                "SHA256 over sorted dist file paths and bytes, excluding release metadata"
            ),
            "nodeVersion": dist_metadata["nodeVersion"],
        },
    }
    _write_json(release_dir / MANIFEST_NAME, manifest)
    return manifest


def _safe_extract_payload(payload_path: Path, destination: Path) -> Path:
    destination.mkdir(parents=True, exist_ok=True)
    seen_names: set[str] = set()
    try:
        archive = tarfile.open(payload_path, mode="r:gz")
    except (OSError, tarfile.TarError) as exc:
        raise ReleaseValidationError(f"invalid frontend payload archive: {exc}") from exc
    with archive:
        members = archive.getmembers()
        if not members:
            raise ReleaseValidationError("frontend payload archive is empty")
        for member in members:
            pure_name = PurePosixPath(member.name)
            if (
                pure_name.is_absolute()
                or ".." in pure_name.parts
                or not pure_name.parts
                or pure_name.parts[0] != "dist"
            ):
                raise ReleaseValidationError(
                    f"unsafe frontend payload path: {member.name!r}"
                )
            normalized_name = pure_name.as_posix()
            if normalized_name in seen_names:
                raise ReleaseValidationError(
                    f"duplicate frontend payload path: {normalized_name!r}"
                )
            seen_names.add(normalized_name)
            target = destination.joinpath(*pure_name.parts)
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            if not member.isfile():
                raise ReleaseValidationError(
                    f"frontend payload contains a non-regular file: {member.name!r}"
                )
            target.parent.mkdir(parents=True, exist_ok=True)
            source = archive.extractfile(member)
            if source is None:
                raise ReleaseValidationError(
                    f"could not read frontend payload member: {member.name!r}"
                )
            with source, target.open("wb") as output:
                shutil.copyfileobj(source, output)
            target.chmod(0o644)
    return destination / "dist"


def _validate_manifest(
    manifest: dict[str, Any],
    *,
    expected_github_sha: str,
    expected_artifact_name: str,
    expected_artifact_identity: str,
    expected_artifact_checksum: str,
    expected_build_id: str,
    expected_node_version: str,
    expected_run_id: str,
    expected_run_attempt: str,
) -> None:
    if manifest.get("schemaVersion") != SCHEMA_VERSION:
        raise ReleaseValidationError(
            f"unsupported or missing manifest schemaVersion: {manifest.get('schemaVersion')!r}"
        )
    release = _mapping(manifest, "release")
    source = _mapping(manifest, "source")
    artifact = _mapping(manifest, "artifact")
    frontend = _mapping(manifest, "frontend")

    normalized_sha = _require_sha(expected_github_sha, "expected github sha")
    manifest_sha = _require_sha(
        _required_string(source, "githubSha", "source"),
        "source.githubSha",
    )
    if manifest_sha != normalized_sha:
        raise ReleaseValidationError(
            f"manifest github SHA mismatch: {manifest_sha} != {normalized_sha}"
        )
    app_commit = _require_sha(
        _required_string(source, "appCommit", "source"),
        "source.appCommit",
    )
    deploy_commit = _require_sha(
        _required_string(source, "deployCommit", "source"),
        "source.deployCommit",
    )
    if deploy_commit != normalized_sha:
        raise ReleaseValidationError(
            f"manifest deploy commit mismatch: {deploy_commit} != {normalized_sha}"
        )
    _required_string(source, "deployCommitSemantics", "source")

    for key in (
        "releaseId",
        "environment",
        "repository",
        "workflow",
        "workflowRunId",
        "workflowRunAttempt",
        "buildTimestamp",
    ):
        _required_string(release, key, "release")
    if release["environment"] != "production":
        raise ReleaseValidationError("release.environment must be production")
    if release["workflowRunId"] != expected_run_id:
        raise ReleaseValidationError("release workflow run id mismatch")
    if release["workflowRunAttempt"] != expected_run_attempt:
        raise ReleaseValidationError("release workflow run attempt mismatch")
    if release["releaseId"] != f"{expected_run_id}-{expected_run_attempt}":
        raise ReleaseValidationError("release id does not match workflow run identity")

    manifest_name = _required_string(artifact, "name", "artifact")
    manifest_identity = _required_string(artifact, "id", "artifact")
    payload_name = _required_string(artifact, "payload", "artifact")
    checksum_algorithm = _required_string(
        artifact,
        "checksumAlgorithm",
        "artifact",
    )
    manifest_checksum = _require_sha256(
        _required_string(artifact, "checksum", "artifact"),
        "artifact.checksum",
    )
    payload_bytes = artifact.get("payloadBytes")
    if not isinstance(payload_bytes, int) or payload_bytes <= 0:
        raise ReleaseValidationError("artifact.payloadBytes must be a positive integer")
    if manifest_name != expected_artifact_name:
        raise ReleaseValidationError("artifact name does not match the build job output")
    if manifest_name != f"frontend-dist-{normalized_sha}":
        raise ReleaseValidationError("artifact name does not match the GitHub SHA")
    if manifest_identity != expected_artifact_identity:
        raise ReleaseValidationError("artifact identity does not match the build job output")
    calculated_identity = artifact_identity(
        release["repository"],
        expected_run_id,
        expected_run_attempt,
        expected_artifact_name,
    )
    if manifest_identity != calculated_identity:
        raise ReleaseValidationError("artifact identity is not derived from the release run")
    if payload_name != PAYLOAD_NAME:
        raise ReleaseValidationError(f"artifact.payload must be {PAYLOAD_NAME}")
    if checksum_algorithm != "sha256":
        raise ReleaseValidationError("artifact checksum algorithm must be sha256")
    if manifest_checksum != _require_sha256(
        expected_artifact_checksum,
        "expected artifact checksum",
    ):
        raise ReleaseValidationError("artifact checksum does not match the build job output")

    manifest_build_id = _require_sha256(
        _required_string(frontend, "buildId", "frontend"),
        "frontend.buildId",
    )
    if manifest_build_id != _require_sha256(
        expected_build_id,
        "expected frontend build id",
    ):
        raise ReleaseValidationError("frontend build id does not match the build job output")
    if _required_string(frontend, "nodeVersion", "frontend") != expected_node_version:
        raise ReleaseValidationError("Node version does not match the build job output")
    if _required_string(frontend, "buildMetaPath", "frontend") != BUILD_META_NAME:
        raise ReleaseValidationError("frontend.buildMetaPath is invalid")
    _required_string(frontend, "buildIdSemantics", "frontend")
    if not app_commit:
        raise ReleaseValidationError("source.appCommit is required")


def public_provenance(
    manifest: dict[str, Any],
    *,
    github_artifact_id: str,
    github_artifact_digest: str,
) -> dict[str, Any]:
    payload = copy.deepcopy(manifest)
    artifact = _mapping(payload, "artifact")
    artifact["githubId"] = _require_github_artifact_id(github_artifact_id)
    artifact["githubDigest"] = _normalize_artifact_digest(github_artifact_digest)
    payload["verification"] = {
        "artifactIdentityVerified": True,
        "githubShaVerified": True,
        "manifestComplete": True,
        "payloadChecksumVerified": True,
    }
    return payload


def _enrich_build_meta(
    build_meta: dict[str, Any],
    provenance: dict[str, Any],
) -> dict[str, Any]:
    release = _mapping(provenance, "release")
    source = _mapping(provenance, "source")
    artifact = _mapping(provenance, "artifact")
    frontend = _mapping(provenance, "frontend")
    enriched = copy.deepcopy(build_meta)
    enriched.update(
        {
            "commit": source["appCommit"],
            "deployCommit": source["deployCommit"],
            "commitMode": (
                "deploy"
                if source["appCommit"] == source["deployCommit"]
                else "application"
            ),
            "builtAt": release["buildTimestamp"],
            "nodeVersion": frontend["nodeVersion"],
            "frontendBuildId": frontend["buildId"],
            "appCommit": source["appCommit"],
            "githubSha": source["githubSha"],
            "artifactName": artifact["name"],
            "artifactId": artifact["id"],
            "artifactChecksum": artifact["checksum"],
            "artifactChecksumAlgorithm": artifact["checksumAlgorithm"],
            "githubArtifactId": artifact["githubId"],
            "githubArtifactDigest": artifact["githubDigest"],
            "releaseId": release["releaseId"],
            "workflowRunId": release["workflowRunId"],
            "workflowRunAttempt": release["workflowRunAttempt"],
            "buildTimestamp": release["buildTimestamp"],
            "deployCommitSemantics": source["deployCommitSemantics"],
            "frontendBuildIdSemantics": frontend["buildIdSemantics"],
        }
    )
    return enriched


def _atomic_replace_directory(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    staged = Path(
        tempfile.mkdtemp(prefix=f".{target.name}.staged-", dir=target.parent)
    )
    shutil.rmtree(staged)
    backup = target.parent / f".{target.name}.previous"
    try:
        shutil.copytree(source, staged)
        if backup.exists():
            shutil.rmtree(backup)
        if target.exists():
            os.replace(target, backup)
        try:
            os.replace(staged, target)
        except Exception:
            if backup.exists() and not target.exists():
                os.replace(backup, target)
            raise
        if backup.exists():
            shutil.rmtree(backup)
    finally:
        if staged.exists():
            shutil.rmtree(staged)


def verify_release(
    *,
    release_dir: Path,
    expected_github_sha: str,
    expected_artifact_name: str,
    expected_artifact_identity: str,
    expected_artifact_checksum: str,
    expected_build_id: str,
    expected_node_version: str,
    expected_run_id: str,
    expected_run_attempt: str,
    github_artifact_id: str,
    github_artifact_digest: str,
    materialize_dir: Path | None = None,
) -> dict[str, Any]:
    manifest = _read_json(release_dir / MANIFEST_NAME)
    _validate_manifest(
        manifest,
        expected_github_sha=expected_github_sha,
        expected_artifact_name=expected_artifact_name,
        expected_artifact_identity=expected_artifact_identity,
        expected_artifact_checksum=expected_artifact_checksum,
        expected_build_id=expected_build_id,
        expected_node_version=expected_node_version,
        expected_run_id=expected_run_id,
        expected_run_attempt=expected_run_attempt,
    )

    artifact = _mapping(manifest, "artifact")
    payload_path = release_dir / artifact["payload"]
    actual_checksum = sha256_file(payload_path)
    if actual_checksum != artifact["checksum"]:
        raise ReleaseValidationError(
            "frontend payload checksum mismatch: "
            f"{actual_checksum} != {artifact['checksum']}"
        )
    if payload_path.stat().st_size != artifact["payloadBytes"]:
        raise ReleaseValidationError("frontend payload size does not match the manifest")

    github_id = _require_github_artifact_id(github_artifact_id)
    github_digest = _normalize_artifact_digest(github_artifact_digest)
    provenance = public_provenance(
        manifest,
        github_artifact_id=github_id,
        github_artifact_digest=github_digest,
    )
    with tempfile.TemporaryDirectory(prefix="frontend-release-verify-") as temp_dir:
        extracted_dist = _safe_extract_payload(payload_path, Path(temp_dir))
        dist_metadata = _validate_dist(
            extracted_dist,
            expected_github_sha=_require_sha(
                expected_github_sha,
                "expected github sha",
            ),
            expected_build_id=expected_build_id,
            expected_node_version=expected_node_version,
        )
        source = _mapping(manifest, "source")
        if dist_metadata["appCommit"] != source["appCommit"]:
            raise ReleaseValidationError("dist app commit does not match the manifest")
        if dist_metadata["buildTimestamp"] != _mapping(manifest, "release")["buildTimestamp"]:
            raise ReleaseValidationError("dist build timestamp does not match the manifest")

        if materialize_dir is not None:
            build_meta = _read_json(extracted_dist / BUILD_META_NAME)
            _write_json(
                extracted_dist / BUILD_META_NAME,
                _enrich_build_meta(build_meta, provenance),
            )
            _write_json(extracted_dist / PUBLIC_PROVENANCE_NAME, provenance)
            _atomic_replace_directory(extracted_dist, materialize_dir)

    return provenance


def validate_public_documents(
    build_meta: dict[str, Any],
    provenance: dict[str, Any],
    expected_provenance: dict[str, Any],
) -> None:
    if provenance != expected_provenance:
        raise ReleaseValidationError("public release provenance does not match the artifact")
    expected_meta = _enrich_build_meta(build_meta, expected_provenance)
    for key in (
        "commit",
        "deployCommit",
        "commitMode",
        "builtAt",
        "appCommit",
        "githubSha",
        "nodeVersion",
        "frontendBuildId",
        "artifactName",
        "artifactId",
        "artifactChecksum",
        "artifactChecksumAlgorithm",
        "githubArtifactId",
        "githubArtifactDigest",
        "releaseId",
        "workflowRunId",
        "workflowRunAttempt",
        "buildTimestamp",
        "deployCommitSemantics",
        "frontendBuildIdSemantics",
    ):
        if build_meta.get(key) != expected_meta.get(key):
            raise ReleaseValidationError(
                f"public build metadata field {key!r} does not match the artifact"
            )


def _fetch_json(url: str, timeout_seconds: float) -> dict[str, Any]:
    request = Request(
        url,
        headers={"Cache-Control": "no-cache", "User-Agent": "jato-release-audit/1"},
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            if response.status != 200:
                raise ReleaseValidationError(
                    f"unexpected HTTP status {response.status} for {url}"
                )
            payload = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise ReleaseValidationError(f"could not fetch valid JSON from {url}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ReleaseValidationError(f"public JSON root must be an object: {url}")
    return payload


def audit_public_origins(
    *,
    origins: list[str],
    expected_provenance: dict[str, Any],
    attempts: int,
    delay_seconds: float,
    timeout_seconds: float,
) -> None:
    if not origins:
        raise ReleaseValidationError("at least one public origin is required")
    if attempts <= 0:
        raise ReleaseValidationError("audit attempts must be positive")
    expected_source = _mapping(expected_provenance, "source")
    expected_sha = expected_source["githubSha"]
    failures: list[str] = []
    for origin in origins:
        normalized_origin = origin.rstrip("/")
        last_error = "not attempted"
        for attempt in range(1, attempts + 1):
            cache_bust = f"release_audit={expected_sha}-{attempt}-{int(time.time())}"
            try:
                build_meta = _fetch_json(
                    f"{normalized_origin}/{BUILD_META_NAME}?{cache_bust}",
                    timeout_seconds,
                )
                provenance = _fetch_json(
                    f"{normalized_origin}/{PUBLIC_PROVENANCE_NAME}?{cache_bust}",
                    timeout_seconds,
                )
                validate_public_documents(build_meta, provenance, expected_provenance)
                print(f"[release-audit] {normalized_origin} serves {expected_sha}")
                break
            except ReleaseValidationError as exc:
                last_error = str(exc)
                print(
                    f"[release-audit] {normalized_origin} attempt "
                    f"{attempt}/{attempts}: {last_error}"
                )
                if attempt < attempts:
                    time.sleep(delay_seconds)
        else:
            failures.append(f"{normalized_origin}: {last_error}")
    if failures:
        raise ReleaseValidationError(
            "public release parity audit failed: " + "; ".join(failures)
        )


def _add_common_verify_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--release-dir", type=Path, required=True)
    parser.add_argument("--expected-github-sha", required=True)
    parser.add_argument("--expected-artifact-name", required=True)
    parser.add_argument("--expected-artifact-identity", required=True)
    parser.add_argument("--expected-artifact-checksum", required=True)
    parser.add_argument("--expected-build-id", required=True)
    parser.add_argument("--expected-node-version", required=True)
    parser.add_argument("--expected-run-id", required=True)
    parser.add_argument("--expected-run-attempt", required=True)
    parser.add_argument("--github-artifact-id", required=True)
    parser.add_argument("--github-artifact-digest", required=True)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    create_parser = subparsers.add_parser("create")
    create_parser.add_argument("--dist-dir", type=Path, required=True)
    create_parser.add_argument("--release-dir", type=Path, required=True)
    create_parser.add_argument("--github-sha", required=True)
    create_parser.add_argument("--artifact-name", required=True)
    create_parser.add_argument("--repository", required=True)
    create_parser.add_argument("--workflow", required=True)
    create_parser.add_argument("--run-id", required=True)
    create_parser.add_argument("--run-attempt", required=True)
    create_parser.add_argument("--github-output", type=Path)

    verify_parser = subparsers.add_parser("verify")
    _add_common_verify_arguments(verify_parser)
    verify_parser.add_argument("--materialize-dir", type=Path)

    audit_parser = subparsers.add_parser("audit-public")
    _add_common_verify_arguments(audit_parser)
    audit_parser.add_argument("--origin", action="append", required=True)
    audit_parser.add_argument("--attempts", type=int, default=1)
    audit_parser.add_argument("--delay-seconds", type=float, default=0)
    audit_parser.add_argument("--timeout-seconds", type=float, default=20)
    return parser.parse_args(argv)


def _verify_from_args(args: argparse.Namespace, materialize_dir: Path | None = None) -> dict[str, Any]:
    return verify_release(
        release_dir=args.release_dir,
        expected_github_sha=args.expected_github_sha,
        expected_artifact_name=args.expected_artifact_name,
        expected_artifact_identity=args.expected_artifact_identity,
        expected_artifact_checksum=args.expected_artifact_checksum,
        expected_build_id=args.expected_build_id,
        expected_node_version=args.expected_node_version,
        expected_run_id=args.expected_run_id,
        expected_run_attempt=args.expected_run_attempt,
        github_artifact_id=args.github_artifact_id,
        github_artifact_digest=args.github_artifact_digest,
        materialize_dir=materialize_dir,
    )


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    try:
        if args.command == "create":
            manifest = create_release(
                dist_dir=args.dist_dir,
                release_dir=args.release_dir,
                github_sha=args.github_sha,
                artifact_name=args.artifact_name,
                repository=args.repository,
                workflow=args.workflow,
                run_id=args.run_id,
                run_attempt=args.run_attempt,
            )
            artifact = _mapping(manifest, "artifact")
            frontend = _mapping(manifest, "frontend")
            source = _mapping(manifest, "source")
            if args.github_output:
                with args.github_output.open("a", encoding="utf-8") as output:
                    output.write(f"artifact-name={artifact['name']}\n")
                    output.write(f"artifact-identity={artifact['id']}\n")
                    output.write(f"artifact-checksum={artifact['checksum']}\n")
                    output.write(f"frontend-build-id={frontend['buildId']}\n")
                    output.write(f"node-version={frontend['nodeVersion']}\n")
                    output.write(f"app-commit={source['appCommit']}\n")
            print(
                "[frontend-release] created "
                f"{artifact['name']} checksum={artifact['checksum']}"
            )
            return 0

        if args.command == "verify":
            provenance = _verify_from_args(args, args.materialize_dir)
            artifact = _mapping(provenance, "artifact")
            print(
                "[frontend-release] verified "
                f"{artifact['name']} github_id={artifact['githubId']}"
            )
            return 0

        if args.command == "audit-public":
            provenance = _verify_from_args(args)
            audit_public_origins(
                origins=args.origin,
                expected_provenance=provenance,
                attempts=args.attempts,
                delay_seconds=args.delay_seconds,
                timeout_seconds=args.timeout_seconds,
            )
            return 0
    except ReleaseValidationError as exc:
        print(f"[frontend-release] ERROR: {exc}", file=sys.stderr)
        return 1
    raise AssertionError(f"unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
