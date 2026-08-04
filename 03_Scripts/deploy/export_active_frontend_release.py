#!/usr/bin/env python3
"""Prove and export the immutable frontend artifact from the current www Active."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
import stat
import subprocess
import sys
from pathlib import Path
from typing import Any


COMMIT_PATTERN = re.compile(r"[0-9a-f]{40}")
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
ACTIVE_LINK = Path("/opt/jato/active")
RELEASES_ROOT = Path("/opt/jato/releases")
ACTIVE_SLOT_FILE = Path("/var/lib/jato-release/active-slot")
SLOT_ENV_ROOT = Path("/etc/jato-fullstack/slots")
MAX_JSON_BYTES = 16 * 1024 * 1024


class ActiveFrontendExportError(RuntimeError):
    """Raised when current Active cannot prove one immutable frontend artifact."""


def _stat_identity(metadata: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _read_regular_snapshot(
    path: Path,
    label: str,
    max_bytes: int,
    *,
    retain_bytes: bool,
) -> tuple[bytes | None, str, int]:
    nofollow = getattr(os, "O_NOFOLLOW", None)
    cloexec = getattr(os, "O_CLOEXEC", None)
    if nofollow is None or cloexec is None:
        raise ActiveFrontendExportError("safe no-follow file reads are unavailable")
    try:
        descriptor = os.open(path, os.O_RDONLY | nofollow | cloexec)
    except OSError as exc:
        raise ActiveFrontendExportError(f"{label} cannot be opened safely") from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise ActiveFrontendExportError(f"{label} must be a regular file")
        if before.st_size <= 0 or before.st_size > max_bytes:
            raise ActiveFrontendExportError(f"{label} has an invalid size")
        digest = hashlib.sha256()
        retained = bytearray() if retain_bytes else None
        bytes_read = 0
        while True:
            block = os.read(descriptor, 1024 * 1024)
            if not block:
                break
            bytes_read += len(block)
            if bytes_read > max_bytes:
                raise ActiveFrontendExportError(f"{label} exceeded its size limit")
            digest.update(block)
            if retained is not None:
                retained.extend(block)
        after = os.fstat(descriptor)
        try:
            path_after = os.lstat(path)
        except OSError as exc:
            raise ActiveFrontendExportError(
                f"{label} path changed while it was read"
            ) from exc
        if (
            _stat_identity(before) != _stat_identity(after)
            or before.st_dev != path_after.st_dev
            or before.st_ino != path_after.st_ino
            or not stat.S_ISREG(path_after.st_mode)
            or bytes_read != before.st_size
        ):
            raise ActiveFrontendExportError(f"{label} changed while it was read")
        return (
            bytes(retained) if retained is not None else None,
            digest.hexdigest(),
            bytes_read,
        )
    finally:
        os.close(descriptor)


def _hash_file(path: Path) -> str:
    _, digest, _ = _read_regular_snapshot(
        path,
        "file",
        2 * 1024 * 1024 * 1024,
        retain_bytes=False,
    )
    return digest


def _read_regular_bytes(path: Path, label: str, max_bytes: int) -> bytes:
    raw, _, _ = _read_regular_snapshot(
        path,
        label,
        max_bytes,
        retain_bytes=True,
    )
    if raw is None:
        raise ActiveFrontendExportError(f"{label} was not retained")
    return raw


def _read_regular_json(path: Path, label: str) -> tuple[dict[str, Any], bytes]:
    raw = _read_regular_bytes(path, label, MAX_JSON_BYTES)
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ActiveFrontendExportError(f"{label} is invalid JSON") from exc
    if not isinstance(payload, dict):
        raise ActiveFrontendExportError(f"{label} root must be an object")
    return payload, raw


def _required_mapping(payload: dict[str, Any], key: str, label: str) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise ActiveFrontendExportError(f"{label}.{key} must be an object")
    return value


def _env_value(path: Path, name: str) -> str:
    raw = _read_regular_bytes(path, "Active slot environment", 1024 * 1024)
    matches: list[str] = []
    for line in raw.decode("utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        if key.strip() != name:
            continue
        try:
            tokens = shlex.split(value, posix=True)
        except ValueError as exc:
            raise ActiveFrontendExportError(
                f"Active slot environment has invalid {name}"
            ) from exc
        if len(tokens) != 1:
            raise ActiveFrontendExportError(
                f"Active slot environment has invalid {name}"
            )
        matches.append(tokens[0])
    if len(matches) != 1:
        raise ActiveFrontendExportError(
            f"Active slot environment must define {name} exactly once"
        )
    return matches[0]


def _validate_active_root(
    active_link: Path,
    releases_root: Path,
    expected_commit: str | None = None,
) -> tuple[Path, str, str]:
    if not active_link.is_symlink():
        raise ActiveFrontendExportError(
            "current www Active is legacy/non-content-addressed; "
            "complete the first fixed Active release before syncing intl"
        )
    try:
        root = active_link.resolve(strict=True)
        relative = root.relative_to(releases_root)
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        raise ActiveFrontendExportError(
            "current www Active is legacy/non-content-addressed; "
            "complete the first fixed Active release before syncing intl"
        ) from exc
    if len(relative.parts) != 2:
        raise ActiveFrontendExportError("Active release root identity is invalid")
    commit, archive_sha256 = relative.parts
    if (
        not COMMIT_PATTERN.fullmatch(commit)
        or not SHA256_PATTERN.fullmatch(archive_sha256)
        or (expected_commit is not None and commit != expected_commit)
    ):
        raise ActiveFrontendExportError("Active release root identity is invalid")
    metadata = root.lstat()
    if root.is_symlink() or not stat.S_ISDIR(metadata.st_mode):
        raise ActiveFrontendExportError("Active release root must be a real directory")
    return root, commit, archive_sha256


def _verify_runtime_seal(
    root: Path,
    runtime_seal: Path,
    source_seal_helper: Path,
    identity: dict[str, Any],
) -> None:
    helper_metadata = source_seal_helper.lstat()
    if source_seal_helper.is_symlink() or not stat.S_ISREG(helper_metadata.st_mode):
        raise ActiveFrontendExportError("trusted source-seal helper is unsafe")
    command = [
        sys.executable,
        "-B",
        str(source_seal_helper),
        "verify",
        "--profile",
        "runtime",
        "--root",
        str(root),
        "--manifest",
        str(runtime_seal),
        "--commit",
        str(identity["commit"]),
        "--archive-sha256",
        str(identity["archiveSha256"]),
        "--frontend-identity",
        str(identity["frontendIdentity"]),
        "--frontend-checksum",
        str(identity["frontendChecksum"]),
    ]
    try:
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=300,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ActiveFrontendExportError("Active runtime seal verification failed") from exc
    if result.returncode != 0:
        raise ActiveFrontendExportError("Active runtime seal verification failed")


def inspect_active(
    *,
    source_seal_helper: Path,
    expected_commit: str | None = None,
    active_link: Path = ACTIVE_LINK,
    releases_root: Path = RELEASES_ROOT,
    active_slot_file: Path = ACTIVE_SLOT_FILE,
    slot_env_root: Path = SLOT_ENV_ROOT,
) -> dict[str, Any]:
    if expected_commit is not None and not COMMIT_PATTERN.fullmatch(expected_commit):
        raise ActiveFrontendExportError("expected Active commit must be a full git SHA")
    root, active_commit, archive_sha256 = _validate_active_root(
        active_link,
        releases_root,
        expected_commit,
    )
    runtime_seal = root / ".jato-runtime-seal.json"
    runtime_payload, runtime_raw = _read_regular_json(runtime_seal, "Active runtime seal")
    identity = _required_mapping(runtime_payload, "releaseIdentity", "runtime seal")
    if set(identity) != {
        "commit",
        "archiveSha256",
        "frontendIdentity",
        "frontendChecksum",
    }:
        raise ActiveFrontendExportError("Active runtime seal identity is invalid")
    if (
        identity.get("commit") != active_commit
        or identity.get("archiveSha256") != archive_sha256
        or not isinstance(identity.get("frontendIdentity"), str)
        or not identity["frontendIdentity"]
        or not isinstance(identity.get("frontendChecksum"), str)
        or not SHA256_PATTERN.fullmatch(identity["frontendChecksum"])
    ):
        raise ActiveFrontendExportError("Active runtime seal identity is invalid")
    _verify_runtime_seal(root, runtime_seal, source_seal_helper, identity)

    slot_raw = _read_regular_bytes(active_slot_file, "Active slot anchor", 32)
    try:
        active_slot = slot_raw.decode("ascii").strip()
    except UnicodeError as exc:
        raise ActiveFrontendExportError("Active slot anchor is invalid") from exc
    if active_slot not in {"8000", "8001"}:
        raise ActiveFrontendExportError("Active slot anchor is invalid")
    slot_link = releases_root.parent / "slots" / active_slot / "current"
    if not slot_link.is_symlink() or slot_link.resolve(strict=True) != root:
        raise ActiveFrontendExportError("Active slot current link does not match Active root")
    slot_env = slot_env_root / f"{active_slot}.env"
    if _env_value(slot_env, "APP_RELEASE_SLOT") != active_slot:
        raise ActiveFrontendExportError("Active slot environment slot is inconsistent")
    if _env_value(slot_env, "APP_RELEASE_SHA") != active_commit:
        raise ActiveFrontendExportError("Active slot environment commit is inconsistent")

    release_dir = root / "hermes/frontend_release"
    manifest, manifest_raw = _read_regular_json(
        release_dir / "frontend-release.json",
        "Active frontend manifest",
    )
    payload_path = release_dir / "frontend-dist.tar.gz"
    _, payload_sha256, payload_bytes = _read_regular_snapshot(
        payload_path,
        "Active frontend payload",
        2 * 1024 * 1024 * 1024,
        retain_bytes=False,
    )
    artifact = _required_mapping(manifest, "artifact", "frontend manifest")
    source = _required_mapping(manifest, "source", "frontend manifest")
    if (
        source.get("githubSha") != active_commit
        or source.get("deployCommit") != active_commit
        or artifact.get("id") != identity["frontendIdentity"]
        or artifact.get("checksum") != identity["frontendChecksum"]
        or artifact.get("checksum") != payload_sha256
        or artifact.get("payloadBytes") != payload_bytes
        or artifact.get("payload") != "frontend-dist.tar.gz"
    ):
        raise ActiveFrontendExportError("Active frontend manifest/runtime identity mismatch")
    deploy_release, _ = _read_regular_json(
        root / "hermes/deploy_release.json",
        "Active deploy metadata",
    )
    enriched_manifest = deploy_release.get("frontendRelease")
    if not isinstance(enriched_manifest, dict):
        raise ActiveFrontendExportError("Active deploy metadata identity mismatch")
    enriched_artifact = enriched_manifest.get("artifact")
    if not isinstance(enriched_artifact, dict):
        raise ActiveFrontendExportError("Active deploy metadata identity mismatch")
    github_artifact_id = enriched_artifact.get("githubId")
    github_artifact_digest = enriched_artifact.get("githubDigest")
    base_enriched_manifest = dict(enriched_manifest)
    base_enriched_artifact = dict(enriched_artifact)
    base_enriched_artifact.pop("githubId", None)
    base_enriched_artifact.pop("githubDigest", None)
    base_enriched_manifest["artifact"] = base_enriched_artifact
    if (
        deploy_release.get("expectedCommitSha") != active_commit
        or deploy_release.get("actualCommitSha") != active_commit
        or deploy_release.get("commitSha") != active_commit
        or base_enriched_manifest != manifest
        or not isinstance(github_artifact_id, str)
        or not github_artifact_id.isdigit()
        or int(github_artifact_id) <= 0
        or not isinstance(github_artifact_digest, str)
        or re.fullmatch(r"sha256:[0-9a-f]{64}", github_artifact_digest) is None
    ):
        raise ActiveFrontendExportError("Active deploy metadata identity mismatch")

    return {
        "schemaVersion": 1,
        "activeRoot": str(root),
        "activeSlot": int(active_slot),
        "commitSha": active_commit,
        "archiveSha256": archive_sha256,
        "runtimeSealSha256": hashlib.sha256(runtime_raw).hexdigest(),
        "frontendIdentity": identity["frontendIdentity"],
        "frontendChecksum": identity["frontendChecksum"],
        "frontendManifestSha256": hashlib.sha256(manifest_raw).hexdigest(),
        "frontendPayloadSha256": payload_sha256,
        "frontendPayloadBytes": payload_bytes,
        "githubArtifactId": github_artifact_id,
        "githubArtifactDigest": github_artifact_digest,
    }


def _validate_proof(
    payload: dict[str, Any],
    expected_commit: str | None = None,
) -> dict[str, Any]:
    expected_keys = {
        "schemaVersion",
        "activeRoot",
        "activeSlot",
        "commitSha",
        "archiveSha256",
        "runtimeSealSha256",
        "frontendIdentity",
        "frontendChecksum",
        "frontendManifestSha256",
        "frontendPayloadSha256",
        "frontendPayloadBytes",
        "githubArtifactId",
        "githubArtifactDigest",
    }
    if set(payload) != expected_keys or payload.get("schemaVersion") != 1:
        raise ActiveFrontendExportError("Active export proof schema is invalid")
    commit_sha = payload.get("commitSha")
    archive_sha256 = payload.get("archiveSha256")
    expected_root = f"/opt/jato/releases/{commit_sha}/{archive_sha256}"
    if (
        not isinstance(commit_sha, str)
        or not COMMIT_PATTERN.fullmatch(commit_sha)
        or (expected_commit is not None and commit_sha != expected_commit)
        or not isinstance(archive_sha256, str)
        or not SHA256_PATTERN.fullmatch(archive_sha256)
        or payload.get("activeRoot") != expected_root
        or payload.get("activeSlot") not in {8000, 8001}
        or not isinstance(payload.get("frontendIdentity"), str)
        or not payload["frontendIdentity"]
        or any(
            not isinstance(payload.get(key), str)
            or not SHA256_PATTERN.fullmatch(payload[key])
            for key in (
                "runtimeSealSha256",
                "frontendChecksum",
                "frontendManifestSha256",
                "frontendPayloadSha256",
            )
        )
        or isinstance(payload.get("frontendPayloadBytes"), bool)
        or not isinstance(payload.get("frontendPayloadBytes"), int)
        or payload["frontendPayloadBytes"] <= 0
        or not isinstance(payload.get("githubArtifactId"), str)
        or not payload["githubArtifactId"].isdigit()
        or int(payload["githubArtifactId"]) <= 0
        or not isinstance(payload.get("githubArtifactDigest"), str)
        or re.fullmatch(
            r"sha256:[0-9a-f]{64}",
            payload["githubArtifactDigest"],
        )
        is None
    ):
        raise ActiveFrontendExportError("Active export proof identity is invalid")
    return payload


def _write_env(path: Path, values: dict[str, str]) -> None:
    if path.is_symlink():
        raise ActiveFrontendExportError("Active export env path is unsafe")
    path.write_text(
        "".join(f"{key}={shlex.quote(value)}\n" for key, value in values.items()),
        encoding="utf-8",
    )
    os.chmod(path, 0o600)


def proof_env(
    proof_path: Path,
    env_output: Path,
    expected_commit: str | None = None,
) -> None:
    proof, _ = _read_regular_json(proof_path, "Active export proof")
    validated = _validate_proof(proof, expected_commit)
    _write_env(
        env_output,
        {
            "ACTIVE_ROOT": validated["activeRoot"],
            "ACTIVE_COMMIT_SHA": validated["commitSha"],
            "ACTIVE_ARCHIVE_SHA256": validated["archiveSha256"],
        },
    )


def verify_download(
    proof_path: Path,
    release_dir: Path,
    env_output: Path,
    expected_commit: str | None = None,
) -> None:
    proof, _ = _read_regular_json(proof_path, "Active export proof")
    validated = _validate_proof(proof, expected_commit)
    manifest, manifest_raw = _read_regular_json(
        release_dir / "frontend-release.json",
        "downloaded frontend manifest",
    )
    payload_path = release_dir / "frontend-dist.tar.gz"
    _, payload_sha256, payload_bytes = _read_regular_snapshot(
        payload_path,
        "downloaded frontend payload",
        2 * 1024 * 1024 * 1024,
        retain_bytes=False,
    )
    if (
        hashlib.sha256(manifest_raw).hexdigest()
        != validated["frontendManifestSha256"]
        or payload_sha256 != validated["frontendPayloadSha256"]
        or payload_bytes != validated["frontendPayloadBytes"]
    ):
        raise ActiveFrontendExportError("downloaded frontend artifact/proof mismatch")
    release = _required_mapping(manifest, "release", "frontend manifest")
    source = _required_mapping(manifest, "source", "frontend manifest")
    artifact = _required_mapping(manifest, "artifact", "frontend manifest")
    frontend = _required_mapping(manifest, "frontend", "frontend manifest")
    github_id = validated["githubArtifactId"]
    github_digest = validated["githubArtifactDigest"]
    values: dict[str, Any] = {
        "ACTIVE_COMMIT_SHA": validated["commitSha"],
        "ACTIVE_ARCHIVE_SHA256": validated["archiveSha256"],
        "ACTIVE_ROOT": validated["activeRoot"],
        "ARTIFACT_NAME": artifact.get("name"),
        "ARTIFACT_IDENTITY": artifact.get("id"),
        "ARTIFACT_CHECKSUM": artifact.get("checksum"),
        "GITHUB_ARTIFACT_ID": github_id,
        "GITHUB_ARTIFACT_DIGEST": github_digest,
        "FRONTEND_BUILD_ID": frontend.get("buildId"),
        "NODE_VERSION": frontend.get("nodeVersion"),
        "BUILD_RUN_ID": release.get("workflowRunId"),
        "BUILD_RUN_ATTEMPT": release.get("workflowRunAttempt"),
    }
    if (
        source.get("githubSha") != validated["commitSha"]
        or source.get("deployCommit") != validated["commitSha"]
        or values["ARTIFACT_NAME"] != f"frontend-dist-{validated['commitSha']}"
        or values["ARTIFACT_IDENTITY"] != validated["frontendIdentity"]
        or values["ARTIFACT_CHECKSUM"] != validated["frontendChecksum"]
        or not isinstance(github_id, str)
        or not github_id.isdigit()
        or int(github_id) <= 0
        or not isinstance(github_digest, str)
        or re.fullmatch(r"sha256:[0-9a-f]{64}", github_digest) is None
        or not isinstance(values["FRONTEND_BUILD_ID"], str)
        or not SHA256_PATTERN.fullmatch(values["FRONTEND_BUILD_ID"])
        or not isinstance(values["NODE_VERSION"], str)
        or re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+", values["NODE_VERSION"])
        is None
        or not isinstance(values["BUILD_RUN_ID"], str)
        or not values["BUILD_RUN_ID"].isdigit()
        or int(values["BUILD_RUN_ID"]) <= 0
        or not isinstance(values["BUILD_RUN_ATTEMPT"], str)
        or not values["BUILD_RUN_ATTEMPT"].isdigit()
        or int(values["BUILD_RUN_ATTEMPT"]) <= 0
    ):
        raise ActiveFrontendExportError("downloaded frontend manifest identity is invalid")
    _write_env(env_output, {key: str(value) for key, value in values.items()})


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    inspect = commands.add_parser("inspect")
    inspect.add_argument("--expected-commit")
    inspect.add_argument("--source-seal-helper", type=Path, required=True)
    read_proof = commands.add_parser("proof-env")
    read_proof.add_argument("--proof", type=Path, required=True)
    read_proof.add_argument("--expected-commit")
    read_proof.add_argument("--env-output", type=Path, required=True)
    verify = commands.add_parser("verify-download")
    verify.add_argument("--proof", type=Path, required=True)
    verify.add_argument("--release-dir", type=Path, required=True)
    verify.add_argument("--expected-commit")
    verify.add_argument("--env-output", type=Path, required=True)
    return parser


def main() -> int:
    arguments = _parser().parse_args()
    try:
        if arguments.command == "inspect":
            payload = inspect_active(
                source_seal_helper=arguments.source_seal_helper,
                expected_commit=arguments.expected_commit,
            )
            print(json.dumps(payload, sort_keys=True))
        elif arguments.command == "proof-env":
            proof_env(
                arguments.proof,
                arguments.env_output,
                arguments.expected_commit,
            )
        else:
            verify_download(
                arguments.proof,
                arguments.release_dir,
                arguments.env_output,
                arguments.expected_commit,
            )
    except (ActiveFrontendExportError, OSError, UnicodeError) as exc:
        print(f"Active frontend export rejected: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
