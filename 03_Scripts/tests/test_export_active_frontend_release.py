from __future__ import annotations

import importlib.util
import json
import subprocess
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
HELPER_PATH = REPO_ROOT / "03_Scripts/deploy/export_active_frontend_release.py"


def _load_helper() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "export_active_frontend_release",
        HELPER_PATH,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load Active frontend export helper")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


HELPER = _load_helper()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _active_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, object]:
    commit = "a" * 40
    archive = "b" * 64
    checksum = "c" * 64
    identity = "gha://tristan419/JATO/actions/runs/42/attempts/3/artifacts/" + (
        f"frontend-dist-{commit}"
    )
    releases_root = tmp_path / "opt/jato/releases"
    root = releases_root / commit / archive
    release_dir = root / "hermes/frontend_release"
    release_dir.mkdir(parents=True)
    payload = release_dir / "frontend-dist.tar.gz"
    payload.write_bytes(b"immutable-frontend")
    checksum = HELPER._hash_file(payload)
    manifest = {
        "schemaVersion": 2,
        "release": {
            "releaseId": "42-3",
            "environment": "production",
            "repository": "tristan419/JATO",
            "workflow": "production-release",
            "workflowRunId": "42",
            "workflowRunAttempt": "3",
            "buildTimestamp": "2026-08-04T00:00:00+00:00",
        },
        "source": {
            "githubSha": commit,
            "appCommit": commit,
            "deployCommit": commit,
        },
        "artifact": {
            "name": f"frontend-dist-{commit}",
            "id": identity,
            "payload": "frontend-dist.tar.gz",
            "payloadBytes": payload.stat().st_size,
            "checksum": checksum,
        },
        "frontend": {
            "buildId": "e" * 64,
            "nodeVersion": "v20.19.0",
        },
    }
    _write_json(release_dir / "frontend-release.json", manifest)
    enriched_manifest = json.loads(json.dumps(manifest))
    enriched_manifest["artifact"]["githubId"] = "77"
    enriched_manifest["artifact"]["githubDigest"] = "sha256:" + "d" * 64
    runtime_seal = {
        "releaseIdentity": {
            "commit": commit,
            "archiveSha256": archive,
            "frontendIdentity": identity,
            "frontendChecksum": checksum,
        }
    }
    _write_json(root / ".jato-runtime-seal.json", runtime_seal)
    _write_json(
        root / "hermes/deploy_release.json",
        {
            "expectedCommitSha": commit,
            "actualCommitSha": commit,
            "commitSha": commit,
            "frontendRelease": enriched_manifest,
        },
    )
    active_link = tmp_path / "opt/jato/active"
    active_link.parent.mkdir(parents=True, exist_ok=True)
    active_link.symlink_to(root)
    slot_link = releases_root.parent / "slots/8000/current"
    slot_link.parent.mkdir(parents=True)
    slot_link.symlink_to(root)
    active_slot_file = tmp_path / "var/lib/jato-release/active-slot"
    active_slot_file.parent.mkdir(parents=True)
    active_slot_file.write_text("8000\n", encoding="ascii")
    slot_env_root = tmp_path / "etc/jato-fullstack/slots"
    slot_env_root.mkdir(parents=True)
    (slot_env_root / "8000.env").write_text(
        f"APP_RELEASE_SLOT=8000\nAPP_RELEASE_SHA={commit}\n",
        encoding="utf-8",
    )
    source_helper = tmp_path / "verify_release_source_seal.py"
    source_helper.write_text("# trusted\n", encoding="utf-8")
    monkeypatch.setattr(
        HELPER.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args[0], 0, "", ""),
    )
    proof = HELPER.inspect_active(
        expected_commit=commit,
        source_seal_helper=source_helper,
        active_link=active_link,
        releases_root=releases_root,
        active_slot_file=active_slot_file,
        slot_env_root=slot_env_root,
    )
    return {
        "commit": commit,
        "root": root,
        "release_dir": release_dir,
        "proof": proof,
    }


def test_inspect_and_verify_download_bind_current_active(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _active_fixture(tmp_path, monkeypatch)
    inspected = fixture["proof"]
    assert inspected["commitSha"] == fixture["commit"]
    assert inspected["activeRoot"] == str(fixture["root"])

    proof = dict(inspected)
    proof["activeRoot"] = (
        f"/opt/jato/releases/{proof['commitSha']}/{proof['archiveSha256']}"
    )
    proof_path = tmp_path / "proof.json"
    _write_json(proof_path, proof)
    env_path = tmp_path / "verified.env"

    HELPER.verify_download(
        proof_path,
        fixture["release_dir"],
        env_path,
    )

    env = env_path.read_text(encoding="utf-8")
    assert f"ACTIVE_COMMIT_SHA={fixture['commit']}\n" in env
    assert f"ACTIVE_ROOT={proof['activeRoot']}\n" in env
    assert "ARTIFACT_NAME=frontend-dist-" in env
    assert "GITHUB_ARTIFACT_ID=77\n" in env


def test_legacy_active_is_rejected(tmp_path: Path) -> None:
    active = tmp_path / "opt/JATO_Analysis_System-main"
    active.mkdir(parents=True)

    with pytest.raises(
        HELPER.ActiveFrontendExportError,
        match="legacy/non-content-addressed",
    ):
        HELPER._validate_active_root(active, tmp_path / "opt/jato/releases")


def test_verify_download_rejects_proof_path_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _active_fixture(tmp_path, monkeypatch)
    proof = dict(fixture["proof"])
    proof["activeRoot"] = "/opt/jato/releases/unsafe"
    proof_path = tmp_path / "bad-proof.json"
    _write_json(proof_path, proof)

    with pytest.raises(
        HELPER.ActiveFrontendExportError,
        match="proof identity is invalid",
    ):
        HELPER.verify_download(
            proof_path,
            fixture["release_dir"],
            tmp_path / "verified.env",
        )


def test_manifest_read_rejects_path_identity_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = tmp_path / "frontend-release.json"
    manifest.write_text('{"schemaVersion": 2}\n', encoding="utf-8")
    real_lstat = HELPER.os.lstat

    def drifted_lstat(path: Path) -> SimpleNamespace:
        metadata = real_lstat(path)
        return SimpleNamespace(
            st_dev=metadata.st_dev,
            st_ino=metadata.st_ino + 1,
            st_mode=metadata.st_mode,
        )

    monkeypatch.setattr(HELPER.os, "lstat", drifted_lstat)
    with pytest.raises(
        HELPER.ActiveFrontendExportError,
        match="changed while it was read",
    ):
        HELPER._read_regular_json(manifest, "downloaded frontend manifest")


def test_download_read_rejects_fd_identity_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = tmp_path / "frontend-dist.tar.gz"
    payload.write_bytes(b"immutable-frontend")
    real_fstat = HELPER.os.fstat
    calls = 0

    def drifted_fstat(descriptor: int) -> object:
        nonlocal calls
        calls += 1
        metadata = real_fstat(descriptor)
        if calls == 1:
            return metadata
        return SimpleNamespace(
            st_dev=metadata.st_dev,
            st_ino=metadata.st_ino,
            st_mode=metadata.st_mode,
            st_size=metadata.st_size,
            st_mtime_ns=metadata.st_mtime_ns + 1,
            st_ctime_ns=metadata.st_ctime_ns,
        )

    monkeypatch.setattr(HELPER.os, "fstat", drifted_fstat)
    with pytest.raises(
        HELPER.ActiveFrontendExportError,
        match="changed while it was read",
    ):
        HELPER._read_regular_snapshot(
            payload,
            "downloaded frontend payload",
            1024,
            retain_bytes=False,
        )
