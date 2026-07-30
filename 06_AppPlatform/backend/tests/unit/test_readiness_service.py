from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import health as health_routes
from app.services import readiness_service
from app.services.readiness_service import (
    ReadinessSettings,
    RuntimeReleaseIdentity,
    build_readiness_report,
    resolve_runtime_release_identity,
)

SHA = "a" * 40
OTHER_SHA = "b" * 40


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_release(
    root: Path,
    *,
    expected_sha: str = SHA,
    actual_sha: str = SHA,
) -> Path:
    release_path = root / "hermes" / "deploy_release.json"
    _write_json(
        release_path,
        {
            "expectedCommitSha": expected_sha,
            "actualCommitSha": actual_sha,
        },
    )
    return release_path


def _write_parquet(path: Path, values: list[int] | None = None) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "value": pa.array(
                [1] if values is None else values,
                type=pa.int64(),
            )
        }
    )
    pq.write_table(table, path)
    return path


def _write_partitioned_dataset(root: Path) -> Path:
    dataset_path = root / "active"
    partition_name = "country=SE"
    _write_json(
        dataset_path / "manifest.json",
        {
            "manifestSchemaVersion": "1.1",
            "parquetFileCount": 1,
            "partitionDirectories": [partition_name],
        },
    )
    parquet_path = dataset_path / partition_name / "part-0.parquet"
    _write_parquet(parquet_path)
    return dataset_path


def _settings(
    root: Path,
    *,
    allow_release_metadata_fallback: bool = False,
) -> ReadinessSettings:
    return ReadinessSettings(
        release_metadata_path=root / "hermes" / "deploy_release.json",
        partitioned_path=root / "active",
        parquet_path=root / "active.parquet",
        allow_release_metadata_fallback=allow_release_metadata_fallback,
    )


def _runtime(commit_sha: str = SHA) -> RuntimeReleaseIdentity:
    return RuntimeReleaseIdentity(
        commit_sha=commit_sha,
        provenance="test_runtime",
    )


def test_build_readiness_report_returns_release_and_dataset_provenance(
    tmp_path: Path,
) -> None:
    _write_release(tmp_path)
    _write_partitioned_dataset(tmp_path)

    report = build_readiness_report(
        _settings(tmp_path),
        runtime_release=_runtime(),
    )

    assert report["status"] == "ready"
    assert report["release"] == {
        "commitSha": SHA,
        "expectedCommitSha": SHA,
        "provenance": "test_runtime",
        "metadataProvenance": "deploy_release_file",
    }
    assert report["checks"]["release"]["code"] == "release_metadata_valid"
    assert (
        report["checks"]["activeDataset"]["code"]
        == "partitioned_dataset_readable"
    )
    assert report["failures"] == []


def test_release_commit_sha_supports_packaged_metadata_shape(
    tmp_path: Path,
) -> None:
    _write_json(
        tmp_path / "hermes" / "deploy_release.json",
        {
            "expectedCommitSha": SHA,
            "commitSha": SHA,
        },
    )
    _write_partitioned_dataset(tmp_path)

    report = build_readiness_report(
        _settings(tmp_path),
        runtime_release=_runtime(),
    )

    assert report["status"] == "ready"
    assert report["release"]["commitSha"] == SHA
    assert report["release"]["expectedCommitSha"] == SHA


def test_missing_release_metadata_fails_closed_by_default(tmp_path: Path) -> None:
    _write_partitioned_dataset(tmp_path)
    provider_called = False

    def fallback_provider() -> dict:
        nonlocal provider_called
        provider_called = True
        return {
            "commitSha": SHA,
            "actualCommitSha": SHA,
            "expectedCommitSha": SHA,
            "source": "environment",
        }

    report = build_readiness_report(
        _settings(tmp_path),
        release_metadata_provider=fallback_provider,
        runtime_release=_runtime(),
    )

    assert report["status"] == "not_ready"
    assert report["failures"] == [
        {"check": "release", "code": "release_metadata_unavailable"}
    ]
    assert provider_called is False


def test_release_fallback_requires_explicit_injected_mode(tmp_path: Path) -> None:
    _write_partitioned_dataset(tmp_path)

    report = build_readiness_report(
        _settings(tmp_path, allow_release_metadata_fallback=True),
        release_metadata_provider=lambda: {
            "commitSha": SHA,
            "actualCommitSha": SHA,
            "expectedCommitSha": SHA,
            "source": "environment",
        },
        runtime_release=_runtime(),
    )

    assert report["status"] == "ready"
    assert report["release"]["commitSha"] == SHA
    assert report["release"]["provenance"] == "test_runtime"
    assert report["release"]["metadataProvenance"] == "environment"


def test_runtime_release_mismatch_is_not_ready(tmp_path: Path) -> None:
    _write_release(tmp_path, expected_sha=OTHER_SHA, actual_sha=OTHER_SHA)
    _write_partitioned_dataset(tmp_path)

    report = build_readiness_report(
        _settings(tmp_path),
        runtime_release=_runtime(SHA),
    )

    assert report["status"] == "not_ready"
    assert report["failures"] == [
        {"check": "release", "code": "runtime_release_mismatch"}
    ]
    assert report["release"]["commitSha"] == SHA
    assert report["release"]["expectedCommitSha"] == OTHER_SHA


def test_release_expected_commit_requires_exact_full_sha(
    tmp_path: Path,
) -> None:
    _write_release(
        tmp_path,
        expected_sha=f"{SHA}0",
        actual_sha=SHA,
    )
    _write_partitioned_dataset(tmp_path)

    report = build_readiness_report(
        _settings(tmp_path),
        runtime_release=_runtime(SHA),
    )

    assert report["status"] == "not_ready"
    assert report["failures"] == [
        {"check": "release", "code": "release_expected_commit_invalid"}
    ]


def test_mutable_metadata_actual_commit_is_not_runtime_identity(
    tmp_path: Path,
) -> None:
    _write_release(
        tmp_path,
        expected_sha=SHA,
        actual_sha=OTHER_SHA,
    )
    _write_partitioned_dataset(tmp_path)

    report = build_readiness_report(
        _settings(tmp_path),
        runtime_release=_runtime(SHA),
    )

    assert report["status"] == "ready"
    assert report["release"]["commitSha"] == SHA
    assert report["release"]["expectedCommitSha"] == SHA


def test_running_process_identity_does_not_follow_new_metadata(
    tmp_path: Path,
) -> None:
    runtime = _runtime(SHA)
    _write_release(tmp_path, expected_sha=SHA, actual_sha=SHA)
    _write_partitioned_dataset(tmp_path)

    first_report = build_readiness_report(
        _settings(tmp_path),
        runtime_release=runtime,
    )
    _write_release(
        tmp_path,
        expected_sha=OTHER_SHA,
        actual_sha=OTHER_SHA,
    )
    second_report = build_readiness_report(
        _settings(tmp_path),
        runtime_release=runtime,
    )

    assert first_report["status"] == "ready"
    assert second_report["status"] == "not_ready"
    assert second_report["release"]["commitSha"] == SHA
    assert second_report["release"]["expectedCommitSha"] == OTHER_SHA
    assert second_report["failures"] == [
        {"check": "release", "code": "runtime_release_mismatch"}
    ]


def test_runtime_identity_prefers_release_env_without_calling_git() -> None:
    git_called = False

    def git_provider(_project_root: Path) -> str:
        nonlocal git_called
        git_called = True
        return OTHER_SHA

    identity = resolve_runtime_release_identity(
        environ={
            "APP_RELEASE_SHA": SHA,
            "APP_GIT_SHA": OTHER_SHA,
        },
        git_commit_provider=git_provider,
    )

    assert identity == RuntimeReleaseIdentity(
        commit_sha=SHA,
        provenance="APP_RELEASE_SHA",
    )
    assert git_called is False


def test_runtime_identity_uses_git_once_when_env_is_absent(
    tmp_path: Path,
) -> None:
    git_calls = 0

    def git_provider(project_root: Path) -> str:
        nonlocal git_calls
        git_calls += 1
        assert project_root == tmp_path
        return SHA

    identity = resolve_runtime_release_identity(
        environ={},
        project_root=tmp_path,
        git_commit_provider=git_provider,
    )

    assert identity == RuntimeReleaseIdentity(
        commit_sha=SHA,
        provenance="git_worktree",
    )
    assert git_calls == 1


def test_partition_manifest_must_reference_existing_partition(
    tmp_path: Path,
) -> None:
    _write_release(tmp_path)
    _write_json(
        tmp_path / "active" / "manifest.json",
        {
            "parquetFileCount": 1,
            "partitionDirectories": ["country=missing"],
        },
    )

    report = build_readiness_report(
        _settings(tmp_path),
        runtime_release=_runtime(),
    )

    assert report["status"] == "not_ready"
    assert report["failures"] == [
        {"check": "activeDataset", "code": "dataset_partition_missing"}
    ]


def test_declared_partition_must_contain_a_parquet_file(
    tmp_path: Path,
) -> None:
    _write_release(tmp_path)
    partition = tmp_path / "active" / "country=SE"
    partition.mkdir(parents=True)
    _write_json(
        tmp_path / "active" / "manifest.json",
        {
            "parquetFileCount": 1,
            "partitionDirectories": ["country=SE"],
        },
    )

    report = build_readiness_report(
        _settings(tmp_path),
        runtime_release=_runtime(),
    )

    assert report["status"] == "not_ready"
    assert report["failures"] == [
        {"check": "activeDataset", "code": "dataset_partition_empty"}
    ]


def test_manifest_parquet_count_must_match_enumerated_files(
    tmp_path: Path,
) -> None:
    _write_release(tmp_path)
    partition = tmp_path / "active" / "country=SE"
    _write_parquet(partition / "part-0.parquet")
    _write_parquet(partition / "part-1.parquet")
    _write_json(
        tmp_path / "active" / "manifest.json",
        {
            "parquetFileCount": 1,
            "partitionDirectories": ["country=SE"],
        },
    )

    report = build_readiness_report(
        _settings(tmp_path),
        runtime_release=_runtime(),
    )

    assert report["status"] == "not_ready"
    assert report["failures"] == [
        {
            "check": "activeDataset",
            "code": "dataset_parquet_count_mismatch",
        }
    ]


def test_manifest_partition_limit_fails_before_path_enumeration(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        readiness_service,
        "_MAX_PARTITION_DIRECTORIES",
        1,
    )
    _write_release(tmp_path)
    _write_json(
        tmp_path / "active" / "manifest.json",
        {
            "parquetFileCount": 1,
            "partitionDirectories": ["country=SE", "country=DK"],
        },
    )

    report = build_readiness_report(
        _settings(tmp_path),
        runtime_release=_runtime(),
    )

    assert report["status"] == "not_ready"
    assert report["failures"] == [
        {
            "check": "activeDataset",
            "code": "dataset_manifest_limit_exceeded",
        }
    ]


def test_partitioned_dataset_accepts_current_production_file_count(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write_release(tmp_path)
    partition_names = [f"country={index:02d}" for index in range(21)]
    parquet_file_count = 264
    for index in range(parquet_file_count):
        parquet_path = (
            tmp_path
            / "active"
            / partition_names[index % len(partition_names)]
            / f"part-{index:03d}.parquet"
        )
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        parquet_path.touch()
    _write_json(
        tmp_path / "active" / "manifest.json",
        {
            "parquetFileCount": parquet_file_count,
            "partitionDirectories": partition_names,
        },
    )
    monkeypatch.setattr(
        readiness_service,
        "_read_parquet_shape",
        lambda _path: (1, 1),
    )

    report = build_readiness_report(
        _settings(tmp_path),
        runtime_release=_runtime(),
    )

    assert report["status"] == "ready"
    assert report["checks"]["activeDataset"] == {
        "status": "ok",
        "code": "partitioned_dataset_readable",
        "source": "partitioned",
        "partitionCount": 21,
        "parquetFileCount": parquet_file_count,
    }


def test_default_parquet_file_limit_accepts_512_and_rejects_513(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write_release(tmp_path)
    partition_name = "country=SE"
    partition = tmp_path / "active" / partition_name
    partition.mkdir(parents=True)
    for index in range(512):
        (partition / f"part-{index:03d}.parquet").touch()
    manifest_path = tmp_path / "active" / "manifest.json"
    _write_json(
        manifest_path,
        {
            "parquetFileCount": 512,
            "partitionDirectories": [partition_name],
        },
    )
    monkeypatch.setattr(
        readiness_service,
        "_read_parquet_shape",
        lambda _path: (1, 1),
    )

    accepted = build_readiness_report(
        _settings(tmp_path),
        runtime_release=_runtime(),
    )

    assert accepted["status"] == "ready"

    (partition / "part-512.parquet").touch()
    _write_json(
        manifest_path,
        {
            "parquetFileCount": 513,
            "partitionDirectories": [partition_name],
        },
    )

    rejected = build_readiness_report(
        _settings(tmp_path),
        runtime_release=_runtime(),
    )

    assert rejected["status"] == "not_ready"
    assert rejected["failures"] == [
        {
            "check": "activeDataset",
            "code": "dataset_manifest_limit_exceeded",
        }
    ]


def test_manifest_parquet_limit_fails_before_file_enumeration(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(readiness_service, "_MAX_PARQUET_FILES", 1)
    _write_release(tmp_path)
    _write_json(
        tmp_path / "active" / "manifest.json",
        {
            "parquetFileCount": 2,
            "partitionDirectories": ["country=SE"],
        },
    )

    report = build_readiness_report(
        _settings(tmp_path),
        runtime_release=_runtime(),
    )

    assert report["status"] == "not_ready"
    assert report["failures"] == [
        {
            "check": "activeDataset",
            "code": "dataset_manifest_limit_exceeded",
        }
    ]


def test_actual_parquet_enumeration_stops_at_hard_limit(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(readiness_service, "_MAX_PARQUET_FILES", 1)
    _write_release(tmp_path)
    partition = tmp_path / "active" / "country=SE"
    _write_parquet(partition / "part-0.parquet")
    _write_parquet(partition / "part-1.parquet")
    _write_json(
        tmp_path / "active" / "manifest.json",
        {
            "parquetFileCount": 1,
            "partitionDirectories": ["country=SE"],
        },
    )

    report = build_readiness_report(
        _settings(tmp_path),
        runtime_release=_runtime(),
    )

    assert report["status"] == "not_ready"
    assert report["failures"] == [
        {
            "check": "activeDataset",
            "code": "dataset_parquet_limit_exceeded",
        }
    ]


def test_dataset_entry_walk_stops_on_non_parquet_entry_limit(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(readiness_service, "_MAX_DATASET_ENTRIES", 2)
    _write_release(tmp_path)
    partition = tmp_path / "active" / "country=SE"
    _write_parquet(partition / "part-0.parquet")
    _write_json(
        tmp_path / "active" / "manifest.json",
        {
            "parquetFileCount": 1,
            "partitionDirectories": ["country=SE"],
        },
    )

    report = build_readiness_report(
        _settings(tmp_path),
        runtime_release=_runtime(),
    )

    assert report["status"] == "not_ready"
    assert report["failures"] == [
        {
            "check": "activeDataset",
            "code": "dataset_entry_limit_exceeded",
        }
    ]


def test_every_partitioned_parquet_footer_must_be_readable(
    tmp_path: Path,
) -> None:
    _write_release(tmp_path)
    partition = tmp_path / "active" / "country=SE"
    _write_parquet(partition / "part-0.parquet")
    corrupt_path = partition / "part-1.parquet"
    corrupt_path.write_bytes(b"PAR1not-a-real-footerPAR1")
    _write_json(
        tmp_path / "active" / "manifest.json",
        {
            "parquetFileCount": 2,
            "partitionDirectories": ["country=SE"],
        },
    )

    report = build_readiness_report(
        _settings(tmp_path),
        runtime_release=_runtime(),
    )

    assert report["status"] == "not_ready"
    assert report["failures"] == [
        {"check": "activeDataset", "code": "dataset_parquet_unreadable"}
    ]


def test_declared_oversized_footer_is_rejected_before_pyarrow(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write_release(tmp_path)
    partition = tmp_path / "active" / "country=SE"
    parquet_path = partition / "part-0.parquet"
    parquet_path.parent.mkdir(parents=True)
    oversized_footer = (
        readiness_service._MAX_PARQUET_FOOTER_METADATA_BYTES + 1
    )
    parquet_path.write_bytes(
        b"PAR1payload"
        + oversized_footer.to_bytes(4, byteorder="little")
        + b"PAR1"
    )
    _write_json(
        tmp_path / "active" / "manifest.json",
        {
            "parquetFileCount": 1,
            "partitionDirectories": ["country=SE"],
        },
    )
    pyarrow_called = False

    def fail_if_called(_path: Path):
        nonlocal pyarrow_called
        pyarrow_called = True
        raise AssertionError("PyArrow must not open an oversized footer")

    monkeypatch.setattr(readiness_service.pq, "ParquetFile", fail_if_called)

    report = build_readiness_report(
        _settings(tmp_path),
        runtime_release=_runtime(),
    )

    assert report["status"] == "not_ready"
    assert report["failures"] == [
        {"check": "activeDataset", "code": "dataset_parquet_unreadable"}
    ]
    assert pyarrow_called is False


def test_zero_row_partitioned_parquet_is_not_ready(tmp_path: Path) -> None:
    _write_release(tmp_path)
    partition = tmp_path / "active" / "country=SE"
    _write_parquet(partition / "part-0.parquet", values=[])
    _write_json(
        tmp_path / "active" / "manifest.json",
        {
            "parquetFileCount": 1,
            "partitionDirectories": ["country=SE"],
        },
    )

    report = build_readiness_report(
        _settings(tmp_path),
        runtime_release=_runtime(),
    )

    assert report["status"] == "not_ready"
    assert report["failures"] == [
        {"check": "activeDataset", "code": "dataset_parquet_empty"}
    ]


def test_empty_partition_directory_cannot_fall_back_to_dataset_root(
    tmp_path: Path,
) -> None:
    _write_release(tmp_path)
    _write_json(
        tmp_path / "active" / "manifest.json",
        {
            "parquetFileCount": 1,
            "partitionDirectories": [""],
        },
    )
    _write_parquet(tmp_path / "active" / "part-0.parquet")

    report = build_readiness_report(
        _settings(tmp_path),
        runtime_release=_runtime(),
    )

    assert report["status"] == "not_ready"
    assert report["failures"] == [
        {"check": "activeDataset", "code": "dataset_partition_missing"}
    ]


def test_single_parquet_fallback_opens_real_footer_without_loading_rows(
    tmp_path: Path,
) -> None:
    _write_release(tmp_path)
    _write_parquet(tmp_path / "active.parquet")

    report = build_readiness_report(
        _settings(tmp_path),
        runtime_release=_runtime(),
    )

    assert report["status"] == "ready"
    assert report["checks"]["activeDataset"] == {
        "status": "ok",
        "code": "parquet_dataset_readable",
        "source": "parquet",
        "parquetFileCount": 1,
    }


def test_single_parquet_fallback_rejects_corrupt_footer(tmp_path: Path) -> None:
    _write_release(tmp_path)
    (tmp_path / "active.parquet").write_bytes(b"PAR1not-parquetPAR1")

    report = build_readiness_report(
        _settings(tmp_path),
        runtime_release=_runtime(),
    )

    assert report["status"] == "not_ready"
    assert report["failures"] == [
        {"check": "activeDataset", "code": "active_dataset_unreadable"}
    ]


def test_health_routes_keep_liveness_separate_from_readiness(
    monkeypatch,
) -> None:
    app = FastAPI()
    app.include_router(health_routes.router)
    client = TestClient(app)

    monkeypatch.setattr(
        health_routes,
        "build_readiness_report",
        lambda: {
            "status": "not_ready",
            "release": {"commitSha": ""},
            "checks": {},
            "failures": [{"check": "release", "code": "test_failure"}],
        },
    )

    health_response = client.get("/healthz")
    readiness_response = client.get("/readyz")

    assert health_response.status_code == 200
    assert health_response.json() == {"status": "ok"}
    assert readiness_response.status_code == 503
    assert readiness_response.json()["status"] == "not_ready"
    assert readiness_response.headers["cache-control"] == "no-store"
