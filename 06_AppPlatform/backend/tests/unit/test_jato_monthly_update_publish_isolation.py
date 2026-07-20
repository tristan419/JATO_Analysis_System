import json
from pathlib import Path
from typing import Any

import pytest
from fastapi import HTTPException

from app.services import jato_monthly_update_service


DIRECTORY_BUNDLE_KEYS = {"partition", "summaries"}


def _configure_project(tmp_path: Path, monkeypatch) -> tuple[Path, Path]:
    project_root = tmp_path / "project"
    job_root = (
        project_root
        / "04_Processed_data"
        / "ops"
        / "jato_monthly_update_jobs"
    )
    monkeypatch.setattr(jato_monthly_update_service, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(
        jato_monthly_update_service,
        "MONTHLY_UPDATE_JOB_ROOT",
        job_root,
    )
    return project_root, job_root


def _prepare_job_state(
    *,
    job_root: Path,
    job_id: str,
    status: str = "success",
    phase: str = "completed",
) -> dict[str, Any]:
    upload_path = job_root / job_id / "uploads" / "patch.xlsx"
    upload_path.parent.mkdir(parents=True, exist_ok=True)
    upload_path.write_bytes(b"test-upload")
    state = jato_monthly_update_service._prepare_initial_job_state(
        job_id=job_id,
        month="2026-06",
        triggered_by="tester",
        upload_filename=upload_path.name,
        stored_upload_path=upload_path,
    )
    state["status"] = status
    state["phase"] = phase
    return state


def _write_bundle_node(path: Path, key: str, marker: str) -> None:
    if key in DIRECTORY_BUNDLE_KEYS:
        path.mkdir(parents=True, exist_ok=True)
        (path / "marker.txt").write_text(marker, encoding="utf-8")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(marker, encoding="utf-8")


def _read_bundle_node(path: Path, key: str) -> str:
    if key in DIRECTORY_BUNDLE_KEYS:
        return (path / "marker.txt").read_text(encoding="utf-8")
    return path.read_text(encoding="utf-8")


def test_publish_request_only_queues_once_without_running_heavy_executor(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _project_root, job_root = _configure_project(tmp_path, monkeypatch)
    job_id = "jato-publish-queue-idempotent"
    state = _prepare_job_state(job_root=job_root, job_id=job_id)
    state["artifacts"]["stagingOutputPath"] = (
        "04_Processed_data/staging/2026-06-r1/candidate.parquet"
    )
    state["reviewApproval"] = {
        "decision": "approved",
        "activeBaseFingerprint": "active-lineage-v1",
    }
    jato_monthly_update_service._persist_job_state(state)

    launches: list[str] = []
    heavy_calls: list[str] = []
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_require_no_running_monthly_update_jobs",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_active_dataset_version",
        lambda: "active-lineage-v1",
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_launch_job_thread",
        lambda launched_job_id: launches.append(launched_job_id),
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_execute_publish_jato_monthly_update_job",
        lambda **_kwargs: heavy_calls.append("publish"),
    )

    first = jato_monthly_update_service.publish_jato_monthly_update_job(
        job_id=job_id,
        triggered_by="publisher",
    )
    replay = jato_monthly_update_service.publish_jato_monthly_update_job(
        job_id=job_id,
        triggered_by="publisher",
    )

    assert first["pendingOperation"]["status"] == "queued"
    assert first["pendingOperation"]["type"] == "publish"
    assert replay["pendingOperation"]["operationId"] == (
        first["pendingOperation"]["operationId"]
    )
    assert replay["pendingOperation"]["status"] == "queued"
    assert launches == [job_id, job_id]
    assert heavy_calls == []
    persisted = jato_monthly_update_service._load_job_state(job_id)
    assert persisted["pendingOperation"] == replay["pendingOperation"]
    assert "publication" not in persisted


@pytest.mark.parametrize(
    ("executor_outcome", "expected_status"),
    [("success", "success"), ("failure", "failed")],
)
def test_worker_dispatches_pending_publish_and_persists_terminal_status(
    tmp_path: Path,
    monkeypatch,
    executor_outcome: str,
    expected_status: str,
) -> None:
    _project_root, job_root = _configure_project(tmp_path, monkeypatch)
    job_id = f"jato-worker-publish-{executor_outcome}"
    state = _prepare_job_state(job_root=job_root, job_id=job_id)
    state["pendingOperation"] = {
        "operationId": f"publish-{executor_outcome}",
        "type": "publish",
        "status": "queued",
        "requestedAt": "2026-07-20T00:00:00+00:00",
        "requestedBy": "publisher",
        "startedAt": None,
        "finishedAt": None,
        "error": None,
        "failureDigest": None,
    }
    jato_monthly_update_service._persist_job_state(state)

    calls: list[tuple[str, str]] = []

    def execute_publish(*, job_id: str, triggered_by: str) -> dict[str, Any]:
        calls.append((job_id, triggered_by))
        if executor_outcome == "failure":
            raise RuntimeError("simulated publish failure")
        return {"jobId": job_id}

    monkeypatch.setattr(
        jato_monthly_update_service,
        "_execute_publish_jato_monthly_update_job",
        execute_publish,
    )

    worker_result = (
        jato_monthly_update_service.run_jato_monthly_update_worker_once()
    )

    assert worker_result["processedJobId"] == job_id
    assert calls == [(job_id, "publisher")]
    persisted = jato_monthly_update_service._load_job_state(job_id)
    operation = persisted["pendingOperation"]
    assert operation["status"] == expected_status
    assert operation["startedAt"]
    assert operation["finishedAt"]
    assert persisted["workerPid"] is None
    if executor_outcome == "success":
        assert operation["error"] is None
        assert operation["failureDigest"] is None
    else:
        assert operation["error"] == "simulated publish failure"
        assert operation["failureDigest"]["phase"] == "publish_running"


@pytest.mark.parametrize(
    "failure_call",
    range(1, len(jato_monthly_update_service.ACTIVE_BUNDLE_KEYS) * 2 + 1),
)
def test_active_bundle_swap_restores_every_old_path_after_any_replace_failure(
    tmp_path: Path,
    monkeypatch,
    failure_call: int,
) -> None:
    _project_root, _job_root = _configure_project(tmp_path, monkeypatch)
    active_paths = jato_monthly_update_service._active_data_paths()
    staging_root = tmp_path / "staged-bundle"
    staged_paths: dict[str, Path | None] = {}
    tracked_sources: set[Path] = set()

    for key in jato_monthly_update_service.ACTIVE_BUNDLE_KEYS:
        active_path = active_paths[key]
        staged_path = staging_root / active_path.name
        _write_bundle_node(active_path, key, f"old-{key}")
        _write_bundle_node(staged_path, key, f"new-{key}")
        staged_paths[key] = staged_path
        tracked_sources.add(active_path)
        tracked_sources.add(staged_path)

    backup_dir = active_paths["backupRoot"] / f"failure-{failure_call}"
    real_replace = jato_monthly_update_service.os.replace
    observed_calls = 0
    failure_raised = False

    def fail_one_bundle_replace(
        source: str | bytes | Path,
        destination: str | bytes | Path,
    ) -> None:
        nonlocal observed_calls, failure_raised
        source_path = Path(source)
        if source_path in tracked_sources:
            observed_calls += 1
            if observed_calls == failure_call:
                failure_raised = True
                raise OSError(f"simulated replace failure {failure_call}")
        real_replace(source, destination)

    monkeypatch.setattr(
        jato_monthly_update_service.os,
        "replace",
        fail_one_bundle_replace,
    )

    with pytest.raises(
        OSError,
        match=f"simulated replace failure {failure_call}",
    ):
        jato_monthly_update_service._swap_staged_active_bundle(
            staged_paths=staged_paths,
            active_paths=active_paths,
            backup_dir=backup_dir,
        )

    assert failure_raised is True
    for key in jato_monthly_update_service.ACTIVE_BUNDLE_KEYS:
        assert active_paths[key].exists()
        assert _read_bundle_node(active_paths[key], key) == f"old-{key}"
    assert not backup_dir.exists()


def test_recover_incomplete_switching_transaction_restores_old_active(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _project_root, _job_root = _configure_project(tmp_path, monkeypatch)
    active_paths = jato_monthly_update_service._active_data_paths()
    backup_dir = active_paths["backupRoot"] / "interrupted-publish"
    backup_dir.mkdir(parents=True, exist_ok=True)
    active_existed: dict[str, bool] = {}

    for key in jato_monthly_update_service.ACTIVE_BUNDLE_KEYS:
        active_path = active_paths[key]
        _write_bundle_node(active_path, key, f"new-{key}")
        existed_before_switch = key != "summaries"
        active_existed[key] = existed_before_switch
        if existed_before_switch:
            _write_bundle_node(
                backup_dir / active_path.name,
                key,
                f"old-{key}",
            )

    abandoned_staging_root = tmp_path / "abandoned-staging"
    abandoned_staging_root.mkdir(parents=True)
    journal_path = (
        backup_dir / jato_monthly_update_service.ACTIVE_TRANSACTION_FILENAME
    )
    journal_path.write_text(
        json.dumps(
            {
                "status": "switching",
                "activeExisted": active_existed,
                "processedKeys": list(
                    jato_monthly_update_service.ACTIVE_BUNDLE_KEYS
                ),
                "installedKeys": list(
                    jato_monthly_update_service.ACTIVE_BUNDLE_KEYS
                ),
                "backedUpKeys": [
                    key
                    for key in jato_monthly_update_service.ACTIVE_BUNDLE_KEYS
                    if active_existed[key]
                ],
                "stagingRoots": [str(abandoned_staging_root)],
            }
        ),
        encoding="utf-8",
    )

    recovered = (
        jato_monthly_update_service._recover_incomplete_active_transactions(
            active_paths
        )
    )

    assert recovered == [str(backup_dir)]
    for key in jato_monthly_update_service.ACTIVE_BUNDLE_KEYS:
        if key == "summaries":
            assert not active_paths[key].exists()
        else:
            assert _read_bundle_node(active_paths[key], key) == f"old-{key}"
    assert not abandoned_staging_root.exists()
    assert json.loads(journal_path.read_text(encoding="utf-8"))["status"] == (
        "recovered"
    )


def test_old_publish_cannot_rollback_after_active_lineage_advances(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _project_root, job_root = _configure_project(tmp_path, monkeypatch)
    job_id = "jato-publish-a"
    state = _prepare_job_state(job_root=job_root, job_id=job_id)
    state["publication"] = {
        "publishedAt": "2026-07-20T01:00:00+00:00",
        "publishedBy": "publisher-a",
        "activeFingerprintAfter": "active-lineage-a",
        "backupDir": "04_Processed_data/.refresh_backups/publish-a",
    }
    jato_monthly_update_service._persist_job_state(state)

    launches: list[str] = []
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_require_no_running_monthly_update_jobs",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_active_dataset_version",
        lambda: "active-lineage-b",
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_launch_job_thread",
        lambda launched_job_id: launches.append(launched_job_id),
    )

    with pytest.raises(HTTPException) as exc_info:
        jato_monthly_update_service.rollback_jato_monthly_update_job(
            job_id=job_id,
            triggered_by="operator",
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == {
        "blockerType": "rollback_target_stale",
        "message": (
            "当前 active 已不是该任务发布的版本，禁止用旧任务覆盖后续发布；"
            "请先处理最新一次发布。"
        ),
        "expectedActiveFingerprint": "active-lineage-a",
        "currentActiveFingerprint": "active-lineage-b",
    }
    assert launches == []
    persisted = jato_monthly_update_service._load_job_state(job_id)
    assert "pendingOperation" not in persisted


def test_recheck_rewakes_queued_job_worker(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _project_root, job_root = _configure_project(tmp_path, monkeypatch)
    job_id = "jato-recheck-queued"
    state = _prepare_job_state(
        job_root=job_root,
        job_id=job_id,
        status="queued",
        phase="queued",
    )
    jato_monthly_update_service._persist_job_state(state)
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_process_exists",
        lambda _pid: False,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_thread_is_alive",
        lambda _job_id: False,
    )
    launches: list[str] = []
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_launch_job_thread",
        lambda launched_job_id: launches.append(launched_job_id),
    )

    result = jato_monthly_update_service.recheck_jato_monthly_update_job(
        job_id=job_id,
        triggered_by="operator",
    )

    assert result["status"] == "queued"
    assert result["phase"] == "queued"
    assert result["runtimeCheck"]["resolvedAs"] == "worker_rewoken"
    assert launches == [job_id]
    assert result["error"] is None


def test_recheck_keeps_running_job_when_worker_pid_is_alive_without_child(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _project_root, job_root = _configure_project(tmp_path, monkeypatch)
    job_id = "jato-recheck-live-worker"
    state = _prepare_job_state(
        job_root=job_root,
        job_id=job_id,
        status="running",
        phase="raw_compare",
    )
    state["workerPid"] = 4242
    state["currentProcess"] = None
    jato_monthly_update_service._persist_job_state(state)
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_process_exists",
        lambda pid: pid == 4242,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_thread_is_alive",
        lambda _job_id: False,
    )

    result = jato_monthly_update_service.recheck_jato_monthly_update_job(
        job_id=job_id,
        triggered_by="operator",
    )

    assert result["status"] == "running"
    assert result["phase"] == "raw_compare"
    assert result["runtimeCheck"]["workerPid"] == 4242
    assert result["runtimeCheck"]["workerAlive"] is True
    assert result["runtimeCheck"].get("resolvedAs") != "stale_failed"
    assert result["error"] is None
