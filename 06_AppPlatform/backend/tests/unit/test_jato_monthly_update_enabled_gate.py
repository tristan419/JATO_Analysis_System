from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any

import pytest
from fastapi import HTTPException, UploadFile
from fastapi.testclient import TestClient

from app.api.routes import msrp_monthly_update
from app.main import app
from app.services import jato_monthly_update_service


def _headers() -> dict[str, str]:
    return {
        "X-Auth-Token": "change-me",
        "X-User-Name": "tester",
    }


def _configure_slot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    release_slot: str,
    active_slot: str,
) -> tuple[Path, Path]:
    active_slot_file = tmp_path / "active-slot"
    active_slot_file.write_text(f"{active_slot}\n", encoding="utf-8")
    deployment_marker = tmp_path / "deployment-in-progress"
    monkeypatch.setenv("APP_RELEASE_SLOT", release_slot)
    monkeypatch.setenv(
        "APP_JATO_MONTHLY_ACTIVE_SLOT_FILE",
        str(active_slot_file),
    )
    monkeypatch.setenv(
        "APP_JATO_MONTHLY_DEPLOYMENT_MARKER",
        str(deployment_marker),
    )
    monkeypatch.delenv("APP_JATO_MONTHLY_ENABLED", raising=False)
    return active_slot_file, deployment_marker


def _configure_job_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Path:
    job_root = tmp_path / "jato-jobs"
    monkeypatch.setattr(
        jato_monthly_update_service,
        "MONTHLY_UPDATE_JOB_ROOT",
        job_root,
    )
    return job_root


def test_jato_monthly_gate_defaults_enabled_for_legacy_deployment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("APP_JATO_MONTHLY_ENABLED", raising=False)
    monkeypatch.delenv("APP_RELEASE_SLOT", raising=False)
    monkeypatch.delenv("APP_JATO_MONTHLY_ACTIVE_SLOT_FILE", raising=False)
    monkeypatch.delenv("APP_JATO_MONTHLY_DEPLOYMENT_MARKER", raising=False)

    availability = jato_monthly_update_service.jato_monthly_availability()

    assert availability == {
        "enabled": True,
        "code": "JATO_MONTHLY_ENABLED",
        "reason": "legacy_no_release_slot",
        "releaseSlot": None,
        "activeSlot": None,
    }


def test_jato_monthly_gate_tracks_active_slot_and_marker_without_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    active_slot_file, deployment_marker = _configure_slot(
        tmp_path,
        monkeypatch,
        release_slot="8001",
        active_slot="8000",
    )

    assert (
        jato_monthly_update_service.jato_monthly_availability()["reason"]
        == "inactive_release_slot"
    )

    active_slot_file.write_text("8001\n", encoding="utf-8")
    assert jato_monthly_update_service.jato_monthly_availability()["enabled"] is True

    deployment_marker.touch()
    assert (
        jato_monthly_update_service.jato_monthly_availability()["reason"]
        == "deployment_in_progress"
    )

    deployment_marker.unlink()
    assert jato_monthly_update_service.jato_monthly_availability()["enabled"] is True


@pytest.mark.parametrize(
    ("environment", "reason"),
    [
        ({"APP_JATO_MONTHLY_ENABLED": "false"}, "explicitly_disabled"),
        (
            {
                "APP_RELEASE_SLOT": "8001",
                "APP_JATO_MONTHLY_DEPLOYMENT_MARKER": "/missing/marker",
            },
            "active_slot_file_not_configured",
        ),
    ],
)
def test_jato_monthly_gate_returns_structured_locked_error(
    monkeypatch: pytest.MonkeyPatch,
    environment: dict[str, str],
    reason: str,
) -> None:
    for name in (
        "APP_JATO_MONTHLY_ENABLED",
        "APP_RELEASE_SLOT",
        "APP_JATO_MONTHLY_ACTIVE_SLOT_FILE",
        "APP_JATO_MONTHLY_DEPLOYMENT_MARKER",
    ):
        monkeypatch.delenv(name, raising=False)
    for name, value in environment.items():
        monkeypatch.setenv(name, value)

    with pytest.raises(HTTPException) as exc_info:
        jato_monthly_update_service.require_jato_monthly_enabled()

    assert exc_info.value.status_code == 423
    assert exc_info.value.detail["code"] == "JATO_MONTHLY_DISABLED"
    assert exc_info.value.detail["reason"] == reason
    assert exc_info.value.detail["retryable"] is True


def test_monthly_router_blocks_get_before_wake_capable_service_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APP_JATO_MONTHLY_ENABLED", "false")
    called = False

    def forbidden_status_call() -> dict[str, object]:
        nonlocal called
        called = True
        return {}

    monkeypatch.setattr(
        msrp_monthly_update,
        "get_jato_monthly_update_maintenance_status",
        forbidden_status_call,
    )

    response = TestClient(app).get(
        "/v1/msrp/monthly-update-maintenance/status",
        headers=_headers(),
    )

    assert response.status_code == 423
    assert response.json()["detail"]["code"] == "JATO_MONTHLY_DISABLED"
    assert response.json()["detail"]["reason"] == "explicitly_disabled"
    assert called is False


def test_every_monthly_route_inherits_the_runtime_gate() -> None:
    for route in msrp_monthly_update.router.routes:
        dependency_calls = {
            dependency.call
            for dependency in route.dependant.dependencies
        }
        assert (
            jato_monthly_update_service.require_jato_monthly_enabled
            in dependency_calls
        ), route.path


def test_final_worker_spawn_points_fail_closed_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APP_JATO_MONTHLY_ENABLED", "false")
    popen_calls: list[tuple[list[str], dict[str, Any]]] = []

    def fake_popen(args: list[str], **kwargs: Any) -> object:
        popen_calls.append((args, kwargs))
        return object()

    monkeypatch.setattr(
        jato_monthly_update_service.subprocess,
        "Popen",
        fake_popen,
    )

    with pytest.raises(HTTPException) as job_exc:
        jato_monthly_update_service._launch_job_thread("jato-candidate")
    with pytest.raises(HTTPException) as digest_exc:
        jato_monthly_update_service._launch_upload_digest_process(
            "jato-upload-candidate"
        )
    with pytest.raises(HTTPException) as worker_entry_exc:
        jato_monthly_update_service.run_jato_monthly_update_worker_once()
    with pytest.raises(HTTPException) as digest_entry_exc:
        jato_monthly_update_service.run_jato_monthly_update_upload_digest(
            "jato-upload-candidate"
        )

    assert job_exc.value.status_code == 423
    assert digest_exc.value.status_code == 423
    assert worker_entry_exc.value.status_code == 423
    assert digest_entry_exc.value.status_code == 423
    assert popen_calls == []


@pytest.mark.parametrize(
    ("name", "value", "reason"),
    [
        ("APP_JATO_MONTHLY_ENABLED", "sometimes", "enabled_flag_invalid"),
        ("APP_RELEASE_SLOT", "green", "release_slot_invalid"),
    ],
)
def test_jato_monthly_gate_rejects_invalid_runtime_ownership_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    value: str,
    reason: str,
) -> None:
    _configure_slot(
        tmp_path,
        monkeypatch,
        release_slot="8001",
        active_slot="8001",
    )
    monkeypatch.setenv(name, value)

    availability = jato_monthly_update_service.jato_monthly_availability()

    assert availability["enabled"] is False
    assert availability["reason"] == reason


def test_jato_monthly_gate_rejects_symlinked_active_slot_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    active_slot_file, _marker = _configure_slot(
        tmp_path,
        monkeypatch,
        release_slot="8001",
        active_slot="8001",
    )
    real_slot_file = tmp_path / "real-active-slot"
    real_slot_file.write_text("8001\n", encoding="utf-8")
    active_slot_file.unlink()
    active_slot_file.symlink_to(real_slot_file)

    availability = jato_monthly_update_service.jato_monthly_availability()

    assert availability["enabled"] is False
    assert availability["reason"] == "active_slot_unavailable"


def test_marker_after_router_gate_blocks_upload_session_before_state_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_root = _configure_job_root(tmp_path, monkeypatch)
    _active_slot_file, deployment_marker = _configure_slot(
        tmp_path,
        monkeypatch,
        release_slot="8001",
        active_slot="8001",
    )
    jato_monthly_update_service.require_jato_monthly_enabled()
    deployment_marker.touch()

    with pytest.raises(HTTPException) as exc_info:
        jato_monthly_update_service.initiate_jato_monthly_update_upload(
            filename="jato-2026-06.xlsx",
            size_bytes=1024,
            resume_key="upload-race-001",
            triggered_by="tester",
        )

    assert exc_info.value.status_code == 423
    assert exc_info.value.detail["reason"] == "deployment_in_progress"
    assert not list(job_root.rglob("upload_state.json"))
    assert not list(job_root.rglob("chunks"))


def test_marker_after_router_gate_blocks_direct_job_before_queue_or_upload_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_root = _configure_job_root(tmp_path, monkeypatch)
    _active_slot_file, deployment_marker = _configure_slot(
        tmp_path,
        monkeypatch,
        release_slot="8001",
        active_slot="8001",
    )
    jato_monthly_update_service.require_jato_monthly_enabled()
    deployment_marker.touch()
    upload = UploadFile(
        filename="jato-2026-06.xlsx",
        file=BytesIO(b"would-be-upload-copy"),
    )

    with pytest.raises(HTTPException) as exc_info:
        jato_monthly_update_service.create_jato_monthly_update_job(
            file=upload,
            triggered_by="tester",
        )

    assert exc_info.value.status_code == 423
    assert exc_info.value.detail["reason"] == "deployment_in_progress"
    assert not list(job_root.rglob("job_state.json"))
    assert not list(job_root.rglob("uploads"))


def test_marker_after_router_gate_blocks_digest_before_attempt_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_root = _configure_job_root(tmp_path, monkeypatch)
    _active_slot_file, deployment_marker = _configure_slot(
        tmp_path,
        monkeypatch,
        release_slot="8001",
        active_slot="8001",
    )
    upload_id = "jato-upload-race001"
    state = jato_monthly_update_service._prepare_upload_session_state(
        upload_id=upload_id,
        filename="jato-2026-06.xlsx",
        size_bytes=1024,
        resume_key="digest-race-001",
        triggered_by="tester",
    )
    jato_monthly_update_service._persist_upload_session(state)
    state_path = (
        job_root
        / "_upload_sessions"
        / upload_id
        / jato_monthly_update_service.UPLOAD_STATE_FILENAME
    )
    state_before = state_path.read_bytes()
    jato_monthly_update_service.require_jato_monthly_enabled()
    deployment_marker.touch()

    with pytest.raises(HTTPException) as exc_info:
        jato_monthly_update_service.complete_jato_monthly_update_upload(
            upload_id=upload_id,
            requested_by="tester",
            requested_role="editor",
        )

    assert exc_info.value.status_code == 423
    assert exc_info.value.detail["reason"] == "deployment_in_progress"
    assert state_path.read_bytes() == state_before
    assert not (
        state_path.parent
        / jato_monthly_update_service.DIGEST_ATTEMPT_DIRNAME
    ).exists()


def test_marker_after_router_gate_blocks_baseline_before_queue_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_job_root(tmp_path, monkeypatch)
    _active_slot_file, deployment_marker = _configure_slot(
        tmp_path,
        monkeypatch,
        release_slot="8001",
        active_slot="8001",
    )
    jato_monthly_update_service.require_jato_monthly_enabled()
    deployment_marker.touch()

    with pytest.raises(HTTPException) as exc_info:
        jato_monthly_update_service.promote_current_active_to_baseline(
            triggered_by="tester",
        )

    assert exc_info.value.status_code == 423
    assert exc_info.value.detail["reason"] == "deployment_in_progress"
    assert not jato_monthly_update_service._baseline_promotion_state_path().exists()
