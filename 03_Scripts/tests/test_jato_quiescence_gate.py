from __future__ import annotations

import fcntl
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
HELPER_PATH = REPO_ROOT / "03_Scripts/deploy/jato_quiescence_gate.py"
TEST_MAIN_PID = 4242


def _load_helper():
    spec = importlib.util.spec_from_file_location("jato_quiescence_gate", HELPER_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


gate = _load_helper()


def _job_root(project_root: Path) -> Path:
    root = project_root / "04_Processed_data/ops/jato_monthly_update_jobs"
    (root / "_maintenance").mkdir(parents=True)
    (root / "_upload_sessions").mkdir()
    return root


def _write(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_fake_proc(
    *,
    proc_root: Path,
    project_root: Path,
    job_root: Path | None,
    main_pid: int = TEST_MAIN_PID,
    start_time: int = 987654,
) -> None:
    process_root = proc_root / str(main_pid)
    process_root.mkdir(parents=True)
    # Fields after comm start at Linux proc stat field 3. starttime is field 22,
    # therefore index 19 in this tail.
    stat_tail = ["S", *(["0"] * 18), str(start_time), "0", "0"]
    (process_root / "stat").write_text(
        f"{main_pid} (uvicorn main) {' '.join(stat_tail)}\n",
        encoding="utf-8",
    )
    environment = [f"APP_PROJECT_ROOT={project_root}".encode()]
    if job_root is not None:
        environment.append(
            f"APP_JATO_MONTHLY_UPDATE_JOB_ROOT={job_root}".encode(),
        )
    (process_root / "environ").write_bytes(b"\0".join(environment) + b"\0")


def _runtime_contract(
    project_root: Path,
    job_root: Path,
    *,
    include_job_root_environment: bool = True,
) -> gate.ActiveRuntimeContract:
    proc_root = project_root / "fake-proc"
    _write_fake_proc(
        proc_root=proc_root,
        project_root=project_root,
        job_root=job_root if include_job_root_environment else None,
    )
    return gate.ActiveRuntimeContract(
        main_pid=TEST_MAIN_PID,
        expected_project_root=project_root,
        active_bundle_lock_path=(
            project_root / "04_Processed_data/active-bundle.lock"
        ),
        proc_root=proc_root,
    )


def _hold(
    *,
    project_root: Path,
    job_root: Path,
    marker: Path,
    evidence: Path,
    command: list[str],
    timeout: float = 2,
) -> int:
    return gate.hold_gate(
        job_root=job_root,
        marker_path=marker,
        timeout_seconds=timeout,
        evidence_path=evidence,
        runtime_contract=_runtime_contract(project_root, job_root),
        command=command,
    )


def _cli_args(
    *,
    contract: gate.ActiveRuntimeContract,
    job_root: Path,
    marker: Path,
    evidence: Path,
    command: list[str],
) -> list[str]:
    return [
        "hold",
        "--job-root",
        str(job_root),
        "--active-main-pid",
        str(contract.main_pid),
        "--expected-project-root",
        str(contract.expected_project_root),
        "--active-bundle-lock",
        str(contract.active_bundle_lock_path),
        "--proc-root",
        str(contract.proc_root),
        "--marker",
        str(marker),
        "--timeout",
        "0.1",
        "--evidence",
        str(evidence),
        "--",
        *command,
    ]


def test_inspection_reports_busy_job_upload_and_baseline(tmp_path: Path) -> None:
    root = _job_root(tmp_path)
    _write(root / "job-1/job_state.json", {"status": "running"})
    _write(
        root / "_upload_sessions/upload-1/upload_state.json",
        {"status": "digesting"},
    )
    _write(
        root / "_maintenance/baseline_promotion_state.json",
        {"status": "queued"},
    )

    result = gate.inspect_state(root)

    assert result["busy"] is True
    assert result["busyJobs"] == ["job-1"]
    assert result["busyUploads"] == ["upload-1"]
    assert result["baselineStatus"] == "queued"


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("pendingOperation", "queued", "pendingOperation"),
        ("pendingOperation", {}, "status must be"),
        ("currentProcess", "123", "currentProcess"),
        ("currentProcess", {}, "currentProcess pid is required"),
        ("currentProcess", {"pid": "123"}, "PID integer"),
        ("workerPid", True, "PID integer"),
        ("workerPid", -1, "PID integer"),
    ],
)
def test_job_nested_schema_is_fail_closed(
    tmp_path: Path,
    field: str,
    value: object,
    message: str,
) -> None:
    root = _job_root(tmp_path)
    _write(root / "job-1/job_state.json", {"status": "success", field: value})

    with pytest.raises(gate.GateError, match=message):
        gate.inspect_state(root)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("digestAttempt", "running", "digestAttempt"),
        ("digestAttempt", {}, "status must be"),
        (
            "digestAttempt",
            {"status": "mystery"},
            "unknown JATO digest attempt status",
        ),
        (
            "digestAttempt",
            {"status": "finished", "supervisorPid": "123"},
            "PID integer",
        ),
        ("digestPid", "123", "PID integer"),
        ("digestWorkerPid", False, "PID integer"),
    ],
)
def test_upload_nested_schema_is_fail_closed(
    tmp_path: Path,
    field: str,
    value: object,
    message: str,
) -> None:
    root = _job_root(tmp_path)
    _write(
        root / "_upload_sessions/upload-1/upload_state.json",
        {"status": "ready", field: value},
    )

    with pytest.raises(gate.GateError, match=message):
        gate.inspect_state(root)


@pytest.mark.parametrize(
    ("attempt_status", "expected_busy"),
    [
        ("launching", True),
        ("running", True),
        ("digesting", True),
        ("worker_finished", False),
        ("finished", False),
    ],
)
def test_actual_digest_attempt_statuses_are_classified(
    tmp_path: Path,
    attempt_status: str,
    expected_busy: bool,
) -> None:
    root = _job_root(tmp_path)
    _write(
        root / "_upload_sessions/upload-1/upload_state.json",
        {
            "status": "ready",
            "digestAttempt": {
                "status": attempt_status,
                "supervisorPid": None,
                "workerPid": None,
            },
        },
    )

    assert gate.inspect_state(root)["busy"] is expected_busy


def test_terminal_digest_attempt_with_live_nested_pid_remains_busy(
    tmp_path: Path,
) -> None:
    root = _job_root(tmp_path)
    _write(
        root / "_upload_sessions/upload-1/upload_state.json",
        {
            "status": "ready",
            "digestAttempt": {
                "status": "worker_finished",
                "supervisorPid": os.getpid(),
                "workerPid": None,
            },
        },
    )

    assert gate.inspect_state(root)["busy"] is True


def test_runtime_contract_proves_live_lock_namespace(tmp_path: Path) -> None:
    root = _job_root(tmp_path)
    contract = _runtime_contract(tmp_path, root)

    proof = gate.verify_active_runtime(job_root=root, contract=contract)

    assert proof["mainPid"] == TEST_MAIN_PID
    assert proof["processStartTime"] == 987654
    assert proof["jobRootSource"] == "explicit_environment"
    assert (
        proof["activeBundleLock"]["parentInode"]
        == (tmp_path / "04_Processed_data").stat().st_ino
    )


def test_first_bluegreen_migration_accepts_proven_config_default_job_root(
    tmp_path: Path,
) -> None:
    root = _job_root(tmp_path)
    contract = _runtime_contract(
        tmp_path,
        root,
        include_job_root_environment=False,
    )

    proof = gate.verify_active_runtime(job_root=root, contract=contract)

    assert proof["jobRootSource"] == "config_default_from_project_root"
    assert proof["jobRoot"]["inode"] == root.stat().st_ino


def test_runtime_contract_rejects_missing_live_project_root(
    tmp_path: Path,
) -> None:
    root = _job_root(tmp_path)
    contract = _runtime_contract(tmp_path, root)
    environ_path = (
        contract.proc_root / str(contract.main_pid) / "environ"
    )
    environ_path.write_bytes(
        f"APP_JATO_MONTHLY_UPDATE_JOB_ROOT={root}\0".encode(),
    )

    with pytest.raises(gate.GateError, match="APP_PROJECT_ROOT is missing"):
        gate.verify_active_runtime(job_root=root, contract=contract)


def test_runtime_contract_rejects_different_live_job_root(tmp_path: Path) -> None:
    root = _job_root(tmp_path)
    other_root = tmp_path / "other-jobs"
    other_root.mkdir()
    proc_root = tmp_path / "fake-proc"
    _write_fake_proc(
        proc_root=proc_root,
        project_root=tmp_path,
        job_root=other_root,
    )
    contract = gate.ActiveRuntimeContract(
        main_pid=TEST_MAIN_PID,
        expected_project_root=tmp_path,
        active_bundle_lock_path=(
            tmp_path / "04_Processed_data/active-bundle.lock"
        ),
        proc_root=proc_root,
    )

    with pytest.raises(gate.GateError, match="job root does not match"):
        gate.verify_active_runtime(job_root=root, contract=contract)


def test_runtime_contract_rejects_different_active_bundle_lock(
    tmp_path: Path,
) -> None:
    root = _job_root(tmp_path)
    contract = _runtime_contract(tmp_path, root)
    wrong_parent = tmp_path / "other-processed"
    wrong_parent.mkdir()
    contract = gate.ActiveRuntimeContract(
        main_pid=contract.main_pid,
        expected_project_root=contract.expected_project_root,
        active_bundle_lock_path=wrong_parent / "active-bundle.lock",
        proc_root=contract.proc_root,
    )

    with pytest.raises(gate.GateError, match="active-bundle.lock does not match"):
        gate.verify_active_runtime(job_root=root, contract=contract)


def test_runtime_contract_rejects_main_pid_identity_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _job_root(tmp_path)
    contract = _runtime_contract(tmp_path, root)
    observed = iter((100, 101))
    monkeypatch.setattr(
        gate,
        "_proc_stat_start_time",
        lambda _path, _pid: next(observed),
    )

    with pytest.raises(gate.GateError, match="changed identity"):
        gate.verify_active_runtime(job_root=root, contract=contract)


def test_gate_keeps_idle_upload_and_runs_under_all_locks(tmp_path: Path) -> None:
    root = _job_root(tmp_path)
    _write(
        root / "_upload_sessions/upload-1/upload_state.json",
        {"status": "uploading"},
    )
    marker = tmp_path / "deployment-maintenance"
    evidence = tmp_path / "quiescence.json"
    output = tmp_path / "ran"
    worker_lock = root / "worker.lock"
    child_code = (
        "import fcntl,os,pathlib;"
        "assert os.environ['JATO_QUIESCENCE_LOCK_HELD']=='1';"
        f"h=open({str(worker_lock)!r},'a+b');"
        "\ntry:\n"
        " fcntl.flock(h.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)\n"
        "except BlockingIOError:\n"
        " pass\n"
        "else:\n"
        " raise SystemExit('worker lock was not inherited')\n"
        f"pathlib.Path({str(output)!r}).write_text('ok')"
    )

    result = _hold(
        project_root=tmp_path,
        job_root=root,
        marker=marker,
        evidence=evidence,
        command=[sys.executable, "-c", child_code],
    )

    assert result == 0
    assert output.read_text() == "ok"
    assert not marker.exists()
    payload = json.loads(evidence.read_text())
    assert payload["schemaVersion"] == 2
    assert payload["status"] == "completed"
    assert payload["runtimeProof"]["mainPid"] == TEST_MAIN_PID
    assert payload["finalLockCount"] >= 6
    for lock_path in gate._final_lock_paths(
        root,
        tmp_path / "04_Processed_data/active-bundle.lock",
    ):
        with lock_path.open("a+b") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_gate_times_out_without_mutating_busy_state(tmp_path: Path) -> None:
    root = _job_root(tmp_path)
    state_path = root / "job-1/job_state.json"
    _write(state_path, {"status": "running", "sentinel": "unchanged"})
    before = state_path.read_bytes()

    result = _hold(
        project_root=tmp_path,
        job_root=root,
        marker=tmp_path / "marker",
        timeout=0.05,
        evidence=tmp_path / "evidence.json",
        command=[sys.executable, "-c", "raise SystemExit(99)"],
    )

    assert result == gate.EXIT_BUSY_TIMEOUT
    assert state_path.read_bytes() == before


def test_gate_rejects_unknown_nonterminal_state(tmp_path: Path) -> None:
    root = _job_root(tmp_path)
    _write(root / "job-1/job_state.json", {"status": "mystery"})

    with pytest.raises(gate.GateError, match="unknown JATO job status"):
        gate.inspect_state(root)


def test_gate_rejects_existing_deployment_marker(tmp_path: Path) -> None:
    root = _job_root(tmp_path)
    marker = tmp_path / "marker"
    marker.write_text("another deploy", encoding="utf-8")

    with pytest.raises(gate.GateError, match="deployment marker already exists"):
        _hold(
            project_root=tmp_path,
            job_root=root,
            marker=marker,
            timeout=0.1,
            evidence=tmp_path / "evidence.json",
            command=[sys.executable, "-c", "raise SystemExit(0)"],
        )
    assert marker.read_text(encoding="utf-8") == "another deploy"


def test_gate_waits_for_worker_lock_then_runs(tmp_path: Path) -> None:
    root = _job_root(tmp_path)
    lock_path = root / "worker.lock"
    lock_path.touch()
    release = threading.Event()

    def hold_worker_lock() -> None:
        with lock_path.open("a+b") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            release.wait(timeout=1)

    thread = threading.Thread(target=hold_worker_lock)
    thread.start()
    time.sleep(0.05)
    output = tmp_path / "ran"

    def release_soon() -> None:
        time.sleep(0.1)
        release.set()

    release_thread = threading.Thread(target=release_soon)
    release_thread.start()
    result = _hold(
        project_root=tmp_path,
        job_root=root,
        marker=tmp_path / "marker",
        timeout=1,
        evidence=tmp_path / "evidence.json",
        command=[sys.executable, "-c", f"open({str(output)!r},'w').write('ok')"],
    )
    thread.join()
    release_thread.join()

    assert result == 0
    assert output.exists()


def test_successful_child_with_final_evidence_failure_retains_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = _job_root(tmp_path)
    contract = _runtime_contract(tmp_path, root)
    marker = tmp_path / "marker"
    evidence = tmp_path / "evidence.json"
    original_write = gate._atomic_write_json

    def fail_final_write(path: Path, payload: dict[str, object]) -> None:
        if payload.get("status") == "completed":
            raise OSError("simulated evidence fsync failure")
        original_write(path, payload)

    monkeypatch.setattr(gate, "_atomic_write_json", fail_final_write)
    result = gate.main(
        _cli_args(
            contract=contract,
            job_root=root,
            marker=marker,
            evidence=evidence,
            command=[sys.executable, "-c", "raise SystemExit(0)"],
        ),
    )

    error = json.loads(capsys.readouterr().err)
    assert result == gate.EXIT_POST_COMMAND_EVIDENCE_FAILED
    assert error["error"]["code"] == "post_command_evidence_failed"
    assert error["error"]["commandExitCode"] == 0
    assert error["error"]["markerRetained"] is True
    assert error["error"]["checkpointDecisionRequired"] is True
    assert marker.exists()
    assert json.loads(evidence.read_text())["status"] == "command_running"


def test_successful_child_with_marker_cleanup_failure_has_distinct_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = _job_root(tmp_path)
    contract = _runtime_contract(tmp_path, root)
    marker = tmp_path / "marker"
    evidence = tmp_path / "evidence.json"

    def fail_marker_cleanup(_path: Path, _identity: gate.MarkerIdentity) -> None:
        raise OSError("simulated marker unlink failure")

    monkeypatch.setattr(gate, "_remove_marker", fail_marker_cleanup)
    result = gate.main(
        _cli_args(
            contract=contract,
            job_root=root,
            marker=marker,
            evidence=evidence,
            command=[sys.executable, "-c", "raise SystemExit(0)"],
        ),
    )

    error = json.loads(capsys.readouterr().err)
    assert result == gate.EXIT_MARKER_CLEANUP_FAILED
    assert error["error"]["code"] == "marker_cleanup_failed"
    assert error["error"]["commandExitCode"] == 0
    assert error["error"]["markerRetained"] is True
    assert error["error"]["checkpointDecisionRequired"] is True
    assert marker.exists()
    assert json.loads(evidence.read_text())["status"] == "completed"


def test_ordinary_child_failure_with_proven_rollback_cleans_marker(
    tmp_path: Path,
) -> None:
    root = _job_root(tmp_path)
    marker = tmp_path / "marker"
    evidence = tmp_path / "evidence.json"

    result = _hold(
        project_root=tmp_path,
        job_root=root,
        marker=marker,
        evidence=evidence,
        command=[sys.executable, "-c", "raise SystemExit(23)"],
    )

    assert result == 23
    assert not marker.exists()
    payload = json.loads(evidence.read_text())
    assert payload["status"] == "command_failed"
    assert payload["commandExitCode"] == 23


def test_unproven_rollback_sentinel_retains_marker_and_returns_81(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = _job_root(tmp_path)
    contract = _runtime_contract(tmp_path, root)
    marker = tmp_path / "marker"
    evidence = tmp_path / "evidence.json"

    result = gate.main(
        _cli_args(
            contract=contract,
            job_root=root,
            marker=marker,
            evidence=evidence,
            command=[
                sys.executable,
                "-c",
                (
                    "raise SystemExit("
                    f"{gate.EXIT_COMMAND_FAILED_MARKER_RETAINED})"
                ),
            ],
        ),
    )

    error = json.loads(capsys.readouterr().err)
    assert result == gate.EXIT_COMMAND_FAILED_MARKER_RETAINED
    assert error["error"]["code"] == "command_failed_marker_retained"
    assert error["error"]["commandExitCode"] == 81
    assert error["error"]["markerRetained"] is True
    assert error["error"]["checkpointDecisionRequired"] is True
    assert marker.exists()
    payload = json.loads(evidence.read_text())
    assert payload["status"] == "command_failed_marker_retained"
    assert payload["commandExitCode"] == 81


def test_command_start_oserror_writes_evidence_and_safely_cleans_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = _job_root(tmp_path)
    contract = _runtime_contract(tmp_path, root)
    marker = tmp_path / "marker"
    evidence = tmp_path / "evidence.json"

    def fail_to_start(*_args: object, **_kwargs: object) -> None:
        raise FileNotFoundError("simulated exec failure")

    monkeypatch.setattr(gate.subprocess, "run", fail_to_start)
    result = gate.main(
        _cli_args(
            contract=contract,
            job_root=root,
            marker=marker,
            evidence=evidence,
            command=["missing-command"],
        ),
    )

    error = json.loads(capsys.readouterr().err)
    assert result == gate.EXIT_COMMAND_START_FAILED
    assert error["error"]["code"] == "command_start_failed"
    assert error["error"]["evidenceCommitted"] is True
    assert error["error"]["markerRetained"] is False
    assert error["error"]["checkpointDecisionRequired"] is False
    assert not marker.exists()
    assert json.loads(evidence.read_text())["status"] == "command_start_failed"


def test_cli_rejects_malformed_state_fail_closed(tmp_path: Path) -> None:
    root = _job_root(tmp_path)
    contract = _runtime_contract(tmp_path, root)
    state = root / "job-1/job_state.json"
    state.parent.mkdir()
    state.write_text("{", encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            str(HELPER_PATH),
            *_cli_args(
                contract=contract,
                job_root=root,
                marker=tmp_path / "marker",
                evidence=tmp_path / "evidence.json",
                command=[sys.executable, "-c", "raise SystemExit(0)"],
            ),
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == gate.EXIT_INVALID_STATE
    assert '"code": "invalid_state"' in result.stderr
