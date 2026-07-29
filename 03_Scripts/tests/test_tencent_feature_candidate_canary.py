from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import socket
import subprocess
import textwrap
from types import ModuleType

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
CONTROLLER = (
    REPO_ROOT / "03_Scripts/deploy/tencent_feature_candidate_canary.sh"
)
GUARD_PATH = (
    REPO_ROOT / "03_Scripts/deploy/jato_feature_canary_guard.py"
)
ACTIVE_UNIT = "jato-fullstack-backend@8000.service"
ACTIVE_HIGH = str(6 * 1024 * 1024 * 1024)
ACTIVE_MAX = str(8 * 1024 * 1024 * 1024)


def _guard() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "jato_feature_canary_guard",
        GUARD_PATH,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _snapshot() -> dict[str, object]:
    return {
        "schemaVersion": 1,
        "capturedAt": "2026-07-24T00:00:00+00:00",
        "public": {
            "origin": "https://www.ojeur.cloud",
            "healthz": {
                "status": 200,
                "error": None,
                "bodySha256": "1" * 64,
                "json": {"status": "ok"},
            },
            "buildMeta": {
                "status": 200,
                "error": None,
                "bodySha256": "2" * 64,
                "json": {"deployCommit": "a" * 40},
            },
        },
        "nginx": {
            "exitCode": 0,
            "configurationSha256": "3" * 64,
            "candidatePortReferenced": False,
        },
        "paths": {
            "/var/lib/jato-release/active-slot": {"kind": "absent"},
            "/opt/jato/active": {"kind": "absent"},
        },
        "units": {
            ACTIVE_UNIT: {
                "LoadState": "loaded",
                "ActiveState": "active",
                "SubState": "running",
                "UnitFileState": "enabled",
                "FragmentPath": (
                    "/etc/systemd/system/"
                    "jato-fullstack-backend@8000.service"
                ),
                "MainPID": "3481565",
                "ExecStart": (
                    "/opt/JATO_Analysis_System-main/.venv/bin/python "
                    "-m uvicorn app.main:app --workers ${APP_BACKEND_WORKERS}"
                ),
                "MemoryHigh": ACTIVE_HIGH,
                "MemoryMax": ACTIVE_MAX,
                "TasksMax": "512",
                "ControlGroup": (
                    "/system.slice/jato-fullstack-backend@8000.service"
                ),
                "ConfiguredBackendWorkers": "2",
                "ConfiguredBackendWorkersSource": "backend_env",
                "LiveBackendWorkerCount": "2",
                "LiveBackendWorkerPids": "3481568,3481569",
            },
            "jato-monthly-worker.service": {
                "LoadState": "loaded",
                "ActiveState": "inactive",
                "SubState": "dead",
                "UnitFileState": "disabled",
                "MainPID": "0",
            },
        },
        "activeUnit": ACTIVE_UNIT,
        "candidatePort": 18001,
        "candidatePortFree": True,
        "monthlyWorkerProcesses": [],
    }


def _candidate_evidence() -> dict[str, object]:
    return {
        "status": "verified",
        "featureCommit": "a" * 40,
        "port": 18001,
        "healthz": {"status": "ok"},
        "readyz": {
            "ok": True,
            "observed": {
                "status": "ready",
                "release": {"commitSha": "a" * 40},
            },
        },
        "monthlyStatus": 423,
        "liveBackendWorkerCount": 2,
        "systemd": {
            "ActiveState": "active",
            "UnitFileState": "transient",
            "DynamicUser": "yes",
            "ProtectSystem": "strict",
            "ProtectHome": "yes",
            "NoNewPrivileges": "yes",
            "MemoryHigh": str(3 * 1024 * 1024 * 1024),
            "MemoryMax": str(4 * 1024 * 1024 * 1024),
            "MemorySwapMax": "0",
            "TasksMax": "512",
            "ExecStart": "python -m uvicorn app.main:app --workers 2",
            "Environment": " ".join(
                (
                    "APP_DATABASE_ENABLED=false",
                    "APP_REDIS_ENABLED=false",
                    "APP_JATO_MONTHLY_ENABLED=false",
                    "APP_JATO_MONTHLY_EXECUTION_MODE=disabled",
                    "APP_GROUPED_TIME_SERIES_PREWARM_ENABLED=false",
                    "APP_DASHBOARD_OVERVIEW_PREWARM_ENABLED=false",
                    "APP_METADATA_PREWARM_ENABLED=false",
                    "APP_ADVANCED_ANALYSIS_WARMUP_ENABLED=false",
                    "HERMES_RUN_ENABLED=false",
                ),
            ),
        },
    }


def test_feature_canary_uses_only_transient_non_routing_units() -> None:
    script = CONTROLLER.read_text(encoding="utf-8")

    for required in (
        'CANARY_ROOT="${CANARY_ROOT:-/opt/jato-canary}"',
        'CANARY_STATE_ROOT="${CANARY_STATE_ROOT:-/var/lib/jato-canary}"',
        'CANARY_PORT="${CANARY_PORT:-18001}"',
        'CANARY_MEMORY_HIGH="${CANARY_MEMORY_HIGH:-3G}"',
        'CANARY_MEMORY_MAX="${CANARY_MEMORY_MAX:-4G}"',
        'CANARY_TASKS_MAX="${CANARY_TASKS_MAX:-512}"',
        "--collect",
        "--wait",
        "--pipe",
        '--service-type=exec',
        '--property="DynamicUser=yes"',
        '--property="ProtectSystem=strict"',
        '--property="ProtectHome=yes"',
        '--property="MemorySwapMax=0"',
        '--property="InaccessiblePaths=$LEGACY_ROOT/01_RAW_DATA $LEGACY_ROOT/04_Processed_data /etc/jato-fullstack"',
        '--property="APP_REDIS_ENABLED=false"',
    ):
        if required == '--property="APP_REDIS_ENABLED=false"':
            # Environment values use systemd-run --setenv, not a unit file.
            required = '--setenv="APP_REDIS_ENABLED=false"'
        assert required in script

    assert '--setenv="APP_JATO_MONTHLY_ENABLED=false"' in script
    assert '--setenv="APP_JATO_MONTHLY_EXECUTION_MODE=disabled"' in script
    for prewarm_flag in (
        "APP_GROUPED_TIME_SERIES_PREWARM_ENABLED",
        "APP_DASHBOARD_OVERVIEW_PREWARM_ENABLED",
        "APP_METADATA_PREWARM_ENABLED",
        "APP_ADVANCED_ANALYSIS_WARMUP_ENABLED",
    ):
        assert f'--setenv="{prewarm_flag}=false"' in script
    assert 'structured explicit HTTP 423' in script
    assert '--host 127.0.0.1 --port "$CANARY_PORT" --workers 2' in script
    assert 'len(worker_pids) != 2' in script
    assert '"liveBackendWorkerCount": len(worker_pids)' in script
    assert "\n    --scope \\" not in script

    forbidden = (
        "install_jato_fullstack_nginx.sh",
        "enable_jato_fullstack_https.sh",
        "systemctl reload nginx",
        "systemctl restart nginx",
        "systemctl daemon-reload",
        "tencent_bluegreen_release.sh",
        "jato_release_storage_guard.py",
        "release_checkpoint.py",
        "pause_schedulers",
        "active-slot\"",
        "/opt/jato/active\"",
    )
    for token in forbidden:
        assert token not in script


def test_feature_canary_holds_lock_and_cleans_only_its_namespace() -> None:
    script = CONTROLLER.read_text(encoding="utf-8")

    assert 'RUN_KEY="${CANARY_COMMIT_SHA:0:12}-${CANARY_RUN_ID}"' in script
    assert 'RUNTIME_ROOT="$CANARY_ROOT/runtime/$RUN_KEY"' in script
    assert 'SERVICE_UNIT="jato-feature-canary-$RUN_KEY.service"' in script
    assert 'RUNTIME_ROOT="${RUNTIME_ROOT:-' not in script
    assert 'SERVICE_UNIT="${SERVICE_UNIT:-' not in script
    assert "canary run id already has durable state and cannot be reused" in script
    assert "jato_acquire_production_mutation_lock" in script
    assert "canary requires the canonical production deploy state directory" in script
    assert (
        '"${DEPLOY_STATE_DIR%/}/production-deploy.lock"'
        in script
    )
    assert 'CANARY_SERVICE_START_ATTEMPTED" == "true"' in script
    assert 'CANARY_BUILD_START_ATTEMPTED" == "true"' in script
    assert "refusing to stop a unit without exact canary identity" in script
    assert 'sudo -n systemctl stop "$unit"' in script
    assert "sudo -n rm -rf --one-file-system \"$RUNTIME_ROOT\"" in script
    assert "sudo -n systemctl stop \"$ACTIVE_UNIT\"" not in script
    assert "sudo -n systemctl restart \"$ACTIVE_UNIT\"" not in script
    assert "sudo -n systemctl enable" not in script
    assert "sudo -n systemctl disable" not in script
    assert "Feature canary receipt retained" in script
    assert 'CANARY_RESULT="expected_failure_verified"' in script
    assert "return 97" in script
    run_body = script.split("run_canary() {", 1)[1]
    assert run_body.index("jato_acquire_production_mutation_lock") < run_body.index(
        'capture_snapshot "$BEFORE_SNAPSHOT"',
    )
    assert run_body.index('capture_snapshot "$BEFORE_SNAPSHOT"') < run_body.index(
        "run_build_scope",
    )
    finalizer_body = script.split("finalize_canary() {", 1)[1].split(
        "\nrun_canary() {",
        1,
    )[0]
    assert finalizer_body.index("cleanup_candidate") < finalizer_body.index(
        "wait_for_candidate_port_release",
    )
    assert finalizer_body.index(
        "wait_for_candidate_port_release",
    ) < finalizer_body.index('capture_snapshot "$AFTER_SNAPSHOT"')
    assert "SO_REUSEADDR, 0" in GUARD_PATH.read_text(encoding="utf-8")


def test_baseline_accepts_absent_legacy_bluegreen_markers() -> None:
    guard = _guard()
    snapshot = _snapshot()

    guard.verify_baseline(snapshot)

    snapshot = _snapshot()
    nginx = snapshot["nginx"]
    assert isinstance(nginx, dict)
    nginx["candidatePortReferenced"] = True
    with pytest.raises(
        guard.CanaryGuardError,
        match="already references",
    ):
        guard.verify_baseline(snapshot)

    snapshot = _snapshot()
    snapshot["candidatePortFree"] = False
    with pytest.raises(
        guard.CanaryGuardError,
        match="already occupied",
    ):
        guard.verify_baseline(snapshot)


def test_baseline_requires_active_limits_workers_and_monthly_fence() -> None:
    guard = _guard()
    snapshot = _snapshot()
    active = snapshot["units"][ACTIVE_UNIT]
    assert isinstance(active, dict)
    active["LiveBackendWorkerCount"] = "1"

    with pytest.raises(
        guard.CanaryGuardError,
        match="two live workers",
    ):
        guard.verify_baseline(snapshot)

    snapshot = _snapshot()
    monthly = snapshot["units"]["jato-monthly-worker.service"]
    assert isinstance(monthly, dict)
    monthly["UnitFileState"] = "enabled"
    with pytest.raises(
        guard.CanaryGuardError,
        match="monthly worker",
    ):
        guard.verify_baseline(snapshot)


def test_before_after_comparison_ignores_time_but_not_production_state() -> None:
    guard = _guard()
    before = _snapshot()
    after = json.loads(json.dumps(before))
    after["capturedAt"] = "2026-07-24T00:01:00+00:00"

    guard.compare_snapshots(before, after)

    after["paths"]["/var/lib/jato-release/active-slot"] = {
        "kind": "file",
        "sha256": "4" * 64,
    }
    with pytest.raises(
        guard.CanaryGuardError,
        match="production state changed",
    ):
        guard.compare_snapshots(before, after)

    after = json.loads(json.dumps(before))
    after["candidatePortFree"] = False
    with pytest.raises(
        guard.CanaryGuardError,
        match="candidatePortFree",
    ):
        guard.compare_snapshots(before, after)


def test_strict_candidate_port_probe_rejects_an_active_listener() -> None:
    guard = _guard()
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]
    try:
        with pytest.raises(
            guard.CanaryGuardError,
            match="not yet available",
        ):
            guard.verify_port_free(port)
    finally:
        listener.close()

    guard.verify_port_free(port)


def test_failed_receipt_records_identity_with_missing_early_snapshots(
    tmp_path: Path,
) -> None:
    guard = _guard()
    identity = {
        "repository": "tristan419/JATO_Analysis_System",
        "featureBranch": "codex/tencent-bluegreen-release",
        "commit": "a" * 40,
        "archiveSha256": "b" * 64,
        "archiveBytes": 123,
        "runId": "canary-1",
        "port": 18001,
    }
    checkpoint = tmp_path / "checkpoint.json"
    guard.record_checkpoint(
        path=checkpoint,
        identity=identity,
        phase="initialized",
        status="in_progress",
        message="test",
    )
    receipt = tmp_path / "receipt.json"
    guard.finalize_receipt(
        path=receipt,
        identity=identity,
        outcome="failed",
        fault="after_candidate_start",
        error="expected fault injection: after_candidate_start",
        before_path=tmp_path / "missing-before.json",
        after_path=tmp_path / "missing-after.json",
        candidate_path=None,
        checkpoint_path=checkpoint,
    )

    payload = json.loads(receipt.read_text(encoding="utf-8"))
    assert payload["identity"]["featureBranch"] == (
        "codex/tencent-bluegreen-release"
    )
    assert payload["identity"]["commit"] == "a" * 40
    assert payload["identity"]["archiveSha256"] == "b" * 64
    assert payload["outcome"] == "failed"
    assert payload["productionBefore"] is None
    assert payload["productionAfter"] is None


def test_success_receipt_rejects_missing_or_changed_evidence(
    tmp_path: Path,
) -> None:
    guard = _guard()
    identity = {
        "repository": "tristan419/JATO_Analysis_System",
        "featureBranch": "codex/tencent-bluegreen-release",
        "commit": "a" * 40,
        "archiveSha256": "b" * 64,
        "archiveBytes": 123,
        "runId": "canary-1",
        "port": 18001,
    }
    checkpoint = tmp_path / "checkpoint.json"
    guard.record_checkpoint(
        path=checkpoint,
        identity=identity,
        phase="expected_failure_verified",
        status="completed",
        message="test",
    )
    with pytest.raises(
        guard.CanaryGuardError,
        match="requires before/after/candidate evidence",
    ):
        guard.finalize_receipt(
            path=tmp_path / "receipt.json",
            identity=identity,
            outcome="expected_failure_verified",
            fault="after_candidate_start",
            error="expected fault injection: after_candidate_start",
            before_path=tmp_path / "missing-before.json",
            after_path=tmp_path / "missing-after.json",
            candidate_path=None,
            checkpoint_path=checkpoint,
        )

    before = _snapshot()
    after = json.loads(json.dumps(before))
    after["paths"]["/opt/jato/active"] = {"kind": "file"}
    before_path = tmp_path / "before.json"
    after_path = tmp_path / "after.json"
    candidate_path = tmp_path / "candidate.json"
    before_path.write_text(json.dumps(before), encoding="utf-8")
    after_path.write_text(json.dumps(after), encoding="utf-8")
    candidate_path.write_text(
        json.dumps(
            {
                "status": "verified",
                "featureCommit": identity["commit"],
                "port": identity["port"],
                "liveBackendWorkerCount": 2,
                "monthlyStatus": 423,
            },
        ),
        encoding="utf-8",
    )
    with pytest.raises(
        guard.CanaryGuardError,
        match="production state changed",
    ):
        guard.finalize_receipt(
            path=tmp_path / "receipt.json",
            identity=identity,
            outcome="expected_failure_verified",
            fault="after_candidate_start",
            error="expected fault injection: after_candidate_start",
            before_path=before_path,
            after_path=after_path,
            candidate_path=candidate_path,
            checkpoint_path=checkpoint,
        )


def test_candidate_evidence_rejects_worker_or_prewarm_drift() -> None:
    guard = _guard()
    identity = {
        "commit": "a" * 40,
        "port": 18001,
    }
    evidence = _candidate_evidence()
    guard.verify_candidate_evidence(evidence, identity)

    evidence["liveBackendWorkerCount"] = 3
    with pytest.raises(
        guard.CanaryGuardError,
        match="invalid candidate evidence",
    ):
        guard.verify_candidate_evidence(evidence, identity)

    evidence = _candidate_evidence()
    systemd = evidence["systemd"]
    assert isinstance(systemd, dict)
    systemd["Environment"] = str(systemd["Environment"]).replace(
        "APP_METADATA_PREWARM_ENABLED=false",
        "APP_METADATA_PREWARM_ENABLED=true",
    )
    with pytest.raises(
        guard.CanaryGuardError,
        match="omitted disabled subsystem",
    ):
        guard.verify_candidate_evidence(evidence, identity)


def test_success_receipt_requires_and_records_complete_evidence(
    tmp_path: Path,
) -> None:
    guard = _guard()
    identity = {
        "repository": "tristan419/JATO_Analysis_System",
        "featureBranch": "codex/tencent-bluegreen-release",
        "commit": "a" * 40,
        "archiveSha256": "b" * 64,
        "archiveBytes": 123,
        "runId": "canary-1",
        "port": 18001,
    }
    before = _snapshot()
    after = json.loads(json.dumps(before))
    after["capturedAt"] = "2026-07-24T00:01:00+00:00"
    before_path = tmp_path / "before.json"
    after_path = tmp_path / "after.json"
    candidate_path = tmp_path / "candidate.json"
    checkpoint_path = tmp_path / "checkpoint.json"
    receipt_path = tmp_path / "receipt.json"
    before_path.write_text(json.dumps(before), encoding="utf-8")
    after_path.write_text(json.dumps(after), encoding="utf-8")
    candidate_path.write_text(
        json.dumps(_candidate_evidence()),
        encoding="utf-8",
    )
    guard.record_checkpoint(
        path=checkpoint_path,
        identity=identity,
        phase="cleanup_verified",
        status="completed",
        message="test",
    )

    guard.finalize_receipt(
        path=receipt_path,
        identity=identity,
        outcome="passed",
        fault="",
        error="",
        before_path=before_path,
        after_path=after_path,
        candidate_path=candidate_path,
        checkpoint_path=checkpoint_path,
    )

    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["outcome"] == "passed"
    assert receipt["productionBefore"] == before
    assert receipt["productionAfter"] == after
    assert receipt["candidate"]["liveBackendWorkerCount"] == 2


def test_identity_rejects_main_instead_of_impersonating_production() -> None:
    guard = _guard()
    arguments = type(
        "Arguments",
        (),
        {
            "branch": "main",
            "commit": "a" * 40,
            "archive_sha256": "b" * 64,
            "archive_bytes": 123,
            "repository": "tristan419/JATO_Analysis_System",
            "run_id": "canary-1",
            "port": 18001,
        },
    )()

    with pytest.raises(
        guard.CanaryGuardError,
        match="non-main",
    ):
        guard._identity(arguments)


def test_guard_cli_parser_builds_without_conflicting_options() -> None:
    guard = _guard()
    parser = guard._build_parser()

    arguments = parser.parse_args(
        [
            "record",
            "--path",
            "/tmp/checkpoint.json",
            "--repository",
            "tristan419/JATO_Analysis_System",
            "--branch",
            "codex/tencent-bluegreen-release",
            "--commit",
            "a" * 40,
            "--archive-sha256",
            "b" * 64,
            "--archive-bytes",
            "123",
            "--run-id",
            "canary-1",
            "--port",
            "18001",
            "--phase",
            "initialized",
            "--status",
            "in_progress",
            "--message",
            "test",
        ],
    )
    assert arguments.commit == "a" * 40


@pytest.mark.parametrize(
    (
        "original_rc",
        "cleanup_rc",
        "port_release_rc",
        "compare_rc",
        "checkpoint_rc",
        "fault",
        "expected_rc",
        "expected_outcome",
    ),
    (
        (0, 0, 0, 0, 0, "", 0, "passed"),
        (
            97,
            0,
            0,
            0,
            0,
            "after_candidate_start",
            0,
            "expected_failure_verified",
        ),
        (0, 1, 0, 0, 0, "", 1, "failed"),
        (0, 0, 1, 0, 0, "", 1, "failed"),
        (0, 0, 0, 1, 0, "", 1, "failed"),
        (0, 0, 0, 0, 1, "", 1, "failed"),
        (
            97,
            1,
            0,
            0,
            0,
            "after_candidate_start",
            1,
            "failed",
        ),
        (
            97,
            0,
            1,
            0,
            0,
            "after_candidate_start",
            1,
            "failed",
        ),
        (
            97,
            0,
            0,
            1,
            0,
            "after_candidate_start",
            1,
            "failed",
        ),
        (
            97,
            0,
            0,
            0,
            1,
            "after_candidate_start",
            1,
            "failed",
        ),
    ),
)
def test_finalizer_control_flow_is_fail_closed(
    tmp_path: Path,
    original_rc: int,
    cleanup_rc: int,
    port_release_rc: int,
    compare_rc: int,
    checkpoint_rc: int,
    fault: str,
    expected_rc: int,
    expected_outcome: str,
) -> None:
    harness = tmp_path / "finalizer-harness.sh"
    harness.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            source "$1"
            root="$2"
            original_rc="$3"
            stub_cleanup_rc="$4"
            stub_port_release_rc="$5"
            stub_compare_rc="$6"
            stub_checkpoint_rc="$7"
            CANARY_FAULT="$8"
            log="$root/calls.log"
            BEFORE_SNAPSHOT="$root/before.json"
            AFTER_SNAPSHOT="$root/after.json"
            CHECKPOINT_FILE="$root/checkpoint.json"
            EVIDENCE_FILE="$root/evidence.json"
            RECEIPT_FILE="$root/receipt.json"
            ACTIVE_UNIT="jato-fullstack-backend@8000.service"
            CANARY_FINALIZING=false
            CANARY_RESULT="failed"
            CANARY_ERROR=""
            CANARY_EXPECTED_FAILURE_OBSERVED=false
            if [[ -n "$CANARY_FAULT" ]]; then
              CANARY_EXPECTED_FAILURE_OBSERVED=true
              CANARY_ERROR="expected fault injection: after_candidate_start"
            fi
            printf '{}\\n' >"$BEFORE_SNAPSHOT"
            printf '{}\\n' >"$CHECKPOINT_FILE"
            printf '{}\\n' >"$EVIDENCE_FILE"

            cleanup_candidate() {
              printf 'cleanup\\n' >>"$log"
              return "$stub_cleanup_rc"
            }
            wait_for_candidate_port_release() {
              printf 'port-wait\\n' >>"$log"
              return "$stub_port_release_rc"
            }
            capture_snapshot() {
              printf 'capture\\n' >>"$log"
              cp "$BEFORE_SNAPSHOT" "$1"
            }
            record_checkpoint() {
              printf 'record:%s:%s\\n' "$1" "$2" >>"$log"
              return "$stub_checkpoint_rc"
            }
            python3() {
              printf 'python:%s\\n' "$*" >>"$log"
              case " $* " in
                *" verify-baseline "*|*" compare "*)
                  return "$stub_compare_rc"
                  ;;
                *" finalize "*)
                  local previous=""
                  local outcome=""
                  local argument=""
                  for argument in "$@"; do
                    if [[ "$previous" == "--outcome" ]]; then
                      outcome="$argument"
                    fi
                    previous="$argument"
                  done
                  printf 'outcome:%s\\n' "$outcome" >>"$log"
                  printf '{}\\n' >"$RECEIPT_FILE"
                  return 0
                  ;;
              esac
              command python3 "$@"
            }

            set +e
            if [[ "$original_rc" -eq 0 ]]; then
              true
              finalize_canary
            else
              (exit "$original_rc")
              finalize_canary
            fi
            """
        ),
        encoding="utf-8",
    )
    result = subprocess.run(
        [
            "bash",
            str(harness),
            str(CONTROLLER),
            str(tmp_path),
            str(original_rc),
            str(cleanup_rc),
            str(port_release_rc),
            str(compare_rc),
            str(checkpoint_rc),
            fault,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == expected_rc, result.stderr
    calls = (tmp_path / "calls.log").read_text(encoding="utf-8")
    expected_prefix = "cleanup\n"
    if cleanup_rc == 0:
        expected_prefix += "port-wait\n"
    expected_prefix += "capture\n"
    assert calls.startswith(expected_prefix)
    assert f"outcome:{expected_outcome}\n" in calls
    if expected_outcome == "passed":
        assert "record:cleanup_verified:completed\n" in calls
    elif expected_outcome == "expected_failure_verified":
        assert "record:expected_failure_verified:completed\n" in calls
    else:
        assert (
            "record:cleanup_verified:failed\n" in calls
            or checkpoint_rc == 1
        )


@pytest.mark.parametrize(
    ("succeed_after", "expected_rc", "expected_attempts", "expected_sleeps"),
    (
        (3, 0, 3, 2),
        (0, 1, 76, 75),
    ),
)
def test_port_release_wait_is_bounded_and_retries_strict_guard(
    tmp_path: Path,
    succeed_after: int,
    expected_rc: int,
    expected_attempts: int,
    expected_sleeps: int,
) -> None:
    harness = tmp_path / "port-wait-harness.sh"
    calls = tmp_path / "calls.log"
    harness.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            source "$1"
            calls="$2"
            succeed_after="$3"
            attempts=0
            sleeps=0
            python3() {
              attempts=$((attempts + 1))
              printf 'attempt:%s:%s\\n' "$attempts" "$*" >>"$calls"
              [[ "$succeed_after" -gt 0 && "$attempts" -ge "$succeed_after" ]]
            }
            sleep() {
              sleeps=$((sleeps + 1))
              printf 'sleep:%s\\n' "$sleeps" >>"$calls"
            }
            wait_for_candidate_port_release
            """
        ),
        encoding="utf-8",
    )
    result = subprocess.run(
        [
            "bash",
            str(harness),
            str(CONTROLLER),
            str(calls),
            str(succeed_after),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == expected_rc
    call_lines = calls.read_text(encoding="utf-8").splitlines()
    attempts = [line for line in call_lines if line.startswith("attempt:")]
    sleeps = [line for line in call_lines if line.startswith("sleep:")]
    assert len(attempts) == expected_attempts
    assert len(sleeps) == expected_sleeps
    assert all(" verify-port-free --port 18001" in line for line in attempts)


@pytest.mark.parametrize("unsafe_identity", (False, True))
def test_cleanup_stops_verified_units_before_removing_runtime(
    tmp_path: Path,
    unsafe_identity: bool,
) -> None:
    harness = tmp_path / "cleanup-harness.sh"
    harness.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            source "$1"
            root="$2"
            unsafe_identity="$3"
            RUN_KEY="aaaaaaaaaaaa-canary-1"
            RUNTIME_ROOT="/opt/jato-canary/runtime/$RUN_KEY"
            CONTROL_ROOT="/opt/jato-canary/control/$RUN_KEY"
            STAGED_SOURCE_ARCHIVE="/opt/jato-canary/sources/$RUN_KEY.tar.gz"
            CONTROL_SCRIPT="$CONTROL_ROOT/03_Scripts/deploy/tencent_feature_candidate_canary.sh"
            SERVICE_UNIT="jato-feature-canary-$RUN_KEY.service"
            BUILD_UNIT="jato-feature-canary-build-$RUN_KEY.service"
            CANARY_PORT="18001"
            CANARY_COMMIT_SHA="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            CANARY_SERVICE_START_ATTEMPTED=true
            CANARY_BUILD_START_ATTEMPTED=true
            log="$root/cleanup.log"
            : >"$log"

            marker_for_path() {
              case "$1" in
                "$RUNTIME_ROOT") printf 'runtime' ;;
                "$CONTROL_ROOT") printf 'control' ;;
                "$STAGED_SOURCE_ARCHIVE") printf 'source' ;;
                *) return 1 ;;
              esac
            }
            systemctl() {
              local action="$1"
              local unit="$2"
              shift 2
              if [[ "$action" != "show" ]]; then
                return 1
              fi
              if [[ "$*" == *"LoadState"* && "$*" == *"--value"* ]]; then
                printf 'loaded\\n'
                return 0
              fi
              if [[ "$*" == *"UnitFileState"* ]]; then
                local fragment="/run/systemd/transient/$unit"
                if [[ "$unsafe_identity" == "true" && "$unit" == "$SERVICE_UNIT" ]]; then
                  fragment="/etc/systemd/system/jato-fullstack-backend@8000.service"
                fi
                printf 'ActiveState=active\\n'
                printf 'UnitFileState=transient\\n'
                printf 'FragmentPath=%s\\n' "$fragment"
                if [[ "$unit" == "$SERVICE_UNIT" ]]; then
                  printf 'ExecStart=%s/.venv/bin/python -m uvicorn app.main:app --port %s --workers 2\\n' \
                    "$RUNTIME_ROOT" "$CANARY_PORT"
                  printf 'Environment=APP_RELEASE_SHA=%s\\n' "$CANARY_COMMIT_SHA"
                else
                  printf 'ExecStart=/usr/bin/bash %s build\\n' "$CONTROL_SCRIPT"
                  printf 'Environment=RUN_KEY=%s\\n' "$RUN_KEY"
                fi
                printf 'ControlGroup=/system.slice/%s\\n' "$unit"
                return 0
              fi
              if [[ "$*" == *"ActiveState"* && "$*" == *"--value"* ]]; then
                if [[ -f "$root/stopped-$unit" ]]; then
                  printf 'inactive\\n'
                else
                  printf 'active\\n'
                fi
                return 0
              fi
              return 1
            }
            sudo() {
              [[ "$1" == "-n" ]] && shift
              if [[ "$1" == "systemctl" ]]; then
                if [[ "$2" == "stop" ]]; then
                  printf 'stop:%s\\n' "$3" >>"$log"
                  : >"$root/stopped-$3"
                fi
                return 0
              fi
              if [[ "$1" == "test" ]]; then
                local predicate="$2"
                local path="$3"
                local marker=""
                marker="$(marker_for_path "$path")" || return 1
                if [[ "$predicate" == "-L" ]]; then
                  return 1
                fi
                [[ ! -f "$root/removed-$marker" ]]
                return
              fi
              if [[ "$1" == "chmod" ]]; then
                return 0
              fi
              if [[ "$1" == "rm" ]]; then
                local path="${@: -1}"
                local marker=""
                marker="$(marker_for_path "$path")" || return 1
                printf 'remove:%s\\n' "$marker" >>"$log"
                : >"$root/removed-$marker"
                return 0
              fi
              return 1
            }

            set +e
            cleanup_candidate
            exit "$?"
            """
        ),
        encoding="utf-8",
    )
    result = subprocess.run(
        [
            "bash",
            str(harness),
            str(CONTROLLER),
            str(tmp_path),
            str(unsafe_identity).lower(),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    calls = (tmp_path / "cleanup.log").read_text(encoding="utf-8")
    if unsafe_identity:
        assert result.returncode == 1
        assert "remove:" not in calls
    else:
        assert result.returncode == 0, result.stderr
        assert f"stop:jato-feature-canary-{('a' * 12)}-canary-1.service" in calls
        assert f"stop:jato-feature-canary-build-{('a' * 12)}-canary-1.service" in calls
        assert "remove:runtime\n" in calls
        assert "remove:control\n" in calls
        assert "remove:source\n" in calls
