from __future__ import annotations

import importlib.util
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import textwrap
import time
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
EVIDENCE_DEPLOY_UID = 1000
EVIDENCE_DEPLOY_GID = 1001


def _shell_function(script: str, name: str) -> str:
    match = re.search(
        rf"(?ms)^{re.escape(name)}\(\) \{{\n"
        rf"(.*?)(?=^[a-zA-Z_][a-zA-Z0-9_]*\(\) \{{|\Z)",
        script,
    )
    assert match is not None, f"missing shell function: {name}"
    return match.group(0)


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
    private_files = [
        {
            "path": "01_RAW_DATA/VOC_Nordic_SUV_Users_100.xlsx",
            "mode": "0600",
            "sha256": "9" * 64,
            "bytes": 111,
        },
        {
            "path": (
                "03_Scripts/diagnostics/artifacts/msrp_backfill/"
                "sweden_swiss_top30_suv/official_evidence_leads.json"
            ),
            "mode": "0600",
            "sha256": "8" * 64,
            "bytes": 222,
        },
        {
            "path": (
                "03_Scripts/diagnostics/artifacts/msrp_backfill/"
                "sweden_swiss_top30_suv/"
                "top30_suv_price_movement_candidates.json"
            ),
            "mode": "0600",
            "sha256": "5" * 64,
            "bytes": 333,
        },
    ]
    private_directories = [
        {"path": "01_RAW_DATA", "mode": "0711"},
        {
            "path": "03_Scripts/diagnostics/artifacts",
            "mode": "0711",
        },
        {
            "path": (
                "03_Scripts/diagnostics/artifacts/msrp_backfill"
            ),
            "mode": "0711",
        },
        {
            "path": (
                "03_Scripts/diagnostics/artifacts/msrp_backfill/"
                "sweden_swiss_top30_suv"
            ),
            "mode": "0711",
        },
    ]
    controls = {
        relative: "7" * 64
        for relative in (
            "03_Scripts/deploy/tencent_feature_candidate_canary.sh",
            "03_Scripts/deploy/jato_feature_canary_guard.py",
            "03_Scripts/deploy/lib/production_mutation_lock.sh",
            "03_Scripts/deploy/verify_backend_readiness.py",
            "03_Scripts/deploy/validate_release_archive.py",
            "03_Scripts/deploy/cleanup_toolkit_egg_info.py",
            "03_Scripts/deploy/verify_release_source_seal.py",
        )
    }
    archive_validation = {
        "schemaVersion": 2,
        "status": "validated",
        "archiveSha256": "b" * 64,
        "archiveBytes": 123,
        "memberCount": 321,
        "expandedBytes": 456_789,
        "rootMode": "0755",
        "modePolicy": {
            "publicFiles": ["0644", "0755"],
            "publicDirectories": ["0755"],
            "privatePrefixes": [
                "01_RAW_DATA",
                "03_Scripts/diagnostics/artifacts",
            ],
            "privateFiles": ["0600", "0711"],
            "privateDirectories": ["0711"],
        },
        "memberClasses": {
            "publicFiles": 300,
            "publicDirectories": 14,
            "privateFiles": len(private_files),
            "privateDirectories": len(private_directories),
        },
        "privateModeEvidence": {
            "requiredWorkbook": {
                "path": "01_RAW_DATA/VOC_Nordic_SUV_Users_100.xlsx",
                "type": "file",
                "mode": "0600",
            },
            "diagnosticsArtifacts": {
                "prefix": "03_Scripts/diagnostics/artifacts/",
                "fileModes": ["0600"],
                "directoryModes": ["0711"],
            },
        },
        "privateEntries": {
            "files": private_files,
            "directories": private_directories,
        },
        "trustedControls": controls,
    }
    roots = {
        "reference": {"mode": "0700", "uid": 0, "gid": 0},
        "candidate": {
            "mode": "0711",
            "uid": EVIDENCE_DEPLOY_UID,
            "gid": EVIDENCE_DEPLOY_GID,
        },
    }

    def materialized(
        items: list[dict[str, object]],
        *,
        uid: int,
        gid: int,
    ) -> list[dict[str, object]]:
        return [
            {**json.loads(json.dumps(item)), "uid": uid, "gid": gid}
            for item in items
        ]

    archive_receipt_bytes = (
        json.dumps(archive_validation, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    source_seal_sha256 = "6" * 64
    return {
        "evidenceSchemaVersion": 2,
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
        "candidateInvocationId": "d" * 32,
        "buildEvidence": {
            "schemaVersion": 3,
            "archiveValidation": archive_validation,
            "referenceAnchor": {
                "schemaVersion": 1,
                "archiveSha256": "b" * 64,
                "archiveBytes": 123,
                "archiveValidationSha256": hashlib.sha256(
                    archive_receipt_bytes
                ).hexdigest(),
                "sourceSealSha256": source_seal_sha256,
                "roots": roots,
            },
            "materialization": {
                "referenceRootMode": "0700",
                "candidateRootMode": "0711",
                "extractFlags": [
                    "--same-permissions",
                    "--no-overwrite-dir",
                ],
                "copyMethod": "independent-sealed-archive-extraction",
                "roots": roots,
            },
            "sourceSeal": {
                "profile": "source",
                "sha256": source_seal_sha256,
                "verifiedAfterBuild": True,
            },
            "toolkitEggInfo": {
                "cleanBeforeEditableInstall": True,
                "cleanAfterEditableInstall": True,
            },
            "privateMaterialization": {
                "reference": {
                    "files": materialized(private_files, uid=0, gid=0),
                    "directories": materialized(
                        private_directories,
                        uid=0,
                        gid=0,
                    ),
                },
                "candidate": {
                    "files": materialized(
                        private_files,
                        uid=EVIDENCE_DEPLOY_UID,
                        gid=EVIDENCE_DEPLOY_GID,
                    ),
                    "directories": materialized(
                        private_directories,
                        uid=EVIDENCE_DEPLOY_UID,
                        gid=EVIDENCE_DEPLOY_GID,
                    ),
                },
            },
        },
        "sourceSealRuntimeVerification": {
            "beforeRuntime": True,
            "afterRuntime": True,
        },
        "startPermit": {
            "supervisorInvocationId": "c" * 32,
            "candidateInvocationId": "d" * 32,
            "unit": (
                "jato-feature-canary-"
                "aaaaaaaaaaaa-canary-1.service"
            ),
        },
        "systemd": {
            "ActiveState": "active",
            "UnitFileState": "transient",
            "DynamicUser": "yes",
            "ProtectSystem": "strict",
            "ProtectHome": "yes",
            "NoNewPrivileges": "yes",
            "Restart": "no",
            "UMask": "0022",
            "MemoryHigh": str(3 * 1024 * 1024 * 1024),
            "MemoryMax": str(4 * 1024 * 1024 * 1024),
            "MemorySwapMax": "0",
            "TasksMax": "512",
            "InvocationID": "d" * 32,
            "ExecStart": "python -m uvicorn app.main:app --workers 2",
            "StopPropagatedFrom": (
                "jato-feature-canary-supervisor-"
                "aaaaaaaaaaaa-canary-1.service"
            ),
            "After": (
                "jato-feature-canary-supervisor-"
                "aaaaaaaaaaaa-canary-1.service"
            ),
            "BindsTo": "",
            "PartOf": "",
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
                    f"CANARY_DEPLOY_UID={EVIDENCE_DEPLOY_UID}",
                    f"CANARY_DEPLOY_GID={EVIDENCE_DEPLOY_GID}",
                    "CANARY_SUPERVISOR_INVOCATION_ID="
                    "cccccccccccccccccccccccccccccccc",
                ),
            ),
        },
    }


def test_feature_canary_uses_only_transient_non_routing_units() -> None:
    script = CONTROLLER.read_text(encoding="utf-8")

    for required in (
        'CANARY_MODE="${1:-launch}"',
        'CANARY_ROOT="${CANARY_ROOT:-/opt/jato-canary}"',
        'CANARY_STATE_ROOT="${CANARY_STATE_ROOT:-/var/lib/jato-canary}"',
        'CANARY_PORT="${CANARY_PORT:-18001}"',
        'CANARY_MEMORY_HIGH="${CANARY_MEMORY_HIGH:-3G}"',
        'CANARY_MEMORY_MAX="${CANARY_MEMORY_MAX:-4G}"',
        'CANARY_TASKS_MAX="${CANARY_TASKS_MAX:-512}"',
        "--collect",
        '--service-type=exec',
        '--property="DynamicUser=yes"',
        '--property="ProtectSystem=strict"',
        '--property="ProtectHome=yes"',
        '--property="MemorySwapMax=0"',
        (
            '--property="InaccessiblePaths=$REFERENCE_ROOT '
            "$LEGACY_ROOT/01_RAW_DATA $LEGACY_ROOT/04_Processed_data "
            '/etc/jato-fullstack"'
        ),
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
    launcher = _shell_function(script, "start_canary_supervisor")
    assert "--wait" not in launcher
    assert "--pipe" not in launcher
    launch = _shell_function(script, "launch_canary")
    assert launch.count("start_canary_supervisor") == 1
    for forbidden_call in (
        "--wait",
        "--pipe",
        "acquire_canary_production_lock",
        "run_canary_controller",
        "run_build_scope",
        "start_candidate_service",
    ):
        assert forbidden_call not in launch
    main = _shell_function(script, "main")
    assert "launch)\n      launch_canary" in main
    assert "supervisor)\n" in main
    assert "run_canary_supervisor" in main
    build = _shell_function(script, "run_build_scope")
    assert "--wait" in build
    assert "--pipe" in build

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


def test_build_and_runtime_integrity_bind_roots_and_source_seal() -> None:
    script = CONTROLLER.read_text(encoding="utf-8")
    build = _shell_function(script, "build_candidate_runtime")
    materialize = _shell_function(script, "prepare_trusted_materialization")
    verify = _shell_function(script, "verify_trusted_candidate_integrity")
    controller = _shell_function(script, "run_canary_controller")

    assert 'python3 -B "$SOURCE_SEAL_HELPER" verify' in build
    assert "trusted-controller-evidence" not in build
    assert 'sudo -n install -m 0444 -o root -g root "$anchor_temp"' in materialize
    assert '"archiveValidationSha256"' in materialize
    assert '"sourceSealSha256"' in materialize
    assert "reference_seal_digest" in verify
    assert "candidate_seal_digest" in verify
    assert 'expected_roots = {' in verify
    assert '"reference": (reference, 0, 0, 0o700)' in verify
    assert '"candidate": (' in verify
    assert 'anchor["roots"].get(label) != actual' in verify
    assert '"schemaVersion": 3' in verify
    assert '"copyMethod": "independent-sealed-archive-extraction"' in verify
    assert controller.count("verify_trusted_candidate_integrity") == 2
    assert (
        'verify_trusted_candidate_integrity "$BUILD_INTEGRITY_EVIDENCE_FILE"'
        in controller
    )


def test_feature_canary_holds_lock_and_cleans_only_its_namespace() -> None:
    script = CONTROLLER.read_text(encoding="utf-8")

    assert 'RUN_KEY="${CANARY_COMMIT_SHA:0:12}-${CANARY_RUN_ID}"' in script
    assert 'REFERENCE_ROOT="$CANARY_ROOT/runtime/$RUN_KEY.reference"' in script
    assert 'RUNTIME_ROOT="$CANARY_ROOT/runtime/$RUN_KEY"' in script
    assert (
        'SUPERVISOR_UNIT="jato-feature-canary-supervisor-$RUN_KEY.service"'
        in script
    )
    assert (
        'CONTROLLER_UNIT="jato-feature-canary-controller-$RUN_KEY.service"'
        in script
    )
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
    assert "CANARY_SERVICE_START_ATTEMPTED" not in script
    assert "CANARY_BUILD_START_ATTEMPTED" not in script
    assert "refusing to stop a unit without exact canary identity" in script
    assert 'sudo -n systemctl stop "$unit"' in script
    assert 'sudo -n rm -rf --one-file-system "$canary_tree"' in script
    assert "sudo -n systemctl stop \"$ACTIVE_UNIT\"" not in script
    assert "sudo -n systemctl restart \"$ACTIVE_UNIT\"" not in script
    assert "sudo -n systemctl enable" not in script
    assert "sudo -n systemctl disable" not in script
    assert "only supervisor reconciliation may write a terminal canary receipt" in script
    assert "--terminal-writer supervisor_reconcile" in script
    assert "--writer-invocation-id" in script
    assert "return 97" in script
    supervisor_body = _shell_function(script, "run_canary_supervisor")
    assert supervisor_body.index("acquire_canary_production_lock") < (
        supervisor_body.index(
            "record_checkpoint controller_unit_started in_progress",
        )
    )
    assert supervisor_body.index(
        "record_checkpoint controller_unit_started in_progress",
    ) < (
        supervisor_body.index("run_canary_controller_unit")
    )
    assert supervisor_body.index("run_canary_controller_unit") < (
        supervisor_body.index("quiesce_canary_controller_unit")
    )
    assert supervisor_body.index("quiesce_canary_controller_unit") < (
        supervisor_body.index('bash "$CONTROL_SCRIPT" reconcile')
    )
    assert "business canary will not be rerun" in supervisor_body
    run_body = _shell_function(script, "run_canary_controller")
    assert "acquire_canary_production_lock" not in run_body
    assert run_body.index("assert_supervisor_production_lock") < run_body.index(
        'capture_snapshot "$BEFORE_SNAPSHOT"',
    )
    assert run_body.index('capture_snapshot "$BEFORE_SNAPSHOT"') < run_body.index(
        "run_build_scope",
    )
    assert "record_checkpoint controller_completed completed" in run_body
    finalizer_body = _shell_function(script, "finalize_canary")
    assert finalizer_body.index("cleanup_candidate") < finalizer_body.index(
        "wait_for_candidate_port_release",
    )
    assert finalizer_body.index(
        "wait_for_candidate_port_release",
    ) < finalizer_body.index('capture_snapshot "$AFTER_SNAPSHOT"')
    assert "verify_retained_control_bundle" not in finalizer_body
    assert '"$CANARY_GUARD" finalize' not in finalizer_body
    cleanup_body = _shell_function(script, "cleanup_candidate")
    assert "$CONTROL_ROOT" not in cleanup_body
    reconcile_body = _shell_function(script, "reconcile_canary_controller")
    assert reconcile_body.index("verify_existing_receipt") < reconcile_body.index(
        "verify_retained_control_bundle",
    )
    writer_body = _shell_function(script, "write_terminal_receipt")
    assert '"$CANARY_GUARD" finalize' in writer_body
    assert '[[ "$CANARY_MODE" != "reconcile" ]]' in writer_body
    assert script.count('"$CANARY_GUARD" finalize') == 1
    assert "SO_REUSEADDR, 0" in GUARD_PATH.read_text(encoding="utf-8")
    retained_control_body = _shell_function(
        script,
        "verify_retained_control_bundle",
    )
    assert "rm -rf" not in retained_control_body
    assert "sudo -n rm" not in retained_control_body


def test_candidate_start_permit_is_atomic_and_precedes_application_exec() -> None:
    script = CONTROLLER.read_text(encoding="utf-8")
    authorize = _shell_function(script, "authorize_candidate_runtime")
    for token in (
        'systemctl show "$SERVICE_UNIT"',
        'Path("/proc") / main_pid',
        '.read_bytes().split(b"\\0")',
        "pre-permit candidate wrapper already changed or escaped",
        "StopPropagatedFrom",
        "assert_supervisor_generation",
        'persist_candidate_start_permit "$candidate_invocation_id"',
    ):
        assert token in authorize

    publisher = _shell_function(script, "persist_candidate_start_permit")
    assert "/dev/stdin" not in publisher
    assert publisher.count("assert_supervisor_generation") == 3
    first_live = publisher.index("assert_supervisor_generation")
    install = publisher.index("install -m 0444 -o root -g root")
    second_live = publisher.index(
        "assert_supervisor_generation",
        first_live + 1,
    )
    publish = publisher.index("mv -T")
    verify = publisher.index(
        'assert_candidate_start_permit "$candidate_invocation_id"',
    )
    third_live = publisher.index(
        "assert_supervisor_generation",
        second_live + 1,
    )
    assert first_live < install < second_live < publish < verify < third_live

    runtime = _shell_function(script, "run_candidate_runtime")
    assert "systemctl" not in runtime
    assert "read_live_supervisor_invocation_id" not in runtime
    assert "assert_supervisor_generation" not in runtime
    assert runtime.index("\n  expected_argv=(\n") < runtime.index(
        "wait_for_candidate_start_permit",
    ) < runtime.index('exec "${expected_argv[@]}"')

    reconcile = _shell_function(script, "reconcile_canary_controller")
    assert reconcile.index("cleanup_candidate_start_permit_temp") < (
        reconcile.index('if [[ -f "$RECEIPT_FILE"')
    )


def test_supervisor_launcher_returns_without_waiting_on_systemd(
    tmp_path: Path,
) -> None:
    calls = tmp_path / "systemd-run.log"
    harness = tmp_path / "launch-harness.sh"
    harness.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -Eeuo pipefail
            source "$1"
            calls="$2"
            SUPERVISOR_UNIT="jato-feature-canary-supervisor-aaaaaaaaaaaa-run.service"
            CONTROL_ROOT="/opt/jato-canary/control/aaaaaaaaaaaa-run"
            CONTROL_SCRIPT="$CONTROL_ROOT/03_Scripts/deploy/tencent_feature_candidate_canary.sh"
            STAGED_SOURCE_ARCHIVE="/opt/jato-canary/sources/aaaaaaaaaaaa-run.tar.gz"
            CANARY_COMMIT_SHA="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            CANARY_SOURCE_SHA256="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            CANARY_SOURCE_BYTES="123"
            CANARY_RUN_ID="run"
            CANARY_BRANCH="codex/test"
            CANARY_REPOSITORY="tristan419/JATO_Analysis_System"
            RUN_KEY="aaaaaaaaaaaa-run"
            DEPLOY_STATE_DIR="$HOME/.local/state/jato-production-release"
            CANARY_INITIAL_LOCK_PATH=""
            assert_supervisor_unit_available() { :; }
            sudo() {
              [[ "$1" == "-n" ]] && shift
              printf '%s\\n' "$*" >>"$calls"
              case " $* " in
                *" --wait "*|*" --pipe "*) sleep 5 ;;
              esac
            }
            start_canary_supervisor
            """
        ),
        encoding="utf-8",
    )
    result = subprocess.run(
        ["bash", str(harness), str(CONTROLLER), str(calls)],
        capture_output=True,
        text=True,
        timeout=2,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    invocation = calls.read_text(encoding="utf-8")
    assert "systemd-run" in invocation
    assert "--wait" not in invocation
    assert "--pipe" not in invocation
    assert (
        "--unit=jato-feature-canary-supervisor-aaaaaaaaaaaa-run.service"
        in invocation
    )
    assert "--service-type=exec" in invocation
    assert (
        "Restart=on-failure" in invocation
        and "StartLimitIntervalSec=0" in invocation
    )
    assert invocation.rstrip().endswith(
        "tencent_feature_candidate_canary.sh supervisor",
    )


def test_launch_pins_the_canonical_lock_identity_before_supervisor_start(
    tmp_path: Path,
) -> None:
    calls = tmp_path / "launch.log"
    harness = tmp_path / "launch-lock-harness.sh"
    harness.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -Eeuo pipefail
            unset JATO_PRODUCTION_DEPLOY_LOCK_PATH
            unset CANARY_INITIAL_LOCK_PATH
            source "$1" launch
            calls="$2"
            DEPLOY_STATE_DIR="$HOME/.local/state/jato-production-release"
            id() {
              case "$1" in
                -u|-g) printf '1000\n' ;;
                *) return 2 ;;
              esac
            }
            validate_static_contract() { :; }
            initialize_paths() { :; }
            ensure_canary_roots() { :; }
            stage_canary_inputs() { :; }
            record_checkpoint() { :; }
            start_canary_supervisor() {
              printf 'initial=%s\\n' "$CANARY_INITIAL_LOCK_PATH" >>"$calls"
              printf 'runtime=%s\\n' "$JATO_PRODUCTION_DEPLOY_LOCK_PATH" >>"$calls"
            }
            launch_canary
            """
        ),
        encoding="utf-8",
    )
    result = subprocess.run(
        ["bash", str(harness), str(CONTROLLER), str(calls)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    canonical = f"{Path.home()}/.local/state/jato-production-release/production-deploy.lock"
    assert calls.read_text(encoding="utf-8").splitlines() == [
        f"initial={canonical}",
        f"runtime={canonical}",
    ]


@pytest.mark.parametrize(
    ("scenario", "expected_ok"),
    (
        ("empty", True),
        ("matching", True),
        ("uid-conflict", False),
        ("gid-conflict", False),
        ("root", False),
    ),
)
def test_launch_pins_only_the_actual_non_root_deploy_identity(
    tmp_path: Path,
    scenario: str,
    expected_ok: bool,
) -> None:
    harness = tmp_path / "deploy-identity-pin-harness.sh"
    harness.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -Eeuo pipefail
            scenario="$2"
            stub_uid="1000"
            stub_gid="1000"
            case "$scenario" in
              matching)
                CANARY_DEPLOY_UID="1000"
                CANARY_DEPLOY_GID="1000"
                ;;
              uid-conflict)
                CANARY_DEPLOY_UID="1001"
                CANARY_DEPLOY_GID="1000"
                ;;
              gid-conflict)
                CANARY_DEPLOY_UID="1000"
                CANARY_DEPLOY_GID="1001"
                ;;
              root)
                stub_uid="0"
                stub_gid="0"
                ;;
            esac
            source "$1" launch
            id() {
              case "$1" in
                -u) printf '%s\n' "$stub_uid" ;;
                -g) printf '%s\n' "$stub_gid" ;;
                *) return 2 ;;
              esac
            }
            pin_canary_deploy_identity
            printf '%s:%s\n' "$CANARY_DEPLOY_UID" "$CANARY_DEPLOY_GID"
            """
        ),
        encoding="utf-8",
    )
    result = subprocess.run(
        ["bash", str(harness), str(CONTROLLER), scenario],
        capture_output=True,
        text=True,
        check=False,
    )
    assert (result.returncode == 0) is expected_ok, result.stderr
    if expected_ok:
        assert result.stdout.strip() == "1000:1000"


@pytest.mark.parametrize(
    ("mode", "scenario", "expected_ok"),
    (
        ("launch", "exact", True),
        ("supervisor", "exact", True),
        ("controller", "exact", True),
        ("build", "exact", True),
        ("reconcile", "exact", True),
        ("controller", "uid-drift", False),
        ("build", "gid-drift", False),
        ("reconcile", "root", False),
    ),
)
def test_control_modes_reject_identity_drift(
    tmp_path: Path,
    mode: str,
    scenario: str,
    expected_ok: bool,
) -> None:
    harness = tmp_path / "control-identity-harness.sh"
    harness.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -Eeuo pipefail
            mode="$2"
            scenario="$3"
            source "$1" "$mode"
            CANARY_DEPLOY_UID="1000"
            CANARY_DEPLOY_GID="1000"
            stub_uid="1000"
            stub_gid="1000"
            case "$scenario" in
              uid-drift) stub_uid="1001" ;;
              gid-drift) stub_gid="1001" ;;
              root)
                stub_uid="0"
                stub_gid="0"
                ;;
            esac
            id() {
              case "$1" in
                -u) printf '%s\n' "$stub_uid" ;;
                -g) printf '%s\n' "$stub_gid" ;;
                *) return 2 ;;
              esac
            }
            validate_canary_deploy_identity
            """
        ),
        encoding="utf-8",
    )
    result = subprocess.run(
        ["bash", str(harness), str(CONTROLLER), mode, scenario],
        capture_output=True,
        text=True,
        check=False,
    )
    assert (result.returncode == 0) is expected_ok, result.stderr


@pytest.mark.parametrize(
    ("scenario", "expected_ok"),
    (
        ("distinct", True),
        ("equal-uid", False),
        ("equal-gid", False),
        ("missing-uid", False),
        ("missing-gid", False),
        ("malformed-uid", False),
        ("malformed-gid", False),
        ("root", False),
    ),
)
def test_dynamic_runtime_identity_must_be_distinct_and_well_formed(
    tmp_path: Path,
    scenario: str,
    expected_ok: bool,
) -> None:
    harness = tmp_path / "runtime-identity-harness.sh"
    harness.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -Eeuo pipefail
            scenario="$2"
            source "$1" runtime
            CANARY_DEPLOY_UID="1000"
            CANARY_DEPLOY_GID="1000"
            stub_uid="2000"
            stub_gid="3000"
            case "$scenario" in
              equal-uid) stub_uid="1000" ;;
              equal-gid) stub_gid="1000" ;;
              missing-uid) CANARY_DEPLOY_UID="" ;;
              missing-gid) CANARY_DEPLOY_GID="" ;;
              malformed-uid) CANARY_DEPLOY_UID="uid-1000" ;;
              malformed-gid) CANARY_DEPLOY_GID="gid-1000" ;;
              root)
                stub_uid="0"
                stub_gid="0"
                ;;
            esac
            id() {
              case "$1" in
                -u) printf '%s\n' "$stub_uid" ;;
                -g) printf '%s\n' "$stub_gid" ;;
                *) return 2 ;;
              esac
            }
            validate_canary_deploy_identity
            """
        ),
        encoding="utf-8",
    )
    result = subprocess.run(
        ["bash", str(harness), str(CONTROLLER), scenario],
        capture_output=True,
        text=True,
        check=False,
    )
    assert (result.returncode == 0) is expected_ok, result.stderr


@pytest.mark.parametrize(
    ("wrong_pinned_gid", "expected_ok"),
    ((False, True), (True, False)),
)
def test_canary_source_parent_uses_the_pinned_deploy_gid(
    tmp_path: Path,
    wrong_pinned_gid: bool,
    expected_ok: bool,
) -> None:
    current_uid = os.getuid()
    current_gid = os.getgid()
    patched = tmp_path / "candidate-canary.sh"
    source = CONTROLLER.read_text(encoding="utf-8")
    replacements = {
        "Path(sys.argv[1]): (0, 0, 0o755),": (
            f"Path(sys.argv[1]): ({current_uid}, {current_gid}, 0o755),"
        ),
        "Path(sys.argv[2]): (0, 0, 0o755),": (
            f"Path(sys.argv[2]): ({current_uid}, {current_gid}, 0o755),"
        ),
        "Path(sys.argv[3]): (0, 0, 0o755),": (
            f"Path(sys.argv[3]): ({current_uid}, {current_gid}, 0o755),"
        ),
        "Path(sys.argv[4]): (0, deploy_gid, 0o750),": (
            f"Path(sys.argv[4]): ({current_uid}, deploy_gid, 0o750),"
        ),
    }
    for original, replacement in replacements.items():
        assert source.count(original) == 1
        source = source.replace(original, replacement)
    patched.write_text(source, encoding="utf-8")

    root = tmp_path / "canary"
    for path, mode in (
        (root, 0o755),
        (root / "runtime", 0o755),
        (root / "control", 0o755),
        (root / "sources", 0o750),
    ):
        path.mkdir(exist_ok=True)
        path.chmod(mode)
    deploy_gid = current_gid + 1 if wrong_pinned_gid else current_gid
    harness = tmp_path / "parent-owner-harness.sh"
    harness.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -Eeuo pipefail
            source "$1" supervisor
            CANARY_ROOT="$2"
            CANARY_DEPLOY_GID="$3"
            verify_canary_parent_roots
            """
        ),
        encoding="utf-8",
    )
    result = subprocess.run(
        [
            "bash",
            str(harness),
            str(patched),
            str(root),
            str(deploy_gid),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert (result.returncode == 0) is expected_ok, result.stderr


def test_dynamic_runtime_can_lstat_root_owned_canary_parents() -> None:
    if not sys.platform.startswith("linux"):
        pytest.skip("requires Linux setpriv permission semantics")

    sudo = shutil.which("sudo")
    setpriv = shutil.which("setpriv")
    if sudo is None or setpriv is None:
        pytest.skip("requires sudo and setpriv")

    sudo_probe = subprocess.run(
        [sudo, "-n", "true"],
        capture_output=True,
        text=True,
        check=False,
    )
    if sudo_probe.returncode != 0:
        pytest.skip("requires non-interactive sudo")

    deploy_gid = 61001
    runtime_uid = 61002
    runtime_gid = 61003
    identity_probe = subprocess.run(
        [
            sudo,
            "-n",
            setpriv,
            f"--reuid={runtime_uid}",
            f"--regid={runtime_gid}",
            "--clear-groups",
            "id",
            "-u",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if (
        identity_probe.returncode != 0
        or identity_probe.stdout.strip() != str(runtime_uid)
    ):
        pytest.skip("setpriv cannot create the isolated runtime identity")

    permission_root = Path(
        tempfile.mkdtemp(prefix="jato-canary-parent-permissions-", dir="/tmp")
    )
    assert permission_root.parent == Path("/tmp")
    assert permission_root.name.startswith(
        "jato-canary-parent-permissions-"
    )
    canary_root = permission_root / "canary"
    controller_copy = permission_root / "candidate-canary.sh"
    harness = permission_root / "verify-parents.sh"
    shutil.copyfile(CONTROLLER, controller_copy)
    harness.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -Eeuo pipefail
            source "$1" runtime
            CANARY_ROOT="$2"
            CANARY_DEPLOY_GID="$3"
            verify_canary_parent_roots
            python3 -B - "$CANARY_ROOT/sources" <<'PY'
            import os
            import stat
            import sys

            metadata = os.lstat(sys.argv[1])
            print(
                "lstat-ok:"
                f"{os.getuid()}:{os.getgid()}:"
                f"{metadata.st_uid}:{metadata.st_gid}:"
                f"{stat.S_IMODE(metadata.st_mode):04o}"
            )
            PY
            """
        ),
        encoding="utf-8",
    )

    try:
        for path in (
            canary_root,
            canary_root / "runtime",
            canary_root / "control",
            canary_root / "sources",
        ):
            path.mkdir(exist_ok=True)

        setup = subprocess.run(
            [
                sudo,
                "-n",
                "chown",
                "0:0",
                str(permission_root),
                str(controller_copy),
                str(harness),
                str(canary_root),
                str(canary_root / "runtime"),
                str(canary_root / "control"),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        assert setup.returncode == 0, setup.stderr
        setup = subprocess.run(
            [
                sudo,
                "-n",
                "chown",
                f"0:{deploy_gid}",
                str(canary_root / "sources"),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        assert setup.returncode == 0, setup.stderr
        setup = subprocess.run(
            [
                sudo,
                "-n",
                "chmod",
                "0755",
                str(permission_root),
                str(controller_copy),
                str(harness),
                str(canary_root),
                str(canary_root / "runtime"),
                str(canary_root / "control"),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        assert setup.returncode == 0, setup.stderr
        setup = subprocess.run(
            [
                sudo,
                "-n",
                "chmod",
                "0750",
                str(canary_root / "sources"),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        assert setup.returncode == 0, setup.stderr

        result = subprocess.run(
            [
                sudo,
                "-n",
                setpriv,
                f"--reuid={runtime_uid}",
                f"--regid={runtime_gid}",
                "--clear-groups",
                "bash",
                str(harness),
                str(controller_copy),
                str(canary_root),
                str(deploy_gid),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == (
            f"lstat-ok:{runtime_uid}:{runtime_gid}:0:{deploy_gid}:0750"
        )
    finally:
        cleanup = subprocess.run(
            [
                sudo,
                "-n",
                "rm",
                "-rf",
                "--one-file-system",
                "--",
                str(permission_root),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        assert cleanup.returncode == 0, cleanup.stderr
        assert not permission_root.exists()


def test_each_transient_scope_propagates_pinned_identity_exactly_once() -> None:
    script = CONTROLLER.read_text(encoding="utf-8")
    control_environment = _shell_function(
        script,
        "build_canary_control_environment",
    )
    for name in ("UID", "GID"):
        assignment = (
            f'"CANARY_DEPLOY_{name}=$CANARY_DEPLOY_{name}"'
        )
        assert control_environment.count(assignment) == 1

    for function_name in (
        "start_canary_supervisor",
        "run_canary_controller_unit",
    ):
        scope = _shell_function(script, function_name)
        assert scope.count("build_canary_control_environment") == 1
        assert scope.count('--uid="$CANARY_DEPLOY_UID"') == 1
        assert scope.count('--gid="$CANARY_DEPLOY_GID"') == 1

    build = _shell_function(script, "run_build_scope")
    assert build.count('--uid="$CANARY_DEPLOY_UID"') == 1
    assert build.count('--gid="$CANARY_DEPLOY_GID"') == 1
    assert (
        build.count(
            '--setenv="CANARY_DEPLOY_UID=$CANARY_DEPLOY_UID"',
        )
        == 1
    )
    assert (
        build.count(
            '--setenv="CANARY_DEPLOY_GID=$CANARY_DEPLOY_GID"',
        )
        == 1
    )

    runtime = _shell_function(script, "start_candidate_service")
    assert runtime.count('--property="DynamicUser=yes"') == 1
    assert '--uid="$CANARY_DEPLOY_UID"' not in runtime
    assert '--gid="$CANARY_DEPLOY_GID"' not in runtime
    assert "SupplementaryGroups" not in runtime
    assert (
        runtime.count(
            '--setenv="CANARY_DEPLOY_UID=$CANARY_DEPLOY_UID"',
        )
        == 1
    )
    assert (
        runtime.count(
            '--setenv="CANARY_DEPLOY_GID=$CANARY_DEPLOY_GID"',
        )
        == 1
    )


@pytest.mark.parametrize(
    "failure_step",
    (
        "ensure_canary_roots",
        "stage_canary_inputs",
        "record_checkpoint",
        "start_canary_supervisor",
    ),
)
def test_pre_supervisor_launch_failure_removes_exact_run_namespace(
    tmp_path: Path,
    failure_step: str,
) -> None:
    harness = tmp_path / "launch-rollback-harness.sh"
    harness.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -Eeuo pipefail
            source "$1" launch
            root="$2"
            failure_step="$3"
            CANARY_ROOT="$root/canary"
            CANARY_STATE_ROOT="$root/state"
            CANARY_COMMIT_SHA="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            CANARY_RUN_ID="rollback"
            DEPLOY_STATE_DIR="$root/deploy-state"
            id() {
              case "$1" in
                -u|-g) printf '1000\n' ;;
                *) return 2 ;;
              esac
            }
            mkdir -p \
              "$CANARY_ROOT/runtime" "$CANARY_ROOT/control" \
              "$CANARY_ROOT/sources" \
              "$CANARY_STATE_ROOT/checkpoints" \
              "$CANARY_STATE_ROOT/receipts" \
              "$CANARY_STATE_ROOT/evidence" \
              "$CANARY_STATE_ROOT/snapshots"
            sudo() {
              [[ "${1:-}" == "-n" ]] && shift
              if [[ "${1:-}" == "rm" ]]; then
                shift
                filtered=()
                for argument in "$@"; do
                  [[ "$argument" == "--one-file-system" ]] && continue
                  filtered+=("$argument")
                done
                command rm "${filtered[@]}"
                return
              fi
              "$@"
            }
            validate_static_contract() { :; }
            verify_canary_parent_roots() { :; }
            cleanup_canary_state_run() {
              local path=""
              for path in \
                "$CHECKPOINT_FILE" "$RECEIPT_FILE" "$EVIDENCE_FILE" \
                "$BEFORE_SNAPSHOT" "$AFTER_SNAPSHOT" \
                "$SUPERVISOR_GENERATION_SOURCE_FILE" \
                "$CANDIDATE_START_PERMIT_SOURCE_FILE"; do
                command rm -f -- "$path"
              done
            }
            start_canary_supervisor() {
              [[ "$failure_step" != "start_canary_supervisor" ]]
            }
            systemctl() {
              printf 'not-found\\n'
            }
            create_run_residue() {
              mkdir -p "$REFERENCE_ROOT" "$RUNTIME_ROOT" "$CONTROL_ROOT"
              printf 'sealed\\n' >"$STAGED_SOURCE_ARCHIVE"
            }
            ensure_canary_roots() {
              if [[ "$failure_step" == "ensure_canary_roots" ]]; then
                create_run_residue
                return 1
              fi
            }
            stage_canary_inputs() {
              create_run_residue
              [[ "$failure_step" != "stage_canary_inputs" ]]
            }
            record_checkpoint() {
              printf 'checkpoint\\n' >"$CHECKPOINT_FILE"
              [[ "$failure_step" != "record_checkpoint" ]]
            }
            set +e
            launch_canary
            launch_rc=$?
            set -e
            [[ "$launch_rc" -eq 1 ]]
            for path in \
              "$REFERENCE_ROOT" "$RUNTIME_ROOT" "$CONTROL_ROOT" \
              "$STAGED_SOURCE_ARCHIVE" "$CHECKPOINT_FILE" "$RECEIPT_FILE" \
              "$EVIDENCE_FILE" "$BEFORE_SNAPSHOT" "$AFTER_SNAPSHOT" \
              "$SUPERVISOR_GENERATION_SOURCE_FILE" \
              "$CANDIDATE_START_PERMIT_SOURCE_FILE"; do
              [[ ! -e "$path" && ! -L "$path" ]]
            done
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
            failure_step,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr + result.stdout


def test_controller_delegates_launch_state_cleanup_to_canonical_guard() -> None:
    script = CONTROLLER.read_text(encoding="utf-8")
    cleanup = _shell_function(script, "cleanup_canary_state_run")
    assert '"$CANARY_GUARD" cleanup-launch-state' in cleanup
    assert '--state-root "$CANARY_STATE_ROOT"' in cleanup
    assert '--run-key "$RUN_KEY"' in cleanup
    guard = GUARD_PATH.read_text(encoding="utf-8")
    assert 'arguments.state_root != Path("/var/lib/jato-canary")' in guard
    assert "canary launch state cleanup root is not reviewed" in guard


def test_ambiguous_supervisor_launch_retains_control_bundle(
    tmp_path: Path,
) -> None:
    script = CONTROLLER.read_text(encoding="utf-8")
    launch = _shell_function(script, "launch_canary")
    assert launch.index("verify_supervisor_unit_absent") < launch.index(
        "cleanup_pre_supervisor_launch",
        launch.index("start_canary_supervisor"),
    )
    verifier = _shell_function(script, "verify_supervisor_unit_absent")
    assert '[[ "$load_state" != "not-found" ]]' in verifier
    assert "unit state is not safely absent" in verifier


def test_controller_runs_in_bounded_transient_systemd_unit(
    tmp_path: Path,
) -> None:
    calls = tmp_path / "systemd-run.log"
    harness = tmp_path / "controller-unit-harness.sh"
    harness.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -Eeuo pipefail
            source "$1"
            calls="$2"
            CANARY_COMMIT_SHA="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            CANARY_SOURCE_SHA256="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            CANARY_SOURCE_BYTES="123"
            CANARY_RUN_ID="run"
            CANARY_BRANCH="codex/test"
            CANARY_REPOSITORY="tristan419/JATO_Analysis_System"
            DEPLOY_STATE_DIR="$HOME/.local/state/jato-production-release"
            CANARY_INITIAL_LOCK_PATH="$DEPLOY_STATE_DIR/production-deploy.lock"
            JATO_PRODUCTION_DEPLOY_LOCK_PATH="$CANARY_INITIAL_LOCK_PATH"
            CANARY_SUPERVISOR_INVOCATION_ID="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            initialize_paths
            assert_controller_unit_available() { :; }
            sudo() {
              [[ "$1" == "-n" ]] && shift
              printf '%s\\n' "$*" >>"$calls"
            }
            run_canary_controller_unit
            """
        ),
        encoding="utf-8",
    )
    result = subprocess.run(
        ["bash", str(harness), str(CONTROLLER), str(calls)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    invocation = calls.read_text(encoding="utf-8")
    assert "--wait" in invocation and "--pipe" in invocation
    assert "--collect" in invocation
    assert (
        "--unit=jato-feature-canary-controller-aaaaaaaaaaaa-run.service"
        in invocation
    )
    for relation in ("StopPropagatedFrom", "After"):
        assert (
            f"{relation}=jato-feature-canary-supervisor-"
            "aaaaaaaaaaaa-run.service"
        ) in invocation
    assert "BindsTo=" not in invocation
    assert "PartOf=" not in invocation
    assert "RuntimeMaxSec=2400s" in invocation
    assert "TimeoutStopSec=900s" in invocation
    assert "KillMode=control-group" in invocation
    assert "SendSIGKILL=yes" in invocation
    assert "Restart=no" in invocation
    assert "MemoryHigh=256M" in invocation
    assert "MemoryMax=512M" in invocation
    assert "MemorySwapMax=0" in invocation
    assert "TasksMax=64" in invocation
    assert invocation.rstrip().endswith(
        "tencent_feature_candidate_canary.sh controller",
    )


@pytest.mark.parametrize(
    ("scenario", "expected_ok", "expected_stop"),
    (
        ("absent-initial", True, False),
        ("collected-after-stop", True, True),
        ("collected-before-stop", True, True),
        ("identity-recreated", False, True),
    ),
)
def test_verified_unit_stop_tolerates_collect_but_rejects_name_reuse(
    tmp_path: Path,
    scenario: str,
    expected_ok: bool,
    expected_stop: bool,
) -> None:
    calls = tmp_path / "calls.log"
    state = tmp_path / "state"
    harness = tmp_path / "stop-unit-harness.sh"
    harness.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -Eeuo pipefail
            source "$1"
            calls="$2"
            state="$3"
            scenario="$4"
            RUN_KEY="aaaaaaaaaaaa-run"
            SUPERVISOR_UNIT="jato-feature-canary-supervisor-$RUN_KEY.service"
            BUILD_UNIT="jato-feature-canary-build-$RUN_KEY.service"
            CONTROL_SCRIPT="/opt/jato-canary/control/$RUN_KEY/03_Scripts/deploy/tencent_feature_candidate_canary.sh"
            BASH_BIN="$(command -v bash)"
            systemctl() {
              local unit="$2"
              shift 2
              if [[ "$scenario" == "absent-initial" && ! -e "$state" ]]; then
                printf 'LoadState=not-found\\n'
                return 0
              fi
              if [[ "$*" == *"--value"* ]]; then
                [[ -e "$state" ]] && printf 'loaded\\n' || printf 'not-found\\n'
                return 0
              fi
              if [[ "$*" == *"UnitFileState"* ]]; then
                printf 'LoadState=loaded\\n'
                printf 'ActiveState=active\\n'
                printf 'UnitFileState=transient\\n'
                printf 'FragmentPath=/run/systemd/transient/%s\\n' "$unit"
                printf 'ExecStart={ path=%s ; argv[]=%s %s build ; ignore_errors=no ; }\\n' \
                  "$BASH_BIN" "$BASH_BIN" "$CONTROL_SCRIPT"
                printf 'Environment=RUN_KEY=%s\\n' "$RUN_KEY"
                printf 'ControlGroup=/system.slice/%s\\n' "$unit"
                printf 'BindsTo=\\n'
                printf 'PartOf=\\n'
                printf 'After=%s\\n' "$SUPERVISOR_UNIT"
                printf 'StopPropagatedFrom=%s\\n' "$SUPERVISOR_UNIT"
                printf 'InvocationID=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\\n'
                if [[ "$scenario" == "collected-before-stop" ]]; then
                  rm -f "$state"
                fi
                return 0
              fi
              if [[ -e "$state" ]]; then
                printf 'LoadState=loaded\\n'
                printf 'ActiveState=inactive\\n'
                if [[ "$scenario" == "identity-recreated" ]]; then
                  printf 'InvocationID=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\\n'
                else
                  printf 'InvocationID=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\\n'
                fi
              else
                printf 'LoadState=not-found\\n'
              fi
            }
            sudo() {
              [[ "$1" == "-n" ]] && shift
              if [[ "$1" == "systemctl" && "$2" == "stop" ]]; then
                printf 'stop\\n' >>"$calls"
                if [[ "$scenario" == "collected-before-stop" ]]; then
                  return 5
                fi
                if [[ "$scenario" != "identity-recreated" ]]; then
                  rm -f "$state"
                fi
                return 0
              fi
              if [[ "$1" == "systemctl" && "$2" == "reset-failed" ]]; then
                return 0
              fi
              return 1
            }
            sleep() { :; }
            [[ "$scenario" == "absent-initial" ]] || : >"$state"
            stop_verified_transient_unit \
              "$BUILD_UNIT" \
              "RUN_KEY=$RUN_KEY" \
              "$BASH_BIN" "$CONTROL_SCRIPT" build
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
            str(state),
            scenario,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert (result.returncode == 0) is expected_ok, result.stderr
    stopped = calls.exists() and "stop" in calls.read_text(encoding="utf-8")
    assert stopped is expected_stop


@pytest.mark.parametrize(
    ("scenario", "mode", "expected_ok"),
    (
        ("exact", "supervisor", True),
        ("exact", "controller", True),
        ("exact", "reconcile", True),
        ("bad-cgroup", "controller", False),
        ("bad-env-prefix", "controller", False),
        ("duplicate-env", "controller", False),
        ("extra-env", "controller", False),
        ("bad-start-path", "controller", False),
        ("bad-start-argument", "controller", False),
        ("inactive-controller", "controller", False),
        ("inactive-reconcile", "reconcile", False),
    ),
)
def test_supervisor_scope_requires_exact_identity_and_cgroup(
    tmp_path: Path,
    scenario: str,
    mode: str,
    expected_ok: bool,
) -> None:
    cgroup_root = tmp_path / "cgroup"
    patched = tmp_path / "candidate-canary.sh"
    source = CONTROLLER.read_text(encoding="utf-8")
    source = source.replace(
        'Path("/sys/fs/cgroup")',
        f"Path({str(cgroup_root)!r})",
    )
    patched.write_text(source, encoding="utf-8")
    harness = tmp_path / "scope-harness.sh"
    harness.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -Eeuo pipefail
            source "$1"
            cgroup_root="$2"
            scenario="$3"
            CANARY_COMMIT_SHA="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            CANARY_SOURCE_SHA256="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            CANARY_SOURCE_BYTES="123"
            CANARY_RUN_ID="run"
            CANARY_BRANCH="codex/test"
            CANARY_REPOSITORY="tristan419/JATO_Analysis_System"
            DEPLOY_STATE_DIR="$HOME/.local/state/jato-production-release"
            initialize_paths
            CANARY_MODE="$4"
            mkdir -p "$cgroup_root/system.slice/$SUPERVISOR_UNIT"
            printf '%s\\n' "$$" >"$cgroup_root/system.slice/$SUPERVISOR_UNIT/cgroup.procs"
            systemctl() {
              local bash_bin=""
              local control_group="/system.slice/$SUPERVISOR_UNIT"
              local active_state="active"
              local start_path="$CONTROL_SCRIPT"
              local start_argument="supervisor"
              local environment="${CANARY_CONTROL_ENVIRONMENT[*]}"
              bash_bin="$(command -v bash)"
              if [[ "$CANARY_MODE" == "reconcile" ]]; then
                active_state="deactivating"
              fi
              case "$scenario" in
                bad-cgroup)
                control_group="/system.slice/not-the-supervisor.service"
                  ;;
                bad-env-prefix)
                  environment="${environment/RUN_KEY=$RUN_KEY/BADRUN_KEY=$RUN_KEY}"
                  ;;
              duplicate-env)
                environment="$environment RUN_KEY=attacker"
                ;;
              extra-env)
                environment="$environment UNREVIEWED=1"
                ;;
                bad-start-path)
                  start_path="${CONTROL_SCRIPT}-attacker"
                  ;;
                bad-start-argument)
                  start_argument="controller-extra"
                  ;;
                inactive-controller)
                  active_state="inactive"
                  ;;
                inactive-reconcile)
                  active_state="inactive"
                  ;;
              esac
              printf 'LoadState=loaded\\n'
              printf 'ActiveState=%s\\n' "$active_state"
              printf 'UnitFileState=transient\\n'
              printf 'FragmentPath=/run/systemd/transient/%s\\n' "$SUPERVISOR_UNIT"
              printf 'ExecStart={ path=%s ; argv[]=%s %s %s ; ignore_errors=no ; }\\n' \
                "$bash_bin" "$bash_bin" "$start_path" "$start_argument"
              printf 'Environment=%s\\n' "$environment"
              printf 'ControlGroup=%s\\n' "$control_group"
              printf 'MainPID=%s\\n' "$$"
              printf 'MemoryHigh=268435456\\n'
              printf 'MemoryMax=536870912\\n'
              printf 'MemorySwapMax=0\\n'
              printf 'TasksMax=64\\n'
              printf 'KillMode=control-group\\n'
              printf 'Restart=on-failure\\n'
            }
            assert_supervisor_scope
            """
        ),
        encoding="utf-8",
    )
    result = subprocess.run(
        [
            "bash",
            str(harness),
            str(patched),
            str(cgroup_root),
            scenario,
            mode,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if expected_ok:
        assert result.returncode == 0, result.stderr
    else:
        assert result.returncode != 0, scenario


@pytest.mark.parametrize(
    ("scenario", "expected_ok"),
    (
        ("exact", True),
        ("bad-cgroup", False),
        ("bad-env-prefix", False),
        ("duplicate-env", False),
        ("extra-env", False),
        ("bad-start-path", False),
        ("bad-start-argument", False),
        ("bad-runtime-timeout", False),
        ("bad-stop-timeout", False),
        ("bad-supervisor-binding", False),
    ),
)
def test_controller_scope_requires_exact_unit_cgroup_and_timeouts(
    tmp_path: Path,
    scenario: str,
    expected_ok: bool,
) -> None:
    cgroup_root = tmp_path / "cgroup"
    patched = tmp_path / "candidate-canary.sh"
    source = CONTROLLER.read_text(encoding="utf-8")
    source = source.replace(
        'Path("/sys/fs/cgroup")',
        f"Path({str(cgroup_root)!r})",
    )
    patched.write_text(source, encoding="utf-8")
    harness = tmp_path / "controller-scope-harness.sh"
    harness.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -Eeuo pipefail
            source "$1"
            cgroup_root="$2"
            scenario="$3"
            CANARY_COMMIT_SHA="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            CANARY_SOURCE_SHA256="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            CANARY_SOURCE_BYTES="123"
            CANARY_RUN_ID="run"
            CANARY_BRANCH="codex/test"
            CANARY_REPOSITORY="tristan419/JATO_Analysis_System"
            DEPLOY_STATE_DIR="$HOME/.local/state/jato-production-release"
            initialize_paths
            CANARY_MODE="controller"
            CANARY_SUPERVISOR_INVOCATION_ID="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            mkdir -p "$cgroup_root/system.slice/$CONTROLLER_UNIT"
            printf '%s\\n' "$$" >"$cgroup_root/system.slice/$CONTROLLER_UNIT/cgroup.procs"
            systemctl() {
              local bash_bin=""
              local control_group="/system.slice/$CONTROLLER_UNIT"
              local environment=""
              local start_path="$CONTROL_SCRIPT"
              local start_argument="controller"
              local runtime_max="40min"
              local stop_timeout="15min"
              local stop_propagated_from="$SUPERVISOR_UNIT"
              bash_bin="$(command -v bash)"
              environment="${CANARY_CONTROL_ENVIRONMENT[*]}"
              case "$scenario" in
                bad-cgroup)
                  control_group="/system.slice/not-controller.service"
                  ;;
                bad-env-prefix)
                  environment="${environment/RUN_KEY=$RUN_KEY/BADRUN_KEY=$RUN_KEY}"
                  ;;
                duplicate-env)
                  environment="$environment RUN_KEY=attacker"
                  ;;
                extra-env)
                  environment="$environment UNREVIEWED=1"
                  ;;
                bad-start-path)
                  start_path="${CONTROL_SCRIPT}-attacker"
                  ;;
                bad-start-argument)
                  start_argument="controller-extra"
                  ;;
                bad-runtime-timeout)
                  runtime_max="39min"
                  ;;
                bad-stop-timeout)
                  stop_timeout="14min"
                  ;;
                bad-supervisor-binding)
                  stop_propagated_from="other.service"
                  ;;
              esac
              printf 'LoadState=loaded\\n'
              printf 'ActiveState=active\\n'
              printf 'UnitFileState=transient\\n'
              printf 'FragmentPath=/run/systemd/transient/%s\\n' "$CONTROLLER_UNIT"
              printf 'ExecStart={ path=%s ; argv[]=%s %s %s ; ignore_errors=no ; }\\n' \
                "$bash_bin" "$bash_bin" "$start_path" "$start_argument"
              printf 'Environment=%s\\n' "$environment"
              printf 'ControlGroup=%s\\n' "$control_group"
              printf 'MainPID=%s\\n' "$$"
              printf 'Restart=no\\n'
              printf 'MemoryHigh=268435456\\n'
              printf 'MemoryMax=536870912\\n'
              printf 'MemorySwapMax=0\\n'
              printf 'TasksMax=64\\n'
              printf 'KillMode=control-group\\n'
              printf 'SendSIGKILL=yes\\n'
              printf 'RuntimeMaxUSec=%s\\n' "$runtime_max"
              printf 'TimeoutStopUSec=%s\\n' "$stop_timeout"
              printf 'BindsTo=\\n'
              printf 'PartOf=\\n'
              printf 'After=%s\\n' "$SUPERVISOR_UNIT"
              printf 'StopPropagatedFrom=%s\\n' "$stop_propagated_from"
            }
            assert_controller_scope
            """
        ),
        encoding="utf-8",
    )
    result = subprocess.run(
        [
            "bash",
            str(harness),
            str(patched),
            str(cgroup_root),
            scenario,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if expected_ok:
        assert result.returncode == 0, result.stderr
    else:
        assert result.returncode != 0, scenario


@pytest.mark.skipif(
    not Path("/proc/self/fd").exists() or shutil.which("flock") is None,
    reason="Linux /proc and flock are required for the fd9 ownership proof",
)
@pytest.mark.parametrize(
    ("holder_mode", "flock_override", "third_party_lock", "expected_ok"),
    (
        ("locked", "real", False, True),
        ("bash_locked", "real", False, True),
        ("open", "real", False, False),
        ("locked", "error", False, False),
        ("open", "real", True, False),
    ),
)
def test_controller_proves_the_supervisor_fd9_actually_holds_flock(
    tmp_path: Path,
    holder_mode: str,
    flock_override: str,
    third_party_lock: bool,
    expected_ok: bool,
) -> None:
    deploy_state = tmp_path / "state"
    deploy_state.mkdir()
    lock_path = deploy_state / "production-deploy.lock"
    pid_file = tmp_path / "holder.pid"
    third_party_pid_file = tmp_path / "third-party.pid"
    third_party: subprocess.Popen[str] | None = None
    if holder_mode == "bash_locked":
        holder_command = [
            "bash",
            "-c",
            'exec 9>"$1"; flock -w 1 9; printf "%s\\n" "$$" >"$2"; sleep 30',
            "_",
            str(lock_path),
            str(pid_file),
        ]
    else:
        holder_command = [
            "python3",
            "-c",
            textwrap.dedent(
                """\
                import fcntl
                import os
                from pathlib import Path
                import sys
                import time

                descriptor = os.open(sys.argv[1], os.O_CREAT | os.O_WRONLY, 0o600)
                os.dup2(descriptor, 9)
                if descriptor != 9:
                    os.close(descriptor)
                if sys.argv[2] == "locked":
                    fcntl.flock(9, fcntl.LOCK_EX)
                Path(sys.argv[3]).write_text(str(os.getpid()), encoding="utf-8")
                time.sleep(30)
                """
            ),
            str(lock_path),
            holder_mode,
            str(pid_file),
        ]
    holder = subprocess.Popen(holder_command)
    try:
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and not pid_file.exists():
            time.sleep(0.01)
        assert pid_file.exists()
        holder_pid = int(pid_file.read_text(encoding="utf-8"))
        if third_party_lock:
            third_party = subprocess.Popen(
                [
                    "python3",
                    "-c",
                    textwrap.dedent(
                        """\
                        import fcntl
                        import os
                        from pathlib import Path
                        import sys
                        import time

                        descriptor = os.open(
                            sys.argv[1],
                            os.O_CREAT | os.O_WRONLY,
                            0o600,
                        )
                        fcntl.flock(descriptor, fcntl.LOCK_EX)
                        Path(sys.argv[2]).write_text(
                            str(os.getpid()),
                            encoding="utf-8",
                        )
                        time.sleep(30)
                        """
                    ),
                    str(lock_path),
                    str(third_party_pid_file),
                ],
            )
            deadline = time.monotonic() + 3
            while (
                time.monotonic() < deadline
                and not third_party_pid_file.exists()
            ):
                time.sleep(0.01)
            assert third_party_pid_file.exists()
        harness = tmp_path / "lock-proof-harness.sh"
        harness.write_text(
            textwrap.dedent(
                """\
                #!/usr/bin/env bash
                set -Eeuo pipefail
                source "$1"
                DEPLOY_STATE_DIR="$2"
                SUPERVISOR_UNIT="jato-feature-canary-supervisor-test.service"
                CANARY_INITIAL_LOCK_PATH="$DEPLOY_STATE_DIR/production-deploy.lock"
                JATO_PRODUCTION_DEPLOY_LOCK_PATH="$CANARY_INITIAL_LOCK_PATH"
                HOLDER_PID="$3"
                FLOCK_OVERRIDE="$4"
                systemctl() {
                  printf '%s\\n' "$HOLDER_PID"
                }
                if [[ "$FLOCK_OVERRIDE" == "error" ]]; then
                  flock() { return 2; }
                fi
                assert_supervisor_production_lock
                """
            ),
            encoding="utf-8",
        )
        result = subprocess.run(
            [
                "bash",
                str(harness),
                str(CONTROLLER),
                str(deploy_state),
                str(holder_pid),
                flock_override,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if expected_ok:
            assert result.returncode == 0, result.stderr
        else:
            assert result.returncode != 0
    finally:
        if third_party is not None:
            third_party.terminate()
            try:
                third_party.wait(timeout=3)
            except subprocess.TimeoutExpired:
                third_party.kill()
                third_party.wait(timeout=3)
        holder.terminate()
        try:
            holder.wait(timeout=3)
        except subprocess.TimeoutExpired:
            holder.kill()
            holder.wait(timeout=3)


def test_build_and_runtime_units_are_bound_to_supervisor(
    tmp_path: Path,
) -> None:
    calls = tmp_path / "child-units.log"
    harness = tmp_path / "child-unit-harness.sh"
    harness.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -Eeuo pipefail
            source "$1"
            calls="$2"
            RUN_KEY="aaaaaaaaaaaa-run"
            SUPERVISOR_UNIT="jato-feature-canary-supervisor-$RUN_KEY.service"
            BUILD_UNIT="jato-feature-canary-build-$RUN_KEY.service"
            SERVICE_UNIT="jato-feature-canary-$RUN_KEY.service"
            SERVICE_RUNTIME_DIRECTORY="jato-feature-canary-$RUN_KEY"
            CANARY_ROOT="/opt/jato-canary"
            RUNTIME_ROOT="$CANARY_ROOT/runtime/$RUN_KEY"
            CONTROL_SCRIPT="$CANARY_ROOT/control/$RUN_KEY/03_Scripts/deploy/tencent_feature_candidate_canary.sh"
            CANARY_COMMIT_SHA="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            CANARY_SOURCE_SHA256="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            CANARY_SOURCE_BYTES="123"
            CANARY_RUN_ID="run"
            CANARY_BRANCH="codex/test"
            CANARY_REPOSITORY="tristan419/JATO_Analysis_System"
            CANARY_SOURCE_ARCHIVE="$CANARY_ROOT/sources/$RUN_KEY.tar.gz"
            CHECKPOINT_FILE="/var/lib/jato-canary/checkpoints/$RUN_KEY.json"
            RECEIPT_FILE="/var/lib/jato-canary/receipts/$RUN_KEY.json"
            EVIDENCE_FILE="/var/lib/jato-canary/evidence/$RUN_KEY.json"
            BEFORE_SNAPSHOT="/var/lib/jato-canary/snapshots/$RUN_KEY.before.json"
            AFTER_SNAPSHOT="/var/lib/jato-canary/snapshots/$RUN_KEY.after.json"
            systemctl() {
              if [[ "$1" == "show" ]]; then
                printf 'not-found\\n'
                return 0
              fi
              return 1
            }
            sudo() {
              [[ "$1" == "-n" ]] && shift
              printf '%s\\n' "$*" >>"$calls"
            }
            run_build_scope
            start_candidate_service
            """
        ),
        encoding="utf-8",
    )
    result = subprocess.run(
        ["bash", str(harness), str(CONTROLLER), str(calls)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    invocations = calls.read_text(encoding="utf-8").splitlines()
    assert len(invocations) == 2
    for invocation in invocations:
        for relation in ("StopPropagatedFrom", "After"):
            assert (
                    f"{relation}=jato-feature-canary-supervisor-"
                "aaaaaaaaaaaa-run.service"
            ) in invocation
        assert "BindsTo=" not in invocation
        assert "PartOf=" not in invocation
        assert "Restart=no" in invocation
    assert invocations[0].count("CANARY_MODE=build") == 1
    assert "CANARY_MODE=runtime" not in invocations[0]
    assert invocations[1].count("CANARY_MODE=runtime") == 1
    assert "CANARY_MODE=build" not in invocations[1]
    assert "--wait" in invocations[0] and "--pipe" in invocations[0]
    assert "--wait" not in invocations[1] and "--pipe" not in invocations[1]


@pytest.mark.parametrize(
    ("scenario", "expected_ok"),
    (
        ("exact", True),
        ("stale", False),
        ("inactive", False),
        ("malformed", False),
    ),
)
def test_child_generation_fence_requires_original_live_supervisor(
    tmp_path: Path,
    scenario: str,
    expected_ok: bool,
) -> None:
    harness = tmp_path / "generation-harness.sh"
    harness.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -Eeuo pipefail
            source "$1"
            scenario="$2"
            SUPERVISOR_UNIT="jato-feature-canary-supervisor-test.service"
            CANARY_SUPERVISOR_INVOCATION_ID="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            systemctl() {
              printf 'LoadState=loaded\\n'
              if [[ "$scenario" == "inactive" ]]; then
                printf 'ActiveState=inactive\\n'
              else
                printf 'ActiveState=active\\n'
              fi
              printf 'MainPID=123\\n'
              case "$scenario" in
                stale)
                  printf 'InvocationID=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\\n'
                  ;;
                malformed)
                  printf 'InvocationID=not-an-id\\n'
                  ;;
                *)
                  printf 'InvocationID=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\\n'
                  ;;
              esac
            }
            assert_supervisor_generation
            """
        ),
        encoding="utf-8",
    )
    result = subprocess.run(
        ["bash", str(harness), str(CONTROLLER), scenario],
        capture_output=True,
        text=True,
        check=False,
    )
    assert (result.returncode == 0) is expected_ok, result.stderr


@pytest.mark.parametrize(
    ("scenario", "expected_ok"),
    (
        ("active", True),
        ("deactivating", True),
        ("inactive", False),
        ("stale", False),
        ("wrong-mode", False),
    ),
)
def test_reconcile_generation_fence_allows_only_current_stopping_supervisor(
    tmp_path: Path,
    scenario: str,
    expected_ok: bool,
) -> None:
    harness = tmp_path / "reconcile-generation-harness.sh"
    harness.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -Eeuo pipefail
            source "$1"
            scenario="$2"
            CANARY_MODE="reconcile"
            [[ "$scenario" == "wrong-mode" ]] && CANARY_MODE="controller"
            SUPERVISOR_UNIT="jato-feature-canary-supervisor-test.service"
            CANARY_SUPERVISOR_INVOCATION_ID="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            systemctl() {
              printf 'LoadState=loaded\\n'
              case "$scenario" in
                inactive)
                  printf 'ActiveState=inactive\\n'
                  ;;
                deactivating)
                  printf 'ActiveState=deactivating\\n'
                  ;;
                *)
                  printf 'ActiveState=active\\n'
                  ;;
              esac
              printf 'MainPID=123\\n'
              if [[ "$scenario" == "stale" ]]; then
                printf 'InvocationID=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\\n'
              else
                printf 'InvocationID=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\\n'
              fi
            }
            assert_reconcile_supervisor_generation
            """
        ),
        encoding="utf-8",
    )
    result = subprocess.run(
        ["bash", str(harness), str(CONTROLLER), scenario],
        capture_output=True,
        text=True,
        check=False,
    )
    assert (result.returncode == 0) is expected_ok, result.stderr


@pytest.mark.parametrize(
    ("scenario", "expected_ok"),
    (("exact", True), ("bad-argv", False)),
)
def test_runtime_wrapper_checks_generation_before_exact_exec(
    tmp_path: Path,
    scenario: str,
    expected_ok: bool,
) -> None:
    calls = tmp_path / "runtime.log"
    harness = tmp_path / "runtime-wrapper-harness.sh"
    harness.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -Eeuo pipefail
            source "$1" runtime
            calls="$2"
            scenario="$3"
            CANARY_DEPLOY_UID="1000"
            CANARY_DEPLOY_GID="1000"
            id() {
              case "$1" in
                -u) printf '2000\n' ;;
                -g) printf '3000\n' ;;
                *) return 2 ;;
              esac
            }
            initialize_paths() {
              RUNTIME_ROOT="/opt/jato-canary/runtime/test"
              printf 'initialize\\n' >>"$calls"
            }
            verify_canary_parent_roots() {
              printf 'parents\\n' >>"$calls"
            }
            validate_feature_identity() {
              printf 'identity\\n' >>"$calls"
            }
            assert_staged_supervisor_generation() {
              printf 'marker\\n' >>"$calls"
            }
            wait_for_candidate_start_permit() {
              printf 'permit\\n' >>"$calls"
            }
            assert_supervisor_generation() {
              printf 'forbidden-live-generation\\n' >>"$calls"
              return 99
            }
            exec() {
              printf 'exec:%s\\n' "$*" >>"$calls"
            }
            port="18001"
            [[ "$scenario" == "bad-argv" ]] && port="18002"
            run_candidate_runtime \
              /opt/jato-canary/runtime/test/.venv/bin/python \
              -m uvicorn app.main:app \
              --host 127.0.0.1 --port "$port" --workers 2
            """
        ),
        encoding="utf-8",
    )
    result = subprocess.run(
        ["bash", str(harness), str(CONTROLLER), str(calls), scenario],
        capture_output=True,
        text=True,
        check=False,
    )
    assert (result.returncode == 0) is expected_ok, result.stderr
    events = calls.read_text(encoding="utf-8").splitlines()
    assert events[:4] == [
        "initialize",
        "parents",
        "identity",
        "marker",
    ]
    assert "forbidden-live-generation" not in events
    if expected_ok:
        assert events[4] == "permit"
        assert len([event for event in events if event.startswith("exec:")]) == 1
    else:
        assert "permit" not in events
        assert not any(event.startswith("exec:") for event in events)


@pytest.mark.parametrize(
    ("scenario", "expected_ok"),
    (
        ("exact", True),
        ("missing", False),
        ("missing-invocation", False),
        ("malformed", False),
        ("wrong-supervisor", False),
        ("wrong-candidate", False),
        ("wrong-unit", False),
        ("wrong-owner", False),
        ("wrong-mode", False),
        ("symlink", False),
    ),
)
def test_dynamic_runtime_requires_exact_root_owned_start_permit(
    tmp_path: Path,
    scenario: str,
    expected_ok: bool,
) -> None:
    control = tmp_path / "control"
    control.mkdir()
    marker = control / "supervisor-invocation-id"
    marker.write_text("a" * 32 + "\n", encoding="utf-8")
    permit = control / "candidate-start-permit"
    permit_payload = (
        f"supervisor={'a' * 32}\n"
        f"candidate={'b' * 32}\n"
        "unit=jato-feature-canary-test.service\n"
    )
    if scenario == "malformed":
        permit_payload = "not-a-permit\n"
    elif scenario == "wrong-supervisor":
        permit_payload = permit_payload.replace("a" * 32, "c" * 32)
    elif scenario == "wrong-candidate":
        permit_payload = permit_payload.replace("b" * 32, "c" * 32)
    elif scenario == "wrong-unit":
        permit_payload = permit_payload.replace(
            "jato-feature-canary-test.service",
            "jato-feature-canary-other.service",
        )
    if scenario not in {"missing", "symlink"}:
        permit.write_text(permit_payload, encoding="utf-8")
    elif scenario == "symlink":
        target = control / "permit-target"
        target.write_text(permit_payload, encoding="utf-8")
        permit.symlink_to(target)

    harness = tmp_path / "permit-harness.sh"
    harness.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -Eeuo pipefail
            source "$1"
            control="$2"
            scenario="$3"
            CANARY_MODE="runtime"
            CANARY_RUNTIME_START_PERMIT_TIMEOUT_SECONDS=1
            CONTROL_ROOT="$control"
            SUPERVISOR_GENERATION_FILE="$control/supervisor-invocation-id"
            CANDIDATE_START_PERMIT_FILE="$control/candidate-start-permit"
            SERVICE_UNIT="jato-feature-canary-test.service"
            CANARY_SUPERVISOR_INVOCATION_ID="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            INVOCATION_ID="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            [[ "$scenario" == "missing-invocation" ]] && INVOCATION_ID=""
            stat() {
              local path="${@: -1}"
              if [[ "$path" == "$CANDIDATE_START_PERMIT_FILE" ]] \
                && [[ "$scenario" == "wrong-owner" ]]; then
                printf '1000:1000:444\\n'
              elif [[ "$path" == "$CANDIDATE_START_PERMIT_FILE" ]] \
                && [[ "$scenario" == "wrong-mode" ]]; then
                printf '0:0:600\\n'
              else
                printf '0:0:444\\n'
              fi
            }
            wait_for_candidate_start_permit
            printf 'authorized\\n'
            """
        ),
        encoding="utf-8",
    )
    result = subprocess.run(
        ["bash", str(harness), str(CONTROLLER), str(control), scenario],
        capture_output=True,
        text=True,
        check=False,
    )
    assert (result.returncode == 0) is expected_ok, result.stderr
    assert ("authorized" in result.stdout) is expected_ok


def test_success_path_persists_controller_completed_marker(
    tmp_path: Path,
) -> None:
    calls = tmp_path / "controller.log"
    harness = tmp_path / "controller-harness.sh"
    harness.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -Eeuo pipefail
            source "$1"
            calls="$2"
            CANARY_MODE="controller"
            CANARY_FAULT=""
            DEPLOY_STATE_DIR="$HOME/.local/state/jato-production-release"
            validate_static_contract() { printf 'validate\\n' >>"$calls"; }
            initialize_paths() { printf 'initialize\\n' >>"$calls"; }
            assert_supervisor_generation() { printf 'generation\\n' >>"$calls"; }
            assert_staged_supervisor_generation() { printf 'marker\\n' >>"$calls"; }
            assert_supervisor_scope() { printf 'scope\\n' >>"$calls"; }
            assert_controller_scope() { printf 'controller-scope\\n' >>"$calls"; }
            assert_supervisor_production_lock() { printf 'lock\\n' >>"$calls"; }
            verify_checkpoint_marker() {
              printf 'verify:%s:%s\\n' "$1" "$2" >>"$calls"
            }
            record_checkpoint() {
              printf 'record:%s:%s\\n' "$1" "$2" >>"$calls"
            }
            resolve_active_unit() { printf 'active\\n' >>"$calls"; }
            capture_snapshot() { printf 'snapshot:%s\\n' "$1" >>"$calls"; }
            python3() { printf 'python:%s\\n' "$*" >>"$calls"; }
            prepare_trusted_materialization() {
              printf 'materialize\\n' >>"$calls"
            }
            run_build_scope() { printf 'build\\n' >>"$calls"; }
            verify_trusted_candidate_integrity() {
              printf 'integrity:%s\\n' "${1:-verify-only}" >>"$calls"
            }
            start_candidate_service() { printf 'runtime\\n' >>"$calls"; }
            authorize_candidate_runtime() { printf 'permit\\n' >>"$calls"; }
            verify_candidate_service() { printf 'candidate\\n' >>"$calls"; }
            persist_candidate_integrity_evidence() {
              printf 'persist-evidence\\n' >>"$calls"
            }
            run_canary_controller
            """
        ),
        encoding="utf-8",
    )
    result = subprocess.run(
        ["bash", str(harness), str(CONTROLLER), str(calls)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    events = calls.read_text(encoding="utf-8").splitlines()
    assert "record:controller_started:in_progress" in events
    assert "record:controller_completed:completed" in events
    assert events.index("record:controller_completed:completed") > events.index(
        "candidate",
    )
    assert events.index("runtime") < events.index("permit") < events.index(
        "candidate",
    )
    assert events.count("generation") == 3
    assert events.index("marker") < events.index("build")


@pytest.mark.parametrize(
    ("service_result", "exit_code", "exit_status"),
    (
        ("signal", "killed", "15"),
        ("signal", "killed", "9"),
    ),
)
def test_signal_reconcile_is_durable_without_process_memory(
    tmp_path: Path,
    service_result: str,
    exit_code: str,
    exit_status: str,
) -> None:
    calls = tmp_path / "reconcile.log"
    before = tmp_path / "before.json"
    before.write_text("{}\n", encoding="utf-8")
    harness = tmp_path / "reconcile-harness.sh"
    harness.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -Eeuo pipefail
            source "$1"
            root="$2"
            calls="$3"
            SERVICE_RESULT="$4"
            EXIT_CODE="$5"
            EXIT_STATUS="$6"
            RECEIPT_FILE="$root/receipt.json"
            BEFORE_SNAPSHOT="$root/before.json"
            AFTER_SNAPSHOT="$root/after.json"
            EVIDENCE_FILE="$root/evidence.json"
            CHECKPOINT_FILE="$root/checkpoint.json"
            CANARY_MODE="reconcile"
            initialize_paths() { :; }
            validate_reconcile_contract() { printf 'validate\\n' >>"$calls"; }
            assert_reconcile_supervisor_generation() {
              printf 'generation\\n' >>"$calls"
            }
            acquire_canary_production_lock() { printf 'lock\\n' >>"$calls"; }
            cleanup_candidate_start_permit_temp() {
              printf 'cleanup-permit-temp\\n' >>"$calls"
            }
            cleanup_candidate() { printf 'cleanup-children\\n' >>"$calls"; }
            wait_for_candidate_port_release() { printf 'port-free\\n' >>"$calls"; }
            resolve_active_unit() {
              ACTIVE_UNIT="jato-fullstack-backend@8000.service"
              printf 'resolve-active\\n' >>"$calls"
            }
            capture_snapshot() {
              printf 'snapshot-after\\n' >>"$calls"
              cp "$BEFORE_SNAPSHOT" "$1"
            }
            python3() { printf 'guard:%s\\n' "$*" >>"$calls"; }
            ensure_checkpoint_marker() {
              printf 'ensure-marker:%s:%s\\n' "$1" "$2" >>"$calls"
            }
            write_terminal_receipt() {
              printf 'write-receipt:%s:%s:%s\\n' \
                "$SERVICE_RESULT" "$EXIT_CODE" "$EXIT_STATUS" >>"$calls"
              printf '{"outcome":"failed"}\\n' >"$RECEIPT_FILE"
            }
            verify_existing_receipt() { printf 'verify-receipt\\n' >>"$calls"; }
            verify_retained_control_bundle() {
              printf 'verify-retained-control\\n' >>"$calls"
            }
            reconcile_canary_controller
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
            str(calls),
            service_result,
            exit_code,
            exit_status,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    events = calls.read_text(encoding="utf-8").splitlines()
    assert "cleanup-children" in events
    assert f"write-receipt:{service_result}:{exit_code}:{exit_status}" in events
    assert events.index("cleanup-children") < events.index("write-receipt:" + ":".join(
        (service_result, exit_code, exit_status),
    ))
    assert events.index("verify-retained-control") < events.index(
        "write-receipt:" + ":".join(
            (service_result, exit_code, exit_status),
        ),
    )


@pytest.mark.parametrize(
    ("scenario", "expected_outcome", "expected_ok"),
    (
        ("passed", "passed", True),
        ("expected-fault", "expected_failure_verified", True),
        ("stop-requested", "failed", True),
        ("contradictory-markers", "failed", True),
        ("production-changed", "failed", True),
        ("cleanup-failed", None, False),
        ("port-busy", None, False),
        ("control-unsafe", None, False),
    ),
)
def test_reconcile_alone_derives_and_writes_terminal_outcome(
    tmp_path: Path,
    scenario: str,
    expected_outcome: str | None,
    expected_ok: bool,
) -> None:
    calls = tmp_path / "terminal-reconcile.log"
    before = tmp_path / "before.json"
    before.write_text("{}\n", encoding="utf-8")
    harness = tmp_path / "terminal-reconcile-harness.sh"
    harness.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -Eeuo pipefail
            source "$1"
            root="$2"
            calls="$3"
            scenario="$4"
            CANARY_MODE="reconcile"
            CANARY_FAULT=""
            [[ "$scenario" == "expected-fault" ]] \
              && CANARY_FAULT="after_candidate_start"
            CANARY_SUPERVISOR_STOP_REQUESTED=false
            [[ "$scenario" == "stop-requested" ]] \
              && CANARY_SUPERVISOR_STOP_REQUESTED=true
            CANARY_CONTROLLER_RC=0
            RECEIPT_FILE="$root/receipt.json"
            BEFORE_SNAPSHOT="$root/before.json"
            AFTER_SNAPSHOT="$root/after.json"
            EVIDENCE_FILE="$root/evidence.json"
            CHECKPOINT_FILE="$root/checkpoint.json"
            initialize_paths() { :; }
            validate_reconcile_contract() { printf 'validate\\n' >>"$calls"; }
            assert_reconcile_supervisor_generation() {
              printf 'generation\\n' >>"$calls"
            }
            acquire_canary_production_lock() { printf 'lock\\n' >>"$calls"; }
            cleanup_candidate_start_permit_temp() {
              printf 'cleanup-permit-temp\\n' >>"$calls"
            }
            cleanup_candidate() {
              printf 'cleanup\\n' >>"$calls"
              [[ "$scenario" != "cleanup-failed" ]]
            }
            wait_for_candidate_port_release() {
              printf 'port-free\\n' >>"$calls"
              [[ "$scenario" != "port-busy" ]]
            }
            resolve_active_unit() {
              ACTIVE_UNIT="jato-fullstack-backend@8000.service"
              printf 'resolve-active\\n' >>"$calls"
            }
            capture_snapshot() {
              printf 'fresh-after\\n' >>"$calls"
              cp "$BEFORE_SNAPSHOT" "$1"
            }
            python3() {
              printf 'guard:%s\\n' "$*" >>"$calls"
              if [[ " $* " == *" compare "* ]] \
                && [[ "$scenario" == "production-changed" ]]; then
                return 1
              fi
              return 0
            }
            verify_retained_control_bundle() {
              printf 'control-safe\\n' >>"$calls"
              [[ "$scenario" != "control-unsafe" ]]
            }
            checkpoint_marker_present() {
              local phase="$1"
              case "$scenario:$phase" in
                passed:controller_completed|passed:cleanup_verified|\
                stop-requested:controller_completed|\
                stop-requested:cleanup_verified|\
                production-changed:controller_completed|\
                production-changed:cleanup_verified|\
                contradictory-markers:controller_completed|\
                contradictory-markers:cleanup_verified|\
                contradictory-markers:fault_observed|\
                contradictory-markers:expected_failure_verified|\
                expected-fault:fault_observed|\
                expected-fault:expected_failure_verified)
                  return 0
                  ;;
              esac
              return 1
            }
            ensure_checkpoint_marker() {
              printf 'ensure:%s:%s\\n' "$1" "$2" >>"$calls"
            }
            write_terminal_receipt() {
              printf 'write:%s:%s\\n' "$1" "$2" >>"$calls"
              printf '{"outcome":"%s"}\\n' "$1" >"$RECEIPT_FILE"
            }
            verify_existing_receipt() { printf 'verify-receipt\\n' >>"$calls"; }
            reconcile_canary_controller
            """
        ),
        encoding="utf-8",
    )
    result = subprocess.run(
        ["bash", str(harness), str(CONTROLLER), str(tmp_path), str(calls), scenario],
        capture_output=True,
        text=True,
        check=False,
    )
    assert (result.returncode == 0) is expected_ok, result.stderr
    events = calls.read_text(encoding="utf-8").splitlines()
    writes = [event for event in events if event.startswith("write:")]
    if expected_outcome is None:
        assert writes == []
        assert not (tmp_path / "receipt.json").exists()
        return
    assert len(writes) == 1
    assert writes[0].startswith(f"write:{expected_outcome}:")
    assert events.index("lock") < events.index("cleanup")
    assert events.index("cleanup") < events.index("port-free")
    assert events.index("port-free") < events.index("fresh-after")
    assert events.index("fresh-after") < events.index("control-safe")
    assert events.index("control-safe") < events.index(writes[0])
    assert events.index(writes[0]) < events.index("verify-receipt")


def test_reconcile_accepts_existing_verified_receipt_without_rewrite(
    tmp_path: Path,
) -> None:
    calls = tmp_path / "existing-receipt.log"
    receipt = tmp_path / "receipt.json"
    receipt.write_text('{"outcome":"failed"}\n', encoding="utf-8")
    harness = tmp_path / "existing-receipt-harness.sh"
    harness.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -Eeuo pipefail
            source "$1"
            calls="$2"
            RECEIPT_FILE="$3"
            CANARY_MODE="reconcile"
            initialize_paths() { :; }
            validate_reconcile_contract() { printf 'validate\\n' >>"$calls"; }
            assert_reconcile_supervisor_generation() {
              printf 'generation\\n' >>"$calls"
            }
            acquire_canary_production_lock() { printf 'lock\\n' >>"$calls"; }
            cleanup_candidate_start_permit_temp() {
              printf 'cleanup-permit-temp\\n' >>"$calls"
            }
            verify_existing_receipt() { printf 'verify-receipt\\n' >>"$calls"; }
            cleanup_candidate() { printf 'cleanup\\n' >>"$calls"; }
            wait_for_candidate_port_release() {
              printf 'port-free\\n' >>"$calls"
            }
            verify_retained_control_bundle() {
              printf 'control-safe\\n' >>"$calls"
            }
            capture_snapshot() { printf 'unexpected-capture\\n' >>"$calls"; }
            ensure_checkpoint_marker() {
              printf 'unexpected-marker\\n' >>"$calls"
            }
            write_terminal_receipt() {
              printf 'unexpected-rewrite\\n' >>"$calls"
            }
            reconcile_canary_controller
            """
        ),
        encoding="utf-8",
    )
    result = subprocess.run(
        ["bash", str(harness), str(CONTROLLER), str(calls), str(receipt)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert calls.read_text(encoding="utf-8").splitlines() == [
            "validate",
            "generation",
            "lock",
            "cleanup-permit-temp",
            "verify-receipt",
        "cleanup",
        "port-free",
        "control-safe",
    ]


def test_supervisor_restart_uses_recovery_contract_after_source_cleanup(
    tmp_path: Path,
) -> None:
    calls = tmp_path / "restart.log"
    receipt = tmp_path / "receipt.json"
    fake_control = tmp_path / "fake-control.sh"
    fake_control.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -Eeuo pipefail
            [[ "$1" == "reconcile" ]]
            printf 'fresh-reconcile\\n' >>"$CALLS_FILE"
            """
        ),
        encoding="utf-8",
    )
    fake_control.chmod(0o755)
    harness = tmp_path / "restart-harness.sh"
    harness.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -Eeuo pipefail
            source "$1"
            export CALLS_FILE="$2"
            RECEIPT_FILE="$3"
            CONTROL_SCRIPT="$4"
            CANARY_MODE="supervisor"
            initialize_paths() { :; }
            validate_static_contract() {
              printf 'unexpected-static-validation\\n' >>"$CALLS_FILE"
              return 91
            }
            validate_reconcile_contract() {
              printf 'recovery-validation\\n' >>"$CALLS_FILE"
            }
            verify_checkpoint_marker() { :; }
            checkpoint_marker_present() {
              [[ "$1" == "controller_unit_started" || "$1" == "supervisor_started" ]]
            }
            acquire_canary_production_lock() {
              printf 'lock-reacquired\\n' >>"$CALLS_FILE"
            }
            capture_supervisor_invocation_id() {
              CANARY_SUPERVISOR_INVOCATION_ID="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
              export CANARY_SUPERVISOR_INVOCATION_ID
            }
            run_canary_controller_unit() {
              printf 'unexpected-business-rerun\\n' >>"$CALLS_FILE"
              return 92
            }
            quiesce_canary_controller_unit() {
              printf 'controller-quiesced\\n' >>"$CALLS_FILE"
            }
            run_canary_supervisor
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
            str(receipt),
            str(fake_control),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    events = calls.read_text(encoding="utf-8").splitlines()
    assert events == [
        "recovery-validation",
        "lock-reacquired",
        "controller-quiesced",
        "fresh-reconcile",
    ]


@pytest.mark.parametrize("controller_signal", (signal.SIGTERM, signal.SIGKILL))
def test_supervisor_survives_real_controller_signal_and_runs_fresh_reconcile(
    tmp_path: Path,
    controller_signal: signal.Signals,
) -> None:
    calls = tmp_path / "supervisor.log"
    controller_pid = tmp_path / "controller.pid"
    controller_descendant_pid = tmp_path / "controller-descendant.pid"
    receipt = tmp_path / "receipt.json"
    fake_control = tmp_path / "fake-control.sh"
    fake_control.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -Eeuo pipefail
            case "$1" in
              controller)
                printf '%s\\n' "$$" >"$CONTROLLER_PID_FILE"
                sleep 300 &
                printf '%s\\n' "$!" >"$CONTROLLER_DESCENDANT_PID_FILE"
                trap 'exit 143' TERM
                wait
                ;;
              reconcile)
                descendant_pid="$(cat "$CONTROLLER_DESCENDANT_PID_FILE")"
                if kill -0 "$descendant_pid" 2>/dev/null; then
                  printf 'reconcile-before-quiescence\\n' >>"$CALLS_FILE"
                  exit 65
                fi
                printf 'fresh-reconcile:rc=%s\\n' \
                  "${CANARY_CONTROLLER_RC:-missing}" >>"$CALLS_FILE"
                printf '{"outcome":"failed"}\\n' >"$RECEIPT_FILE"
                ;;
              *)
                exit 64
                ;;
            esac
            """
        ),
        encoding="utf-8",
    )
    fake_control.chmod(0o755)
    harness = tmp_path / "supervisor-harness.sh"
    harness.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -Eeuo pipefail
            source "$1"
            export CALLS_FILE="$2"
            export CONTROLLER_PID_FILE="$3"
            export CONTROLLER_DESCENDANT_PID_FILE="$4"
            export RECEIPT_FILE="$5"
            CONTROL_SCRIPT="$6"
            CANARY_MODE="supervisor"
            CANARY_BUILD_TIMEOUT=1
            CANARY_RUNTIME_TIMEOUT=1
            validate_static_contract() { :; }
            initialize_paths() { :; }
            assert_supervisor_scope() { :; }
            verify_checkpoint_marker() { :; }
            checkpoint_marker_present() { return 1; }
            record_checkpoint() {
              printf 'record:%s:%s\\n' "$1" "$2" >>"$CALLS_FILE"
            }
            acquire_canary_production_lock() {
              printf 'lock:%s\\n' "$$" >>"$CALLS_FILE"
            }
            capture_supervisor_invocation_id() {
              CANARY_SUPERVISOR_INVOCATION_ID="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
              export CANARY_SUPERVISOR_INVOCATION_ID
            }
            run_canary_controller_unit() {
              bash "$CONTROL_SCRIPT" controller
            }
            quiesce_canary_controller_unit() {
              local descendant_pid=""
              descendant_pid="$(cat "$CONTROLLER_DESCENDANT_PID_FILE")"
              kill -TERM "$descendant_pid" 2>/dev/null || true
              for _attempt in $(seq 1 100); do
                if ! kill -0 "$descendant_pid" 2>/dev/null; then
                  printf 'controller-tree-quiesced\\n' >>"$CALLS_FILE"
                  return 0
                fi
                sleep 0.01
              done
              kill -KILL "$descendant_pid" 2>/dev/null || true
              for _attempt in $(seq 1 100); do
                if ! kill -0 "$descendant_pid" 2>/dev/null; then
                  printf 'controller-tree-quiesced\\n' >>"$CALLS_FILE"
                  return 0
                fi
                sleep 0.01
              done
              return 1
            }
            run_canary_supervisor
            """
        ),
        encoding="utf-8",
    )
    process = subprocess.Popen(
        [
            "bash",
            str(harness),
            str(CONTROLLER),
            str(calls),
            str(controller_pid),
            str(controller_descendant_pid),
            str(receipt),
            str(fake_control),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline and (
        not controller_pid.exists() or not controller_descendant_pid.exists()
    ):
        time.sleep(0.01)
    assert controller_pid.exists(), "controller subprocess did not start"
    assert controller_descendant_pid.exists(), "controller descendant did not start"
    child_pid = int(controller_pid.read_text(encoding="utf-8").strip())
    try:
        os.kill(child_pid, controller_signal)
        stdout, stderr = process.communicate(timeout=5)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
    assert process.returncode == 0, f"{stdout}\n{stderr}"
    assert receipt.exists()
    events = calls.read_text(encoding="utf-8").splitlines()
    assert events[0].startswith("lock:")
    assert "record:supervisor_started:in_progress" in events
    assert "controller-tree-quiesced" in events
    assert "reconcile-before-quiescence" not in events
    reconcile_events = [
        event for event in events if event.startswith("fresh-reconcile:rc=")
    ]
    assert len(reconcile_events) == 1
    assert reconcile_events[0] != "fresh-reconcile:rc=0"
    assert events.index("controller-tree-quiesced") < events.index(
        reconcile_events[0],
    )


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
    guard.record_checkpoint(
        path=checkpoint,
        identity=identity,
        phase="supervisor_reconciled",
        status="completed",
        message="terminal failure reconciled by supervisor",
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
        terminal_writer="supervisor_reconcile",
        writer_invocation_id="c" * 32,
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


@pytest.mark.parametrize(
    ("scenario", "message"),
    (
        ("missing-marker", "exactly one durable"),
        ("missing-writer", "not written by supervisor"),
        ("missing-error", "failure reason"),
    ),
)
def test_failed_receipt_still_requires_supervisor_terminal_contract(
    tmp_path: Path,
    scenario: str,
    message: str,
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
    checkpoint = {
        "schemaVersion": 1,
        "identity": identity,
        "phase": "supervisor_reconciled",
        "status": "completed",
        "events": [
            {
                "at": "2026-07-24T00:02:00+00:00",
                "phase": "supervisor_reconciled",
                "status": "completed",
                "message": "terminal failure reconciled",
            },
        ],
    }
    payload = {
        "schemaVersion": 1,
        "identity": identity,
        "outcome": "failed",
        "faultInjection": None,
        "error": "controller failed",
        "finishedAt": "2026-07-24T00:02:00+00:00",
        "terminalWriter": "supervisor_reconcile",
        "writerInvocationId": "c" * 32,
        "productionBefore": None,
        "productionAfter": None,
        "candidate": None,
        "checkpoint": checkpoint,
    }
    if scenario == "missing-marker":
        checkpoint["events"] = []
    elif scenario == "missing-writer":
        payload.pop("terminalWriter")
    elif scenario == "missing-error":
        payload["error"] = None

    with pytest.raises(guard.CanaryGuardError, match=message):
        guard.verify_receipt_payload(payload, identity)


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
        phase="fault_observed",
        status="completed",
        message="expected fault observed",
    )
    guard.record_checkpoint(
        path=checkpoint,
        identity=identity,
        phase="expected_failure_verified",
        status="completed",
        message="test",
    )
    guard.record_checkpoint(
        path=checkpoint,
        identity=identity,
        phase="supervisor_reconciled",
        status="completed",
        message="terminal outcome reconciled by supervisor",
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
            terminal_writer="supervisor_reconcile",
            writer_invocation_id="c" * 32,
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
            terminal_writer="supervisor_reconcile",
            writer_invocation_id="c" * 32,
        )


def test_candidate_evidence_rejects_worker_or_prewarm_drift() -> None:
    guard = _guard()
    identity = {
        "commit": "a" * 40,
        "archiveSha256": "b" * 64,
        "archiveBytes": 123,
        "port": 18001,
        "runId": "canary-1",
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

    evidence = _candidate_evidence()
    systemd = evidence["systemd"]
    assert isinstance(systemd, dict)
    systemd["Environment"] = (
        str(systemd["Environment"])
        + " APP_JATO_MONTHLY_ENABLED=false"
    )
    with pytest.raises(
        guard.CanaryGuardError,
        match="malformed or ambiguous",
    ):
        guard.verify_candidate_evidence(evidence, identity)

    evidence = _candidate_evidence()
    systemd = evidence["systemd"]
    assert isinstance(systemd, dict)
    systemd["Restart"] = "on-failure"
    with pytest.raises(
        guard.CanaryGuardError,
        match="property Restart is not no",
    ):
        guard.verify_candidate_evidence(evidence, identity)

    evidence = _candidate_evidence()
    systemd = evidence["systemd"]
    assert isinstance(systemd, dict)
    systemd["StopPropagatedFrom"] = "other.service"
    with pytest.raises(
        guard.CanaryGuardError,
        match="exact stop-only supervisor contract",
    ):
        guard.verify_candidate_evidence(evidence, identity)

    evidence = _candidate_evidence()
    systemd = evidence["systemd"]
    assert isinstance(systemd, dict)
    systemd["BindsTo"] = str(systemd["StopPropagatedFrom"])
    with pytest.raises(
        guard.CanaryGuardError,
        match="exact stop-only supervisor contract",
    ):
        guard.verify_candidate_evidence(evidence, identity)

    evidence = _candidate_evidence()
    systemd = evidence["systemd"]
    assert isinstance(systemd, dict)
    systemd["Environment"] = str(systemd["Environment"]).replace(
        "cccccccccccccccccccccccccccccccc",
        "not-an-invocation-id",
    )
    with pytest.raises(
        guard.CanaryGuardError,
        match="original supervisor generation",
    ):
        guard.verify_candidate_evidence(evidence, identity)

    evidence = _candidate_evidence()
    systemd = evidence["systemd"]
    assert isinstance(systemd, dict)
    systemd["InvocationID"] = 123
    with pytest.raises(
        guard.CanaryGuardError,
        match="exact transient generation",
    ):
        guard.verify_candidate_evidence(evidence, identity)

    evidence = _candidate_evidence()
    evidence["candidateInvocationId"] = "e" * 32
    with pytest.raises(
        guard.CanaryGuardError,
        match="exact transient generation",
    ):
        guard.verify_candidate_evidence(evidence, identity)

    for key, replacement in (
        ("supervisorInvocationId", "e" * 32),
        ("candidateInvocationId", "e" * 32),
        ("unit", "jato-feature-canary-other.service"),
    ):
        evidence = _candidate_evidence()
        permit = evidence["startPermit"]
        assert isinstance(permit, dict)
        permit[key] = replacement
        with pytest.raises(
            guard.CanaryGuardError,
            match="root-owned start permit",
        ):
            guard.verify_candidate_evidence(evidence, identity)


@pytest.mark.parametrize(
    ("scenario", "message"),
    (
        ("missing-uid", "lacks positive pinned deploy identities"),
        ("missing-gid", "lacks positive pinned deploy identities"),
        ("tampered-uid", "differs from trusted materialization"),
        ("tampered-gid", "differs from trusted materialization"),
        ("duplicate-uid", "malformed or ambiguous"),
        ("duplicate-gid", "malformed or ambiguous"),
    ),
)
def test_terminal_candidate_evidence_binds_one_exact_deploy_identity(
    scenario: str,
    message: str,
) -> None:
    guard = _guard()
    identity = {
        "commit": "a" * 40,
        "archiveSha256": "b" * 64,
        "archiveBytes": 123,
        "port": 18001,
        "runId": "canary-1",
    }
    evidence = _candidate_evidence()
    systemd = evidence["systemd"]
    assert isinstance(systemd, dict)
    tokens = str(systemd["Environment"]).split()
    uid_token = f"CANARY_DEPLOY_UID={EVIDENCE_DEPLOY_UID}"
    gid_token = f"CANARY_DEPLOY_GID={EVIDENCE_DEPLOY_GID}"
    if scenario == "missing-uid":
        tokens.remove(uid_token)
    elif scenario == "missing-gid":
        tokens.remove(gid_token)
    elif scenario == "tampered-uid":
        tokens[tokens.index(uid_token)] = (
            f"CANARY_DEPLOY_UID={EVIDENCE_DEPLOY_UID + 2}"
        )
    elif scenario == "tampered-gid":
        tokens[tokens.index(gid_token)] = (
            f"CANARY_DEPLOY_GID={EVIDENCE_DEPLOY_GID + 2}"
        )
    elif scenario == "duplicate-uid":
        tokens.append(uid_token)
    elif scenario == "duplicate-gid":
        tokens.append(gid_token)
    systemd["Environment"] = " ".join(tokens)

    with pytest.raises(guard.CanaryGuardError, match=message):
        guard.verify_candidate_evidence(evidence, identity)


@pytest.mark.parametrize(
    "scenario",
    (
        "root-mode",
        "mode-policy",
        "member-count",
        "expanded-bytes",
        "root-owner",
        "root-owner-mismatch",
    ),
)
def test_candidate_evidence_rejects_archive_or_root_integrity_drift(
    scenario: str,
) -> None:
    guard = _guard()
    identity = {
        "commit": "a" * 40,
        "archiveSha256": "b" * 64,
        "archiveBytes": 123,
        "port": 18001,
        "runId": "canary-1",
    }
    evidence = _candidate_evidence()
    build = evidence["buildEvidence"]
    assert isinstance(build, dict)
    archive = build["archiveValidation"]
    materialization = build["materialization"]
    assert isinstance(archive, dict)
    assert isinstance(materialization, dict)
    roots = materialization["roots"]
    assert isinstance(roots, dict)
    if scenario == "root-mode":
        archive["rootMode"] = "0700"
    elif scenario == "mode-policy":
        mode_policy = archive["modePolicy"]
        assert isinstance(mode_policy, dict)
        mode_policy["publicFiles"] = ["0644"]
    elif scenario == "member-count":
        archive["memberCount"] = True
    elif scenario == "expanded-bytes":
        archive["expandedBytes"] = 0
    elif scenario == "root-owner":
        reference = roots["reference"]
        assert isinstance(reference, dict)
        reference["uid"] = -1
    elif scenario == "root-owner-mismatch":
        candidate = roots["candidate"]
        assert isinstance(candidate, dict)
        candidate["gid"] = EVIDENCE_DEPLOY_GID + 1

    with pytest.raises(
        guard.CanaryGuardError,
        match="archive-validation|root ownership|private file",
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
    for phase, message in (
        ("source_anchored", "trusted source reference anchored"),
        ("source_verified", "candidate source verified after build"),
        ("candidate_verified", "candidate runtime verified"),
    ):
        guard.record_checkpoint(
            path=checkpoint_path,
            identity=identity,
            phase=phase,
            status="completed",
            message=message,
        )
    guard.record_checkpoint(
        path=checkpoint_path,
        identity=identity,
        phase="controller_completed",
        status="completed",
        message="candidate verified under durable controller",
    )
    guard.record_checkpoint(
        path=checkpoint_path,
        identity=identity,
        phase="cleanup_verified",
        status="completed",
        message="test",
    )
    guard.record_checkpoint(
        path=checkpoint_path,
        identity=identity,
        phase="supervisor_reconciled",
        status="completed",
        message="terminal outcome reconciled by supervisor",
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
        terminal_writer="supervisor_reconcile",
        writer_invocation_id="c" * 32,
    )

    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["outcome"] == "passed"
    assert receipt["terminalWriter"] == "supervisor_reconcile"
    assert receipt["writerInvocationId"] == "c" * 32
    assert receipt["productionBefore"] == before
    assert receipt["productionAfter"] == after
    assert receipt["candidate"]["liveBackendWorkerCount"] == 2


@pytest.mark.parametrize("unsafe_level", ("state_root", "checkpoints"))
def test_launch_state_cleanup_rejects_linked_parent_without_external_delete(
    tmp_path: Path,
    unsafe_level: str,
) -> None:
    guard = _guard()
    anchor = tmp_path / "var/lib"
    anchor.mkdir(parents=True)
    anchor.chmod(0o755)
    state_root = anchor / "jato-canary"
    outside = tmp_path / "outside"
    outside.mkdir()
    outside.chmod(0o750)
    run_key = "aaaaaaaaaaaa-run"

    if unsafe_level == "state_root":
        (outside / "checkpoints").mkdir(mode=0o750)
        sentinel = outside / "checkpoints" / f"{run_key}.json"
        sentinel.write_text("must remain", encoding="utf-8")
        state_root.symlink_to(outside, target_is_directory=True)
    else:
        state_root.mkdir(mode=0o750)
        for directory in ("receipts", "evidence", "snapshots"):
            (state_root / directory).mkdir(mode=0o750)
        sentinel = outside / f"{run_key}.json"
        sentinel.write_text("must remain", encoding="utf-8")
        (state_root / "checkpoints").symlink_to(
            outside,
            target_is_directory=True,
        )

    with pytest.raises(
        guard.CanaryGuardError,
        match="unavailable or linked canary state directory",
    ):
        guard.cleanup_launch_state(
            state_root=state_root,
            run_key=run_key,
            expected_uid=os.getuid(),
            expected_gid=os.getgid(),
            anchor=anchor,
            expected_anchor_uid=os.getuid(),
            expected_anchor_gid=os.getgid(),
        )

    assert sentinel.read_text(encoding="utf-8") == "must remain"


def test_launch_state_cleanup_unlinks_only_exact_run_files(tmp_path: Path) -> None:
    guard = _guard()
    anchor = tmp_path / "var/lib"
    anchor.mkdir(parents=True)
    anchor.chmod(0o755)
    state_root = anchor / "jato-canary"
    state_root.mkdir(mode=0o750)
    for directory in ("checkpoints", "receipts", "evidence", "snapshots"):
        (state_root / directory).mkdir(mode=0o750)
    run_key = "aaaaaaaaaaaa-run"
    exact_files = (
        state_root / "checkpoints" / f"{run_key}.json",
        state_root / "receipts" / f"{run_key}.json",
        state_root / "evidence" / f"{run_key}.json",
        state_root / "snapshots" / f"{run_key}.before.json",
        state_root / "snapshots" / f"{run_key}.after.json",
        state_root / f".{run_key}.supervisor-invocation-id.source",
        state_root / f".{run_key}.candidate-start-permit.source",
    )
    for path in exact_files:
        path.write_text("run state", encoding="utf-8")
    unrelated = state_root / "checkpoints/unrelated.json"
    unrelated.write_text("keep", encoding="utf-8")

    guard.cleanup_launch_state(
        state_root=state_root,
        run_key=run_key,
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
        anchor=anchor,
        expected_anchor_uid=os.getuid(),
        expected_anchor_gid=os.getgid(),
    )

    assert all(not path.exists() and not path.is_symlink() for path in exact_files)
    assert unrelated.read_text(encoding="utf-8") == "keep"


@pytest.mark.parametrize(
    ("scenario", "message"),
    (
        ("missing-writer", "not written by supervisor"),
        ("bad-generation", "valid supervisor writer generation"),
        ("generation-mismatch", "different supervisor generations"),
        ("missing-terminal-marker", "exactly one durable"),
        ("reordered-terminal-marker", "out of order"),
        ("missing-source-marker", "exactly one durable"),
        ("reordered-source-marker", "out of order"),
    ),
)
def test_receipt_rejects_forged_or_unreconciled_terminal_writer(
    scenario: str,
    message: str,
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
    checkpoint = {
        "schemaVersion": 1,
        "identity": identity,
        "phase": "supervisor_reconciled",
        "status": "completed",
        "events": [
            {
                "at": "2026-07-23T23:57:00+00:00",
                "phase": "source_anchored",
                "status": "completed",
                "message": "trusted source reference anchored",
            },
            {
                "at": "2026-07-23T23:58:00+00:00",
                "phase": "source_verified",
                "status": "completed",
                "message": "candidate source verified after build",
            },
            {
                "at": "2026-07-23T23:59:00+00:00",
                "phase": "candidate_verified",
                "status": "completed",
                "message": "candidate runtime verified",
            },
            {
                "at": "2026-07-24T00:00:00+00:00",
                "phase": "controller_completed",
                "status": "completed",
                "message": "candidate complete",
            },
            {
                "at": "2026-07-24T00:01:00+00:00",
                "phase": "cleanup_verified",
                "status": "completed",
                "message": "controller cleanup complete",
            },
            {
                "at": "2026-07-24T00:02:00+00:00",
                "phase": "supervisor_reconciled",
                "status": "completed",
                "message": "fresh supervisor reconciliation complete",
            },
        ],
    }
    payload = {
        "schemaVersion": 1,
        "identity": identity,
        "outcome": "passed",
        "faultInjection": None,
        "error": None,
        "finishedAt": "2026-07-24T00:02:00+00:00",
        "terminalWriter": "supervisor_reconcile",
        "writerInvocationId": "c" * 32,
        "productionBefore": before,
        "productionAfter": after,
        "candidate": _candidate_evidence(),
        "checkpoint": checkpoint,
    }
    if scenario == "missing-writer":
        payload.pop("terminalWriter")
    elif scenario == "bad-generation":
        payload["writerInvocationId"] = "not-an-invocation"
    elif scenario == "generation-mismatch":
        payload["writerInvocationId"] = "d" * 32
    elif scenario == "missing-terminal-marker":
        checkpoint["events"] = checkpoint["events"][:-1]
    elif scenario == "reordered-terminal-marker":
        checkpoint["events"] = [
            checkpoint["events"][-1],
            *checkpoint["events"][:-1],
        ]
    elif scenario == "missing-source-marker":
        checkpoint["events"] = [
            event
            for event in checkpoint["events"]
            if event["phase"] != "source_verified"
        ]
    elif scenario == "reordered-source-marker":
        checkpoint["events"][0], checkpoint["events"][1] = (
            checkpoint["events"][1],
            checkpoint["events"][0],
        )

    with pytest.raises(guard.CanaryGuardError, match=message):
        guard.verify_receipt_payload(payload, identity)


@pytest.mark.parametrize(
    ("scenario", "message"),
    (
        ("missing", "exactly one durable"),
        ("duplicate", "exactly one durable"),
        ("wrong-identity", "identity differs"),
    ),
)
def test_checkpoint_marker_rejects_missing_duplicate_or_wrong_identity(
    tmp_path: Path,
    scenario: str,
    message: str,
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
    checkpoint_path = tmp_path / "checkpoint.json"
    if scenario != "missing":
        guard.record_checkpoint(
            path=checkpoint_path,
            identity=identity,
            phase="controller_completed",
            status="completed",
            message="candidate complete",
        )
    if scenario == "duplicate":
        guard.record_checkpoint(
            path=checkpoint_path,
            identity=identity,
            phase="controller_completed",
            status="completed",
            message="duplicate marker",
        )
    if scenario == "missing":
        guard.record_checkpoint(
            path=checkpoint_path,
            identity=identity,
            phase="cleanup_verified",
            status="completed",
            message="cleanup only",
        )
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    expected_identity = dict(identity)
    if scenario == "wrong-identity":
        expected_identity["runId"] = "other-run"
    with pytest.raises(guard.CanaryGuardError, match=message):
        guard.verify_checkpoint_marker(
            checkpoint,
            expected_identity,
            phase="controller_completed",
            status="completed",
        )


def test_supervisor_terminal_marker_is_idempotent_but_duplicate_fails(
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
    checkpoint_path = tmp_path / "checkpoint.json"
    guard.record_checkpoint(
        path=checkpoint_path,
        identity=identity,
        phase="initialized",
        status="in_progress",
        message="test",
    )
    for _attempt in range(2):
        guard.ensure_checkpoint_marker(
            path=checkpoint_path,
            identity=identity,
            phase="supervisor_reconciled",
            status="completed",
            message="fresh reconciliation complete",
        )
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    events = checkpoint["events"]
    assert isinstance(events, list)
    assert sum(
        event.get("phase") == "supervisor_reconciled"
        for event in events
        if isinstance(event, dict)
    ) == 1

    guard.record_checkpoint(
        path=checkpoint_path,
        identity=identity,
        phase="supervisor_reconciled",
        status="completed",
        message="forged duplicate",
    )
    with pytest.raises(guard.CanaryGuardError, match="duplicate durable"):
        guard.ensure_checkpoint_marker(
            path=checkpoint_path,
            identity=identity,
            phase="supervisor_reconciled",
            status="completed",
            message="must not append again",
        )


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
            verify_checkpoint_marker() {
              printf 'marker:%s\\n' "$1" >>"$log"
              return 0
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
    expected_prefix = ""
    if original_rc == 0:
        expected_prefix += "marker:controller_completed\n"
    elif original_rc == 97 and fault:
        expected_prefix += "marker:fault_observed\n"
    expected_prefix += "cleanup\n"
    if cleanup_rc == 0:
        expected_prefix += "port-wait\n"
    expected_prefix += "capture\n"
    assert calls.startswith(expected_prefix)
    assert "outcome:" not in calls
    assert not (tmp_path / "receipt.json").exists()
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


@pytest.mark.parametrize(
    ("scenario", "expected_ok"),
    (
        ("exact", True),
        ("bad-fragment", False),
        ("bad-env-prefix", False),
        ("duplicate-env", False),
        ("bad-runtime-argv", False),
        ("bad-runtime-exec-path", False),
        ("bad-build-env-prefix", False),
        ("bad-build-argv", False),
    ),
)
def test_cleanup_stops_verified_units_without_in_memory_attempt_flags(
    tmp_path: Path,
    scenario: str,
    expected_ok: bool,
) -> None:
    harness = tmp_path / "cleanup-harness.sh"
    harness.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            source "$1"
            root="$2"
            scenario="$3"
            RUN_KEY="aaaaaaaaaaaa-canary-1"
            REFERENCE_ROOT="/opt/jato-canary/runtime/$RUN_KEY.reference"
            RUNTIME_ROOT="/opt/jato-canary/runtime/$RUN_KEY"
            CONTROL_ROOT="/opt/jato-canary/control/$RUN_KEY"
            STAGED_SOURCE_ARCHIVE="/opt/jato-canary/sources/$RUN_KEY.tar.gz"
            CONTROL_SCRIPT="$CONTROL_ROOT/03_Scripts/deploy/tencent_feature_candidate_canary.sh"
            SUPERVISOR_UNIT="jato-feature-canary-supervisor-$RUN_KEY.service"
            SERVICE_UNIT="jato-feature-canary-$RUN_KEY.service"
            BUILD_UNIT="jato-feature-canary-build-$RUN_KEY.service"
            CANARY_PORT="18001"
            CANARY_COMMIT_SHA="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            BASH_BIN="$(command -v bash)"
            log="$root/cleanup.log"
            : >"$log"

            marker_for_path() {
              case "$1" in
                "$REFERENCE_ROOT") printf 'reference' ;;
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
                if [[ -f "$root/stopped-$unit" ]]; then
                  printf 'not-found\\n'
                else
                  printf 'loaded\\n'
                fi
                return 0
              fi
              if [[ "$*" == *"LoadState"* && "$*" == *"InvocationID"* ]] \
                && [[ "$*" != *"UnitFileState"* ]]; then
                if [[ -f "$root/stopped-$unit" ]]; then
                  printf 'LoadState=not-found\\n'
                else
                  printf 'LoadState=loaded\\n'
                  printf 'ActiveState=active\\n'
                  printf 'InvocationID=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\\n'
                fi
                return 0
              fi
              if [[ "$*" == *"UnitFileState"* ]]; then
                local fragment="/run/systemd/transient/$unit"
                local exec_path=""
                local exec_argument=""
                local environment=""
                if [[ "$scenario" == "bad-fragment" && "$unit" == "$SERVICE_UNIT" ]]; then
                  fragment="/etc/systemd/system/jato-fullstack-backend@8000.service"
                fi
                printf 'LoadState=loaded\\n'
                printf 'ActiveState=active\\n'
                printf 'UnitFileState=transient\\n'
                printf 'FragmentPath=%s\\n' "$fragment"
                if [[ "$unit" == "$SERVICE_UNIT" ]]; then
                  exec_path="$RUNTIME_ROOT/.venv/bin/python"
                  exec_argument="--host 127.0.0.1 --port $CANARY_PORT"
                  environment="APP_RELEASE_SHA=$CANARY_COMMIT_SHA"
                  [[ "$scenario" == "bad-runtime-exec-path" ]] \
                    && exec_path="${exec_path}-attacker"
                  [[ "$scenario" == "bad-runtime-argv" ]] \
                    && exec_argument="${exec_argument}-extra"
                  [[ "$scenario" == "bad-env-prefix" ]] \
                    && environment="BADAPP_RELEASE_SHA=$CANARY_COMMIT_SHA"
                  [[ "$scenario" == "duplicate-env" ]] \
                    && environment="$environment APP_RELEASE_SHA=attacker"
                  printf 'ExecStart={ path=%s ; argv[]=%s %s runtime %s '\
'-m uvicorn app.main:app %s --workers 2 ; ignore_errors=no ; }\\n' \
                    "$BASH_BIN" "$BASH_BIN" "$CONTROL_SCRIPT" \
                    "$exec_path" "$exec_argument"
                else
                  exec_argument="build"
                  environment="RUN_KEY=$RUN_KEY"
                  [[ "$scenario" == "bad-build-argv" ]] \
                    && exec_argument="build-extra"
                  [[ "$scenario" == "bad-build-env-prefix" ]] \
                    && environment="BADRUN_KEY=$RUN_KEY"
                  printf 'ExecStart={ path=%s ; argv[]=%s %s %s ; ignore_errors=no ; }\\n' \
                    "$BASH_BIN" "$BASH_BIN" "$CONTROL_SCRIPT" "$exec_argument"
                fi
                printf 'Environment=%s\\n' "$environment"
                printf 'ControlGroup=/system.slice/%s\\n' "$unit"
                printf 'BindsTo=\\n'
                printf 'PartOf=\\n'
                printf 'After=%s\\n' "$SUPERVISOR_UNIT"
                printf 'StopPropagatedFrom=%s\\n' "$SUPERVISOR_UNIT"
                printf 'InvocationID=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\\n'
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
            scenario,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    calls = (tmp_path / "cleanup.log").read_text(encoding="utf-8")
    if not expected_ok:
        assert result.returncode == 1
        assert "remove:" not in calls
    else:
        assert result.returncode == 0, result.stderr
        assert f"stop:jato-feature-canary-{('a' * 12)}-canary-1.service" in calls
        assert f"stop:jato-feature-canary-build-{('a' * 12)}-canary-1.service" in calls
        assert "remove:reference\n" in calls
        assert "remove:runtime\n" in calls
        assert "remove:source\n" in calls
        assert "remove:control\n" not in calls
        events = calls.splitlines()
        runtime_stop = events.index(
            f"stop:jato-feature-canary-{('a' * 12)}-canary-1.service",
        )
        build_stop = events.index(
            f"stop:jato-feature-canary-build-{('a' * 12)}-canary-1.service",
        )
        reference_remove = events.index("remove:reference")
        runtime_remove = events.index("remove:runtime")
        source_remove = events.index("remove:source")
        assert runtime_stop < runtime_remove
        assert runtime_stop < reference_remove
        assert runtime_stop < source_remove
        assert build_stop < runtime_remove
        assert build_stop < reference_remove
        assert build_stop < source_remove
