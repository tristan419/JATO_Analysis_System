from __future__ import annotations

import hashlib
import json
import os
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
OUTER = REPO_ROOT / "03_Scripts/deploy/fullstack_remote_release.sh"
CONTROLLER = REPO_ROOT / "03_Scripts/deploy/tencent_bluegreen_release.sh"
INNER = REPO_ROOT / "03_Scripts/ops/deploy_fullstack_server.sh"
NGINX = REPO_ROOT / "03_Scripts/deploy/nginx/jato_fullstack.conf.example"
SLOT_UNIT = (
    REPO_ROOT
    / "03_Scripts/deploy/systemd/jato-fullstack-backend@.service"
)

TARGET_SHA = "a" * 40
OLD_SHA = "b" * 40


def _shell_function(script: str, name: str) -> str:
    start = script.index(f"{name}() {{")
    end = script.index("\n}\n", start) + len("\n}\n")
    return script[start:end]


def _candidate_data_contract_validator() -> str:
    script = CONTROLLER.read_text(encoding="utf-8")
    function_start = script.index("verify_candidate_data_access_contract() {")
    marker = "<<'PY'\n"
    start = script.index(marker, function_start) + len(marker)
    end = script.index("\nPY\n  then", start)
    return script[start:end]


def _run_candidate_data_contract(
    candidate_environment: dict[str, str],
    active_environment: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    active = subprocess.Popen(["sleep", "30"], env=active_environment)
    candidate = subprocess.Popen(["sleep", "30"], env=candidate_environment)
    try:
        return subprocess.run(
            ["python3", "-", str(candidate.pid), str(active.pid)],
            input=_candidate_data_contract_validator(),
            text=True,
            capture_output=True,
            check=False,
        )
    finally:
        candidate.terminate()
        active.terminate()
        candidate.wait(timeout=5)
        active.wait(timeout=5)


def _controller_prelude() -> str:
    script = CONTROLLER.read_text(encoding="utf-8")
    return script[: script.index('\ncase "$BLUEGREEN_MODE" in')]


def _controller_environment(tmp_path: Path) -> dict[str, str]:
    return {
        **os.environ,
        "BLUEGREEN_ROOT": str(tmp_path / "bluegreen"),
        "RELEASES_ROOT": str(tmp_path / "bluegreen/releases"),
        "SLOTS_ROOT": str(tmp_path / "bluegreen/slots"),
        "SHARED_ROOT": str(tmp_path / "bluegreen/shared"),
        "ACTIVE_RELEASE_LINK": str(tmp_path / "bluegreen/active"),
        "BLUEGREEN_STATE_ROOT": str(tmp_path / "state"),
        "ACTIVE_SLOT_FILE": str(tmp_path / "state/active-slot"),
        "DEPLOYMENT_MARKER": str(tmp_path / "state/deployment-maintenance"),
        "NGINX_ACTIVE_RELEASE_CONF": str(tmp_path / "nginx/active-release.conf"),
        "SLOT_ENV_ROOT": str(tmp_path / "slot-env"),
        "BACKEND_ENV_FILE": str(tmp_path / "backend.env"),
        "LEGACY_ROOT": str(tmp_path / "legacy"),
        "JATO_JOB_ROOT": str(tmp_path / "jobs"),
        "DEPLOY_COMMIT_SHA": TARGET_SHA,
        "DEPLOY_ARCHIVE_SHA256": "c" * 64,
        "DEPLOY_ARCHIVE_BYTES": "123",
        "DEPLOY_REPOSITORY": "example/repository",
        "DEPLOY_RUN_ID": "42",
        "DEPLOY_RUN_ATTEMPT": "1",
        "DEPLOY_APPROVAL_RUN_ID": "84",
        "DEPLOY_APPROVAL_RUN_ATTEMPT": "2",
        "DEPLOY_CANDIDATE_ATTESTATION_SHA256": "e" * 64,
        "DEPLOY_BRANCH": "main",
        "FRONTEND_ARTIFACT_IDENTITY": "frontend-artifact",
        "FRONTEND_ARTIFACT_CHECKSUM": "d" * 64,
        "RELEASE_WORKTREE": str(tmp_path / "transient-worktree"),
        "PREBUILT_FRONTEND_DIR": str(tmp_path / "transient-frontend"),
        "CHECKPOINT_FILE": str(tmp_path / "checkpoint.json"),
        "CHECKPOINT_JOURNAL": str(tmp_path / "checkpoint.jsonl"),
        "BLUEGREEN_CHECKPOINT_HELPER_OVERRIDE": str(
            REPO_ROOT / "03_Scripts/deploy/release_checkpoint.py"
        ),
        "BLUEGREEN_STORAGE_GUARD_OVERRIDE": str(
            REPO_ROOT / "03_Scripts/deploy/jato_release_storage_guard.py"
        ),
    }


def _run_controller_harness(
    tmp_path: Path,
    body: str,
    *,
    env_overrides: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    environment = _controller_environment(tmp_path)
    if env_overrides:
        environment.update(env_overrides)
    return subprocess.run(
        ["bash", "-s"],
        input=f"{_controller_prelude()}\n{body}\n",
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )


def _release_metadata_bytes(commit: str, *, marker: str) -> bytes:
    return (
        json.dumps(
            {
                "actualCommitSha": commit,
                "marker": marker,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )


def _expected_previous_metadata_path(tmp_path: Path) -> Path:
    return (
        tmp_path
        / "state"
        / "previous-metadata"
        / TARGET_SHA
        / f"{'c' * 64}.json"
    )


def _candidate_previous_metadata_path(
    tmp_path: Path,
    *,
    commit: str,
    archive: str,
) -> Path:
    return tmp_path / "state" / "previous-metadata" / commit / f"{archive}.json"


def test_outer_unconditionally_hands_off_without_legacy_live_tree_mutation() -> None:
    outer = OUTER.read_text(encoding="utf-8")
    handoff = outer.index(
        'bash "$RELEASE_WORKTREE/03_Scripts/deploy/tencent_bluegreen_release.sh"',
    )
    handoff_exit = outer.index('exit "$BLUEGREEN_RC"', handoff)

    assert handoff < handoff_exit
    assert 'rm -rf "$REPO_DIR/$release_path"' not in outer
    assert "bash 03_Scripts/deploy_fullstack_server.sh" not in outer
    assert outer.rstrip().endswith('exit "$BLUEGREEN_RC"')
    assert "03_Scripts/deploy/jato_quiescence_gate.py" in outer
    assert "03_Scripts/deploy/tencent_bluegreen_release.sh" in outer
    assert "jato-fullstack-backend-slot.env.example" in outer


def test_outer_routes_candidate_cleanup_by_exact_checkpoint_phase() -> None:
    outer = OUTER.read_text(encoding="utf-8")
    start = outer.index(
        'if [[ "$CHECKPOINT_DECISION" == "candidate-cleanup-required" ]]',
    )
    end = outer.index(
        'if [[ "$CHECKPOINT_DECISION" == "reconcile-required" ]]',
        start,
    )
    gate = outer[start:end]

    assert "rollback_completed:discard-candidate" in gate
    assert "active_updated:release-candidate" in gate
    assert "explicit Candidate discard is required" in gate
    assert "explicit Candidate release is required" in gate
    assert "exit 1" in gate
    assert "bluegreen_reconciliation_pending" not in gate


def test_outer_allows_fixed_active_reconciliation_only_through_approval() -> None:
    outer = OUTER.read_text(encoding="utf-8")
    start = outer.index('if [[ "$CHECKPOINT_DECISION" == "reconcile-required" ]]')
    end = outer.index(
        'if [[ "$CHECKPOINT_DECISION" == "already-candidate-prepare-aborted" ]]',
        start,
    )
    gate = outer[start:end]

    assert "approve-candidate-to-active:restore-previous-active" in gate
    assert "approve-candidate-to-active:finalize-active-update" in gate
    assert "approve-candidate-to-active:resume-rollback" in gate
    assert "before any other mode" in gate
    assert "exit 1" in gate


def test_previous_metadata_sidecar_uses_release_identity_outside_checkpoints() -> None:
    script = CONTROLLER.read_text(encoding="utf-8")
    preserve = _shell_function(script, "preserve_previous_release_metadata")
    prepare = _shell_function(script, "prepare_and_switch")

    assert "$BLUEGREEN_STATE_ROOT/previous-metadata" in script
    assert "$DEPLOY_COMMIT_SHA/$DEPLOY_ARCHIVE_SHA256.json" in script
    assert (
        'PREVIOUS_RELEASE_METADATA_PATH="${CHECKPOINT_FILE%.json}.'
        'previous-release.json"'
        not in script
    )
    assert 'sudo -n python3 -B "$CHECKPOINT_HELPER"' in preserve
    assert '--owner-uid "$(id -u)"' in preserve
    assert '--owner-gid "$(id -g)"' in preserve
    assert prepare.index("ensure_bluegreen_state_root") < prepare.index(
        "preserve_previous_release_metadata"
    )


def test_first_bluegreen_release_preserves_exact_legacy_metadata(
    tmp_path: Path,
) -> None:
    legacy_metadata = tmp_path / "legacy/hermes/deploy_release.json"
    legacy_metadata.parent.mkdir(parents=True)
    expected_bytes = _release_metadata_bytes(OLD_SHA, marker="legacy-first")
    legacy_metadata.write_bytes(expected_bytes)
    result = _run_controller_harness(
        tmp_path,
        """
sudo() {
  if [[ "${1:-}" == "-n" ]]; then shift; fi
  "$@"
}
CURRENT_ACTIVE_SLOT=8000
preserve_previous_release_metadata
printf 'sidecar=%s\\n' "$PREVIOUS_DEPLOY_RELEASE_FILE"
""",
    )
    sidecar = _expected_previous_metadata_path(tmp_path)

    assert result.returncode == 0, result.stderr + result.stdout
    assert f"sidecar={sidecar}" in result.stdout
    assert sidecar.read_bytes() == expected_bytes
    assert not (tmp_path / "checkpoint.previous-release.json").exists()


def test_later_release_prefers_active_slot_metadata_over_legacy(
    tmp_path: Path,
) -> None:
    active_root = tmp_path / f"bluegreen/releases/{OLD_SHA}/{'e' * 64}"
    active_metadata = active_root / "hermes/deploy_release.json"
    active_metadata.parent.mkdir(parents=True)
    expected_bytes = _release_metadata_bytes(OLD_SHA, marker="active-slot")
    active_metadata.write_bytes(expected_bytes)
    slot_link = tmp_path / "bluegreen/slots/8000/current"
    slot_link.parent.mkdir(parents=True)
    slot_link.symlink_to(active_root)
    legacy_metadata = tmp_path / "legacy/hermes/deploy_release.json"
    legacy_metadata.parent.mkdir(parents=True)
    legacy_metadata.write_bytes(
        _release_metadata_bytes("9" * 40, marker="stale-legacy")
    )
    result = _run_controller_harness(
        tmp_path,
        """
sudo() {
  if [[ "${1:-}" == "-n" ]]; then shift; fi
  "$@"
}
CURRENT_ACTIVE_SLOT=8000
preserve_previous_release_metadata
""",
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert _expected_previous_metadata_path(tmp_path).read_bytes() == expected_bytes


def test_previous_metadata_retry_reuses_exact_bytes_and_rejects_source_drift(
    tmp_path: Path,
) -> None:
    legacy_metadata = tmp_path / "legacy/hermes/deploy_release.json"
    legacy_metadata.parent.mkdir(parents=True)
    original_bytes = _release_metadata_bytes(OLD_SHA, marker="stable")
    legacy_metadata.write_bytes(original_bytes)
    result = _run_controller_harness(
        tmp_path,
        f"""
sudo() {{
  if [[ "${{1:-}}" == "-n" ]]; then shift; fi
  "$@"
}}
CURRENT_ACTIVE_SLOT=8000
preserve_previous_release_metadata
first_digest="$(sha256sum "$PREVIOUS_DEPLOY_RELEASE_FILE" | awk '{{print $1}}')"
unset PREVIOUS_DEPLOY_RELEASE_FILE
preserve_previous_release_metadata
second_digest="$(sha256sum "$PREVIOUS_DEPLOY_RELEASE_FILE" | awk '{{print $1}}')"
printf 'retry=%s:%s\\n' "$first_digest" "$second_digest"
printf '%s' '{json.dumps({"actualCommitSha": "9" * 40, "marker": "drift"})}' \
  > "{legacy_metadata}"
unset PREVIOUS_DEPLOY_RELEASE_FILE
if preserve_previous_release_metadata; then
  printf 'unexpected-source-drift-acceptance\\n'
  exit 91
fi
""",
    )
    sidecar = _expected_previous_metadata_path(tmp_path)

    assert result.returncode == 0, result.stderr + result.stdout
    first_digest, second_digest = (
        result.stdout.split("retry=", 1)[1].splitlines()[0].split(":")
    )
    assert first_digest == second_digest
    assert sidecar.read_bytes() == original_bytes
    assert "unexpected-source-drift-acceptance" not in result.stdout


def test_consecutive_a_then_b_release_preserves_previous_metadata_chain(
    tmp_path: Path,
) -> None:
    legacy_metadata = tmp_path / "legacy/hermes/deploy_release.json"
    legacy_metadata.parent.mkdir(parents=True)
    legacy_bytes = _release_metadata_bytes(OLD_SHA, marker="legacy-before-a")
    legacy_metadata.write_bytes(legacy_bytes)

    first = _run_controller_harness(
        tmp_path,
        """
sudo() {
  if [[ "${1:-}" == "-n" ]]; then shift; fi
  "$@"
}
CURRENT_ACTIVE_SLOT=8000
preserve_previous_release_metadata
""",
    )
    sidecar_a = _candidate_previous_metadata_path(
        tmp_path,
        commit=TARGET_SHA,
        archive="c" * 64,
    )
    assert first.returncode == 0, first.stderr + first.stdout
    assert sidecar_a.read_bytes() == legacy_bytes

    active_a_root = tmp_path / f"bluegreen/releases/{TARGET_SHA}/{'c' * 64}"
    active_a_metadata = active_a_root / "hermes/deploy_release.json"
    active_a_metadata.parent.mkdir(parents=True)
    active_a_bytes = _release_metadata_bytes(TARGET_SHA, marker="active-a")
    active_a_metadata.write_bytes(active_a_bytes)
    active_link = tmp_path / "bluegreen/slots/8000/current"
    active_link.parent.mkdir(parents=True)
    active_link.symlink_to(active_a_root)

    commit_b = "e" * 40
    archive_b = "f" * 64
    second = _run_controller_harness(
        tmp_path,
        """
sudo() {
  if [[ "${1:-}" == "-n" ]]; then shift; fi
  "$@"
}
CURRENT_ACTIVE_SLOT=8000
preserve_previous_release_metadata
""",
        env_overrides={
            "DEPLOY_COMMIT_SHA": commit_b,
            "DEPLOY_ARCHIVE_SHA256": archive_b,
            "CHECKPOINT_FILE": str(tmp_path / "checkpoint-b.json"),
            "CHECKPOINT_JOURNAL": str(tmp_path / "checkpoint-b.jsonl"),
        },
    )
    sidecar_b = _candidate_previous_metadata_path(
        tmp_path,
        commit=commit_b,
        archive=archive_b,
    )

    assert second.returncode == 0, second.stderr + second.stdout
    assert sidecar_a.read_bytes() == legacy_bytes
    assert sidecar_b.read_bytes() == active_a_bytes
    assert sidecar_a != sidecar_b
    assert "checkpoint" not in str(sidecar_a.relative_to(tmp_path / "state"))
    assert "checkpoint" not in str(sidecar_b.relative_to(tmp_path / "state"))


@pytest.mark.parametrize("symlink_kind", ("source", "sidecar"))
def test_previous_metadata_symlinks_fail_closed(
    tmp_path: Path,
    symlink_kind: str,
) -> None:
    legacy_metadata = tmp_path / "legacy/hermes/deploy_release.json"
    legacy_metadata.parent.mkdir(parents=True)
    real_metadata = tmp_path / "real-deploy-release.json"
    real_metadata.write_bytes(_release_metadata_bytes(OLD_SHA, marker="real"))
    if symlink_kind == "source":
        legacy_metadata.symlink_to(real_metadata)
    else:
        matching_bytes = _release_metadata_bytes(OLD_SHA, marker="legacy")
        legacy_metadata.write_bytes(matching_bytes)
        real_metadata.write_bytes(matching_bytes)
        sidecar = _expected_previous_metadata_path(tmp_path)
        sidecar.parent.mkdir(parents=True)
        sidecar.symlink_to(real_metadata)
    result = _run_controller_harness(
        tmp_path,
        """
sudo() {
  if [[ "${1:-}" == "-n" ]]; then shift; fi
  "$@"
}
CURRENT_ACTIVE_SLOT=8000
if preserve_previous_release_metadata; then
  printf 'unexpected-symlink-acceptance\\n'
  exit 92
fi
""",
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert "unexpected-symlink-acceptance" not in result.stdout


def test_candidate_is_verified_before_quiescence_and_nginx_switch() -> None:
    script = CONTROLLER.read_text(encoding="utf-8")
    prepare = script[script.index("prepare_and_switch()"):]
    candidate = prepare.index("\n  verify_candidate\n")
    supervisor = prepare.index("\n  run_switch_supervisor\n", candidate)
    persistent = _shell_function(script, "run_quiescence_supervisor")

    assert candidate < supervisor
    assert '"$python_bin" -B "$helper" hold' in persistent
    assert '-- "$bash_bin" "$controller" "$locked_mode"' in persistent
    assert "active-update-locked" in persistent
    assert "restore-previous-active-locked" in persistent
    assert '--working-directory="$RELEASE_DIR"' in persistent
    for required in (
        "/healthz",
        "/readyz",
        "expected-commit",
        "candidate monthly gate did not return structured HTTP 423",
        "verify_candidate_cgroup",
        "jato_monthly_worker.py",
    ):
        assert required in script


def test_prepare_candidate_stops_at_manual_review_without_public_mutation() -> None:
    script = CONTROLLER.read_text(encoding="utf-8")
    prepare = _shell_function(script, "prepare_candidate")
    preview = _shell_function(script, "start_candidate_preview")
    preview_stop = _shell_function(script, "stop_candidate_preview")
    preview_verify = _shell_function(script, "verify_candidate_preview_unit")
    preview_render = _shell_function(script, "render_candidate_preview_config")
    preview_http = _shell_function(script, "verify_candidate_preview_http")

    ordered = (
        "materialize_release_source",
        "run_candidate_build_scope",
        "verify_final_runtime_seal",
        "assert_no_database_migration_delta",
        "assert_runtime_storage_reserve",
        "install_slot_runtime",
        "verify_candidate",
        "verify_candidate_data_access_contract",
        "start_candidate_preview",
        "verify_candidate_preview",
        "checkpoint_write candidate_ready completed inspect_then_resume",
        "PRE_SUPERVISOR_CANDIDATE_ARMED=false",
    )
    offsets = []
    cursor = 0
    for token in ordered:
        cursor = prepare.index(token, cursor)
        offsets.append(cursor)
        cursor += len(token)
    assert offsets == sorted(offsets)
    binding = prepare.index('binding="$(evidence_binding)"')
    checkpoint = prepare.index(
        "checkpoint_write candidate_ready completed inspect_then_resume",
    )
    assert binding < checkpoint
    assert "; $binding" in prepare[checkpoint:]
    for forbidden in (
        "prepare_stable_nginx_boot_infrastructure",
        "arm_pre_switch_static_boot_safety",
        "run_switch_supervisor",
        "pause_schedulers",
        "mark_maintenance_required",
        "clear_maintenance_marker",
        "systemctl reload nginx",
        "ACTIVE_RELEASE_LINK",
    ):
        assert forbidden not in prepare
    assert "--collect" in preview
    assert "--service-type=exec" in preview
    assert "MemoryHigh=$BLUEGREEN_CANDIDATE_PREVIEW_MEMORY_HIGH" in preview
    assert "MemoryMax=$BLUEGREEN_CANDIDATE_PREVIEW_MEMORY_MAX" in preview
    assert "MemorySwapMax=0" in preview
    assert "CPUQuota=25%" in preview
    assert "TasksMax=$BLUEGREEN_CANDIDATE_PREVIEW_TASKS_MAX" in preview
    assert 'BindsTo=${SERVICE_PREFIX}${CANDIDATE_SLOT}.service' in preview
    assert '"${SERVICE_PREFIX}${CANDIDATE_SLOT}.service"' in preview_verify
    assert '"${SERVICE_PREFIX}${CANDIDATE_SLOT}.service"' in preview_stop
    assert 'JATO_CANDIDATE_PREVIEW_ID=$identity' in preview
    assert "--wait" not in preview
    assert '"$DEPLOY_ARCHIVE_SHA256"' in preview_render
    assert '"archiveSha256": archive_sha256' in preview_render
    assert '"$DEPLOY_ARCHIVE_SHA256"' in preview_http
    assert '"archiveSha256": sys.argv[4]' in preview_http


def test_prepare_candidate_success_reuses_pipeline_and_does_not_switch(
    tmp_path: Path,
) -> None:
    trace = tmp_path / "prepare-candidate.trace"
    result = _run_controller_harness(
        tmp_path,
        f"""
TRACE={trace}
PHASE=prepared
record() {{ printf '%s\\n' "$1" >> "$TRACE"; }}
require_environment() {{ record require; }}
assert_inherited_production_lock() {{ record lock; }}
ensure_bluegreen_state_root() {{ record state-root; }}
ensure_bluegreen_runtime_roots() {{ record runtime-roots; }}
assert_no_active_switch_unit() {{ record no-switch; }}
read_checkpoint_phase_status() {{
  CHECKPOINT_PHASE="$PHASE"
  CHECKPOINT_STATUS=completed
}}
resolve_active_slot() {{ CURRENT_ACTIVE_SLOT=8000; CANDIDATE_SLOT=8001; }}
resolve_current_frontend_root() {{ record current-frontend; }}
prepare_shared_runtime() {{ record shared; }}
ensure_current_slot_restartable() {{ record restartable; }}
preserve_previous_release_metadata() {{ record metadata; }}
guard_release_storage() {{ record storage; }}
assert_host_memory_budget() {{ record memory; }}
materialize_release_source() {{ record materialize; }}
run_candidate_build_scope() {{ record build; PHASE=migrated; }}
verify_final_runtime_seal() {{ record seal; }}
assert_no_database_migration_delta() {{ record database; }}
assert_runtime_storage_reserve() {{ record runtime-storage; }}
install_slot_runtime() {{ record install; PRE_SUPERVISOR_CANDIDATE_ARMED=true; }}
verify_candidate() {{ record candidate; }}
verify_candidate_data_access_contract() {{ record data-access; }}
start_candidate_preview() {{ record preview-start; }}
verify_candidate_preview() {{ record preview-verify; }}
evidence_binding() {{
  printf 'evidence_path=/state/candidate.evidence.json evidence_sha256={'d' * 64}'
}}
checkpoint_write() {{
  record "checkpoint:$1:$2:$3:$4"
  PHASE="$1"
}}
candidate_ready_state_is_legal() {{ record ready-legal; }}
prepare_stable_nginx_boot_infrastructure() {{ record unexpected-public-nginx; }}
arm_pre_switch_static_boot_safety() {{ record unexpected-boot-arm; }}
run_switch_supervisor() {{ record unexpected-switch; }}
pause_schedulers() {{ record unexpected-scheduler-pause; }}
mark_maintenance_required() {{ record unexpected-marker; }}
prepare_candidate
printf 'armed=%s\\n' "$PRE_SUPERVISOR_CANDIDATE_ARMED"
""",
    )
    events = trace.read_text(encoding="utf-8").splitlines()

    assert result.returncode == 0, result.stderr + result.stdout
    expected_binding = (
        "evidence_path=/state/candidate.evidence.json "
        f"evidence_sha256={'d' * 64}"
    )
    checkpoint_event = next(
        event
        for event in events
        if event.startswith(
            "checkpoint:candidate_ready:completed:inspect_then_resume:",
        )
    )
    assert expected_binding in checkpoint_event
    assert "armed=false" in result.stdout
    assert not any(event.startswith("unexpected-") for event in events)
    assert events.index("preview-start") < events.index("preview-verify")
    assert events.index("preview-verify") < events.index(
        checkpoint_event,
    )


def test_fixed_active_approval_keeps_slot_and_candidate_roles_stable() -> None:
    script = CONTROLLER.read_text(encoding="utf-8")
    approve = _shell_function(script, "approve_candidate_to_active")
    install = _shell_function(script, "install_release_on_fixed_active")
    rollback = _shell_function(script, "rollback_fixed_active_update")

    assert "resolve_active_slot" in approve
    assert "resolve_existing_candidate_slot" not in approve
    assert "atomic_text" not in approve
    assert "atomic_text" not in install
    assert '"$CURRENT_ACTIVE_SLOT"' in install
    assert install.index("systemctl restart") < install.index(
        "verify_slot_release_exact"
    ) < install.index("systemctl reload nginx")
    assert "verify_fixed_active_candidate_retained" in install
    assert "stop_candidate_preview" not in rollback
    assert "restore_candidate_runtime_preimage" not in rollback
    assert rollback.index("verify_public_release_exact") < rollback.index(
        "verify_fixed_active_candidate_retained"
    ) < rollback.index("checkpoint_write rollback_completed")
    assert "approve-candidate-to-active)" in script


def test_content_addressed_previous_proof_binds_both_verified_seals(
    tmp_path: Path,
) -> None:
    archive = "7" * 64
    previous = tmp_path / f"bluegreen/releases/{OLD_SHA}/{archive}"
    previous.mkdir(parents=True)
    source_seal = previous / ".jato-source-seal.json"
    source_seal.write_text('{"source":"sealed"}\n', encoding="utf-8")
    source_digest = hashlib.sha256(source_seal.read_bytes()).hexdigest()
    runtime_seal = previous / ".jato-runtime-seal.json"
    runtime_seal.write_text(
        json.dumps(
            {
                "releaseIdentity": {
                    "commit": OLD_SHA,
                    "archiveSha256": archive,
                    "frontendIdentity": "frontend-old",
                    "frontendChecksum": "8" * 64,
                },
                "sourceSealSha256": source_digest,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    verifier = tmp_path / "seal-verifier.py"
    verifier.write_text("raise SystemExit(0)\n", encoding="utf-8")
    result = _run_controller_harness(
        tmp_path,
        f"""
sudo() {{
  if [[ "${{1:-}}" == "-n" ]]; then shift; fi
  "$@"
}}
PREVIOUS_RELEASE_ROOT={previous}
PREVIOUS_RELEASE_SHA={OLD_SHA}
SOURCE_SEAL_HELPER={verifier}
fixed_active_previous_release_proof
""",
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert result.stdout.strip() == (
        f"content-addressed:{OLD_SHA}:{archive}:{source_digest}:"
        f"{hashlib.sha256(runtime_seal.read_bytes()).hexdigest()}"
    )

    source_seal.write_text('{"source":"drifted"}\n', encoding="utf-8")
    drifted = _run_controller_harness(
        tmp_path,
        f"""
sudo() {{
  if [[ "${{1:-}}" == "-n" ]]; then shift; fi
  "$@"
}}
PREVIOUS_RELEASE_ROOT={previous}
PREVIOUS_RELEASE_SHA={OLD_SHA}
SOURCE_SEAL_HELPER={verifier}
fixed_active_previous_release_proof
""",
    )

    assert drifted.returncode != 0
    assert "previous runtime seal identity is invalid" in drifted.stderr


def test_fixed_active_approval_success_updates_active_without_candidate_cleanup(
    tmp_path: Path,
) -> None:
    trace = tmp_path / "fixed-active.trace"
    result = _run_controller_harness(
        tmp_path,
        f"""
TRACE={trace}
PHASE=candidate_ready
record() {{ printf '%s\n' "$1" >> "$TRACE"; }}
require_environment() {{ record require; }}
assert_inherited_production_lock() {{ record lock; }}
ensure_bluegreen_state_root() {{ record state-root; }}
ensure_bluegreen_runtime_roots() {{ record runtime-roots; }}
assert_no_active_switch_unit() {{ record no-switch; }}
verify_quiescence_hold_context() {{ record quiescence-held; }}
resolve_active_slot() {{ CURRENT_ACTIVE_SLOT=8000; CANDIDATE_SLOT=8001; }}
capture_fixed_active_slot_anchor() {{ FIXED_ACTIVE_SLOT_DIGEST={'f' * 64}; }}
read_checkpoint_phase_status() {{
  CHECKPOINT_PHASE="$PHASE"
  CHECKPOINT_STATUS=completed
}}
candidate_ready_state_is_legal_under_hold() {{ record candidate-revalidated; }}
verify_durable_route_ownership() {{ record previous-active-proven; }}
verify_fixed_active_unit_compatibility() {{ record unit-compatible; }}
prepare_fixed_active_targets() {{
  ACTIVE_UPDATE_TARGET_ENV=/tmp/fixed-active-env
  ACTIVE_UPDATE_TARGET_NGINX=/tmp/fixed-active-nginx
  record targets
}}
fixed_active_preimage_command() {{ record "preimage:$1"; }}
fixed_active_previous_release_proof() {{ record legacy-proof; }}
verify_fixed_active_previous_release_source() {{ record source-revalidated; }}
evidence_binding() {{
  printf 'evidence_path=/state/active.evidence.json evidence_sha256={'c' * 64}'
}}
checkpoint_write() {{ record "checkpoint:$1:$2:$3:$4"; PHASE="$1"; }}
pause_schedulers() {{ record pause; }}
install_release_on_fixed_active() {{ record active-install; }}
resume_schedulers() {{ record resume; }}
mark_fixed_active_legacy_bootstrap_complete() {{ record legacy-consumed; }}
fixed_active_update_is_verified_under_hold() {{ record active-verified; }}
fixed_active_update_is_committed_under_hold() {{ record candidate-retained; }}
remove_fixed_active_target_temporaries() {{ record temp-clean; }}
stop_candidate_preview() {{ record unexpected-preview-stop; }}
restore_candidate_runtime_preimage() {{ record unexpected-candidate-restore; }}
active_update_locked
printf 'active=%s candidate=%s phase=%s armed=%s\n' \
  "$CURRENT_ACTIVE_SLOT" "$CANDIDATE_SLOT" "$PHASE" \
  "$ACTIVE_UPDATE_HANDLER_ARMED"
""",
    )
    events = trace.read_text(encoding="utf-8").splitlines()

    assert result.returncode == 0, result.stderr + result.stdout
    assert "active=8000 candidate=8001 phase=active_updated armed=false" in result.stdout
    assert "preimage:capture" in events
    expected_approval = (
        "candidate_run=42/1 approval_run=84/2 "
        f"candidate_attestation_sha256={'e' * 64}"
    )
    expected_evidence = (
        "evidence_path=/state/active.evidence.json "
        f"evidence_sha256={'c' * 64}"
    )
    started = next(
        event
        for event in events
        if event.startswith(
            "checkpoint:active_update_started:in_progress:rollback_required:",
        )
    )
    updated = next(
        event
        for event in events
        if event.startswith(
            "checkpoint:active_updated:completed:inspect_then_resume:",
        )
    )
    verified = next(
        event
        for event in events
        if event.startswith(
            "checkpoint:active_update_verified:completed:inspect_then_resume:",
        )
    )
    for event in (started, verified, updated):
        assert expected_approval in event
        assert expected_evidence in event
    assert events.index("pause") < events.index("active-install")
    assert events.index("active-install") < events.index("legacy-consumed")
    assert events.index("legacy-consumed") < events.index("active-verified")
    assert events.index("active-verified") < events.index(verified)
    assert events.index(verified) < events.index("resume")
    assert events.index("resume") < events.index("candidate-retained")
    assert "unexpected-preview-stop" not in events
    assert "unexpected-candidate-restore" not in events


@pytest.mark.parametrize("phase", ["active_update_started", "rollback_started"])
def test_interrupted_fixed_active_update_resumes_exact_restore(
    tmp_path: Path,
    phase: str,
) -> None:
    trace = tmp_path / f"{phase}.trace"
    result = _run_controller_harness(
        tmp_path,
        f"""
TRACE={trace}
PHASE={phase}
record() {{ printf '%s\n' "$1" >> "$TRACE"; }}
require_environment() {{ record require; }}
verify_quiescence_hold_context() {{ record hold; }}
resolve_active_slot() {{ CURRENT_ACTIVE_SLOT=8000; CANDIDATE_SLOT=8001; }}
capture_fixed_active_slot_anchor() {{ FIXED_ACTIVE_SLOT_DIGEST={'f' * 64}; }}
read_checkpoint_phase_status() {{
  CHECKPOINT_PHASE="$PHASE"
  CHECKPOINT_STATUS=in_progress
}}
prepare_fixed_active_targets() {{ record targets; }}
load_fixed_active_previous_identity() {{ record previous; }}
rollback_fixed_active_update() {{
  record "rollback:$1:$2"
  PHASE=rollback_completed
}}
active_update_locked
""",
    )
    events = trace.read_text(encoding="utf-8").splitlines()

    assert result.returncode != 0
    assert f"rollback::{phase}" in events
    assert "interrupted fixed Active update was restored exactly" in result.stderr


def test_verified_fixed_active_update_can_finish_scheduler_commit(
    tmp_path: Path,
) -> None:
    trace = tmp_path / "active-update-verified.trace"
    result = _run_controller_harness(
        tmp_path,
        f"""
TRACE={trace}
PHASE=active_update_verified
record() {{ printf '%s\n' "$1" >> "$TRACE"; }}
require_environment() {{ record require; }}
verify_quiescence_hold_context() {{ record hold; }}
resolve_active_slot() {{ CURRENT_ACTIVE_SLOT=8000; CANDIDATE_SLOT=8001; }}
capture_fixed_active_slot_anchor() {{ FIXED_ACTIVE_SLOT_DIGEST={'f' * 64}; }}
read_checkpoint_phase_status() {{
  CHECKPOINT_PHASE="$PHASE"
  CHECKPOINT_STATUS=completed
}}
prepare_fixed_active_targets() {{ record targets; }}
load_fixed_active_previous_identity() {{ record previous; }}
fixed_active_update_is_committed_under_hold() {{ record committed; }}
evidence_binding() {{
  printf 'evidence_path=/state/active.evidence.json evidence_sha256={'c' * 64}'
}}
checkpoint_write() {{ record "checkpoint:$1:$2:$3"; PHASE="$1"; }}
remove_fixed_active_target_temporaries() {{ record temp-clean; }}
active_update_locked
printf 'phase=%s armed=%s\n' "$PHASE" "$ACTIVE_UPDATE_HANDLER_ARMED"
""",
    )
    events = trace.read_text(encoding="utf-8").splitlines()

    assert result.returncode == 0, result.stderr + result.stdout
    assert "committed" in events
    assert "checkpoint:active_updated:completed:inspect_then_resume" in events
    assert "phase=active_updated armed=false" in result.stdout


def test_prepare_failure_cleanup_seals_checkpoint_as_aborted(tmp_path: Path) -> None:
    trace = tmp_path / "prepare-abort.trace"
    checkpoint = tmp_path / "checkpoint.json"
    checkpoint.write_text("{}\n", encoding="utf-8")
    result = _run_controller_harness(
        tmp_path,
        f"""
TRACE={trace}
PHASE=migrated
CHECKPOINT_FILE={checkpoint}
record() {{ printf '%s\n' "$1" >> "$TRACE"; }}
read_checkpoint_phase_status() {{
  CHECKPOINT_PHASE="$PHASE"
  CHECKPOINT_STATUS=completed
}}
checkpoint_write() {{ record "checkpoint:$1:$2:$3"; PHASE="$1"; }}
settle_candidate_checkpoint_after_cleanup
printf 'phase=%s\n' "$PHASE"
""",
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert (
        "checkpoint:candidate_prepare_aborted:completed:automatic"
        in trace.read_text(encoding="utf-8")
    )
    assert "phase=candidate_prepare_aborted" in result.stdout


def test_fixed_active_approval_failure_restores_active_and_retains_candidate(
    tmp_path: Path,
) -> None:
    trace = tmp_path / "fixed-active-failure.trace"
    result = _run_controller_harness(
        tmp_path,
        f"""
TRACE={trace}
PHASE=candidate_ready
record() {{ printf '%s\n' "$1" >> "$TRACE"; }}
require_environment() {{ :; }}
assert_inherited_production_lock() {{ :; }}
ensure_bluegreen_state_root() {{ :; }}
ensure_bluegreen_runtime_roots() {{ :; }}
assert_no_active_switch_unit() {{ :; }}
verify_quiescence_hold_context() {{ :; }}
resolve_active_slot() {{ CURRENT_ACTIVE_SLOT=8000; CANDIDATE_SLOT=8001; }}
capture_fixed_active_slot_anchor() {{ FIXED_ACTIVE_SLOT_DIGEST={'f' * 64}; }}
read_checkpoint_phase_status() {{
  CHECKPOINT_PHASE="$PHASE"
  CHECKPOINT_STATUS=completed
}}
candidate_ready_state_is_legal_under_hold() {{ :; }}
verify_durable_route_ownership() {{ :; }}
verify_fixed_active_unit_compatibility() {{ :; }}
prepare_fixed_active_targets() {{
  ACTIVE_UPDATE_TARGET_ENV=/tmp/fixed-active-env
  ACTIVE_UPDATE_TARGET_NGINX=/tmp/fixed-active-nginx
}}
fixed_active_preimage_command() {{ record "preimage:$1"; }}
fixed_active_previous_release_proof() {{ :; }}
verify_fixed_active_previous_release_source() {{ :; }}
evidence_binding() {{ printf 'evidence_path=/e evidence_sha256={'c' * 64}'; }}
checkpoint_write() {{ record "checkpoint:$1"; PHASE="$1"; }}
pause_schedulers() {{ record pause; }}
install_release_on_fixed_active() {{ record active-install-failed; return 1; }}
rollback_fixed_active_update() {{
  record rollback
  record candidate-retained
  PHASE=rollback_completed
}}
remove_fixed_active_target_temporaries() {{ record temp-clean; }}
trap fixed_active_update_exit_handler EXIT
active_update_locked
""",
    )
    events = trace.read_text(encoding="utf-8").splitlines()

    assert result.returncode != 0
    assert "active-install-failed" in events
    assert "rollback" in events
    assert "candidate-retained" in events
    assert events.index("active-install-failed") < events.index("rollback")


def test_evidence_binding_fails_closed_when_evidence_is_missing(
    tmp_path: Path,
) -> None:
    result = _run_controller_harness(
        tmp_path,
        """
set +e
binding="$(evidence_binding)"
rc=$?
set -e
printf 'rc=%s binding=%s\n' "$rc" "$binding"
""",
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert "rc=1 binding=" in result.stdout
    assert "release evidence is unavailable" in result.stderr


def test_restore_previous_active_is_gated_and_keeps_candidate() -> None:
    script = CONTROLLER.read_text(encoding="utf-8")
    restore = _shell_function(script, "restore_previous_active")
    locked = _shell_function(script, "restore_previous_active_locked")
    rollback = _shell_function(script, "rollback_fixed_active_update")
    exit_handler = _shell_function(
        script,
        "restore_previous_active_exit_handler",
    )

    hold = locked.index("verify_quiescence_hold_context")
    exact = locked.index("fixed_active_update_is_committed_under_hold")
    pause = locked.index("pause_schedulers", exact)
    checkpoint = locked.index(
        "checkpoint_write rollback_started in_progress rollback_required",
        pause,
    )
    rollback_call = locked.index("rollback_fixed_active_update", checkpoint)
    assert hold < exact < pause < checkpoint < rollback_call
    assert '"$CHECKPOINT_PHASE" != "active_updated"' in restore
    assert '"$CHECKPOINT_STATUS" != "completed"' in restore
    assert "fixed_active_approval_binding" in restore
    assert "evidence_binding" in locked
    assert "run_quiescence_supervisor restore-previous-active-locked" in restore
    assert "stop_candidate_preview" not in restore
    assert "restore_candidate_runtime_preimage" not in restore
    assert 'verify_active_cgroup "$CURRENT_ACTIVE_SLOT"' in rollback
    assert "fixed_active_preimage_command restore" in rollback
    assert (
        'verify_slot_release_exact "$CURRENT_ACTIVE_SLOT" '
        '"$PREVIOUS_RELEASE_SHA"'
    ) in rollback
    assert 'verify_public_release_exact "$PREVIOUS_RELEASE_SHA"' in rollback
    assert '"$PREVIOUS_RELEASE_ROOT/06_AppPlatform/frontend/dist"' in rollback
    assert "verify_fixed_active_candidate_retained" in rollback
    assert "previous_fixed_active_restore_is_committed" in rollback
    assert "previous_fixed_active_restore_is_committed" in exit_handler
    assert "restore-previous-active)" in script


def test_restore_previous_active_success_is_bound_and_ordered(
    tmp_path: Path,
) -> None:
    trace = tmp_path / "restore-previous-active.trace"
    result = _run_controller_harness(
        tmp_path,
        f"""
TRACE={trace}
PHASE=active_updated
record() {{ printf '%s\n' "$1" >> "$TRACE"; }}
require_environment() {{ record require; }}
assert_inherited_production_lock() {{ record lock; }}
ensure_bluegreen_state_root() {{ record state-root; }}
ensure_bluegreen_runtime_roots() {{ record runtime-roots; }}
assert_no_active_switch_unit() {{ record no-switch; }}
verify_quiescence_hold_context() {{ record quiescence-held; }}
resolve_active_slot() {{ CURRENT_ACTIVE_SLOT=8000; CANDIDATE_SLOT=8001; }}
capture_fixed_active_slot_anchor() {{ record active-slot-anchor; }}
read_checkpoint_phase_status() {{
  CHECKPOINT_PHASE="$PHASE"
  CHECKPOINT_STATUS=completed
}}
prepare_fixed_active_targets() {{ record targets; }}
load_fixed_active_previous_identity() {{
  record previous-identity
  PREVIOUS_RELEASE_SHA={'9' * 40}
  PREVIOUS_RELEASE_ROOT=/old
}}
fixed_active_update_is_committed_under_hold() {{ record active-and-candidate-exact; }}
evidence_binding() {{
  printf 'evidence_path=/state/restore.evidence.json evidence_sha256={'8' * 64}'
}}
pause_schedulers() {{ record pause; }}
checkpoint_write() {{ record "checkpoint:$1:$2:$3:$4"; PHASE="$1"; }}
rollback_fixed_active_update() {{
  record "restore:$1:$2"
  PHASE=rollback_completed
}}
remove_fixed_active_target_temporaries() {{ record temp-clean; }}
restore_previous_active_locked
printf 'phase=%s armed=%s\n' "$PHASE" "$PREVIOUS_ACTIVE_RESTORE_ARMED"
""",
    )
    events = trace.read_text(encoding="utf-8").splitlines()

    assert result.returncode == 0, result.stderr + result.stdout
    assert "phase=rollback_completed armed=false" in result.stdout
    exact = events.index("active-and-candidate-exact")
    pause = events.index("pause")
    checkpoint_index = next(
        index
        for index, event in enumerate(events)
        if event.startswith("checkpoint:rollback_started:in_progress:")
    )
    restore_index = next(
        index for index, event in enumerate(events) if event.startswith("restore:")
    )
    assert exact < pause < checkpoint_index < restore_index
    checkpoint_event = events[checkpoint_index]
    assert "candidate_run=42/1 approval_run=84/2" in checkpoint_event
    assert f"candidate_attestation_sha256={'e' * 64}" in checkpoint_event
    assert "evidence_path=/state/restore.evidence.json" in checkpoint_event
    assert f"evidence_sha256={'8' * 64}" in checkpoint_event
    assert events[restore_index] == (
        f"restore:evidence_path=/state/restore.evidence.json "
        f"evidence_sha256={'8' * 64}:active_updated"
    )


def test_restore_previous_active_failure_retains_maintenance_fence(
    tmp_path: Path,
) -> None:
    result = _run_controller_harness(
        tmp_path,
        f"""
require_environment() {{ :; }}
assert_inherited_production_lock() {{ :; }}
ensure_bluegreen_state_root() {{ :; }}
ensure_bluegreen_runtime_roots() {{ :; }}
assert_no_active_switch_unit() {{ :; }}
verify_quiescence_hold_context() {{ :; }}
resolve_active_slot() {{ CURRENT_ACTIVE_SLOT=8000; CANDIDATE_SLOT=8001; }}
capture_fixed_active_slot_anchor() {{ :; }}
read_checkpoint_phase_status() {{
  CHECKPOINT_PHASE=active_updated
  CHECKPOINT_STATUS=completed
}}
prepare_fixed_active_targets() {{ :; }}
load_fixed_active_previous_identity() {{
  PREVIOUS_RELEASE_SHA={'9' * 40}
  PREVIOUS_RELEASE_ROOT=/old
}}
fixed_active_update_is_committed_under_hold() {{ printf 'candidate-retained\n'; }}
evidence_binding() {{ printf 'evidence_path=/e evidence_sha256={'8' * 64}'; }}
retain_fixed_active_maintenance_fence() {{ printf 'hold-retained\n'; }}
pause_schedulers() {{ printf 'schedulers-paused\n'; }}
checkpoint_write() {{ :; }}
rollback_fixed_active_update() {{ printf 'restore-failed\n'; return 1; }}
remove_fixed_active_target_temporaries() {{ printf 'temporaries-removed\n'; }}
trap restore_previous_active_exit_handler EXIT
restore_previous_active_locked
""",
    )

    assert result.returncode == 81
    assert "candidate-retained" in result.stdout
    assert "restore-failed" in result.stdout
    assert "hold-retained" in result.stdout
    assert "temporaries-removed" in result.stdout


@pytest.mark.parametrize("committed", (True, False))
def test_restore_exit_handler_reconciles_rollback_completed_crash_window(
    tmp_path: Path,
    committed: bool,
) -> None:
    exact_result = "return 0" if committed else "return 1"
    result = _run_controller_harness(
        tmp_path,
        f"""
PREVIOUS_ACTIVE_RESTORE_ARMED=true
read_checkpoint_phase_status() {{
  CHECKPOINT_PHASE=rollback_completed
  CHECKPOINT_STATUS=completed
}}
previous_fixed_active_restore_is_committed() {{
  printf 'settled-state-revalidated\n'
  {exact_result}
}}
mark_maintenance_required() {{ printf 'maintenance-fenced\n'; }}
remove_fixed_active_target_temporaries() {{ printf 'temporaries-removed\n'; }}
trap restore_previous_active_exit_handler EXIT
false
""",
    )

    assert "settled-state-revalidated" in result.stdout
    assert "temporaries-removed" in result.stdout
    if committed:
        assert result.returncode == 1
        assert "maintenance-fenced" not in result.stdout
    else:
        assert result.returncode == 81
        assert "maintenance-fenced" in result.stdout


def test_restore_previous_active_rejects_wrong_phase_before_fencing(
    tmp_path: Path,
) -> None:
    result = _run_controller_harness(
        tmp_path,
        """
require_environment() { :; }
assert_inherited_production_lock() { :; }
ensure_bluegreen_state_root() { :; }
ensure_bluegreen_runtime_roots() { :; }
assert_no_active_switch_unit() { :; }
resolve_active_slot() { CURRENT_ACTIVE_SLOT=8000; CANDIDATE_SLOT=8001; }
capture_fixed_active_slot_anchor() { :; }
read_checkpoint_phase_status() {
  CHECKPOINT_PHASE=candidate_ready
  CHECKPOINT_STATUS=completed
}
mark_maintenance_required() { printf 'unexpected-marker\n'; }
pause_schedulers() { printf 'unexpected-pause\n'; }
restore_previous_active
""",
    )

    assert result.returncode != 0
    assert "requires active_updated/completed" in result.stderr
    assert "unexpected-marker" not in result.stdout
    assert "unexpected-pause" not in result.stdout


@pytest.mark.parametrize("active_slot", ("8000", "8001"))
def test_fixed_active_slot_anchor_is_byte_stable(
    tmp_path: Path,
    active_slot: str,
) -> None:
    active_slot_file = tmp_path / "active-slot"
    active_slot_file.write_text(f"{active_slot}\n", encoding="utf-8")
    result = _run_controller_harness(
        tmp_path,
        f"""
sudo() {{
  if [[ "${{1:-}}" == "-n" ]]; then shift; fi
  "$@"
}}
ACTIVE_SLOT_FILE={active_slot_file}
CURRENT_ACTIVE_SLOT={active_slot}
capture_fixed_active_slot_anchor
verify_fixed_active_slot_anchor
printf 'other\n' > "$ACTIVE_SLOT_FILE"
set +e
verify_fixed_active_slot_anchor
rc=$?
set -e
printf 'drift-rc=%s\n' "$rc"
""",
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert "drift-rc=1" in result.stdout
    assert "active-slot owner changed" in result.stderr


def test_active_updated_exit_window_revalidates_instead_of_rolling_back(
    tmp_path: Path,
) -> None:
    result = _run_controller_harness(
        tmp_path,
        """
ACTIVE_UPDATE_HANDLER_ARMED=true
read_checkpoint_phase_status() {
  CHECKPOINT_PHASE=active_updated
  CHECKPOINT_STATUS=completed
}
fixed_active_update_is_committed() { printf 'committed-revalidated\n'; }
rollback_fixed_active_update() { printf 'unexpected-rollback\n'; return 1; }
remove_fixed_active_target_temporaries() { printf 'temporaries-removed\n'; }
trap fixed_active_update_exit_handler EXIT
false
""",
    )

    assert result.returncode == 1
    assert "committed-revalidated" in result.stdout
    assert "temporaries-removed" in result.stdout
    assert "unexpected-rollback" not in result.stdout


def test_discard_candidate_mutates_only_candidate_runtime() -> None:
    script = CONTROLLER.read_text(encoding="utf-8")
    discard = _shell_function(script, "discard_candidate")
    active_gate = _shell_function(
        script,
        "previous_active_is_exact_for_candidate_discard",
    )

    assert "previous_active_is_exact_for_candidate_discard" in discard
    assert "stop_candidate_preview" in discard
    assert "restore_candidate_runtime_preimage" in discard
    assert "candidate_release_is_complete" in discard
    assert "checkpoint_write candidate_discarded completed automatic" in discard
    assert "rollback_completed" in discard
    assert "fixed_active_preimage_command restore" not in discard
    assert 'verify_active_cgroup "$CURRENT_ACTIVE_SLOT"' in active_gate
    assert "verify_slot_release_exact" in active_gate
    assert "verify_public_release_exact" in active_gate
    assert "verify_durable_route_ownership" in active_gate
    assert "verify_active_monthly_gate_released" in active_gate
    for forbidden in (
        "pause_schedulers",
        "resume_schedulers",
        "mark_maintenance_required",
        "clear_maintenance_marker",
        "systemctl restart",
        "systemctl reload nginx",
        "atomic_symlink",
        "atomic_text",
        "durable_install_file",
        "discard_candidate_runtime_preimage",
    ):
        assert forbidden not in discard
    assert "discard-candidate)" in script


def test_discard_candidate_is_ordered_and_binds_evidence(tmp_path: Path) -> None:
    trace = tmp_path / "discard-candidate.trace"
    result = _run_controller_harness(
        tmp_path,
        f"""
TRACE={trace}
PHASE=candidate_ready
CANDIDATE_CLEAN=false
record() {{ printf '%s\n' "$1" >> "$TRACE"; }}
require_environment() {{ record require; }}
assert_inherited_production_lock() {{ record lock; }}
ensure_bluegreen_state_root() {{ record state-root; }}
ensure_bluegreen_runtime_roots() {{ record runtime-roots; }}
assert_no_active_switch_unit() {{ record no-switch; }}
resolve_active_slot() {{ CURRENT_ACTIVE_SLOT=8000; CANDIDATE_SLOT=8001; }}
read_checkpoint_phase_status() {{
  CHECKPOINT_PHASE="$PHASE"
  CHECKPOINT_STATUS=completed
}}
evidence_binding() {{
  printf 'evidence_path=/state/discard.evidence.json evidence_sha256={'b' * 64}'
}}
previous_active_is_exact_for_candidate_discard() {{ record active-exact; }}
stop_candidate_preview() {{ record preview-stopped; }}
candidate_cleanup_is_complete() {{
  record candidate-clean-check
  [[ "$CANDIDATE_CLEAN" == true ]]
}}
restore_candidate_runtime_preimage() {{
  record candidate-restored
  CANDIDATE_CLEAN=true
}}
candidate_release_is_complete() {{
  record candidate-release-exact
  [[ "$CANDIDATE_CLEAN" == true ]]
}}
checkpoint_write() {{
  record "checkpoint:$1:$2:$3:$4"
  PHASE="$1"
}}
discard_candidate
printf 'phase=%s\n' "$PHASE"
""",
    )
    events = trace.read_text(encoding="utf-8").splitlines()

    assert result.returncode == 0, result.stderr + result.stdout
    assert "phase=candidate_discarded" in result.stdout
    active_checks = [
        index for index, event in enumerate(events) if event == "active-exact"
    ]
    checkpoint_index = next(
        index
        for index, event in enumerate(events)
        if event.startswith("checkpoint:candidate_discarded:completed:automatic:")
    )
    assert len(active_checks) == 2
    assert active_checks[0] < events.index("preview-stopped")
    assert events.index("preview-stopped") < events.index("candidate-restored")
    assert events.index("candidate-restored") < events.index(
        "candidate-release-exact",
    )
    assert events.index("candidate-release-exact") < active_checks[1]
    assert active_checks[1] < checkpoint_index
    expected_binding = (
        "evidence_path=/state/discard.evidence.json "
        f"evidence_sha256={'b' * 64}"
    )
    assert expected_binding in events[checkpoint_index]


def test_discard_candidate_can_resume_after_interrupted_cleanup(
    tmp_path: Path,
) -> None:
    trace = tmp_path / "discard-resume.trace"
    result = _run_controller_harness(
        tmp_path,
        f"""
TRACE={trace}
PHASE=candidate_ready
CANDIDATE_CLEAN=false
RESTORE_CALLS=0
record() {{ printf '%s\n' "$1" >> "$TRACE"; }}
require_environment() {{ :; }}
assert_inherited_production_lock() {{ :; }}
ensure_bluegreen_state_root() {{ :; }}
ensure_bluegreen_runtime_roots() {{ :; }}
assert_no_active_switch_unit() {{ :; }}
resolve_active_slot() {{ CURRENT_ACTIVE_SLOT=8000; CANDIDATE_SLOT=8001; }}
read_checkpoint_phase_status() {{
  CHECKPOINT_PHASE="$PHASE"
  CHECKPOINT_STATUS=completed
}}
evidence_binding() {{ printf 'evidence_path=/e evidence_sha256={'a' * 64}'; }}
previous_active_is_exact_for_candidate_discard() {{ record active-exact; }}
stop_candidate_preview() {{ record preview-stopped; }}
candidate_cleanup_is_complete() {{ [[ "$CANDIDATE_CLEAN" == true ]]; }}
restore_candidate_runtime_preimage() {{
  RESTORE_CALLS=$((RESTORE_CALLS + 1))
  record "restore-$RESTORE_CALLS"
  if [[ "$RESTORE_CALLS" -eq 1 ]]; then return 1; fi
  CANDIDATE_CLEAN=true
}}
candidate_release_is_complete() {{ [[ "$CANDIDATE_CLEAN" == true ]]; }}
checkpoint_write() {{ record "checkpoint:$1"; PHASE="$1"; }}
set +e
discard_candidate
first_rc=$?
set -e
discard_candidate
printf 'first=%s phase=%s\n' "$first_rc" "$PHASE"
""",
    )
    events = trace.read_text(encoding="utf-8").splitlines()

    assert result.returncode == 0, result.stderr + result.stdout
    assert "first=1 phase=candidate_discarded" in result.stdout
    assert events.count("preview-stopped") == 2
    assert "restore-1" in events
    assert "restore-2" in events
    assert events.count("checkpoint:candidate_discarded") == 1


def test_candidate_discarded_rerun_is_read_only(tmp_path: Path) -> None:
    result = _run_controller_harness(
        tmp_path,
        """
require_environment() { :; }
assert_inherited_production_lock() { :; }
ensure_bluegreen_state_root() { :; }
ensure_bluegreen_runtime_roots() { :; }
assert_no_active_switch_unit() { :; }
resolve_active_slot() { CURRENT_ACTIVE_SLOT=8001; CANDIDATE_SLOT=8000; }
read_checkpoint_phase_status() {
  CHECKPOINT_PHASE=candidate_discarded
  CHECKPOINT_STATUS=completed
}
previous_active_is_exact_for_candidate_discard() { printf 'active-read-only\n'; }
candidate_release_is_complete() { printf 'candidate-read-only\n'; }
stop_candidate_preview() { printf 'unexpected-preview-stop\n'; }
restore_candidate_runtime_preimage() { printf 'unexpected-candidate-restore\n'; }
checkpoint_write() { printf 'unexpected-checkpoint-write\n'; }
discard_candidate
""",
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert "active-read-only" in result.stdout
    assert "candidate-read-only" in result.stdout
    assert "unexpected-preview-stop" not in result.stdout
    assert "unexpected-candidate-restore" not in result.stdout
    assert "unexpected-checkpoint-write" not in result.stdout


def test_release_candidate_mutates_only_candidate_runtime() -> None:
    script = CONTROLLER.read_text(encoding="utf-8")
    release = _shell_function(script, "release_candidate")

    assert "fixed_active_runtime_is_exact" in release
    assert "stop_candidate_preview" in release
    assert "restore_candidate_runtime_preimage" in release
    assert "candidate_release_is_complete" in release
    assert "checkpoint_write candidate_released completed automatic" in release
    for forbidden in (
        "pause_schedulers",
        "resume_schedulers",
        "mark_maintenance_required",
        "clear_maintenance_marker",
        "systemctl restart",
        "systemctl reload nginx",
        "atomic_symlink",
        "atomic_text",
        "durable_install_file",
        "discard_candidate_runtime_preimage",
    ):
        assert forbidden not in release
    assert "release-candidate)" in script


def test_release_candidate_is_ordered_and_checkpointed_after_exact_cleanup(
    tmp_path: Path,
) -> None:
    trace = tmp_path / "release-candidate.trace"
    result = _run_controller_harness(
        tmp_path,
        f"""
TRACE={trace}
PHASE=active_updated
record() {{ printf '%s\n' "$1" >> "$TRACE"; }}
require_environment() {{ record require; }}
assert_inherited_production_lock() {{ record lock; }}
ensure_bluegreen_state_root() {{ record state-root; }}
ensure_bluegreen_runtime_roots() {{ record runtime-roots; }}
assert_no_active_switch_unit() {{ record no-switch; }}
resolve_active_slot() {{ CURRENT_ACTIVE_SLOT=8000; CANDIDATE_SLOT=8001; }}
capture_fixed_active_slot_anchor() {{ FIXED_ACTIVE_SLOT_DIGEST={'f' * 64}; }}
read_checkpoint_phase_status() {{
  CHECKPOINT_PHASE="$PHASE"
  CHECKPOINT_STATUS=completed
}}
prepare_fixed_active_targets() {{
  ACTIVE_UPDATE_TARGET_ENV=/tmp/release-candidate-env
  ACTIVE_UPDATE_TARGET_NGINX=/tmp/release-candidate-nginx
  record targets
}}
load_fixed_active_previous_identity() {{ record old-active-preimage; }}
fixed_active_runtime_is_exact() {{ record active-exact; }}
stop_candidate_preview() {{ record preview-stopped; }}
restore_candidate_runtime_preimage() {{ record candidate-restored; }}
candidate_release_is_complete() {{ record candidate-quiescent; }}
checkpoint_write() {{ record "checkpoint:$1:$2:$3"; PHASE="$1"; }}
remove_fixed_active_target_temporaries() {{ record temp-clean; }}
release_candidate
printf 'phase=%s active=%s candidate=%s\n' \
  "$PHASE" "$CURRENT_ACTIVE_SLOT" "$CANDIDATE_SLOT"
""",
    )
    events = trace.read_text(encoding="utf-8").splitlines()

    assert result.returncode == 0, result.stderr + result.stdout
    assert "phase=candidate_released active=8000 candidate=8001" in result.stdout
    assert events.count("active-exact") == 2
    assert events.index("preview-stopped") < events.index("candidate-restored")
    assert events.index("candidate-restored") < events.index("candidate-quiescent")
    assert events.index("candidate-quiescent") < events.index(
        "checkpoint:candidate_released:completed:automatic"
    )


def test_release_candidate_retry_after_checkpoint_is_read_only(
    tmp_path: Path,
) -> None:
    result = _run_controller_harness(
        tmp_path,
        f"""
require_environment() {{ :; }}
assert_inherited_production_lock() {{ :; }}
ensure_bluegreen_state_root() {{ :; }}
ensure_bluegreen_runtime_roots() {{ :; }}
assert_no_active_switch_unit() {{ :; }}
resolve_active_slot() {{ CURRENT_ACTIVE_SLOT=8001; CANDIDATE_SLOT=8000; }}
capture_fixed_active_slot_anchor() {{ FIXED_ACTIVE_SLOT_DIGEST={'f' * 64}; }}
read_checkpoint_phase_status() {{
  CHECKPOINT_PHASE=candidate_released
  CHECKPOINT_STATUS=completed
}}
prepare_fixed_active_targets() {{
  ACTIVE_UPDATE_TARGET_ENV=/tmp/released-env
  ACTIVE_UPDATE_TARGET_NGINX=/tmp/released-nginx
}}
load_fixed_active_previous_identity() {{ :; }}
fixed_active_runtime_is_exact() {{ printf 'active-read-only-verified\n'; }}
candidate_release_is_complete() {{ printf 'candidate-read-only-verified\n'; }}
stop_candidate_preview() {{ printf 'unexpected-preview-mutation\n'; }}
restore_candidate_runtime_preimage() {{ printf 'unexpected-candidate-mutation\n'; }}
checkpoint_write() {{ printf 'unexpected-checkpoint-write\n'; }}
remove_fixed_active_target_temporaries() {{ :; }}
release_candidate
""",
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert "active-read-only-verified" in result.stdout
    assert "candidate-read-only-verified" in result.stdout
    assert "unexpected-preview-mutation" not in result.stdout
    assert "unexpected-candidate-mutation" not in result.stdout
    assert "unexpected-checkpoint-write" not in result.stdout


def test_release_candidate_failure_never_writes_success_or_touches_active(
    tmp_path: Path,
) -> None:
    result = _run_controller_harness(
        tmp_path,
        f"""
require_environment() {{ :; }}
assert_inherited_production_lock() {{ :; }}
ensure_bluegreen_state_root() {{ :; }}
ensure_bluegreen_runtime_roots() {{ :; }}
assert_no_active_switch_unit() {{ :; }}
resolve_active_slot() {{ CURRENT_ACTIVE_SLOT=8000; CANDIDATE_SLOT=8001; }}
capture_fixed_active_slot_anchor() {{ FIXED_ACTIVE_SLOT_DIGEST={'f' * 64}; }}
read_checkpoint_phase_status() {{
  CHECKPOINT_PHASE=active_updated
  CHECKPOINT_STATUS=completed
}}
prepare_fixed_active_targets() {{
  ACTIVE_UPDATE_TARGET_ENV=/tmp/release-failure-env
  ACTIVE_UPDATE_TARGET_NGINX=/tmp/release-failure-nginx
}}
load_fixed_active_previous_identity() {{ :; }}
fixed_active_runtime_is_exact() {{ :; }}
stop_candidate_preview() {{ printf 'preview-stopped\n'; }}
restore_candidate_runtime_preimage() {{ printf 'candidate-restore-failed\n'; return 1; }}
checkpoint_write() {{ printf 'unexpected-success-checkpoint\n'; }}
pause_schedulers() {{ printf 'unexpected-scheduler-mutation\n'; }}
mark_maintenance_required() {{ printf 'unexpected-active-marker\n'; }}
set +e
release_candidate
rc=$?
set -e
printf 'rc=%s\n' "$rc"
""",
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert "rc=1" in result.stdout
    assert "preview-stopped" in result.stdout
    assert "candidate-restore-failed" in result.stdout
    assert "unexpected-success-checkpoint" not in result.stdout
    assert "unexpected-scheduler-mutation" not in result.stdout
    assert "unexpected-active-marker" not in result.stdout


@pytest.mark.parametrize(
    ("checkpoint_legal", "cleanup_expected"),
    ((True, True), (False, True)),
)
def test_prepare_exit_always_discards_a_nonzero_local_prepare(
    tmp_path: Path,
    checkpoint_legal: bool,
    cleanup_expected: bool,
) -> None:
    legal_result = "return 0" if checkpoint_legal else "return 1"
    result = _run_controller_harness(
        tmp_path,
        f"""
BLUEGREEN_MODE=prepare-candidate
PRE_SUPERVISOR_CANDIDATE_ARMED=true
read_checkpoint_phase_status() {{
  CHECKPOINT_PHASE=candidate_ready
  CHECKPOINT_STATUS=completed
}}
candidate_ready_state_is_legal() {{ {legal_result}; }}
cleanup_pre_switch_candidate() {{ printf 'candidate-cleaned\\n'; }}
trap prepare_exit_handler EXIT
false
""",
    )

    assert result.returncode == 1
    assert ("candidate-cleaned" in result.stdout) is cleanup_expected
    assert "Preserved exact candidate_ready state" not in result.stderr


@pytest.mark.parametrize(
    ("listeners", "accepted"),
    (
        ("LISTEN 0 128 127.0.0.1:18002 0.0.0.0:*", True),
        ("", False),
        ("LISTEN 0 128 0.0.0.0:18002 0.0.0.0:*", False),
        ("LISTEN 0 128 [::]:18002 [::]:*", False),
        (
            "LISTEN 0 128 127.0.0.1:18002 0.0.0.0:*\n"
            + "LISTEN 0 128 [::1]:18002 [::]:*",
            False,
        ),
    ),
)
def test_candidate_preview_listener_accepts_exact_ipv4_loopback_only(
    tmp_path: Path,
    listeners: str,
    accepted: bool,
) -> None:
    result = _run_controller_harness(
        tmp_path,
        f"""
ss() {{ printf '%b\\n' {listeners!r}; }}
candidate_preview_listener_is_loopback_only
""",
    )

    assert (result.returncode == 0) is accepted, result.stderr + result.stdout


@pytest.mark.parametrize("failure", ("readiness", "monthly", "cgroup"))
def test_real_candidate_validation_failures_run_pre_supervisor_cleanup(
    tmp_path: Path,
    failure: str,
) -> None:
    readiness_curl = "return 1" if failure == "readiness" else "return 0"
    monthly_gate = "return 1" if failure == "monthly" else "return 0"
    cgroup_gate = "return 1" if failure == "cgroup" else "return 0"
    result = _run_controller_harness(
        tmp_path,
        f"""
CURRENT_ACTIVE_SLOT=8000
CANDIDATE_SLOT=8001
PRE_SUPERVISOR_CANDIDATE_ARMED=true
cleanup_pre_switch_candidate() {{ printf 'cleanup:{failure}\\n'; }}
    sudo() {{ return 0; }}
    unit_property_equals() {{ return 0; }}
    sleep() {{ return 0; }}
curl() {{ {readiness_curl}; }}
python3() {{ return 0; }}
verify_candidate_monthly_gate() {{
  printf 'monthly-check\\n'
  {monthly_gate}
}}
verify_candidate_cgroup() {{
  printf 'cgroup-check\\n'
  {cgroup_gate}
}}
verify_final_runtime_seal() {{ return 0; }}
trap prepare_exit_handler EXIT
verify_candidate
""",
    )

    assert result.returncode == 1
    assert f"cleanup:{failure}" in result.stdout
    if failure == "readiness":
        assert "monthly-check" not in result.stdout
    elif failure == "monthly":
        assert "monthly-check" in result.stdout
        assert "cgroup-check" not in result.stdout
    else:
        assert "monthly-check" in result.stdout
        assert "cgroup-check" in result.stdout


def test_install_runtime_mid_failure_is_armed_before_candidate_disable(
    tmp_path: Path,
) -> None:
    script = CONTROLLER.read_text(encoding="utf-8")
    install = _shell_function(script, "install_slot_runtime")
    captured = install.index("prepare_candidate_runtime_preimage")
    armed = install.index("PRE_SUPERVISOR_CANDIDATE_ARMED=true")
    first_write = install.index("candidate_durable_install_file")
    disable = install.index(
        'systemctl disable "${SERVICE_PREFIX}${CANDIDATE_SLOT}"',
    )
    set_property = install.index(
        'systemctl set-property "${SERVICE_PREFIX}${CANDIDATE_SLOT}"',
    )
    assert captured < armed < first_write < disable < set_property

    result = _run_controller_harness(
        tmp_path,
        f"""
CANDIDATE_SLOT=8001
SYSTEMD_TEMPLATE={SLOT_UNIT}
SLOT_ENV_TEMPLATE={REPO_ROOT / "03_Scripts/deploy/systemd/jato-fullstack-backend-slot.env.example"}
prepare_candidate_runtime_preimage() {{ return 0; }}
cleanup_pre_switch_candidate() {{ printf 'mid-install-cleanup\\n'; }}
candidate_durable_install_file() {{ return 0; }}
candidate_atomic_symlink() {{ return 0; }}
unit_property_equals() {{ return 0; }}
sudo() {{
  [[ "${{1:-}}" == -n ]] && shift
  if [[ "$*" == "systemctl set-property jato-fullstack-backend@8001 MemoryHigh=3G MemoryMax=4G CPUQuota=100%" ]]; then
    return 1
  fi
  return 0
}}
trap prepare_exit_handler EXIT
install_slot_runtime
""",
    )

    assert result.returncode == 1
    assert "mid-install-cleanup" in result.stdout


@pytest.mark.parametrize(
    "failure",
    ("unit", "env", "sandbox_remove", "slot_link", "set_property"),
)
def test_each_candidate_install_failure_restores_captured_preimage(
    tmp_path: Path,
    failure: str,
) -> None:
    result = _run_controller_harness(
        tmp_path,
        f"""
CANDIDATE_SLOT=8001
SYSTEMD_TEMPLATE={SLOT_UNIT}
SLOT_ENV_TEMPLATE={REPO_ROOT / "03_Scripts/deploy/systemd/jato-fullstack-backend-slot.env.example"}
prepare_candidate_runtime_preimage() {{ printf 'preimage-captured\\n'; }}
cleanup_pre_switch_candidate() {{ printf 'preimage-restored\\n'; }}
candidate_durable_install_file() {{
  case "{failure}:$2" in
    unit:/etc/systemd/system/jato-fullstack-backend@8001.service) return 1 ;;
    env:*/8001.env) return 1 ;;
  esac
  return 0
}}
durable_remove_path() {{ [[ "{failure}" != sandbox_remove ]]; }}
candidate_atomic_symlink() {{
  [[ "{failure}" != slot_link ]]
}}
unit_property_equals() {{ return 0; }}
sudo() {{
  shift
  if [[ "{failure}" == set_property ]] &&
    [[ "$*" == "systemctl set-property jato-fullstack-backend@8001 MemoryHigh=3G MemoryMax=4G CPUQuota=100%" ]]; then
    return 1
  fi
  return 0
}}
trap prepare_exit_handler EXIT
install_slot_runtime
""",
    )

    assert result.returncode != 0
    assert "preimage-captured" in result.stdout
    assert "preimage-restored" in result.stdout
    assert result.stdout.index("preimage-captured") < result.stdout.index(
        "preimage-restored"
    )


def test_candidate_preimage_scope_excludes_active_and_previous_metadata() -> None:
    script = CONTROLLER.read_text(encoding="utf-8")
    command = _shell_function(script, "candidate_runtime_preimage_command")
    restore = _shell_function(script, "restore_candidate_runtime_preimage")

    for required in (
        "--slot-link",
        "--slot-link-stage",
        "--slot-env",
        "--slot-env-stage",
        "--explicit-unit",
        "--explicit-unit-stage",
        "--instance-dropins",
        "--persistent-control-dropins",
        "--runtime-control-dropins",
        "--candidate-cache-link",
        "--candidate-cache-private",
    ):
        assert required in command
    assert "ACTIVE_RELEASE_LINK" not in command
    assert "ACTIVE_SLOT_FILE" not in command
    assert "PREVIOUS_RELEASE_METADATA_PATH" not in command
    assert "disable --now" in restore
    assert restore.index("candidate_runtime_is_quiescent") < restore.index(
        "candidate_runtime_preimage_command restore"
    )
    assert restore.index("candidate_runtime_preimage_command restore") < restore.index(
        "systemctl daemon-reload"
    )
    assert restore.index("systemctl daemon-reload") < restore.rindex(
        "candidate_runtime_is_quiescent"
    )


def test_candidate_install_uses_only_deterministic_transaction_writers() -> None:
    script = CONTROLLER.read_text(encoding="utf-8")
    install = _shell_function(script, "install_slot_runtime")

    assert install.count("candidate_durable_install_file") == 2
    assert install.count("candidate_atomic_symlink") == 1
    assert "durable_install_file" not in install.replace(
        "candidate_durable_install_file",
        "",
    )
    assert "atomic_symlink" not in install.replace("candidate_atomic_symlink", "")
    for suffix in (
        ".service.jato-candidate-installing",
        ".env.jato-candidate-installing",
        ".current.jato-candidate-installing",
    ):
        assert suffix in install


def test_candidate_transaction_file_writer_rejects_stale_stage(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    target = tmp_path / "target"
    stage = tmp_path / ".target.jato-candidate-installing"
    source.write_text("new\n", encoding="utf-8")
    target.write_text("old\n", encoding="utf-8")
    stage.write_text("stale\n", encoding="utf-8")
    result = _run_controller_harness(
        tmp_path,
        f"""
sudo() {{
  [[ "${{1:-}}" == -n ]] && shift
  "$@"
}}
candidate_durable_install_file {source} {target} 0644 {stage}
""",
    )

    assert result.returncode != 0
    assert target.read_text(encoding="utf-8") == "old\n"
    assert stage.read_text(encoding="utf-8") == "stale\n"


def test_candidate_quiescence_fails_closed_when_listener_probe_errors(
    tmp_path: Path,
) -> None:
    result = _run_controller_harness(
        tmp_path,
        """
CANDIDATE_SLOT=8001
systemctl() {
  case "$*" in
    *LoadState*) printf 'loaded\n' ;;
    *ActiveState*) printf 'inactive\n' ;;
    *UnitFileState*) printf 'disabled\n' ;;
    *MainPID*) printf '0\n' ;;
  esac
}
ss() { return 42; }
sudo() { return 1; }
candidate_runtime_is_quiescent
""",
    )

    assert result.returncode != 0
    assert "cannot prove the Candidate listener state" in result.stderr


@pytest.mark.parametrize(
    ("load_state", "unit_file_state"),
    (("loaded", "disabled"), ("not-found", ""), ("not-found", "not-found")),
)
def test_candidate_quiescence_accepts_real_safe_systemd_state_pairs(
    tmp_path: Path,
    load_state: str,
    unit_file_state: str,
) -> None:
    result = _run_controller_harness(
        tmp_path,
        f"""
CANDIDATE_SLOT=8001
systemctl() {{
  case "$*" in
    *LoadState*) printf '{load_state}\n' ;;
    *ActiveState*) printf 'inactive\n' ;;
    *UnitFileState*) printf '{unit_file_state}\n' ;;
    *MainPID*) printf '0\n' ;;
  esac
}}
ss() {{ return 0; }}
sudo() {{ return 1; }}
candidate_runtime_is_quiescent
""",
    )

    assert result.returncode == 0, result.stderr


def test_candidate_quiescence_rejects_loaded_unit_without_disabled_state(
    tmp_path: Path,
) -> None:
    result = _run_controller_harness(
        tmp_path,
        """
CANDIDATE_SLOT=8001
systemctl() {
  case "$*" in
    *LoadState*) printf 'loaded\n' ;;
    *ActiveState*) printf 'inactive\n' ;;
    *UnitFileState*) printf '\n' ;;
    *MainPID*) printf '0\n' ;;
  esac
}
ss() { return 0; }
sudo() { return 1; }
candidate_runtime_is_quiescent
""",
    )

    assert result.returncode != 0


def test_switch_prerequisites_fail_before_route_mutation_when_old_is_unproven(
    tmp_path: Path,
) -> None:
    script = CONTROLLER.read_text(encoding="utf-8")
    switch = _shell_function(script, "switch_locked")
    assert switch.index("verify_switch_prerequisites") < switch.index(
        "SWITCH_BACKUP="
    ) < switch.index("pause_schedulers") < switch.index(
        "checkpoint_write switch_started"
    )

    for scenario in ("metadata_missing", "old_sha_mismatch"):
        resolve = (
            "return 1"
            if scenario == "metadata_missing"
            else f"PREVIOUS_RELEASE_SHA={OLD_SHA}; PREVIOUS_RELEASE_ROOT=/old"
        )
        direct = (
            "return 1"
            if scenario == "old_sha_mismatch"
            else "printf 'unexpected-direct\\n'; return 0"
        )
        result = _run_controller_harness(
            tmp_path,
            f"""
CURRENT_ACTIVE_SLOT=8000
CANDIDATE_SLOT=8001
verify_boot_reconciler_installation() {{ return 0; }}
resolve_previous_release_identity() {{ {resolve}; }}
verify_slot_release_exact() {{ {direct}; }}
verify_public_release_exact() {{ printf 'unexpected-public\\n'; return 0; }}
verify_candidate_cgroup() {{ printf 'unexpected-cgroup\\n'; return 0; }}
verify_candidate_monthly_gate() {{ printf 'unexpected-monthly\\n'; return 0; }}
set +e
verify_switch_prerequisites
rc=$?
set -e
printf 'rc=%s\\n' "$rc"
""",
        )

        assert result.returncode == 0, result.stderr + result.stdout
        assert "rc=1" in result.stdout
        assert "unexpected-public" not in result.stdout
        assert "unexpected-cgroup" not in result.stdout
        assert "unexpected-monthly" not in result.stdout


def test_switch_is_atomic_and_rolls_back_before_old_slot_can_be_lost() -> None:
    script = CONTROLLER.read_text(encoding="utf-8")
    switch = _shell_function(script, "switch_locked")
    activate = _shell_function(script, "complete_candidate_activation")
    rollback = _shell_function(script, "restore_previous_route")
    nginx_replace = switch.index(
        'durable_install_file "$candidate_conf" "$NGINX_ACTIVE_RELEASE_CONF" 0644',
    )
    nginx_test = switch.index("nginx -t", nginx_replace)
    nginx_reload = switch.index("systemctl reload nginx", nginx_test)
    nginx_verify = switch.index("verify_nginx_candidate", nginx_reload)
    switched = switch.index("checkpoint_write switched", nginx_verify)
    complete = switch.index("complete_candidate_activation", switched)
    disable_old = activate.index(
        'systemctl disable \\\n    "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}"',
    )
    stop_old = activate.index(
        'systemctl stop "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}"',
    )
    promote_memory = activate.index(
        '"MemoryHigh=$BLUEGREEN_ACTIVE_MEMORY_HIGH"',
        stop_old,
    )
    healthy = activate.index("checkpoint_write backend_healthy", promote_memory)
    handoff = activate.index("commit_backend_unit_template", promote_memory)
    direct_old = rollback.index("verify_slot_release_exact")
    public_old = rollback.index("verify_public_release_exact", direct_old)
    rollback_complete = rollback.index(
        "checkpoint_write rollback_completed",
        public_old,
    )

    assert nginx_replace < nginx_test < nginx_reload < nginx_verify < switched < complete
    assert disable_old < stop_old < promote_memory < handoff < healthy
    assert direct_old < public_old < rollback_complete
    assert "trap switch_exit_handler EXIT" in script
    assert "trap 'switch_signal_handler 1' HUP" in script
    assert "trap 'switch_signal_handler 2' INT" in script
    assert "trap 'switch_signal_handler 15' TERM" in script
    assert "EXIT_COMMAND_FAILED_MARKER_RETAINED=81" in script
    assert "restore_previous_route" in script
    assert "rollback_started" in script
    assert "rollback_completed" in script
    assert "candidate_start" in script
    assert "candidate_ready" in script
    assert "nginx_test" in script
    assert "nginx_reload" in script
    assert "post_switch_readiness" in script


def test_supervisor_evidence_failure_never_stops_a_routed_candidate(
    tmp_path: Path,
) -> None:
    result = _run_controller_harness(
        tmp_path,
        f"""
read_checkpoint_phase_status() {{
  CHECKPOINT_PHASE=source_installed
  CHECKPOINT_STATUS=completed
}}
resolve_existing_candidate_slot() {{
  CURRENT_ACTIVE_SLOT=8000
  CANDIDATE_SLOT=8001
}}
resolve_previous_release_identity() {{
  PREVIOUS_RELEASE_ROOT=/old
  PREVIOUS_RELEASE_SHA={OLD_SHA}
}}
verify_slot_release_exact() {{
  printf 'direct:%s:%s\\n' "$1" "$2"
  return 0
}}
verify_public_release_exact() {{
  printf 'public:%s\\n' "$1"
  [[ "$1" == "$DEPLOY_COMMIT_SHA" ]]
}}
mark_maintenance_required() {{ printf 'marker-retained\\n'; }}
keep_candidate_route_healthy() {{ printf 'candidate-kept\\n'; }}
cleanup_pre_switch_candidate() {{ printf 'candidate-stopped\\n'; }}
set +e
reconcile_supervisor_result 79
rc=$?
set -e
printf 'rc=%s\\n' "$rc"
""",
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert "candidate-kept" in result.stdout
    assert "candidate-stopped" not in result.stdout
    assert "rc=1" in result.stdout


def test_pre_switch_cleanup_requires_exact_old_public_route(
    tmp_path: Path,
) -> None:
    result = _run_controller_harness(
        tmp_path,
        f"""
resolve_existing_candidate_slot() {{
  CURRENT_ACTIVE_SLOT=8000
  CANDIDATE_SLOT=8001
}}
resolve_previous_release_identity() {{
  PREVIOUS_RELEASE_ROOT=/old
  PREVIOUS_RELEASE_SHA={OLD_SHA}
}}
verify_slot_release_exact() {{
  printf 'direct:%s:%s\\n' "$1" "$2"
  return 0
}}
verify_public_release_exact() {{
  printf 'public:%s\\n' "$1"
  [[ "$1" == "{OLD_SHA}" ]]
}}
cleanup_pre_switch_candidate() {{ printf 'candidate-stopped\\n'; }}
clear_maintenance_marker() {{ printf 'marker-cleared\\n'; }}
mark_maintenance_required() {{ printf 'marker-retained\\n'; }}
keep_candidate_route_healthy() {{ printf 'candidate-kept\\n'; }}
reconcile_pre_switch_state
""",
    )

    assert result.returncode == 0, result.stderr + result.stdout
    old_public = result.stdout.index(f"public:{OLD_SHA}")
    candidate_stopped = result.stdout.index("candidate-stopped")
    assert old_public < candidate_stopped
    assert "candidate-kept" not in result.stdout
    assert "marker-cleared" in result.stdout


def test_persistent_supervisor_cleans_candidate_without_mutating_nginx(
    tmp_path: Path,
) -> None:
    result = _run_controller_harness(
        tmp_path,
        f"""
BLUEGREEN_MODE=switch-locked
CURRENT_ACTIVE_SLOT=8000
CANDIDATE_SLOT=8001
SCHEDULER_STATE_FILE={tmp_path / "scheduler-state.tsv"}
TRACE_FILE={tmp_path / "supervisor-cleanup.trace"}
printf 'snapshot\\n' > "$SCHEDULER_STATE_FILE"
CANDIDATE_CLEAN=false
resolve_previous_release_identity() {{
  PREVIOUS_RELEASE_ROOT=/old
  PREVIOUS_RELEASE_SHA={OLD_SHA}
}}
verify_slot_release_exact() {{ printf 'old-direct\\n'; }}
verify_public_release_exact() {{
  printf 'old-public\\n'
  printf 'old-public\\n' >> "$TRACE_FILE"
}}
restore_nginx_preimage() {{ printf 'unsafe-preimage-restore\\n'; }}
stop_candidate_preview() {{ return 0; }}
mark_maintenance_required() {{ printf 'marker-retained\\n'; }}
candidate_cleanup_is_complete() {{ [[ "$CANDIDATE_CLEAN" == true ]]; }}
unit_property_equals() {{ return 0; }}
restore_candidate_runtime_preimage() {{
  CANDIDATE_CLEAN=true
  printf 'candidate-preimage-restored\\n'
  printf 'candidate-stopped\\n' >> "$TRACE_FILE"
}}
resume_schedulers() {{ printf 'schedulers-restored\\n'; }}
sudo() {{
  [[ "${{1:-}}" == -n ]] && shift
  printf 'sudo:%s\\n' "$*"
  if [[ "$*" == "systemctl disable --now jato-fullstack-backend@8001" ]]; then
    printf 'unexpected-direct-stop\\n' >> "$TRACE_FILE"
  fi
}}
set +e
cleanup_pre_switch_candidate
rc=$?
set -e
printf 'rc=%s\\n' "$rc"
cat "$TRACE_FILE"
""",
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert "old-direct" in result.stdout
    assert "old-public" in result.stdout
    assert "marker-retained" in result.stdout
    assert "unsafe-preimage-restore" not in result.stdout
    trace = (tmp_path / "supervisor-cleanup.trace").read_text(encoding="utf-8")
    assert trace.index("old-public") < trace.index("candidate-stopped")
    assert "candidate-preimage-restored" in result.stdout
    assert "schedulers-restored" in result.stdout
    assert "rc=1" in result.stdout


def test_rollback_checkpoint_is_written_only_after_exact_old_route(
    tmp_path: Path,
) -> None:
    backup = tmp_path / "active-release.backup"
    backup.write_text("old nginx config", encoding="utf-8")
    result = _run_controller_harness(
        tmp_path,
        f"""
SWITCH_BACKUP={backup}
CURRENT_ACTIVE_SLOT=8000
CANDIDATE_SLOT=8001
ROLLBACK_COMPLETED_WRITTEN=false
CANDIDATE_STOPPED_BEFORE_CHECKPOINT=false
read_checkpoint_phase_status() {{
  CHECKPOINT_PHASE=switched
  CHECKPOINT_STATUS=completed
}}
evidence_binding() {{ printf 'evidence'; }}
checkpoint_write() {{
  printf 'checkpoint:%s:%s\\n' "$1" "$2"
  if [[ "$1" == rollback_completed ]]; then
    [[ "$CANDIDATE_STOPPED_BEFORE_CHECKPOINT" == true ]] || return 1
    ROLLBACK_COMPLETED_WRITTEN=true
  fi
}}
resolve_previous_release_identity() {{
  PREVIOUS_RELEASE_ROOT=/old
  PREVIOUS_RELEASE_SHA={OLD_SHA}
}}
sudo() {{
  [[ "${{1:-}}" == -n ]] && shift
  if [[ "$*" == "systemctl disable --now jato-fullstack-backend@8001" ]]; then
    [[ "$ROLLBACK_COMPLETED_WRITTEN" == false ]] || return 1
    CANDIDATE_STOPPED_BEFORE_CHECKPOINT=true
  fi
  printf 'sudo:%s\\n' "$*"
}}
verify_slot_release_exact() {{
  printf 'direct:%s:%s\\n' "$1" "$2"
}}
verify_public_release_exact() {{ printf 'public:%s\\n' "$1"; }}
atomic_text() {{ printf 'active-slot:%s\\n' "$2"; }}
atomic_symlink() {{ printf 'active-link:%s\\n' "$1"; }}
verify_durable_route_ownership() {{ printf 'durable-route:%s:%s\\n' "$1" "$3"; }}
restore_candidate_runtime_preimage() {{
  CANDIDATE_STOPPED_BEFORE_CHECKPOINT=true
  printf 'candidate-preimage-restored\\n'
}}
restore_backend_template_preimage() {{ return 0; }}
remove_backend_template_preimage() {{ return 0; }}
unit_property_equals() {{ return 0; }}
resume_schedulers() {{ printf 'schedulers-restored\\n'; }}
mark_maintenance_required() {{ printf 'marker-retained\\n'; }}
keep_candidate_route_healthy() {{ printf 'candidate-kept\\n'; }}
restore_previous_route
printf 'rolled-back=%s\\n' "$RELEASE_ROLLED_BACK"
printf 'candidate-stopped-before-checkpoint=%s\\n' "$CANDIDATE_STOPPED_BEFORE_CHECKPOINT"
""",
    )

    assert result.returncode == 0, result.stderr + result.stdout
    output = result.stdout
    rollback_started = output.index("checkpoint:rollback_started:in_progress")
    direct_old = output.index(f"direct:8000:{OLD_SHA}")
    nginx_test = output.index("sudo:nginx -t")
    public_old = output.index(f"public:{OLD_SHA}")
    durable_route = output.index(f"durable-route:8000:{OLD_SHA}")
    schedulers = output.index("schedulers-restored")
    rollback_complete = output.index("checkpoint:rollback_completed:completed")
    assert (
        rollback_started
        < direct_old
        < nginx_test
        < public_old
        < durable_route
        < schedulers
        < rollback_complete
    )
    assert "candidate-kept" not in output
    assert "rolled-back=true" in output
    assert "candidate-stopped-before-checkpoint=true" in output


def test_failed_old_public_verification_keeps_candidate_and_nonterminal_checkpoint(
    tmp_path: Path,
) -> None:
    backup = tmp_path / "active-release.backup"
    backup.write_text("old nginx config", encoding="utf-8")
    result = _run_controller_harness(
        tmp_path,
        f"""
SWITCH_BACKUP={backup}
CURRENT_ACTIVE_SLOT=8000
CANDIDATE_SLOT=8001
read_checkpoint_phase_status() {{
  CHECKPOINT_PHASE=switched
  CHECKPOINT_STATUS=completed
}}
evidence_binding() {{ printf 'evidence'; }}
checkpoint_write() {{ printf 'checkpoint:%s:%s\\n' "$1" "$2"; }}
resolve_previous_release_identity() {{
  PREVIOUS_RELEASE_ROOT=/old
  PREVIOUS_RELEASE_SHA={OLD_SHA}
}}
sudo() {{
  [[ "${{1:-}}" == -n ]] && shift
  printf 'sudo:%s\\n' "$*"
}}
verify_slot_release_exact() {{ printf 'direct-old\\n'; }}
verify_public_release_exact() {{
  printf 'public-old-failed\\n'
  return 1
}}
atomic_text() {{ printf 'unexpected-active-slot\\n'; }}
atomic_symlink() {{ printf 'unexpected-active-link\\n'; }}
resume_schedulers() {{ printf 'unexpected-resume\\n'; }}
mark_maintenance_required() {{ printf 'marker-retained\\n'; }}
keep_candidate_route_healthy() {{ printf 'candidate-kept\\n'; }}
set +e
restore_previous_route
rc=$?
set -e
printf 'rc=%s\\n' "$rc"
""",
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert "checkpoint:rollback_started:in_progress" in result.stdout
    assert "checkpoint:rollback_completed" not in result.stdout
    assert "candidate-kept" in result.stdout
    assert "disable --now jato-fullstack-backend@8001" not in result.stdout
    assert "rc=1" in result.stdout


def test_unresolved_term_uses_marker_retention_protocol(tmp_path: Path) -> None:
    result = _run_controller_harness(
        tmp_path,
        """
BLUEGREEN_MODE=switch-locked
SWITCH_COMPLETED=false
read_checkpoint_phase_status() {
  CHECKPOINT_PHASE=switched
  CHECKPOINT_STATUS=completed
}
restore_previous_route() {
  printf 'rollback-unresolved\\n'
  return 1
}
trap switch_exit_handler EXIT
trap 'switch_signal_handler 15' TERM
kill -TERM "$$"
""",
    )

    assert result.returncode == 81
    assert "rollback-unresolved" in result.stdout


def test_hup_int_term_and_exit_all_run_route_reconciliation(
    tmp_path: Path,
) -> None:
    scenarios = (
        ("HUP", 1, 129),
        ("INT", 2, 130),
        ("TERM", 15, 143),
    )
    for signal_name, signal_number, expected_rc in scenarios:
        result = _run_controller_harness(
            tmp_path,
            f"""
BLUEGREEN_MODE=switch-locked
SWITCH_COMPLETED=false
read_checkpoint_phase_status() {{
  CHECKPOINT_PHASE=switched
  CHECKPOINT_STATUS=completed
}}
restore_previous_route() {{ printf 'reconciled-{signal_name}\\n'; }}
trap switch_exit_handler EXIT
trap 'switch_signal_handler {signal_number}' {signal_name}
kill -{signal_name} "$$"
""",
        )
        assert result.returncode == expected_rc
        assert f"reconciled-{signal_name}" in result.stdout

    exit_result = _run_controller_harness(
        tmp_path,
        """
BLUEGREEN_MODE=switch-locked
SWITCH_COMPLETED=false
read_checkpoint_phase_status() {
  CHECKPOINT_PHASE=switched
  CHECKPOINT_STATUS=completed
}
restore_previous_route() { printf 'reconciled-EXIT\\n'; }
trap switch_exit_handler EXIT
exit 7
""",
    )
    assert exit_result.returncode == 7
    assert "reconciled-EXIT" in exit_result.stdout


def test_committed_candidate_reconciliation_is_idempotent_and_opens_monthly_gate(
    tmp_path: Path,
) -> None:
    result = _run_controller_harness(
        tmp_path,
        """
read_checkpoint_phase_status() {
  CHECKPOINT_PHASE=backend_healthy
  CHECKPOINT_STATUS=completed
}
resolve_existing_candidate_slot() {
  CURRENT_ACTIVE_SLOT=8000
  CANDIDATE_SLOT=8001
}
verify_slot_release_exact() { printf 'candidate-direct\\n'; }
verify_public_release_exact() { printf 'candidate-public\\n'; }
atomic_text() { printf 'owner:%s\\n' "$2"; }
atomic_symlink() { printf 'link:%s\\n' "$1"; }
verify_active_cgroup() { printf 'active-cgroup\\n'; }
verify_final_runtime_seal() { printf 'runtime-seal\\n'; }
verify_candidate_reboot_gate() { printf 'reboot-gate\\n'; }
unit_property_equals() { return 0; }
verify_durable_route_ownership() { printf 'durable-candidate-route\\n'; }
run_post_commit_global_reconciliation() { printf 'global-reconciled\\n'; }
remove_backend_template_preimage() { printf 'template-preimage-removed\\n'; }
remove_nginx_preimage() { printf 'preimage-removed\\n'; }
discard_candidate_runtime_preimage() { printf 'candidate-preimage-discarded\\n'; }
clear_maintenance_marker() { printf 'marker-cleared\\n'; }
verify_active_monthly_gate_released() { printf 'monthly-open:%s\\n' "$1"; }
sudo() {
  [[ "${1:-}" == -n ]] && shift
  if [[ "$*" == "systemctl disable --now jato-fullstack-backend@8000" ]]; then
    OLD_DISABLED=true
  fi
  return 0
}
OLD_DISABLED=false
reconcile_existing_switch
printf 'old-disabled=%s\\n' "$OLD_DISABLED"
""",
    )

    assert result.returncode == 0, result.stderr + result.stdout
    for token in (
        "candidate-direct",
        "candidate-public",
        "owner:8001",
        "durable-candidate-route",
        "global-reconciled",
        "candidate-preimage-discarded",
        "preimage-removed",
        "marker-cleared",
        "monthly-open:8001",
        "old-disabled=true",
    ):
        assert token in result.stdout


def test_rollback_completed_reconciliation_rechecks_durable_old_route(
    tmp_path: Path,
) -> None:
    result = _run_controller_harness(
        tmp_path,
        f"""
read_checkpoint_phase_status() {{
  CHECKPOINT_PHASE=rollback_completed
  CHECKPOINT_STATUS=completed
}}
resolve_existing_candidate_slot() {{
  CURRENT_ACTIVE_SLOT=8000
  CANDIDATE_SLOT=8001
}}
resolve_previous_release_identity() {{
  PREVIOUS_RELEASE_ROOT=/old
  PREVIOUS_RELEASE_SHA={OLD_SHA}
}}
verify_slot_release_exact() {{ printf 'old-direct\\n'; }}
verify_public_release_exact() {{ printf 'old-public-and-frontend\\n'; }}
atomic_text() {{ printf 'owner:%s\\n' "$2"; }}
atomic_symlink() {{ printf 'link:%s\\n' "$1"; }}
verify_durable_route_ownership() {{ printf 'durable-old-route\\n'; }}
restore_backend_template_preimage() {{ return 0; }}
remove_backend_template_preimage() {{ return 0; }}
restore_candidate_runtime_preimage() {{ printf 'candidate-preimage-restored\\n'; }}
unit_property_equals() {{ return 0; }}
clear_maintenance_marker() {{ printf 'marker-cleared\\n'; }}
verify_active_monthly_gate_released() {{ printf 'monthly-open:%s\\n' "$1"; }}
sudo() {{ return 0; }}
reconcile_existing_switch
printf 'rolled-back=%s\\n' "$RELEASE_ROLLED_BACK"
""",
    )

    assert result.returncode == 0, result.stderr + result.stdout
    durable = result.stdout.index("durable-old-route")
    cleared = result.stdout.index("marker-cleared")
    assert durable < cleared
    assert "old-public-and-frontend" in result.stdout
    assert "monthly-open:8000" in result.stdout
    assert "rolled-back=true" in result.stdout


def test_persistent_supervisor_uses_global_unit_and_only_durable_paths(
    tmp_path: Path,
) -> None:
    environment = _controller_environment(tmp_path)
    durable_release = (
        Path(environment["RELEASES_ROOT"])
        / TARGET_SHA
        / environment["DEPLOY_ARCHIVE_SHA256"]
    )
    result = _run_controller_harness(
        tmp_path,
        f"""
CURRENT_ACTIVE_SLOT=8000
CANDIDATE_SLOT=8001
systemctl() {{
  if [[ "$*" == *MainPID* ]]; then
    printf '4321\\n'
  elif [[ "$*" == *LoadState* ]]; then
    printf 'not-found\\n'
  else
    printf 'inactive\\n'
  fi
}}
slot_release_root() {{ printf '%s\\n' '{tmp_path / "old-release"}'; }}
sudo() {{
  [[ "${{1:-}}" == -n ]] && shift
  printf 'arg:%s\\n' "$@"
}}
run_switch_supervisor
""",
    )

    assert result.returncode == 0, result.stderr + result.stdout
    output = result.stdout
    assert "arg:systemd-run" in output
    assert "arg:--unit=jato-bluegreen-production.service" in output
    assert "arg:--wait" in output
    assert "arg:--collect" in output
    assert "arg:--pipe" not in output
    assert "arg:--property=KillMode=control-group" in output
    assert "arg:--active-main-pid" in output
    assert "arg:4321" in output
    assert "arg:--expected-project-root" in output
    assert "arg:--active-bundle-lock" in output
    assert f"arg:{durable_release}/03_Scripts/deploy/jato_quiescence_gate.py" in output
    assert f"arg:{durable_release}/03_Scripts/deploy/tencent_bluegreen_release.sh" in output
    assert environment["RELEASE_WORKTREE"] not in output
    assert environment["PREBUILT_FRONTEND_DIR"] not in output
    assert "SECRET" not in output
    assert "API_KEY" not in output


def test_global_persistent_unit_blocks_overlapping_release(tmp_path: Path) -> None:
    result = _run_controller_harness(
        tmp_path,
        """
systemctl() {
  if [[ "$*" == *LoadState* ]]; then
    printf 'loaded\\n'
  else
    printf 'active\\n'
  fi
}
set +e
assert_no_active_switch_unit
rc=$?
set -e
printf 'rc=%s\\n' "$rc"
""",
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert "rc=1" in result.stdout
    assert "another persistent production blue/green controller is active" in result.stderr


def test_memory_and_monthly_worker_contract_is_fail_closed() -> None:
    script = CONTROLLER.read_text(encoding="utf-8")
    unit = SLOT_UNIT.read_text(encoding="utf-8")
    nginx = NGINX.read_text(encoding="utf-8")
    install = _shell_function(script, "install_slot_runtime")

    assert "BLUEGREEN_CANDIDATE_MEMORY_HIGH:-3G" in script
    assert "BLUEGREEN_CANDIDATE_MEMORY_MAX:-4G" in script
    assert "BLUEGREEN_ACTIVE_MEMORY_HIGH:-6G" in script
    assert "BLUEGREEN_ACTIVE_MEMORY_MAX:-8G" in script
    assert "APP_JATO_MONTHLY_ACTIVE_SLOT_FILE" in script
    assert "APP_JATO_MONTHLY_DEPLOYMENT_MARKER" in script
    assert "APP_JATO_MONTHLY_ENABLED=true" in script
    assert "APP_JATO_MONTHLY_EXECUTION_MODE=subprocess" in script
    assert "ProtectSystem=strict" not in install
    assert "DynamicUser=yes" not in install
    assert "APP_REDIS_ENABLED=false" not in install
    assert "default_transaction_read_only=on" not in install
    assert "ReadOnlyPaths=" not in install
    assert 'durable_remove_path "$sandbox_dropin"' in install
    data_access = _shell_function(script, "verify_candidate_data_access_contract")
    assert "APP_DATABASE_URL" in data_access
    assert "APP_REDIS_URL" in data_access
    assert "Candidate and Active differ for data connection key" in data_access
    assert "HERMES_RUN_ENABLED" in data_access
    assert "PREWARM_ENABLED" in data_access
    assert 'local service_target="/etc/systemd/system/${SERVICE_PREFIX}${CANDIDATE_SLOT}.service"' in install
    assert 'service_target="/etc/systemd/system/jato-fullstack-backend@.service"' not in install
    disable_candidate = install.index(
        'systemctl disable "${SERVICE_PREFIX}${CANDIDATE_SLOT}"',
    )
    candidate_limit = install.index(
        '"MemoryMax=$BLUEGREEN_CANDIDATE_MEMORY_MAX"',
        disable_candidate,
    )
    assert disable_candidate < candidate_limit
    assert 'systemctl enable "${SERVICE_PREFIX}${CANDIDATE_SLOT}"' not in install
    verify = _shell_function(script, "verify_candidate")
    assert "UnitFileState disabled" in verify
    prerequisites = _shell_function(script, "verify_switch_prerequisites")
    assert "candidate became boot-enabled before the atomic route switch" in prerequisites
    unsandbox = _shell_function(script, "remove_candidate_sandbox_before_switch")
    assert unsandbox.index('durable_remove_path "$dropin"') < unsandbox.index(
        'systemctl restart "${SERVICE_PREFIX}${CANDIDATE_SLOT}"'
    ) < unsandbox.index("verify_candidate")
    assert "MemoryAccounting=yes" in unit
    assert "KillMode=control-group" in unit
    assert "if (-f /var/lib/jato-release/deployment-maintenance)" in nginx
    assert "location ^~ /v1/msrp/monthly-update" in nginx


@pytest.mark.skipif(
    os.name != "posix" or not Path("/proc/self/environ").exists(),
    reason="requires Linux procfs",
)
@pytest.mark.parametrize("different_path", (False, True))
def test_candidate_data_contract_resolves_runtime_paths(
    tmp_path: Path,
    different_path: bool,
) -> None:
    legacy_data = tmp_path / "legacy-data"
    shared_data = tmp_path / "shared-data"
    legacy_data.mkdir()
    shared_data.symlink_to(legacy_data, target_is_directory=True)
    path_keys = {
        "JATO_PARQUET_PATH",
        "JATO_PARTITIONED_PATH",
        "APP_CRUD_DATA_PATH",
        "APP_ENGINEERING_IMPORT_ROOT",
        "MSRP_GOVERNANCE_EVIDENCE_ROOT",
        "APP_LOCAL_WIKI_DB_PATH",
    }
    common = {
        **os.environ,
        "APP_DATABASE_ENABLED": "true",
        "APP_DATABASE_URL": "postgresql://runtime",
        "APP_REDIS_ENABLED": "true",
        "APP_REDIS_URL": "redis://runtime",
        "PGOPTIONS": "-c statement_timeout=30s",
        **{key: str(legacy_data) for key in path_keys},
    }
    candidate = {
        **common,
        **{key: str(shared_data) for key in path_keys},
    }
    if different_path:
        different_data = tmp_path / "different-data"
        different_data.mkdir()
        candidate["JATO_PARQUET_PATH"] = str(different_data)

    result = _run_candidate_data_contract(candidate, common)

    assert (result.returncode != 0) is different_path, result.stderr
    if different_path:
        assert "different runtime paths for JATO_PARQUET_PATH" in result.stderr


@pytest.mark.skipif(
    os.name != "posix" or not Path("/proc/self/environ").exists(),
    reason="requires Linux procfs",
)
@pytest.mark.parametrize("different_path", (False, True))
def test_candidate_data_contract_resolves_legacy_defaults(
    tmp_path: Path,
    different_path: bool,
) -> None:
    legacy_root = tmp_path / "legacy"
    candidate_root = tmp_path / "candidate"
    legacy_raw_data = legacy_root / "01_RAW_DATA"
    legacy_data = legacy_root / "04_Processed_data"
    legacy_raw_data.mkdir(parents=True)
    legacy_data.mkdir(parents=True)
    candidate_root.mkdir()
    (candidate_root / "01_RAW_DATA").symlink_to(
        legacy_raw_data,
        target_is_directory=True,
    )
    (candidate_root / "04_Processed_data").symlink_to(
        legacy_data,
        target_is_directory=True,
    )
    common = {
        **os.environ,
        "APP_DATABASE_ENABLED": "true",
        "APP_DATABASE_URL": "postgresql://runtime",
        "APP_PROJECT_ROOT": str(legacy_root),
    }
    candidate = {
        **common,
        "APP_PROJECT_ROOT": str(candidate_root),
        "MSRP_GOVERNANCE_EVIDENCE_ROOT": str(
            candidate_root
            / "04_Processed_data"
            / "ops"
            / "msrp_source_evidence"
        ),
    }
    if different_path:
        candidate["MSRP_GOVERNANCE_EVIDENCE_ROOT"] = str(
            tmp_path / "different-evidence"
        )

    result = _run_candidate_data_contract(candidate, common)

    assert (result.returncode != 0) is different_path, result.stderr
    if different_path:
        assert (
            "different runtime paths for MSRP_GOVERNANCE_EVIDENCE_ROOT"
            in result.stderr
        )


@pytest.mark.parametrize("active_slot", ("8000", "8001"))
def test_active_cgroup_verification_uses_explicit_active_slot(
    tmp_path: Path,
    active_slot: str,
) -> None:
    slot_env = tmp_path / "slot-env" / f"{active_slot}.env"
    slot_env.parent.mkdir(parents=True)
    slot_env.write_text("APP_BACKEND_WORKERS=2\n", encoding="utf-8")
    result = _run_controller_harness(
        tmp_path,
        f"""
EXPECTED_SLOT={active_slot}
systemctl() {{
  [[ "${{1:-}}" == show ]]
  [[ "${{2:-}}" == "${{SERVICE_PREFIX}}${{EXPECTED_SLOT}}" ]]
  case "${{4:-}}" in
    MemoryHigh) printf '%s\n' "$((6 * 1024 * 1024 * 1024))" ;;
    MemoryMax) printf '%s\n' "$((8 * 1024 * 1024 * 1024))" ;;
    *) return 91 ;;
  esac
}}
sudo() {{
  [[ "${{1:-}}" == -n ]] && shift
  "$@"
}}
verify_backend_cgroup_processes_only() {{
  printf 'process-slot=%s\n' "$1"
  [[ "$1" == "$EXPECTED_SLOT" ]]
}}
verify_active_cgroup "$EXPECTED_SLOT"
""",
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert f"process-slot={active_slot}" in result.stdout


def test_active_cgroup_verification_rejects_implicit_candidate_slot(
    tmp_path: Path,
) -> None:
    result = _run_controller_harness(
        tmp_path,
        """
CANDIDATE_SLOT=8001
systemctl() { printf 'unexpected-systemctl\n'; return 0; }
sudo() { printf 'unexpected-sudo\n'; return 0; }
verify_backend_cgroup_processes_only() {
  printf 'unexpected-process-check\n'
  return 0
}
set +e
verify_active_cgroup
rc=$?
set -e
printf 'rc=%s\n' "$rc"
""",
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert "rc=1" in result.stdout
    assert "unexpected-systemctl" not in result.stdout
    assert "unexpected-sudo" not in result.stdout
    assert "unexpected-process-check" not in result.stdout


def test_active_cgroup_call_sites_use_the_runtime_owning_slot() -> None:
    script = CONTROLLER.read_text(encoding="utf-8")
    fixed_exact = _shell_function(script, "fixed_active_runtime_is_exact_base")
    fixed_install = _shell_function(script, "install_release_on_fixed_active")
    legacy_activation = _shell_function(script, "complete_candidate_activation")
    legacy_reconcile = _shell_function(script, "reconcile_existing_switch")

    assert 'verify_active_cgroup "$CURRENT_ACTIVE_SLOT"' in fixed_exact
    assert 'verify_active_cgroup "$CURRENT_ACTIVE_SLOT"' in fixed_install
    assert 'verify_active_cgroup "$CANDIDATE_SLOT"' in legacy_activation
    assert 'verify_active_cgroup "$CANDIDATE_SLOT"' in legacy_reconcile


def test_bluegreen_v1_forbids_schema_delta_and_disables_prewarm() -> None:
    script = CONTROLLER.read_text(encoding="utf-8")
    assert "Blue/green v1 forbids Alembic changes" in script
    assert "current != heads" in script
    run_inner = _shell_function(script, "run_inner_prepare")
    assert "RUN_DATABASE_MIGRATIONS=verify_only" in run_inner
    assert "RUN_DATABASE_MIGRATIONS=false" not in run_inner
    assert "RUN_DATABASE_MIGRATIONS=false" in _shell_function(
        script,
        "run_post_activation",
    )
    assert "RUN_DATABASE_MIGRATIONS=false" in _shell_function(
        script,
        "run_post_commit_global_reconciliation",
    )
    assert "RUN_GROUPED_TIME_SERIES_PREWARM=false" in script
    assert "APP_GROUPED_TIME_SERIES_PREWARM_ENABLED=false" in (
        REPO_ROOT
        / "03_Scripts/deploy/systemd/jato-fullstack-backend-slot.env.example"
    ).read_text(encoding="utf-8")


def test_inner_has_separate_prepare_and_post_activation_modes() -> None:
    inner = INNER.read_text(encoding="utf-8")
    assert "BLUEGREEN_PREPARE_ONLY" in inner
    assert "BLUEGREEN_POST_ACTIVATION_ONLY" in inner
    assert "SYSTEMD_RUNTIME_ROOT" in inner
    prepare_exit = inner.index(
        "Blue/green candidate preparation completed before service start",
    )
    in_place_switch = inner.index(
        'write_release_checkpoint switch_started in_progress rollback_required',
    )
    assert prepare_exit < in_place_switch


def test_public_release_verification_checks_each_validated_hostname() -> None:
    script = CONTROLLER.read_text(encoding="utf-8")
    normalize = script[
        script.index("normalized_deploy_server_names()"):
        script.index("verify_nginx_payloads()")
    ]
    verify = _shell_function(script, "verify_nginx_candidate")

    assert 'names = raw.split() or ["_"]' in normalize
    assert 'if "_" in names:' in normalize
    assert "label_pattern.fullmatch(label)" in normalize
    assert 'for server_name in "${server_names[@]}"' in verify
    assert '--resolve "${server_name}:443:127.0.0.1"' in verify
    assert '"https://${server_name}/readyz"' in verify
    assert '"https://${server_name}/build-meta.json"' in verify
    assert '--resolve "${DEPLOY_SERVER_NAME}:443:127.0.0.1"' not in verify


def test_scheduler_pause_and_resume_preserve_exact_prior_state() -> None:
    script = CONTROLLER.read_text(encoding="utf-8")
    pause = script[
        script.index("pause_schedulers()"):
        script.index("resume_schedulers()")
    ]
    resume = script[
        script.index("resume_schedulers()"):
        script.index("normalized_deploy_server_names()")
    ]

    assert pause.index("snapshot_scheduler_state") < pause.index(
        'systemctl stop "$timer"',
    )
    assert "systemctl is-enabled" in script
    assert "systemctl is-active --quiet" in script
    assert "No scheduler state snapshot exists; scheduler restore is a no-op" in resume
    assert 'restore_scheduler_enablement "$timer" "$enabled_state"' in resume
    assert 'if [[ "$active_state" == "true" ]]' in resume
    assert 'systemctl start "$timer" || true' not in resume
    assert 'durable_remove_path "$SCHEDULER_STATE_FILE"' in resume


def test_scheduler_restore_behavior_does_not_start_a_previously_inactive_timer(
    tmp_path: Path,
) -> None:
    script = CONTROLLER.read_text(encoding="utf-8")
    scheduler_functions = script[
        script.index("is_known_scheduler_timer()"):
        script.index("normalized_deploy_server_names()")
    ]
    harness = (
        "set -Eeuo pipefail\n"
        f"SCHEDULER_STATE_FILE={tmp_path / 'scheduler-state.tsv'}\n"
        "SCHEDULER_TIMERS=(example.timer)\n"
        "SCHEDULER_SERVICES=(example.service)\n"
        "SCHEDULERS_PAUSED=false\n"
        "ENABLE_STATE=disabled\n"
        "ACTIVE_STATE=false\n"
        "fail() { printf '[ERROR] %s\\n' \"$*\" >&2; return 1; }\n"
        "durable_install_file() { cp \"$1\" \"$2\"; chmod \"$3\" \"$2\"; }\n"
        "durable_remove_path() { rm -f \"$1\"; }\n"
        "sudo() { if [[ \"${1:-}\" == '-n' ]]; then shift; fi; \"$@\"; }\n"
        "systemctl() {\n"
        "  local command=\"$1\"\n"
        "  shift\n"
        "  case \"$command\" in\n"
        "    cat) return 0 ;;\n"
        "    is-enabled)\n"
        "      printf '%s\\n' \"$ENABLE_STATE\"\n"
        "      [[ \"$ENABLE_STATE\" != disabled ]]\n"
        "      ;;\n"
        "    is-active)\n"
        "      if [[ \"${1:-}\" == '--quiet' ]]; then shift; fi\n"
        "      [[ \"$ACTIVE_STATE\" == true ]]\n"
        "      ;;\n"
        "    stop) ACTIVE_STATE=false ;;\n"
        "    start) ACTIVE_STATE=true ;;\n"
        "    unmask) [[ \"$ENABLE_STATE\" != masked ]] || ENABLE_STATE=disabled ;;\n"
        "    disable) ENABLE_STATE=disabled ;;\n"
        "    enable)\n"
        "      if [[ \"${1:-}\" == '--runtime' ]]; then\n"
        "        ENABLE_STATE=enabled-runtime\n"
        "      else\n"
        "        ENABLE_STATE=enabled\n"
        "      fi\n"
        "      ;;\n"
        "    mask)\n"
        "      if [[ \"${1:-}\" == '--runtime' ]]; then\n"
        "        ENABLE_STATE=masked-runtime\n"
        "      else\n"
        "        ENABLE_STATE=masked\n"
        "      fi\n"
        "      ;;\n"
        "    *) return 1 ;;\n"
        "  esac\n"
        "}\n"
        + scheduler_functions
        + "\n"
        "pause_schedulers\n"
        "[[ -f \"$SCHEDULER_STATE_FILE\" ]]\n"
        "ENABLE_STATE=enabled\n"
        "ACTIVE_STATE=true\n"
        "resume_schedulers\n"
        "[[ \"$ENABLE_STATE\" == disabled ]]\n"
        "[[ \"$ACTIVE_STATE\" == false ]]\n"
        "[[ ! -e \"$SCHEDULER_STATE_FILE\" ]]\n"
        "resume_schedulers\n"
        "[[ \"$ENABLE_STATE\" == disabled ]]\n"
        "[[ \"$ACTIVE_STATE\" == false ]]\n"
    )

    result = subprocess.run(
        ["bash", "-s"],
        input=harness,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr + result.stdout


@pytest.mark.parametrize(
    "snapshot",
    ("", "one.timer\tdisabled\tfalse\n"),
)
def test_scheduler_restore_rejects_blank_or_truncated_snapshot(
    tmp_path: Path,
    snapshot: str,
) -> None:
    state = tmp_path / "scheduler-state.tsv"
    state.write_text(snapshot, encoding="utf-8")
    script = CONTROLLER.read_text(encoding="utf-8")
    scheduler_functions = script[
        script.index("is_known_scheduler_timer()"):
        script.index("normalized_deploy_server_names()")
    ]
    result = subprocess.run(
        [
            "bash",
            "-c",
            "set -Eeuo pipefail\n"
            + f"SCHEDULER_STATE_FILE={state}\n"
            + "SCHEDULER_TIMERS=(one.timer two.timer)\n"
            + "SCHEDULER_SERVICES=()\n"
            + "SCHEDULERS_PAUSED=true\n"
            + "fail() { printf '[ERROR] %s\\n' \"$*\" >&2; return 1; }\n"
            + "restore_scheduler_enablement() { return 0; }\n"
            + "durable_remove_path() { printf 'unexpected-remove\\n'; }\n"
            + "systemctl() {\n"
            + "  case \"$1\" in\n"
            + "    cat|stop) return 0 ;;\n"
            + "    is-active) return 1 ;;\n"
            + "    *) return 0 ;;\n"
            + "  esac\n"
            + "}\n"
            + "sudo() { [[ \"${1:-}\" == -n ]] && shift; \"$@\"; }\n"
            + scheduler_functions
            + "\nset +e\nresume_schedulers\nrc=$?\nset -e\n"
            + "printf 'rc=%s\\n' \"$rc\"\n",
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert "rc=1" in result.stdout
    assert "unexpected-remove" not in result.stdout
    assert state.exists()


def test_bluegreen_post_commit_preserves_timer_state_and_migrates_msrp_api() -> None:
    inner = INNER.read_text(encoding="utf-8")
    reconcile = inner[
        inner.index("reconcile_scraper_schedulers()"):
        inner.index("run_post_deploy_readiness_audits()")
    ]
    global_only = inner[
        inner.index('if [[ "$BLUEGREEN_GLOBAL_RECONCILE_ONLY" == "true" ]]'):
        inner.index('if [[ "$BLUEGREEN_POST_ACTIVATION_ONLY" == "true" ]]')
    ]
    post_only = inner[
        inner.index('if [[ "$BLUEGREEN_POST_ACTIVATION_ONLY" == "true" ]]'):
        inner.index('CURRENT_STEP="Validate sudo access"')
    ]

    assert "RECONCILE_SCRAPER_TIMER_STATE" in reconcile
    assert "without changing timer enabled/active state" in reconcile
    assert "JATO_API_BASE" in reconcile
    assert "http://127.0.0.1:18000/v1" in reconcile
    assert global_only.index("RECONCILE_SCRAPER_TIMER_STATE=false") < global_only.index(
        "reconcile_scraper_schedulers",
    )
    assert "reconcile_scraper_schedulers" not in post_only
    assert "upsert_managed_env_value()" in inner
    assert 'sudo -n mv -f "$remote_candidate" "$target_path"' in inner


def test_runtime_seal_retry_and_switch_order_is_fail_closed() -> None:
    script = CONTROLLER.read_text(encoding="utf-8")
    prepare_runtime = _shell_function(script, "prepare_candidate_runtime")
    finalize = _shell_function(script, "finalize_runtime_seal")
    verify_candidate = _shell_function(script, "verify_candidate")
    unsandbox = _shell_function(script, "remove_candidate_sandbox_before_switch")
    switch = _shell_function(script, "switch_locked")
    build = _shell_function(script, "build_candidate_runtime_locked")
    build_scope = _shell_function(script, "run_candidate_build_scope")
    prepare = _shell_function(script, "prepare_and_switch")

    assert prepare_runtime.index("umask 0022") < prepare_runtime.index(
        'sudo -n test -e "$RELEASE_RUNTIME_SEAL_FILE"',
    )
    assert prepare_runtime.index("verify_final_runtime_seal") < prepare_runtime.index(
        "RUNTIME_ALREADY_SEALED=true",
    )
    assert 'for relative in (".venv", "06_AppPlatform/frontend/dist")' in prepare_runtime
    assert "refusing to overwrite an existing final runtime seal" in finalize
    assert verify_candidate.index("verify_final_runtime_seal") < verify_candidate.index(
        "systemctl start",
    )
    assert unsandbox.index("verify_final_runtime_seal") < unsandbox.index(
        'durable_remove_path "$dropin"',
    )
    assert switch.index("verify_switch_prerequisites") < switch.index(
        'durable_install_file "$NGINX_ACTIVE_RELEASE_CONF" "$SWITCH_BACKUP"',
    )
    assert build.index("umask 0022") < build.index(
        "require_environment",
    ) < build.index("assert_candidate_build_scope") < build.index(
        "assert_inherited_production_lock",
    ) < build.index("prepare_candidate_runtime")
    assert build.index("write_candidate_deploy_status") < build.index(
        "finalize_runtime_seal",
    ) < build.index("verify_final_runtime_seal")
    assert "--scope" in build_scope
    assert "--wait" not in build_scope
    assert "--property=\"MemoryHigh=$BLUEGREEN_CANDIDATE_MEMORY_HIGH\"" in build_scope
    assert "--property=\"MemoryMax=$BLUEGREEN_CANDIDATE_MEMORY_MAX\"" in build_scope
    assert "--property=\"TasksMax=512\"" in build_scope
    assert "build-candidate-runtime" in build_scope
    assert prepare.index("materialize_release_source") < prepare.index(
        "run_candidate_build_scope",
    ) < prepare.index("verify_final_runtime_seal") < prepare.index(
        "assert_no_database_migration_delta",
    ) < prepare.index(
        "install_slot_runtime",
    )
    assert "write_candidate_deploy_status" not in switch


def test_candidate_build_resets_restrictive_ssh_umask_before_writes(
    tmp_path: Path,
) -> None:
    probe = tmp_path / "build-output"
    result = _run_controller_harness(
        tmp_path,
        f"""
umask 077
require_environment() {{ :; }}
assert_candidate_build_scope() {{ :; }}
assert_inherited_production_lock() {{ :; }}
prepare_candidate_runtime() {{
  mkdir -p "{probe}"
  : > "{probe / 'artifact'}"
  RUNTIME_ALREADY_SEALED=true
}}
assert_no_database_migration_delta() {{ :; }}
verify_final_runtime_seal() {{ :; }}
CURRENT_ACTIVE_SLOT=8000
CANDIDATE_SLOT=8001
build_candidate_runtime_locked
""",
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert probe.stat().st_mode & 0o777 == 0o755
    assert (probe / "artifact").stat().st_mode & 0o777 == 0o644


def test_runtime_roots_are_0755_under_umask_077(tmp_path: Path) -> None:
    paths = (
        tmp_path / "state",
        tmp_path / "bluegreen",
        tmp_path / "bluegreen/releases",
        tmp_path / f"bluegreen/releases/{TARGET_SHA}",
        tmp_path / "bluegreen/slots",
        tmp_path / "bluegreen/slots/8000",
        tmp_path / "bluegreen/slots/8001",
        tmp_path / "bluegreen/shared",
    )
    result = _run_controller_harness(
        tmp_path,
        f"""
umask 077
mkdir -p {" ".join(str(path) for path in paths)}
chmod 0700 {" ".join(str(path) for path in paths)}
sudo() {{
  if [[ "${{1:-}}" == "-n" ]]; then shift; fi
  "$@"
}}
ensure_bluegreen_state_root
ensure_bluegreen_runtime_roots
""",
    )

    assert result.returncode == 0, result.stderr + result.stdout
    for path in paths:
        assert path.is_dir()
        assert path.stat().st_mode & 0o777 == 0o755


def test_candidate_build_scope_propagates_systemd_run_failure(
    tmp_path: Path,
) -> None:
    controller = (
        tmp_path
        / f"bluegreen/releases/{TARGET_SHA}/{'c' * 64}"
        / "03_Scripts/deploy/tencent_bluegreen_release.sh"
    )
    controller.parent.mkdir(parents=True)
    controller.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    result = _run_controller_harness(
        tmp_path,
        """
sudo() {
  if [[ "${1:-}" == "-n" ]]; then shift; fi
  if [[ "${1:-}" == "systemd-run" ]]; then
    printf 'scope-args:%s\\n' "$*"
    return 73
  fi
  return 1
}
set +e
run_candidate_build_scope
rc=$?
set -e
printf 'scope-rc=%s\\n' "$rc"
""",
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert "scope-rc=73" in result.stdout
    assert "--scope" in result.stdout
    assert "--collect" in result.stdout
    assert "--wait" not in result.stdout
    assert "MemoryHigh=3G" in result.stdout
    assert "MemoryMax=4G" in result.stdout
    assert "TasksMax=512" in result.stdout


@pytest.mark.parametrize(
    "failure_stage",
    ("pre_disk", "pre_memory", "post_disk", "post_memory"),
)
def test_prepare_resource_failure_blocks_candidate_mutation(
    tmp_path: Path,
    failure_stage: str,
) -> None:
    trace = tmp_path / "trace"
    result = _run_controller_harness(
        tmp_path,
        f"""
TRACE={trace}
MEMORY_CALLS=0
record() {{ printf '%s\\n' "$1" >> "$TRACE"; }}
require_environment() {{ record require; }}
assert_inherited_production_lock() {{ record lock; }}
ensure_bluegreen_state_root() {{ record state-root; }}
ensure_bluegreen_runtime_roots() {{ record runtime-roots; }}
assert_no_active_switch_unit() {{ record no-switch; }}
read_checkpoint_phase_status() {{
  CHECKPOINT_PHASE=prepared
  CHECKPOINT_STATUS=completed
}}
resolve_active_slot() {{ CURRENT_ACTIVE_SLOT=8000; CANDIDATE_SLOT=8001; }}
resolve_current_frontend_root() {{ record frontend; }}
prepare_shared_runtime() {{ record shared; }}
ensure_current_slot_restartable() {{ record restartable; }}
preserve_previous_release_metadata() {{ record metadata; }}
guard_release_storage() {{
  record pre-disk
  [[ "{failure_stage}" != "pre_disk" ]]
}}
assert_host_memory_budget() {{
  MEMORY_CALLS=$((MEMORY_CALLS + 1))
  record "memory-$MEMORY_CALLS"
  if [[ "{failure_stage}" == "pre_memory" && "$MEMORY_CALLS" -eq 1 ]]; then
    return 1
  fi
  if [[ "{failure_stage}" == "post_memory" && "$MEMORY_CALLS" -eq 2 ]]; then
    return 1
  fi
}}
materialize_release_source() {{ record materialize; }}
run_candidate_build_scope() {{ record build-scope; }}
verify_final_runtime_seal() {{ record final-seal; }}
assert_no_database_migration_delta() {{ record database-gate; }}
assert_runtime_storage_reserve() {{
  record post-disk
  [[ "{failure_stage}" != "post_disk" ]]
}}
install_slot_runtime() {{ record unexpected-install; }}
prepare_stable_nginx_boot_infrastructure() {{ record unexpected-nginx; }}
verify_candidate() {{ record unexpected-start; }}
prepare_and_switch
""",
    )
    events = trace.read_text(encoding="utf-8").splitlines()

    assert result.returncode != 0, result.stderr + result.stdout
    assert "unexpected-install" not in events
    assert "unexpected-nginx" not in events
    assert "unexpected-start" not in events
    if failure_stage.startswith("pre_"):
        assert "materialize" not in events
        assert "build-scope" not in events
    else:
        assert "materialize" in events
        assert "build-scope" in events
        assert "final-seal" in events


def test_inherited_lock_failure_precedes_all_storage_mutation(
    tmp_path: Path,
) -> None:
    trace = tmp_path / "trace"
    result = _run_controller_harness(
        tmp_path,
        f"""
TRACE={trace}
require_environment() {{ printf 'require\\n' >> "$TRACE"; }}
assert_inherited_production_lock() {{
  printf 'lock-rejected\\n' >> "$TRACE"
  return 1
}}
ensure_bluegreen_state_root() {{ printf 'unexpected-state\\n' >> "$TRACE"; }}
ensure_bluegreen_runtime_roots() {{ printf 'unexpected-runtime\\n' >> "$TRACE"; }}
guard_release_storage() {{ printf 'unexpected-gc\\n' >> "$TRACE"; }}
materialize_release_source() {{ printf 'unexpected-materialize\\n' >> "$TRACE"; }}
prepare_and_switch
""",
    )
    events = trace.read_text(encoding="utf-8").splitlines()

    assert result.returncode != 0, result.stderr + result.stdout
    assert events == ["require", "lock-rejected"]


def test_candidate_template_handoff_is_reboot_safe_before_commit() -> None:
    script = CONTROLLER.read_text(encoding="utf-8")
    handoff = _shell_function(script, "commit_backend_unit_template")
    activate = _shell_function(script, "complete_candidate_activation")
    reconcile = _shell_function(script, "reconcile_existing_switch")

    disable = handoff.index('systemctl disable "$candidate_unit"')
    remove = handoff.index('durable_remove_path "$explicit_candidate"', disable)
    reload = handoff.index("systemctl daemon-reload", remove)
    fragment = handoff.index("FragmentPath", reload)
    enable = handoff.index('systemctl enable "$candidate_unit"', fragment)
    gate = handoff.index("verify_candidate_reboot_gate", enable)
    assert disable < remove < reload < fragment < enable < gate

    disable_old = activate.index(
        'systemctl disable \\\n    "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}"',
    )
    old_disabled = activate.index("UnitFileState disabled", disable_old)
    stop_old = activate.index("systemctl stop", old_disabled)
    handoff_call = activate.index("commit_backend_unit_template", stop_old)
    committed = activate.index("checkpoint_write backend_healthy", handoff_call)
    assert disable_old < old_disabled < stop_old < handoff_call < committed
    assert "|| true" not in activate[disable_old:stop_old]
    assert "enable --now" in reconcile
    assert "verify_candidate_reboot_gate" in reconcile


def test_boot_reconciler_owns_pre_switch_static_startup_and_nginx_dependency() -> None:
    script = CONTROLLER.read_text(encoding="utf-8")
    prepare = _shell_function(script, "prepare_and_switch")
    stable_infrastructure = _shell_function(
        script,
        "prepare_stable_nginx_boot_infrastructure",
    )
    nginx_installer = _shell_function(script, "run_stable_nginx_installer")
    install = _shell_function(script, "install_boot_reconciler")
    verify = _shell_function(script, "verify_boot_reconciler_installation")
    arm = _shell_function(script, "arm_pre_switch_static_boot_safety")
    cleanup = _shell_function(script, "cleanup_pre_switch_candidate")

    slot_install = prepare.index("install_slot_runtime")
    infrastructure = prepare.index(
        "prepare_stable_nginx_boot_infrastructure",
        slot_install,
    )
    candidate_verify = prepare.index("verify_candidate", infrastructure)
    static_arm = prepare.index(
        "arm_pre_switch_static_boot_safety",
        candidate_verify,
    )
    supervisor = prepare.index("run_switch_supervisor", static_arm)
    assert (
        slot_install
        < infrastructure
        < candidate_verify
        < static_arm
        < supervisor
    )

    interrupted_restore = stable_infrastructure.index("restore_nginx_preimage")
    interrupted_preimage_cleanup = stable_infrastructure.index(
        "remove_nginx_preimage",
        interrupted_restore,
    )
    nginx_install = stable_infrastructure.index(
        "run_stable_nginx_installer",
        interrupted_preimage_cleanup,
    )
    old_route_verify = stable_infrastructure.index(
        "verify_stable_current_nginx_route",
        nginx_install,
    )
    migration_commit = stable_infrastructure.index(
        "remove_nginx_preimage",
        old_route_verify,
    )
    boot_install = stable_infrastructure.index(
        "install_boot_reconciler",
        migration_commit,
    )
    boot_verify = stable_infrastructure.index(
        "verify_boot_reconciler_installation",
        boot_install,
    )
    post_boot_route_verify = stable_infrastructure.index(
        "verify_stable_current_nginx_route",
        boot_verify,
    )
    assert (
        interrupted_restore
        < interrupted_preimage_cleanup
        < nginx_install
        < old_route_verify
        < migration_commit
        < boot_install
        < boot_verify
        < post_boot_route_verify
    )
    assert 'bash "$NGINX_INSTALLER"' in nginx_installer
    assert "durable_install_file" in install
    assert 'systemctl enable "$BOOT_RECONCILE_UNIT"' in install
    assert "verify_boot_reconciler_installation" in install
    assert "Requires" in verify
    assert "After" in verify
    assert "nginx.service" in verify

    disable_old = arm.index(
        'systemctl disable \\\n    "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}"',
    )
    assert "systemctl stop" not in arm
    assert arm.index("CANDIDATE_SLOT") < disable_old
    assert arm.index("UnitFileState disabled", disable_old) < arm.index(
        "ActiveState active",
        disable_old,
    )
    assert "restore_old_static_boot_owner" in cleanup


def test_first_migration_candidate_failure_keeps_old_route_and_boot_contract(
    tmp_path: Path,
) -> None:
    route = tmp_path / "nginx/active-release.conf"
    preimage = tmp_path / "state/nginx-preimage"
    helper = tmp_path / "boot/reconcile.py"
    unit = tmp_path / "boot/reconcile.service"
    dropin = tmp_path / "boot/nginx.conf"
    trace = tmp_path / "trace"
    result = _run_controller_harness(
        tmp_path,
        f"""
CURRENT_ACTIVE_SLOT=8000
CANDIDATE_SLOT=8001
CURRENT_FRONTEND_ROOT=/opt/old/frontend/dist
NGINX_ACTIVE_RELEASE_CONF={route}
NGINX_PREIMAGE_DIR={preimage}
BOOT_HELPER_MARKER={helper}
BOOT_UNIT_MARKER={unit}
BOOT_DROPIN_MARKER={dropin}
TRACE={trace}
PRE_SUPERVISOR_CANDIDATE_ARMED=true
run_stable_nginx_installer() {{
  mkdir -p "$(dirname "$NGINX_ACTIVE_RELEASE_CONF")" "$NGINX_PREIMAGE_DIR"
  printf 'stable-old-route\\n' > "$NGINX_ACTIVE_RELEASE_CONF"
  printf 'nginx-installed\\n' >> "$TRACE"
}}
verify_stable_current_nginx_route() {{
  test -f "$NGINX_ACTIVE_RELEASE_CONF"
  grep -Fxq 'stable-old-route' "$NGINX_ACTIVE_RELEASE_CONF"
  printf 'old-route-verified\\n' >> "$TRACE"
}}
remove_nginx_preimage() {{
  rm -rf "$NGINX_PREIMAGE_DIR"
  printf 'preimage-removed\\n' >> "$TRACE"
}}
install_boot_reconciler() {{
  mkdir -p "$(dirname "$BOOT_HELPER_MARKER")"
  printf 'helper\\n' > "$BOOT_HELPER_MARKER"
  printf 'unit\\n' > "$BOOT_UNIT_MARKER"
  printf 'dropin\\n' > "$BOOT_DROPIN_MARKER"
  printf 'boot-installed\\n' >> "$TRACE"
}}
verify_boot_reconciler_installation() {{
  test -f "$BOOT_HELPER_MARKER"
  test -f "$BOOT_UNIT_MARKER"
  test -f "$BOOT_DROPIN_MARKER"
  printf 'boot-verified\\n' >> "$TRACE"
}}
resolve_previous_release_identity() {{
  PREVIOUS_RELEASE_ROOT=/opt/old
  PREVIOUS_RELEASE_SHA={OLD_SHA}
}}
verify_slot_release_exact() {{ return 0; }}
verify_public_release_exact() {{
  test -f "$NGINX_ACTIVE_RELEASE_CONF"
  grep -Fxq 'stable-old-route' "$NGINX_ACTIVE_RELEASE_CONF"
}}
stop_candidate_preview() {{ return 0; }}
restore_old_static_boot_owner() {{ printf 'old-owner-restored\\n' >> "$TRACE"; }}
candidate_cleanup_is_complete() {{ return 0; }}
mark_maintenance_required() {{ printf 'unexpected-maintenance\\n' >> "$TRACE"; }}
trap prepare_exit_handler EXIT
prepare_stable_nginx_boot_infrastructure
printf 'candidate-validation-failed\\n' >> "$TRACE"
false
""",
    )

    assert result.returncode == 1, result.stderr + result.stdout
    assert route.read_text(encoding="utf-8") == "stable-old-route\n"
    assert helper.is_file()
    assert unit.is_file()
    assert dropin.is_file()
    assert not preimage.exists()
    events = trace.read_text(encoding="utf-8").splitlines()
    assert events.index("nginx-installed") < events.index("preimage-removed")
    assert events.index("preimage-removed") < events.index("boot-installed")
    assert events.index("boot-installed") < events.index("boot-verified")
    assert events.index("boot-verified") < events.index(
        "candidate-validation-failed",
    )
    assert "old-owner-restored" in events
    assert "unexpected-maintenance" not in events


def test_interrupted_partial_nginx_migration_restores_then_replays_installer(
    tmp_path: Path,
) -> None:
    route = tmp_path / "nginx/active-release.conf"
    site = tmp_path / "nginx/site.conf"
    preimage = tmp_path / "state/nginx-preimage"
    boot = tmp_path / "boot-contract"
    trace = tmp_path / "trace"
    route.parent.mkdir(parents=True)
    preimage.mkdir(parents=True)
    # The installer writes active-release.conf before the site includes it.
    # This is the dangerous interruption window: public traffic is still on
    # the legacy site even though the active include already looks correct.
    route.write_text("stable-old-route\n", encoding="utf-8")
    site.write_text("legacy-site-without-include\n", encoding="utf-8")
    result = _run_controller_harness(
        tmp_path,
        f"""
NGINX_ACTIVE_RELEASE_CONF={route}
NGINX_PREIMAGE_DIR={preimage}
SITE_MARKER={site}
BOOT_MARKER={boot}
TRACE={trace}
verify_stable_current_nginx_route() {{
  grep -Fxq 'stable-old-route' "$NGINX_ACTIVE_RELEASE_CONF"
  grep -Fxq 'stable-site-with-include' "$SITE_MARKER"
  printf 'verified\\n' >> "$TRACE"
}}
run_stable_nginx_installer() {{
  grep -Fxq restored-preimage "$SITE_MARKER"
  test ! -e "$NGINX_PREIMAGE_DIR"
  mkdir -p "$NGINX_PREIMAGE_DIR"
  printf 'stable-old-route\\n' > "$NGINX_ACTIVE_RELEASE_CONF"
  printf 'stable-site-with-include\\n' > "$SITE_MARKER"
  printf 'installer-replayed\\n' >> "$TRACE"
}}
restore_nginx_preimage() {{
  printf 'restored-preimage\\n' > "$SITE_MARKER"
  rm -f "$NGINX_ACTIVE_RELEASE_CONF"
  printf 'preimage-restored\\n' >> "$TRACE"
}}
remove_nginx_preimage() {{
  rm -rf "$NGINX_PREIMAGE_DIR"
  printf 'preimage-removed\\n' >> "$TRACE"
}}
install_boot_reconciler() {{
  grep -Fxq 'stable-old-route' "$NGINX_ACTIVE_RELEASE_CONF"
  grep -Fxq 'stable-site-with-include' "$SITE_MARKER"
  test ! -e "$NGINX_PREIMAGE_DIR"
  printf 'installed\\n' > "$BOOT_MARKER"
  printf 'boot-installed\\n' >> "$TRACE"
}}
verify_boot_reconciler_installation() {{ grep -Fxq installed "$BOOT_MARKER"; }}
prepare_stable_nginx_boot_infrastructure
""",
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert route.read_text(encoding="utf-8") == "stable-old-route\n"
    assert site.read_text(encoding="utf-8") == "stable-site-with-include\n"
    assert not preimage.exists()
    assert boot.read_text(encoding="utf-8") == "installed\n"
    events = trace.read_text(encoding="utf-8").splitlines()
    assert events == [
        "preimage-restored",
        "preimage-removed",
        "installer-replayed",
        "verified",
        "preimage-removed",
        "boot-installed",
        "verified",
    ]
