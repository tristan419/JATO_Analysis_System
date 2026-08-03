from __future__ import annotations

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
        ["bash", "-c", f"{_controller_prelude()}\n{body}\n"],
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
    persistent = _shell_function(script, "run_switch_supervisor")

    assert candidate < supervisor
    assert '"$python_bin" -B "$helper" hold' in persistent
    assert '-- "$bash_bin" "$controller" switch-locked' in persistent
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
    armed = install.index("PRE_SUPERVISOR_CANDIDATE_ARMED=true")
    disable = install.index(
        'systemctl disable "${SERVICE_PREFIX}${CANDIDATE_SLOT}"',
    )
    set_property = install.index(
        'systemctl set-property "${SERVICE_PREFIX}${CANDIDATE_SLOT}"',
    )
    assert armed < disable < set_property

    result = _run_controller_harness(
        tmp_path,
        f"""
CANDIDATE_SLOT=8001
SYSTEMD_TEMPLATE={SLOT_UNIT}
SLOT_ENV_TEMPLATE={REPO_ROOT / "03_Scripts/deploy/systemd/jato-fullstack-backend-slot.env.example"}
cleanup_pre_switch_candidate() {{ printf 'mid-install-cleanup\\n'; }}
durable_install_file() {{ return 0; }}
atomic_symlink() {{ return 0; }}
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
mark_maintenance_required() {{ printf 'marker-retained\\n'; }}
candidate_cleanup_is_complete() {{ [[ "$CANDIDATE_CLEAN" == true ]]; }}
unit_property_equals() {{ return 0; }}
remove_candidate_explicit_unit() {{ printf 'candidate-unit-removed\\n'; }}
resume_schedulers() {{ printf 'schedulers-restored\\n'; }}
sudo() {{
  [[ "${{1:-}}" == -n ]] && shift
  printf 'sudo:%s\\n' "$*"
  if [[ "$*" == "systemctl disable --now jato-fullstack-backend@8001" ]]; then
    CANDIDATE_CLEAN=true
    printf 'candidate-stopped\\n' >> "$TRACE_FILE"
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
    assert "candidate-unit-removed" in result.stdout
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
remove_candidate_explicit_unit() {{ printf 'candidate-unit-removed\\n'; }}
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
remove_candidate_explicit_unit() {{ return 0; }}
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
    assert "ProtectSystem=strict" in install
    assert "DynamicUser=yes" in install
    assert "PrivateTmp=true" in install
    assert "PrivateDevices=true" in install
    assert "NoNewPrivileges=true" in install
    assert "CapabilityBoundingSet=" in install
    assert "AmbientCapabilities=" in install
    assert "RestrictNamespaces=true" in install
    assert "APP_REDIS_ENABLED=false" in install
    assert "default_transaction_read_only=on" in install
    assert "/var/cache/jato-candidate-$CANDIDATE_SLOT" in install
    assert "/var/cache/jato/candidate-" not in install
    assert 'printf \'ReadOnlyPaths=%s %s\\n\' "$SHARED_ROOT" "$BLUEGREEN_STATE_ROOT"' in install
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
        ["bash", "-c", harness],
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
