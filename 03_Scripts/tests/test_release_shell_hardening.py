from __future__ import annotations

import io
import json
import os
from pathlib import Path
import subprocess
import tarfile

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
REMOTE_SCRIPT = REPO_ROOT / "03_Scripts/deploy/fullstack_remote_release.sh"
BLUEGREEN_SCRIPT = REPO_ROOT / "03_Scripts/deploy/tencent_bluegreen_release.sh"
SERVER_SCRIPT = REPO_ROOT / "03_Scripts/ops/deploy_fullstack_server.sh"
BACKUP_SCRIPT = REPO_ROOT / "03_Scripts/ops/backup_production_data.sh"
RECOVERY_CONTROLLER = (
    REPO_ROOT
    / "03_Scripts/deploy/tencent_pre_switch_checkpoint_recovery.sh"
)
RECOVERY_HELPER = (
    REPO_ROOT / "03_Scripts/deploy/pre_switch_checkpoint_recovery.py"
)


def _shell_function(script_path: Path, name: str) -> str:
    script = script_path.read_text(encoding="utf-8")
    start = script.index(f"{name}() {{")
    end = script.index("\n}\n", start) + len("\n}\n")
    return script[start:end]


def test_remote_release_validates_content_address_before_bluegreen_handoff() -> None:
    script = REMOTE_SCRIPT.read_text(encoding="utf-8")
    for token in (
        "DEPLOY_ARCHIVE_PATH",
        "DEPLOY_ARCHIVE_BYTES",
        "DEPLOY_ARCHIVE_SHA256",
        '.cache/jato-releases/archives/${DEPLOY_COMMIT_SHA}/${DEPLOY_ARCHIVE_SHA256}.tar.gz',
        "flock -w 300 9",
        "Unsupported release archive member",
        "Release archive size mismatch",
        "Release archive SHA-256 mismatch",
    ):
        assert token in script
    assert 'rm -f "$RELEASE_ARCHIVE"' not in script
    assert "release_evidence_matches" in script
    assert 'EVIDENCE_HELPER="$RELEASE_WORKTREE/03_Scripts/deploy/release_evidence.py"' in script
    assert 'sudo -n "${verifier[@]}"' in script
    assert 'python3 -B "$EVIDENCE_HELPER" verify' in script
    lock_acquired = script.index("flock -w 300 9")
    helper_extracted = script.index(
        'CHECKPOINT_HELPER="$RELEASE_WORKTREE/03_Scripts/deploy/release_checkpoint.py"'
    )
    cross_release_gate = script.index("assert-cross-release-safe")
    prepared_write = script.index("--phase prepared", cross_release_gate)
    handoff = script.index(
        'bash "$RELEASE_WORKTREE/03_Scripts/deploy/tencent_bluegreen_release.sh"',
    )
    handoff_exit = script.index('exit "$BLUEGREEN_RC"', handoff)
    assert (
        lock_acquired
        < helper_extracted
        < cross_release_gate
        < prepared_write
        < handoff
        < handoff_exit
    )
    assert script.rstrip().endswith('exit "$BLUEGREEN_RC"')
    for legacy_token in (
        'rm -rf "$REPO_DIR/$release_path"',
        "install_backend_env_atomically",
        "bash 03_Scripts/deploy_fullstack_server.sh",
        "PRODUCTION_MUTATION_STARTED",
        'mv -f "$STATUS_TEMP" "$DIST/_deploy_status.txt"',
        "--phase backend_healthy",
    ):
        assert legacy_token not in script


def test_remote_release_cleanup_is_best_effort_for_every_handoff_result() -> None:
    script = REMOTE_SCRIPT.read_text(encoding="utf-8")
    cleanup = _shell_function(REMOTE_SCRIPT, "remove_transient_release_paths")
    exit_cleanup = _shell_function(REMOTE_SCRIPT, "cleanup_release_staging")

    assert 'if ! rm -rf -- "$transient_path"; then' in cleanup
    assert 'echo "[WARN] Failed to remove transient deployment path:' in cleanup
    assert cleanup.rstrip().endswith("return 0\n}")
    assert (
        'for transient_path in "$RELEASE_WORKTREE" "$PREBUILT_FRONTEND_DIR"; do'
        in cleanup
    )
    assert "remove_transient_release_paths" in exit_cleanup
    assert "PRODUCTION_MUTATION_STARTED" not in script
    assert "REMOTE_DEPLOY_SUCCEEDED" not in script
    assert "trap cleanup_release_staging EXIT" in script
    assert "BLUEGREEN_RC=$?" in script
    assert script.rstrip().endswith('exit "$BLUEGREEN_RC"')


def test_outer_disconnect_cleanup_preserves_durable_bluegreen_supervisor_files(
    tmp_path: Path,
) -> None:
    transient_worktree = tmp_path / "transient-worktree"
    transient_frontend = tmp_path / "transient-frontend"
    durable_release = tmp_path / "releases/target/archive"
    for path in (transient_worktree, transient_frontend, durable_release):
        path.mkdir(parents=True)
    durable_controller = (
        durable_release / "03_Scripts/deploy/tencent_bluegreen_release.sh"
    )
    durable_helper = durable_release / "03_Scripts/deploy/jato_quiescence_gate.py"
    durable_controller.parent.mkdir(parents=True)
    durable_controller.write_text("controller", encoding="utf-8")
    durable_helper.write_text("helper", encoding="utf-8")

    result = subprocess.run(
        [
            "bash",
            "-c",
            "set -Eeuo pipefail\n"
            + _shell_function(REMOTE_SCRIPT, "remove_transient_release_paths")
            + _shell_function(REMOTE_SCRIPT, "cleanup_release_staging")
            + "\ncleanup_release_staging\n",
        ],
        env={
            **os.environ,
            "RELEASE_WORKTREE": str(transient_worktree),
            "PREBUILT_FRONTEND_DIR": str(transient_frontend),
        },
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert not transient_worktree.exists()
    assert not transient_frontend.exists()
    assert durable_controller.read_text(encoding="utf-8") == "controller"
    assert durable_helper.read_text(encoding="utf-8") == "helper"


def test_outer_noop_requires_exact_active_release_and_verified_source_seal(
    tmp_path: Path,
) -> None:
    script = REMOTE_SCRIPT.read_text(encoding="utf-8")
    local_match = _shell_function(REMOTE_SCRIPT, "local_release_matches")
    source_match = _shell_function(
        REMOTE_SCRIPT,
        "verified_active_source_seal_matches",
    )
    runtime_match = _shell_function(
        REMOTE_SCRIPT,
        "verified_active_runtime_seal_matches",
    )
    assert '[[ "$active_root" == "$expected_root" ]]' in local_match
    assert 'verified_active_source_seal_matches "$active_root"' in local_match
    assert 'verified_active_runtime_seal_matches "$active_root"' in local_match
    assert 'sudo -n cmp -s "$expected_seal" "$stored_seal"' in source_match
    assert '"$helper" verify' in source_match
    assert "--profile runtime" in runtime_match
    assert '--commit "$DEPLOY_COMMIT_SHA"' in runtime_match

    expected_root = tmp_path / "releases" / ("a" * 40) / ("b" * 64)
    result = subprocess.run(
        [
            "bash",
            "-c",
            "set -Eeuo pipefail\n"
            + local_match
            + source_match
            + runtime_match
            + "\nverified_active_source_seal_matches() { return 1; }\n"
            + "verified_active_runtime_seal_matches() { return 0; }\n"
            + "sudo() {\n"
            + "  [[ \"${1:-}\" == -n ]] && shift\n"
            + "  case \"$*\" in\n"
            + "    \"test -f $ACTIVE_SLOT_FILE\") return 0 ;;\n"
            + "    \"cat $ACTIVE_SLOT_FILE\") printf '8000\\n' ;;\n"
            + "    \"test -L /opt/jato/active\") return 0 ;;\n"
            + f"    \"realpath /opt/jato/active\") printf '%s\\n' '{expected_root}' ;;\n"
            + "    *) return 0 ;;\n"
            + "  esac\n"
            + "}\n"
            + "set +e\nlocal_release_matches\nrc=$?\nset -e\n"
            + "printf 'rc=%s\\n' \"$rc\"\n",
        ],
        env={
            **os.environ,
            "ACTIVE_SLOT_FILE": str(tmp_path / "active-slot"),
            "BLUEGREEN_RELEASES_ROOT": str(tmp_path / "releases"),
            "DEPLOY_COMMIT_SHA": "a" * 40,
            "DEPLOY_ARCHIVE_SHA256": "b" * 64,
            "REPO_DIR": str(tmp_path / "legacy"),
            "RELEASE_WORKTREE": str(tmp_path / "worktree"),
        },
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert "rc=1" in result.stdout


def _archive_member_validator() -> str:
    script = REMOTE_SCRIPT.read_text(encoding="utf-8")
    marker = "<<'PY_VALIDATE_RELEASE_ARCHIVE'\n"
    start = script.index(marker) + len(marker)
    end = script.index("\nPY_VALIDATE_RELEASE_ARCHIVE\n", start)
    return script[start:end]


def _write_archive(path: Path, members: list[tarfile.TarInfo]) -> None:
    with tarfile.open(path, mode="w:gz") as archive:
        for member in members:
            payload = None
            if member.isfile():
                content = b"verified payload"
                member.size = len(content)
                payload = io.BytesIO(content)
            archive.addfile(member, payload)


def _run_archive_member_validator(path: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["python3", "-", str(path)],
        input=_archive_member_validator(),
        text=True,
        capture_output=True,
        check=False,
    )


def _root_tar_info() -> tarfile.TarInfo:
    root = tarfile.TarInfo(".")
    root.type = tarfile.DIRTYPE
    root.mode = 0o755
    return root


def test_archive_member_validator_accepts_single_gnu_tar_root_directory(
    tmp_path: Path,
) -> None:
    archive_path = tmp_path / "valid.tar.gz"
    payload = tarfile.TarInfo("./payload.txt")
    payload.mode = 0o644
    _write_archive(archive_path, [_root_tar_info(), payload])

    result = _run_archive_member_validator(archive_path)

    assert result.returncode == 0, result.stderr
    assert "passed fail-closed validation" in result.stdout


def test_archive_member_validator_accepts_private_business_asset_mode(
    tmp_path: Path,
) -> None:
    archive_path = tmp_path / "private-valid.tar.gz"
    payload = tarfile.TarInfo("./01_RAW_DATA/private.xlsx")
    payload.mode = 0o600
    _write_archive(archive_path, [_root_tar_info(), payload])

    result = _run_archive_member_validator(archive_path)

    assert result.returncode == 0, result.stderr


def test_archive_member_validator_rejects_traversal_link_and_duplicate(
    tmp_path: Path,
) -> None:
    invalid_members: dict[str, list[tarfile.TarInfo]] = {
        "root_file": [tarfile.TarInfo(".")],
        "traversal": [_root_tar_info(), tarfile.TarInfo("../escaped.txt")],
        "link": [_root_tar_info(), tarfile.TarInfo("payload.txt")],
        "duplicate": [
            _root_tar_info(),
            tarfile.TarInfo("./payload.txt"),
            tarfile.TarInfo("payload.txt"),
        ],
    }
    invalid_members["link"][1].type = tarfile.SYMTYPE
    invalid_members["link"][1].linkname = "/etc/passwd"
    group_writable = tarfile.TarInfo("./group-writable.txt")
    group_writable.mode = 0o664
    invalid_members["group_writable"] = [_root_tar_info(), group_writable]
    special_mode = tarfile.TarInfo("./setuid")
    special_mode.mode = 0o4755
    invalid_members["special_mode"] = [_root_tar_info(), special_mode]
    private_directory = tarfile.TarInfo("./private")
    private_directory.type = tarfile.DIRTYPE
    private_directory.mode = 0o700
    invalid_members["private_directory"] = [_root_tar_info(), private_directory]
    public_file_with_private_mode = tarfile.TarInfo("./public.py")
    public_file_with_private_mode.mode = 0o600
    invalid_members["public_file_with_private_mode"] = [
        _root_tar_info(),
        public_file_with_private_mode,
    ]
    private_file_with_public_mode = tarfile.TarInfo(
        "./01_RAW_DATA/private.xlsx"
    )
    private_file_with_public_mode.mode = 0o644
    invalid_members["private_file_with_public_mode"] = [
        _root_tar_info(),
        private_file_with_public_mode,
    ]

    for name, members in invalid_members.items():
        archive_path = tmp_path / f"{name}.tar.gz"
        _write_archive(archive_path, members)
        result = _run_archive_member_validator(archive_path)
        assert result.returncode != 0, name


def test_bluegreen_controller_alone_publishes_status_and_target_health() -> None:
    outer = REMOTE_SCRIPT.read_text(encoding="utf-8")
    controller = BLUEGREEN_SCRIPT.read_text(encoding="utf-8")
    switch = _shell_function(BLUEGREEN_SCRIPT, "switch_locked")
    activate = _shell_function(BLUEGREEN_SCRIPT, "complete_candidate_activation")

    assert "_deploy_status.txt" in outer
    assert "write_candidate_deploy_status" not in outer
    assert "checkpoint_write backend_healthy" not in outer
    assert "merge_previous_frontend_assets" not in controller
    assert 'cp -p "$source" "$target"' not in controller
    build = _shell_function(BLUEGREEN_SCRIPT, "build_candidate_runtime_locked")
    source_verify = build.index("\n    verify_materialized_release_source\n")
    database_gate = build.index("\n    assert_no_database_migration_delta\n")
    status_write = build.index("\n    write_candidate_deploy_status\n")
    runtime_seal = build.index("\n    finalize_runtime_seal\n")
    switch_checkpoint = switch.index(
        "\n  checkpoint_write switched completed automatic",
    )
    activation_call = switch.index(
        "\n  complete_candidate_activation",
        switch_checkpoint,
    )
    healthy_checkpoint = activate.index(
        "\n  checkpoint_write backend_healthy completed automatic",
    )
    assert source_verify < database_gate < status_write < runtime_seal
    assert switch_checkpoint < activation_call
    assert healthy_checkpoint > activate.index("verify_active_cgroup")
    assert "restore_previous_route" in controller
    assert "checkpoint_write rollback_completed completed automatic" in controller


def test_server_cleans_only_toolkit_egg_info_around_editable_install() -> None:
    script = SERVER_SCRIPT.read_text(encoding="utf-8")
    install = _shell_function(
        SERVER_SCRIPT,
        "install_scraping_toolkit_editable",
    )

    assert "trap cleanup_toolkit_on_exit EXIT" in install
    assert install.count("cleanup_scraping_toolkit_egg_info") == 2
    exit_cleanup = _shell_function(SERVER_SCRIPT, "cleanup_toolkit_on_exit")
    assert "cleanup_scraping_toolkit_egg_info" in exit_cleanup
    editable_install = install.index('python -m pip install -e "$TOOLKIT_DIR"')
    assert install.index("cleanup_scraping_toolkit_egg_info") < editable_install
    assert install.index(
        "cleanup_scraping_toolkit_egg_info",
        editable_install,
    ) > editable_install
    assert "pip wheel" not in install


def test_archive_permissions_survive_both_remote_extractions() -> None:
    outer = REMOTE_SCRIPT.read_text(encoding="utf-8")
    controller = BLUEGREEN_SCRIPT.read_text(encoding="utf-8")

    assert "tar --same-permissions --no-overwrite-dir" in outer
    assert '-xzf "$RELEASE_ARCHIVE" -C "$RELEASE_WORKTREE"' in outer
    assert (
        ") | sudo -n tar --same-permissions --no-overwrite-dir"
        in controller
    )


def test_gnu_tar_normalizes_and_restores_release_modes_twice(
    tmp_path: Path,
) -> None:
    version = subprocess.run(
        ["tar", "--version"],
        text=True,
        capture_output=True,
        check=False,
    )
    if version.returncode != 0 or "GNU tar" not in version.stdout:
        pytest.skip("production GNU tar semantics are verified on Ubuntu CI")

    source = tmp_path / "source"
    nested = source / "06_AppPlatform/frontend"
    backend = source / "06_AppPlatform/backend"
    private_dir = source / "01_RAW_DATA"
    nested.mkdir(parents=True)
    backend.mkdir()
    private_dir.mkdir()
    normal = nested / "normal.txt"
    executable = nested / "executable.sh"
    misplaced_dataset = backend / "misplaced.parquet"
    private_workbook = private_dir / "private.xlsx"
    normal.write_text("normal\n", encoding="utf-8")
    executable.write_text("#!/bin/sh\n", encoding="utf-8")
    misplaced_dataset.write_text("must-not-ship\n", encoding="utf-8")
    private_workbook.write_text("private\n", encoding="utf-8")
    source.chmod(0o700)
    (source / "06_AppPlatform").chmod(0o700)
    nested.chmod(0o700)
    private_dir.chmod(0o700)
    normal.chmod(0o600)
    executable.chmod(0o700)
    private_workbook.chmod(0o600)

    archive = tmp_path / "release.tar"
    common = [
        "tar",
        "--sort=name",
        "--format=gnu",
        "--owner=0",
        "--group=0",
        "--numeric-owner",
        "--mtime=@0",
        "--no-acls",
        "--no-xattrs",
        "--no-selinux",
    ]
    create = subprocess.run(
        [
            *common,
            "--mode=u=rwX,go=rX",
            "-cf",
            str(archive),
            "--exclude=01_RAW_DATA",
            "--exclude=06_AppPlatform/backend/*.parquet",
            "-C",
            str(source),
            ".",
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    assert create.returncode == 0, create.stderr
    append_private = subprocess.run(
        [
            *common,
            "--mode=u=rwX,go=X",
            "-rf",
            str(archive),
            "-C",
            str(source),
            "01_RAW_DATA/private.xlsx",
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    assert append_private.returncode == 0, append_private.stderr

    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir(mode=0o700)
    second.mkdir(mode=0o711)
    extract = subprocess.run(
        [
            "bash",
            "-c",
            (
                "set -Eeuo pipefail; umask 077; "
                'tar --same-permissions --no-overwrite-dir '
                '-xf "$1" -C "$2"; '
                '(cd "$2" && tar cf - .) '
                '| tar --same-permissions --no-overwrite-dir '
                '-xf - -C "$3"'
            ),
            "_",
            str(archive),
            str(first),
            str(second),
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    assert extract.returncode == 0, extract.stderr

    assert first.stat().st_mode & 0o777 == 0o700
    assert second.stat().st_mode & 0o777 == 0o711
    for root in (first, second):
        assert (root / "06_AppPlatform").stat().st_mode & 0o777 == 0o755
        assert (
            root / "06_AppPlatform/frontend"
        ).stat().st_mode & 0o777 == 0o755
        assert (
            root / "06_AppPlatform/frontend/normal.txt"
        ).stat().st_mode & 0o777 == 0o644
        assert (
            root / "06_AppPlatform/frontend/executable.sh"
        ).stat().st_mode & 0o777 == 0o755
        assert not (
            root / "06_AppPlatform/backend/misplaced.parquet"
        ).exists()
        assert (root / "01_RAW_DATA").stat().st_mode & 0o777 == 0o700
        assert (
            root / "01_RAW_DATA/private.xlsx"
        ).stat().st_mode & 0o777 == 0o600


def test_persistent_release_retry_safely_cleans_egg_info_before_source_seal() -> None:
    outer = REMOTE_SCRIPT.read_text(encoding="utf-8")
    controller = BLUEGREEN_SCRIPT.read_text(encoding="utf-8")
    materialize = _shell_function(BLUEGREEN_SCRIPT, "materialize_release_source")

    assert "03_Scripts/deploy/cleanup_toolkit_egg_info.py" in outer
    cleanup = materialize.index(
        'python3 -B "$TOOLKIT_EGG_INFO_HELPER"',
    )
    source_seal = materialize.index(
        'sudo -n test -L "$RELEASE_SOURCE_SEAL_FILE"',
        cleanup,
    )
    verify = materialize.index(
        'python3 -B "$SOURCE_SEAL_HELPER" verify',
        source_seal,
    )
    assert cleanup < source_seal < verify


def test_server_checkpoint_boundaries_are_fail_closed_and_resume_safe() -> None:
    script = SERVER_SCRIPT.read_text(encoding="utf-8")
    assert "checkpoint_completed_or_past" in script
    assert '[[ "$current_rank" -gt "$wanted_rank" ]]' in script
    assert "if checkpoint_completed_or_past backup_verified; then" in script
    assert "migration_started in_progress manual_db_recovery" in script
    assert "REQUIRE_DATABASE_BACKUP" in script
    assert 'fail_deploy "Cannot resolve database migration policy from backend env"' in script
    assert 'tr "[:upper:]" "[:lower:]"' in script
    assert '1|true|yes|on)' in script
    assert "if checkpoint_enabled \\\n" in script
    assert "verify_release_evidence" in script
    assert 'sudo -n python3 -B "$RELEASE_EVIDENCE_HELPER" verify' in script
    assert "CHECKPOINT_ALREADY_COMPLETE" in script
    assert "direct server deploy is a no-op" in script
    assert "backend_healthy completed" not in script
    assert "DATABASE_READ_ONLY_GATE_FAILED" in script
    assert (
        "Preserving the resumable backup checkpoint because the database gate was read-only"
        in script
    )
    health = script.index('echo "[INFO] Verify backend health"')
    schedulers = script.index(
        'echo "[INFO] Reconcile scraper schedulers after backend health"'
    )
    assert health < schedulers


def _run_database_policy(
    tmp_path: Path,
    *,
    mode: str,
    enabled: bool,
    prepare_only: bool = True,
    deploy_branch: str = "main",
    production_workflow: bool = True,
) -> subprocess.CompletedProcess[str]:
    backend_env = tmp_path / "backend.env"
    backend_env.write_text(
        "\n".join(
            (
                f"APP_DATABASE_ENABLED={'true' if enabled else 'false'}",
                "APP_DATABASE_URL=postgresql://example.invalid/jato",
                "",
            )
        ),
        encoding="utf-8",
    )
    policy = _shell_function(SERVER_SCRIPT, "resolve_database_migration_policy")
    script = "\n".join(
        (
            "set -Eeuo pipefail",
            'checkpoint_enabled() { return 0; }',
            'fail_deploy() { printf "%s\\n" "$1" >&2; exit 1; }',
            (
                "run_privileged_bash() { "
                'local command="$1"; shift; bash -c "$command" _ "$@"; }'
            ),
            policy,
            'BACKEND_ENV_FILE="$TEST_BACKEND_ENV_FILE"',
            'RUN_DATABASE_MIGRATIONS="$TEST_DATABASE_MODE"',
            'BLUEGREEN_PREPARE_ONLY="$TEST_PREPARE_ONLY"',
            'DEPLOY_BRANCH="$TEST_DEPLOY_BRANCH"',
            'PRODUCTION_RELEASE_WORKFLOW="$TEST_PRODUCTION_WORKFLOW"',
            "resolve_database_migration_policy",
            (
                "printf '%s|%s|%s|%s|%s\\n' "
                '"$DATABASE_ENABLED" "$DATABASE_BACKUP_REQUIRED" '
                '"$DATABASE_MIGRATION_REQUIRED" '
                '"$DATABASE_MIGRATION_VERIFY_ONLY" "$RUN_DATABASE_MIGRATIONS"'
            ),
        )
    )
    return subprocess.run(
        ["bash", "-c", script],
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "TEST_BACKEND_ENV_FILE": str(backend_env),
            "TEST_DATABASE_MODE": mode,
            "TEST_PREPARE_ONLY": "true" if prepare_only else "false",
            "TEST_DEPLOY_BRANCH": deploy_branch,
            "TEST_PRODUCTION_WORKFLOW": (
                "true" if production_workflow else "false"
            ),
        },
    )


@pytest.mark.parametrize(
    ("mode", "enabled", "expected"),
    (
        ("auto", True, "true|true|true|false|run"),
        ("auto", False, "false|false|false|false|skip"),
        ("verify_only", True, "true|true|false|true|verify_only"),
        ("verify_only", False, "false|false|false|false|skip"),
    ),
)
def test_database_policy_separates_enabled_state_from_action(
    tmp_path: Path,
    mode: str,
    enabled: bool,
    expected: str,
) -> None:
    result = _run_database_policy(
        tmp_path,
        mode=mode,
        enabled=enabled,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == expected


def test_database_policy_rejects_skip_for_enabled_database(tmp_path: Path) -> None:
    result = _run_database_policy(tmp_path, mode="false", enabled=True)

    assert result.returncode != 0
    assert "Cannot skip migration evidence while the database is enabled" in result.stderr


def test_database_policy_rejects_unknown_mode(tmp_path: Path) -> None:
    result = _run_database_policy(tmp_path, mode="surprise", enabled=False)

    assert result.returncode != 0
    assert "Unsupported database migration policy: surprise" in result.stderr


@pytest.mark.parametrize(
    "overrides",
    (
        {"prepare_only": False},
        {"deploy_branch": "codex/feature"},
        {"production_workflow": False},
    ),
)
def test_verify_only_policy_is_restricted_to_bluegreen_main_production_prepare(
    tmp_path: Path,
    overrides: dict[str, object],
) -> None:
    result = _run_database_policy(
        tmp_path,
        mode="verify_only",
        enabled=True,
        **overrides,
    )

    assert result.returncode != 0
    assert "restricted to blue/green main production preparation" in result.stderr


@pytest.mark.parametrize(
    ("current", "heads", "matches"),
    (
        ("20260715_0046 (head)", "20260715_0046 (head)", True),
        (
            "20260715_0046\n20260716_0047",
            "20260716_0047\n20260715_0046",
            True,
        ),
        ("20260715_0046", "20260716_0047 (head)", False),
        ("", "20260716_0047 (head)", False),
    ),
)
def test_alembic_revision_comparison_is_nonempty_and_exact(
    current: str,
    heads: str,
    matches: bool,
) -> None:
    comparison = _shell_function(
        SERVER_SCRIPT,
        "assert_alembic_revision_sets_equal",
    )
    script = "\n".join(
        (
            "set -Eeuo pipefail",
            comparison,
            (
                "assert_alembic_revision_sets_equal "
                '"$TEST_CURRENT" current "$TEST_HEADS" heads '
                '"database schema mismatch"'
            ),
        )
    )
    result = subprocess.run(
        ["bash", "-c", script],
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "TEST_CURRENT": current,
            "TEST_HEADS": heads,
        },
    )

    assert (result.returncode == 0) is matches
    if not matches:
        assert "database schema mismatch" in result.stderr


def test_verify_only_gate_is_read_only_and_runs_before_backup_completion() -> None:
    script = SERVER_SCRIPT.read_text(encoding="utf-8")
    verifier = _shell_function(
        SERVER_SCRIPT,
        "verify_database_schema_without_migration",
    )
    assert "read_database_current_revision" in verifier
    assert "read_candidate_migration_heads" in verifier
    assert "assert_alembic_revision_sets_equal" in verifier
    assert "alembic upgrade" not in verifier
    assert "default_transaction_read_only=on" in script
    assert verifier.index('DATABASE_READ_ONLY_GATE_FAILED="true"') < verifier.index(
        "read_database_current_revision",
    )

    backup_start = script.index(
        'write_release_checkpoint backup_verified in_progress automatic',
    )
    evidence_start = script.index(
        'write_release_evidence "not_started"',
        backup_start,
    )
    read_only_gate = script.index(
        "verify_database_schema_without_migration",
        evidence_start,
    )
    backup_complete = script.index(
        "write_release_checkpoint backup_verified completed automatic",
        read_only_gate,
    )
    assert backup_start < evidence_start < read_only_gate < backup_complete

    verify_start = script.index(
        'elif [[ "$DATABASE_MIGRATION_VERIFY_ONLY" == "true" ]]',
    )
    verify_branch = script[verify_start:script.index("\nelse\n", verify_start)]
    assert 'write_release_evidence "completed"' in verify_branch
    assert "write_release_checkpoint migrated completed automatic" in verify_branch
    assert "alembic upgrade" not in verify_branch


@pytest.mark.parametrize(
    ("read_only_failed", "checkpoint_written"),
    ((True, False), (False, True)),
)
def test_read_only_gate_failure_preserves_resumable_checkpoint(
    read_only_failed: bool,
    checkpoint_written: bool,
) -> None:
    record_failure = _shell_function(SERVER_SCRIPT, "record_failure_checkpoint")
    script = "\n".join(
        (
            "set -Eeuo pipefail",
            'checkpoint_enabled() { return 0; }',
            'write_release_checkpoint() { printf "checkpoint-write\\n"; }',
            record_failure,
            "CHECKPOINT_WRITING_FAILURE=false",
            (
                "DATABASE_READ_ONLY_GATE_FAILED="
                f"{'true' if read_only_failed else 'false'}"
            ),
            "CURRENT_CHECKPOINT_PHASE=backup_verified",
            'record_failure_checkpoint "read-only current command failed"',
        )
    )
    result = subprocess.run(
        ["bash", "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert ("Preserving the resumable backup checkpoint" in result.stdout) is (
        read_only_failed
    )
    assert ("checkpoint-write" in result.stdout) is checkpoint_written


def test_frontend_public_permissions_only_mutate_excluded_dist(
    tmp_path: Path,
) -> None:
    repo_dir = tmp_path / "repo"
    frontend_dir = repo_dir / "06_AppPlatform/frontend"
    dist_dir = frontend_dir / "dist"
    asset_dir = dist_dir / "assets"
    asset_dir.mkdir(parents=True)
    (dist_dir / "index.html").write_text("ok", encoding="utf-8")
    (asset_dir / "app.js").write_text("ok", encoding="utf-8")
    for directory in (repo_dir, repo_dir / "06_AppPlatform", frontend_dir):
        directory.chmod(0o755)
    for directory in (dist_dir, asset_dir):
        directory.chmod(0o700)
    for file_path in (dist_dir / "index.html", asset_dir / "app.js"):
        file_path.chmod(0o600)
    parent_modes_before = {
        directory: directory.stat().st_mode & 0o777
        for directory in (repo_dir, repo_dir / "06_AppPlatform", frontend_dir)
    }

    result = subprocess.run(
        [
            "bash",
            "-c",
            "set -Eeuo pipefail\n"
            + _shell_function(SERVER_SCRIPT, "normalize_frontend_public_permissions")
            + '\nnormalize_frontend_public_permissions "$FRONTEND_DIR/dist"\n',
        ],
        env={
            **os.environ,
            "REPO_DIR": str(repo_dir),
            "FRONTEND_DIR": str(frontend_dir),
        },
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr + result.stdout
    for directory in (repo_dir, repo_dir / "06_AppPlatform", frontend_dir):
        assert directory.stat().st_mode & 0o777 == parent_modes_before[directory]
    for directory in (dist_dir, asset_dir):
        assert directory.stat().st_mode & 0o777 == 0o755
    for file_path in (dist_dir / "index.html", asset_dir / "app.js"):
        assert file_path.stat().st_mode & 0o777 == 0o644

    install = _shell_function(SERVER_SCRIPT, "install_prebuilt_frontend")
    normalize = _shell_function(
        SERVER_SCRIPT,
        "normalize_frontend_public_permissions",
    )
    assert 'chmod a+x "$parent_dir"' not in normalize
    normalize_staging = install.index(
        'normalize_frontend_public_permissions "$PREBUILT_FRONTEND_DIR"'
    )
    move_live_dist = install.index('mv "$target_dir" "$backup_dir"')
    assert normalize_staging < move_live_dist


def test_frontend_public_permissions_reject_private_sealed_parent(
    tmp_path: Path,
) -> None:
    repo_dir = tmp_path / "repo"
    frontend_dir = repo_dir / "06_AppPlatform/frontend"
    dist_dir = frontend_dir / "dist"
    dist_dir.mkdir(parents=True)
    repo_dir.chmod(0o700)
    (repo_dir / "06_AppPlatform").chmod(0o755)
    frontend_dir.chmod(0o755)

    result = subprocess.run(
        [
            "bash",
            "-c",
            "set -Eeuo pipefail\n"
            + _shell_function(SERVER_SCRIPT, "normalize_frontend_public_permissions")
            + '\nnormalize_frontend_public_permissions "$FRONTEND_DIR/dist"\n',
        ],
        env={
            **os.environ,
            "REPO_DIR": str(repo_dir),
            "FRONTEND_DIR": str(frontend_dir),
        },
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert repo_dir.stat().st_mode & 0o777 == 0o700
    assert "must already be a real safe traversable directory" in result.stdout


def test_frontend_public_permissions_reject_symlinks(tmp_path: Path) -> None:
    repo_dir = tmp_path / "repo"
    frontend_dir = repo_dir / "06_AppPlatform/frontend"
    dist_dir = frontend_dir / "dist"
    dist_dir.mkdir(parents=True)
    (dist_dir / "unsafe-link").symlink_to(tmp_path / "outside")

    result = subprocess.run(
        [
            "bash",
            "-c",
            "set -Eeuo pipefail\n"
            + _shell_function(SERVER_SCRIPT, "normalize_frontend_public_permissions")
            + '\nnormalize_frontend_public_permissions "$FRONTEND_DIR/dist"\n',
        ],
        env={
            **os.environ,
            "REPO_DIR": str(repo_dir),
            "FRONTEND_DIR": str(frontend_dir),
        },
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert "Frontend dist must not contain symlinks" in result.stdout


def _run_backup(tmp_path: Path, *, env_text: str, pg_dump_body: str) -> subprocess.CompletedProcess[str]:
    repo = tmp_path / "repo"
    evidence_root = repo / "04_Processed_data/ops/msrp_source_evidence"
    evidence_root.mkdir(parents=True)
    env_file = tmp_path / "backend.env"
    env_file.write_text(env_text, encoding="utf-8")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    pg_dump = bin_dir / "pg_dump"
    pg_dump.write_text(pg_dump_body, encoding="utf-8")
    pg_dump.chmod(0o755)
    integrity_script = tmp_path / "msrp_evidence_integrity.py"
    integrity_script.write_text(
        """#!/usr/bin/env python3
import argparse
import json

parser = argparse.ArgumentParser()
parser.add_argument("--allow-uninitialized", action="store_true")
parser.add_argument("--evidence-root", required=True)
parser.add_argument("--output", required=True)
parser.add_argument("--object-list-output", required=True)
args = parser.parse_args()
payload = {
    "status": "healthy",
    "summary": {"healthyObjectCount": 0, "verifiedObjectBytes": 0},
}
with open(args.output, "w", encoding="utf-8") as handle:
    json.dump(payload, handle)
with open(args.object_list_output, "w", encoding="utf-8"):
    pass
""",
        encoding="utf-8",
    )
    integrity_script.chmod(0o755)
    environment = os.environ.copy()
    environment.update(
        {
            "PATH": f"{bin_dir}:{environment['PATH']}",
            "REPO_DIR": str(repo),
            "BACKEND_ENV_FILE": str(env_file),
            "BACKUP_ROOT": str(tmp_path / "backups"),
            "REQUIRE_DATABASE_BACKUP": "true",
            "MSRP_EVIDENCE_INTEGRITY_SCRIPT": str(integrity_script),
            "MSRP_GOVERNANCE_EVIDENCE_ROOT": str(evidence_root),
            "MSRP_RELEASE_PATHS_LIB": str(
                REPO_ROOT / "03_Scripts/deploy/lib/release_paths.sh"
            ),
        }
    )
    return subprocess.run(
        ["bash", str(BACKUP_SCRIPT)],
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )


def test_required_database_backup_records_nonempty_dump_identity(tmp_path: Path) -> None:
    result = _run_backup(
        tmp_path,
        env_text=(
            "APP_DATABASE_ENABLED=true\n"
            "APP_DATABASE_URL=postgresql://example.invalid/jato\n"
        ),
        pg_dump_body=(
            "#!/usr/bin/env bash\n"
            "set -eu\n"
            "while [ \"$#\" -gt 0 ]; do\n"
            "  if [ \"$1\" = -f ]; then shift; printf 'verified-dump' > \"$1\"; exit 0; fi\n"
            "  shift\n"
            "done\n"
            "exit 2\n"
        ),
    )
    assert result.returncode == 0, result.stderr + result.stdout
    manifests = list((tmp_path / "backups/manifests").glob("backup-*.json"))
    assert len(manifests) == 1
    database = json.loads(manifests[0].read_text(encoding="utf-8"))["database"]
    assert database["required"] is True
    assert database["status"] == "completed"
    assert database["dumpBytes"] > 0
    assert len(database["dumpSha256"]) == 64
    assert manifests[0].stat().st_mode & 0o777 == 0o600
    assert Path(database["dumpPath"]).stat().st_mode & 0o777 == 0o600
    assert (tmp_path / "backups").stat().st_mode & 0o777 == 0o700


def test_required_database_backup_accepts_case_insensitive_enabled_flag(tmp_path: Path) -> None:
    result = _run_backup(
        tmp_path,
        env_text=(
            "APP_DATABASE_ENABLED=ON\n"
            "APP_DATABASE_URL=postgresql://example.invalid/jato\n"
        ),
        pg_dump_body=(
            "#!/usr/bin/env bash\n"
            "set -eu\n"
            "while [ \"$#\" -gt 0 ]; do\n"
            "  if [ \"$1\" = -f ]; then shift; printf 'verified-dump' > \"$1\"; exit 0; fi\n"
            "  shift\n"
            "done\n"
        ),
    )
    assert result.returncode == 0, result.stderr + result.stdout


def test_required_database_backup_rejects_missing_url(tmp_path: Path) -> None:
    result = _run_backup(
        tmp_path,
        env_text="APP_DATABASE_ENABLED=true\n",
        pg_dump_body="#!/usr/bin/env bash\nexit 0\n",
    )
    assert result.returncode != 0
    assert "required APP_DATABASE_URL/DATABASE_URL is empty" in result.stdout


def test_required_database_backup_rejects_empty_dump(tmp_path: Path) -> None:
    result = _run_backup(
        tmp_path,
        env_text=(
            "APP_DATABASE_ENABLED=true\n"
            "APP_DATABASE_URL=postgresql://example.invalid/jato\n"
        ),
        pg_dump_body=(
            "#!/usr/bin/env bash\n"
            "set -eu\n"
            "while [ \"$#\" -gt 0 ]; do\n"
            "  if [ \"$1\" = -f ]; then shift; : > \"$1\"; exit 0; fi\n"
            "  shift\n"
            "done\n"
        ),
    )
    assert result.returncode != 0
    assert "postgres backup is empty" in result.stdout


def test_checkpoint_recovery_is_lock_bound_and_read_only_except_settlement() -> None:
    controller = RECOVERY_CONTROLLER.read_text(encoding="utf-8")
    helper = RECOVERY_HELPER.read_text(encoding="utf-8")
    source_lock = controller.index('source "$LOCK_LIBRARY"')
    acquire_lock = controller.index("jato_acquire_production_mutation_lock")
    validate_fd = controller.index('"${DEPLOY_LOCK_FD:-}" != "9"')
    privileged_helper = controller.index("sudo -n env")

    assert source_lock < acquire_lock < validate_fd < privileged_helper
    assert (
        'RECOVERY_APPLY_CONFIRMATION="ABORT 2026-07-30-ce5 PRE-SWITCH"'
        in controller
    )
    assert (
        '[[ "$RECOVERY_MODE" == "dry-run" && -n "$RECOVERY_CONFIRMATION" ]]'
        in controller
    )
    for forbidden in (
        "systemctl start",
        "systemctl restart",
        "systemctl enable",
        "systemctl disable",
        "systemctl reload",
        "nginx -s",
        "nginx -t",
        "alembic upgrade",
        "alembic downgrade",
        "tencent_bluegreen_release.sh",
        "fullstack_remote_release.sh",
    ):
        assert forbidden not in controller
        assert forbidden not in helper


def test_aborted_release_cannot_be_replayed_by_outer_or_inner_deployer() -> None:
    outer = REMOTE_SCRIPT.read_text(encoding="utf-8")
    inner = SERVER_SCRIPT.read_text(encoding="utf-8")
    decision = 'CHECKPOINT_DECISION" == "already-pre-switch-aborted"'
    inner_decision = 'resume_decision" == "already-pre-switch-aborted"'

    assert decision in outer
    assert outer.index(decision) < outer.index(
        'bash "$RELEASE_WORKTREE/03_Scripts/deploy/tencent_bluegreen_release.sh"',
    )
    assert "create a new reviewed release instead of replaying it" in outer
    assert "pre_switch_aborted) echo 12" in inner
    assert inner_decision in inner
    assert inner.index(inner_decision) < inner.index(
        "verify_release_evidence",
        inner.index("initialize_release_checkpoint()"),
    )
