from __future__ import annotations

import io
import json
import os
from pathlib import Path
import subprocess
import tarfile


REPO_ROOT = Path(__file__).resolve().parents[2]
REMOTE_SCRIPT = REPO_ROOT / "03_Scripts/deploy/fullstack_remote_release.sh"
BLUEGREEN_SCRIPT = REPO_ROOT / "03_Scripts/deploy/tencent_bluegreen_release.sh"
SERVER_SCRIPT = REPO_ROOT / "03_Scripts/ops/deploy_fullstack_server.sh"
BACKUP_SCRIPT = REPO_ROOT / "03_Scripts/ops/backup_production_data.sh"


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
    return root


def test_archive_member_validator_accepts_single_gnu_tar_root_directory(
    tmp_path: Path,
) -> None:
    archive_path = tmp_path / "valid.tar.gz"
    payload = tarfile.TarInfo("./payload.txt")
    _write_archive(archive_path, [_root_tar_info(), payload])

    result = _run_archive_member_validator(archive_path)

    assert result.returncode == 0, result.stderr
    assert "passed fail-closed validation" in result.stdout


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
    prepare = _shell_function(BLUEGREEN_SCRIPT, "prepare_and_switch")
    status_write = prepare.index("\n    write_candidate_deploy_status\n")
    runtime_seal = prepare.index("\n    finalize_runtime_seal\n")
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
    assert status_write < runtime_seal
    assert switch_checkpoint < activation_call
    assert healthy_checkpoint > activate.index("verify_active_cgroup")
    assert "restore_previous_route" in controller
    assert "checkpoint_write rollback_completed completed automatic" in controller


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
    health = script.index('echo "[INFO] Verify backend health"')
    schedulers = script.index(
        'echo "[INFO] Reconcile scraper schedulers after backend health"'
    )
    assert health < schedulers


def test_frontend_public_permissions_are_normalized_under_private_umask(
    tmp_path: Path,
) -> None:
    repo_dir = tmp_path / "repo"
    frontend_dir = repo_dir / "06_AppPlatform/frontend"
    dist_dir = frontend_dir / "dist"
    asset_dir = dist_dir / "assets"
    asset_dir.mkdir(parents=True)
    (dist_dir / "index.html").write_text("ok", encoding="utf-8")
    (asset_dir / "app.js").write_text("ok", encoding="utf-8")
    for directory in (repo_dir, repo_dir / "06_AppPlatform", frontend_dir, dist_dir, asset_dir):
        directory.chmod(0o700)
    for file_path in (dist_dir / "index.html", asset_dir / "app.js"):
        file_path.chmod(0o600)

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
        assert directory.stat().st_mode & 0o777 == 0o711
    for directory in (dist_dir, asset_dir):
        assert directory.stat().st_mode & 0o777 == 0o755
    for file_path in (dist_dir / "index.html", asset_dir / "app.js"):
        assert file_path.stat().st_mode & 0o777 == 0o644

    install = _shell_function(SERVER_SCRIPT, "install_prebuilt_frontend")
    normalize_staging = install.index(
        'normalize_frontend_public_permissions "$PREBUILT_FRONTEND_DIR"'
    )
    move_live_dist = install.index('mv "$target_dir" "$backup_dir"')
    assert normalize_staging < move_live_dist


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
