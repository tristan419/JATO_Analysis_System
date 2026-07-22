from __future__ import annotations

import io
import json
import os
from pathlib import Path
import subprocess
import tarfile


REPO_ROOT = Path(__file__).resolve().parents[2]
REMOTE_SCRIPT = REPO_ROOT / "03_Scripts/deploy/fullstack_remote_release.sh"
SERVER_SCRIPT = REPO_ROOT / "03_Scripts/ops/deploy_fullstack_server.sh"
BACKUP_SCRIPT = REPO_ROOT / "03_Scripts/ops/backup_production_data.sh"


def _shell_function(script_path: Path, name: str) -> str:
    script = script_path.read_text(encoding="utf-8")
    start = script.index(f"{name}() {{")
    end = script.index("\n}\n", start) + len("\n}\n")
    return script[start:end]


def test_remote_release_validates_content_address_and_finishes_checkpoint_last() -> None:
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
    assert "evidence_sha256=" in script
    assert "source_install_started" in script
    assert "source_installed" in script
    assert 'chmod a+x "$public_parent"' in script
    lock_acquired = script.index("flock -w 300 9")
    helper_extracted = script.index(
        'CHECKPOINT_HELPER="$RELEASE_WORKTREE/03_Scripts/deploy/release_checkpoint.py"'
    )
    cross_release_gate = script.index("assert-cross-release-safe")
    prepared_write = script.index("--phase prepared", cross_release_gate)
    assert lock_acquired < helper_extracted < cross_release_gate < prepared_write
    status_publish = script.index('mv -f "$STATUS_TEMP" "$DIST/_deploy_status.txt"')
    healthy_complete = script.index(
        "--phase backend_healthy --status completed --retry-class automatic"
    )
    assert status_publish < healthy_complete


def test_successful_remote_release_cleanup_cannot_override_deploy_result() -> None:
    script = REMOTE_SCRIPT.read_text(encoding="utf-8")
    cleanup = _shell_function(REMOTE_SCRIPT, "remove_transient_release_paths")

    assert 'if ! rm -rf -- "$transient_path"; then' in cleanup
    assert 'echo "[WARN] Failed to remove transient deployment path:' in cleanup
    assert cleanup.rstrip().endswith("return 0\n}")
    successful_cleanup = script.index('if [[ "$REMOTE_DEPLOY_SUCCEEDED" == "true" ]]')
    cleanup_call = script.index("remove_transient_release_paths", successful_cleanup)
    cleanup_return = script.index("return", cleanup_call)
    assert successful_cleanup < cleanup_call < cleanup_return


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


def test_migration_failure_cannot_advance_when_status_publish_also_fails() -> None:
    script = REMOTE_SCRIPT.read_text(encoding="utf-8")
    start = script.index('if ! mv -f "$STATUS_TEMP" "$DIST/_deploy_status.txt"; then')
    end = script.index('if [[ "$FINAL_RC" -ne 0 ]]; then', start)
    status_failure = script[start:end]
    guard = status_failure.index('if [[ "$DEPLOY_RC" -eq 0 ]]; then')
    healthy_failure = status_failure.index(
        "--phase backend_healthy --status failed --retry-class automatic"
    )
    assert guard < healthy_failure
    assert status_failure.count(
        "--phase backend_healthy --status failed --retry-class automatic"
    ) == 1


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
    (repo / "04_Processed_data/ops").mkdir(parents=True)
    env_file = tmp_path / "backend.env"
    env_file.write_text(env_text, encoding="utf-8")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    pg_dump = bin_dir / "pg_dump"
    pg_dump.write_text(pg_dump_body, encoding="utf-8")
    pg_dump.chmod(0o755)
    environment = os.environ.copy()
    environment.update(
        {
            "PATH": f"{bin_dir}:{environment['PATH']}",
            "REPO_DIR": str(repo),
            "BACKEND_ENV_FILE": str(env_file),
            "BACKUP_ROOT": str(tmp_path / "backups"),
            "REQUIRE_DATABASE_BACKUP": "true",
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
