from __future__ import annotations

import importlib.util
import errno
import json
import os
from pathlib import Path
import shutil
import sys

import pytest


ROOT = Path(__file__).resolve().parents[2]
DEPLOY_DIR = ROOT / "03_Scripts/deploy"
HELPER = DEPLOY_DIR / "jato_release_storage_guard.py"
sys.path.insert(0, str(DEPLOY_DIR))
SPEC = importlib.util.spec_from_file_location(
    "jato_release_storage_guard",
    HELPER,
)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)

import release_checkpoint as CHECKPOINT  # noqa: E402


GIB = 1024**3
DAY_SECONDS = 24 * 60 * 60
NOW_NS = 100 * DAY_SECONDS * 1_000_000_000
COMMIT_TARGET = "a" * 40
ARCHIVE_TARGET = "1" * 64


def release_id(commit: str, archive: str) -> str:
    return f"{commit}/{archive}"


def make_release(
    releases_root: Path,
    *,
    commit: str,
    archive: str,
    modified_ns: int,
    with_identity: bool = True,
    identity: str | None = None,
) -> Path:
    release = releases_root / commit / archive
    release.mkdir(parents=True)
    if with_identity:
        (release / MODULE.IDENTITY_FILE_NAME).write_text(
            identity or f"commit={commit} archive={archive}",
            encoding="utf-8",
        )
    (release / "payload.txt").write_text("immutable release\n", encoding="utf-8")
    os.utime(release, ns=(modified_ns, modified_ns))
    return release


def checkpoint_identity(commit: str, archive: str, run_id: int) -> object:
    return CHECKPOINT.ReleaseIdentity.create(
        repository="example/JATO_Analysis_System",
        commit=commit,
        archive_sha256=archive,
        archive_bytes=22_000_000,
        run_id=run_id,
        run_attempt=1,
        frontend_identity=(
            f"gha://example/JATO_Analysis_System/artifacts/frontend-{run_id}"
        ),
        frontend_checksum=f"{run_id:064x}",
    )


def write_checkpoint(
    checkpoints_root: Path,
    *,
    commit: str,
    archive: str,
    run_id: int,
    phase: str,
    status: str,
    retry_class: str = "automatic",
) -> Path:
    path = checkpoints_root / commit / f"{archive}.json"
    journal = (
        checkpoints_root.parent
        / "journals"
        / commit
        / f"{archive}.jsonl"
    )
    CHECKPOINT.write_checkpoint(
        checkpoint_path=path,
        journal_path=journal,
        identity=checkpoint_identity(commit, archive, run_id),
        phase=phase,
        status=status,
        retry_class=retry_class,
        now="2026-07-24T00:00:00.000Z",
    )
    return path


class FilesystemSequence:
    def __init__(self, *available: int, total: int = 200 * GIB) -> None:
        assert available
        self.available = list(available)
        self.total = total
        self.calls = 0

    def __call__(self, _path: Path) -> object:
        index = min(self.calls, len(self.available) - 1)
        self.calls += 1
        return MODULE.FilesystemUsage(
            available_bytes=self.available[index],
            total_bytes=self.total,
        )


class GuardHarness:
    def __init__(self, tmp_path: Path) -> None:
        self.root = tmp_path
        self.releases_root = tmp_path / "releases"
        self.checkpoints_root = tmp_path / "checkpoints"
        self.target = make_release(
            self.releases_root,
            commit=COMMIT_TARGET,
            archive=ARCHIVE_TARGET,
            modified_ns=NOW_NS,
        )
        self.current_checkpoint = write_checkpoint(
            self.checkpoints_root,
            commit=COMMIT_TARGET,
            archive=ARCHIVE_TARGET,
            run_id=1,
            phase="source_install_started",
            status="in_progress",
        )
        self.nginx = tmp_path / "active-release.conf"
        self.nginx.write_text("# injected reference provider\n", encoding="utf-8")
        self.references: set[str] = set()
        self.mount_points: tuple[Path, ...] = ()
        self.next_run_id = 2

    def old_release(
        self,
        *,
        age_days: int,
        settled: bool = True,
        commit_digit: int | None = None,
        archive_digit: int | None = None,
    ) -> tuple[str, Path]:
        run_id = self.next_run_id
        self.next_run_id += 1
        digit = commit_digit if commit_digit is not None else run_id
        archive_value = archive_digit if archive_digit is not None else run_id
        commit = f"{digit:040x}"
        archive = f"{archive_value:064x}"
        path = make_release(
            self.releases_root,
            commit=commit,
            archive=archive,
            modified_ns=NOW_NS - age_days * DAY_SECONDS * 1_000_000_000,
        )
        write_checkpoint(
            self.checkpoints_root,
            commit=commit,
            archive=archive,
            run_id=run_id,
            phase="backend_healthy" if settled else "switch_started",
            status="completed" if settled else "in_progress",
        )
        return release_id(commit, archive), path

    def kwargs(
        self,
        *,
        usage: FilesystemSequence,
        minimum_available_bytes: int = 1,
        minimum_available_percent: float = 0,
        keep_unreferenced: int = 3,
        normal_min_age_seconds: int = 14 * DAY_SECONDS,
        emergency_min_age_seconds: int = DAY_SECONDS,
        check_only: bool = False,
    ) -> dict[str, object]:
        return {
            "releases_root": self.releases_root,
            "target_root": self.target,
            "protected_roots": (),
            "checkpoints_root": self.checkpoints_root,
            "current_checkpoint": self.current_checkpoint,
            "expected_repository": "example/JATO_Analysis_System",
            "nginx_active_release_conf": self.nginx,
            "expected_active_slot": "8000",
            "minimum_available_bytes": minimum_available_bytes,
            "minimum_available_percent": minimum_available_percent,
            "keep_unreferenced": keep_unreferenced,
            "normal_min_age_seconds": normal_min_age_seconds,
            "emergency_min_age_seconds": emergency_min_age_seconds,
            "check_only": check_only,
            "proc_root": self.root,
            "now_ns": NOW_NS,
            "filesystem_usage_provider": usage,
            "reference_provider": lambda: frozenset(self.references),
            "mount_points_provider": lambda: self.mount_points,
        }


def test_exact_byte_reserve_threshold_passes(tmp_path: Path) -> None:
    harness = GuardHarness(tmp_path)

    report = MODULE.guard_release_storage(
        **harness.kwargs(
            usage=FilesystemSequence(15 * GIB),
            minimum_available_bytes=15 * GIB,
        )
    )

    assert report.available_before_bytes == 15 * GIB
    assert report.available_after_bytes == 15 * GIB
    assert report.minimum_available_bytes == 15 * GIB
    assert report.pruned_release_ids == ()


def test_one_byte_below_reserve_fails_when_nothing_is_eligible(
    tmp_path: Path,
) -> None:
    harness = GuardHarness(tmp_path)

    with pytest.raises(
        MODULE.StorageGuardError,
        match=r"after safe GC.*available=.*required=",
    ):
        MODULE.guard_release_storage(
            **harness.kwargs(
                usage=FilesystemSequence(15 * GIB - 1),
                minimum_available_bytes=15 * GIB,
            )
        )

    assert harness.target.is_dir()


def test_percent_reserve_uses_larger_of_bytes_and_percentage(
    tmp_path: Path,
) -> None:
    harness = GuardHarness(tmp_path)
    usage = FilesystemSequence(16 * GIB, total=200 * GIB)

    report = MODULE.guard_release_storage(
        **harness.kwargs(
            usage=usage,
            minimum_available_bytes=15 * GIB,
            minimum_available_percent=8,
        )
    )

    assert report.minimum_available_bytes == 16 * GIB
    assert report.minimum_available_percent == 8


def test_percentage_threshold_rounds_up_and_fails_one_byte_below(
    tmp_path: Path,
) -> None:
    harness = GuardHarness(tmp_path)

    with pytest.raises(
        MODULE.StorageGuardError,
        match=r"required=34",
    ):
        MODULE.guard_release_storage(
            **harness.kwargs(
                usage=FilesystemSequence(33, total=333),
                minimum_available_bytes=1,
                minimum_available_percent=10,
            )
        )


def test_gc_that_still_cannot_restore_reserve_fails_closed(
    tmp_path: Path,
) -> None:
    harness = GuardHarness(tmp_path)
    old_id, old_path = harness.old_release(age_days=30)

    with pytest.raises(
        MODULE.StorageGuardError,
        match="insufficient release filesystem capacity after safe GC",
    ):
        MODULE.guard_release_storage(
            **harness.kwargs(
                usage=FilesystemSequence(10, 11),
                minimum_available_bytes=12,
                keep_unreferenced=0,
            )
        )

    assert not old_path.exists()
    assert old_id
    assert harness.target.is_dir()


def test_filesystem_capacity_change_during_gc_fails_closed(
    tmp_path: Path,
) -> None:
    harness = GuardHarness(tmp_path)
    _release, _path = harness.old_release(age_days=30)
    calls = 0

    def changing_usage(_path: Path) -> object:
        nonlocal calls
        calls += 1
        return MODULE.FilesystemUsage(
            available_bytes=10,
            total_bytes=(200 + calls - 1) * GIB,
        )

    kwargs = harness.kwargs(
        usage=FilesystemSequence(10),
        keep_unreferenced=0,
        normal_min_age_seconds=0,
        emergency_min_age_seconds=0,
    )
    kwargs["filesystem_usage_provider"] = changing_usage

    with pytest.raises(
        MODULE.StorageGuardError,
        match="total capacity changed",
    ):
        MODULE.guard_release_storage(**kwargs)


def test_nonsettled_checkpoint_protects_release(tmp_path: Path) -> None:
    harness = GuardHarness(tmp_path)
    release, path = harness.old_release(age_days=30, settled=False)

    report = MODULE.guard_release_storage(
        **harness.kwargs(
            usage=FilesystemSequence(1),
            keep_unreferenced=0,
            normal_min_age_seconds=0,
            emergency_min_age_seconds=0,
        )
    )

    assert release in report.protected_release_ids
    assert report.pruned_release_ids == ()
    assert path.is_dir()


def test_checkpoint_repository_mismatch_fails_before_pruning(
    tmp_path: Path,
) -> None:
    harness = GuardHarness(tmp_path)
    _release, path = harness.old_release(age_days=30)
    kwargs = harness.kwargs(
        usage=FilesystemSequence(1),
        keep_unreferenced=0,
        normal_min_age_seconds=0,
        emergency_min_age_seconds=0,
    )
    kwargs["expected_repository"] = "other/repository"

    with pytest.raises(
        MODULE.StorageGuardError,
        match="checkpoint repository differs",
    ):
        MODULE.guard_release_storage(**kwargs)

    assert path.is_dir()
    assert harness.target.is_dir()
    assert not list(harness.releases_root.rglob(".gc-*"))


def test_dangerous_retry_class_fails_before_pruning(tmp_path: Path) -> None:
    harness = GuardHarness(tmp_path)
    commit = "8" * 40
    archive = "9" * 64
    path = make_release(
        harness.releases_root,
        commit=commit,
        archive=archive,
        modified_ns=NOW_NS - 30 * DAY_SECONDS * 1_000_000_000,
    )
    write_checkpoint(
        harness.checkpoints_root,
        commit=commit,
        archive=archive,
        run_id=99,
        phase="switch_started",
        status="in_progress",
        retry_class="rollback_required",
    )

    with pytest.raises(
        MODULE.StorageGuardError,
        match="requires operator recovery",
    ):
        MODULE.guard_release_storage(
            **harness.kwargs(
                usage=FilesystemSequence(1),
                keep_unreferenced=0,
                normal_min_age_seconds=0,
                emergency_min_age_seconds=0,
            )
        )

    assert path.is_dir()
    assert harness.target.is_dir()
    assert not list(harness.releases_root.rglob(".gc-*"))


@pytest.mark.parametrize(
    "source",
    ["active", "slot-8000", "slot-8001", "nginx", "process"],
)
def test_runtime_reference_sources_protect_release(
    tmp_path: Path,
    source: str,
) -> None:
    harness = GuardHarness(tmp_path)
    release, path = harness.old_release(age_days=30)
    harness.references.add(release)

    report = MODULE.guard_release_storage(
        **harness.kwargs(
            usage=FilesystemSequence(1),
            keep_unreferenced=0,
            normal_min_age_seconds=0,
            emergency_min_age_seconds=0,
        )
    )

    assert source
    assert release in report.protected_release_ids
    assert report.pruned_release_ids == ()
    assert path.is_dir()


def test_reference_is_rescanned_before_quarantine(tmp_path: Path) -> None:
    harness = GuardHarness(tmp_path)
    release, path = harness.old_release(age_days=30)
    calls = 0

    def references() -> frozenset[str]:
        nonlocal calls
        calls += 1
        return frozenset({release}) if calls >= 2 else frozenset()

    kwargs = harness.kwargs(
        usage=FilesystemSequence(1),
        keep_unreferenced=0,
        normal_min_age_seconds=0,
        emergency_min_age_seconds=0,
    )
    kwargs["reference_provider"] = references

    with pytest.raises(
        MODULE.StorageGuardError,
        match="became referenced before quarantine",
    ):
        MODULE.guard_release_storage(**kwargs)

    assert path.is_dir()
    assert not list(path.parent.glob(".gc-*"))


def test_reference_after_rename_restores_original_release(
    tmp_path: Path,
) -> None:
    harness = GuardHarness(tmp_path)
    release, path = harness.old_release(age_days=30)
    calls = 0

    def references() -> frozenset[str]:
        nonlocal calls
        calls += 1
        return frozenset({release}) if calls >= 3 else frozenset()

    kwargs = harness.kwargs(
        usage=FilesystemSequence(1),
        keep_unreferenced=0,
        normal_min_age_seconds=0,
        emergency_min_age_seconds=0,
    )
    kwargs["reference_provider"] = references

    with pytest.raises(
        MODULE.StorageGuardError,
        match="became referenced after quarantine",
    ):
        MODULE.guard_release_storage(**kwargs)

    assert path.is_dir()
    assert not list(path.parent.glob(".gc-*"))


def test_actual_nginx_and_process_reference_collection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = GuardHarness(tmp_path)
    release, path = harness.old_release(age_days=30)
    frontend = path / "06_AppPlatform/frontend/dist"
    frontend.mkdir(parents=True)
    proc_root = tmp_path / "proc"
    process = proc_root / "123"
    process.mkdir(parents=True)
    (process / "cwd").symlink_to(path, target_is_directory=True)
    (process / "exe").symlink_to(path / "payload.txt")
    monkeypatch.setattr(MODULE, "read_active_slot", lambda _path: "8000")
    monkeypatch.setattr(
        MODULE,
        "read_active_frontend_root",
        lambda _path: frontend,
    )

    references = MODULE.collect_release_references(
        releases_root=harness.releases_root,
        protected_roots=(path,),
        nginx_active_release_conf=harness.nginx,
        expected_active_slot="8000",
        expected_active_root=path,
        proc_root=proc_root,
    )

    assert references == frozenset({release})


def test_process_root_reference_protects_chrooted_release(tmp_path: Path) -> None:
    harness = GuardHarness(tmp_path)
    release, path = harness.old_release(age_days=30)
    proc_root = tmp_path / "proc"
    process = proc_root / "123"
    process.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    (process / "cwd").symlink_to(outside, target_is_directory=True)
    (process / "exe").symlink_to(outside / "python")
    (process / "root").symlink_to(path, target_is_directory=True)

    assert MODULE.collect_process_release_ids(
        harness.releases_root,
        proc_root=proc_root,
    ) == frozenset({release})


def test_nginx_frontend_must_match_controller_active_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = GuardHarness(tmp_path)
    _release, active_root = harness.old_release(age_days=30)
    active_frontend = active_root / "06_AppPlatform/frontend/dist"
    active_frontend.mkdir(parents=True)
    _other_release, other_root = harness.old_release(age_days=31)
    other_frontend = other_root / "06_AppPlatform/frontend/dist"
    other_frontend.mkdir(parents=True)
    proc_root = tmp_path / "proc"
    proc_root.mkdir()
    monkeypatch.setattr(MODULE, "read_active_slot", lambda _path: "8000")
    monkeypatch.setattr(
        MODULE,
        "read_active_frontend_root",
        lambda _path: other_frontend,
    )

    with pytest.raises(
        MODULE.StorageGuardError,
        match="frontend root differs",
    ):
        MODULE.collect_release_references(
            releases_root=harness.releases_root,
            protected_roots=(active_root,),
            nginx_active_release_conf=harness.nginx,
            expected_active_slot="8000",
            expected_active_root=active_root,
            proc_root=proc_root,
        )


def test_mountinfo_parser_decodes_bind_mount_paths(tmp_path: Path) -> None:
    mountinfo = tmp_path / "mountinfo"
    mountinfo.write_text(
        "36 25 0:32 / /opt/jato/releases/"
        f"{'a' * 40}/{'b' * 64}/bound\\040data rw,relatime"
        " - ext4 /dev/vda1 rw\n",
        encoding="utf-8",
    )

    assert MODULE.read_mount_points(mountinfo) == (
        Path(
            "/opt/jato/releases/"
            f"{'a' * 40}/{'b' * 64}/bound data"
        ),
    )


def test_storage_cli_exercises_default_reference_discovery(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = GuardHarness(tmp_path)
    _active_id, active_root = harness.old_release(age_days=2)
    active_frontend = active_root / "06_AppPlatform/frontend/dist"
    active_frontend.mkdir(parents=True)
    (active_frontend / "index.html").write_text("active", encoding="utf-8")
    harness.nginx.write_text(
        "upstream jato_fullstack_api {\n"
        "  server 127.0.0.1:8000;\n"
        "}\n"
        "map $host $jato_frontend_root {\n"
        f'  default "{active_frontend}";\n'
        "}\n",
        encoding="utf-8",
    )
    proc_root = tmp_path / "proc"
    proc_root.mkdir()
    monkeypatch.setattr(MODULE, "read_active_slot", lambda _path: "8000")
    monkeypatch.setattr(
        MODULE,
        "read_active_frontend_root",
        lambda _path: active_frontend,
    )

    result = MODULE.main(
        [
            "storage",
            "--releases-root",
            str(harness.releases_root),
            "--target-root",
            str(harness.target),
            "--protected-root",
            str(active_root),
            "--checkpoints-root",
            str(harness.checkpoints_root),
            "--current-checkpoint",
            str(harness.current_checkpoint),
            "--expected-repository",
            "example/JATO_Analysis_System",
            "--nginx-active-release-conf",
            str(harness.nginx),
            "--expected-active-slot",
            "8000",
            "--expected-active-root",
            str(active_root),
            "--minimum-available-bytes",
            "1",
            "--proc-root",
            str(proc_root),
            "--check-only",
        ]
    )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert release_id(COMMIT_TARGET, ARCHIVE_TARGET) in payload[
        "protected_release_ids"
    ]


def test_storage_cli_non_check_only_uses_mount_scan_and_prunes_release(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = GuardHarness(tmp_path)
    _active_id, active_root = harness.old_release(age_days=2)
    active_frontend = active_root / "06_AppPlatform/frontend/dist"
    active_frontend.mkdir(parents=True)
    (active_frontend / "index.html").write_text("active", encoding="utf-8")
    old_id, old_root = harness.old_release(age_days=30)
    harness.nginx.write_text(
        "upstream jato_fullstack_api {\n"
        "  server 127.0.0.1:8000;\n"
        "}\n"
        "map $host $jato_frontend_root {\n"
        f'  default "{active_frontend}";\n'
        "}\n",
        encoding="utf-8",
    )
    proc_root = tmp_path / "proc"
    proc_root.mkdir()
    usage = FilesystemSequence(9, 10)
    mount_calls = 0

    def record_mount_scan() -> tuple[Path, ...]:
        nonlocal mount_calls
        mount_calls += 1
        return ()

    monkeypatch.setattr(MODULE, "filesystem_usage", usage)
    monkeypatch.setattr(MODULE, "read_mount_points", record_mount_scan)
    monkeypatch.setattr(
        MODULE,
        "read_active_frontend_root",
        lambda _path: active_frontend,
    )

    result = MODULE.main(
        [
            "storage",
            "--releases-root",
            str(harness.releases_root),
            "--target-root",
            str(harness.target),
            "--protected-root",
            str(active_root),
            "--checkpoints-root",
            str(harness.checkpoints_root),
            "--current-checkpoint",
            str(harness.current_checkpoint),
            "--expected-repository",
            "example/JATO_Analysis_System",
            "--nginx-active-release-conf",
            str(harness.nginx),
            "--expected-active-slot",
            "8000",
            "--expected-active-root",
            str(active_root),
            "--minimum-available-bytes",
            "10",
            "--keep-unreferenced",
            "0",
            "--normal-min-age-seconds",
            "0",
            "--emergency-min-age-seconds",
            "0",
            "--proc-root",
            str(proc_root),
        ]
    )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert old_id in payload["pruned_release_ids"]
    assert not old_root.exists()
    assert active_root.is_dir()
    assert harness.target.is_dir()
    assert not list(harness.releases_root.rglob(".gc-*"))
    assert mount_calls >= 2


@pytest.mark.parametrize(
    ("failure", "expected_error"),
    (
        ("backend", "active slot differs"),
        ("frontend", "frontend root differs"),
        ("symlink", "cannot prove the active Nginx release"),
        (
            "missing",
            "allowed only for an entirely legacy first deployment",
        ),
    ),
)
def test_storage_cli_rejects_unproven_nginx_route(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
    expected_error: str,
) -> None:
    harness = GuardHarness(tmp_path)
    _active_id, active_root = harness.old_release(age_days=2)
    active_frontend = active_root / "06_AppPlatform/frontend/dist"
    active_frontend.mkdir(parents=True)
    (active_frontend / "index.html").write_text("active", encoding="utf-8")
    _old_id, old_root = harness.old_release(age_days=30)
    other_frontend = tmp_path / "other-frontend"
    other_frontend.mkdir()
    (other_frontend / "index.html").write_text("other", encoding="utf-8")
    routed_slot = "8001" if failure == "backend" else "8000"
    routed_frontend = (
        other_frontend if failure == "frontend" else active_frontend
    )
    route_text = (
        "upstream jato_fullstack_api {\n"
        f"  server 127.0.0.1:{routed_slot};\n"
        "}\n"
        "map $host $jato_frontend_root {\n"
        f'  default "{routed_frontend}";\n'
        "}\n"
    )
    extra_args: list[str] = []
    if failure == "symlink":
        real_route = tmp_path / "real-active-release.conf"
        real_route.write_text(route_text, encoding="utf-8")
        harness.nginx.unlink()
        harness.nginx.symlink_to(real_route)
    elif failure == "missing":
        harness.nginx.unlink()
        extra_args.append("--allow-missing-nginx-legacy")
    else:
        harness.nginx.write_text(route_text, encoding="utf-8")
    if failure in {"backend", "frontend"}:
        monkeypatch.setattr(
            MODULE,
            "read_active_frontend_root",
            lambda _path: routed_frontend,
        )
    proc_root = tmp_path / "proc"
    proc_root.mkdir()

    result = MODULE.main(
        [
            "storage",
            "--releases-root",
            str(harness.releases_root),
            "--target-root",
            str(harness.target),
            "--protected-root",
            str(active_root),
            "--checkpoints-root",
            str(harness.checkpoints_root),
            "--current-checkpoint",
            str(harness.current_checkpoint),
            "--expected-repository",
            "example/JATO_Analysis_System",
            "--nginx-active-release-conf",
            str(harness.nginx),
            "--expected-active-slot",
            "8000",
            "--expected-active-root",
            str(active_root),
            "--minimum-available-bytes",
            "1",
            "--proc-root",
            str(proc_root),
            "--check-only",
            *extra_args,
        ]
    )

    captured = capsys.readouterr()
    assert result == 1
    assert expected_error in captured.err
    assert old_root.is_dir()
    assert active_root.is_dir()
    assert harness.target.is_dir()
    assert not list(harness.releases_root.rglob(".gc-*"))


def test_process_reference_scan_fails_closed_on_io_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    releases_root = tmp_path / "releases"
    releases_root.mkdir()
    proc_root = tmp_path / "proc"
    (proc_root / "123").mkdir(parents=True)

    def fail_readlink(_path: Path) -> str:
        raise OSError(errno.EIO, "injected I/O error")

    monkeypatch.setattr(MODULE.os, "readlink", fail_readlink)

    with pytest.raises(
        MODULE.StorageGuardError,
        match="cannot inspect process release reference",
    ):
        MODULE.collect_process_release_ids(
            releases_root,
            proc_root=proc_root,
        )


@pytest.mark.parametrize(
    ("where", "name"),
    [
        ("commit", "not-a-commit"),
        ("archive", "not-an-archive"),
        ("archive", ".gc-not-valid"),
    ],
)
def test_illegal_release_namespace_entry_fails_closed(
    tmp_path: Path,
    where: str,
    name: str,
) -> None:
    harness = GuardHarness(tmp_path)
    if where == "commit":
        (harness.releases_root / name).mkdir()
    else:
        (harness.releases_root / ("b" * 40) / name).mkdir(parents=True)

    with pytest.raises(
        MODULE.StorageGuardError,
        match="unexpected entry|unrecognized",
    ):
        MODULE.guard_release_storage(
            **harness.kwargs(usage=FilesystemSequence(1))
        )


@pytest.mark.parametrize("level", ["root", "commit", "archive"])
def test_release_namespace_symlink_fails_closed(
    tmp_path: Path,
    level: str,
) -> None:
    harness = GuardHarness(tmp_path)
    if level == "root":
        linked = tmp_path / "linked-releases"
        linked.symlink_to(harness.releases_root, target_is_directory=True)
        harness.releases_root = linked
        harness.target = linked / COMMIT_TARGET / ARCHIVE_TARGET
    elif level == "commit":
        other = tmp_path / "commit-target"
        other.mkdir()
        (harness.releases_root / ("b" * 40)).symlink_to(
            other,
            target_is_directory=True,
        )
    else:
        commit = harness.releases_root / ("b" * 40)
        commit.mkdir()
        other = tmp_path / "archive-target"
        other.mkdir()
        (commit / ("2" * 64)).symlink_to(other, target_is_directory=True)

    with pytest.raises(MODULE.StorageGuardError, match="symlink|unsafe"):
        MODULE.guard_release_storage(
            **harness.kwargs(usage=FilesystemSequence(1))
        )


def test_identity_mismatch_fails_before_pruning(tmp_path: Path) -> None:
    harness = GuardHarness(tmp_path)
    commit = "b" * 40
    archive = "2" * 64
    make_release(
        harness.releases_root,
        commit=commit,
        archive=archive,
        modified_ns=1,
        identity=f"commit={'c' * 40} archive={'3' * 64}",
    )
    write_checkpoint(
        harness.checkpoints_root,
        commit=commit,
        archive=archive,
        run_id=2,
        phase="backend_healthy",
        status="completed",
    )

    with pytest.raises(MODULE.StorageGuardError, match="identity mismatch"):
        MODULE.guard_release_storage(
            **harness.kwargs(
                usage=FilesystemSequence(1),
                keep_unreferenced=0,
                normal_min_age_seconds=0,
                emergency_min_age_seconds=0,
            )
        )


@pytest.mark.parametrize("location", ("release-child", "commit-parent"))
def test_mount_or_bind_mount_boundary_blocks_gc(
    tmp_path: Path,
    location: str,
) -> None:
    harness = GuardHarness(tmp_path)
    _release, path = harness.old_release(age_days=30)
    mount_point = (
        path / "bound-data"
        if location == "release-child"
        else path.parent
    )
    harness.mount_points = (mount_point,)

    with pytest.raises(
        MODULE.StorageGuardError,
        match="mount or bind-mount boundary",
    ):
        MODULE.guard_release_storage(
            **harness.kwargs(
                usage=FilesystemSequence(1),
                keep_unreferenced=0,
                normal_min_age_seconds=0,
                emergency_min_age_seconds=0,
            )
        )

    assert path.is_dir()


def test_inode_replacement_before_quarantine_fails_closed(
    tmp_path: Path,
) -> None:
    harness = GuardHarness(tmp_path)
    _release, path = harness.old_release(age_days=30)
    calls = 0

    def references() -> frozenset[str]:
        nonlocal calls
        calls += 1
        if calls == 2:
            shutil.rmtree(path)
            path.mkdir()
            (path / MODULE.IDENTITY_FILE_NAME).write_text(
                f"commit={path.parent.name} archive={path.name}",
                encoding="utf-8",
            )
        return frozenset()

    kwargs = harness.kwargs(
        usage=FilesystemSequence(1),
        keep_unreferenced=0,
        normal_min_age_seconds=0,
        emergency_min_age_seconds=0,
    )
    kwargs["reference_provider"] = references

    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
    )
    try:
        with pytest.raises(
            MODULE.StorageGuardError,
            match="changed between validation and pruning",
        ):
            MODULE.guard_release_storage(**kwargs)
    finally:
        os.close(descriptor)

    assert path.is_dir()


def test_normal_gc_keeps_newest_three_and_prunes_old_beyond_retention(
    tmp_path: Path,
) -> None:
    harness = GuardHarness(tmp_path)
    entries = [
        harness.old_release(age_days=age)
        for age in (30, 20, 10, 5, 2)
    ]

    report = MODULE.guard_release_storage(
        **harness.kwargs(
            usage=FilesystemSequence(1),
            keep_unreferenced=3,
            normal_min_age_seconds=14 * DAY_SECONDS,
            emergency_min_age_seconds=DAY_SECONDS,
        )
    )

    assert report.pruned_release_ids == (entries[0][0], entries[1][0])
    assert report.retained_unreferenced_release_ids == tuple(
        sorted(release for release, _path in entries[2:])
    )
    assert all(not path.exists() for _release, path in entries[:2])
    assert all(path.is_dir() for _release, path in entries[2:])


def test_emergency_gc_may_prune_retained_release_older_than_one_day(
    tmp_path: Path,
) -> None:
    harness = GuardHarness(tmp_path)
    release, path = harness.old_release(age_days=2)

    report = MODULE.guard_release_storage(
        **harness.kwargs(
            usage=FilesystemSequence(9, 9, 10),
            minimum_available_bytes=10,
            keep_unreferenced=3,
            normal_min_age_seconds=14 * DAY_SECONDS,
            emergency_min_age_seconds=DAY_SECONDS,
        )
    )

    assert report.pruned_release_ids == (release,)
    assert not path.exists()


def test_emergency_gc_never_prunes_release_younger_than_one_day(
    tmp_path: Path,
) -> None:
    harness = GuardHarness(tmp_path)
    release, path = harness.old_release(age_days=0)

    with pytest.raises(
        MODULE.StorageGuardError,
        match="insufficient release filesystem capacity after safe GC",
    ):
        MODULE.guard_release_storage(
            **harness.kwargs(
                usage=FilesystemSequence(9),
                minimum_available_bytes=10,
                keep_unreferenced=0,
                normal_min_age_seconds=14 * DAY_SECONDS,
                emergency_min_age_seconds=DAY_SECONDS,
            )
        )

    assert release
    assert path.is_dir()


def test_quarantine_is_resumed_after_interrupted_delete(
    tmp_path: Path,
) -> None:
    harness = GuardHarness(tmp_path)
    release, path = harness.old_release(age_days=30)

    def interrupt_delete(
        _quarantine: object,
        _root: Path,
        _mounts: object,
    ) -> None:
        raise OSError("injected delete interruption")

    first_kwargs = harness.kwargs(
        usage=FilesystemSequence(1),
        keep_unreferenced=0,
        normal_min_age_seconds=0,
        emergency_min_age_seconds=0,
    )
    first_kwargs["quarantine_remover"] = interrupt_delete
    with pytest.raises(OSError, match="injected delete interruption"):
        MODULE.guard_release_storage(**first_kwargs)

    assert not path.exists()
    assert len(list(path.parent.glob(".gc-*.marker.json"))) == 1
    assert len(
        [
            candidate
            for candidate in path.parent.glob(".gc-*")
            if not candidate.name.endswith(".marker.json")
        ]
    ) == 1

    report = MODULE.guard_release_storage(
        **harness.kwargs(
            usage=FilesystemSequence(1),
            keep_unreferenced=0,
            normal_min_age_seconds=0,
            emergency_min_age_seconds=0,
        )
    )

    assert report.resumed_quarantine_release_ids == (release,)
    assert report.pruned_release_ids == (release,)
    assert not path.parent.exists()


def test_quarantine_marker_binds_directory_archive_after_partial_delete(
    tmp_path: Path,
) -> None:
    harness = GuardHarness(tmp_path)
    _release, _path = harness.old_release(age_days=30)

    def interrupt_delete(
        _quarantine: object,
        _root: Path,
        _mounts: object,
    ) -> None:
        raise OSError("injected delete interruption")

    kwargs = harness.kwargs(
        usage=FilesystemSequence(1),
        keep_unreferenced=0,
        normal_min_age_seconds=0,
        emergency_min_age_seconds=0,
    )
    kwargs["quarantine_remover"] = interrupt_delete
    with pytest.raises(OSError):
        MODULE.guard_release_storage(**kwargs)

    marker = next(harness.releases_root.rglob(".gc-*.marker.json"))
    quarantine = next(
        candidate
        for candidate in marker.parent.glob(".gc-*")
        if candidate.is_dir()
    )
    (quarantine / MODULE.IDENTITY_FILE_NAME).unlink()
    payload = json.loads(marker.read_text(encoding="utf-8"))
    payload["quarantineName"] = (
        f".gc-{'f' * 64}-{payload['quarantineName'].rsplit('-', 1)[1]}"
    )
    marker.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(
        MODULE.StorageGuardError,
        match="quarantine marker values are invalid",
    ):
        MODULE.guard_release_storage(
            **harness.kwargs(usage=FilesystemSequence(1))
        )


def test_interrupted_quarantine_marker_temporary_is_safely_removed(
    tmp_path: Path,
) -> None:
    harness = GuardHarness(tmp_path)
    temporary = (
        harness.target.parent
        / (
            f".gc-{ARCHIVE_TARGET}-{'e' * 16}.marker.json."
            "abcdef12.tmp"
        )
    )
    temporary.write_text('{"partial":', encoding="utf-8")

    report = MODULE.guard_release_storage(
        **harness.kwargs(usage=FilesystemSequence(1))
    )

    assert report.pruned_release_ids == ()
    assert not temporary.exists()
    assert harness.target.is_dir()


def test_check_only_never_runs_gc(tmp_path: Path) -> None:
    harness = GuardHarness(tmp_path)
    _release, path = harness.old_release(age_days=30)
    calls = 0

    def forbidden_remover(
        _quarantine: object,
        _root: Path,
        _mounts: object,
    ) -> None:
        nonlocal calls
        calls += 1
        raise AssertionError("check-only must not remove releases")

    kwargs = harness.kwargs(
        usage=FilesystemSequence(10),
        minimum_available_bytes=10,
        keep_unreferenced=0,
        normal_min_age_seconds=0,
        emergency_min_age_seconds=0,
        check_only=True,
    )
    kwargs["quarantine_remover"] = forbidden_remover
    report = MODULE.guard_release_storage(**kwargs)

    assert report.pruned_release_ids == ()
    assert calls == 0
    assert path.is_dir()


def test_check_only_fails_one_byte_below_without_gc(tmp_path: Path) -> None:
    harness = GuardHarness(tmp_path)
    _release, path = harness.old_release(age_days=30)

    with pytest.raises(
        MODULE.StorageGuardError,
        match=r"insufficient release filesystem capacity.*available=9 required=10",
    ):
        MODULE.guard_release_storage(
            **harness.kwargs(
                usage=FilesystemSequence(9),
                minimum_available_bytes=10,
                check_only=True,
            )
        )

    assert path.is_dir()


def test_legacy_active_root_outside_release_store_is_ignored_safely(
    tmp_path: Path,
) -> None:
    harness = GuardHarness(tmp_path)
    legacy = tmp_path / "legacy-install"
    legacy.mkdir()
    (legacy / "sentinel").write_text("keep\n", encoding="utf-8")
    monkey_references = MODULE.collect_release_references

    # The pure reference boundary must not map the first-deploy legacy root
    # into the content-addressed release store.
    assert (
        MODULE._release_id_from_path(
            harness.releases_root,
            legacy,
            require_exists=True,
        )
        is None
    )
    assert monkey_references
    assert (legacy / "sentinel").read_text(encoding="utf-8") == "keep\n"


def test_only_current_source_install_target_may_lack_identity(
    tmp_path: Path,
) -> None:
    harness = GuardHarness(tmp_path)
    (harness.target / MODULE.IDENTITY_FILE_NAME).unlink()

    report = MODULE.guard_release_storage(
        **harness.kwargs(usage=FilesystemSequence(1))
    )

    assert release_id(COMMIT_TARGET, ARCHIVE_TARGET) in (
        report.protected_release_ids
    )


def test_non_target_release_without_identity_fails_closed(
    tmp_path: Path,
) -> None:
    harness = GuardHarness(tmp_path)
    release, path = harness.old_release(age_days=30)
    (path / MODULE.IDENTITY_FILE_NAME).unlink()

    with pytest.raises(
        MODULE.MissingReleaseIdentityError,
        match="release identity is missing",
    ):
        MODULE.guard_release_storage(
            **harness.kwargs(usage=FilesystemSequence(1))
        )
    assert release


def test_memory_exact_five_gib_available_passes() -> None:
    report = MODULE.validate_memory_budget(
        total_bytes=16 * GIB,
        available_bytes=5 * GIB,
        active_bytes=1 * GIB,
        active_memory_high_bytes=6 * GIB,
        active_memory_max_bytes=8 * GIB,
        expected_active_memory_high_bytes=6 * GIB,
        expected_active_memory_max_bytes=8 * GIB,
        minimum_total_bytes=15 * GIB,
        minimum_available_bytes=5 * GIB,
        candidate_max_bytes=4 * GIB,
        os_reserve_bytes=2 * GIB,
    )

    assert report.available_bytes == 5 * GIB
    assert report.total_bytes == 16 * GIB


def test_memory_one_byte_below_five_gib_fails() -> None:
    with pytest.raises(
        MODULE.StorageGuardError,
        match="available RAM is below the candidate-start minimum",
    ):
        MODULE.validate_memory_budget(
            total_bytes=16 * GIB,
            available_bytes=5 * GIB - 1,
            active_bytes=1 * GIB,
            active_memory_high_bytes=6 * GIB,
            active_memory_max_bytes=8 * GIB,
            expected_active_memory_high_bytes=6 * GIB,
            expected_active_memory_max_bytes=8 * GIB,
            minimum_total_bytes=15 * GIB,
            minimum_available_bytes=5 * GIB,
            candidate_max_bytes=4 * GIB,
            os_reserve_bytes=2 * GIB,
        )


def test_memory_active_plus_candidate_cannot_consume_os_reserve() -> None:
    with pytest.raises(
        MODULE.StorageGuardError,
        match="consume the OS reserve",
    ):
        MODULE.validate_memory_budget(
            total_bytes=16 * GIB,
            available_bytes=5 * GIB,
            active_bytes=11 * GIB + 1,
            active_memory_high_bytes=6 * GIB,
            active_memory_max_bytes=8 * GIB,
            expected_active_memory_high_bytes=6 * GIB,
            expected_active_memory_max_bytes=8 * GIB,
            minimum_total_bytes=15 * GIB,
            minimum_available_bytes=5 * GIB,
            candidate_max_bytes=3 * GIB,
            os_reserve_bytes=2 * GIB,
        )


def test_memory_cli_reads_meminfo_and_active_cgroup(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    meminfo = tmp_path / "meminfo"
    meminfo.write_text(
        "MemTotal:       16777216 kB\n"
        "MemAvailable:    5242880 kB\n",
        encoding="utf-8",
    )
    commands: list[list[str]] = []

    def memory_property(command: list[str], *, text: bool) -> str:
        assert text is True
        commands.append(command)
        property_name = command[-2]
        return {
            "MemoryCurrent": str(GIB),
            "MemoryHigh": str(6 * GIB),
            "MemoryMax": str(8 * GIB),
        }[property_name]

    monkeypatch.setattr(MODULE.subprocess, "check_output", memory_property)

    result = MODULE.main(
        [
            "memory",
            "--active-service",
            "jato-fullstack-backend@8000",
            "--meminfo-path",
            str(meminfo),
            "--expected-active-memory-high-bytes",
            str(6 * GIB),
            "--expected-active-memory-max-bytes",
            str(8 * GIB),
            "--minimum-total-bytes",
            str(14 * GIB),
            "--minimum-available-bytes",
            str(5 * GIB),
            "--candidate-max-bytes",
            str(4 * GIB),
            "--os-reserve-bytes",
            str(2 * GIB),
        ]
    )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["total_bytes"] == 16 * GIB
    assert payload["available_bytes"] == 5 * GIB
    assert payload["active_bytes"] == GIB
    assert payload["active_memory_high_bytes"] == 6 * GIB
    assert payload["active_memory_max_bytes"] == 8 * GIB
    assert commands == [
        [
            "systemctl",
            "show",
            "jato-fullstack-backend@8000",
            "-p",
            "MemoryCurrent",
            "--value",
        ],
        [
            "systemctl",
            "show",
            "jato-fullstack-backend@8000",
            "-p",
            "MemoryHigh",
            "--value",
        ],
        [
            "systemctl",
            "show",
            "jato-fullstack-backend@8000",
            "-p",
            "MemoryMax",
            "--value",
        ],
    ]


@pytest.mark.parametrize(
    ("property_name", "property_value", "error_pattern"),
    (
        ("MemoryHigh", "infinity", "MemoryHigh is invalid"),
        ("MemoryHigh", str(7 * GIB), "MemoryHigh differs"),
        ("MemoryMax", "infinity", "MemoryMax is invalid"),
        ("MemoryMax", str(9 * GIB), "MemoryMax differs"),
    ),
)
def test_memory_cli_rejects_active_cgroup_limit_drift(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    property_name: str,
    property_value: str,
    error_pattern: str,
) -> None:
    meminfo = tmp_path / "meminfo"
    meminfo.write_text(
        "MemTotal:       16777216 kB\n"
        "MemAvailable:    9437184 kB\n",
        encoding="utf-8",
    )

    def memory_property(command: list[str], *, text: bool) -> str:
        assert text is True
        current_property = command[-2]
        if current_property == property_name:
            return property_value
        return {
            "MemoryCurrent": str(GIB),
            "MemoryHigh": str(6 * GIB),
            "MemoryMax": str(8 * GIB),
        }[current_property]

    monkeypatch.setattr(MODULE.subprocess, "check_output", memory_property)

    result = MODULE.main(
        [
            "memory",
            "--active-service",
            "jato-fullstack-backend@8000",
            "--meminfo-path",
            str(meminfo),
            "--expected-active-memory-high-bytes",
            str(6 * GIB),
            "--expected-active-memory-max-bytes",
            str(8 * GIB),
            "--minimum-total-bytes",
            str(14 * GIB),
            "--minimum-available-bytes",
            str(5 * GIB),
            "--candidate-max-bytes",
            str(4 * GIB),
            "--os-reserve-bytes",
            str(2 * GIB),
        ]
    )

    assert result == 1
    assert error_pattern in capsys.readouterr().err
