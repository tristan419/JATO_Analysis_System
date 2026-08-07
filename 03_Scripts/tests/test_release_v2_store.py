from __future__ import annotations

import importlib.util
import fcntl
import json
import os
from pathlib import Path
import stat
import sys

import pytest


ROOT = Path(__file__).resolve().parents[2]
HELPER = ROOT / "03_Scripts/deploy/release_v2_store.py"
SPEC = importlib.util.spec_from_file_location("release_v2_store", HELPER)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)

ACTIVE = MODULE.ReleaseIdentity("a" * 40, "1" * 64)
PREVIOUS = MODULE.ReleaseIdentity("b" * 40, "2" * 64)
CANDIDATE = MODULE.ReleaseIdentity("c" * 40, "3" * 64)
UNREFERENCED = MODULE.ReleaseIdentity("d" * 40, "4" * 64)


def make_layout(tmp_path: Path) -> MODULE.ReleaseLayout:
    layout = MODULE.ReleaseLayout(
        release_root=tmp_path / "releases",
        slots_root=tmp_path / "slots",
        expected_owner_uid=os.getuid(),
    )
    layout.release_root.mkdir()
    for slot in MODULE.SLOTS:
        (layout.slots_root / slot).mkdir(parents=True)
    return layout


def make_release(
    layout: MODULE.ReleaseLayout,
    identity: MODULE.ReleaseIdentity,
) -> Path:
    release = layout.release_path(identity)
    release.mkdir(parents=True)
    (release / "release-v2-manifest.json").write_bytes(
        MODULE.canonical_manifest_bytes(valid_manifest(identity))
    )
    return release


def make_cached_archive(
    cache_root: Path,
    identity: MODULE.ReleaseIdentity,
) -> dict[str, Path]:
    identity_root = cache_root / identity.commit_sha
    identity_root.mkdir(parents=True, exist_ok=True)
    basename = f"{identity.archive_sha256}.tar.gz"
    paths = {
        "final": identity_root / basename,
        "partial": identity_root / f"{basename}.partial",
        "checksum": identity_root / f"{basename}.sha256",
        "lock": identity_root / f"{basename}.lock",
    }
    paths["final"].write_bytes(b"archive")
    paths["partial"].write_bytes(b"partial")
    paths["checksum"].write_text(f"{identity.archive_sha256}\n", encoding="utf-8")
    paths["lock"].write_bytes(b"")
    return paths


def test_pointer_namespace_requires_expected_owner_and_safe_mode(
    tmp_path: Path,
) -> None:
    layout = make_layout(tmp_path)
    make_release(layout, ACTIVE)
    MODULE.atomic_symlink(layout, "8000", "current", ACTIVE)

    wrong_owner = MODULE.ReleaseLayout(
        release_root=layout.release_root,
        slots_root=layout.slots_root,
        expected_owner_uid=os.getuid() + 1,
    )
    with pytest.raises(MODULE.ReleaseStoreError) as caught:
        MODULE.read_pointer(wrong_owner, "8000", "current")
    assert caught.value.code == "pointer_parent_unsafe"

    (layout.slots_root / "8000").chmod(0o777)
    with pytest.raises(MODULE.ReleaseStoreError) as caught:
        MODULE.read_pointer(layout, "8000", "current")
    assert caught.value.code == "pointer_parent_unsafe"


def valid_manifest(
    identity: MODULE.ReleaseIdentity = ACTIVE,
) -> MODULE.ReleaseManifest:
    return MODULE.ReleaseManifest(
        repository="tristan419/JATO_Analysis_System",
        identity=identity,
        archive_bytes=22_000_000,
        frontend_artifact_identity=f"frontend-dist-{ACTIVE.commit_sha}",
        frontend_artifact_checksum="5" * 64,
        frontend_build_id="6" * 64,
        build_metadata_sha256="7" * 64,
    )


def test_manifest_round_trip_is_canonical_and_retry_stable() -> None:
    manifest = valid_manifest()
    payload = MODULE.canonical_manifest_bytes(manifest)
    digest = MODULE.manifest_sha256(payload)

    parsed = MODULE.parse_manifest_bytes(payload, expected_sha256=digest)

    assert parsed == manifest
    assert payload.endswith(b"\n")
    assert b"runId" not in payload
    assert b"runAttempt" not in payload
    assert MODULE.canonical_manifest_bytes(parsed) == payload


def test_manifest_rejects_unknown_execution_metadata_and_noncanonical_json() -> None:
    manifest = valid_manifest()
    parsed = manifest.to_dict()
    parsed["github"] = {"runId": "123", "runAttempt": 2}
    with pytest.raises(MODULE.ReleaseStoreError) as unknown:
        MODULE.parse_manifest_bytes(
            (json.dumps(parsed, sort_keys=True, separators=(",", ":")) + "\n").encode(),
        )
    assert unknown.value.code == "manifest_shape_invalid"

    pretty = (json.dumps(manifest.to_dict(), indent=2, sort_keys=True) + "\n").encode()
    with pytest.raises(MODULE.ReleaseStoreError) as noncanonical:
        MODULE.parse_manifest_bytes(pretty)
    assert noncanonical.value.code == "manifest_not_canonical"


def test_manifest_file_rejects_symlink_and_group_writable_file(tmp_path: Path) -> None:
    payload = MODULE.canonical_manifest_bytes(valid_manifest())
    digest = MODULE.manifest_sha256(payload)
    target = tmp_path / "manifest.json"
    target.write_bytes(payload)
    link = tmp_path / "manifest-link.json"
    link.symlink_to(target)

    with pytest.raises(MODULE.ReleaseStoreError) as symlink_error:
        MODULE.read_manifest_file(link, expected_sha256=digest)
    assert symlink_error.value.code == "manifest_path_unsafe"

    target.chmod(0o664)
    with pytest.raises(MODULE.ReleaseStoreError) as mode_error:
        MODULE.read_manifest_file(target, expected_sha256=digest)
    assert mode_error.value.code == "manifest_permissions_unsafe"
    assert stat.S_IMODE(target.stat().st_mode) == 0o664


def test_atomic_pointer_uses_single_slots_namespace(tmp_path: Path) -> None:
    layout = make_layout(tmp_path)
    make_release(layout, ACTIVE)

    MODULE.atomic_symlink(layout, "8000", "current", ACTIVE)

    pointer = layout.pointer_path("8000", "current")
    assert pointer.is_symlink()
    assert not Path(pointer.readlink()).is_absolute()
    assert MODULE.read_pointer(layout, "8000", "current") == ACTIVE
    assert MODULE.read_pointer_pair(layout, "8000") == MODULE.PointerPair(
        current=ACTIVE,
        previous=None,
    )


def test_pointer_operations_refuse_regular_and_out_of_store_paths(
    tmp_path: Path,
) -> None:
    layout = make_layout(tmp_path)
    make_release(layout, ACTIVE)
    pointer = layout.pointer_path("8001", "current")
    pointer.write_text("do not overwrite\n", encoding="utf-8")

    with pytest.raises(MODULE.ReleaseStoreError) as regular_error:
        MODULE.atomic_symlink(layout, "8001", "current", ACTIVE)
    assert regular_error.value.code == "pointer_path_unsafe"
    with pytest.raises(MODULE.ReleaseStoreError) as clear_error:
        MODULE.clear_pointer(layout, "8001", "current")
    assert clear_error.value.code == "pointer_path_unsafe"
    assert pointer.read_text(encoding="utf-8") == "do not overwrite\n"

    pointer.unlink()
    outside = tmp_path / "outside"
    outside.mkdir()
    pointer.symlink_to(outside)
    with pytest.raises(MODULE.ReleaseStoreError) as outside_error:
        MODULE.read_pointer(layout, "8001", "current")
    assert outside_error.value.code == "pointer_target_outside_store"


def test_clear_pointer_is_idempotent_and_never_deletes_release(tmp_path: Path) -> None:
    layout = make_layout(tmp_path)
    release = make_release(layout, CANDIDATE)
    MODULE.atomic_symlink(layout, "8001", "current", CANDIDATE)

    assert MODULE.clear_pointer(layout, "8001", "current") is True
    assert MODULE.clear_pointer(layout, "8001", "current") is False
    assert release.is_dir()


def test_pointer_rejects_exact_legacy_directory_without_v2_manifest(
    tmp_path: Path,
) -> None:
    layout = make_layout(tmp_path)
    legacy = layout.release_path(UNREFERENCED)
    legacy.mkdir(parents=True)
    pointer = layout.pointer_path("8000", "current")
    pointer.symlink_to(legacy)

    with pytest.raises(MODULE.ReleaseStoreError) as caught:
        MODULE.read_pointer(layout, "8000", "current")

    assert caught.value.code == "artifact_unreadable"


def test_gc_plan_protects_four_pointers_and_additional_inflight_release(
    tmp_path: Path,
) -> None:
    layout = make_layout(tmp_path)
    for identity in (ACTIVE, PREVIOUS, CANDIDATE, UNREFERENCED):
        make_release(layout, identity)
    MODULE.atomic_symlink(layout, "8000", "current", ACTIVE)
    MODULE.atomic_symlink(layout, "8000", "previous", PREVIOUS)
    MODULE.atomic_symlink(layout, "8001", "current", CANDIDATE)
    MODULE.atomic_symlink(layout, "8001", "previous", PREVIOUS)

    plan = MODULE.plan_garbage_collection(
        layout,
        additional_protected=(UNREFERENCED,),
    )

    assert plan.protected == tuple(sorted((ACTIVE, PREVIOUS, CANDIDATE, UNREFERENCED)))
    assert plan.removable == ()
    assert plan.diagnostics == ()
    assert all(layout.release_path(identity).is_dir() for identity in plan.protected)

    plan_without_inflight = MODULE.plan_garbage_collection(layout)
    assert plan_without_inflight.removable == (UNREFERENCED,)
    assert layout.release_path(UNREFERENCED).is_dir()


def test_store_scan_reports_unexpected_entries_without_following_symlinks(
    tmp_path: Path,
) -> None:
    layout = make_layout(tmp_path)
    make_release(layout, ACTIVE)
    (layout.release_root / "not-a-commit").mkdir()
    unsafe_commit = layout.release_root / ("e" * 40)
    unsafe_commit.symlink_to(layout.release_path(ACTIVE).parent)

    scan = MODULE.scan_release_store(layout)

    assert scan.releases == (ACTIVE,)
    assert {item["code"] for item in scan.diagnostics} == {
        "unexpected_release_root_entry",
        "unsafe_release_commit_entry",
    }


def test_gc_deletes_only_release_not_held_by_four_pointers(tmp_path: Path) -> None:
    layout = make_layout(tmp_path)
    for identity in (ACTIVE, PREVIOUS, CANDIDATE, UNREFERENCED):
        make_release(layout, identity)
    MODULE.atomic_symlink(layout, "8000", "current", ACTIVE)
    MODULE.atomic_symlink(layout, "8000", "previous", PREVIOUS)
    MODULE.atomic_symlink(layout, "8001", "current", CANDIDATE)

    removed = MODULE.collect_garbage(layout)

    assert removed == (UNREFERENCED,)
    assert not layout.release_path(UNREFERENCED).exists()
    assert all(
        layout.release_path(identity).is_dir()
        for identity in (ACTIVE, PREVIOUS, CANDIDATE)
    )


def test_gc_refuses_unexpected_store_entries_without_deleting(tmp_path: Path) -> None:
    layout = make_layout(tmp_path)
    release = make_release(layout, UNREFERENCED)
    (layout.release_root / "unexpected").mkdir()

    with pytest.raises(MODULE.ReleaseStoreError) as caught:
        MODULE.collect_garbage(layout)

    assert caught.value.code == "gc_store_ambiguous"
    assert release.is_dir()


def test_gc_preserves_unmanaged_legacy_while_collecting_valid_v2(
    tmp_path: Path,
) -> None:
    layout = make_layout(tmp_path)
    legacy = layout.release_path(UNREFERENCED)
    legacy.mkdir(parents=True)
    (legacy / "legacy.txt").write_text("keep\n", encoding="utf-8")
    valid_v2 = make_release(layout, ACTIVE)

    scan = MODULE.scan_release_store(layout)
    removed = MODULE.collect_garbage(layout)

    assert scan.releases == (ACTIVE,)
    assert scan.preserved_unmanaged == (UNREFERENCED,)
    assert scan.diagnostics == ()
    assert removed == (ACTIVE,)
    assert legacy.is_dir()
    assert not valid_v2.exists()


@pytest.mark.parametrize("manifest_state", ("symlink", "damaged", "mismatch"))
def test_gc_refuses_existing_unsafe_or_invalid_v2_manifest(
    tmp_path: Path,
    manifest_state: str,
) -> None:
    layout = make_layout(tmp_path)
    release = make_release(layout, UNREFERENCED)
    manifest_path = release / "release-v2-manifest.json"
    if manifest_state == "symlink":
        payload = manifest_path.read_bytes()
        target = tmp_path / "manifest-target.json"
        target.write_bytes(payload)
        manifest_path.unlink()
        manifest_path.symlink_to(target)
    elif manifest_state == "damaged":
        manifest_path.write_text("{}\n", encoding="utf-8")
    else:
        manifest_path.write_bytes(
            MODULE.canonical_manifest_bytes(valid_manifest(ACTIVE))
        )

    scan = MODULE.scan_release_store(layout)

    assert scan.releases == ()
    assert scan.preserved_unmanaged == ()
    assert len(scan.diagnostics) == 1
    with pytest.raises(MODULE.ReleaseStoreError) as caught:
        MODULE.collect_garbage(layout)
    assert caught.value.code == "gc_store_ambiguous"
    assert release.exists()


def test_remove_if_unreferenced_refuses_pointer_held_release(tmp_path: Path) -> None:
    layout = make_layout(tmp_path)
    release = make_release(layout, CANDIDATE)
    MODULE.atomic_symlink(layout, "8001", "current", CANDIDATE)

    assert MODULE.remove_if_unreferenced(layout, CANDIDATE) is False
    assert release.is_dir()
    assert MODULE.read_pointer(layout, "8001", "current") == CANDIDATE


def test_remove_if_unreferenced_deletes_only_manifest_proven_release(
    tmp_path: Path,
) -> None:
    layout = make_layout(tmp_path)
    release = make_release(layout, UNREFERENCED)
    unmanaged = layout.release_path(PREVIOUS)
    unmanaged.mkdir(parents=True)

    assert MODULE.remove_if_unreferenced(layout, UNREFERENCED) is True
    assert not release.exists()
    assert MODULE.remove_if_unreferenced(layout, UNREFERENCED) is False
    with pytest.raises(MODULE.ReleaseStoreError) as caught:
        MODULE.remove_if_unreferenced(layout, PREVIOUS)
    assert caught.value.code == "artifact_unreadable"
    assert unmanaged.is_dir()


def test_archive_cache_gc_protects_four_pointers_and_removes_only_data(
    tmp_path: Path,
) -> None:
    layout = make_layout(tmp_path)
    for identity in (ACTIVE, PREVIOUS, CANDIDATE):
        make_release(layout, identity)
    MODULE.atomic_symlink(layout, "8000", "current", ACTIVE)
    MODULE.atomic_symlink(layout, "8000", "previous", PREVIOUS)
    MODULE.atomic_symlink(layout, "8001", "current", CANDIDATE)
    cache_root = tmp_path / "archive-cache"
    protected_paths = make_cached_archive(cache_root, ACTIVE)
    removed_paths = make_cached_archive(cache_root, UNREFERENCED)

    result = MODULE.collect_archive_cache(layout, cache_root)

    assert result.protected == tuple(sorted((ACTIVE, PREVIOUS, CANDIDATE)))
    assert result.removed == (UNREFERENCED,)
    assert len(result.removed_paths) == 3
    assert result.diagnostics == ()
    assert all(path.exists() for path in protected_paths.values())
    assert removed_paths["lock"].is_file()
    assert all(
        not removed_paths[kind].exists()
        for kind in ("final", "partial", "checksum")
    )


def test_archive_cache_gc_skips_held_lock_then_retries_idempotently(
    tmp_path: Path,
) -> None:
    layout = make_layout(tmp_path)
    cache_root = tmp_path / "archive-cache"
    paths = make_cached_archive(cache_root, UNREFERENCED)
    descriptor = os.open(paths["lock"], os.O_RDWR)
    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        held = MODULE.collect_archive_cache(layout, cache_root)
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)

    assert held.removed == ()
    assert {item["code"] for item in held.diagnostics} == {
        "archive_cache_lock_busy"
    }
    assert paths["final"].is_file()

    removed = MODULE.collect_archive_cache(layout, cache_root)
    repeated = MODULE.collect_archive_cache(layout, cache_root)

    assert removed.removed == (UNREFERENCED,)
    assert repeated.removed == ()
    assert repeated.removed_paths == ()
    assert repeated.diagnostics == ()
    assert paths["lock"].is_file()


def test_archive_cache_gc_preserves_malformed_and_symlink_entries(
    tmp_path: Path,
) -> None:
    layout = make_layout(tmp_path)
    cache_root = tmp_path / "archive-cache"
    valid_paths = make_cached_archive(cache_root, UNREFERENCED)
    malformed = cache_root / "not-a-commit"
    malformed.mkdir()
    unsafe_root = cache_root / ACTIVE.commit_sha
    unsafe_root.mkdir()
    target = tmp_path / "outside-archive"
    target.write_bytes(b"outside")
    unsafe_final = unsafe_root / f"{ACTIVE.archive_sha256}.tar.gz"
    unsafe_final.symlink_to(target)
    unsafe_lock = unsafe_root / f"{ACTIVE.archive_sha256}.tar.gz.lock"
    unsafe_lock.write_bytes(b"")
    (unsafe_root / "unmanaged.txt").write_text("keep\n", encoding="utf-8")

    result = MODULE.collect_archive_cache(layout, cache_root)

    assert result.removed == (UNREFERENCED,)
    assert valid_paths["lock"].is_file()
    assert unsafe_final.is_symlink()
    assert target.read_bytes() == b"outside"
    assert malformed.is_dir()
    assert {item["code"] for item in result.diagnostics} == {
        "archive_cache_entry_unsafe",
        "archive_cache_path_unmanaged",
    }


def test_promote_staged_release_is_atomic_and_idempotent(tmp_path: Path) -> None:
    layout = make_layout(tmp_path)
    staging_root = tmp_path / "staging"
    staging_root.mkdir()
    staging = staging_root / "candidate-1"
    staging.mkdir()
    payload = MODULE.canonical_manifest_bytes(valid_manifest())
    digest = MODULE.manifest_sha256(payload)
    (staging / "release-v2-manifest.json").write_bytes(payload)

    assert MODULE.promote_staged_release(
        layout,
        ACTIVE,
        staging,
        expected_manifest_sha256=digest,
    ) is True
    assert not staging.exists()
    assert MODULE.read_manifest_file(
        layout.release_path(ACTIVE) / "release-v2-manifest.json",
        expected_sha256=digest,
    ) == valid_manifest()

    retry = staging_root / "candidate-2"
    retry.mkdir()
    (retry / "release-v2-manifest.json").write_bytes(payload)
    assert MODULE.promote_staged_release(
        layout,
        ACTIVE,
        retry,
        expected_manifest_sha256=digest,
    ) is False
    assert retry.is_dir()


def test_promote_staged_release_rejects_staging_inside_store(tmp_path: Path) -> None:
    layout = make_layout(tmp_path)
    staging = layout.release_root / "staging"
    staging.mkdir()
    payload = MODULE.canonical_manifest_bytes(valid_manifest())
    (staging / "release-v2-manifest.json").write_bytes(payload)

    with pytest.raises(MODULE.ReleaseStoreError) as caught:
        MODULE.promote_staged_release(
            layout,
            ACTIVE,
            staging,
            expected_manifest_sha256=MODULE.manifest_sha256(payload),
        )

    assert caught.value.code == "staging_directory_unsafe"
