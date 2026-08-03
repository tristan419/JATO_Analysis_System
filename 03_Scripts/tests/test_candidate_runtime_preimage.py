from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
HELPER = REPO_ROOT / "03_Scripts/deploy/candidate_runtime_preimage.py"
COMMIT = "a" * 40
ARCHIVE = "b" * 64
BOOT_ID = "11111111-2222-3333-4444-555555555555"


def _load_helper_module():
    specification = importlib.util.spec_from_file_location(
        "candidate_runtime_preimage_tested",
        HELPER,
    )
    assert specification is not None and specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def _layout(tmp_path: Path) -> dict[str, Path]:
    state = tmp_path / "state"
    state.mkdir(mode=0o755)
    return {
        "preimage": state / "candidate-preimages" / COMMIT / ARCHIVE,
        "slot_link": tmp_path / "slots/8001/current",
        "slot_link_stage": tmp_path
        / "slots/8001/.current.jato-candidate-installing",
        "slot_env": tmp_path / "etc/slots/8001.env",
        "slot_env_stage": tmp_path
        / "etc/slots/.8001.env.jato-candidate-installing",
        "explicit_unit": tmp_path / "etc/systemd/8001.service",
        "explicit_unit_stage": tmp_path
        / "etc/systemd/.8001.service.jato-candidate-installing",
        "instance_dropins": tmp_path / "etc/systemd/8001.service.d",
        "persistent_control_dropins": tmp_path / "etc/system.control/8001.service.d",
        "runtime_control_dropins": tmp_path / "run/system.control/8001.service.d",
        "candidate_cache_link": tmp_path / "cache/jato-candidate-8001",
        "candidate_cache_private": tmp_path
        / "cache/private/jato-candidate-8001",
        "boot_id_file": tmp_path / "boot_id",
        "post_env_source": tmp_path / "sources/8001.env",
        "post_unit_source": tmp_path / "sources/8001.service",
        "post_sandbox_source": tmp_path / "sources/10-candidate-sandbox.conf",
    }


def _arguments(layout: dict[str, Path], command: str) -> list[str]:
    arguments = [
        str(HELPER),
        command,
        "--preimage",
        str(layout["preimage"]),
        "--commit",
        COMMIT,
        "--archive-sha256",
        ARCHIVE,
        "--candidate-slot",
        "8001",
        "--boot-id-file",
        str(layout["boot_id_file"]),
        "--slot-link",
        str(layout["slot_link"]),
        "--slot-link-stage",
        str(layout["slot_link_stage"]),
        "--slot-env",
        str(layout["slot_env"]),
        "--slot-env-stage",
        str(layout["slot_env_stage"]),
        "--explicit-unit",
        str(layout["explicit_unit"]),
        "--explicit-unit-stage",
        str(layout["explicit_unit_stage"]),
        "--instance-dropins",
        str(layout["instance_dropins"]),
        "--persistent-control-dropins",
        str(layout["persistent_control_dropins"]),
        "--runtime-control-dropins",
        str(layout["runtime_control_dropins"]),
        "--candidate-cache-link",
        str(layout["candidate_cache_link"]),
        "--candidate-cache-private",
        str(layout["candidate_cache_private"]),
    ]
    if command == "capture":
        arguments.extend(
            [
                "--post-slot-link-target",
                "/opt/jato/releases/new/archive",
                "--post-env-source",
                str(layout["post_env_source"]),
                "--post-unit-source",
                str(layout["post_unit_source"]),
                "--post-sandbox-source",
                str(layout["post_sandbox_source"]),
                "--post-memory-high-bytes",
                str(3 * 1024**3),
                "--post-memory-max-bytes",
                str(4 * 1024**3),
                "--post-cpu-quota-percent",
                "100",
                "--post-active-memory-high-bytes",
                str(6 * 1024**3),
                "--post-active-memory-max-bytes",
                str(8 * 1024**3),
                "--post-active-cpu-quota-percent",
                "200",
                "--candidate-cache-max-bytes",
                "16",
            ]
        )
    return arguments


def _run(
    layout: dict[str, Path], command: str
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["python3", *_arguments(layout, command)],
        text=True,
        capture_output=True,
        check=False,
    )


def _identity_layout(
    layout: dict[str, Path],
    commit: str,
    archive: str,
) -> dict[str, Path]:
    result = dict(layout)
    result["preimage"] = (
        layout["preimage"].parents[2]
        / "candidate-preimages"
        / commit
        / archive
    )
    return result


def _run_identity(
    layout: dict[str, Path],
    command: str,
    commit: str,
    archive: str,
) -> subprocess.CompletedProcess[str]:
    arguments = _arguments(layout, command)
    arguments[arguments.index(COMMIT)] = commit
    arguments[arguments.index(ARCHIVE)] = archive
    return subprocess.run(
        ["python3", *arguments],
        text=True,
        capture_output=True,
        check=False,
    )


def _seed(layout: dict[str, Path]) -> None:
    layout["boot_id_file"].write_text(f"{BOOT_ID}\n", encoding="ascii")
    layout["post_env_source"].parent.mkdir(parents=True)
    layout["post_env_source"].write_text("APP_RELEASE_SHA=new\n", encoding="utf-8")
    layout["post_unit_source"].write_text("[Service]\nExecStart=new\n", encoding="utf-8")
    layout["post_sandbox_source"].write_text(
        "[Service]\nProtectSystem=strict\n", encoding="utf-8"
    )
    layout["slot_link"].parent.mkdir(parents=True)
    layout["slot_link"].symlink_to("/opt/jato/releases/old/archive")
    layout["slot_env"].parent.mkdir(parents=True)
    layout["slot_env"].write_text("APP_RELEASE_SHA=old\n", encoding="utf-8")
    os.chmod(layout["slot_env"], 0o600)
    layout["explicit_unit"].parent.mkdir(parents=True)
    layout["explicit_unit"].write_text("[Service]\n", encoding="utf-8")
    os.chmod(layout["explicit_unit"], 0o644)
    layout["instance_dropins"].mkdir()
    (layout["instance_dropins"] / "old.conf").write_text(
        "[Service]\nPrivateTmp=yes\n", encoding="utf-8"
    )
    layout["persistent_control_dropins"].mkdir(parents=True)
    (layout["persistent_control_dropins"] / "50-MemoryMax.conf").write_text(
        "[Service]\nMemoryMax=4G\n", encoding="utf-8"
    )
    # Runtime control drop-ins and both cache paths intentionally start absent.


def _manifest(layout: dict[str, Path]) -> dict[str, object]:
    return json.loads(
        (layout["preimage"] / "manifest.json").read_text(encoding="utf-8")
    )


def _owner_path(layout: dict[str, Path]) -> Path:
    return layout["preimage"].parents[2] / "slot-owners/8001.json"


def _module_restore_context(module, layout: dict[str, Path]):
    arguments = module._parser().parse_args(_arguments(layout, "restore")[1:])
    identity = module._identity(arguments)
    roles = module._role_paths(arguments)
    preimage = module._preimage_path(arguments, identity)
    boot_id = module._boot_id(arguments)
    module._require_current_slot_owner(preimage, identity)
    manifest = module._load_preimage(preimage, identity, roles)
    return preimage, manifest, roles, boot_id


def test_capture_restore_covers_present_absent_file_symlink_and_trees(
    tmp_path: Path,
) -> None:
    layout = _layout(tmp_path)
    _seed(layout)
    expected_env = layout["slot_env"].read_bytes()
    expected_link = os.readlink(layout["slot_link"])

    captured = _run(layout, "capture")
    assert captured.returncode == 0, captured.stderr
    assert json.loads(captured.stdout)["decision"] == "captured"

    layout["slot_link"].unlink()
    layout["slot_link"].symlink_to("/opt/jato/releases/new/archive")
    shutil.copyfile(layout["post_env_source"], layout["slot_env"])
    os.chmod(layout["slot_env"], 0o600)
    shutil.copyfile(
        layout["post_sandbox_source"],
        layout["instance_dropins"] / "10-candidate-sandbox.conf",
    )
    os.chmod(layout["instance_dropins"] / "10-candidate-sandbox.conf", 0o644)
    layout["runtime_control_dropins"].mkdir(parents=True)
    (layout["runtime_control_dropins"] / "50-CPUQuota.conf").write_text(
        "# generated\n[Service]\nCPUQuota=100%\n", encoding="utf-8"
    )
    os.chmod(layout["runtime_control_dropins"] / "50-CPUQuota.conf", 0o644)
    layout["candidate_cache_private"].mkdir(parents=True)
    (layout["candidate_cache_private"] / "cache.bin").write_bytes(b"cache")
    layout["candidate_cache_link"].symlink_to(
        "private/jato-candidate-8001"
    )

    restored = _run(layout, "restore")
    assert restored.returncode == 0, restored.stderr
    assert layout["slot_env"].read_bytes() == expected_env
    assert os.readlink(layout["slot_link"]) == expected_link
    assert not (layout["instance_dropins"] / "new.conf").exists()
    assert (layout["instance_dropins"] / "old.conf").is_file()
    assert (
        layout["persistent_control_dropins"] / "50-MemoryMax.conf"
    ).is_file()
    assert not layout["runtime_control_dropins"].exists()
    assert not layout["candidate_cache_link"].exists()
    assert not layout["candidate_cache_private"].exists()
    verified = _run(layout, "verify-live")
    assert verified.returncode == 0, verified.stderr


def test_restore_rejects_unexpected_dropin_before_deleting_any_role(
    tmp_path: Path,
) -> None:
    layout = _layout(tmp_path)
    _seed(layout)
    assert _run(layout, "capture").returncode == 0
    original_env = layout["slot_env"].read_bytes()
    shutil.copyfile(layout["post_env_source"], layout["slot_env"])
    os.chmod(layout["slot_env"], 0o600)
    (layout["instance_dropins"] / "unexpected.conf").write_text(
        "[Service]\nExecStart=/bin/false\n", encoding="utf-8"
    )

    rejected = _run(layout, "restore")
    assert rejected.returncode == 2
    # Preflight covers every role before the first rename/unlink.  The exact
    # release env therefore remains in place instead of being partially
    # restored when a later drop-in is unowned.
    assert layout["slot_env"].read_bytes() == layout["post_env_source"].read_bytes()
    assert layout["slot_env"].read_bytes() != original_env
    assert (layout["instance_dropins"] / "unexpected.conf").is_file()


def test_restore_rejects_oversized_cache_before_deleting_any_role(
    tmp_path: Path,
) -> None:
    layout = _layout(tmp_path)
    _seed(layout)
    assert _run(layout, "capture").returncode == 0
    shutil.copyfile(layout["post_env_source"], layout["slot_env"])
    os.chmod(layout["slot_env"], 0o600)
    layout["candidate_cache_private"].mkdir(parents=True)
    (layout["candidate_cache_private"] / "oversized.bin").write_bytes(b"x" * 17)

    rejected = _run(layout, "restore")
    assert rejected.returncode == 2
    assert layout["slot_env"].read_bytes() == layout["post_env_source"].read_bytes()
    assert (layout["candidate_cache_private"] / "oversized.bin").is_file()


def test_capture_rejects_oversized_existing_cache(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _seed(layout)
    layout["candidate_cache_private"].mkdir(parents=True)
    (layout["candidate_cache_private"] / "oversized.bin").write_bytes(b"x" * 17)

    rejected = _run(layout, "capture")
    assert rejected.returncode == 2
    assert "cache exceeds the safe preimage contract" in rejected.stderr
    assert not layout["preimage"].exists()


def test_restore_requires_dynamic_user_cache_link_to_exact_private_backing(
    tmp_path: Path,
) -> None:
    layout = _layout(tmp_path)
    _seed(layout)
    assert _run(layout, "capture").returncode == 0
    shutil.copyfile(layout["post_env_source"], layout["slot_env"])
    os.chmod(layout["slot_env"], 0o600)
    layout["candidate_cache_private"].mkdir(parents=True)
    layout["candidate_cache_link"].symlink_to("private/wrong-slot")

    rejected = _run(layout, "restore")

    assert rejected.returncode == 2
    assert layout["slot_env"].read_bytes() == layout["post_env_source"].read_bytes()
    assert layout["candidate_cache_link"].is_symlink()
    assert layout["candidate_cache_private"].is_dir()


def test_capture_does_not_repermission_existing_preimage_parent(
    tmp_path: Path,
) -> None:
    layout = _layout(tmp_path)
    _seed(layout)
    os.chmod(layout["preimage"].parents[2], 0o750)

    captured = _run(layout, "capture")
    assert captured.returncode == 0, captured.stderr
    assert stat.S_IMODE(layout["preimage"].parents[2].stat().st_mode) == 0o750


def test_capture_rejects_group_writable_preimage_parent_without_chmod(
    tmp_path: Path,
) -> None:
    layout = _layout(tmp_path)
    _seed(layout)
    os.chmod(layout["preimage"].parents[2], 0o770)

    rejected = _run(layout, "capture")
    assert rejected.returncode == 2
    assert "owner-controlled directory" in rejected.stderr
    assert stat.S_IMODE(layout["preimage"].parents[2].stat().st_mode) == 0o770


def test_capture_reuses_only_when_live_state_matches(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _seed(layout)
    first = _run(layout, "capture")
    manifest_bytes = (layout["preimage"] / "manifest.json").read_bytes()
    second = _run(layout, "capture")

    assert first.returncode == 0, first.stderr
    assert second.returncode == 0, second.stderr
    assert json.loads(second.stdout)["decision"] == "reused"
    assert (layout["preimage"] / "manifest.json").read_bytes() == manifest_bytes

    layout["slot_env"].write_text("drift\n", encoding="utf-8")
    rejected = _run(layout, "capture")
    assert rejected.returncode == 2
    assert "differs from captured preimage" in rejected.stderr


@pytest.mark.parametrize("tamper", ("manifest", "payload", "path"))
def test_tampered_preimage_or_path_binding_is_rejected(
    tmp_path: Path, tamper: str
) -> None:
    layout = _layout(tmp_path)
    _seed(layout)
    assert _run(layout, "capture").returncode == 0
    if tamper == "manifest":
        payload = _manifest(layout)
        payload["identity"]["commit"] = "c" * 40
        (layout["preimage"] / "manifest.json").write_text(
            json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8"
        )
    elif tamper == "payload":
        (layout["preimage"] / "payload/slot_env").write_text(
            "tampered\n", encoding="utf-8"
        )
    else:
        layout["slot_env"] = tmp_path / "different/8001.env"

    result = _run(layout, "restore")
    assert result.returncode == 2


def test_restore_replays_after_partial_temporary_state(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _seed(layout)
    assert _run(layout, "capture").returncode == 0
    token = COMMIT[:12]
    old = layout["slot_env"].parent / (
        f".{layout['slot_env'].name}.jato-{token}-slot_env.old"
    )
    new = layout["slot_env"].parent / (
        f".{layout['slot_env'].name}.jato-{token}-slot_env.new"
    )
    layout["slot_env"].rename(old)
    shutil.copy2(layout["preimage"] / "payload/slot_env", new)

    result = _run(layout, "restore")
    assert result.returncode == 0, result.stderr
    assert layout["slot_env"].read_text(encoding="utf-8") == "APP_RELEASE_SHA=old\n"
    assert not old.exists()
    assert not new.exists()


@pytest.mark.parametrize(
    "fault",
    (
        "intent_temp_write",
        "intent_rename",
        "intent_parent_fsync",
        "new_delete",
        "stale_old_delete",
        "copy",
        "copy_parent_fsync",
        "target_to_old_rename",
        "target_to_old_parent_fsync",
        "new_to_target_rename",
        "new_to_target_parent_fsync",
        "final_old_delete",
        "final_old_parent_fsync",
    ),
)
def test_restore_retries_every_durable_intent_and_replacement_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    module = _load_helper_module()
    layout = _layout(tmp_path)
    _seed(layout)
    assert _run(layout, "capture").returncode == 0
    shutil.copyfile(layout["post_env_source"], layout["slot_env"])
    os.chmod(layout["slot_env"], 0o600)
    preimage, manifest, roles, boot_id = _module_restore_context(module, layout)
    token = COMMIT[:12]
    target = layout["slot_env"]
    new, old = module._temporary_paths(target, "slot_env", token)
    intent, intent_temporary = module._restore_intent_paths(preimage)
    if fault == "new_delete":
        new.write_bytes((preimage / "payload/slot_env").read_bytes()[:7])
        os.chmod(new, 0o600)
    if fault == "stale_old_delete":
        shutil.copyfile(layout["post_env_source"], old)
        os.chmod(old, 0o600)

    class InjectedCrash(BaseException):
        pass

    phase = ""
    original_write_manifest = module._write_manifest
    original_remove_node = module._remove_node
    original_copy_semantic = module._copy_semantic
    original_replace = module.os.replace
    original_fsync_directory = module._fsync_directory

    def interrupted_write_manifest(path: Path, payload) -> None:
        original_write_manifest(path, payload)
        if fault == "intent_temp_write" and path == intent_temporary:
            raise InjectedCrash

    def interrupted_remove_node(path: Path) -> None:
        nonlocal phase
        original_remove_node(path)
        if path == new and fault == "new_delete":
            raise InjectedCrash
        if path == old:
            if fault == "stale_old_delete":
                raise InjectedCrash
            if fault == "final_old_delete":
                raise InjectedCrash
            phase = "final_old_delete"

    def interrupted_copy_semantic(
        source: Path,
        destination: Path,
        *,
        records=None,
    ) -> None:
        nonlocal phase
        original_copy_semantic(source, destination, records=records)
        if destination == new:
            if fault == "copy":
                raise InjectedCrash
            phase = "copy"

    def interrupted_replace(source, destination) -> None:
        nonlocal phase
        original_replace(source, destination)
        source_path = Path(source)
        destination_path = Path(destination)
        if source_path == intent_temporary and destination_path == intent:
            if fault == "intent_rename":
                raise InjectedCrash
            phase = "intent_rename"
        elif source_path == target and destination_path == old:
            if fault == "target_to_old_rename":
                raise InjectedCrash
            phase = "target_to_old_rename"
        elif source_path == new and destination_path == target:
            if fault == "new_to_target_rename":
                raise InjectedCrash
            phase = "new_to_target_rename"

    def interrupted_fsync_directory(path: Path) -> None:
        original_fsync_directory(path)
        if fault == "intent_parent_fsync" and phase == "intent_rename":
            raise InjectedCrash
        if fault == "copy_parent_fsync" and phase == "copy" and path == target.parent:
            raise InjectedCrash
        if (
            fault == "target_to_old_parent_fsync"
            and phase == "target_to_old_rename"
            and path == target.parent
        ):
            raise InjectedCrash
        if (
            fault == "new_to_target_parent_fsync"
            and phase == "new_to_target_rename"
            and path == target.parent
        ):
            raise InjectedCrash
        if (
            fault == "final_old_parent_fsync"
            and phase == "final_old_delete"
            and path == target.parent
        ):
            raise InjectedCrash

    monkeypatch.setattr(module, "_write_manifest", interrupted_write_manifest)
    monkeypatch.setattr(module, "_remove_node", interrupted_remove_node)
    monkeypatch.setattr(module, "_copy_semantic", interrupted_copy_semantic)
    monkeypatch.setattr(module.os, "replace", interrupted_replace)
    monkeypatch.setattr(module, "_fsync_directory", interrupted_fsync_directory)

    with pytest.raises(InjectedCrash):
        module.restore(preimage, manifest, roles, boot_id)

    replay = _run(layout, "restore")
    assert replay.returncode == 0, replay.stderr
    assert target.read_text(encoding="utf-8") == "APP_RELEASE_SHA=old\n"
    assert not new.exists() and not new.is_symlink()
    assert not old.exists() and not old.is_symlink()
    assert not intent.exists()
    assert not intent_temporary.exists()
    assert not _owner_path(layout).exists()


@pytest.mark.parametrize("residue", ("old_only", "new_only", "none"))
def test_armed_restore_recovers_target_gap_with_one_or_zero_temporaries(
    tmp_path: Path,
    residue: str,
) -> None:
    module = _load_helper_module()
    layout = _layout(tmp_path)
    _seed(layout)
    assert _run(layout, "capture").returncode == 0
    shutil.copyfile(layout["post_env_source"], layout["slot_env"])
    os.chmod(layout["slot_env"], 0o600)
    preimage, manifest, roles, boot_id = _module_restore_context(module, layout)
    module._preflight_restore(
        preimage,
        manifest,
        roles,
        boot_id,
        intent_armed=False,
    )
    module._publish_restore_intent(preimage, manifest)
    token = COMMIT[:12]
    target = layout["slot_env"]
    new, old = module._temporary_paths(target, "slot_env", token)
    if residue == "old_only":
        target.rename(old)
    else:
        target.unlink()
        if residue == "new_only":
            module._copy_semantic(
                preimage / "payload/slot_env",
                new,
                records={entry["role"]: entry for entry in manifest["paths"]}[
                    "slot_env"
                ]["tree"],
            )

    replay = _run(layout, "restore")

    assert replay.returncode == 0, replay.stderr
    assert target.read_text(encoding="utf-8") == "APP_RELEASE_SHA=old\n"
    assert not new.exists()
    assert not old.exists()
    assert not _owner_path(layout).exists()


@pytest.mark.parametrize("tamper", ("target", "new", "old", "intent"))
def test_armed_restore_intent_still_rejects_foreign_or_tampered_state(
    tmp_path: Path,
    tamper: str,
) -> None:
    module = _load_helper_module()
    layout = _layout(tmp_path)
    _seed(layout)
    assert _run(layout, "capture").returncode == 0
    preimage, manifest, roles, boot_id = _module_restore_context(module, layout)
    module._preflight_restore(
        preimage,
        manifest,
        roles,
        boot_id,
        intent_armed=False,
    )
    module._publish_restore_intent(preimage, manifest)
    token = COMMIT[:12]
    target = layout["slot_env"]
    new, old = module._temporary_paths(target, "slot_env", token)
    intent, _ = module._restore_intent_paths(preimage)
    if tamper == "intent":
        payload = json.loads(intent.read_text(encoding="utf-8"))
        payload["manifestSha256"] = "0" * 64
        intent.write_text(json.dumps(payload) + "\n", encoding="utf-8")
        os.chmod(intent, 0o600)
        tampered_path = intent
    else:
        tampered_path = {"target": target, "new": new, "old": old}[tamper]
        tampered_path.write_bytes(b"foreign-state-with-wrong-prefix\n")
        os.chmod(tampered_path, 0o600)
    before = tampered_path.read_bytes()

    rejected = _run(layout, "restore")

    assert rejected.returncode == 2
    assert tampered_path.read_bytes() == before
    assert _owner_path(layout).is_file()


def test_discard_validates_payload_and_never_touches_live_paths(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _seed(layout)
    assert _run(layout, "capture").returncode == 0
    live_digest = hashlib.sha256(layout["slot_env"].read_bytes()).hexdigest()

    result = _run(layout, "discard")
    assert result.returncode == 0, result.stderr
    assert not layout["preimage"].exists()
    assert hashlib.sha256(layout["slot_env"].read_bytes()).hexdigest() == live_digest
    replay = _run(layout, "discard")
    assert replay.returncode == 0, replay.stderr
    assert json.loads(replay.stdout)["decision"] == "already-discarded"


def test_discard_replays_from_partial_tombstone(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _seed(layout)
    assert _run(layout, "capture").returncode == 0
    tombstone = layout["preimage"].parent / f".{ARCHIVE}.discarding"
    layout["preimage"].rename(tombstone)
    (tombstone / "manifest.json").unlink()

    replay = _run(layout, "discard")
    assert replay.returncode == 0, replay.stderr
    assert not tombstone.exists()


def test_foreign_release_cannot_capture_a_slot_with_an_armed_owner(
    tmp_path: Path,
) -> None:
    layout = _layout(tmp_path)
    _seed(layout)
    assert _run(layout, "capture").returncode == 0
    foreign_commit = "c" * 40
    foreign_archive = "d" * 64
    foreign = _identity_layout(layout, foreign_commit, foreign_archive)

    rejected = _run_identity(
        foreign,
        "capture",
        foreign_commit,
        foreign_archive,
    )

    assert rejected.returncode == 2
    assert "outstanding preimage owned" in rejected.stderr
    assert not foreign["preimage"].exists()


def test_restored_owner_is_settled_and_the_next_release_can_capture(
    tmp_path: Path,
) -> None:
    layout = _layout(tmp_path)
    _seed(layout)
    assert _run(layout, "capture").returncode == 0
    shutil.copyfile(layout["post_env_source"], layout["slot_env"])
    os.chmod(layout["slot_env"], 0o600)

    restored = _run(layout, "restore")
    assert restored.returncode == 0, restored.stderr
    assert not _owner_path(layout).exists()

    foreign_commit = "c" * 40
    foreign_archive = "d" * 64
    foreign = _identity_layout(layout, foreign_commit, foreign_archive)
    captured = _run_identity(
        foreign,
        "capture",
        foreign_commit,
        foreign_archive,
    )
    assert captured.returncode == 0, captured.stderr


def test_capture_rebinds_an_exact_preimage_after_owner_write_crash(
    tmp_path: Path,
) -> None:
    layout = _layout(tmp_path)
    _seed(layout)
    assert _run(layout, "capture").returncode == 0
    _owner_path(layout).unlink()

    replay = _run(layout, "capture")

    assert replay.returncode == 0, replay.stderr
    assert json.loads(replay.stdout)["decision"] == "reused"
    assert _owner_path(layout).is_file()


def test_owner_digest_tamper_blocks_restore_before_live_mutation(
    tmp_path: Path,
) -> None:
    layout = _layout(tmp_path)
    _seed(layout)
    assert _run(layout, "capture").returncode == 0
    owner = json.loads(_owner_path(layout).read_text(encoding="utf-8"))
    owner["manifestSha256"] = "0" * 64
    _owner_path(layout).write_text(
        json.dumps(owner, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    before = layout["slot_env"].read_bytes()

    rejected = _run(layout, "restore")

    assert rejected.returncode == 2
    assert layout["slot_env"].read_bytes() == before


def test_discard_replays_after_payload_removal_before_owner_clear(
    tmp_path: Path,
) -> None:
    layout = _layout(tmp_path)
    _seed(layout)
    assert _run(layout, "capture").returncode == 0
    shutil.rmtree(layout["preimage"])

    replay = _run(layout, "discard")

    assert replay.returncode == 0, replay.stderr
    assert json.loads(replay.stdout)["decision"] == "discarded"
    assert not _owner_path(layout).exists()


@pytest.mark.parametrize(
    "stage_role",
    ("slot_link_stage", "slot_env_stage", "explicit_unit_stage", "sandbox"),
)
def test_restore_removes_owned_candidate_install_staging(
    tmp_path: Path,
    stage_role: str,
) -> None:
    layout = _layout(tmp_path)
    _seed(layout)
    assert _run(layout, "capture").returncode == 0
    if stage_role == "slot_link_stage":
        stage = layout[stage_role]
        stage.symlink_to("/opt/jato/releases/new/archive")
    elif stage_role == "sandbox":
        stage = layout["instance_dropins"] / (
            ".10-candidate-sandbox.conf.jato-candidate-installing"
        )
        stage.write_bytes(layout["post_sandbox_source"].read_bytes()[:8])
        os.chmod(stage, 0o600)
    else:
        stage = layout[stage_role]
        source_key = (
            "post_env_source"
            if stage_role == "slot_env_stage"
            else "post_unit_source"
        )
        stage.write_bytes(layout[source_key].read_bytes()[:8])
        os.chmod(stage, 0o600)

    restored = _run(layout, "restore")

    assert restored.returncode == 0, restored.stderr
    assert not stage.exists() and not stage.is_symlink()


def test_wrong_full_candidate_stage_blocks_all_restore_mutation(
    tmp_path: Path,
) -> None:
    layout = _layout(tmp_path)
    _seed(layout)
    assert _run(layout, "capture").returncode == 0
    shutil.copyfile(layout["post_env_source"], layout["slot_env"])
    os.chmod(layout["slot_env"], 0o600)
    layout["slot_env_stage"].write_bytes(
        b"x" * layout["post_env_source"].stat().st_size
    )
    os.chmod(layout["slot_env_stage"], 0o600)

    rejected = _run(layout, "restore")

    assert rejected.returncode == 2
    assert layout["slot_env"].read_bytes() == layout["post_env_source"].read_bytes()
    assert layout["slot_env_stage"].is_file()


def test_capture_rejects_candidate_stage_before_publishing_preimage(
    tmp_path: Path,
) -> None:
    layout = _layout(tmp_path)
    _seed(layout)
    layout["slot_env_stage"].write_bytes(b"partial")
    os.chmod(layout["slot_env_stage"], 0o600)

    rejected = _run(layout, "capture")

    assert rejected.returncode == 2
    assert "transaction staging remains" in rejected.stderr
    assert not layout["preimage"].exists()


def test_discard_refuses_staging_and_keeps_owner_armed(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _seed(layout)
    assert _run(layout, "capture").returncode == 0
    layout["slot_env_stage"].write_bytes(b"partial")
    os.chmod(layout["slot_env_stage"], 0o600)

    rejected = _run(layout, "discard")

    assert rejected.returncode == 2
    assert layout["preimage"].is_dir()
    assert _owner_path(layout).is_file()


def test_baseline_sandbox_can_be_overwritten_then_removed_before_restore(
    tmp_path: Path,
) -> None:
    layout = _layout(tmp_path)
    _seed(layout)
    sandbox = layout["instance_dropins"] / "10-candidate-sandbox.conf"
    sandbox.write_text("[Service]\nOldSandbox=yes\n", encoding="utf-8")
    original = sandbox.read_bytes()
    assert _run(layout, "capture").returncode == 0
    sandbox.unlink()

    restored = _run(layout, "restore")

    assert restored.returncode == 0, restored.stderr
    assert sandbox.read_bytes() == original


def test_systemd_delta_preserves_uncontrolled_baseline_dropins(
    tmp_path: Path,
) -> None:
    layout = _layout(tmp_path)
    _seed(layout)
    extra = layout["persistent_control_dropins"] / "70-Custom.conf"
    extra.write_text("[Service]\nTasksMax=100\n", encoding="utf-8")
    assert _run(layout, "capture").returncode == 0
    (layout["persistent_control_dropins"] / "50-MemoryHigh.conf").write_text(
        "[Service]\nMemoryHigh=3G\n",
        encoding="utf-8",
    )
    os.chmod(
        layout["persistent_control_dropins"] / "50-MemoryHigh.conf",
        0o644,
    )

    restored = _run(layout, "restore")

    assert restored.returncode == 0, restored.stderr
    assert extra.read_text(encoding="utf-8") == "[Service]\nTasksMax=100\n"
    assert not (
        layout["persistent_control_dropins"] / "50-MemoryHigh.conf"
    ).exists()


def test_runtime_control_preimage_is_not_resurrected_after_reboot(
    tmp_path: Path,
) -> None:
    layout = _layout(tmp_path)
    _seed(layout)
    layout["runtime_control_dropins"].mkdir(parents=True)
    (layout["runtime_control_dropins"] / "50-CPUQuota.conf").write_text(
        "[Service]\nCPUQuota=200%\n",
        encoding="utf-8",
    )
    assert _run(layout, "capture").returncode == 0
    shutil.rmtree(layout["runtime_control_dropins"])
    layout["boot_id_file"].write_text(
        "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee\n",
        encoding="ascii",
    )

    restored = _run(layout, "restore")

    assert restored.returncode == 0, restored.stderr
    assert not layout["runtime_control_dropins"].exists()


def test_partial_helper_restore_staging_is_rebuilt(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _seed(layout)
    assert _run(layout, "capture").returncode == 0
    token = COMMIT[:12]
    helper_stage = layout["slot_env"].parent / (
        f".{layout['slot_env'].name}.jato-{token}-slot_env.new"
    )
    helper_stage.write_bytes(
        (layout["preimage"] / "payload/slot_env").read_bytes()[:7]
    )
    os.chmod(helper_stage, 0o600)
    shutil.copyfile(layout["post_env_source"], layout["slot_env"])
    os.chmod(layout["slot_env"], 0o600)

    restored = _run(layout, "restore")

    assert restored.returncode == 0, restored.stderr
    assert not helper_stage.exists()
    assert layout["slot_env"].read_text(encoding="utf-8") == "APP_RELEASE_SHA=old\n"


def test_bounded_cache_rejects_oversize_before_hashing_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_helper_module()
    cache = tmp_path / "cache"
    cache.mkdir()
    oversized = cache / "oversized.bin"
    oversized.write_bytes(b"x" * 17)
    original = module._sha256_file

    def guarded(path: Path) -> str:
        if path == oversized:
            raise AssertionError("oversized file must be rejected before hashing")
        return original(path)

    monkeypatch.setattr(module, "_sha256_file", guarded)
    with pytest.raises(module.PreimageError, match="byte limit"):
        module._bounded_cache_tree(cache, {"maxNodes": 8, "maxBytes": 16})


def test_semantic_copy_fsyncs_payload_files_before_nested_directories(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_helper_module()
    source = tmp_path / "source"
    nested = source / "nested"
    nested.mkdir(parents=True)
    (nested / "payload.bin").write_bytes(b"payload")
    target = tmp_path / "target"
    events: list[tuple[str, Path]] = []
    monkeypatch.setattr(
        module,
        "_fsync_regular",
        lambda path: events.append(("file", path)),
    )
    monkeypatch.setattr(
        module,
        "_fsync_directory",
        lambda path: events.append(("directory", path)),
    )

    module._copy_semantic(source, target)

    file_event = ("file", target / "nested/payload.bin")
    nested_event = ("directory", target / "nested")
    root_event = ("directory", target)
    assert file_event in events
    assert nested_event in events
    assert root_event in events
    assert events.index(file_event) < events.index(nested_event) < events.index(
        root_event
    )


def test_special_files_and_nested_role_paths_are_rejected(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _seed(layout)
    fifo = layout["candidate_cache_private"]
    fifo.parent.mkdir(parents=True)
    os.mkfifo(fifo)
    result = _run(layout, "capture")
    assert result.returncode == 2
    assert "cache exceeds the safe preimage contract" in result.stderr

    fifo.unlink()
    layout["candidate_cache_private"] = layout["instance_dropins"] / "nested"
    nested = _run(layout, "capture")
    assert nested.returncode == 2
    assert "must not contain one another" in nested.stderr
