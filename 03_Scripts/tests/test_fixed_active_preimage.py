from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
HELPER = REPO_ROOT / "03_Scripts/deploy/fixed_active_preimage.py"
TARGET_SHA = "a" * 40
OLD_SHA = "b" * 40
ARCHIVE_SHA = "c" * 64
CONTENT_PROOF = (
    f"content-addressed:{OLD_SHA}:{'d' * 64}:{'e' * 64}:{'f' * 64}"
)


def _fixture(tmp_path: Path) -> dict[str, Path]:
    paths = {
        "state": tmp_path / "state",
        "slots": tmp_path / "slots",
        "env_root": tmp_path / "slot-env",
        "slot_link": tmp_path / "slots/8000/current",
        "slot_env": tmp_path / "slot-env/8000.env",
        "active_link": tmp_path / "active",
        "nginx": tmp_path / "nginx/active-release.conf",
        "old": tmp_path / "releases/old",
        "legacy": tmp_path / "legacy",
        "target": tmp_path / "releases/target",
        "target_env": tmp_path / "target.env",
        "target_nginx": tmp_path / "target-nginx.conf",
    }
    for directory in (
        paths["state"],
        paths["slots"] / "8000",
        paths["env_root"],
        paths["nginx"].parent,
        paths["old"],
        paths["target"],
    ):
        directory.mkdir(parents=True, exist_ok=True)
    paths["slot_link"].symlink_to(paths["old"])
    paths["active_link"].symlink_to(paths["old"])
    paths["slot_env"].write_text("APP_RELEASE_SHA=old\n", encoding="utf-8")
    paths["slot_env"].chmod(0o600)
    paths["nginx"].write_text("root old;\n", encoding="utf-8")
    paths["nginx"].chmod(0o644)
    paths["target_env"].write_text("APP_RELEASE_SHA=new\n", encoding="utf-8")
    paths["target_env"].chmod(0o600)
    paths["target_nginx"].write_text("root new;\n", encoding="utf-8")
    paths["target_nginx"].chmod(0o644)
    return paths


def _command(paths: dict[str, Path], operation: str) -> list[str]:
    previous_proof = CONTENT_PROOF
    if paths["old"] == paths["legacy"]:
        previous_proof = f"legacy-private-fingerprint:{OLD_SHA}"
    return [
        "python3",
        str(HELPER),
        operation,
        "--state-root",
        str(paths["state"]),
        "--slots-root",
        str(paths["slots"]),
        "--slot-env-root",
        str(paths["env_root"]),
        "--slot-link",
        str(paths["slot_link"]),
        "--slot-env",
        str(paths["slot_env"]),
        "--active-release-link",
        str(paths["active_link"]),
        "--nginx-conf",
        str(paths["nginx"]),
        "--previous-release-root",
        str(paths["old"]),
        "--legacy-root",
        str(paths["legacy"]),
        "--target-release-root",
        str(paths["target"]),
        "--target-env",
        str(paths["target_env"]),
        "--target-nginx",
        str(paths["target_nginx"]),
        "--commit",
        TARGET_SHA,
        "--archive-sha256",
        ARCHIVE_SHA,
        "--active-slot",
        "8000",
        "--previous-release-sha",
        OLD_SHA,
        "--previous-release-proof",
        previous_proof,
    ]


def _run(paths: dict[str, Path], operation: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        _command(paths, operation),
        text=True,
        capture_output=True,
        check=False,
    )


def _replace_link(path: Path, target: Path) -> None:
    temporary = path.with_name(f".{path.name}.new")
    temporary.symlink_to(target)
    os.replace(temporary, path)


def test_capture_binds_identity_and_restore_accepts_only_authorized_target(
    tmp_path: Path,
) -> None:
    paths = _fixture(tmp_path)
    captured = _run(paths, "capture")

    assert captured.returncode == 0, captured.stderr
    assert json.loads(captured.stdout)["decision"] == "captured"
    preimage = (
        paths["state"]
        / "active-update-preimages"
        / TARGET_SHA
        / ARCHIVE_SHA
    )
    assert (preimage / "manifest.json").is_file()

    paths["slot_env"].write_bytes(paths["target_env"].read_bytes())
    paths["slot_env"].chmod(0o600)
    paths["nginx"].write_bytes(paths["target_nginx"].read_bytes())
    paths["nginx"].chmod(0o644)
    _replace_link(paths["slot_link"], paths["target"])
    _replace_link(paths["active_link"], paths["target"])

    verified = _run(paths, "verify")
    restored = _run(paths, "restore")

    assert verified.returncode == 0, verified.stderr
    assert restored.returncode == 0, restored.stderr
    assert paths["slot_env"].read_text(encoding="utf-8") == "APP_RELEASE_SHA=old\n"
    assert paths["nginx"].read_text(encoding="utf-8") == "root old;\n"
    assert paths["slot_link"].resolve() == paths["old"]
    assert paths["active_link"].resolve() == paths["old"]


@pytest.mark.parametrize("role", ("slot_env", "nginx", "slot_link", "active_link"))
def test_restore_refuses_third_party_drift(tmp_path: Path, role: str) -> None:
    paths = _fixture(tmp_path)
    captured = _run(paths, "capture")
    assert captured.returncode == 0, captured.stderr

    if role in {"slot_env", "nginx"}:
        paths[role].write_text("foreign-drift\n", encoding="utf-8")
        paths[role].chmod(0o600 if role == "slot_env" else 0o644)
    else:
        foreign = tmp_path / "foreign"
        foreign.mkdir()
        _replace_link(paths[role], foreign)

    restored = _run(paths, "restore")

    assert restored.returncode != 0
    assert "refusing to overwrite drifted fixed Active" in restored.stderr


def test_capture_rejects_slot_path_outside_selected_active_slot(
    tmp_path: Path,
) -> None:
    paths = _fixture(tmp_path)
    command = _command(paths, "capture")
    slot_link_index = command.index("--slot-link") + 1
    command[slot_link_index] = str(tmp_path / "foreign/current")

    result = subprocess.run(
        command,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert "outside the selected fixed Active slot" in result.stderr


def test_content_addressed_preimage_binds_recomputed_seal_proof(
    tmp_path: Path,
) -> None:
    paths = _fixture(tmp_path)
    captured = _run(paths, "capture")
    assert captured.returncode == 0, captured.stderr
    command = _command(paths, "verify")
    proof_index = command.index("--previous-release-proof") + 1
    command[proof_index] = (
        f"content-addressed:{OLD_SHA}:{'d' * 64}:{'0' * 64}:{'f' * 64}"
    )

    verified = subprocess.run(
        command,
        text=True,
        capture_output=True,
        check=False,
    )

    assert verified.returncode != 0
    assert "fixed Active manifest identity is invalid" in verified.stderr


def _make_legacy_active(paths: dict[str, Path]) -> None:
    paths["legacy"] = paths["old"]
    for directory in (
        paths["old"] / "03_Scripts",
        paths["old"] / "06_AppPlatform/frontend/dist",
        paths["old"] / "hermes",
        paths["old"] / ".venv/bin",
        paths["old"] / ".venv/lib/python3.12/site-packages/fastapi",
        paths["old"] / "01_RAW_DATA",
        paths["old"] / "04_Processed_data",
    ):
        directory.mkdir(parents=True, exist_ok=True)
    (paths["old"] / "03_Scripts/app.py").write_text(
        "print('legacy')\n",
        encoding="utf-8",
    )
    (paths["old"] / "06_AppPlatform/frontend/dist/index.html").write_text(
        "legacy frontend\n",
        encoding="utf-8",
    )
    (paths["old"] / "hermes/deploy_release.json").write_text(
        json.dumps({"actualCommitSha": OLD_SHA}) + "\n",
        encoding="utf-8",
    )
    (paths["old"] / ".venv/bin/python").symlink_to(sys.executable)
    (paths["old"] / ".venv/lib/python3.12/site-packages/fastapi/__init__.py").write_text(
        "__version__ = 'legacy'\n",
        encoding="utf-8",
    )
    (paths["old"] / "01_RAW_DATA/live.xlsx").write_text(
        "mutable raw\n",
        encoding="utf-8",
    )
    (paths["old"] / "04_Processed_data/live.parquet").write_text(
        "mutable processed\n",
        encoding="utf-8",
    )


def test_legacy_capture_is_private_and_never_writes_legacy_root(
    tmp_path: Path,
) -> None:
    paths = _fixture(tmp_path)
    _make_legacy_active(paths)
    before = sorted(
        path.relative_to(paths["old"]).as_posix()
        for path in paths["old"].rglob("*")
    )

    captured = _run(paths, "capture")

    assert captured.returncode == 0, captured.stderr
    after = sorted(
        path.relative_to(paths["old"]).as_posix()
        for path in paths["old"].rglob("*")
    )
    assert after == before
    preimage = (
        paths["state"]
        / "active-update-preimages"
        / TARGET_SHA
        / ARCHIVE_SHA
    )
    manifest = json.loads((preimage / "manifest.json").read_text())
    fingerprint = manifest["legacyPreviousReleaseFingerprint"]
    assert fingerprint["previousReleaseSha"] == OLD_SHA
    assert fingerprint["entryCount"] > 0
    assert len(fingerprint["treeSha256"]) == 64
    assert fingerprint["runtimeInterpreter"]["path"] == ".venv/bin/python"
    assert not any(path.name.startswith(".jato") for path in paths["old"].rglob("*"))


def test_legacy_fingerprint_ignores_only_reviewed_shared_data_paths(
    tmp_path: Path,
) -> None:
    paths = _fixture(tmp_path)
    _make_legacy_active(paths)
    assert _run(paths, "capture").returncode == 0

    (paths["old"] / "01_RAW_DATA/live.xlsx").write_text(
        "new raw data\n",
        encoding="utf-8",
    )
    (paths["old"] / "04_Processed_data/live.parquet").write_text(
        "new processed data\n",
        encoding="utf-8",
    )

    verified = _run(paths, "verify")
    assert verified.returncode == 0, verified.stderr


@pytest.mark.parametrize(
    "relative",
    (
        "03_Scripts/app.py",
        "06_AppPlatform/frontend/dist/index.html",
        "hermes/deploy_release.json",
        ".venv/lib/python3.12/site-packages/fastapi/__init__.py",
    ),
)
def test_legacy_fingerprint_rejects_source_frontend_or_metadata_drift(
    tmp_path: Path,
    relative: str,
) -> None:
    paths = _fixture(tmp_path)
    _make_legacy_active(paths)
    assert _run(paths, "capture").returncode == 0

    (paths["old"] / relative).write_text("drifted\n", encoding="utf-8")
    verified = _run(paths, "verify")

    assert verified.returncode != 0
    assert "legacy Active source/runtime fingerprint drifted" in verified.stderr


def test_legacy_fingerprint_rejects_python_entry_drift(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    _make_legacy_active(paths)
    assert _run(paths, "capture").returncode == 0
    entry = paths["old"] / ".venv/bin/python"
    entry.unlink()
    entry.write_bytes(Path(sys.executable).read_bytes())

    verified = _run(paths, "verify")

    assert verified.returncode != 0
    assert "legacy Active source/runtime fingerprint drifted" in verified.stderr
