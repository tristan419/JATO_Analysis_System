from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
HELPER = REPO_ROOT / "03_Scripts/deploy/verify_release_source_seal.py"
CRITICAL_FILES = (
    "03_Scripts/deploy/fixed_release_v2.py",
    "03_Scripts/deploy/fixed_release_v2_remote.sh",
    "03_Scripts/deploy/frontend_release_artifact.py",
    "03_Scripts/deploy/jato_quiescence_gate.py",
    "03_Scripts/deploy/release_v2_admission.py",
    "03_Scripts/deploy/release_v2_store.py",
    "03_Scripts/deploy/validate_release_archive.py",
    "03_Scripts/deploy/verify_release_source_seal.py",
    "03_Scripts/deploy/nginx/jato_active_release_v2.conf",
    "03_Scripts/deploy/nginx/jato_candidate_preview_v2.conf",
    "03_Scripts/deploy/systemd/jato-candidate-preview.service",
    "03_Scripts/deploy/systemd/jato-fullstack-backend@.service",
    "03_Scripts/deploy/systemd/jato-fullstack-backend@8001.service.d/"
    "20-candidate-readonly.conf",
)
RUNTIME_COMMIT = "a" * 40
RUNTIME_ARCHIVE = "b" * 64
RUNTIME_FRONTEND_IDENTITY = "gha://owner/repo/actions/runs/1/attempts/1/artifacts/frontend"
RUNTIME_FRONTEND_CHECKSUM = "c" * 64


@pytest.fixture()
def sealed_tree(tmp_path: Path) -> tuple[Path, Path]:
    root = tmp_path / "release"
    for relative in CRITICAL_FILES:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"critical:{relative}\n", encoding="utf-8")
        path.chmod(0o755 if path.suffix in {".sh", ".py"} else 0o644)
    ordinary = root / "06_AppPlatform/backend/app/main.py"
    ordinary.parent.mkdir(parents=True)
    ordinary.write_text("VALUE = 1\n", encoding="utf-8")
    (root / "06_AppPlatform/frontend").mkdir(parents=True)
    manifest = tmp_path / "expected-seal.json"
    result = subprocess.run(
        [
            "python3",
            str(HELPER),
            "build",
            "--root",
            str(root),
            "--output",
            str(manifest),
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return root, manifest


def _verify(root: Path, manifest: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "python3",
            str(HELPER),
            "verify",
            "--root",
            str(root),
            "--manifest",
            str(manifest),
        ],
        text=True,
        capture_output=True,
        check=False,
    )


def _runtime_arguments() -> list[str]:
    return [
        "--profile",
        "runtime",
        "--commit",
        RUNTIME_COMMIT,
        "--archive-sha256",
        RUNTIME_ARCHIVE,
        "--frontend-identity",
        RUNTIME_FRONTEND_IDENTITY,
        "--frontend-checksum",
        RUNTIME_FRONTEND_CHECKSUM,
    ]


@pytest.fixture()
def sealed_runtime(tmp_path: Path) -> tuple[Path, Path]:
    root = tmp_path / "release"
    python = root / ".venv/bin/python"
    python.parent.mkdir(parents=True)
    python.write_bytes(b"trusted-python-runtime\n")
    python.chmod(0o755)
    site_packages = root / ".venv/lib/python3.12/site-packages"
    for package in ("fastapi", "uvicorn"):
        package_root = site_packages / package
        package_root.mkdir(parents=True)
        (package_root / "__init__.py").write_text(
            f"PACKAGE = {package!r}\n",
            encoding="utf-8",
        )
    dist = root / "06_AppPlatform/frontend/dist"
    dist.mkdir(parents=True)
    for name in (
        "index.html",
        "build-meta.json",
        "release-provenance.json",
        "_deploy_status.txt",
    ):
        (dist / name).write_text(f"trusted:{name}\n", encoding="utf-8")
    (root / ".jato-source-seal.json").write_text(
        '{"verified":"source"}\n',
        encoding="utf-8",
    )
    manifest = tmp_path / "runtime-seal.json"
    result = subprocess.run(
        [
            sys.executable,
            str(HELPER),
            "build",
            "--root",
            str(root),
            "--output",
            str(manifest),
            *_runtime_arguments(),
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return root, manifest


def _verify_runtime(
    root: Path,
    manifest: Path,
    *identity_override: str,
) -> subprocess.CompletedProcess[str]:
    identity = list(identity_override) or _runtime_arguments()
    return subprocess.run(
        [
            sys.executable,
            str(HELPER),
            "verify",
            "--root",
            str(root),
            "--manifest",
            str(manifest),
            *identity,
        ],
        text=True,
        capture_output=True,
        check=False,
    )


@pytest.mark.parametrize("mutation", ("content", "mode", "added", "symlink"))
def test_source_seal_rejects_persistent_source_tampering(
    sealed_tree: tuple[Path, Path],
    tmp_path: Path,
    mutation: str,
) -> None:
    root, manifest = sealed_tree
    ordinary = root / "06_AppPlatform/backend/app/main.py"
    if mutation == "content":
        ordinary.write_text("VALUE = 2\n", encoding="utf-8")
    elif mutation == "mode":
        ordinary.chmod(0o755)
    elif mutation == "added":
        (ordinary.parent / "injected.py").write_text(
            "INJECTED = True\n",
            encoding="utf-8",
        )
    else:
        target = tmp_path / "attacker.py"
        target.write_text("VALUE = 1\n", encoding="utf-8")
        ordinary.unlink()
        ordinary.symlink_to(target)

    result = _verify(root, manifest)

    assert result.returncode != 0
    assert "does not match the verified archive seal" in result.stderr


def test_source_seal_rejects_generated_egg_info(
    sealed_tree: tuple[Path, Path],
) -> None:
    root, manifest = sealed_tree
    egg_info = root / "07_ScrapingToolkit/jato_scraping_toolkit.egg-info"
    egg_info.mkdir(parents=True)
    (egg_info / "PKG-INFO").write_text(
        "Metadata-Version: 2.4\nName: jato-scraping-toolkit\n",
        encoding="utf-8",
    )

    result = _verify(root, manifest)

    assert result.returncode != 0
    assert "does not match the verified archive seal" in result.stderr


def test_source_seal_rejects_directory_mode_and_type_changes(
    sealed_tree: tuple[Path, Path],
    tmp_path: Path,
) -> None:
    root, manifest = sealed_tree
    directory = root / "06_AppPlatform/frontend"
    original_mode = directory.stat().st_mode & 0o777
    directory.chmod(0o700 if original_mode != 0o700 else 0o755)
    assert _verify(root, manifest).returncode != 0

    directory.rmdir()
    replacement = tmp_path / "frontend"
    replacement.mkdir()
    directory.symlink_to(replacement, target_is_directory=True)
    result = _verify(root, manifest)

    assert result.returncode != 0
    assert "does not match the verified archive seal" in result.stderr


def test_source_seal_intentionally_ignores_mtime_only_changes(
    sealed_tree: tuple[Path, Path],
) -> None:
    root, manifest = sealed_tree
    ordinary = root / "06_AppPlatform/backend/app/main.py"
    directory = ordinary.parent
    for path in (ordinary, directory):
        metadata = path.stat()
        os.utime(
            path,
            ns=(metadata.st_atime_ns, metadata.st_mtime_ns + 1_000_000_000),
        )

    result = _verify(root, manifest)

    assert result.returncode == 0, result.stderr


def test_source_seal_allows_only_declared_mutable_runtime_paths(
    sealed_tree: tuple[Path, Path],
    tmp_path: Path,
) -> None:
    root, manifest = sealed_tree
    (root / ".venv/bin").mkdir(parents=True)
    (root / ".venv/bin/python").write_text("mutable venv\n", encoding="utf-8")
    dist = root / "06_AppPlatform/frontend/dist"
    dist.mkdir(parents=True)
    (dist / "index.html").write_text("new frontend\n", encoding="utf-8")
    raw_target = tmp_path / "raw-v1"
    raw_target.mkdir()
    processed_target = tmp_path / "processed-v1"
    processed_target.mkdir()
    (root / "01_RAW_DATA").symlink_to(raw_target, target_is_directory=True)
    (root / "04_Processed_data").symlink_to(
        processed_target,
        target_is_directory=True,
    )
    pycache = root / "06_AppPlatform/backend/app/__pycache__"
    pycache.mkdir()
    (pycache / "main.cpython-312.pyc").write_bytes(b"runtime cache")

    first = _verify(root, manifest)
    assert first.returncode == 0, first.stderr

    (root / ".venv/bin/python").write_text("mutated venv\n", encoding="utf-8")
    (dist / "index.html").write_text("mutated frontend\n", encoding="utf-8")
    (root / "01_RAW_DATA").unlink()
    raw_target_v2 = tmp_path / "raw-v2"
    raw_target_v2.mkdir()
    (root / "01_RAW_DATA").symlink_to(raw_target_v2, target_is_directory=True)
    os.chmod(pycache / "main.cpython-312.pyc", 0o777)

    second = _verify(root, manifest)
    assert second.returncode == 0, second.stderr


def test_source_seal_requires_every_critical_controller_file(
    tmp_path: Path,
) -> None:
    root = tmp_path / "release"
    root.mkdir()
    output = tmp_path / "seal.json"

    result = subprocess.run(
        [
            "python3",
            str(HELPER),
            "build",
            "--root",
            str(root),
            "--output",
            str(output),
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert "critical release source file is missing" in result.stderr


@pytest.mark.parametrize(
    "relative",
    (
        ".venv/bin/python",
        ".venv/lib/python3.12/site-packages/fastapi/__init__.py",
        ".venv/lib/python3.12/site-packages/uvicorn/loader.py",
        "06_AppPlatform/frontend/dist/index.html",
        "06_AppPlatform/frontend/dist/build-meta.json",
        "06_AppPlatform/frontend/dist/release-provenance.json",
        "06_AppPlatform/frontend/dist/_deploy_status.txt",
        "06_AppPlatform/frontend/dist/assets/old.js",
    ),
)
def test_runtime_seal_rejects_venv_and_frontend_mutation(
    sealed_runtime: tuple[Path, Path],
    relative: str,
) -> None:
    root, manifest = sealed_runtime
    target = root / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b"tampered-runtime\n")
    if relative == ".venv/bin/python":
        target.chmod(0o755)

    result = _verify_runtime(root, manifest)

    assert result.returncode != 0
    assert "does not match the verified archive seal" in result.stderr


def test_runtime_seal_ignores_only_pycache(
    sealed_runtime: tuple[Path, Path],
) -> None:
    root, manifest = sealed_runtime
    pycache = (
        root
        / ".venv/lib/python3.12/site-packages/fastapi/__pycache__/app.cpython-312.pyc"
    )
    pycache.parent.mkdir()
    pycache.write_bytes(b"runtime-cache")
    assert _verify_runtime(root, manifest).returncode == 0

    outside_cache = (
        root / ".venv/lib/python3.12/site-packages/fastapi/app.cpython-312.pyc"
    )
    outside_cache.write_bytes(b"importable-bytecode")
    assert _verify_runtime(root, manifest).returncode != 0


def test_runtime_seal_binds_release_identity_and_source_seal(
    sealed_runtime: tuple[Path, Path],
) -> None:
    root, manifest = sealed_runtime
    wrong_identity = _runtime_arguments()
    wrong_identity[wrong_identity.index(RUNTIME_COMMIT)] = "d" * 40
    assert _verify_runtime(
        root,
        manifest,
        *wrong_identity,
    ).returncode != 0

    (root / ".jato-source-seal.json").write_text(
        '{"verified":"different"}\n',
        encoding="utf-8",
    )
    assert _verify_runtime(root, manifest).returncode != 0


def test_runtime_seal_records_resolved_symlink_interpreter(
    sealed_runtime: tuple[Path, Path],
    tmp_path: Path,
) -> None:
    root, _old_manifest = sealed_runtime
    interpreter = root / ".venv/bin/python"
    external = tmp_path / "trusted-system-python"
    external.write_bytes(b"external-python-v1\n")
    external.chmod(0o755)
    interpreter.unlink()
    interpreter.symlink_to(external)
    manifest = tmp_path / "symlink-runtime-seal.json"
    build = subprocess.run(
        [
            sys.executable,
            str(HELPER),
            "build",
            "--root",
            str(root),
            "--output",
            str(manifest),
            *_runtime_arguments(),
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    assert build.returncode == 0, build.stderr
    assert _verify_runtime(root, manifest).returncode == 0

    external.write_bytes(b"external-python-v2\n")
    assert _verify_runtime(root, manifest).returncode != 0


def test_relocatable_runtime_seal_rejects_external_interpreter(
    sealed_runtime: tuple[Path, Path],
    tmp_path: Path,
) -> None:
    root, _old_manifest = sealed_runtime
    interpreter = root / ".venv/bin/python"
    external = tmp_path / "system-python"
    external.write_bytes(b"external-python\n")
    external.chmod(0o755)
    interpreter.unlink()
    interpreter.symlink_to(external)

    result = subprocess.run(
        [
            sys.executable,
            str(HELPER),
            "build",
            "--root",
            str(root),
            "--output",
            str(tmp_path / "relocatable-runtime-seal.json"),
            *_runtime_arguments(),
            "--recorded-runtime-root",
            str(tmp_path / "final-release"),
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert "requires an interpreter inside the release root" in result.stderr


def test_relocatable_runtime_seal_verifies_after_atomic_promotion(
    sealed_runtime: tuple[Path, Path],
    tmp_path: Path,
) -> None:
    staging_root, _staging_manifest = sealed_runtime
    final_root = tmp_path / "releases" / RUNTIME_COMMIT / RUNTIME_ARCHIVE
    final_root.parent.mkdir(parents=True)
    staging_manifest = staging_root / ".jato-runtime-seal.json"

    build = subprocess.run(
        [
            sys.executable,
            str(HELPER),
            "build",
            "--root",
            str(staging_root),
            "--output",
            str(staging_manifest),
            *_runtime_arguments(),
            "--recorded-runtime-root",
            str(final_root),
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert build.returncode == 0, build.stderr
    staging_verify = _verify_runtime(
        staging_root,
        staging_manifest,
        *_runtime_arguments(),
        "--recorded-runtime-root",
        str(final_root),
    )
    assert staging_verify.returncode == 0, staging_verify.stderr
    os.replace(staging_root, final_root)
    final_manifest = final_root / ".jato-runtime-seal.json"
    assert _verify_runtime(final_root, final_manifest).returncode == 0
    payload = json.loads(final_manifest.read_text(encoding="utf-8"))
    assert payload["runtimeInterpreter"]["resolvedPath"].startswith(
        str(final_root)
    )
