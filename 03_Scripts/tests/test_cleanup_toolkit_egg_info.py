from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
HELPER = REPO_ROOT / "03_Scripts/deploy/cleanup_toolkit_egg_info.py"
EGG_INFO_FILES = (
    "PKG-INFO",
    "SOURCES.txt",
    "dependency_links.txt",
    "entry_points.txt",
    "requires.txt",
    "top_level.txt",
)


def _run(toolkit: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            "-B",
            str(HELPER),
            "--toolkit-root",
            str(toolkit),
        ],
        text=True,
        capture_output=True,
        check=False,
    )


def _toolkit(tmp_path: Path) -> tuple[Path, Path]:
    toolkit = tmp_path / "07_ScrapingToolkit"
    toolkit.mkdir()
    toolkit.chmod(0o711)
    egg_info = toolkit / "jato_scraping_toolkit.egg-info"
    egg_info.mkdir()
    for name in EGG_INFO_FILES:
        (egg_info / name).write_text(f"generated:{name}\n", encoding="utf-8")
    return toolkit, egg_info


def test_cleanup_removes_only_exact_owned_packaging_metadata(
    tmp_path: Path,
) -> None:
    toolkit, egg_info = _toolkit(tmp_path)
    source = toolkit / "jato_scraper.py"
    source.write_text("VALUE = 1\n", encoding="utf-8")

    result = _run(toolkit)

    assert result.returncode == 0, result.stderr
    assert not egg_info.exists()
    assert source.read_text(encoding="utf-8") == "VALUE = 1\n"
    assert _run(toolkit).returncode == 0


@pytest.mark.parametrize("unsafe_kind", ("directory", "symlink", "fifo", "hardlink"))
def test_cleanup_rejects_unsafe_allowed_name_without_recursive_delete(
    tmp_path: Path,
    unsafe_kind: str,
) -> None:
    toolkit, egg_info = _toolkit(tmp_path)
    marker = egg_info / "PKG-INFO"
    marker.unlink()
    if unsafe_kind == "directory":
        marker.mkdir()
        (marker / "keep.txt").write_text("keep", encoding="utf-8")
    elif unsafe_kind == "symlink":
        marker.symlink_to(egg_info / "SOURCES.txt")
    elif unsafe_kind == "fifo":
        os.mkfifo(marker)
    else:
        os.link(egg_info / "SOURCES.txt", marker)

    result = _run(toolkit)

    assert result.returncode != 0
    assert egg_info.exists()
    assert marker.exists() or marker.is_symlink()


@pytest.mark.parametrize("name", ("unexpected.py", "editable.pth"))
def test_cleanup_rejects_extra_file_names(
    tmp_path: Path,
    name: str,
) -> None:
    toolkit, egg_info = _toolkit(tmp_path)
    extra = egg_info / name
    extra.write_text("keep", encoding="utf-8")

    result = _run(toolkit)

    assert result.returncode != 0
    assert egg_info.exists()
    assert extra.exists()


def test_cleanup_rejects_symlink_toolkit_and_egg_info(
    tmp_path: Path,
) -> None:
    toolkit, egg_info = _toolkit(tmp_path)
    toolkit_link = tmp_path / "toolkit-link"
    toolkit_link.symlink_to(toolkit, target_is_directory=True)
    result = _run(toolkit_link)
    assert result.returncode != 0
    assert egg_info.exists()

    for child in egg_info.iterdir():
        child.unlink()
    egg_info.rmdir()
    external = tmp_path / "external"
    external.mkdir()
    egg_info.symlink_to(external, target_is_directory=True)
    result = _run(toolkit)
    assert result.returncode != 0
    assert egg_info.is_symlink()


def test_cleanup_rejects_any_second_egg_info_directory(
    tmp_path: Path,
) -> None:
    toolkit, egg_info = _toolkit(tmp_path)
    second = toolkit / "unexpected.egg-info"
    second.mkdir()

    result = _run(toolkit)

    assert result.returncode != 0
    assert egg_info.exists()
    assert second.exists()


@pytest.mark.parametrize("unsafe_kind", ("mode", "size"))
def test_cleanup_rejects_unsafe_file_mode_or_size(
    tmp_path: Path,
    unsafe_kind: str,
) -> None:
    toolkit, egg_info = _toolkit(tmp_path)
    target = egg_info / "PKG-INFO"
    if unsafe_kind == "mode":
        target.chmod(0o666)
    else:
        target.write_bytes(b"x" * (8 * 1024 * 1024 + 1))

    result = _run(toolkit)

    assert result.returncode != 0
    assert egg_info.exists()


def test_editable_install_keeps_core_and_repository_resources_importable(
    tmp_path: Path,
) -> None:
    toolkit = tmp_path / "07_ScrapingToolkit"
    shutil.copytree(REPO_ROOT / "07_ScrapingToolkit", toolkit)
    prefix = tmp_path / "editable-prefix"
    install = subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--no-build-isolation",
            "--no-deps",
            "--prefix",
            str(prefix),
            "-e",
            str(toolkit),
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    assert install.returncode == 0, install.stderr + install.stdout

    cleanup = _run(toolkit)
    assert cleanup.returncode == 0, cleanup.stderr
    assert not (toolkit / "jato_scraping_toolkit.egg-info").exists()

    site_packages = tuple(prefix.glob("lib/python*/site-packages"))
    assert len(site_packages) == 1
    smoke = subprocess.run(
        [
            sys.executable,
            "-B",
            "-c",
            (
                "import site,sys;"
                "site.addsitedir(sys.argv[1]);"
                "import jato_scraper.core.job as job;"
                "from pathlib import Path;"
                "root=Path(job.__file__).resolve().parents[2];"
                "assert (root/'config/brand_whitelist.yaml').is_file();"
                "assert (root/'sources/_template.yaml').is_file();"
                "assert job.canonical_job_id("
                "kind='msrp',country_code='se',source_code='official'"
                ") == 'msrp:se:official'"
            ),
            str(site_packages[0]),
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    assert smoke.returncode == 0, smoke.stderr + smoke.stdout
