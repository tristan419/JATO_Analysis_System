#!/usr/bin/env python3
"""Safely remove only setuptools metadata created by the editable toolkit install."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import stat
import sys


EGG_INFO_DIRECTORY = "jato_scraping_toolkit.egg-info"
MAX_FILE_BYTES = 8 * 1024 * 1024
MAX_TOTAL_BYTES = 16 * 1024 * 1024
ALLOWED_FILES = frozenset(
    {
        "PKG-INFO",
        "SOURCES.txt",
        "dependency_links.txt",
        "entry_points.txt",
        "requires.txt",
        "top_level.txt",
    }
)


class CleanupError(ValueError):
    """Raised when packaging metadata cannot be proven safe to remove."""


def _is_safe_owned_directory_mode(mode: int) -> bool:
    permissions = stat.S_IMODE(mode)
    return (
        permissions & 0o700 == 0o700
        and permissions & 0o022 == 0
        and permissions & 0o7000 == 0
    )


def _open_real_directory(path: Path) -> int:
    if not path.is_absolute():
        raise CleanupError("toolkit root must be absolute")
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise CleanupError(f"toolkit root must be a real directory: {path}") from exc
    metadata = os.fstat(descriptor)
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or metadata.st_gid != os.getegid()
        or not _is_safe_owned_directory_mode(metadata.st_mode)
    ):
        os.close(descriptor)
        raise CleanupError(
            "toolkit root must be a real safely-mode-owned directory"
        )
    return descriptor


def cleanup(toolkit_root: Path) -> bool:
    root_descriptor = _open_real_directory(toolkit_root)
    egg_descriptor = -1
    try:
        sibling_egg_info = sorted(
            name
            for name in os.listdir(root_descriptor)
            if name.endswith(".egg-info") and name != EGG_INFO_DIRECTORY
        )
        if sibling_egg_info:
            raise CleanupError(
                f"toolkit root contains unexpected egg-info entries: {sibling_egg_info}"
            )
        try:
            target_metadata = os.stat(
                EGG_INFO_DIRECTORY,
                dir_fd=root_descriptor,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            return False
        if (
            not stat.S_ISDIR(target_metadata.st_mode)
            or target_metadata.st_uid != os.geteuid()
            or target_metadata.st_gid != os.getegid()
            or not _is_safe_owned_directory_mode(target_metadata.st_mode)
        ):
            raise CleanupError(
                "toolkit egg-info must be a real safely-mode-owned directory"
            )

        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            egg_descriptor = os.open(
                EGG_INFO_DIRECTORY,
                flags,
                dir_fd=root_descriptor,
            )
        except OSError as exc:
            raise CleanupError("toolkit egg-info cannot be opened safely") from exc
        opened_metadata = os.fstat(egg_descriptor)
        if (
            (opened_metadata.st_dev, opened_metadata.st_ino)
            != (target_metadata.st_dev, target_metadata.st_ino)
            or not stat.S_ISDIR(opened_metadata.st_mode)
            or opened_metadata.st_uid != os.geteuid()
            or opened_metadata.st_gid != os.getegid()
            or not _is_safe_owned_directory_mode(opened_metadata.st_mode)
        ):
            raise CleanupError("toolkit egg-info changed while it was opened")

        names = set(os.listdir(egg_descriptor))
        unexpected = sorted(names - ALLOWED_FILES)
        if unexpected:
            raise CleanupError(
                f"toolkit egg-info contains unexpected entries: {unexpected}"
            )

        validated: dict[str, tuple[int, int]] = {}
        total_bytes = 0
        for name in sorted(names):
            metadata = os.stat(
                name,
                dir_fd=egg_descriptor,
                follow_symlinks=False,
            )
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != os.geteuid()
                or metadata.st_gid != os.getegid()
                or metadata.st_nlink != 1
                or stat.S_IMODE(metadata.st_mode) not in {0o600, 0o640, 0o644}
                or metadata.st_size > MAX_FILE_BYTES
            ):
                raise CleanupError(
                    "toolkit egg-info entry is not a bounded, safely-mode-owned "
                    f"single-link file: {name}"
                )
            total_bytes += metadata.st_size
            validated[name] = (metadata.st_dev, metadata.st_ino)
        if total_bytes > MAX_TOTAL_BYTES:
            raise CleanupError("toolkit egg-info exceeds the safe total size")

        for name, identity in validated.items():
            metadata = os.stat(
                name,
                dir_fd=egg_descriptor,
                follow_symlinks=False,
            )
            if (
                (metadata.st_dev, metadata.st_ino) != identity
                or not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != os.geteuid()
                or metadata.st_gid != os.getegid()
                or metadata.st_nlink != 1
                or stat.S_IMODE(metadata.st_mode) not in {0o600, 0o640, 0o644}
                or metadata.st_size > MAX_FILE_BYTES
            ):
                raise CleanupError(
                    f"toolkit egg-info entry changed before removal: {name}"
                )
            os.unlink(name, dir_fd=egg_descriptor)

        current_target = os.stat(
            EGG_INFO_DIRECTORY,
            dir_fd=root_descriptor,
            follow_symlinks=False,
        )
        if (
            (current_target.st_dev, current_target.st_ino)
            != (target_metadata.st_dev, target_metadata.st_ino)
            or not stat.S_ISDIR(current_target.st_mode)
            or current_target.st_uid != os.geteuid()
            or current_target.st_gid != os.getegid()
            or not _is_safe_owned_directory_mode(current_target.st_mode)
        ):
            raise CleanupError("toolkit egg-info changed before directory removal")
        os.rmdir(EGG_INFO_DIRECTORY, dir_fd=root_descriptor)
        return True
    finally:
        if egg_descriptor >= 0:
            os.close(egg_descriptor)
        os.close(root_descriptor)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--toolkit-root", type=Path, required=True)
    arguments = parser.parse_args()
    try:
        removed = cleanup(arguments.toolkit_root)
    except (CleanupError, OSError) as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1
    action = "removed" if removed else "not present"
    print(f"[INFO] Safe toolkit egg-info cleanup: {action}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
