#!/usr/bin/env python3
"""Prepare backend release identity files without third-party dependencies."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
import re
import shlex
import stat
import sys
import tempfile
from typing import Any


SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
ENV_RELEASE_PATTERN = re.compile(
    r"^[ \t]*(?:export[ \t]+)?APP_RELEASE_SHA[ \t]*=",
)
MAX_METADATA_BYTES = 256 * 1024


class ReleasePreparationError(RuntimeError):
    """A fail-closed release preparation error."""


def _validated_commit(value: str) -> str:
    commit = value.strip()
    if not SHA_PATTERN.fullmatch(commit):
        raise ReleasePreparationError(
            "release commit must be a full lowercase git SHA",
        )
    return commit


def update_env(path: Path, *, commit: str) -> None:
    """Replace APP_RELEASE_SHA once while preserving all unrelated settings."""

    release_commit = _validated_commit(commit)
    if not path.is_file() or path.is_symlink():
        raise ReleasePreparationError(
            f"backend env candidate is missing or unsafe: {path}",
        )
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError) as exc:
        raise ReleasePreparationError(
            f"backend env candidate is unreadable: {path}",
        ) from exc
    retained = [line for line in lines if not ENV_RELEASE_PATTERN.match(line)]
    retained.append(f"APP_RELEASE_SHA={shlex.quote(release_commit)}")
    path.write_text("\n".join(retained).rstrip() + "\n", encoding="utf-8")


def _read_metadata(path: Path, *, require_existing: bool) -> dict[str, Any]:
    if not path.exists():
        if require_existing:
            raise ReleasePreparationError(
                f"checkpoint release metadata is missing: {path}",
            )
        return {}
    if path.is_symlink() or not path.is_file():
        raise ReleasePreparationError(f"release metadata path is unsafe: {path}")
    try:
        size = path.stat().st_size
        if size <= 0 or size > MAX_METADATA_BYTES:
            raise ReleasePreparationError(
                f"release metadata size is invalid: {size}",
            )
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReleasePreparationError(
            f"release metadata is unreadable: {path}",
        ) from exc
    if not isinstance(payload, dict):
        raise ReleasePreparationError("release metadata must be a JSON object")
    return payload


def _actual_commit(payload: dict[str, Any]) -> str:
    candidate = str(
        payload.get("actualCommitSha") or payload.get("commitSha") or "",
    ).strip()
    return candidate if SHA_PATTERN.fullmatch(candidate) else ""


def _write_metadata_atomically(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = 0o644
    if path.exists():
        mode = stat.S_IMODE(path.stat().st_mode)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
        directory_descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    except Exception:
        try:
            os.close(descriptor)
        except OSError:
            pass
        try:
            os.unlink(temporary_name)
        except OSError:
            pass
        raise


def prepare_metadata(
    path: Path,
    *,
    commit: str,
    branch: str,
    source: str,
    require_existing: bool,
    previous_metadata_path: Path | None = None,
) -> dict[str, Any]:
    """Prepare a target without claiming that it is the running release."""

    release_commit = _validated_commit(commit)
    payload = _read_metadata(path, require_existing=require_existing)
    if require_existing:
        packaged_commit = str(
            payload.get("expectedCommitSha") or payload.get("commitSha") or "",
        ).strip()
        if packaged_commit != release_commit:
            raise ReleasePreparationError(
                "checkpoint release metadata does not match the target commit",
            )

    previous_actual = _actual_commit(payload)
    if previous_metadata_path is not None:
        previous_payload = _read_metadata(
            previous_metadata_path,
            require_existing=True,
        )
        previous_actual = _actual_commit(previous_payload)

    now = dt.datetime.now(dt.UTC).isoformat()
    payload.update(
        {
            "service": payload.get("service") or "jato-fullstack-backend",
            "environment": payload.get("environment") or "production",
            "branch": branch,
            "source": (
                payload.get("source") or source
                if require_existing
                else source
            ),
            "expectedCommitSha": release_commit,
            "expectedShortSha": release_commit[:8],
            "actualCommitSha": previous_actual,
            "actualShortSha": previous_actual[:8],
            "commitSha": previous_actual,
            "shortSha": previous_actual[:8],
            "releasePreparedAt": now,
        },
    )
    _write_metadata_atomically(path, payload)
    return payload


def confirm_metadata(
    path: Path,
    *,
    commit: str,
    service: str,
) -> dict[str, Any]:
    """Seal the actual release only after the caller verified readiness."""

    release_commit = _validated_commit(commit)
    payload = _read_metadata(path, require_existing=True)
    expected_commit = str(
        payload.get("expectedCommitSha") or "",
    ).strip()
    if expected_commit != release_commit:
        raise ReleasePreparationError(
            "release metadata expected commit does not match the ready process",
        )
    now = dt.datetime.now(dt.UTC).isoformat()
    payload.update(
        {
            "service": payload.get("service") or service,
            "environment": payload.get("environment") or "production",
            "deployMethod": (
                payload.get("deployMethod")
                or payload.get("source")
                or "manual_script"
            ),
            "actualCommitSha": release_commit,
            "actualShortSha": release_commit[:8],
            "commitSha": release_commit,
            "shortSha": release_commit[:8],
            "deployedAt": now,
            "serviceRestartedAt": now,
            "healthz": "ok",
            "readyz": "ready",
        },
    )
    _write_metadata_atomically(path, payload)
    return payload


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare immutable backend release identity.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    env_parser = commands.add_parser("update-env")
    env_parser.add_argument("--path", type=Path, required=True)
    env_parser.add_argument("--commit", required=True)

    metadata_parser = commands.add_parser("prepare-metadata")
    metadata_parser.add_argument("--path", type=Path, required=True)
    metadata_parser.add_argument("--commit", required=True)
    metadata_parser.add_argument("--branch", required=True)
    metadata_parser.add_argument("--source", required=True)
    metadata_parser.add_argument("--require-existing", action="store_true")
    metadata_parser.add_argument("--previous-metadata", type=Path)

    confirm_parser = commands.add_parser("confirm-metadata")
    confirm_parser.add_argument("--path", type=Path, required=True)
    confirm_parser.add_argument("--commit", required=True)
    confirm_parser.add_argument("--service", required=True)
    return parser


def main() -> int:
    arguments = _build_parser().parse_args()
    try:
        if arguments.command == "update-env":
            update_env(arguments.path, commit=arguments.commit)
            print(
                json.dumps(
                    {
                        "action": "backend_env_release_sha",
                        "commitSha": arguments.commit,
                        "status": "prepared",
                    },
                    sort_keys=True,
                ),
            )
        elif arguments.command == "prepare-metadata":
            payload = prepare_metadata(
                arguments.path,
                commit=arguments.commit,
                branch=arguments.branch,
                source=arguments.source,
                require_existing=arguments.require_existing,
                previous_metadata_path=arguments.previous_metadata,
            )
            print(
                json.dumps(
                    {
                        "action": "backend_release_metadata",
                        "actualCommitSha": payload["actualCommitSha"],
                        "expectedCommitSha": payload["expectedCommitSha"],
                        "status": "prepared",
                    },
                    sort_keys=True,
                ),
            )
        else:
            payload = confirm_metadata(
                arguments.path,
                commit=arguments.commit,
                service=arguments.service,
            )
            print(
                json.dumps(
                    {
                        "action": "backend_release_metadata",
                        "actualCommitSha": payload["actualCommitSha"],
                        "expectedCommitSha": payload["expectedCommitSha"],
                        "status": "confirmed",
                    },
                    sort_keys=True,
                ),
            )
    except ReleasePreparationError as exc:
        print(
            json.dumps(
                {
                    "action": "backend_release_preparation",
                    "error": str(exc),
                    "status": "failed",
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
