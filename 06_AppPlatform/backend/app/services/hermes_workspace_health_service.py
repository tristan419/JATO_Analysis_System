"""Hermes workspace health service.

Centralises git subprocess queries used by both the ``/dev/workspace-health``
endpoint and the Sentinel ``probe_workspace`` probe, avoiding duplicated (and
slightly divergent) inline git commands.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

_project_root: Path | None = None

CODE_EXTS: frozenset[str] = frozenset(
    {".py", ".ts", ".tsx", ".yaml", ".yml", ".json", ".css", ".js"}
)


def _root() -> Path:
    global _project_root
    if _project_root is None:
        # Circular-import-safe lazy import — matches pattern in other hermes services.
        from app.api.routes.hermes import PROJECT_ROOT  # type: ignore[import]

        _project_root = PROJECT_ROOT
    return _project_root


def _git_lines(*args: str, timeout: int = 10) -> list[str]:
    """Run a git subcommand and return non-empty output lines.

    Returns an empty list on any failure (git unavailable, timeout, etc.).
    """
    try:
        r = subprocess.run(
            ["git", *args],
            cwd=str(_root()),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return [f for f in r.stdout.strip().split("\n") if f]
    except Exception:
        return []


def _is_code_file(path: str) -> bool:
    """True if *path* is a tracked code file (by extension)."""
    return (
        any(path.endswith(ext) for ext in CODE_EXTS)
        and "node_modules" not in path
        and ".venv" not in path
    )


def get_workspace_health() -> dict[str, Any]:
    """Assess local workspace health via git queries.

    Returns
    -------
    dict with keys:
        changedFiles       list[str]  — unstaged modified files
        stagedFiles        list[str]  — staged modified files
        committedUnpushed  list[str]  — commit subjects not pushed
        unlinkedChanges    int        — code files changed without dev_events.jsonl
        riskLevel          str        — ``"low"`` | ``"medium"`` | ``"high"``
        warnings           list[str]  — human-readable risk descriptions
        gitAvailable       bool       — whether git commands succeeded
    """
    try:
        changed = _git_lines("diff", "--name-only")
        staged = _git_lines("diff", "--cached", "--name-only")
        unpushed = _git_lines("log", "origin/main..HEAD", "--oneline")
        git_available = bool(_git_lines("rev-parse", "--git-dir"))
    except Exception:
        changed, staged, unpushed = [], [], []
        git_available = False

    all_changed = changed + staged
    code_changed = [f for f in all_changed if _is_code_file(f)]
    dev_events_changed = any("dev_events.jsonl" in f for f in all_changed)
    unlinked = len(code_changed) if not dev_events_changed else 0

    risk = "low"
    warnings: list[str] = []

    if unlinked > 10:
        risk = "high"
        warnings.append(f"{unlinked} code files changed without dev event update")
    elif unlinked > 3:
        risk = "medium"
        warnings.append(f"{unlinked} code files changed without dev event update")
    elif unlinked > 0:
        warnings.append("Some code changes not in dev events")

    if len(unpushed) > 3:
        risk = "medium" if risk == "low" else risk
        warnings.append(f"{len(unpushed)} unpushed commits")

    if not git_available:
        warnings.append("git unavailable — cannot assess workspace health")

    return {
        "changedFiles": changed,
        "stagedFiles": staged,
        "committedUnpushed": unpushed,
        "unlinkedChanges": unlinked,
        "riskLevel": risk,
        "warnings": warnings,
        "gitAvailable": git_available,
    }
