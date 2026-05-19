"""Hermes deploy status helpers.

Tracks the deployed release artifact separately from git metadata. The Tencent
deploy workflow ships archive payloads without ``.git``, so runtime version
checks must prefer explicit release metadata over ``git rev-parse``.
"""

from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_project_root: Path | None = None


def _root() -> Path:
    global _project_root
    if _project_root is None:
        from app.api.routes.hermes import PROJECT_ROOT
        _project_root = PROJECT_ROOT
    return _project_root


def _deploy_release_path() -> Path:
    return _root() / "hermes" / "deploy_release.json"


def _deploy_expected_path() -> Path:
    return _root() / "hermes" / "deploy_expected.json"


def _deploy_status_path() -> Path:
    return _root() / "06_AppPlatform" / "frontend" / "dist" / "_deploy_status.txt"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"parseError": f"Unable to parse {path.name}"}
    return data if isinstance(data, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _clean_sha(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return "".join(ch for ch in text if ch.isalnum())[:40]


def _short_sha(value: Any) -> str:
    return _clean_sha(value)[:8]


def _sha_matches(left: str, right: str) -> bool:
    left_clean = _clean_sha(left)
    right_clean = _clean_sha(right)
    if not left_clean or not right_clean:
        return False
    if left_clean == right_clean:
        return True
    if min(len(left_clean), len(right_clean)) < 7:
        return False
    return left_clean.startswith(right_clean) or right_clean.startswith(left_clean)


def _current_git_sha() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(_root()),
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except Exception:
        return ""
    return _clean_sha(result.stdout) if result.returncode == 0 else ""


def _commits_between(base_sha: str, head_sha: str) -> int | None:
    base = _clean_sha(base_sha)
    head = _clean_sha(head_sha)
    if not base or not head or not (_root() / ".git").is_dir():
        return None
    try:
        result = subprocess.run(
            ["git", "rev-list", "--count", f"{base}..{head}"],
            cwd=str(_root()),
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except Exception:
        return None
    if result.returncode != 0:
        return None
    try:
        return int(result.stdout.strip())
    except ValueError:
        return None


def record_expected_deploy(payload: dict[str, Any], *, source: str | None = None) -> dict[str, Any] | None:
    """Record the latest commit that production is expected to run.

    DevSync calls this even when code deploy later fails, which gives Hermes a
    stable source of truth for detecting production drift.
    """
    commit_sha = _clean_sha(payload.get("commitSha") if isinstance(payload, dict) else "")
    if not commit_sha:
        return None

    record = {
        "commitSha": commit_sha,
        "shortSha": _short_sha(commit_sha),
        "branch": str(payload.get("branch") or "").strip() if isinstance(payload, dict) else "",
        "workflowRunId": str(payload.get("workflowRunId") or "").strip() if isinstance(payload, dict) else "",
        "workflowRunAttempt": str(payload.get("workflowRunAttempt") or "").strip() if isinstance(payload, dict) else "",
        "repository": str(payload.get("repository") or "").strip() if isinstance(payload, dict) else "",
        "source": source or str(payload.get("source") or "").strip() or "unknown",
        "receivedAt": _now_iso(),
    }
    _write_json(_deploy_expected_path(), record)
    return record


def get_release_metadata() -> dict[str, Any]:
    """Return the deployed release metadata, preferring archive metadata."""
    release = _read_json(_deploy_release_path())
    if release and not release.get("parseError"):
        commit_sha = _clean_sha(release.get("commitSha") or release.get("gitSha"))
        release["commitSha"] = commit_sha
        release["shortSha"] = release.get("shortSha") or _short_sha(commit_sha)
        release["source"] = release.get("source") or "deploy_release_file"
        release["metadataPath"] = str(_deploy_release_path().relative_to(_root()))
        release["confidence"] = "high"
        return release

    env_sha = _clean_sha(os.getenv("APP_GIT_SHA") or os.getenv("APP_RELEASE_SHA") or os.getenv("GITHUB_SHA"))
    if env_sha:
        return {
            "commitSha": env_sha,
            "shortSha": _short_sha(env_sha),
            "source": "environment",
            "confidence": "medium",
        }

    git_sha = _current_git_sha()
    if git_sha:
        return {
            "commitSha": git_sha,
            "shortSha": _short_sha(git_sha),
            "source": "git_metadata_fallback",
            "confidence": "low",
            "warning": "Git metadata may be stale for archive-based Tencent deploys.",
        }

    return {
        "commitSha": "",
        "shortSha": "",
        "source": "unknown",
        "confidence": "none",
    }


def get_expected_deploy() -> dict[str, Any]:
    expected = _read_json(_deploy_expected_path())
    if expected and not expected.get("parseError"):
        commit_sha = _clean_sha(expected.get("commitSha"))
        expected["commitSha"] = commit_sha
        expected["shortSha"] = expected.get("shortSha") or _short_sha(commit_sha)
        expected["metadataPath"] = str(_deploy_expected_path().relative_to(_root()))
        return expected
    return expected


def get_last_deploy_status() -> dict[str, Any]:
    path = _deploy_status_path()
    if not path.is_file():
        return {"available": False}

    text = path.read_text(encoding="utf-8", errors="replace")
    parsed: dict[str, Any] = {"available": True, "path": str(path.relative_to(_root()))}
    for line in text.splitlines():
        if line.startswith("deploy_exit_code="):
            parsed["deployExitCode"] = line.split("=", 1)[1].strip()
        elif line.startswith("timestamp="):
            parsed["timestamp"] = line.split("=", 1)[1].strip()
    parsed["summary"] = "\n".join(text.splitlines()[:24])
    return parsed


def get_deploy_status() -> dict[str, Any]:
    release = get_release_metadata()
    expected = get_expected_deploy()
    last_deploy = get_last_deploy_status()

    release_sha = release.get("commitSha", "")
    expected_sha = expected.get("commitSha", "")
    has_expected = bool(expected_sha)
    has_release = bool(release_sha)
    is_drift = bool(has_expected and has_release and not _sha_matches(release_sha, expected_sha))
    release_unknown = bool(has_expected and not has_release)
    last_deploy_failed = last_deploy.get("deployExitCode") not in {None, "", "0"} if last_deploy.get("available") else False

    warnings: list[str] = []
    if release.get("source") == "git_metadata_fallback":
        warnings.append("Release metadata file is missing; using git metadata fallback.")
    if release_unknown:
        warnings.append("Expected commit is known, but deployed release commit is unknown.")
    if is_drift:
        warnings.append("Deployed release does not match latest expected GitHub commit.")
    if last_deploy_failed:
        warnings.append("Last deploy status reports a non-zero deploy exit code.")

    commits_behind = _commits_between(release_sha, expected_sha) if is_drift else 0
    severity = "ok"
    if is_drift or last_deploy_failed:
        severity = "critical"
    elif release_unknown or warnings:
        severity = "warning"

    return {
        "status": severity,
        "release": release,
        "expected": expected,
        "drift": {
            "isDrift": is_drift,
            "releaseUnknown": release_unknown,
            "releaseCommitSha": release_sha,
            "expectedCommitSha": expected_sha,
            "commitsBehind": commits_behind,
        },
        "lastDeploy": last_deploy,
        "warnings": warnings,
        "checkedAt": _now_iso(),
    }
