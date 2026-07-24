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


def normalize_commit_sha(value: Any) -> str:
    """Return a normalized commit identifier suitable for comparisons."""
    text = str(value or "").strip()
    if not text:
        return ""
    return "".join(ch for ch in text if ch.isalnum())[:40]


def commit_shas_match(left: str, right: str) -> bool:
    """Compare full or unambiguous abbreviated commit identifiers."""
    left_clean = normalize_commit_sha(left)
    right_clean = normalize_commit_sha(right)
    if not left_clean or not right_clean:
        return False
    if left_clean == right_clean:
        return True
    if min(len(left_clean), len(right_clean)) < 7:
        return False
    return left_clean.startswith(right_clean) or right_clean.startswith(left_clean)


def normalize_release_metadata(
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Normalize an already-read immutable release metadata payload."""
    if not isinstance(payload, dict):
        return {}
    release = dict(payload)

    expected_sha = normalize_commit_sha(
        release.get("expectedCommitSha")
        or release.get("commitSha")
        or release.get("commit")
    )
    actual_sha = normalize_commit_sha(
        release.get("actualCommitSha")
        or release.get("actualCommit")
        or release.get("commitSha")
        or release.get("commit")
        or release.get("gitSha")
    )
    release["expectedCommitSha"] = expected_sha
    release["actualCommitSha"] = actual_sha
    release["commitSha"] = actual_sha
    release["shortSha"] = release.get("shortSha") or _short_sha(actual_sha)
    release["actualShortSha"] = (
        release.get("actualShortSha") or _short_sha(actual_sha)
    )
    release["expectedShortSha"] = (
        release.get("expectedShortSha") or _short_sha(expected_sha)
    )
    release["source"] = release.get("source") or "deploy_release_file"
    release["environment"] = release.get("environment") or "production"
    release["confidence"] = "high"
    return release


def read_release_metadata_file(path: Path | None = None) -> dict[str, Any]:
    """Read and normalize immutable release metadata without fallbacks."""
    release_path = path or _deploy_release_path()
    release = _read_json(release_path)
    if not release or release.get("parseError"):
        return release
    return normalize_release_metadata(release)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _clean_sha(value: Any) -> str:
    return normalize_commit_sha(value)


def _short_sha(value: Any) -> str:
    return _clean_sha(value)[:8]


def _sha_matches(left: str, right: str) -> bool:
    return commit_shas_match(left, right)


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
    commit_sha = _clean_sha(
        (payload.get("expectedCommitSha") or payload.get("commitSha") or payload.get("commit"))
        if isinstance(payload, dict)
        else ""
    )
    if not commit_sha:
        return None

    record = {
        "commitSha": commit_sha,
        "expectedCommitSha": commit_sha,
        "shortSha": _short_sha(commit_sha),
        "branch": str(payload.get("branch") or "").strip() if isinstance(payload, dict) else "",
        "environment": str(payload.get("environment") or "production").strip() if isinstance(payload, dict) else "production",
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
    release = read_release_metadata_file()
    if release and not release.get("parseError"):
        release["metadataPath"] = str(_deploy_release_path().relative_to(_root()))
        return release

    env_sha = _clean_sha(os.getenv("APP_GIT_SHA") or os.getenv("APP_RELEASE_SHA") or os.getenv("GITHUB_SHA"))
    if env_sha:
        return {
            "commitSha": env_sha,
            "actualCommitSha": env_sha,
            "shortSha": _short_sha(env_sha),
            "actualShortSha": _short_sha(env_sha),
            "environment": "production",
            "source": "environment",
            "confidence": "medium",
        }

    git_sha = _current_git_sha()
    if git_sha:
        return {
            "commitSha": git_sha,
            "actualCommitSha": git_sha,
            "shortSha": _short_sha(git_sha),
            "actualShortSha": _short_sha(git_sha),
            "environment": "production",
            "source": "git_metadata_fallback",
            "confidence": "low",
            "warning": "Git metadata may be stale for archive-based Tencent deploys.",
        }

    return {
        "commitSha": "",
        "actualCommitSha": "",
        "shortSha": "",
        "actualShortSha": "",
        "environment": "production",
        "source": "unknown",
        "confidence": "none",
    }


def get_expected_deploy() -> dict[str, Any]:
    expected = _read_json(_deploy_expected_path())
    if expected and not expected.get("parseError"):
        commit_sha = _clean_sha(expected.get("expectedCommitSha") or expected.get("commitSha") or expected.get("commit"))
        expected["commitSha"] = commit_sha
        expected["expectedCommitSha"] = commit_sha
        expected["shortSha"] = expected.get("shortSha") or _short_sha(commit_sha)
        expected["environment"] = expected.get("environment") or "production"
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

    release_sha = release.get("actualCommitSha") or release.get("commitSha", "")
    expected_sha = expected.get("expectedCommitSha") or expected.get("commitSha", "")
    has_expected = bool(expected_sha)
    has_release = bool(release_sha)
    is_drift = bool(has_expected and has_release and not _sha_matches(release_sha, expected_sha))
    release_unknown = bool(has_expected and not has_release)
    last_deploy_failed = last_deploy.get("deployExitCode") not in {None, "", "0"} if last_deploy.get("available") else False
    commits_behind = _commits_between(release_sha, expected_sha) if is_drift else 0

    production_condition = {
        "id": "production_revision",
        "status": "ok",
        "type": "production_revision_ok",
        "message": "Production actual commit matches the expected commit.",
        "releaseCommitSha": release_sha,
        "actualCommitSha": release_sha,
        "expectedCommitSha": expected_sha,
        "commitsBehind": commits_behind,
        "releaseUnknown": release_unknown,
        "isDrift": is_drift,
        "environment": release.get("environment") or expected.get("environment") or "production",
        "releaseMetadataPath": release.get("metadataPath"),
        "expectedMetadataPath": expected.get("metadataPath"),
    }
    if is_drift:
        production_condition.update({
            "status": "critical",
            "type": "production_commit_drift",
            "message": "Production actual commit does not match the latest expected commit.",
        })
    elif release_unknown:
        production_condition.update({
            "status": "warning",
            "type": "missing_release_metadata",
            "message": "Expected commit is known, but production actual commit is unknown.",
        })
    elif not has_expected:
        production_condition.update({
            "status": "unknown",
            "type": "expected_commit_missing",
            "message": "No expected deploy commit has been recorded yet.",
        })

    deploy_pipeline_condition = {
        "id": "deploy_pipeline",
        "status": "ok" if last_deploy.get("available") else "unknown",
        "type": "last_deploy_ok" if last_deploy.get("available") else "last_deploy_status_missing",
        "message": "Last deploy status is successful." if last_deploy.get("available") else "No last deploy status file is available.",
        "deployExitCode": last_deploy.get("deployExitCode"),
        "lastDeployStatusPath": last_deploy.get("path"),
        "lastDeployTimestamp": last_deploy.get("timestamp"),
        "environment": release.get("environment") or expected.get("environment") or "production",
    }
    if last_deploy_failed:
        deploy_pipeline_condition.update({
            "status": "critical",
            "type": "last_deploy_failed",
            "message": "Last deploy status reports a non-zero deploy exit code.",
        })

    warnings: list[str] = []
    if release.get("source") == "git_metadata_fallback":
        warnings.append("Release metadata file is missing; using git metadata fallback.")
    if release_unknown:
        warnings.append("Expected commit is known, but production actual commit is unknown.")
    if is_drift:
        warnings.append("Production actual commit does not match latest expected GitHub commit.")
    if last_deploy_failed:
        warnings.append("Last deploy status reports a non-zero deploy exit code.")

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
            "actualCommitSha": release_sha,
            "expectedCommitSha": expected_sha,
            "commitsBehind": commits_behind,
        },
        "lastDeploy": last_deploy,
        "conditions": {
            "productionRevision": production_condition,
            "deployPipeline": deploy_pipeline_condition,
        },
        "warnings": warnings,
        "checkedAt": _now_iso(),
    }
