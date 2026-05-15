#!/usr/bin/env python3
"""Auto-generate a Hermes dev event from the latest git commit.

Works regardless of which tool made the change — Claude Code, other
agents, manual edits, etc.  All changes go through git, so the commit
is the single source of truth.

Usage:
    python hermes_dev_event_generator.py [--push] [--sync]

    --push   Also commit and push the updated dev_events.jsonl
    --sync   Also call POST /hermes/dev/sync on the running backend
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DEV_EVENTS_PATH = REPO_ROOT / "hermes" / "dev_events" / "dev_events.jsonl"

# File path patterns → likely feature areas
FEATURE_INFERENCE: list[tuple[list[str], str]] = [
    (["frontend/src/pages/DashboardPage", "frontend/src/pages/DataManagementPage"], "data-management-ui"),
    (["frontend/src/pages/CountryChatPage", "backend/app/services/country_chat"], "country-copilot"),
    (["frontend/src/pages/MarketScanPage", "backend/app/services/market_scan"], "market-scan"),
    (["frontend/src/pages/MsrpPage", "backend/app/api/routes/msrp"], "msrp-pricing"),
    (["frontend/src/pages/ReviewCasesPage", "backend/app/api/routes/review"], "review-workbench"),
    (["backend/app/api/routes/presence", "frontend/src/components/Layout", "presence"], "presence-websocket"),
    (["backend/app/services/hermes_chat_service", "backend/app/api/routes/hermes"], "hermes-chat-gateway"),
    (["backend/app/services/hermes_devsync_service"], "hermes-devsync"),
    (["07_ScrapingToolkit"], "scraping-toolkit"),
    (["03_Scripts/hermes"], "hermes-scripts"),
    (["airflow"], "airflow-pipelines"),
    (["frontend/"], "frontend"),
    (["backend/"], "backend"),
]


def run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, **kwargs)


def get_latest_commit_info() -> dict:
    """Extract commit metadata from the latest git commit."""
    result = run(["git", "log", "-1", "--format=%H%n%an%n%s%n%b"])
    lines = result.stdout.strip().split("\n")
    return {
        "hash": lines[0].strip() if len(lines) > 0 else "unknown",
        "author": lines[1].strip() if len(lines) > 1 else "unknown",
        "subject": lines[2].strip() if len(lines) > 2 else "no message",
        "body": "\n".join(lines[3:]).strip() if len(lines) > 3 else "",
    }


def get_changed_files() -> dict:
    """Get changed, added, and deleted files from the latest commit."""
    result = run(["git", "diff", "--name-status", "HEAD~1", "HEAD"])
    changed: list[str] = []
    added: list[str] = []
    deleted: list[str] = []

    for line in result.stdout.strip().split("\n"):
        if not line:
            continue
        parts = line.split("\t")
        status = parts[0][0]  # M, A, D, R, etc.
        fpath = parts[-1] if len(parts) > 1 else ""
        if not fpath:
            continue
        if status == "A":
            added.append(fpath)
        elif status == "D":
            deleted.append(fpath)
        else:
            changed.append(fpath)

    return {"changed": changed + added, "added": added, "deleted": deleted}


def infer_features(files: list[str], subject: str) -> list[str]:
    """Infer likely feature IDs from changed files and commit subject."""
    scores: dict[str, int] = {}
    for fpath in files:
        for patterns, feat_id in FEATURE_INFERENCE:
            for pat in patterns:
                if pat in fpath:
                    scores[feat_id] = scores.get(feat_id, 0) + 1
    # Deduplicate and sort by score
    ranked = sorted(scores.items(), key=lambda x: -x[1])
    features = [fid for fid, _ in ranked[:3]]
    if not features:
        # Fallback: slugify subject
        import re
        slug = re.sub(r"[^a-z0-9]+", "-", subject.lower()).strip("-")[:50]
        if slug:
            features = [slug]
    return features


def infer_event_type(subject: str) -> str:
    s = subject.lower()
    if any(w in s for w in ["fix", "bug", "修复", "修正"]):
        return "bug_fix"
    if any(w in s for w in ["refactor", "重构", "clean"]):
        return "refactor"
    if any(w in s for w in ["test", "测试"]):
        return "test_run"
    if any(w in s for w in ["doc", "readme", "文档"]):
        return "docs_update"
    return "implementation_completed"


def classify_endpoints(files: list[str]) -> dict:
    """Scan Python files for new/changed route decorators."""
    added_eps: list[str] = []
    updated_eps: list[str] = []
    # Only scan files in routes/
    route_files = [f for f in files if "routes" in f and f.endswith(".py")]
    for fpath in route_files:
        full_path = REPO_ROOT / fpath
        if not full_path.is_file():
            continue
        try:
            content = full_path.read_text()
        except Exception:
            continue
        import re
        for m in re.finditer(r'@router\.(get|post|put|patch|delete)\("([^"]+)"\)', content):
            method = m.group(1).upper()
            ep = f"{method} {m.group(2)}"
            added_eps.append(ep)
    return {"addedEndpoints": added_eps[:10], "updatedEndpoints": updated_eps[:10]}


def generate_event() -> dict:
    """Generate a dev event from the latest git commit."""
    commit = get_latest_commit_info()
    files = get_changed_files()
    all_files = files["changed"]
    features = infer_features(all_files, commit["subject"])
    event_type = infer_event_type(commit["subject"])
    endpoints = classify_endpoints(all_files)

    summary = commit["subject"]
    if commit["body"]:
        summary += "\n" + commit["body"][:500]

    event: dict = {
        "eventId": f"dev_evt_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}",
        "eventType": event_type,
        "source": "git_commit",
        "title": commit["subject"][:120],
        "summary": summary[:600],
        "linkedFeatureIds": features,
        "changedFiles": all_files,
        "addedFiles": files["added"],
        "deletedFiles": files["deleted"],
        "addedEndpoints": endpoints.get("addedEndpoints", []) or [],
        "frontendChanges": [f for f in all_files if "frontend" in f][:10],
        "backendChanges": [f for f in all_files if "backend" in f][:10],
        "tests": {},
        "risks": ["Auto-generated dev event from git commit — tests not auto-verified."],
        "nextSteps": ["Run tests", "Verify in Hermes UI Dev tab"],
        "createdAt": datetime.now(timezone.utc).isoformat(),
        "_gitCommit": commit["hash"][:8],
        "_gitAuthor": commit["author"],
    }
    return event


def write_event(event: dict) -> Path:
    """Append the dev event to the JSONL file."""
    DEV_EVENTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(DEV_EVENTS_PATH, "a") as fh:
        fh.write(json.dumps(event, ensure_ascii=False) + "\n")
    return DEV_EVENTS_PATH


def try_sync() -> bool:
    """Try to call POST /hermes/dev/sync on the running backend."""
    import urllib.request
    try:
        url = "http://127.0.0.1:8000/v1/hermes/dev/sync"
        req = urllib.request.Request(url, method="POST")
        urllib.request.urlopen(req, timeout=5)
        return True
    except Exception:
        return False


def main():
    do_push = "--push" in sys.argv
    do_sync = "--sync" in sys.argv

    event = generate_event()
    path = write_event(event)
    print(f"Dev event written: {event['eventId']}")
    print(f"  Title: {event['title']}")
    print(f"  Features: {event['linkedFeatureIds']}")
    print(f"  Files: {len(event['changedFiles'])} changed")
    print(f"  Path: {path}")

    if do_push:
        result = run(["git", "add", "hermes/dev_events/dev_events.jsonl"])
        if result.returncode == 0:
            run(["git", "commit", "-m", f"hermes: auto dev event {event['eventId']}"])
            run(["git", "push"])
            print("  Pushed dev event to remote.")

    if do_sync:
        ok = try_sync()
        if ok:
            print("  DevSync triggered successfully.")
        else:
            print("  DevSync: backend not reachable (this is OK — sync from UI Dev tab later).")


if __name__ == "__main__":
    main()
