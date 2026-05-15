#!/usr/bin/env python3
"""Check whether code changes have corresponding dev events.

Usage:
    python hermes_dev_event_check.py          # check (exit 1 if risk)
    python hermes_dev_event_check.py --warn   # warn only, never block
    python hermes_dev_event_check.py --json   # JSON output for automation
"""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DEV_EVENTS_PATH = REPO_ROOT / "hermes" / "dev_events" / "dev_events.jsonl"

CODE_EXTS = {".py", ".ts", ".tsx", ".yaml", ".yml", ".json", ".css", ".js"}


def run(cmd: list[str]) -> str:
    return subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True).stdout.strip()


def get_changed_code_files() -> list[str]:
    """Get all changed code files in working tree + staging."""
    files: list[str] = []
    for cmd in (["git", "diff", "--name-only"], ["git", "diff", "--cached", "--name-only"]):
        for f in run(cmd).split("\n"):
            f = f.strip()
            if f and any(f.endswith(ext) for ext in CODE_EXTS):
                if "node_modules" not in f and ".venv" not in f and "__pycache__" not in f:
                    files.append(f)
    return list(set(files))


def dev_events_changed() -> bool:
    """Check if dev_events.jsonl is among the changed files."""
    for cmd in (["git", "diff", "--name-only"], ["git", "diff", "--cached", "--name-only"]):
        if "dev_events.jsonl" in run(cmd):
            return True
    return False


def get_last_dev_event_time() -> str:
    """Get timestamp of the most recent dev event."""
    if not DEV_EVENTS_PATH.is_file():
        return "never"
    last_line = ""
    with open(DEV_EVENTS_PATH) as f:
        for line in f:
            if line.strip():
                last_line = line
    if not last_line:
        return "never"
    try:
        evt = json.loads(last_line)
        return evt.get("createdAt", "unknown")
    except Exception:
        return "unknown"


def main():
    warn_only = "--warn" in sys.argv
    json_out = "--json" in sys.argv

    changed = get_changed_code_files()
    has_dev_event = dev_events_changed()
    last_evt = get_last_dev_event_time()

    result = {
        "changedCodeFiles": len(changed),
        "devEventUpdated": has_dev_event,
        "lastDevEvent": last_evt,
        "riskLevel": "low",
        "action": "none",
    }

    if not changed:
        result["action"] = "clean"
    elif has_dev_event:
        result["action"] = "ok"
    elif len(changed) > 10:
        result["riskLevel"] = "high"
        result["action"] = "warn" if warn_only else "block"
    elif len(changed) > 3:
        result["riskLevel"] = "medium"
        result["action"] = "warn"
    else:
        result["riskLevel"] = "low"
        result["action"] = "warn"

    if json_out:
        print(json.dumps(result, indent=2))
        return 0 if result["action"] != "block" else 1

    if result["action"] == "clean":
        print("✅ No code changes detected.")
        return 0
    elif result["action"] == "ok":
        print("✅ Code changed and dev event updated.")
        return 0
    elif result["action"] == "block":
        print(f"⛔ {len(changed)} code files changed WITHOUT dev event update!")
        print("   Write a dev event to hermes/dev_events/dev_events.jsonl first.")
        print("   See: Markdown_Readme/Hermes/HERMES_CLAUDE_CODE_DEVSYNC_CONTRACT.md")
        return 1
    else:
        print(f"⚠️  {len(changed)} code files changed, no dev event update detected.")
        print(f"   Last dev event: {last_evt}")
        print("   Consider writing a dev event before commit.")
        return 0 if warn_only else 0  # never block for warn-only


if __name__ == "__main__":
    sys.exit(main())
