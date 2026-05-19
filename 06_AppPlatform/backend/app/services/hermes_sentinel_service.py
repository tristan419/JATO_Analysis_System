"""Hermes Sentinel — Unified Proactive Monitoring Center.

Runs probes across all modules, deduplicates findings, and generates
notifications. This is the ONLY module that proactively alerts.
All other modules only write facts (events, evidence, gaps).
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

_project_root: Path | None = None


def _root() -> Path:
    global _project_root
    if _project_root is None:
        from app.api.routes.hermes import PROJECT_ROOT
        _project_root = PROJECT_ROOT
    return _project_root


def _notifications_path() -> Path:
    return _root() / "hermes" / "sentinel_notifications.jsonl"


# ── Notification helpers ──────────────────────────────────────────────

def _load_notifications(limit: int = 100) -> list[dict[str, Any]]:
    p = _notifications_path()
    if not p.is_file():
        return []
    entries: list[dict] = []
    for line in p.read_text().strip().split("\n"):
        if line.strip():
            try:
                entries.append(json.loads(line))
            except Exception:
                pass
    entries.sort(key=lambda e: e.get("createdAt", ""), reverse=True)
    return entries[:limit]


def _save_notification(notif: dict[str, Any]) -> None:
    p = _notifications_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "a") as fh:
        fh.write(json.dumps(notif, ensure_ascii=False) + "\n")


def _write_notifications(notifs: list[dict[str, Any]]) -> None:
    p = _notifications_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(
        "\n".join(json.dumps(n, ensure_ascii=False) for n in notifs) + ("\n" if notifs else ""),
        encoding="utf-8",
    )


def _cooldown_ok(probe_name: str, minutes: int = 30) -> bool:
    """Check if enough time passed since last notification from this probe."""
    recent = [n for n in _load_notifications(20)
              if n.get("source") == probe_name and n.get("status") not in {"archived", "resolved"}]
    if not recent:
        return True
    newest = datetime.fromisoformat(recent[0]["createdAt"].replace("Z", "+00:00"))
    return datetime.now(timezone.utc) - newest > timedelta(minutes=minutes)


def _similar_notification_exists(title: str, within_minutes: int = 60) -> bool:
    """Avoid duplicate alerts with the same title."""
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=within_minutes)
    for n in _load_notifications(50):
        if n.get("title") == title:
            try:
                ct = datetime.fromisoformat(n["createdAt"].replace("Z", "+00:00"))
                if ct > cutoff:
                    return True
            except Exception:
                pass
    return False


def _emit(
    probe: str,
    severity: str,
    title: str,
    body: str,
    actions: list[str] | None = None,
    cooldown_min: int = 30,
    context: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Emit a notification if cooldown allows and no duplicate exists."""
    if not _cooldown_ok(probe, cooldown_min):
        return None
    if _similar_notification_exists(title, cooldown_min):
        return None
    context = context or {}
    action_level = str(
        context.get("actionLevel")
        or ("blocking" if severity in {"high", "critical"} else "needs_review")
    )
    blocking = bool(context.get("blocking", severity in {"high", "critical"}))
    recommended_action = str(
        context.get("recommendedAction")
        or (actions[0] if actions else "Review Sentinel finding")
    )
    notif = {
        "id": f"notif_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}",
        "severity": severity,
        "source": probe,
        "title": title,
        "body": body,
        "actions": actions or [],
        "actionLevel": action_level,
        "blocking": blocking,
        "recommendedAction": recommended_action,
        "context": context,
        "status": "new",
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }
    _save_notification(notif)
    return notif


# ── Probes ────────────────────────────────────────────────────────────

def probe_devsync() -> dict[str, Any]:
    """Check DevSync health: unlinked events, missing docs, missing tests."""
    from app.services.hermes_devsync_service import list_features, list_dev_events
    features = list_features()
    events = list_dev_events()

    findings: list[dict] = []
    missing_docs = [f for f in features if not (f.get("docs") or [])]
    missing_tests = [f for f in features if not (f.get("tests") or {})]

    if events:
        pushed_events = [e for e in events if e.get("source") == "git_commit"]
        unlinked = [e for e in pushed_events
                    if not (e.get("linkedFeatureIds") or [])][:5]

        if len(pushed_events) > 0 and len(unlinked) == len(pushed_events):
            findings.append({
                "type": "all_events_unlinked",
                "severity": "medium",
                "message": f"All {len(pushed_events)} pushed events have no linked features.",
                "count": len(pushed_events),
            })

    if missing_docs:
        findings.append({
            "type": "missing_docs",
            "severity": "medium" if len(missing_docs) < 5 else "high",
            "message": f"{len(missing_docs)} features have no documentation.",
            "count": len(missing_docs),
            "features": [f["featureId"] for f in missing_docs[:5]],
        })

    if missing_tests:
        findings.append({
            "type": "missing_tests",
            "severity": "medium" if len(missing_tests) < 5 else "high",
            "message": f"{len(missing_tests)} features have no test results.",
            "count": len(missing_tests),
            "features": [f["featureId"] for f in missing_tests[:5]],
        })

    overall = "ok"
    if any(f["severity"] == "high" for f in findings):
        overall = "critical"
    elif any(f["severity"] == "medium" for f in findings):
        overall = "warning"

    return {"probe": "devsync", "overall": overall, "findings": findings}


def probe_workspace() -> dict[str, Any]:
    """Check workspace: uncommitted files, unpushed commits."""
    import subprocess as _sp

    findings: list[dict] = []
    root = str(_root())

    try:
        r = _sp.run(["git", "diff", "--name-only"], cwd=root,
                     capture_output=True, text=True, timeout=10)
        changed = [f for f in r.stdout.strip().split("\n") if f]
        code_changed = [f for f in changed
                        if any(f.endswith(ext) for ext in
                               {".py", ".ts", ".tsx", ".yaml", ".json", ".css"})
                        and "node_modules" not in f and ".venv" not in f]

        if code_changed:
            dev_events_changed = any("dev_events.jsonl" in f for f in changed)
            if not dev_events_changed:
                findings.append({
                    "type": "unlinked_changes",
                    "severity": "high" if len(code_changed) > 10 else "medium",
                    "message": f"{len(code_changed)} code files changed without dev event update.",
                    "count": len(code_changed),
                })

        r2 = _sp.run(["git", "log", "origin/main..HEAD", "--oneline"], cwd=root,
                      capture_output=True, text=True, timeout=10)
        unpushed = [f for f in r2.stdout.strip().split("\n") if f]
        if len(unpushed) > 3:
            findings.append({
                "type": "unpushed_commits",
                "severity": "medium",
                "message": f"{len(unpushed)} unpushed commits.",
                "count": len(unpushed),
            })
    except Exception:
        pass

    overall = "ok"
    if any(f["severity"] == "high" for f in findings):
        overall = "critical"
    elif findings:
        overall = "warning"

    return {"probe": "workspace", "overall": overall, "findings": findings}


def probe_gaps() -> dict[str, Any]:
    """Check governance gaps: count by severity."""
    from app.services.hermes_devsync_service import _load_gaps
    gaps = _load_gaps()
    open_gaps = [g for g in gaps if g.get("status") == "open"]
    high = [g for g in open_gaps if g.get("severity") == "high"]
    medium = [g for g in open_gaps if g.get("severity") == "medium"]

    findings: list[dict] = []
    if high:
        findings.append({
            "type": "high_gaps",
            "severity": "high",
            "message": f"{len(high)} high-severity gaps open.",
            "count": len(high),
            "gaps": [g["gapId"] for g in high[:5]],
        })
    if medium:
        findings.append({
            "type": "medium_gaps",
            "severity": "medium",
            "message": f"{len(medium)} medium-severity gaps open.",
            "count": len(medium),
        })

    overall = "ok"
    if high:
        overall = "critical"
    elif medium:
        overall = "warning"

    return {"probe": "gaps", "overall": overall, "findings": findings}


def probe_evidence() -> dict[str, Any]:
    """Check evidence ledger freshness."""
    p = _root() / "hermes" / "evidence_ledger.jsonl"
    findings: list[dict] = []
    if not p.is_file():
        return {"probe": "evidence", "overall": "warning",
                "findings": [{"type": "no_evidence_file", "severity": "low",
                              "message": "No evidence ledger file found."}]}

    entries: list[dict] = []
    for line in p.read_text().strip().split("\n"):
        if line.strip():
            try:
                entries.append(json.loads(line))
            except Exception:
                pass

    if not entries:
        findings.append({"type": "empty_ledger", "severity": "low",
                         "message": "Evidence ledger is empty."})
    else:
        newest = entries[-1].get("createdAt", "")
        if newest:
            try:
                newest_dt = datetime.fromisoformat(newest.replace("Z", "+00:00"))
                age = datetime.now(timezone.utc) - newest_dt
                if age > timedelta(hours=48):
                    findings.append({
                        "type": "stale_evidence",
                        "severity": "medium",
                        "message": f"Evidence ledger last updated {age.days}d {age.seconds//3600}h ago.",
                    })
            except Exception:
                pass

    overall = "warning" if findings else "ok"
    return {"probe": "evidence", "overall": overall, "findings": findings}


def probe_gha() -> dict[str, Any]:
    """Check GitHub Actions health (best-effort, from evidence records)."""
    findings: list[dict] = []

    # Check if recent sync evidence exists
    p = _root() / "hermes" / "evidence_ledger.jsonl"
    has_sync_evidence = False
    if p.is_file():
        for line in p.read_text().strip().split("\n"):
            if "devsync" in line.lower() or "sync" in line.lower():
                try:
                    e = json.loads(line)
                    ct = e.get("createdAt", "")
                    if ct:
                        dt = datetime.fromisoformat(ct.replace("Z", "+00:00"))
                        if datetime.now(timezone.utc) - dt < timedelta(hours=24):
                            has_sync_evidence = True
                            break
                except Exception:
                    pass

    if not has_sync_evidence:
        findings.append({
            "type": "no_recent_sync",
            "severity": "low",
            "message": "No DevSync evidence in the last 24 hours.",
        })

    overall = "warning" if findings else "ok"
    return {"probe": "gha", "overall": overall, "findings": findings}


def probe_deploy() -> dict[str, Any]:
    """Check whether production release metadata matches latest expected commit."""
    from app.services.hermes_deploy_status_service import get_deploy_status

    status = get_deploy_status()
    findings: list[dict[str, Any]] = []
    drift = status.get("drift", {}) or {}
    release = status.get("release", {}) or {}
    expected = status.get("expected", {}) or {}
    last_deploy = status.get("lastDeploy", {}) or {}

    if drift.get("isDrift"):
        release_short = release.get("shortSha") or str(drift.get("releaseCommitSha", ""))[:8] or "unknown"
        expected_short = expected.get("shortSha") or str(drift.get("expectedCommitSha", ""))[:8] or "unknown"
        behind = drift.get("commitsBehind")
        behind_text = f" ({behind} commits behind)" if isinstance(behind, int) and behind > 0 else ""
        findings.append({
            "type": "production_commit_drift",
            "severity": "high",
            "message": f"Production release {release_short} does not match latest expected commit {expected_short}{behind_text}.",
            "releaseCommitSha": drift.get("releaseCommitSha"),
            "expectedCommitSha": drift.get("expectedCommitSha"),
            "count": behind,
            "recommendedAction": "Open deploy-fullstack-tencent logs and redeploy or fix SSH deploy failure.",
        })

    if drift.get("releaseUnknown"):
        findings.append({
            "type": "missing_release_metadata",
            "severity": "medium",
            "message": "Latest expected commit is known, but production release metadata is missing.",
            "expectedCommitSha": drift.get("expectedCommitSha"),
            "recommendedAction": "Deploy a package containing hermes/deploy_release.json.",
        })

    deploy_exit_code = last_deploy.get("deployExitCode")
    if deploy_exit_code not in {None, "", "0"}:
        findings.append({
            "type": "last_deploy_failed",
            "severity": "high",
            "message": f"Last deploy status reports exit code {deploy_exit_code}.",
            "recommendedAction": "Inspect _deploy_status.txt and the GitHub Actions SSH deploy step output.",
        })

    overall = "ok"
    if any(f["severity"] == "high" for f in findings):
        overall = "critical"
    elif findings:
        overall = "warning"

    return {"probe": "deploy", "overall": overall, "findings": findings, "status": status}


# ── Aggregation ───────────────────────────────────────────────────────

def run_all_probes() -> dict[str, Any]:
    """Run all probes and return aggregated status + notifications."""
    probes = [
        probe_devsync(),
        probe_workspace(),
        probe_gaps(),
        probe_evidence(),
        probe_gha(),
        probe_deploy(),
    ]

    # Determine overall severity
    overall = "ok"
    if any(p["overall"] == "critical" for p in probes):
        overall = "critical"
    elif any(p["overall"] == "warning" for p in probes):
        overall = "warning"

    # Emit notifications for non-ok findings
    emitted: list[dict] = []
    for p in probes:
        for f in p.get("findings", []):
            sev = f.get("severity", "low")
            context = {
                "findingType": f.get("type", ""),
                "actionLevel": "blocking" if sev in {"high", "critical"} else "needs_review",
                "blocking": sev in {"high", "critical"},
                "recommendedAction": f.get("recommendedAction") or "View details",
                "count": f.get("count"),
            }
            # Only emit for medium+ severity
            if sev in ("high", "critical"):
                n = _emit(p["probe"], sev, f.get("type", "").replace("_", " ").title(),
                          f.get("message", ""), ["View details"], cooldown_min=30,
                          context=context)
            elif sev == "medium":
                n = _emit(p["probe"], sev, f.get("type", "").replace("_", " ").title(),
                          f.get("message", ""), ["View details"], cooldown_min=120,
                          context=context)
            else:
                n = None
            if n:
                emitted.append(n)

    mailbox_notifications = _load_notifications(100)
    return {
        "overall": overall,
        "probes": probes,
        "notifications": mailbox_notifications,
        "emittedNotifications": emitted,
        "unreadCount": len([n for n in mailbox_notifications if n.get("status") == "new"]),
        "checkedAt": datetime.now(timezone.utc).isoformat(),
    }


def get_notifications(limit: int = 50, status: str | None = None) -> list[dict]:
    notifs = _load_notifications(limit)
    if status and status != "all":
        notifs = [n for n in notifs if n.get("status") == status]
    return notifs


def set_notification_status(notif_id: str, status: str) -> dict[str, Any] | None:
    """Set a notification mailbox status."""
    normalized_status = str(status or "").strip().lower()
    if normalized_status not in {"new", "read", "acked", "archived", "resolved"}:
        raise ValueError(f"Unsupported notification status: {status}")
    notifs = _load_notifications(200)
    for n in notifs:
        if n.get("id") == notif_id:
            n["status"] = normalized_status
            n["updatedAt"] = datetime.now(timezone.utc).isoformat()
            _write_notifications(notifs)
            return n
    return None


def ack_notification(notif_id: str) -> dict[str, Any] | None:
    """Mark a notification as acknowledged."""
    return set_notification_status(notif_id, "acked")
