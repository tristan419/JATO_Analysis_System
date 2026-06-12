"""Hermes history and progress aggregation.

This service is intentionally read-only. It turns existing Hermes ledgers,
registries, Sentinel facts, deploy metadata, and recent git commits into
human-readable timeline and progress records for the Hermes cockpit UI.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_project_root: Path | None = None

PHASES = ["PRD", "Implemented", "Tested", "Deployed", "Verified", "Resolved"]
HISTORY_LEVELS = {"epic", "workstream", "feature", "session", "commit"}
Y_AXIS_MODES = {"workstream", "phase", "risk", "session"}

_RISK_ORDER = {"low": 0, "medium": 1, "high": 2, "critical": 3, "blocking": 4}


def _root() -> Path:
    global _project_root
    if _project_root is None:
        from app.api.routes.hermes import PROJECT_ROOT

        _project_root = PROJECT_ROOT
    return _project_root


def _hermes_dir() -> Path:
    return _root() / "hermes"


def _dev_events_path() -> Path:
    return _hermes_dir() / "dev_events" / "dev_events.jsonl"


def _evidence_ledger_path() -> Path:
    return _hermes_dir() / "evidence_ledger.jsonl"


def _gaps_path() -> Path:
    return _hermes_dir() / "governance_gaps.yaml"


def _sentinel_notifications_path() -> Path:
    return _hermes_dir() / "sentinel_notifications.jsonl"


def _devsync_features_path() -> Path:
    return _hermes_dir() / "registry" / "features.yaml"


def _kanban_features_path() -> Path:
    return _hermes_dir() / "feature_registry.yaml"


def _deploy_release_path() -> Path:
    return _hermes_dir() / "deploy_release.json"


def _deploy_expected_path() -> Path:
    return _hermes_dir() / "deploy_expected.json"


def _pipeline_status_dir() -> Path:
    return _hermes_dir() / "reports" / "pipeline_status"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _read_jsonl(path: Path, max_lines: int = 1000) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    records: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines()[-max_lines:]:
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except Exception:
            continue
        if isinstance(item, dict):
            records.append(item)
    return records


def _read_yaml_list(path: Path, key: str) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    try:
        import yaml

        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return []
    values = data.get(key, []) if isinstance(data, dict) else []
    return [item for item in values if isinstance(item, dict)]


def _parse_dt(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso(dt: datetime | None) -> str:
    if dt is None:
        return ""
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _timestamp_from_record(record: dict[str, Any]) -> datetime | None:
    for key in (
        "createdAt",
        "timestamp",
        "lastSeenAt",
        "updatedAt",
        "finishedAt",
        "lastRunAt",
        "deployedAt",
        "packagedAt",
    ):
        parsed = _parse_dt(record.get(key))
        if parsed:
            return parsed
    return None


def _string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if isinstance(value, dict):
        return [str(key) for key in value.keys() if str(key).strip()]
    if value:
        return [str(value)]
    return []


def _tests_count(value: Any) -> int:
    if isinstance(value, dict):
        return len([key for key, val in value.items() if str(key).strip() or str(val).strip()])
    if isinstance(value, list):
        return len(value)
    return 1 if value else 0


def _feature_key(feature_id: str) -> str:
    raw = str(feature_id or "").strip().lower()
    raw = re.sub(r"^feature[._-]", "", raw)
    return re.sub(r"[-_.]+", "-", raw).strip("-")


def _title_from_feature_id(feature_id: str) -> str:
    clean = _feature_key(feature_id)
    if not clean:
        return "Unmapped work"
    words = []
    for part in clean.split("-"):
        if part in {"api", "ci", "ui", "mcp"}:
            words.append(part.upper())
        elif part == "jato":
            words.append("JATO")
        elif part == "msrp":
            words.append("MSRP")
        else:
            words.append(part.capitalize())
    return " ".join(words)


def _append_unique(values: list[str], item: Any) -> None:
    text = str(item or "").strip()
    if text and text not in values:
        values.append(text)


def _contains_any(haystack: str, needles: tuple[str, ...]) -> bool:
    return any(needle in haystack for needle in needles)


def infer_workstream(feature_id: str = "", title: str = "", files: list[str] | None = None) -> str:
    files = files or []
    haystack = " ".join([feature_id, title, *files]).lower()
    if _contains_any(haystack, ("astrbot", "jato_agent", "countrycopilot", "country_copilot", "country_chat", "/mcp/")):
        return "AstrBot / CountryCopilot"
    if _contains_any(haystack, ("marketscan", "market_scan", "market-scan")):
        return "MarketScan"
    if _contains_any(haystack, ("jato_monthly", "monthly_update", "monthly-update", "msrp", "07_scrapingtoolkit", "scrapingtoolkit", "source_drafts")):
        return "JATO Monthly / MSRP"
    if _contains_any(haystack, ("hermes", "sentinel", "devsync", "governance")):
        return "Hermes"
    if _contains_any(haystack, ("engineering_config", "configcomparison", "configmatrix", "pageNavigation".lower(), "app.tsx")):
        return "Config Comparison"
    if _contains_any(haystack, (".github/workflows", "deploy", "pipeline", "systemd")):
        return "Deploy / CI"
    if _contains_any(haystack, ("markdown_readme", "/tests/", "test_", ".md")):
        return "Docs / Tests"
    if _contains_any(haystack, ("frontend/src", ".tsx", ".ts")):
        return "Frontend"
    if _contains_any(haystack, ("backend/app", ".py")):
        return "Backend"
    return "General"


def _infer_feature_id(title: str, files: list[str]) -> str:
    workstream = infer_workstream("", title, files)
    haystack = " ".join([title, *files]).lower()
    if workstream == "Hermes":
        if "history" in haystack or "progress" in haystack:
            return "proposal.hermes_history_progress_cockpit"
        if "sentinel" in haystack or "deploy" in haystack:
            return "feature.hermes_deploy_sentinel"
        return "hermes-devsync"
    if workstream == "JATO Monthly / MSRP":
        return "feature.jato_monthly_update"
    if workstream == "MarketScan":
        return "feature.market_scan"
    if workstream == "AstrBot / CountryCopilot":
        return "feature.country_copilot"
    if workstream == "Config Comparison":
        return "feature.engineering"
    slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
    return slug[:60] or "unmapped"


def _infer_phase_from_event(event_type: str, source: str, title: str, tests: Any, severity: str = "") -> str:
    text = f"{event_type} {source} {title}".lower()
    if severity in {"high", "critical", "blocking"} or "blocked" in text or "failed" in text:
        return "Blocked"
    if "deploy" in text or "release" in text:
        return "Deployed"
    if tests or "test" in text or "pytest" in text or "vitest" in text:
        return "Tested"
    if "evidence" in text or "verified" in text:
        return "Verified"
    if "resolved" in text or "done" in text:
        return "Resolved"
    if "implementation" in text or "feat" in text or "fix" in text or "commit" in text:
        return "Implemented"
    return "PRD"


def _risk_from_event(record: dict[str, Any]) -> str:
    severity = str(record.get("severity") or record.get("risk") or record.get("riskLevel") or "").lower()
    if severity in _RISK_ORDER:
        return severity
    risks = _string_list(record.get("risks"))
    if any("block" in item.lower() or "fail" in item.lower() for item in risks):
        return "medium"
    return "low"


def _status_from_phase_risk(phase: str, risk: str) -> str:
    if risk in {"high", "critical", "blocking"} or phase == "Blocked":
        return "blocking"
    if phase == "Resolved":
        return "resolved"
    if phase == "Verified":
        return "verified"
    if phase == "Deployed":
        return "deployed"
    if phase == "Tested":
        return "ready_for_pr"
    if phase == "Implemented":
        return "implemented"
    return "in_progress"


def _event_id(prefix: str, record: dict[str, Any]) -> str:
    for key in ("eventId", "id", "evidenceId", "runId"):
        value = str(record.get(key) or "").strip()
        if value:
            return value
    digest = hashlib.sha1(json.dumps(record, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:12]
    return f"{prefix}_{digest}"


def _event_from_dev_event(record: dict[str, Any]) -> dict[str, Any]:
    files = _string_list(record.get("changedFiles")) + _string_list(record.get("addedFiles"))
    linked_features = _string_list(record.get("linkedFeatureIds"))
    feature_id = linked_features[0] if linked_features else _infer_feature_id(str(record.get("title") or ""), files)
    source = str(record.get("source") or "devsync")
    event_type = str(record.get("eventType") or "dev_event")
    risk = _risk_from_event(record)
    phase = _infer_phase_from_event(event_type, source, str(record.get("title") or ""), record.get("tests"), risk)
    timestamp = _timestamp_from_record(record)
    session_id = str(record.get("sessionId") or record.get("branch") or record.get("_gitCommit") or source)
    return {
        "eventId": _event_id("dev", record),
        "timestamp": _iso(timestamp),
        "source": source,
        "type": event_type,
        "title": str(record.get("title") or event_type),
        "summary": str(record.get("summary") or ""),
        "featureId": feature_id,
        "workstream": infer_workstream(feature_id, str(record.get("title") or ""), files),
        "phase": phase,
        "risk": risk,
        "status": _status_from_phase_risk(phase, risk),
        "sessionId": session_id,
        "model": source.replace("_", " "),
        "commitSha": str(record.get("_gitCommit") or record.get("commitSha") or ""),
        "files": files[:40],
        "tests": _string_list(record.get("tests")),
        "testCount": _tests_count(record.get("tests")),
        "evidenceRefs": [],
        "gapRefs": [],
        "artifactRefs": [],
    }


def _event_from_evidence(record: dict[str, Any]) -> dict[str, Any]:
    artifact_id = str(record.get("artifactId") or "")
    feature_id = artifact_id.removeprefix("feature.") if artifact_id.startswith("feature.") else artifact_id
    if feature_id and not feature_id.startswith("feature.") and not feature_id.startswith("hermes"):
        feature_id = f"feature.{feature_id}"
    title = str(record.get("claim") or record.get("fact") or record.get("event") or "Evidence added")
    risk = "low"
    phase = "Verified"
    timestamp = _timestamp_from_record(record)
    return {
        "eventId": _event_id("evidence", record),
        "timestamp": _iso(timestamp),
        "source": "evidence",
        "type": str(record.get("evidenceType") or "evidence_added"),
        "title": title[:140],
        "summary": title,
        "featureId": feature_id or _infer_feature_id(title, []),
        "workstream": infer_workstream(feature_id, title, [str(record.get("sourceRef") or "")]),
        "phase": phase,
        "risk": risk,
        "status": "verified",
        "sessionId": "hermes-evidence",
        "model": "Hermes",
        "commitSha": "",
        "files": _string_list(record.get("sourceRef")),
        "tests": [],
        "testCount": 0,
        "evidenceRefs": _string_list(record.get("evidenceId")),
        "gapRefs": [],
        "artifactRefs": _string_list(record.get("artifactId")),
    }


def _event_from_sentinel(record: dict[str, Any]) -> dict[str, Any]:
    context = record.get("context") if isinstance(record.get("context"), dict) else {}
    severity = str(record.get("severity") or "medium").lower()
    title = str(record.get("title") or "Sentinel finding")
    timestamp = _timestamp_from_record(record)
    source = str(record.get("source") or "sentinel")
    feature_id = _infer_feature_id(title, _string_list(context.get("artifactRefs")))
    return {
        "eventId": _event_id("sentinel", record),
        "timestamp": _iso(timestamp),
        "source": "sentinel",
        "type": str(context.get("findingType") or source),
        "title": title,
        "summary": str(record.get("body") or ""),
        "featureId": feature_id,
        "workstream": infer_workstream(feature_id, title, _string_list(context.get("artifactRefs"))),
        "phase": "Blocked" if severity in {"high", "critical"} else "PRD",
        "risk": "blocking" if bool(context.get("blocking")) else severity,
        "status": "blocking" if severity in {"high", "critical"} else "needs_review",
        "sessionId": "sentinel",
        "model": "Hermes Sentinel",
        "commitSha": "",
        "files": _string_list(context.get("statusPath")),
        "tests": [],
        "testCount": 0,
        "evidenceRefs": [],
        "gapRefs": [],
        "artifactRefs": _string_list(context.get("artifactRefs")),
    }


def _event_from_pipeline(record: dict[str, Any]) -> dict[str, Any]:
    status = str(record.get("status") or "unknown").lower()
    pipeline_id = str(record.get("pipelineId") or record.get("id") or "pipeline")
    risk = "high" if status in {"failed", "failure", "critical"} else "medium" if status in {"warning", "missing"} else "low"
    timestamp = _timestamp_from_record(record)
    return {
        "eventId": f"pipeline_{pipeline_id}",
        "timestamp": _iso(timestamp),
        "source": "pipeline",
        "type": f"pipeline_{status}",
        "title": f"Pipeline {pipeline_id} {status}",
        "summary": str(record.get("message") or ""),
        "featureId": _infer_feature_id(pipeline_id, _string_list(record.get("artifactRefs"))),
        "workstream": infer_workstream("", pipeline_id, _string_list(record.get("artifactRefs"))),
        "phase": "Blocked" if risk == "high" else "Deployed",
        "risk": risk,
        "status": "blocking" if risk == "high" else status,
        "sessionId": "pipeline",
        "model": "GitHub Actions",
        "commitSha": "",
        "files": _string_list(record.get("statusPath")),
        "tests": [],
        "testCount": 0,
        "evidenceRefs": [],
        "gapRefs": [],
        "artifactRefs": _string_list(record.get("artifactRefs")),
    }


def _event_from_deploy(record: dict[str, Any], event_id: str, title: str) -> dict[str, Any]:
    status = str(record.get("status") or record.get("healthz") or "unknown").lower()
    risk = "low" if status in {"ok", "success"} else "medium"
    timestamp = _timestamp_from_record(record)
    commit_sha = str(record.get("actualCommitSha") or record.get("expectedCommitSha") or record.get("commitSha") or "")
    return {
        "eventId": event_id,
        "timestamp": _iso(timestamp),
        "source": "deploy",
        "type": "deploy_metadata",
        "title": title,
        "summary": str(record.get("source") or record.get("deployMethod") or ""),
        "featureId": "deploy-fullstack-tencent",
        "workstream": "Deploy / CI",
        "phase": "Deployed",
        "risk": risk,
        "status": status or "unknown",
        "sessionId": "deploy",
        "model": "GitHub Actions",
        "commitSha": commit_sha,
        "files": ["hermes/deploy_release.json"],
        "tests": [],
        "testCount": 0,
        "evidenceRefs": [],
        "gapRefs": [],
        "artifactRefs": [],
    }


def _git_commit_events(max_commits: int = 80) -> list[dict[str, Any]]:
    try:
        proc = subprocess.run(
            [
                "git",
                "-C",
                str(_root()),
                "log",
                f"--max-count={max_commits}",
                "--date=iso-strict",
                "--pretty=format:%x1e%H%x1f%h%x1f%aI%x1f%an%x1f%s",
                "--name-only",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except Exception:
        return []
    if proc.returncode != 0:
        return []

    events: list[dict[str, Any]] = []
    for block in proc.stdout.split("\x1e"):
        lines = [line for line in block.splitlines() if line.strip()]
        if not lines:
            continue
        header = lines[0].split("\x1f")
        if len(header) < 5:
            continue
        sha, short_sha, authored_at, author, subject = header[:5]
        files = lines[1:]
        feature_id = _infer_feature_id(subject, files)
        risk = "medium" if subject.lower().startswith(("revert", "hotfix")) else "low"
        phase = _infer_phase_from_event("commit", "git", subject, None, risk)
        events.append({
            "eventId": f"git_{short_sha}",
            "timestamp": _iso(_parse_dt(authored_at)),
            "source": "git",
            "type": "commit",
            "title": subject,
            "summary": subject,
            "featureId": feature_id,
            "workstream": infer_workstream(feature_id, subject, files),
            "phase": phase,
            "risk": risk,
            "status": _status_from_phase_risk(phase, risk),
            "sessionId": author or "git",
            "model": "Manual / Git",
            "commitSha": sha,
            "files": files[:40],
            "tests": [path for path in files if "/test" in path or path.startswith("tests/")][:20],
            "testCount": len([path for path in files if "/test" in path or path.startswith("tests/")]),
            "evidenceRefs": [],
            "gapRefs": [],
            "artifactRefs": [],
        })
    return events


def _pipeline_events() -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if not _pipeline_status_dir().is_dir():
        return events
    for path in sorted(_pipeline_status_dir().glob("*.json")):
        record = _read_json(path)
        if record:
            record.setdefault("statusPath", str(path.relative_to(_root())))
            events.append(_event_from_pipeline(record))
    return events


def _deploy_events() -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    release = _read_json(_deploy_release_path())
    if release:
        events.append(_event_from_deploy(release, "deploy_release", "Production release metadata recorded"))
    expected = _read_json(_deploy_expected_path())
    if expected:
        events.append(_event_from_deploy(expected, "deploy_expected", "Expected production commit recorded"))
    return events


def _event_from_usage_record(record: dict[str, Any]) -> dict[str, Any]:
    usage_source = str(record.get("usageSource") or "usage")
    mode = str(record.get("answerMode") or record.get("type") or "model_run")
    model = str(record.get("model") or record.get("modelUsed") or "unknown")
    title = f"{usage_source} {mode} usage"
    files = _string_list(record.get("retrievalPaths")) + _string_list(record.get("files"))
    feature_id = _infer_feature_id(f"{title} {model}", files)
    timestamp = _timestamp_from_record(record)
    return {
        "eventId": _event_id("usage", record),
        "timestamp": _iso(timestamp),
        "source": "usage",
        "type": "model_run",
        "title": title,
        "summary": f"{model} cost {float(record.get('estimatedCostCny') or 0):.4f} CNY",
        "featureId": feature_id,
        "workstream": infer_workstream(feature_id, title, files),
        "phase": "Implemented",
        "risk": "low",
        "status": "implemented",
        "sessionId": str(record.get("sessionId") or usage_source),
        "model": model,
        "commitSha": "",
        "files": files[:40],
        "tests": [],
        "testCount": 0,
        "evidenceRefs": _string_list(record.get("answerId") or record.get("usageId")),
        "gapRefs": [],
        "artifactRefs": _string_list(record.get("toolsUsed")),
    }


def _usage_events() -> list[dict[str, Any]]:
    try:
        from app.services.hermes_cost_ledger_service import load_cost_records

        records = load_cost_records(_root())
    except Exception:
        return []
    return [
        _event_from_usage_record(record)
        for record in records
        if record.get("createdAt") or record.get("recordedAt")
    ]


def _all_history_events(max_git_commits: int = 80) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    events.extend(_git_commit_events(max_commits=max_git_commits))
    events.extend(_event_from_dev_event(item) for item in _read_jsonl(_dev_events_path(), max_lines=700))
    events.extend(_event_from_evidence(item) for item in _read_jsonl(_evidence_ledger_path(), max_lines=600))
    events.extend(_event_from_sentinel(item) for item in _read_jsonl(_sentinel_notifications_path(), max_lines=300))
    events.extend(_pipeline_events())
    events.extend(_deploy_events())
    events.extend(_usage_events())

    deduped: dict[str, dict[str, Any]] = {}
    for event in events:
        event_id = str(event.get("eventId") or "")
        if event_id:
            deduped[event_id] = event
    return sorted(
        deduped.values(),
        key=lambda event: event.get("timestamp") or "",
        reverse=True,
    )


def list_history_events(
    source: str | None = None,
    workstream: str | None = None,
    model: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    events = _all_history_events()
    if source:
        events = [event for event in events if str(event.get("source")) == source]
    if workstream:
        events = [event for event in events if str(event.get("workstream")) == workstream]
    if model:
        needle = model.lower()
        events = [event for event in events if needle in str(event.get("model") or "").lower()]
    events = events[:limit]
    return {
        "summary": _history_summary(events),
        "events": events,
    }


def _history_summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "totalEvents": len(events),
        "sources": dict(Counter(str(event.get("source") or "unknown") for event in events)),
        "workstreams": dict(Counter(str(event.get("workstream") or "General") for event in events)),
        "risks": dict(Counter(str(event.get("risk") or "low") for event in events)),
        "models": dict(Counter(str(event.get("model") or "unknown") for event in events)),
    }


def _bucket_key(event: dict[str, Any], level: str) -> str:
    dt = _parse_dt(event.get("timestamp"))
    date_key = dt.date().isoformat() if dt else "unknown-date"
    week_key = f"{dt.isocalendar().year}-W{dt.isocalendar().week:02d}" if dt else "unknown-week"
    month_key = f"{dt.year}-{dt.month:02d}" if dt else "unknown-month"
    workstream = str(event.get("workstream") or "General")
    feature_id = str(event.get("featureId") or "unmapped")
    session_id = str(event.get("sessionId") or "unknown")
    if level == "epic":
        return f"{month_key}|{workstream}"
    if level == "workstream":
        return f"{week_key}|{workstream}"
    if level == "session":
        return f"{date_key}|{session_id}|{feature_id}"
    if level == "commit":
        return str(event.get("eventId") or f"{date_key}|{feature_id}")
    return f"{date_key}|{feature_id}|{workstream}"


def _axis_value(event: dict[str, Any], y_axis: str) -> str:
    if y_axis == "phase":
        return str(event.get("phase") or "PRD")
    if y_axis == "risk":
        return str(event.get("risk") or "low")
    if y_axis == "session":
        return str(event.get("sessionId") or "unknown")
    return str(event.get("workstream") or "General")


def _cluster_title(events: list[dict[str, Any]], level: str) -> str:
    first = events[0]
    if level == "commit":
        return str(first.get("title") or first.get("eventId"))
    if level in {"epic", "workstream"}:
        return str(first.get("workstream") or "General")
    feature_id = str(first.get("featureId") or "")
    if feature_id:
        return _title_from_feature_id(feature_id)
    return str(first.get("title") or "Unmapped work")


def _highest_risk(events: list[dict[str, Any]]) -> str:
    return max(
        (str(event.get("risk") or "low") for event in events),
        key=lambda risk: _RISK_ORDER.get(risk, 0),
        default="low",
    )


def _dominant_value(events: list[dict[str, Any]], key: str) -> str:
    values = [str(event.get(key) or "") for event in events if event.get(key)]
    if not values:
        return ""
    return Counter(values).most_common(1)[0][0]


def _status_for_cluster(events: list[dict[str, Any]]) -> str:
    risk = _highest_risk(events)
    if risk in {"high", "critical", "blocking"}:
        return "blocking"
    phases = {str(event.get("phase") or "") for event in events}
    for phase, status in (
        ("Resolved", "resolved"),
        ("Verified", "verified"),
        ("Deployed", "deployed"),
        ("Tested", "ready_for_pr"),
        ("Implemented", "implemented"),
    ):
        if phase in phases:
            return status
    return "in_progress"


def _cluster_from_events(cluster_id: str, level: str, y_axis: str, events: list[dict[str, Any]]) -> dict[str, Any]:
    sorted_events = sorted(events, key=lambda event: event.get("timestamp") or "")
    start = sorted_events[0].get("timestamp") if sorted_events else ""
    end = sorted_events[-1].get("timestamp") if sorted_events else ""
    files: list[str] = []
    sources: list[str] = []
    children: list[str] = []
    for event in sorted_events:
        _append_unique(children, event.get("eventId"))
        _append_unique(sources, event.get("source"))
        for path in event.get("files", []) or []:
            _append_unique(files, path)
    risk = _highest_risk(events)
    return {
        "clusterId": cluster_id,
        "level": level,
        "yAxis": y_axis,
        "lane": _axis_value(events[0], y_axis) if events else "General",
        "startAt": start,
        "endAt": end,
        "title": _cluster_title(events, level),
        "workstream": _dominant_value(events, "workstream") or "General",
        "phase": _dominant_value(events, "phase") or "PRD",
        "risk": risk,
        "status": _status_for_cluster(events),
        "eventCount": len(events),
        "commitCount": sum(1 for event in events if event.get("source") == "git" or event.get("commitSha")),
        "testCount": sum(int(event.get("testCount") or 0) for event in events),
        "evidenceCount": sum(len(event.get("evidenceRefs", []) or []) for event in events),
        "gapCount": sum(len(event.get("gapRefs", []) or []) for event in events),
        "sources": sources,
        "children": children,
        "topFiles": files[:8],
    }


def list_history_clusters(
    level: str = "feature",
    y_axis: str = "workstream",
    workstream: str | None = None,
    limit: int = 160,
) -> dict[str, Any]:
    if level not in HISTORY_LEVELS:
        level = "feature"
    if y_axis not in Y_AXIS_MODES:
        y_axis = "workstream"
    events = list_history_events(workstream=workstream, limit=500)["events"]
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        grouped[_bucket_key(event, level)].append(event)
    clusters = [
        _cluster_from_events(f"cluster_{hashlib.sha1(key.encode('utf-8')).hexdigest()[:12]}", level, y_axis, values)
        for key, values in grouped.items()
    ]
    clusters.sort(key=lambda cluster: cluster.get("startAt") or "", reverse=True)
    clusters = clusters[:limit]
    lanes = sorted({str(cluster.get("lane") or "General") for cluster in clusters})
    return {
        "summary": {
            **_history_summary(events),
            "level": level,
            "yAxis": y_axis,
            "clusterCount": len(clusters),
            "lanes": lanes,
        },
        "clusters": clusters,
    }


def _load_feature_records() -> list[dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}

    for feature in _read_yaml_list(_devsync_features_path(), "features"):
        fid = str(feature.get("featureId") or "")
        if not fid:
            continue
        records[_feature_key(fid)] = dict(feature)

    for feature in _read_yaml_list(_kanban_features_path(), "features"):
        fid = str(feature.get("featureId") or "")
        if not fid:
            continue
        key = _feature_key(fid)
        current = records.get(key, {})
        merged = {**feature, **current}
        merged["featureId"] = current.get("featureId") or feature.get("featureId")
        merged["title"] = current.get("title") or feature.get("name") or feature.get("title") or _title_from_feature_id(fid)
        for list_key in ("docs", "tests", "backendApis", "routes", "knownIssues", "dependencies"):
            merged_values: list[str] = []
            for item in _string_list(feature.get(list_key)) + _string_list(current.get(list_key)):
                _append_unique(merged_values, item)
            if merged_values:
                merged[list_key] = merged_values
        records[key] = merged

    return list(records.values())


def _feature_refs_from_gap(gap: dict[str, Any]) -> list[str]:
    refs = _string_list(gap.get("affectedAssets")) + _string_list(gap.get("affectedFeatures"))
    if gap.get("featureId"):
        refs.append(str(gap["featureId"]))
    gap_id = str(gap.get("gapId") or "")
    if gap_id.startswith("gap.devsync."):
        refs.append(gap_id.removeprefix("gap.devsync.").rsplit(".", 1)[0])
    return refs


def _evidence_feature_ref(record: dict[str, Any]) -> str:
    artifact_id = str(record.get("artifactId") or "")
    if artifact_id.startswith("feature."):
        return artifact_id.removeprefix("feature.")
    source_ref = str(record.get("sourceRef") or "")
    if "::" in source_ref:
        return source_ref.split("::", 1)[-1]
    return artifact_id


def _phase_statuses(
    feature: dict[str, Any],
    events: list[dict[str, Any]],
    evidence: list[dict[str, Any]],
    open_gaps: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    status = str(feature.get("status") or feature.get("implementationStatus") or "").lower()
    implementation_status = str(feature.get("implementationStatus") or "").lower()
    has_implemented = status in {"implemented", "verified", "done", "active"} or implementation_status in {"implemented", "deployed"}
    has_tests = bool(_string_list(feature.get("tests"))) or any(int(event.get("testCount") or 0) > 0 for event in events)
    has_deploy = status in {"active", "deployed", "verified", "done"} or implementation_status == "deployed"
    has_evidence = bool(evidence) or status in {"verified", "done"}
    has_resolved = has_evidence and not open_gaps
    complete = {
        "PRD": True,
        "Implemented": has_implemented,
        "Tested": has_tests,
        "Deployed": has_deploy,
        "Verified": has_evidence,
        "Resolved": has_resolved,
    }
    statuses: list[dict[str, Any]] = []
    for phase in PHASES:
        phase_events = [event for event in events if event.get("phase") == phase]
        timestamp = phase_events[0].get("timestamp") if phase_events else ""
        state = "complete" if complete[phase] else "attention" if phase == "Resolved" and open_gaps else "pending"
        statuses.append({
            "phase": phase,
            "status": state,
            "timestamp": timestamp,
            "eventIds": [str(event.get("eventId")) for event in phase_events[:5]],
        })
    return statuses


def _current_phase(phase_statuses: list[dict[str, Any]]) -> str:
    completed = [item["phase"] for item in phase_statuses if item.get("status") == "complete"]
    return completed[-1] if completed else "PRD"


def _next_action(current_phase: str, open_gap_count: int, has_tests: bool, has_evidence: bool) -> str:
    if open_gap_count:
        return "Review blocking gaps and attach evidence before closing."
    if current_phase == "Implemented" and not has_tests:
        return "Run targeted backend/frontend tests and record evidence."
    if current_phase == "Tested":
        return "Push branch, open PR, then deploy through the tracked pipeline."
    if current_phase == "Deployed" and not has_evidence:
        return "Attach smoke evidence and verify the deployed behavior."
    if current_phase in {"Verified", "Resolved"}:
        return "Keep monitoring Sentinel and pipeline status."
    return "Record PRD or implementation evidence."


def list_progress_features() -> list[dict[str, Any]]:
    features = _load_feature_records()
    events = _all_history_events()
    evidence_records = _read_jsonl(_evidence_ledger_path(), max_lines=1200)
    gaps = _read_yaml_list(_gaps_path(), "gaps")

    events_by_feature: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        events_by_feature[_feature_key(str(event.get("featureId") or ""))].append(event)

    evidence_by_feature: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in evidence_records:
        ref = _evidence_feature_ref(record)
        if ref:
            evidence_by_feature[_feature_key(ref)].append(record)

    gaps_by_feature: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for gap in gaps:
        for ref in _feature_refs_from_gap(gap):
            gaps_by_feature[_feature_key(ref)].append(gap)

    progress: list[dict[str, Any]] = []
    for feature in features:
        fid = str(feature.get("featureId") or "")
        key = _feature_key(fid)
        title = str(feature.get("title") or feature.get("name") or _title_from_feature_id(fid))
        feature_events = sorted(events_by_feature.get(key, []), key=lambda event: event.get("timestamp") or "", reverse=True)
        feature_evidence = evidence_by_feature.get(key, [])
        feature_gaps = gaps_by_feature.get(key, [])
        open_gaps = [gap for gap in feature_gaps if str(gap.get("status") or "open") != "resolved"]
        phases = _phase_statuses(feature, feature_events, feature_evidence, open_gaps)
        current_phase = _current_phase(phases)
        tests = _string_list(feature.get("tests"))
        docs = _string_list(feature.get("docs"))
        risk = str(feature.get("riskLevel") or "low").lower()
        if any(str(gap.get("severity") or "").lower() in {"high", "critical"} for gap in open_gaps):
            risk = "blocking"
        if risk not in _RISK_ORDER:
            risk = "low"
        has_tests = bool(tests) or any(int(event.get("testCount") or 0) > 0 for event in feature_events)
        has_evidence = bool(feature_evidence)
        status = _status_from_phase_risk(current_phase, risk)
        if open_gaps and risk in {"high", "critical", "blocking"}:
            status = "blocking"
        progress.append({
            "featureId": fid,
            "title": title,
            "workstream": infer_workstream(fid, title, _string_list(feature.get("frontend")) + _string_list(feature.get("backend"))),
            "phase": current_phase,
            "status": status,
            "risk": risk,
            "owner": str(feature.get("owner") or ""),
            "sessionId": str(feature_events[0].get("sessionId") or "") if feature_events else "",
            "lastEventAt": str(feature_events[0].get("timestamp") or "") if feature_events else str(feature.get("lastUpdatedAt") or feature.get("lastAuditAt") or ""),
            "lastMeaningfulEvent": str(feature_events[0].get("title") or "") if feature_events else str(feature.get("summary") or ""),
            "evidenceCount": len(feature_evidence),
            "openGapCount": len(open_gaps),
            "testsCount": len(tests) or sum(int(event.get("testCount") or 0) for event in feature_events),
            "docsCount": len(docs),
            "commitCount": sum(1 for event in feature_events if event.get("source") == "git" or event.get("commitSha")),
            "deployStatus": "tracked" if current_phase in {"Deployed", "Verified", "Resolved"} else "not_deployed",
            "nextAction": _next_action(current_phase, len(open_gaps), has_tests, has_evidence),
            "phases": phases,
            "evidenceRefs": [str(record.get("evidenceId") or "") for record in feature_evidence[:8]],
            "gapRefs": [str(gap.get("gapId") or gap.get("title") or "") for gap in open_gaps[:8]],
            "topFiles": _string_list(feature.get("frontend"))[:4] + _string_list(feature.get("backend"))[:4],
        })

    status_rank = {
        "blocking": 0,
        "ready_for_pr": 1,
        "implemented": 2,
        "deployed": 3,
        "verified": 4,
        "resolved": 5,
    }
    progress.sort(key=lambda item: str(item.get("lastEventAt") or ""), reverse=True)
    progress.sort(key=lambda item: status_rank.get(str(item.get("status") or ""), 6))
    return progress


def get_progress_swimlanes() -> dict[str, Any]:
    features = list_progress_features()
    lanes_map: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for feature in features:
        lanes_map[str(feature.get("workstream") or "General")].append(feature)
    lanes = [
        {"workstream": workstream, "features": lane_features}
        for workstream, lane_features in sorted(lanes_map.items())
    ]
    return {
        "summary": {
            "total": len(features),
            "blocking": sum(1 for feature in features if feature.get("status") == "blocking"),
            "readyForPr": sum(1 for feature in features if feature.get("status") == "ready_for_pr"),
            "deployed": sum(1 for feature in features if feature.get("status") in {"deployed", "verified", "resolved"}),
            "verified": sum(1 for feature in features if feature.get("status") in {"verified", "resolved"}),
            "workstreamCount": len(lanes),
        },
        "phases": PHASES,
        "lanes": lanes,
    }


def _top_values(events: list[dict[str, Any]], key: str, limit: int = 8) -> list[str]:
    counter: Counter[str] = Counter()
    for event in events:
        value = event.get(key)
        if isinstance(value, list):
            counter.update(str(item) for item in value if str(item).strip())
        elif value:
            counter[str(value)] += 1
    return [item for item, _count in counter.most_common(limit)]


def _session_workflow_record(session_id: str, events: list[dict[str, Any]]) -> dict[str, Any]:
    sorted_events = sorted(events, key=lambda event: event.get("timestamp") or "", reverse=True)
    latest = sorted_events[0] if sorted_events else {}
    risk = _highest_risk(events)
    return {
        "sessionId": session_id,
        "model": _dominant_value(events, "model") or "unknown",
        "status": _status_for_cluster(events),
        "risk": risk,
        "latestAt": str(latest.get("timestamp") or ""),
        "lastEventTitle": str(latest.get("title") or ""),
        "eventCount": len(events),
        "commitCount": sum(1 for event in events if event.get("source") == "git" or event.get("commitSha")),
        "testCount": sum(int(event.get("testCount") or 0) for event in events),
        "evidenceCount": sum(len(event.get("evidenceRefs", []) or []) for event in events),
        "gapCount": sum(len(event.get("gapRefs", []) or []) for event in events),
        "sources": _top_values(events, "source", limit=6),
        "workstreams": _top_values(events, "workstream", limit=6),
        "featureIds": _top_values(events, "featureId", limit=8),
        "topFiles": _top_values(events, "files", limit=8),
        "events": [
            {
                "eventId": str(event.get("eventId") or ""),
                "timestamp": str(event.get("timestamp") or ""),
                "source": str(event.get("source") or ""),
                "type": str(event.get("type") or ""),
                "title": str(event.get("title") or ""),
                "featureId": str(event.get("featureId") or ""),
                "workstream": str(event.get("workstream") or ""),
                "commitSha": str(event.get("commitSha") or ""),
            }
            for event in sorted_events[:8]
        ],
    }


def _model_workflow_record(model: str, sessions: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "model": model,
        "sessionCount": len(sessions),
        "eventCount": sum(int(session.get("eventCount") or 0) for session in sessions),
        "commitCount": sum(int(session.get("commitCount") or 0) for session in sessions),
        "testCount": sum(int(session.get("testCount") or 0) for session in sessions),
        "latestAt": max((str(session.get("latestAt") or "") for session in sessions), default=""),
        "workstreams": sorted({
            workstream
            for session in sessions
            for workstream in session.get("workstreams", [])
            if str(workstream).strip()
        }),
    }


def _workflow_review_items(
    sessions: list[dict[str, Any]],
    features: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for feature in features:
        status = str(feature.get("status") or "")
        if status in {"blocking", "ready_for_pr"} or int(feature.get("openGapCount") or 0) > 0:
            items.append({
                "kind": "feature",
                "priority": "high" if status == "blocking" else "medium",
                "title": str(feature.get("title") or feature.get("featureId") or ""),
                "reason": str(feature.get("nextAction") or ""),
                "targetId": str(feature.get("featureId") or ""),
            })
    for session in sessions:
        if session.get("risk") in {"high", "critical", "blocking"}:
            items.append({
                "kind": "session",
                "priority": "high",
                "title": str(session.get("sessionId") or ""),
                "reason": str(session.get("lastEventTitle") or "Review high-risk session activity."),
                "targetId": str(session.get("sessionId") or ""),
            })
    priority_order = {"high": 0, "medium": 1, "low": 2}
    items.sort(key=lambda item: priority_order.get(str(item.get("priority")), 3))
    return items[:3]


def get_workflow_cockpit() -> dict[str, Any]:
    events = _all_history_events()
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        session_id = str(event.get("sessionId") or event.get("model") or "unknown")
        grouped[session_id].append(event)

    sessions = [
        _session_workflow_record(session_id, session_events)
        for session_id, session_events in grouped.items()
    ]
    sessions.sort(key=lambda session: str(session.get("latestAt") or ""), reverse=True)

    sessions_by_model: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for session in sessions:
        sessions_by_model[str(session.get("model") or "unknown")].append(session)
    models = [
        _model_workflow_record(model, model_sessions)
        for model, model_sessions in sessions_by_model.items()
    ]
    models.sort(key=lambda model: int(model.get("eventCount") or 0), reverse=True)

    features = list_progress_features()
    return {
        "summary": {
            "totalEvents": len(events),
            "sessionCount": len(sessions),
            "modelCount": len(models),
            "commitCount": sum(int(session.get("commitCount") or 0) for session in sessions),
            "testCount": sum(int(session.get("testCount") or 0) for session in sessions),
            "blockingSessions": sum(1 for session in sessions if session.get("risk") in {"high", "critical", "blocking"}),
            "latestAt": sessions[0].get("latestAt") if sessions else "",
        },
        "models": models,
        "sessions": sessions[:40],
        "reviewItems": _workflow_review_items(sessions, features),
    }
