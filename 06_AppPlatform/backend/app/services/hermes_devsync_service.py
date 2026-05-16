"""Hermes DevSync — Development Governance Loop.

Reads Claude Code dev events from JSONL, syncs them into:
  - Feature Registry (YAML)
  - Auto-generated Markdown docs
  - Evidence Ledger (JSONL)
  - Governance Gaps (YAML)
"""

from __future__ import annotations

import json
import re
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


def _dev_events_path() -> Path:
    return _root() / "hermes" / "dev_events" / "dev_events.jsonl"


def _features_path() -> Path:
    return _root() / "hermes" / "registry" / "features.yaml"


def _evidence_ledger_path() -> Path:
    return _root() / "hermes" / "evidence_ledger.jsonl"


def _gaps_path() -> Path:
    return _root() / "hermes" / "governance_gaps.yaml"


def _features_docs_dir() -> Path:
    d = _root() / "Markdown_Readme" / "features"
    d.mkdir(parents=True, exist_ok=True)
    return d


# ── Dev Events ────────────────────────────────────────────────────────

def _ensure_dev_events_dir() -> None:
    _dev_events_path().parent.mkdir(parents=True, exist_ok=True)


def append_dev_event(event: dict[str, Any]) -> dict[str, Any]:
    if not event.get("eventId"):
        event["eventId"] = f"dev_evt_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    if not event.get("createdAt"):
        event["createdAt"] = datetime.now(timezone.utc).isoformat()
    _ensure_dev_events_dir()
    with open(_dev_events_path(), "a") as fh:
        fh.write(json.dumps(event, ensure_ascii=False) + "\n")
    return event


def list_dev_events(
    event_type: str | None = None,
    source: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    p = _dev_events_path()
    if not p.is_file():
        return []
    events: list[dict] = []
    for line in p.read_text().strip().split("\n"):
        if line.strip():
            try:
                e = json.loads(line)
                if event_type and e.get("eventType") != event_type:
                    continue
                if source and e.get("source") != source:
                    continue
                events.append(e)
            except Exception:
                pass
    events.sort(key=lambda e: e.get("createdAt", ""), reverse=True)
    return events[:limit]


# ── Feature Registry ──────────────────────────────────────────────────

def _load_features() -> list[dict[str, Any]]:
    p = _features_path()
    if not p.is_file():
        return []
    import yaml
    data = yaml.safe_load(p.read_text())
    features = data.get("features", []) if data else []
    return _normalize_feature_records(features)


def _save_features(features: list[dict[str, Any]]) -> None:
    p = _features_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    import yaml
    features = _normalize_feature_records(features)
    p.write_text(yaml.safe_dump({"features": features}, allow_unicode=True, sort_keys=False))


_FEATURE_TITLE_WORDS = {
    "api": "API",
    "apis": "APIs",
    "auth": "Auth",
    "backend": "Backend",
    "chat": "Chat",
    "command": "Command",
    "dev": "Dev",
    "devsync": "DevSync",
    "frontend": "Frontend",
    "gateway": "Gateway",
    "hermes": "Hermes",
    "scraping": "Scraping",
    "sentinel": "Sentinel",
    "sync": "Sync",
    "toolkit": "Toolkit",
    "ui": "UI",
    "websocket": "WebSocket",
    "workspace": "Workspace",
}


def _feature_title_from_id(feature_id: str) -> str:
    clean_id = _canonical_feature_id(feature_id)
    parts = [p for p in re.split(r"[-_.]+", clean_id) if p]
    if not parts:
        return str(feature_id or "").strip()

    words: list[str] = []
    for part in parts:
        lower = part.lower()
        words.append(_FEATURE_TITLE_WORDS.get(lower, part.capitalize()))
    return " ".join(words)


def _canonical_feature_id(feature_id: str) -> str:
    clean_id = re.sub(r"^feature[._-]", "", str(feature_id or "").strip())
    return re.sub(r"[-_.]+", "-", clean_id).strip("-")


def _feature_ids_match(left: str, right: str) -> bool:
    if not left or not right:
        return False
    if left == right:
        return True
    return _canonical_feature_id(left) == _canonical_feature_id(right)


def _merge_feature_records(
    base: dict[str, Any],
    incoming: dict[str, Any],
) -> dict[str, Any]:
    merged = {**base, **incoming}
    for key in (
        "linkedEventIds", "endpoints", "frontend", "backend", "risks",
        "nextSteps", "docs", "gaps",
    ):
        values: list[Any] = []
        for item in list(base.get(key, []) or []) + list(incoming.get(key, []) or []):
            if item not in values:
                values.append(item)
        if values:
            merged[key] = values
    if base.get("tests") or incoming.get("tests"):
        merged["tests"] = {**(base.get("tests") or {}), **(incoming.get("tests") or {})}
    if base.get("featureId"):
        merged["featureId"] = base["featureId"]
    if base.get("title"):
        merged["title"] = base["title"]
    return merged


def _normalize_feature_records(
    features: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    title_counts: dict[str, int] = {}
    for feature in features:
        title = str(feature.get("title") or "").strip()
        if title:
            title_counts[title] = title_counts.get(title, 0) + 1

    seen: dict[str, int] = {}
    normalized: list[dict[str, Any]] = []
    for feature in features:
        record = dict(feature)
        fid = str(record.get("featureId") or "").strip()
        title = str(record.get("title") or "").strip()
        if fid and (
            not title or title_counts.get(title, 0) > 1 or _is_git_commit_title(title)
        ):
            record["title"] = _feature_title_from_id(fid)

        if not fid:
            normalized.append(record)
            continue

        lookup_id = _canonical_feature_id(fid)
        existing_idx = seen.get(lookup_id)
        if existing_idx is None:
            seen[lookup_id] = len(normalized)
            normalized.append(record)
        else:
            normalized[existing_idx] = _merge_feature_records(
                normalized[existing_idx],
                record,
            )
    return normalized


def list_features(
    status: str | None = None,
    category: str | None = None,
) -> list[dict[str, Any]]:
    features = _load_features()
    if status:
        features = [f for f in features if f.get("status") == status]
    if category:
        features = [f for f in features if f.get("category") == category]
    return features


def get_feature(feature_id: str) -> dict[str, Any] | None:
    for f in _load_features():
        if _feature_ids_match(str(f.get("featureId") or ""), feature_id):
            return f
    return None


def upsert_feature(feature_data: dict[str, Any]) -> dict[str, Any]:
    features = _load_features()
    fid = str(feature_data.get("featureId", ""))
    now = datetime.now(timezone.utc).isoformat()
    for i, f in enumerate(features):
        if _feature_ids_match(str(f.get("featureId") or ""), fid):
            merged = _merge_feature_records(f, feature_data)
            merged["lastUpdatedAt"] = now
            features[i] = merged
            _save_features(features)
            return get_feature(fid) or merged
    feature_data.setdefault("createdAt", now)
    feature_data["lastUpdatedAt"] = now
    features.append(feature_data)
    _save_features(features)
    return get_feature(fid) or _normalize_feature_records([feature_data])[0]


# ── Feature ID inference ──────────────────────────────────────────────

_SLUG_RE = re.compile(r"[^a-z0-9]+")


def infer_feature_id(title: str) -> str:
    slug = _SLUG_RE.sub("-", title.lower()).strip("-")
    return slug[:60]


# ── Markdown Generation ───────────────────────────────────────────────

def generate_feature_markdown(feature: dict[str, Any]) -> Path:
    fid = feature.get("featureId", "unknown")
    title = feature.get("title", fid)
    status = feature.get("status", "unknown")
    summary = feature.get("summary", "")
    category = feature.get("category", "")
    source = feature.get("source", "")
    endpoints = feature.get("endpoints", []) or []
    frontend = feature.get("frontend", []) or []
    backend = feature.get("backend", []) or []
    tests = feature.get("tests", {}) or {}
    docs_list = feature.get("docs", []) or []
    linked_events = feature.get("linkedEventIds", []) or []
    risks = feature.get("risks", []) or []
    next_steps = feature.get("nextSteps", []) or []
    gaps = feature.get("gaps", []) or []

    lines = [
        f"# {title}", "", "## Status", status, "",
        "## Category", f"{category} · source: {source}", "",
    ]
    if summary:
        lines += ["## Summary", summary, ""]
    if endpoints:
        lines += ["## Endpoints", ""] + [f"- `{ep}`" for ep in endpoints] + [""]
    if backend:
        lines += ["## Backend", ""] + [f"- {b}" for b in backend] + [""]
    if frontend:
        lines += ["## Frontend", ""] + [f"- {f}" for f in frontend] + [""]
    if tests:
        lines += ["## Tests", ""] + [f"- **{k}**: {v}" for k, v in tests.items()] + [""]
    if linked_events:
        lines += ["## Linked Dev Events", ""] + [f"- `{eid}`" for eid in linked_events] + [""]
    if docs_list:
        lines += ["## Docs", ""] + [f"- {d}" for d in docs_list] + [""]
    if gaps:
        lines += ["## Gaps", ""] + [f"- {g}" for g in gaps] + [""]
    if risks:
        lines += ["## Risks", ""] + [f"- {r}" for r in risks] + [""]
    if next_steps:
        lines += ["## Next Steps", ""] + [f"- {ns}" for ns in next_steps] + [""]
    lines.append(f"*Auto-generated by Hermes DevSync. Last updated: {feature.get('lastUpdatedAt', '')}*")
    lines.append("")

    path = _features_docs_dir() / f"{fid}.md"
    path.write_text("\n".join(lines))
    return path


# ── Gap Creation ──────────────────────────────────────────────────────

def _load_gaps() -> list[dict[str, Any]]:
    p = _gaps_path()
    if not p.is_file():
        return []
    import yaml
    data = yaml.safe_load(p.read_text())
    return data.get("gaps", []) if data else []


def _save_gaps(gaps: list[dict[str, Any]]) -> None:
    p = _gaps_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    import yaml
    p.write_text(yaml.safe_dump({"gaps": gaps}, allow_unicode=True, sort_keys=False))


def _gap_exists(gap_id: str) -> bool:
    return any(g.get("gapId") == gap_id for g in _load_gaps())


def create_missing_docs_gap(feature: dict[str, Any]) -> dict[str, Any] | None:
    fid = feature.get("featureId", "")
    gap_id = f"gap.devsync.{fid}.missing_docs"
    if _gap_exists(gap_id):
        return None
    docs = feature.get("docs", []) or []
    if docs:
        return None
    gap = {
        "gapId": gap_id,
        "title": f"Feature '{feature.get('title', fid)}' has no documentation",
        "category": "docs", "severity": "medium", "status": "open",
        "affectedAssets": [fid],
        "evidence": ["DevSync auto-detected: feature implemented but no docs linked."],
        "recommendedAction": f"Write docs for {fid} or link existing docs in features.yaml.",
        "owner": "", "notes": "Created automatically by Hermes DevSync.",
    }
    gaps = _load_gaps()
    gaps.append(gap)
    _save_gaps(gaps)
    return gap


def create_missing_tests_gap(feature: dict[str, Any]) -> dict[str, Any] | None:
    fid = feature.get("featureId", "")
    gap_id = f"gap.devsync.{fid}.missing_tests"
    if _gap_exists(gap_id):
        return None
    tests = feature.get("tests", {}) or {}
    if tests:
        return None
    gap = {
        "gapId": gap_id,
        "title": f"Feature '{feature.get('title', fid)}' has no test results recorded",
        "category": "test", "severity": "high", "status": "open",
        "affectedAssets": [fid],
        "evidence": ["DevSync auto-detected: feature registered but no test results."],
        "recommendedAction": f"Run and record tests for {fid}.",
        "owner": "", "notes": "Created automatically by Hermes DevSync.",
    }
    gaps = _load_gaps()
    gaps.append(gap)
    _save_gaps(gaps)
    return gap


# ── Evidence Record ───────────────────────────────────────────────────

def write_dev_evidence_record(event: dict[str, Any], feature: dict[str, Any]) -> dict[str, Any]:
    record = {
        "evidenceId": f"evidence.{event.get('eventId', 'unknown')}",
        "evidenceType": "dev_event",
        "claim": f"Feature '{feature.get('title', '')}' {event.get('eventType', '')}: {event.get('summary', '')[:200]}",
        "sourceRef": f"dev_events.jsonl::{event.get('eventId', '')}",
        "artifactId": f"feature.{feature.get('featureId', '')}",
        "confidence": 1.0, "supportCount": 0, "contradictionCount": 0,
        "createdAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    p = _evidence_ledger_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "a") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")
    return record


# ── Sync ──────────────────────────────────────────────────────────────

def _event_to_status(event_type: str) -> str:
    return {
        "implementation_completed": "implemented",
        "test_run": "implemented", "bug_fix": "implemented",
        "refactor": "implemented", "docs_update": "implemented",
        "verification_completed": "verified",
    }.get(event_type, "implemented")


def _infer_category(event: dict[str, Any]) -> str:
    title = (event.get("title") or "").lower()
    if "frontend" in title or "ui" in title:
        return "frontend"
    if "backend" in title or "api" in title:
        return "backend"
    if "hermes" in title:
        return "governance"
    changed = event.get("changedFiles", []) or []
    front_f = sum(1 for f in changed if "frontend" in f)
    back_f = sum(1 for f in changed if "backend" in f)
    if front_f > back_f:
        return "frontend"
    if back_f > front_f:
        return "backend"
    return "governance"


def sync_dev_events() -> dict[str, Any]:
    events = list_dev_events(limit=200)
    if not events:
        return {"synced": 0, "featuresUpdated": [], "featuresCreated": [],
                "docsGenerated": 0, "evidenceWritten": 0, "gapsCreated": 0}

    feats_updated: list[str] = []
    feats_created: list[str] = []
    docs_count = 0
    evidence_count = 0
    gaps_count = 0

    for event in events:
        linked_ids: list[str] = event.get("linkedFeatureIds", []) or []
        if not linked_ids:
            inferred = infer_feature_id(event.get("title", ""))
            if inferred:
                linked_ids = [inferred]

        for fid in linked_ids:
            existing = get_feature(fid)
            feature_data = {
                "featureId": fid,
                "title": event.get("title", fid),
                "status": _event_to_status(event.get("eventType", "")),
                "category": _infer_category(event),
                "source": event.get("source", "claude_code"),
                "summary": event.get("summary", ""),
                "linkedEventIds": [event.get("eventId", "")],
                "endpoints": event.get("addedEndpoints", []) or [],
                "frontend": event.get("frontendChanges", []) or [],
                "backend": event.get("backendChanges", []) or [],
                "tests": event.get("tests", {}) or {},
                "risks": event.get("risks", []) or [],
                "nextSteps": event.get("nextSteps", []) or [],
            }

            upserted = upsert_feature(feature_data)
            if existing:
                feats_updated.append(fid)
            else:
                feats_created.append(fid)

            generate_feature_markdown(upserted)
            docs_count += 1
            write_dev_evidence_record(event, upserted)
            evidence_count += 1

            doc_gap = create_missing_docs_gap(upserted)
            test_gap = create_missing_tests_gap(upserted)
            if doc_gap:
                gaps_count += 1
                gs = list(upserted.get("gaps", []) or [])
                gs.append(doc_gap["gapId"])
                upsert_feature({**upserted, "gaps": gs})
            if test_gap:
                gaps_count += 1

    # Sync to kanban feature_registry.yaml
    kanban_synced = _sync_to_kanban(feats_created + feats_updated)

    return {
        "synced": len(events),
        "featuresUpdated": feats_updated,
        "featuresCreated": feats_created,
        "docsGenerated": docs_count,
        "evidenceWritten": evidence_count,
        "gapsCreated": gaps_count,
        "kanbanUpdated": kanban_synced,
    }


def _kanban_registry_path() -> Path:
    return _root() / "hermes" / "feature_registry.yaml"


def _is_git_commit_title(name: str) -> bool:
    """Return True if the name looks like a git commit message."""
    if not name:
        return True
    return name.startswith("fix:") or name.startswith("feat:") or name.startswith("hermes:") or name.startswith("trigger:") or len(name) > 60


def _sync_to_kanban(feature_ids: list[str]) -> int:
    """Upsert DevSync features into the kanban feature_registry.yaml.

    Rules:
    - One featureId = one kanban entry (deduplication)
    - Original kanban names are preserved (never overwritten with git commit titles)
    - New features get a clean name derived from the DevSync featureId
    - Status/implementationStatus updated from DevSync data
    """
    import yaml

    dev_features = {
        _canonical_feature_id(str(f["featureId"])): f
        for f in list_features()
        if f.get("featureId")
    }

    kp = _kanban_registry_path()
    kanban_data: dict[str, Any] = {}
    if kp.is_file():
        kanban_data = yaml.safe_load(kp.read_text()) or {}
    kanban_features: list[dict] = kanban_data.get("features", [])

    # Deduplicate existing kanban entries by canonical featureId (keep first)
    seen: set[str] = set()
    deduped: list[dict] = []
    for kf in kanban_features:
        fid = str(kf.get("featureId", ""))
        lookup_id = _canonical_feature_id(fid)
        if lookup_id and lookup_id in seen:
            continue  # skip duplicate
        if lookup_id:
            seen.add(lookup_id)
        deduped.append(kf)
    kanban_features = deduped

    # Build index
    kanban_index: dict[str, int] = {}
    for i, kf in enumerate(kanban_features):
        fid = str(kf.get("featureId", ""))
        lookup_id = _canonical_feature_id(fid)
        if lookup_id:
            kanban_index[lookup_id] = i

    count = 0
    for fid in set(feature_ids):  # deduplicate input
        lookup_id = _canonical_feature_id(str(fid))
        df = dev_features.get(lookup_id)
        if not df:
            continue
        entry_feature_id = str(df.get("featureId") or fid)
        existing_idx = kanban_index.get(lookup_id)
        if existing_idx is not None:
            entry_feature_id = str(kanban_features[existing_idx].get("featureId") or entry_feature_id)

        ds_status = df.get("status", "implemented")
        kanban_status = "active"
        impl_status = "implemented"
        if ds_status in ("planned", "idea"):
            kanban_status = "planned"; impl_status = "planned"
        elif ds_status == "in_progress":
            kanban_status = "beta"; impl_status = "partial"
        elif ds_status in ("blocked", "deprecated"):
            kanban_status = "archived"; impl_status = "partial"

        risk = "low"
        if df.get("gaps"):
            risk = "medium"
        if any("missing_tests" in g for g in (df.get("gaps") or [])):
            risk = "medium"

        # Determine a clean name
        dv_title = df.get("title", "")
        if existing_idx is not None:
            existing_name = kanban_features[existing_idx].get("name", "")
            # Keep existing name if it's good; only replace if empty or looks like commit msg
            if existing_name and not _is_git_commit_title(existing_name):
                clean_name = existing_name
            elif dv_title and not _is_git_commit_title(dv_title):
                clean_name = dv_title
            else:
                clean_name = existing_name or fid.replace("-", " ").title()
        else:
            # New feature: use DevSync title if clean, otherwise derive from featureId
            if dv_title and not _is_git_commit_title(dv_title):
                clean_name = dv_title
            else:
                clean_name = fid.replace("-", " ").title()

        kanban_entry = {
            "featureId": entry_feature_id,
            "name": clean_name,
            "status": kanban_status,
            "implementationStatus": impl_status,
            "riskLevel": risk,
            "routes": [],
            "backendApis": df.get("endpoints", []) or [],
            "scheduledJobs": [],
            "dataSources": [],
            "artifacts": [],
            "docs": df.get("docs", []) or [],
            "tests": list(df.get("tests", {}).keys()) if df.get("tests") else [],
            "dependencies": [],
            "owner": "",
            "governanceStatus": "registered",
            "knownIssues": df.get("risks", []) or [],
            "lastAuditAt": df.get("lastUpdatedAt", ""),
        }

        if existing_idx is not None:
            existing = kanban_features[existing_idx]
            merged = {
                **kanban_entry,
                "name": clean_name,
                "routes": existing.get("routes", []),
                "scheduledJobs": existing.get("scheduledJobs", []),
                "dataSources": existing.get("dataSources", []),
                "artifacts": existing.get("artifacts", []),
                "dependencies": existing.get("dependencies", []),
                "owner": existing.get("owner", ""),
            }
            kanban_features[existing_idx] = merged
        else:
            kanban_features.append(kanban_entry)
        count += 1

    kanban_data["features"] = kanban_features
    kp.parent.mkdir(parents=True, exist_ok=True)
    kp.write_text(yaml.safe_dump(kanban_data, allow_unicode=True, sort_keys=False))
    return count
