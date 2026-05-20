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

_FEATURE_ID_ALIASES = {
    "country-copilot": "feature.country_copilot",
    "data-management-ui": "feature.data_management",
    "docs-add-frontend-monthly-update-ux-guardrails-to": "feature.jato_monthly_update",
    "docs-add-publish-guards-flowchart-to-diagrams-inde": "feature.jato_monthly_update",
    "fix-add-write-permissions-and-pull-before-push-for": "hermes-devsync",
    "backend-infra": "feature.market_scan",
    "backend-query-service": "feature.dashboard",
    "hermes-command-gateway": "hermes-chat-gateway",
    "hermes-dev-ui": "hermes-devsync",
    "hermes-scripts": "hermes-devsync",
    "jato-monthly-update": "feature.jato_monthly_update",
    "market-scan": "feature.market_scan",
    "msrp-pricing": "feature.msrp_workbench",
    "presence-websocket": "feature.presence_websocket",
    "review-workbench": "feature.review_workbench",
    "redis-cache": "feature.market_scan",
    "scraping-toolkit": "feature.scraping_toolkit",
    "time-series-lens": "feature.dashboard",
    "ui-animations": "feature.ui_animation_toolkit",
    "ui-animation-toolkit": "feature.ui_animation_toolkit",
}

_NOISY_GENERIC_FEATURE_IDS = {"backend", "frontend"}
_NOISY_COMMIT_FEATURE_PREFIXES = (
    "fix-",
    "merge-",
    "trigger-",
    "docs-",
    "hermes-auto-dev-event-",
    "hermes-record-",
)


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


def _feature_lookup_key(feature_id: str) -> str:
    clean_id = re.sub(r"^feature[._-]", "", str(feature_id or "").strip().lower())
    return re.sub(r"[-_.]+", "-", clean_id).strip("-")


def _canonical_feature_id(feature_id: str) -> str:
    lookup_id = _feature_lookup_key(feature_id)
    alias_target = _FEATURE_ID_ALIASES.get(lookup_id)
    if alias_target:
        target_lookup_id = _feature_lookup_key(alias_target)
        if target_lookup_id != lookup_id:
            return _canonical_feature_id(alias_target)
    return lookup_id


def _canonical_record_feature_id(feature_id: str) -> str:
    raw_id = str(feature_id or "").strip()
    lookup_id = _feature_lookup_key(raw_id)
    alias_target = _FEATURE_ID_ALIASES.get(lookup_id)
    if alias_target:
        return alias_target
    return raw_id


def _append_unique(values: list[Any], value: Any) -> None:
    if value and value not in values:
        values.append(value)


def _is_noisy_commit_feature_id(feature_id: str) -> bool:
    raw_lookup_id = _feature_lookup_key(feature_id)
    if raw_lookup_id in _FEATURE_ID_ALIASES:
        return False
    return raw_lookup_id.startswith(_NOISY_COMMIT_FEATURE_PREFIXES)


def _is_noisy_generic_feature_record(feature: dict[str, Any]) -> bool:
    fid = str(feature.get("featureId") or "")
    if feature.get("source") != "git_commit":
        return False
    if _canonical_feature_id(fid) in _NOISY_GENERIC_FEATURE_IDS:
        return True
    return _is_noisy_commit_feature_id(fid)


def _clean_generated_doc_refs(feature: dict[str, Any]) -> dict[str, Any]:
    fid = str(feature.get("featureId") or "")
    if not fid:
        return feature
    if feature.get("aliases"):
        feature["title"] = _feature_title_from_id(fid)
    docs = list(feature.get("docs", []) or [])
    if not docs:
        return feature
    canonical_doc_ref = f"Markdown_Readme/features/{fid}.md"
    cleaned: list[str] = []
    for doc in docs:
        doc_ref = str(doc)
        if doc_ref.startswith("Markdown_Readme/features/") and doc_ref != canonical_doc_ref:
            continue
        _append_unique(cleaned, doc_ref)
    feature["docs"] = cleaned
    return feature


def _known_aliases_for_feature_id(feature_id: str) -> list[str]:
    lookup_id = _canonical_feature_id(feature_id)
    aliases: list[str] = []
    for alias, target in _FEATURE_ID_ALIASES.items():
        if _canonical_feature_id(target) != lookup_id:
            continue
        _append_unique(aliases, alias)
        if str(target).startswith("feature."):
            _append_unique(aliases, f"feature.{alias.replace('-', '_')}")
    return aliases


def _is_low_quality_summary(summary: str) -> bool:
    first_line = summary.strip().splitlines()[0].strip().lower() if summary.strip() else ""
    return (
        not first_line
        or first_line == "merge"
        or first_line.startswith("merge:")
        or first_line.startswith("trigger ")
        or first_line.startswith("trigger:")
    )


def _summary_quality(record: dict[str, Any]) -> float:
    summary = str(record.get("summary") or "").strip()
    if not summary:
        return -100.0
    score = min(len(summary), 800) / 100.0
    if record.get("source") == "claude_code":
        score += 20.0
    if _is_low_quality_summary(summary):
        score -= 50.0
    return score


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
        "nextSteps", "docs", "gaps", "aliases",
    ):
        values: list[Any] = []
        for item in list(base.get(key, []) or []) + list(incoming.get(key, []) or []):
            _append_unique(values, item)
        if values:
            merged[key] = values
    if base.get("tests") or incoming.get("tests"):
        merged["tests"] = {**(base.get("tests") or {}), **(incoming.get("tests") or {})}
    if base.get("featureId"):
        merged["featureId"] = base["featureId"]
    if base.get("title"):
        merged["title"] = base["title"]
    incoming_summary = str(incoming.get("summary") or "").strip()
    if incoming_summary and _summary_quality(incoming) >= _summary_quality(base):
        merged["summary"] = incoming_summary
    elif base.get("summary"):
        merged["summary"] = base["summary"]
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
        original_fid = str(record.get("featureId") or "").strip()
        fid = _canonical_record_feature_id(original_fid)
        if fid and original_fid and fid != original_fid:
            record["featureId"] = fid
            record["title"] = _feature_title_from_id(fid)
            aliases = list(record.get("aliases", []) or [])
            _append_unique(aliases, original_fid)
            record["aliases"] = aliases
        elif fid:
            record["featureId"] = fid

        if _is_noisy_generic_feature_record(record):
            continue

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
            normalized.append(_clean_generated_doc_refs(record))
        else:
            normalized[existing_idx] = _merge_feature_records(
                normalized[existing_idx],
                record,
            )
            normalized[existing_idx] = _clean_generated_doc_refs(normalized[existing_idx])
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
    normalized_feature_data = _normalize_feature_records([feature_data])
    if not normalized_feature_data:
        return feature_data
    feature_data = normalized_feature_data[0]
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

_FEATURE_PATH_RULES: list[tuple[tuple[str, ...], str, int]] = [
    ((".github/workflows/hermes-devsync.yml", ".githooks/", "03_Scripts/hermes/hermes_dev_event", "hermes_devsync_service.py"), "hermes-devsync", 100),
    (("hermes_chat_service.py", "HermesAskResponseCard", "POST /hermes/chat"), "hermes-chat-gateway", 100),
    (("backend/app/api/routes/presence.py", "presence_service.py", "PresenceWidget", "usePresence"), "feature.presence_websocket", 100),
    (("usePageTransition", "useStaggerEntrance", "anime"), "feature.ui_animation_toolkit", 90),
    (("jato_monthly_update_service.py", "JatoMonthlyUpdatePage", "JATO_MONTHLY_UPDATE_DATA_LIFECYCLE"), "feature.jato_monthly_update", 100),
    (("MarketScanPage", "market_scan_service.py"), "feature.market_scan", 100),
    (("engineering_config", "EngineeringConfigPage", "ConfigMatrix"), "feature.engineering", 100),
    (("CountryChatPage", "country_chat_service.py", "assistant/country"), "feature.country_copilot", 100),
    (("07_ScrapingToolkit",), "feature.scraping_toolkit", 90),
    (("frontend/",), "frontend", 1),
    (("backend/",), "backend", 1),
]

_FEATURE_TITLE_RULES: list[tuple[tuple[str, ...], str, int]] = [
    (("hermes", "devsync"), "hermes-devsync", 100),
    (("hermes", "chat"), "hermes-chat-gateway", 100),
    (("hermes", "command"), "hermes-chat-gateway", 95),
    (("jato", "monthly"), "feature.jato_monthly_update", 100),
    (("market", "scan"), "feature.market_scan", 95),
    (("engineering", "config"), "feature.engineering", 120),
    (("presence",), "feature.presence_websocket", 90),
    (("animation",), "feature.ui_animation_toolkit", 70),
    (("scraping",), "feature.scraping_toolkit", 80),
]


def infer_feature_id(title: str) -> str:
    slug = _SLUG_RE.sub("-", title.lower()).strip("-")
    alias_target = _FEATURE_ID_ALIASES.get(slug[:50]) or _FEATURE_ID_ALIASES.get(slug[:60])
    return alias_target or slug[:60]


def _infer_feature_ids_from_event(event: dict[str, Any]) -> list[str]:
    scores: dict[str, int] = {}
    changed_files = [str(f) for f in (event.get("changedFiles", []) or [])]
    searchable_files = changed_files + [str(f) for f in (event.get("frontendChanges", []) or [])]
    searchable_files += [str(f) for f in (event.get("backendChanges", []) or [])]
    for fpath in searchable_files:
        for patterns, feature_id, weight in _FEATURE_PATH_RULES:
            if any(pattern in fpath for pattern in patterns):
                scores[feature_id] = scores.get(feature_id, 0) + weight

    normalized_title = str(event.get("title") or "").lower()
    for keywords, feature_id, weight in _FEATURE_TITLE_RULES:
        if all(keyword in normalized_title for keyword in keywords):
            scores[feature_id] = scores.get(feature_id, 0) + weight

    ranked = sorted(scores.items(), key=lambda item: (-item[1], item[0]))
    max_score = ranked[0][1] if ranked else 0
    feature_ids = [
        _canonical_record_feature_id(fid)
        for fid, score in ranked
        if score > 1 and score >= max_score * 0.65
    ]
    return _dedupe_feature_ids(feature_ids)


def _dedupe_feature_ids(feature_ids: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for fid in feature_ids:
        canonical_fid = _canonical_record_feature_id(fid)
        lookup_id = _canonical_feature_id(canonical_fid)
        if not lookup_id or lookup_id in seen:
            continue
        seen.add(lookup_id)
        result.append(canonical_fid)

    has_specific = any(_canonical_feature_id(fid) not in _NOISY_GENERIC_FEATURE_IDS for fid in result)
    if has_specific:
        result = [
            fid for fid in result
            if _canonical_feature_id(fid) not in _NOISY_GENERIC_FEATURE_IDS
        ]
    return result


def _filter_event_feature_ids(feature_ids: list[str], event: dict[str, Any]) -> list[str]:
    if event.get("source") != "git_commit":
        return feature_ids
    return [
        fid for fid in feature_ids
        if _canonical_feature_id(fid) not in _NOISY_GENERIC_FEATURE_IDS
        and not _is_noisy_commit_feature_id(fid)
    ]


def _linked_feature_ids_for_event(event: dict[str, Any]) -> list[str]:
    explicit_ids = _dedupe_feature_ids(list(event.get("linkedFeatureIds", []) or []))
    inferred_ids = _infer_feature_ids_from_event(event)

    if event.get("source") == "git_commit" and inferred_ids:
        return _filter_event_feature_ids(inferred_ids, event)

    if not explicit_ids:
        if inferred_ids:
            return _filter_event_feature_ids(inferred_ids, event)
        if event.get("source") == "git_commit":
            return []
        inferred = infer_feature_id(event.get("title", ""))
        return _dedupe_feature_ids([inferred]) if inferred else []

    explicit_has_only_generic = all(
        _canonical_feature_id(fid) in _NOISY_GENERIC_FEATURE_IDS
        for fid in explicit_ids
    )
    if explicit_has_only_generic and inferred_ids:
        return _filter_event_feature_ids(inferred_ids, event)

    if event.get("source") != "git_commit":
        return explicit_ids

    return _filter_event_feature_ids(_dedupe_feature_ids(explicit_ids + inferred_ids), event)


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


def _relative_project_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(_root().resolve()))
    except ValueError:
        return str(path)


def _ensure_feature_doc_link(feature: dict[str, Any], doc_path: Path) -> dict[str, Any]:
    docs = list(feature.get("docs", []) or [])
    doc_ref = _relative_project_path(doc_path)
    _append_unique(docs, doc_ref)
    if docs == list(feature.get("docs", []) or []):
        return feature
    return upsert_feature({**feature, "docs": docs})


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


def _feature_gap_id_candidates(feature: dict[str, Any], suffix: str) -> list[str]:
    ids = [str(feature.get("featureId") or "")]
    ids += [str(alias) for alias in (feature.get("aliases", []) or [])]
    ids += _known_aliases_for_feature_id(str(feature.get("featureId") or ""))
    candidates: list[str] = []
    for fid in ids:
        _append_unique(candidates, f"gap.devsync.{fid}.{suffix}")
    return candidates


def _resolve_existing_devsync_gaps(
    feature: dict[str, Any],
    suffix: str,
    reason: str,
) -> int:
    gap_ids = set(_feature_gap_id_candidates(feature, suffix))
    gaps = _load_gaps()
    changed = 0
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for gap in gaps:
        if gap.get("gapId") not in gap_ids:
            continue
        if gap.get("status") == "resolved":
            continue
        gap["status"] = "resolved"
        notes = str(gap.get("notes") or "").strip()
        resolution = f"Resolved {now}: {reason}"
        gap["notes"] = f"{notes} {resolution}".strip() if notes else resolution
        changed += 1
    if changed:
        _save_gaps(gaps)
    return changed


def reconcile_feature_gaps(feature: dict[str, Any]) -> int:
    resolved = 0
    if feature.get("docs"):
        resolved += _resolve_existing_devsync_gaps(
            feature,
            "missing_docs",
            "DevSync feature now has documentation linked.",
        )
    if feature.get("tests"):
        resolved += _resolve_existing_devsync_gaps(
            feature,
            "missing_tests",
            "DevSync feature now has test results recorded.",
        )
    return resolved


def retire_noisy_devsync_gaps() -> int:
    gaps = _load_gaps()
    changed = 0
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for gap in gaps:
        gap_id = str(gap.get("gapId") or "")
        if not gap_id.startswith("gap.devsync.") or gap.get("status") == "resolved":
            continue
        feature_part = gap_id.removeprefix("gap.devsync.").rsplit(".", 1)[0]
        if (
            _canonical_feature_id(feature_part) not in _NOISY_GENERIC_FEATURE_IDS
            and not _is_noisy_commit_feature_id(feature_part)
        ):
            continue
        gap["status"] = "resolved"
        notes = str(gap.get("notes") or "").strip()
        resolution = (
            f"Resolved {now}: closed as DevSync featureId inference noise; "
            "future git events without a reliable feature match are no longer registered."
        )
        gap["notes"] = f"{notes} {resolution}".strip() if notes else resolution
        changed += 1
    if changed:
        _save_gaps(gaps)
    return changed


def create_missing_docs_gap(feature: dict[str, Any]) -> dict[str, Any] | None:
    fid = feature.get("featureId", "")
    if (
        _canonical_feature_id(str(fid)) in _NOISY_GENERIC_FEATURE_IDS
        or _is_noisy_commit_feature_id(str(fid))
    ):
        return None
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
    if (
        _canonical_feature_id(str(fid)) in _NOISY_GENERIC_FEATURE_IDS
        or _is_noisy_commit_feature_id(str(fid))
    ):
        return None
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

def _evidence_exists(evidence_id: str) -> bool:
    p = _evidence_ledger_path()
    if not p.is_file():
        return False
    for line in p.read_text().splitlines():
        if not line.strip():
            continue
        try:
            if json.loads(line).get("evidenceId") == evidence_id:
                return True
        except Exception:
            continue
    return False


def write_dev_evidence_record(event: dict[str, Any], feature: dict[str, Any]) -> dict[str, Any]:
    feature_lookup_id = _canonical_feature_id(str(feature.get("featureId", ""))) or "unknown"
    evidence_id = f"evidence.{event.get('eventId', 'unknown')}.{feature_lookup_id}"
    record = {
        "evidenceId": evidence_id,
        "evidenceType": "dev_event",
        "claim": f"Feature '{feature.get('title', '')}' {event.get('eventType', '')}: {event.get('summary', '')[:200]}",
        "sourceRef": f"dev_events.jsonl::{event.get('eventId', '')}",
        "artifactId": f"feature.{feature.get('featureId', '')}",
        "confidence": 1.0, "supportCount": 0, "contradictionCount": 0,
        "createdAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    if _evidence_exists(evidence_id):
        return record
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
        linked_ids = _linked_feature_ids_for_event(event)

        for fid in linked_ids:
            if _canonical_feature_id(fid) in _NOISY_GENERIC_FEATURE_IDS:
                continue
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

            doc_path = generate_feature_markdown(upserted)
            upserted = _ensure_feature_doc_link(upserted, doc_path)
            generate_feature_markdown(upserted)
            docs_count += 1
            write_dev_evidence_record(event, upserted)
            evidence_count += 1

            gaps_count += reconcile_feature_gaps(upserted)
            doc_gap = create_missing_docs_gap(upserted)
            test_gap = create_missing_tests_gap(upserted)
            if doc_gap:
                gaps_count += 1
                gs = list(upserted.get("gaps", []) or [])
                gs.append(doc_gap["gapId"])
                upsert_feature({**upserted, "gaps": gs})
            if test_gap:
                gaps_count += 1

    gaps_count += retire_noisy_devsync_gaps()

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
    lower = name.strip().lower()
    return (
        lower == "merge"
        or lower.startswith("fix:")
        or lower.startswith("fix ")
        or lower.startswith("feat:")
        or lower.startswith("feat ")
        or lower.startswith("docs:")
        or lower.startswith("docs ")
        or lower.startswith("merge:")
        or lower.startswith("merge ")
        or lower.startswith("refactor:")
        or lower.startswith("refactor ")
        or lower.startswith("chore:")
        or lower.startswith("chore ")
        or lower.startswith("hermes:")
        or lower.startswith("trigger:")
        or lower.startswith("trigger ")
        or len(name) > 60
    )


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
