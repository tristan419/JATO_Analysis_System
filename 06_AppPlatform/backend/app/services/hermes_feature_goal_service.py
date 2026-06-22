"""Hermes Feature PMO goal aggregation.

This service reads Markdown feature contracts and combines them with existing
Hermes history/progress evidence. It is read-only and does not mutate docs,
registries, ledgers, or git state.
"""

from __future__ import annotations

import re
import subprocess
from collections import defaultdict
from pathlib import Path
from typing import Any

from app.services.hermes_feature_state_machine import (
    build_evidence_checklist,
    compute_feature_state,
)
from app.services.hermes_reuse_radar_service import list_reuse_candidates

_project_root: Path | None = None

_FRONTMATTER_RE = re.compile(r"\A---\s*\n(?P<body>.*?)\n---\s*\n", re.DOTALL)
_FENCED_CODE_RE = re.compile(r"```.*?```", re.DOTALL)
_CHECKBOX_RE = re.compile(r"^\s*[-*]\s+\[(?P<mark>[ xX])\]\s+(?P<label>.+?)\s*$", re.MULTILINE)
_HEADING_RE = re.compile(r"^#\s+(?P<title>.+?)\s*$", re.MULTILINE)
_PATH_RE = re.compile(r"`(?P<path>(?:0[1-9]_|Markdown_Readme/|hermes/|\.github/)[^`]+?)`")
_GENERATED_STATUS_PREFIXES = (
    "06_AppPlatform/frontend/coverage/",
    "06_AppPlatform/frontend/playwright-report/",
    "06_AppPlatform/frontend/test-results/",
)
_GENERATED_STATUS_FILES = {
    "hermes/sentinel_notifications.jsonl",
}
_HERMES_SCOPE_PREFIXES = (
    "Markdown_Readme/Fullstack/Hermes/",
    "06_AppPlatform/backend/app/services/hermes",
    "06_AppPlatform/backend/tests/unit/test_hermes",
    "06_AppPlatform/frontend/src/components/Hermes",
    "06_AppPlatform/frontend/src/tests/unit/hermes",
    "hermes/",
)
_HERMES_SCOPE_FILES = {
    "06_AppPlatform/backend/app/api/routes/hermes.py",
    "06_AppPlatform/frontend/src/api/client.ts",
    "06_AppPlatform/frontend/src/pages/DataManagementPage.tsx",
    "06_AppPlatform/frontend/src/types/hermes.ts",
}
_GENERIC_WORKSTREAMS = {"Backend", "Frontend", "Docs / Tests", "General"}


def _root() -> Path:
    global _project_root
    if _project_root is None:
        from app.api.routes.hermes import PROJECT_ROOT

        _project_root = PROJECT_ROOT
    return _project_root


def _relative(path: Path) -> str:
    try:
        return str(path.relative_to(_root()))
    except ValueError:
        return str(path)


def _feature_key(feature_id: str) -> str:
    raw = str(feature_id or "").strip().lower()
    raw = re.sub(r"^feature[._-]", "", raw)
    raw = re.sub(r"^proposal[._-]", "", raw)
    return re.sub(r"[-_.]+", "-", raw).strip("-")


def _safe_slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def _append_unique(values: list[str], item: Any) -> None:
    text = str(item or "").strip()
    if text and text not in values:
        values.append(text)


def _normalize_branch(value: str) -> str:
    raw = str(value or "").strip()
    if raw.startswith("refs/heads/"):
        return raw.removeprefix("refs/heads/")
    return raw


def _parse_git_worktree_porcelain(output: str) -> list[dict[str, str]]:
    worktrees: list[dict[str, str]] = []
    current: dict[str, str] = {}
    for line in output.splitlines():
        if not line.strip():
            if current:
                worktrees.append(current)
                current = {}
            continue
        key, _, value = line.partition(" ")
        value = value.strip()
        if key == "worktree":
            current = {"path": value}
        elif key == "HEAD":
            current["head"] = value
        elif key == "branch":
            current["branch"] = _normalize_branch(value)
    if current:
        worktrees.append(current)
    return worktrees


def _list_git_worktrees() -> list[dict[str, str]]:
    try:
        result = subprocess.run(
            ["git", "worktree", "list", "--porcelain"],
            cwd=_root(),
            capture_output=True,
            text=True,
            timeout=4,
            check=False,
        )
    except Exception:
        return []
    if result.returncode != 0:
        return []
    return _parse_git_worktree_porcelain(result.stdout)


def _linked_worktree_for_branch(branch: str, worktrees: list[dict[str, str]]) -> str:
    target = _normalize_branch(branch)
    if not target:
        return ""
    for worktree in worktrees:
        if _normalize_branch(worktree.get("branch", "")) == target:
            return worktree.get("path", "")
    return ""


def _empty_worktree_status(path: str = "", state: str = "unlinked") -> dict[str, Any]:
    return {
        "path": path,
        "state": state,
        "isDirty": False,
        "stagedCount": 0,
        "modifiedCount": 0,
        "untrackedCount": 0,
        "deletedCount": 0,
        "conflictedCount": 0,
        "files": [],
        "scopeWorkstream": "",
        "scopeState": "not_applicable",
        "inScopeCount": 0,
        "outOfScopeCount": 0,
        "unknownScopeCount": 0,
        "generatedCount": 0,
        "inScopeFiles": [],
        "outOfScopeFiles": [],
        "unknownScopeFiles": [],
        "generatedFiles": [],
    }


def _porcelain_path(line: str) -> str:
    raw = line[3:] if len(line) > 3 else ""
    if " -> " in raw:
        raw = raw.split(" -> ", 1)[1]
    return raw.strip().strip('"')


def _parse_git_status_porcelain(output: str, path: str = "") -> dict[str, Any]:
    status = _empty_worktree_status(path, "clean")
    conflict_states = {"DD", "AU", "UD", "UA", "DU", "AA", "UU"}
    for line in output.splitlines():
        if not line.strip():
            continue
        code = line[:2]
        file_path = _porcelain_path(line)
        _append_unique(status["files"], file_path)
        if len(status["files"]) > 12:
            status["files"] = status["files"][:12]
        if code == "??":
            status["untrackedCount"] += 1
            continue
        if code in conflict_states:
            status["conflictedCount"] += 1
        if code[0] not in {" ", "?"}:
            status["stagedCount"] += 1
        if code[1] not in {" ", "?"}:
            status["modifiedCount"] += 1
        if "D" in code:
            status["deletedCount"] += 1
    dirty_count = (
        status["stagedCount"]
        + status["modifiedCount"]
        + status["untrackedCount"]
        + status["conflictedCount"]
    )
    if dirty_count > 0:
        status["state"] = "dirty"
        status["isDirty"] = True
    return status


def _worktree_status(path: str) -> dict[str, Any]:
    if not path:
        return _empty_worktree_status()
    worktree_path = Path(path)
    if not worktree_path.is_dir():
        return _empty_worktree_status(path, "missing")
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain=v1", "--untracked-files=all"],
            cwd=worktree_path,
            capture_output=True,
            text=True,
            timeout=4,
            check=False,
        )
    except Exception:
        return _empty_worktree_status(path, "error")
    if result.returncode != 0:
        return _empty_worktree_status(path, "error")
    return _parse_git_status_porcelain(result.stdout, path)


def _is_generated_status_file(path: str) -> bool:
    return path in _GENERATED_STATUS_FILES or any(path.startswith(prefix) for prefix in _GENERATED_STATUS_PREFIXES)


def _is_hermes_scope_file(path: str) -> bool:
    return path in _HERMES_SCOPE_FILES or any(path.startswith(prefix) for prefix in _HERMES_SCOPE_PREFIXES)


def _normalized_workstream(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def _scope_worktree_status(status: dict[str, Any], workstream: str, feature_files: list[str]) -> dict[str, Any]:
    scoped = dict(status)
    scoped.setdefault("files", [])
    target = str(workstream or "").strip()
    feature_file_set = {str(path or "").strip() for path in feature_files if str(path or "").strip()}
    in_scope_files: list[str] = []
    out_of_scope_files: list[str] = []
    unknown_scope_files: list[str] = []
    generated_files: list[str] = []

    from app.services.hermes_history_service import infer_workstream

    for path in scoped["files"]:
        clean_path = str(path or "").strip()
        if not clean_path:
            continue
        if _is_generated_status_file(clean_path):
            _append_unique(generated_files, clean_path)
            continue
        inferred_workstream = infer_workstream("", "", [clean_path])
        same_workstream = _normalized_workstream(inferred_workstream) == _normalized_workstream(target)
        if (
            clean_path in feature_file_set
            or same_workstream
            or (_normalized_workstream(target) == "hermes" and _is_hermes_scope_file(clean_path))
        ):
            _append_unique(in_scope_files, clean_path)
        elif inferred_workstream in _GENERIC_WORKSTREAMS:
            _append_unique(unknown_scope_files, clean_path)
        else:
            _append_unique(out_of_scope_files, clean_path)

    scoped["scopeWorkstream"] = target
    scoped["inScopeCount"] = len(in_scope_files)
    scoped["outOfScopeCount"] = len(out_of_scope_files)
    scoped["unknownScopeCount"] = len(unknown_scope_files)
    scoped["generatedCount"] = len(generated_files)
    scoped["inScopeFiles"] = in_scope_files[:8]
    scoped["outOfScopeFiles"] = out_of_scope_files[:8]
    scoped["unknownScopeFiles"] = unknown_scope_files[:8]
    scoped["generatedFiles"] = generated_files[:8]

    if scoped.get("state") == "unlinked":
        scoped["scopeState"] = "not_applicable"
    elif scoped.get("state") in {"missing", "error"}:
        scoped["scopeState"] = "unknown"
    elif not scoped["files"]:
        scoped["scopeState"] = "clean"
    elif out_of_scope_files and (in_scope_files or unknown_scope_files):
        scoped["scopeState"] = "mixed_scope"
    elif out_of_scope_files:
        scoped["scopeState"] = "out_of_scope"
    elif unknown_scope_files:
        scoped["scopeState"] = "unknown"
    elif in_scope_files:
        scoped["scopeState"] = "in_scope"
    elif generated_files:
        scoped["scopeState"] = "generated_only"
    else:
        scoped["scopeState"] = "not_applicable"
    return scoped


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return ""


def _strip_fenced_code_blocks(text: str) -> str:
    return _FENCED_CODE_RE.sub("", text)


def _parse_frontmatter(text: str) -> dict[str, Any]:
    match = _FRONTMATTER_RE.match(text)
    if not match:
        return {}
    try:
        import yaml

        data = yaml.safe_load(match.group("body")) or {}
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _frontmatter_value(frontmatter: dict[str, Any], *keys: str) -> str:
    for key in keys:
        if frontmatter.get(key):
            return str(frontmatter[key]).strip()
    return ""


def _body_field(text: str, *labels: str) -> str:
    for label in labels:
        pattern = re.compile(
            rf"^\s*(?:>\s*)?(?:[-*]\s*)?{re.escape(label)}\s*:\s*`?(?P<value>[^`\n]+?)`?\s*$",
            re.IGNORECASE | re.MULTILINE,
        )
        match = pattern.search(text)
        if match:
            return match.group("value").strip()
    return ""


def _title_from_path(path: Path) -> str:
    stem = path.stem
    clean = re.sub(r"^feature[._-]", "", stem)
    return " ".join(part.upper() if part in {"api", "pmo", "mcp", "ui"} else part.capitalize() for part in re.split(r"[-_.]+", clean) if part)


def _extract_title(path: Path, text: str) -> str:
    match = _HEADING_RE.search(text)
    if match:
        return match.group("title").strip()
    return _title_from_path(path)


def _extract_feature_id(path: Path, text: str, frontmatter: dict[str, Any]) -> str:
    explicit = _frontmatter_value(frontmatter, "featureId", "feature_id", "featureID")
    if explicit and "[name]" not in explicit and "example" not in explicit:
        return explicit
    explicit = _body_field(text, "Target featureId", "Feature ID", "featureId")
    if explicit and "[name]" not in explicit and "example" not in explicit:
        return explicit
    if path.stem.startswith("feature."):
        return path.stem
    if "Markdown_Readme/features" in _relative(path):
        return path.stem
    return ""


def _extract_workstream(feature_id: str, title: str, text: str, frontmatter: dict[str, Any], files: list[str]) -> str:
    explicit = _frontmatter_value(frontmatter, "workstream", "category")
    explicit = explicit or _body_field(text, "Workstream")
    if explicit:
        return explicit
    from app.services.hermes_history_service import infer_workstream

    return infer_workstream(feature_id, title, files)


def _checkbox_key(label: str) -> str:
    normalized = label.lower()
    if "prd" in normalized or "feature md" in normalized:
        return "prd_md_exists"
    if "reuse" in normalized or "复用" in normalized:
        return "reuse_candidates_identified"
    if "backend contract" in normalized or "api" in normalized and "defined" in normalized:
        return "backend_contract_defined"
    if "backend implemented" in normalized or "backend implementation" in normalized:
        return "backend_implemented"
    if "frontend contract" in normalized or "typescript" in normalized and "contract" in normalized:
        return "frontend_contract_defined"
    if "frontend implemented" in normalized or "frontend implementation" in normalized:
        return "frontend_implemented"
    if "unit test" in normalized or "tests added" in normalized or "tests updated" in normalized:
        return "unit_tests_added"
    if "type" in normalized or "build" in normalized:
        return "type_build_checks_passed"
    if "smoke" in normalized or "evidence" in normalized:
        return "smoke_evidence_attached"
    if "docs" in normalized or "document" in normalized:
        return "docs_updated"
    if "pr opened" in normalized:
        return "pr_opened"
    if "pr merged" in normalized:
        return "pr_merged"
    if "deployed" in normalized:
        return "deployed"
    if "verified" in normalized:
        return "verified"
    return _safe_slug(label)


def _extract_checkboxes(text: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for match in _CHECKBOX_RE.finditer(text):
        label = match.group("label").strip()
        items.append({
            "key": _checkbox_key(label),
            "label": label,
            "checked": match.group("mark").lower() == "x",
        })
    return items


def _extract_paths(text: str) -> list[str]:
    values: list[str] = []
    for match in _PATH_RE.finditer(text):
        path = match.group("path").strip()
        if path.endswith(".") or len(path) > 180:
            continue
        _append_unique(values, path)
    return values


def _extract_markdown_goals() -> list[dict[str, Any]]:
    markdown_root = _root() / "Markdown_Readme"
    if not markdown_root.is_dir():
        return []
    goals: list[dict[str, Any]] = []
    for path in sorted(markdown_root.rglob("*.md")):
        rel = _relative(path)
        if "/_archived/" in rel or path.name == "HERMES_PRD_TEMPLATE.md":
            continue
        text = _read_text(path)
        if not text:
            continue
        frontmatter = _parse_frontmatter(text)
        field_text = _strip_fenced_code_blocks(text)
        feature_id = _extract_feature_id(path, field_text, frontmatter)
        checkboxes = _extract_checkboxes(field_text)
        if not feature_id and not checkboxes:
            continue
        if not feature_id:
            feature_id = f"feature.{_safe_slug(path.stem)}"
        files = _extract_paths(text)
        title = _extract_title(path, field_text)
        goals.append({
            "featureId": feature_id,
            "featureKey": _feature_key(feature_id),
            "title": title,
            "workstream": _extract_workstream(feature_id, title, field_text, frontmatter, files),
            "status": _frontmatter_value(frontmatter, "status") or _body_field(field_text, "Status") or "",
            "owner": _frontmatter_value(frontmatter, "owner") or _body_field(field_text, "Owner") or "",
            "branch": _frontmatter_value(frontmatter, "branch") or _body_field(field_text, "Branch", "Current base") or "",
            "sourceDocs": [rel],
            "declaredChecklist": checkboxes,
            "declaredChecks": {item["key"]: bool(item["checked"]) for item in checkboxes},
            "fileRefs": files,
            "content": text[:12000],
        })
    return goals


def _events_by_feature() -> dict[str, list[dict[str, Any]]]:
    from app.services.hermes_history_service import list_history_events

    try:
        events = list_history_events(limit=500).get("events", [])
    except Exception:
        events = []
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        grouped[_feature_key(str(event.get("featureId") or ""))].append(event)
    return grouped


def _progress_by_feature() -> dict[str, dict[str, Any]]:
    from app.services.hermes_history_service import list_progress_features

    try:
        features = list_progress_features()
    except Exception:
        features = []
    return {
        _feature_key(str(feature.get("featureId") or "")): feature
        for feature in features
        if str(feature.get("featureId") or "").strip()
    }


def _event_files(events: list[dict[str, Any]]) -> list[str]:
    files: list[str] = []
    for event in events:
        for path in event.get("files", []) or []:
            _append_unique(files, path)
    return files


def _event_text(events: list[dict[str, Any]]) -> str:
    values: list[str] = []
    for event in events:
        values.append(str(event.get("title") or ""))
        values.append(str(event.get("summary") or ""))
        values.extend(str(item) for item in event.get("tests", []) or [])
    return " ".join(values).lower()


def _add_source(sources: dict[str, list[str]], key: str, label: str) -> None:
    _append_unique(sources[key], label)


def _evidence_sources(
    goal: dict[str, Any],
    progress: dict[str, Any] | None,
    events: list[dict[str, Any]],
    reuse_candidates: list[dict[str, Any]],
) -> dict[str, list[str]]:
    progress = progress or {}
    sources: dict[str, list[str]] = defaultdict(list)
    event_files = _event_files(events)
    progress_files = progress.get("topFiles", [])
    doc_files = goal.get("fileRefs", [])
    implementation_files = event_files + progress_files
    contract_files = implementation_files + doc_files
    doc_text = str(goal.get("content") or "").lower()
    event_text = _event_text(events)

    if goal.get("sourceDocs"):
        for doc in goal["sourceDocs"]:
            _add_source(sources, "prd_md_exists", doc)
            _add_source(sources, "docs_updated", doc)
    if reuse_candidates:
        _add_source(sources, "reuse_candidates_identified", "Hermes reuse radar")

    if "new api endpoints" in doc_text or "modified api endpoints" in doc_text or any("/api/routes/" in path for path in contract_files):
        _add_source(sources, "backend_contract_defined", "API/route contract detected")
    if any(path.startswith("06_AppPlatform/backend/app/") for path in implementation_files):
        _add_source(sources, "backend_implemented", "backend code evidence")

    if "frontend requirements" in doc_text or any(path.endswith(("client.ts", "hermes.ts")) for path in contract_files):
        _add_source(sources, "frontend_contract_defined", "frontend contract detected")
    if any(path.startswith("06_AppPlatform/frontend/src/") and path.endswith((".ts", ".tsx")) for path in implementation_files):
        _add_source(sources, "frontend_implemented", "frontend code evidence")

    if int(progress.get("testsCount") or 0) > 0 or any("/tests/" in path or "/src/tests/" in path or "test_" in path for path in event_files):
        _add_source(sources, "unit_tests_added", "test evidence")
    if any(token in event_text for token in ("typecheck", "check:types", "npm run build", "build passed", "tsc")):
        _add_source(sources, "type_build_checks_passed", "type/build evidence")
    if int(progress.get("evidenceCount") or 0) > 0 or any(token in event_text for token in ("smoke", "playwright", "screenshot", "manual verification")):
        _add_source(sources, "smoke_evidence_attached", "smoke/evidence ledger")

    if str(progress.get("sessionId") or goal.get("branch") or "").strip() or events:
        _add_source(sources, "active_development", "branch/session/event activity")
    if any("pr opened" in str(event.get("title") or "").lower() for event in events):
        _add_source(sources, "pr_opened", "DevSync PR event")
    if any("pr merged" in str(event.get("title") or "").lower() or "merge" == str(event.get("type") or "").lower() for event in events):
        _add_source(sources, "pr_merged", "merge event")
    if str(progress.get("deployStatus") or "") == "tracked" or any("deploy" in str(event.get("title") or "").lower() for event in events):
        _add_source(sources, "deployed", "deploy status/event")
    if str(progress.get("phase") or "") in {"Verified", "Resolved"} and sources.get("smoke_evidence_attached"):
        _add_source(sources, "verified", "verified smoke evidence")

    return dict(sources)


def _merge_goal_with_progress(goal: dict[str, Any], progress: dict[str, Any] | None) -> dict[str, Any]:
    if not progress:
        return goal
    merged = dict(goal)
    for key in ("title", "workstream", "owner", "status"):
        if not merged.get(key) and progress.get(key):
            merged[key] = progress[key]
    merged["progressPhase"] = str(progress.get("phase") or "")
    merged["progressStatus"] = str(progress.get("status") or "")
    return merged


def _goal_record(
    goal: dict[str, Any],
    progress: dict[str, Any] | None,
    events: list[dict[str, Any]],
    worktrees: list[dict[str, str]],
) -> dict[str, Any]:
    merged = _merge_goal_with_progress(goal, progress)
    files = _event_files(events) + merged.get("fileRefs", []) + (progress or {}).get("topFiles", [])
    branch = str(merged.get("branch") or "")
    reuse_candidates = list_reuse_candidates(
        feature_id=str(merged.get("featureId") or ""),
        title=str(merged.get("title") or ""),
        workstream=str(merged.get("workstream") or ""),
        content=str(merged.get("content") or ""),
        files=files,
    )
    evidence_sources = _evidence_sources(merged, progress, events, reuse_candidates)
    checklist = build_evidence_checklist(evidence_sources, merged.get("declaredChecks", {}))
    evidence_flags = {item["key"]: item["checked"] for item in checklist}
    evidence_flags["active_development"] = bool(evidence_sources.get("active_development"))
    state = compute_feature_state(
        evidence_flags,
        declared_status=str(merged.get("status") or (progress or {}).get("status") or ""),
        risk=str((progress or {}).get("risk") or "low"),
        open_gap_count=int((progress or {}).get("openGapCount") or 0),
    )
    linked_worktree = _linked_worktree_for_branch(branch, worktrees)
    worktree_status = _scope_worktree_status(
        _worktree_status(linked_worktree),
        str(merged.get("workstream") or "General"),
        files,
    )
    return {
        "featureId": str(merged.get("featureId") or ""),
        "title": str(merged.get("title") or "Untitled feature"),
        "workstream": str(merged.get("workstream") or "General"),
        "owner": str(merged.get("owner") or ""),
        "branch": branch,
        "state": state["state"],
        "blocked": state["blocked"],
        "risk": str((progress or {}).get("risk") or "low"),
        "nextAction": state["nextAction"],
        "missingEvidence": state["missingEvidence"],
        "sourceDocs": merged.get("sourceDocs", []),
        "linkedPrs": [],
        "linkedWorktree": linked_worktree,
        "worktreeStatus": worktree_status,
        "lastEventAt": str((progress or {}).get("lastEventAt") or (events[0].get("timestamp") if events else "")),
        "lastMeaningfulEvent": str((progress or {}).get("lastMeaningfulEvent") or (events[0].get("title") if events else "")),
        "checklist": checklist,
        "declaredChecklist": merged.get("declaredChecklist", []),
        "reuseCandidates": reuse_candidates,
        "evidenceSummary": {
            "events": len(events),
            "docs": len(merged.get("sourceDocs", [])),
            "tests": int((progress or {}).get("testsCount") or 0),
            "evidence": int((progress or {}).get("evidenceCount") or 0),
            "openGaps": int((progress or {}).get("openGapCount") or 0),
            "commits": int((progress or {}).get("commitCount") or 0),
        },
        "topFiles": files[:10],
    }


def list_feature_goals() -> dict[str, Any]:
    markdown_goals = _extract_markdown_goals()
    events_map = _events_by_feature()
    progress_map = _progress_by_feature()
    worktrees = _list_git_worktrees()
    by_key: dict[str, dict[str, Any]] = {}

    for goal in markdown_goals:
        key = str(goal.get("featureKey") or _feature_key(str(goal.get("featureId") or "")))
        current = by_key.get(key)
        if current:
            current["sourceDocs"].extend(doc for doc in goal["sourceDocs"] if doc not in current["sourceDocs"])
            current["content"] = f"{current.get('content', '')}\n{goal.get('content', '')}"[:12000]
            current["declaredChecklist"].extend(goal.get("declaredChecklist", []))
            current["declaredChecks"].update(goal.get("declaredChecks", {}))
            current["fileRefs"].extend(path for path in goal.get("fileRefs", []) if path not in current["fileRefs"])
        else:
            by_key[key] = goal

    for key, progress in progress_map.items():
        if not key:
            continue
        by_key.setdefault(key, {
            "featureId": str(progress.get("featureId") or ""),
            "featureKey": key,
            "title": str(progress.get("title") or ""),
            "workstream": str(progress.get("workstream") or "General"),
            "status": str(progress.get("status") or ""),
            "owner": str(progress.get("owner") or ""),
            "branch": "",
            "sourceDocs": [],
            "declaredChecklist": [],
            "declaredChecks": {},
            "fileRefs": progress.get("topFiles", []),
            "content": "",
        })

    features = [
        _goal_record(
            goal,
            progress_map.get(key),
            sorted(events_map.get(key, []), key=lambda event: str(event.get("timestamp") or ""), reverse=True),
            worktrees,
        )
        for key, goal in by_key.items()
    ]
    state_rank = {
        "blocked": 0,
        "ready_for_pr": 1,
        "in_review": 2,
        "in_progress": 3,
        "implemented": 4,
        "tested": 5,
        "prd_ready": 6,
        "ready_for_dev": 7,
        "deployed": 8,
        "verified": 9,
        "done": 10,
        "draft": 11,
        "archived": 12,
    }
    features.sort(key=lambda feature: str(feature.get("lastEventAt") or ""), reverse=True)
    features.sort(key=lambda feature: state_rank.get(str(feature.get("state") or ""), 20))
    return {
        "summary": {
            "total": len(features),
            "blocked": sum(1 for feature in features if feature.get("state") == "blocked"),
            "readyForPr": sum(1 for feature in features if feature.get("state") == "ready_for_pr"),
            "inProgress": sum(1 for feature in features if feature.get("state") in {"in_progress", "implemented", "tested"}),
            "verified": sum(1 for feature in features if feature.get("state") in {"verified", "done"}),
            "workstreamCount": len({feature.get("workstream") for feature in features}),
        },
        "features": features,
    }


def get_feature_goal(feature_id: str) -> dict[str, Any]:
    key = _feature_key(feature_id)
    for feature in list_feature_goals()["features"]:
        if _feature_key(str(feature.get("featureId") or "")) == key:
            return feature
    from fastapi import HTTPException

    raise HTTPException(status_code=404, detail=f"Feature goal not found: {feature_id}")


def get_feature_goal_swimlanes() -> dict[str, Any]:
    data = list_feature_goals()
    lanes_map: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for feature in data["features"]:
        lanes_map[str(feature.get("workstream") or "General")].append(feature)
    lanes = [
        {"workstream": workstream, "features": features}
        for workstream, features in sorted(lanes_map.items())
    ]
    return {**data, "lanes": lanes}


def get_reuse_candidates_for_feature(feature_id: str) -> dict[str, Any]:
    feature = get_feature_goal(feature_id)
    return {
        "featureId": feature["featureId"],
        "title": feature["title"],
        "workstream": feature["workstream"],
        "candidates": feature["reuseCandidates"],
    }
