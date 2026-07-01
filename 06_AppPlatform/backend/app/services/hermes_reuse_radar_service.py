"""Rules-based Hermes reuse radar.

This first implementation is intentionally deterministic. It recommends known
project assets before development without installing tools or running code.
"""

from __future__ import annotations

from typing import Any


_REUSE_CATALOG: list[dict[str, Any]] = [
    {
        "category": "Hermes backend aggregation",
        "path": "06_AppPlatform/backend/app/services/hermes_history_service.py",
        "reason": "Existing normalized history/progress/event readers should be reused for feature status evidence.",
        "keywords": ("hermes", "history", "progress", "state", "event", "evidence", "gap", "workflow"),
        "workstreams": ("Hermes",),
    },
    {
        "category": "Hermes API route",
        "path": "06_AppPlatform/backend/app/api/routes/hermes.py",
        "reason": "Hermes endpoints already share role gating, route prefix, and response patterns here.",
        "keywords": ("hermes", "api", "endpoint", "route", "fastapi", "backend"),
        "workstreams": ("Hermes",),
    },
    {
        "category": "Hermes frontend cockpit",
        "path": "06_AppPlatform/frontend/src/pages/DataManagementPage.tsx",
        "reason": "Hermes is surfaced in Data Management; new cockpit panels should attach to this existing page.",
        "keywords": ("hermes", "ui", "page", "cockpit", "dashboard", "frontend", "pmo"),
        "workstreams": ("Hermes", "Frontend"),
    },
    {
        "category": "Hermes progress component",
        "path": "06_AppPlatform/frontend/src/components/HermesProgressSwimlane.tsx",
        "reason": "Existing phase and evidence summary UI can guide feature lifecycle presentation.",
        "keywords": ("progress", "phase", "swimlane", "checklist", "feature", "pmo"),
        "workstreams": ("Hermes",),
    },
    {
        "category": "Hermes history component",
        "path": "06_AppPlatform/frontend/src/components/HermesHistoryMap.tsx",
        "reason": "Git History Cluster already handles Hermes timeline loading and dense card interactions.",
        "keywords": ("history", "cluster", "timeline", "git", "feature", "dashboard"),
        "workstreams": ("Hermes",),
    },
    {
        "category": "API client",
        "path": "06_AppPlatform/frontend/src/api/client.ts",
        "reason": "Frontend API calls should reuse the central typed request wrapper.",
        "keywords": ("api", "client", "frontend", "endpoint", "contract"),
        "workstreams": ("Frontend", "Hermes"),
    },
    {
        "category": "TypeScript contracts",
        "path": "06_AppPlatform/frontend/src/types/hermes.ts",
        "reason": "Hermes response shapes belong in the existing typed Hermes contract file.",
        "keywords": ("type", "typescript", "contract", "frontend", "schema", "hermes"),
        "workstreams": ("Frontend", "Hermes"),
    },
    {
        "category": "Backend tests",
        "path": "06_AppPlatform/backend/tests/unit/",
        "reason": "Hermes services are covered with focused pytest unit tests using tmp_path and monkeypatch.",
        "keywords": ("backend", "pytest", "test", "state", "service", "route"),
        "workstreams": ("Backend", "Hermes"),
    },
    {
        "category": "Frontend tests",
        "path": "06_AppPlatform/frontend/src/tests/unit/",
        "reason": "Hermes frontend changes should use Vitest and Testing Library patterns already in place.",
        "keywords": ("frontend", "vitest", "test", "tsx", "component", "ui"),
        "workstreams": ("Frontend", "Hermes"),
    },
    {
        "category": "Deck controls",
        "path": "06_AppPlatform/frontend/src/components/deckControls/",
        "reason": "Existing drawer and control patterns should be reused for dense analytical page controls.",
        "keywords": ("drawer", "deck", "control", "layout", "filter", "panel"),
        "workstreams": ("Frontend",),
    },
    {
        "category": "AstrBot cost ledger",
        "path": "06_AppPlatform/backend/app/services/hermes_cost_ledger_service.py",
        "reason": "AstrBot usage is already normalized into Hermes cost records and should not be recounted elsewhere.",
        "keywords": ("astrbot", "usage", "cost", "agent", "model", "token"),
        "workstreams": ("AstrBot / CountryCopilot", "Hermes"),
    },
    {
        "category": "MSRP toolkit tests",
        "path": "07_ScrapingToolkit/tests/",
        "reason": "MSRP scraper changes should reuse toolkit-level tests and source-draft validation patterns.",
        "keywords": ("msrp", "scraping", "source", "extractor", "yaml", "toolkit"),
        "workstreams": ("JATO Monthly / MSRP",),
    },
]


def list_reuse_candidates(
    *,
    feature_id: str = "",
    title: str = "",
    workstream: str = "",
    content: str = "",
    files: list[str] | None = None,
    limit: int = 12,
) -> list[dict[str, Any]]:
    """Return deterministic reuse candidates for a feature context."""

    files = files or []
    haystack = " ".join([feature_id, title, workstream, content, *files]).lower()
    scored: list[dict[str, Any]] = []
    for item in _REUSE_CATALOG:
        keyword_hits = [
            keyword for keyword in item["keywords"]
            if keyword.lower() in haystack
        ]
        workstream_match = workstream and workstream in item["workstreams"]
        path_match = any(str(path).startswith(str(item["path"])) for path in files)
        score = len(keyword_hits) + (3 if workstream_match else 0) + (2 if path_match else 0)
        if score <= 0:
            continue
        scored.append({
            "category": item["category"],
            "path": item["path"],
            "reason": item["reason"],
            "score": score,
            "matchedSignals": sorted(set(keyword_hits + ([workstream] if workstream_match else []))),
        })

    if not scored:
        for item in _REUSE_CATALOG[:6]:
            scored.append({
                "category": item["category"],
                "path": item["path"],
                "reason": item["reason"],
                "score": 1,
                "matchedSignals": ["default"],
            })

    scored.sort(key=lambda candidate: int(candidate["score"]), reverse=True)
    return scored[:limit]
