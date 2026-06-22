import json
from pathlib import Path

import yaml


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(record) for record in records) + "\n", encoding="utf-8")


def _write_yaml(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")


def _write_feature_md(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        """---
featureId: feature.hermes_feature_pmo_cockpit
workstream: Hermes
status: in_progress
owner: codex
branch: codex/hermes-feature-pmo-goal-board
---

# Hermes Feature PMO Cockpit

## Goal

Show feature lifecycle state with evidence checklists.

## Backend Goal

New API endpoints:

- GET /v1/hermes/goals/features

## Frontend Goal

New component: `06_AppPlatform/frontend/src/components/HermesFeaturePmoBoard.tsx`

## Phase Checklist

- [x] PRD / feature MD exists
- [ ] Unit tests added or updated

## Acceptance Criteria

- [ ] Each feature has a computed lifecycle state.
""",
        encoding="utf-8",
    )


def test_feature_goal_service_parses_markdown_and_evidence(tmp_path, monkeypatch):
    from app.services import hermes_feature_goal_service as goals
    from app.services import hermes_history_service as history

    monkeypatch.setattr(goals, "_project_root", tmp_path)
    monkeypatch.setattr(history, "_project_root", tmp_path)
    monkeypatch.setattr(goals, "_list_git_worktrees", lambda: [
        {
            "path": str(tmp_path / "JATO_Analysis_System_hermes_feature_pmo"),
            "branch": "codex/hermes-feature-pmo-goal-board",
            "head": "abc123",
        }
    ])
    monkeypatch.setattr(goals, "_worktree_status", lambda path: {
        "path": path,
        "state": "dirty",
        "isDirty": True,
        "stagedCount": 1,
        "modifiedCount": 2,
        "untrackedCount": 1,
        "deletedCount": 0,
        "conflictedCount": 0,
        "files": ["06_AppPlatform/backend/app/services/hermes_feature_goal_service.py"],
    })
    _write_feature_md(tmp_path / "Markdown_Readme" / "Fullstack" / "Hermes" / "HERMES_FEATURE_PMO_GOAL_2026-06-17.md")
    _write_yaml(tmp_path / "hermes" / "registry" / "features.yaml", {
        "features": [
            {
                "featureId": "feature.hermes_feature_pmo_cockpit",
                "title": "Hermes Feature PMO Cockpit",
                "status": "implemented",
                "tests": {"backend": "pending"},
                "docs": ["Markdown_Readme/Fullstack/Hermes/HERMES_FEATURE_PMO_GOAL_2026-06-17.md"],
            }
        ]
    })
    _write_jsonl(tmp_path / "hermes" / "dev_events" / "dev_events.jsonl", [
        {
            "eventId": "evt_goal_backend",
            "eventType": "implementation_completed",
            "source": "codex",
            "title": "Hermes Feature PMO backend",
            "linkedFeatureIds": ["feature.hermes_feature_pmo_cockpit"],
            "changedFiles": [
                "06_AppPlatform/backend/app/services/hermes_feature_goal_service.py",
                "06_AppPlatform/backend/tests/unit/test_hermes_feature_goal_service.py",
            ],
            "tests": {"backend": "1 passed"},
            "createdAt": "2026-06-17T08:00:00Z",
        }
    ])

    result = goals.list_feature_goals()
    feature = [item for item in result["features"] if item["featureId"] == "feature.hermes_feature_pmo_cockpit"][0]

    assert result["summary"]["total"] == 1
    assert feature["workstream"] == "Hermes"
    assert feature["state"] == "ready_for_pr"
    assert feature["checklist"][0]["checked"] is True
    assert any(candidate["path"].endswith("hermes_history_service.py") for candidate in feature["reuseCandidates"])
    assert feature["evidenceSummary"]["tests"] == 1
    assert feature["linkedWorktree"].endswith("JATO_Analysis_System_hermes_feature_pmo")
    assert feature["worktreeStatus"]["state"] == "dirty"
    assert feature["worktreeStatus"]["untrackedCount"] == 1
    assert feature["worktreeStatus"]["scopeState"] == "in_scope"
    assert feature["worktreeStatus"]["inScopeCount"] == 1


def test_feature_goal_service_parses_git_worktree_porcelain():
    from app.services.hermes_feature_goal_service import _parse_git_worktree_porcelain

    worktrees = _parse_git_worktree_porcelain(
        """worktree /repo/main
HEAD aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
branch refs/heads/main

worktree /repo/hermes
HEAD bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
branch refs/heads/codex/hermes-history-progress-cockpit-clean

"""
    )

    assert worktrees == [
        {"path": "/repo/main", "head": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "branch": "main"},
        {
            "path": "/repo/hermes",
            "head": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "branch": "codex/hermes-history-progress-cockpit-clean",
        },
    ]


def test_feature_goal_service_parses_git_status_porcelain():
    from app.services.hermes_feature_goal_service import _parse_git_status_porcelain

    status = _parse_git_status_porcelain(
        """ M 06_AppPlatform/frontend/src/components/HermesFeaturePmoBoard.tsx
A  Markdown_Readme/Fullstack/Hermes/HERMES_FEATURE_PMO_GOAL_2026-06-17.md
?? 06_AppPlatform/backend/app/services/hermes_feature_goal_service.py
UU 06_AppPlatform/frontend/src/types/hermes.ts
D  old_file.py
""",
        "/repo/hermes",
    )

    assert status["path"] == "/repo/hermes"
    assert status["state"] == "dirty"
    assert status["isDirty"] is True
    assert status["modifiedCount"] == 2
    assert status["stagedCount"] == 3
    assert status["untrackedCount"] == 1
    assert status["deletedCount"] == 1
    assert status["conflictedCount"] == 1
    assert status["files"][0] == "06_AppPlatform/frontend/src/components/HermesFeaturePmoBoard.tsx"


def test_feature_goal_service_classifies_worktree_scope():
    from app.services.hermes_feature_goal_service import _parse_git_status_porcelain, _scope_worktree_status

    status = _parse_git_status_porcelain(
        """ M 06_AppPlatform/frontend/src/components/HermesFeaturePmoBoard.tsx
 M 06_AppPlatform/backend/app/services/msrp_scraping_service.py
 M hermes/sentinel_notifications.jsonl
""",
        "/repo/hermes",
    )

    scoped = _scope_worktree_status(
        status,
        "Hermes",
        ["06_AppPlatform/frontend/src/components/HermesFeaturePmoBoard.tsx"],
    )

    assert scoped["scopeState"] == "mixed_scope"
    assert scoped["scopeWorkstream"] == "Hermes"
    assert scoped["inScopeCount"] == 1
    assert scoped["outOfScopeCount"] == 1
    assert scoped["generatedCount"] == 1
    assert scoped["outOfScopeFiles"] == ["06_AppPlatform/backend/app/services/msrp_scraping_service.py"]
    assert scoped["generatedFiles"] == ["hermes/sentinel_notifications.jsonl"]


def test_feature_goal_service_ignores_example_branch_inside_code_block(tmp_path, monkeypatch):
    from app.services import hermes_feature_goal_service as goals
    from app.services import hermes_history_service as history

    monkeypatch.setattr(goals, "_project_root", tmp_path)
    monkeypatch.setattr(history, "_project_root", tmp_path)
    monkeypatch.setattr(goals, "_list_git_worktrees", lambda: [
        {
            "path": str(tmp_path / "JATO_Analysis_System_hermes_history_clean"),
            "branch": "codex/hermes-history-progress-cockpit-clean",
            "head": "def456",
        }
    ])
    monkeypatch.setattr(goals, "_worktree_status", lambda path: {
        "path": path,
        "state": "clean",
        "isDirty": False,
        "stagedCount": 0,
        "modifiedCount": 0,
        "untrackedCount": 0,
        "deletedCount": 0,
        "conflictedCount": 0,
        "files": [],
    })
    path = tmp_path / "Markdown_Readme" / "Fullstack" / "Hermes" / "HERMES_FEATURE_PMO_GOAL_2026-06-17.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        """# Hermes Feature PMO Goal

> Target featureId: `feature.hermes_feature_pmo_cockpit`
> Current base: `codex/hermes-history-progress-cockpit-clean`

```md
---
featureId: feature.example
branch: codex/example-feature
---
```
""",
        encoding="utf-8",
    )

    feature = goals.get_feature_goal("feature.hermes_feature_pmo_cockpit")

    assert feature["branch"] == "codex/hermes-history-progress-cockpit-clean"
    assert feature["linkedWorktree"].endswith("JATO_Analysis_System_hermes_history_clean")
    assert feature["worktreeStatus"]["state"] == "clean"


def test_feature_goal_service_keeps_verified_blocked_without_smoke(tmp_path, monkeypatch):
    from app.services import hermes_feature_goal_service as goals
    from app.services import hermes_history_service as history

    monkeypatch.setattr(goals, "_project_root", tmp_path)
    monkeypatch.setattr(history, "_project_root", tmp_path)
    _write_feature_md(tmp_path / "Markdown_Readme" / "features" / "feature.example_verified.md")
    _write_yaml(tmp_path / "hermes" / "registry" / "features.yaml", {
        "features": [
            {
                "featureId": "feature.hermes_feature_pmo_cockpit",
                "title": "Hermes Feature PMO Cockpit",
                "status": "deployed",
                "tests": {"backend": "1 passed"},
            }
        ]
    })

    feature = goals.get_feature_goal("feature.hermes_feature_pmo_cockpit")

    assert feature["state"] in {"tested", "ready_for_pr", "deployed"}
    assert feature["state"] != "verified"


def test_feature_goal_swimlanes_groups_by_workstream(tmp_path, monkeypatch):
    from app.services import hermes_feature_goal_service as goals
    from app.services import hermes_history_service as history

    monkeypatch.setattr(goals, "_project_root", tmp_path)
    monkeypatch.setattr(history, "_project_root", tmp_path)
    _write_feature_md(tmp_path / "Markdown_Readme" / "features" / "feature.hermes_feature_pmo_cockpit.md")

    result = goals.get_feature_goal_swimlanes()

    assert result["summary"]["workstreamCount"] == 1
    assert result["lanes"][0]["workstream"] == "Hermes"
    assert result["lanes"][0]["features"][0]["featureId"] == "feature.hermes_feature_pmo_cockpit"
