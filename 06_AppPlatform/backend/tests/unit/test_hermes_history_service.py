import json
from pathlib import Path

import yaml


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(record) for record in records) + "\n", encoding="utf-8")


def _write_yaml(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")


def test_history_events_normalize_devsync_evidence_and_sentinel(tmp_path, monkeypatch):
    from app.services import hermes_history_service as history

    monkeypatch.setattr(history, "_project_root", tmp_path)
    _write_jsonl(tmp_path / "hermes" / "dev_events" / "dev_events.jsonl", [
        {
            "eventId": "evt_1",
            "eventType": "implementation_completed",
            "source": "codex",
            "title": "Hermes History Map implementation",
            "summary": "Added history map.",
            "linkedFeatureIds": ["proposal.hermes_history_progress_cockpit"],
            "changedFiles": ["06_AppPlatform/backend/app/services/hermes_history_service.py"],
            "tests": {"backend": "2 passed"},
            "createdAt": "2026-06-12T08:00:00Z",
        }
    ])
    _write_jsonl(tmp_path / "hermes" / "evidence_ledger.jsonl", [
        {
            "evidenceId": "ev_1",
            "evidenceType": "test",
            "claim": "History service tests passed.",
            "artifactId": "feature.proposal.hermes_history_progress_cockpit",
            "createdAt": "2026-06-12T08:10:00Z",
        }
    ])
    _write_jsonl(tmp_path / "hermes" / "sentinel_notifications.jsonl", [
        {
            "id": "notif_1",
            "severity": "high",
            "source": "gaps",
            "title": "High Gaps",
            "body": "One high gap.",
            "context": {"findingType": "high_gaps", "blocking": True},
            "createdAt": "2026-06-12T08:20:00Z",
        }
    ])

    result = history.list_history_events(limit=20)

    assert result["summary"]["totalEvents"] == 3
    assert result["summary"]["workstreams"]["Hermes"] == 2
    assert [event["eventId"] for event in result["events"]][:2] == ["notif_1", "ev_1"]
    dev_event = [event for event in result["events"] if event["eventId"] == "evt_1"][0]
    assert dev_event["phase"] == "Tested"
    assert dev_event["testCount"] == 1


def test_history_clusters_group_by_feature_and_axis(tmp_path, monkeypatch):
    from app.services import hermes_history_service as history

    monkeypatch.setattr(history, "_project_root", tmp_path)
    _write_jsonl(tmp_path / "hermes" / "dev_events" / "dev_events.jsonl", [
        {
            "eventId": "evt_a",
            "eventType": "implementation_completed",
            "source": "codex",
            "title": "MarketScan cache hardening",
            "linkedFeatureIds": ["feature.market_scan"],
            "changedFiles": ["06_AppPlatform/backend/app/services/market_scan_service.py"],
            "createdAt": "2026-06-12T08:00:00Z",
        },
        {
            "eventId": "evt_b",
            "eventType": "test_run",
            "source": "codex",
            "title": "MarketScan tests",
            "linkedFeatureIds": ["feature.market_scan"],
            "changedFiles": ["06_AppPlatform/backend/tests/unit/test_market_scan_service.py"],
            "tests": {"backend": "1 passed"},
            "createdAt": "2026-06-12T09:00:00Z",
        },
    ])

    result = history.list_history_clusters(level="feature", y_axis="phase")

    assert result["summary"]["clusterCount"] == 1
    cluster = result["clusters"][0]
    assert cluster["eventCount"] == 2
    assert cluster["workstream"] == "MarketScan"
    assert cluster["testCount"] == 1
    assert cluster["lane"] in {"Implemented", "Tested"}


def test_history_clusters_merge_semantic_timeline_work(tmp_path, monkeypatch):
    from app.services import hermes_history_service as history

    monkeypatch.setattr(history, "_project_root", tmp_path)
    _write_jsonl(tmp_path / "hermes" / "dev_events" / "dev_events.jsonl", [
        {
            "eventId": "evt_timeline",
            "eventType": "implementation_completed",
            "source": "codex",
            "title": "Hermes timeline cluster visualization",
            "summary": "Render clusters on a horizontal timeline.",
            "linkedFeatureIds": ["feature.hermes_timeline_visualization"],
            "changedFiles": ["06_AppPlatform/frontend/src/components/HermesHistoryMap.tsx"],
            "createdAt": "2026-06-10T08:00:00Z",
        },
        {
            "eventId": "evt_semantic",
            "eventType": "implementation_completed",
            "source": "codex",
            "title": "Semantic cluster timeline axis",
            "summary": "Merge related timeline events by title and files.",
            "linkedFeatureIds": ["feature.hermes_semantic_cluster"],
            "changedFiles": ["06_AppPlatform/frontend/src/components/HermesHistoryMap.tsx"],
            "createdAt": "2026-06-12T09:00:00Z",
        },
    ])

    result = history.list_history_clusters(level="feature", y_axis="workstream")

    assert result["summary"]["semanticMode"] == "feature_file_title_similarity"
    assert result["summary"]["clusterCount"] == 1
    cluster = result["clusters"][0]
    assert cluster["eventCount"] == 2
    assert cluster["startAt"] == "2026-06-10T08:00:00Z"
    assert cluster["endAt"] == "2026-06-12T09:00:00Z"
    assert cluster["semanticScore"] > 0
    assert "timeline" in cluster["semanticSignals"]
    assert set(cluster["children"]) == {"evt_timeline", "evt_semantic"}


def test_progress_swimlanes_mark_ready_for_pr_with_tests(tmp_path, monkeypatch):
    from app.services import hermes_history_service as history

    monkeypatch.setattr(history, "_project_root", tmp_path)
    _write_yaml(tmp_path / "hermes" / "registry" / "features.yaml", {
        "features": [
            {
                "featureId": "proposal.hermes_history_progress_cockpit",
                "title": "Hermes History Progress Cockpit",
                "status": "implemented",
                "tests": {"backend": "2 passed"},
                "docs": ["Markdown_Readme/features/feature.hermes_history_progress_cockpit.md"],
            }
        ]
    })
    _write_jsonl(tmp_path / "hermes" / "dev_events" / "dev_events.jsonl", [
        {
            "eventId": "evt_1",
            "eventType": "test_run",
            "source": "codex",
            "title": "History tests",
            "linkedFeatureIds": ["proposal.hermes_history_progress_cockpit"],
            "changedFiles": ["06_AppPlatform/backend/tests/unit/test_hermes_history_service.py"],
            "tests": {"backend": "2 passed"},
            "createdAt": "2026-06-12T09:00:00Z",
        }
    ])

    result = history.get_progress_swimlanes()

    assert result["summary"]["total"] == 1
    feature = result["lanes"][0]["features"][0]
    assert feature["phase"] == "Tested"
    assert feature["status"] == "ready_for_pr"
    assert feature["docsCount"] == 1


def test_workflow_cockpit_groups_sessions_and_usage_events(tmp_path, monkeypatch):
    from app.services import hermes_history_service as history

    monkeypatch.setattr(history, "_project_root", tmp_path)
    _write_jsonl(tmp_path / "hermes" / "dev_events" / "dev_events.jsonl", [
        {
            "eventId": "evt_codex",
            "eventType": "implementation_completed",
            "source": "codex",
            "sessionId": "hermes-session",
            "title": "Hermes workflow cockpit",
            "linkedFeatureIds": ["proposal.hermes_history_progress_cockpit"],
            "changedFiles": ["06_AppPlatform/frontend/src/components/HermesWorkflowView.tsx"],
            "tests": {"frontend": "typecheck"},
            "createdAt": "2026-06-12T10:00:00Z",
        }
    ])
    _write_jsonl(tmp_path / "hermes" / "agent_usage.jsonl", [
        {
            "usageId": "agent_usage_astrbot_1",
            "recordedAt": "2026-06-12T10:05:00Z",
            "model": "deepseek-chat",
            "pricingModel": "deepseek-v4-flash",
            "inputTokens": 100,
            "outputTokens": 20,
            "estimatedCostCny": 0.01,
            "selectedTool": "country_copilot",
        }
    ])

    events = history.list_history_events(source="usage", limit=10)
    cockpit = history.get_workflow_cockpit()

    assert events["summary"]["totalEvents"] == 1
    assert events["events"][0]["source"] == "usage"
    assert cockpit["summary"]["sessionCount"] == 2
    assert any(session["sessionId"] == "hermes-session" for session in cockpit["sessions"])
    assert any(session["sessionId"] == "astrbot" for session in cockpit["sessions"])
    assert any(model["model"] == "deepseek-v4-flash" for model in cockpit["models"])
