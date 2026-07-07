from __future__ import annotations

import json

import pytest

from app.api.routes import msrp_workflow
from app.api.schemas import MsrpMonitorSourceIssueRequest
from app.services import msrp_workflow_service


def _payload() -> dict[str, object]:
    return {
        "target_type": "movement",
        "target_id": "event-1|ch",
        "country": "ch",
        "country_label": "Switzerland",
        "brand": "VOLVO",
        "jato_model": "XC60",
        "jato_trim": "Plug-in Hybrid",
        "jato_powertrain": "PHEV",
        "price_semantics": "official_offer",
        "source_url": "https://www.volvocars.com/ch/offers",
        "source_label": "Volvo Switzerland official offers",
        "evidence_label": "Official offer boundary",
        "note": "Analyst flagged source issue from MSRP monitor.",
    }


def test_queue_msrp_monitor_source_issue_writes_and_reuses_backlog(tmp_path, monkeypatch):
    backlog_path = tmp_path / "03_Scripts" / "diagnostics" / "artifacts" / "msrp_monitor_source_issue_backlog.json"
    monkeypatch.setattr(msrp_workflow_service, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(msrp_workflow_service, "MSRP_MONITOR_SOURCE_ISSUE_BACKLOG_PATH", backlog_path)

    first = msrp_workflow_service.queue_msrp_monitor_source_issue(_payload())
    second = msrp_workflow_service.queue_msrp_monitor_source_issue(_payload())

    assert first["issueId"] == second["issueId"]
    assert first["reused"] is False
    assert second["reused"] is True
    assert second["backlogPath"] == "03_Scripts/diagnostics/artifacts/msrp_monitor_source_issue_backlog.json"

    backlog = json.loads(backlog_path.read_text(encoding="utf-8"))
    assert backlog["schemaVersion"] == "msrp_monitor_source_issue_backlog_v1"
    assert backlog["openIssueCount"] == 1
    assert backlog["totalIssueCount"] == 1
    assert backlog["items"][0]["country"] == "CH"
    assert backlog["items"][0]["flaggedCount"] == 2


def test_queue_msrp_monitor_source_issue_rejects_invalid_source_url(tmp_path, monkeypatch):
    backlog_path = tmp_path / "03_Scripts" / "diagnostics" / "artifacts" / "msrp_monitor_source_issue_backlog.json"
    monkeypatch.setattr(msrp_workflow_service, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(msrp_workflow_service, "MSRP_MONITOR_SOURCE_ISSUE_BACKLOG_PATH", backlog_path)
    payload = _payload()
    payload["source_url"] = "not-a-url"

    with pytest.raises(Exception) as exc_info:
        msrp_workflow_service.queue_msrp_monitor_source_issue(payload)

    assert "Invalid source issue source_url" in str(exc_info.value)
    assert not backlog_path.exists()


def test_post_msrp_monitor_source_issue_route_writes_backlog(tmp_path, monkeypatch):
    backlog_path = tmp_path / "03_Scripts" / "diagnostics" / "artifacts" / "msrp_monitor_source_issue_backlog.json"
    monkeypatch.setattr(msrp_workflow_service, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(msrp_workflow_service, "MSRP_MONITOR_SOURCE_ISSUE_BACKLOG_PATH", backlog_path)

    body = msrp_workflow.post_msrp_monitor_source_issue(
        MsrpMonitorSourceIssueRequest(**_payload())
    )

    assert body["item"]["status"] == "open"
    assert body["item"]["backlogPath"] == "03_Scripts/diagnostics/artifacts/msrp_monitor_source_issue_backlog.json"
    assert backlog_path.exists()
