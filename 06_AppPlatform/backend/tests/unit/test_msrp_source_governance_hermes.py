from datetime import datetime, timezone
from uuid import uuid4

import pytest
from fastapi import HTTPException

from app.api.msrp_source_governance_schemas import AgentRunResultV1
from app.db.msrp_source_governance_models import (
    MsrpGovernanceAuditEvent,
    MsrpGovernanceRepairCase,
)
from app.services.msrp_source_governance import service as governance_service


class _FakeSession:
    def __init__(self) -> None:
        self.added: list[object] = []
        self.commits = 0

    def add(self, item) -> None:
        self.added.append(item)

    def flush(self) -> None:
        return None

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        return None

    def refresh(self, _item) -> None:
        return None


def _case(run_id: str) -> MsrpGovernanceRepairCase:
    now = datetime(2026, 7, 14, 8, 0, tzinfo=timezone.utc)
    return MsrpGovernanceRepairCase(
        case_id=uuid4(),
        open_dedupe_key="dedupe-1",
        repair_domain="source",
        target_id=uuid4(),
        case_type="source_run:failed",
        failure_classifier="anti_bot",
        severity="high",
        priority=80,
        first_seen_at_utc=now,
        last_seen_at_utc=now,
        occurrence_count=1,
        recent_run_ids_json=["source-run-1"],
        evidence_refs_json=[],
        manual_evidence_required=False,
        agent_run_refs_json=[run_id],
        proposal_refs_json=[],
        case_status="diagnosing",
        created_by="governance-worker",
        row_version=1,
        created_at_utc=now,
        updated_at_utc=now,
    )


def test_hermes_result_callback_escalates_evidence_without_dpv4_actor(
    monkeypatch,
) -> None:
    run_id = "hermes-run-1"
    case = _case(run_id)
    session = _FakeSession()
    monkeypatch.setattr(
        governance_service.repo,
        "get_audit_event_by_idempotency",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        governance_service.repo,
        "get_repair_case",
        lambda *_args, **_kwargs: case,
    )

    result = governance_service.MsrpSourceGovernanceService(
        session
    ).record_hermes_run_result(
        case.case_id,
        AgentRunResultV1(
            run_id=run_id,
            status="failed",
            plan_version="composer-v3",
            stop_reason="anti_bot_requires_manual_evidence",
            human_escalation={"requiredAction": "upload_official_pdf"},
            completed_at=datetime(2026, 7, 14, 8, 5, tzinfo=timezone.utc),
        ),
        actor="hermes-service",
        actor_role="editor",
        idempotency_key="hermes-result-1",
    )

    assert result["case"]["caseStatus"] == "awaiting_evidence"
    assert result["case"]["manualEvidenceRequired"] is True
    audit = next(
        item for item in session.added if isinstance(item, MsrpGovernanceAuditEvent)
    )
    assert audit.metadata_json["dpv4Actor"] is False
    assert audit.metadata_json["agentRuntimePersistence"] == "hermes_owned"
    assert session.commits == 1


def test_hermes_result_callback_rejects_unregistered_run(monkeypatch) -> None:
    case = _case("hermes-run-registered")
    session = _FakeSession()
    monkeypatch.setattr(
        governance_service.repo,
        "get_audit_event_by_idempotency",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        governance_service.repo,
        "get_repair_case",
        lambda *_args, **_kwargs: case,
    )

    with pytest.raises(HTTPException, match="not registered") as exc_info:
        governance_service.MsrpSourceGovernanceService(
            session
        ).record_hermes_run_result(
            case.case_id,
            AgentRunResultV1(
                run_id="hermes-run-forged",
                status="failed",
                plan_version="composer-v3",
                completed_at=datetime(2026, 7, 14, 8, 5, tzinfo=timezone.utc),
            ),
            actor="hermes-service",
            actor_role="editor",
            idempotency_key="hermes-result-forged",
        )

    assert exc_info.value.status_code == 409
    assert session.added == []
