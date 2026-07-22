from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi import HTTPException
from pydantic import ValidationError

from app.api.msrp_source_governance_schemas import (
    AgentRunRequestV1,
    EvidenceReference,
    GateResult,
    RepairProposalCreate,
    SourceRunResultV1,
    UrlEvidenceCreate,
)
from app.services.msrp_source_governance.service import build_target_key
from app.services.msrp_source_governance import service as governance_service
from app.db.msrp_source_governance_models import MsrpMonitoringTarget
from app.api.routes.msrp_source_governance import router as governance_router


def test_dpv4_cannot_be_a_proposal_origin_or_bypass_hermes() -> None:
    with pytest.raises(ValidationError, match="DPV4 metadata"):
        RepairProposalCreate(
            proposal_origin="manual",
            proposal_type="selector_change",
            dpv4_metadata={"provider": "deepseek", "model": "dpv4"},
            proposed_change={"selector": ".price"},
        )


def test_hermes_proposal_requires_agent_run_identity() -> None:
    with pytest.raises(ValidationError, match="agent_run_id"):
        RepairProposalCreate(
            proposal_origin="hermes_agent",
            proposal_type="parser_change",
            proposed_change={"jsonPath": "$.offers[*].price"},
        )


def test_hermes_proposal_must_reference_case_registered_agent_run(
    monkeypatch,
) -> None:
    case_id = uuid4()
    case = SimpleNamespace(
        case_id=case_id,
        case_status="diagnosing",
        agent_run_refs_json=["run-registered"],
        target_id=uuid4(),
        source_id=None,
    )
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
    service = governance_service.MsrpSourceGovernanceService(object())

    with pytest.raises(HTTPException, match="not registered") as exc_info:
        service.create_proposal(
            case_id,
            RepairProposalCreate(
                proposal_origin="hermes_agent",
                proposal_type="selector_change",
                agent_run_id="run-forged",
                proposed_change={"selector": ".price"},
            ),
            actor="hermes-service",
            actor_role="editor",
            idempotency_key="proposal-forged-agent-run",
        )

    assert exc_info.value.status_code == 409


def test_agent_run_request_serializes_versioned_camel_case_contract() -> None:
    evidence = EvidenceReference(
        evidence_asset_id=uuid4(),
        sha256="a" * 64,
        evidence_type="official_url",
    )
    request = AgentRunRequestV1(
        run_id="run-1",
        case_id=uuid4(),
        target_id=uuid4(),
        repair_domain="parser",
        evidence_refs=[evidence],
        source_gate_snapshot=GateResult(
            status="fail",
            reasons=["targeted_dryrun_not_passed"],
            policy_version="source-v1",
        ),
        mapping_gate_snapshot=GateResult(
            status="pass",
            reasons=[],
            policy_version="mapping-v1",
        ),
        allowed_tool_ids=["source.targeted_dryrun"],
        authority_policy_version="authority-v1",
        composer_policy_version="composer-v1",
        cost_budget_usd=Decimal("2.50"),
        requested_by="operator@example.test",
    )

    payload = request.model_dump(mode="json", by_alias=True)

    assert payload["schemaVersion"] == "1.0"
    assert payload["repairDomain"] == "parser"
    assert payload["evidenceRefs"][0]["evidenceAssetId"] == str(
        evidence.evidence_asset_id
    )
    assert payload["costBudgetUsd"] == "2.50"
    assert "run_id" not in payload


def test_url_evidence_rejects_inverted_validity_window() -> None:
    with pytest.raises(ValidationError, match="valid_until"):
        UrlEvidenceCreate(
            source_url="https://www.volvocars.com/se/cars/xc60/",
            official_domain="volvocars.com",
            source_type="official_web",
            semantic_lane="msrp",
            valid_from="2026-08-01",
            valid_until="2026-07-01",
        )


def test_target_key_is_scope_stable_across_case_and_whitespace() -> None:
    assert build_target_key(" se ", " Volvo ", "XC60", " Ultra ", " PHEV ") == (
        "SE::volvo::xc60::ultra::phev"
    )


def test_contract_accepts_alias_input_from_an_adjacent_session() -> None:
    item = EvidenceReference.model_validate(
        {
            "evidenceAssetId": str(uuid4()),
            "sha256": "b" * 64,
            "evidenceType": "uploaded_pdf",
        }
    )

    assert item.evidence_type == "uploaded_pdf"
    assert datetime.now(timezone.utc).tzinfo is not None


def test_source_run_selector_failure_routes_to_parser_case(monkeypatch) -> None:
    target_id = uuid4()
    now = datetime(2026, 7, 14, 4, 0, tzinfo=timezone.utc)
    target = MsrpMonitoringTarget(
        target_id=target_id,
        target_key="SE::volvo::xc60::::",
        country="SE",
        brand="Volvo",
        model="XC60",
        roster_type="manual",
        monitoring_status="degraded",
        row_version=1,
        created_at_utc=now,
        updated_at_utc=now,
    )
    monkeypatch.setattr(
        governance_service.repo,
        "get_target_by_key",
        lambda *_args, **_kwargs: target,
    )
    monkeypatch.setattr(
        governance_service.repo,
        "get_target",
        lambda *_args, **_kwargs: target,
    )
    monkeypatch.setattr(
        governance_service.MsrpSourceGovernanceService,
        "_validated_evidence_references",
        lambda *_args, **_kwargs: [],
    )
    service = governance_service.MsrpSourceGovernanceService(object())
    captured = {}

    def record_case(value, **_kwargs):
        captured["finding"] = value
        return {"caseId": "case-1"}

    service.open_or_update_case = record_case
    result = SourceRunResultV1(
        run_id="run-selector-1",
        target_key=target.target_key,
        source_code="volvo_se",
        runtime_source_id=uuid4(),
        status="failed",
        failure_class="selector_not_found",
        retryability="non_retryable",
        extractor_name="scrapling_static",
        extractor_version="v1",
        source_url="https://www.volvocars.com/se/cars/xc60/",
        extracted_count=0,
        valid_count=0,
        rejected_count=0,
        started_at=now,
        completed_at=now,
    )

    response = service.ingest_source_run_result(
        result,
        actor="source-repair-service",
        actor_role="editor",
        idempotency_key="source-run-selector-1",
    )

    assert response["case"] == {"caseId": "case-1"}
    assert captured["finding"].repair_domain == "parser"
    assert captured["finding"].manual_evidence_required is False


def test_router_composes_with_shared_v1_prefix_once() -> None:
    app = FastAPI()
    app.include_router(governance_router, prefix="/v1")
    paths = {route.path for route in app.routes}
    gate_route = next(
        route
        for route in app.routes
        if route.path == "/v1/msrp/source-governance/gate-decisions/evaluate"
    )

    assert "/v1/msrp/source-governance/targets" in paths
    assert "/v1/msrp/source-governance/gate-decisions/evaluate" in paths
    assert (
        "/v1/msrp/source-governance/targets/{target_id}/gate-decisions/latest"
        in paths
    )
    assert not any(path.startswith("/v1/v1/") for path in paths)
    assert not any("dpv4" in path.casefold() for path in paths)
    assert gate_route.status_code == 201


def test_router_exposes_the_governance_owned_endpoint_surface() -> None:
    app = FastAPI()
    app.include_router(governance_router, prefix="/v1")
    operations = {
        (method, route.path)
        for route in app.routes
        for method in (route.methods or set())
    }
    prefix = "/v1/msrp/source-governance"
    expected = {
        ("GET", f"{prefix}/targets"),
        ("POST", f"{prefix}/targets"),
        ("GET", f"{prefix}/targets/{{target_id}}"),
        ("POST", f"{prefix}/targets/{{target_id}}/url-evidence"),
        ("POST", f"{prefix}/evidence-uploads/initiate"),
        ("PUT", f"{prefix}/evidence-uploads/{{upload_session_id}}/parts/{{part_number}}"),
        ("POST", f"{prefix}/evidence-uploads/{{upload_session_id}}/complete"),
        ("GET", f"{prefix}/cases"),
        ("POST", f"{prefix}/cases/findings"),
        ("POST", f"{prefix}/findings/source-runs"),
        ("POST", f"{prefix}/findings/monitor-anomalies"),
        ("GET", f"{prefix}/cases/{{case_id}}"),
        ("GET", f"{prefix}/conflicts"),
        ("POST", f"{prefix}/cases/{{case_id}}/request-hermes-diagnosis"),
        ("POST", f"{prefix}/cases/{{case_id}}/agent-run-results"),
        ("POST", f"{prefix}/cases/{{case_id}}/proposals"),
        ("POST", f"{prefix}/cases/{{case_id}}/resolve"),
        ("GET", f"{prefix}/proposals/{{proposal_id}}"),
        ("POST", f"{prefix}/proposals/{{proposal_id}}/dryrun"),
        ("POST", f"{prefix}/proposals/{{proposal_id}}/submit"),
        ("POST", f"{prefix}/source-versions"),
        ("GET", f"{prefix}/source-versions/{{source_version_id}}"),
        ("POST", f"{prefix}/source-versions/{{source_version_id}}/publish"),
        ("POST", f"{prefix}/source-versions/{{source_version_id}}/rollback"),
        ("POST", f"{prefix}/gate-decisions/evaluate"),
        ("GET", f"{prefix}/targets/{{target_id}}/gate-decisions/latest"),
        ("POST", f"{prefix}/result-corrections"),
        ("GET", f"{prefix}/result-corrections/{{correction_id}}"),
        ("POST", f"{prefix}/result-corrections/{{correction_id}}/approve"),
        ("GET", f"{prefix}/fx-runs"),
        ("POST", f"{prefix}/fx-runs"),
        ("POST", f"{prefix}/fx-runs/{{fx_run_id}}/approve"),
        ("GET", f"{prefix}/audit-events"),
    }

    assert expected.issubset(operations)
