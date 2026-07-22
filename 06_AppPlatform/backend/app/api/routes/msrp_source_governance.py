from uuid import UUID

from fastapi import APIRouter, Body, Depends, Header, Path, Query
from sqlalchemy.orm import Session

from app.api.msrp_source_governance_schemas import (
    AgentRunResultV1,
    ApprovalRequest,
    EvidenceUploadComplete,
    EvidenceUploadInitiate,
    FxNormalizationCreate,
    GateEvaluationRequest,
    HermesDiagnosisRequest,
    MonitoringTargetCreate,
    MonitorAnomalyV1,
    ProposalVerificationUpdate,
    PublishSourceVersionRequest,
    RepairCaseFindingCreate,
    RepairProposalCreate,
    ResolveRepairCaseRequest,
    ResultCorrectionCreate,
    SourceVersionCreate,
    SourceRunResultV1,
    SubmitProposalRequest,
    UrlEvidenceCreate,
)
from app.core.security import UserContext, require_min_role
from app.db.session import get_db_session
from app.services.msrp_source_governance import MsrpSourceGovernanceService


router = APIRouter(
    prefix="/msrp/source-governance",
    tags=["msrp-source-governance"],
)


def _idempotency_key(
    value: str = Header(
        alias="X-Idempotency-Key",
        min_length=1,
        max_length=200,
    ),
) -> str:
    return value


@router.get("/targets")
def get_targets(
    country: str | None = Query(default=None),
    brand: str | None = Query(default=None),
    monitoring_status: str | None = Query(default=None),
    roster_type: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_db_session),
    _user: UserContext = Depends(require_min_role("viewer")),
) -> dict[str, object]:
    return MsrpSourceGovernanceService(session).list_targets(
        country=country,
        brand=brand,
        monitoring_status=monitoring_status,
        roster_type=roster_type,
        limit=limit,
        offset=offset,
    )


@router.get("/targets/{target_id}")
def get_target_detail(
    target_id: UUID,
    session: Session = Depends(get_db_session),
    _user: UserContext = Depends(require_min_role("viewer")),
) -> dict[str, object]:
    return MsrpSourceGovernanceService(session).get_target_detail(target_id)


@router.post("/targets", status_code=201)
def post_target(
    payload: MonitoringTargetCreate,
    idempotency_key: str = Depends(_idempotency_key),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    item = MsrpSourceGovernanceService(session).create_target(
        payload,
        actor=user.name,
        actor_role=user.role,
        idempotency_key=idempotency_key,
    )
    return {"item": item}


@router.post("/targets/{target_id}/url-evidence", status_code=201)
def post_target_url_evidence(
    target_id: UUID,
    payload: UrlEvidenceCreate,
    idempotency_key: str = Depends(_idempotency_key),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    item = MsrpSourceGovernanceService(session).add_url_evidence(
        target_id,
        payload,
        actor=user.name,
        actor_role=user.role,
        idempotency_key=idempotency_key,
    )
    return {"item": item}


@router.post("/evidence-uploads/initiate", status_code=201)
def post_evidence_upload_initiate(
    payload: EvidenceUploadInitiate,
    idempotency_key: str = Depends(_idempotency_key),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    item = MsrpSourceGovernanceService(session).initiate_evidence_upload(
        payload,
        actor=user.name,
        actor_role=user.role,
        idempotency_key=idempotency_key,
    )
    return {"item": item}


@router.put("/evidence-uploads/{upload_session_id}/parts/{part_number}")
def put_evidence_upload_part(
    upload_session_id: UUID,
    part_number: int = Path(ge=1),
    content: bytes = Body(media_type="application/octet-stream"),
    part_sha256: str = Header(
        alias="X-Part-Sha256",
        pattern=r"^[0-9a-fA-F]{64}$",
    ),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    item = MsrpSourceGovernanceService(session).upload_evidence_part(
        upload_session_id,
        part_number,
        content,
        part_sha256,
        actor=user.name,
        actor_role=user.role,
    )
    return {"item": item}


@router.post("/evidence-uploads/{upload_session_id}/complete")
def post_evidence_upload_complete(
    upload_session_id: UUID,
    payload: EvidenceUploadComplete,
    idempotency_key: str = Depends(_idempotency_key),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    item = MsrpSourceGovernanceService(session).complete_evidence_upload(
        upload_session_id,
        payload,
        actor=user.name,
        actor_role=user.role,
        idempotency_key=idempotency_key,
    )
    return {"item": item}


@router.get("/cases")
def get_cases(
    repair_domain: str | None = Query(default=None),
    case_status: str | None = Query(default=None),
    severity: str | None = Query(default=None),
    target_id: UUID | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_db_session),
    _user: UserContext = Depends(require_min_role("viewer")),
) -> dict[str, object]:
    return MsrpSourceGovernanceService(session).list_cases(
        repair_domain=repair_domain,
        case_status=case_status,
        severity=severity,
        target_id=target_id,
        limit=limit,
        offset=offset,
    )


@router.post("/cases/findings", status_code=201)
def post_case_finding(
    payload: RepairCaseFindingCreate,
    idempotency_key: str = Depends(_idempotency_key),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    item = MsrpSourceGovernanceService(session).open_or_update_case(
        payload,
        actor=user.name,
        actor_role=user.role,
        idempotency_key=idempotency_key,
    )
    return {"item": item}


@router.post("/findings/source-runs", status_code=202)
def post_source_run_result(
    payload: SourceRunResultV1,
    idempotency_key: str = Depends(_idempotency_key),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    return MsrpSourceGovernanceService(session).ingest_source_run_result(
        payload,
        actor=user.name,
        actor_role=user.role,
        idempotency_key=idempotency_key,
    )


@router.post("/findings/monitor-anomalies", status_code=202)
def post_monitor_anomaly(
    payload: MonitorAnomalyV1,
    idempotency_key: str = Depends(_idempotency_key),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    return MsrpSourceGovernanceService(session).ingest_monitor_anomaly(
        payload,
        actor=user.name,
        actor_role=user.role,
        idempotency_key=idempotency_key,
    )


@router.get("/cases/{case_id}")
def get_case_detail(
    case_id: UUID,
    session: Session = Depends(get_db_session),
    _user: UserContext = Depends(require_min_role("viewer")),
) -> dict[str, object]:
    return MsrpSourceGovernanceService(session).get_case_detail(case_id)


@router.get("/conflicts")
def get_conflicts(
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_db_session),
    _user: UserContext = Depends(require_min_role("viewer")),
) -> dict[str, object]:
    return MsrpSourceGovernanceService(session).list_conflicts(
        limit=limit,
        offset=offset,
    )


@router.post("/cases/{case_id}/request-hermes-diagnosis", status_code=202)
def post_hermes_diagnosis(
    case_id: UUID,
    payload: HermesDiagnosisRequest,
    idempotency_key: str = Depends(_idempotency_key),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    request = MsrpSourceGovernanceService(session).request_hermes_diagnosis(
        case_id,
        payload,
        actor=user.name,
        actor_role=user.role,
        idempotency_key=idempotency_key,
    )
    return {
        "item": request,
        "dispatchStatus": "pending_integration",
    }


@router.post("/cases/{case_id}/agent-run-results", status_code=202)
def post_hermes_run_result(
    case_id: UUID,
    payload: AgentRunResultV1,
    idempotency_key: str = Depends(_idempotency_key),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    item = MsrpSourceGovernanceService(session).record_hermes_run_result(
        case_id,
        payload,
        actor=user.name,
        actor_role=user.role,
        idempotency_key=idempotency_key,
    )
    return {"item": item}


@router.post("/cases/{case_id}/proposals", status_code=201)
def post_case_proposal(
    case_id: UUID,
    payload: RepairProposalCreate,
    idempotency_key: str = Depends(_idempotency_key),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    item = MsrpSourceGovernanceService(session).create_proposal(
        case_id,
        payload,
        actor=user.name,
        actor_role=user.role,
        idempotency_key=idempotency_key,
    )
    return {"item": item}


@router.get("/proposals/{proposal_id}")
def get_proposal(
    proposal_id: UUID,
    session: Session = Depends(get_db_session),
    _user: UserContext = Depends(require_min_role("viewer")),
) -> dict[str, object]:
    return {
        "item": MsrpSourceGovernanceService(session).get_proposal(proposal_id)
    }


@router.post("/proposals/{proposal_id}/dryrun")
def post_proposal_verification(
    proposal_id: UUID,
    payload: ProposalVerificationUpdate,
    idempotency_key: str = Depends(_idempotency_key),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    item = MsrpSourceGovernanceService(session).record_proposal_verification(
        proposal_id,
        payload,
        actor=user.name,
        actor_role=user.role,
        idempotency_key=idempotency_key,
    )
    return {"item": item}


@router.post("/proposals/{proposal_id}/submit")
def post_proposal_submit(
    proposal_id: UUID,
    payload: SubmitProposalRequest,
    idempotency_key: str = Depends(_idempotency_key),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    item = MsrpSourceGovernanceService(session).submit_proposal(
        proposal_id,
        payload,
        actor=user.name,
        actor_role=user.role,
        idempotency_key=idempotency_key,
    )
    return {"item": item}


@router.post("/source-versions", status_code=201)
def post_source_version(
    payload: SourceVersionCreate,
    idempotency_key: str = Depends(_idempotency_key),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    item = MsrpSourceGovernanceService(session).create_source_version(
        payload,
        actor=user.name,
        actor_role=user.role,
        idempotency_key=idempotency_key,
    )
    return {"item": item}


@router.get("/source-versions/{source_version_id}")
def get_source_version(
    source_version_id: UUID,
    session: Session = Depends(get_db_session),
    _user: UserContext = Depends(require_min_role("viewer")),
) -> dict[str, object]:
    return {
        "item": MsrpSourceGovernanceService(session).get_source_version(
            source_version_id
        )
    }


@router.post("/source-versions/{source_version_id}/publish")
def post_source_version_publish(
    source_version_id: UUID,
    payload: PublishSourceVersionRequest,
    idempotency_key: str = Depends(_idempotency_key),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("admin")),
) -> dict[str, object]:
    item = MsrpSourceGovernanceService(session).publish_source_version(
        source_version_id,
        payload,
        actor=user.name,
        actor_role=user.role,
        idempotency_key=idempotency_key,
    )
    return {"item": item}


@router.post("/source-versions/{source_version_id}/rollback")
def post_source_version_rollback(
    source_version_id: UUID,
    payload: PublishSourceVersionRequest,
    idempotency_key: str = Depends(_idempotency_key),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("admin")),
) -> dict[str, object]:
    item = MsrpSourceGovernanceService(session).rollback_source_version(
        source_version_id,
        payload,
        actor=user.name,
        actor_role=user.role,
        idempotency_key=idempotency_key,
    )
    return {"item": item}


@router.post("/cases/{case_id}/resolve")
def post_case_resolve(
    case_id: UUID,
    payload: ResolveRepairCaseRequest,
    idempotency_key: str = Depends(_idempotency_key),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("admin")),
) -> dict[str, object]:
    item = MsrpSourceGovernanceService(session).resolve_case(
        case_id,
        payload,
        actor=user.name,
        actor_role=user.role,
        idempotency_key=idempotency_key,
    )
    return {"item": item}


@router.post("/gate-decisions/evaluate", status_code=201)
def post_gate_evaluation(
    payload: GateEvaluationRequest,
    idempotency_key: str = Depends(_idempotency_key),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    item = MsrpSourceGovernanceService(session).evaluate_and_record_gate_decision(
        payload,
        actor=user.name,
        actor_role=user.role,
        idempotency_key=idempotency_key,
    )
    return {"item": item}


@router.get("/targets/{target_id}/gate-decisions/latest")
def get_latest_gate_decision(
    target_id: UUID,
    session: Session = Depends(get_db_session),
    _user: UserContext = Depends(require_min_role("viewer")),
) -> dict[str, object]:
    item = MsrpSourceGovernanceService(session).get_latest_gate_decision(target_id)
    return {"item": item}


@router.get("/result-corrections/{correction_id}")
def get_result_correction(
    correction_id: UUID,
    session: Session = Depends(get_db_session),
    _user: UserContext = Depends(require_min_role("viewer")),
) -> dict[str, object]:
    return {
        "item": MsrpSourceGovernanceService(session).get_result_correction(
            correction_id
        )
    }


@router.post("/result-corrections", status_code=201)
def post_result_correction(
    payload: ResultCorrectionCreate,
    idempotency_key: str = Depends(_idempotency_key),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    item = MsrpSourceGovernanceService(session).create_result_correction(
        payload,
        actor=user.name,
        actor_role=user.role,
        idempotency_key=idempotency_key,
    )
    return {"item": item}


@router.post("/result-corrections/{correction_id}/approve")
def post_result_correction_approve(
    correction_id: UUID,
    payload: ApprovalRequest,
    idempotency_key: str = Depends(_idempotency_key),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("admin")),
) -> dict[str, object]:
    item = MsrpSourceGovernanceService(session).approve_result_correction(
        correction_id,
        payload,
        actor=user.name,
        actor_role=user.role,
        idempotency_key=idempotency_key,
    )
    return {"item": item}


@router.get("/fx-runs")
def get_fx_runs(
    observation_id: UUID | None = Query(default=None),
    run_status: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    session: Session = Depends(get_db_session),
    _user: UserContext = Depends(require_min_role("viewer")),
) -> dict[str, object]:
    return MsrpSourceGovernanceService(session).list_fx_runs(
        observation_id=observation_id,
        run_status=run_status,
        limit=limit,
    )


@router.post("/fx-runs", status_code=201)
def post_fx_run(
    payload: FxNormalizationCreate,
    idempotency_key: str = Depends(_idempotency_key),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    item = MsrpSourceGovernanceService(session).create_fx_run(
        payload,
        actor=user.name,
        actor_role=user.role,
        idempotency_key=idempotency_key,
    )
    return {"item": item}


@router.post("/fx-runs/{fx_run_id}/approve")
def post_fx_run_approve(
    fx_run_id: UUID,
    payload: ApprovalRequest,
    idempotency_key: str = Depends(_idempotency_key),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("admin")),
) -> dict[str, object]:
    item = MsrpSourceGovernanceService(session).approve_fx_run(
        fx_run_id,
        payload,
        actor=user.name,
        actor_role=user.role,
        idempotency_key=idempotency_key,
    )
    return {"item": item}


@router.get("/audit-events")
def get_audit_events(
    entity_type: str | None = Query(default=None),
    entity_id: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    session: Session = Depends(get_db_session),
    _user: UserContext = Depends(require_min_role("viewer")),
) -> dict[str, object]:
    return MsrpSourceGovernanceService(session).list_audit_events(
        entity_type=entity_type,
        entity_id=entity_id,
        limit=limit,
    )
