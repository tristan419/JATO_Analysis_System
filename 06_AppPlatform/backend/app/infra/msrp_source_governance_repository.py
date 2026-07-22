from __future__ import annotations

from uuid import UUID

from sqlalchemy import Select, func, or_, select
from sqlalchemy.orm import Session, joinedload

from app.db.msrp_source_governance_models import (
    MsrpEvidenceUploadSession,
    MsrpFxNormalizationRun,
    MsrpGovernanceAuditEvent,
    MsrpGovernanceGateDecision,
    MsrpGovernanceRepairCase,
    MsrpMonitoringTarget,
    MsrpObservationEvidenceLink,
    MsrpRepairProposal,
    MsrpResultCorrectionDecision,
    MsrpSourceEvidenceAsset,
    MsrpSourceVersion,
)


def add(session: Session, item):
    session.add(item)
    return item


def get_target(
    session: Session,
    target_id: UUID,
    *,
    for_update: bool = False,
) -> MsrpMonitoringTarget | None:
    return session.get(
        MsrpMonitoringTarget,
        target_id,
        with_for_update=for_update,
    )


def get_target_by_key(
    session: Session,
    target_key: str,
) -> MsrpMonitoringTarget | None:
    stmt = select(MsrpMonitoringTarget).where(
        MsrpMonitoringTarget.target_key == target_key
    )
    return session.execute(stmt).scalar_one_or_none()


def list_latest_gate_decisions_by_target(
    session: Session,
    target_ids: list[UUID],
) -> dict[UUID, MsrpGovernanceGateDecision]:
    if not target_ids:
        return {}
    stmt = (
        select(MsrpGovernanceGateDecision)
        .where(MsrpGovernanceGateDecision.target_id.in_(target_ids))
        .order_by(
            MsrpGovernanceGateDecision.target_id.asc(),
            MsrpGovernanceGateDecision.evaluated_at_utc.desc(),
            MsrpGovernanceGateDecision.created_at_utc.desc(),
            MsrpGovernanceGateDecision.gate_decision_id.desc(),
        )
    )
    decisions: dict[UUID, MsrpGovernanceGateDecision] = {}
    for row in session.execute(stmt).scalars():
        decisions.setdefault(row.target_id, row)
    return decisions


def get_latest_gate_decision_for_target(
    session: Session,
    target_id: UUID,
) -> MsrpGovernanceGateDecision | None:
    stmt = (
        select(MsrpGovernanceGateDecision)
        .where(MsrpGovernanceGateDecision.target_id == target_id)
        .order_by(
            MsrpGovernanceGateDecision.evaluated_at_utc.desc(),
            MsrpGovernanceGateDecision.created_at_utc.desc(),
            MsrpGovernanceGateDecision.gate_decision_id.desc(),
        )
        .limit(1)
    )
    return session.execute(stmt).scalar_one_or_none()


def get_gate_decision(
    session: Session,
    gate_decision_id: UUID,
) -> MsrpGovernanceGateDecision | None:
    return session.get(MsrpGovernanceGateDecision, gate_decision_id)


def list_targets(
    session: Session,
    *,
    country: str | None,
    brand: str | None,
    monitoring_status: str | None,
    roster_type: str | None,
    limit: int,
    offset: int,
) -> tuple[int, list[MsrpMonitoringTarget]]:
    filters = []
    if country:
        filters.append(func.upper(MsrpMonitoringTarget.country) == country.upper())
    if brand:
        filters.append(
            func.lower(MsrpMonitoringTarget.brand).contains(brand.strip().lower())
        )
    if monitoring_status:
        filters.append(MsrpMonitoringTarget.monitoring_status == monitoring_status)
    if roster_type:
        filters.append(MsrpMonitoringTarget.roster_type == roster_type)

    count_stmt = select(func.count()).select_from(MsrpMonitoringTarget)
    stmt: Select[tuple[MsrpMonitoringTarget]] = select(MsrpMonitoringTarget)
    if filters:
        count_stmt = count_stmt.where(*filters)
        stmt = stmt.where(*filters)
    total = int(session.execute(count_stmt).scalar_one())
    rows = session.execute(
        stmt.order_by(
            MsrpMonitoringTarget.country.asc(),
            MsrpMonitoringTarget.roster_rank.asc().nullslast(),
            MsrpMonitoringTarget.brand.asc(),
            MsrpMonitoringTarget.model.asc(),
        )
        .offset(max(0, offset))
        .limit(max(1, min(limit, 500)))
    ).scalars().all()
    return total, rows


def get_evidence_asset(
    session: Session,
    evidence_asset_id: UUID,
) -> MsrpSourceEvidenceAsset | None:
    return session.get(MsrpSourceEvidenceAsset, evidence_asset_id)


def add_observation_evidence_links(
    session: Session,
    links: list[MsrpObservationEvidenceLink],
) -> list[MsrpObservationEvidenceLink]:
    session.add_all(links)
    return links


def list_observation_evidence_links(
    session: Session,
    observation_ids: list[UUID],
) -> list[MsrpObservationEvidenceLink]:
    if not observation_ids:
        return []
    stmt = (
        select(MsrpObservationEvidenceLink)
        .options(joinedload(MsrpObservationEvidenceLink.evidence_asset))
        .where(MsrpObservationEvidenceLink.observation_id.in_(observation_ids))
        .order_by(
            MsrpObservationEvidenceLink.observation_id.asc(),
            MsrpObservationEvidenceLink.evidence_role.asc(),
            MsrpObservationEvidenceLink.evidence_asset_id.asc(),
        )
    )
    return session.execute(stmt).scalars().unique().all()


def get_evidence_by_sha256(
    session: Session,
    target_id: UUID,
    sha256: str,
) -> MsrpSourceEvidenceAsset | None:
    stmt = select(MsrpSourceEvidenceAsset).where(
        MsrpSourceEvidenceAsset.target_id == target_id,
        MsrpSourceEvidenceAsset.sha256 == sha256
    )
    return session.execute(stmt).scalar_one_or_none()


def list_evidence_for_target(
    session: Session,
    target_id: UUID,
    limit: int = 100,
) -> list[MsrpSourceEvidenceAsset]:
    stmt = (
        select(MsrpSourceEvidenceAsset)
        .where(MsrpSourceEvidenceAsset.target_id == target_id)
        .order_by(MsrpSourceEvidenceAsset.captured_at_utc.desc())
        .limit(max(1, min(limit, 500)))
    )
    return session.execute(stmt).scalars().all()


def list_all_evidence_assets(session: Session) -> list[MsrpSourceEvidenceAsset]:
    stmt = select(MsrpSourceEvidenceAsset).order_by(
        MsrpSourceEvidenceAsset.storage_key.asc().nullslast(),
        MsrpSourceEvidenceAsset.evidence_asset_id.asc(),
    )
    return session.execute(stmt).scalars().all()


def get_evidence_upload_session(
    session: Session,
    upload_session_id: UUID,
    *,
    for_update: bool = False,
) -> MsrpEvidenceUploadSession | None:
    return session.get(
        MsrpEvidenceUploadSession,
        upload_session_id,
        with_for_update=for_update,
    )


def get_source_version(
    session: Session,
    source_version_id: UUID,
    *,
    for_update: bool = False,
) -> MsrpSourceVersion | None:
    return session.get(
        MsrpSourceVersion,
        source_version_id,
        with_for_update=for_update,
    )


def get_published_source_version(
    session: Session,
    source_id: UUID,
    *,
    for_update: bool = False,
) -> MsrpSourceVersion | None:
    stmt = select(MsrpSourceVersion).where(
        MsrpSourceVersion.source_id == source_id,
        MsrpSourceVersion.version_status == "published",
    )
    if for_update:
        stmt = stmt.with_for_update()
    return session.execute(stmt).scalar_one_or_none()


def next_source_version_number(session: Session, source_id: UUID) -> int:
    stmt = select(func.max(MsrpSourceVersion.version_number)).where(
        MsrpSourceVersion.source_id == source_id
    )
    current = session.execute(stmt).scalar_one_or_none()
    return int(current or 0) + 1


def list_source_versions(
    session: Session,
    *,
    target_id: UUID | None = None,
    source_id: UUID | None = None,
    limit: int = 100,
) -> list[MsrpSourceVersion]:
    stmt: Select[tuple[MsrpSourceVersion]] = select(MsrpSourceVersion)
    if target_id:
        stmt = stmt.where(MsrpSourceVersion.target_id == target_id)
    if source_id:
        stmt = stmt.where(MsrpSourceVersion.source_id == source_id)
    stmt = stmt.order_by(
        MsrpSourceVersion.source_id.asc(),
        MsrpSourceVersion.version_number.desc(),
    ).limit(max(1, min(limit, 500)))
    return session.execute(stmt).scalars().all()


def get_repair_case(
    session: Session,
    case_id: UUID,
    *,
    for_update: bool = False,
) -> MsrpGovernanceRepairCase | None:
    return session.get(
        MsrpGovernanceRepairCase,
        case_id,
        with_for_update=for_update,
    )


def get_open_case_by_dedupe_key(
    session: Session,
    open_dedupe_key: str,
    *,
    for_update: bool = False,
) -> MsrpGovernanceRepairCase | None:
    stmt = select(MsrpGovernanceRepairCase).where(
        MsrpGovernanceRepairCase.open_dedupe_key == open_dedupe_key
    )
    if for_update:
        stmt = stmt.with_for_update()
    return session.execute(stmt).scalar_one_or_none()


def list_repair_cases(
    session: Session,
    *,
    repair_domain: str | None,
    case_status: str | None,
    severity: str | None,
    target_id: UUID | None,
    limit: int,
    offset: int,
) -> tuple[int, list[MsrpGovernanceRepairCase]]:
    filters = []
    if repair_domain:
        filters.append(MsrpGovernanceRepairCase.repair_domain == repair_domain)
    if case_status:
        filters.append(MsrpGovernanceRepairCase.case_status == case_status)
    if severity:
        filters.append(MsrpGovernanceRepairCase.severity == severity)
    if target_id:
        filters.append(MsrpGovernanceRepairCase.target_id == target_id)
    count_stmt = select(func.count()).select_from(MsrpGovernanceRepairCase)
    stmt: Select[tuple[MsrpGovernanceRepairCase]] = select(
        MsrpGovernanceRepairCase
    )
    if filters:
        count_stmt = count_stmt.where(*filters)
        stmt = stmt.where(*filters)
    total = int(session.execute(count_stmt).scalar_one())
    rows = session.execute(
        stmt.order_by(
            MsrpGovernanceRepairCase.priority.desc(),
            MsrpGovernanceRepairCase.last_seen_at_utc.desc(),
        )
        .offset(max(0, offset))
        .limit(max(1, min(limit, 500)))
    ).scalars().all()
    return total, rows


def list_unresolved_conflicts(
    session: Session,
    *,
    limit: int,
    offset: int,
) -> tuple[int, list[MsrpGovernanceRepairCase]]:
    filters = (
        MsrpGovernanceRepairCase.repair_domain.in_(("source", "semantic")),
        MsrpGovernanceRepairCase.open_dedupe_key.is_not(None),
        or_(
            func.lower(MsrpGovernanceRepairCase.case_type).contains("conflict"),
            func.lower(MsrpGovernanceRepairCase.failure_classifier).contains(
                "conflict"
            ),
        ),
    )
    count_stmt = (
        select(func.count())
        .select_from(MsrpGovernanceRepairCase)
        .where(*filters)
    )
    stmt = (
        select(MsrpGovernanceRepairCase)
        .where(*filters)
        .order_by(
            MsrpGovernanceRepairCase.priority.desc(),
            MsrpGovernanceRepairCase.last_seen_at_utc.desc(),
        )
        .offset(max(0, offset))
        .limit(max(1, min(limit, 500)))
    )
    total = int(session.execute(count_stmt).scalar_one())
    return total, session.execute(stmt).scalars().all()


def summarize_open_cases_by_target(
    session: Session,
    target_ids: list[UUID],
) -> dict[UUID, dict[str, int]]:
    if not target_ids:
        return {}
    stmt = (
        select(
            MsrpGovernanceRepairCase.target_id,
            func.count().label("open_count"),
            func.count().filter(
                MsrpGovernanceRepairCase.manual_evidence_required.is_(True)
            ).label("manual_count"),
        )
        .where(
            MsrpGovernanceRepairCase.target_id.in_(target_ids),
            MsrpGovernanceRepairCase.open_dedupe_key.is_not(None),
        )
        .group_by(MsrpGovernanceRepairCase.target_id)
    )
    return {
        row.target_id: {
            "openCaseCount": int(row.open_count),
            "manualEvidenceCaseCount": int(row.manual_count),
        }
        for row in session.execute(stmt)
        if row.target_id is not None
    }


def get_proposal(
    session: Session,
    proposal_id: UUID,
    *,
    for_update: bool = False,
) -> MsrpRepairProposal | None:
    return session.get(
        MsrpRepairProposal,
        proposal_id,
        with_for_update=for_update,
    )


def list_proposals_for_case(
    session: Session,
    case_id: UUID,
) -> list[MsrpRepairProposal]:
    stmt = (
        select(MsrpRepairProposal)
        .where(MsrpRepairProposal.case_id == case_id)
        .order_by(MsrpRepairProposal.created_at_utc.desc())
    )
    return session.execute(stmt).scalars().all()


def get_result_correction(
    session: Session,
    correction_decision_id: UUID,
    *,
    for_update: bool = False,
) -> MsrpResultCorrectionDecision | None:
    return session.get(
        MsrpResultCorrectionDecision,
        correction_decision_id,
        with_for_update=for_update,
    )


def list_result_corrections_for_observations(
    session: Session,
    observation_ids: list[UUID],
) -> list[MsrpResultCorrectionDecision]:
    if not observation_ids:
        return []
    stmt = (
        select(MsrpResultCorrectionDecision)
        .where(
            MsrpResultCorrectionDecision.original_observation_id.in_(
                observation_ids
            )
        )
        .order_by(MsrpResultCorrectionDecision.created_at_utc.desc())
    )
    return session.execute(stmt).scalars().all()


def get_fx_run(
    session: Session,
    fx_run_id: UUID,
    *,
    for_update: bool = False,
) -> MsrpFxNormalizationRun | None:
    return session.get(
        MsrpFxNormalizationRun,
        fx_run_id,
        with_for_update=for_update,
    )


def list_approved_fx_runs_for_observation(
    session: Session,
    observation_id: UUID,
    *,
    for_update: bool = False,
) -> list[MsrpFxNormalizationRun]:
    stmt = select(MsrpFxNormalizationRun).where(
        MsrpFxNormalizationRun.observation_id == observation_id,
        MsrpFxNormalizationRun.run_status == "approved",
    )
    if for_update:
        stmt = stmt.with_for_update()
    return session.execute(stmt).scalars().all()


def list_fx_runs(
    session: Session,
    *,
    observation_id: UUID | None,
    run_status: str | None,
    limit: int,
) -> list[MsrpFxNormalizationRun]:
    stmt: Select[tuple[MsrpFxNormalizationRun]] = select(MsrpFxNormalizationRun)
    if observation_id:
        stmt = stmt.where(MsrpFxNormalizationRun.observation_id == observation_id)
    if run_status:
        stmt = stmt.where(MsrpFxNormalizationRun.run_status == run_status)
    stmt = stmt.order_by(MsrpFxNormalizationRun.created_at_utc.desc()).limit(
        max(1, min(limit, 500))
    )
    return session.execute(stmt).scalars().all()


def list_fx_runs_for_observations(
    session: Session,
    observation_ids: list[UUID],
) -> list[MsrpFxNormalizationRun]:
    if not observation_ids:
        return []
    stmt = (
        select(MsrpFxNormalizationRun)
        .where(MsrpFxNormalizationRun.observation_id.in_(observation_ids))
        .order_by(MsrpFxNormalizationRun.created_at_utc.desc())
    )
    return session.execute(stmt).scalars().all()


def get_audit_event_by_idempotency(
    session: Session,
    *,
    action: str,
    idempotency_key: str,
) -> MsrpGovernanceAuditEvent | None:
    stmt = select(MsrpGovernanceAuditEvent).where(
        MsrpGovernanceAuditEvent.action == action,
        MsrpGovernanceAuditEvent.idempotency_key == idempotency_key,
    )
    return session.execute(stmt).scalar_one_or_none()


def list_audit_events(
    session: Session,
    *,
    entity_type: str | None,
    entity_id: str | None,
    limit: int,
) -> list[MsrpGovernanceAuditEvent]:
    stmt: Select[tuple[MsrpGovernanceAuditEvent]] = select(
        MsrpGovernanceAuditEvent
    )
    if entity_type:
        stmt = stmt.where(MsrpGovernanceAuditEvent.entity_type == entity_type)
    if entity_id:
        stmt = stmt.where(MsrpGovernanceAuditEvent.entity_id == entity_id)
    stmt = stmt.order_by(MsrpGovernanceAuditEvent.occurred_at_utc.desc()).limit(
        max(1, min(limit, 500))
    )
    return session.execute(stmt).scalars().all()
