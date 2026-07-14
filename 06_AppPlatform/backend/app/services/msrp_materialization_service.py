from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import re
from uuid import UUID, uuid4

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.db.models import MsrpObservation
from app.db.msrp_materialization_models import (
    MsrpMaterializationApproval,
    MsrpMaterializationApprovalItem,
    MsrpMaterializationExecution,
)
from app.db.msrp_source_governance_models import MsrpGovernanceGateDecision
from app.infra import msrp_materialization_repository as approval_repo
from app.infra import msrp_repository as msrp_repo
from app.infra import msrp_source_governance_repository as governance_repo
from app.services.msrp_evidence_verifier import verify_observation_evidence


_CONTEXT_SEAL = object()
_EDITOR_ROLES = frozenset({"editor", "admin"})
_SERVICE_ACTOR_PATTERN = re.compile(
    r"(^|[-_.])(airflow|auto|bot|cron|daemon|runner|service|system|timer)([-_.]|$)",
    re.IGNORECASE,
)
_MAX_APPROVAL_LIFETIME = timedelta(hours=24)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _normalized_actor(value: str) -> str:
    return str(value or "").strip()


def _require_human_editor(actor: str, role: str) -> None:
    if role not in _EDITOR_ROLES:
        raise HTTPException(
            status_code=403,
            detail="Materialization approval requires an editor role.",
        )
    if not actor or _SERVICE_ACTOR_PATTERN.search(actor):
        raise HTTPException(
            status_code=403,
            detail=(
                "Cron, timer, service, runner, bot, and auto identities cannot "
                "create editor materialization approvals."
            ),
        )


def _require_authenticated_identity(identity_source: str) -> None:
    if identity_source != "authenticated_session":
        raise HTTPException(
            status_code=403,
            detail=(
                "MSRP fact approval/execution requires an authenticated login "
                "session identity."
            ),
        )


def _gate_status_passed(value: object) -> bool:
    return isinstance(value, dict) and value.get("status") == "pass"


def _canonical_evidence_ref_tokens(
    evidence_refs: object,
) -> tuple[str, ...]:
    if not isinstance(evidence_refs, (list, tuple)):
        return ()
    tokens = [
        json.dumps(
            dict(item),
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        for item in evidence_refs
        if isinstance(item, dict)
    ]
    if len(tokens) != len(evidence_refs):
        return ()
    return tuple(sorted(tokens))


@dataclass(frozen=True, slots=True)
class MaterializationEvidenceBinding:
    observation_id: UUID
    gate_decision_id: UUID
    source_version_id: UUID
    evidence_ref_tokens: tuple[str, ...]

    def payload(self) -> dict[str, object]:
        return {
            "observationId": str(self.observation_id),
            "gateDecisionId": str(self.gate_decision_id),
            "sourceVersionId": str(self.source_version_id),
            "verifiedEvidenceRefs": [
                json.loads(token) for token in self.evidence_ref_tokens
            ],
        }


def _require_gate_evidence_binding(
    session: Session,
    observation: MsrpObservation,
    gate_decision: MsrpGovernanceGateDecision,
) -> MaterializationEvidenceBinding:
    verification = verify_observation_evidence(
        session,
        observation,
        target_id=gate_decision.target_id,
    )
    if (
        not verification.passed
        or verification.source_version_id is None
        or not verification.evidence_refs
    ):
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Current replayable evidence verification failed.",
                "observationId": str(observation.observation_id),
                "reasons": list(verification.reasons),
            },
        )

    context = getattr(gate_decision, "evaluation_context_json", None)
    context = context if isinstance(context, dict) else {}
    expected_source_version_id = str(context.get("sourceVersionId") or "")
    expected_tokens = _canonical_evidence_ref_tokens(
        context.get("verifiedEvidenceRefs")
    )
    current_tokens = _canonical_evidence_ref_tokens(verification.evidence_refs)
    if (
        expected_source_version_id != str(verification.source_version_id)
        or not expected_tokens
        or expected_tokens != current_tokens
    ):
        raise HTTPException(
            status_code=409,
            detail={
                "message": "GateDecision evidence binding is stale or changed.",
                "observationId": str(observation.observation_id),
            },
        )
    return MaterializationEvidenceBinding(
        observation_id=observation.observation_id,
        gate_decision_id=gate_decision.gate_decision_id,
        source_version_id=verification.source_version_id,
        evidence_ref_tokens=current_tokens,
    )


def _require_current_gate_decision(
    session: Session,
    observation: MsrpObservation,
    gate_decision: MsrpGovernanceGateDecision | None,
    *,
    operation: str,
    evidence_bindings: dict[UUID, MaterializationEvidenceBinding] | None = None,
) -> MsrpGovernanceGateDecision:
    if gate_decision is None:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Observation {observation.observation_id} has no persisted "
                "GateDecision."
            ),
        )
    if gate_decision.observation_id != observation.observation_id:
        raise HTTPException(
            status_code=409,
            detail="GateDecision does not belong to the scoped observation.",
        )
    latest = governance_repo.get_latest_gate_decision_for_target(
        session,
        gate_decision.target_id,
    )
    if latest is None or latest.gate_decision_id != gate_decision.gate_decision_id:
        raise HTTPException(
            status_code=409,
            detail="GateDecision is stale or superseded.",
        )
    target = governance_repo.get_target(session, gate_decision.target_id)
    if target is None:
        raise HTTPException(status_code=409, detail="GateDecision target is missing.")
    if target.country.strip().upper() != observation.country.strip().upper():
        raise HTTPException(
            status_code=409,
            detail="GateDecision target country does not match observation scope.",
        )
    if target.brand.strip().casefold() != observation.brand.strip().casefold():
        raise HTTPException(
            status_code=409,
            detail="GateDecision target brand does not match observation scope.",
        )
    if _as_utc(gate_decision.evaluated_at_utc) < _as_utc(
        observation.observed_at_utc
    ):
        raise HTTPException(
            status_code=409,
            detail="GateDecision predates the observation and is invalid.",
        )
    if operation == "materialize":
        if not gate_decision.eligible_for_local_materialization:
            raise HTTPException(
                status_code=409,
                detail="GateDecision is not eligible for local materialization.",
            )
        if not _gate_status_passed(gate_decision.source_gate_json):
            raise HTTPException(status_code=409, detail="Source Gate did not pass.")
        if not _gate_status_passed(gate_decision.mapping_gate_json):
            raise HTTPException(status_code=409, detail="Mapping Gate did not pass.")
        binding = _require_gate_evidence_binding(
            session,
            observation,
            gate_decision,
        )
        if evidence_bindings is not None:
            evidence_bindings[observation.observation_id] = binding
    return gate_decision


@dataclass(frozen=True, slots=True)
class MaterializationExecutionContext:
    """Capability created only after approval and GateDecision revalidation."""

    execution_id: UUID
    approval_id: UUID
    operation: str
    observation_ids: frozenset[UUID]
    gate_decision_ids: frozenset[UUID]
    evidence_bindings: tuple[MaterializationEvidenceBinding, ...]
    _seal: object

    def require_observation(self, observation_id: UUID, operation: str) -> None:
        if self._seal is not _CONTEXT_SEAL:
            raise HTTPException(
                status_code=403,
                detail="Materialization execution context is not validated.",
            )
        if self.operation != operation:
            raise HTTPException(
                status_code=409,
                detail="Materialization execution context operation mismatch.",
            )
        if observation_id not in self.observation_ids:
            raise HTTPException(
                status_code=409,
                detail="Observation is outside the approved materialization scope.",
            )

    def require_verified_evidence(
        self,
        observation_id: UUID,
        source_version_id: UUID | None,
        evidence_refs: object,
    ) -> None:
        self.require_observation(observation_id, "materialize")
        binding = next(
            (
                item
                for item in self.evidence_bindings
                if item.observation_id == observation_id
            ),
            None,
        )
        if (
            binding is None
            or source_version_id != binding.source_version_id
            or _canonical_evidence_ref_tokens(evidence_refs)
            != binding.evidence_ref_tokens
        ):
            raise HTTPException(
                status_code=409,
                detail=(
                    "Replayable evidence changed after GateDecision validation."
                ),
            )


def _approval_payload(
    approval: MsrpMaterializationApproval,
    items: list[MsrpMaterializationApprovalItem],
) -> dict[str, object]:
    return {
        "approvalId": str(approval.approval_id),
        "operation": approval.operation,
        "scopeKind": approval.scope_kind,
        "scrapeBatchId": (
            str(approval.scrape_batch_id) if approval.scrape_batch_id else None
        ),
        "country": approval.country,
        "status": approval.status,
        "editorActor": approval.editor_actor,
        "editorRole": approval.editor_role,
        "editorIdentitySource": approval.editor_identity_source,
        "approvedAtUtc": approval.approved_at_utc.isoformat(),
        "expiresAtUtc": approval.expires_at_utc.isoformat(),
        "reason": approval.reason,
        "rollbackPlanRef": approval.rollback_plan_ref,
        "compensationPlanRef": approval.compensation_plan_ref,
        "observationIds": [str(item.observation_id) for item in items],
        "gateDecisionIds": [str(item.gate_decision_id) for item in items],
    }


def execution_payload(execution: MsrpMaterializationExecution) -> dict[str, object]:
    return {
        "executionId": str(execution.execution_id),
        "approvalId": str(execution.approval_id),
        "operation": execution.operation,
        "runId": execution.run_id,
        "idempotencyKey": execution.idempotency_key,
        "status": execution.status,
        "editorActor": execution.editor_actor,
        "executedByActor": execution.executed_by_actor,
        "executedByRole": execution.executed_by_role,
        "executedByIdentitySource": execution.executed_by_identity_source,
        "executionContext": execution.execution_context,
        "scope": execution.scope_json,
        "gateDecisionIds": execution.gate_decision_ids_json,
        "observationIds": execution.observation_ids_json,
        "beforeFactRefs": execution.before_fact_refs_json,
        "afterFactRefs": execution.after_fact_refs_json,
        "rollbackRef": execution.rollback_ref,
        "compensationRef": execution.compensation_ref,
        "errorCode": execution.error_code,
        "errorDetail": execution.error_detail,
        "startedAtUtc": execution.started_at_utc.isoformat(),
        "finishedAtUtc": (
            execution.finished_at_utc.isoformat()
            if execution.finished_at_utc
            else None
        ),
    }


def create_editor_approval(
    session: Session,
    data: dict[str, object],
    *,
    actor_name: str,
    actor_role: str,
    actor_identity_source: str,
) -> dict[str, object]:
    actor = _normalized_actor(actor_name)
    role = str(actor_role or "").strip().lower()
    identity_source = str(actor_identity_source or "").strip().lower()
    _require_authenticated_identity(identity_source)
    _require_human_editor(actor, role)

    operation = str(data.get("operation") or "materialize").strip()
    if operation != "materialize":
        raise HTTPException(
            status_code=409,
            detail=(
                "Only materialize approvals are implemented; remap/cleanup "
                "remain disabled until a dedicated compensation executor exists."
            ),
        )
    scope_kind = str(data.get("scope_kind") or "").strip()
    observation_ids = [UUID(str(value)) for value in data.get("observation_ids") or []]
    gate_decision_ids = [
        UUID(str(value)) for value in data.get("gate_decision_ids") or []
    ]
    if not observation_ids or len(observation_ids) != len(gate_decision_ids):
        raise HTTPException(
            status_code=400,
            detail="Approval requires exact observation/GateDecision pairs.",
        )

    batch_id_value = data.get("scrape_batch_id")
    scrape_batch_id = UUID(str(batch_id_value)) if batch_id_value else None
    observations: list[MsrpObservation] = []
    decisions: list[MsrpGovernanceGateDecision] = []
    for observation_id, gate_decision_id in zip(
        observation_ids,
        gate_decision_ids,
        strict=True,
    ):
        observation = msrp_repo.get_observation(session, observation_id)
        if observation is None:
            raise HTTPException(
                status_code=404,
                detail=f"Observation not found: {observation_id}",
            )
        gate_decision = governance_repo.get_gate_decision(
            session,
            gate_decision_id,
        )
        decisions.append(
            _require_current_gate_decision(
                session,
                observation,
                gate_decision,
                operation=operation,
            )
        )
        observations.append(observation)

    countries = {item.country.strip().upper() for item in observations}
    if len(countries) != 1:
        raise HTTPException(
            status_code=400,
            detail="One approval cannot span multiple countries.",
        )
    if scope_kind == "batch":
        if scrape_batch_id is None:
            raise HTTPException(status_code=400, detail="Batch scope requires batch id.")
        if any(item.scrape_batch_id != scrape_batch_id for item in observations):
            raise HTTPException(
                status_code=409,
                detail="Observation is outside the approved scrape batch.",
            )
    elif scope_kind != "observations" or scrape_batch_id is not None:
        raise HTTPException(status_code=400, detail="Invalid approval scope.")

    now = _utc_now()
    expires_at = data.get("expires_at_utc")
    if not isinstance(expires_at, datetime):
        expires_at = now + timedelta(hours=4)
    expires_at = _as_utc(expires_at)
    if expires_at <= now or expires_at > now + _MAX_APPROVAL_LIFETIME:
        raise HTTPException(
            status_code=400,
            detail="Approval expiry must be within the next 24 hours.",
        )

    approval = MsrpMaterializationApproval(
        approval_id=uuid4(),
        operation=operation,
        scope_kind=scope_kind,
        scrape_batch_id=scrape_batch_id,
        country=next(iter(countries)),
        status="approved",
        editor_actor=actor,
        editor_role=role,
        editor_identity_source=identity_source,
        reason=str(data.get("reason") or "").strip(),
        rollback_plan_ref=str(data.get("rollback_plan_ref") or "").strip(),
        compensation_plan_ref=str(
            data.get("compensation_plan_ref") or ""
        ).strip(),
        approved_at_utc=now,
        expires_at_utc=expires_at,
    )
    if not approval.reason or not approval.rollback_plan_ref or not approval.compensation_plan_ref:
        raise HTTPException(
            status_code=400,
            detail="Approval reason and rollback/compensation plan refs are required.",
        )
    approval_repo.add(session, approval)
    items = [
        MsrpMaterializationApprovalItem(
            approval_item_id=uuid4(),
            approval_id=approval.approval_id,
            observation_id=observation.observation_id,
            gate_decision_id=decision.gate_decision_id,
            ordinal=index,
        )
        for index, (observation, decision) in enumerate(
            zip(observations, decisions, strict=True)
        )
    ]
    for item in items:
        approval_repo.add(session, item)
    session.commit()
    return _approval_payload(approval, items)


def _fact_refs(
    session: Session,
    observations: list[MsrpObservation],
) -> list[dict[str, object]]:
    refs: list[dict[str, object]] = []
    for observation in observations:
        current_price = msrp_repo.get_current_price_by_key(
            session,
            observation.country,
            observation.brand,
            observation.jato_model,
            observation.jato_trim,
            observation.jato_powertrain,
        )
        open_period = (
            msrp_repo.get_open_price_period(
                session,
                observation.country,
                observation.brand,
                observation.jato_model,
                observation.jato_trim,
                observation.jato_powertrain,
            )
            if msrp_repo.has_price_history_table(session)
            else None
        )
        refs.append(
            {
                "observationId": str(observation.observation_id),
                "businessKey": {
                    "country": observation.country,
                    "brand": observation.brand,
                    "jatoModel": observation.jato_model,
                    "jatoTrim": observation.jato_trim,
                    "jatoPowertrain": str(observation.jato_powertrain or ""),
                },
                "currentPriceId": (
                    str(current_price.current_price_id) if current_price else None
                ),
                "currentPriceValue": (
                    str(current_price.source_msrp_value)
                    if current_price is not None
                    else None
                ),
                "currentPriceCurrency": (
                    current_price.source_currency if current_price else None
                ),
                "effectiveObservationId": (
                    str(current_price.effective_observation_id)
                    if current_price
                    else None
                ),
                "currentSourceVersionId": (
                    str(current_price.source_version_id)
                    if current_price
                    and getattr(current_price, "source_version_id", None)
                    else None
                ),
                "currentEvidenceRefs": (
                    list(
                        getattr(current_price, "evidence_refs_json", None)
                        or []
                    )
                    if current_price
                    else []
                ),
                "lastPriceChangeAtUtc": (
                    current_price.last_price_change_at_utc.isoformat()
                    if current_price
                    and current_price.last_price_change_at_utc
                    else None
                ),
                "priceHistoryId": (
                    str(open_period.price_history_id) if open_period else None
                ),
                "priceHistoryValidToUtc": (
                    open_period.valid_to_utc.isoformat()
                    if open_period and open_period.valid_to_utc
                    else None
                ),
                "priceHistoryValidFromUtc": (
                    open_period.valid_from_utc.isoformat()
                    if open_period
                    else None
                ),
                "priceHistoryLastConfirmedAtUtc": (
                    open_period.last_confirmed_at_utc.isoformat()
                    if open_period
                    else None
                ),
                "priceHistoryValue": (
                    str(open_period.msrp_value) if open_period else None
                ),
                "priceHistoryCurrency": (
                    open_period.currency if open_period else None
                ),
                "priceHistorySourceValue": (
                    str(open_period.source_msrp_value)
                    if open_period
                    else None
                ),
                "priceHistorySourceCurrency": (
                    open_period.source_currency if open_period else None
                ),
                "priceHistorySourceVersionId": (
                    str(open_period.source_version_id)
                    if open_period
                    and getattr(open_period, "source_version_id", None)
                    else None
                ),
                "priceHistoryEvidenceRefs": (
                    list(
                        getattr(open_period, "evidence_refs_json", None)
                        or []
                    )
                    if open_period
                    else []
                ),
                "priceHistoryStartedByObservationId": (
                    str(open_period.started_by_observation_id)
                    if open_period
                    else None
                ),
                "priceHistoryEndedByObservationId": (
                    str(open_period.ended_by_observation_id)
                    if open_period and open_period.ended_by_observation_id
                    else None
                ),
                "priceHistoryLastConfirmedByObservationId": (
                    str(open_period.last_confirmed_by_observation_id)
                    if open_period
                    else None
                ),
            }
        )
    return refs


def _validated_execution_scope(
    session: Session,
    approval: MsrpMaterializationApproval,
    items: list[MsrpMaterializationApprovalItem],
) -> tuple[
    list[MsrpObservation],
    list[MsrpGovernanceGateDecision],
    tuple[MaterializationEvidenceBinding, ...],
]:
    observations: list[MsrpObservation] = []
    decisions: list[MsrpGovernanceGateDecision] = []
    evidence_bindings: dict[UUID, MaterializationEvidenceBinding] = {}
    for item in items:
        observation = msrp_repo.get_observation(session, item.observation_id)
        if observation is None:
            raise HTTPException(status_code=409, detail="Approved observation is missing.")
        gate = governance_repo.get_gate_decision(session, item.gate_decision_id)
        decisions.append(
            _require_current_gate_decision(
                session,
                observation,
                gate,
                operation=approval.operation,
                evidence_bindings=evidence_bindings,
            )
        )
        if (
            approval.scope_kind == "batch"
            and observation.scrape_batch_id != approval.scrape_batch_id
        ):
            raise HTTPException(
                status_code=409,
                detail="Approved batch scope changed before execution.",
            )
        if observation.country.strip().upper() != approval.country:
            raise HTTPException(
                status_code=409,
                detail="Approved country scope changed before execution.",
            )
        observations.append(observation)
    return (
        observations,
        decisions,
        tuple(
            evidence_bindings[observation.observation_id]
            for observation in observations
        ),
    )


def execute_materialization(
    session: Session,
    *,
    approval_id: UUID,
    run_id: str,
    idempotency_key: str,
    executed_by_actor: str,
    executed_by_role: str,
    executed_by_identity_source: str,
    execution_context: str,
    limit: int = 500,
) -> dict[str, object]:
    actor = _normalized_actor(executed_by_actor)
    role = str(executed_by_role or "").strip().lower()
    identity_source = str(executed_by_identity_source or "").strip().lower()
    context_name = str(execution_context or "").strip().lower()
    _require_authenticated_identity(identity_source)
    _require_human_editor(actor, role)
    if context_name != "interactive_editor":
        raise HTTPException(
            status_code=403,
            detail="Materialization execution is disabled outside interactive editor context.",
        )
    normalized_run_id = str(run_id).strip()
    normalized_key = str(idempotency_key).strip()
    if len(normalized_run_id) < 3 or len(normalized_key) < 8:
        raise HTTPException(
            status_code=400,
            detail="runId and idempotencyKey are required stable request fields.",
        )
    normalized_limit = max(1, min(int(limit), 500))
    existing = approval_repo.get_execution_by_idempotency_key(
        session,
        normalized_key,
    )
    if existing is not None:
        stable_request = (
            existing.approval_id == approval_id
            and existing.operation == "materialize"
            and existing.run_id == normalized_run_id
            and existing.executed_by_actor == actor
            and existing.executed_by_role == role
            and existing.executed_by_identity_source == identity_source
            and existing.execution_context == context_name
            and int(existing.scope_json.get("requestedLimit") or 0)
            == normalized_limit
        )
        if not stable_request:
            raise HTTPException(
                status_code=409,
                detail=(
                    "Idempotency key is already bound to a different stable "
                    "execution request."
                ),
            )
        return execution_payload(existing)

    approval = approval_repo.get_approval(session, approval_id, for_update=True)
    if approval is None:
        raise HTTPException(status_code=404, detail="Materialization approval not found.")
    if approval.operation != "materialize":
        raise HTTPException(status_code=409, detail="Approval operation is not materialize.")
    now = _utc_now()
    if approval.status != "approved":
        raise HTTPException(status_code=409, detail="Approval is not executable.")
    if _as_utc(approval.expires_at_utc) <= now:
        approval.status = "expired"
        session.commit()
        raise HTTPException(status_code=409, detail="Approval has expired.")

    items = approval_repo.list_approval_items(session, approval.approval_id)
    if not items:
        raise HTTPException(status_code=409, detail="Approval scope is empty.")
    if len(items) > normalized_limit:
        raise HTTPException(
            status_code=409,
            detail="Approval scope exceeds the requested materialization limit.",
        )
    observations, decisions, evidence_bindings = _validated_execution_scope(
        session,
        approval,
        items,
    )
    execution_id = uuid4()
    execution = MsrpMaterializationExecution(
        execution_id=execution_id,
        approval_id=approval.approval_id,
        scrape_batch_id=approval.scrape_batch_id,
        operation="materialize",
        run_id=normalized_run_id,
        idempotency_key=normalized_key,
        editor_actor=approval.editor_actor,
        executed_by_actor=actor,
        executed_by_role=role,
        executed_by_identity_source=identity_source,
        execution_context=context_name,
        status="running",
        scope_json={
            "scopeKind": approval.scope_kind,
            "country": approval.country,
            "scrapeBatchId": (
                str(approval.scrape_batch_id) if approval.scrape_batch_id else None
            ),
            "requestedLimit": normalized_limit,
            "verifiedEvidenceBindings": [
                binding.payload() for binding in evidence_bindings
            ],
        },
        gate_decision_ids_json=[str(item.gate_decision_id) for item in items],
        observation_ids_json=[str(item.observation_id) for item in items],
        before_fact_refs_json=_fact_refs(session, observations),
        after_fact_refs_json=[],
        rollback_ref=f"execution:{execution_id}:beforeFactRefs",
        compensation_ref=approval.compensation_plan_ref,
        started_at_utc=now,
    )
    approval_repo.add(session, execution)
    approval.status = "executing"
    approval.reserved_at_utc = now
    session.commit()

    context = MaterializationExecutionContext(
        execution_id=execution.execution_id,
        approval_id=approval.approval_id,
        operation="materialize",
        observation_ids=frozenset(item.observation_id for item in items),
        gate_decision_ids=frozenset(
            decision.gate_decision_id for decision in decisions
        ),
        evidence_bindings=evidence_bindings,
        _seal=_CONTEXT_SEAL,
    )
    try:
        from app.services.msrp_workflow_service import (
            materialize_current_price_from_observation,
        )

        materialized = 0
        history_enabled = msrp_repo.has_price_history_table(session)
        for observation in observations:
            current_price = materialize_current_price_from_observation(
                session,
                observation,
                context=context,
                price_history_enabled=history_enabled,
            )
            if current_price is None:
                raise HTTPException(
                    status_code=409,
                    detail="Approved observation is not materialization-eligible.",
                )
            session.flush()
            materialized += 1

        execution = approval_repo.get_execution(
            session,
            execution.execution_id,
            for_update=True,
        )
        approval = approval_repo.get_approval(
            session,
            approval.approval_id,
            for_update=True,
        )
        if execution is None or approval is None:
            raise RuntimeError("Materialization provenance rows disappeared.")
        execution.after_fact_refs_json = _fact_refs(session, observations)
        execution.status = "succeeded"
        execution.finished_at_utc = _utc_now()
        approval.status = "consumed"
        approval.consumed_at_utc = execution.finished_at_utc
        session.commit()
        payload = execution_payload(execution)
        payload["candidateObservations"] = len(observations)
        payload["materializedKeys"] = materialized
        return payload
    except Exception as exc:
        session.rollback()
        failed = approval_repo.get_execution(session, execution.execution_id)
        failed_approval = approval_repo.get_approval(
            session,
            approval.approval_id,
            for_update=True,
        )
        if failed is not None:
            failed.status = "failed"
            failed.finished_at_utc = _utc_now()
            failed.error_code = type(exc).__name__
            failed.error_detail = str(exc)[:2000]
        if failed_approval is not None:
            failed_approval.status = "consumed"
            failed_approval.consumed_at_utc = _utc_now()
        session.commit()
        raise
