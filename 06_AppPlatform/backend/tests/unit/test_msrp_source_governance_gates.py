from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import HTTPException

from app.api.msrp_source_governance_schemas import (
    FxGateInput,
    GateEvaluationRequest,
    MappingGateInput,
    SourceGateInput,
)
from app.db.msrp_source_governance_models import MsrpGovernanceGateDecision
from app.services.msrp_materialization_eligibility_service import (
    evaluate_materialization_eligibility,
)
from app.services.msrp_source_governance import service as governance_service


def _source_gate(**overrides) -> SourceGateInput:
    values = {
        "policy_version": "source-gate-v1",
        "source_version_status": "published",
        "verified_official_evidence": True,
        "immutable_evidence": True,
        "deterministic_extraction": True,
        "semantic_lane_valid": True,
        "currency_valid": True,
        "tax_mode_valid": True,
        "validity_window_valid": True,
        "blocking_conflict": False,
        "unresolved_result_correction": False,
        "schema_valid": True,
        "targeted_dryrun_passed": True,
    }
    values.update(overrides)
    return SourceGateInput(**values)


def _mapping_gate(**overrides) -> MappingGateInput:
    values = {
        "policy_version": "mapping-gate-v1",
        "decision_status": "auto_accepted",
        "hard_constraints_passed": True,
        "score_threshold_passed": True,
        "margin_threshold_passed": True,
        "policy_version_accepted": True,
        "human_approved": False,
    }
    values.update(overrides)
    return MappingGateInput(**values)


def _fx_gate(**overrides) -> FxGateInput:
    values = {
        "policy_version": "fx-gate-v1",
        "provider_approved": True,
        "rate_positive": True,
        "effective_date_valid": True,
        "freshness_valid": True,
        "local_value_unchanged": True,
    }
    values.update(overrides)
    return FxGateInput(**values)


def _evaluate(source, mapping, fx=None):
    evaluated_at = datetime(2026, 7, 14, 3, 30, tzinfo=timezone.utc)
    return evaluate_materialization_eligibility(
        target_id=uuid4(),
        observation_id=uuid4(),
        source_gate=source,
        mapping_gate=mapping,
        fx_gate=fx,
        evaluated_at=evaluated_at,
    )


def test_dual_gate_and_fx_pass_materializes_both_lanes() -> None:
    decision = _evaluate(_source_gate(), _mapping_gate(), _fx_gate())

    assert decision.source_gate.status == "pass"
    assert decision.mapping_gate.status == "pass"
    assert decision.fx_gate is not None
    assert decision.fx_gate.status == "pass"
    assert decision.eligible_for_local_materialization is True
    assert decision.eligible_for_normalized_materialization is True


def test_source_gate_lists_every_blocking_reason() -> None:
    decision = _evaluate(
        _source_gate(
            source_version_status="dryrun_passed",
            verified_official_evidence=False,
            blocking_conflict=True,
            unresolved_result_correction=True,
            targeted_dryrun_passed=False,
        ),
        _mapping_gate(),
    )

    assert decision.source_gate.status == "fail"
    assert decision.source_gate.reasons == [
        "source_version_not_published",
        "official_evidence_not_verified",
        "blocking_source_or_semantic_conflict",
        "unresolved_result_correction",
        "targeted_dryrun_not_passed",
    ]
    assert decision.eligible_for_local_materialization is False


def test_human_mapping_approval_can_replace_score_and_margin_thresholds() -> None:
    decision = _evaluate(
        _source_gate(),
        _mapping_gate(
            decision_status="manual_approved",
            score_threshold_passed=False,
            margin_threshold_passed=False,
            human_approved=True,
        ),
    )

    assert decision.mapping_gate.status == "pass"
    assert decision.eligible_for_local_materialization is True


def test_human_mapping_approval_cannot_bypass_hard_constraints() -> None:
    decision = _evaluate(
        _source_gate(),
        _mapping_gate(
            decision_status="manual_approved",
            hard_constraints_passed=False,
            score_threshold_passed=False,
            margin_threshold_passed=False,
            human_approved=True,
        ),
    )

    assert decision.mapping_gate.status == "fail"
    assert decision.mapping_gate.reasons == ["mapping_hard_constraints_failed"]
    assert decision.eligible_for_local_materialization is False


def test_fx_failure_never_invalidates_official_local_msrp() -> None:
    decision = _evaluate(
        _source_gate(),
        _mapping_gate(),
        _fx_gate(freshness_valid=False, local_value_unchanged=False),
    )

    assert decision.eligible_for_local_materialization is True
    assert decision.eligible_for_normalized_materialization is False
    assert decision.fx_gate is not None
    assert decision.fx_gate.reasons == [
        "fx_rate_stale",
        "fx_repair_changed_local_msrp",
    ]


def test_missing_fx_gate_keeps_local_lane_valid_and_normalized_lane_pending() -> None:
    decision = _evaluate(_source_gate(), _mapping_gate())

    assert decision.fx_gate is None
    assert decision.eligible_for_local_materialization is True
    assert decision.eligible_for_normalized_materialization is False


class _GateSession:
    def __init__(self, observation) -> None:
        self.observation = observation
        self.added: list[object] = []
        self.commits = 0

    def get(self, _model, _identity):
        return self.observation

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


def test_gate_evaluation_is_persisted_as_append_only_read_model(monkeypatch) -> None:
    target_id = uuid4()
    observation_id = uuid4()
    target = SimpleNamespace(target_id=target_id, country="SE", brand="Volvo")
    observation = SimpleNamespace(
        observation_id=observation_id,
        country="se",
        brand=" volvo ",
    )
    session = _GateSession(observation)
    monkeypatch.setattr(
        governance_service.repo,
        "get_audit_event_by_idempotency",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        governance_service.repo,
        "get_target",
        lambda *_args, **_kwargs: target,
    )
    payload = GateEvaluationRequest(
        target_id=target_id,
        observation_id=observation_id,
        source_gate=_source_gate(),
        mapping_gate=_mapping_gate(),
        fx_gate=_fx_gate(),
        evaluation_context={"sourceRunId": "run-1", "mappingDecisionId": "map-1"},
    )

    result = governance_service.MsrpSourceGovernanceService(
        session
    ).evaluate_and_record_gate_decision(
        payload,
        actor="governance-worker",
        actor_role="editor",
        idempotency_key="gate-run-1",
    )

    rows = [item for item in session.added if isinstance(item, MsrpGovernanceGateDecision)]
    assert len(rows) == 1
    assert rows[0].source_gate_json["status"] == "pass"
    assert rows[0].mapping_gate_json["status"] == "pass"
    assert rows[0].fx_gate_json["status"] == "pass"
    assert rows[0].evaluation_context_json["sourceRunId"] == "run-1"
    assert result["gateDecisionId"] == str(rows[0].gate_decision_id)
    assert result["eligibleForLocalMaterialization"] is True
    assert result["eligibleForNormalizedMaterialization"] is True
    assert session.commits == 1


def test_gate_evaluation_rejects_cross_country_target(monkeypatch) -> None:
    target_id = uuid4()
    observation_id = uuid4()
    session = _GateSession(
        SimpleNamespace(
            observation_id=observation_id,
            country="CH",
            brand="Volvo",
        )
    )
    monkeypatch.setattr(
        governance_service.repo,
        "get_audit_event_by_idempotency",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        governance_service.repo,
        "get_target",
        lambda *_args, **_kwargs: SimpleNamespace(
            target_id=target_id,
            country="SE",
            brand="Volvo",
        ),
    )
    payload = GateEvaluationRequest(
        target_id=target_id,
        observation_id=observation_id,
        source_gate=_source_gate(),
        mapping_gate=_mapping_gate(),
    )

    with pytest.raises(HTTPException, match="country differs") as exc_info:
        governance_service.MsrpSourceGovernanceService(
            session
        ).evaluate_and_record_gate_decision(
            payload,
            actor="governance-worker",
            actor_role="editor",
            idempotency_key="gate-run-country-mismatch",
        )

    assert exc_info.value.status_code == 422
    assert session.added == []


def test_latest_gate_read_model_returns_persisted_metadata(monkeypatch) -> None:
    target_id = uuid4()
    observation_id = uuid4()
    evaluated_at = datetime(2026, 7, 14, 7, 0, tzinfo=timezone.utc)
    decision = evaluate_materialization_eligibility(
        target_id=target_id,
        observation_id=observation_id,
        source_gate=_source_gate(),
        mapping_gate=_mapping_gate(),
        evaluated_at=evaluated_at,
    )
    row = MsrpGovernanceGateDecision(
        gate_decision_id=uuid4(),
        schema_version="1.0",
        target_id=target_id,
        observation_id=observation_id,
        source_gate_json=decision.source_gate.model_dump(mode="json", by_alias=True),
        mapping_gate_json=decision.mapping_gate.model_dump(mode="json", by_alias=True),
        fx_gate_json=None,
        eligible_for_local_materialization=True,
        eligible_for_normalized_materialization=False,
        evaluation_context_json={"mappingDecisionId": "mapping-1"},
        evaluated_at_utc=evaluated_at,
        created_by="governance-worker",
        created_at_utc=evaluated_at,
    )
    monkeypatch.setattr(
        governance_service.repo,
        "get_target",
        lambda *_args, **_kwargs: SimpleNamespace(target_id=target_id),
    )
    monkeypatch.setattr(
        governance_service.repo,
        "get_latest_gate_decision_for_target",
        lambda *_args, **_kwargs: row,
    )

    result = governance_service.MsrpSourceGovernanceService(
        object()
    ).get_latest_gate_decision(target_id)

    assert result is not None
    assert result["gateDecisionId"] == str(row.gate_decision_id)
    assert result["evaluationContext"] == {"mappingDecisionId": "mapping-1"}
    assert result["createdBy"] == "governance-worker"
