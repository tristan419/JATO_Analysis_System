from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import HTTPException

from app.api.msrp_source_governance_schemas import (
    EvidenceReference,
    MappingGateInput,
    ResultCorrectionCreate,
    SourceGateInput,
)
from app.db.models import MsrpObservation
from app.db.msrp_source_governance_models import MsrpGovernanceGateDecision
from app.services.msrp_materialization_eligibility_service import (
    evaluate_materialization_eligibility,
)
from app.services.msrp_source_governance import service as governance_service


class _FakeSession:
    def __init__(self, observation) -> None:
        self.observation = observation
        self.added: list[object] = []

    def get(self, model, identity):
        if model is MsrpObservation and identity == self.observation.observation_id:
            return self.observation
        return None

    def add(self, item) -> None:
        self.added.append(item)

    def flush(self) -> None:
        now = datetime(2026, 7, 14, 6, 0, tzinfo=timezone.utc)
        for item in self.added:
            if hasattr(item, "created_at_utc") and item.created_at_utc is None:
                item.created_at_utc = now
            if hasattr(item, "updated_at_utc") and item.updated_at_utc is None:
                item.updated_at_utc = now

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def refresh(self, _item) -> None:
        return None


def _persisted_gate_decision(observation_id, target_id):
    decision = evaluate_materialization_eligibility(
        target_id=target_id,
        observation_id=observation_id,
        source_gate=SourceGateInput(
            policy_version="source-v1",
            source_version_status="published",
            verified_official_evidence=True,
            immutable_evidence=True,
            deterministic_extraction=True,
            semantic_lane_valid=True,
            currency_valid=True,
            tax_mode_valid=True,
            validity_window_valid=True,
            schema_valid=True,
            targeted_dryrun_passed=True,
        ),
        mapping_gate=MappingGateInput(
            policy_version="mapping-v1",
            decision_status="auto_accepted",
            hard_constraints_passed=True,
            score_threshold_passed=True,
            margin_threshold_passed=True,
            policy_version_accepted=True,
        ),
    )
    return MsrpGovernanceGateDecision(
        gate_decision_id=uuid4(),
        schema_version=decision.schema_version,
        target_id=target_id,
        observation_id=observation_id,
        source_gate_json=decision.source_gate.model_dump(mode="json", by_alias=True),
        mapping_gate_json=decision.mapping_gate.model_dump(mode="json", by_alias=True),
        fx_gate_json=None,
        eligible_for_local_materialization=True,
        eligible_for_normalized_materialization=False,
        evaluated_at_utc=decision.evaluated_at,
        created_by="governance-worker",
        created_at_utc=decision.evaluated_at,
    )


def _payload(observation_id, gate_decision_id, evidence) -> ResultCorrectionCreate:
    return ResultCorrectionCreate(
        original_observation_id=observation_id,
        gate_decision_id=gate_decision_id,
        correction_type="parser_replay",
        reason="Verified parser output against immutable official evidence",
        evidence_refs=[
            EvidenceReference(
                evidence_asset_id=evidence.evidence_asset_id,
                sha256=evidence.sha256,
                evidence_type="official_url",
            )
        ],
        corrected_inputs={"selector": "[data-msrp]"},
        replay_result={"status": "passed"},
    )


def test_result_correction_reuses_persisted_gate_without_mutating_fact(
    monkeypatch,
) -> None:
    target_id = uuid4()
    observation = SimpleNamespace(observation_id=uuid4(), marker="immutable")
    gate = _persisted_gate_decision(observation.observation_id, target_id)
    evidence = SimpleNamespace(
        evidence_asset_id=uuid4(),
        target_id=target_id,
        sha256="a" * 64,
        evidence_type="official_url",
        official_domain_verified=True,
        lifecycle_state="active",
    )
    session = _FakeSession(observation)
    monkeypatch.setattr(
        governance_service.repo,
        "get_audit_event_by_idempotency",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        governance_service.repo,
        "get_gate_decision",
        lambda *_args, **_kwargs: gate,
    )
    monkeypatch.setattr(
        governance_service.repo,
        "get_evidence_asset",
        lambda *_args, **_kwargs: evidence,
    )

    result = governance_service.MsrpSourceGovernanceService(
        session
    ).create_result_correction(
        _payload(observation.observation_id, gate.gate_decision_id, evidence),
        actor="editor@example.test",
        actor_role="editor",
        idempotency_key="result-correction-1",
    )

    assert observation.marker == "immutable"
    assert result["gateDecisionId"] == str(gate.gate_decision_id)
    assert result["gateResult"]["eligibleForLocalMaterialization"] is True
    assert result["decisionStatus"] == "submitted"


def test_result_correction_rejects_evidence_from_another_gate_target(
    monkeypatch,
) -> None:
    target_id = uuid4()
    observation = SimpleNamespace(observation_id=uuid4())
    gate = _persisted_gate_decision(observation.observation_id, target_id)
    evidence = SimpleNamespace(
        evidence_asset_id=uuid4(),
        target_id=uuid4(),
        sha256="b" * 64,
        evidence_type="official_url",
        official_domain_verified=True,
        lifecycle_state="active",
    )
    session = _FakeSession(observation)
    monkeypatch.setattr(
        governance_service.repo,
        "get_audit_event_by_idempotency",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        governance_service.repo,
        "get_gate_decision",
        lambda *_args, **_kwargs: gate,
    )
    monkeypatch.setattr(
        governance_service.repo,
        "get_evidence_asset",
        lambda *_args, **_kwargs: evidence,
    )

    with pytest.raises(HTTPException, match="not active verified official") as exc_info:
        governance_service.MsrpSourceGovernanceService(
            session
        ).create_result_correction(
            _payload(observation.observation_id, gate.gate_decision_id, evidence),
            actor="editor@example.test",
            actor_role="editor",
            idempotency_key="result-correction-cross-target",
        )

    assert exc_info.value.status_code == 422
    assert session.added == []
