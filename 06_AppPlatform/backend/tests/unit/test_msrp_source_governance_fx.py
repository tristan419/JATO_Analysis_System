from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

from app.api.msrp_source_governance_schemas import (
    FxGateInput,
    FxNormalizationCreate,
    MappingGateInput,
    SourceGateInput,
)
from app.db.models import MsrpObservation
from app.db.msrp_source_governance_models import (
    MsrpFxNormalizationRun,
    MsrpGovernanceGateDecision,
)
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

    def flush(self) -> None:
        now = datetime(2026, 7, 14, 5, 0, tzinfo=timezone.utc)
        for item in self.added:
            if hasattr(item, "created_at_utc") and getattr(
                item, "created_at_utc", None
            ) is None:
                item.created_at_utc = now

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def refresh(self, _item) -> None:
        pass


def _gate_decision(observation_id):
    target_id = uuid4()
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
        fx_gate=FxGateInput(
            policy_version="fx-v1",
            provider_approved=True,
            rate_positive=True,
            effective_date_valid=True,
            freshness_valid=True,
            local_value_unchanged=True,
        ),
    )
    return MsrpGovernanceGateDecision(
        gate_decision_id=uuid4(),
        schema_version=decision.schema_version,
        target_id=target_id,
        observation_id=observation_id,
        source_gate_json=decision.source_gate.model_dump(mode="json", by_alias=True),
        mapping_gate_json=decision.mapping_gate.model_dump(mode="json", by_alias=True),
        fx_gate_json=decision.fx_gate.model_dump(mode="json", by_alias=True),
        eligible_for_local_materialization=True,
        eligible_for_normalized_materialization=True,
        evaluated_at_utc=decision.evaluated_at,
        created_by="governance-worker",
        created_at_utc=decision.evaluated_at,
    )


def test_fx_repair_creates_derived_run_without_mutating_local_observation(
    monkeypatch,
) -> None:
    observation = SimpleNamespace(
        observation_id=uuid4(),
        source_currency="SEK",
        source_msrp_value=Decimal("773000.00"),
    )
    original_currency = observation.source_currency
    original_value = observation.source_msrp_value
    gate_decision = _gate_decision(observation.observation_id)
    session = _FakeSession(observation)
    state: dict[str, MsrpFxNormalizationRun | None] = {"run": None}

    def add(_session, item):
        session.added.append(item)
        if isinstance(item, MsrpFxNormalizationRun):
            state["run"] = item
        return item

    monkeypatch.setattr(governance_service.repo, "add", add)
    monkeypatch.setattr(
        governance_service.repo,
        "get_audit_event_by_idempotency",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        governance_service.repo,
        "get_gate_decision",
        lambda *_args, **_kwargs: gate_decision,
    )

    payload = governance_service.MsrpSourceGovernanceService(session).create_fx_run(
        FxNormalizationCreate(
            observation_id=observation.observation_id,
            gate_decision_id=gate_decision.gate_decision_id,
            local_currency="SEK",
            local_value=Decimal("773000.00"),
            fx_provider="ecb-approved-adapter",
            rate_to_normalized=Decimal("0.0875"),
            rate_effective_date=date(2026, 7, 13),
            rate_retrieved_at_utc=datetime(
                2026,
                7,
                14,
                2,
                0,
                tzinfo=timezone.utc,
            ),
            policy_version="fx-v1",
        ),
        actor="editor@example.test",
        actor_role="editor",
        idempotency_key="fx-create-1",
    )

    assert observation.source_currency == original_currency
    assert observation.source_msrp_value == original_value
    assert payload["localValue"] == "773000.00"
    assert payload["normalizedValue"] == "67637.50"
    assert payload["runStatus"] == "validated"
    assert payload["gateDecisionId"] == str(gate_decision.gate_decision_id)
    assert state["run"] is not None
