from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import HTTPException

from app.db.msrp_materialization_models import (
    MsrpMaterializationApproval,
    MsrpMaterializationApprovalItem,
    MsrpMaterializationExecution,
)
from app.services import (
    msrp_materialization_service as service,
    msrp_workflow_service,
)


NOW = datetime(2026, 7, 14, 8, 0, tzinfo=timezone.utc)


class FakeSession:
    def __init__(self) -> None:
        self.added: list[object] = []
        self.commits = 0
        self.rollbacks = 0
        self.flushes = 0

    def add(self, item) -> None:
        self.added.append(item)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def flush(self) -> None:
        self.flushes += 1


def _observation(country: str = "HU") -> SimpleNamespace:
    return SimpleNamespace(
        observation_id=uuid4(),
        scrape_batch_id=uuid4(),
        country=country,
        brand="KGM",
        jato_model="Korando",
        jato_trim="Style",
        jato_powertrain="ICE",
        observed_at_utc=NOW,
    )


def _gate(observation, *, eligible: bool = True) -> SimpleNamespace:
    return SimpleNamespace(
        gate_decision_id=uuid4(),
        target_id=uuid4(),
        observation_id=observation.observation_id,
        source_gate_json={"status": "pass"},
        mapping_gate_json={"status": "pass"},
        eligible_for_local_materialization=eligible,
        evaluated_at_utc=NOW + timedelta(minutes=1),
    )


def _approval(observation, gate) -> MsrpMaterializationApproval:
    return MsrpMaterializationApproval(
        approval_id=uuid4(),
        operation="materialize",
        scope_kind="observations",
        scrape_batch_id=None,
        country=observation.country,
        status="approved",
        editor_actor="human.editor",
        editor_role="editor",
        editor_identity_source="authenticated_session",
        reason="Reviewed official HU observation evidence",
        rollback_plan_ref="runbook:msrp-rollback",
        compensation_plan_ref="runbook:msrp-compensate",
        approved_at_utc=NOW,
        expires_at_utc=NOW + timedelta(hours=1),
    )


@pytest.mark.parametrize(
    "actor",
    ["msrp-cron", "airflow.service", "timer_runner", "pricing-bot"],
)
def test_service_identity_cannot_create_editor_approval(actor: str) -> None:
    with pytest.raises(HTTPException) as exc_info:
        service.create_editor_approval(
            FakeSession(),
            {},
            actor_name=actor,
            actor_role="admin",
            actor_identity_source="authenticated_session",
        )

    assert exc_info.value.status_code == 403


def test_developer_role_cannot_create_editor_approval() -> None:
    with pytest.raises(HTTPException) as exc_info:
        service.create_editor_approval(
            FakeSession(),
            {},
            actor_name="developer.user",
            actor_role="developer",
            actor_identity_source="authenticated_session",
        )

    assert exc_info.value.status_code == 403


def test_human_admin_role_is_an_explicit_operational_override() -> None:
    service._require_human_editor("human.admin", "admin")


def test_unimplemented_compensation_operation_cannot_be_approved() -> None:
    with pytest.raises(HTTPException) as exc_info:
        service.create_editor_approval(
            FakeSession(),
            {"operation": "cleanup"},
            actor_name="human.editor",
            actor_role="editor",
            actor_identity_source="authenticated_session",
        )

    assert exc_info.value.status_code == 409
    assert "compensation executor" in str(exc_info.value.detail)


def test_static_token_identity_source_cannot_create_approval() -> None:
    with pytest.raises(HTTPException) as exc_info:
        service.create_editor_approval(
            FakeSession(),
            {},
            actor_name="forged.human.editor",
            actor_role="editor",
            actor_identity_source="static_token_role_map",
        )

    assert exc_info.value.status_code == 403


def test_missing_approval_cannot_execute_materialization(monkeypatch) -> None:
    monkeypatch.setattr(
        service.approval_repo,
        "get_execution_by_idempotency_key",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        service.approval_repo,
        "get_approval",
        lambda *_args, **_kwargs: None,
    )

    with pytest.raises(HTTPException) as exc_info:
        service.execute_materialization(
            FakeSession(),
            approval_id=uuid4(),
            run_id="manual-run",
            idempotency_key="missing-approval-key",
            executed_by_actor="human.editor",
            executed_by_role="editor",
            executed_by_identity_source="authenticated_session",
            execution_context="interactive_editor",
        )

    assert exc_info.value.status_code == 404


def test_static_token_identity_source_cannot_execute_materialization() -> None:
    with pytest.raises(HTTPException) as exc_info:
        service.execute_materialization(
            FakeSession(),
            approval_id=uuid4(),
            run_id="manual-run",
            idempotency_key="static-token-execution-key",
            executed_by_actor="forged.human.editor",
            executed_by_role="editor",
            executed_by_identity_source="static_token_role_map",
            execution_context="interactive_editor",
        )

    assert exc_info.value.status_code == 403


@pytest.mark.parametrize(
    "execution_context",
    ["systemd_scheduled", "airflow_scheduled", "pipeline_wrapper"],
)
def test_scheduled_context_cannot_execute_materialization(
    execution_context: str,
) -> None:
    with pytest.raises(HTTPException) as exc_info:
        service.execute_materialization(
            FakeSession(),
            approval_id=uuid4(),
            run_id="scheduled-run",
            idempotency_key="scheduled-execution-key",
            executed_by_actor="human.editor",
            executed_by_role="editor",
            executed_by_identity_source="authenticated_session",
            execution_context=execution_context,
        )

    assert exc_info.value.status_code == 403


def test_hu_observation_without_validated_context_cannot_write_facts() -> None:
    observation = _observation("HU")

    with pytest.raises(HTTPException) as exc_info:
        msrp_workflow_service.materialize_current_price_from_observation(
            FakeSession(),
            observation,
            context=SimpleNamespace(),
            price_history_enabled=True,
        )

    assert exc_info.value.status_code == 403


def test_missing_gate_decision_blocks_editor_approval(monkeypatch) -> None:
    observation = _observation()
    monkeypatch.setattr(
        service.msrp_repo,
        "get_observation",
        lambda *_args: observation,
    )
    monkeypatch.setattr(
        service.governance_repo,
        "get_gate_decision",
        lambda *_args: None,
    )

    with pytest.raises(HTTPException) as exc_info:
        service.create_editor_approval(
            FakeSession(),
            {
                "operation": "materialize",
                "scope_kind": "observations",
                "observation_ids": [observation.observation_id],
                "gate_decision_ids": [uuid4()],
                "reason": "Reviewed official observation evidence",
                "rollback_plan_ref": "runbook:rollback",
                "compensation_plan_ref": "runbook:compensate",
            },
            actor_name="human.editor",
            actor_role="editor",
            actor_identity_source="authenticated_session",
        )

    assert exc_info.value.status_code == 409
    assert "no persisted GateDecision" in str(exc_info.value.detail)


def test_superseded_gate_decision_blocks_editor_approval(monkeypatch) -> None:
    observation = _observation()
    gate = _gate(observation)
    monkeypatch.setattr(
        service.msrp_repo,
        "get_observation",
        lambda *_args: observation,
    )
    monkeypatch.setattr(
        service.governance_repo,
        "get_gate_decision",
        lambda *_args: gate,
    )
    monkeypatch.setattr(
        service.governance_repo,
        "get_latest_gate_decision_for_target",
        lambda *_args: SimpleNamespace(gate_decision_id=uuid4()),
    )

    with pytest.raises(HTTPException) as exc_info:
        service.create_editor_approval(
            FakeSession(),
            {
                "operation": "materialize",
                "scope_kind": "observations",
                "observation_ids": [observation.observation_id],
                "gate_decision_ids": [gate.gate_decision_id],
                "reason": "Reviewed official observation evidence",
                "rollback_plan_ref": "runbook:rollback",
                "compensation_plan_ref": "runbook:compensate",
            },
            actor_name="human.editor",
            actor_role="editor",
            actor_identity_source="authenticated_session",
        )

    assert exc_info.value.status_code == 409
    assert "stale or superseded" in str(exc_info.value.detail)


def test_editor_approved_scope_materializes_once_and_is_idempotent(
    monkeypatch,
) -> None:
    observation = _observation()
    gate = _gate(observation)
    approval = _approval(observation, gate)
    item = MsrpMaterializationApprovalItem(
        approval_item_id=uuid4(),
        approval_id=approval.approval_id,
        observation_id=observation.observation_id,
        gate_decision_id=gate.gate_decision_id,
        ordinal=0,
    )
    session = FakeSession()
    state: dict[str, MsrpMaterializationExecution | None] = {
        "execution": None
    }
    materialized_contexts = []

    monkeypatch.setattr(service, "_utc_now", lambda: NOW)
    monkeypatch.setattr(
        service.approval_repo,
        "get_execution_by_idempotency_key",
        lambda _session, key: (
            state["execution"]
            if state["execution"] is not None
            and state["execution"].idempotency_key == key
            else None
        ),
    )
    monkeypatch.setattr(
        service.approval_repo,
        "get_approval",
        lambda *_args, **_kwargs: approval,
    )
    monkeypatch.setattr(
        service.approval_repo,
        "list_approval_items",
        lambda *_args: [item],
    )
    monkeypatch.setattr(
        service,
        "_validated_execution_scope",
        lambda *_args: ([observation], [gate]),
    )
    monkeypatch.setattr(
        service,
        "_fact_refs",
        lambda *_args: [
            {
                "observationId": str(observation.observation_id),
                "currentPriceId": "fact-ref",
            }
        ],
    )

    def _add(_session, item_to_add):
        session.add(item_to_add)
        if isinstance(item_to_add, MsrpMaterializationExecution):
            state["execution"] = item_to_add
        return item_to_add

    monkeypatch.setattr(service.approval_repo, "add", _add)
    monkeypatch.setattr(
        service.approval_repo,
        "get_execution",
        lambda *_args, **_kwargs: state["execution"],
    )

    def _materialize(_session, incoming, *, context, price_history_enabled):
        context.require_observation(incoming.observation_id, "materialize")
        materialized_contexts.append(context)
        return SimpleNamespace(current_price_id=uuid4())

    monkeypatch.setattr(
        msrp_workflow_service,
        "materialize_current_price_from_observation",
        _materialize,
    )
    monkeypatch.setattr(
        service.msrp_repo,
        "has_price_history_table",
        lambda *_args: True,
    )

    first = service.execute_materialization(
        session,
        approval_id=approval.approval_id,
        run_id="manual-hu-approval-run",
        idempotency_key="hu-observation-materialization-001",
        executed_by_actor="human.executor",
        executed_by_role="editor",
        executed_by_identity_source="authenticated_session",
        execution_context="interactive_editor",
    )
    second = service.execute_materialization(
        session,
        approval_id=approval.approval_id,
        run_id="manual-hu-approval-run",
        idempotency_key="hu-observation-materialization-001",
        executed_by_actor="human.executor",
        executed_by_role="editor",
        executed_by_identity_source="authenticated_session",
        execution_context="interactive_editor",
    )

    assert first["status"] == "succeeded"
    assert first["materializedKeys"] == 1
    assert second["executionId"] == first["executionId"]
    assert len(materialized_contexts) == 1
    assert approval.status == "consumed"
    assert state["execution"] is not None
    assert state["execution"].before_fact_refs_json
    assert state["execution"].after_fact_refs_json
    assert state["execution"].rollback_ref
    assert state["execution"].compensation_ref == approval.compensation_plan_ref
    assert state["execution"].executed_by_actor == "human.executor"

    with pytest.raises(HTTPException) as exc_info:
        service.execute_materialization(
            session,
            approval_id=approval.approval_id,
            run_id="different-run",
            idempotency_key="hu-observation-materialization-001",
            executed_by_actor="human.executor",
            executed_by_role="editor",
            executed_by_identity_source="authenticated_session",
            execution_context="interactive_editor",
        )
    assert exc_info.value.status_code == 409


def test_crash_after_reservation_leaves_approval_fail_closed(
    monkeypatch,
) -> None:
    observation = _observation()
    gate = _gate(observation)
    approval = _approval(observation, gate)
    item = MsrpMaterializationApprovalItem(
        approval_item_id=uuid4(),
        approval_id=approval.approval_id,
        observation_id=observation.observation_id,
        gate_decision_id=gate.gate_decision_id,
        ordinal=0,
    )
    session = FakeSession()
    state: dict[str, MsrpMaterializationExecution | None] = {
        "execution": None
    }

    monkeypatch.setattr(service, "_utc_now", lambda: NOW)
    monkeypatch.setattr(
        service.approval_repo,
        "get_execution_by_idempotency_key",
        lambda _session, key: (
            state["execution"]
            if state["execution"] is not None
            and state["execution"].idempotency_key == key
            else None
        ),
    )
    monkeypatch.setattr(
        service.approval_repo,
        "get_approval",
        lambda *_args, **_kwargs: approval,
    )
    monkeypatch.setattr(
        service.approval_repo,
        "list_approval_items",
        lambda *_args: [item],
    )
    monkeypatch.setattr(
        service,
        "_validated_execution_scope",
        lambda *_args: ([observation], [gate]),
    )
    monkeypatch.setattr(service, "_fact_refs", lambda *_args: [])

    def _add(_session, item_to_add):
        if isinstance(item_to_add, MsrpMaterializationExecution):
            state["execution"] = item_to_add
        return item_to_add

    monkeypatch.setattr(service.approval_repo, "add", _add)
    monkeypatch.setattr(
        service.msrp_repo,
        "has_price_history_table",
        lambda *_args: True,
    )
    monkeypatch.setattr(
        msrp_workflow_service,
        "materialize_current_price_from_observation",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(SystemExit("crash")),
    )

    with pytest.raises(SystemExit):
        service.execute_materialization(
            session,
            approval_id=approval.approval_id,
            run_id="crash-run",
            idempotency_key="crash-reservation-001",
            executed_by_actor="human.executor",
            executed_by_role="editor",
            executed_by_identity_source="authenticated_session",
            execution_context="interactive_editor",
        )

    assert approval.status == "executing"
    assert approval.reserved_at_utc == NOW
    assert state["execution"] is not None
    assert state["execution"].status == "running"

    with pytest.raises(HTTPException) as exc_info:
        service.execute_materialization(
            session,
            approval_id=approval.approval_id,
            run_id="second-run",
            idempotency_key="different-key-after-crash",
            executed_by_actor="human.executor",
            executed_by_role="editor",
            executed_by_identity_source="authenticated_session",
            execution_context="interactive_editor",
        )
    assert exc_info.value.status_code == 409


def test_naive_gate_timestamp_is_compared_as_utc(monkeypatch) -> None:
    observation = _observation()
    observation.observed_at_utc = NOW.replace(tzinfo=None)
    gate = _gate(observation)
    gate.evaluated_at_utc = (NOW + timedelta(minutes=1)).replace(tzinfo=None)
    monkeypatch.setattr(
        service.governance_repo,
        "get_latest_gate_decision_for_target",
        lambda *_args: gate,
    )
    monkeypatch.setattr(
        service.governance_repo,
        "get_target",
        lambda *_args: SimpleNamespace(country="HU", brand="KGM"),
    )

    assert (
        service._require_current_gate_decision(
            FakeSession(),
            observation,
            gate,
            operation="materialize",
        )
        is gate
    )


def test_fact_refs_capture_reversible_current_and_history_state(monkeypatch) -> None:
    observation = _observation()
    previous_observation_id = uuid4()
    current_price = SimpleNamespace(
        current_price_id=uuid4(),
        effective_observation_id=previous_observation_id,
        source_msrp_value=12345,
        source_currency="HUF",
        last_price_change_at_utc=NOW - timedelta(days=1),
    )
    open_period = SimpleNamespace(
        price_history_id=uuid4(),
        valid_from_utc=NOW - timedelta(days=10),
        valid_to_utc=None,
        last_confirmed_at_utc=NOW - timedelta(days=1),
        msrp_value=31.5,
        currency="EUR",
        source_msrp_value=12345,
        source_currency="HUF",
        started_by_observation_id=previous_observation_id,
        ended_by_observation_id=None,
        last_confirmed_by_observation_id=previous_observation_id,
    )
    monkeypatch.setattr(
        service.msrp_repo,
        "get_current_price_by_key",
        lambda *_args: current_price,
    )
    monkeypatch.setattr(
        service.msrp_repo,
        "has_price_history_table",
        lambda *_args: True,
    )
    monkeypatch.setattr(
        service.msrp_repo,
        "get_open_price_period",
        lambda *_args: open_period,
    )

    ref = service._fact_refs(FakeSession(), [observation])[0]

    assert ref["businessKey"] == {
        "country": "HU",
        "brand": "KGM",
        "jatoModel": "Korando",
        "jatoTrim": "Style",
        "jatoPowertrain": "ICE",
    }
    assert ref["effectiveObservationId"] == str(previous_observation_id)
    assert ref["priceHistoryValidFromUtc"] == open_period.valid_from_utc.isoformat()
    assert ref["currentPriceValue"] == "12345"
    assert ref["priceHistoryValue"] == "31.5"
    assert ref["priceHistorySourceValue"] == "12345"
    assert ref["priceHistorySourceCurrency"] == "HUF"
    assert ref["priceHistoryStartedByObservationId"] == str(
        previous_observation_id
    )
    assert ref["priceHistoryLastConfirmedByObservationId"] == str(
        previous_observation_id
    )
