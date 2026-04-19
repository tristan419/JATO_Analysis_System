from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError

from app.db.models import CurrentPrice, MsrpObservation
from app.services import msrp_workflow_service


def _make_observation() -> MsrpObservation:
    now = datetime(2026, 4, 11, 9, 10, tzinfo=timezone.utc)
    return MsrpObservation(
        observation_id=uuid4(),
        scrape_batch_id=uuid4(),
        source_id=uuid4(),
        country="瑞典",
        brand="Volvo",
        jato_model="XC60",
        jato_trim="Ultra",
        jato_powertrain="PHEV",
        official_model="XC60 Recharge",
        official_trim="Ultra",
        official_edition=None,
        official_powertrain="PHEV",
        msrp_value=Decimal("67217.39"),
        currency="EUR",
        source_msrp_value=Decimal("773000.00"),
        source_currency="SEK",
        fx_rate_to_eur=Decimal("0.08695652"),
        fx_rate_as_of_date=date(2026, 4, 11),
        fx_source="static-fallback",
        tax_included=True,
        price_label="List price",
        availability_text=None,
        observed_at_utc=now,
        source_url="https://example.test/xc60",
        source_snapshot_path=None,
        source_payload_hash="hash-1",
        extraction_version="v1",
        match_confidence=Decimal("0.9100"),
        match_status="auto_accepted",
        match_reason_json={"source": "unit-test"},
        created_at_utc=now,
        updated_at_utc=now,
    )


def _make_current_price(observation: MsrpObservation) -> CurrentPrice:
    return CurrentPrice(
        current_price_id=uuid4(),
        country=observation.country,
        brand=observation.brand,
        jato_model=observation.jato_model,
        jato_trim=observation.jato_trim,
        jato_powertrain=observation.jato_powertrain,
        official_model="XC60",
        official_trim=observation.official_trim,
        official_edition=None,
        official_powertrain=observation.official_powertrain,
        effective_observation_id=uuid4(),
        current_msrp_value=Decimal("67217.39"),
        currency=observation.currency,
        source_msrp_value=Decimal("773000.00"),
        source_currency=observation.source_currency,
        fx_rate_to_eur=Decimal("0.08695652"),
        fx_rate_as_of_date=observation.fx_rate_as_of_date,
        fx_source=observation.fx_source,
        tax_included=True,
        match_confidence=Decimal("0.9100"),
        match_status="auto_accepted",
        source_url=observation.source_url,
        source_snapshot_path=None,
        last_price_change_at_utc=observation.observed_at_utc,
        updated_at_utc=observation.updated_at_utc,
    )


def test_list_price_history_returns_empty_when_table_is_missing(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "has_price_history_table",
        lambda session: False,
    )

    def _unexpected_query(*args, **kwargs):
        raise AssertionError(
            "list_price_history should not query a missing table"
        )

    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "list_price_history",
        _unexpected_query,
    )

    payload = msrp_workflow_service.list_price_history(
        None,
        "Sweden",
        "Volvo",
        "XC60",
        "Ultra",
        "PHEV",
        10,
    )

    assert payload == {
        "rows": 0,
        "items": [],
        "warning": "price_history_unavailable",
    }


def test_list_current_prices_returns_window_metadata_and_alert_count(
    monkeypatch,
) -> None:
    observation = _make_observation()
    current_price = _make_current_price(observation)

    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "count_current_prices",
        lambda *args, **kwargs: 612,
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "list_current_prices",
        lambda *args, **kwargs: [current_price],
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "list_observations_by_ids",
        lambda *args, **kwargs: [
            SimpleNamespace(
                observation_id=current_price.effective_observation_id,
                source_id=observation.source_id,
            )
        ],
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "list_sources_by_ids",
        lambda *args, **kwargs: [
            SimpleNamespace(
                source_id=observation.source_id,
                source_code="evkx_us_catalog",
                source_type="reference_catalog",
                extractor_name="evkx_catalog",
                extractor_version="v1",
            )
        ],
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "count_current_price_alerts",
        lambda *args, **kwargs: 17,
    )

    payload = msrp_workflow_service.list_current_prices(
        None,
        "Sweden",
        "Volvo",
        "XC60",
        500,
        500,
    )

    assert payload["rows"] == 1
    assert payload["total"] == 612
    assert payload["limit"] == 500
    assert payload["offset"] == 500
    assert payload["priceAlertCount"] == 17
    assert len(payload["items"]) == 1
    assert payload["items"][0]["country"] == "Sweden"
    assert payload["items"][0]["sourceCode"] == "evkx_us_catalog"
    assert payload["items"][0]["sourceType"] == "reference_catalog"


def test_materialize_current_price_backfills_open_period_when_history_is_empty(
    monkeypatch,
) -> None:
    observation = _make_observation()
    current_price = _make_current_price(observation)
    recorded: list[object] = []

    monkeypatch.setattr(
        msrp_workflow_service,
        "apply_canonical_mapping",
        lambda *args, **kwargs: {"resolverKind": "observation_payload"},
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "get_current_price_by_key",
        lambda *args, **kwargs: current_price,
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "get_open_price_period",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        msrp_workflow_service,
        "_record_price_period",
        lambda session, incoming_observation, **kwargs: recorded.append(
            incoming_observation.observation_id
        ),
    )

    result = msrp_workflow_service.materialize_current_price_from_observation(
        None,
        observation,
        price_history_enabled=True,
    )

    assert result is current_price
    assert recorded == [observation.observation_id]


def test_materialize_current_price_applies_canonical_mapping_before_update(
    monkeypatch,
) -> None:
    observation = _make_observation()
    current_price = _make_current_price(observation)

    def _apply_mapping(_session, incoming_observation):
        incoming_observation.official_model = "XC60"
        incoming_observation.official_trim = "Ultra Dark"
        incoming_observation.match_status = "override_applied"
        return {"resolverKind": "match_override"}

    monkeypatch.setattr(
        msrp_workflow_service,
        "apply_canonical_mapping",
        _apply_mapping,
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "get_current_price_by_key",
        lambda *args, **kwargs: current_price,
    )

    result = msrp_workflow_service.materialize_current_price_from_observation(
        None,
        observation,
        price_history_enabled=False,
    )

    assert result is current_price
    assert current_price.official_model == "XC60"
    assert current_price.official_trim == "Ultra Dark"
    assert current_price.match_status == "override_applied"


def test_refreshes_open_period_when_price_is_unchanged(
    monkeypatch,
) -> None:
    observation = _make_observation()
    current_price = _make_current_price(observation)
    open_period = SimpleNamespace(
        last_confirmed_at_utc=observation.observed_at_utc.replace(day=10),
        last_confirmed_by_observation_id=uuid4(),
    )
    recorded: list[object] = []

    monkeypatch.setattr(
        msrp_workflow_service,
        "apply_canonical_mapping",
        lambda *args, **kwargs: {"resolverKind": "observation_payload"},
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "get_current_price_by_key",
        lambda *args, **kwargs: current_price,
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "get_open_price_period",
        lambda *args, **kwargs: open_period,
    )
    monkeypatch.setattr(
        msrp_workflow_service,
        "_record_price_period",
        lambda *args, **kwargs: recorded.append("called"),
    )

    result = msrp_workflow_service.materialize_current_price_from_observation(
        None,
        observation,
        price_history_enabled=True,
    )

    assert result is current_price
    assert recorded == []
    assert open_period.last_confirmed_at_utc == observation.observed_at_utc
    assert (
        open_period.last_confirmed_by_observation_id
        == observation.observation_id
    )


def test_commit_or_conflict_flushes_when_commit_is_disabled() -> None:
    calls: list[str] = []
    session = SimpleNamespace(
        commit=lambda: calls.append("commit"),
        flush=lambda: calls.append("flush"),
        rollback=lambda: calls.append("rollback"),
    )

    msrp_workflow_service._commit_or_conflict(
        session,
        "detail",
        commit=False,
    )

    assert calls == ["flush"]


def test_commit_or_conflict_rolls_back_integrity_errors() -> None:
    calls: list[str] = []

    def _raise_integrity_error():
        calls.append("commit")
        raise IntegrityError("stmt", "params", Exception("boom"))

    session = SimpleNamespace(
        commit=_raise_integrity_error,
        flush=lambda: calls.append("flush"),
        rollback=lambda: calls.append("rollback"),
    )

    try:
        msrp_workflow_service._commit_or_conflict(session, "detail")
    except HTTPException as exc:
        assert exc.status_code == 409
        assert exc.detail == "detail"
    else:
        raise AssertionError("Expected HTTPException")

    assert calls == ["commit", "rollback"]


def test_remap_current_price_reopens_review_and_removes_current_price(
    monkeypatch,
) -> None:
    now = datetime(2026, 4, 12, 8, 30, tzinfo=timezone.utc)
    observation = _make_observation()
    current_price = _make_current_price(observation)
    current_price.effective_observation_id = observation.observation_id
    review_case = SimpleNamespace(
        review_case_id=uuid4(),
        candidate_matches_json=[{"source": "existing-review"}],
        review_status="approved",
        current_assignee="analyst-1",
        updated_at_utc=observation.updated_at_utc,
    )
    open_period = SimpleNamespace(
        valid_to_utc=None,
        ended_by_observation_id=uuid4(),
    )
    deleted_current_prices: list[object] = []
    added_decisions: list[object] = []
    refreshed: list[object] = []
    ensured_calls: list[tuple[object, object, object]] = []

    monkeypatch.setattr(msrp_workflow_service, "_utc_now", lambda: now)
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "get_current_price",
        lambda *args, **kwargs: current_price,
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "get_observation",
        lambda *args, **kwargs: observation,
    )
    monkeypatch.setattr(
        msrp_workflow_service.review_repo,
        "get_review_case_by_observation",
        lambda *args, **kwargs: review_case,
    )

    def _ensure_review_case(
        session,
        incoming_observation,
        candidate_matches_json,
    ):
        ensured_calls.append(
            (session, incoming_observation, candidate_matches_json)
        )
        return review_case

    monkeypatch.setattr(
        msrp_workflow_service,
        "_ensure_review_case",
        _ensure_review_case,
    )
    monkeypatch.setattr(
        msrp_workflow_service,
        "_retire_active_overrides",
        lambda *args, **kwargs: 2,
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "has_price_history_table",
        lambda *args, **kwargs: True,
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "get_open_price_period",
        lambda *args, **kwargs: open_period,
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "delete_current_price",
        lambda session, price: deleted_current_prices.append(price),
    )
    monkeypatch.setattr(
        msrp_workflow_service.review_repo,
        "add_review_decision",
        lambda session, decision: added_decisions.append(decision),
    )
    monkeypatch.setattr(
        msrp_workflow_service,
        "_commit_or_conflict",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "get_source",
        lambda *args, **kwargs: SimpleNamespace(
            source_id=observation.source_id
        ),
    )
    monkeypatch.setattr(
        msrp_workflow_service,
        "review_case_payload",
        lambda case, obs, source: {
            "reviewCaseId": str(case.review_case_id),
            "observationId": str(obs.observation_id),
            "sourceId": str(source.source_id),
        },
    )
    monkeypatch.setattr(
        msrp_workflow_service,
        "review_decision_payload",
        lambda decision: {
            "reviewDecisionId": str(decision.review_case_id),
            "decision": decision.decision,
            "note": decision.note,
        },
    )

    session = SimpleNamespace(refresh=lambda obj: refreshed.append(obj))

    payload = msrp_workflow_service.remap_current_price(
        session,
        str(current_price.current_price_id),
        {
            "decided_by": "tester",
            "note": "source price looks wrong",
        },
    )

    assert ensured_calls == [
        (session, observation, review_case.candidate_matches_json)
    ]
    assert review_case.review_status == "open"
    assert review_case.current_assignee is None
    assert review_case.updated_at_utc == now
    assert observation.match_status == "review_required"
    assert observation.updated_at_utc == now
    assert observation.match_reason_json["returnedFromCurrentPrice"] == {
        "currentPriceId": str(current_price.current_price_id),
        "returnedBy": "tester",
        "returnedAtUtc": now.isoformat(),
        "note": "source price looks wrong",
    }
    assert open_period.valid_to_utc == now
    assert open_period.ended_by_observation_id is None
    assert deleted_current_prices == [current_price]
    assert len(added_decisions) == 1
    assert added_decisions[0].decision == "reopen"
    assert added_decisions[0].note == "source price looks wrong"
    assert payload == {
        "currentPriceId": str(current_price.current_price_id),
        "observationId": str(observation.observation_id),
        "reviewCase": {
            "reviewCaseId": str(review_case.review_case_id),
            "observationId": str(observation.observation_id),
            "sourceId": str(observation.source_id),
        },
        "decision": {
            "reviewDecisionId": str(review_case.review_case_id),
            "decision": "reopen",
            "note": "source price looks wrong",
        },
        "overridesRetired": 2,
        "removedFromCurrentPrices": True,
    }
    assert refreshed == [review_case, added_decisions[0]]
