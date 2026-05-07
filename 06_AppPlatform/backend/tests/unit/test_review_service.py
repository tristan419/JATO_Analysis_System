from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

from app.services import review_service


def test_list_review_cases_returns_window_metadata(
    monkeypatch,
) -> None:
    now = datetime(2026, 4, 12, 8, 30, tzinfo=timezone.utc)
    observation_id = uuid4()
    source_id = uuid4()
    review_case_id = uuid4()
    review_case = SimpleNamespace(
        review_case_id=review_case_id,
        observation_id=observation_id,
        country="瑞典",
        brand="Volvo",
        jato_model="XC60",
        jato_trim="Ultra",
        jato_powertrain="PHEV",
        official_model="XC60",
        official_trim="Ultra",
        official_edition=None,
        official_powertrain="PHEV",
        candidate_matches_json=None,
        match_confidence=Decimal("0.9100"),
        review_status="open",
        source_url="https://example.test/xc60",
        source_snapshot_path=None,
        current_assignee=None,
        created_at_utc=now,
        updated_at_utc=now,
    )
    observation = SimpleNamespace(
        observation_id=observation_id,
        source_id=source_id,
        msrp_value=Decimal("67217.39"),
        currency="EUR",
        source_msrp_value=Decimal("773000.00"),
        source_currency="SEK",
        fx_rate_to_eur=Decimal("0.08695652"),
        fx_rate_as_of_date=date(2026, 4, 12),
        fx_source="ECB",
        price_label="List price",
        observed_at_utc=now,
        source_url="https://example.test/xc60",
        source_snapshot_path=None,
        match_reason_json={"source": "unit-test"},
    )
    source = SimpleNamespace(
        source_id=source_id,
        source_code="volvo_xc60_se",
        source_url="https://example.test/source-registry",
        source_type="brand_site",
        extractor_name="scrapling",
        extractor_version="v1",
    )

    monkeypatch.setattr(
        review_service.repo,
        "count_review_cases",
        lambda *args, **kwargs: 845,
    )
    monkeypatch.setattr(
        review_service.repo,
        "list_review_cases",
        lambda *args, **kwargs: [review_case],
    )
    monkeypatch.setattr(
        review_service.msrp_repository,
        "list_observations_by_ids",
        lambda *args, **kwargs: [observation],
    )
    monkeypatch.setattr(
        review_service.msrp_repository,
        "list_sources_by_ids",
        lambda *args, **kwargs: [source],
    )

    payload = review_service.list_review_cases(
        None,
        "open",
        "Sweden",
        "Volvo",
        None,
        500,
        500,
    )

    assert payload["rows"] == 1
    assert payload["total"] == 845
    assert payload["limit"] == 500
    assert payload["offset"] == 500
    assert len(payload["items"]) == 1
    assert payload["items"][0]["reviewCaseId"] == str(review_case_id)
    assert payload["items"][0]["country"] == "Sweden"


def test_create_review_decision_persists_link_and_mismatch_category(
    monkeypatch,
) -> None:
    review_case_id = uuid4()
    observation_id = uuid4()
    source_id = uuid4()
    review_case = SimpleNamespace(
        review_case_id=review_case_id,
        observation_id=observation_id,
        review_status="open",
        current_assignee=None,
        official_model="XC60",
        official_trim="Ultra",
        official_edition=None,
        official_powertrain="PHEV",
        jato_powertrain="PHEV",
        updated_at_utc=datetime(2026, 4, 12, 8, 30, tzinfo=timezone.utc),
    )
    observation = SimpleNamespace(
        observation_id=observation_id,
        source_id=source_id,
        country="瑞典",
        brand="Volvo",
        jato_model="XC60",
        jato_trim="Ultra",
        jato_powertrain="PHEV",
        official_model="XC60",
        official_trim="Ultra",
        official_edition=None,
        official_powertrain="PHEV",
        observed_at_utc=datetime(2026, 4, 12, 10, 0, tzinfo=timezone.utc),
        match_status="review_required",
        match_reason_json={},
        updated_at_utc=datetime(2026, 4, 12, 10, 0, tzinfo=timezone.utc),
    )
    source = SimpleNamespace(
        source_id=source_id,
        source_code="volvo_xc60_se",
        source_url="https://example.test/source-registry",
        source_type="brand_site",
        extractor_name="scrapling",
        extractor_version="v1",
    )
    link = SimpleNamespace(
        link_id=uuid4(),
        country="瑞典",
        brand="Volvo",
        jato_model="XC60",
        jato_trim="Ultra",
        jato_powertrain="PHEV",
        official_model="XC60",
        official_trim="Ultra Dark",
        official_edition=None,
        official_powertrain="PHEV",
        confidence=99,
        link_source="review_decision",
        is_active=True,
        notes="[naming_mismatch] confirmed by reviewer",
        created_at_utc=datetime(2026, 4, 12, 10, 5, tzinfo=timezone.utc),
        updated_at_utc=datetime(2026, 4, 12, 10, 5, tzinfo=timezone.utc),
    )
    current_price = SimpleNamespace(
        current_price_id=uuid4(),
        effective_observation_id=observation_id,
    )
    captured = {}

    def _upsert_link(*args, **kwargs):
        captured["link_payload"] = kwargs
        return link

    monkeypatch.setattr(
        review_service.repo,
        "get_review_case",
        lambda *args, **kwargs: review_case,
    )
    monkeypatch.setattr(
        review_service.msrp_repository,
        "get_observation",
        lambda *args, **kwargs: observation,
    )
    monkeypatch.setattr(
        review_service.msrp_repository,
        "get_source",
        lambda *args, **kwargs: source,
    )
    monkeypatch.setattr(
        review_service,
        "materialize_current_price_from_observation",
        lambda *args, **kwargs: current_price,
    )
    monkeypatch.setattr(
        review_service.repo,
        "add_review_decision",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        review_service,
        "_commit_or_conflict",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        review_service,
        "upsert_jato_msrp_link",
        _upsert_link,
    )
    monkeypatch.setattr(
        review_service,
        "current_price_payload",
        lambda *args, **kwargs: {"currentPriceId": "cp-1"},
    )
    monkeypatch.setattr(
        review_service,
        "review_case_payload",
        lambda *args, **kwargs: {"reviewCaseId": str(review_case_id)},
    )
    monkeypatch.setattr(
        review_service,
        "review_decision_payload",
        lambda *args, **kwargs: {"decision": "approve"},
    )
    monkeypatch.setattr(
        review_service,
        "observation_payload",
        lambda *args, **kwargs: {
            "observationId": str(observation_id),
            "matchStatus": observation.match_status,
        },
    )
    monkeypatch.setattr(
        review_service,
        "override_payload",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        review_service,
        "jato_msrp_link_payload",
        lambda item: {"linkId": str(item.link_id), "officialTrim": item.official_trim},
    )

    payload = review_service.create_review_decision(
        SimpleNamespace(refresh=lambda *args, **kwargs: None),
        str(review_case_id),
        {
            "decision": "approve",
            "decided_by": "analyst",
            "decided_official_model": "XC60",
            "decided_official_trim": "Ultra Dark",
            "note": "Confirmed with official configurator",
            "link_confidence": 99,
        },
    )

    assert observation.match_status == "human_approved"
    assert captured["link_payload"]["official_trim"] == "Ultra Dark"
    assert captured["link_payload"]["confidence"] == 99
    assert observation.match_reason_json["mismatchCategory"] == "naming_mismatch"
    assert payload["link"]["officialTrim"] == "Ultra Dark"


def test_auto_resolve_review_cases_approves_link_backed_open_cases(
    monkeypatch,
) -> None:
    now = datetime(2026, 4, 21, 0, 30, tzinfo=timezone.utc)
    review_case_id = uuid4()
    observation_id = uuid4()
    source_id = uuid4()
    review_case = SimpleNamespace(
        review_case_id=review_case_id,
        observation_id=observation_id,
        review_status="open",
        current_assignee=None,
        official_model="XC60",
        official_trim="Ultra",
        official_edition=None,
        official_powertrain="PHEV",
        jato_powertrain="PHEV",
        updated_at_utc=now,
    )
    observation = SimpleNamespace(
        observation_id=observation_id,
        source_id=source_id,
        country="瑞典",
        brand="Volvo",
        jato_model="XC60",
        jato_trim="Ultra",
        jato_powertrain="PHEV",
        official_model="XC60",
        official_trim="Ultra",
        official_edition=None,
        official_powertrain="PHEV",
        match_status="review_required",
        match_reason_json={},
        updated_at_utc=now,
    )
    source = SimpleNamespace(
        source_id=source_id,
        source_code="volvo_xc60_se",
        source_url="https://example.test/source-registry",
        source_type="brand_site",
        extractor_name="scrapling",
        extractor_version="v1",
    )
    current_price = SimpleNamespace(
        current_price_id=uuid4(),
        effective_observation_id=observation_id,
    )
    added_decisions = []

    def _apply_mapping(_session, incoming_observation):
        incoming_observation.official_trim = "Ultra Dark"
        incoming_observation.match_status = "auto_accepted"
        return {
            "resolverKind": "jato_link",
            "linkId": "link-1",
            "overrideId": None,
        }

    monkeypatch.setattr(
        review_service.repo,
        "list_review_cases",
        lambda *args, **kwargs: [review_case],
    )
    monkeypatch.setattr(
        review_service.msrp_repository,
        "list_observations_by_ids",
        lambda *args, **kwargs: [observation],
    )
    monkeypatch.setattr(
        review_service.msrp_repository,
        "list_sources_by_ids",
        lambda *args, **kwargs: [source],
    )
    monkeypatch.setattr(
        review_service,
        "apply_canonical_mapping",
        _apply_mapping,
    )
    monkeypatch.setattr(review_service, "_utc_now", lambda: now)
    monkeypatch.setattr(
        review_service,
        "materialize_current_price_from_observation",
        lambda *args, **kwargs: current_price,
    )
    monkeypatch.setattr(
        review_service.repo,
        "add_review_decision",
        lambda _session, decision: added_decisions.append(decision),
    )
    monkeypatch.setattr(
        review_service,
        "_commit_or_conflict",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        review_service,
        "review_case_payload",
        lambda case, obs, src: {
            "reviewCaseId": str(case.review_case_id),
            "observationId": str(obs.observation_id),
            "sourceId": str(src.source_id),
        },
    )
    monkeypatch.setattr(
        review_service,
        "review_decision_payload",
        lambda decision: {
            "decision": decision.decision,
            "note": decision.note,
        },
    )
    monkeypatch.setattr(
        review_service,
        "current_price_payload",
        lambda price, src: {
            "currentPriceId": str(price.current_price_id),
            "sourceId": str(src.source_id),
        },
    )

    payload = review_service.auto_resolve_review_cases(
        None,
        {
            "decided_by": "msrp-auto-review",
            "country": "Sweden",
            "brand": "Volvo",
            "limit": 100,
        },
    )

    assert review_case.review_status == "approved"
    assert review_case.current_assignee == "msrp-auto-review"
    assert observation.match_status == "auto_accepted"
    assert observation.match_reason_json["autoReviewDecision"] == {
        "decision": "approve",
        "decidedBy": "msrp-auto-review",
        "decidedAtUtc": now.isoformat(),
        "resolverKind": "jato_link",
        "linkId": "link-1",
        "overrideId": None,
        "note": "Auto-approved via active MSRP link",
    }
    assert len(added_decisions) == 1
    assert added_decisions[0].decision == "approve"
    assert payload["candidateCases"] == 1
    assert payload["autoApprovedCount"] == 1
    assert payload["linkAppliedCount"] == 1
    assert payload["overrideAppliedCount"] == 0
    assert payload["unresolvedCount"] == 0
    assert payload["missingObservationCount"] == 0
