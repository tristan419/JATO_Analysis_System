from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import uuid4

from app.db.models import CurrentPrice, MsrpObservation, MsrpSource, PriceHistory
from app.services.payload_serializers import (
    current_price_payload,
    observation_payload,
    price_history_payload,
)


def test_current_price_payload_uses_current_price_contract() -> None:
    last_change = datetime(2026, 4, 10, 8, 0, tzinfo=timezone.utc)
    updated_at = datetime(2026, 4, 10, 9, 0, tzinfo=timezone.utc)

    current_price = CurrentPrice(
        current_price_id=uuid4(),
        country="瑞典",
        brand="Volvo",
        jato_model="XC60",
        jato_trim="Ultra",
        jato_powertrain="PHEV",
        official_model="XC60",
        official_trim="Ultra",
        official_edition="Black Edition",
        official_powertrain="PHEV",
        effective_observation_id=uuid4(),
        current_msrp_value=Decimal("529900.00"),
        currency="EUR",
        source_msrp_value=Decimal("569900.00"),
        source_currency="SEK",
        fx_rate_to_eur=Decimal("0.09281000"),
        fx_rate_as_of_date=date(2026, 4, 10),
        fx_source="frankfurter",
        tax_included=True,
        match_confidence=Decimal("0.9600"),
        match_status="matched",
        source_url="https://example.test/xc60",
        source_snapshot_path=None,
        last_price_change_at_utc=last_change,
        updated_at_utc=updated_at,
    )

    payload = current_price_payload(current_price)

    assert payload["currentMsrpValue"] == 529900.0
    assert payload["country"] == "Sweden"
    assert payload["sourceMsrpValue"] == 569900.0
    assert payload["sourceCurrency"] == "SEK"
    assert payload["jatoPowertrain"] == "PHEV"
    assert payload["officialEdition"] == "Black Edition"
    assert payload["officialPowertrain"] == "PHEV"
    assert payload["lastPriceChangeAtUtc"] == last_change.isoformat()
    assert payload["updatedAtUtc"] == updated_at.isoformat()
    assert "msrpValue" not in payload
    assert "observedAtUtc" not in payload
    assert "materializedAt" not in payload


def test_current_price_payload_can_include_source_metadata() -> None:
    now = datetime(2026, 4, 10, 9, 0, tzinfo=timezone.utc)
    current_price = CurrentPrice(
        current_price_id=uuid4(),
        country="美国",
        brand="Tesla",
        jato_model="MODEL Y",
        jato_trim="Long Range",
        jato_powertrain="BEV",
        official_model="Model Y",
        official_trim="Long Range",
        official_edition=None,
        official_powertrain="BEV AWD",
        effective_observation_id=uuid4(),
        current_msrp_value=Decimal("46779.82"),
        currency="EUR",
        source_msrp_value=Decimal("50990.00"),
        source_currency="USD",
        fx_rate_to_eur=Decimal("0.91743119"),
        fx_rate_as_of_date=date(2026, 4, 17),
        fx_source="static-fallback",
        tax_included=False,
        match_confidence=Decimal("0.8800"),
        match_status="human_approved",
        source_url="https://evkx.net/vehicle/tesla/model-y",
        source_snapshot_path=None,
        last_price_change_at_utc=now,
        updated_at_utc=now,
    )
    source = MsrpSource(
        source_id=uuid4(),
        source_code="evkx_us_catalog",
        country="美国",
        brand="Tesla",
        source_url="https://evkx.net/evsearch",
        source_type="reference_catalog",
        extractor_name="evkx_catalog",
        extractor_version="v1",
        price_semantics="base_msrp",
        requires_location=False,
        enabled=True,
        notes=None,
        created_at_utc=now,
        updated_at_utc=now,
    )

    payload = current_price_payload(current_price, source)

    assert payload["country"] == "United States"
    assert payload["sourceCode"] == "evkx_us_catalog"
    assert payload["sourceType"] == "reference_catalog"
    assert payload["extractorName"] == "evkx_catalog"
    assert payload["extractorVersion"] == "v1"


def test_observation_payload_keeps_observation_contract() -> None:
    observed_at = datetime(2026, 4, 10, 8, 0, tzinfo=timezone.utc)
    updated_at = datetime(2026, 4, 10, 9, 0, tzinfo=timezone.utc)

    observation = MsrpObservation(
        observation_id=uuid4(),
        scrape_batch_id=uuid4(),
        source_id=uuid4(),
        country="瑞典",
        brand="Volvo",
        jato_model="XC60",
        jato_trim="Ultra",
        jato_powertrain="PHEV",
        official_model="XC60",
        official_trim="Ultra",
        official_edition="Black Edition",
        official_powertrain="PHEV",
        msrp_value=Decimal("529900.00"),
        currency="EUR",
        source_msrp_value=Decimal("569900.00"),
        source_currency="SEK",
        fx_rate_to_eur=Decimal("0.09281000"),
        fx_rate_as_of_date=date(2026, 4, 10),
        fx_source="frankfurter",
        tax_included=True,
        price_label="List price",
        availability_text=None,
        observed_at_utc=observed_at,
        source_url="https://example.test/xc60",
        source_snapshot_path=None,
        source_payload_hash="hash-1",
        extraction_version="v1",
        match_confidence=Decimal("0.9600"),
        match_status="matched",
        match_reason_json={"source": "unit-test"},
        source_context_json={"source": "EVKX", "evId": "07828991"},
        created_at_utc=observed_at,
        updated_at_utc=updated_at,
    )

    payload = observation_payload(observation)

    assert payload["msrpValue"] == 529900.0
    assert payload["country"] == "Sweden"
    assert payload["sourceMsrpValue"] == 569900.0
    assert payload["sourceCurrency"] == "SEK"
    assert payload["jatoPowertrain"] == "PHEV"
    assert payload["officialEdition"] == "Black Edition"
    assert payload["officialPowertrain"] == "PHEV"
    assert payload["observedAtUtc"] == observed_at.isoformat()
    assert payload["updatedAtUtc"] == updated_at.isoformat()
    assert payload["sourceContext"] == {"source": "EVKX", "evId": "07828991"}
    assert "currentMsrpValue" not in payload
    assert "lastPriceChangeAtUtc" not in payload


def test_price_history_payload_exposes_last_confirmation_fields() -> None:
    now = datetime(2026, 4, 10, 8, 0, tzinfo=timezone.utc)
    confirmed_at = datetime(2026, 4, 11, 8, 0, tzinfo=timezone.utc)
    price_history = PriceHistory(
        price_history_id=uuid4(),
        country="瑞典",
        brand="Volvo",
        jato_model="XC60",
        jato_trim="Ultra",
        jato_powertrain="PHEV",
        msrp_value=Decimal("529900.00"),
        currency="EUR",
        source_msrp_value=Decimal("569900.00"),
        source_currency="SEK",
        valid_from_utc=now,
        valid_to_utc=None,
        last_confirmed_at_utc=confirmed_at,
        started_by_observation_id=uuid4(),
        ended_by_observation_id=None,
        last_confirmed_by_observation_id=uuid4(),
        created_at_utc=confirmed_at,
    )

    payload = price_history_payload(price_history)

    assert payload["country"] == "Sweden"
    assert payload["jatoPowertrain"] == "PHEV"
    assert payload["lastConfirmedAtUtc"] == confirmed_at.isoformat()
    assert payload["lastConfirmedByObservationId"] == str(
        price_history.last_confirmed_by_observation_id
    )


# ── review_case_payload ──────────────────────────


def test_review_case_payload_contract() -> None:
    from app.db.models import MsrpSource, ReviewCase
    from app.services.payload_serializers import review_case_payload

    now = datetime(2026, 4, 11, 10, 0, tzinfo=timezone.utc)
    rc = ReviewCase(
        review_case_id=uuid4(),
        observation_id=uuid4(),
        country="德国",
        brand="BMW",
        jato_model="X5",
        jato_trim="xDrive40i",
        jato_powertrain="PHEV",
        official_model="X5",
        official_trim="xDrive40i",
        official_edition="M Sport Edition",
        official_powertrain="PHEV",
        candidate_matches_json=[{"model": "X5", "score": 0.98}],
        match_confidence=Decimal("0.9800"),
        review_status="open",
        source_url="https://example.test/x5",
        source_snapshot_path=None,
        current_assignee="analyst-1",
        created_at_utc=now,
        updated_at_utc=now,
    )
    source = MsrpSource(
        source_id=uuid4(),
        source_code="DE-BMW-OFFICIAL",
        country="德国",
        brand="BMW",
        source_url="https://www.bmw.de/configure",
        source_type="official_configurator",
        extractor_name="bmw_de_extractor",
        extractor_version="v2",
        price_semantics="msrp_incl_vat",
        requires_location=False,
        enabled=True,
        notes=None,
        created_at_utc=now,
        updated_at_utc=now,
    )
    observation = MsrpObservation(
        observation_id=rc.observation_id,
        scrape_batch_id=uuid4(),
        source_id=source.source_id,
        country="德国",
        brand="BMW",
        jato_model="X5",
        jato_trim="xDrive40i",
        jato_powertrain="PHEV",
        official_model="X5",
        official_trim="xDrive40i",
        official_edition="M Sport Edition",
        official_powertrain="PHEV",
        msrp_value=Decimal("72000.00"),
        currency="EUR",
        source_msrp_value=Decimal("72000.00"),
        source_currency="EUR",
        fx_rate_to_eur=Decimal("1.00000000"),
        fx_rate_as_of_date=now.date(),
        fx_source="frankfurter",
        tax_included=True,
        price_label="List price",
        availability_text=None,
        observed_at_utc=now,
        source_url="https://example.test/x5",
        source_snapshot_path=None,
        source_payload_hash="hash-review-case",
        extraction_version="v2",
        match_confidence=Decimal("0.9800"),
        match_status="matched",
        match_reason_json={"strategy": "unit-test"},
        created_at_utc=now,
        updated_at_utc=now,
    )
    payload = review_case_payload(rc, obs=observation, source=source)

    # canonical fields
    assert "reviewCaseId" in payload
    assert "observationId" in payload
    assert payload["country"] == "Germany"
    assert payload["brand"] == "BMW"
    assert payload["sourceCode"] == "DE-BMW-OFFICIAL"
    assert payload["sourceRegistryUrl"] == "https://www.bmw.de/configure"
    assert payload["extractorName"] == "bmw_de_extractor"
    assert payload["extractorVersion"] == "v2"
    assert payload["jatoModel"] == "X5"
    assert payload["jatoTrim"] == "xDrive40i"
    assert payload["jatoPowertrain"] == "PHEV"
    assert payload["officialModel"] == "X5"
    assert payload["officialTrim"] == "xDrive40i"
    assert payload["officialEdition"] == "M Sport Edition"
    assert payload["officialPowertrain"] == "PHEV"
    assert payload["matchConfidence"] == 0.98
    assert payload["reviewStatus"] == "open"
    assert payload["sourceUrl"] == "https://example.test/x5"
    assert payload["currentAssignee"] == "analyst-1"
    assert payload["createdAtUtc"] == now.isoformat()
    assert payload["updatedAtUtc"] == now.isoformat()
    assert payload["candidateMatches"] == [{"model": "X5", "score": 0.98}]
    assert payload["matchReason"] == {"strategy": "unit-test"}

    # must NOT leak observation or current-price field names
    assert "currentMsrpValue" not in payload


def test_review_decision_payload_contract() -> None:
    from app.db.models import ReviewDecision
    from app.services.payload_serializers import review_decision_payload

    decided = datetime(2026, 4, 11, 11, 0, tzinfo=timezone.utc)
    rd = ReviewDecision(
        review_decision_id=uuid4(),
        review_case_id=uuid4(),
        observation_id=uuid4(),
        decision="approve",
        decided_official_model=None,
        decided_official_trim=None,
        note="Looks correct",
        decided_by="analyst-1",
        decided_at_utc=decided,
    )
    payload = review_decision_payload(rd)

    assert "reviewDecisionId" in payload
    assert "reviewCaseId" in payload
    assert "observationId" in payload
    assert payload["decision"] == "approve"
    assert payload["decidedBy"] == "analyst-1"
    assert payload["decidedAtUtc"] == decided.isoformat()
    assert payload["note"] == "Looks correct"
    assert payload["decidedOfficialModel"] is None
    assert payload["decidedOfficialTrim"] is None


def test_source_payload_contract() -> None:
    from app.db.models import MsrpSource
    from app.services.payload_serializers import source_payload

    now = datetime(2026, 4, 11, 10, 0, tzinfo=timezone.utc)
    src = MsrpSource(
        source_id=uuid4(),
        source_code="DE-BMW-OFFICIAL",
        country="德国",
        brand="BMW",
        source_url="https://www.bmw.de/configure",
        source_type="official_configurator",
        extractor_name="bmw_de_extractor",
        extractor_version="v2",
        price_semantics="msrp_incl_vat",
        requires_location=False,
        enabled=True,
        notes=None,
        created_at_utc=now,
        updated_at_utc=now,
    )
    payload = source_payload(src)

    assert "sourceId" in payload
    assert payload["sourceCode"] == "DE-BMW-OFFICIAL"
    assert payload["country"] == "Germany"
    assert payload["brand"] == "BMW"
    assert payload["sourceType"] == "official_configurator"
    assert payload["extractorName"] == "bmw_de_extractor"
    assert payload["extractorVersion"] == "v2"
    assert payload["priceSemantics"] == "msrp_incl_vat"
    assert payload["requiresLocation"] is False
    assert payload["enabled"] is True
    assert payload["createdAtUtc"] == now.isoformat()
    assert payload["updatedAtUtc"] == now.isoformat()


def test_override_payload_contract() -> None:
    from app.db.models import MatchOverride
    from app.services.payload_serializers import override_payload

    now = datetime(2026, 4, 11, 10, 0, tzinfo=timezone.utc)
    o = MatchOverride(
        override_id=uuid4(),
        country="德国",
        brand="BMW",
        jato_model="X5",
        jato_trim="xDrive40i",
        jato_powertrain="PHEV",
        official_model="X5",
        official_trim="xDrive40i",
        valid_from_date=date(2026, 1, 1),
        valid_to_date=None,
        override_reason="Manual remap",
        created_by="admin",
        created_at_utc=now,
        updated_at_utc=now,
    )
    payload = override_payload(o)

    assert "overrideId" in payload
    assert payload["country"] == "Germany"
    assert payload["brand"] == "BMW"
    assert payload["jatoModel"] == "X5"
    assert payload["jatoPowertrain"] == "PHEV"
    assert payload["officialModel"] == "X5"
    assert payload["validFromDate"] == "2026-01-01"
    assert payload["validToDate"] is None
    assert payload["overrideReason"] == "Manual remap"
    assert payload["createdBy"] == "admin"
    assert payload["createdAtUtc"] == now.isoformat()
