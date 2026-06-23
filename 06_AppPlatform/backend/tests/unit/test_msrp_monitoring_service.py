from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID, uuid4

from app.db.models import CurrentPrice, MsrpObservation, MsrpSource, PriceHistory
from app.services import msrp_monitoring_service


def _observation(
    *,
    observation_id: UUID,
    source_id: UUID,
    scrape_batch_id: UUID,
    observed_at: datetime,
    country: str = "se",
    brand: str = "Volvo",
    jato_model: str = "XC60",
    jato_trim: str = "Ultra",
    jato_powertrain: str = "PHEV",
    msrp_value: str = "65000.00",
    source_msrp_value: str = "747500.00",
    source_currency: str = "SEK",
    match_confidence: str = "0.7600",
    match_status: str = "review_required",
    length_mm: int = 4708,
    dryrun_run_id: str | None = "msrp-dryrun-20260620-010203",
    source_payload_hash: str = "hash-monitoring",
) -> MsrpObservation:
    return MsrpObservation(
        observation_id=observation_id,
        scrape_batch_id=scrape_batch_id,
        source_id=source_id,
        country=country,
        brand=brand,
        jato_model=jato_model,
        jato_trim=jato_trim,
        jato_powertrain=jato_powertrain,
        official_model=jato_model,
        official_trim=jato_trim,
        official_edition=None,
        official_powertrain=jato_powertrain,
        msrp_value=Decimal(msrp_value),
        currency="EUR",
        source_msrp_value=Decimal(source_msrp_value),
        source_currency=source_currency,
        fx_rate_to_eur=Decimal("0.08695652"),
        fx_rate_as_of_date=date(2026, 6, 20),
        fx_source="unit-test",
        tax_included=True,
        price_label="List price",
        availability_text=None,
        observed_at_utc=observed_at,
        source_url=f"https://example.test/{country}/{brand}/{jato_model}",
        source_snapshot_path="snapshots/volvo-xc60.html",
        source_payload_hash=source_payload_hash,
        extraction_version="v1",
        match_confidence=Decimal(match_confidence),
        match_status=match_status,
        match_reason_json={"resolver": "unit-test"},
        source_context_json={
            "dryrunRunId": dryrun_run_id,
            "vehicle": {"length_mm": length_mm},
        },
        created_at_utc=observed_at,
        updated_at_utc=observed_at,
    )


def _current_price(
    *,
    current_price_id: UUID,
    observation: MsrpObservation,
    updated_at: datetime,
) -> CurrentPrice:
    return CurrentPrice(
        current_price_id=current_price_id,
        country=observation.country,
        brand=observation.brand,
        jato_model=observation.jato_model,
        jato_trim=observation.jato_trim,
        jato_powertrain=observation.jato_powertrain or "",
        official_model=observation.official_model,
        official_trim=observation.official_trim,
        official_edition=observation.official_edition,
        official_powertrain=observation.official_powertrain,
        effective_observation_id=observation.observation_id,
        current_msrp_value=observation.msrp_value,
        currency=observation.currency,
        source_msrp_value=observation.source_msrp_value,
        source_currency=observation.source_currency,
        fx_rate_to_eur=observation.fx_rate_to_eur,
        fx_rate_as_of_date=observation.fx_rate_as_of_date,
        fx_source=observation.fx_source,
        tax_included=observation.tax_included,
        match_confidence=observation.match_confidence,
        match_status=observation.match_status,
        source_url=observation.source_url,
        source_snapshot_path=observation.source_snapshot_path,
        last_price_change_at_utc=updated_at,
        updated_at_utc=updated_at,
    )


def _source(
    source_id: UUID,
    *,
    country: str = "se",
    brand: str = "Volvo",
    source_code: str = "volvo_xc60_se_draft_scrapling",
    source_type: str = "third_party_reference",
) -> MsrpSource:
    return MsrpSource(
        source_id=source_id,
        source_code=source_code,
        country=country,
        brand=brand,
        source_url="https://www.volvocars.com/se/cars/xc60/",
        source_type=source_type,
        tier=3,
        extractor_name="scrapling_static",
        extractor_version="v1",
        price_semantics="msrp",
        requires_location=False,
        enabled=True,
        notes=None,
    )


def _price_period(
    *,
    price_history_id: UUID,
    observation_id: UUID,
    valid_from: datetime,
    msrp_value: str,
    source_msrp_value: str,
    country: str = "se",
    brand: str = "Volvo",
    jato_model: str = "XC60",
    jato_trim: str = "Ultra",
    jato_powertrain: str = "PHEV",
    source_currency: str = "SEK",
) -> PriceHistory:
    return PriceHistory(
        price_history_id=price_history_id,
        country=country,
        brand=brand,
        jato_model=jato_model,
        jato_trim=jato_trim,
        jato_powertrain=jato_powertrain,
        msrp_value=Decimal(msrp_value),
        currency="EUR",
        source_msrp_value=Decimal(source_msrp_value),
        source_currency=source_currency,
        valid_from_utc=valid_from,
        valid_to_utc=None,
        last_confirmed_at_utc=valid_from,
        started_by_observation_id=observation_id,
        ended_by_observation_id=None,
        last_confirmed_by_observation_id=observation_id,
        created_at_utc=valid_from,
    )


def test_build_msrp_monitoring_events_returns_warning_when_history_missing(
    monkeypatch,
) -> None:
    now = datetime(2026, 6, 23, 8, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(msrp_monitoring_service, "_utc_now", lambda: now)
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "has_price_history_table",
        lambda session: False,
    )

    payload = msrp_monitoring_service.build_msrp_monitoring_events(
        "session",
        country="se",
        brand="Volvo",
        jato_model="XC60",
        window_days=400,
        threshold_pct=2.5,
        limit=999,
    )

    assert payload["schemaVersion"] == "msrp_monitoring_events_v1"
    assert payload["warnings"] == ["price_history_unavailable"]
    assert payload["filters"] == {
        "country": "se",
        "brand": "Volvo",
        "jatoModel": "XC60",
        "windowDays": 365,
        "thresholdPct": 2.5,
        "limit": 500,
    }
    assert payload["summary"]["eventCount"] == 0
    assert payload["events"] == []


def test_build_msrp_monitoring_events_groups_price_changes_with_evidence(
    monkeypatch,
) -> None:
    now = datetime(2026, 6, 23, 8, 0, tzinfo=timezone.utc)
    scrape_batch_id = uuid4()
    source_id = uuid4()
    observation_id = uuid4()
    current_price_id = uuid4()
    observation = _observation(
        observation_id=observation_id,
        source_id=source_id,
        scrape_batch_id=scrape_batch_id,
        observed_at=now - timedelta(days=2),
    )
    current_price = _current_price(
        current_price_id=current_price_id,
        observation=observation,
        updated_at=now - timedelta(days=2),
    )
    source = _source(source_id)
    current_period = _price_period(
        price_history_id=uuid4(),
        observation_id=observation_id,
        valid_from=now - timedelta(days=2),
        msrp_value="65000.00",
        source_msrp_value="747500.00",
    )
    previous_period = _price_period(
        price_history_id=uuid4(),
        observation_id=uuid4(),
        valid_from=now - timedelta(days=30),
        msrp_value="68000.00",
        source_msrp_value="782000.00",
    )

    monkeypatch.setattr(msrp_monitoring_service, "_utc_now", lambda: now)
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "has_price_history_table",
        lambda session: True,
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_current_price_alerts",
        lambda *args, **kwargs: [current_price],
    )
    monkeypatch.setattr(
        msrp_monitoring_service,
        "_load_market_scan_length_lookup",
        lambda current_prices: ({}, {}, None),
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_observations_by_ids",
        lambda session, ids: [
            observation
            for item_id in ids
            if UUID(str(item_id)) == observation.observation_id
        ],
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_sources_by_ids",
        lambda session, ids: [
            source
            for item_id in ids
            if UUID(str(item_id)) == source.source_id
        ],
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_price_history",
        lambda *args, **kwargs: [current_period, previous_period],
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "get_scrape_batch",
        lambda *args, **kwargs: SimpleNamespace(
            batch_code="msrp-dryrun-20260620-010203",
        ),
    )

    payload = msrp_monitoring_service.build_msrp_monitoring_events(
        "session",
        window_days=30,
        threshold_pct=1.0,
    )

    assert payload["warnings"] == []
    assert payload["summary"] == {
        "eventCount": 1,
        "timelineEventCount": 1,
        "affectedCountryCount": 1,
        "sourceRiskCount": 1,
        "reviewRequiredCount": 1,
        "outlierCount": 0,
        "lengthMissingCount": 0,
    }
    event = payload["events"][0]
    assert event["eventId"] == "Volvo|XC60|PHEV"
    assert event["lengthMm"] == 4708
    assert event["lengthSource"] == "observation_context"
    assert event["confidence"] == "low"
    assert event["medianChangePct"] == -4.41
    assert event["medianOldMsrpEur"] == 68000.0
    assert event["medianCurrentMsrpEur"] == 65000.0
    assert event["riskReasons"] == {
        "match_status:review_required": 1,
        "low_match_confidence": 1,
        "non_official_source:third_party_reference": 1,
    }

    country = event["countries"][0]
    assert country["country"] == "se"
    assert country["countryLabel"] == "Sweden"
    assert country["sourceStatus"] == "review_required"
    assert country["reviewFlag"] is True
    assert country["outlier"] is False
    assert country["suspectedFalsePositive"] is True
    assert country["changeAmountEur"] == -3000.0
    assert country["changePct"] == -4.41
    assert country["source"]["sourceCode"] == "volvo_xc60_se_draft_scrapling"
    assert country["source"]["sourceType"] == "third_party_reference"
    assert country["evidence"]["sourceSnapshotPath"] == "snapshots/volvo-xc60.html"
    assert country["evidence"]["sourcePayloadHash"] == "hash-monitoring"
    assert country["evidence"]["dryrunRunId"] == "msrp-dryrun-20260620-010203"
    assert country["evidence"]["scrapeBatchCode"] == "msrp-dryrun-20260620-010203"


def test_build_msrp_monitoring_events_groups_multi_country_sync_and_outlier(
    monkeypatch,
) -> None:
    now = datetime(2026, 6, 23, 8, 0, tzinfo=timezone.utc)
    batch_by_id: dict[UUID, SimpleNamespace] = {}
    observations: list[MsrpObservation] = []
    current_prices: list[CurrentPrice] = []
    sources: list[MsrpSource] = []
    history_by_country: dict[str, list[PriceHistory]] = {}

    rows = [
        ("de", "Tesla", "Model Y", "Long Range", "BEV", "48000.00", "50000.00", "confirmed", "0.9600", "manufacturer_official"),
        ("nl", "Tesla", "Model Y", "Long Range", "BEV", "49400.00", "52000.00", "confirmed", "0.9500", "manufacturer_official"),
        ("dk", "Tesla", "Model Y", "Long Range", "BEV", "49000.00", "70000.00", "review_required", "0.7100", "third_party_reference"),
    ]
    for country, brand, model, trim, powertrain, current_value, previous_value, match_status, confidence, source_type in rows:
        observation_id = uuid4()
        source_id = uuid4()
        batch_id = uuid4()
        batch_by_id[batch_id] = SimpleNamespace(
            batch_code=f"msrp-dryrun-20260620-{country}",
        )
        observation = _observation(
            observation_id=observation_id,
            source_id=source_id,
            scrape_batch_id=batch_id,
            observed_at=now - timedelta(days=1),
            country=country,
            brand=brand,
            jato_model=model,
            jato_trim=trim,
            jato_powertrain=powertrain,
            msrp_value=current_value,
            source_msrp_value=current_value,
            source_currency="EUR",
            match_confidence=confidence,
            match_status=match_status,
            length_mm=4790,
            dryrun_run_id=f"msrp-dryrun-20260620-{country}",
        )
        observations.append(observation)
        current_prices.append(
            _current_price(
                current_price_id=uuid4(),
                observation=observation,
                updated_at=now - timedelta(days=1),
            )
        )
        sources.append(
            _source(
                source_id,
                country=country,
                brand=brand,
                source_code=f"tesla_model_y_{country}",
                source_type=source_type,
            )
        )
        history_by_country[country] = [
            _price_period(
                price_history_id=uuid4(),
                observation_id=observation_id,
                valid_from=now - timedelta(days=1),
                msrp_value=current_value,
                source_msrp_value=current_value,
                country=country,
                brand=brand,
                jato_model=model,
                jato_trim=trim,
                jato_powertrain=powertrain,
                source_currency="EUR",
            ),
            _price_period(
                price_history_id=uuid4(),
                observation_id=uuid4(),
                valid_from=now - timedelta(days=12),
                msrp_value=previous_value,
                source_msrp_value=previous_value,
                country=country,
                brand=brand,
                jato_model=model,
                jato_trim=trim,
                jato_powertrain=powertrain,
                source_currency="EUR",
            ),
        ]

    monkeypatch.setattr(msrp_monitoring_service, "_utc_now", lambda: now)
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "has_price_history_table",
        lambda session: True,
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_current_price_alerts",
        lambda *args, **kwargs: current_prices,
    )
    monkeypatch.setattr(
        msrp_monitoring_service,
        "_load_market_scan_length_lookup",
        lambda prices: ({}, {}, None),
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_observations_by_ids",
        lambda session, ids: [
            observation
            for observation in observations
            if observation.observation_id in {UUID(str(item_id)) for item_id in ids}
        ],
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_sources_by_ids",
        lambda session, ids: [
            source
            for source in sources
            if source.source_id in {UUID(str(item_id)) for item_id in ids}
        ],
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_price_history",
        lambda session, country, *args: history_by_country[country],
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "get_scrape_batch",
        lambda session, batch_id: batch_by_id[batch_id],
    )

    payload = msrp_monitoring_service.build_msrp_monitoring_events(
        "session",
        window_days=30,
        threshold_pct=1.0,
    )

    assert payload["summary"]["eventCount"] == 1
    assert payload["summary"]["timelineEventCount"] == 3
    assert payload["summary"]["affectedCountryCount"] == 3
    assert payload["summary"]["sourceRiskCount"] == 1
    assert payload["summary"]["outlierCount"] == 1

    event = payload["events"][0]
    assert event["eventId"] == "Tesla|Model Y|BEV"
    assert event["powertrainColor"] == "#16a34a"
    assert event["lengthMm"] == 4790
    assert event["affectedCountryCount"] == 3
    assert event["multiCountrySync"] is True
    assert event["medianChangePct"] == -5.0
    assert event["medianOldMsrpEur"] == 52000.0
    assert event["medianCurrentMsrpEur"] == 49000.0
    assert event["sourceRiskCount"] == 1
    assert event["reviewRequiredCount"] == 1
    assert event["outlierCount"] == 1
    assert event["suspectedFalsePositiveCount"] == 1

    countries = {item["country"]: item for item in event["countries"]}
    assert set(countries) == {"de", "nl", "dk"}
    assert countries["de"]["changePct"] == -4.0
    assert countries["nl"]["changePct"] == -5.0
    assert countries["dk"]["changePct"] == -30.0
    assert countries["dk"]["outlier"] is True
    assert countries["dk"]["reviewFlag"] is True
    assert countries["dk"]["suspectedFalsePositive"] is True
    assert countries["dk"]["evidence"]["dryrunRunId"] == "msrp-dryrun-20260620-dk"
