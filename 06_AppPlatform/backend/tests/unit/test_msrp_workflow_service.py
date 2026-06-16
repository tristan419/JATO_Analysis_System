from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import pandas as pd
from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError

from app.db.models import CurrentPrice, FinanceObservation, MsrpObservation
from app.services import advanced_analysis_service, msrp_workflow_service


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


def _make_price_period(
    observation: MsrpObservation,
    *,
    source_msrp_value: str,
    msrp_value: str,
    valid_from_utc: datetime,
    valid_to_utc: datetime | None,
) -> SimpleNamespace:
    return SimpleNamespace(
        price_history_id=uuid4(),
        country=observation.country,
        brand=observation.brand,
        jato_model=observation.jato_model,
        jato_trim=observation.jato_trim,
        jato_powertrain=observation.jato_powertrain,
        msrp_value=Decimal(msrp_value),
        currency="EUR",
        source_msrp_value=Decimal(source_msrp_value),
        source_currency=observation.source_currency,
        valid_from_utc=valid_from_utc,
        valid_to_utc=valid_to_utc,
        last_confirmed_at_utc=valid_to_utc or valid_from_utc,
        started_by_observation_id=uuid4(),
        ended_by_observation_id=uuid4() if valid_to_utc else None,
        last_confirmed_by_observation_id=uuid4(),
        created_at_utc=valid_from_utc,
    )


def _make_reconciliation_observation(
    *,
    source_id,
    source_msrp_value: str,
    msrp_value: str,
    observed_at_utc: datetime,
) -> MsrpObservation:
    observation = _make_observation()
    observation.observation_id = uuid4()
    observation.source_id = source_id
    observation.source_msrp_value = Decimal(source_msrp_value)
    observation.msrp_value = Decimal(msrp_value)
    observation.observed_at_utc = observed_at_utc
    observation.created_at_utc = observed_at_utc
    observation.updated_at_utc = observed_at_utc
    observation.source_url = f"https://example.test/{source_id}"
    return observation


def _make_finance_observation(
    observation: MsrpObservation | None = None,
) -> FinanceObservation:
    obs = observation or _make_observation()
    now = obs.observed_at_utc
    return FinanceObservation(
        finance_observation_id=uuid4(),
        observation_id=obs.observation_id,
        scrape_batch_id=obs.scrape_batch_id,
        country=obs.country,
        brand=obs.brand,
        jato_model=obs.jato_model,
        jato_trim=obs.jato_trim,
        jato_powertrain=obs.jato_powertrain,
        official_model=obs.official_model,
        official_trim=obs.official_trim,
        official_edition=obs.official_edition,
        official_powertrain=obs.official_powertrain,
        price_semantics="lease_monthly",
        finance_type="private_lease",
        monthly_payment=Decimal("5990.00"),
        monthly_payment_eur=Decimal("520.87"),
        down_payment=Decimal("40000.00"),
        down_payment_eur=Decimal("3478.26"),
        down_payment_pct=Decimal("5.0000"),
        term_months=36,
        apr=Decimal("3.9000"),
        effective_apr=Decimal("4.2000"),
        balloon_payment=Decimal("250000.00"),
        balloon_payment_eur=Decimal("21739.13"),
        total_credit_cost=Decimal("45000.00"),
        total_credit_cost_eur=Decimal("3913.04"),
        total_amount_payable=Decimal("860000.00"),
        total_amount_payable_eur=Decimal("74782.61"),
        annual_mileage_limit=15000,
        offer_valid_until=date(2026, 6, 30),
        subsidy_amount=Decimal("25000.00"),
        subsidy_amount_eur=Decimal("2173.91"),
        net_price_after_subsidy=Decimal("748000.00"),
        net_price_after_subsidy_eur=Decimal("65043.48"),
        currency="SEK",
        source_url=obs.source_url,
        observed_at_utc=now,
        finance_context_json={"price_semantics": "lease_monthly"},
        created_at_utc=now,
        updated_at_utc=now,
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


def test_list_current_price_alerts_returns_empty_when_history_is_missing(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "has_price_history_table",
        lambda session: False,
    )

    def _unexpected_query(*args, **kwargs):
        raise AssertionError(
            "list_current_price_alerts should not query a missing table"
        )

    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "list_current_price_alerts",
        _unexpected_query,
    )

    payload = msrp_workflow_service.list_current_price_alerts(
        None,
        "Sweden",
        "Volvo",
        "XC60",
        10,
        0,
    )

    assert payload == {
        "rows": 0,
        "total": 0,
        "limit": 10,
        "offset": 0,
        "thresholdPct": 3.0,
        "summary": {
            "priceChangeEventCount": 0,
            "thresholdAlertCount": 0,
            "highPriorityAlertCount": 0,
            "directionCounts": {},
            "severityCounts": {},
        },
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


def test_finance_observation_from_payload_reads_pricing_context(
    monkeypatch,
) -> None:
    observation = _make_observation()
    payload = {
        "source_context_json": {
            "pricingContext": {
                "price_semantics": "lease_monthly",
                "monthly_payment": 5990.0,
                "down_payment": 40000.0,
                "down_payment_pct": 5,
                "term_months": 36,
                "apr": 3.9,
                "effective_apr": 4.2,
                "balloon_payment": 250000.0,
                "finance_type": "private_lease",
                "total_credit_cost": 45000.0,
                "total_amount_payable": 860000.0,
                "annual_mileage_limit": 15000,
                "offer_valid_until": "2026-06-30",
                "subsidy_amount": 25000.0,
                "net_price_after_subsidy": 748000.0,
            }
        },
        "monthly_payment": 5790.0,
    }

    monkeypatch.setattr(
        msrp_workflow_service,
        "convert_amount_to_eur",
        lambda amount, currency, observed_at: (
            round(float(amount) * 0.1, 2),
            SimpleNamespace(rate_to_eur=0.1),
        ),
    )

    finance_observation = (
        msrp_workflow_service._finance_observation_from_payload(
            observation,
            payload,
            "base_msrp",
        )
    )

    assert finance_observation is not None
    assert finance_observation.observation_id == observation.observation_id
    assert finance_observation.price_semantics == "lease_monthly"
    assert finance_observation.finance_type == "private_lease"
    assert finance_observation.monthly_payment == 5790.0
    assert finance_observation.monthly_payment_eur == 579.0
    assert finance_observation.down_payment_eur == 4000.0
    assert finance_observation.offer_valid_until == date(2026, 6, 30)
    assert finance_observation.net_price_after_subsidy_eur == 74800.0


def test_create_scrape_batch_ingest_persists_finance_observations(
    monkeypatch,
) -> None:
    class FakeSession:
        def flush(self) -> None:
            pass

        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            pass

        def refresh(self, _item) -> None:
            pass

    source_id = uuid4()
    captured: dict[str, list[FinanceObservation]] = {}

    def add_scrape_batch(_session, batch):
        batch.scrape_batch_id = batch.scrape_batch_id or uuid4()
        return batch

    def add_observations(_session, observations):
        now = datetime(2026, 6, 16, 9, 0, tzinfo=timezone.utc)
        for observation in observations:
            observation.observation_id = observation.observation_id or uuid4()
            observation.created_at_utc = observation.created_at_utc or now
            observation.updated_at_utc = observation.updated_at_utc or now
        return observations

    def add_finance_observations(_session, observations):
        now = datetime(2026, 6, 16, 9, 0, tzinfo=timezone.utc)
        for item in observations:
            item.finance_observation_id = (
                item.finance_observation_id or uuid4()
            )
            item.created_at_utc = item.created_at_utc or now
            item.updated_at_utc = item.updated_at_utc or now
        captured["items"] = observations
        return observations

    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "add_scrape_batch",
        add_scrape_batch,
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "has_price_history_table",
        lambda session: False,
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "has_finance_observations_table",
        lambda session: True,
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "get_source",
        lambda session, item_id: SimpleNamespace(
            source_id=item_id,
            price_semantics="base_msrp",
        ),
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "add_observations",
        add_observations,
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "add_finance_observations",
        add_finance_observations,
    )
    monkeypatch.setattr(
        msrp_workflow_service,
        "convert_amount_to_eur",
        lambda amount, currency, observed_at: (
            round(float(amount) * 0.1, 2),
            SimpleNamespace(
                rate_to_eur=0.1,
                as_of_date=date(2026, 6, 16),
                source="unit-test",
            ),
        ),
    )

    payload = {
        "batch_code": "finance-batch",
        "trigger_type": "manual",
        "scope_country": "瑞典",
        "scope_brands": ["Volvo"],
        "observations": [
            {
                "source_id": str(source_id),
                "country": "瑞典",
                "brand": "Volvo",
                "jato_model": "XC60",
                "jato_trim": "Ultra",
                "jato_powertrain": "PHEV",
                "official_model": "XC60",
                "official_trim": "Ultra",
                "msrp_value": 773000,
                "currency": "SEK",
                "tax_included": True,
                "price_label": "Lease monthly",
                "observed_at_utc": datetime(
                    2026, 6, 16, 9, 0, tzinfo=timezone.utc
                ),
                "source_url": "https://example.test/xc60",
                "source_payload_hash": "hash-finance",
                "extraction_version": "test",
                "match_confidence": 0.91,
                "match_status": "auto_accepted",
                "source_context_json": {
                    "pricingContext": {
                        "price_semantics": "lease_monthly",
                        "monthly_payment": 5990,
                        "term_months": 36,
                        "finance_type": "private_lease",
                        "finance_currency": "SEK",
                    },
                },
            }
        ],
    }

    result = msrp_workflow_service.create_scrape_batch_ingest(
        FakeSession(),
        payload,
        commit=True,
    )

    assert result["financeObservationsCreated"] == 1
    assert result["currentPricesTouched"] == 0
    assert result["nonMsrpPriceObservationCount"] == 1
    assert captured["items"][0].price_semantics == "lease_monthly"
    assert captured["items"][0].monthly_payment_eur == 599.0


def test_finance_observation_from_payload_skips_plain_msrp() -> None:
    assert (
        msrp_workflow_service._finance_observation_from_payload(
            _make_observation(),
            {"source_context_json": {}},
            "base_msrp",
        )
        is None
    )


def test_finance_context_from_payload_serializes_date_for_jsonb() -> None:
    context = msrp_workflow_service._finance_context_from_payload(
        {
            "price_semantics": "lease_monthly",
            "monthly_payment": 499.0,
            "offer_valid_until": date(2026, 6, 30),
        },
        "base_msrp",
    )

    assert context["price_semantics"] == "lease_monthly"
    assert context["monthly_payment"] == 499.0
    assert context["offer_valid_until"] == "2026-06-30"


def test_list_finance_observations_returns_empty_when_table_missing(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "has_finance_observations_table",
        lambda session: False,
    )

    payload = msrp_workflow_service.list_finance_observations(
        None,
        "Sweden",
        "Volvo",
        "XC60",
        "lease_monthly",
        None,
        None,
        None,
        None,
        10,
        0,
    )

    assert payload == {
        "rows": 0,
        "total": 0,
        "limit": 10,
        "offset": 0,
        "summary": {
            "priceSemanticsCounts": {},
            "financeTypeCounts": {},
            "monthlyPaymentCount": 0,
            "monthlyPaymentEurMin": None,
            "monthlyPaymentEurMax": None,
            "netPriceAfterSubsidyCount": 0,
            "netPriceAfterSubsidyEurMin": None,
            "netPriceAfterSubsidyEurMax": None,
            "subsidyObservationCount": 0,
        },
        "items": [],
        "warning": "finance_observations_unavailable",
    }


def test_list_finance_observations_returns_summary_and_items(
    monkeypatch,
) -> None:
    item = _make_finance_observation()
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "has_finance_observations_table",
        lambda session: True,
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "count_finance_observations",
        lambda *args, **kwargs: 7,
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "list_finance_observations",
        lambda *args, **kwargs: [item],
    )

    payload = msrp_workflow_service.list_finance_observations(
        None,
        "Sweden",
        "Volvo",
        "XC60",
        "lease_monthly",
        "private_lease",
        True,
        True,
        True,
        10,
        0,
    )

    assert payload["rows"] == 1
    assert payload["total"] == 7
    assert payload["summary"]["priceSemanticsCounts"] == {
        "lease_monthly": 1
    }
    assert payload["summary"]["financeTypeCounts"] == {"private_lease": 1}
    assert payload["summary"]["monthlyPaymentCount"] == 1
    assert payload["summary"]["subsidyObservationCount"] == 1
    assert payload["items"][0]["financeObservationId"] == str(
        item.finance_observation_id
    )
    assert payload["items"][0]["monthlyPaymentEur"] == 520.87
    assert payload["items"][0]["netPriceAfterSubsidyEur"] == 65043.48


def test_list_finance_observations_passes_finance_presence_filters(
    monkeypatch,
) -> None:
    captured: dict[str, tuple[object, ...]] = {}

    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "has_finance_observations_table",
        lambda session: True,
    )

    def _capture_count(*args):
        captured["count"] = args
        return 0

    def _capture_list(*args):
        captured["list"] = args
        return []

    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "count_finance_observations",
        _capture_count,
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "list_finance_observations",
        _capture_list,
    )

    payload = msrp_workflow_service.list_finance_observations(
        None,
        "France",
        "Renault",
        "Megane",
        "net_after_subsidy",
        None,
        False,
        True,
        True,
        25,
        5,
    )

    assert payload["rows"] == 0
    assert captured["count"][-3:] == (False, True, True)
    assert captured["list"][-5:] == (False, True, True, 25, 5)


def test_list_current_price_alerts_returns_delta_detail(
    monkeypatch,
) -> None:
    observation = _make_observation()
    current_price = _make_current_price(observation)
    current_price.effective_observation_id = observation.observation_id
    previous_period = _make_price_period(
        observation,
        source_msrp_value="733000.00",
        msrp_value="64000.00",
        valid_from_utc=datetime(2026, 3, 11, 9, 10, tzinfo=timezone.utc),
        valid_to_utc=observation.observed_at_utc,
    )
    latest_period = _make_price_period(
        observation,
        source_msrp_value="773000.00",
        msrp_value="67217.39",
        valid_from_utc=observation.observed_at_utc,
        valid_to_utc=None,
    )

    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "has_price_history_table",
        lambda *args, **kwargs: True,
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "count_current_price_alerts",
        lambda *args, **kwargs: 1,
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "list_current_price_alerts",
        lambda *args, **kwargs: [current_price],
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "list_price_history",
        lambda *args, **kwargs: [latest_period, previous_period],
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "list_observations_by_ids",
        lambda *args, **kwargs: [
            SimpleNamespace(
                observation_id=observation.observation_id,
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
                source_code="volvo_se_official",
                source_type="official_site",
                extractor_name="pdf_text",
                extractor_version="v1",
            )
        ],
    )

    payload = msrp_workflow_service.list_current_price_alerts(
        None,
        "Sweden",
        "Volvo",
        "XC60",
        10,
        0,
    )

    assert payload["rows"] == 1
    assert payload["total"] == 1
    assert payload["limit"] == 10
    assert payload["offset"] == 0
    item = payload["items"][0]
    assert item["country"] == "Sweden"
    assert item["brand"] == "Volvo"
    assert item["jatoModel"] == "XC60"
    assert item["direction"] == "increase"
    assert item["eventType"] == "price_change"
    assert item["severity"] == "warning"
    assert item["thresholdPct"] == 3.0
    assert item["isThresholdAlert"] is True
    assert item["isHighPriority"] is False
    assert item["recommendedAction"] == "review_price_increase_and_notify_market_team"
    assert item["currentSourceMsrpValue"] == 773000.0
    assert item["previousSourceMsrpValue"] == 733000.0
    assert item["deltaSourceMsrpValue"] == 40000.0
    assert item["deltaMsrpValue"] == 3217.39
    assert item["deltaPct"] == 5.46
    assert item["changedAtUtc"] == observation.observed_at_utc.isoformat()
    assert item["currentPrice"]["sourceCode"] == "volvo_se_official"
    assert item["latestPrice"]["sourceMsrpValue"] == 773000.0
    assert item["previousPrice"]["sourceMsrpValue"] == 733000.0
    assert payload["thresholdPct"] == 3.0
    assert payload["summary"] == {
        "priceChangeEventCount": 1,
        "thresholdAlertCount": 1,
        "highPriorityAlertCount": 0,
        "directionCounts": {"increase": 1},
        "severityCounts": {"warning": 1},
    }


def test_price_alert_payload_keeps_small_change_as_event_not_threshold_alert() -> None:
    observation = _make_observation()
    current_price = _make_current_price(observation)
    current_price.effective_observation_id = observation.observation_id
    previous_period = _make_price_period(
        observation,
        source_msrp_value="770000.00",
        msrp_value="66956.52",
        valid_from_utc=datetime(2026, 3, 11, 9, 10, tzinfo=timezone.utc),
        valid_to_utc=observation.observed_at_utc,
    )
    latest_period = _make_price_period(
        observation,
        source_msrp_value="773000.00",
        msrp_value="67217.39",
        valid_from_utc=observation.observed_at_utc,
        valid_to_utc=None,
    )

    payload = msrp_workflow_service._price_alert_payload(
        current_price,
        latest_period,
        previous_period,
        None,
        threshold_pct=3.0,
    )

    assert payload["eventType"] == "price_change"
    assert payload["direction"] == "increase"
    assert payload["deltaPct"] == 0.39
    assert payload["severity"] == "info"
    assert payload["isThresholdAlert"] is False
    assert payload["isHighPriority"] is False
    assert payload["recommendedAction"] == "keep_monitoring"


def test_build_current_price_snapshot_reuses_current_prices_and_alerts(
    monkeypatch,
) -> None:
    now = datetime(2026, 6, 11, 16, 30, tzinfo=timezone.utc)
    calls: list[tuple[str, tuple[object, ...]]] = []

    def fake_list_current_prices(*args):
        calls.append(("current", args))
        return {
            "rows": 1,
            "total": 12,
            "items": [{"currentPriceId": "cp-1"}],
        }

    def fake_list_current_price_alerts(*args):
        calls.append(("alerts", args))
        return {
            "rows": 1,
            "total": 2,
            "items": [{"direction": "decrease"}],
            "warning": "price_history_unavailable",
        }

    monkeypatch.setattr(msrp_workflow_service, "_utc_now", lambda: now)
    monkeypatch.setattr(
        msrp_workflow_service,
        "list_current_prices",
        fake_list_current_prices,
    )
    monkeypatch.setattr(
        msrp_workflow_service,
        "list_current_price_alerts",
        fake_list_current_price_alerts,
    )

    payload = msrp_workflow_service.build_current_price_snapshot(
        "session",
        "Sweden",
        "Volvo",
        "XC60",
        500,
    )

    assert calls == [
        ("current", ("session", "Sweden", "Volvo", "XC60", 500, 0)),
        ("alerts", ("session", "Sweden", "Volvo", "XC60", 500, 0, 3.0)),
    ]
    assert payload == {
        "schemaVersion": "msrp_current_price_snapshot_v1",
        "generatedAtUtc": now.isoformat(),
        "snapshotWeek": "2026-W24",
        "filters": {
            "country": "Sweden",
            "brand": "Volvo",
            "jatoModel": "XC60",
        },
        "summary": {
            "currentPriceCount": 12,
            "returnedCurrentPriceCount": 1,
            "priceAlertCount": 2,
            "returnedPriceAlertCount": 1,
            "priceAlertThresholdPct": 3.0,
            "priceAlertSummary": {
                "priceChangeEventCount": 1,
                "thresholdAlertCount": 0,
                "highPriorityAlertCount": 0,
                "directionCounts": {"decrease": 1},
                "severityCounts": {"info": 1},
            },
            "limit": 500,
        },
        "currentPrices": [{"currentPriceId": "cp-1"}],
        "priceAlerts": [{"direction": "decrease"}],
        "warnings": ["price_history_unavailable"],
    }


def test_build_multi_source_reconciliation_flags_source_conflict(
    monkeypatch,
) -> None:
    now = datetime(2026, 6, 11, 16, 40, tzinfo=timezone.utc)
    source_a = uuid4()
    source_b = uuid4()
    source_a_latest = _make_reconciliation_observation(
        source_id=source_a,
        source_msrp_value="50000.00",
        msrp_value="50000.00",
        observed_at_utc=now,
    )
    source_a_old = _make_reconciliation_observation(
        source_id=source_a,
        source_msrp_value="49000.00",
        msrp_value="49000.00",
        observed_at_utc=datetime(2026, 6, 4, 16, 40, tzinfo=timezone.utc),
    )
    source_b_latest = _make_reconciliation_observation(
        source_id=source_b,
        source_msrp_value="52000.00",
        msrp_value="52000.00",
        observed_at_utc=now,
    )
    current_price = _make_current_price(source_a_latest)
    current_price.current_msrp_value = Decimal("50000.00")
    current_price.source_msrp_value = Decimal("50000.00")

    monkeypatch.setattr(msrp_workflow_service, "_utc_now", lambda: now)
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "list_reconciliation_observations",
        lambda *args, **kwargs: [
            source_a_latest,
            source_a_old,
            source_b_latest,
        ],
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "list_sources_by_ids",
        lambda *args, **kwargs: [
            SimpleNamespace(
                source_id=source_a,
                source_code="volvo_primary",
                source_type="manufacturer_official",
            ),
            SimpleNamespace(
                source_id=source_b,
                source_code="volvo_pdf",
                source_type="official_pdf",
            ),
        ],
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "get_current_price_by_key",
        lambda *args, **kwargs: current_price,
    )

    payload = msrp_workflow_service.build_multi_source_reconciliation(
        "session",
        "Sweden",
        "Volvo",
        "XC60",
        500,
        1.0,
    )

    assert payload["schemaVersion"] == "msrp_multi_source_reconciliation_v1"
    assert payload["generatedAtUtc"] == now.isoformat()
    assert payload["summary"] == {
        "observationRows": 3,
        "reconciliationGroupCount": 1,
        "statusCounts": {"conflict": 1},
        "limit": 500,
    }
    item = payload["items"][0]
    assert item["status"] == "conflict"
    assert item["recommendedAction"] == "review_conflicting_sources"
    assert item["sourceCount"] == 2
    assert item["observationCount"] == 3
    assert item["minMsrpValue"] == 50000.0
    assert item["maxMsrpValue"] == 52000.0
    assert item["avgMsrpValue"] == 51000.0
    assert item["spreadValue"] == 2000.0
    assert item["spreadPct"] == 3.92
    assert item["currentPrice"]["currentMsrpValue"] == 50000.0
    assert [
        source["sourceCode"] for source in item["sourceObservations"]
    ] == ["volvo_primary", "volvo_pdf"]
    assert [
        source["msrpValue"] for source in item["sourceObservations"]
    ] == [50000.0, 52000.0]


def test_build_multi_source_reconciliation_marks_single_source(
    monkeypatch,
) -> None:
    source_id = uuid4()
    observation = _make_reconciliation_observation(
        source_id=source_id,
        source_msrp_value="50000.00",
        msrp_value="50000.00",
        observed_at_utc=datetime(2026, 6, 11, 16, 40, tzinfo=timezone.utc),
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "list_reconciliation_observations",
        lambda *args, **kwargs: [observation],
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "list_sources_by_ids",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "get_current_price_by_key",
        lambda *args, **kwargs: None,
    )

    payload = msrp_workflow_service.build_multi_source_reconciliation(
        "session",
        None,
        None,
        None,
        500,
        1.0,
    )

    assert payload["summary"]["statusCounts"] == {"single_source": 1}
    item = payload["items"][0]
    assert item["status"] == "single_source"
    assert item["recommendedAction"] == "add_secondary_source"
    assert item["sourceCount"] == 1
    assert item["currentPrice"] is None


def test_queue_reconciliation_conflicts_for_review_is_idempotent(
    monkeypatch,
) -> None:
    now = datetime(2026, 6, 11, 17, 10, tzinfo=timezone.utc)
    source_a = uuid4()
    source_b = uuid4()
    source_a_latest = _make_reconciliation_observation(
        source_id=source_a,
        source_msrp_value="50000.00",
        msrp_value="50000.00",
        observed_at_utc=now,
    )
    source_b_latest = _make_reconciliation_observation(
        source_id=source_b,
        source_msrp_value="53000.00",
        msrp_value="53000.00",
        observed_at_utc=now,
    )
    current_price = _make_current_price(source_a_latest)
    current_price.effective_observation_id = (
        source_a_latest.observation_id
    )
    observations_by_id = {
        source_a_latest.observation_id: source_a_latest,
        source_b_latest.observation_id: source_b_latest,
    }
    cases_by_observation: dict[object, object] = {}
    added_cases: list[object] = []
    commits: list[tuple[str, bool]] = []

    monkeypatch.setattr(msrp_workflow_service, "_utc_now", lambda: now)
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "list_reconciliation_observations",
        lambda *args, **kwargs: [source_a_latest, source_b_latest],
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "list_sources_by_ids",
        lambda *args, **kwargs: [
            SimpleNamespace(
                source_id=source_a,
                source_code="volvo_primary",
                source_type="manufacturer_official",
            ),
            SimpleNamespace(
                source_id=source_b,
                source_code="volvo_pdf",
                source_type="official_pdf",
            ),
        ],
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "get_current_price_by_key",
        lambda *args, **kwargs: current_price,
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "get_observation",
        lambda session, observation_id: observations_by_id.get(
            observation_id
        ),
    )
    monkeypatch.setattr(
        msrp_workflow_service.review_repo,
        "get_review_case_by_observation",
        lambda session, observation_id: cases_by_observation.get(
            observation_id
        ),
    )

    def _add_review_case(session, review_case):
        review_case.review_case_id = uuid4()
        review_case.created_at_utc = now
        review_case.updated_at_utc = now
        cases_by_observation[review_case.observation_id] = review_case
        added_cases.append(review_case)
        return review_case

    monkeypatch.setattr(
        msrp_workflow_service.review_repo,
        "add_review_case",
        _add_review_case,
    )
    monkeypatch.setattr(
        msrp_workflow_service,
        "_commit_or_conflict",
        lambda session, detail, *, commit=True: commits.append(
            (detail, commit)
        ),
    )
    monkeypatch.setattr(
        msrp_workflow_service,
        "review_case_payload",
        lambda review_case, observation, source: {
            "reviewCaseId": str(review_case.review_case_id),
            "observationId": str(observation.observation_id),
            "sourceCode": source.source_code if source else None,
            "candidateMatches": review_case.candidate_matches_json,
        },
    )

    first = msrp_workflow_service.queue_reconciliation_conflicts_for_review(
        "session",
        "Sweden",
        "Volvo",
        "XC60",
        500,
        1.0,
    )
    added_cases[0].review_status = "approved"
    second = msrp_workflow_service.queue_reconciliation_conflicts_for_review(
        "session",
        "Sweden",
        "Volvo",
        "XC60",
        500,
        1.0,
    )

    assert first["schemaVersion"] == "msrp_reconciliation_review_queue_v1"
    assert first["summary"]["conflictGroupCount"] == 1
    assert first["summary"]["reviewCasesQueued"] == 1
    assert first["summary"]["reviewCasesCreated"] == 1
    assert first["summary"]["reviewCasesReused"] == 0
    assert second["summary"]["reviewCasesCreated"] == 0
    assert second["summary"]["reviewCasesReused"] == 1
    assert len(added_cases) == 1
    assert added_cases[0].review_status == "open"
    assert source_a_latest.match_status == "auto_accepted"
    assert len(added_cases[0].candidate_matches_json) == 2
    assert {
        item["candidateType"]
        for item in added_cases[0].candidate_matches_json
    } == {"source_observation"}
    assert [
        item["msrpValue"]
        for item in added_cases[0].candidate_matches_json
    ] == [50000.0, 53000.0]
    assert first["sampleReviewCases"][0]["sourceCode"] == "volvo_primary"
    assert commits == [
        (
            "MSRP reconciliation review case queueing hit a conflict",
            True,
        ),
        (
            "MSRP reconciliation review case queueing hit a conflict",
            True,
        ),
    ]


def test_queue_reconciliation_conflicts_for_review_skips_aligned_groups(
    monkeypatch,
) -> None:
    source_a = uuid4()
    source_b = uuid4()
    now = datetime(2026, 6, 11, 17, 20, tzinfo=timezone.utc)
    observations = [
        _make_reconciliation_observation(
            source_id=source_a,
            source_msrp_value="50000.00",
            msrp_value="50000.00",
            observed_at_utc=now,
        ),
        _make_reconciliation_observation(
            source_id=source_b,
            source_msrp_value="50100.00",
            msrp_value="50100.00",
            observed_at_utc=now,
        ),
    ]
    commits: list[tuple[str, bool]] = []

    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "list_reconciliation_observations",
        lambda *args, **kwargs: observations,
    )
    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "list_sources_by_ids",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(
        msrp_workflow_service.review_repo,
        "get_review_case_by_observation",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("aligned groups must not touch review cases")
        ),
    )
    monkeypatch.setattr(
        msrp_workflow_service,
        "_commit_or_conflict",
        lambda session, detail, *, commit=True: commits.append(
            (detail, commit)
        ),
    )

    payload = msrp_workflow_service.queue_reconciliation_conflicts_for_review(
        "session",
        None,
        None,
        None,
        500,
        1.0,
    )

    assert payload["summary"]["conflictGroupCount"] == 0
    assert payload["summary"]["reviewCasesQueued"] == 0
    assert payload["summary"]["reviewCasesCreated"] == 0
    assert payload["summary"]["reviewCasesReused"] == 0
    assert payload["sampleReviewCases"] == []
    assert commits == [
        (
            "MSRP reconciliation review case queueing hit a conflict",
            True,
        )
    ]


def test_build_price_sales_effectiveness_compares_sales_windows(
    monkeypatch,
) -> None:
    now = datetime(2026, 6, 11, 16, 45, tzinfo=timezone.utc)
    calls: list[dict[str, object]] = []
    fact = pd.DataFrame(
        [
            {"country": "Sweden", "make": "Volvo", "model": "XC60", "period": "2025-12", "sales": 100},
            {"country": "Sweden", "make": "Volvo", "model": "XC60", "period": "2026-01", "sales": 110},
            {"country": "Sweden", "make": "Volvo", "model": "XC60", "period": "2026-02", "sales": 90},
            {"country": "Sweden", "make": "Volvo", "model": "XC60", "period": "2026-04", "sales": 130},
            {"country": "Sweden", "make": "Volvo", "model": "XC60", "period": "2026-05", "sales": 160},
            {"country": "Sweden", "make": "Volvo", "model": "XC60", "period": "2026-06", "sales": 190},
            {"country": "Sweden", "make": "Tesla", "model": "MODEL Y", "period": "2026-04", "sales": 999},
        ]
    )

    monkeypatch.setattr(msrp_workflow_service, "_utc_now", lambda: now)
    monkeypatch.setattr(
        msrp_workflow_service,
        "list_current_price_alerts",
        lambda *args, **kwargs: {
            "rows": 1,
            "total": 1,
            "items": [
                {
                    "country": "Sweden",
                    "brand": "Volvo",
                    "jatoModel": "XC60",
                    "jatoTrim": "Ultra",
                    "direction": "decrease",
                    "changedAtUtc": "2026-03-11T08:00:00+00:00",
                    "deltaMsrpValue": -3000.0,
                    "deltaPct": -5.0,
                }
            ],
        },
    )

    def fake_build_fact_sales_monthly(**kwargs):
        calls.append(kwargs)
        return fact

    monkeypatch.setattr(
        advanced_analysis_service,
        "build_fact_sales_monthly",
        fake_build_fact_sales_monthly,
    )

    payload = msrp_workflow_service.build_price_sales_effectiveness(
        "session",
        "Sweden",
        "Volvo",
        "XC60",
        100,
        3,
        3,
        1,
        1,
    )

    assert calls == [{"country": "Sweden"}]
    assert payload["schemaVersion"] == "msrp_price_sales_effectiveness_v1"
    assert payload["generatedAtUtc"] == now.isoformat()
    assert payload["summary"] == {
        "priceEventCount": 1,
        "analyzedEventCount": 1,
        "labelCounts": {"positive": 1},
        "limit": 100,
    }
    item = payload["items"][0]
    assert item["analysisId"] == "msrp-effectiveness:sweden:volvo:xc60:2026-03"
    assert item["priceEventMonth"] == "2026-03"
    assert item["priceChangeDirection"] == "down"
    assert item["baselineWindowMonths"] == ["2025-12", "2026-01", "2026-02"]
    assert item["postWindowMonths"] == ["2026-04", "2026-05", "2026-06"]
    assert item["baselineAvgSales"] == 100.0
    assert item["postAvgSales"] == 160.0
    assert item["salesDelta"] == 60.0
    assert item["salesDeltaPct"] == 60.0
    assert item["effectivenessLabel"] == "positive"
    assert item["sourcePriceAlert"]["deltaMsrpValue"] == -3000.0


def test_build_price_sales_effectiveness_marks_missing_sales_insufficient(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        msrp_workflow_service,
        "list_current_price_alerts",
        lambda *args, **kwargs: {
            "rows": 1,
            "total": 1,
            "items": [
                {
                    "country": "Sweden",
                    "brand": "Volvo",
                    "jatoModel": "XC60",
                    "direction": "decrease",
                    "changedAtUtc": "2026-03-11T08:00:00+00:00",
                }
            ],
        },
    )
    monkeypatch.setattr(
        advanced_analysis_service,
        "build_fact_sales_monthly",
        lambda **kwargs: pd.DataFrame(
            columns=["country", "make", "model", "period", "sales"]
        ),
    )

    payload = msrp_workflow_service.build_price_sales_effectiveness(
        "session",
        "Sweden",
        "Volvo",
        "XC60",
        100,
    )

    assert payload["summary"]["labelCounts"] == {"insufficient_data": 1}
    item = payload["items"][0]
    assert item["effectivenessLabel"] == "insufficient_data"
    assert item["baselineAvgSales"] is None
    assert item["postAvgSales"] is None


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


def test_record_price_period_replaces_open_period_at_same_timestamp(
    monkeypatch,
) -> None:
    observation = _make_observation()
    open_period = SimpleNamespace(
        msrp_value=Decimal("49000.00"),
        currency="EUR",
        source_msrp_value=Decimal("49000.00"),
        source_currency="EUR",
        valid_from_utc=observation.observed_at_utc,
        valid_to_utc=None,
        started_by_observation_id=uuid4(),
        ended_by_observation_id=uuid4(),
        last_confirmed_at_utc=observation.observed_at_utc,
        last_confirmed_by_observation_id=uuid4(),
    )
    added_periods: list[object] = []

    monkeypatch.setattr(
        msrp_workflow_service.msrp_repo,
        "add_price_history",
        lambda session, price_history: added_periods.append(price_history),
    )

    msrp_workflow_service._record_price_period(
        "session",
        observation,
        open_period=open_period,
    )

    assert added_periods == []
    assert open_period.msrp_value == observation.msrp_value
    assert open_period.source_msrp_value == observation.source_msrp_value
    assert open_period.valid_from_utc == observation.observed_at_utc
    assert open_period.valid_to_utc is None
    assert open_period.started_by_observation_id == observation.observation_id
    assert open_period.ended_by_observation_id is None
    assert (
        open_period.last_confirmed_by_observation_id
        == observation.observation_id
    )


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
