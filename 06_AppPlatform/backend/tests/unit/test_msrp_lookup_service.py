from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

from app.db.models import CurrentPrice
from app.services import msrp_lookup_service


def _make_current_price(
    *,
    model: str = "XC60",
    trim: str = "Ultra",
    powertrain: str = "PHEV",
    value: str = "529900.00",
) -> CurrentPrice:
    return CurrentPrice(
        current_price_id=uuid4(),
        country="瑞典",
        brand="VOLVO",
        jato_model=model,
        jato_trim=trim,
        jato_powertrain=powertrain,
        official_model=model,
        official_trim=trim,
        official_edition=None,
        official_powertrain=powertrain,
        effective_observation_id=uuid4(),
        current_msrp_value=Decimal(value),
        currency="SEK",
        source_msrp_value=Decimal(value),
        source_currency="SEK",
        fx_rate_to_eur=Decimal("0.09100000"),
        fx_rate_as_of_date=date(2026, 4, 18),
        fx_source="unit-test",
        tax_included=True,
        match_confidence=Decimal("0.9500"),
        match_status="human_approved",
        source_url="https://example.test/price",
        source_snapshot_path=None,
        last_price_change_at_utc=datetime(2026, 4, 18, 8, 0, tzinfo=timezone.utc),
        updated_at_utc=datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
    )


def test_lookup_current_msrp_returns_rows_with_source_metadata(monkeypatch) -> None:
    price = _make_current_price()
    observation = SimpleNamespace(
        observation_id=price.effective_observation_id,
        source_id=uuid4(),
    )
    source = SimpleNamespace(
        source_id=observation.source_id,
        source_code="volvo_official_se",
        source_type="official_site",
        tier=1,
    )

    monkeypatch.setattr(
        msrp_lookup_service.msrp_repository,
        "list_current_prices",
        lambda *args, **kwargs: [price],
    )
    monkeypatch.setattr(
        msrp_lookup_service.msrp_repository,
        "list_observations_by_ids",
        lambda *args, **kwargs: [observation],
    )
    monkeypatch.setattr(
        msrp_lookup_service.msrp_repository,
        "list_sources_by_ids",
        lambda *args, **kwargs: [source],
    )

    result = msrp_lookup_service.lookup_current_msrp(
        object(),
        country="瑞典",
        models=["XC60"],
        powertrain="PHEV",
    )

    assert result["matchedModels"] == ["XC60"]
    assert result["modelSummaries"][0]["entryMsrp"] == 529900.0
    assert result["items"][0]["sourceTier"] == 1
    assert result["items"][0]["sourceCode"] == "volvo_official_se"
    assert result["sourceSummary"] == [{"tier": 1, "count": 1}]


def test_lookup_current_msrp_filters_non_matching_powertrain(monkeypatch) -> None:
    phev_price = _make_current_price(powertrain="PHEV")
    ice_price = _make_current_price(
        powertrain="ICE",
        trim="Core",
        value="489900.00",
    )

    monkeypatch.setattr(
        msrp_lookup_service.msrp_repository,
        "list_current_prices",
        lambda *args, **kwargs: [ice_price, phev_price],
    )
    monkeypatch.setattr(
        msrp_lookup_service.msrp_repository,
        "list_observations_by_ids",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(
        msrp_lookup_service.msrp_repository,
        "list_sources_by_ids",
        lambda *args, **kwargs: [],
    )

    result = msrp_lookup_service.lookup_current_msrp(
        object(),
        country="瑞典",
        models=["XC60"],
        powertrain="PHEV",
    )

    assert len(result["items"]) == 1
    assert result["items"][0]["powertrain"] == "PHEV"
