import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from app.services import evkx_import_service


def _write_catalog(tmp_path: Path) -> Path:
    payload = {
        "metadata": {
            "pricingCountry": "UnitedStates",
            "availabilityFilter": "current",
        },
        "records": [
            {
                "evId": "07828991",
                "name": "Acura ZDX A-Spec AWD",
                "infoUrl": "https://evkx.net/models/acura/zdx/zdx_a-spec_awd/",
                "thumbnailUrl": "https://media.evkx.net/acura.webp",
                "pricingCountry": "UnitedStates",
                "startPrice": 68500,
                "currency": "USD",
                "isConverted": False,
                "searchSummary": {
                    "modelYear": ["MY2024"],
                    "pricingCountry": "UnitedStates",
                },
                "pricingByMarket": [
                    {
                        "marketLabel": "USA",
                        "priceText": "$68,500",
                        "amount": 68500,
                        "currency": "USD",
                    }
                ],
                "specifications": {
                    "Performance": {"Peak power": "365 kW"},
                    "Battery & Charging": {
                        "Battery net": "107.5 kWh",
                        "Max DC charging": "190 kW",
                    },
                    "Range & Consumption": {
                        "EPA range Learn more": "304 mi",
                    },
                },
                "schemaOrg": [
                    {
                        "@type": "BreadcrumbList",
                        "itemListElement": [
                            {"position": 3, "name": "Acura"},
                            {"position": 4, "name": "ZDX"},
                            {"position": 5, "name": "ZDX A-Spec AWD"},
                        ],
                    },
                    {
                        "@type": "Car",
                        "brand": {"name": "Acura"},
                        "bodyType": "SUV",
                        "vehicleModelDate": "2024",
                    },
                ],
            },
            {
                "evId": "skip-me",
                "name": "Acura No US Price",
                "pricingCountry": "China",
                "pricingByMarket": [
                    {
                        "marketLabel": "China",
                        "amount": 100000,
                        "currency": "CNY",
                    }
                ],
                "schemaOrg": [],
            },
        ],
    }
    path = tmp_path / "evkx.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_build_evkx_scrape_batch_payload_builds_review_rows(
    monkeypatch,
    tmp_path: Path,
) -> None:
    catalog_path = _write_catalog(tmp_path)
    source = SimpleNamespace(source_id=uuid4())
    candidate = SimpleNamespace(
        current_price_id=uuid4(),
        jato_model="ZDX",
        jato_trim="A-Spec AWD",
        jato_powertrain="BEV",
        official_model="ZDX",
        official_trim="A-Spec AWD",
        official_edition=None,
        official_powertrain="BEV",
        current_msrp_value=Decimal("68500.00"),
        currency="USD",
        updated_at_utc=None,
    )

    monkeypatch.setattr(
        evkx_import_service.msrp_repository,
        "get_source_by_code",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        evkx_import_service.msrp_repository,
        "add_source",
        lambda session, created_source: created_source,
    )

    def _execute(stmt):
        class _Result:
            def scalars(self):
                class _Scalars:
                    def all(self):
                        return [candidate]

                return _Scalars()

        return _Result()

    session = SimpleNamespace(
        flush=lambda: None,
        execute=lambda stmt: _execute(stmt),
    )

    monkeypatch.setattr(
        evkx_import_service,
        "_ensure_source",
        lambda *args, **kwargs: source,
    )

    payload = evkx_import_service.build_evkx_scrape_batch_payload(
        session,
        catalog_path,
    )

    assert payload["scope_country"] == "United States"
    assert payload["scope_brands"] == ["Acura"]
    assert payload["failed_count"] == 1
    assert len(payload["observations"]) == 1
    observation = payload["observations"][0]
    assert observation["brand"] == "Acura"
    assert observation["jato_model"] == "ZDX"
    assert observation["jato_trim"] == "A-Spec AWD"
    assert observation["jato_powertrain"] == "BEV"
    assert observation["match_status"] == "review_required"
    assert observation["candidate_matches_json"][0]["score"] >= 0.9
    assert observation["source_context_json"]["selectedMarketPrice"]["marketLabel"] == "United States"
    assert observation["match_reason_json"]["confidenceRule"]["mode"] == evkx_import_service.EVKX_CONFIDENCE_PROFILE


def test_import_evkx_catalog_file_calls_batch_ingest(
    monkeypatch,
    tmp_path: Path,
) -> None:
    catalog_path = _write_catalog(tmp_path)

    monkeypatch.setattr(
        evkx_import_service,
        "build_evkx_scrape_batch_payload",
        lambda *args, **kwargs: {
            "batch_code": "evkx-united-states-20260417010101",
            "trigger_type": "manual_evkx_import",
            "scope_country": "United States",
            "scope_brands": ["Acura"],
            "failed_count": 1,
            "notes": "test import",
            "started_at_utc": "2026-04-17T01:01:01+00:00",
            "finished_at_utc": "2026-04-17T01:01:01+00:00",
            "observations": [{"brand": "Acura"}],
            "skipped": [{"evId": "skip-me"}],
            "targetCountry": "United States",
        },
    )
    captured = {}

    def _create_scrape_batch_ingest(session, payload):
        captured["payload"] = payload
        return {"scrapeBatch": {"batchCode": payload["batch_code"]}}

    monkeypatch.setattr(
        evkx_import_service,
        "create_scrape_batch_ingest",
        _create_scrape_batch_ingest,
    )

    result = evkx_import_service.import_evkx_catalog_file(
        SimpleNamespace(),
        catalog_path,
    )

    assert result["dryRun"] is False
    assert result["observationCount"] == 1
    assert result["skippedCount"] == 1
    assert result["ingestResult"]["scrapeBatch"]["batchCode"] == "evkx-united-states-20260417010101"
    assert "skipped" not in captured["payload"]
    assert "targetCountry" not in captured["payload"]
