from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import analysis as analysis_routes
from app.api.routes import assistant as assistant_routes
from app.api.routes import market_scan as market_scan_routes
from app.api.routes import metadata as metadata_routes
from app.core.config import API_PREFIX
from app.services.query_service import (
    DashboardOverviewQueryResult,
    GroupedTimeSeriesQueryResult,
)


@pytest.fixture
def client() -> Iterator[TestClient]:
    app = FastAPI()
    app.include_router(analysis_routes.router, prefix=API_PREFIX)
    app.include_router(assistant_routes.router, prefix=API_PREFIX)
    app.include_router(market_scan_routes.router, prefix=API_PREFIX)
    app.include_router(metadata_routes.router, prefix=API_PREFIX)
    with TestClient(app) as test_client:
        yield test_client


def _market_scan_contract_payload() -> dict[str, Any]:
    delta = {"value": 0.12, "display": "+12.0%", "tone": "positive"}
    data_quality = {
        "requestedCountry": "Sweden",
        "resolvedCountry": "Sweden",
        "resolvedCountryLabel": "Sweden",
        "countryFallbackApplied": False,
        "requestedPeriod": "2026-03",
        "normalizedRequestedPeriod": "2026-03",
        "resolvedPeriod": "2026-03",
        "periodFallbackApplied": False,
        "requestedTimeRange": {"start": "2026-01", "end": "2026-03"},
        "resolvedTimeRange": {"start": "2026-01", "end": "2026-03"},
        "timeRangeFallbackApplied": False,
        "requestedFuelTypes": ["PHEV"],
        "resolvedFuelTypes": ["PHEV"],
        "unavailableFuelTypes": [],
        "fuelFallbackApplied": False,
        "fuelRowsExcluded": 0,
        "totalSourceRows": 1,
        "filteredSourceRows": 1,
        "warnings": [],
    }
    ranking_item = {
        "rank": 1,
        "brand": "VOLVO",
        "model": "XC60",
        "volume": 1200,
        "sharePct": 0.18,
        "shareDisplay": "18.0%",
        "driveSharePct": 0.52,
        "driveShareDisplay": "52.0%",
        "yoy": delta,
        "mom": {"value": 0.04, "display": "+4.0%", "tone": "positive"},
        "barPct": 1.0,
        "fuelMix": {"PHEV": 1200},
        "driveMix": {"4WD": 624, "2WD": 576},
        "registrationMix": {"Business": 700, "Private": 450, "Other": 50},
    }
    ranking_group = {"title": "Top brands", "items": [ranking_item]}
    matrix = {
        "columns": ["currentMonth"],
        "rows": [
            {
                "metricKey": "volume",
                "label": "Volume",
                "cells": [{"key": "currentMonth", "value": 1200, "display": "1,200", "tone": "neutral"}],
            }
        ],
    }
    fuel_trend = {"items": [{"label": "26.03", "totalVolume": 1200, "fuelMix": {"PHEV": 1200}}]}
    fuel_panel = {
        "fuelType": "PHEV",
        "monthTitle": "PHEV month",
        "ytdTitle": "PHEV YTD",
        "rolling12Title": "PHEV rolling 12",
        "monthRanking": [ranking_item],
        "ytdRanking": [ranking_item],
        "rolling12Ranking": [ranking_item],
    }
    drilldown = {
        "segment": "SUV-B",
        "segmentLabel": "SUV-B",
        "title": "SUV-B",
        "summaryText": "SUV-B summary",
        "monthTotalRanking": ranking_group,
        "totalRanking": ranking_group,
        "rolling12TotalRanking": ranking_group,
        "monthFuelTrend": fuel_trend,
        "ytdFuelTrend": fuel_trend,
        "rolling12FuelTrend": fuel_trend,
        "fuelPanels": [fuel_panel],
    }
    return {
        "metadata": {
            "protocolVersion": "market-scan/v1",
            "requestedPeriod": "2026-03",
            "resolvedPeriod": "2026-03",
            "selectedTimeRange": {"start": "2026-01", "end": "2026-03"},
            "customRangeActive": True,
            "latestPeriod": "2026-03",
            "priorPeriod": "2026-02",
            "sameMonthLastYearPeriod": "2025-03",
            "selectedCountry": "Sweden",
            "selectedCountryLabel": "Sweden",
            "selectedFuelTypes": ["PHEV"],
            "selectedDrilldownSegment": "SUV-B",
            "dataQuality": data_quality,
            "availableCountries": [{"value": "Sweden", "label": "Sweden"}],
            "availablePeriods": [{"value": "2026-03", "label": "26.03"}],
            "availableFuelTypes": ["PHEV"],
            "availableSegments": [{"value": "SUV-B", "label": "SUV-B"}],
            "labels": {
                "pageTitle": "Sweden 2026-03 Market Scan",
                "currentMonthShort": "26.03",
                "previousMonthShort": "26.02",
                "sameMonthLastYearShort": "25.03",
                "currentYtd": "2026 YTD",
                "priorYtd": "2025 YTD",
                "ytdWindow": "Jan-Mar",
            },
        },
        "dataQuality": data_quality,
        "results": {
            "overview": {
                "summary": {
                    "headline": "Sweden total market",
                    "subheadline": "Stable",
                    "currentMonthVolume": 1200,
                    "currentMonthYoY": delta,
                    "rolling12Volume": 14200,
                    "rolling12YoY": delta,
                    "ytdVolume": 3600,
                    "ytdYoY": delta,
                    "customRangeVolume": 3600,
                    "customRangeYoY": delta,
                    "customRangeLabel": "2026-01 ~ 2026-03",
                },
                "trend": {
                    "periods": ["2026-03"],
                    "items": [
                        {
                            "period": "2026-03",
                            "label": "26.03",
                            "totalVolume": 1200,
                            "fuelMix": {"PHEV": 1200},
                            "mom": delta,
                            "yoy": delta,
                        }
                    ],
                },
                "ytdBrandRanking": ranking_group,
                "rolling12BrandRanking": ranking_group,
                "monthlyBrandRanking": ranking_group,
                "customRangeBrandRanking": ranking_group,
            },
            "origin": {
                "summaryText": "Origin summary",
                "trend": {"series": [{"origin": "Europe", "points": [{"period": "2026-03", "label": "26.03", "volume": 1200, "sharePct": 1.0}]}]},
                "brandTrend": {"groups": [{"origin": "Europe", "series": [{"brand": "VOLVO", "points": [{"period": "2026-03", "label": "26.03", "volume": 1200, "sharePct": 1.0}]}]}]},
                "matrix": matrix,
            },
            "segment": {
                "summaryText": "Segment summary",
                "matrix": matrix,
                "bodyShareTrend": {"items": [{"period": "2026-03", "label": "26.03", "totalVolume": 1200, "suvSharePct": 1.0, "sedanSharePct": 0.0}]},
                "suvSegmentShareTrend": {"items": [{"period": "2026-03", "label": "26.03", "totalVolume": 1200, "segmentSharePct": {"SUV-B": 1.0}}]},
                "channelMix": {
                    "options": [{"value": "overall", "label": "Overall"}],
                    "month": {"title": "Month", "items": [{"label": "Overall", "volume": 1200, "channelMix": {"Business": 700}, "channelSharePct": {"Business": 0.58}}]},
                    "ytd": {"title": "YTD", "items": []},
                    "rolling12": {"title": "Rolling 12", "items": []},
                    "customRange": {"title": "Custom", "items": []},
                },
            },
            "drilldown": drilldown,
            "suvAll": drilldown,
            "suvA": drilldown,
            "suvB": drilldown,
        },
    }


def test_market_scan_deck_contract_carries_custom_range(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_query_market_scan_deck(**kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return _market_scan_contract_payload()

    monkeypatch.setattr(
        market_scan_routes,
        "query_market_scan_deck",
        fake_query_market_scan_deck,
    )

    response = client.post(
        "/v1/market-scan/deck",
        json={
            "country": "Sweden",
            "target_period": "2026-03",
            "time_range": {"start": "2026-01", "end": "2026-03"},
            "fuel_types": ["PHEV"],
            "ranking_limit": 10,
            "drilldown_segment": "SUV-B",
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert captured["country"] == "Sweden"
    assert captured["time_range"] == {"start": "2026-01", "end": "2026-03"}
    assert data["metadata"]["protocolVersion"] == "market-scan/v1"
    assert data["metadata"]["customRangeActive"] is True
    assert data["dataQuality"]["warnings"] == []
    assert data["metadata"]["dataQuality"]["resolvedTimeRange"] == {"start": "2026-01", "end": "2026-03"}
    assert set(data["results"]) == {
        "overview",
        "origin",
        "segment",
        "drilldown",
        "suvAll",
        "suvA",
        "suvB",
    }
    assert "rolling12BrandRanking" in data["results"]["overview"]
    assert "channelMix" in data["results"]["segment"]


def test_market_scan_deck_rejects_invalid_ranking_limit(client: TestClient) -> None:
    response = client.post("/v1/market-scan/deck", json={"ranking_limit": 9})

    assert response.status_code == 422


def test_filter_metadata_snapshot_contract(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        metadata_routes,
        "metadata_filter_snapshot",
        lambda: {
            "columns": ["Country", "Powertrain"],
            "options": {
                "Country": ["Sweden"],
                "Powertrain": ["BEV"],
            },
        },
    )

    response = client.get("/v1/metadata/filter-snapshot")

    assert response.status_code == 200
    assert response.json() == {
        "columns": ["Country", "Powertrain"],
        "options": {
            "Country": ["Sweden"],
            "Powertrain": ["BEV"],
        },
    }
    assert "stale-while-revalidate" in response.headers["cache-control"]
    assert response.headers["etag"].startswith('W/"metadata-filter-snapshot-')


def test_analysis_query_contract(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_query_analysis(
        filters: dict[str, list[str]],
        group_by: str | None,
        metric_candidates: list[str],
        top_n: int,
        prefer_precomputed: bool,
    ) -> dict[str, Any]:
        captured.update(
            {
                "filters": filters,
                "group_by": group_by,
                "metric_candidates": metric_candidates,
                "top_n": top_n,
                "prefer_precomputed": prefer_precomputed,
            }
        )
        return {
            "route": "dynamic-aggregate",
            "groupBy": group_by,
            "rows": 1,
            "items": [{"Brand": "VOLVO", "Sales": 1200}],
        }

    monkeypatch.setattr(analysis_routes, "query_analysis", fake_query_analysis)

    response = client.post(
        "/v1/analysis/query",
        json={
            "filters": {"Country": ["Sweden"]},
            "group_by": "Brand",
            "metric_candidates": ["Sales"],
            "top_n": 5,
            "prefer_precomputed": False,
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert captured == {
        "filters": {"Country": ["Sweden"]},
        "group_by": "Brand",
        "metric_candidates": ["Sales"],
        "top_n": 5,
        "prefer_precomputed": False,
    }
    assert data["route"] == "dynamic-aggregate"
    assert data["rows"] == 1
    assert data["items"][0]["Brand"] == "VOLVO"


def test_grouped_time_series_exposes_server_cache_header(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_query_grouped_time_series_with_cache_state(**kwargs: Any):
        captured.update(kwargs)
        return GroupedTimeSeriesQueryResult(
            payload={
                "grain": "year",
                "rows": 1,
                "items": [{"time": "2024", "value": 1200, "series": "VOLVO"}],
            },
            cache_state="MEMORY",
        )

    monkeypatch.setattr(
        analysis_routes,
        "query_grouped_time_series_with_cache_state",
        fake_query_grouped_time_series_with_cache_state,
    )

    response = client.post(
        "/v1/analysis/time-series-grouped",
        json={
            "filters": {"Country": ["Sweden"]},
            "grain": "year",
            "group_by": "Brand",
            "top_n": 5,
            "include_others": False,
        },
    )

    assert response.status_code == 200
    assert response.headers["x-jato-server-cache"] == "MEMORY"
    assert response.json()["rows"] == 1
    assert captured["filters"] == {"Country": ["Sweden"]}
    assert captured["grain"] == "year"
    assert captured["group_by"] == "Brand"


def test_overview_exposes_server_cache_header(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_query_overview_with_cache_state(**kwargs: Any):
        captured.update(kwargs)
        return DashboardOverviewQueryResult(
            payload={
                "route": "dynamic-aggregate",
                "kpis": {"totalRows": 10},
                "monthSeries": [],
                "yearSeries": [],
            },
            cache_state="DISK",
        )

    monkeypatch.setattr(
        analysis_routes,
        "query_overview_with_cache_state",
        fake_query_overview_with_cache_state,
    )

    response = client.post(
        "/v1/analysis/overview",
        json={
            "filters": {"Country": ["Sweden"]},
            "prefer_precomputed": True,
            "top_n": 10,
        },
    )

    assert response.status_code == 200
    assert response.headers["x-jato-server-cache"] == "DISK"
    assert response.json()["kpis"]["totalRows"] == 10
    assert captured["filters"] == {"Country": ["Sweden"]}
    assert captured["prefer_precomputed"] is True
    assert captured["top_n"] == 10
    assert captured["cache_scope"] == "admin"


def test_country_chat_contract(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_answer_country_question(
        *,
        country: str,
        question: str,
        history: list[dict[str, Any]],
        news_payload_override: dict[str, Any] | None,
        chat_model: str | None,
    ) -> dict[str, Any]:
        captured.update(
            {
                "country": country,
                "question": question,
                "history": history,
                "news_payload_override": news_payload_override,
                "chat_model": chat_model,
            }
        )
        return {
            "country": country,
            "question": question,
            "answer": "Sweden PHEV demand is stable.",
            "intent": "market_overview",
            "primaryIntent": "market_overview",
            "focusedIntents": ["market_overview"],
            "intentRoute": "market-overview",
            "provider": "stub",
            "model": chat_model,
            "providerAvailable": True,
            "contextSnapshot": {
                "country": country,
                "route": "dynamic-aggregate",
                "kpis": {
                    "totalRows": 1,
                    "countryCount": 1,
                    "brandCount": 1,
                    "modelCount": 1,
                    "versionCount": 1,
                    "cumulativeSales": 1200,
                },
                "yearSeries": [{"time": "2026", "value": 1200}],
                "monthSeries": [{"time": "2026-03", "value": 1200}],
                "topBrands": [{"label": "VOLVO", "value": 1200}],
                "topModels": [{"label": "XC60", "value": 1200}],
                "powertrainMix": [{"label": "PHEV", "value": 1200}],
            },
            "suggestedPrompts": ["Show PHEV mix"],
            "chartLinks": [{"label": "Market Scan", "href": "/market-scan?country=Sweden"}],
        }

    monkeypatch.setattr(
        assistant_routes,
        "answer_country_question",
        fake_answer_country_question,
    )

    response = client.post(
        "/v1/assistant/country/chat",
        json={
            "country": "Sweden",
            "question": "What changed in March?",
            "history": [{"role": "user", "content": "Look at PHEV"}],
            "refresh_news": False,
            "model": "stub-model",
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert captured["country"] == "Sweden"
    assert captured["history"] == [{"role": "user", "content": "Look at PHEV", "extracted_params": {}, "intent_route": None}]
    assert captured["chat_model"] == "stub-model"
    assert data["answer"]
    assert data["contextSnapshot"]["kpis"]["cumulativeSales"] == 1200
    assert data["suggestedPrompts"] == ["Show PHEV mix"]


def test_frontend_api_client_contract_paths_stay_in_sync() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    api_client = repo_root / "06_AppPlatform" / "frontend" / "src" / "api" / "client.ts"
    type_index = repo_root / "06_AppPlatform" / "frontend" / "src" / "types" / "index.ts"
    country_chat_types = repo_root / "06_AppPlatform" / "frontend" / "src" / "types" / "countryChat.ts"

    api_text = api_client.read_text(encoding="utf-8")
    index_text = type_index.read_text(encoding="utf-8")
    country_chat_text = country_chat_types.read_text(encoding="utf-8")

    assert '"/market-scan/deck"' in api_text
    assert '"/analysis/query"' in api_text
    assert '"/assistant/country/chat"' in api_text
    assert "interface MarketScanDeckResponse" in index_text
    assert "interface MarketScanDeckRequest" in index_text
    assert "interface AnalysisQuery" in index_text
    assert "interface CountryChatResponse" in country_chat_text
