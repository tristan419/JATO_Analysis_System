import pandas as pd
import pytest

from app.services import country_chat_service


# --------------- helpers ---------------

_STUB_OVERVIEW = {
    "route": "dynamic-aggregate",
    "kpis": {
        "brandCount": 3,
        "modelCount": 5,
        "versionCount": 6,
        "cumulativeSales": 260000,
    },
    "yearSeries": [{"time": "2025", "value": 123000}],
    "monthSeries": [{"time": "2025 12", "value": 10000}],
}

_STUB_VEHICLE_FRAME = pd.DataFrame(
    {
        "Brand": ["VOLVO", "VOLVO", "MERCEDES", "MERCEDES", "MERCEDES", "VOLKSWAGEN"],
        "Model": ["XC60", "XC40", "GLC", "C-CLASS", "A-CLASS", "ID.4"],
        "Powertrain": ["PHEV", "BEV", "ICE", "MHEV", "PHEV", "BEV"],
        "Sales": [80000, 30000, 25000, 20000, 14000, 60000],
    }
)

_STUB_DECK = {
    "metadata": {
        "resolvedPeriod": "2025-02",
        "labels": {"pageTitle": "Sweden - Feb 2025"},
    },
    "results": {
        "overview": {
            "summary": {"totalVolume": 260000},
            "ytdBrandRanking": [
                {"brand": "VOLVO", "volume": 110000, "share": 42.3, "ytdYoy": 5.1},
            ],
            "monthlyBrandRanking": [],
        },
        "origin": {
            "summaryText": "欧系品牌占比 68%。",
            "matrix": {},
        },
        "segment": {
            "matrix": {
                "rows": [
                    {"segment": "SUV-B", "currentMonth": 5000},
                    {"segment": "SUV-C", "currentMonth": 4200},
                ],
            },
            "bodyShareTrend": [],
        },
        "drilldown": {"segment": "SUV-B", "totalRanking": [], "ytdFuelTrend": []},
        "suvA": {"segment": "SUV-A", "totalRanking": [], "ytdFuelTrend": []},
    },
}


@pytest.fixture()
def _patch_base(monkeypatch):
    """Patch shared dependencies for build_country_snapshot."""
    monkeypatch.setattr(country_chat_service, "_resolve_country_column", lambda: "国家")
    monkeypatch.setattr(
        country_chat_service.query_service, "query_overview", lambda **kw: _STUB_OVERVIEW,
    )
    monkeypatch.setattr(
        country_chat_service.query_service,
        "_build_vehicle_frame",
        lambda filters: _STUB_VEHICLE_FRAME.copy(),
    )
    monkeypatch.setattr(
        country_chat_service.market_scan_service,
        "query_market_scan_deck",
        lambda **kw: _STUB_DECK,
    )


# --------------- snapshot tests ---------------


@pytest.mark.usefixtures("_patch_base")
def test_build_country_snapshot_aggregates_rankings_by_sales() -> None:
    snapshot = country_chat_service.build_country_snapshot("瑞典")

    assert snapshot["topBrands"][0] == {"label": "VOLVO", "value": 110000}
    assert snapshot["topBrands"][1] == {"label": "VOLKSWAGEN", "value": 60000}
    assert snapshot["topBrands"][2] == {"label": "MERCEDES", "value": 59000}
    assert snapshot["topModels"][0] == {"label": "XC60", "value": 80000}
    assert snapshot["powertrainMix"][0] == {"label": "PHEV", "value": 94000}


@pytest.mark.usefixtures("_patch_base")
def test_snapshot_includes_market_scan_panels() -> None:
    snapshot = country_chat_service.build_country_snapshot("瑞典")

    assert snapshot["periodLabel"] == "Sweden - Feb 2025"
    assert snapshot["ytdBrandRanking"][0]["brand"] == "VOLVO"
    assert snapshot["originAnalysis"]["summaryText"] == "欧系品牌占比 68%。"
    assert len(snapshot["segmentMatrix"]["rows"]) == 2


@pytest.mark.usefixtures("_patch_base")
def test_snapshot_survives_deck_failure(monkeypatch) -> None:
    monkeypatch.setattr(
        country_chat_service.market_scan_service,
        "query_market_scan_deck",
        lambda **kw: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    snapshot = country_chat_service.build_country_snapshot("瑞典")

    assert "topBrands" in snapshot
    assert "ytdBrandRanking" not in snapshot


def test_nvidia_provider_available_when_key_present(monkeypatch) -> None:
    monkeypatch.setenv("NVIDIA_API_KEY", "secret")
    monkeypatch.delenv("NVAPI_KEY", raising=False)

    assert country_chat_service._nvidia_provider_available() is True


def test_nvidia_provider_unavailable_without_keys(monkeypatch) -> None:
    monkeypatch.delenv("NVIDIA_API_KEY", raising=False)
    monkeypatch.delenv("NVAPI_KEY", raising=False)

    assert country_chat_service._nvidia_provider_available() is False


# --------------- intent classification ---------------

@pytest.mark.parametrize(
    "question,expected",
    [
        ("品牌排名怎么样？", "brand-ranking"),
        ("哪个品牌最强", "brand-ranking"),
        ("SUV segment 分析", "segment-analysis"),
        ("分析一下细分市场", "segment-analysis"),
        ("日系品牌表现如何", "origin-analysis"),
        ("中系车在欧洲表现怎么样", "origin-analysis"),
        ("BEV 渗透率", "nev-analysis"),
        ("新能源份额多少", "nev-analysis"),
        ("动力结构", "powertrain-mix"),
        ("价格走势", "pricing-summary"),
        ("趋势如何", "trend-summary"),
        ("丰田和大众对比", "competitive"),
        ("你好", "general-summary"),
    ],
)
def test_infer_intent(question, expected) -> None:
    assert country_chat_service.infer_country_chat_intent(question) == expected


# --------------- context selection ---------------

@pytest.mark.usefixtures("_patch_base")
def test_context_for_segment_intent_includes_matrix() -> None:
    snapshot = country_chat_service.build_country_snapshot("瑞典")
    ctx = country_chat_service._select_context_for_intent(snapshot, "segment-analysis")

    assert "segmentMatrix" in ctx
    assert "suvSedanTrend" in ctx
    # should NOT include ytdBrandRanking for segment intent
    assert "ytdBrandRanking" not in ctx


@pytest.mark.usefixtures("_patch_base")
def test_context_for_brand_intent_includes_rankings() -> None:
    snapshot = country_chat_service.build_country_snapshot("瑞典")
    ctx = country_chat_service._select_context_for_intent(snapshot, "brand-ranking")

    assert "ytdBrandRanking" in ctx
    assert "topBrands" in ctx
