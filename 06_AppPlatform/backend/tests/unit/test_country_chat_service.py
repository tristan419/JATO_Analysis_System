import copy
import json

import pandas as pd
import pytest

from app.services import country_chat_service
from app.services import country_chat_models
from app.services import country_profiles
from app.services import engineering_variant_diff_service
from app.services import insight_card_service
from app.services import local_wiki_service
from app.services import msrp_lookup_service


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
        "drilldown": {
            "segment": "SUV-B",
            "segmentLabel": "SUV-B",
            "summaryText": "SUV-B 由 VOLVO XC60 领跑。",
            "totalRanking": {
                "items": [
                    {
                        "model": "XC60",
                        "volume": 80000,
                        "shareDisplay": "32.0%",
                        "yoy": {"display": "+5.0%"},
                        "fuelMix": {"BEV": 0, "PHEV": 80000},
                        "driveMix": {"4WD": 48000, "2WD": 32000, "OTHER": 0},
                        "registrationMix": {"Business": 52000, "Private": 26000, "Other": 2000},
                    },
                    {
                        "model": "GLC",
                        "volume": 25000,
                        "shareDisplay": "10.0%",
                        "yoy": {"display": "+2.0%"},
                        "fuelMix": {"ICE": 25000},
                        "driveMix": {"4WD": 12500, "2WD": 12500, "OTHER": 0},
                        "registrationMix": {"Business": 12000, "Private": 11000, "Other": 2000},
                    },
                ],
            },
            "ytdFuelTrend": [],
            "fuelPanels": [
                {
                    "fuelType": "PHEV",
                    "ytdRanking": [
                        {
                            "model": "XC60",
                            "volume": 80000,
                            "shareDisplay": "32.0%",
                            "registrationMix": {"Business": 52000, "Private": 26000, "Other": 2000},
                            "driveMix": {"4WD": 48000, "2WD": 32000, "OTHER": 0},
                        },
                    ],
                    "monthRanking": [],
                },
                {
                    "fuelType": "ICE",
                    "ytdRanking": [
                        {
                            "model": "GLC",
                            "volume": 25000,
                            "shareDisplay": "10.0%",
                            "registrationMix": {"Business": 12000, "Private": 11000, "Other": 2000},
                            "driveMix": {"4WD": 12500, "2WD": 12500, "OTHER": 0},
                        },
                    ],
                    "monthRanking": [],
                },
            ],
        },
        "suvA": {
            "segment": "SUV-A",
            "segmentLabel": "SUV-A",
            "summaryText": "EX40 目前领跑 SUV-A。",
            "totalRanking": {
                "items": [
                    {
                        "model": "EX40",
                        "volume": 18000,
                        "shareDisplay": "28.0%",
                        "yoy": {"display": "+12.0%"},
                        "fuelMix": {"BEV": 18000},
                        "driveMix": {"4WD": 12000, "2WD": 6000},
                        "registrationMix": {"Business": 9000, "Private": 8500, "Other": 500},
                    },
                    {
                        "model": "E-2008",
                        "volume": 7000,
                        "shareDisplay": "10.9%",
                        "yoy": {"display": "+4.0%"},
                        "fuelMix": {"BEV": 7000},
                        "driveMix": {"2WD": 7000},
                        "registrationMix": {"Business": 2500, "Private": 4300, "Other": 200},
                    },
                ],
            },
            "ytdFuelTrend": [],
            "fuelPanels": [
                {
                    "fuelType": "BEV",
                    "ytdRanking": [
                        {
                            "model": "EX40",
                            "volume": 18000,
                            "shareDisplay": "28.0%",
                            "registrationMix": {"Business": 9000, "Private": 8500, "Other": 500},
                            "driveMix": {"4WD": 12000, "2WD": 6000},
                        }
                    ],
                    "monthRanking": [],
                }
            ],
        },
    },
}

_STUB_INSIGHT_CARDS = [
    {
        "id": "overview-瑞典",
        "country": "瑞典",
        "period": "2025-02",
        "category": "overview",
        "title": "市场总量概览",
        "conclusion": "瑞典累计销量 260,000，同比上升。",
        "supportingData": {"totalVolume": 260000},
        "tone": "positive",
        "relatedChartLink": "/market-scan?country=瑞典&activePage=overview",
        "tags": ["overview", "volume"],
    },
    {
        "id": "segment-瑞典",
        "country": "瑞典",
        "period": "2025-02",
        "category": "segment",
        "title": "细分市场结构",
        "conclusion": "SUV-B 当月销量领先。",
        "supportingData": {},
        "tone": "neutral",
        "relatedChartLink": "/market-scan?country=瑞典&activePage=segment",
        "tags": ["segment"],
    },
]

_STUB_ADVANCED_CHARTS = {
    ("price_value", "length_vs_price"): {
        "items": [{"label": "4000-4500", "value": 120}],
        "rows": 1,
    },
    ("nev_analysis", "nev_range_distribution"): {
        "items": [{"label": "500-600km", "value": 80}],
        "rows": 1,
    },
    ("market_structure", "segment_share"): {
        "items": [{"label": "SUV-C", "value": 45}],
        "rows": 1,
    },
    ("price_value", "powertrain_vs_price"): {
        "items": [{"powertrain": "BEV", "avgMsrp": 42000}],
        "rows": 1,
    },
    ("market_structure", "segment_share_by_length"): {
        "items": [{"segment": "SUV-C", "share": 32}],
        "rows": 1,
    },
    ("market_structure", "powertrain_bubble"): {
        "items": [{"brand": "VOLVO", "sales": 110000, "bevShare": 0.41}],
        "rows": 1,
    },
    ("price_value", "price_migration"): {
        "items": [{"time": "2025-12", "value": 43000}],
        "rows": 1,
    },
    ("time_insight", "seasonality_heatmap"): {
        "items": [
            {"year": "2024", "month": "Jan", "value": 100},
            {"year": "2025", "month": "Jan", "value": 120},
        ],
        "rows": 2,
    },
    ("cost_analysis", "estimated_tco"): {
        "items": [{"powertrain": "BEV", "tco": 0.42}],
        "rows": 1,
    },
    ("nev_analysis", "nev_capacity_vs_msrp"): {
        "items": [{"model": "XC40", "batteryKwh": 77, "msrp": 45000}],
        "rows": 1,
    },
    ("price_value", "price_per_meter"): {
        "items": [{"model": "XC60", "pricePerMeter": 9.6}],
        "rows": 1,
    },
    ("price_value", "sales_vs_price"): {
        "items": [{"model": "XC60", "sales": 80000, "msrp": 45000}],
        "rows": 1,
    },
}


def _build_stub_news_payload(country: str) -> dict[str, object]:
    normalized = str(country).strip().lower()
    if normalized in {"火星", "mars"}:
        return {
            "countryCode": None,
            "countryLabel": str(country).strip(),
            "marketEvents": [],
            "newsDigest": None,
        }

    code = "DE" if normalized in {"germany", "德国"} else "SE"
    label = "Germany / 德国" if code == "DE" else "Sweden / 瑞典"
    english_name = label.split("/", 1)[0].strip()
    market_events = [
        {
            "sourceCode": f"{code.lower()}_google_auto_market",
            "countryCode": code,
            "countryLabel": label,
            "publisher": "Reuters",
            "title": f"{english_name} reviews EV company-car tax support",
            "summary": "Fleet incentives remain central for electrification uptake.",
            "url": f"https://example.com/{code.lower()}/tax-support",
            "publishedAt": "2026-04-15T08:00:00+00:00",
            "tags": ["market", "policy", "automotive"],
        },
        {
            "sourceCode": f"{code.lower()}_google_auto_market",
            "countryCode": code,
            "countryLabel": label,
            "publisher": "Automotive News Europe",
            "title": f"Chinese-brand pricing pressure rises in {english_name}",
            "summary": "Competitive pricing is reshaping mainstream EV comparisons.",
            "url": f"https://example.com/{code.lower()}/pricing-pressure",
            "publishedAt": "2026-04-14T07:30:00+00:00",
            "tags": ["market", "competition", "automotive"],
        },
    ]
    return {
        "countryCode": code,
        "countryLabel": label,
        "marketEvents": market_events,
        "newsDigest": {
            "countryCode": code,
            "countryLabel": label,
            "articleCount": len(market_events),
            "updatedAt": market_events[0]["publishedAt"],
            "headline": f"2026-04-15 Reuters: {english_name} reviews EV company-car tax support",
            "summary": (
                f"2026-04-15 Reuters: {english_name} reviews EV company-car tax support；"
                f"2026-04-14 Automotive News Europe: Chinese-brand pricing pressure rises in {english_name}"
            ),
            "highlights": [
                f"2026-04-15 Reuters: {english_name} reviews EV company-car tax support",
                f"2026-04-14 Automotive News Europe: Chinese-brand pricing pressure rises in {english_name}",
            ],
            "stale": False,
        },
    }


@pytest.fixture()
def _patch_base(monkeypatch):
    """Patch shared dependencies for build_country_snapshot."""
    monkeypatch.setattr(country_chat_service, "_resolve_country_column", lambda: "国家")
    monkeypatch.setattr(country_chat_service.repo, "list_columns", lambda: ["国家", "Powertrain"])
    monkeypatch.setattr(
        country_chat_service.query_service, "query_overview", lambda **kw: _STUB_OVERVIEW,
    )
    monkeypatch.setattr(
        country_chat_service.query_service,
        "_build_vehicle_frame",
        lambda filters, **kwargs: _STUB_VEHICLE_FRAME.copy(),
    )
    monkeypatch.setattr(
        country_chat_service.market_scan_service,
        "query_market_scan_deck",
        lambda **kw: _STUB_DECK,
    )
    monkeypatch.setattr(
        insight_card_service,
        "get_insight_cards",
        lambda country, force=False: list(_STUB_INSIGHT_CARDS),
    )
    monkeypatch.setattr(
        country_chat_service.news_digest_service,
        "get_country_news_payload",
        lambda country, **kwargs: copy.deepcopy(_build_stub_news_payload(country)),
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
    assert snapshot["drilldown"]["fuelPanels"]


@pytest.mark.usefixtures("_patch_base")
def test_snapshot_includes_country_news_digest() -> None:
    snapshot = country_chat_service.build_country_snapshot("Germany")

    assert snapshot["marketEvents"]
    assert snapshot["newsDigest"] is not None
    assert snapshot["newsDigest"]["articleCount"] == 2
    assert "EV company-car tax support" in snapshot["newsDigest"]["summary"]


@pytest.mark.usefixtures("_patch_base")
def test_snapshot_prefers_news_payload_override(monkeypatch) -> None:
    monkeypatch.setattr(
        country_chat_service.news_digest_service,
        "get_country_news_payload",
        lambda *args, **kwargs: {
            "countryCode": "SE",
            "countryLabel": "Sweden / 瑞典",
            "marketEvents": [],
            "newsDigest": {"headline": "should-not-be-used", "articleCount": 0},
        },
    )

    snapshot = country_chat_service.build_country_snapshot(
        "Germany",
        news_payload_override={
            "countryCode": "DE",
            "countryLabel": "Germany / 德国",
            "marketEvents": [
                {
                    "title": "override title",
                    "url": "https://example.com/override",
                }
            ],
            "newsDigest": {
                "headline": "override headline",
                "articleCount": 1,
            },
        },
    )

    assert snapshot["newsDigest"]["headline"] == "override headline"
    assert snapshot["marketEvents"][0]["title"] == "override title"


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


def test_gemini_provider_available_when_key_present(monkeypatch) -> None:
    monkeypatch.setenv("GEMINI_API_KEY", "gemini-secret")
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)

    assert country_chat_service._gemini_provider_available() is True


def test_metadata_exposes_available_chat_models(monkeypatch) -> None:
    monkeypatch.setenv("NVIDIA_API_KEY", "secret")
    monkeypatch.setenv("GEMINI_API_KEY", "gemini-secret")
    monkeypatch.setenv(
        "APP_COUNTRY_CHAT_MODEL_OPTIONS",
        "nvidia:meta/llama-3.3-70b-instruct,gemini:gemini-2.5-flash",
    )
    monkeypatch.setattr(country_chat_service, "_resolve_country_column", lambda: "国家")
    monkeypatch.setattr(country_chat_service.repo, "load_distinct_options", lambda *_: ["瑞典"])

    metadata = country_chat_service.get_country_chat_metadata()

    assert metadata["defaultChatModel"] == country_chat_models.AUTO_CHAT_MODEL_ID
    assert metadata["providerAvailable"] is True
    assert [item["id"] for item in metadata["availableChatModels"]] == [
        "auto",
        "nvidia:meta/llama-3.3-70b-instruct",
        "gemini:gemini-2.5-flash",
    ]


def test_query_local_wiki_returns_xc60_price_and_dimensions(
    tmp_path,
    monkeypatch,
) -> None:
    if local_wiki_service.chromadb is None:
        pytest.skip("chromadb not installed")

    db_dir = tmp_path / "chroma_db"
    monkeypatch.setenv("APP_LOCAL_WIKI_DB_PATH", str(db_dir))
    monkeypatch.setenv("APP_LOCAL_WIKI_COLLECTION", "vehicle_wiki")
    local_wiki_service.clear_local_wiki_caches()

    frame = pd.DataFrame(
        [
            {
                "Countries": "Germany",
                "Make": "VOLVO",
                "Model": "XC60",
                "Trim level": "Ultra",
                "Powertrain type": "PHEV",
                "Version name": "XC60 Ultra T8",
                "Currency": "EUR",
                "MSRP including delivery charge": 52000,
                "Base price": 50000,
                "Retail price": 51850,
                "length (mm)": 4708,
                "width (mm)": 1902,
                "height (mm)": 1653,
                "wheelbase (mm)": 2865,
                "Body type": "SUV",
                "Fuel type": "PHEV",
                "Transmission type": "AUTO",
                "Driven wheels": "AWD",
                "Battery range": 81,
                "Seating capacity": 5,
                "cargo volume (l)": 468,
            },
            {
                "Countries": "Germany",
                "Make": "MERCEDES",
                "Model": "GLC",
                "Trim level": "Base",
                "Powertrain type": "ICE",
                "Version name": "GLC 300",
                "Currency": "EUR",
                "MSRP including delivery charge": 61000,
                "Base price": 59000,
                "Retail price": 60500,
                "length (mm)": 4716,
                "width (mm)": 1890,
                "height (mm)": 1640,
                "wheelbase (mm)": 2888,
                "Body type": "SUV",
                "Fuel type": "ICE",
                "Transmission type": "AUTO",
                "Driven wheels": "RWD",
                "Battery range": 0,
                "Seating capacity": 5,
                "cargo volume (l)": 620,
            },
        ]
    )

    manifest = local_wiki_service.build_vehicle_wiki_from_dataframe(
        frame,
        source_path=tmp_path / "stub.parquet",
    )

    docs = country_chat_service._query_local_wiki(
        "VOLVO XC60 的价格和尺寸",
        "Germany",
        "VOLVO",
        "XC60",
    )

    assert manifest["collectionName"] == "vehicle_wiki"
    assert manifest["documentCount"] == 2
    manifest_path = local_wiki_service.get_local_wiki_manifest_path()
    assert manifest_path.exists()
    stored_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert stored_manifest["documentCount"] == 2
    assert docs
    assert "XC60" in docs[0]
    assert "4708" in docs[0]
    assert "52000" in docs[0]

    local_wiki_service.clear_local_wiki_caches()


@pytest.mark.usefixtures("_patch_base")
def test_query_news_wiki_returns_policy_and_competition_hits() -> None:
    snapshot = country_chat_service.build_country_snapshot("Germany")

    hits = country_chat_service._query_news_wiki(
        "德国 company-car tax support 和中国品牌定价压力",
        snapshot,
    )

    assert hits
    titles = " ".join(str(item.get("title", "")) for item in hits)
    assert "tax support" in titles or "pricing pressure" in titles
    assert any(item.get("kind") in {"digest", "event"} for item in hits)


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


def test_infer_multi_intents() -> None:
    intents = country_chat_service.infer_country_chat_intents(
        "中系SUV定价35000，续航和竞品表现怎么样？",
    )

    assert intents == [
        "positioning-analysis",
        "competitive",
        "segment-analysis",
        "origin-analysis",
        "nev-analysis",
    ]


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


# --------------- insight card integration ---------------

@pytest.mark.usefixtures("_patch_base")
def test_snapshot_includes_insight_cards() -> None:
    snapshot = country_chat_service.build_country_snapshot("瑞典")

    assert "insightCards" in snapshot
    assert len(snapshot["insightCards"]) > 0
    card = snapshot["insightCards"][0]
    assert "title" in card
    assert "conclusion" in card
    assert "tone" in card
    assert "relatedChartLink" in card


@pytest.mark.usefixtures("_patch_base")
def test_snapshot_all_insight_cards_internal_key() -> None:
    snapshot = country_chat_service.build_country_snapshot("瑞典")

    assert "_allInsightCards" in snapshot
    assert len(snapshot["_allInsightCards"]) == len(_STUB_INSIGHT_CARDS)


@pytest.mark.usefixtures("_patch_base")
def test_answer_includes_chart_links(monkeypatch) -> None:
    monkeypatch.delenv("NVIDIA_API_KEY", raising=False)
    monkeypatch.delenv("NVAPI_KEY", raising=False)

    result = country_chat_service.answer_country_question("瑞典", "SUV 细分市场分析")

    assert "chartLinks" in result
    assert isinstance(result["chartLinks"], list)


@pytest.mark.usefixtures("_patch_base")
def test_answer_uses_requested_nvidia_chat_model(monkeypatch) -> None:
    monkeypatch.setenv("NVIDIA_API_KEY", "secret")
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setenv(
        "APP_COUNTRY_CHAT_MODEL_OPTIONS",
        "nvidia:meta/llama-3.1-70b-instruct",
    )

    captured: dict[str, object] = {}

    def _fake_answer_with_nvidia(**kwargs):
        captured["chat_model"] = kwargs.get("chat_model")
        return "nvidia ok"

    monkeypatch.setattr(
        country_chat_service,
        "_answer_with_nvidia",
        _fake_answer_with_nvidia,
    )

    result = country_chat_service.answer_country_question(
        "瑞典",
        "SUV 细分市场分析",
        chat_model="nvidia:meta/llama-3.1-70b-instruct",
    )

    assert captured["chat_model"] == "meta/llama-3.1-70b-instruct"
    assert result["provider"] == "nvidia"
    assert result["model"] == "meta/llama-3.1-70b-instruct"
    assert result["chatModelId"] == "nvidia:meta/llama-3.1-70b-instruct"


@pytest.mark.usefixtures("_patch_base")
def test_answer_auto_falls_back_to_gemini(monkeypatch) -> None:
    monkeypatch.setenv("NVIDIA_API_KEY", "secret")
    monkeypatch.setenv("GEMINI_API_KEY", "gemini-secret")
    monkeypatch.setenv(
        "APP_COUNTRY_CHAT_MODEL_OPTIONS",
        "nvidia:meta/llama-3.3-70b-instruct,gemini:gemini-2.5-flash",
    )
    monkeypatch.setattr(
        country_chat_service,
        "_answer_with_nvidia",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("quota hit")),
    )
    monkeypatch.setattr(
        country_chat_service,
        "_answer_with_gemini",
        lambda **kwargs: "gemini ok",
    )

    result = country_chat_service.answer_country_question(
        "瑞典",
        "SUV 细分市场分析",
        chat_model="auto",
    )

    assert result["provider"] == "gemini"
    assert result["answerMode"] == "grounded-model"
    assert result["model"] == "gemini-2.5-flash"
    assert result["chatModelId"] == "auto"
    assert "quota hit" in str(result["providerReason"])
    assert result["grounding"]["strategyLabel"].startswith("Snapshot")
    assert result["grounding"]["layers"][0]["kind"] == "snapshot"
    assert result["grounding"]["evidenceTables"]


@pytest.mark.usefixtures("_patch_base")
def test_answer_passes_prefetched_execution_plan_to_model(monkeypatch) -> None:
    monkeypatch.setenv("NVIDIA_API_KEY", "secret")
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setenv(
        "APP_COUNTRY_CHAT_MODEL_OPTIONS",
        "nvidia:meta/llama-3.3-70b-instruct",
    )

    captured: dict[str, object] = {}

    def _fake_answer_with_nvidia(**kwargs):
        captured["execution_plan"] = kwargs.get("execution_plan")
        captured["planner_context"] = kwargs.get("planner_context")
        return "nvidia ok"

    monkeypatch.setattr(
        country_chat_service,
        "_answer_with_nvidia",
        _fake_answer_with_nvidia,
    )
    monkeypatch.setattr(
        country_chat_service.news_wiki_service,
        "query_news_wiki",
        lambda *args, **kwargs: [
            {
                "title": "Stub news fact",
                "publishedAt": "2026-03-01T00:00:00+00:00",
                "reason": "policy",
            }
        ],
    )

    result = country_chat_service.answer_country_question(
        "瑞典",
        "瑞典最近的补贴政策是什么？",
    )

    assert result["intentRoute"] == "market-context"
    assert result["provider"] == "nvidia"
    assert result["answerMode"] == "grounded-model"
    assert result["executionPlan"]["orchestrationMode"] == "prefetch-first"
    assert any(
        item["key"] == "news-wiki"
        for item in result["executionPlan"]["sourcePlan"]
    )
    assert any(
        item["key"] == "dashboard-analytics"
        for item in result["executionPlan"]["sourcePlan"]
    )
    assert captured["execution_plan"] == result["executionPlan"]
    planner_context = captured["planner_context"]
    assert isinstance(planner_context, dict)
    assert planner_context["prefetchedEvidence"][0]["toolName"] == "query_news_wiki"
    assert planner_context["evidencePacks"]["dashboard"]["periodLabel"] == "Sweden - Feb 2025"
    assert result["grounding"]["trust"]["confidence"] == "medium"
    assert result["grounding"]["trust"]["sourceCoverage"]["requiredReady"] == 3
    assert any(
        layer["label"] == "新闻快照已同步"
        for layer in result["grounding"]["layers"]
    )
    assert any(
        "最近同步时间" in finding
        for finding in result["grounding"]["keyFindings"]
    )
    assert result["grounding"]["answerPath"]["steps"][0] == "先锁定政策 / 新闻 / 市场事件范围。"
    assert "政策/新闻问题" in result["grounding"]["reasoningNotes"][0]


@pytest.mark.usefixtures("_patch_base")
def test_answer_market_context_marks_stale_news_snapshot(monkeypatch) -> None:
    monkeypatch.setenv("NVIDIA_API_KEY", "secret")
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setenv(
        "APP_COUNTRY_CHAT_MODEL_OPTIONS",
        "nvidia:meta/llama-3.3-70b-instruct",
    )
    monkeypatch.setattr(
        country_chat_service,
        "_answer_with_nvidia",
        lambda **kwargs: "nvidia ok",
    )
    monkeypatch.setattr(
        country_chat_service.news_wiki_service,
        "query_news_wiki",
        lambda *args, **kwargs: [
            {
                "title": "Stub news fact",
                "publishedAt": "2026-03-01T00:00:00+00:00",
                "reason": "policy",
            }
        ],
    )
    monkeypatch.setattr(
        country_chat_service.news_digest_service,
        "get_country_news_payload",
        lambda country, **kwargs: {
            **copy.deepcopy(_build_stub_news_payload(country)),
            "newsDigest": {
                **copy.deepcopy(_build_stub_news_payload(country))["newsDigest"],
                "stale": True,
            },
        },
    )

    result = country_chat_service.answer_country_question(
        "瑞典",
        "瑞典最近的补贴政策是什么？",
    )

    assert result["intentRoute"] == "market-context"
    assert result["provider"] == "nvidia"
    assert any(
        layer["label"] == "新闻快照偏旧"
        for layer in result["grounding"]["layers"]
    )
    assert result["grounding"]["trust"]["confidence"] == "low"
    assert any(
        "新闻快照偏旧" in fact or "谨慎解释" in fact
        for fact in result["grounding"]["trust"]["missingFacts"]
    )
    assert any(
        "证据边界理解" in note
        for note in result["grounding"]["reasoningNotes"]
    )


@pytest.mark.usefixtures("_patch_base")
def test_answer_with_nvidia_uses_single_tool_round_then_forces_final_answer(monkeypatch) -> None:
    chat_calls: list[dict[str, object]] = []

    class _FakeResponse:
        def __init__(self, first_message: dict[str, object], text: str = "") -> None:
            self.first_message = first_message
            self.text = text

    class _FakeNvidiaClient:
        def __init__(self, default_model: str) -> None:
            self.default_model = default_model
            self.call_count = 0

        def chat(self, messages, **kwargs):  # noqa: ANN001, ANN003
            self.call_count += 1
            chat_calls.append(
                {
                    "messages": messages,
                    "tools": kwargs.get("tools"),
                }
            )
            if self.call_count == 1:
                return _FakeResponse(
                    {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": "tc-1",
                                "function": {
                                    "name": "query_market_kpis",
                                    "arguments": "{}",
                                },
                            }
                        ],
                    }
                )
            return _FakeResponse(
                {
                    "role": "assistant",
                    "content": "final nvidia answer",
                },
                "final nvidia answer",
            )

    monkeypatch.setattr(country_chat_service, "NvidiaChatClient", _FakeNvidiaClient)

    answer = country_chat_service._answer_with_nvidia(
        country="瑞典",
        question="瑞典市场总量怎么样？",
        intents=["general-summary"],
        intent_route="market-overview",
        user_params={},
        snapshot={"country": "瑞典", "kpis": {"modelCount": 5}},
        history=[],
        chat_model="meta/llama-3.3-70b-instruct",
        execution_plan={"orchestrationMode": "tool-first"},
    )

    assert answer == "final nvidia answer"
    assert len(chat_calls) == 2
    assert chat_calls[0]["tools"]
    assert chat_calls[1]["tools"] == []


@pytest.mark.usefixtures("_patch_base")
def test_answer_with_nvidia_uses_route_bounded_mode_for_precise_lookup(monkeypatch) -> None:
    chat_calls: list[dict[str, object]] = []

    class _FakeResponse:
        def __init__(self, text: str) -> None:
            self.first_message = {
                "role": "assistant",
                "content": text,
            }
            self.text = text

    class _FakeNvidiaClient:
        def __init__(self, default_model: str) -> None:
            self.default_model = default_model

        def chat(self, messages, **kwargs):  # noqa: ANN001, ANN003
            chat_calls.append(
                {
                    "messages": messages,
                    "tools": kwargs.get("tools"),
                }
            )
            return _FakeResponse("route bounded answer")

    monkeypatch.setattr(country_chat_service, "NvidiaChatClient", _FakeNvidiaClient)

    answer = country_chat_service._answer_with_nvidia(
        country="瑞典",
        question="XC60 尺寸和售价怎么样？",
        intents=["pricing-summary"],
        intent_route="precise-lookup",
        user_params={"model": "XC60"},
        snapshot={"country": "瑞典", "kpis": {"modelCount": 5}},
        history=[],
        chat_model="meta/llama-3.3-70b-instruct",
        execution_plan={"orchestrationMode": "route-bounded"},
        planner_context={
            "prefetchedEvidence": [
                {
                    "toolName": "query_local_wiki",
                    "result": {"wiki_facts": ["XC60 wheelbase 2865mm"]},
                }
            ]
        },
    )

    assert answer == "route bounded answer"
    assert len(chat_calls) == 1
    assert chat_calls[0]["tools"] == []


@pytest.mark.usefixtures("_patch_base")
def test_answer_with_nvidia_raises_when_final_pass_still_requests_tools(monkeypatch) -> None:
    class _FakeResponse:
        first_message = {
            "role": "assistant",
            "tool_calls": [
                {
                    "id": "tc-1",
                    "function": {
                        "name": "query_market_kpis",
                        "arguments": "{}",
                    },
                }
            ],
        }
        text = ""

    class _FakeNvidiaClient:
        def __init__(self, default_model: str) -> None:
            self.default_model = default_model

        def chat(self, *args, **kwargs):  # noqa: ANN002, ANN003
            return _FakeResponse()

    monkeypatch.setattr(country_chat_service, "NvidiaChatClient", _FakeNvidiaClient)

    with pytest.raises(RuntimeError, match="继续请求额外工具"):
        country_chat_service._answer_with_nvidia(
            country="瑞典",
            question="瑞典市场总量怎么样？",
            intents=["general-summary"],
            intent_route="market-overview",
            user_params={},
            snapshot={"country": "瑞典", "kpis": {"modelCount": 5}},
            history=[],
            chat_model="meta/llama-3.3-70b-instruct",
            execution_plan={"orchestrationMode": "tool-first"},
        )


@pytest.mark.usefixtures("_patch_base")
def test_answer_falls_back_after_nvidia_tool_depth_limit(monkeypatch) -> None:
    monkeypatch.setenv("NVIDIA_API_KEY", "secret")
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setenv(
        "APP_COUNTRY_CHAT_MODEL_OPTIONS",
        "nvidia:meta/llama-3.3-70b-instruct",
    )
    monkeypatch.setattr(
        country_chat_service,
        "_build_snapshot_first_answer",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        country_chat_service,
        "_answer_with_nvidia",
        lambda **kwargs: (_ for _ in ()).throw(
            RuntimeError("NVIDIA 在工具结果返回后仍继续请求额外工具")
        ),
    )

    result = country_chat_service.answer_country_question(
        "瑞典",
        "XC60 尺寸和售价怎么样？",
        chat_model="nvidia:meta/llama-3.3-70b-instruct",
    )

    assert result["provider"] == "fallback"
    assert result["answerMode"] == "grounded-fallback"
    assert "分析中断" not in str(result["answer"])
    assert "当前聊天模型未返回稳定结果" in str(result["answer"])
    assert "工具调用轮次过多" in str(result["providerReason"])


@pytest.mark.usefixtures("_patch_base")
def test_answer_rejects_unknown_chat_model(monkeypatch) -> None:
    monkeypatch.setenv("NVIDIA_API_KEY", "secret")
    monkeypatch.setenv(
        "APP_COUNTRY_CHAT_MODEL_OPTIONS",
        "nvidia:meta/llama-3.3-70b-instruct",
    )

    with pytest.raises(ValueError, match="不支持的聊天模型"):
        country_chat_service.answer_country_question(
            "瑞典",
            "SUV 细分市场分析",
            chat_model="gemini:gemini-2.5-pro",
        )


@pytest.mark.usefixtures("_patch_base")
def test_context_for_intent_includes_insight_cards() -> None:
    snapshot = country_chat_service.build_country_snapshot("瑞典")
    ctx = country_chat_service._select_context_for_intent(snapshot, "segment-analysis")

    assert "insightCards" in ctx


# --------------- parameter extraction ---------------

@pytest.mark.parametrize(
    "question,expected_keys",
    [
        ("JAECOO J7 是4500的车, HEV, 定价35000, 有机会吗", {"brand", "model", "powertrain", "length", "msrp"}),
        ("BEV 渗透率", {"powertrain"}),
        ("VOLVO XC60 在瑞典怎么样", {"brand", "model"}),
        ("4500mm的SUV定价多少合适", {"length"}),
        ("车长4820", {"length"}),
        ("你好", set()),
    ],
)
def test_extract_user_params(question, expected_keys) -> None:
    params = country_chat_service.extract_user_params(question)
    assert set(params.keys()) == expected_keys


def test_extract_user_params_values() -> None:
    params = country_chat_service.extract_user_params(
        "JAECOO J7 是4500的车, HEV, 定价35000"
    )
    assert params["brand"] == "JAECOO"
    assert params["model"] == "J7"
    assert params["powertrain"] == "HEV"
    assert params["length"] == 4500
    assert params["msrp"] == 35000


def test_extract_user_params_parses_bare_length_prompt() -> None:
    params = country_chat_service.extract_user_params("车长4820")

    assert params["length"] == 4820


def test_extract_user_params_chinese_powertrain() -> None:
    params = country_chat_service.extract_user_params("这款纯电SUV怎么样")
    assert params.get("powertrain") == "BEV"

    params2 = country_chat_service.extract_user_params("插混市场如何")
    assert params2.get("powertrain") == "PHEV"


def test_extract_user_params_parses_segment_and_ranking() -> None:
    params = country_chat_service.extract_user_params("SUV-B中卖得最好的PHEV是哪几个？")

    assert params["segment"] == "SUV-B"
    assert params["powertrain"] == "PHEV"
    assert params["ranking"] == "top"


def test_extract_user_params_does_not_treat_segment_alias_as_model() -> None:
    params = country_chat_service.extract_user_params("suvA谁卖的好，EX40为什么卖得好")

    assert params["segment"] == "SUV-A"
    assert params["ranking"] == "top"
    assert params["model"] == "EX40"


def test_extract_user_params_parses_explicit_market_scan_page_scope() -> None:
    params = country_chat_service.extract_user_params(
        "看一下 activePage=segment 里哪个级别最大"
    )

    assert params["marketScanPage"] == "segment"


def test_extract_user_params_parses_explicit_positioning_page_scope() -> None:
    params = country_chat_service.extract_user_params(
        "看一下 /positioning-pricing?country=瑞典&activePage=suvA 里哪个价带最挤"
    )

    assert params["positioningPage"] == "suvA"


def test_extract_user_params_parses_model_list_for_precise_lookup() -> None:
    params = country_chat_service.extract_user_params(
        "rav4 sportage kona 这些hev的具体版型和价格呢？"
    )

    assert params["models"] == ["RAV4", "SPORTAGE", "KONA"]
    assert params["powertrain"] == "HEV"


def test_merge_followup_user_params_inherits_segment_from_history() -> None:
    merged = country_chat_service._merge_followup_user_params(
        question="那其中卖得最好的PHEV呢？",
        user_params={"powertrain": "PHEV", "ranking": "top"},
        history=[
            {
                "role": "assistant",
                "content": "上一轮已判断是 SUV-B。",
                "extracted_params": {"segment": "SUV-B"},
            }
        ],
    )

    assert merged["segment"] == "SUV-B"


def test_merge_followup_user_params_inherits_market_scan_page_from_history() -> None:
    merged = country_chat_service._merge_followup_user_params(
        question="那里面哪个级别最大？",
        user_params={"ranking": "top"},
        history=[
            {
                "role": "assistant",
                "content": "上一轮在 segment 页。",
                "extracted_params": {"marketScanPage": "segment"},
            }
        ],
    )

    assert merged["marketScanPage"] == "segment"


def test_merge_followup_user_params_inherits_positioning_page_from_history() -> None:
    merged = country_chat_service._merge_followup_user_params(
        question="那里面哪个价带最挤？",
        user_params={"ranking": "top"},
        history=[
            {
                "role": "assistant",
                "content": "上一轮在 positioning 的 SUV-A 页。",
                "extracted_params": {"positioningPage": "suvA"},
            }
        ],
    )

    assert merged["positioningPage"] == "suvA"


# --------------- positioning-analysis intent ---------------

@pytest.mark.parametrize(
    "question",
    [
        "JAECOO J7 定价35000有机会吗",
        "这个定位有竞争力吗",
        "打算卖30000",
    ],
)
def test_infer_intent_positioning(question) -> None:
    assert country_chat_service.infer_country_chat_intent(question) == "positioning-analysis"


# --------------- enrichment + answer integration ---------------

_STUB_POSITIONING_MAP = {
    "rows": 2,
    "items": [
        {"Brand": "VOLVO", "Model": "XC60", "Segment": "SUV-B", "Powertrain": "PHEV", "Length": 4688, "MSRP": 45000, "Sales": 80000, "cluster": 0},
        {"Brand": "MERCEDES", "Model": "GLC", "Segment": "SUV-B", "Powertrain": "ICE", "Length": 4700, "MSRP": 48000, "Sales": 25000, "cluster": 0},
    ],
    "target": {"Length": 4500, "MSRP": 35000},
    "cluster_top3": ["VOLVO XC60", "MERCEDES GLC"],
    "peerCorridor": {
        "peerCount": 2,
        "salesTotal": 105000,
        "lengthMin": 4688,
        "lengthMax": 4700,
        "msrpP25": 45000,
        "msrpMedian": 45000,
        "msrpP75": 48000,
        "pricePerMeterMedian": 9598.98,
        "targetLength": 4500,
        "targetMsrp": 35000,
        "targetPricePerMeter": 7777.78,
        "targetResidual": -10000,
        "targetResidualPct": -22.22,
        "targetPricePerMeterResidualPct": -18.97,
        "positionLabel": "below-peer-range",
        "stanceCode": "aggressive-share-take",
        "stanceLabel": "进攻切入价",
        "stanceDetail": "明显低于 peer 中位数，更偏 volume / share take。",
        "salesWeighted": True,
    },
}

_STUB_MODEL_VERSION_BUBBLE = {
    "rows": 2,
    "items": [
        {"Version": "XC60 Core", "Powertrain": "PHEV", "Trim": "Core", "Length": 4688, "MSRP": 45000, "Sales": 12000},
        {"Version": "XC60 Ultra", "Powertrain": "PHEV", "Trim": "Ultra", "Length": 4688, "MSRP": 52000, "Sales": 9000},
    ],
}

_STUB_POSITIONING_PRICING_DECK = {
    "metadata": {
        "resolvedPeriod": "2025-02",
        "priceOverlay": {
            "sourceMode": "duckdb-overlay",
            "matchedRows": 8,
            "matchedModels": 2,
            "linkMatches": 6,
            "directMatches": 2,
            "candidateRows": 10,
            "linkCandidateRows": 6,
        },
        "labels": {
            "pageTitle": "Sweden 2025-02 positioning pricing",
            "currentMonthShort": "2025 Feb",
            "salesModeLabel": "当月",
        },
    },
    "pages": {
        "overview": {
            "key": "overview",
            "title": "市场总览",
            "subtitle": "全市场价格带与动力定位",
            "summaryText": "全市场当前集中在主流价格带。",
            "metrics": [],
            "priceBands": {"bandSize": 5000, "range": {"min": 20000, "max": 60000}, "items": []},
            "bubbleChart": {"items": [], "bubbleLimit": 0},
        },
        "suvA0": {
            "key": "suvA0",
            "title": "SUV-A0",
            "subtitle": "SUV A0 价格带与动力定位",
            "summaryText": "SUV-A0 当前由入门价格带承接。",
            "metrics": [],
            "priceBands": {"bandSize": 5000, "range": {"min": 20000, "max": 60000}, "items": []},
            "bubbleChart": {"items": [], "bubbleLimit": 0},
        },
        "suvA": {
            "key": "suvA",
            "title": "SUV-A",
            "subtitle": "SUV A 价格带与动力定位",
            "summaryText": "SUV-A 当前集中在 35k-40k 价格带，BEV 权重更高。",
            "metrics": [
                {"label": "当月销量", "value": 18000, "detail": "当月销量"},
                {"label": "Compared Models", "value": 6, "detail": "当前对比 Model 数"},
                {"label": "Visible Versions", "value": 8, "detail": "右侧版型气泡数"},
            ],
            "priceBands": {
                "bandSize": 5000,
                "range": {"min": 20000, "max": 60000},
                "items": [
                    {"bandStart": 35000, "bandEnd": 40000, "bandMid": 37500, "bandWidth": 5000, "label": "35k-40k", "sales": 8200, "fuelMix": {"BEV": 6200, "PHEV": 2000}},
                    {"bandStart": 30000, "bandEnd": 35000, "bandMid": 32500, "bandWidth": 5000, "label": "30k-35k", "sales": 5400, "fuelMix": {"BEV": 2100, "PHEV": 1800, "HEV": 1500}},
                    {"bandStart": 40000, "bandEnd": 45000, "bandMid": 42500, "bandWidth": 5000, "label": "40k-45k", "sales": 2600, "fuelMix": {"BEV": 1600, "PHEV": 1000}},
                ],
            },
            "bubbleChart": {
                "items": [
                    {"brand": "VOLVO", "model": "EX40", "powertrain": "BEV", "segment": "SUV A", "length": 4440, "msrp": 38900, "msrpMin": 36900, "msrpMax": 41900, "sales": 5200, "variantCount": 3},
                    {"brand": "PEUGEOT", "model": "E-2008", "powertrain": "BEV", "segment": "SUV A", "length": 4300, "msrp": 34900, "msrpMin": 32900, "msrpMax": 36900, "sales": 2800, "variantCount": 2},
                ],
                "bubbleLimit": 20,
            },
        },
        "suvBPlus": {
            "key": "suvBPlus",
            "title": "SUV-B+",
            "subtitle": "SUV B+ 价格带与动力定位",
            "summaryText": "SUV-B+ 更集中在更高 MSRP 段。",
            "metrics": [],
            "priceBands": {"bandSize": 5000, "range": {"min": 25000, "max": 70000}, "items": []},
            "bubbleChart": {"items": [], "bubbleLimit": 0},
        },
    },
}


@pytest.fixture()
def _patch_dashboard(monkeypatch):
    """Patch query_service to return stub dashboard data."""
    monkeypatch.setattr(
        country_chat_service.query_service,
        "query_positioning_map",
        lambda **kw: dict(_STUB_POSITIONING_MAP),
    )
    monkeypatch.setattr(
        country_chat_service.query_service,
        "query_advanced_chart",
        lambda **kw: copy.deepcopy(
            _STUB_ADVANCED_CHARTS.get(
                (kw.get("group"), kw.get("chart")),
                {"items": [{"label": "stub", "value": 1}], "rows": 1},
            )
        ),
    )
    monkeypatch.setattr(
        country_chat_service.query_service,
        "query_model_versions",
        lambda **kw: dict(_STUB_MODEL_VERSION_BUBBLE),
    )


@pytest.mark.usefixtures("_patch_base", "_patch_dashboard")
def test_answer_positioning_includes_map(monkeypatch) -> None:
    monkeypatch.delenv("NVIDIA_API_KEY", raising=False)
    monkeypatch.delenv("NVAPI_KEY", raising=False)

    result = country_chat_service.answer_country_question(
        "瑞典", "JAECOO J7 是4500的车, HEV, 定价35000, 有机会吗",
    )

    assert result["intent"] == "positioning-analysis"
    assert result.get("extractedParams")
    assert result["extractedParams"]["brand"] == "JAECOO"
    snapshot = result["contextSnapshot"]
    assert "positioningMap" in snapshot


@pytest.mark.usefixtures("_patch_base", "_patch_dashboard")
def test_answer_uses_snapshot_direct_for_positioning_page_scope(monkeypatch) -> None:
    monkeypatch.setenv("NVIDIA_API_KEY", "secret")
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setenv(
        "APP_COUNTRY_CHAT_MODEL_OPTIONS",
        "nvidia:meta/llama-3.3-70b-instruct",
    )
    monkeypatch.setattr(
        country_chat_service,
        "_answer_with_nvidia",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("should not call nvidia")),
    )
    monkeypatch.setattr(
        country_chat_service.market_scan_service,
        "query_positioning_pricing_deck",
        lambda **kw: copy.deepcopy(_STUB_POSITIONING_PRICING_DECK),
    )

    result = country_chat_service.answer_country_question(
        "瑞典",
        "看一下 /positioning-pricing?country=瑞典&activePage=suvA 里哪个价带最挤",
    )

    assert result["intentRoute"] == "positioning-focus"
    assert result["provider"] == "snapshot"
    assert result["answerMode"] == "grounded-direct"
    assert result["extractedParams"]["positioningPage"] == "suvA"
    assert result["contextSnapshot"]["positioningPageScope"]["pageKey"] == "suvA"
    assert result["contextSnapshot"]["positioningPageScope"]["priceOverlay"]["matchedRows"] == 8
    assert "35k-40k" in result["answer"]
    assert "头部竞品" in result["answer"]
    assert "reviewed PG current price" in result["answer"]
    assert result["renderHints"][0]["kind"] == "positioning-summary"
    assert "reviewed PG current price overlay" in str(result["providerReason"])
    assert any(
        layer["label"] == "Reviewed MSRP overlay 已命中"
        for layer in result["grounding"]["layers"]
    )
    assert any(
        table["title"] == "SUV-A 价格带排名"
        for table in result["grounding"]["evidenceTables"]
    )
    assert any(
        layer["label"] == "参数线索推导"
        for layer in result["grounding"]["layers"]
    )
    assert any(
        "suvA page" in step
        for step in result["grounding"]["answerPath"]["steps"]
    )


@pytest.mark.usefixtures("_patch_base", "_patch_dashboard")
def test_answer_positioning_page_scope_marks_parquet_fallback(monkeypatch) -> None:
    monkeypatch.delenv("NVIDIA_API_KEY", raising=False)
    monkeypatch.delenv("NVAPI_KEY", raising=False)
    fallback_deck = copy.deepcopy(_STUB_POSITIONING_PRICING_DECK)
    fallback_deck["metadata"]["priceOverlay"] = {
        "sourceMode": "parquet-only",
        "candidateRows": 10,
        "linkCandidateRows": 6,
        "matchedRows": 0,
        "matchedModels": 0,
        "linkMatches": 0,
        "directMatches": 0,
        "reason": "no-overlay-matches",
    }
    monkeypatch.setattr(
        country_chat_service.market_scan_service,
        "query_positioning_pricing_deck",
        lambda **kw: fallback_deck,
    )

    result = country_chat_service.answer_country_question(
        "瑞典",
        "看一下 /positioning-pricing?country=瑞典&activePage=suvA 里哪个价带最挤",
    )

    assert result["intentRoute"] == "positioning-focus"
    assert "parquet MSRP fallback" in str(result["providerReason"])
    assert any(
        layer["label"] == "Parquet MSRP fallback"
        for layer in result["grounding"]["layers"]
    )
    assert result["grounding"]["trust"]["confidence"] == "medium"
    assert any(
        "parquet MSRP fallback" in fact
        for fact in result["grounding"]["trust"]["missingFacts"]
    )


@pytest.mark.usefixtures("_patch_base", "_patch_dashboard")
def test_enrichment_nev_adds_range(monkeypatch) -> None:
    monkeypatch.delenv("NVIDIA_API_KEY", raising=False)
    monkeypatch.delenv("NVAPI_KEY", raising=False)

    result = country_chat_service.answer_country_question("瑞典", "BEV 续航分布怎么样")

    assert result["intent"] == "nev-analysis"
    snapshot = result["contextSnapshot"]
    assert "nevRangeDistribution" in snapshot


@pytest.mark.usefixtures("_patch_base", "_patch_dashboard")
def test_context_positioning_intent() -> None:
    snapshot = country_chat_service.build_country_snapshot("瑞典")
    snapshot["positioningMap"] = _STUB_POSITIONING_MAP
    snapshot["priceDistribution"] = [{"label": "4000-4500", "value": 120}]

    ctx = country_chat_service._select_context_for_intent(snapshot, "positioning-analysis")

    assert "positioningMap" in ctx
    assert "priceDistribution" in ctx
    assert ctx["positioningMap"]["target"] == {"Length": 4500, "MSRP": 35000}
    assert ctx["positioningMap"]["peerCorridor"]["positionLabel"] == "below-peer-range"
    assert ctx["positioningMap"]["peerCorridor"]["stanceLabel"] == "进攻切入价"


@pytest.mark.usefixtures("_patch_base")
def test_context_merges_multi_intents() -> None:
    snapshot = country_chat_service.build_country_snapshot("瑞典")
    snapshot["positioningMap"] = _STUB_POSITIONING_MAP
    snapshot["priceDistribution"] = [{"label": "4000-4500", "value": 120}]
    snapshot["nevRangeDistribution"] = [{"label": "500-600km", "value": 80}]

    ctx = country_chat_service._select_context_for_intents(
        snapshot,
        ["positioning-analysis", "segment-analysis", "nev-analysis"],
    )

    assert ctx["primaryIntent"] == "positioning-analysis"
    assert ctx["intents"] == [
        "positioning-analysis",
        "segment-analysis",
        "nev-analysis",
    ]
    assert "positioningMap" in ctx
    assert "segmentMatrix" in ctx
    assert "nevRangeDistribution" in ctx


@pytest.mark.usefixtures("_patch_base")
def test_fallback_positioning(monkeypatch) -> None:
    monkeypatch.delenv("NVIDIA_API_KEY", raising=False)
    monkeypatch.delenv("NVAPI_KEY", raising=False)

    snapshot = country_chat_service.build_country_snapshot("瑞典")
    snapshot["positioningMap"] = _STUB_POSITIONING_MAP

    answer = country_chat_service._build_fallback_answer(
        country="瑞典",
        question="定价35000有机会吗",
        intent="positioning-analysis",
        snapshot=snapshot,
        provider_error=None,
    )

    assert "目标定位" in answer
    assert "VOLVO" in answer
    assert "Peer 价格走廊" in answer
    assert "进攻切入价" in answer


@pytest.mark.usefixtures("_patch_base", "_patch_dashboard")
def test_answer_returns_multi_intent_fields(monkeypatch) -> None:
    monkeypatch.delenv("NVIDIA_API_KEY", raising=False)
    monkeypatch.delenv("NVAPI_KEY", raising=False)

    result = country_chat_service.answer_country_question(
        "瑞典",
        "中系SUV定价35000，续航和竞品表现怎么样？",
    )

    assert result["intent"] == "positioning-analysis"
    assert result["primaryIntent"] == "positioning-analysis"
    assert "segment-analysis" in result["intents"]
    assert "nev-analysis" in result["intents"]
    assert "competitive" in result["intents"]
    assert isinstance(result["chartLinks"], list)


def test_build_country_chat_route_uses_precise_lookup_for_model_price_questions() -> None:
    route = country_chat_service._build_country_chat_route(
        "VOLVO XC60 PHEV 定价 45000 有机会吗",
        {
            "brand": "VOLVO",
            "model": "XC60",
            "powertrain": "PHEV",
            "msrp": 45000,
        },
        ["positioning-analysis", "competitive"],
    )

    assert route["intentRoute"] == "precise-lookup"
    assert route["focusedIntents"] == ["positioning-analysis", "pricing-summary"]


def test_build_country_chat_route_uses_positioning_focus_for_bare_length() -> None:
    route = country_chat_service._build_country_chat_route(
        "车长4820",
        {"length": 4820},
        ["positioning-analysis"],
    )

    assert route["intentRoute"] == "positioning-focus"
    assert route["focusedIntents"] == ["positioning-analysis"]


def test_build_country_chat_route_uses_segment_fuel_focus_for_segment_powertrain_ranking() -> None:
    route = country_chat_service._build_country_chat_route(
        "SUV-B中卖得最好的PHEV是哪几个？",
        {"segment": "SUV-B", "powertrain": "PHEV", "ranking": "top"},
        ["segment-analysis", "powertrain-mix"],
    )

    assert route["intentRoute"] == "segment-fuel-focus"
    assert route["focusedIntents"] == ["segment-analysis", "powertrain-mix"]


def test_build_country_chat_route_uses_market_scan_scope_for_segment_ranking_with_model_why() -> None:
    route = country_chat_service._build_country_chat_route(
        "suvA谁卖的好，EX40为什么卖得好",
        {"segment": "SUV-A", "ranking": "top", "model": "EX40"},
        ["segment-analysis", "competitive"],
    )

    assert route["intentRoute"] == "market-scan-scope"
    assert route["focusedIntents"] == ["segment-analysis", "competitive"]


def test_build_country_chat_route_uses_market_scan_scope_for_explicit_page_scope() -> None:
    route = country_chat_service._build_country_chat_route(
        "segment页里哪个级别最大",
        {"marketScanPage": "segment", "ranking": "top"},
        ["segment-analysis"],
    )

    assert route["intentRoute"] == "market-scan-scope"
    assert route["focusedIntents"] == ["segment-analysis"]


def test_build_country_chat_route_uses_positioning_focus_for_explicit_positioning_page_scope() -> None:
    route = country_chat_service._build_country_chat_route(
        "positioning-pricing 的 suvA 页哪个价带最挤",
        {"positioningPage": "suvA", "ranking": "top"},
        ["pricing-summary"],
    )

    assert route["intentRoute"] == "positioning-focus"
    assert route["focusedIntents"] == ["positioning-analysis", "pricing-summary"]


def test_extract_user_params_parses_compare_subjects_for_version_diff() -> None:
    params = country_chat_service.extract_user_params(
        "XC60 Ultra vs XC60 Core 配置差异"
    )

    assert params["model"] == "XC60"
    assert params["compare_subjects"] == [
        {"model": "XC60", "variantQuery": "ULTRA"},
        {"model": "XC60", "variantQuery": "CORE"},
    ]


@pytest.mark.usefixtures("_patch_base", "_patch_dashboard")
def test_answer_returns_focused_intents_and_render_hints(monkeypatch) -> None:
    monkeypatch.delenv("NVIDIA_API_KEY", raising=False)
    monkeypatch.delenv("NVAPI_KEY", raising=False)

    result = country_chat_service.answer_country_question(
        "瑞典",
        "VOLVO XC60 PHEV 定价 45000 有机会吗",
    )

    assert result["intentRoute"] == "precise-lookup"
    assert result["focusedIntents"] == ["positioning-analysis", "pricing-summary"]
    assert result["renderHints"]
    assert result["renderHints"][0]["kind"] == "positioning-summary"


@pytest.mark.usefixtures("_patch_base", "_patch_dashboard")
def test_answer_uses_snapshot_direct_for_positioning_focus(monkeypatch) -> None:
    monkeypatch.setenv("NVIDIA_API_KEY", "secret")
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setenv(
        "APP_COUNTRY_CHAT_MODEL_OPTIONS",
        "nvidia:meta/llama-3.3-70b-instruct",
    )
    monkeypatch.setattr(
        country_chat_service,
        "_answer_with_nvidia",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("should not call nvidia")),
    )

    result = country_chat_service.answer_country_question(
        "瑞典",
        "车长4820的车在这个国家属于什么segment？",
    )

    assert result["intentRoute"] == "positioning-focus"
    assert result["provider"] == "snapshot"
    assert result["answerMode"] == "grounded-direct"
    assert "positioningLookup" in result["contextSnapshot"]
    assert result["extractedParams"]["segment"] == "SUV-B"
    assert "同尺寸邻近车型" in result["answer"]
    assert "Peer 价格走廊 / residual" in result["answer"]
    assert "进攻切入价" in result["answer"]
    assert "渠道占比" in result["answer"]
    assert "未再进入" in str(result["providerReason"])
    assert result["grounding"]["strategyLabel"].startswith("Snapshot + Dynamic")
    assert any(
        layer["kind"] == "dynamic" for layer in result["grounding"]["layers"]
    )
    assert result["grounding"]["evidenceTables"][0]["title"] == "同尺寸邻近车型"
    assert any(
        table["title"] == "Peer 价格走廊 / residual"
        for table in result["grounding"]["evidenceTables"]
    )
    assert any(
        "进攻切入价" in finding
        for finding in result["grounding"]["keyFindings"]
    )
    assert any(
        table["title"] == "相关新闻 / 政策佐证"
        for table in result["grounding"]["evidenceTables"]
    )


@pytest.mark.usefixtures("_patch_base", "_patch_dashboard")
def test_answer_uses_snapshot_direct_for_segment_fuel_followup(monkeypatch) -> None:
    monkeypatch.setenv("NVIDIA_API_KEY", "secret")
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setenv(
        "APP_COUNTRY_CHAT_MODEL_OPTIONS",
        "nvidia:meta/llama-3.3-70b-instruct",
    )
    monkeypatch.setattr(
        country_chat_service,
        "_answer_with_nvidia",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("should not call nvidia")),
    )

    result = country_chat_service.answer_country_question(
        "瑞典",
        "那其中卖得最好的PHEV呢？",
        history=[
            {
                "role": "assistant",
                "content": "在瑞典，车长约 4820 mm 的车型主要落在 SUV-B。",
                "extracted_params": {"segment": "SUV-B"},
            }
        ],
    )

    assert result["intentRoute"] == "segment-fuel-focus"
    assert result["provider"] == "snapshot"
    assert result["answerMode"] == "grounded-direct"
    assert result["extractedParams"]["segment"] == "SUV-B"
    assert result["extractedParams"]["powertrain"] == "PHEV"
    assert "XC60" in result["answer"]
    assert result["grounding"]["evidenceTables"][0]["title"] == "SUV-B · PHEV 销量排名"


@pytest.mark.usefixtures("_patch_base", "_patch_dashboard")
def test_answer_uses_snapshot_direct_for_market_scan_scope(monkeypatch) -> None:
    monkeypatch.setenv("NVIDIA_API_KEY", "secret")
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setenv(
        "APP_COUNTRY_CHAT_MODEL_OPTIONS",
        "nvidia:meta/llama-3.3-70b-instruct",
    )
    monkeypatch.setattr(
        country_chat_service,
        "_answer_with_nvidia",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("should not call nvidia")),
    )
    monkeypatch.setattr(
        country_chat_service.repo,
        "list_columns",
        lambda: ["国家", "Powertrain", "Model", "Body type", "2026-03"],
    )
    monkeypatch.setattr(
        country_chat_service.repo,
        "load_slice",
        lambda **kwargs: pd.DataFrame(
            [
                {"Model": "EX40", "Body type": "SUV", "2026-03": 120},
                {"Model": "EX40", "Body type": "SUV Coupe", "2026-03": 60},
                {"Model": "XC40", "Body type": "SUV", "2026-03": 40},
            ]
        ),
    )

    result = country_chat_service.answer_country_question(
        "瑞典",
        "suvA谁卖的好，EX40为什么卖得好",
    )

    assert result["intentRoute"] == "market-scan-scope"
    assert result["provider"] == "snapshot"
    assert result["answerMode"] == "grounded-direct"
    assert result["extractedParams"]["segment"] == "SUV-A"
    assert result["extractedParams"]["model"] == "EX40"
    assert result["extractedParams"]["marketScanPage"] == "suvA"
    assert result["contextSnapshot"]["marketScanScope"]["pageKey"] == "suvA"
    performance = result["contextSnapshot"]["marketScanScope"]["modelPerformance"]
    assert performance["model"] == "EX40"
    assert performance["channelMix"][0]["label"] == "Business"
    assert performance["awdShareDisplay"] == "66.7%"
    assert performance["bodyStyleDistribution"][0]["label"] == "SUV"
    assert performance["versionAxis"] == "trim"
    assert performance["versionDistribution"][0]["label"] == "Core"
    assert "EX40" in result["answer"]
    assert "EX40" in result["answer"]
    assert "渠道 mix" in result["answer"]
    assert "AWD / 4WD 比例" in result["answer"]
    assert "车身 / body style 分布" in result["answer"]
    assert "版本 / trim 分布" in result["answer"]
    assert "细分页 rank/share context" in result["answer"]
    assert result["renderHints"][0]["kind"] == "model-performance-summary"
    assert result["renderHints"][1]["kind"] == "model-version-mix"
    assert any(
        table["title"] == "SUV-A Top Ranking"
        for table in result["grounding"]["evidenceTables"]
    )
    assert any(
        table["title"] == "EX40 Body Style 分布"
        for table in result["grounding"]["evidenceTables"]
    )
    assert any(
        table["title"] == "EX40 Trim 分布"
        for table in result["grounding"]["evidenceTables"]
    )
    assert any(
        table["title"] == "相关新闻 / 政策佐证"
        for table in result["grounding"]["evidenceTables"]
    )


@pytest.mark.usefixtures("_patch_base", "_patch_dashboard")
def test_answer_uses_snapshot_direct_for_market_scan_segment_page_scope(monkeypatch) -> None:
    monkeypatch.setenv("NVIDIA_API_KEY", "secret")
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setenv(
        "APP_COUNTRY_CHAT_MODEL_OPTIONS",
        "nvidia:meta/llama-3.3-70b-instruct",
    )
    monkeypatch.setattr(
        country_chat_service,
        "_answer_with_nvidia",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("should not call nvidia")),
    )
    custom_deck = copy.deepcopy(_STUB_DECK)
    custom_deck["results"]["segment"] = {
        "summaryText": "SUV-B 当前是最大级别，SUV 仍明显高于 Sedan。",
        "matrix": {
            "columns": ["SUV-B", "SUV-C", "CAR-C"],
            "rows": [
                {
                    "metricKey": "current_volume",
                    "label": "当月销量",
                    "cells": [
                        {"key": "SUV-B", "value": 5000, "display": "5,000", "tone": "neutral"},
                        {"key": "SUV-C", "value": 4200, "display": "4,200", "tone": "neutral"},
                        {"key": "CAR-C", "value": 2200, "display": "2,200", "tone": "neutral"},
                    ],
                },
                {
                    "metricKey": "yoy",
                    "label": "YoY",
                    "cells": [
                        {"key": "SUV-B", "value": 0.08, "display": "+8.0%", "tone": "positive"},
                        {"key": "SUV-C", "value": 0.02, "display": "+2.0%", "tone": "positive"},
                        {"key": "CAR-C", "value": -0.03, "display": "-3.0%", "tone": "negative"},
                    ],
                },
            ],
        },
        "bodyShareTrend": {
            "items": [
                {
                    "period": "2025-02",
                    "label": "2025 Feb",
                    "totalVolume": 11400,
                    "suvSharePct": 0.67,
                    "sedanSharePct": 0.33,
                }
            ]
        },
        "suvSegmentShareTrend": {"items": []},
    }
    monkeypatch.setattr(
        country_chat_service.market_scan_service,
        "query_market_scan_deck",
        lambda **kw: copy.deepcopy(custom_deck),
    )

    result = country_chat_service.answer_country_question(
        "瑞典",
        "segment页里哪个级别最大？",
    )

    assert result["intentRoute"] == "market-scan-scope"
    assert result["provider"] == "snapshot"
    assert result["answerMode"] == "grounded-direct"
    assert result["extractedParams"]["marketScanPage"] == "segment"
    assert result["contextSnapshot"]["marketScanScope"]["pageKey"] == "segment"
    assert result["contextSnapshot"]["marketScanScope"]["scopeKind"] == "matrix"
    assert result["contextSnapshot"]["marketScanScope"]["subjectLabel"] == "级别"
    assert "SUV-B" in result["answer"]
    assert "当前结构" in result["answer"]
    assert result["renderHints"][0]["kind"] == "market-scan-summary"
    assert any(
        table["title"] == "Segment Top Ranking"
        for table in result["grounding"]["evidenceTables"]
    )


@pytest.mark.usefixtures("_patch_base", "_patch_dashboard")
def test_answer_uses_snapshot_direct_for_market_scan_origin_page_scope(monkeypatch) -> None:
    monkeypatch.setenv("NVIDIA_API_KEY", "secret")
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setenv(
        "APP_COUNTRY_CHAT_MODEL_OPTIONS",
        "nvidia:meta/llama-3.3-70b-instruct",
    )
    monkeypatch.setattr(
        country_chat_service,
        "_answer_with_nvidia",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("should not call nvidia")),
    )
    custom_deck = copy.deepcopy(_STUB_DECK)
    custom_deck["results"]["origin"] = {
        "summaryText": "欧系仍是当前最大阵营，中系份额上升更快。",
        "trend": {
            "series": [
                {"origin": "EU", "points": []},
                {"origin": "CN", "points": []},
            ]
        },
        "brandTrend": {"groups": []},
        "matrix": {
            "columns": ["EU", "CN", "JP"],
            "rows": [
                {
                    "metricKey": "current_volume",
                    "label": "当月销量",
                    "cells": [
                        {"key": "EU", "value": 9000, "display": "9,000", "tone": "neutral"},
                        {"key": "CN", "value": 3200, "display": "3,200", "tone": "neutral"},
                        {"key": "JP", "value": 2100, "display": "2,100", "tone": "neutral"},
                    ],
                },
                {
                    "metricKey": "yoy",
                    "label": "YoY",
                    "cells": [
                        {"key": "EU", "value": 0.01, "display": "+1.0%", "tone": "positive"},
                        {"key": "CN", "value": 0.18, "display": "+18.0%", "tone": "positive"},
                        {"key": "JP", "value": -0.04, "display": "-4.0%", "tone": "negative"},
                    ],
                },
            ],
        },
    }
    monkeypatch.setattr(
        country_chat_service.market_scan_service,
        "query_market_scan_deck",
        lambda **kw: copy.deepcopy(custom_deck),
    )

    result = country_chat_service.answer_country_question(
        "瑞典",
        "origin页里哪个车系最大？",
    )

    assert result["intentRoute"] == "market-scan-scope"
    assert result["provider"] == "snapshot"
    assert result["answerMode"] == "grounded-direct"
    assert result["extractedParams"]["marketScanPage"] == "origin"
    assert result["contextSnapshot"]["marketScanScope"]["pageKey"] == "origin"
    assert result["contextSnapshot"]["marketScanScope"]["scopeKind"] == "matrix"
    assert result["contextSnapshot"]["marketScanScope"]["subjectLabel"] == "车系"
    assert "EU" in result["answer"]
    assert "趋势覆盖" in result["answer"]
    assert result["renderHints"][0]["kind"] == "market-scan-summary"
    assert any(
        table["title"] == "Origin Top Ranking"
        for table in result["grounding"]["evidenceTables"]
    )


@pytest.mark.usefixtures("_patch_base", "_patch_dashboard")
def test_answer_uses_snapshot_direct_for_precise_lookup(monkeypatch) -> None:
    monkeypatch.setenv("NVIDIA_API_KEY", "secret")
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setenv(
        "APP_COUNTRY_CHAT_MODEL_OPTIONS",
        "nvidia:meta/llama-3.3-70b-instruct",
    )
    monkeypatch.setattr(
        country_chat_service,
        "_answer_with_nvidia",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("should not call nvidia")),
    )
    monkeypatch.setattr(
        msrp_lookup_service,
        "lookup_current_msrp_from_db",
        lambda **kwargs: {
            "queryModels": ["RAV4", "SPORTAGE", "KONA"],
            "matchedModels": ["RAV4", "SPORTAGE", "KONA"],
            "powertrain": "HEV",
            "latestUpdatedAt": "2026-04-18T09:00:00+00:00",
            "sourceSummary": [{"tier": 1, "count": 3}],
            "modelSummaries": [
                {"model": "RAV4", "trimCount": 3, "entryMsrp": 414900.0, "maxMsrp": 504900.0, "currency": "SEK"},
                {"model": "SPORTAGE", "trimCount": 3, "entryMsrp": 399900.0, "maxMsrp": 459900.0, "currency": "SEK"},
                {"model": "KONA", "trimCount": 3, "entryMsrp": 334900.0, "maxMsrp": 394900.0, "currency": "SEK"},
            ],
            "items": [
                {"model": "RAV4", "trim": "Active", "powertrain": "HEV", "msrp": 414900.0, "currency": "SEK", "updatedAt": "2026-04-18T09:00:00+00:00", "sourceTier": 1},
                {"model": "SPORTAGE", "trim": "Action", "powertrain": "HEV", "msrp": 399900.0, "currency": "SEK", "updatedAt": "2026-04-18T09:00:00+00:00", "sourceTier": 1},
                {"model": "KONA", "trim": "Essential", "powertrain": "HEV", "msrp": 334900.0, "currency": "SEK", "updatedAt": "2026-04-18T09:00:00+00:00", "sourceTier": 1},
            ],
        },
    )

    result = country_chat_service.answer_country_question(
        "瑞典",
        "rav4 sportage kona 这些hev的具体版型和价格呢？",
    )

    assert result["intentRoute"] == "precise-lookup"
    assert result["provider"] == "snapshot"
    assert result["answerMode"] == "grounded-direct"
    assert result["extractedParams"]["models"] == ["RAV4", "SPORTAGE", "KONA"]
    assert "当前 MSRP / 版型命中" in result["answer"]
    assert "KONA" in result["answer"]
    assert result["grounding"]["evidenceTables"][0]["title"] == "当前 MSRP / 版型命中"


@pytest.mark.usefixtures("_patch_base", "_patch_dashboard")
def test_answer_uses_trim_sales_for_best_selling_variant_lookup(monkeypatch) -> None:
    monkeypatch.setenv("NVIDIA_API_KEY", "secret")
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setenv(
        "APP_COUNTRY_CHAT_MODEL_OPTIONS",
        "nvidia:meta/llama-3.3-70b-instruct",
    )
    monkeypatch.setattr(
        country_chat_service,
        "_answer_with_nvidia",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("should not call nvidia")),
    )
    monkeypatch.setattr(
        msrp_lookup_service,
        "lookup_current_msrp_from_db",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("should not call msrp lookup")),
    )

    result = country_chat_service.answer_country_question(
        "瑞典",
        "找一下XC60哪个版型卖得最好",
    )

    assert result["intentRoute"] == "precise-lookup"
    assert result["provider"] == "snapshot"
    assert result["answerMode"] == "grounded-direct"
    assert result["extractedParams"]["model"] == "XC60"
    assert result["contextSnapshot"]["preciseLookup"]["kind"] == "trim-sales"
    assert "卖得最好的是 **Core**" in result["answer"]
    assert "XC60 版型销量分布" in result["answer"]
    assert "当前 MSRP / 版型命中" not in result["answer"]
    assert result["grounding"]["evidenceTables"][0]["title"] == "XC60 版型销量分布"
    assert result["executionPlan"]["answerStrategy"] == "snapshot-first"
    assert any(
        item["key"] == "trim-sales"
        for item in result["executionPlan"]["sourcePlan"]
    )
    trim_source = next(
        item for item in result["executionPlan"]["sourcePlan"] if item["key"] == "trim-sales"
    )
    assert trim_source["status"] == "ready"
    assert result["grounding"]["trust"]["confidence"] == "high"
    assert result["grounding"]["trust"]["evidenceSufficiency"] == "strong"


@pytest.mark.usefixtures("_patch_base", "_patch_dashboard")
def test_answer_uses_snapshot_direct_for_variant_diff(monkeypatch) -> None:
    monkeypatch.setenv("NVIDIA_API_KEY", "secret")
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setenv(
        "APP_COUNTRY_CHAT_MODEL_OPTIONS",
        "nvidia:meta/llama-3.3-70b-instruct",
    )
    monkeypatch.setattr(
        country_chat_service,
        "_answer_with_nvidia",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("should not call nvidia")),
    )
    monkeypatch.setattr(
        engineering_variant_diff_service,
        "compare_market_variants_from_db",
        lambda **kwargs: {
            "country": "瑞典",
            "queryModels": ["XC60"],
            "subjects": [
                {
                    "queryModel": "XC60",
                    "selectionMode": "explicit-match",
                    "selectionNote": None,
                    "model": "XC60",
                    "trim": "Ultra",
                    "version": "2026 MY",
                    "powertrain": "PHEV",
                    "targetMsrp": 55900.0,
                    "subjectLabel": "XC60 Ultra 2026 MY",
                    "latestUpdatedAt": "2026-04-18T09:00:00+00:00",
                },
                {
                    "queryModel": "XC60",
                    "selectionMode": "explicit-match",
                    "selectionNote": None,
                    "model": "XC60",
                    "trim": "Core",
                    "version": "2026 MY",
                    "powertrain": "PHEV",
                    "targetMsrp": 51900.0,
                    "subjectLabel": "XC60 Core 2026 MY",
                    "latestUpdatedAt": "2026-04-18T09:00:00+00:00",
                },
            ],
            "differentFeatures": [
                {
                    "featureCode": "battery_kwh",
                    "featureLabel": "Battery kWh",
                    "values": ["18.8", "16"],
                },
                {
                    "featureCode": "massage_seats_pack",
                    "featureLabel": "Massage Seats Pack",
                    "values": ["package", "-"],
                },
            ],
            "commonFeatures": [
                {
                    "featureCode": "drive_type",
                    "featureLabel": "Drive Type",
                    "value": "AWD",
                }
            ],
            "selectionNotes": [],
            "latestUpdatedAt": "2026-04-18T09:00:00+00:00",
        },
    )

    result = country_chat_service.answer_country_question(
        "瑞典",
        "XC60 Ultra vs XC60 Core 配置差异是什么？",
    )

    assert result["intentRoute"] == "precise-lookup"
    assert result["provider"] == "snapshot"
    assert result["answerMode"] == "grounded-direct"
    assert result["extractedParams"]["compare_subjects"] == [
        {"model": "XC60", "variantQuery": "ULTRA"},
        {"model": "XC60", "variantQuery": "CORE"},
    ]
    assert "核心配置差异" in result["answer"]
    assert "Battery kWh" in result["answer"]
    assert result["grounding"]["evidenceTables"][0]["title"] == "版本 / 配置差异"


@pytest.mark.usefixtures("_patch_base", "_patch_dashboard")
def test_build_country_chart_deck_returns_targeted_snapshot() -> None:
    result = country_chat_service.build_country_chart_deck(
        "瑞典",
        question="中系SUV定价35000，续航和竞品表现怎么样？",
        extracted_params={"brand": "JAECOO", "length": 4500, "msrp": 35000},
    )

    snapshot = result["contextSnapshot"]
    assert result["country"] == "瑞典"
    assert result["primaryIntent"] == "positioning-analysis"
    assert "positioning-analysis" in result["deckIntents"]
    assert len(result["intents"]) <= country_chat_service.MAX_DECK_BASE_INTENTS
    assert len(result["deckIntents"]) <= country_chat_service.MAX_DECK_INTENTS
    assert result["intentRoute"] == "precise-lookup"
    assert "positioningMap" in snapshot
    assert "modelVersionBubble" in snapshot
    assert "priceDistribution" in snapshot
    assert "nevRangeDistribution" not in snapshot
    assert "segmentShareByLength" not in snapshot
    assert "pricePerMeter" not in snapshot
    assert "priceMigration" not in snapshot
    assert snapshot["marketEvents"]
    assert snapshot["newsDigest"] is not None
    assert result["controls"]["selectedModel"]


@pytest.mark.usefixtures("_patch_base", "_patch_dashboard")
def test_build_country_chart_deck_keeps_market_context_scope_tight() -> None:
    result = country_chat_service.build_country_chart_deck(
        "德国",
        question="德国最近补贴政策有什么变化？",
    )

    assert result["intentRoute"] == "market-context"
    assert result["deckIntents"] == ["market-context", "trend-summary"]


@pytest.mark.usefixtures("_patch_base", "_patch_dashboard")
def test_build_country_chart_deck_keeps_segment_fuel_scope_tight() -> None:
    result = country_chat_service.build_country_chart_deck(
        "瑞典",
        question="SUV-B中卖得最好的PHEV是哪几个？",
    )

    assert result["intentRoute"] == "segment-fuel-focus"
    assert result["deckIntents"] == ["segment-analysis", "powertrain-mix"]
    assert result["extractedParams"]["segment"] == "SUV-B"
    assert result["extractedParams"]["powertrain"] == "PHEV"


def test_chart_deck_intents_are_capped_and_keep_primary_first() -> None:
    deck_intents = country_chat_service._chart_deck_intents(
        [
            "positioning-analysis",
            "competitive",
            "segment-analysis",
            "origin-analysis",
            "nev-analysis",
        ],
    )

    assert len(deck_intents) <= country_chat_service.MAX_DECK_INTENTS
    assert deck_intents[0] == "positioning-analysis"
    assert "competitive" in deck_intents


@pytest.mark.usefixtures("_patch_base", "_patch_dashboard")
def test_build_country_chart_deck_respects_selected_year_and_model_controls(monkeypatch) -> None:
    monkeypatch.setattr(
        country_chat_service.query_service,
        "_sales_columns_for_scope",
        lambda columns, **kwargs: (["2024"], ["2023", "2024", "2025"], "2024"),
    )

    result = country_chat_service.build_country_chart_deck(
        "瑞典",
        question="看 2024 年 XC60 的版本结构",
        selected_year=2024,
        selected_model="XC60",
        model_top_n=12,
    )

    assert result["controls"]["selectedYear"] == 2024
    assert result["controls"]["selectedModel"] == "XC60"
    assert result["controls"]["modelTopN"] == 12
    assert result["controls"]["availableYears"] == ["2023", "2024", "2025"]


# --------------- deep-link templates ---------------

def test_insight_card_positioning_link_template() -> None:
    assert "positioning" in insight_card_service._CHART_LINK_TEMPLATES
    assert "positioning-analysis" in insight_card_service.INTENT_TO_CATEGORIES
    assert insight_card_service._CHART_LINK_TEMPLATES["positioning"] == "/positioning-pricing?country={country}&activePage=overview"


# --------------- improved intent recognition ---------------


@pytest.mark.parametrize(
    "question,expected_in_intents",
    [
        # Wide-scope queries should trigger multiple intents
        ("帮我看看这个市场", ["brand-ranking", "segment-analysis", "trend-summary"]),
        ("整体情况怎么样", ["brand-ranking", "segment-analysis", "trend-summary"]),
        ("给个概述", ["brand-ranking", "segment-analysis", "trend-summary"]),
        ("分析一下大盘", ["brand-ranking", "segment-analysis", "trend-summary"]),
        # New keywords
        ("纯电市场如何", ["nev-analysis"]),
        ("增程车型表现", ["nev-analysis"]),
        ("车企排名", ["brand-ranking"]),
        ("能不能进这个市场", ["positioning-analysis"]),
        ("对手是谁", ["competitive"]),
        ("销量变化", ["trend-summary"]),
        ("增长最快的品牌", ["trend-summary"]),
        ("溢价空间", ["pricing-summary"]),
    ],
)
def test_infer_intent_improved_keywords(question, expected_in_intents) -> None:
    intents = country_chat_service.infer_country_chat_intents(question)
    for expected in expected_in_intents:
        assert expected in intents, f"'{expected}' not found in {intents} for question: {question}"


# --------------- heatmap year filtering ---------------


def test_heatmap_not_filtered_by_deck_year() -> None:
    """Heatmap should retain all years even when deck control selects a year."""
    items = [
        {"year": "2022", "month": "Jan", "value": 100},
        {"year": "2023", "month": "Jan", "value": 200},
        {"year": "2024", "month": "Jan", "value": 300},
    ]
    # Old behavior would filter to selected_year, new behavior keeps all
    result = country_chat_service._filter_heatmap_items_for_year(items, None)
    assert len(result) == 3

    # Selected year still filters (for chat mode where user explicitly asked)
    result_filtered = country_chat_service._filter_heatmap_items_for_year(items, 2023)
    assert len(result_filtered) == 1
    assert result_filtered[0]["year"] == "2023"


@pytest.mark.usefixtures("_patch_base", "_patch_dashboard")
def test_enrichment_heatmap_keeps_all_years(monkeypatch) -> None:
    """In deck mode, heatmap enrichment should NOT filter by selected year."""
    heatmap_items = [
        {"year": "2023", "month": "Jan", "value": 100},
        {"year": "2024", "month": "Jan", "value": 200},
    ]
    monkeypatch.setattr(
        country_chat_service.query_service,
        "query_advanced_chart",
        lambda **kw: {"items": list(heatmap_items), "rows": 2}
        if kw.get("chart") == "seasonality_heatmap"
        else {"items": [{"label": "stub", "value": 1}], "rows": 1},
    )

    snapshot = country_chat_service.build_country_snapshot("瑞典", user_params={"year": 2024})
    country_chat_service._enrich_snapshot_for_intent(
        snapshot, "trend-summary", {"year": 2024},
    )

    # Heatmap should have BOTH years, not just 2024
    assert len(snapshot.get("seasonalityHeatmap", [])) == 2


# --------------- clause splitting ---------------


@pytest.mark.parametrize(
    "question,expected_count",
    [
        ("SUV市场怎么样？新能源呢？", 2),
        ("品牌排名，价格趋势，动力分布", 3),
        ("你好", 1),
        ("分析一下这个市场", 1),
        # Short tail clause merges into previous
        ("趋势如何？嗯？", 1),
    ],
)
def test_split_clauses(question, expected_count) -> None:
    clauses = country_chat_service._split_clauses(question)
    assert len(clauses) == expected_count


def test_split_clauses_content() -> None:
    clauses = country_chat_service._split_clauses("SUV市场怎么样？新能源呢？")
    assert "SUV" in clauses[0]
    assert "新能源" in clauses[1]


# --------------- weighted scoring ---------------


def test_score_clause_basic() -> None:
    scores = country_chat_service._score_clause("SUV细分市场分析")
    assert "segment-analysis" in scores
    # "细分" (weight 3) + "suv" (weight 2) = at least 5
    assert scores["segment-analysis"] >= 5


def test_score_clause_empty() -> None:
    assert country_chat_service._score_clause("你好世界") == {}


def test_score_clause_multi_intent() -> None:
    scores = country_chat_service._score_clause("品牌趋势分析")
    assert "brand-ranking" in scores
    assert "trend-summary" in scores


# --------------- compound question intent detection ---------------


@pytest.mark.parametrize(
    "question,expected_intents",
    [
        # Compound: two clauses, two different intents
        ("SUV市场怎么样？新能源呢？", ["segment-analysis", "nev-analysis"]),
        # Three clauses
        ("品牌排名，价格走势，新能源渗透率", ["nev-analysis", "pricing-summary", "brand-ranking", "trend-summary"]),
        # Mixed Chinese + English
        ("BEV渗透率如何？定位有竞争力吗？", ["nev-analysis", "positioning-analysis"]),
    ],
)
def test_compound_question_intents(question, expected_intents) -> None:
    intents = country_chat_service.infer_country_chat_intents(question)
    for expected in expected_intents:
        assert expected in intents, f"'{expected}' not in {intents} for: {question}"


# --------------- negation handling ---------------


@pytest.mark.parametrize(
    "question,excluded,included",
    [
        ("不看价格，只看销量趋势", "pricing-summary", "trend-summary"),
        ("不管品牌，分析细分市场", "brand-ranking", "segment-analysis"),
        ("先不看动力结构，说说新能源", "powertrain-mix", "nev-analysis"),
    ],
)
def test_negation(question, excluded, included) -> None:
    intents = country_chat_service.infer_country_chat_intents(question)
    assert excluded not in intents, f"'{excluded}' should be negated in {intents}"
    assert included in intents, f"'{included}' should be present in {intents}"


# --------------- market-context intent ---------------


@pytest.mark.parametrize(
    "question",
    [
        "德国有什么新能源补贴政策？",
        "关税对中国品牌有什么影响？",
        "最近有什么市场热点？",
    ],
)
def test_market_context_intent(question) -> None:
    intents = country_chat_service.infer_country_chat_intents(question)
    assert "market-context" in intents


# --------------- country profiles ---------------


def test_country_profile_lookup() -> None:
    profile = country_profiles.get_country_profile("Germany")
    assert profile is not None
    assert "key_policies" in profile
    assert "hot_topics" in profile
    assert len(profile["key_policies"]) > 0


def test_country_profile_alias() -> None:
    assert country_profiles.get_country_profile("德国") is country_profiles.get_country_profile("germany")
    assert country_profiles.get_country_profile("DE") is country_profiles.get_country_profile("deutschland")


def test_country_profile_finland_alias() -> None:
    assert country_profiles.get_country_profile(
        "芬兰"
    ) is country_profiles.get_country_profile("finland")
    assert country_profiles.get_country_profile(
        "FI"
    ) is country_profiles.get_country_profile("suomi")


@pytest.mark.parametrize(
    "left,right",
    [
        ("匈牙利", "hungary"),
        ("HU", "magyarorszag"),
        ("捷克", "czech republic"),
        ("CZ", "czechia"),
        ("斯洛伐克", "slovakia"),
        ("SK", "slovensko"),
        ("克罗地亚", "croatia"),
        ("HR", "hrvatska"),
        ("斯洛文尼亚", "slovenia"),
        ("SI", "slovenija"),
        ("奥地利", "austria"),
        ("AT", "osterreich"),
        ("瑞士", "switzerland"),
        ("CH", "schweiz"),
        ("罗马尼亚", "romania"),
        ("RO", "rou"),
        ("希腊", "greece"),
        ("GR", "ellada"),
        ("丹麦", "denmark"),
        ("DK", "danmark"),
    ],
)
def test_country_profile_batch_one_aliases(left: str, right: str) -> None:
    assert country_profiles.get_country_profile(
        left
    ) is country_profiles.get_country_profile(right)


def test_country_profile_missing() -> None:
    assert country_profiles.get_country_profile("火星") is None


def test_compact_profile() -> None:
    compact = country_profiles.get_compact_profile("France")
    assert compact is not None
    assert "key_policies" in compact
    assert "hot_topics" in compact
    assert "powertrain_context" in compact
    # Compact should NOT have market_characteristics
    assert "market_characteristics" not in compact


# --------------- knowledge injection ---------------


@pytest.mark.usefixtures("_patch_base", "_patch_dashboard")
def test_enrichment_market_context_injects_profile(monkeypatch) -> None:
    monkeypatch.delenv("NVIDIA_API_KEY", raising=False)
    monkeypatch.delenv("NVAPI_KEY", raising=False)

    snapshot = country_chat_service.build_country_snapshot("Germany")
    country_chat_service._enrich_snapshot_for_intent(
        snapshot, "market-context", {},
    )

    assert "countryProfile" in snapshot
    assert "Umweltbonus" in str(snapshot["countryProfile"]["key_policies"])


@pytest.mark.usefixtures("_patch_base")
def test_context_always_includes_compact_profile() -> None:
    """Country profile should be injected into context for ALL intents."""
    snapshot = country_chat_service.build_country_snapshot("Germany")
    ctx = country_chat_service._select_context_for_intent(
        snapshot, "brand-ranking",
    )

    assert "countryProfile" in ctx
    assert ctx["countryProfile"]["market_label"] == "Germany / 德国"


@pytest.mark.usefixtures("_patch_base")
def test_context_market_context_includes_news_digest() -> None:
    snapshot = country_chat_service.build_country_snapshot("Germany")
    ctx = country_chat_service._select_context_for_intent(
        snapshot, "market-context",
    )

    assert "newsDigest" in ctx
    assert "marketEvents" in ctx
    assert ctx["marketEvents"][0]["publisher"] == "Reuters"


# --------------- fallback answer with market-context ---------------


@pytest.mark.usefixtures("_patch_base")
def test_fallback_market_context_with_profile() -> None:
    snapshot = country_chat_service.build_country_snapshot("Germany")

    answer = country_chat_service._build_fallback_answer(
        country="Germany",
        question="有什么政策热点?",
        intent="market-context",
        snapshot=snapshot,
        provider_error=None,
    )

    assert "关键政策" in answer
    assert "市场热点" in answer
    assert "最新事件" in answer


@pytest.mark.usefixtures("_patch_base")
def test_fallback_market_context_without_profile() -> None:
    snapshot = country_chat_service.build_country_snapshot("火星")

    answer = country_chat_service._build_fallback_answer(
        country="火星",
        question="有什么政策热点?",
        intent="market-context",
        snapshot=snapshot,
        provider_error=None,
    )

    assert "暂无" in answer
