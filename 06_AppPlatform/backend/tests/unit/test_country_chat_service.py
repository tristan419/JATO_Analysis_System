import copy

import pandas as pd
import pytest

from app.services import country_chat_service
from app.services import country_profiles
from app.services import insight_card_service


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


def test_extract_user_params_chinese_powertrain() -> None:
    params = country_chat_service.extract_user_params("这款纯电SUV怎么样")
    assert params.get("powertrain") == "BEV"

    params2 = country_chat_service.extract_user_params("插混市场如何")
    assert params2.get("powertrain") == "PHEV"


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
        {"Brand": "VOLVO", "Model": "XC60", "Length": 4688, "MSRP": 45000, "Sales": 80000, "cluster": 0},
        {"Brand": "MERCEDES", "Model": "GLC", "Length": 4700, "MSRP": 48000, "Sales": 25000, "cluster": 0},
    ],
    "target": {"Length": 4500, "MSRP": 35000},
    "cluster_top3": ["VOLVO XC60", "MERCEDES GLC"],
}

_STUB_MODEL_VERSION_BUBBLE = {
    "rows": 2,
    "items": [
        {"Version": "XC60 Core", "Powertrain": "PHEV", "Trim": "Core", "Length": 4688, "MSRP": 45000, "Sales": 12000},
        {"Version": "XC60 Ultra", "Powertrain": "PHEV", "Trim": "Ultra", "Length": 4688, "MSRP": 52000, "Sales": 9000},
    ],
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


@pytest.mark.usefixtures("_patch_base", "_patch_dashboard")
def test_build_country_chart_deck_returns_full_snapshot() -> None:
    result = country_chat_service.build_country_chart_deck(
        "瑞典",
        question="中系SUV定价35000，续航和竞品表现怎么样？",
        extracted_params={"brand": "JAECOO", "length": 4500, "msrp": 35000},
    )

    snapshot = result["contextSnapshot"]
    assert result["country"] == "瑞典"
    assert result["primaryIntent"] == "positioning-analysis"
    assert "positioning-analysis" in result["deckIntents"]
    assert "trend-summary" in result["deckIntents"]
    assert "positioningMap" in snapshot
    assert "nevRangeDistribution" in snapshot
    assert "segmentShareByLength" in snapshot
    assert "priceMigration" in snapshot
    assert "modelVersionBubble" in snapshot
    assert "pricePerMeter" in snapshot
    assert "salesVsPrice" in snapshot
    assert "nevCapacityVsMsrp" in snapshot
    assert snapshot["marketEvents"]
    assert snapshot["newsDigest"] is not None
    assert result["controls"]["selectedModel"]


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
