import pandas as pd
import pytest

from app.services import market_scan_service
from app.api.schemas import MarketScanDeckRequest, PositioningPricingDeckRequest, VersionComparisonDeckRequest
from pydantic import ValidationError


def test_total_ranking_drive_share_is_4wd_over_total() -> None:
    """driveSharePct = 4WD / total volume (simple percentage)."""
    frame = pd.DataFrame(
        {
            "__model": ["XC60", "XC60", "XC60", "GLC", "GLC"],
            "__powertrain": ["BEV", "BEV", "BEV", "BEV", "BEV"],
            "__drive_type": ["4WD", "2WD", "OTHER", "4WD", "OTHER"],
            "m1": [40.0, 40.0, 20.0, 25.0, 25.0],
        },
    )

    items = market_scan_service._build_total_ranking_items(
        frame,
        current_columns=["m1"],
        prior_columns=[],
        fuel_order=["BEV"],
        ranking_limit=5,
    )

    xc60 = next(item for item in items if item["model"] == "XC60")
    glc = next(item for item in items if item["model"] == "GLC")

    # XC60: 4WD=40, total=100 → 40%
    assert xc60["driveMix"]["OTHER"] == pytest.approx(20.0)
    assert xc60["driveSharePct"] == pytest.approx(0.4)
    assert "driveCoveragePct" not in xc60
    # GLC: 4WD=25, total=50 → 50%
    assert glc["driveSharePct"] == pytest.approx(0.5)
    assert "driveCoveragePct" not in glc


def test_total_ranking_includes_registration_mix() -> None:
    frame = pd.DataFrame(
        {
            "__model": ["XC60", "XC60", "XC60"],
            "__powertrain": ["BEV", "BEV", "PHEV"],
            "__drive_type": ["4WD", "2WD", "4WD"],
            "__registration_type": ["Business", "Private", "Other"],
            "m1": [50.0, 30.0, 20.0],
        },
    )

    items = market_scan_service._build_total_ranking_items(
        frame,
        current_columns=["m1"],
        prior_columns=[],
        fuel_order=["BEV", "PHEV"],
        ranking_limit=5,
    )

    xc60 = next(item for item in items if item["model"] == "XC60")

    assert xc60["registrationMix"]["Business"] == pytest.approx(50.0)
    assert xc60["registrationMix"]["Private"] == pytest.approx(30.0)
    assert xc60["registrationMix"]["Other"] == pytest.approx(20.0)


def test_single_fuel_ranking_sorted_by_volume() -> None:
    """Single-fuel rankings must sort by volume, not share."""
    frame = pd.DataFrame(
        {
            "__model": ["A", "B", "C"],
            "__powertrain": ["BEV", "BEV", "BEV"],
            "__drive_type": ["4WD", "2WD", "4WD"],
            "m1": [100.0, 300.0, 200.0],
        },
    )

    items = market_scan_service._build_single_fuel_ranking_items(
        frame,
        fuel_type="BEV",
        current_columns=["m1"],
        prior_columns=[],
        segment_total=600.0,
        ranking_limit=5,
    )

    assert [it["model"] for it in items] == ["B", "C", "A"]
    assert items[0]["barPct"] == pytest.approx(1.0)
    assert items[1]["barPct"] == pytest.approx(200 / 300)


def test_segment_payload_includes_suv_segment_share_breakdown() -> None:
    frame = pd.DataFrame(
        {
            "__segment_raw": ["SUV A00", "SUV A0", "SUV A", "SUV B", "Car A"],
            "2026 Jan": [10.0, 20.0, 30.0, 40.0, 100.0],
            "2026 Feb": [15.0, 25.0, 35.0, 45.0, 80.0],
        },
    )

    payload = market_scan_service._build_segment_payload(
        frame=frame,
        available_periods=["2026-01", "2026-02"],
        resolved_period="2026-02",
        prior_period="2026-01",
        same_month_last_year_period=None,
        body_window_months=2,
    )

    items = payload["suvSegmentShareTrend"]["items"]
    assert len(items) == 2
    latest = items[-1]
    latest_body_share = payload["bodyShareTrend"]["items"][-1]
    assert latest["label"] == "26.02"
    assert latest["segmentSharePct"]["SUV-A00"] == pytest.approx(15 / 200)
    assert latest["segmentSharePct"]["SUV-A0"] == pytest.approx(25 / 200)
    assert latest["segmentSharePct"]["SUV-A"] == pytest.approx(35 / 200)
    assert latest["segmentSharePct"]["≥SUV-B"] == pytest.approx(45 / 200)
    assert sum(latest["segmentSharePct"].values()) == pytest.approx(latest_body_share["suvSharePct"])


def test_segment_payload_includes_overall_origin_channel_mix() -> None:
    frame = pd.DataFrame(
        {
            "__segment_raw": ["SUV A0", "SUV A0", "Car A", "Car A"],
            "__origin": ["欧系", "欧系", "日系", "日系"],
            "__registration_type": ["Business", "Private", "Private", "Other"],
            "2026 Jan": [40.0, 20.0, 35.0, 5.0],
            "2026 Feb": [60.0, 40.0, 50.0, 10.0],
        },
    )

    payload = market_scan_service._build_segment_payload(
        frame=frame,
        available_periods=["2026-01", "2026-02"],
        resolved_period="2026-02",
        prior_period="2026-01",
        same_month_last_year_period=None,
        body_window_months=2,
    )

    assert payload["channelMix"]["options"][0]["value"] == "overall"
    assert payload["channelMix"]["month"]["defaultView"] == "origin"

    month_items = payload["channelMix"]["month"]["items"]
    overall = month_items[0]
    origin_items = payload["channelMix"]["month"]["views"]["origin"]["items"]
    europe = next(item for item in origin_items if item["label"] == "欧系")
    japan = next(item for item in origin_items if item["label"] == "日系")

    assert overall["label"] == "整体市场"
    assert overall["channelSharePct"]["Business"] == pytest.approx(60 / 160)
    assert europe["volume"] == pytest.approx(100.0)
    assert europe["channelMix"]["Business"] == pytest.approx(60.0)
    assert europe["channelSharePct"]["Business"] == pytest.approx(0.6)
    assert japan["channelMix"]["Private"] == pytest.approx(50.0)
    assert japan["channelSharePct"]["Other"] == pytest.approx(10 / 60)


def test_origin_payload_includes_brand_level_trend_groups() -> None:
    frame = pd.DataFrame(
        {
            "__origin": ["欧系", "欧系", "欧系", "欧系", "欧系", "日系", "日系"],
            "__brand": ["VOLVO", "BMW", "VW", "AUDI", "SKODA", "TOYOTA", "NISSAN"],
            "2026 Jan": [50.0, 40.0, 30.0, 20.0, 10.0, 60.0, 30.0],
            "2026 Feb": [55.0, 45.0, 35.0, 25.0, 5.0, 70.0, 20.0],
        },
    )

    payload = market_scan_service._build_origin_payload(
        frame=frame,
        available_periods=["2026-01", "2026-02"],
        resolved_period="2026-02",
        prior_period="2026-01",
        same_month_last_year_period=None,
        origin_window_months=2,
    )

    groups = payload["brandTrend"]["groups"]
    europe = next(group for group in groups if group["origin"] == "欧系")
    japan = next(group for group in groups if group["origin"] == "日系")

    assert [series["brand"] for series in europe["series"]] == ["VOLVO", "BMW", "VW", "AUDI"]
    assert europe["series"][0]["points"][-1]["volume"] == pytest.approx(55.0)
    assert europe["series"][0]["points"][-1]["label"] == "26.02"
    assert [series["brand"] for series in japan["series"]] == ["TOYOTA", "NISSAN"]


def test_monthly_brand_ranking_includes_model_breakdown() -> None:
    frame = pd.DataFrame(
        {
            "__brand": ["VOLVO", "VOLVO", "VOLVO", "BMW", "BMW"],
            "__model": ["XC60", "EX30", "XC40", "X3", "iX1"],
            "__powertrain": ["PHEV", "BEV", "BEV", "ICE", "BEV"],
            "m1": [60.0, 30.0, 10.0, 70.0, 20.0],
            "m0": [55.0, 25.0, 5.0, 65.0, 15.0],
            "m_12": [50.0, 20.0, 8.0, 60.0, 10.0],
        }
    )

    items = market_scan_service._build_brand_ranking_items(
        frame,
        current_columns=["m1"],
        prior_columns=["m_12"],
        prior_month_columns=["m0"],
        ranking_limit=5,
        include_model_breakdown=True,
    )

    volvo = next(item for item in items if item["brand"] == "VOLVO")
    bmw = next(item for item in items if item["brand"] == "BMW")

    assert [entry["model"] for entry in volvo["modelBreakdown"]] == ["XC60", "EX30", "XC40"]
    assert [entry["powertrain"] for entry in volvo["modelBreakdown"]] == ["PHEV", "BEV", "BEV"]
    assert volvo["modelBreakdown"][0]["volume"] == pytest.approx(60.0)
    assert volvo["modelBreakdown"][0]["sharePct"] == pytest.approx(0.6)
    assert [entry["model"] for entry in bmw["modelBreakdown"]] == ["X3", "iX1"]


def test_overview_payload_includes_monthly_and_rolling12_model_breakdown() -> None:
    frame = pd.DataFrame(
        {
            "__brand": ["VOLVO", "VOLVO", "BMW", "BMW"],
            "__model": ["XC60", "EX30", "X3", "iX1"],
            "__powertrain": ["BEV", "BEV", "BEV", "BEV"],
            "2025 Jan": [100.0, 80.0, 70.0, 60.0],
            "2025 Feb": [110.0, 90.0, 75.0, 65.0],
            "2026 Jan": [120.0, 100.0, 85.0, 70.0],
            "2026 Feb": [130.0, 95.0, 90.0, 75.0],
        }
    )

    payload = market_scan_service._build_overview_payload(
        frame=frame,
        selected_fuels=["ICE", "MHEV", "HEV", "PHEV", "BEV", "LPG"],
        available_periods=["2025-01", "2025-02", "2026-01", "2026-02"],
        resolved_period="2026-02",
        prior_period="2026-01",
        same_month_last_year_period="2025-02",
        ranking_limit=10,
    )

    assert payload["monthlyBrandRanking"]["items"][0]["modelBreakdown"]
    assert payload["rolling12BrandRanking"]["items"][0]["modelBreakdown"]
    assert payload["ytdBrandRanking"]["items"][0]["modelBreakdown"]
    assert payload["summary"]["rolling12Volume"] > 0


def test_overview_payload_includes_suv_fuel_mix_for_trend() -> None:
    frame = pd.DataFrame(
        {
            "__brand": ["TOYOTA", "KIA", "TOYOTA", "VOLVO"],
            "__model": ["C-HR", "Niro", "Corolla", "XC40"],
            "__powertrain": ["HEV", "HEV", "HEV", "BEV"],
            "__segment_raw": ["SUV A", "SUV A0", "C", "SUV A"],
            "2026 Jan": [100.0, 14.0, 280.0, 60.0],
            "2026 Feb": [120.0, 18.0, 260.0, 80.0],
        }
    )

    payload = market_scan_service._build_overview_payload(
        frame=frame,
        selected_fuels=["HEV", "BEV"],
        available_periods=["2026-01", "2026-02"],
        resolved_period="2026-02",
        prior_period="2026-01",
        same_month_last_year_period=None,
        ranking_limit=10,
    )

    latest = payload["trend"]["items"][-1]
    assert latest["fuelMix"]["HEV"] == pytest.approx(398.0)
    assert latest["suvFuelMix"]["HEV"] == pytest.approx(138.0)
    assert latest["suvFuelMix"]["BEV"] == pytest.approx(80.0)
    assert latest["suvTotalVolume"] == pytest.approx(218.0)


def test_overview_payload_supports_custom_range_prior_label() -> None:
    frame = pd.DataFrame(
        {
            "__brand": ["VOLVO", "BMW"],
            "__model": ["EX30", "X3"],
            "__powertrain": ["BEV", "ICE"],
            "__segment_raw": ["SUV A0", "SUV A"],
            "2025 Jan": [50.0, 30.0],
            "2025 Feb": [60.0, 40.0],
            "2025 Mar": [70.0, 50.0],
            "2026 Jan": [100.0, 80.0],
            "2026 Feb": [120.0, 90.0],
            "2026 Mar": [140.0, 110.0],
        }
    )

    payload = market_scan_service._build_overview_payload(
        frame=frame,
        selected_fuels=["BEV", "ICE"],
        available_periods=[
            "2025-01",
            "2025-02",
            "2025-03",
            "2026-01",
            "2026-02",
            "2026-03",
        ],
        resolved_period="2026-03",
        prior_period="2026-02",
        same_month_last_year_period="2025-03",
        ranking_limit=10,
        custom_range_periods=["2026-01", "2026-02", "2026-03"],
    )

    assert payload["summary"]["customRangeVolume"] == pytest.approx(640.0)
    assert payload["summary"]["customRangeLabel"] == "26.01 - 26.03"
    assert payload["customRangeBrandRanking"]["priorLabel"] == "25.01 - 25.03"
    assert payload["customRangeBrandRanking"]["items"][0]["brand"] == "VOLVO"


def test_drilldown_payload_includes_month_rolling12_and_ytd_variants() -> None:
    frame = pd.DataFrame(
        {
            "__segment_raw": ["SUV A0", "SUV A0", "SUV A0", "SUV A0"],
            "__model": ["EX30", "XC40", "EX30", "XC40"],
            "__powertrain": ["BEV", "BEV", "PHEV", "PHEV"],
            "__drive_type": ["4WD", "2WD", "4WD", "2WD"],
            "__registration_type": ["Private", "Business", "Private", "Business"],
            "2025 Apr": [80.0, 40.0, 30.0, 20.0],
            "2026 Jan": [90.0, 45.0, 35.0, 18.0],
            "2026 Feb": [95.0, 48.0, 36.0, 19.0],
            "2026 Mar": [100.0, 50.0, 40.0, 20.0],
            "2026 Apr": [60.0, 55.0, 25.0, 22.0],
        }
    )

    drilldown_map = market_scan_service._build_all_drilldowns(
        frame=frame,
        available_periods=["2025-04", "2026-01", "2026-02", "2026-03", "2026-04"],
        resolved_period="2026-04",
        same_month_last_year_period="2025-04",
        segment_values=["SUV A0"],
        fuel_panels=("BEV", "PHEV"),
        ranking_limit=10,
    )
    payload = drilldown_map["SUV A0"]

    assert payload["monthTotalRanking"]["title"] == "Monthly Total Model Ranking"
    assert payload["monthTotalRanking"]["items"][0]["model"] == "EX30"
    assert payload["rolling12TotalRanking"]["title"] == "Rolling 12M Total Model Ranking"
    assert payload["rolling12TotalRanking"]["items"][0]["model"] == "EX30"
    assert payload["totalRanking"]["title"] == "YTD Total Model Ranking"
    assert payload["monthFuelTrend"]["items"][-1]["label"] == "26.04"
    assert payload["rolling12FuelTrend"]["items"][-1]["label"] == "L12M 26.04"
    assert payload["ytdFuelTrend"]["items"][-1]["label"] == "26,1-04"


def test_market_scan_deck_request_defaults_to_top10() -> None:
    payload = MarketScanDeckRequest()
    assert payload.ranking_limit == 10


def test_market_scan_deck_request_rejects_ranking_limit_below_top10() -> None:
    with pytest.raises(ValidationError):
        MarketScanDeckRequest(ranking_limit=6)


def test_query_market_scan_deck_clamps_ranking_limit_to_top10(monkeypatch: pytest.MonkeyPatch) -> None:
    observed: dict[str, int] = {}

    def _stub_query_market_scan_deck_impl(
        country: str | None,
        target_period: str | None,
        time_range: dict[str, str] | None,
        fuel_types: list[str],
        trend_window_months: int,
        origin_window_months: int,
        body_window_months: int,
        ranking_limit: int,
        drilldown_segments: list[str],
        body_types: list[str],
        view: str | None = None,
    ) -> dict[str, object]:
        observed["ranking_limit"] = ranking_limit
        observed["drilldown_segments"] = len(drilldown_segments)
        observed["body_types"] = len(body_types)
        return {"ok": True}

    market_scan_service._deck_cache.clear()
    monkeypatch.setattr(market_scan_service, "get_redis_client", lambda: None)
    monkeypatch.setattr(market_scan_service.repo, "current_dataset_token", lambda: "test-token")
    monkeypatch.setattr(market_scan_service, "_query_market_scan_deck_impl", _stub_query_market_scan_deck_impl)

    result = market_scan_service.query_market_scan_deck(
        country="SE",
        target_period="2025-02",
        time_range=None,
        fuel_types=["BEV"],
        trend_window_months=24,
        origin_window_months=24,
        body_window_months=24,
        ranking_limit=6,
        drilldown_segments=["SUV A0"],
        body_types=[],
    )

    assert result == {"ok": True}
    assert observed["ranking_limit"] == 10
    assert observed["drilldown_segments"] == 1
    assert observed["body_types"] == 0


def test_query_ranking_trend_returns_iso_months_and_market_share_denominator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeTable:
        def __init__(self, frame: pd.DataFrame) -> None:
            self._frame = frame

        def to_pandas(self) -> pd.DataFrame:
            return self._frame.copy()

    class FakeDataset:
        def __init__(self, frame: pd.DataFrame) -> None:
            self._frame = frame

        def to_table(self, columns: list[str], filter: object | None = None) -> FakeTable:
            return FakeTable(self._frame[columns])

    columns = market_scan_service.ColumnMap(
        country_value="Country",
        country_label=None,
        make="Make",
        model="Model",
        version=None,
        trim=None,
        length="Length",
        msrp="MSRP",
        origin=None,
        segment="Segment",
        powertrain="Powertrain",
        body_type=None,
        drive_type=None,
        registration_type=None,
        month_columns=("2025 Apr", "2025 Feb", "2025 Jan", "2025 Dec"),
    )
    frame = pd.DataFrame(
        [
            {
                "Country": "Spain",
                "Make": "KIA",
                "Model": "KIA EV3",
                "Segment": "SUV A0",
                "Powertrain": "BEV",
                "Length": 4300,
                "MSRP": 40000,
                "2025 Apr": 30,
                "2025 Feb": 20,
                "2025 Jan": 10,
                "2025 Dec": 40,
            },
            {
                "Country": "Spain",
                "Make": "VOLVO",
                "Model": "Volvo EX30",
                "Segment": "SUV A0",
                "Powertrain": "BEV",
                "Length": 4233,
                "MSRP": 41000,
                "2025 Apr": 70,
                "2025 Feb": 80,
                "2025 Jan": 90,
                "2025 Dec": 60,
            },
        ],
    )

    monkeypatch.setattr(market_scan_service, "_get_columns", lambda: columns)
    monkeypatch.setattr(market_scan_service, "_country_options", lambda dataset_token: [{"value": "Spain", "label": "Spain"}])
    monkeypatch.setattr(market_scan_service.repo, "current_dataset_token", lambda: "test-token")
    monkeypatch.setattr(market_scan_service.repo, "_open_dataset", lambda: FakeDataset(frame))
    monkeypatch.setattr(market_scan_service.repo, "_build_filter_expression", lambda filters: None)

    result = market_scan_service.query_ranking_trend(
        country="Spain",
        brand="KIA",
        model="KIA EV3",
        segment="SUV A0",
        fuel_types=["BEV"],
    )

    assert [item["month"] for item in result["trend"]] == ["2025-01", "2025-02", "2025-04", "2025-12"]
    assert [item["sales"] for item in result["trend"]] == [10, 20, 30, 40]
    assert [item["marketShare"] for item in result["trend"]] == pytest.approx([0.1, 0.2, 0.3, 0.4])
    assert result["summary"]["marketShare"] == pytest.approx(0.4)


def test_clear_market_scan_local_cache_removes_cached_decks() -> None:
    market_scan_service._deck_cache.clear()
    market_scan_service._deck_cache["Sweden|2026-03"] = (1.0, "token", {"ok": True})

    result = market_scan_service.clear_market_scan_local_cache()

    assert result == {"enabled": True, "clearedCount": 1}
    assert market_scan_service._deck_cache == {}


def test_normalize_positioning_sales_mode_defaults_to_month() -> None:
    assert market_scan_service._normalize_positioning_sales_mode(None) == "month"
    assert market_scan_service._normalize_positioning_sales_mode("bad-mode") == "month"
    assert market_scan_service._normalize_positioning_sales_mode("ytd") == "ytd"
    assert market_scan_service._normalize_positioning_sales_mode("rolling12") == "rolling12"


def test_resolve_positioning_sales_window_supports_month_ytd_and_rolling12() -> None:
    available_periods = [
        "2025-04", "2025-05", "2025-06", "2025-07", "2025-08", "2025-09",
        "2025-10", "2025-11", "2025-12", "2026-01", "2026-02", "2026-03",
    ]

    month_periods, month_label, month_metric = market_scan_service._resolve_positioning_sales_window(
        available_periods,
        "2026-03",
        "month",
    )
    ytd_periods, ytd_label, ytd_metric = market_scan_service._resolve_positioning_sales_window(
        available_periods,
        "2026-03",
        "ytd",
    )
    rolling_periods, rolling_label, rolling_metric = market_scan_service._resolve_positioning_sales_window(
        available_periods,
        "2026-03",
        "rolling12",
    )

    assert month_periods == ["2026-03"]
    assert month_label == "当月"
    assert month_metric == "Current Month Sales"
    assert ytd_periods == ["2026-01", "2026-02", "2026-03"]
    assert ytd_label == "YTD"
    assert ytd_metric == "YTD Sales"
    assert rolling_periods == available_periods
    assert rolling_label == "近12个月"
    assert rolling_metric == "Rolling 12M Sales"


def test_normalize_period_range_returns_custom_interval_and_skips_default_latest() -> None:
    available_periods = ["2026-01", "2026-02", "2026-03", "2026-04"]

    assert market_scan_service._normalize_period_range(
        available_periods,
        {"start": "2026-02", "end": "2026-04"},
        "2026-04",
    ) == ["2026-02", "2026-03", "2026-04"]
    assert market_scan_service._normalize_period_range(
        available_periods,
        {"start": "2026-04", "end": "2026-04"},
        "2026-04",
    ) is None
    assert market_scan_service._normalize_period_range(
        available_periods,
        {"start": "2026-02", "end": "2026-02"},
        "2026-02",
    ) is None
    assert market_scan_service._resolve_period("2026 Mar", available_periods) == "2026-03"
    assert market_scan_service._normalize_period_range(
        available_periods,
        {"start": "2026 Jan", "end": "2026 Mar"},
        "2026-04",
    ) == ["2026-01", "2026-02", "2026-03"]


def test_market_scan_data_quality_reports_fallbacks_and_fuel_scope() -> None:
    available_periods = ["2026-01", "2026-02", "2026-03"]
    range_detail = market_scan_service._resolve_period_range_detail(
        available_periods,
        {"start": "2026-01", "end": "2026-04"},
        "2026-03",
    )

    quality = market_scan_service._build_market_scan_data_quality(
        requested_country="Atlantis",
        selected_country={"value": "瑞典", "label": "Sweden"},
        requested_period="2026-04",
        resolved_period="2026-03",
        range_detail=range_detail,
        requested_fuels=["BEV", "Hydrogen"],
        available_fuels=["BEV", "ICE"],
        selected_fuels=["BEV"],
        source_row_count=12,
        filtered_row_count=10,
    )

    assert quality["countryFallbackApplied"] is True
    assert quality["periodFallbackApplied"] is True
    assert quality["timeRangeFallbackApplied"] is True
    assert quality["resolvedTimeRange"] == {"start": "2026-01", "end": "2026-03"}
    assert quality["unavailableFuelTypes"] == ["HYDROGEN"]
    assert quality["fuelRowsExcluded"] == 2
    assert len(quality["warnings"]) == 5


def test_market_scan_default_country_is_sweden() -> None:
    selected = market_scan_service._normalize_country_lookup(
        None,
        [
            {"value": "匈牙利", "label": "Hungary"},
            {"value": "瑞典", "label": "Sweden"},
        ],
    )

    assert selected == {"value": "瑞典", "label": "Sweden"}


def test_resolve_positioning_sales_window_prefers_custom_range() -> None:
    available_periods = ["2026-01", "2026-02", "2026-03", "2026-04"]

    periods, label, metric = market_scan_service._resolve_positioning_sales_window(
        available_periods,
        "2026-04",
        "month",
        ["2026-02", "2026-03"],
    )

    assert periods == ["2026-02", "2026-03"]
    assert label == "自定义区间"
    assert metric == "Custom Range Sales"


def test_positioning_pricing_deck_request_accepts_ytd_sales_mode() -> None:
    payload = PositioningPricingDeckRequest(sales_mode="ytd", length_min=4200, length_max=5000)
    assert payload.sales_mode == "ytd"
    assert payload.length_min == pytest.approx(4200)
    assert payload.length_max == pytest.approx(5000)


def test_version_comparison_deck_request_accepts_ytd_sales_mode() -> None:
    payload = VersionComparisonDeckRequest(sales_mode="ytd", refill_models=True)
    assert payload.sales_mode == "ytd"
    assert payload.refill_models is True


def test_query_version_comparison_deck_uses_local_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_impl(**kwargs: object) -> dict[str, object]:
        calls.append(kwargs)
        return {
            "metadata": {"selectedModels": kwargs.get("models", [])},
            "page": {"bubbleChart": {"items": []}},
        }

    monkeypatch.setattr(market_scan_service.repo, "current_dataset_token", lambda: "dataset-token")
    monkeypatch.setattr(market_scan_service, "get_redis_client", lambda: None)
    monkeypatch.setattr(market_scan_service, "_query_version_comparison_deck_impl", fake_impl)
    market_scan_service._deck_cache.clear()

    kwargs = {
        "country": "Denmark",
        "target_period": "2026-05",
        "time_range": None,
        "fuel_types": ["BEV"],
        "sales_mode": "rolling12",
        "comparison_mode": "same_segment",
        "segment": "SUV A0",
        "models": ["BYD::ATTO 2"],
        "refill_models": False,
        "msrp_min": None,
        "msrp_max": None,
        "price_band_size": None,
        "body_type": None,
        "drive_types": [],
        "segments": [],
        "length_min": None,
        "length_max": None,
    }

    try:
        first = market_scan_service.query_version_comparison_deck(**kwargs)
        second = market_scan_service.query_version_comparison_deck(**kwargs)
        assert first is second
        assert len(calls) == 1

        market_scan_service.query_version_comparison_deck(**{**kwargs, "models": ["MG::MG ZS"]})
        assert len(calls) == 2

        market_scan_service.query_version_comparison_deck(**{**kwargs, "refill_models": True})
        assert len(calls) == 3
    finally:
        market_scan_service._deck_cache.clear()


def test_resolve_version_comparison_models_defaults_to_top3() -> None:
    frame = pd.DataFrame(
        {
            "__model": ["A", "B", "C", "D"],
            "__comparison_sales": [400.0, 300.0, 200.0, 100.0],
        }
    )

    selected, options = market_scan_service._resolve_version_comparison_models(
        frame,
        [],
        sales_column="__comparison_sales",
    )

    assert selected == ["A", "B", "C"]
    assert [option["value"] for option in options] == ["A", "B", "C", "D"]


def test_resolve_version_comparison_models_caps_requested_models_at_10() -> None:
    frame = pd.DataFrame(
        {
            "__model": list("ABCDEFGHIJKL"),
            "__comparison_sales": [1200.0, 1100.0, 1000.0, 900.0, 800.0, 700.0, 600.0, 500.0, 400.0, 300.0, 200.0, 100.0],
        }
    )

    selected, _ = market_scan_service._resolve_version_comparison_models(
        frame,
        ["L", "K", "J", "I", "H", "G", "F", "E", "D", "C", "B", "A", "A"],
        sales_column="__comparison_sales",
    )

    assert selected == ["L", "K", "J", "I", "H", "G", "F", "E", "D", "C"]


def test_resolve_version_comparison_models_refills_valid_requested_models() -> None:
    frame = pd.DataFrame(
        {
            "__model": ["A", "B", "C", "D"],
            "__comparison_sales": [400.0, 300.0, 200.0, 100.0],
        }
    )

    selected, _ = market_scan_service._resolve_version_comparison_models(
        frame,
        ["D", "Missing"],
        sales_column="__comparison_sales",
        refill_models=True,
    )

    assert selected == ["D", "A", "B"]


def test_version_comparison_model_options_use_brand_model_stable_key() -> None:
    frame = pd.DataFrame(
        {
            "__brand": ["BRAND A", "BRAND B", "BRAND C"],
            "__model": ["Twin", "Twin", "Solo"],
            "__model_key": ["BRAND A::Twin", "BRAND B::Twin", "BRAND C::Solo"],
            "__comparison_sales": [300.0, 200.0, 100.0],
        }
    )

    selected, options = market_scan_service._resolve_version_comparison_models(
        frame,
        ["BRAND B::Twin"],
        sales_column="__comparison_sales",
    )

    assert selected == ["BRAND B::Twin"]
    assert [option["value"] for option in options] == ["BRAND A::Twin", "BRAND B::Twin", "BRAND C::Solo"]
    assert [option["label"] for option in options[:2]] == ["Twin", "Twin"]
    assert [option["brand"] for option in options[:2]] == ["BRAND A", "BRAND B"]


def test_version_comparison_bubbles_keep_same_model_names_separate_by_brand() -> None:
    frame = pd.DataFrame(
        {
            "__brand": ["BRAND A", "BRAND B"],
            "__model": ["Twin", "Twin"],
            "__model_key": ["BRAND A::Twin", "BRAND B::Twin"],
            "__version": ["Base", "Base"],
            "__trim": ["Base", "Base"],
            "__powertrain": ["BEV", "BEV"],
            "__length": [4300.0, 4400.0],
            "__msrp": [30000.0, 32000.0],
            "__comparison_sales": [300.0, 200.0],
        }
    )

    items = market_scan_service._build_version_comparison_bubble_items(
        frame[frame["__model_key"] == "BRAND B::Twin"],
        sales_column="__comparison_sales",
    )

    assert len(items) == 1
    assert items[0]["modelKey"] == "BRAND B::Twin"
    assert items[0]["brand"] == "BRAND B"
    assert items[0]["model"] == "Twin"
    assert items[0]["sales"] == pytest.approx(200.0)


def test_build_positioning_price_bands_stacks_sales_by_fuel() -> None:
    frame = pd.DataFrame(
        {
            "__powertrain": ["BEV", "PHEV", "BEV", "ICE"],
            "__msrp": [21000.0, 22000.0, 28000.0, 41000.0],
            "2026 Apr": [120.0, 80.0, 60.0, 40.0],
        }
    )

    payload = market_scan_service._build_positioning_price_bands(
        frame,
        sales_column="2026 Apr",
        selected_fuels=["BEV", "PHEV", "ICE"],
    )

    assert payload["bandSize"] == 2500
    assert payload["items"][0]["fuelMix"]["BEV"] == pytest.approx(120.0)
    assert payload["items"][0]["fuelMix"]["PHEV"] == pytest.approx(80.0)
    assert payload["items"][-1]["fuelMix"]["ICE"] == pytest.approx(40.0)


def test_build_positioning_price_bands_respects_custom_range_and_step() -> None:
    frame = pd.DataFrame(
        {
            "__powertrain": ["BEV", "ICE", "HEV"],
            "__msrp": [46000.0, 61000.0, 72000.0],
            "2026 Apr": [120.0, 80.0, 60.0],
        }
    )

    payload = market_scan_service._build_positioning_price_bands(
        frame,
        sales_column="2026 Apr",
        selected_fuels=["BEV", "HEV", "ICE"],
        msrp_min=45000.0,
        msrp_max=65000.0,
        band_size=5000,
    )

    assert payload["bandSize"] == 5000
    assert payload["range"] == {"min": 45000.0, "max": 65000.0}
    assert [item["label"] for item in payload["items"]] == [
        "45,000-50,000",
        "50,000-55,000",
        "55,000-60,000",
        "60,000-65,000",
    ]
    assert payload["items"][0]["bandMid"] == pytest.approx(47500.0)
    assert payload["items"][0]["fuelMix"]["BEV"] == pytest.approx(120.0)
    assert payload["items"][1]["sales"] == pytest.approx(0.0)
    assert payload["items"][-1]["fuelMix"]["ICE"] == pytest.approx(80.0)


def test_positioning_page_rows_supports_all_suv_scope() -> None:
    frame = pd.DataFrame(
        {
            "__segment_raw": ["SUV A0", "SUV A", "SUV B", "SUV C", "Car A"],
            "__model": ["A", "B", "C", "D", "E"],
        }
    )

    rows = market_scan_service._positioning_page_rows(frame, "suvAll")

    assert list(rows["__segment_raw"]) == ["SUV A0", "SUV A", "SUV B", "SUV C"]


def test_build_positioning_bubble_items_uses_min_msrp_and_sales_sum() -> None:
    frame = pd.DataFrame(
        {
            "__brand": ["BYD", "BYD", "TESLA"],
            "__model": ["Song", "Song", "Model Y"],
            "__powertrain": ["BEV", "BEV", "BEV"],
            "__segment_raw": ["SUV A", "SUV A", "SUV B"],
            "__length": [4700.0, 4720.0, 4790.0],
            "__msrp": [18900.0, 20900.0, 39900.0],
            "2026 Apr": [150.0, 50.0, 90.0],
        }
    )

    items = market_scan_service._build_positioning_bubble_items(
        frame,
        sales_column="2026 Apr",
        bubble_limit=10,
    )

    song = next(item for item in items if item["model"] == "Song")
    assert song["msrpMin"] == pytest.approx(18900.0)
    assert song["msrpMax"] == pytest.approx(20900.0)
    assert song["sales"] == pytest.approx(200.0)
    assert song["variantCount"] == 2


def test_build_positioning_bubble_items_keep_same_model_names_separate_by_brand() -> None:
    frame = pd.DataFrame(
        {
            "__brand": ["BRAND A", "BRAND B"],
            "__model": ["Twin", "Twin"],
            "__model_key": ["BRAND A::Twin", "BRAND B::Twin"],
            "__powertrain": ["BEV", "BEV"],
            "__segment_raw": ["SUV A", "SUV A"],
            "__length": [4300.0, 4400.0],
            "__msrp": [30000.0, 32000.0],
            "2026 Apr": [300.0, 200.0],
        }
    )

    payload = market_scan_service._build_positioning_page_payload(
        frame,
        page_key="suvAll",
        title="全 SUV",
        subtitle="全 SUV 价格带与动力定位",
        sales_column="2026 Apr",
        sales_metric_label="Current Month Sales",
        sales_metric_detail="当月销量",
        selected_fuels=["BEV"],
        top_n=10,
        msrp_min=None,
        msrp_max=None,
        price_band_size=10000,
    )

    tracked_models = next(metric for metric in payload["metrics"] if metric["label"] == "Tracked Models")
    assert tracked_models["value"] == 2
    assert {item["modelKey"] for item in payload["bubbleChart"]["items"]} == {"BRAND A::Twin", "BRAND B::Twin"}


def test_build_positioning_bubble_items_respects_range_and_top_n() -> None:
    frame = pd.DataFrame(
        {
            "__brand": ["A", "B", "C", "D"],
            "__model": ["One", "Two", "Three", "Four"],
            "__powertrain": ["BEV", "PHEV", "HEV", "ICE"],
            "__segment_raw": ["SUV A", "SUV A", "SUV A", "SUV A"],
            "__length": [4600.0, 4650.0, 4700.0, 4750.0],
            "__msrp": [43000.0, 50000.0, 56000.0, 62000.0],
            "2026 Apr": [300.0, 250.0, 220.0, 200.0],
        }
    )

    items = market_scan_service._build_positioning_bubble_items(
        frame,
        sales_column="2026 Apr",
        bubble_limit=2,
        msrp_min=45000.0,
        msrp_max=60000.0,
    )

    assert [item["model"] for item in items] == ["Two", "Three"]


def test_build_positioning_bubble_items_respects_length_range() -> None:
    frame = pd.DataFrame(
        {
            "__brand": ["A", "B", "C", "D"],
            "__model": ["One", "Two", "Three", "Four"],
            "__powertrain": ["BEV", "PHEV", "HEV", "ICE"],
            "__segment_raw": ["SUV A", "SUV A", "SUV A", "SUV A"],
            "__length": [4500.0, 4650.0, 4700.0, 4850.0],
            "__msrp": [43000.0, 50000.0, 56000.0, 62000.0],
            "2026 Apr": [300.0, 250.0, 220.0, 200.0],
        }
    )

    items = market_scan_service._build_positioning_bubble_items(
        frame,
        sales_column="2026 Apr",
        bubble_limit=10,
        length_min=4600.0,
        length_max=4750.0,
    )

    assert [item["model"] for item in items] == ["Two", "Three"]


def test_build_positioning_page_payload_applies_length_range_to_bands() -> None:
    frame = pd.DataFrame(
        {
            "__brand": ["A", "B"],
            "__model": ["Short", "Long"],
            "__powertrain": ["BEV", "ICE"],
            "__segment_raw": ["SUV A", "SUV B"],
            "__length": [4300.0, 4800.0],
            "__msrp": [22000.0, 32000.0],
            "2026 Apr": [80.0, 120.0],
        }
    )

    payload = market_scan_service._build_positioning_page_payload(
        frame,
        page_key="suvAll",
        title="全 SUV",
        subtitle="全 SUV 价格带与动力定位",
        sales_column="2026 Apr",
        sales_metric_label="Current Month Sales",
        sales_metric_detail="当月销量",
        selected_fuels=["BEV", "ICE"],
        top_n=10,
        msrp_min=None,
        msrp_max=None,
        price_band_size=10000,
        length_min=4700.0,
        length_max=4900.0,
    )

    assert payload["lengthRange"] == {"min": 4700.0, "max": 4900.0}
    assert sum(item["sales"] for item in payload["priceBands"]["items"]) == pytest.approx(120.0)
    assert [item["model"] for item in payload["bubbleChart"]["items"]] == ["Long"]


def test_build_positioning_current_price_candidates_keeps_jato_and_official_keys() -> None:
    price_frame = pd.DataFrame(
        {
            "brand": ["VOLVO"],
            "jato_model": ["XC60"],
            "jato_trim": ["Ultra AWD"],
            "jato_powertrain": ["PHEV"],
            "official_model": ["XC60 Recharge"],
            "official_trim": ["Ultra"],
            "official_edition": ["AWD"],
            "official_powertrain": ["PHEV"],
            "current_msrp_value": [52000.0],
            "currency": ["EUR"],
            "source_url": ["https://example.com/xc60"],
            "match_confidence": [0.93],
            "updated_at_utc": ["2026-04-17T00:00:00+00:00"],
        }
    )

    candidates = market_scan_service._build_positioning_current_price_candidates(
        price_frame,
    )

    assert set(candidates["match_variant"].tolist()) == {"jato", "official"}
    assert set(candidates["model_norm"].tolist()) == {"xc60", "xc60recharge"}
    assert "ultraawd" in set(candidates["trim_norm"].tolist())


def test_build_positioning_jato_link_candidates_keeps_jato_and_official_keys() -> None:
    link_frame = pd.DataFrame(
        {
            "country": ["Germany"],
            "brand": ["VOLVO"],
            "jato_model": ["XC60"],
            "jato_trim": ["Plus AWD"],
            "jato_powertrain": ["PHEV"],
            "official_model": ["XC60 Recharge"],
            "official_trim": ["Plus"],
            "official_edition": ["AWD"],
            "official_powertrain": ["PHEV"],
            "confidence": [95],
            "link_source": ["manual"],
        }
    )

    candidates = market_scan_service._build_positioning_jato_link_candidates(
        link_frame,
    )

    row = candidates.iloc[0]
    assert row["brand_norm"] == "volvo"
    assert row["jato_model_norm"] == "xc60"
    assert row["jato_trim_norm"] == "plusawd"
    assert row["official_model_norm"] == "xc60recharge"
    assert row["official_trim_norm"] == "plusawd"
    assert row["official_powertrain_norm"] == "phev"


def test_apply_positioning_current_price_overlay_prefers_trim_match() -> None:
    if market_scan_service.duckdb is None:
        pytest.skip("duckdb not installed")

    frame = pd.DataFrame(
        {
            "__brand": ["VOLVO", "VOLVO"],
            "__model": ["XC60", "XC60"],
            "__trim": ["Ultra AWD", "Core"],
            "__powertrain": ["PHEV", "PHEV"],
            "__segment_raw": ["SUV C", "SUV C"],
            "__length": [4708.0, 4708.0],
            "__msrp": [50000.0, 47000.0],
            "2026 Apr": [120.0, 90.0],
        }
    )
    price_frame = pd.DataFrame(
        {
            "brand": ["VOLVO", "VOLVO"],
            "jato_model": ["XC60", "XC60"],
            "jato_trim": ["Ultra AWD", ""],
            "jato_powertrain": ["PHEV", "PHEV"],
            "official_model": ["XC60", "XC60"],
            "official_trim": ["Ultra", ""],
            "official_edition": ["AWD", ""],
            "official_powertrain": ["PHEV", "PHEV"],
            "current_msrp_value": [52000.0, 48000.0],
            "currency": ["EUR", "EUR"],
            "source_url": ["https://example.com/xc60-ultra", "https://example.com/xc60"],
            "match_confidence": [0.95, 0.7],
            "source_tier": [1, 3],
            "source_code": ["de_volvo_official", "de_volvo_catalog"],
            "source_type": ["official_site", "reference_catalog"],
            "updated_at_utc": [
                "2026-04-17T00:00:00+00:00",
                "2026-04-16T00:00:00+00:00",
            ],
        }
    )

    candidates = market_scan_service._build_positioning_current_price_candidates(
        price_frame,
    )
    overlayed, meta = market_scan_service._apply_positioning_current_price_overlay(
        frame,
        candidates,
    )

    ultra = overlayed.loc[overlayed["__trim"] == "Ultra AWD"].iloc[0]
    core = overlayed.loc[overlayed["__trim"] == "Core"].iloc[0]

    assert meta["mode"] == "duckdb-overlay"
    assert meta["matchedRows"] == 2
    assert meta["directMatches"] == 2
    assert ultra["__msrp"] == pytest.approx(52000.0)
    assert ultra["__msrp_source"] == "current_prices"
    assert ultra["__msrp_source_tier"] == 1
    assert core["__msrp"] == pytest.approx(48000.0)
    assert core["__msrp_source"] == "current_prices"


def test_apply_positioning_current_price_overlay_prefers_explicit_link() -> None:
    if market_scan_service.duckdb is None:
        pytest.skip("duckdb not installed")

    frame = pd.DataFrame(
        {
            "__brand": ["VOLVO"],
            "__model": ["XC60"],
            "__trim": ["Plus AWD"],
            "__powertrain": ["PHEV"],
            "__segment_raw": ["SUV C"],
            "__length": [4708.0],
            "__msrp": [50000.0],
            "2026 Apr": [120.0],
        }
    )
    price_frame = pd.DataFrame(
        {
            "brand": ["VOLVO", "VOLVO"],
            "jato_model": ["XC60", "XC60"],
            "jato_trim": ["", "Plus AWD"],
            "jato_powertrain": ["PHEV", "PHEV"],
            "official_model": ["XC60 Recharge", "XC60"],
            "official_trim": ["Plus", "Core"],
            "official_edition": ["AWD", ""],
            "official_powertrain": ["PHEV", "PHEV"],
            "current_msrp_value": [51000.0, 52000.0],
            "currency": ["EUR", "EUR"],
            "source_url": ["https://example.com/xc60-plus", "https://example.com/xc60-core"],
            "match_confidence": [0.88, 0.92],
            "source_tier": [1, 4],
            "source_code": ["de_volvo_official", "de_volvo_media"],
            "source_type": ["official_site", "automotive_media"],
            "updated_at_utc": [
                "2026-04-17T00:00:00+00:00",
                "2026-04-16T00:00:00+00:00",
            ],
        }
    )
    link_frame = pd.DataFrame(
        {
            "country": ["Germany"],
            "brand": ["VOLVO"],
            "jato_model": ["XC60"],
            "jato_trim": ["Plus AWD"],
            "jato_powertrain": ["PHEV"],
            "official_model": ["XC60 Recharge"],
            "official_trim": ["Plus"],
            "official_edition": ["AWD"],
            "official_powertrain": ["PHEV"],
            "confidence": [95],
            "link_source": ["manual"],
        }
    )

    current_price_candidates = market_scan_service._build_positioning_current_price_candidates(
        price_frame,
    )
    link_candidates = market_scan_service._build_positioning_jato_link_candidates(
        link_frame,
    )
    overlayed, meta = market_scan_service._apply_positioning_current_price_overlay(
        frame,
        current_price_candidates,
        link_candidates,
    )

    row = overlayed.iloc[0]
    assert row["__msrp"] == pytest.approx(51000.0)
    assert row["__msrp_overlay_strategy"] == "link"
    assert row["__msrp_source_tier"] == 1
    assert row["__msrp_source_code"] == "de_volvo_official"
    assert row["__msrp_link_source"] == "manual"
    assert meta["linkMatches"] == 1
    assert meta["directMatches"] == 0


def test_build_version_comparison_bubble_items_groups_model_versions() -> None:
    frame = pd.DataFrame(
        {
            "__model": ["XC60", "XC60", "GLC"],
            "__version": ["Plus", "Plus", "AMG"],
            "__trim": ["Plus AWD", "Plus AWD", "AMG Line"],
            "__powertrain": ["PHEV", "PHEV", "ICE"],
            "__length": [4708.0, 4708.0, 4720.0],
            "__msrp": [56000.0, 58000.0, 62000.0],
            "__comparison_sales": [120.0, 80.0, 90.0],
        }
    )

    items = market_scan_service._build_version_comparison_bubble_items(
        frame,
        sales_column="__comparison_sales",
        msrp_min=54000.0,
        msrp_max=60000.0,
    )

    assert len(items) == 1
    assert items[0]["model"] == "XC60"
    assert items[0]["version"] == "Plus"
    assert items[0]["sales"] == pytest.approx(200.0)
    assert items[0]["msrp"] == pytest.approx(57000.0)


# ── YoY drill-down fixes ──────────────────────────────────────────────


def test_prior_ytd_columns_not_gated_when_same_month_last_year_missing() -> None:
    """Case A: even when 2025-05 is missing, 2025-01..2025-04 should still contribute to prior YTD."""
    available = [
        "2025-01", "2025-02", "2025-03", "2025-04",
        "2026-01", "2026-02", "2026-03", "2026-04", "2026-05",
    ]
    mappings = market_scan_service._build_period_column_mappings(
        available_periods=available,
        resolved_period="2026-05",
        prior_period=None,
        same_month_last_year_period=None,  # 2025-05 missing
        custom_range_periods=None,
    )
    # Prior YTD should still include the 4 months that exist for 2025
    assert len(mappings["prior_ytd_columns"]) == 4
    assert "2025 Jan" in mappings["prior_ytd_columns"]
    assert "2025 Apr" in mappings["prior_ytd_columns"]
    # Same-month column legitimately empty
    assert mappings["same_month_columns"] == []


def test_prior_rolling12_partial_when_single_month_missing() -> None:
    """Case B: if one month in the prior rolling12 window is missing, prior should not be empty."""
    months = [f"{y}-{m:02d}" for y in (2025, 2026) for m in range(1, 13)]
    # Remove 2025-05 to simulate the exact-same-month-last-year being absent
    available = [p for p in months if p != "2025-05" and p <= "2026-05"]

    mappings = market_scan_service._build_period_column_mappings(
        available_periods=available,
        resolved_period="2026-05",
        prior_period=None,
        same_month_last_year_period=None,
        custom_range_periods=None,
    )
    # Current rolling12 should have all 12 months
    assert len(mappings["current_rolling12_columns"]) == 12
    # Prior rolling12: each current month shifted -12; 2025-05 is missing so 11 of 12
    assert len(mappings["prior_rolling12_columns"]) > 0
    assert len(mappings["prior_rolling12_columns"]) < 12


def test_fuel_panel_prior_not_lost_when_current_zero_row() -> None:
    """Case C: a model/fuel had prior volume on a row whose current=0.

    Before the fix the current>0 filter dropped that row and its prior, producing
    a fake "New" YoY.  After the fix prior is aggregated from the unfiltered frame.
    """
    frame = pd.DataFrame(
        {
            "__segment_raw": ["SUV A0", "SUV A0"],
            "__model": ["EX30", "EX30"],
            "__powertrain": ["BEV", "BEV"],
            "__drive_type": ["4WD", "4WD"],
            "__registration_type": ["Private", "Private"],
            # Row 0: prior-only row — current=0 so it gets dropped by current>0 filter
            "2025 Jan": [100.0, 0.0],
            "2026 Jan": [0.0, 200.0],
        }
    )

    rankings = market_scan_service._precompute_single_fuel_panel_rankings(
        frame,
        windows={"month": (["2026 Jan"], ["2025 Jan"])},
        ranking_limit=10,
    )
    items = rankings.get(("SUV A0", "BEV", "month"), [])
    ex30 = next((it for it in items if it["model"] == "EX30"), None)
    assert ex30 is not None
    assert ex30["volume"] == pytest.approx(200.0)
    # Prior must not be zero — row 0 contributed 100 prior even though its current was 0
    assert ex30["yoy"]["display"] != "New"
    assert ex30["yoy"]["display"] == "100.0%"


def test_needed_month_columns_includes_prior_months_when_same_month_missing() -> None:
    """Prior YTD and prior rolling12 periods must contribute to the parquet read set
    even when same_month_last_year_period is None — and NOT be masked by trend windows.

    Uses _compute_needed_periods() to assert on individual period sources, then
    confirms _compute_needed_month_columns() flattens them into column names.
    """
    available = [
        "2025-01", "2025-02", "2025-03", "2025-04",
        "2026-01", "2026-02", "2026-03", "2026-04", "2026-05",
    ]
    periods = market_scan_service._compute_needed_periods(
        available_periods=available,
        resolved_period="2026-05",
        trend_window_months=1,       # keep trend small so it can't mask prior gaps
        origin_window_months=1,
        body_window_months=1,
        same_month_last_year_period=None,  # 2025-05 missing
        prior_period=None,
        custom_periods=None,
    )
    # ── Source-level assertions (not confounded by trend window) ──
    # prior_ytd must be non-empty even though same_month_last_year_period is None
    assert periods["prior_ytd"] == ["2025-01", "2025-02", "2025-03", "2025-04"]
    # prior_r12: current r12 for 2026-05 includes 2026-01..2026-05 and
    # 2025-01..2025-04 (9 months available).  Shift -12: 2025-01..2025-04 survive,
    # 2025-05 is absent → 4 prior months.
    assert len(periods["prior_r12"]) == 4
    assert "2025-05" not in periods["prior_r12"]
    # same_month is legitimately empty
    assert periods["same_month"] == []
    # current_ytd should have 5 months
    assert len(periods["current_ytd"]) == 5
    assert "rolling12_trend" in periods

    # ── Flattened column output ──
    columns = market_scan_service._compute_needed_month_columns(
        available_periods=available,
        resolved_period="2026-05",
        trend_window_months=1,
        origin_window_months=1,
        body_window_months=1,
        same_month_last_year_period=None,
        prior_period=None,
        custom_periods=None,
    )
    for month_col in ("2025 Jan", "2025 Feb", "2025 Mar", "2025 Apr"):
        assert month_col in columns, f"{month_col} missing from needed_month_columns"
    for month_col in ("2026 Jan", "2026 Feb", "2026 Mar", "2026 Apr", "2026 May"):
        assert month_col in columns, f"{month_col} missing from needed_month_columns"
    assert "2025 May" not in columns


def test_rolling12_trend_periods_cover_all_three_windows() -> None:
    """L12M bars need 3 full 12-month windows: target, target-12m, target-24m.

    Without explicit inclusion, trend_window_months=24 omits months older than
    24 months from resolved_period, starving the earliest L12M bar.
    """
    # 39 months of history: 2023-01 through 2026-03
    available = [f"{y}-{m:02d}" for y in range(2023, 2027) for m in range(1, 13) if (y, m) <= (2026, 3)]

    periods = market_scan_service._compute_needed_periods(
        available_periods=available,
        resolved_period="2026-03",
        trend_window_months=24,
        origin_window_months=24,
        body_window_months=24,
        same_month_last_year_period="2025-03",
        prior_period="2026-02",
        custom_periods=None,
    )

    rt = periods["rolling12_trend"]
    # L12M 24.03 needs 2023-04 through 2024-03 — all 12 must be present
    for m in range(4, 13):
        assert f"2023-{m:02d}" in rt, f"2023-{m:02d} missing from rolling12_trend"
    for m in range(1, 4):
        assert f"2024-{m:02d}" in rt, f"2024-{m:02d} missing from rolling12_trend"
    # L12M 25.03 needs 2024-04 onward
    for m in range(4, 13):
        assert f"2024-{m:02d}" in rt, f"2024-{m:02d} missing from rolling12_trend"
    # L12M 26.03 needs 2025-04 onward
    for m in range(4, 13):
        assert f"2025-{m:02d}" in rt, f"2025-{m:02d} missing from rolling12_trend"

    # 2023-01 through 2023-03 should NOT be included (outside the 3 windows)
    assert "2023-01" not in rt
    assert "2023-03" not in rt

    # Verify the columns pass through to _compute_needed_month_columns
    columns = market_scan_service._compute_needed_month_columns(
        available_periods=available,
        resolved_period="2026-03",
        trend_window_months=24,
        origin_window_months=24,
        body_window_months=24,
        same_month_last_year_period="2025-03",
        prior_period="2026-02",
        custom_periods=None,
    )
    assert "2023 Apr" in columns
    assert "2024 Mar" in columns


def test_rolling12_fuel_trend_includes_month_count() -> None:
    """Each L12M bar must report monthCount and coverageRatio for data quality."""
    from app.services.market_scan_service import NUMBER_TO_MONTH_NAME
    month_names = [NUMBER_TO_MONTH_NAME[i] for i in range(1, 13)]  # Jan..Dec
    frame = pd.DataFrame(
        {
            "__segment_raw": ["SUV A"] * 24,
            "__powertrain": (["BEV"] * 12) + (["PHEV"] * 12),
            **{f"2025 {m}": [10.0] * 24 for m in month_names},
            **{f"2026 {m}": [10.0] * 24 for m in month_names[:3]},
        },
    )

    available = ["2025-01", "2025-02", "2025-03", "2025-04", "2025-05", "2025-06",
                  "2025-07", "2025-08", "2025-09", "2025-10", "2025-11", "2025-12",
                  "2026-01", "2026-02", "2026-03"]

    result = market_scan_service._build_rolling12_fuel_trend(
        frame=frame,
        fuel_order=["BEV", "PHEV"],
        resolved_period="2026-03",
        available_periods=available,
    )
    items = result["items"]
    assert len(items) == 3
    labels = [it["label"] for it in items]
    assert labels == ["L12M 24.03", "L12M 25.03", "L12M 26.03"]

    # L12M 24.03: 2024-04 through 2025-03 — but no 2024 columns in frame
    assert items[0]["monthCount"] < 12
    assert items[0]["coverageRatio"] < 1.0

    # L12M 26.03: 2025-04 through 2026-03 — all 12 should be present (or less if data truncates)
    assert items[2]["monthCount"] > 0

    # Every item must have monthCount and coverageRatio
    for it in items:
        assert "monthCount" in it
        assert "coverageRatio" in it
        assert 0 <= it["coverageRatio"] <= 1.0
