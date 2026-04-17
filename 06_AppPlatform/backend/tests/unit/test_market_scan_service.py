import pandas as pd
import pytest

from app.services import market_scan_service
from app.api.schemas import MarketScanDeckRequest
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


def test_overview_payload_includes_ytd_and_monthly_model_breakdown() -> None:
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
    assert payload["ytdBrandRanking"]["items"][0]["modelBreakdown"]


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
        fuel_types: list[str],
        trend_window_months: int,
        origin_window_months: int,
        body_window_months: int,
        ranking_limit: int,
        drilldown_segment: str | None,
    ) -> dict[str, object]:
        observed["ranking_limit"] = ranking_limit
        return {"ok": True}

    market_scan_service._deck_cache.clear()
    monkeypatch.setattr(market_scan_service.repo, "current_dataset_token", lambda: "test-token")
    monkeypatch.setattr(market_scan_service, "_query_market_scan_deck_impl", _stub_query_market_scan_deck_impl)

    result = market_scan_service.query_market_scan_deck(
        country="SE",
        target_period="2025-02",
        fuel_types=["BEV"],
        trend_window_months=24,
        origin_window_months=24,
        body_window_months=24,
        ranking_limit=6,
        drilldown_segment="SUV A0",
    )

    assert result == {"ok": True}
    assert observed["ranking_limit"] == 10


def test_normalize_positioning_sales_mode_defaults_to_month() -> None:
    assert market_scan_service._normalize_positioning_sales_mode(None) == "month"
    assert market_scan_service._normalize_positioning_sales_mode("bad-mode") == "month"
    assert market_scan_service._normalize_positioning_sales_mode("rolling12") == "rolling12"


def test_resolve_positioning_sales_window_supports_month_and_rolling12() -> None:
    available_periods = [
        "2025-04", "2025-05", "2025-06", "2025-07", "2025-08", "2025-09",
        "2025-10", "2025-11", "2025-12", "2026-01", "2026-02", "2026-03",
    ]

    month_periods, month_label, month_metric = market_scan_service._resolve_positioning_sales_window(
        available_periods,
        "2026-03",
        "month",
    )
    rolling_periods, rolling_label, rolling_metric = market_scan_service._resolve_positioning_sales_window(
        available_periods,
        "2026-03",
        "rolling12",
    )

    assert month_periods == ["2026-03"]
    assert month_label == "当月"
    assert month_metric == "Current Month Sales"
    assert rolling_periods == available_periods
    assert rolling_label == "近12个月"
    assert rolling_metric == "Rolling 12M Sales"


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
