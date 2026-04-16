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
