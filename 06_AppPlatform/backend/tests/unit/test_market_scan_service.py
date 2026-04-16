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
