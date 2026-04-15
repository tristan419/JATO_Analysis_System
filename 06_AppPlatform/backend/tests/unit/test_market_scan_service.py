import pandas as pd
import pytest

from app.services import market_scan_service


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