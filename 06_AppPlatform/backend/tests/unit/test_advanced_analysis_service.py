import pandas as pd
import pytest

from app.services import advanced_analysis_service


@pytest.fixture(autouse=True)
def clear_advanced_analysis_cache() -> None:
    advanced_analysis_service.clear_advanced_analysis_cache()


def test_transfer_mart_repeated_scope_filters_are_or_filters(monkeypatch: pytest.MonkeyPatch) -> None:
    fact = pd.DataFrame(
        [
            {
                "country": "瑞典",
                "segment": "SUV-A0",
                "make": "BrandA",
                "model": "Model A",
                "powertrain": "BEV",
                "drive_type": "2WD",
                "registration_type": "Business",
                "origin": "欧系",
                "period": "2025-01",
                "sales": 80.0,
            },
            {
                "country": "瑞典",
                "segment": "SUV-A0",
                "make": "BrandB",
                "model": "Model B",
                "powertrain": "PHEV",
                "drive_type": "4WD",
                "registration_type": "Private",
                "origin": "欧系",
                "period": "2025-01",
                "sales": 40.0,
            },
            {
                "country": "瑞典",
                "segment": "SUV-A0",
                "make": "BrandC",
                "model": "Model C",
                "powertrain": "ICE",
                "drive_type": "2WD",
                "registration_type": "Business",
                "origin": "日系",
                "period": "2025-01",
                "sales": 500.0,
            },
            {
                "country": "瑞典",
                "segment": "SUV-A0",
                "make": "BrandA",
                "model": "Model A",
                "powertrain": "BEV",
                "drive_type": "2WD",
                "registration_type": "Business",
                "origin": "欧系",
                "period": "2026-01",
                "sales": 80.0,
            },
            {
                "country": "瑞典",
                "segment": "SUV-A0",
                "make": "BrandB",
                "model": "Model B",
                "powertrain": "PHEV",
                "drive_type": "4WD",
                "registration_type": "Private",
                "origin": "欧系",
                "period": "2026-01",
                "sales": 70.0,
            },
            {
                "country": "瑞典",
                "segment": "SUV-A0",
                "make": "BrandC",
                "model": "Model C",
                "powertrain": "ICE",
                "drive_type": "2WD",
                "registration_type": "Business",
                "origin": "日系",
                "period": "2026-01",
                "sales": 800.0,
            },
        ]
    )

    monkeypatch.setattr(advanced_analysis_service, "build_fact_sales_monthly", lambda **_: fact.copy())

    result = advanced_analysis_service.compute_transfer_mart(
        country="瑞典",
        target_period="2026-01",
        base_period="2025-01",
        scope_filters=[
            {"dim": "powertrain", "value": "BEV"},
            {"dim": "powertrain", "value": "PHEV"},
        ],
        top_n=10,
    )

    assert result["scope_summary"]["total_sales_base"] == pytest.approx(120.0)
    assert result["scope_summary"]["total_sales_tgt"] == pytest.approx(150.0)
    assert {row["model"] for row in result["models"]} == {"Model A", "Model B"}
    assert result["winners"][0]["model"] == "Model B"
    assert result["losers"][0]["model"] == "Model A"


def test_competitor_set_returns_product_battlefield_and_channel_series(monkeypatch: pytest.MonkeyPatch) -> None:
    fact = pd.DataFrame(
        [
            ("瑞典", "SUV-A0", "BrandA", "Target", "BEV", "SUV", "2WD", "Business", "欧系", "2025-01", 50.0),
            ("瑞典", "SUV-A0", "BrandA", "Target", "BEV", "SUV", "2WD", "Private", "欧系", "2025-01", 50.0),
            ("瑞典", "SUV-A0", "BrandB", "Donor", "BEV", "SUV", "2WD", "Business", "欧系", "2025-01", 100.0),
            ("瑞典", "SUV-A0", "BrandB", "Donor", "BEV", "SUV", "2WD", "Private", "欧系", "2025-01", 100.0),
            ("瑞典", "SUV-A0", "BrandA", "Target", "BEV", "SUV", "2WD", "Business", "欧系", "2026-01", 90.0),
            ("瑞典", "SUV-A0", "BrandA", "Target", "BEV", "SUV", "2WD", "Private", "欧系", "2026-01", 80.0),
            ("瑞典", "SUV-A0", "BrandB", "Donor", "BEV", "SUV", "2WD", "Business", "欧系", "2026-01", 60.0),
            ("瑞典", "SUV-A0", "BrandB", "Donor", "BEV", "SUV", "2WD", "Private", "欧系", "2026-01", 70.0),
        ],
        columns=[
            "country",
            "segment",
            "make",
            "model",
            "powertrain",
            "body_type",
            "drive_type",
            "registration_type",
            "origin",
            "period",
            "sales",
        ],
    )

    monkeypatch.setattr(advanced_analysis_service, "build_fact_sales_monthly", lambda **_: fact.copy())

    result = advanced_analysis_service.compute_competitor_set(
        country="瑞典",
        target_period="2026-01",
        base_period="2025-01",
        target_model="Target",
        top_n=5,
    )

    assert result["target_model"] == "Target"
    assert result["competitors"][0]["model"] == "Donor"
    assert result["competitors"][0]["role"] == "likely_source"
    assert result["battle_flows"][0]["source"] == "Donor"
    assert result["battle_flows"][0]["target"] == "Target"
    assert result["model_channel_timeseries"]
    assert {row["channel"] for row in result["model_channel_timeseries"] if row["model"] == "Target"} == {"Business", "Private"}


def test_competitor_set_supports_missing_target_profile_filters(monkeypatch: pytest.MonkeyPatch) -> None:
    fact = pd.DataFrame(
        [
            ("瑞典", "SUV-A0", "BrandA", "A", "BEV", "SUV", "2WD", "Business", "欧系", "2025-01", 40.0),
            ("瑞典", "SUV-A", "BrandB", "B", "BEV", "SUV", "4WD", "Private", "欧系", "2025-01", 80.0),
            ("瑞典", "SUV-B", "BrandC", "C", "ICE", "SUV", "2WD", "Business", "日系", "2025-01", 500.0),
            ("瑞典", "SUV-A0", "BrandA", "A", "BEV", "SUV", "2WD", "Business", "欧系", "2026-01", 70.0),
            ("瑞典", "SUV-A", "BrandB", "B", "BEV", "SUV", "4WD", "Private", "欧系", "2026-01", 70.0),
            ("瑞典", "SUV-B", "BrandC", "C", "ICE", "SUV", "2WD", "Business", "日系", "2026-01", 600.0),
        ],
        columns=[
            "country",
            "segment",
            "make",
            "model",
            "powertrain",
            "body_type",
            "drive_type",
            "registration_type",
            "origin",
            "period",
            "sales",
        ],
    )

    monkeypatch.setattr(advanced_analysis_service, "build_fact_sales_monthly", lambda **_: fact.copy())

    result = advanced_analysis_service.compute_competitor_set(
        country="瑞典",
        target_period="2026-01",
        base_period="2025-01",
        scope_filters=[
            {"dim": "segment", "value": "SUV-A0"},
            {"dim": "segment", "value": "SUV-A"},
            {"dim": "powertrain", "value": "BEV"},
        ],
        target_model=None,
        top_n=5,
    )

    assert result["analysis_mode"] == "profile"
    assert result["target_model"].startswith("Profile:")
    assert {row["model"] for row in result["competitors"]} == {"A", "B"}
    assert {row["model"] for row in result["model_channel_timeseries"]} >= {result["target_model"]}
    assert all(row["model"] != "C" for row in result["competitors"])


def test_competitor_set_uses_product_specs_as_match_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    fact = pd.DataFrame(
        [
            ("瑞典", "SUV-A0", "BrandA", "Close", "BEV", "SUV", "2WD", "Private", "欧系", 4430.0, 36000.0, 430.0, 60.0, "2025-01", 100.0),
            ("瑞典", "SUV-A0", "BrandB", "Far", "BEV", "SUV", "2WD", "Private", "欧系", 4900.0, 52000.0, 250.0, 80.0, "2025-01", 100.0),
            ("瑞典", "SUV-A0", "BrandA", "Close", "BEV", "SUV", "2WD", "Private", "欧系", 4430.0, 36000.0, 430.0, 60.0, "2026-01", 150.0),
            ("瑞典", "SUV-A0", "BrandB", "Far", "BEV", "SUV", "2WD", "Private", "欧系", 4900.0, 52000.0, 250.0, 80.0, "2026-01", 110.0),
        ],
        columns=[
            "country",
            "segment",
            "make",
            "model",
            "powertrain",
            "body_type",
            "drive_type",
            "registration_type",
            "origin",
            "length_mm",
            "msrp",
            "ev_range",
            "battery_kwh",
            "period",
            "sales",
        ],
    )

    monkeypatch.setattr(advanced_analysis_service, "build_fact_sales_monthly", lambda **_: fact.copy())

    result = advanced_analysis_service.compute_competitor_set(
        country="瑞典",
        target_period="2026-01",
        base_period="2025-01",
        scope_filters=[
            {"dim": "segment", "value": "SUV-A0"},
            {"dim": "powertrain", "value": "BEV"},
        ],
        profile_specs={"length_mm": 4420.0, "msrp": 36500.0, "ev_range": 420.0, "battery_kwh": 61.0},
        target_model=None,
        top_n=5,
    )

    assert result["analysis_mode"] == "profile"
    assert result["competitors"][0]["model"] == "Close"
    close = next(row for row in result["competitors"] if row["model"] == "Close")
    far = next(row for row in result["competitors"] if row["model"] == "Far")
    assert close["similarity_score"] > far["similarity_score"]
    assert {"length_mm", "msrp", "ev_range"} <= {item["field"] for item in close["match_evidence"]}
