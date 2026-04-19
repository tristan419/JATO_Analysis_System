from uuid import uuid4

from app.db.models import (
    ConfigBaseVariant,
    ConfigMarketFeatureOverride,
    ConfigMarketVariant,
    ConfigProject,
)
from app.services import engineering_variant_diff_service


def test_compare_market_variants_matches_explicit_versions(monkeypatch) -> None:
    project_id = uuid4()
    ultra_base_id = uuid4()
    core_base_id = uuid4()
    ultra_market_id = uuid4()
    core_market_id = uuid4()
    project = ConfigProject(
        project_id=project_id,
        project_code="VOLVO-XC60-SE",
        brand="Volvo",
        model="XC60",
        market_country="Sweden",
        display_name="Volvo XC60 Sweden",
        status="active",
    )
    ultra = ConfigBaseVariant(
        base_variant_id=ultra_base_id,
        project_id=project_id,
        business_key="volvo|xc60|ultra|2026 my|phev",
        brand="Volvo",
        model="XC60",
        trim_name="Ultra",
        version_name="2026 MY",
        powertrain="PHEV",
        base_features_json={
            "body_style": "SUV",
            "drive_type": "AWD",
            "battery_kwh": 18.8,
            "range_km": 82.0,
        },
        base_feature_labels_json={
            "body_style": "Body Style",
            "drive_type": "Drive Type",
            "battery_kwh": "Battery kWh",
            "range_km": "Range Km",
        },
        source_variant_count=1,
        market_count=1,
    )
    core = ConfigBaseVariant(
        base_variant_id=core_base_id,
        project_id=project_id,
        business_key="volvo|xc60|core|2026 my|phev",
        brand="Volvo",
        model="XC60",
        trim_name="Core",
        version_name="2026 MY",
        powertrain="PHEV",
        base_features_json={
            "body_style": "SUV",
            "drive_type": "AWD",
            "battery_kwh": 16.0,
            "range_km": 72.0,
        },
        base_feature_labels_json={
            "body_style": "Body Style",
            "drive_type": "Drive Type",
            "battery_kwh": "Battery kWh",
            "range_km": "Range Km",
        },
        source_variant_count=1,
        market_count=1,
    )
    ultra_market = ConfigMarketVariant(
        market_variant_id=ultra_market_id,
        project_id=project_id,
        base_variant_id=ultra_base_id,
        source_variant_id=uuid4(),
        external_row_key="SE-ULTRA",
        market_country="Sweden",
        target_msrp=55900.0,
        source_file_path="imports/xc60.xlsx",
        override_count=1,
    )
    core_market = ConfigMarketVariant(
        market_variant_id=core_market_id,
        project_id=project_id,
        base_variant_id=core_base_id,
        source_variant_id=uuid4(),
        external_row_key="SE-CORE",
        market_country="Sweden",
        target_msrp=51900.0,
        source_file_path="imports/xc60.xlsx",
        override_count=0,
    )
    ultra_override = ConfigMarketFeatureOverride(
        feature_override_id=uuid4(),
        project_id=project_id,
        market_variant_id=ultra_market_id,
        source_variant_id=ultra_market.source_variant_id,
        feature_code="massage_seats_pack",
        feature_label="Massage Seats Pack",
        value_type="text",
        bool_value=None,
        number_value=None,
        text_value="package",
        json_value=None,
        availability="package",
        package_code="package",
    )

    monkeypatch.setattr(
        engineering_variant_diff_service.repo,
        "list_projects",
        lambda *args, **kwargs: [project],
    )
    monkeypatch.setattr(
        engineering_variant_diff_service.repo,
        "list_base_variants",
        lambda *args, **kwargs: [ultra, core],
    )
    monkeypatch.setattr(
        engineering_variant_diff_service.repo,
        "list_market_variants",
        lambda *args, **kwargs: [ultra_market, core_market],
    )
    monkeypatch.setattr(
        engineering_variant_diff_service.repo,
        "list_market_feature_overrides",
        lambda *args, **kwargs: [ultra_override],
    )

    result = engineering_variant_diff_service.compare_market_variants(
        object(),  # type: ignore[arg-type]
        country="瑞典",
        compare_subjects=[
            {"model": "XC60", "variantQuery": "Ultra"},
            {"model": "XC60", "variantQuery": "Core"},
        ],
        powertrain="PHEV",
    )

    assert [item["trim"] for item in result["subjects"]] == ["Ultra", "Core"]
    assert result["selectionNotes"] == []
    assert any(
        item["featureLabel"] == "Battery kWh" for item in result["differentFeatures"]
    )
    assert any(
        item["featureLabel"] == "Massage Seats Pack"
        for item in result["differentFeatures"]
    )
    assert any(
        item["featureLabel"] == "Drive Type" for item in result["commonFeatures"]
    )


def test_compare_market_variants_defaults_to_entry_variant_per_model(monkeypatch) -> None:
    rav4_project_id = uuid4()
    sportage_project_id = uuid4()
    rav4_entry_base_id = uuid4()
    rav4_high_base_id = uuid4()
    sportage_entry_base_id = uuid4()
    sportage_high_base_id = uuid4()
    rav4_project = ConfigProject(
        project_id=rav4_project_id,
        project_code="TOYOTA-RAV4-SE",
        brand="Toyota",
        model="RAV4",
        market_country="Sweden",
        display_name="Toyota RAV4 Sweden",
        status="active",
    )
    sportage_project = ConfigProject(
        project_id=sportage_project_id,
        project_code="KIA-SPORTAGE-SE",
        brand="Kia",
        model="SPORTAGE",
        market_country="Sweden",
        display_name="Kia Sportage Sweden",
        status="active",
    )
    base_variants = {
        rav4_project_id: [
            ConfigBaseVariant(
                base_variant_id=rav4_entry_base_id,
                project_id=rav4_project_id,
                business_key="toyota|rav4|active|2026 my|hev",
                brand="Toyota",
                model="RAV4",
                trim_name="Active",
                version_name="2026 MY",
                powertrain="HEV",
                base_features_json={"range_km": 780.0},
                base_feature_labels_json={"range_km": "Range Km"},
                source_variant_count=1,
                market_count=1,
            ),
            ConfigBaseVariant(
                base_variant_id=rav4_high_base_id,
                project_id=rav4_project_id,
                business_key="toyota|rav4|lounge|2026 my|hev",
                brand="Toyota",
                model="RAV4",
                trim_name="Lounge",
                version_name="2026 MY",
                powertrain="HEV",
                base_features_json={"range_km": 780.0},
                base_feature_labels_json={"range_km": "Range Km"},
                source_variant_count=1,
                market_count=1,
            ),
        ],
        sportage_project_id: [
            ConfigBaseVariant(
                base_variant_id=sportage_entry_base_id,
                project_id=sportage_project_id,
                business_key="kia|sportage|action|2026 my|hev",
                brand="Kia",
                model="SPORTAGE",
                trim_name="Action",
                version_name="2026 MY",
                powertrain="HEV",
                base_features_json={"range_km": 730.0},
                base_feature_labels_json={"range_km": "Range Km"},
                source_variant_count=1,
                market_count=1,
            ),
            ConfigBaseVariant(
                base_variant_id=sportage_high_base_id,
                project_id=sportage_project_id,
                business_key="kia|sportage|gt-line|2026 my|hev",
                brand="Kia",
                model="SPORTAGE",
                trim_name="GT-Line",
                version_name="2026 MY",
                powertrain="HEV",
                base_features_json={"range_km": 730.0},
                base_feature_labels_json={"range_km": "Range Km"},
                source_variant_count=1,
                market_count=1,
            ),
        ],
    }
    market_variants = {
        rav4_project_id: [
            ConfigMarketVariant(
                market_variant_id=uuid4(),
                project_id=rav4_project_id,
                base_variant_id=rav4_entry_base_id,
                source_variant_id=uuid4(),
                external_row_key="SE-ACTIVE",
                market_country="Sweden",
                target_msrp=414900.0,
                source_file_path="imports/rav4.xlsx",
                override_count=0,
            ),
            ConfigMarketVariant(
                market_variant_id=uuid4(),
                project_id=rav4_project_id,
                base_variant_id=rav4_high_base_id,
                source_variant_id=uuid4(),
                external_row_key="SE-LOUNGE",
                market_country="Sweden",
                target_msrp=474900.0,
                source_file_path="imports/rav4.xlsx",
                override_count=0,
            ),
        ],
        sportage_project_id: [
            ConfigMarketVariant(
                market_variant_id=uuid4(),
                project_id=sportage_project_id,
                base_variant_id=sportage_entry_base_id,
                source_variant_id=uuid4(),
                external_row_key="SE-ACTION",
                market_country="Sweden",
                target_msrp=399900.0,
                source_file_path="imports/sportage.xlsx",
                override_count=0,
            ),
            ConfigMarketVariant(
                market_variant_id=uuid4(),
                project_id=sportage_project_id,
                base_variant_id=sportage_high_base_id,
                source_variant_id=uuid4(),
                external_row_key="SE-GTLINE",
                market_country="Sweden",
                target_msrp=459900.0,
                source_file_path="imports/sportage.xlsx",
                override_count=0,
            ),
        ],
    }

    def _list_projects(_session, _status, _brand, market_country, _limit):
        if market_country != "Sweden":
            return []
        return [rav4_project, sportage_project]

    monkeypatch.setattr(
        engineering_variant_diff_service.repo,
        "list_projects",
        _list_projects,
    )
    monkeypatch.setattr(
        engineering_variant_diff_service.repo,
        "list_base_variants",
        lambda _session, project_id, _model, _limit: base_variants[project_id],
    )
    monkeypatch.setattr(
        engineering_variant_diff_service.repo,
        "list_market_variants",
        lambda _session, project_id, _base_variant_id, _market_country, _limit: market_variants[project_id],
    )
    monkeypatch.setattr(
        engineering_variant_diff_service.repo,
        "list_market_feature_overrides",
        lambda *args, **kwargs: [],
    )

    result = engineering_variant_diff_service.compare_market_variants(
        object(),  # type: ignore[arg-type]
        country="瑞典",
        models=["RAV4", "SPORTAGE"],
        powertrain="HEV",
    )

    assert [item["trim"] for item in result["subjects"]] == ["Active", "Action"]
    assert all(item["selectionMode"] == "entry-variant" for item in result["subjects"])
    assert len(result["selectionNotes"]) == 2
    assert any(
        item["featureLabel"] == "Range Km" for item in result["differentFeatures"]
    )
