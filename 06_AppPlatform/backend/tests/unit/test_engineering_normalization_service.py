from uuid import uuid4

from app.db.models import ConfigProject, ConfigVariant
from app.services import engineering_normalization_service
from app.services.engineering_normalization_service import (
    _build_normalized_rows,
    rebuild_project_normalized_config,
)


def test_build_normalized_rows_creates_base_and_sparse_overrides() -> None:
    project_id = uuid4()
    import_batch_id = uuid4()
    project = ConfigProject(
        project_id=project_id,
        project_code="VOLVO-XC60-EU",
        brand="Volvo",
        model="XC60",
        market_country="Europe",
        display_name="Volvo XC60 Europe",
        status="active",
    )
    sweden_variant = ConfigVariant(
        variant_id=uuid4(),
        project_id=project_id,
        config_import_batch_id=import_batch_id,
        external_row_key="SE-001",
        brand="Volvo",
        model="XC60",
        trim_name="Ultra",
        version_name="2026 MY",
        market_country="Sweden",
        powertrain="PHEV",
        body_style="SUV",
        drive_type="AWD",
        battery_kwh=18.8,
        range_km=82.0,
        target_msrp=55900.0,
        is_active=True,
        row_hash="se-row",
        attributes_json={"Heated Steering Wheel": "standard"},
        source_file_path="imports/xc60.xlsx",
    )
    germany_variant = ConfigVariant(
        variant_id=uuid4(),
        project_id=project_id,
        config_import_batch_id=import_batch_id,
        external_row_key="DE-001",
        brand="Volvo",
        model="XC60",
        trim_name="Ultra",
        version_name="2026 MY",
        market_country="Germany",
        powertrain="PHEV",
        body_style="SUV",
        drive_type="AWD",
        battery_kwh=18.8,
        range_km=82.0,
        target_msrp=54900.0,
        is_active=True,
        row_hash="de-row",
        attributes_json={
            "Heated Steering Wheel": "optional",
            "Massage Seats Pack": "package",
        },
        source_file_path="imports/xc60.xlsx",
    )

    base_variants, market_variants, overrides = _build_normalized_rows(
        project,
        [sweden_variant, germany_variant],
    )

    assert len(base_variants) == 1
    assert len(market_variants) == 2
    assert len(overrides) == 3

    base = base_variants[0]
    assert base.brand == "Volvo"
    assert base.model == "XC60"
    assert base.base_features_json == {
        "battery_kwh": 18.8,
        "body_style": "SUV",
        "drive_type": "AWD",
        "range_km": 82.0,
    }
    assert base.market_count == 2

    overrides_by_market = {
        item.market_country: item.override_count for item in market_variants
    }
    assert overrides_by_market == {"Germany": 2, "Sweden": 1}

    heated_values = {
        item.text_value
        for item in overrides
        if item.feature_code == "heated_steering_wheel"
    }
    assert heated_values == {"standard", "optional"}
    package_codes = {
        item.package_code
        for item in overrides
        if item.feature_code == "massage_seats_pack"
    }
    assert package_codes == {"package"}


def test_build_normalized_rows_keeps_single_market_variant_in_base() -> None:
    project_id = uuid4()
    import_batch_id = uuid4()
    project = ConfigProject(
        project_id=project_id,
        project_code="OMODA-9-EU",
        brand="Omoda",
        model="9",
        market_country="Europe",
        display_name="Omoda 9 Europe",
        status="active",
    )
    variant = ConfigVariant(
        variant_id=uuid4(),
        project_id=project_id,
        config_import_batch_id=import_batch_id,
        external_row_key="EU-001",
        brand="Omoda",
        model="9",
        trim_name="Premium",
        version_name="2026 MY",
        market_country="Finland",
        powertrain="BEV",
        body_style="SUV",
        drive_type="AWD",
        battery_kwh=95.0,
        range_km=520.0,
        target_msrp=48900.0,
        is_active=True,
        row_hash="eu-row",
        attributes_json={"Panoramic Roof": "standard"},
        source_file_path="imports/omoda9.xlsx",
    )

    base_variants, market_variants, overrides = _build_normalized_rows(
        project,
        [variant],
    )

    assert len(base_variants) == 1
    assert len(market_variants) == 1
    assert len(overrides) == 0
    assert base_variants[0].base_features_json == {
        "battery_kwh": 95.0,
        "body_style": "SUV",
        "drive_type": "AWD",
        "panoramic_roof": "standard",
        "range_km": 520.0,
    }
    assert market_variants[0].override_count == 0


def test_rebuild_project_normalized_config_clears_existing_rows_without_variants(
    monkeypatch,
) -> None:
    project_id = uuid4()
    project = ConfigProject(
        project_id=project_id,
        project_code="BMW-X5-DE",
        brand="BMW",
        model="X5",
        market_country="Germany",
        display_name="BMW X5 Germany",
        status="active",
    )
    recorded: list[object] = []

    monkeypatch.setattr(
        engineering_normalization_service.repo,
        "list_active_variants_for_project",
        lambda _session, _project_id: [],
    )
    monkeypatch.setattr(
        engineering_normalization_service.repo,
        "replace_project_normalized_variants",
        lambda _session, incoming_project_id: recorded.append(
            incoming_project_id
        ),
    )

    payload = rebuild_project_normalized_config(object(), project)

    assert recorded == [project_id]
    assert payload == {
        "baseVariantCount": 0,
        "marketVariantCount": 0,
        "featureOverrideCount": 0,
    }
