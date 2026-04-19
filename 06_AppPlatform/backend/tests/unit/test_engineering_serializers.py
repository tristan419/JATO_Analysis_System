from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import uuid4

from app.db.models import (
    ConfigBaseVariant,
    ConfigImportBatch,
    ConfigMarketFeatureOverride,
    ConfigMarketVariant,
    ConfigProject,
    ConfigVariant,
    ImportBatch,
)
from app.services.engineering_normalization_service import (
    _base_variant_payload,
    _market_feature_override_payload,
    _market_variant_payload,
)
from app.services.engineering_service import (
    _config_import_batch_payload,
    _config_variant_payload,
    _import_batch_payload,
    _project_payload,
)


def test_project_payload_contract() -> None:
    now = datetime(2026, 4, 11, 10, 0, tzinfo=timezone.utc)
    project = ConfigProject(
        project_id=uuid4(),
        project_code="BMW-X5-DE",
        brand="BMW",
        model="X5",
        market_country="Germany",
        display_name="BMW X5 Germany Config",
        status="active",
        created_at_utc=now,
        updated_at_utc=now,
    )
    payload = _project_payload(project)

    assert "projectId" in payload
    assert payload["projectCode"] == "BMW-X5-DE"
    assert payload["brand"] == "BMW"
    assert payload["model"] == "X5"
    assert payload["marketCountry"] == "Germany"
    assert payload["displayName"] == "BMW X5 Germany Config"
    assert payload["status"] == "active"
    assert payload["createdAtUtc"] == now.isoformat()
    assert payload["updatedAtUtc"] == now.isoformat()

    # must NOT leak snake_case field names
    assert "project_code" not in payload
    assert "market_country" not in payload


def test_config_import_batch_payload_contract() -> None:
    now = datetime(2026, 4, 11, 10, 0, tzinfo=timezone.utc)
    batch = ConfigImportBatch(
        config_import_batch_id=uuid4(),
        project_id=uuid4(),
        import_batch_id=uuid4(),
        source_schema_version="v2",
        replace_mode="append",
        import_status="completed",
        row_count=120,
        valid_from_date=date(2026, 4, 1),
        notes=None,
        created_at_utc=now,
    )
    payload = _config_import_batch_payload(batch)

    assert "configImportBatchId" in payload
    assert "projectId" in payload
    assert "importBatchId" in payload
    assert payload["sourceSchemaVersion"] == "v2"
    assert payload["replaceMode"] == "append"
    assert payload["importStatus"] == "completed"
    assert payload["rowCount"] == 120
    assert payload["validFromDate"] == "2026-04-01"
    assert payload["createdAtUtc"] == now.isoformat()

    # must NOT leak snake_case or frontend-only names
    assert "config_import_batch_id" not in payload
    assert "source_schema_version" not in payload


def test_config_variant_payload_contract() -> None:
    now = datetime(2026, 4, 11, 10, 0, tzinfo=timezone.utc)
    variant = ConfigVariant(
        variant_id=uuid4(),
        project_id=uuid4(),
        config_import_batch_id=uuid4(),
        external_row_key="ROW-001",
        brand="BMW",
        model="X5",
        trim_name="xDrive40i",
        version_name="2026 MY",
        market_country="Germany",
        powertrain="PHEV",
        body_style="SUV",
        drive_type="AWD",
        battery_kwh=Decimal("25.70"),
        range_km=Decimal("92.00"),
        target_msrp=Decimal("79900.00"),
        is_active=True,
        row_hash="abc123",
        attributes_json={"color": "black"},
        source_file_path="/imports/x5.xlsx",
        created_at_utc=now,
        updated_at_utc=now,
    )
    payload = _config_variant_payload(variant)

    assert "variantId" in payload
    assert "projectId" in payload
    assert "configImportBatchId" in payload
    assert payload["externalRowKey"] == "ROW-001"
    assert payload["brand"] == "BMW"
    assert payload["model"] == "X5"
    assert payload["trimName"] == "xDrive40i"
    assert payload["versionName"] == "2026 MY"
    assert payload["marketCountry"] == "Germany"
    assert payload["powertrain"] == "PHEV"
    assert payload["bodyStyle"] == "SUV"
    assert payload["driveType"] == "AWD"
    assert payload["batteryKwh"] == 25.7
    assert payload["rangeKm"] == 92.0
    assert payload["targetMsrp"] == 79900.0
    assert payload["isActive"] is True
    assert payload["rowHash"] == "abc123"
    assert payload["attributes"] == {"color": "black"}
    assert payload["sourceFilePath"] == "/imports/x5.xlsx"
    assert payload["createdAtUtc"] == now.isoformat()
    assert payload["updatedAtUtc"] == now.isoformat()

    # must NOT leak snake_case
    assert "variant_id" not in payload
    assert "trim_name" not in payload
    assert "battery_kwh" not in payload


def test_import_batch_payload_contract() -> None:
    started = datetime(2026, 4, 11, 9, 0, tzinfo=timezone.utc)
    finished = datetime(2026, 4, 11, 9, 5, tzinfo=timezone.utc)
    created = datetime(2026, 4, 11, 8, 55, tzinfo=timezone.utc)
    batch = ImportBatch(
        import_batch_id=uuid4(),
        domain="engineering",
        source_file_name="x5_config.xlsx",
        source_file_path="/imports/x5_config.xlsx",
        source_file_hash="sha256-abc",
        import_status="success",
        row_count=100,
        error_count=2,
        triggered_by="admin",
        started_at_utc=started,
        finished_at_utc=finished,
        created_at_utc=created,
    )
    payload = _import_batch_payload(batch)

    assert "importBatchId" in payload
    assert payload["domain"] == "engineering"
    assert payload["sourceFileName"] == "x5_config.xlsx"
    assert payload["sourceFilePath"] == "/imports/x5_config.xlsx"
    assert payload["sourceFileHash"] == "sha256-abc"
    assert payload["importStatus"] == "success"
    assert payload["rowCount"] == 100
    assert payload["errorCount"] == 2
    assert payload["triggeredBy"] == "admin"
    assert payload["startedAtUtc"] == started.isoformat()
    assert payload["finishedAtUtc"] == finished.isoformat()
    assert payload["createdAtUtc"] == created.isoformat()

    # must NOT leak snake_case
    assert "import_batch_id" not in payload
    assert "source_file_name" not in payload


def test_base_variant_payload_contract() -> None:
    now = datetime(2026, 4, 17, 11, 0, tzinfo=timezone.utc)
    item = ConfigBaseVariant(
        base_variant_id=uuid4(),
        project_id=uuid4(),
        business_key="bmw|x5|xdrive40i|2026 my|phev",
        brand="BMW",
        model="X5",
        trim_name="xDrive40i",
        version_name="2026 MY",
        powertrain="PHEV",
        base_features_json={"body_style": "SUV", "battery_kwh": 25.7},
        base_feature_labels_json={
            "body_style": "Body Style",
            "battery_kwh": "Battery kWh",
        },
        source_variant_count=2,
        market_count=2,
        created_at_utc=now,
        updated_at_utc=now,
    )
    payload = _base_variant_payload(item)

    assert payload["brand"] == "BMW"
    assert payload["model"] == "X5"
    assert payload["trimName"] == "xDrive40i"
    assert payload["sourceVariantCount"] == 2
    assert payload["marketCount"] == 2
    assert payload["baseFeatures"] == [
        {
            "featureCode": "battery_kwh",
            "featureLabel": "Battery kWh",
            "value": 25.7,
            "valueType": "number",
        },
        {
            "featureCode": "body_style",
            "featureLabel": "Body Style",
            "value": "SUV",
            "valueType": "text",
        },
    ]
    assert "base_variant_id" not in payload


def test_market_variant_payload_contract() -> None:
    now = datetime(2026, 4, 17, 11, 0, tzinfo=timezone.utc)
    item = ConfigMarketVariant(
        market_variant_id=uuid4(),
        project_id=uuid4(),
        base_variant_id=uuid4(),
        source_variant_id=uuid4(),
        external_row_key="SE-001",
        market_country="Sweden",
        target_msrp=Decimal("55900.00"),
        source_file_path="imports/xc60.xlsx",
        override_count=3,
        created_at_utc=now,
        updated_at_utc=now,
    )
    payload = _market_variant_payload(item)

    assert payload["marketCountry"] == "Sweden"
    assert payload["targetMsrp"] == 55900.0
    assert payload["overrideCount"] == 3
    assert payload["externalRowKey"] == "SE-001"
    assert "market_variant_id" not in payload


def test_market_feature_override_payload_contract() -> None:
    now = datetime(2026, 4, 17, 11, 0, tzinfo=timezone.utc)
    item = ConfigMarketFeatureOverride(
        feature_override_id=uuid4(),
        project_id=uuid4(),
        market_variant_id=uuid4(),
        source_variant_id=uuid4(),
        feature_code="heated_steering_wheel",
        feature_label="Heated Steering Wheel",
        value_type="text",
        bool_value=None,
        number_value=None,
        text_value="standard",
        json_value=None,
        availability="standard",
        package_code=None,
        created_at_utc=now,
        updated_at_utc=now,
    )
    payload = _market_feature_override_payload(item)

    assert payload["featureCode"] == "heated_steering_wheel"
    assert payload["featureLabel"] == "Heated Steering Wheel"
    assert payload["valueType"] == "text"
    assert payload["value"] == "standard"
    assert payload["availability"] == "standard"
    assert "feature_override_id" not in payload
