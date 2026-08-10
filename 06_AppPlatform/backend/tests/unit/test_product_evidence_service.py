from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID, uuid4

from app.services import product_evidence_service


@dataclass
class FakeTrim:
    trim_id: UUID
    brand: str
    model_name: str
    trim_name: str
    full_trim_name: str
    market: str
    material_no: str | None
    energy_type: str
    model_year: str


@dataclass
class FakeVersion:
    version_id: UUID
    trim_id: UUID
    status: str
    version_no: int
    source_upload_id: UUID
    snapshot_values: list[dict]
    snapshot_feature_count: int
    market: str
    model_year: str
    published_at_utc: datetime
    created_at_utc: datetime


@dataclass
class FakeBatch:
    import_batch_id: UUID
    source_file_name: str
    source_file_hash: str


@dataclass
class FakeContext:
    country: str
    market: str
    powertrain: str
    status: str = "active"
    source_role: str | None = None
    document_type: str | None = None
    source_url: str | None = None
    effective_from: str | None = None
    effective_to: str | None = None


def _record(
    *,
    brand: str,
    model: str,
    trim_name: str,
    file_name: str,
    source_role: str,
    values: list[dict],
) -> product_evidence_service.PublishedConfigRecord:
    trim_id = uuid4()
    source_id = uuid4()
    now = datetime.now(timezone.utc)
    trim = FakeTrim(
        trim_id=trim_id,
        brand=brand,
        model_name=model,
        trim_name=trim_name,
        full_trim_name=f"{model} {trim_name}",
        market="Switzerland",
        material_no="O7-001" if source_role == "own_product" else None,
        energy_type="PHEV",
        model_year="MY27",
    )
    version = FakeVersion(
        version_id=uuid4(),
        trim_id=trim_id,
        status="published",
        version_no=1,
        source_upload_id=source_id,
        snapshot_values=values,
        snapshot_feature_count=len(values),
        market="Switzerland",
        model_year="MY27",
        published_at_utc=now,
        created_at_utc=now,
    )
    batch = FakeBatch(
        import_batch_id=source_id,
        source_file_name=file_name,
        source_file_hash=f"hash-{source_id}",
    )
    context = FakeContext(
        country="Switzerland",
        market="Switzerland",
        powertrain="PHEV",
        source_role=source_role,
        document_type="official_pdf" if file_name.endswith(".pdf") else "configuration_workbook",
        source_url="https://example.test/source",
        effective_from="2026-05-01",
    )
    return product_evidence_service.PublishedConfigRecord(
        trim=trim,  # type: ignore[arg-type]
        version=version,  # type: ignore[arg-type]
        source_batch=batch,  # type: ignore[arg-type]
        source_contexts=(context,),  # type: ignore[arg-type]
    )


def _feature(
    code: str,
    name: str,
    value: str,
    *,
    page: int | None = None,
    sheet: str | None = None,
    cell: str | None = None,
) -> dict:
    source: dict[str, object] = {}
    if page is not None:
        source["pageNumber"] = page
    if sheet is not None:
        source["sheetName"] = sheet
    if cell is not None:
        source["cell"] = cell
    return {
        "featureCode": code,
        "featureName": name,
        "category": "Comfort",
        "rawValue": value,
        "normalizedValue": value,
        "availability": "STANDARD",
        "unit": None,
        "source": source,
        "sourceRow": 12,
        "sourceColumn": cell or "",
    }


def test_search_product_evidence_returns_filename_page_and_version(monkeypatch) -> None:
    sportage = _record(
        brand="Kia",
        model="Sportage",
        trim_name="GT-Line PHEV",
        file_name="kia_ch_new_sportage_price_list_2026-05_de.pdf",
        source_role="competitor",
        values=[_feature("seat_heating", "Seat heating", "Standard", page=7)],
    )
    monkeypatch.setattr(product_evidence_service, "_load_published_records", lambda _session: [sportage])

    result = product_evidence_service.search_product_evidence(
        object(),  # type: ignore[arg-type]
        country="CH",
        query="Sportage seat heating",
        subjects=["Sportage"],
    )

    assert result["status"] == "ok"
    evidence = result["evidenceRefs"][0]
    assert evidence["fileName"] == "kia_ch_new_sportage_price_list_2026-05_de.pdf"
    assert evidence["locationType"] == "page"
    assert evidence["location"] == "p.7"
    assert evidence["publishedVersionId"] == str(sportage.version.version_id)
    assert evidence["verificationStatus"] == "published_verified"


def test_search_product_evidence_fails_closed_for_wrong_country(monkeypatch) -> None:
    sportage = _record(
        brand="Kia",
        model="Sportage",
        trim_name="GT-Line PHEV",
        file_name="sportage.pdf",
        source_role="competitor",
        values=[_feature("seat_heating", "Seat heating", "Standard", page=7)],
    )
    monkeypatch.setattr(product_evidence_service, "_load_published_records", lambda _session: [sportage])

    result = product_evidence_service.search_product_evidence(
        object(),  # type: ignore[arg-type]
        country="Sweden",
        query="Sportage",
    )

    assert result["status"] == "insufficient_evidence"
    assert result["evidenceRefs"] == []


def test_compare_published_product_configs_keeps_cell_level_sources(monkeypatch) -> None:
    omoda = _record(
        brand="OMODA",
        model="OMODA 7",
        trim_name="Premium PHEV",
        file_name="omoda_7_configuration.xlsx",
        source_role="own_product",
        values=[
            _feature("seat_heating", "Seat heating", "Front and rear", sheet="OMODA 7", cell="F42"),
            _feature("hud", "HUD", "Standard", sheet="OMODA 7", cell="F50"),
            _feature("power_tailgate", "Power tailgate", "●", sheet="OMODA 7", cell="F51"),
        ],
    )
    sportage = _record(
        brand="Kia",
        model="Sportage",
        trim_name="GT-Line PHEV",
        file_name="sportage.pdf",
        source_role="competitor",
        values=[
            _feature("seat_heating", "Seat heating", "Front", page=7),
            _feature("hud", "HUD", "Not available", page=9),
            _feature("power_tailgate", "Power tailgate", "●", page=10),
        ],
    )
    monkeypatch.setattr(
        product_evidence_service,
        "_load_published_records",
        lambda _session: [omoda, sportage],
    )

    result = product_evidence_service.compare_published_product_configs(
        object(),  # type: ignore[arg-type]
        country="Switzerland",
        subject="O7",
        competitors=["Sportage"],
        powertrain="PHEV",
    )

    assert result["status"] == "ok"
    assert len(result["subjects"]) == 2
    assert len(result["differentFeatures"]) == 2
    assert len(result["commonFeatures"]) == 1
    assert result["commonFeatures"][0]["featureCode"] == "power_tailgate"
    hud = next(item for item in result["differentFeatures"] if item["featureCode"] == "hud")
    assert hud["values"][0]["evidenceRef"]["location"] == "OMODA 7!F50"
    assert hud["values"][1]["evidenceRef"]["location"] == "p.9"
    assert len(result["evidenceRefs"]) == 6


def test_product_evidence_feature_filter_reuses_governed_semantic_keys() -> None:
    assert product_evidence_service._matches_feature(
        {
            "featureName": "Panoramic skylight / 全景天窗",
            "featureCode": "roof_feature",
        },
        "panoramic roof",
    )
    assert product_evidence_service._matches_feature(
        {
            "featureName": "Induction electric tailgate (key induction) / 感应电动尾门（钥匙感应）",
            "featureCode": "tailgate_feature",
        },
        "power tailgate",
    )
    assert not product_evidence_service._matches_feature(
        {"featureName": "Manual tailgate", "featureCode": "manual_tailgate"},
        "power tailgate",
    )


def test_product_evidence_price_scope_filters_powertrain_and_drivetrain() -> None:
    record = _record(
        brand="Kia",
        model="Sportage",
        trim_name="GT-Line PHEV",
        file_name="sportage.pdf",
        source_role="competitor",
        values=[],
    )
    assert product_evidence_service._matches_snapshot_scope(
        record,
        {"category": "Prices", "featureName": "MSRP / 1.6 T-GDi PHEV / 4×4 / Automat"},
        query="Sportage PHEV AWD MSRP",
        features=["MSRP"],
    )
    assert not product_evidence_service._matches_snapshot_scope(
        record,
        {"category": "Prices", "featureName": "MSRP / 1.6 T-GDi HEV / 4×4 / Automat"},
        query="Sportage PHEV AWD MSRP",
        features=["MSRP"],
    )
    assert not product_evidence_service._matches_snapshot_scope(
        record,
        {"category": "Prices", "featureName": "MSRP / 1.6 T-GDi PHEV / 2WD / Automat"},
        query="Sportage PHEV AWD MSRP",
        features=["MSRP"],
    )
