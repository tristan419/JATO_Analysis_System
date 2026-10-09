"""Real SQLAlchemy transactions: exercise public pricing paths, not mock prices."""
from datetime import date, timedelta
from uuid import uuid4
from types import SimpleNamespace

import pytest
from sqlalchemy import MetaData, create_engine, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session

from app.db import models
from app.infra import order_genius_repository as repo
from app.services import order_genius_service as service
from app.services import order_genius_vehicle_service as vehicle_service
from fastapi import HTTPException
from app.api.routes import order_genius as routes


@compiles(JSONB, "sqlite")
def compile_jsonb_sqlite(_type, _compiler, **_kw):
    return "JSON"


@pytest.fixture
def db():
    engine = create_engine("sqlite://", execution_options={"schema_translate_map": {"ordering": None}})
    tables = [models.MaterialBaselineVersion, models.MaterialSkuMaster,
              models.CountrySkuFobResolved, models.FobResolvedHistory,
              models.BrandColourSurchargeRule, models.SpecialColourSurchargeRule,
              models.BrandColourSwatchRule, models.CountryPaymentTermMaster,
              models.CountryMaterialFinance, models.CountryFobSourceMapping,
              models.CountryTemplateFobPeriod, models.OrderQuantityCell,
              models.QuantityCellHistory, models.MaterialSkuRemarkHistory]
    metadata = MetaData()
    for model in tables:
        table = model.__table__.to_metadata(metadata)
        # SQLite supports partial indexes, but does not read postgresql_where.
        for index in table.indexes:
            predicate = index.dialect_options["postgresql"].get("where")
            if predicate is not None:
                index.dialect_options["sqlite"]["where"] = predicate
        table.create(engine)
    with Session(engine) as session:
        baseline = repo.create_baseline_version(session, "test", None, "test", "test")
        session.flush()
        session.info["baseline"] = baseline.baseline_version_id
        for brand, amount in [("OMODA", 200), ("JAECOO", 300)]:
            for tier in ("dual", "special"):
                session.add(models.BrandColourSurchargeRule(brand=brand, colour_type=tier, surcharge_eur=amount))
        session.commit()
        yield session
    engine.dispose()


def sku(db, code, tier, brand="JAECOO", template="T**001", model="JAECOO7 SHS", name="Colour"):
    value = models.MaterialSkuMaster(
        baseline_version_id=db.info["baseline"], material_code=code, brand=brand,
        model_name=model, version="Premium", powertrain="PHEV", bom_template=template,
        exterior_color_code=code, exterior_color_name=name, exterior_color_type=tier,
        colour_tier=tier, is_active=True, lifecycle_status="active",
    )
    db.add(value)
    db.flush()
    return value


def fob(db, code, value, base=None, surcharge=None, term="TT", country="CH", source="template_base"):
    row = models.CountrySkuFobResolved(
        baseline_version_id=db.info["baseline"], material_code=code, country_code=country,
        payment_term_code=term, final_fob_eur=value, base_fob_eur=base,
        uploaded_fob_eur=base, colour_surcharge_eur=surcharge, fob_source_mode=source,
    )
    db.add(row)
    db.flush()
    return row


def test_material_colour_tier_has_no_implicit_default() -> None:
    column = models.MaterialSkuMaster.__table__.c.colour_tier

    assert column.nullable is False
    assert column.default is None
    assert column.server_default is None


def test_product_save_is_atomic_and_returns_saved_powertrain(db):
    first = sku(db, "A", "single")
    second = sku(db, "B", "dual")
    first.model_name = second.model_name = "JAECOO8 SHS"
    fob(db, "A", 1000, base=1000)
    fob(db, "B", 1300, base=1000, surcharge=300)
    db.commit()
    body = {"materialCodes": ["A", "B"], "version": "7 seats", "powertrain": "HEV",
            "remark": "new note", "rowVersions": {"A": 1, "B": 99}}
    with pytest.raises(HTTPException) as error:
        routes.patch_sku_metadata("A", body, db, SimpleNamespace(name="test", role="admin"))
    assert error.value.status_code == 409
    db.expire_all()
    assert first.remark is None and first.row_version == 1
    assert first.version == "Premium" and first.powertrain == "PHEV"
    body["rowVersions"]["B"] = 1
    result = routes.patch_sku_metadata("A", body, db, SimpleNamespace(name="test", role="admin"))
    db.expire_all()
    assert result["productFields"]["powertrain"] == "HEV"
    assert {row["powertrain"] for row in service.build_matrix(db, "CH", 2026)["rows"]} == {"HEV"}
    assert first.powertrain == second.powertrain == "HEV"
    assert first.remark == second.remark == "new note"
    assert first.row_version == second.row_version == 2
    assert len(db.scalars(select(models.MaterialSkuRemarkHistory)).all()) == 2
    # Re-saving unchanged remarks neither fabricates a version nor adds history.
    routes.patch_sku_metadata("A", body, db, SimpleNamespace(name="test", role="admin"))
    db.expire_all()
    assert first.row_version == second.row_version == 2
    assert len(db.scalars(select(models.MaterialSkuRemarkHistory)).all()) == 2


@pytest.mark.parametrize("saved,expected", [("HEV", "HEV"), ("Other", "OTHER"), (None, "")])
def test_powertrain_survives_name_template_and_lifecycle_edits(db, saved, expected):
    original = sku(db, "ORIGINAL", "single", template="T**0008", model="OMODA5 HEV")
    original.powertrain = saved
    fob(db, original.material_code, 1000, base=1000)
    db.commit()
    # The same metadata path is used when editing an upgraded template.
    repo.update_sku_metadata(db, [original.material_code], model_name="OMODA5 BEV", version="Upgraded")
    original.bom_template = "T**0011"
    original.lifecycle_status = "historical"
    db.commit()
    db.expire_all()
    rows, _ = repo.list_bom_with_fob(db)
    assert rows[0]["powertrain"] == expected
    assert original.powertrain == saved
    assert repo.get_sku_by_material_code(db, original.material_code).powertrain == saved


@pytest.mark.parametrize("source_tier", ["single", "dual", "special"])
@pytest.mark.parametrize("target_tier", ["single", "dual", "special"])
@pytest.mark.parametrize("brand,amount", [("OMODA", 200), ("JAECOO", 300)])
def test_create_every_tier_from_every_source(db, source_tier, target_tier, brand, amount):
    source = sku(db, "SOURCE", source_tier, brand=brand)
    target = sku(db, "NEW", target_tier, brand=brand)
    fob(db, source.material_code, 10000 + (0 if source_tier == "single" else amount), 10000,
        None if source_tier == "single" else amount)
    result = repo.initialize_sku_fobs_from_source(db, target.material_code, source.material_code)
    db.commit()
    db.expire_all()
    saved = repo.get_fob_for_country_sku(db, "CH", "NEW")
    assert result["created"] == 1
    assert saved.base_fob_eur == 10000
    assert saved.final_fob_eur == 10000 + (0 if target_tier == "single" else amount)


@pytest.mark.parametrize("reverse", [False, True])
def test_country_adjustment_uses_frozen_base(db, reverse, monkeypatch):
    sku(db, "S", "single")
    sku(db, "D", "dual")
    single = fob(db, "S", 1000)
    dual = fob(db, "D", 1000, source="copied_from_country")
    rows = [dual, single] if reverse else [single, dual]
    monkeypatch.setattr(repo, "list_fob_by_country", lambda *_: rows)
    result = repo.adjust_country_fobs(db, "CH", 200)
    db.commit()
    assert result["adjusted"] == 2
    assert single.final_fob_eur == 1200
    assert dual.base_fob_eur == 1200
    assert dual.final_fob_eur == 1500


def test_audit_apply_reload_is_idempotent_and_matches_matrix(db):
    sku(db, "S", "single")
    sku(db, "D", "dual")
    sku(db, "U", "", template="U**001")
    fob(db, "S", 19350)
    fob(db, "D", 19350, source="copied_from_country")
    fob(db, "U", 1000)
    db.commit()
    audit = repo.audit_colour_surcharge_reprice(db)
    assert audit["summary"]["missingTier"] == 1
    assert audit["summary"]["autoReprice"] == 1
    applied = routes.apply_colour_surcharge_reprice({"previewFingerprint": audit["fingerprint"]}, db, SimpleNamespace(name="test", role="admin"))
    assert applied["totals"]["updated"] == 1
    db.expire_all()
    saved = repo.get_fob_for_country_sku(db, "CH", "D")
    assert saved.final_fob_eur == 19650
    again = repo.audit_colour_surcharge_reprice(db)
    assert again["summary"]["autoReprice"] == 0
    assert repo.apply_colour_surcharge_reprice_audit(db, again["fingerprint"])["totals"]["updated"] == 0
    with pytest.raises(ValueError, match="stale"):
        repo.apply_colour_surcharge_reprice_audit(db, audit["fingerprint"])
    bom, _countries = repo.list_bom_with_fob(db)
    assert next(row for row in bom if row["materialCode"] == "D")["fobByCountry"]["CH"]["finalFobEur"] == 19650
    matrix = service.build_matrix(db, "CH", 2026)
    assert next(row for row in matrix["rows"] if row["materialCode"] == "D")["fobEur"] == 19650


def test_matrix_selection_date_uses_country_template_base_and_saved_tier(db):
    sku(db, "S", "single")
    sku(db, "D", "dual")
    fob(db, "S", 1000)
    fob(db, "D", 1300, base=1000, surcharge=300)
    repo.save_country_template_fob_period(
        db,
        country_code="CH",
        bom_template="T**001",
        valid_from=date(2026, 7, 15),
        valid_to=date(2026, 7, 31),
        base_fob_eur=1200,
        remark="July update",
        changed_by="test",
    )
    db.commit()

    matrix = service.build_matrix(db, "CH", 2026, selection_date=date(2026, 7, 20))
    rows = {row["materialCode"]: row for row in matrix["rows"]}

    assert rows["S"]["fobEur"] == 1200
    assert rows["D"]["fobEur"] == 1500
    assert rows["D"]["fobPeriod"]["colourTier"] == "dual"
    assert rows["D"]["fobPeriod"]["surchargeEur"] == 300


def test_country_template_periods_remain_country_specific(db):
    sku(db, "S", "single")
    fob(db, "S", 1000, country="CH")
    fob(db, "S", 900, country="SK")
    for country, base in (("CH", 1200), ("SK", 950)):
        repo.save_country_template_fob_period(
            db,
            country_code=country,
            bom_template="T**001",
            valid_from=date(2026, 7, 1),
            valid_to=date(2026, 7, 31),
            base_fob_eur=base,
            remark=None,
            changed_by="test",
        )
    db.commit()

    ch = service.build_matrix(db, "CH", 2026, selection_date=date(2026, 7, 20))
    sk = service.build_matrix(db, "SK", 2026, selection_date=date(2026, 7, 20))

    assert next(row for row in ch["rows"] if row["materialCode"] == "S")["fobEur"] == 1200
    assert next(row for row in sk["rows"] if row["materialCode"] == "S")["fobEur"] == 950


def test_zero_period_marks_sku_stopped_without_default_fallback(db):
    sku(db, "S", "single")
    fob(db, "S", 1000)
    repo.save_country_template_fob_period(
        db,
        country_code="CH",
        bom_template="T**001",
        valid_from=date(2026, 8, 1),
        valid_to=date(2026, 8, 31),
        base_fob_eur=0,
        remark="Stopped",
        changed_by="test",
    )
    db.commit()

    matrix = service.build_matrix(db, "CH", 2026, selection_date=date(2026, 8, 10))
    row = next(item for item in matrix["rows"] if item["materialCode"] == "S")

    assert row["fobEur"] is None
    assert row["fobPeriod"]["status"] == "stopped"


def test_configured_period_gap_never_falls_back_to_legacy_fob(db):
    sku(db, "S", "single")
    fob(db, "S", 1000)
    repo.save_country_template_fob_period(
        db,
        country_code="CH",
        bom_template="T**001",
        valid_from=date(2026, 1, 1),
        valid_to=date(2026, 1, 31),
        base_fob_eur=1100,
        remark=None,
        changed_by="test",
    )
    db.commit()

    matrix = service.build_matrix(db, "CH", 2026, selection_date=date(2026, 2, 10))
    row = next(item for item in matrix["rows"] if item["materialCode"] == "S")

    assert row["fobEur"] is None
    assert row["fobPeriod"]["status"] == "no_price"
    assert row["fobConflict"]["reason"] == "No FOB is available on 2026-02-10"


def test_no_period_schedule_keeps_undated_legacy_fob(db):
    sku(db, "S", "single")
    fob(db, "S", 1000)
    db.commit()

    matrix = service.build_matrix(db, "CH", 2026, selection_date=date(2026, 2, 10))
    row = next(item for item in matrix["rows"] if item["materialCode"] == "S")

    assert row["fobEur"] == 1000
    assert row["fobPeriod"] is None


def test_deleting_last_period_previews_default_and_retains_history(db):
    single = sku(db, "S", "single")
    fob(db, "S", 1000, base=1000)
    row = repo.save_country_template_fob_period(db, country_code="CH", bom_template="T**001",
        valid_from=date(2026, 8, 1), valid_to=date(2026, 8, 31), base_fob_eur=1200, remark=None, changed_by="test")
    db.commit()
    preview = routes.delete_bom_template_fob_period(row.country_template_fob_period_id, row.row_version,
        preview_only=True, fingerprint=None, session=db, user=SimpleNamespace(name="test", role="admin"))
    assert preview["defaultBaseFobEur"] == 1000 and preview["lastPeriod"] is True
    with pytest.raises(HTTPException) as exc:
        routes.delete_bom_template_fob_period(row.country_template_fob_period_id, row.row_version,
            preview_only=False, fingerprint="stale", session=db, user=SimpleNamespace(name="test", role="admin"))
    assert exc.value.status_code == 409
    confirmed = routes.delete_bom_template_fob_period(row.country_template_fob_period_id, row.row_version,
        preview_only=False, fingerprint=preview["fingerprint"], session=db, user=SimpleNamespace(name="test", role="admin"))
    assert confirmed["deleted"] is True
    db.commit(); db.expire_all()
    assert repo.list_country_template_fob_periods(db, "CH", "T**001") == []
    assert repo.has_country_template_fob_periods(db, "CH", "T**001") is False
    amount, conflict, evidence = service.resolve_date_effective_fob(db, "CH", single, date(2026, 8, 2))
    assert amount is None and conflict is None and evidence is None  # caller now uses its undated default
    assert service.build_matrix(db, "CH", 2026, selection_date=date(2026, 8, 2))["rows"][0]["fobEur"] == 1000
    assert db.get(models.CountryTemplateFobPeriod, row.country_template_fob_period_id).status == "deleted"
    saved = service.update_quantity_cell(db, "CH", 2026, 8, "S", 2, "test", 1)
    assert saved["fob_eur"] == 1000
    # The same start date can be used again without deleting audit history.
    repo.save_country_template_fob_period(db, country_code="CH", bom_template="T**001",
        valid_from=date(2026, 8, 1), valid_to=date(2026, 8, 31), base_fob_eur=1300, remark=None, changed_by="test")
    db.commit()
    assert repo.has_country_template_fob_periods(db, "CH", "T**001") is True


def test_period_only_bom_country_search_includes_materials_and_keeps_raw_default(db):
    sku(db, "S", "single"); sku(db, "D", "dual")
    sku(db, "OTHER", "single", template="OTHER**001")
    fob(db, "OTHER", 900, country="NL")
    first = repo.save_country_template_fob_period(db, country_code="CH", bom_template="T**001",
        valid_from=date(2026, 8, 1), valid_to=date(2026, 8, 31), base_fob_eur=1200, remark=None, changed_by="test")
    repo.save_country_template_fob_period(db, country_code="CH", bom_template="T**001",
        valid_from=date(2027, 1, 1), valid_to=None, base_fob_eur=1300, remark=None, changed_by="test")
    db.commit()
    items, countries = repo.list_bom_with_fob(db, country_code="CH")
    assert countries == ["CH", "NL"]
    assert {item["materialCode"] for item in items} == {"S", "D"}
    assert items[0]["fobByCountry"] == {}
    assert [p["baseFobEur"] for p in items[0]["fobPeriodsByCountry"]["CH"]] == [1200, 1300]
    assert repo.list_active_fob_material_codes(db, "CH") == {"S", "D"}
    preview = repo.preview_country_template_fob_period_deletion(db, first.country_template_fob_period_id, first.row_version)
    assert preview["lastPeriod"] is False
    repo.delete_country_template_fob_period(db, first.country_template_fob_period_id, first.row_version)
    db.commit()
    assert service.resolve_date_effective_fob(db, "CH", repo.get_sku_by_material_code(db, "S"), date(2026, 8, 2))[0] is None


@pytest.mark.parametrize("default", [None, 0, 1000])
def test_last_period_deletion_returns_to_default_or_no_price_without_restore(db, default):
    sku(db, "S", "single")
    if default is not None:
        fob(db, "S", default, base=default)
    row = repo.save_country_template_fob_period(db, country_code="CH", bom_template="T**001",
        valid_from=date(2026, 8, 1), valid_to=date(2026, 8, 31), base_fob_eur=1200, remark=None, changed_by="test")
    db.commit()
    preview = repo.preview_country_template_fob_period_deletion(db, row.country_template_fob_period_id, row.row_version)
    assert preview["defaultBaseFobEur"] == default
    routes.delete_bom_template_fob_period(row.country_template_fob_period_id, row.row_version,
        preview_only=False, fingerprint=preview["fingerprint"], session=db, user=SimpleNamespace(name="test", role="admin"))
    assert repo.has_country_template_fob_periods(db, "CH", "T**001") is False
    if not default:
        with pytest.raises(HTTPException) as error:
            vehicle_service._resolve_pi_pricing_date(db, "CH", "S", 2026, 8, date(2026, 8, 1))
        assert error.value.status_code == 409
    else:
        assert vehicle_service._resolve_pi_pricing_date(db, "CH", "S", 2026, 8, date(2026, 8, 1)) == date(2026, 8, 1)


def test_last_period_delete_does_not_pick_ambiguous_default_and_rechecks_changes(db):
    sku(db, "S", "single")
    first = fob(db, "S", 1000, base=1000)
    row = repo.save_country_template_fob_period(db, country_code="CH", bom_template="T**001",
        valid_from=date(2026, 8, 1), valid_to=date(2026, 8, 31), base_fob_eur=1200, remark=None, changed_by="test")
    db.commit()
    preview = repo.preview_country_template_fob_period_deletion(db, row.country_template_fob_period_id, row.row_version)
    first.final_fob_eur = first.base_fob_eur = 1100
    db.commit()
    with pytest.raises(HTTPException) as error:
        routes.delete_bom_template_fob_period(row.country_template_fob_period_id, row.row_version,
            preview_only=False, fingerprint=preview["fingerprint"], session=db, user=SimpleNamespace(name="test", role="admin"))
    assert error.value.status_code == 409
    assert row.status == "active"
    sku(db, "OTHER", "single")
    fob(db, "OTHER", 1300, base=1300)
    db.commit()
    with pytest.raises(ValueError, match="(?i)conflict|ambiguous"):
        repo.preview_country_template_fob_period_deletion(db, row.country_template_fob_period_id, row.row_version)
    assert row.status == "active"


def test_country_month_availability_prices_all_colours_and_keeps_other_country(db):
    sku(db, "S", "single"); sku(db, "D", "dual")
    for country in ("CH", "SK"):
        fob(db, "S", 1000, base=1000, country=country)
        fob(db, "D", 1300, base=1000, surcharge=300, country=country)
    for month, price in ((8, 1200), (12, 1500)):
        repo.save_country_template_fob_period(db, country_code="CH", bom_template="T**001",
            valid_from=date(2026, month, 14 if month == 12 else 1), valid_to=None if month == 12 else date(2026, 8, 31),
            base_fob_eur=price, remark=None, changed_by="test")
    db.commit()
    ch = {row["materialCode"]: row for row in service.build_matrix(db, "CH", 2026)["rows"]}
    sk = {row["materialCode"]: row for row in service.build_matrix(db, "SK", 2026)["rows"]}
    assert ch["S"]["months"]["8"]["fobEur"] == 1200
    assert ch["D"]["months"]["8"]["fobEur"] == 1500
    assert ch["D"]["months"]["9"]["isEditable"] is False
    assert ch["D"]["months"]["12"]["availableRanges"] == [{"from": "2026-12-14", "to": "2026-12-31"}]
    assert ch["D"]["months"]["12"]["requiresOrderDate"] is True
    assert sk["D"]["months"]["9"]["fobEur"] == 1300
    with pytest.raises(ValueError, match="No country FOB"):
        service.update_quantity_cell(db, "CH", 2026, 9, "D", 2, "test", 1)
    saved = service.update_quantity_cell(db, "CH", 2026, 12, "D", 2, "test", 1)
    db.commit(); db.expire_all()
    assert saved["fob_eur"] == 1800
    cell = repo.list_quantities_for_country_year(db, "CH", 2026)[0]
    assert cell.fob_eur == 1800


def test_period_only_prices_are_visible_and_pi_uses_the_same_month_and_date(db):
    sku(db, "S", "single"); sku(db, "D", "dual")
    # No old SKU FOB is needed: the country's template period is authoritative.
    repo.save_country_template_fob_period(db, country_code="CH", bom_template="T**001",
        valid_from=date(2026, 8, 14), valid_to=date(2026, 8, 31), base_fob_eur=1200, remark=None, changed_by="test")
    db.commit(); db.expire_all()
    rows = {row["materialCode"]: row for row in service.build_matrix(db, "CH", 2026)["rows"]}
    assert rows["D"]["months"]["8"]["fobEur"] == 1500
    with pytest.raises(HTTPException, match="choose an available orderDate"):
        vehicle_service._resolve_pi_pricing_date(db, "CH", "D", 2026, 8, None)
    for day in (14, 31):
        resolved = vehicle_service._resolve_pi_pricing_date(db, "CH", "D", 2026, 8, date(2026, 8, day))
        assert vehicle_service._line_payload_from_material(db, "CH", "D", {}, pricing_date=resolved)["fobEur"] == 1500
    with pytest.raises(HTTPException):
        vehicle_service._line_payload_from_material(db, "CH", "D", {}, pricing_date=date(2026, 8, 13))


def test_pi_full_month_same_price_periods_and_legacy_conflict(db):
    sku(db, "S", "single")
    for start, end in ((1, 14), (15, 31)):
        repo.save_country_template_fob_period(db, country_code="CH", bom_template="T**001",
            valid_from=date(2026, 8, start), valid_to=date(2026, 8, end), base_fob_eur=1000, remark=None, changed_by="test")
    db.commit()
    assert vehicle_service._resolve_pi_pricing_date(db, "CH", "S", 2026, 8, None) == date(2026, 8, 1)
    second = repo.list_country_template_fob_periods(db, "CH", "T**001")[1]
    second.base_fob_eur = 1200  # An existing legacy conflict, not allowed through CRUD.
    db.commit()
    with pytest.raises(HTTPException, match="Conflicting monthly"):
        vehicle_service._resolve_pi_pricing_date(db, "CH", "S", 2026, 8, date(2026, 8, 20))


def test_lifecycle_preview_rejects_inverted_dates_without_writing(db):
    single = sku(db, "S", "single")
    db.commit()
    with pytest.raises(ValueError, match="Final order date"):
        repo.preview_bom_template_lifecycle_update(db, "S", date(2026, 9, 30), date(2026, 8, 1))
    db.expire_all()
    assert single.effective_from_date is None and single.effective_to_date is None


def test_planned_template_accepts_month_plan_before_its_start_day(db):
    future = date.today() + timedelta(days=45)
    single = sku(db, "S", "single")
    single.effective_from_date = date(future.year, future.month, 14)
    fob(db, "S", 1000, base=1000)
    db.commit()
    matrix = service.build_matrix(db, "CH", future.year)
    row = next(row for row in matrix["rows"] if row["materialCode"] == "S")
    assert row["months"][str(future.month)]["isEditable"] is True
    assert row["months"][str(future.month)]["availableRanges"][0]["from"].endswith("-14")
    assert service.update_quantity_cell(db, "CH", future.year, future.month, "S", 5, "test", 1)["fob_eur"] == 1000


def test_different_bases_in_disjoint_days_of_one_month_rejected(db):
    sku(db, "S", "single")
    repo.save_country_template_fob_period(db, country_code="CH", bom_template="T**001",
        valid_from=date(2026, 8, 1), valid_to=date(2026, 8, 10), base_fob_eur=1000, remark=None, changed_by="test")
    db.commit()
    with pytest.raises(ValueError, match="One month"):
        repo.save_country_template_fob_period(db, country_code="CH", bom_template="T**001",
            valid_from=date(2026, 8, 20), valid_to=date(2026, 8, 31), base_fob_eur=1200, remark=None, changed_by="test")
    repo.save_country_template_fob_period(db, country_code="CH", bom_template="T**001",
        valid_from=date(2026, 8, 20), valid_to=date(2026, 8, 31), base_fob_eur=1000, remark=None, changed_by="test")
    db.commit()


def test_legacy_monthly_price_conflict_blocks_only_affected_country(db):
    sku(db, "S", "single"); fob(db, "S", 1000, country="CH"); fob(db, "S", 900, country="SK")
    for day, end, base in ((1, 10, 1000), (20, 31, 1200)):
        db.add(models.CountryTemplateFobPeriod(country_code="CH", bom_template="T**001",
            valid_from=date(2026, 8, day), valid_to=date(2026, 8, end), base_fob_eur=base))
    db.commit()
    ch = service.build_matrix(db, "CH", 2026)["rows"][0]["months"]["8"]
    assert ch["isEditable"] is False and "Conflicting" in ch["reason"]
    assert service.build_matrix(db, "SK", 2026)["rows"][0]["months"]["8"]["isEditable"] is True


def test_paused_period_never_adds_surcharge_or_saves_quantity(db):
    sku(db, "D", "dual"); fob(db, "D", 1300, base=1000, surcharge=300)
    repo.save_country_template_fob_period(db, country_code="CH", bom_template="T**001",
        valid_from=date(2026, 8, 1), valid_to=date(2026, 8, 31), base_fob_eur=0, remark=None, changed_by="test")
    db.commit()
    with pytest.raises(ValueError, match="paused"):
        service.update_quantity_cell(db, "CH", 2026, 8, "D", 1, "test", 1)
    assert repo.list_quantities_for_country_year(db, "CH", 2026) == []


def test_template_lifecycle_updates_every_colour_and_is_date_derived(db):
    single = sku(db, "S", "single")
    dual = sku(db, "D", "dual")
    result = repo.update_bom_template_lifecycle(
        db,
        material_code="S",
        lifecycle_status="active",
        effective_from=date(2026, 3, 14),
        effective_to=date(2026, 9, 30),
        expected_version=single.row_version,
    )
    db.commit()

    assert result["materialCodes"] == ["D", "S"]
    assert single.effective_from_date == dual.effective_from_date == date(2026, 3, 14)
    assert single.effective_to_date == dual.effective_to_date == date(2026, 9, 30)
    assert repo.resolve_effective_lifecycle_status(single, date(2026, 3, 13)) == "not_yet_active"
    assert repo.resolve_effective_lifecycle_status(single, date(2026, 3, 14)) == "phase_out"
    assert repo.resolve_effective_lifecycle_status(single, date(2026, 9, 30)) == "phase_out"
    assert repo.resolve_effective_lifecycle_status(single, date(2026, 10, 1)) == "historical"


def test_default_matrix_scope_excludes_expired_templates_until_historical_opt_in(db):
    expired = sku(db, "OLD", "single", template="OLD**001")
    planned = sku(db, "NEW", "single", template="NEW**001")
    today = date.today()
    expired.effective_from_date = today - timedelta(days=60)
    expired.effective_to_date = today - timedelta(days=1)
    expired.is_active = False
    expired.lifecycle_status = "historical"
    planned.effective_from_date = today + timedelta(days=10)
    planned.is_active = False
    db.commit()

    rows = repo.list_active_skus(
        db,
        target_date=today + timedelta(days=10),
    )

    assert {row.material_code for row in rows} == {"NEW"}


def test_matrix_historical_opt_in_exposes_editable_backfill_row_without_reactivation(db):
    expired = sku(db, "OLD", "single", template="OLD**001")
    expired.effective_from_date = date.today() - timedelta(days=60)
    expired.effective_to_date = date.today() - timedelta(days=1)
    expired.is_active = False
    expired.lifecycle_status = "historical"
    fob(db, "OLD", 18000)
    db.commit()

    default_matrix = service.build_matrix(db, "CH", date.today().year)
    backfill_matrix = service.build_matrix(
        db,
        "CH",
        date.today().year,
        include_historical=True,
    )

    assert all(row["materialCode"] != "OLD" for row in default_matrix["rows"])
    row = next(row for row in backfill_matrix["rows"] if row["materialCode"] == "OLD")
    assert row["lifecycleStatus"] == "historical"
    assert row["historicalBackfill"] is True
    assert row["editable"] is True
    assert row["priceSource"] == "undated_default"
    assert expired.lifecycle_status == "historical"
    assert expired.is_active is False


def test_historical_quantity_requires_opt_in_and_rejects_future_month(db):
    expired = sku(db, "OLD", "single", template="OLD**001")
    expired.effective_to_date = date.today() - timedelta(days=1)
    expired.is_active = False
    expired.lifecycle_status = "historical"
    fob(db, "OLD", 18000)
    db.commit()

    with pytest.raises(ValueError, match="explicit includeHistorical"):
        service.update_quantity_cell(
            db, "CH", date.today().year, date.today().month, "OLD", 2, "tester", 1,
        )

    saved = service.update_quantity_cell(
        db,
        "CH",
        date.today().year,
        date.today().month,
        "OLD",
        2,
        "tester",
        1,
        include_historical=True,
    )
    assert saved["quantity"] == 2
    assert expired.lifecycle_status == "historical"
    assert expired.is_active is False

    future_year = date.today().year + (1 if date.today().month == 12 else 0)
    future_month = 1 if date.today().month == 12 else date.today().month + 1
    with pytest.raises(ValueError, match="future month"):
        service.update_quantity_cell(
            db,
            "CH",
            future_year,
            future_month,
            "OLD",
            1,
            "tester",
            1,
            include_historical=True,
        )


def test_historical_backfill_turns_lifecycle_rejection_into_price_preview(db):
    expired = sku(db, "OLD", "single", template="OLD**001")
    expired.effective_from_date = date(2026, 1, 1)
    expired.effective_to_date = date(2026, 6, 30)
    expired.is_active = False
    expired.lifecycle_status = "historical"
    fob(db, "OLD", 18000)
    db.commit()

    blocked_fob, blocked_conflict, _ = service.resolve_date_effective_fob(
        db, "CH", expired, date(2026, 8, 15),
    )
    preview_fob, preview_conflict, preview_evidence = service.resolve_date_effective_fob(
        db,
        "CH",
        expired,
        date(2026, 8, 15),
        allow_historical_backfill=True,
    )

    assert blocked_fob is None
    assert "expired" in blocked_conflict["reason"]
    assert preview_fob is None
    assert preview_conflict is None
    assert preview_evidence is None


def test_historical_surcharge_review_compares_saved_evidence_with_current_rule(db):
    dual = sku(db, "OLD", "dual", template="OLD**001")
    saved_fob = fob(db, "OLD", 18200, base=18000, surcharge=200)

    review = service._historical_surcharge_review(db, dual, saved_fob, None)

    assert review["status"] == "changed"
    assert review["savedSurchargeEur"] == 200
    assert review["currentSurchargeEur"] == 300
    assert review["requiresConfirmation"] is True


def test_country_period_must_stay_inside_template_lifecycle(db):
    single = sku(db, "S", "single")
    sku(db, "D", "dual")
    repo.update_bom_template_lifecycle(
        db,
        material_code="S",
        lifecycle_status="active",
        effective_from=date(2026, 3, 14),
        effective_to=date(2026, 9, 30),
        expected_version=single.row_version,
    )

    with pytest.raises(ValueError, match="starts before"):
        repo.save_country_template_fob_period(
            db,
            country_code="CH",
            bom_template="T**001",
            valid_from=date(2026, 3, 1),
            valid_to=date(2026, 6, 30),
            base_fob_eur=1000,
            remark=None,
            changed_by="test",
        )
    with pytest.raises(ValueError, match="exceeds"):
        repo.save_country_template_fob_period(
            db,
            country_code="CH",
            bom_template="T**001",
            valid_from=date(2026, 3, 14),
            valid_to=None,
            base_fob_eur=1000,
            remark=None,
            changed_by="test",
        )


def test_lifecycle_preview_lists_price_periods_that_would_be_outside(db):
    sku(db, "S", "single")
    repo.save_country_template_fob_period(
        db,
        country_code="CH",
        bom_template="T**001",
        valid_from=date(2026, 1, 1),
        valid_to=date(2026, 12, 31),
        base_fob_eur=1000,
        remark=None,
        changed_by="test",
    )
    db.commit()

    preview = repo.preview_bom_template_lifecycle_update(
        db,
        "S",
        date(2026, 1, 1),
        date(2026, 9, 30),
    )

    assert preview["canApply"] is False
    assert preview["affectedPeriods"][0]["afterTemplateEnd"] is True
    with pytest.raises(ValueError, match="exceed"):
        repo.update_bom_template_lifecycle(
            db,
            material_code="S",
            lifecycle_status="phase_out",
            effective_from=date(2026, 1, 1),
            effective_to=date(2026, 9, 30),
            expected_version=1,
        )


def test_lifecycle_route_maps_legacy_months_and_updates_template_group(db):
    single = sku(db, "S", "single")
    dual = sku(db, "D", "dual")
    result = routes.patch_sku_lifecycle(
        "S",
        {
            "lifecycleStatus": "phase_out",
            "effectiveFrom": "2026-03",
            "effectiveTo": "2026-09",
            "rowVersion": single.row_version,
        },
        session=db,
        user=SimpleNamespace(name="test", role="admin"),
    )

    assert result["effectiveFrom"] == "2026-03-01"
    assert result["effectiveTo"] == "2026-09-30"
    assert result["materialCodes"] == ["D", "S"]
    assert single.effective_from_date == dual.effective_from_date == date(2026, 3, 1)
    assert single.effective_to_date == dual.effective_to_date == date(2026, 9, 30)


def test_lifecycle_route_preserves_omitted_boundary_and_clears_explicit_null(db):
    single = sku(db, "S", "single")
    single.effective_from_date = date(2026, 3, 14)
    single.effective_to_date = date(2026, 9, 30)
    db.commit()

    preserved = routes.patch_sku_lifecycle(
        "S",
        {
            "lifecycleStatus": "phase_out",
            "effectiveFrom": "2026-04-01",
            "rowVersion": single.row_version,
        },
        session=db,
        user=SimpleNamespace(name="test", role="admin"),
    )
    assert preserved["effectiveFrom"] == "2026-04-01"
    assert preserved["effectiveTo"] == "2026-09-30"

    cleared = routes.patch_sku_lifecycle(
        "S",
        {
            "lifecycleStatus": "active",
            "effectiveTo": None,
            "rowVersion": preserved["rowVersions"]["S"],
        },
        session=db,
        user=SimpleNamespace(name="test", role="admin"),
    )
    assert cleared["effectiveFrom"] == "2026-04-01"
    assert cleared["effectiveTo"] is None


def test_lifecycle_route_returns_actionable_period_conflict(db):
    single = sku(db, "S", "single")
    repo.save_country_template_fob_period(
        db,
        country_code="CH",
        bom_template="T**001",
        valid_from=date(2026, 1, 1),
        valid_to=date(2026, 12, 31),
        base_fob_eur=1000,
        remark=None,
        changed_by="test",
    )
    db.commit()

    preview = routes.patch_sku_lifecycle(
        "S",
        {
            "lifecycleStatus": "phase_out",
            "effectiveFrom": "2026-01-01",
            "effectiveTo": "2026-09-30",
            "rowVersion": single.row_version,
            "previewOnly": True,
        },
        session=db,
        user=SimpleNamespace(name="test", role="admin"),
    )
    assert preview["canApply"] is False

    with pytest.raises(routes.HTTPException) as exc:
        routes.patch_sku_lifecycle(
            "S",
            {
                "lifecycleStatus": "phase_out",
                "effectiveFrom": "2026-01-01",
                "effectiveTo": "2026-09-30",
                "rowVersion": single.row_version,
            },
            session=db,
            user=SimpleNamespace(name="test", role="admin"),
        )

    assert exc.value.status_code == 409
    assert exc.value.detail["code"] == "template_lifecycle_fob_period_conflict"
    assert exc.value.detail["preview"]["affectedPeriods"][0]["countryCode"] == "CH"


def test_template_country_fob_periods_reject_overlap(db):
    repo.save_country_template_fob_period(
        db,
        country_code="CH",
        bom_template="T**001",
        valid_from=date(2026, 9, 1),
        valid_to=date(2026, 9, 15),
        base_fob_eur=1000,
        remark=None,
        changed_by="test",
    )
    with pytest.raises(ValueError, match="cannot overlap"):
        repo.save_country_template_fob_period(
            db,
            country_code="CH",
            bom_template="T**001",
            valid_from=date(2026, 9, 15),
            valid_to=date(2026, 9, 30),
            base_fob_eur=1200,
            remark=None,
            changed_by="test",
        )


def test_template_country_fob_period_update_keeps_original_scope(db, monkeypatch):
    period = repo.save_country_template_fob_period(
        db,
        country_code="CH",
        bom_template="T**001",
        valid_from=date(2026, 10, 1),
        valid_to=date(2026, 10, 31),
        base_fob_eur=1000,
        remark=None,
        changed_by="test",
    )
    db.commit()
    calls = []
    monkeypatch.setattr(
        routes,
        "validate_country_access",
        lambda _session, _name, _role, country: calls.append(country),
    )

    with pytest.raises(routes.HTTPException) as exc:
        routes.put_bom_template_fob_period(
            body={
                "periodId": str(period.country_template_fob_period_id),
                "rowVersion": period.row_version,
                "countryCode": "RO",
                "bomTemplate": "T**001",
                "validFrom": "2026-10-01",
                "validTo": "2026-10-31",
                "baseFobEur": 1100,
            },
            session=db,
            user=SimpleNamespace(name="tester", role="admin"),
        )

    assert exc.value.status_code == 400
    assert calls == ["RO", "CH"]
    db.refresh(period)
    assert period.country_code == "CH"
    assert float(period.base_fob_eur) == 1000


def test_conflicting_single_payment_terms_remain_ambiguous(db):
    sku(db, "S", "single")
    sku(db, "D", "dual")
    fob(db, "S", 1000, term="TT")
    fob(db, "S", 1200, term="LC90")
    dual = fob(db, "D", 1000, base=1000, source="manual_edit")
    audit = repo.audit_colour_surcharge_reprice(db)
    assert audit["summary"]["ambiguousBase"] == 1
    assert audit["duplicateCountryPrices"][0]["status"] == "conflict"
    assert repo.reprice_sku_colour_surcharge_fobs(db, "D")["skippedAmbiguous"] == 1
    assert dual.final_fob_eur == 1000
    with pytest.raises(repo.CountryFobConflict):
        repo.get_fob_for_country_sku(db, "CH", "S")
    fob_map, conflicts = repo.list_fobs_for_country_material_codes(
        db, "CH", ["S"], include_conflicts=True,
    )
    assert fob_map == {}
    assert conflicts[0]["materialCode"] == "S"


def test_no_single_requires_consistent_template_base_evidence(db):
    sku(db, "D", "dual")
    row = fob(db, "D", 1300, base=1000, surcharge=300, source="manual_edit")
    assert repo.audit_colour_surcharge_reprice(db)["summary"]["missingBase"] == 1
    row.fob_source_mode = "template_base"
    assert repo.audit_colour_surcharge_reprice(db)["summary"]["alreadyCorrect"] == 1
    sku(db, "E", "special")
    fob(db, "E", 1500, base=1200)
    assert repo.audit_colour_surcharge_reprice(db)["summary"]["ambiguousBase"] == 2


@pytest.mark.parametrize("name", ["Matte black", "Black & White"])
def test_explicit_single_name_never_changes_pricing_tier(db, name):
    matte = sku(db, "CP", "single", "OMODA", "BLACK**001", "OMODA9 SHS", name)
    fob(db, "CP", 26500, base=26500)
    repo.upsert_special_colour_surcharge(db, "OMODA", "CP", 300, model_name="OMODA9 SHS")
    repo.reprice_sku_colour_surcharge_fobs(db, matte.material_code)
    assert repo.get_fob_for_country_sku(db, "CH", "CP").final_fob_eur == 26500


def test_missing_rule_blocks_create_but_explicit_zero_is_valid(db):
    sku(db, "S", "single", brand="OTHER")
    sku(db, "D", "dual", brand="OTHER")
    fob(db, "S", 1000)
    assert repo.initialize_sku_fobs_from_source(db, "D", "S")["skippedMissingRule"] == 1
    assert repo.get_fob_for_country_sku(db, "CH", "D") is None
    db.add(models.BrandColourSurchargeRule(brand="OTHER", colour_type="dual", surcharge_eur=0))
    assert repo.initialize_sku_fobs_from_source(db, "D", "S")["created"] == 1
    assert repo.get_fob_for_country_sku(db, "CH", "D").final_fob_eur == 1000


def test_colour_tier_patch_requires_explicit_tier(db):
    with pytest.raises(routes.HTTPException) as error:
        routes.patch_sku_colour_tier("D", {}, db, SimpleNamespace(name="test", role="admin"))
    assert error.value.status_code == 400
    assert error.value.detail == "colourTier is required"


def test_publish_preserves_single_matte_despite_higher_imported_fob(db):
    db.add(models.CountryPaymentTermMaster(country_code="CH", country_name="Switzerland",
        payment_term_code="TT", payment_method="TT", lc_days=0))
    parsed = {"rows": [
        {"material_code": "CP", "bom_template": "BLACK**001", "brand": "OMODA",
         "model_name": "OMODA9 SHS", "version": "Premium", "colour_tier": "single",
         "exterior_color_type": "single", "exterior_color_name": "Matte black (Black Edition)",
         "exterior_color_code": "CP", "country_fobs": {"Switzerland": 26500}},
        {"material_code": "BW", "bom_template": "REGULAR**001", "brand": "OMODA",
         "model_name": "OMODA9 SHS", "version": "Premium", "colour_tier": "single",
         "exterior_color_type": "single", "exterior_color_name": "White",
         "exterior_color_code": "BW", "country_fobs": {"Switzerland": 26000}},
    ]}
    service.publish_baseline(db, parsed, "test-import", None, "test")
    db.commit()
    assert repo.get_sku_by_material_code(db, "CP").colour_tier == "single"
    assert repo.get_fob_for_country_sku(db, "CH", "CP").final_fob_eur == 26500


def test_rule_priority_tier_and_zero_with_real_records(db):
    car = sku(db, "UE", "special", "OMODA", model="OMODA9 SHS", name="Matte gray")
    fob(db, "UE", 10000, 10000)
    repo.upsert_special_colour_surcharge(db, "OMODA", "UE", 250)
    repo.upsert_special_colour_surcharge(db, "OMODA", "UE", 300, model_name="OMODA9 SHS")
    assert repo.resolve_colour_surcharge_for_sku(db, car, "special")["amount"] == 300
    assert repo.resolve_colour_surcharge_for_sku(db, car, "dual")["amount"] == 200
    assert repo.resolve_colour_surcharge_for_sku(db, car, "single")["amount"] == 0
    repo.upsert_special_colour_surcharge(db, "OMODA", "UE", 0, model_name="OMODA9 SHS")
    assert repo.resolve_colour_surcharge_for_sku(db, car, "special")["status"] == "explicit_zero"


def test_apply_rejects_changed_rule_and_route_rolls_back(db):
    sku(db, "S", "single")
    sku(db, "D", "dual")
    fob(db, "S", 1000)
    fob(db, "D", 1000)
    db.commit()
    audit = repo.audit_colour_surcharge_reprice(db)
    rule = repo.get_brand_colour_surcharge(db, "JAECOO", "dual")
    rule.surcharge_eur = 400
    with pytest.raises(routes.HTTPException) as error:
        routes.apply_colour_surcharge_reprice({"previewFingerprint": audit["fingerprint"]}, db, SimpleNamespace(name="test", role="admin"))
    assert error.value.status_code == 409
    assert repo.get_fob_for_country_sku(db, "CH", "D").final_fob_eur == 1000
    assert repo.get_brand_colour_surcharge(db, "JAECOO", "dual").surcharge_eur == 300


@pytest.mark.parametrize("overwrite", [False, True])
def test_country_copy_uses_target_base_or_replaces_whole_base(db, overwrite):
    sku(db, "S", "single")
    sku(db, "D", "dual")
    fob(db, "S", 1000, country="CZ")
    fob(db, "D", 1000, country="CZ", source="uploaded_final_fob")
    fob(db, "S", 2000, country="CH")
    result = repo.copy_country_fobs(db, "CZ", "CH", overwrite_existing=overwrite)
    db.commit()
    expected_base = 1000 if overwrite else 2000
    assert repo.get_fob_for_country_sku(db, "CH", "D").final_fob_eur == expected_base + 300
    assert repo.get_fob_for_country_sku(db, "CH", "S").final_fob_eur == expected_base
    assert result["repriced"] == 1


def test_country_adjust_skips_conflicting_single_and_derived_rows(db):
    sku(db, "S", "single")
    sku(db, "D", "dual")
    fob(db, "S", 1000)
    fob(db, "S", 1200, term="LC90")
    fob(db, "D", 1300, 1000, 300, source="manual_edit")
    result = repo.adjust_country_fobs(db, "CH", 200)
    assert result["skippedAmbiguous"] == 3
    assert result["adjusted"] == 0
    assert db.scalars(select(models.FobResolvedHistory)).all() == []


def test_equal_payment_terms_do_not_choose_or_adjust_different_prices(db):
    sku(db, "S", "single")
    sku(db, "D", "dual")
    fob(db, "S", 1000, term="TT")
    fob(db, "S", 1000, term="LC90")
    fob(db, "D", 1000, term="TT")
    fob(db, "D", 1000, term="LC90")
    db.commit()
    audit = repo.audit_colour_surcharge_reprice(db)
    result = repo.apply_colour_surcharge_reprice_audit(db, audit["fingerprint"])
    assert result["totals"]["requested"] == 1
    assert result["totals"]["updated"] == 2
    assert repo.get_fob_for_country_sku(db, "CH", "D", "TT").final_fob_eur == 1300
    assert repo.get_fob_for_country_sku(db, "CH", "D", "LC90").final_fob_eur == 1300


def test_upsert_bom_price_normalizes_all_payment_term_rows(db):
    sku(db, "D", "dual")
    fob(db, "D", 1300, base=1000, surcharge=300, term="TT")
    fob(db, "D", 1500, base=1200, surcharge=300, term="LC90")
    replacement = models.CountrySkuFobResolved(
        baseline_version_id=db.info["baseline"],
        material_code="D", country_code="CH", payment_term_code="TT",
        base_fob_eur=1100, uploaded_fob_eur=1100,
        colour_surcharge_eur=300, final_fob_eur=1400,
        fob_source_mode="template_base",
    )
    repo.upsert_fob_resolved(db, replacement)
    db.flush()
    rows = db.scalars(select(models.CountrySkuFobResolved).where(
        models.CountrySkuFobResolved.material_code == "D",
        models.CountrySkuFobResolved.country_code == "CH",
    )).all()
    assert {(row.payment_term_code, float(row.base_fob_eur), float(row.final_fob_eur)) for row in rows} == {
        ("TT", 1100, 1400), ("LC90", 1100, 1400),
    }


def test_bom_and_matrix_keep_normal_rows_when_one_country_group_conflicts(db):
    sku(db, "BAD", "single", template="BAD**001")
    sku(db, "OK", "single", template="OK**001")
    fob(db, "BAD", 1000, term="TT")
    fob(db, "BAD", 1200, term="LC90")
    fob(db, "OK", 900)
    items, _countries, conflicts = repo.list_bom_with_fob(db, include_conflicts=True)
    bad = next(item for item in items if item["materialCode"] == "BAD")
    good = next(item for item in items if item["materialCode"] == "OK")
    assert bad["fobByCountry"]["CH"]["status"] == "conflict"
    assert good["fobByCountry"]["CH"]["finalFobEur"] == 900
    assert conflicts[0]["materialCode"] == "BAD"

    matrix = service.build_matrix(db, "CH", 2026)
    matrix_rows = {row["materialCode"]: row for row in matrix["rows"]}
    assert matrix_rows["BAD"]["fobEur"] is None
    assert matrix_rows["BAD"]["fobConflict"]["status"] == "conflict"
    assert matrix_rows["OK"]["fobEur"] == 900
    with pytest.raises(ValueError, match="Export blocked"):
        service.export_matrix(db, "CH", 2026)


def test_copy_country_conflict_isolated_to_material_country_group(db):
    sku(db, "BAD", "single", template="BAD**001")
    sku(db, "OK", "single", template="OK**001")
    fob(db, "BAD", 1000, country="CZ", term="TT")
    fob(db, "BAD", 1200, country="CZ", term="LC90")
    fob(db, "OK", 1000, country="CZ")

    result = repo.copy_country_fobs(db, "CZ", "CH")
    db.flush()

    assert result["skippedAmbiguous"] == 1
    assert repo.get_fob_for_country_sku(db, "CH", "BAD") is None
    assert repo.get_fob_for_country_sku(db, "CH", "OK").final_fob_eur == 1000


def test_copy_country_rederives_conflicting_colour_outputs_from_single_base(db):
    sku(db, "S", "single", template="T**001")
    sku(db, "D", "dual", template="T**001")
    fob(db, "S", 1000, country="CZ")
    fob(db, "D", 1200, base=1000, surcharge=200, country="CZ", term="TT")
    fob(db, "D", 1400, base=1100, surcharge=300, country="CZ", term="LC90")

    result = repo.copy_country_fobs(db, "CZ", "CH")
    db.flush()

    assert result["skippedAmbiguous"] == 0
    assert repo.get_fob_for_country_sku(db, "CH", "S").final_fob_eur == 1000
    copied_dual = repo.get_fob_for_country_sku(db, "CH", "D")
    assert copied_dual.base_fob_eur == 1000
    assert copied_dual.colour_surcharge_eur == 300
    assert copied_dual.final_fob_eur == 1300


def test_publish_requires_explicit_colour_tier(db):
    parsed = {"rows": [{
        "material_code": "NO-TIER",
        "bom_template": "NO-TIER**001",
        "brand": "OMODA",
        "model_name": "OMODA5",
        "version": "Premium",
        "exterior_color_type": "",
        "colour_tier": "",
        "exterior_color_name": "Unknown",
        "exterior_color_code": "XX",
        "country_fobs": {},
    }]}
    with pytest.raises(ValueError, match="Colour tier is required"):
        service.publish_baseline(db, parsed, "missing-tier", None, "test")


def test_country_fob_fallback_is_country_only_and_rejects_conflicting_sources(db):
    db.add_all([
        models.CountryFobSourceMapping(
            target_country_code="RO",
            target_payment_term_code="TT",
            source_country_code="HR",
            is_active=True,
        ),
        models.CountryFobSourceMapping(
            target_country_code="RO",
            target_payment_term_code="LC90",
            source_country_code="HR",
            is_active=True,
        ),
    ])
    db.flush()

    assert repo.get_country_fob_source_mapping(db, "ro", "LC90") == "HR"

    db.add(models.CountryFobSourceMapping(
        target_country_code="RO",
        target_payment_term_code="LC180",
        source_country_code="CZ",
        is_active=True,
    ))
    db.flush()
    with pytest.raises(ValueError, match="Conflicting FOB source mappings"):
        repo.get_country_fob_source_mapping(db, "RO", "TT")
