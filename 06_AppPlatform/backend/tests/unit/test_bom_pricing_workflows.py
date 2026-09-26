"""Real SQLAlchemy transactions: exercise public pricing paths, not mock prices."""
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
from app.services.material_master_parser import _detect_colour_tier
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
              models.OrderQuantityCell]
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
    applied = routes.apply_colour_surcharge_reprice({"previewFingerprint": audit["fingerprint"]}, db, SimpleNamespace(name="test"))
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


def test_explicit_single_matte_never_gets_special_override(db):
    matte = sku(db, "CP", "single", "OMODA", "BLACK**001", "OMODA9 SHS", "Matte black")
    assert _detect_colour_tier("Matte black", "single", "Black Edition") == "single"
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
        routes.patch_sku_colour_tier("D", {}, db, SimpleNamespace(name="test"))
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
        routes.apply_colour_surcharge_reprice({"previewFingerprint": audit["fingerprint"]}, db, SimpleNamespace(name="test"))
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
