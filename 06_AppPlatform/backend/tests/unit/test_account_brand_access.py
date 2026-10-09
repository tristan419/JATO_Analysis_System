"""Country × saved-brand permissions, using the existing ordering DB fixture."""
from contextlib import contextmanager
import importlib.util
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest
from fastapi import HTTPException
from sqlalchemy import text

from app.api.routes import auth, order_genius as ordering, order_genius_vehicle_allocation as allocation
from app.core import security
from app.core.security import UserContext, ordering_brands, ordering_countries, require_roles, validate_brand_access
from app.db import models, session as db_session
from app.infra import order_genius_repository as materials
from app.infra import order_genius_vehicle_repository as vehicles
from app.services import auth_service, order_genius_vehicle_service as pi_service
from test_order_genius_vehicle_allocation import vehicle_db, _colour_sku, _vin_fill_pi


def account(db, name="filler", role="order_filler", brands=None, countries=None):
    user = models.User(username=name, password_hash="test", role=role, is_active=True,
                       primary_country_code="CH", secondary_country_codes=countries or [],
                       brands=brands if brands is not None else ["OMODA"])
    db.add(user)
    db.commit()
    return user, UserContext(name=name, role=role)


def mixed_pi(db):
    pi, cars = _vin_fill_pi(db, materials=("OLD-OMODA", "OLD-JAECOO"), quantity=2)
    header = vehicles.get_header_by_code(db, pi)
    lines = vehicles.list_lines_by_pi(db, header.pi_id)
    for line, brand in zip(lines, ("OMODA", "JAECOO")):
        line.brand = brand
        for car in cars:
            if car.pi_line_id == line.pi_line_id:
                car.brand = brand
    db.commit()
    return pi, cars


def test_migration_backfills_only_existing_fillers_and_new_users_stay_empty(vehicle_db, monkeypatch):
    filler, _ = account(vehicle_db, brands=[])
    editor, _ = account(vehicle_db, "editor", "editor", [])
    admin, _ = account(vehicle_db, "admin", "admin", [])
    path = Path(__file__).parents[2] / "alembic/versions/20261009_0055_user_brands.py"
    spec = importlib.util.spec_from_file_location("brand_migration", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    added = []
    def execute(sql):
        sql = sql.replace("auth.users", "users")
        if vehicle_db.bind.dialect.name == "sqlite":
            sql = sql.replace("::jsonb", "")
        vehicle_db.execute(text(sql))
    monkeypatch.setattr(migration, "op", SimpleNamespace(
        add_column=lambda table, column, **kwargs: added.append((table, column.name)), execute=execute,
    ))
    migration.upgrade()
    vehicle_db.commit()
    vehicle_db.expire_all()
    assert filler.brands == ["OMODA", "JAECOO"]
    assert editor.brands == admin.brands == []
    assert filler.primary_country_code == "CH" and filler.role == "order_filler"
    new = models.User(username="new", password_hash="test", role="order_filler", is_active=True)
    vehicle_db.add(new)
    vehicle_db.flush()
    assert new.brands == []
    assert added == [("users", "brands"), ("role_upgrade_requests", "requested_brands")]


@pytest.mark.parametrize("role", ["admin", "developer"])
def test_admin_level_ignores_saved_country_and_brand_assignments(vehicle_db, role):
    _, user = account(vehicle_db, role, role, [])
    assert ordering_brands(vehicle_db, user) is None
    assert ordering_countries(vehicle_db, user) is None
    assert require_roles("editor", "admin")(user) == user


def test_viewer_needs_no_brands_but_cannot_use_order_write_dependency(vehicle_db):
    _, user = account(vehicle_db, "viewer", "viewer", [])
    assert ordering_brands(vehicle_db, user) is None
    with pytest.raises(HTTPException) as denied:
        require_roles("order_filler", "editor", "admin")(user)
    assert denied.value.status_code == 403


@pytest.mark.parametrize("role", ["order_filler", "editor"])
def test_empty_brands_do_not_mean_all_and_parameters_cannot_expand_scope(vehicle_db, role):
    _, user = account(vehicle_db, role, role, [])
    mixed_pi(vehicle_db)
    rows, total = vehicles.list_vehicles(vehicle_db, brand="OMODA", allowed_countries={"CH"},
                                        allowed_brands=ordering_brands(vehicle_db, user))
    assert rows == [] and total == 0
    headers, count = vehicles.list_headers(vehicle_db, allowed_countries={"CH"}, allowed_brands=set())
    assert headers == [] and count == 0
    with pytest.raises(HTTPException):
        validate_brand_access(vehicle_db, user, "OMODA")


def test_mixed_pi_projection_summary_months_and_export_use_saved_brands(vehicle_db):
    _, user = account(vehicle_db)
    pi, cars = mixed_pi(vehicle_db)
    # A new master with the same code cannot change the brand of the historical order.
    _colour_sku(vehicle_db, "OLD-OMODA", brand="JAECOO")
    vehicle_db.commit()
    detail = allocation.get_pi_order(pi, vehicle_db, user)
    assert len(detail["lines"]) == 1 and detail["vehicleTotal"] == 2
    assert detail["summary"]["totalUnits"] == 2 and detail["canDelete"] is False
    rows, total = vehicles.list_vehicles(vehicle_db, allowed_countries={"CH"}, allowed_brands={"OMODA"}, page_size=1)
    assert len(rows) == 1 and total == 2
    assert vehicles.pi_month_summary(vehicle_db, 2026, allowed_countries={"CH"}, allowed_brands={"OMODA"})["items"][0]["vehicleCount"] == 2
    with pytest.raises(HTTPException) as denied:
        allocation.export_vehicle_allocation({"carCodes": [cars[-1].car_code]}, vehicle_db, user)
    assert denied.value.status_code == 403


def test_mixed_batch_write_and_whole_pi_delete_reject_before_any_write(vehicle_db):
    _, user = account(vehicle_db, "editor", "editor")
    pi, cars = mixed_pi(vehicle_db)
    before = [(car.car_code, car.row_version, car.remark) for car in cars]
    body = {"piCode": pi, "carCodes": [car.car_code for car in cars], "rowVersions": {car.car_code: car.row_version for car in cars},
            "fields": {"remark": "must not save", "freightEur": 100}}
    with pytest.raises(HTTPException) as denied:
        allocation.bulk_update_vehicles(body, vehicle_db, user)
    assert denied.value.status_code == 403
    with pytest.raises(HTTPException):
        allocation.delete_pi_order(pi, vehicle_db, user)
    assert [(car.car_code, car.row_version, car.remark) for car in cars] == before
    assert vehicles.get_header_by_code(vehicle_db, pi) is not None
    stored = vehicle_db.query(models.User).filter_by(username="editor").one()
    stored.brands = ["OMODA", "JAECOO"]
    vehicle_db.commit()
    assert allocation.get_pi_order(pi, vehicle_db, user)["canDelete"] is True
    allocation.delete_pi_order(pi, vehicle_db, user)
    assert vehicles.get_header_by_code(vehicle_db, pi) is None


def test_line_write_checks_actual_vehicle_brand_not_only_line_brand(vehicle_db):
    _, user = account(vehicle_db, "editor", "editor")
    pi, cars = mixed_pi(vehicle_db)
    cars[0].brand = "CHERY"
    vehicle_db.commit()
    detail = pi_service.get_pi_detail(vehicle_db, pi)
    with pytest.raises(HTTPException) as denied:
        allocation._validate_line_countries(vehicle_db, user, detail, cars[0].pi_line_code)
    assert denied.value.status_code == 403


@pytest.mark.parametrize("role,allowed", [("viewer", True), ("editor", True), ("order_filler", False)])
def test_self_country_changes_and_brand_escalation(vehicle_db, role, allowed):
    stored, user = account(vehicle_db, role, role)
    body = auth.UserProfileBody(primaryCountry="SE")
    if allowed:
        auth.update_my_profile(body, vehicle_db, user)
        assert stored.primary_country_code == "SE"
    else:
        with pytest.raises(HTTPException):
            auth.update_my_profile(body, vehicle_db, user)
        assert stored.primary_country_code == "CH"
    with pytest.raises(HTTPException):
        auth.update_my_profile(auth.UserProfileBody(brands=["CHERY"]), vehicle_db, user)
    assert stored.brands == ["OMODA"]


def test_brand_application_reuses_approval_without_role_upgrade(vehicle_db):
    stored, user = account(vehicle_db, brands=[])
    request = auth.request_role_upgrade(auth.RoleUpgradeRequestBody(requestedBrands=["omoda", "jaecoo"],
                                       reason="Responsible for CH vehicle deliveries"), vehicle_db, user)
    with pytest.raises(HTTPException) as duplicate:
        auth.request_role_upgrade(auth.RoleUpgradeRequestBody(requestedBrands=["OMODA"], reason="again"), vehicle_db, user)
    assert duplicate.value.status_code == 409
    reviewed = auth.review_role_upgrade(UUID(request["requestId"]), auth.ReviewUpgradeBody(status="approved", brands=["JAECOO"]),
                                       vehicle_db, UserContext(name="admin", role="admin"))
    assert reviewed["brands"] == ["JAECOO"] and stored.role == "order_filler"
    assert stored.brands == ["JAECOO"]


def test_old_token_uses_current_db_role_active_status_and_scope(vehicle_db, monkeypatch):
    stored, _ = account(vehicle_db, "changed", "admin")
    token = auth_service.session_store.create("changed", "admin")
    @contextmanager
    def context():
        yield vehicle_db
    monkeypatch.setattr(db_session, "get_session_factory", lambda: context)
    stored.role, stored.brands = "order_filler", []
    vehicle_db.commit()
    current = auth_service.session_store.lookup(token)
    assert current.role == "order_filler"
    assert ordering_brands(vehicle_db, UserContext(current.role, current.username)) == set()
    stored.is_active = False
    vehicle_db.commit()
    assert auth_service.session_store.lookup(token) is None


def test_required_auth_does_not_accept_legacy_static_role_tokens(monkeypatch):
    monkeypatch.setattr(security, "AUTH_REQUIRED", True)
    monkeypatch.setattr(security, "TOKEN_ROLE_MAP", {"legacy": "admin"})
    monkeypatch.setattr(security.session_store, "lookup", lambda token: None)
    with pytest.raises(HTTPException) as denied:
        security.get_authenticated_user("legacy", "admin")
    assert denied.value.status_code == 401


def test_editor_shared_colour_scope_is_brand_not_country_and_keeps_pi_prices(vehicle_db):
    _, user = account(vehicle_db, "editor", "editor")
    sku = _colour_sku(vehicle_db, "OLD-OMODA")
    pi, cars = _vin_fill_pi(vehicle_db, materials=(sku.material_code,), quantity=2)
    for car in cars:
        car.brand, car.exterior_color_code, car.country_code = "OMODA", "BX", "SE"
    vehicle_db.commit()
    before = [(car.material_code, car.exterior_color_name, car.bom) for car in cars]
    ordering.set_colour_hex_rule_standard({"brand": "OMODA", "colourCode": "BX", "colourName": "Reviewed white", "colourHex": "#112233"}, vehicle_db, user)
    assert sku.colour_hex == "#112233"
    assert pi_service.get_pi_detail(vehicle_db, pi)["vehicles"][0]["colourHex"] == "#112233"
    assert [(car.material_code, car.exterior_color_name, car.bom) for car in cars] == before
    with pytest.raises(HTTPException):
        ordering.set_colour_hex_rule_standard({"brand": "JAECOO", "colourCode": "BX", "colourName": "White"}, vehicle_db, user)


def test_revoked_brand_cannot_apply_a_previously_valid_colour_preview(vehicle_db):
    stored, user = account(vehicle_db, "editor", "editor")
    sku = _colour_sku(vehicle_db, "OLD-OMODA")
    vehicle_db.commit()
    preview = materials.preview_colour_rule_fills(vehicle_db, allowed_brands={"OMODA"})
    stored.brands = []
    vehicle_db.commit()
    with pytest.raises(HTTPException):
        ordering.apply_colour_rule_fills({"materialCodes": [sku.material_code], "previewFingerprint": preview["fingerprint"]}, vehicle_db, user)
    assert sku.colour_hex is None


def test_colour_reads_do_not_authorize_missing_brand_by_model_name(vehicle_db):
    _, user = account(vehicle_db, "editor", "editor")
    sku = _colour_sku(vehicle_db, "UNKNOWN-BRAND", brand="OMODA")
    sku.brand = ""
    vehicle_db.commit()
    assert ordering.list_colour_hex_rules(vehicle_db, user)["items"] == []
    lookup = ordering.lookup_colour_rule("OMODA", "BX", None, vehicle_db, user)
    assert lookup["source"] == "none" and lookup["colourHex"] is None
    with pytest.raises(HTTPException):
        ordering.get_sku_fob(sku.material_code, "SE", vehicle_db, user)


def test_country_fob_read_rejects_other_country_even_for_assigned_brand(vehicle_db):
    _, user = account(vehicle_db, "editor", "editor")
    sku = _colour_sku(vehicle_db, "ALLOWED-OMODA")
    vehicle_db.commit()
    with pytest.raises(HTTPException) as denied:
        ordering.get_sku_fob(sku.material_code, "SE", vehicle_db, user)
    assert denied.value.status_code == 403


def test_editor_surcharge_reprice_uses_stored_brand_not_name_guess(vehicle_db):
    known = _colour_sku(vehicle_db, "ALLOWED-OMODA")
    unknown = _colour_sku(vehicle_db, "UNKNOWN-OMODA")
    unknown.brand = ""
    vehicle_db.commit()
    result = materials.reprice_brand_colour_surcharge_fobs(vehicle_db, "OMODA", "single",
              allowed_brands={"OMODA"}, allowed_countries={"CH"})
    assert result["skus"] == 1
    known.colour_tier = unknown.colour_tier = "special"
    vehicle_db.flush()
    result = materials.reprice_special_colour_surcharge_fobs(vehicle_db, "OMODA", "BX",
              allowed_brands={"OMODA"}, allowed_countries={"CH"})
    assert result["skus"] == 1
