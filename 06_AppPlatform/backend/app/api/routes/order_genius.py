"""API routes for Order Genius — Material Master upload, FOB matrix, export."""

from __future__ import annotations

import io
from uuid import UUID
import uuid as uuid_module
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.api.order_genius_schemas import (
    ColourSurchargeUpdate,
    ExportRequest,
    PublishBaselineRequest,
    QuantityCellUpdate,
    RemarkUpdate,
)
from app.core.config import PROJECT_ROOT
from app.core.security import require_min_role, require_roles, validate_country_access
from app.db.models import CountryPaymentTermMaster, PaymentTermAuditLog
from app.db.session import get_db_session
from app.infra import order_genius_repository as repo
from app.services import material_master_upload_service as upload_svc
from app.services.backup_utils import backup_ordering_schema
from app.services.order_genius_service import (
    apply_order_quantity_import,
    build_matrix,
    build_matrix_batch,
    build_options,
    export_matrix,
    export_pi_matrix,
    get_fob_for_sku,
    preview_order_quantity_import,
    update_quantity_cell,
    update_remark,
)
from app.services.ordering_normalization import (
    clean_text,
    infer_colour_tier,
    merge_colour_tiers,
    normalize_brand,
    normalize_brand_text,
)
from app.services.order_quantity_parser import parse_order_quantity_xlsx
from app.services.country_material_finance_import_service import (
    parse_country_material_finance_image,
    parse_country_material_finance_text,
    parse_country_material_finance_xlsx,
)

router = APIRouter(prefix="/order-genius", tags=["order_genius"])


def _body_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _selected_month_from_body(body: dict) -> int | None:
    selected_month_raw = body.get("selectedMonth", body.get("selected_month"))
    if selected_month_raw in (None, ""):
        return None
    try:
        selected_month = int(selected_month_raw)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="selectedMonth must be 1-12") from exc
    if selected_month < 1 or selected_month > 12:
        raise HTTPException(status_code=400, detail="selectedMonth must be 1-12")
    return selected_month


def _optional_float_from_body(body: dict, key: str) -> float | None:
    value = body.get(key)
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=f"{key} must be numeric") from exc
    if parsed < 0:
        raise HTTPException(status_code=400, detail=f"{key} must be non-negative")
    return parsed


def _export_filter_params(body: dict) -> dict:
    return {
        "brand": body.get("brand"),
        "model_name": body.get("model"),
        "powertrain": body.get("powertrain"),
        "version": body.get("version"),
        "colour": body.get("colour"),
        "material_code_search": body.get("materialCodeSearch") or body.get("material_code_search"),
        "selected_month": _selected_month_from_body(body),
        "hide_empty_rows": _body_bool(body.get("hideEmptyRows", body.get("hide_empty_rows", False))),
    }


def _export_filename_suffix(filters: dict) -> str:
    is_filtered = any([
        filters.get("brand"),
        filters.get("model_name"),
        filters.get("powertrain"),
        filters.get("version"),
        filters.get("colour"),
        filters.get("material_code_search"),
        filters.get("selected_month"),
        filters.get("hide_empty_rows"),
    ])
    suffix = "_filtered" if is_filtered else ""
    if filters.get("selected_month"):
        suffix += f"_M{filters['selected_month']:02d}"
    return suffix


# ── Upload helpers (filesystem-based, mirroring engineering_config) ───


UPLOAD_SESSION_DIR = PROJECT_ROOT / "04_Processed_data" / "ops" / "order_genius_uploads"


# ── Material Master Upload ────────────────────────────────────────────


@router.post("/material-master-uploads/initiate")
def initiate_material_master_upload(
    file_name: str = Query(),
    total_size: int = Query(ge=1),
    chunk_size: int = Query(default=5 * 1024 * 1024, ge=1024, le=50 * 1024 * 1024),
    _=Depends(require_min_role("editor")),
) -> dict:
    meta = upload_svc.initiate_upload(file_name, total_size, chunk_size)
    return meta


@router.get("/material-master-uploads/{upload_id}")
def get_material_master_upload_session(
    upload_id: str,
    _=Depends(require_min_role("editor")),
) -> dict:
    try:
        return upload_svc.get_session(upload_id)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.put("/material-master-uploads/{upload_id}/parts/{part_number}")
async def upload_material_master_chunk(
    upload_id: str,
    part_number: int,
    request: Request,
    _=Depends(require_min_role("editor")),
) -> dict:
    chunk_data = await request.body()
    try:
        return upload_svc.upload_chunk(upload_id, part_number, chunk_data)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e


@router.post("/material-master-uploads/{upload_id}/complete")
def complete_material_master_upload(
    upload_id: str,
    _=Depends(require_min_role("editor")),
) -> dict:
    try:
        return upload_svc.complete_upload(upload_id)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e


@router.post("/material-master-uploads/{upload_id}/parse")
def parse_material_master_upload(
    upload_id: str,
    _=Depends(require_min_role("editor")),
) -> dict:
    try:
        return upload_svc.parse_upload(upload_id)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e


@router.get("/material-master-uploads/{upload_id}/preview")
def preview_material_master_upload(
    upload_id: str,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict:
    try:
        return upload_svc.preview_upload(session, upload_id)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))


@router.post("/material-master-uploads/{upload_id}/publish")
def publish_material_master(
    upload_id: str,
    body: dict | None = None,
    session: Session = Depends(get_db_session),
    user=Depends(require_min_role("admin")),
) -> dict:
    try:
        backup_ordering_schema("publish_baseline")
        notes = body.get("notes") if body else None
        result = upload_svc.publish_upload(
            session, upload_id, user.name, notes
        )
        session.commit()
        return result
    except FileNotFoundError as e:
        session.rollback()
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        session.rollback()
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=str(e))


# ── Payment Terms & Colour Surcharges ─────────────────────────────────


@router.get("/payment-terms")
def list_payment_terms(
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict:
    rules = repo.list_payment_term_rules(session)
    return {
        "items": [
            {
                "paymentTermRuleId": str(r.payment_term_rule_id),
                "paymentTermCode": r.payment_term_code,
                "paymentMethod": r.payment_method,
                "lcDays": r.lc_days,
                "fobAdjustmentEur": float(r.fob_adjustment_eur),
                "adjustmentRate": (
                    float(r.adjustment_rate) if r.adjustment_rate else None
                ),
                "isActive": r.is_active,
            }
            for r in rules
        ],
    }


@router.get("/colour-surcharges")
def list_colour_surcharges(
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict:
    rules = repo.list_colour_surcharges(session)
    return {
        "items": [
            {
                "colourSurchargeRuleId": str(r.colour_surcharge_rule_id),
                "brand": r.brand,
                "colourType": r.colour_type,
                "surchargeEur": float(r.surcharge_eur),
                "isActive": r.is_active,
            }
            for r in rules
        ],
    }


@router.patch("/colour-surcharges")
def update_colour_surcharge(
    body: ColourSurchargeUpdate,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict:
    try:
        rule = repo.upsert_colour_surcharge(
            session,
            body.brand,
            body.colourType,
            body.surchargeEur,
        )
        session.commit()
        session.refresh(rule)
    except ValueError as exc:
        session.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except IntegrityError as exc:
        session.rollback()
        raise HTTPException(
            status_code=409,
            detail="Colour surcharge rule already exists",
        ) from exc
    return {
        "colourSurchargeRuleId": str(rule.colour_surcharge_rule_id),
        "brand": rule.brand,
        "colourType": rule.colour_type,
        "surchargeEur": float(rule.surcharge_eur),
        "isActive": rule.is_active,
    }


@router.get("/colour-hex-rules")
def list_colour_hex_rules(
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict:
    """Return derived colour swatch rules and conflicts from material SKUs."""
    return {"items": repo.list_colour_hex_rules(session)}


@router.patch("/colour-hex-rules/standard")
def set_colour_hex_rule_standard(
    body: dict,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict:
    """Apply one standard swatch to a brand/code/name colour rule."""
    try:
        result = repo.set_standard_colour_hex_for_rule(
            session,
            brand=str(body.get("brand") or ""),
            colour_code=str(body.get("colourCode", body.get("colour_code")) or ""),
            colour_name=str(body.get("colourName", body.get("colour_name")) or ""),
            colour_hex=str(body.get("colourHex", body.get("colour_hex")) or ""),
        )
    except ValueError as exc:
        session.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    session.commit()
    return result


@router.get("/countries")
def list_countries_with_payment_terms(
    session: Session = Depends(get_db_session),
    user=Depends(require_min_role("viewer")),
) -> dict:
    countries = repo.list_ordering_country_options(session)

    # order_filler users can only see their assigned countries
    if user.role == "order_filler":
        from app.db.models import User as UserModel
        db_user = session.query(UserModel).filter(UserModel.username == user.name).first()
        if db_user:
            allowed = {db_user.primary_country_code} if db_user.primary_country_code else set()
            for sc in (db_user.secondary_country_codes or []):
                allowed.add(sc)
            countries = [c for c in countries if c["countryCode"] in allowed]

    return {
        "items": [
            {
                "countryCode": c["countryCode"],
                "countryName": c["countryName"],
                "paymentTermCode": c["paymentTermCode"],
                "paymentMethod": c["paymentMethod"],
                "lcDays": c["lcDays"],
            }
            for c in countries
        ],
    }


@router.get("/account-country-options")
def list_account_country_options(
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict:
    """Return countries that can be assigned as account preferences."""
    return {"items": repo.list_ordering_country_options(session)}


@router.get("/fob-countries")
def list_order_genius_fob_countries(
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict:
    """Return countries that currently have active material FOB rows."""
    return {"countries": repo.list_active_fob_country_codes(session)}


# ── Order Genius Matrix ───────────────────────────────────────────────


@router.get("/options")
def get_order_genius_options(
    country: str = Query(),
    brand: str | None = Query(default=None),
    model: str | None = Query(default=None),
    powertrain: str | None = Query(default=None),
    version: str | None = Query(default=None),
    colour: str | None = Query(default=None),
    session: Session = Depends(get_db_session),
    user=Depends(require_min_role("viewer")),
) -> dict:
    validate_country_access(session, user.name, user.role, country)
    return build_options(
        session,
        country_code=country,
        brand=brand,
        model_name=model,
        powertrain=powertrain,
        version=version,
        colour=colour,
    )


@router.get("/matrix")
def get_order_genius_matrix(
    country: str = Query(),
    year: int = Query(),
    brand: str | None = Query(default=None),
    model: str | None = Query(default=None),
    powertrain: str | None = Query(default=None),
    version: str | None = Query(default=None),
    colour: str | None = Query(default=None),
    material_code_search: str | None = Query(default=None),
    session: Session = Depends(get_db_session),
    user=Depends(require_min_role("viewer")),
) -> dict:
    validate_country_access(session, user.name, user.role, country)
    return build_matrix(
        session,
        country_code=country,
        year=year,
        brand=brand,
        model_name=model,
        powertrain=powertrain,
        version=version,
        colour=colour,
        material_code_search=material_code_search,
    )


@router.post("/matrix/batch")
def get_order_genius_matrix_batch(
    body: dict,
    session: Session = Depends(get_db_session),
    user=Depends(require_min_role("viewer")),
) -> dict:
    countries_raw = body.get("countries")
    if not isinstance(countries_raw, list):
        raise HTTPException(status_code=400, detail="countries must be a list")

    countries: list[str] = []
    seen: set[str] = set()
    for value in countries_raw:
        country = str(value or "").strip().upper()
        if country and country not in seen:
            countries.append(country)
            seen.add(country)
    if not countries:
        raise HTTPException(status_code=400, detail="countries is required")
    if len(countries) > 80:
        raise HTTPException(status_code=400, detail="too many countries")

    try:
        year = int(body.get("year"))
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="year is required") from exc

    filters = {
        "brand": body.get("brand") or None,
        "model_name": body.get("model") or body.get("modelName") or None,
        "powertrain": body.get("powertrain") or None,
        "version": body.get("version") or None,
        "colour": body.get("colour") or None,
        "material_code_search": (
            body.get("materialCodeSearch")
            or body.get("material_code_search")
            or None
        ),
    }

    errors: dict[str, str] = {}
    valid_countries: list[str] = []
    for country in countries:
        try:
            validate_country_access(session, user.name, user.role, country)
            valid_countries.append(country)
        except HTTPException as exc:
            errors[country] = str(exc.detail)

    matrices = build_matrix_batch(
        session,
        country_codes=valid_countries,
        year=year,
        **filters,
    )
    return {"matrices": matrices, "errors": errors}


@router.patch("/quantity-cell")
def patch_quantity_cell(
    body: dict,
    session: Session = Depends(get_db_session),
    user=Depends(require_roles("editor", "admin", "order_filler")),
) -> dict:
    try:
        country_code = body.get("countryCode", body.get("country_code", ""))
        validate_country_access(session, user.name, user.role, country_code)
        result = update_quantity_cell(
            session,
            country_code=country_code,
            order_year=body.get("orderYear", body.get("order_year", 0)),
            order_month=body.get("orderMonth", body.get("order_month", 0)),
            material_code=body.get("materialCode", body.get("material_code", "")),
            quantity=body.get("quantity", 0),
            updated_by=user.name,
            expected_version=body.get("rowVersion", body.get("row_version", 1)),
        )
        session.commit()
        return result
    except ValueError as e:
        session.rollback()
        msg = str(e)
        if "Historical" in msg:
            raise HTTPException(status_code=400, detail=msg)
        if "Concurrent" in msg:
            raise HTTPException(status_code=409, detail=msg)
        raise HTTPException(status_code=400, detail=msg)


@router.patch("/material-skus/{material_code}/colour-hex")
def patch_sku_colour_hex(
    material_code: str,
    body: dict,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict:
    """Update the custom colour hex for a material SKU."""
    sku = repo.get_sku_by_material_code(session, material_code)
    if not sku:
        raise HTTPException(status_code=404, detail="Material code not found")
    try:
        colour_hex = repo.normalize_colour_hex_value(body.get("colourHex"))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    sku.colour_hex = colour_hex
    session.commit()
    return {"materialCode": sku.material_code, "colourHex": sku.colour_hex}


@router.patch("/material-skus/{material_code}/confirm-colour-code")
def confirm_colour_code(
    material_code: str,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict:
    """Mark a material SKU's colour code as confirmed (not auto-generated)."""
    sku = repo.get_sku_by_material_code(session, material_code)
    if not sku:
        raise HTTPException(status_code=404)
    sku.colour_code_confirmed = True
    session.commit()
    return {"materialCode": sku.material_code, "colourCodeConfirmed": True}


@router.patch("/material-skus/{material_code}/colour-code")
def patch_colour_code(
    material_code: str,
    body: dict,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict:
    """Update colour code and regenerate material code."""
    new_code = clean_text(body.get("colourCode")).upper()
    try:
        sku, old_material_code = repo.update_sku_colour_code(
            session,
            material_code,
            new_code,
            new_colour_name=body.get("colourName", body.get("colour_name")),
            new_colour_hex=body.get("colourHex", body.get("colour_hex")) if ("colourHex" in body or "colour_hex" in body) else None,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        detail = str(exc)
        status_code = 409 if "already exists" in detail.lower() else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc
    session.commit()
    return {
        "oldMaterialCode": old_material_code,
        "materialCode": sku.material_code,
        "colourCode": sku.exterior_color_code,
        "colour": sku.exterior_color_name,
        "colourHex": sku.colour_hex,
        "colourCodeConfirmed": sku.colour_code_confirmed,
    }


@router.patch("/material-skus/{material_code}/material-code")
def patch_material_code(
    material_code: str,
    body: dict,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict:
    """Update material code directly and regenerate if pattern contains **."""
    new_mc = body.get("materialCode", "").strip()
    if not new_mc:
        raise HTTPException(status_code=400, detail="materialCode is required")
    try:
        mapping = repo.update_bom_template_material_codes(
            session,
            [material_code],
            new_mc,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        detail = str(exc)
        status_code = 409 if "already exists" in detail.lower() else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc
    session.commit()
    return {
        "oldMaterialCode": material_code,
        "materialCode": mapping[clean_text(material_code).upper()],
        "bomTemplate": clean_text(new_mc).upper(),
    }


@router.patch("/bom-templates/material-code")
def patch_bom_template_material_code(
    body: dict,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict:
    material_codes_raw = body.get("materialCodes")
    material_codes = (
        [code for code in (clean_text(item).upper() for item in material_codes_raw) if code]
        if isinstance(material_codes_raw, list)
        else []
    )
    bom_template = clean_text(body.get("bomTemplate") or body.get("materialCode")).upper()
    if not material_codes:
        raise HTTPException(status_code=400, detail="materialCodes is required")
    if not bom_template:
        raise HTTPException(status_code=400, detail="bomTemplate is required")
    try:
        mapping = repo.update_bom_template_material_codes(
            session,
            material_codes,
            bom_template,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        detail = str(exc)
        status_code = 409 if "already exists" in detail.lower() else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc
    session.commit()
    return {
        "bomTemplate": bom_template,
        "materialCodes": list(mapping.values()),
        "updated": len(mapping),
    }


@router.patch("/material-skus/{material_code}/colour-tier")
def patch_sku_colour_tier(
    material_code: str,
    body: dict,
    session: Session = Depends(get_db_session),
    user=Depends(require_min_role("editor")),
) -> dict:
    colour_tier = body.get("colourTier", body.get("colour_tier", "single"))
    if colour_tier not in ("single", "dual", "special"):
        raise HTTPException(status_code=400, detail="colour_tier must be single, dual, or special")
    ok = repo.update_sku_colour_tier(session, material_code, colour_tier)
    if not ok:
        raise HTTPException(status_code=404, detail="SKU not found")
    session.commit()
    return {"materialCode": material_code, "colourTier": colour_tier}


@router.patch("/material-skus/{material_code}/interior")
def patch_sku_interior(
    material_code: str,
    body: dict,
    session: Session = Depends(get_db_session),
    user=Depends(require_min_role("editor")),
) -> dict:
    """Update interior fields for a material SKU."""
    from sqlalchemy import update as sa_update
    from app.db.models import MaterialSkuMaster

    vals = {}
    if "interiorColorName" in body:
        vals["interior_color_name"] = body["interiorColorName"] or None
    if "interiorColourCode" in body:
        vals["interior_colour_code"] = body["interiorColourCode"] or None
    if "interiorPackage" in body:
        vals["interior_package"] = body["interiorPackage"] or None
    if "editionTag" in body:
        vals["edition_tag"] = body["editionTag"] or None
    if not vals:
        raise HTTPException(status_code=400, detail="No interior fields provided")

    result = session.execute(
        sa_update(MaterialSkuMaster)
        .where(MaterialSkuMaster.material_code == material_code)
        .values(**vals)
    )
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="SKU not found")
    session.commit()
    return {"materialCode": material_code, "interiorColorName": vals.get("interior_color_name"),
            "interiorColourCode": vals.get("interior_colour_code"),
            "interiorPackage": vals.get("interior_package"),
            "editionTag": vals.get("edition_tag")}


@router.patch("/material-skus/{material_code}/remark")
def patch_sku_remark(
    material_code: str,
    body: dict,
    session: Session = Depends(get_db_session),
    user=Depends(require_min_role("editor")),
) -> dict:
    try:
        result = update_remark(
            session,
            material_code=material_code,
            remark=body.get("remark", ""),
            changed_by=user.name,
            expected_version=body.get("rowVersion", body.get("row_version", 1)),
        )
        session.commit()
        return result
    except ValueError as e:
        session.rollback()
        msg = str(e)
        if "Concurrent" in msg:
            raise HTTPException(status_code=409, detail=msg)
        raise HTTPException(status_code=400, detail=msg)


@router.get("/material-skus/{material_code}/fob")
def get_sku_fob(
    material_code: str,
    country: str = Query(),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict:
    fob = get_fob_for_sku(session, country, material_code)
    if not fob:
        raise HTTPException(status_code=404, detail="FOB not resolved for this SKU")
    return fob


@router.get("/country-material-finance")
def list_country_material_finance(
    country: str = Query(),
    brand: str | None = Query(default=None),
    model: str | None = Query(default=None),
    powertrain: str | None = Query(default=None),
    version: str | None = Query(default=None),
    material_code: list[str] | None = Query(default=None),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict:
    """Return country finance/CBU rows over BOM SKUs."""
    rows = repo.list_country_material_finance(
        session,
        country,
        material_codes=material_code,
        brand=brand,
        model_name=model,
        powertrain=powertrain,
        version=version,
    )
    return {"items": rows}


@router.get("/country-material-finance/options")
def list_country_material_finance_options(
    country: str = Query(default="NL"),
    brand: str | None = Query(default=None),
    model: str | None = Query(default=None),
    powertrain: str | None = Query(default=None),
    version: str | None = Query(default=None),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict:
    """Return CBU filter options from BOM templates, independent of country FOB coverage."""
    return repo.list_country_material_finance_options(
        session,
        country,
        brand=brand,
        model_name=model,
        powertrain=powertrain,
        version=version,
    )


@router.get("/country-material-finance/history")
def list_country_material_finance_history(
    country: str = Query(),
    material_code: str = Query(),
    limit: int = Query(default=50, ge=1, le=200),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict:
    """Return edit history for one BOM-template country finance row."""
    rows = repo.list_country_material_finance_history(
        session,
        country,
        material_code,
        limit=limit,
    )
    return {"items": rows}


@router.post("/country-material-finance/import-preview")
async def preview_country_material_finance_import(
    country: str = Form(...),
    text_body: str | None = Form(default=None, alias="text"),
    file: UploadFile | None = File(default=None),
    _=Depends(require_min_role("editor")),
) -> dict:
    """Parse CBU finance imports into preview rows without writing to DB."""
    country_code = clean_text(country).upper()
    if not country_code:
        raise HTTPException(status_code=400, detail="country is required")

    if file is not None:
        content = await file.read()
        file_name = file.filename or "upload"
        suffix = Path(file_name).suffix.lower()
        content_type = (file.content_type or "").lower()
        if suffix in {".xlsx", ".xlsm"}:
            return parse_country_material_finance_xlsx(
                content,
                country_code,
                file_name=file_name,
            )
        if content_type.startswith("image/") or suffix in {".png", ".jpg", ".jpeg", ".webp", ".bmp"}:
            return parse_country_material_finance_image(
                content,
                country_code,
                file_name=file_name,
                mime_type=content_type or "image/png",
            )
        if suffix in {".csv", ".tsv", ".txt"} or content_type.startswith("text/"):
            try:
                decoded = content.decode("utf-8-sig")
            except UnicodeDecodeError as exc:
                raise HTTPException(status_code=400, detail="Text file must be UTF-8 encoded") from exc
            return parse_country_material_finance_text(
                decoded,
                country_code,
                source_mode="uploaded",
                source_payload={"entryMode": "text_upload", "fileName": file_name},
            )
        raise HTTPException(status_code=400, detail="Supported files: xlsx, csv, tsv, txt, or image")

    if text_body and text_body.strip():
        return parse_country_material_finance_text(
            text_body,
            country_code,
            source_mode="uploaded",
            source_payload={"entryMode": "excel_paste"},
        )
    raise HTTPException(status_code=400, detail="file or text is required")


@router.get("/material-skus/{material_code}/country-finance")
def get_material_sku_country_finance(
    material_code: str,
    country: str = Query(),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict:
    """Return one material's country finance/CBU row."""
    rows = repo.list_country_material_finance(
        session,
        country,
        material_codes=[material_code],
    )
    if not rows:
        raise HTTPException(status_code=404, detail="Material finance row not found")
    return rows[0]


@router.patch("/material-skus/{material_code}/country-finance")
def patch_material_sku_country_finance(
    material_code: str,
    body: dict,
    session: Session = Depends(get_db_session),
    user=Depends(require_min_role("editor")),
) -> dict:
    """Create or update one material's country finance/CBU note without changing BOM FOB."""
    country = clean_text(body.get("countryCode") or body.get("country_code")).upper()
    if not country:
        raise HTTPException(status_code=400, detail="countryCode is required")

    field_map = {
        "fob_eur": "fobEur",
        "retail_price_eur": "retailPriceEur",
        "wholesale_price_eur": "wholesalePriceEur",
        "dealer_price_eur": "dealerPriceEur",
        "cost_eur": "costEur",
        "margin_eur": "marginEur",
        "margin_rate": "marginRate",
        "vehicle_margin_eur": "vehicleMarginEur",
        "vehicle_margin_rate": "vehicleMarginRate",
        "vehicle_profit_eur": "vehicleProfitEur",
        "vehicle_profit_rate": "vehicleProfitRate",
        "fob_delta_eur": "fobDeltaEur",
        "margin_delta_eur": "marginDeltaEur",
    }
    values: dict = {}
    for db_field, api_field in field_map.items():
        if api_field in body:
            values[db_field] = _optional_float_from_body(body, api_field)
    if "memo" in body:
        values["memo"] = clean_text(body.get("memo")) or None
    if "sourcePayload" in body:
        payload = body.get("sourcePayload")
        values["source_payload_json"] = payload if isinstance(payload, dict) else None
    values["source_mode"] = clean_text(body.get("sourceMode") or "manual")

    if len(values) == 1 and "source_mode" in values:
        raise HTTPException(status_code=400, detail="No finance fields provided")

    result = repo.upsert_country_material_finance(
        session,
        country,
        material_code,
        values,
        updated_by=user.name,
    )
    if result is None:
        raise HTTPException(status_code=404, detail="Material code not found")
    session.commit()
    return result


@router.patch("/material-skus/{material_code}/lifecycle")
def patch_sku_lifecycle(
    material_code: str,
    body: dict,
    session: Session = Depends(get_db_session),
    user=Depends(require_min_role("editor")),
) -> dict:
    """Update a material SKU's lifecycle status (active/historical/phase_out)."""
    result = repo.update_sku_lifecycle(
        session,
        material_code=material_code,
        lifecycle_status=body.get("lifecycleStatus", "active"),
        effective_from=body.get("effectiveFrom"),
        effective_to=body.get("effectiveTo"),
        expected_version=body.get("rowVersion", 1),
    )
    if result is None:
        sku = repo.get_sku_by_material_code_any_status(session, material_code)
        if not sku:
            raise HTTPException(status_code=404, detail="Material code not found")
        raise HTTPException(status_code=409, detail="Concurrent update conflict")
    session.commit()
    return {
        "materialCode": result.material_code,
        "lifecycleStatus": result.lifecycle_status,
        "rowVersion": result.row_version,
        "effectiveFrom": result.effective_from_month,
        "effectiveTo": result.effective_to_month,
    }


@router.delete("/material-skus/{material_code}")
def delete_material_sku(
    material_code: str,
    session: Session = Depends(get_db_session),
    user=Depends(require_min_role("admin")),
) -> dict:
    """Hard-delete a material SKU and its FOB records (admin only)."""
    existing_sku = repo.get_sku_by_material_code_any_status(session, material_code)
    bom_template = existing_sku.bom_template if existing_sku else None
    deleted = repo.delete_sku(session, material_code)
    if not deleted:
        raise HTTPException(status_code=404, detail="Material code not found")
    # Also clean up FOB and finance records
    from sqlalchemy import delete as sa_delete
    from app.db.models import CountryMaterialFinance, CountrySkuFobResolved
    session.execute(
        sa_delete(CountrySkuFobResolved).where(
            CountrySkuFobResolved.material_code == material_code
        )
    )
    session.execute(
        sa_delete(CountryMaterialFinance).where(
            CountryMaterialFinance.material_code == material_code
        )
    )
    deleted_template_finance = repo.delete_orphan_template_finance(session, bom_template)
    session.commit()
    return {
        "materialCode": material_code,
        "deleted": True,
        "deletedTemplateFinance": deleted_template_finance,
    }


@router.patch("/material-skus/{material_code}/fob")
def patch_sku_fob(
    material_code: str,
    body: dict,
    session: Session = Depends(get_db_session),
    user=Depends(require_min_role("editor")),
) -> dict:
    """Update or create FOB for a material code in a specific country. Send 0 or null to clear."""
    country = body.get("countryCode", "")
    fob_raw = body.get("finalFobEur")
    if not country:
        raise HTTPException(status_code=400, detail="countryCode is required")
    fob_val = None if fob_raw is None else float(fob_raw)
    if fob_val is not None and fob_val <= 0:
        fob_val = None
    pt_code = body.get("paymentTermCode")
    has_remark = "remark" in body
    remark = body.get("remark") if has_remark else None
    result = repo.update_sku_fob_for_country(
        session,
        material_code,
        country,
        fob_val,
        pt_code,
        remark=remark,
        update_remark=has_remark,
        changed_by=user.name,
    )
    if not result and fob_val is not None:
        raise HTTPException(status_code=404, detail="Could not update FOB")
    session.commit()
    return {
        "materialCode": result.material_code if result else material_code,
        "countryCode": result.country_code if result else country,
        "finalFobEur": float(result.final_fob_eur) if result and fob_val is not None else None,
        "paymentTermCode": result.payment_term_code if result else pt_code,
        "fobSourceMode": result.fob_source_mode if result else None,
        "fobSourceCountryCode": result.fob_source_country_code if result else None,
        "remark": result.remark if result else clean_text(remark) if has_remark else None,
    }


@router.post("/bom-templates/sync-fobs")
def sync_bom_template_fobs(
    body: dict,
    session: Session = Depends(get_db_session),
    user=Depends(require_min_role("editor")),
) -> dict:
    """Backfill missing concrete SKU FOB rows from sibling colours in a BOM template."""
    bom_template = clean_text(body.get("bomTemplate") or body.get("materialCode")).upper()
    material_codes_raw = body.get("materialCodes")
    material_codes = (
        [code for code in (clean_text(item).upper() for item in material_codes_raw) if code]
        if isinstance(material_codes_raw, list)
        else None
    )
    if not bom_template:
        raise HTTPException(status_code=400, detail="bomTemplate is required")
    try:
        result = repo.sync_missing_template_fobs(
            session,
            bom_template=bom_template,
            target_material_codes=material_codes,
            changed_by=user.name,
            reprice_existing_colour_surcharges=_body_bool(body.get("repriceExistingColourSurcharges")),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    session.commit()
    return result


@router.post("/countries/copy-fobs")
def copy_country_fobs(
    body: dict,
    session: Session = Depends(get_db_session),
    user=Depends(require_min_role("editor")),
) -> dict:
    """Copy active material FOB rows from one country to another."""
    source = clean_text(body.get("sourceCountryCode")).upper()
    target = clean_text(body.get("targetCountryCode")).upper()
    overwrite_existing = _body_bool(body.get("overwriteExisting"))
    if not source or not target:
        raise HTTPException(
            status_code=400,
            detail="sourceCountryCode and targetCountryCode are required",
        )
    if len(source) != 2 or len(target) != 2:
        raise HTTPException(status_code=400, detail="Country code must be 2 letters")
    if source == target:
        raise HTTPException(status_code=400, detail="Source and target countries must differ")

    result = repo.copy_country_fobs(
        session,
        source,
        target,
        overwrite_existing=overwrite_existing,
        changed_by=user.name,
    )
    if result["sourceRows"] == 0:
        raise HTTPException(status_code=404, detail=f"No active FOB rows found for {source}")
    session.commit()
    return result


@router.post("/countries/adjust-fobs")
def adjust_country_fobs(
    body: dict,
    session: Session = Depends(get_db_session),
    user=Depends(require_min_role("editor")),
) -> dict:
    """Apply a fixed EUR delta to all active material FOB rows for one country."""
    country = clean_text(body.get("countryCode")).upper()
    delta_raw = body.get("deltaEur")
    if not country:
        raise HTTPException(status_code=400, detail="countryCode is required")
    if len(country) != 2:
        raise HTTPException(status_code=400, detail="Country code must be 2 letters")
    try:
        delta = round(float(delta_raw), 2)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="deltaEur must be a number") from exc
    if delta == 0:
        raise HTTPException(status_code=400, detail="deltaEur must not be zero")

    result = repo.adjust_country_fobs(
        session,
        country,
        delta,
        changed_by=user.name,
    )
    if result["rows"] == 0:
        raise HTTPException(status_code=404, detail=f"No active FOB rows found for {country}")
    session.commit()
    return result


@router.post("/material-skus")
def create_material_sku(
    body: dict,
    session: Session = Depends(get_db_session),
    user=Depends(require_min_role("admin")),
) -> dict:
    """Create a new material SKU manually (BOM Admin)."""
    from uuid import uuid4
    from app.db.models import MaterialSkuMaster

    material_code = clean_text(body.get("materialCode")).upper()
    brand = normalize_brand(body.get("brand"))
    model_name = normalize_brand_text(body.get("modelName"))
    version = clean_text(body.get("version"))
    colour_code = clean_text(body.get("colourCode")).upper()
    colour = clean_text(body.get("colour")) or colour_code
    colour_type = clean_text(body.get("colourType")) or "single"
    requested_colour_tier = clean_text(body.get("colourTier")).lower()
    powertrain = clean_text(body.get("powertrain")) or "Other"
    bom_template = clean_text(body.get("bomTemplate")).upper() or material_code
    source_bom_template = clean_text(body.get("sourceBomTemplate")).upper()
    remark = clean_text(body.get("remark"))

    missing = [
        label
        for label, value in (
            ("materialCode", material_code),
            ("brand", brand),
            ("modelName", model_name),
            ("version", version),
            ("colourCode", colour_code),
        )
        if not value
    ]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing required fields: {', '.join(missing)}",
        )

    if repo.get_sku_by_material_code_any_status(session, material_code):
        raise HTTPException(status_code=409, detail=f"Material code already exists: {material_code}")

    baseline = repo.get_latest_baseline(session)
    if baseline is None:
        baseline = repo.create_baseline_version(
            session=session,
            source_file_name="manual_admin",
            source_file_hash=None,
            baseline_name="manual_admin",
            published_by=user.name,
        )
        session.flush()

    try:
        explicit_colour_hex = repo.normalize_colour_hex_value(body.get("colourHex"))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    colour_hex = explicit_colour_hex or repo.find_reusable_colour_hex(
        session,
        brand=brand,
        colour_code=colour_code,
        colour_name=colour,
    )

    sku = MaterialSkuMaster(
        material_sku_id=uuid4(),
        material_code=material_code,
        brand=brand,
        model_name=model_name,
        version=version,
        exterior_color_name=colour,
        exterior_color_code=colour_code,
        exterior_color_type=colour_type,
        bom_template=bom_template,
        powertrain=powertrain,
        colour_hex=colour_hex,
        colour_tier=merge_colour_tiers(
            requested_colour_tier if requested_colour_tier in {"single", "dual", "special"} else None,
            infer_colour_tier(
                colour,
                colour_type,
                body.get("editionTag", body.get("edition_tag")),
                colour_code,
                colour_hex,
            ),
        ),
        lifecycle_status="active",
        remark=remark or None,
        is_active=True,
        is_published=False,
        baseline_version_id=baseline.baseline_version_id,
    )
    session.add(sku)
    try:
        session.flush()
        copied_finance_rows = repo.copy_country_material_finance_template(
            session,
            source_bom_template,
            bom_template,
            updated_by=user.name,
        )
        copied_fob_rows = repo.sync_missing_template_fobs(
            session,
            bom_template=bom_template,
            target_material_codes=[material_code],
            changed_by=user.name,
        )
        session.commit()
    except IntegrityError as exc:
        session.rollback()
        raise HTTPException(
            status_code=409,
            detail=f"Could not create material SKU: {material_code}",
        ) from exc
    return {
        "materialCode": sku.material_code,
        "id": str(sku.material_sku_id),
        "copiedFinanceRows": copied_finance_rows,
        "copiedFobRows": copied_fob_rows,
    }


@router.patch("/material-skus/{material_code}/metadata")
def patch_sku_metadata(
    material_code: str,
    body: dict,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict:
    """Update shared product metadata for one or more material SKUs."""
    material_codes_raw = body.get("materialCodes")
    material_codes = (
        [code for code in (clean_text(item).upper() for item in material_codes_raw) if code]
        if isinstance(material_codes_raw, list)
        else []
    )
    base_code = clean_text(material_code).upper()
    if base_code and base_code not in material_codes:
        material_codes.insert(0, base_code)

    values = {
        "brand": body.get("brand") if "brand" in body else None,
        "model_name": body.get("modelName") if "modelName" in body else None,
        "version": clean_text(body.get("version")) if "version" in body else None,
        "powertrain": clean_text(body.get("powertrain")) if "powertrain" in body else None,
    }
    if all(value is None for value in values.values()):
        raise HTTPException(status_code=400, detail="No metadata fields provided")
    blank_fields = [
        field
        for field, value in (
            ("brand", values["brand"]),
            ("modelName", values["model_name"]),
            ("version", values["version"]),
            ("powertrain", values["powertrain"]),
        )
        if value is not None and not clean_text(value)
    ]
    if blank_fields:
        raise HTTPException(
            status_code=400,
            detail=f"Blank metadata fields: {', '.join(blank_fields)}",
        )

    updated = repo.update_sku_metadata(
        session,
        material_codes,
        brand=values["brand"],
        model_name=values["model_name"],
        version=values["version"],
        powertrain=values["powertrain"],
    )
    if updated == 0:
        raise HTTPException(status_code=404, detail="SKU not found")
    session.commit()
    return {"materialCodes": material_codes, "updated": updated}


@router.get("/bom-admin")
def get_bom_admin(
    brand: str | None = Query(default=None),
    search: str | None = Query(default=None),
    country: str | None = Query(default=None),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict:
    """Return BOM data with FOB per country, for the BOM admin panel."""
    items, countries = repo.list_bom_with_fob(session, brand=brand, search=search, country_code=country)
    return {
        "items": items,
        "countries": countries,
        "activeFobCountries": repo.list_active_fob_country_codes(session),
    }


@router.get("/material-skus-admin")
def list_material_skus_admin(
    country: str | None = Query(default=None),
    brand: str | None = Query(default=None),
    search: str | None = Query(default=None),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict:
    """List all SKUs for the BOM admin panel."""
    rows = repo.list_all_material_skus_for_admin(
        session, country_code=country, brand=brand, search=search,
    )
    return {
        "items": [
            {
                "materialCode": r.material_code,
                "brand": r.brand,
                "modelName": r.model_name,
                "version": r.version,
                "colour": r.exterior_color_name or "",
                "lifecycleStatus": r.lifecycle_status,
                "isActive": r.is_active,
                "effectiveFrom": r.effective_from_month,
                "effectiveTo": r.effective_to_month,
                "rowVersion": r.row_version,
            }
            for r in rows
        ],
    }


@router.get("/material-lifecycle")
def get_lifecycle(
    country: str,
    material_code: str = "",
    product_identity: str = "",
    session: Session = Depends(get_db_session),
) -> list[dict]:
    rows = []
    if product_identity:
        rows = repo.list_lifecycle_for_product(session, country, product_identity)
    elif material_code:
        rows = repo.get_material_lifecycle(session, country, material_code)
    return [
        {
            "lifecycleId": str(r.lifecycle_id),
            "countryCode": r.country_code,
            "materialCode": r.material_code,
            "productIdentity": r.product_identity,
            "validFrom": r.valid_from.isoformat() if r.valid_from else None,
            "validTo": r.valid_to.isoformat() if r.valid_to else None,
            "lifecycleStatus": r.lifecycle_status,
            "replacedByCode": r.replaced_by_code,
            "remark": r.remark,
        }
        for r in rows
    ]


# ── Payment Term Admin ──────────────────────────────────────────────


@router.get("/payment-terms/countries")
def list_country_payment_term_admin(
    session: Session = Depends(get_db_session),
    user=Depends(require_min_role("admin")),
) -> dict:
    """Return all payment term records (active + historical) for Admin panel."""
    rows = repo.list_all_payment_terms(session)
    return {
        "items": [
            {
                "id": str(r.country_payment_term_id),
                "countryCode": r.country_code,
                "countryName": r.country_name,
                "paymentTermCode": r.payment_term_code,
                "paymentMethod": r.payment_method,
                "lcDays": r.lc_days,
                "validFrom": r.valid_from_month,
                "validTo": r.valid_to_month,
                "isActive": r.is_active,
                "remark": r.remark,
            }
            for r in rows
        ]
    }


@router.post("/payment-terms/countries")
def create_payment_term(
    body: dict,
    session: Session = Depends(get_db_session),
    user=Depends(require_min_role("admin")),
) -> dict:
    """Create a new payment term for a country."""
    code = body["countryCode"]
    if _has_overlap(session, code, body.get("validFrom"), body.get("validTo")):
        raise HTTPException(status_code=409, detail="Overlap with existing active term")
    row = repo.create_payment_term(
        session,
        country_code=code,
        country_name=body.get("countryName", code),
        payment_term_code=body["paymentTermCode"],
        payment_method=body.get("paymentMethod", "LC"),
        lc_days=body.get("lcDays", 0),
        valid_from_month=body.get("validFrom"),
        valid_to_month=body.get("validTo"),
        remark=body.get("remark"),
    )
    session.commit()
    _audit(session, code, "create", actor=user.name, new_payment_term_code=row.payment_term_code,
           new_valid_from=row.valid_from_month, new_valid_to=row.valid_to_month)
    session.commit()
    return {"id": str(row.country_payment_term_id)}


@router.patch("/payment-terms/countries/{term_id}")
def update_payment_term(
    term_id: str,
    body: dict,
    session: Session = Depends(get_db_session),
    user=Depends(require_min_role("admin")),
) -> dict:
    """Update a payment term. If modifying historical months, returns impact."""
    row = session.get(CountryPaymentTermMaster, UUID(term_id))
    if not row:
        raise HTTPException(status_code=404)

    is_correction = body.get("correction", False)
    old_pt = row.payment_term_code
    old_from = row.valid_from_month
    old_to = row.valid_to_month

    if body.get("validFrom") is not None:
        row.valid_from_month = body["validFrom"]
    if body.get("validTo") is not None:
        row.valid_to_month = body["validTo"]
    if body.get("paymentTermCode"):
        row.payment_term_code = body["paymentTermCode"]
    if body.get("remark") is not None:
        row.remark = body["remark"]
    row.is_active = body.get("isActive", row.is_active)

    impact = _check_fob_impact(session, row.country_code, old_pt, row.payment_term_code,
                                row.valid_from_month, row.valid_to_month)

    session.commit()
    action = "correct" if is_correction else "update"
    _audit(session, row.country_code, action, actor=user.name,
           old_payment_term_code=old_pt, new_payment_term_code=row.payment_term_code,
           old_valid_from=old_from, old_valid_to=old_to,
           new_valid_from=row.valid_from_month, new_valid_to=row.valid_to_month,
           impacted_order_months=impact.get("orderMonths", 0))
    session.commit()
    return {"id": str(row.country_payment_term_id), "impact": impact}


@router.post("/payment-terms/countries/{term_id}/close")
def close_payment_term(
    term_id: str,
    body: dict,
    session: Session = Depends(get_db_session),
    user=Depends(require_min_role("admin")),
) -> dict:
    """End a payment term's validity at a given month."""
    row = session.get(CountryPaymentTermMaster, UUID(term_id))
    if not row:
        raise HTTPException(status_code=404)
    end_month = body.get("endMonth")  # YYYY-MM
    if not end_month:
        raise HTTPException(status_code=400, detail="endMonth required")
    row.valid_to_month = end_month
    if body.get("deactivate"):
        row.is_active = False
    session.commit()
    _audit(session, row.country_code, "close", actor=user.name,
           old_payment_term_code=row.payment_term_code, old_valid_from=row.valid_from_month, old_valid_to=end_month)
    session.commit()
    return {"id": str(row.country_payment_term_id)}


@router.get("/payment-terms/countries/impact")
def check_payment_term_impact(
    country: str,
    oldPaymentTerm: str,
    newPaymentTerm: str,
    validFrom: str = "",
    validTo: str = "",
    session: Session = Depends(get_db_session),
    user=Depends(require_min_role("admin")),
) -> dict:
    """Preview impact of a payment term change on FOB and orders."""
    return _check_fob_impact(session, country, oldPaymentTerm, newPaymentTerm,
                              validFrom, validTo)


def _has_overlap(session: Session, country: str, vf: str | None, vt: str | None) -> bool:
    if not vf:
        return False
    conditions = ["country_code = :c", "is_active = true"]
    params = {"c": country}
    if vt:
        conditions.append("valid_from_month <= :vt AND (valid_to_month IS NULL OR valid_to_month >= :vf)")
        params["vt"] = vt
        params["vf"] = vf
    else:
        conditions.append("(valid_to_month IS NULL OR valid_to_month >= :vf)")
        params["vf"] = vf
    sql = f"SELECT 1 FROM ordering.country_payment_term_master WHERE {' AND '.join(conditions)} LIMIT 1"
    r = session.execute(text(sql), params).fetchone()
    return r is not None


def _check_fob_impact(session: Session, country: str, old_pt: str, new_pt: str,
                      vf: str | None, vt: str | None) -> dict:
    """Count FOB rows and order rows potentially affected by a PT change."""
    months = []
    if vf:
        months = [vf]
        if vt:
            months.append(vt)
    fob_count = 0
    order_count = 0
    if months:
        r = session.execute(
            text("SELECT COUNT(*) FROM ordering.country_sku_fob_resolved "
                 "WHERE country_code=:c AND payment_term_code=:p"),
            {"c": country, "p": old_pt},
        ).fetchone()
        fob_count = r[0] if r else 0
        r = session.execute(
            text("SELECT COUNT(DISTINCT order_year||'-'||LPAD(order_month::text,2,'0')) "
                 "FROM ordering.order_quantity_cell "
                 "WHERE country_code=:c AND order_year||'-'||LPAD(order_month::text,2,'0') BETWEEN :vf AND COALESCE(:vt,:vf)"),
            {"c": country, "vf": vf, "vt": vt or vf},
        ).fetchone()
        order_count = r[0] if r else 0
    return {
        "fobRows": fob_count,
        "orderMonths": order_count,
        "message": (
            f"Changing {country} payment term from {old_pt} to {new_pt}. "
            f"{fob_count} FOB rows, {order_count} order months affected. "
            "Order snapshots WILL NOT be recalculated. "
            "Run FOB Check separately if FOB values need correction."
        ) if (fob_count or order_count) else None,
    }


def _audit(session: Session, country: str, action: str, **kw) -> None:
    session.add(PaymentTermAuditLog(
        country_code=country, action=action, **kw,
    ))


# ── Excel Export ──────────────────────────────────────────────────────


@router.post("/export")
def export_order_genius(
    body: dict,
    session: Session = Depends(get_db_session),
    user=Depends(require_min_role("viewer")),
) -> StreamingResponse:
    country = body.get("country", "")
    validate_country_access(session, user.name, user.role, country)
    year = body.get("year", 2026)
    include_hist = body.get("includeHistoricalWithQuantity", True)
    filters = _export_filter_params(body)
    quantities_only = _body_bool(body.get("quantitiesOnly", False))
    buf = export_matrix(session, country, year, include_hist,
                        **filters,
                        quantities_only=quantities_only)
    from datetime import date as _date
    today = _date.today().strftime("%Y%m%d")
    suffix = _export_filename_suffix(filters)
    filename = f"{country}_{year}_{today}{suffix}.xlsx"
    return StreamingResponse(
        buf,
        media_type=(
            "application/vnd.openxmlformats-officedocument"
            ".spreadsheetml.sheet"
        ),
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/export-pi")
def export_order_genius_pi(
    body: dict,
    session: Session = Depends(get_db_session),
    user=Depends(require_min_role("viewer")),
) -> StreamingResponse:
    country = body.get("country", "")
    validate_country_access(session, user.name, user.role, country)
    year = body.get("year", 2026)
    filters = _export_filter_params(body)
    buf = export_pi_matrix(
        session,
        country,
        year,
        **filters,
        freight_eur=_optional_float_from_body(body, "freightEur"),
        insurance_eur=_optional_float_from_body(body, "insuranceEur"),
        domestic_freight_eur=_optional_float_from_body(body, "domesticFreightEur"),
        domestic_insurance_eur=_optional_float_from_body(body, "domesticInsuranceEur"),
    )
    from datetime import date as _date
    today = _date.today().strftime("%Y%m%d")
    suffix = _export_filename_suffix(filters)
    filename = f"PI_{country}_{year}_{today}{suffix}.xlsx"
    return StreamingResponse(
        buf,
        media_type=(
            "application/vnd.openxmlformats-officedocument"
            ".spreadsheetml.sheet"
        ),
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ── Baseline Versions ─────────────────────────────────────────────────


@router.get("/baselines")
def list_baselines(
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict:
    baselines = repo.list_baseline_versions(session)
    return {
        "items": [
            {
                "baselineVersionId": str(b.baseline_version_id),
                "baselineName": b.baseline_name,
                "sourceFileName": b.source_file_name,
                "status": b.status,
                "publishedBy": b.published_by,
                "publishedAtUtc": (
                    b.published_at_utc.isoformat()
                    if b.published_at_utc else None
                ),
                "createdAtUtc": b.created_at_utc.isoformat(),
            }
            for b in baselines
        ],
    }


# ── Order Quantity Import (round-trip: export → edit → re-import) ─────

_IMPORT_SESSION_DIR = PROJECT_ROOT / "04_Processed_data" / "ops" / "order_quantity_imports"


@router.post("/import-quantities/preview")
def preview_quantity_import(
    file: UploadFile = File(...),
    session: Session = Depends(get_db_session),
    user=Depends(require_roles("editor", "admin", "order_filler")),
) -> dict:
    """Upload an exported Order Genius XLSX, parse it, and return a preview diff."""
    import json
    import uuid as _uuid
    import shutil

    _IMPORT_SESSION_DIR.mkdir(parents=True, exist_ok=True)
    import_id = str(_uuid.uuid4())
    tmp_path = _IMPORT_SESSION_DIR / f"{import_id}.xlsx"

    try:
        with tmp_path.open("wb") as f:
            shutil.copyfileobj(file.file, f)
    finally:
        file.file.close()

    try:
        parsed = parse_order_quantity_xlsx(tmp_path)
    except Exception as e:
        tmp_path.unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail=f"Failed to parse file: {e}")

    validate_country_access(session, user.name, user.role, parsed.country_code)

    if parsed.errors and not parsed.rows:
        tmp_path.unlink(missing_ok=True)
        return {
            "importId": import_id,
            "countryCode": parsed.country_code,
            "year": parsed.year,
            "matchedRows": [],
            "newRows": [],
            "fobChanges": [],
            "totalCells": 0,
            "errorCells": 0,
            "errors": parsed.errors,
            "status": "error",
        }

    preview = preview_order_quantity_import(session, parsed)

    # Save parsed data as JSON for the apply step
    json_path = _IMPORT_SESSION_DIR / f"{import_id}.json"
    rows_data = []
    for row in parsed.rows:
        rows_data.append({
            "material_code": row.material_code,
            "fob_eur": row.fob_eur,
            "model_name": row.model_name,
            "version": row.version,
            "colour": row.colour,
            "cells": [
                {
                    "material_code": c.material_code,
                    "month": c.month,
                    "quantity": c.quantity,
                    "fob_eur": c.fob_eur,
                    "error": c.error,
                }
                for c in row.cells
            ],
            "errors": row.errors,
        })
    with json_path.open("w") as f:
        json.dump({
            "country_code": parsed.country_code,
            "year": parsed.year,
            "rows": rows_data,
        }, f)

    preview["importId"] = import_id
    preview["status"] = "ok" if not preview["errors"] else "warning"
    return preview


@router.post("/import-quantities/{import_id}/apply")
def apply_quantity_import(
    import_id: str,
    session: Session = Depends(get_db_session),
    user=Depends(require_roles("editor", "admin", "order_filler")),
) -> dict:
    """Apply a previously previewed quantity import."""
    import json

    json_path = _IMPORT_SESSION_DIR / f"{import_id}.json"
    xlsx_path = _IMPORT_SESSION_DIR / f"{import_id}.xlsx"

    if not json_path.exists():
        raise HTTPException(status_code=404, detail="Import session not found or expired")

    try:
        with json_path.open() as f:
            data = json.load(f)
    except Exception:
        raise HTTPException(status_code=400, detail="Failed to read import session data")

    validate_country_access(session, user.name, user.role, data.get("country_code", ""))

    from app.services.order_quantity_parser import ImportedQuantityCell, ImportedRow, OrderQuantityImport

    rows = []
    for rd in data["rows"]:
        cells = [
            ImportedQuantityCell(
                material_code=cd["material_code"],
                month=cd["month"],
                quantity=cd["quantity"],
                fob_eur=cd.get("fob_eur"),
                error=cd.get("error", ""),
            )
            for cd in rd["cells"]
        ]
        rows.append(ImportedRow(
            material_code=rd["material_code"],
            fob_eur=rd.get("fob_eur"),
            model_name=rd.get("model_name", ""),
            version=rd.get("version", ""),
            colour=rd.get("colour", ""),
            cells=cells,
            errors=rd.get("errors", []),
        ))

    parsed = OrderQuantityImport(
        country_code=data["country_code"],
        year=data["year"],
        rows=rows,
    )

    result = apply_order_quantity_import(session, parsed, user.name)
    session.commit()

    # Cleanup session files
    json_path.unlink(missing_ok=True)
    xlsx_path.unlink(missing_ok=True)

    return {"importId": import_id, "status": "applied", **result}
