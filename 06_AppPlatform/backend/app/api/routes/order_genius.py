"""API routes for Order Genius — Material Master upload, FOB matrix, export."""

from __future__ import annotations

import io
from uuid import UUID
import uuid as uuid_module
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.api.order_genius_schemas import (
    ExportRequest,
    PublishBaselineRequest,
    QuantityCellUpdate,
    RemarkUpdate,
)
from app.core.config import PROJECT_ROOT
from app.core.security import require_min_role
from app.db.models import CountryPaymentTermMaster, PaymentTermAuditLog
from app.db.session import get_db_session
from app.infra import order_genius_repository as repo
from app.services import material_master_upload_service as upload_svc
from app.services.order_genius_service import (
    build_matrix,
    build_options,
    export_matrix,
    get_fob_for_sku,
    update_quantity_cell,
    update_remark,
)

router = APIRouter(prefix="/order-genius", tags=["order_genius"])


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


@router.get("/countries")
def list_countries_with_payment_terms(
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict:
    countries = repo.list_country_payment_terms(session)
    return {
        "items": [
            {
                "countryCode": c.country_code,
                "countryName": c.country_name,
                "paymentTermCode": c.payment_term_code,
                "paymentMethod": c.payment_method,
                "lcDays": c.lc_days,
            }
            for c in countries
        ],
    }


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
    _=Depends(require_min_role("viewer")),
) -> dict:
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
    _=Depends(require_min_role("viewer")),
) -> dict:
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


@router.patch("/quantity-cell")
def patch_quantity_cell(
    body: dict,
    session: Session = Depends(get_db_session),
    user=Depends(require_min_role("editor")),
) -> dict:
    try:
        result = update_quantity_cell(
            session,
            country_code=body.get("countryCode", body.get("country_code", "")),
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
    _audit(session, code, "create", actor=user.name, new_pt=row.payment_term_code,
           new_from=row.valid_from_month, new_to=row.valid_to_month)
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
           old_pt=old_pt, new_pt=row.payment_term_code,
           old_from=old_from, old_to=old_to,
           new_from=row.valid_from_month, new_to=row.valid_to_month,
           impacted=impact.get("orderMonths", 0))
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
           old_pt=row.payment_term_code, old_from=row.valid_from_month, old_to=end_month)
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
    _=Depends(require_min_role("viewer")),
) -> StreamingResponse:
    country = body.get("country", "")
    year = body.get("year", 2026)
    include_hist = body.get("includeHistoricalWithQuantity", True)
    buf = export_matrix(session, country, year, include_hist)
    filename = f"Order_Genius_{country}-{year}.xlsx"
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
