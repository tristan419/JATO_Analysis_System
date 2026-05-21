"""API routes for Order Genius — Material Master upload, FOB matrix, export."""

from __future__ import annotations

import io
import uuid as uuid_module
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from app.api.order_genius_schemas import (
    ExportRequest,
    PublishBaselineRequest,
    QuantityCellUpdate,
    RemarkUpdate,
)
from app.core.config import PROJECT_ROOT
from app.core.security import require_min_role
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
    return upload_svc.get_session(upload_id)


@router.put("/material-master-uploads/{upload_id}/parts/{part_number}")
async def upload_material_master_chunk(
    upload_id: str,
    part_number: int,
    request: Request,
    _=Depends(require_min_role("editor")),
) -> dict:
    chunk_data = await request.body()
    return upload_svc.upload_chunk(upload_id, part_number, chunk_data)


@router.post("/material-master-uploads/{upload_id}/complete")
def complete_material_master_upload(
    upload_id: str,
    _=Depends(require_min_role("editor")),
) -> dict:
    return upload_svc.complete_upload(upload_id)


@router.post("/material-master-uploads/{upload_id}/parse")
def parse_material_master_upload(
    upload_id: str,
    _=Depends(require_min_role("editor")),
) -> dict:
    return upload_svc.parse_upload(upload_id)


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
    body: PublishBaselineRequest = PublishBaselineRequest(),
    session: Session = Depends(get_db_session),
    user: str = Depends(require_min_role("admin")),
) -> dict:
    try:
        result = upload_svc.publish_upload(
            session, upload_id, user.name, body.notes
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
    body: QuantityCellUpdate,
    session: Session = Depends(get_db_session),
    user=Depends(require_min_role("editor")),
) -> dict:
    try:
        result = update_quantity_cell(
            session,
            country_code=body.country_code,
            order_year=body.order_year,
            order_month=body.order_month,
            material_code=body.material_code,
            quantity=body.quantity,
            updated_by=user.name,
            expected_version=body.row_version,
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
    body: RemarkUpdate,
    session: Session = Depends(get_db_session),
    user=Depends(require_min_role("editor")),
) -> dict:
    try:
        result = update_remark(
            session,
            material_code=material_code,
            remark=body.remark,
            changed_by=user.name,
            expected_version=body.row_version,
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


# ── Excel Export ──────────────────────────────────────────────────────


@router.post("/export")
def export_order_genius(
    body: ExportRequest,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> StreamingResponse:
    buf = export_matrix(
        session, body.country, body.year, body.include_historical_with_quantity
    )
    filename = f"Order_Genius_{body.country}-{body.year}.xlsx"
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
