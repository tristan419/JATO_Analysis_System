"""API routes for Order Genius PI vehicle allocation."""

from __future__ import annotations

import json
import shutil
from datetime import date
from uuid import uuid4

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from app.core.config import PROJECT_ROOT
from app.core.security import UserContext, require_min_role, require_roles, validate_country_access
from app.db.session import get_db_session
from app.infra import order_genius_vehicle_repository as vehicle_repo
from app.services.order_genius_vehicle_import_parser import parse_vehicle_allocation_xlsx
from app.services.order_genius_vehicle_service import (
    apply_vehicle_import,
    create_pi_header,
    create_pi_line,
    export_vehicle_units,
    generate_from_order_matrix,
    get_order_matrix_allocation_plan,
    get_pi_detail,
    get_vehicle_detail,
    list_pi_headers,
    list_vehicle_units,
    parse_car_code,
    parse_pi_code,
    preview_vehicle_import,
    search_vehicle_allocation,
    update_pi_header,
    update_pi_line,
    update_vehicle_unit,
)

router = APIRouter(
    prefix="/order-genius/vehicle-allocation",
    tags=["order_genius_vehicle_allocation"],
)

IMPORT_SESSION_DIR = PROJECT_ROOT / "04_Processed_data" / "ops" / "vehicle_allocation_imports"
XLSX_MEDIA_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def _clean(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _country(value: object) -> str | None:
    text = _clean(value)
    return text.upper() if text else None


def _parse_date_query(value: str | None, field: str) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"{field} must be YYYY-MM-DD") from exc


def _validate_country(session: Session, user: UserContext, country: str | None) -> None:
    validate_country_access(session, user.name, user.role, country or "")


def _validate_optional_country(session: Session, user: UserContext, country: str | None) -> None:
    if country or user.role == "order_filler":
        _validate_country(session, user, country)


def _validate_pi_detail_access(session: Session, user: UserContext, detail: dict) -> None:
    header = detail.get("header") if isinstance(detail, dict) else None
    country = header.get("countryCode") if isinstance(header, dict) else None
    market_countries = header.get("marketCountryCodes") if isinstance(header, dict) else None
    if isinstance(market_countries, list) and market_countries:
        last_error: HTTPException | None = None
        for market_country in market_countries:
            try:
                _validate_optional_country(session, user, _country(market_country))
                return
            except HTTPException as exc:
                last_error = exc
        if last_error:
            raise last_error
    _validate_optional_country(session, user, country)


def _validate_vehicle_access(session: Session, user: UserContext, vehicle: dict) -> None:
    _validate_optional_country(session, user, _country(vehicle.get("countryCode")))


def _row_country(session: Session, row: dict) -> str | None:
    country = _country(row.get("country_code"))
    if country:
        return country
    car_code = _clean(row.get("car_code"))
    if car_code:
        parsed_car = parse_car_code(car_code.upper())
        if parsed_car:
            return str(parsed_car["countryCode"])
    pi_code = _clean(row.get("pi_code"))
    if pi_code:
        parsed_pi = parse_pi_code(pi_code.upper())
        if parsed_pi and len(str(parsed_pi["countryCode"])) == 2:
            return str(parsed_pi["countryCode"])
    vin = _clean(row.get("vin"))
    if vin:
        vehicle = vehicle_repo.get_vehicle_by_vin(session, vin)
        if vehicle:
            return vehicle.country_code
    return None


def _validate_import_access(session: Session, user: UserContext, rows: list[dict]) -> None:
    for row in rows:
        country = _row_country(session, row)
        if country or user.role == "order_filler":
            try:
                _validate_country(session, user, country)
            except HTTPException as exc:
                source_row = row.get("sourceRow")
                detail = f"Row {source_row}: {exc.detail}" if source_row else str(exc.detail)
                raise HTTPException(status_code=exc.status_code, detail=detail) from exc


def _vehicle_filters(
    keyword: str | None = None,
    pi_code: str | None = None,
    pi_line_code: str | None = None,
    car_code: str | None = None,
    vin: str | None = None,
    material_code: str | None = None,
    bom: str | None = None,
    country: str | None = None,
    ship_name: str | None = None,
    allocation_status: str | None = None,
    logistics_status: str | None = None,
    eta_from: str | None = None,
    eta_to: str | None = None,
    ready_from: str | None = None,
    ready_to: str | None = None,
    vin_missing_only: bool = False,
    unallocated_only: bool = False,
    page: int = 1,
    page_size: int = 100,
) -> dict:
    return {
        "keyword": _clean(keyword),
        "pi_code": _clean(pi_code),
        "pi_line_code": _clean(pi_line_code),
        "car_code": _clean(car_code),
        "vin": _clean(vin),
        "material_code": _clean(material_code),
        "bom": _clean(bom),
        "country": _country(country),
        "ship_name": _clean(ship_name),
        "allocation_status": _clean(allocation_status),
        "logistics_status": _clean(logistics_status),
        "eta_from": _parse_date_query(eta_from, "etaFrom"),
        "eta_to": _parse_date_query(eta_to, "etaTo"),
        "ready_from": _parse_date_query(ready_from, "readyFrom"),
        "ready_to": _parse_date_query(ready_to, "readyTo"),
        "vin_missing_only": vin_missing_only,
        "unallocated_only": unallocated_only,
        "page": max(page, 1),
        "page_size": max(1, min(page_size, 500)),
    }


@router.get("/pi")
def list_pi_orders(
    country: str | None = Query(default=None),
    month: str | None = Query(default=None),
    status: str | None = Query(default=None),
    keyword: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("viewer")),
) -> dict:
    selected_country = _country(country)
    _validate_optional_country(session, user, selected_country)
    return list_pi_headers(
        session,
        country=selected_country,
        month=_clean(month),
        status=_clean(status),
        keyword=_clean(keyword),
        page=page,
        page_size=page_size,
    )


@router.post("/pi")
def create_pi_order(
    body: dict,
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_roles("order_filler", "editor", "admin")),
) -> dict:
    selected_country = _country(body.get("countryCode") or body.get("country"))
    _validate_country(session, user, selected_country)
    result = create_pi_header(session, body, user.name)
    session.commit()
    return result


@router.get("/pi/{pi_code}")
def get_pi_order(
    pi_code: str,
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("viewer")),
) -> dict:
    detail = get_pi_detail(session, pi_code.upper())
    _validate_pi_detail_access(session, user, detail)
    return detail


@router.patch("/pi/{pi_code}")
def patch_pi_order(
    pi_code: str,
    body: dict,
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_roles("order_filler", "editor", "admin")),
) -> dict:
    detail = get_pi_detail(session, pi_code.upper())
    _validate_pi_detail_access(session, user, detail)
    result = update_pi_header(session, pi_code.upper(), body, user.name)
    session.commit()
    return result


@router.post("/pi/{pi_code}/lines")
def create_pi_order_line(
    pi_code: str,
    body: dict,
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_roles("order_filler", "editor", "admin")),
) -> dict:
    detail = get_pi_detail(session, pi_code.upper())
    _validate_pi_detail_access(session, user, detail)
    result = create_pi_line(session, pi_code.upper(), body, user.name)
    session.commit()
    return result


@router.patch("/lines/{pi_line_code}")
def patch_pi_order_line(
    pi_line_code: str,
    body: dict,
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_roles("order_filler", "editor", "admin")),
) -> dict:
    existing = vehicle_repo.get_line_by_code(session, pi_line_code.upper())
    if not existing:
        raise HTTPException(status_code=404, detail="PI line not found")
    detail = get_pi_detail(session, existing.pi_code)
    _validate_pi_detail_access(session, user, detail)
    line = update_pi_line(session, pi_line_code.upper(), body, user.name)
    session.commit()
    return line


@router.get("/vehicles")
def list_vehicles(
    keyword: str | None = Query(default=None),
    pi_code: str | None = Query(default=None),
    pi_line_code: str | None = Query(default=None),
    car_code: str | None = Query(default=None),
    vin: str | None = Query(default=None),
    material_code: str | None = Query(default=None),
    bom: str | None = Query(default=None),
    country: str | None = Query(default=None),
    ship_name: str | None = Query(default=None),
    allocation_status: str | None = Query(default=None),
    logistics_status: str | None = Query(default=None),
    eta_from: str | None = Query(default=None),
    eta_to: str | None = Query(default=None),
    ready_from: str | None = Query(default=None),
    ready_to: str | None = Query(default=None),
    vin_missing_only: bool = Query(default=False),
    unallocated_only: bool = Query(default=False),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=500),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("viewer")),
) -> dict:
    filters = _vehicle_filters(
        keyword=keyword,
        pi_code=pi_code,
        pi_line_code=pi_line_code,
        car_code=car_code,
        vin=vin,
        material_code=material_code,
        bom=bom,
        country=country,
        ship_name=ship_name,
        allocation_status=allocation_status,
        logistics_status=logistics_status,
        eta_from=eta_from,
        eta_to=eta_to,
        ready_from=ready_from,
        ready_to=ready_to,
        vin_missing_only=vin_missing_only,
        unallocated_only=unallocated_only,
        page=page,
        page_size=page_size,
    )
    _validate_optional_country(session, user, filters["country"])
    return list_vehicle_units(session, **filters)


@router.get("/vehicles/{car_code}")
def get_vehicle(
    car_code: str,
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("viewer")),
) -> dict:
    vehicle = get_vehicle_detail(session, car_code.upper())
    _validate_vehicle_access(session, user, vehicle)
    return vehicle


@router.patch("/vehicles/{car_code}")
def patch_vehicle(
    car_code: str,
    body: dict,
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_roles("order_filler", "editor", "admin")),
) -> dict:
    vehicle = get_vehicle_detail(session, car_code.upper())
    _validate_vehicle_access(session, user, vehicle)
    result = update_vehicle_unit(session, car_code.upper(), body, user.name)
    session.commit()
    return result


@router.get("/search")
def search_allocation(
    keyword: str = Query(min_length=1),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("viewer")),
) -> dict:
    result = search_vehicle_allocation(session, keyword)
    if result.get("type") == "pi":
        _validate_pi_detail_access(session, user, result["item"])
    elif result.get("type") == "vehicle":
        _validate_vehicle_access(session, user, result["item"])
    return result


@router.get("/order-matrix-plan")
def get_order_matrix_plan(
    country: str = Query(min_length=1),
    year: int = Query(ge=2000),
    month: int = Query(ge=1, le=12),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("viewer")),
) -> dict:
    selected_country = _country(country)
    _validate_country(session, user, selected_country)
    return get_order_matrix_allocation_plan(session, selected_country or "", year, month)


@router.post("/generate-from-order-matrix")
def generate_pi_from_order_matrix(
    body: dict,
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_roles("order_filler", "editor", "admin")),
) -> dict:
    selected_country = _country(body.get("countryCode"))
    _validate_country(session, user, selected_country)
    market_countries = body.get("marketCountryCodes")
    if isinstance(market_countries, list):
        for market_country in market_countries:
            _validate_country(session, user, _country(market_country))
    line_items = body.get("lineItems")
    if isinstance(line_items, list):
        for item in line_items:
            if not isinstance(item, dict):
                continue
            allocations = item.get("allocations")
            if not isinstance(allocations, list):
                continue
            for allocation in allocations:
                if isinstance(allocation, dict):
                    _validate_country(session, user, _country(allocation.get("countryCode") or allocation.get("marketCountryCode")))
    result = generate_from_order_matrix(session, body, user.name)
    session.commit()
    return result


@router.post("/import/preview")
def preview_import(
    file: UploadFile = File(...),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_roles("order_filler", "editor", "admin")),
) -> dict:
    IMPORT_SESSION_DIR.mkdir(parents=True, exist_ok=True)
    import_id = str(uuid4())
    tmp_path = IMPORT_SESSION_DIR / f"{import_id}.xlsx"
    try:
        with tmp_path.open("wb") as handle:
            shutil.copyfileobj(file.file, handle)
    finally:
        file.file.close()

    try:
        rows = parse_vehicle_allocation_xlsx(tmp_path)
    except Exception as exc:
        tmp_path.unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail=f"Failed to parse file: {exc}") from exc

    _validate_import_access(session, user, rows)
    preview = preview_vehicle_import(session, rows)
    json_path = IMPORT_SESSION_DIR / f"{import_id}.json"
    with json_path.open("w") as handle:
        json.dump({"rows": rows}, handle)
    preview["importId"] = import_id
    return preview


@router.post("/import/{import_id}/apply")
def apply_import(
    import_id: str,
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_roles("order_filler", "editor", "admin")),
) -> dict:
    json_path = IMPORT_SESSION_DIR / f"{import_id}.json"
    xlsx_path = IMPORT_SESSION_DIR / f"{import_id}.xlsx"
    if not json_path.exists():
        raise HTTPException(status_code=404, detail="Import session not found or expired")
    try:
        with json_path.open() as handle:
            payload = json.load(handle)
        rows = payload.get("rows") or []
        if not isinstance(rows, list):
            raise ValueError("rows must be a list")
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Failed to read import session data") from exc

    _validate_import_access(session, user, rows)
    result = apply_vehicle_import(session, rows, user.name)
    session.commit()
    json_path.unlink(missing_ok=True)
    xlsx_path.unlink(missing_ok=True)
    return result


@router.post("/export")
def export_vehicle_allocation(
    body: dict | None = None,
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("viewer")),
) -> StreamingResponse:
    payload = body or {}
    filters = _vehicle_filters(
        keyword=payload.get("keyword"),
        pi_code=payload.get("piCode") or payload.get("pi_code"),
        pi_line_code=payload.get("piLineCode") or payload.get("pi_line_code"),
        car_code=payload.get("carCode") or payload.get("car_code"),
        vin=payload.get("vin"),
        material_code=payload.get("materialCode") or payload.get("material_code"),
        bom=payload.get("bom"),
        country=payload.get("country") or payload.get("countryCode"),
        ship_name=payload.get("shipName") or payload.get("ship_name"),
        allocation_status=payload.get("allocationStatus") or payload.get("allocation_status"),
        logistics_status=payload.get("logisticsStatus") or payload.get("logistics_status"),
        eta_from=payload.get("etaFrom") or payload.get("eta_from"),
        eta_to=payload.get("etaTo") or payload.get("eta_to"),
        ready_from=payload.get("readyFrom") or payload.get("ready_from"),
        ready_to=payload.get("readyTo") or payload.get("ready_to"),
        vin_missing_only=bool(payload.get("vinMissingOnly") or payload.get("vin_missing_only")),
        unallocated_only=bool(payload.get("unallocatedOnly") or payload.get("unallocated_only")),
        page=1,
        page_size=500,
    )
    filters["page_size"] = 50_000
    _validate_optional_country(session, user, filters["country"])
    buffer = export_vehicle_units(session, **filters)
    today = date.today().strftime("%Y%m%d")
    country_part = filters["country"] or "ALL"
    filename = f"Vehicle_Allocation_{country_part}_{today}.xlsx"
    return StreamingResponse(
        buffer,
        media_type=XLSX_MEDIA_TYPE,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
