"""API routes for Order Genius PI vehicle allocation."""

from __future__ import annotations

import json
import hashlib
import shutil
import tempfile
from pathlib import Path
from datetime import date
from uuid import uuid4

from typing import Any

from fastapi import APIRouter, Body, Depends, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from starlette.background import BackgroundTask
from sqlalchemy.orm import Session

from app.core.config import COC_MATCH_JOB_ROOT
from app.core.security import (UserContext, require_min_role, require_roles, validate_country_access,
                               validate_brand_access, ordering_brands, ordering_countries, is_admin)
from app.infra import order_genius_repository as ordering_repo
from app.db.session import get_db_session
from app.infra import order_genius_vehicle_repository as vehicle_repo
from app.services.order_genius_vehicle_import_parser import (
    parse_vehicle_allocation_xlsx,
    parse_vehicle_vin_list_xlsx,
)
from app.services.order_genius_vehicle_service import (
    apply_vehicle_import,
    bulk_update_vehicle_units,
    create_pi_header,
    create_pi_line,
    delete_pi,
    delete_pi_line,
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
from app.services.vehicle_status_flow_config import get_vehicle_status_flow_config
from app.services.order_genius_vehicle_exporter import generate_pi_invoice_excel, pi_invoice_context
from app.services import pi_invoice_templates as invoice_templates
from app.db.models import User, VehicleCustomColumn
from sqlalchemy import select, func
from app.core.security import ORDERING_BRANDS

router = APIRouter(
    prefix="/order-genius/vehicle-allocation",
    tags=["order_genius_vehicle_allocation"],
)

# Share the configured writable operations directory, not the immutable release tree.
IMPORT_SESSION_DIR = COC_MATCH_JOB_ROOT.parent / "vehicle_allocation_imports"
XLSX_MEDIA_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def _coc_pi_vehicles(session: Session, user: UserContext, pi_code: str) -> list:
    if not vehicle_repo.get_header_by_code(session, pi_code):
        raise HTTPException(404, "PI not found / PI不存在")
    vehicles = vehicle_repo.list_vehicles_for_bulk_update(session, pi_code=pi_code)
    permitted = []
    brands = ordering_brands(session, user)
    countries = {}
    for vehicle in vehicles:
        country = vehicle.country_code
        if country not in countries:
            try:
                _validate_country(session, user, country)
                countries[country] = True
            except HTTPException as exc:
                if exc.status_code != 403:
                    raise
                countries[country] = False
        if not countries[country] or (brands is not None and str(vehicle.brand or "").upper() not in brands):
            continue
        permitted.append(vehicle)
    if not permitted:
        raise HTTPException(403, "No accessible vehicles in this PI / 本PI没有有权访问的车辆")
    return permitted


@router.get("/pi/{pi_code}/coc-library")
def lookup_pi_cocs(pi_code: str, session: Session = Depends(get_db_session),
                   user: UserContext = Depends(require_roles("viewer", "editor", "admin", "order_filler"))) -> dict:
    from app.services.coc_library_service import lookup_vins
    vehicles = _coc_pi_vehicles(session, user, pi_code)
    matches = lookup_vins([v.vin for v in vehicles if v.vin])
    items = [{"carCode": v.car_code, "vin": v.vin,
              "status": "awaiting_vin" if not v.vin else "available" if v.vin.upper() in matches else "missing"} for v in vehicles]
    return {"items": items, "total": len(items),
            "available": sum(i["status"] == "available" for i in items),
            "awaitingVin": sum(i["status"] == "awaiting_vin" for i in items),
            "missing": sum(i["status"] == "missing" for i in items)}


@router.post("/pi/{pi_code}/coc-download")
def download_pi_cocs(pi_code: str, payload: dict, session: Session = Depends(get_db_session),
                      user: UserContext = Depends(require_roles("viewer", "editor", "admin", "order_filler"))) -> FileResponse:
    from app.services.coc_library_service import write_pdf_zip
    vehicles = {v.car_code: v for v in _coc_pi_vehicles(session, user, pi_code)}
    codes = payload.get("carCodes")
    if not isinstance(codes, list) or not codes or not all(isinstance(c, str) for c in codes) or not set(codes) <= vehicles.keys():
        raise HTTPException(400, "Select accessible vehicles from this PI / 请勾选本PI有权访问的车辆")
    selected = [vehicles[c] for c in codes]
    if any(not v.vin for v in selected):
        raise HTTPException(409, "VIN missing; select available rows / 存在未录VIN车辆，请勾选可用项")
    confirmed_vins = payload.get("vinsByCarCode")
    if (not isinstance(confirmed_vins, dict) or set(confirmed_vins) != set(codes)
            or any(not isinstance(confirmed_vins[v.car_code], str)
                   or confirmed_vins[v.car_code].strip().upper() != v.vin.upper() for v in selected)):
        raise HTTPException(409, "VIN confirmation changed or missing; search the library again / VIN确认已变化或缺失，请重新查库")
    temporary = tempfile.TemporaryDirectory(prefix="jato-coc-download-")
    path = Path(temporary.name) / "coc.zip"
    try:
        write_pdf_zip([v.vin.upper() for v in selected], path)
    except Exception:
        temporary.cleanup()
        raise
    return FileResponse(path, media_type="application/zip", filename=f"{pi_code}-COC.zip",
                        background=BackgroundTask(temporary.cleanup))


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
    if country:
        _validate_country(session, user, country)


def _normalise_import_rows_payload(payload: object) -> list[dict[str, Any]]:
    raw_rows = payload.get("rows") if isinstance(payload, dict) else payload
    if not isinstance(raw_rows, list):
        raise HTTPException(status_code=400, detail="rows must be a list")

    rows: list[dict[str, Any]] = []
    for index, raw_row in enumerate(raw_rows, start=1):
        if not isinstance(raw_row, dict):
            raise HTTPException(status_code=400, detail=f"rows[{index - 1}] must be an object")
        row = dict(raw_row)
        row.setdefault("sourceRow", index)
        rows.append(row)
    return rows


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
    _validate_country(session, user, _country(vehicle.get("countryCode")))
    validate_brand_access(session, user, vehicle.get("brand"))


def _accessible_pi_detail(session: Session, user: UserContext, pi_code: str) -> dict:
    detail = get_pi_detail(session, pi_code)
    can_delete = is_admin(user.role)
    if user.role == "editor":
        try:
            _validate_whole_pi_access(session, user, detail)
            can_delete = True
        except HTTPException as exc:
            if exc.status_code != 403:
                raise
    result = get_pi_detail(session, pi_code, countries=ordering_countries(session, user),
                           brands=ordering_brands(session, user))
    if not is_admin(user.role) and not result["lines"]:
        raise HTTPException(403, "No authorized lines in this PI / 本 PI 没有有权访问的明细")
    result["canDelete"] = can_delete
    return result


def _validate_whole_pi_access(session: Session, user: UserContext, detail: dict) -> None:
    _validate_target_countries(session, user, detail["vehicles"])
    for line in detail["lines"]:
        _validate_line_countries(session, user, detail, line["piLineCode"])
    if not detail["lines"]:
        _validate_country(session, user, detail["header"]["countryCode"])
        if not is_admin(user.role):
            raise HTTPException(403, "PI has no authorized brand / PI 缺少可核对的品牌")


def _accessible_market_countries(session: Session, user: UserContext, countries: set[str]) -> set[str]:
    allowed = set()
    for country in countries:
        try:
            _validate_country(session, user, country)
            allowed.add(country)
        except HTTPException as exc:
            if exc.status_code != 403:
                raise
    return allowed


def _validate_target_countries(session: Session, user: UserContext, vehicles: list[dict]) -> None:
    # Validate the complete target set before any write; never silently trim a batch.
    for country in {vehicle["countryCode"] for vehicle in vehicles}:
        _validate_country(session, user, country)
    for brand in {vehicle.get("brand") for vehicle in vehicles}:
        validate_brand_access(session, user, brand)


def _validate_line_countries(session: Session, user: UserContext, detail: dict, line_code: str) -> None:
    targets = [vehicle for vehicle in detail["vehicles"] if vehicle["piLineCode"] == line_code]
    _validate_target_countries(session, user, targets)
    countries = {vehicle["countryCode"] for vehicle in targets}
    for line in detail["lines"]:
        if line["piLineCode"] == line_code:
            validate_brand_access(session, user, line.get("brand"))
            countries.update(allocation["marketCountryCode"] for allocation in line["allocations"])
    for country in countries or {detail["header"]["countryCode"]}:
        _validate_country(session, user, country)


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
        vehicle = (vehicle_repo.get_vehicle_by_car_code(session, str(row.get("car_code") or "").upper())
                   if row.get("car_code") else vehicle_repo.get_vehicle_by_vin(session, str(row.get("vin") or "")))
        if vehicle:
            _validate_vehicle_access(session, user, {"countryCode": vehicle.country_code, "brand": vehicle.brand})
        else:
            sku = ordering_repo.get_sku_by_material_code_any_status(session, str(row.get("material_code") or ""))
            validate_brand_access(session, user, sku.brand if sku else None)
        country = _row_country(session, row)
        if country or not is_admin(user.role):
            try:
                _validate_country(session, user, country)
            except HTTPException as exc:
                source_row = row.get("sourceRow")
                detail = f"Row {source_row}: {exc.detail}" if source_row else str(exc.detail)
                raise HTTPException(status_code=exc.status_code, detail=detail) from exc


def _vin_import_countries(session: Session, user: UserContext, pi_code: str) -> set[str]:
    detail = get_pi_detail(session, pi_code)
    _validate_pi_detail_access(session, user, detail)
    countries = {v.country_code for v in vehicle_repo.list_vehicles_for_bulk_update(session, pi_code=pi_code)}
    allowed: set[str] = set()
    for country in countries:
        try:
            _validate_country(session, user, country)
            allowed.add(country)
        except HTTPException as exc:
            if exc.status_code != 403:
                raise
    return allowed


def _store_import_preview(session: Session, user: UserContext, rows: list[dict], import_id: str,
                          pi_code: str | None, allow_replacing: bool, source: str | None = None, remove_vins: bool = False,
                          pi_codes: list[str] | None = None) -> dict:
    payload: dict = {"rows": rows}
    if source:
        payload["source"] = source
    if pi_code or pi_codes is not None:
        codes = [pi_code] if pi_code else pi_codes
        if (not isinstance(codes, list) or not codes or len(codes) > 100
                or any(not isinstance(code, str) or not code.strip() for code in codes)):
            raise HTTPException(400, "Select valid PIs / 请选择有效PI")
        codes = list(dict.fromkeys(code.strip().upper() for code in codes))
        if remove_vins and len(codes) > 1:
            raise HTTPException(400, "Select one PI to clear VINs / 清VIN请明确选择单张PI")
        countries = set().union(*(_vin_import_countries(session, user, code) for code in codes))
        preview = preview_vehicle_import(session, rows, pi_codes=codes,
                                         allow_replacing=allow_replacing, allowed_countries=countries,
                                         allowed_brands=ordering_brands(session, user), remove_vins=remove_vins)
        payload.update({"owner": user.name, "vinPreview": preview})
    else:
        _validate_import_access(session, user, rows)
        preview = preview_vehicle_import(session, rows)
    IMPORT_SESSION_DIR.mkdir(parents=True, exist_ok=True)
    with (IMPORT_SESSION_DIR / f"{import_id}.json").open("w") as handle:
        json.dump(payload, handle)
    preview["importId"] = import_id
    return preview


def _vehicle_filters(
    keyword: str | None = None,
    pi_code: str | None = None,
    pi_line_code: str | None = None,
    car_code: str | None = None,
    vin: str | None = None,
    material_code: str | None = None,
    bom: str | None = None,
    brand: str | None = None,
    model_name: str | None = None,
    version: str | None = None,
    powertrain: str | None = None,
    exterior_color_name: str | None = None,
    interior_color_name: str | None = None,
    order_month: str | None = None,
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
        "brand": _clean(brand),
        "model_name": _clean(model_name),
        "version": _clean(version),
        "powertrain": _clean(powertrain),
        "exterior_color_name": _clean(exterior_color_name),
        "interior_color_name": _clean(interior_color_name),
        "order_month": _clean(order_month),
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


@router.get("/pi-months")
def list_pi_months(
    year: int = Query(ge=1, le=9999),
    country: str | None = Query(default=None),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("viewer")),
) -> dict:
    selected_country = _country(country)
    _validate_optional_country(session, user, selected_country)
    return vehicle_repo.pi_month_summary(session, year=year, country=selected_country,
                                        allowed_countries=ordering_countries(session, user),
                                        allowed_brands=ordering_brands(session, user))


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
    result = list_pi_headers(
        session,
        country=selected_country,
        month=_clean(month),
        status=_clean(status),
        keyword=_clean(keyword),
        page=page,
        page_size=page_size,
        allowed_countries=ordering_countries(session, user),
        allowed_brands=ordering_brands(session, user),
    )
    if not is_admin(user.role):
        markets = vehicle_repo.visible_header_markets(session, [header["piCode"] for header in result["items"]],
                                                      ordering_countries(session, user), ordering_brands(session, user))
        for header in result["items"]:
            header["marketCountryCodes"] = [country for country in header["marketCountryCodes"]
                                            if country in markets.get(header["piCode"], set())]
    return result


@router.post("/pi")
def create_pi_order(
    body: dict,
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_roles("order_filler", "editor", "admin")),
) -> dict:
    selected_country = _country(body.get("countryCode") or body.get("country"))
    _validate_country(session, user, selected_country)
    if ordering_brands(session, user) == set():
        raise HTTPException(403, "No brands assigned / 尚未分配品牌")
    result = create_pi_header(session, body, user.name)
    session.commit()
    return result


@router.get("/pi/{pi_code}")
def get_pi_order(
    pi_code: str,
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("viewer")),
) -> dict:
    return _accessible_pi_detail(session, user, pi_code.upper())


def _invoice_detail(session: Session, user: UserContext, pi_code: str) -> dict:
    detail = get_pi_detail(session, pi_code.upper())
    _validate_whole_pi_access(session, user, detail)
    return detail


def _template_scope(session: Session, user: UserContext, pi_code: str) -> tuple:
    detail = _invoice_detail(session, user, pi_code)
    country = str(detail["header"].get("orderingAccountCode") or detail["header"]["countryCode"]).upper()
    brands = {str(line.get("brand") or "").upper() for line in detail["lines"]}
    if not brands <= set(ORDERING_BRANDS):
        raise HTTPException(409, "PI brand identity missing or unknown / PI品牌身份缺失或未知")
    _validate_country(session, user, country)
    for brand in brands:
        validate_brand_access(session, user, brand)
    if not brands or "" in brands:
        raise HTTPException(409, "PI brand identity missing / PI品牌身份缺失")
    return detail, country, brands


def _selected_template(session: Session, user: UserContext, pi_code: str, template_id: str = "") -> dict | None:
    _, country, brands = _template_scope(session, user, pi_code)
    return _scoped_template(user, country, brands, template_id)


def _scoped_template(user: UserContext, country: str, brands: set[str], template_id: str = "") -> dict | None:
    if not template_id:
        items = invoice_templates.catalog(user.name, is_admin(user.role), country, brands)["items"]
        template_id = items[0]["id"] if items else ""
    if user.role == "viewer" and template_id and template_id != f"{country}_OJ":
        raise HTTPException(403, "Personal PI templates require a business role / 个人PI模板需业务角色")
    if not template_id or template_id == f"{country}_OJ":
        return None
    return invoice_templates.load_template(template_id, user.name, is_admin(user.role), country, brands)


def _template_error(exc: Exception) -> HTTPException:
    return HTTPException(403 if isinstance(exc, PermissionError) else 404 if isinstance(exc, FileNotFoundError) else 409,
                         "Template file missing / 模板文件缺失，请重新上传" if isinstance(exc, FileNotFoundError) else str(exc))


def _management_scope(session: Session, user: UserContext, country: str, brands: list[str]) -> tuple[str, set[str]]:
    country = str(country or "").strip().upper()
    if len(country) != 2 or not country.isascii() or not country.isalpha():
        raise HTTPException(400, "Choose one country / 请选择一个国家")
    if not isinstance(brands, list) or not brands or not all(isinstance(brand, str) for brand in brands):
        raise HTTPException(400, "Choose at least one brand / 请至少选择一个品牌")
    selected = {brand.strip().upper() for brand in brands}
    if not selected <= set(ORDERING_BRANDS):
        raise HTTPException(400, "Unknown template brand / 模板品牌未知")
    _validate_country(session, user, country)
    for brand in selected:
        validate_brand_access(session, user, brand)
    return country, selected


def _template_catalog(user: UserContext, country: str, brands: set[str], owner: str) -> dict:
    try:
        return {**invoice_templates.catalog(user.name, is_admin(user.role), country, brands, owner),
                "country": country, "brands": sorted(brands), "admin": is_admin(user.role), "actor": user.name}
    except (ValueError, PermissionError, FileNotFoundError) as exc:
        raise _template_error(exc) from exc


@router.get("/pi/{pi_code}/invoice-templates")
def list_invoice_templates(pi_code: str, owner: str = "", session: Session = Depends(get_db_session),
                           user: UserContext = Depends(require_roles("order_filler", "editor", "admin"))) -> dict:
    _, country, brands = _template_scope(session, user, pi_code)
    return _template_catalog(user, country, brands, owner)


@router.get("/invoice-templates")
def list_scoped_invoice_templates(country: str, brands: str, owner: str = "", session: Session = Depends(get_db_session),
                                  user: UserContext = Depends(require_roles("order_filler", "editor", "admin"))) -> dict:
    country, selected = _management_scope(session, user, country, brands.split(","))
    return _template_catalog(user, country, selected, owner)


def _monthly_invoice_source(session: Session, user: UserContext, country: str, month: str,
                            scope: str = "single", pi_code: str = "") -> tuple:
    country = country.strip().upper()
    if len(country) != 2 or not country.isascii() or not country.isalpha():
        raise HTTPException(400, "Choose one country / 请选择一个国家")
    try:
        date.fromisoformat(f"{month}-01")
    except (ValueError, TypeError):
        raise HTTPException(400, "Choose YYYY-MM / 请选择有效年月") from None
    _validate_country(session, user, country)
    if scope not in {"single", "monthly"}:
        raise HTTPException(400, "Choose single or monthly / 请选择单批或当月汇总")
    batches = []
    page = 1
    while True:
        result = list_pi_headers(session, country=country, month=month, page=page, page_size=200,
                                 allowed_countries=ordering_countries(session, user), allowed_brands=ordering_brands(session, user))
        batches.extend(row for row in result["items"] if row["status"] != "cancelled"
                       and (row.get("orderingAccountCode") or row["countryCode"]) == country)
        if page * 200 >= result["total"]:
            break
        page += 1
    if not batches:
        allowed = ordering_brands(session, user)
        if allowed == set():
            raise HTTPException(403, "No authorized brands / 尚无授权品牌")
        return None, country, set(ORDERING_BRANDS if allowed is None else allowed), []
    selected = batches if scope == "monthly" else [next((row for row in batches if row["piCode"] == pi_code), batches[0])]
    if scope == "single" and pi_code and selected[0]["piCode"] != pi_code:
        raise HTTPException(409, {"code": "source_changed", "message": "Selected PI is no longer available / 所选PI已不可用",
                                  "suggestion": "Reload and choose a saved batch. / 请重读并选择已保存批次。"})
    details = [_template_scope(session, user, row["piCode"])[0] for row in selected]
    header = details[0]["header"] if scope == "single" else {
        "piCode": f"Monthly-Summary-{country}-{month.replace('-', '')}", "countryCode": country}
    detail = {"header": header, "lines": [line for item in details for line in item["lines"]],
              "vehicles": [vehicle for item in details for vehicle in item["vehicles"]],
              "sourcePiCodes": [row["piCode"] for row in selected], "summaryExport": scope == "monthly"}
    brands = {line["brand"].upper() for line in detail["lines"]}
    # Detect changed batch membership or invoice snapshots, not unrelated logistics edits.
    snapshot = [detail["sourcePiCodes"], {key: detail["header"].get(key) for key in ("piCode", "officialPiNo", "orderDate", "portOfDischarge")},
                [(line["piLineCode"], line["quantity"]) for line in detail["lines"]],
                [{key: vehicle.get(key) for key in ("carCode", "piLineCode", "brand", "modelName", "version", "powertrain", "materialCode", "exteriorColorName", "exteriorColorCode", "interiorColorName", "fobEur", "freightEur", "insuranceEur")} for vehicle in detail["vehicles"]]]
    detail["sourceRevision"] = hashlib.sha256(json.dumps(snapshot, sort_keys=True, default=str).encode()).hexdigest()
    return detail, country, brands, batches


@router.get("/monthly-invoice")
def preview_monthly_invoice(country: str, month: str, template_id: str = "", scope: str = "single", pi_code: str = "", session: Session = Depends(get_db_session),
                            user: UserContext = Depends(require_roles("order_filler", "editor", "admin"))) -> dict:
    try:
        detail, country, brands, batches = _monthly_invoice_source(session, user, country, month, scope, pi_code)
        catalog = _template_catalog(user, country, brands, "")
        if detail is None:
            return {"catalog": catalog, "batches": [], "context": None, "issue": {"code": "no_saved_pi", "country": country, "month": month,
                "message": "No saved PI for this country/month. A template is not an order. / 该国家当月尚无已保存PI，模板不等于订单。",
                "suggestion": "Select pending quantities and create a PI in PI Batch, then export one batch or the monthly summary. / 请到PI Batch勾选待入批数量并创建PI，再导出单批或当月汇总。"}}
        try:
            template = _scoped_template(user, country, brands, template_id) if catalog["items"] or template_id else None
            context = {**pi_invoice_context(detail, template), "sourceRevision": detail["sourceRevision"],
                       "sourcePiCodes": detail["sourcePiCodes"]} if catalog["items"] or template_id else None
        except (ValueError, FileNotFoundError) as exc:
            return {"catalog": catalog, "batches": batches, "context": None, "error": str(_template_error(exc).detail)}
        return {"catalog": catalog, "batches": batches, "context": context}
    except (ValueError, PermissionError, FileNotFoundError) as exc:
        raise _template_error(exc) from exc


@router.post("/monthly-invoice")
def export_monthly_invoice(country: str, month: str, body: dict, session: Session = Depends(get_db_session),
                           user: UserContext = Depends(require_roles("order_filler", "editor", "admin"))) -> StreamingResponse:
    try:
        detail, country, brands, _ = _monthly_invoice_source(session, user, country, month, body.get("scope", "single"), body.get("piCode", ""))
        if detail is None:
            raise HTTPException(409, {"code": "no_saved_pi", "country": country, "month": month,
                "message": "No saved PI / 尚无已保存PI", "suggestion": "Create a batch in PI Batch first. / 请先到PI Batch创建批次。"})
        if body.get("sourceRevision") != detail["sourceRevision"]:
            raise HTTPException(409, {"code": "source_changed", "message": "Saved PI source changed / 已保存PI来源已变化",
                "suggestion": "Reload the source and review quantities/prices; your fields are retained. / 请重读来源并核对数量/成交价，当前输入已保留。"})
        template = _scoped_template(user, country, brands, body.get("templateId", ""))
        if template and body.get("templateRevision") != template["updatedAt"]:
            raise HTTPException(409, {"code": "template_changed", "message": "Template changed since preview / 模板自预览后有变化",
                "suggestion": "Reload the template, review fixed details, then export. Entered costs are retained. / 请重读模板并核对固定资料后导出，已填费用保留。"})
        buf = generate_pi_invoice_excel(detail, body, template)
    except (ValueError, PermissionError, FileNotFoundError) as exc:
        raise _template_error(exc) from exc
    filename = f"Proforma_{detail['header']['piCode']}.xlsx"
    return StreamingResponse(buf, media_type=XLSX_MEDIA_TYPE,
                             headers={"Content-Disposition": f'attachment; filename="{filename}"'})


@router.post("/invoice-templates/preview")
async def parse_invoice_template(country: str, brands: str, file: UploadFile = File(...), session: Session = Depends(get_db_session),
                                 user: UserContext = Depends(require_roles("order_filler", "editor", "admin"))) -> dict:
    try:
        _management_scope(session, user, country, brands.split(","))
        return invoice_templates.analyse(await file.read(invoice_templates.MAX_TEMPLATE_BYTES + 1))
    except ValueError as exc:
        raise _template_error(exc) from exc
    finally:
        await file.close()


def _save_template(session: Session, user: UserContext, content: bytes, payload: dict) -> dict:
    country, brands = _management_scope(session, user, payload.get("country"), payload.get("brands"))
    if payload.get("id"):
        invoice_templates.load_template(payload["id"], user.name, is_admin(user.role), country, brands)
    return invoice_templates.save(content, {**payload, "country": country, "brands": sorted(brands)}, user.name, is_admin(user.role))


@router.post("/invoice-templates")
async def import_invoice_template(metadata: str = Form(...), file: UploadFile = File(...),
                                  session: Session = Depends(get_db_session),
                                  user: UserContext = Depends(require_roles("order_filler", "editor", "admin"))) -> dict:
    try:
        payload = json.loads(metadata)
        if not isinstance(payload, dict):
            raise ValueError("Invalid template metadata / 模板资料无效")
        return _save_template(session, user, await file.read(invoice_templates.MAX_TEMPLATE_BYTES + 1), payload)
    except (ValueError, PermissionError, FileNotFoundError, KeyError) as exc:
        raise _template_error(exc) from exc
    finally:
        await file.close()


@router.patch("/invoice-templates/{template_id}")
def update_invoice_template(template_id: str, body: dict, session: Session = Depends(get_db_session),
                            user: UserContext = Depends(require_roles("order_filler", "editor", "admin"))) -> dict:
    try:
        country, brands = _management_scope(session, user, body.get("country"), body.get("brands"))
        source = invoice_templates.load_template(template_id, user.name, is_admin(user.role), country, brands)
        return _save_template(session, user, source["path"].read_bytes(), {**body, "id": template_id})
    except (ValueError, PermissionError, FileNotFoundError) as exc:
        raise _template_error(exc) from exc


@router.post("/invoice-templates/{template_id}/copy")
def copy_invoice_template(template_id: str, country: str, brands: str, session: Session = Depends(get_db_session),
                          user: UserContext = Depends(require_roles("order_filler", "editor", "admin"))) -> dict:
    try:
        country, selected = _management_scope(session, user, country, brands.split(","))
        source = invoice_templates.load_template(template_id, user.name, is_admin(user.role), country, selected)
        return _save_template(session, user, source["path"].read_bytes(),
                              {**source, "id": "", "name": source["name"][:93] + " (copy)", "brands": sorted(selected), "shared": False})
    except (ValueError, PermissionError, FileNotFoundError) as exc:
        raise _template_error(exc) from exc


@router.delete("/invoice-templates/{template_id}")
def delete_invoice_template(template_id: str, country: str, brands: str, session: Session = Depends(get_db_session),
                            user: UserContext = Depends(require_roles("order_filler", "editor", "admin"))) -> dict:
    country, selected = _management_scope(session, user, country, brands.split(","))
    try:
        invoice_templates.remove(template_id, user.name, is_admin(user.role), country, selected)
        return {"deleted": True}
    except (ValueError, PermissionError, FileNotFoundError) as exc:
        raise _template_error(exc) from exc


@router.put("/invoice-template-order")
def order_invoice_templates(body: dict, session: Session = Depends(get_db_session),
                            user: UserContext = Depends(require_roles("order_filler", "editor", "admin"))) -> dict:
    country, brands = _management_scope(session, user, body.get("country"), body.get("brands"))
    try:
        owner = body.get("owner", "")
        if owner and owner != user.name:
            if not is_admin(user.role):
                raise PermissionError("Only admin can manage another owner / 仅管理员可管理其他账号")
            if not session.query(User).filter(User.username == owner).first():
                raise ValueError("Target account does not exist / 目标账号不存在")
        invoice_templates.reorder(user.name, is_admin(user.role), country, brands, body.get("ids"), body.get("owner", ""))
        return {"saved": True}
    except (ValueError, PermissionError) as exc:
        raise _template_error(exc) from exc


@router.get("/pi/{pi_code}/invoice")
def preview_pi_invoice(pi_code: str, session: Session = Depends(get_db_session),
                       user: UserContext = Depends(require_min_role("viewer"))) -> dict:
    try:
        return pi_invoice_context(_invoice_detail(session, user, pi_code), _selected_template(session, user, pi_code))
    except (ValueError, PermissionError, FileNotFoundError) as exc:
        raise _template_error(exc) from exc


@router.get("/pi/{pi_code}/invoice-context/{template_id}")
def selected_invoice_context(pi_code: str, template_id: str, session: Session = Depends(get_db_session),
                              user: UserContext = Depends(require_roles("order_filler", "editor", "admin"))) -> dict:
    try:
        return pi_invoice_context(_invoice_detail(session, user, pi_code), _selected_template(session, user, pi_code, template_id))
    except (ValueError, PermissionError, FileNotFoundError) as exc:
        raise _template_error(exc) from exc


@router.post("/pi/{pi_code}/invoice")
def export_pi_invoice(pi_code: str, body: dict,
                      session: Session = Depends(get_db_session),
                      user: UserContext = Depends(require_min_role("viewer"))) -> StreamingResponse:
    try:
        detail = _invoice_detail(session, user, pi_code)
        template = _selected_template(session, user, pi_code, body.get("templateId", ""))
        if template and body.get("templateRevision") != template["updatedAt"]:
            raise ValueError("Template changed since preview; reload before exporting / 模板自预览后有变化，请重读再导出")
        buf = generate_pi_invoice_excel(detail, body, template)
    except (ValueError, PermissionError, FileNotFoundError) as exc:
        raise _template_error(exc) from exc
    # Use the saved canonical identifier, not arbitrary request text, as a filename.
    filename = f"Proforma_{detail['header']['piCode']}.xlsx"
    return StreamingResponse(buf, media_type=XLSX_MEDIA_TYPE,
                             headers={"Content-Disposition": f'attachment; filename="{filename}"'})


@router.patch("/pi/{pi_code}")
def patch_pi_order(
    pi_code: str,
    body: dict,
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_roles("order_filler", "editor", "admin")),
) -> dict:
    detail = get_pi_detail(session, pi_code.upper())
    _validate_whole_pi_access(session, user, detail)
    for country in body.get("marketCountryCodes", []):
        _validate_country(session, user, _country(country))
    result = update_pi_header(session, pi_code.upper(), body, user.name)
    session.commit()
    return result


@router.delete("/pi/{pi_code}")
def delete_pi_order(
    pi_code: str,
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_roles("editor", "admin")),
) -> dict:
    """Delete only when the entire PI, including hidden lines, is authorized."""
    _validate_whole_pi_access(session, user, get_pi_detail(session, pi_code.upper()))
    try:
        result = delete_pi(session, pi_code.upper())
        session.commit()
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/pi/{pi_code}/lines")
def create_pi_order_line(
    pi_code: str,
    body: dict,
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_roles("order_filler", "editor", "admin")),
) -> dict:
    detail = get_pi_detail(session, pi_code.upper())
    _validate_pi_detail_access(session, user, detail)
    _validate_country(session, user, detail["header"]["countryCode"])
    sku = ordering_repo.get_sku_by_material_code_any_status(session, str(body.get("materialCode") or ""))
    validate_brand_access(session, user, sku.brand if sku else body.get("brand"))
    if "brand" in body:
        validate_brand_access(session, user, body["brand"])
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
    _validate_line_countries(session, user, detail, existing.pi_line_code)
    if "brand" in body:
        validate_brand_access(session, user, body["brand"])
    if "materialCode" in body:
        sku = ordering_repo.get_sku_by_material_code_any_status(session, str(body["materialCode"] or ""))
        validate_brand_access(session, user, sku.brand if sku else None)
    if "quantity" in body:
        _validate_country(session, user, detail["header"]["countryCode"])
    line = update_pi_line(session, pi_line_code.upper(), body, user.name)
    session.commit()
    return line


@router.delete("/lines/{pi_line_code}")
def delete_pi_order_line(
    pi_line_code: str,
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_roles("order_filler", "editor", "admin")),
) -> dict:
    """Hard-delete a PI line and its allocations, vehicles."""
    existing = vehicle_repo.get_line_by_code(session, pi_line_code.upper())
    if not existing:
        raise HTTPException(status_code=404, detail="PI line not found")
    _validate_line_countries(session, user, get_pi_detail(session, existing.pi_code), existing.pi_line_code)
    try:
        result = delete_pi_line(session, pi_line_code.upper())
        session.commit()
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/vehicles")
def list_vehicles(
    keyword: str | None = Query(default=None),
    pi_code: str | None = Query(default=None),
    pi_line_code: str | None = Query(default=None),
    car_code: str | None = Query(default=None),
    vin: str | None = Query(default=None),
    material_code: str | None = Query(default=None),
    bom: str | None = Query(default=None),
    brand: str | None = Query(default=None),
    model_name: str | None = Query(default=None),
    version: str | None = Query(default=None),
    powertrain: str | None = Query(default=None),
    exterior_color_name: str | None = Query(default=None),
    interior_color_name: str | None = Query(default=None),
    order_month: str | None = Query(default=None),
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
        brand=brand,
        model_name=model_name,
        version=version,
        powertrain=powertrain,
        exterior_color_name=exterior_color_name,
        interior_color_name=interior_color_name,
        order_month=order_month,
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
    filters.update(allowed_countries=ordering_countries(session, user), allowed_brands=ordering_brands(session, user))
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


@router.post("/vehicles/bulk-update")
def bulk_update_vehicles(
    body: dict,
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_roles("order_filler", "editor", "admin")),
) -> dict:
    result = bulk_update_vehicle_units(session, body, user.name,
                                      allowed_countries=ordering_countries(session, user),
                                      allowed_brands=ordering_brands(session, user))
    session.commit()
    return result


@router.get("/status-flow")
def get_status_flow_config(
    country: str | None = Query(default=None),
    ordering_account: str | None = Query(default=None),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("viewer")),
) -> dict:
    selected_country = _country(country)
    _validate_optional_country(session, user, selected_country)
    return get_vehicle_status_flow_config(selected_country, _clean(ordering_account))


@router.get("/custom-columns")
def list_custom_columns(session: Session = Depends(get_db_session),
                        user: UserContext = Depends(require_min_role("viewer"))) -> dict:
    columns = session.scalars(select(VehicleCustomColumn).where(VehicleCustomColumn.is_active.is_(True))
                              .order_by(VehicleCustomColumn.created_at_utc, VehicleCustomColumn.column_id)).all()
    return {"items": [{"id": c.column_id, "label": c.label, "createdBy": c.created_by,
                       "canDelete": is_admin(user.role) or c.created_by == user.name} for c in columns]}


@router.post("/custom-columns")
def create_custom_column(body: dict, session: Session = Depends(get_db_session),
                         user: UserContext = Depends(require_min_role("viewer"))) -> dict:
    from app.services.order_genius_vehicle_exporter import COLUMN_LABELS
    label = body.get("label")
    reserved = {text.casefold() for text in [*COLUMN_LABELS, *COLUMN_LABELS.values()]}
    if not isinstance(label, str) or not label.strip() or len(label.strip()) > 60 or label.strip().casefold() in reserved:
        raise HTTPException(400, "Use 1–60 characters, not a system column / 请填1–60字且非系统列的名称")
    if session.scalar(select(VehicleCustomColumn.column_id).where(VehicleCustomColumn.is_active.is_(True),
            func.lower(VehicleCustomColumn.label) == label.strip().lower())):
        raise HTTPException(409, "Column name already exists / 列名已存在")
    column = VehicleCustomColumn(label=label.strip(), created_by=user.name)
    session.add(column)
    from sqlalchemy.exc import IntegrityError
    try:
        session.commit()
    except IntegrityError:
        session.rollback()
        raise HTTPException(409, "Column name already exists / 列名已存在") from None
    return {"id": column.column_id, "label": column.label, "createdBy": column.created_by, "canDelete": True}


@router.delete("/custom-columns/{column_id}")
def delete_custom_column(column_id: str, session: Session = Depends(get_db_session),
                         user: UserContext = Depends(require_min_role("viewer"))) -> dict:
    column = session.scalar(select(VehicleCustomColumn).where(VehicleCustomColumn.column_id == column_id).with_for_update())
    if column is None:
        raise HTTPException(404, "Custom column not found; system columns cannot be deleted / 自定义列不存在，系统列不可删除")
    if not is_admin(user.role) and column.created_by != user.name:
        raise HTTPException(403, "Only the creator or Admin can delete / 仅创建者或Admin可删除")
    column.is_active = False
    session.commit()
    return {"deleted": True}


@router.post("/custom-values")
def update_custom_values(body: dict, session: Session = Depends(get_db_session),
                         user: UserContext = Depends(require_min_role("viewer"))) -> dict:
    # Viewing access grants only shared-text editing, never logistics, VIN or prices.
    if set(body) - {"carCodes", "rowVersions", "fields"} or not isinstance(body.get("fields"), dict) or set(body["fields"]) != {"customValues"}:
        raise HTTPException(400, "Only shared custom values / 此入口仅可填写自定义列")
    result = bulk_update_vehicle_units(session, body, user.name, allowed_countries=ordering_countries(session, user),
                                      allowed_brands=ordering_brands(session, user))
    session.commit()
    return result


@router.post("/search")
def search_allocation(
    body: dict,
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("viewer")),
) -> dict:
    keyword, vins = body.get("keyword", ""), body.get("vins")
    if not isinstance(keyword, str) or (vins is not None and (not isinstance(vins, list) or
            any(not isinstance(vin, str) for vin in vins))):
        raise HTTPException(400, "Invalid search / 搜索格式不正确")
    page = body.get("page", 1)
    if not isinstance(page, int) or isinstance(page, bool) or page < 1:
        raise HTTPException(400, "Invalid page / 页码不正确")
    return search_vehicle_allocation(session, keyword, vins=vins, page=page,
        allowed_countries=ordering_countries(session, user), allowed_brands=ordering_brands(session, user))


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
    return get_order_matrix_allocation_plan(session, selected_country or "", year, month,
                                           allowed_brands=ordering_brands(session, user))


@router.post("/generate-from-order-matrix")
def generate_pi_from_order_matrix(
    body: dict,
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_roles("order_filler", "editor", "admin")),
) -> dict:
    selected_country = _country(body.get("countryCode"))
    _validate_country(session, user, selected_country)
    if body.get("allowDuplicate") and not is_admin(user.role):
        raise HTTPException(403, "Duplicate generation requires admin / 重复生成仅限管理员")
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
    result = generate_from_order_matrix(session, body, user.name, allowed_brands=ordering_brands(session, user))
    session.commit()
    return result


@router.post("/import/preview")
def preview_import(
    file: UploadFile = File(...),
    pi_code: str | None = Query(default=None),
    pi_codes: str | None = Query(default=None),
    include_hidden: bool = Query(default=False),
    allow_replacing: bool = Query(default=False),
    remove_vins: bool = Query(default=False),
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
        rows = parse_vehicle_allocation_xlsx(tmp_path, vin_fill=bool(pi_code or pi_codes))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to parse file: {exc}") from exc
    finally:
        tmp_path.unlink(missing_ok=True)
    file_rows = len(rows)
    visible_rows = sum(not row.get("sourceHidden") for row in rows)
    if not include_hidden:
        rows = [row for row in rows if not row.get("sourceHidden")]
    preview = _store_import_preview(session, user, rows, import_id, _clean(pi_code.upper()) if pi_code else None,
        allow_replacing, remove_vins=remove_vins is True, pi_codes=pi_codes.split(",") if pi_codes is not None else None)
    return {**preview, "fileRows": file_rows, "visibleRows": visible_rows, "includeHidden": include_hidden}


@router.post("/import/vin-list")
def extract_vin_list_import(
    file: UploadFile = File(...),
    user: UserContext = Depends(require_roles("order_filler", "editor", "admin")),
) -> dict:
    IMPORT_SESSION_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = IMPORT_SESSION_DIR / f"{uuid4()}-vin-list.xlsx"
    try:
        with tmp_path.open("wb") as handle:
            shutil.copyfileobj(file.file, handle)
    finally:
        file.file.close()

    try:
        vins = parse_vehicle_vin_list_xlsx(tmp_path)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to parse VIN file: {exc}") from exc
    finally:
        tmp_path.unlink(missing_ok=True)

    return {
        "fileName": file.filename,
        "totalRows": len(vins),
        "vins": vins,
        "uploadedBy": user.name,
    }


@router.post("/import/preview-rows")
def preview_import_rows(
    body: object = Body(...),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_roles("order_filler", "editor", "admin")),
) -> dict:
    rows = _normalise_import_rows_payload(body)
    import_id = str(uuid4())
    pi_code = _clean(body.get("piCode")) if isinstance(body, dict) else None
    allow_replacing = body.get("allowReplacing") is True if isinstance(body, dict) else False
    remove_vins = body.get("removeVins") is True if isinstance(body, dict) else False
    return _store_import_preview(session, user, rows, import_id, pi_code.upper() if pi_code else None,
                                 allow_replacing, source="parsed_rows", remove_vins=remove_vins,
                                 pi_codes=body.get("piCodes") if isinstance(body, dict) else None)


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

    vin_preview = payload.get("vinPreview")
    if vin_preview:
        if payload.get("owner") != user.name:
            raise HTTPException(403, "Preview belongs to another user. Upload again / 此预览属于其他账号，请重新上传")
        if not vin_preview.get("piCodes"):
            raise HTTPException(409, "Preview expired; upload again / 预览已过期，请重新上传")
        countries = set().union(*(_vin_import_countries(session, user, code) for code in vin_preview["piCodes"]))
        result = apply_vehicle_import(session, rows, user.name, vin_preview=vin_preview,
                                      allowed_countries=countries, allowed_brands=ordering_brands(session, user))
    else:
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
    pi_codes = payload.get("piCodes")
    if pi_codes is not None and (not isinstance(pi_codes, list) or not pi_codes or len(pi_codes) > 100
                                or any(not isinstance(code, str) or not code.strip() for code in pi_codes)):
        raise HTTPException(400, "Select valid PIs / 请选择有效PI")
    filters = _vehicle_filters(
        keyword=payload.get("keyword"),
        pi_code=payload.get("piCode") or payload.get("pi_code"),
        pi_line_code=payload.get("piLineCode") or payload.get("pi_line_code"),
        car_code=payload.get("carCode") or payload.get("car_code"),
        vin=payload.get("vin"),
        material_code=payload.get("materialCode") or payload.get("material_code"),
        bom=payload.get("bom"),
        brand=payload.get("brand"),
        model_name=payload.get("modelName") or payload.get("model_name"),
        version=payload.get("version"),
        powertrain=payload.get("powertrain"),
        exterior_color_name=payload.get("exteriorColorName") or payload.get("exterior_color_name"),
        interior_color_name=payload.get("interiorColorName") or payload.get("interior_color_name"),
        order_month=payload.get("orderMonth") or payload.get("order_month"),
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
    if pi_codes is not None:
        filters["pi_codes"] = list(dict.fromkeys(code.strip().upper() for code in pi_codes))
        for code in filters["pi_codes"]:
            _accessible_pi_detail(session, user, code)
    columns = payload.get("columns")
    car_codes = payload.get("carCodes")
    if columns is not None and (not isinstance(columns, list) or len(columns) > 40 or any(not isinstance(key, str) for key in columns)):
        raise HTTPException(400, "Invalid export columns / 导出列无效")
    if car_codes is not None and (not isinstance(car_codes, list) or len(car_codes) > 50000 or any(not isinstance(code, str) for code in car_codes)):
        raise HTTPException(400, "Invalid vehicle selection / 车辆范围无效")
    if car_codes is not None:
        for code in car_codes:
            vehicle = vehicle_repo.get_vehicle_by_car_code(session, code)
            if vehicle is None:
                raise HTTPException(400, "Selected vehicle not found / 勾选车辆不存在")
            _validate_vehicle_access(session, user, {"countryCode": vehicle.country_code, "brand": vehicle.brand})
    if filters["country"]:
        _validate_country(session, user, filters["country"])
    filters.update(allowed_countries=ordering_countries(session, user), allowed_brands=ordering_brands(session, user))
    if not is_admin(user.role) and filters["pi_code"]:
        detail = _accessible_pi_detail(session, user, filters["pi_code"])
        allowed_codes = {vehicle["carCode"] for vehicle in detail["vehicles"]}
        if car_codes is not None and not set(car_codes).issubset(allowed_codes):
            raise HTTPException(403, "Export contains vehicles outside your countries / 导出包含无权访问的国家车辆")
        if car_codes is None:
            rows, _ = vehicle_repo.list_vehicles(session, **filters)
            car_codes = [vehicle.car_code for vehicle in rows if vehicle.car_code in allowed_codes]
    else:
        _validate_optional_country(session, user, filters["country"])
    buffer = export_vehicle_units(session, columns=columns, car_codes=car_codes, pivot=payload.get("pivot"), **filters)
    today = date.today().strftime("%Y%m%d")
    country_part = filters["country"] or "ALL"
    filename = f"Vehicle_Allocation_{country_part}_{today}.xlsx"
    return StreamingResponse(
        buffer,
        media_type=XLSX_MEDIA_TYPE,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
