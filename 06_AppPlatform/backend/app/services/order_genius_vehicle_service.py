"""Business logic for Order Genius PI vehicle allocation."""

from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.db.models import PiOrderHeader, PiOrderLine, PiOrderLineAllocation, PiVehicleUnit
from app.infra import order_genius_repository as og_repo
from app.infra import order_genius_vehicle_repository as repo
from app.services.order_genius_vehicle_exporter import generate_vehicle_allocation_excel


PI_STATUSES = {
    "draft", "ordered", "in_production", "shipped", "arrived",
    "ready_for_pickup", "closed", "cancelled",
}
ALLOCATION_STATUSES = {"unallocated", "reserved", "allocated", "delivered", "cancelled"}
LOGISTICS_STATUSES = {
    "pending", "in_production", "ready_for_shipping", "on_vessel",
    "arrived_at_port", "in_warehouse", "ready_for_pickup", "delivered",
}
PI_SCOPE_CODE_RE = r"[A-Z0-9]{2,12}"


def build_pi_code(scope_code: str, year: int, month: int, sequence: int) -> str:
    return f"PI-{scope_code.upper()}-{year}{month:02d}-{sequence:03d}"


def build_pi_line_code(pi_code: str, line_sequence: int) -> str:
    return f"{pi_code}-L{line_sequence:02d}"


def build_car_code(country_code: str, year: int, month: int, pi_sequence: int, line_sequence: int, unit_sequence: int) -> str:
    return f"CAR-{country_code.upper()}-{year % 100:02d}{month:02d}-{pi_sequence:03d}-L{line_sequence:02d}-{unit_sequence:04d}"


def parse_pi_code(pi_code: str) -> dict[str, int | str] | None:
    match = re.fullmatch(rf"PI-({PI_SCOPE_CODE_RE})-(\d{{4}})(\d{{2}})-(\d{{3}})", pi_code.strip().upper())
    if not match:
        return None
    return {
        "countryCode": match.group(1),
        "scopeCode": match.group(1),
        "year": int(match.group(2)),
        "month": int(match.group(3)),
        "sequence": int(match.group(4)),
        "orderMonth": f"{match.group(2)}-{match.group(3)}",
    }


def parse_car_code(car_code: str) -> dict[str, int | str] | None:
    match = re.fullmatch(r"CAR-([A-Z]{2})-(\d{2})(\d{2})-(\d{3})-L(\d{2})-(\d{4})", car_code.strip().upper())
    if not match:
        return None
    return {
        "countryCode": match.group(1),
        "yearSuffix": int(match.group(2)),
        "month": int(match.group(3)),
        "piSequence": int(match.group(4)),
        "lineSequence": int(match.group(5)),
        "unitSequence": int(match.group(6)),
    }


def create_pi_header(session: Session, payload: dict[str, Any], username: str) -> dict:
    country = str(payload.get("countryCode") or payload.get("country") or "").upper()
    if not country:
        raise HTTPException(status_code=400, detail="countryCode is required")
    order_year, order_month_num, order_month = _resolve_order_month(payload)
    ordering_account_code = _account_code(payload.get("orderingAccountCode") or country)
    market_country_codes = _market_country_codes(payload.get("marketCountryCodes"), country)
    seq = repo.next_pi_sequence_for_account(session, ordering_account_code, order_month)
    pi_code = build_pi_code(ordering_account_code, order_year, order_month_num, seq)
    header = PiOrderHeader(
        pi_code=pi_code,
        official_pi_no=_clean(payload.get("officialPiNo")),
        country_code=country,
        country_name=_clean(payload.get("countryName")),
        ordering_account_code=ordering_account_code,
        ordering_account_name=_clean(payload.get("orderingAccountName")) or _clean(payload.get("countryName")),
        market_country_codes=market_country_codes,
        shipment_batch_code=_clean(payload.get("shipmentBatchCode")),
        port_of_discharge=_clean(payload.get("portOfDischarge")),
        order_date=_parse_date(payload.get("orderDate")),
        order_month=order_month,
        pi_sequence_no=seq,
        status=_status(payload.get("status"), PI_STATUSES, "draft"),
        remark=_clean(payload.get("remark")),
        created_by=username,
        updated_by=username,
    )
    _apply_header_updates(header, payload, username, allow_status=True)
    repo.add_header(session, header)
    return header_to_dict(header)


def update_pi_header(session: Session, pi_code: str, payload: dict[str, Any], username: str) -> dict:
    header = repo.get_header_by_code(session, pi_code)
    if not header:
        raise HTTPException(status_code=404, detail="PI not found")
    _apply_header_updates(header, payload, username, allow_status=True)
    header.row_version += 1
    session.flush()
    return header_to_dict(header)


def create_pi_line(session: Session, pi_code: str, payload: dict[str, Any], username: str) -> dict:
    header = repo.get_header_by_code(session, pi_code)
    if not header:
        raise HTTPException(status_code=404, detail="PI not found")
    line_seq = int(payload.get("lineSequenceNo") or repo.next_line_sequence(session, header.pi_id))
    material_code = _clean(payload.get("materialCode"))
    line_payload = _line_payload_from_material(session, header.country_code, material_code, payload)
    line = _build_line(session, header, line_seq, line_payload, username)
    repo.add_line(session, line)
    _add_line_allocations(
        session,
        header,
        line,
        [{"countryCode": header.country_code, "quantity": line.quantity, "fobEur": line.fob_eur}],
        username,
    )
    _ensure_vehicle_units_for_line(session, header, line, username)
    result = line_to_dict(line)
    result["allocations"] = [allocation_to_dict(row) for row in repo.list_allocations_by_line(session, line.pi_line_id)]
    return result


def update_pi_line(session: Session, pi_line_code: str, payload: dict[str, Any], username: str) -> dict:
    line = repo.get_line_by_code(session, pi_line_code)
    if not line:
        raise HTTPException(status_code=404, detail="PI line not found")
    header = repo.get_header_by_code(session, line.pi_code)
    if not header:
        raise HTTPException(status_code=404, detail="PI not found")
    material_code = _clean(payload.get("materialCode")) if "materialCode" in payload else line.material_code
    payload = _line_payload_from_material(session, header.country_code, material_code, payload)
    for attr, key in {
        "material_code": "materialCode",
        "bom": "bom",
        "brand": "brand",
        "model_name": "modelName",
        "version": "version",
        "powertrain": "powertrain",
        "exterior_color_name": "exteriorColorName",
        "exterior_color_code": "exteriorColorCode",
        "interior_color_name": "interiorColorName",
        "interior_colour_code": "interiorColourCode",
        "remark": "remark",
    }.items():
        if key in payload:
            setattr(line, attr, _clean(payload.get(key)))
    quantity_changed = "quantity" in payload
    if quantity_changed:
        line.quantity = _non_negative_int(payload["quantity"], "quantity")
    if "fobEur" in payload:
        line.fob_eur = _decimal_or_none(payload.get("fobEur"))
    line.amount_eur = _amount(line.quantity, line.fob_eur)
    line.updated_by = username
    line.row_version += 1
    session.flush()
    if quantity_changed:
        _sync_default_line_allocation(session, header, line, username)
        _ensure_vehicle_units_for_line(session, header, line, username)
    _sync_vehicles_from_line(session, line, username)
    result = line_to_dict(line)
    result["allocations"] = [allocation_to_dict(row) for row in repo.list_allocations_by_line(session, line.pi_line_id)]
    return result


def generate_from_order_matrix(session: Session, payload: dict[str, Any], username: str) -> dict:
    country = str(payload.get("countryCode") or "").upper()
    year = int(payload.get("orderYear") or 0)
    month = int(payload.get("orderMonth") or 0)
    if not country or year < 2000 or month < 1 or month > 12:
        raise HTTPException(status_code=400, detail="countryCode, orderYear, orderMonth are required")
    line_items = payload.get("lineItems")
    explicit_line_items = line_items is not None
    if line_items is None:
        plan = get_order_matrix_allocation_plan(session, country, year, month)
        line_items = plan["selectedLineItems"] if _truthy(payload.get("allowDuplicate")) else plan["remainingLineItems"]
    if not isinstance(line_items, list):
        raise HTTPException(status_code=400, detail="lineItems must be a list")
    if explicit_line_items and not _truthy(payload.get("allowDuplicate")):
        _validate_line_items_against_remaining(session, country, year, month, line_items)
    if not line_items:
        raise HTTPException(status_code=400, detail="No remaining positive order quantities found for this country/month")
    market_country_codes = _market_country_codes_for_line_items(
        payload.get("marketCountryCodes"),
        country,
        line_items,
    )

    header = create_pi_header(session, {
        "countryCode": country,
        "countryName": payload.get("countryName"),
        "orderYear": year,
        "orderMonth": month,
        "officialPiNo": payload.get("officialPiNo"),
        "orderDate": payload.get("orderDate"),
        "orderingAccountCode": payload.get("orderingAccountCode"),
        "orderingAccountName": payload.get("orderingAccountName"),
        "marketCountryCodes": market_country_codes,
        "shipmentBatchCode": payload.get("shipmentBatchCode"),
        "portOfDischarge": payload.get("portOfDischarge"),
        "shipName": payload.get("shipName"),
        "etd": payload.get("etd"),
        "eta": payload.get("eta"),
        "readyForPickupDate": payload.get("readyForPickupDate"),
        "status": payload.get("status") or "draft",
        "remark": payload.get("remark"),
    }, username)
    header_model = repo.get_header_by_code(session, header["piCode"])
    if not header_model:
        raise HTTPException(status_code=500, detail="Failed to create PI")

    line_count = 0
    vehicle_count = 0
    for idx, item in enumerate(line_items, start=1):
        if not isinstance(item, dict):
            raise HTTPException(status_code=400, detail="lineItems must contain objects")
        allocations = _line_item_allocations(item, country)
        quantity = sum(int(allocation["quantity"]) for allocation in allocations)
        if quantity <= 0:
            continue
        material_code = _clean(item.get("materialCode"))
        line_payload = _line_payload_from_material(session, country, material_code, item)
        line_payload["quantity"] = quantity
        line = _build_line(session, header_model, idx, line_payload, username)
        repo.add_line(session, line)
        _add_line_allocations(session, header_model, line, allocations, username)
        line_count += 1
        _ensure_vehicle_units_for_line_allocations(session, header_model, line, allocations, username)
        vehicle_count += quantity

    return {"piCode": header_model.pi_code, "lineCount": line_count, "vehicleCount": vehicle_count}


def get_order_matrix_allocation_plan(session: Session, country: str, year: int, month: int) -> dict:
    country_code = country.upper()
    if not country_code or year < 2000 or month < 1 or month > 12:
        raise HTTPException(status_code=400, detail="country, year, month are required")

    order_month = f"{year}-{month:02d}"
    selected_items = _line_items_from_order_quantities(session, country_code, year, month)
    selected_by_material = {
        str(item["materialCode"]): item
        for item in selected_items
        if item.get("materialCode")
    }

    existing_allocations = repo.list_allocations_for_country_month(session, country_code, year, month)
    existing_by_material: dict[str, int] = {}
    existing_vehicle_by_material: dict[str, int] = {}
    existing_line_rows: list[dict[str, Any]] = []
    for allocation in existing_allocations:
        material_code = allocation.material_code or ""
        if material_code:
            existing_by_material[material_code] = existing_by_material.get(material_code, 0) + int(allocation.quantity or 0)
            vehicle_count = repo.count_vehicles_for_line_country(session, allocation.pi_line_code, country_code)
            existing_vehicle_by_material[material_code] = existing_vehicle_by_material.get(material_code, 0) + vehicle_count
        existing_line_rows.append(allocation_to_dict(allocation))

    legacy_lines = repo.list_lines_without_allocations_for_country_month(session, country_code, order_month)
    for line in legacy_lines:
        material_code = line.material_code or ""
        if material_code:
            existing_by_material[material_code] = existing_by_material.get(material_code, 0) + int(line.quantity or 0)
            vehicle_count = repo.count_vehicles_for_line(session, line.pi_line_code)
            existing_vehicle_by_material[material_code] = existing_vehicle_by_material.get(material_code, 0) + vehicle_count
        existing_line_rows.append(line_to_dict(line))

    line_rows: list[dict[str, Any]] = []
    remaining_items: list[dict[str, Any]] = []
    for item in selected_items:
        material_code = str(item.get("materialCode") or "")
        selected_quantity = int(item.get("quantity") or 0)
        generated_quantity = existing_by_material.get(material_code, 0)
        generated_vehicle_count = existing_vehicle_by_material.get(material_code, 0)
        remaining_quantity = max(selected_quantity - generated_quantity, 0)
        over_generated_quantity = max(generated_quantity - selected_quantity, 0)
        payload = _line_payload_from_material(session, country_code, material_code, item)
        row = {
            **payload,
            "selectedQuantity": selected_quantity,
            "generatedQuantity": generated_quantity,
            "generatedVehicleCount": generated_vehicle_count,
            "remainingQuantity": remaining_quantity,
            "overGeneratedQuantity": over_generated_quantity,
        }
        line_rows.append(row)
        if remaining_quantity > 0:
            remaining_items.append({
                **payload,
                "quantity": remaining_quantity,
            })

    for material_code, generated_quantity in sorted(existing_by_material.items()):
        if material_code in selected_by_material:
            continue
        payload = _line_payload_from_material(session, country_code, material_code, {"materialCode": material_code})
        line_rows.append({
            **payload,
            "selectedQuantity": 0,
            "generatedQuantity": generated_quantity,
            "generatedVehicleCount": existing_vehicle_by_material.get(material_code, 0),
            "remainingQuantity": 0,
            "overGeneratedQuantity": generated_quantity,
        })

    totals = {
        "selectedQuantity": sum(int(row["selectedQuantity"]) for row in line_rows),
        "generatedQuantity": sum(int(row["generatedQuantity"]) for row in line_rows),
        "generatedVehicleCount": sum(int(row["generatedVehicleCount"]) for row in line_rows),
        "remainingQuantity": sum(int(row["remainingQuantity"]) for row in line_rows),
        "overGeneratedQuantity": sum(int(row["overGeneratedQuantity"]) for row in line_rows),
    }
    return {
        "countryCode": country_code,
        "year": year,
        "month": month,
        "orderMonth": order_month,
        "lineItems": line_rows,
        "selectedLineItems": selected_items,
        "remainingLineItems": remaining_items,
        "existingLines": existing_line_rows,
        "totals": totals,
        "status": "complete" if totals["remainingQuantity"] == 0 else "pending",
    }


def list_pi_headers(session: Session, **filters) -> dict:
    rows, total = repo.list_headers(session, **filters)
    return {"items": [header_to_dict(row) for row in rows], "total": total}


def get_pi_detail(session: Session, pi_code: str) -> dict:
    header = repo.get_header_by_code(session, pi_code)
    if not header:
        raise HTTPException(status_code=404, detail="PI not found")
    lines = repo.list_lines_by_pi(session, header.pi_id)
    allocations = repo.list_allocations_by_pi(session, header.pi_id)
    allocations_by_line: dict[Any, list[PiOrderLineAllocation]] = {}
    for allocation in allocations:
        allocations_by_line.setdefault(allocation.pi_line_id, []).append(allocation)
    line_rows: list[dict[str, Any]] = []
    for line in lines:
        row = line_to_dict(line)
        row["allocations"] = [allocation_to_dict(allocation) for allocation in allocations_by_line.get(line.pi_line_id, [])]
        line_rows.append(row)
    vehicles, total = repo.list_vehicles(session, pi_code=pi_code, page=1, page_size=5000)
    return {
        "header": header_to_dict(header),
        "lines": line_rows,
        "summary": repo.vehicle_summary(session, pi_code),
        "vehicles": vehicles_to_dict(session, vehicles),
        "vehicleTotal": total,
    }


def list_vehicle_units(session: Session, **filters) -> dict:
    rows, total = repo.list_vehicles(session, **filters)
    return {"items": vehicles_to_dict(session, rows), "total": total}


def get_vehicle_detail(session: Session, car_code: str) -> dict:
    vehicle = repo.get_vehicle_by_car_code(session, car_code)
    if not vehicle:
        raise HTTPException(status_code=404, detail="Vehicle not found")
    return vehicle_to_dict(session, vehicle)


def update_vehicle_unit(session: Session, car_code: str, payload: dict[str, Any], username: str) -> dict:
    vehicle = repo.get_vehicle_by_car_code(session, car_code)
    if not vehicle:
        raise HTTPException(status_code=404, detail="Vehicle not found")
    _apply_vehicle_updates(session, vehicle, payload, username)
    session.flush()
    return vehicle_to_dict(session, vehicle)


def bulk_update_vehicle_units(session: Session, payload: dict[str, Any], username: str) -> dict:
    pi_code = _clean(payload.get("piCode") or payload.get("pi_code"))
    pi_line_code = _clean(payload.get("piLineCode") or payload.get("pi_line_code"))
    if pi_code:
        pi_code = pi_code.upper()
    if pi_line_code:
        pi_line_code = pi_line_code.upper()

    if pi_line_code:
        line = repo.get_line_by_code(session, pi_line_code)
        if not line:
            raise HTTPException(status_code=404, detail="PI line not found")
        if pi_code and line.pi_code != pi_code:
            raise HTTPException(status_code=400, detail="PI Line does not belong to PI")
        pi_code = line.pi_code
    if not pi_code:
        raise HTTPException(status_code=400, detail="piCode or piLineCode is required")
    if not repo.get_header_by_code(session, pi_code):
        raise HTTPException(status_code=404, detail="PI not found")

    vehicles = repo.list_vehicles_for_bulk_update(
        session,
        pi_code=pi_code,
        pi_line_code=pi_line_code,
    )
    if not vehicles:
        return {
            "piCode": pi_code,
            "piLineCode": pi_line_code,
            "matchedUnits": 0,
            "updatedUnits": 0,
            "vinAssigned": 0,
            "fieldsUpdated": [],
        }

    # Car-code-based selection (checkbox multi-select)
    car_codes: list[str] | None = None
    raw_car_codes = payload.get("carCodes") or payload.get("car_codes")
    if raw_car_codes and isinstance(raw_car_codes, list):
        car_codes = [str(c).strip().upper() for c in raw_car_codes if c]
    if car_codes:
        code_set = set(car_codes)
        vehicles = [v for v in vehicles if v.car_code in code_set]

    field_payload = _bulk_field_payload(payload.get("fields"))
    vin_list = _bulk_vin_list(payload.get("vinList") or payload.get("vins"))
    if not field_payload and not vin_list:
        raise HTTPException(status_code=400, detail="No VINs or bulk fields provided")

    empty_vin_vehicles = [vehicle for vehicle in vehicles if not _clean(vehicle.vin)]
    if len(vin_list) > len(empty_vin_vehicles):
        raise HTTPException(
            status_code=400,
            detail=(
                f"Pasted {len(vin_list)} VINs but only {len(empty_vin_vehicles)} vehicles "
                "in scope have empty VIN"
            ),
        )

    vin_by_car_code = {
        vehicle.car_code: vin
        for vehicle, vin in zip(empty_vin_vehicles, vin_list, strict=False)
    }
    updated_units = 0
    for vehicle in vehicles:
        vehicle_payload = dict(field_payload)
        assigned_vin = vin_by_car_code.get(vehicle.car_code)
        if assigned_vin:
            vehicle_payload["vin"] = assigned_vin
        if not vehicle_payload:
            continue
        _apply_vehicle_updates(session, vehicle, vehicle_payload, username)
        updated_units += 1
    session.flush()
    return {
        "piCode": pi_code,
        "piLineCode": pi_line_code,
        "matchedUnits": len(vehicles),
        "updatedUnits": updated_units,
        "vinAssigned": len(vin_list),
        "fieldsUpdated": sorted(field_payload.keys()),
    }


def search_vehicle_allocation(session: Session, keyword: str) -> dict:
    kw = keyword.strip()
    if not kw:
        return {"type": "empty", "item": None}
    header = repo.get_header_by_code(session, kw.upper())
    if header:
        return {"type": "pi", "item": get_pi_detail(session, header.pi_code)}
    vehicle = repo.get_vehicle_by_car_code(session, kw.upper())
    if vehicle:
        return {"type": "vehicle", "item": vehicle_to_dict(session, vehicle)}
    vehicle = repo.get_vehicle_by_vin(session, kw)
    if vehicle:
        return {"type": "vehicle", "item": vehicle_to_dict(session, vehicle)}
    return {"type": "empty", "item": None}


def preview_vehicle_import(session: Session, rows: list[dict[str, Any]]) -> dict:
    warnings: list[str] = []
    errors: list[str] = []
    preview_rows: list[dict[str, Any]] = []
    new_headers: set[str] = set()
    new_lines: set[str] = set()
    new_units = 0
    updated_units = 0
    seen_vins: dict[str, int] = {}
    seen_cars: dict[str, int] = {}

    for row in rows:
        row_warnings, row_errors = _validate_import_row(row)
        vin = _clean(row.get("vin"))
        car_code = _clean(row.get("car_code"))
        if vin:
            if vin in seen_vins:
                row_errors.append(f"VIN duplicates row {seen_vins[vin]}")
            seen_vins[vin] = int(row.get("sourceRow") or 0)
        if car_code:
            car_upper = car_code.upper()
            if car_upper in seen_cars:
                row_errors.append(f"Car Code duplicates row {seen_cars[car_upper]}")
            seen_cars[car_upper] = int(row.get("sourceRow") or 0)

        vehicle = repo.get_vehicle_by_vin(session, vin) if vin else None
        if not vehicle and car_code:
            vehicle = repo.get_vehicle_by_car_code(session, car_code.upper())
        action = "update" if vehicle else "create"
        if action == "create":
            new_units += 1
            pi_code = _clean(row.get("pi_code"))
            if pi_code and not repo.get_header_by_code(session, pi_code.upper()):
                new_headers.add(pi_code.upper())
            line_code = _line_code_for_import_row(session, row)
            if line_code and not repo.get_line_by_code(session, line_code):
                new_lines.add(line_code)
        else:
            updated_units += 1
        warnings.extend(f"Row {row.get('sourceRow')}: {msg}" for msg in row_warnings)
        errors.extend(f"Row {row.get('sourceRow')}: {msg}" for msg in row_errors)
        preview_rows.append({
            "sourceRow": row.get("sourceRow"),
            "action": action,
            "piCode": _clean(row.get("pi_code")),
            "carCode": car_code,
            "vin": vin,
            "materialCode": _clean(row.get("material_code")),
            "warnings": row_warnings,
            "errors": row_errors,
        })

    return {
        "totalRows": len(rows),
        "newHeaders": len(new_headers),
        "newLines": len(new_lines),
        "newUnits": new_units,
        "updatedUnits": updated_units,
        "warnings": warnings,
        "errors": errors,
        "previewRows": preview_rows[:200],
        "status": "error" if errors else "ok",
    }


def apply_vehicle_import(session: Session, rows: list[dict[str, Any]], username: str) -> dict:
    preview = preview_vehicle_import(session, rows)
    if preview["errors"]:
        raise HTTPException(status_code=400, detail="Import has blocking errors")
    created = 0
    updated = 0
    for row in rows:
        vehicle = None
        vin = _clean(row.get("vin"))
        car_code = _clean(row.get("car_code"))
        if vin:
            vehicle = repo.get_vehicle_by_vin(session, vin)
        if not vehicle and car_code:
            vehicle = repo.get_vehicle_by_car_code(session, car_code.upper())
        if vehicle:
            header = repo.get_header_by_code(session, vehicle.pi_code)
            if header:
                _apply_header_import_updates(header, row, username)
            _apply_vehicle_updates(session, vehicle, _import_row_to_update_payload(row), username)
            updated += 1
        else:
            _create_vehicle_from_import_row(session, row, username)
            created += 1
    return {"createdUnits": created, "updatedUnits": updated, "warnings": preview["warnings"]}


def export_vehicle_units(session: Session, **filters):
    vehicles = list_vehicle_units(session, **filters)["items"]
    return generate_vehicle_allocation_excel(vehicles)


def header_to_dict(header: PiOrderHeader) -> dict:
    return {
        "piId": str(header.pi_id),
        "piCode": header.pi_code,
        "officialPiNo": header.official_pi_no,
        "countryCode": header.country_code,
        "countryName": header.country_name,
        "orderingAccountCode": header.ordering_account_code,
        "orderingAccountName": header.ordering_account_name,
        "marketCountryCodes": list(header.market_country_codes or [header.country_code]),
        "shipmentBatchCode": header.shipment_batch_code,
        "portOfDischarge": header.port_of_discharge,
        "orderDate": _date_str(header.order_date),
        "orderMonth": header.order_month,
        "piSequenceNo": header.pi_sequence_no,
        "shippingScheduleUrl": header.shipping_schedule_url,
        "feishuTrackingUrl": header.feishu_tracking_url,
        "shipName": header.ship_name,
        "etd": _date_str(header.etd),
        "eta": _date_str(header.eta),
        "actualDepartureDate": _date_str(header.actual_departure_date),
        "actualArrivalDate": _date_str(header.actual_arrival_date),
        "readyForPickupDate": _date_str(header.ready_for_pickup_date),
        "status": header.status,
        "remark": header.remark,
        "rowVersion": header.row_version,
        "createdAtUtc": header.created_at_utc.isoformat() if header.created_at_utc else None,
        "updatedAtUtc": header.updated_at_utc.isoformat() if header.updated_at_utc else None,
    }


def line_to_dict(line: PiOrderLine) -> dict:
    return {
        "piLineId": str(line.pi_line_id),
        "piCode": line.pi_code,
        "piLineCode": line.pi_line_code,
        "lineSequenceNo": line.line_sequence_no,
        "materialCode": line.material_code,
        "bom": line.bom,
        "brand": line.brand,
        "modelName": line.model_name,
        "version": line.version,
        "powertrain": line.powertrain,
        "exteriorColorName": line.exterior_color_name,
        "exteriorColorCode": line.exterior_color_code,
        "interiorColorName": line.interior_color_name,
        "interiorColourCode": line.interior_colour_code,
        "quantity": line.quantity,
        "fobEur": _float(line.fob_eur),
        "amountEur": _float(line.amount_eur),
        "remark": line.remark,
        "rowVersion": line.row_version,
    }


def allocation_to_dict(allocation: PiOrderLineAllocation) -> dict:
    return {
        "piLineAllocationId": str(allocation.pi_line_allocation_id),
        "piCode": allocation.pi_code,
        "piLineCode": allocation.pi_line_code,
        "marketCountryCode": allocation.market_country_code,
        "orderYear": allocation.order_year,
        "orderMonth": allocation.order_month,
        "materialCode": allocation.material_code,
        "quantity": allocation.quantity,
        "fobEur": _float(allocation.fob_eur),
    }


def vehicles_to_dict(session: Session, vehicles: list[PiVehicleUnit]) -> list[dict]:
    header_cache: dict[str, PiOrderHeader | None] = {}
    return [vehicle_to_dict(session, vehicle, header_cache) for vehicle in vehicles]


def vehicle_to_dict(
    session: Session,
    vehicle: PiVehicleUnit,
    header_cache: dict[str, PiOrderHeader | None] | None = None,
) -> dict:
    cache = header_cache if header_cache is not None else {}
    if vehicle.pi_code not in cache:
        cache[vehicle.pi_code] = repo.get_header_by_code(session, vehicle.pi_code)
    header = cache.get(vehicle.pi_code)
    return {
        "vehicleUnitId": str(vehicle.vehicle_unit_id),
        "piCode": vehicle.pi_code,
        "officialPiNo": header.official_pi_no if header else None,
        "orderingAccountCode": header.ordering_account_code if header else None,
        "orderingAccountName": header.ordering_account_name if header else None,
        "shipmentBatchCode": header.shipment_batch_code if header else None,
        "portOfDischarge": header.port_of_discharge if header else None,
        "piLineCode": vehicle.pi_line_code,
        "carCode": vehicle.car_code,
        "vin": vehicle.vin,
        "materialCode": vehicle.material_code,
        "bom": vehicle.bom,
        "brand": vehicle.brand,
        "modelName": vehicle.model_name,
        "version": vehicle.version,
        "powertrain": vehicle.powertrain,
        "exteriorColorName": vehicle.exterior_color_name,
        "exteriorColorCode": vehicle.exterior_color_code,
        "interiorColorName": vehicle.interior_color_name,
        "interiorColourCode": vehicle.interior_colour_code,
        "orderDate": _date_str(header.order_date) if header else None,
        "productionDate": _date_str(vehicle.production_date),
        "etd": _date_str(vehicle.etd),
        "eta": _date_str(vehicle.eta),
        "actualDepartureDate": _date_str(vehicle.actual_departure_date),
        "actualArrivalDate": _date_str(vehicle.actual_arrival_date),
        "readyForPickupDate": _date_str(vehicle.ready_for_pickup_date),
        "shipName": vehicle.ship_name,
        "countryCode": vehicle.country_code,
        "dealerCode": vehicle.dealer_code,
        "dealerName": vehicle.dealer_name,
        "customerRef": vehicle.customer_ref,
        "allocationStatus": vehicle.allocation_status,
        "logisticsStatus": vehicle.logistics_status,
        "shippingScheduleUrl": header.shipping_schedule_url if header else None,
        "feishuTrackingUrl": header.feishu_tracking_url if header else None,
        "remark": vehicle.remark,
        "rowVersion": vehicle.row_version,
    }


def _resolve_order_month(payload: dict[str, Any]) -> tuple[int, int, str]:
    raw_month = payload.get("orderMonth")
    if isinstance(raw_month, str) and re.fullmatch(r"\d{4}-\d{2}", raw_month):
        year = int(raw_month[:4])
        month = int(raw_month[5:7])
        return year, month, raw_month
    year = int(payload.get("orderYear") or datetime.utcnow().year)
    month = int(raw_month or datetime.utcnow().month)
    if month < 1 or month > 12:
        raise HTTPException(status_code=400, detail="orderMonth must be 1-12 or YYYY-MM")
    return year, month, f"{year}-{month:02d}"


def _apply_header_updates(header: PiOrderHeader, payload: dict[str, Any], username: str, allow_status: bool) -> None:
    fields = {
        "official_pi_no": "officialPiNo",
        "country_name": "countryName",
        "ordering_account_name": "orderingAccountName",
        "shipment_batch_code": "shipmentBatchCode",
        "port_of_discharge": "portOfDischarge",
        "shipping_schedule_url": "shippingScheduleUrl",
        "feishu_tracking_url": "feishuTrackingUrl",
        "ship_name": "shipName",
        "remark": "remark",
    }
    for attr, key in fields.items():
        if key in payload:
            setattr(header, attr, _clean(payload.get(key)))
    for attr, key in {
        "order_date": "orderDate",
        "etd": "etd",
        "eta": "eta",
        "actual_departure_date": "actualDepartureDate",
        "actual_arrival_date": "actualArrivalDate",
        "ready_for_pickup_date": "readyForPickupDate",
    }.items():
        if key in payload:
            setattr(header, attr, _parse_date(payload.get(key)))
    if allow_status and "status" in payload:
        header.status = _status(payload.get("status"), PI_STATUSES, header.status)
    if "marketCountryCodes" in payload:
        header.market_country_codes = _market_country_codes(payload.get("marketCountryCodes"), header.country_code)
    _validate_dates(header.etd, header.eta, header.ready_for_pickup_date, errors=True)
    header.updated_by = username


def _build_line(session: Session, header: PiOrderHeader, line_seq: int, payload: dict[str, Any], username: str) -> PiOrderLine:
    quantity = _non_negative_int(payload.get("quantity", 0), "quantity")
    fob = _decimal_or_none(payload.get("fobEur"))
    return PiOrderLine(
        pi_id=header.pi_id,
        pi_code=header.pi_code,
        pi_line_code=build_pi_line_code(header.pi_code, line_seq),
        line_sequence_no=line_seq,
        material_code=_clean(payload.get("materialCode")),
        bom=_clean(payload.get("bom")),
        brand=_clean(payload.get("brand")),
        model_name=_clean(payload.get("modelName")),
        version=_clean(payload.get("version")),
        powertrain=_clean(payload.get("powertrain")),
        exterior_color_name=_clean(payload.get("exteriorColorName")),
        exterior_color_code=_clean(payload.get("exteriorColorCode")),
        interior_color_name=_clean(payload.get("interiorColorName")),
        interior_colour_code=_clean(payload.get("interiorColourCode")),
        quantity=quantity,
        fob_eur=fob,
        amount_eur=_amount(quantity, fob),
        remark=_clean(payload.get("remark")),
        created_by=username,
        updated_by=username,
    )


def _build_vehicle_from_line(
    header: PiOrderHeader,
    line: PiOrderLine,
    unit_seq: int,
    username: str,
    country_code: str | None = None,
) -> PiVehicleUnit:
    year = int(header.order_month[:4])
    month = int(header.order_month[5:7])
    vehicle_country = (country_code or header.country_code).upper()
    return PiVehicleUnit(
        pi_id=header.pi_id,
        pi_line_id=line.pi_line_id,
        pi_code=header.pi_code,
        pi_line_code=line.pi_line_code,
        car_code=build_car_code(vehicle_country, year, month, header.pi_sequence_no, line.line_sequence_no, unit_seq),
        material_code=line.material_code,
        bom=line.bom,
        brand=line.brand,
        model_name=line.model_name,
        version=line.version,
        powertrain=line.powertrain,
        exterior_color_name=line.exterior_color_name,
        exterior_color_code=line.exterior_color_code,
        interior_color_name=line.interior_color_name,
        interior_colour_code=line.interior_colour_code,
        etd=header.etd,
        eta=header.eta,
        ready_for_pickup_date=header.ready_for_pickup_date,
        ship_name=header.ship_name,
        country_code=vehicle_country,
        created_by=username,
        updated_by=username,
    )


def _ensure_vehicle_units_for_line(session: Session, header: PiOrderHeader, line: PiOrderLine, username: str) -> None:
    existing_units = repo.count_vehicles_for_line(session, line.pi_line_code)
    if line.quantity < existing_units:
        raise HTTPException(
            status_code=409,
            detail="PI line quantity cannot be lower than existing vehicle units",
        )
    for unit_seq in range(existing_units + 1, line.quantity + 1):
        repo.add_vehicle(session, _build_vehicle_from_line(header, line, unit_seq, username))


def _ensure_vehicle_units_for_line_allocations(
    session: Session,
    header: PiOrderHeader,
    line: PiOrderLine,
    allocations: list[dict[str, Any]],
    username: str,
) -> None:
    existing_units = repo.count_vehicles_for_line(session, line.pi_line_code)
    if existing_units > 0:
        if line.quantity < existing_units:
            raise HTTPException(
                status_code=409,
                detail="PI line quantity cannot be lower than existing vehicle units",
            )
        return
    unit_seq = 1
    for allocation in allocations:
        country_code = str(allocation["countryCode"]).upper()
        quantity = int(allocation["quantity"] or 0)
        for _ in range(quantity):
            repo.add_vehicle(session, _build_vehicle_from_line(header, line, unit_seq, username, country_code))
            unit_seq += 1


def _sync_vehicles_from_line(session: Session, line: PiOrderLine, username: str) -> None:
    for vehicle in repo.list_vehicles_by_line(session, line.pi_line_code):
        vehicle.material_code = line.material_code
        vehicle.bom = line.bom
        vehicle.brand = line.brand
        vehicle.model_name = line.model_name
        vehicle.version = line.version
        vehicle.powertrain = line.powertrain
        vehicle.exterior_color_name = line.exterior_color_name
        vehicle.exterior_color_code = line.exterior_color_code
        vehicle.interior_color_name = line.interior_color_name
        vehicle.interior_colour_code = line.interior_colour_code
        vehicle.updated_by = username


def _line_items_from_order_quantities(session: Session, country: str, year: int, month: int) -> list[dict[str, Any]]:
    cells = og_repo.list_quantities_for_country_month(
        session,
        country_code=country,
        order_year=year,
        order_month=month,
        positive_only=True,
    )
    return [
        {
            "materialCode": cell.material_code,
            "quantity": cell.quantity,
            "fobEur": float(cell.fob_eur) if cell.fob_eur is not None else None,
        }
        for cell in cells
        if cell.quantity > 0
    ]


def _line_payload_from_material(session: Session, country: str, material_code: str | None, item: dict[str, Any]) -> dict:
    payload = dict(item)
    if not material_code:
        return payload
    sku = og_repo.get_sku_by_material_code_any_status(session, material_code)
    fob = og_repo.get_fob_for_country_sku(session, country, material_code)
    if sku:
        payload.update({
            "materialCode": sku.material_code,
            "bom": sku.bom_template,
            "brand": sku.brand,
            "modelName": sku.model_name,
            "version": sku.version,
            "powertrain": sku.powertrain,
            "exteriorColorName": sku.exterior_color_name,
            "exteriorColorCode": sku.exterior_color_code,
            "interiorColorName": sku.interior_color_name,
            "interiorColourCode": sku.interior_colour_code,
        })
    if fob:
        payload["fobEur"] = float(fob.final_fob_eur)
    return payload


def _validate_line_items_against_remaining(
    session: Session,
    country: str,
    year: int,
    month: int,
    line_items: list[dict[str, Any]],
) -> None:
    requested_by_country_material: dict[tuple[str, str], int] = {}
    for item in line_items:
        if not isinstance(item, dict):
            raise HTTPException(status_code=400, detail="lineItems must contain objects")
        material_code = _clean(item.get("materialCode"))
        if not material_code:
            raise HTTPException(status_code=400, detail="materialCode is required for PI line items")
        for allocation in _line_item_allocations(item, country):
            quantity = int(allocation["quantity"] or 0)
            if quantity <= 0:
                continue
            allocation_country = str(allocation["countryCode"]).upper()
            key = (allocation_country, material_code)
            requested_by_country_material[key] = requested_by_country_material.get(key, 0) + quantity

    remaining_cache: dict[str, dict[str, int]] = {}
    for allocation_country, material_code in requested_by_country_material:
        if allocation_country not in remaining_cache:
            plan = get_order_matrix_allocation_plan(session, allocation_country, year, month)
            remaining_cache[allocation_country] = {
                str(item.get("materialCode")): int(item.get("quantity") or 0)
                for item in plan["remainingLineItems"]
                if item.get("materialCode")
            }

    for (allocation_country, material_code), requested_quantity in requested_by_country_material.items():
        remaining_quantity = remaining_cache.get(allocation_country, {}).get(material_code, 0)
        if requested_quantity > remaining_quantity:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Requested quantity exceeds remaining order quantity for {allocation_country} {material_code}: "
                    f"requested {requested_quantity}, remaining {remaining_quantity}"
                ),
            )


def _line_item_allocations(item: dict[str, Any], default_country: str) -> list[dict[str, Any]]:
    raw_allocations = item.get("allocations")
    source_rows = raw_allocations if isinstance(raw_allocations, list) else [
        {
            "countryCode": default_country,
            "quantity": item.get("quantity"),
            "fobEur": item.get("fobEur"),
        }
    ]
    by_country: dict[str, dict[str, Any]] = {}
    for raw in source_rows:
        if not isinstance(raw, dict):
            raise HTTPException(status_code=400, detail="allocations must contain objects")
        country_code = str(raw.get("countryCode") or raw.get("marketCountryCode") or default_country).upper()
        if not country_code:
            raise HTTPException(status_code=400, detail="allocation countryCode is required")
        quantity = _non_negative_int(raw.get("quantity"), "allocation quantity")
        if quantity <= 0:
            continue
        current = by_country.get(country_code)
        if current:
            current["quantity"] = int(current["quantity"]) + quantity
        else:
            by_country[country_code] = {
                "countryCode": country_code,
                "quantity": quantity,
                "fobEur": raw.get("fobEur", item.get("fobEur")),
            }
    return list(by_country.values())


def _market_country_codes_for_line_items(
    raw_market_countries: Any,
    default_country: str,
    line_items: list[dict[str, Any]],
) -> list[str]:
    market_country_codes = _market_country_codes(raw_market_countries, default_country)
    seen = set(market_country_codes)
    for item in line_items:
        if not isinstance(item, dict):
            continue
        for allocation in _line_item_allocations(item, default_country):
            country_code = str(allocation["countryCode"]).upper()
            if country_code not in seen:
                market_country_codes.append(country_code)
                seen.add(country_code)
    return market_country_codes


def _add_line_allocations(
    session: Session,
    header: PiOrderHeader,
    line: PiOrderLine,
    allocations: list[dict[str, Any]],
    username: str,
) -> None:
    year = int(header.order_month[:4])
    month = int(header.order_month[5:7])
    for allocation in allocations:
        quantity = int(allocation["quantity"] or 0)
        if quantity <= 0:
            continue
        repo.add_line_allocation(
            session,
            PiOrderLineAllocation(
                pi_id=header.pi_id,
                pi_line_id=line.pi_line_id,
                pi_code=header.pi_code,
                pi_line_code=line.pi_line_code,
                market_country_code=str(allocation["countryCode"]).upper(),
                order_year=year,
                order_month=month,
                material_code=line.material_code,
                quantity=quantity,
                fob_eur=_decimal_or_none(allocation.get("fobEur")) if allocation.get("fobEur") is not None else line.fob_eur,
                created_by=username,
                updated_by=username,
            ),
        )


def _sync_default_line_allocation(session: Session, header: PiOrderHeader, line: PiOrderLine, username: str) -> None:
    allocations = repo.list_allocations_by_line(session, line.pi_line_id)
    if not allocations:
        _add_line_allocations(
            session,
            header,
            line,
            [{"countryCode": header.country_code, "quantity": line.quantity, "fobEur": line.fob_eur}],
            username,
        )
        return
    if len(allocations) == 1 and allocations[0].market_country_code == header.country_code:
        allocation = allocations[0]
        allocation.quantity = line.quantity
        allocation.material_code = line.material_code
        allocation.fob_eur = line.fob_eur
        allocation.updated_by = username


def _apply_vehicle_updates(session: Session, vehicle: PiVehicleUnit, payload: dict[str, Any], username: str) -> None:
    vin = _clean(payload.get("vin")) if "vin" in payload else vehicle.vin
    if vin and vin != vehicle.vin:
        existing = repo.get_vehicle_by_vin(session, vin)
        if existing and existing.vehicle_unit_id != vehicle.vehicle_unit_id:
            raise HTTPException(status_code=409, detail=f"VIN already assigned: {vin}")
    string_fields = {
        "vin": "vin",
        "dealer_code": "dealerCode",
        "dealer_name": "dealerName",
        "customer_ref": "customerRef",
        "ship_name": "shipName",
        "remark": "remark",
    }
    for attr, key in string_fields.items():
        if key in payload:
            setattr(vehicle, attr, _clean(payload.get(key)))
    for attr, key in {
        "production_date": "productionDate",
        "etd": "etd",
        "eta": "eta",
        "actual_departure_date": "actualDepartureDate",
        "actual_arrival_date": "actualArrivalDate",
        "ready_for_pickup_date": "readyForPickupDate",
    }.items():
        if key in payload:
            setattr(vehicle, attr, _parse_date(payload.get(key)))
    if "allocationStatus" in payload:
        vehicle.allocation_status = _status(payload.get("allocationStatus"), ALLOCATION_STATUSES, vehicle.allocation_status)
    if "logisticsStatus" in payload:
        vehicle.logistics_status = _status(payload.get("logisticsStatus"), LOGISTICS_STATUSES, vehicle.logistics_status)
    _validate_dates(vehicle.etd, vehicle.eta, vehicle.ready_for_pickup_date, errors=True)
    vehicle.updated_by = username
    if vehicle.vehicle_unit_id is None:
        vehicle.row_version = vehicle.row_version or 1
    else:
        vehicle.row_version = (vehicle.row_version or 1) + 1


def _bulk_field_payload(raw_fields: Any) -> dict[str, Any]:
    if not isinstance(raw_fields, dict):
        return {}
    allowed_fields = {
        "productionDate",
        "etd",
        "eta",
        "actualDepartureDate",
        "actualArrivalDate",
        "readyForPickupDate",
        "shipName",
        "dealerCode",
        "dealerName",
        "customerRef",
        "allocationStatus",
        "logisticsStatus",
        "remark",
    }
    return {key: raw_fields.get(key) for key in allowed_fields if key in raw_fields}


def _bulk_vin_list(raw_vins: Any) -> list[str]:
    if raw_vins is None:
        return []
    if isinstance(raw_vins, str):
        parts = re.split(r"[\s,;]+", raw_vins)
    elif isinstance(raw_vins, list):
        parts = [str(item) for item in raw_vins]
    else:
        raise HTTPException(status_code=400, detail="vinList must be a string or list")
    vins = [part.strip().upper() for part in parts if part and part.strip()]
    seen: set[str] = set()
    duplicates: list[str] = []
    for vin in vins:
        if vin in seen and vin not in duplicates:
            duplicates.append(vin)
        seen.add(vin)
    if duplicates:
        raise HTTPException(status_code=400, detail=f"Duplicate VINs in paste: {', '.join(duplicates[:5])}")
    return vins


def _validate_import_row(row: dict[str, Any]) -> tuple[list[str], list[str]]:
    warnings: list[str] = []
    errors: list[str] = []
    pi_code = _clean(row.get("pi_code"))
    car_code = _clean(row.get("car_code"))
    vin = _clean(row.get("vin"))
    country = _clean(row.get("country_code"))
    if not (pi_code or car_code or vin):
        errors.append("At least one of PI Code, Car Code, VIN is required")
    if pi_code and not parse_pi_code(pi_code.upper()):
        errors.append("PI Code format is invalid")
    if car_code and not parse_car_code(car_code.upper()):
        errors.append("Car Code format is invalid")
    if not country and pi_code:
        parsed = parse_pi_code(pi_code.upper())
        if parsed and len(str(parsed["countryCode"])) == 2:
            row["country_code"] = parsed["countryCode"]
    allocation = _clean(row.get("allocation_status"))
    logistics = _clean(row.get("logistics_status"))
    if allocation and allocation not in ALLOCATION_STATUSES:
        errors.append(f"Invalid allocation_status: {allocation}")
    if logistics and logistics not in LOGISTICS_STATUSES:
        errors.append(f"Invalid logistics_status: {logistics}")
    etd, etd_error = _parse_import_date(row.get("etd"), "ETD")
    eta, eta_error = _parse_import_date(row.get("eta"), "ETA")
    ready, ready_error = _parse_import_date(row.get("ready_for_pickup_date"), "Ready for Pickup Date")
    errors.extend([msg for msg in [etd_error, eta_error, ready_error] if msg])
    date_warnings = _validate_dates(etd, eta, ready, errors=False)
    warnings.extend(date_warnings)
    if eta and etd and eta < etd:
        errors.append("ETA should not be earlier than ETD")
    return warnings, errors


def _line_code_for_import_row(session: Session, row: dict[str, Any]) -> str | None:
    car_code = _clean(row.get("car_code"))
    pi_code = _clean(row.get("pi_code"))
    if car_code:
        parsed_car = parse_car_code(car_code.upper())
        if parsed_car and pi_code:
            return build_pi_line_code(pi_code.upper(), int(parsed_car["lineSequence"]))
    if not pi_code:
        return None
    header = repo.get_header_by_code(session, pi_code.upper())
    existing = repo.get_line_for_material(session, header.pi_id, _clean(row.get("material_code")), _clean(row.get("bom"))) if header else None
    if existing:
        return existing.pi_line_code
    next_seq = repo.next_line_sequence(session, header.pi_id) if header else 1
    return build_pi_line_code(pi_code.upper(), next_seq)


def _import_row_to_update_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "vin": _clean(row.get("vin")),
        "productionDate": row.get("production_date"),
        "etd": row.get("etd"),
        "eta": row.get("eta"),
        "actualDepartureDate": row.get("actual_departure_date"),
        "actualArrivalDate": row.get("actual_arrival_date"),
        "readyForPickupDate": row.get("ready_for_pickup_date"),
        "shipName": _clean(row.get("ship_name")),
        "dealerCode": _clean(row.get("dealer_code")),
        "dealerName": _clean(row.get("dealer_name")),
        "customerRef": _clean(row.get("customer_ref")),
        "allocationStatus": _clean(row.get("allocation_status")) or "unallocated",
        "logisticsStatus": _clean(row.get("logistics_status")) or "pending",
        "remark": _clean(row.get("remark")),
    }


def _create_vehicle_from_import_row(session: Session, row: dict[str, Any], username: str) -> PiVehicleUnit:
    pi_code = _clean(row.get("pi_code"))
    if not pi_code:
        raise HTTPException(status_code=400, detail=f"Row {row.get('sourceRow')}: PI Code is required for new vehicle")
    header = repo.get_header_by_code(session, pi_code.upper())
    parsed_pi = parse_pi_code(pi_code.upper())
    if not header:
        if not parsed_pi:
            raise HTTPException(status_code=400, detail=f"Row {row.get('sourceRow')}: invalid PI Code")
        import_country = _clean(row.get("country_code"))
        header_country = import_country.upper() if import_country else str(parsed_pi["countryCode"])
        header = PiOrderHeader(
            pi_code=pi_code.upper(),
            official_pi_no=_clean(row.get("official_pi_no")),
            country_code=header_country,
            country_name=_clean(row.get("country_name")),
            ordering_account_code=str(parsed_pi["scopeCode"]),
            ordering_account_name=_clean(row.get("country_name")),
            market_country_codes=[header_country],
            order_date=_parse_date(row.get("order_date")),
            order_month=str(parsed_pi["orderMonth"]),
            pi_sequence_no=int(parsed_pi["sequence"]),
            ship_name=_clean(row.get("ship_name")),
            shipping_schedule_url=_clean(row.get("shipping_schedule_url")),
            feishu_tracking_url=_clean(row.get("feishu_tracking_url")),
            etd=_parse_date(row.get("etd")),
            eta=_parse_date(row.get("eta")),
            ready_for_pickup_date=_parse_date(row.get("ready_for_pickup_date")),
            created_by=username,
            updated_by=username,
        )
        repo.add_header(session, header)

    _apply_header_import_updates(header, row, username)
    line = repo.get_line_for_material(session, header.pi_id, _clean(row.get("material_code")), _clean(row.get("bom")))
    if not line:
        car_code = _clean(row.get("car_code"))
        car_parts = parse_car_code(car_code.upper()) if car_code else None
        line_seq = int(car_parts["lineSequence"]) if car_parts else repo.next_line_sequence(session, header.pi_id)
        line_payload = _line_payload_from_material(session, header.country_code, _clean(row.get("material_code")), {
            "materialCode": _clean(row.get("material_code")),
            "bom": _clean(row.get("bom")),
            "brand": _clean(row.get("brand")),
            "modelName": _clean(row.get("model_name")),
            "version": _clean(row.get("version")),
            "powertrain": _clean(row.get("powertrain")),
            "exteriorColorName": _clean(row.get("exterior_color_name")),
            "interiorColorName": _clean(row.get("interior_color_name")),
            "quantity": 0,
        })
        line = _build_line(session, header, line_seq, line_payload, username)
        repo.add_line(session, line)

    car_code = _clean(row.get("car_code"))
    if not car_code:
        year = int(header.order_month[:4])
        month = int(header.order_month[5:7])
        car_code = build_car_code(header.country_code, year, month, header.pi_sequence_no, line.line_sequence_no, repo.next_unit_sequence(session, line.pi_line_code))
    vehicle = PiVehicleUnit(
        pi_id=header.pi_id,
        pi_line_id=line.pi_line_id,
        pi_code=header.pi_code,
        pi_line_code=line.pi_line_code,
        car_code=car_code.upper(),
        material_code=line.material_code or _clean(row.get("material_code")),
        bom=line.bom or _clean(row.get("bom")),
        brand=line.brand or _clean(row.get("brand")),
        model_name=line.model_name or _clean(row.get("model_name")),
        version=line.version or _clean(row.get("version")),
        powertrain=line.powertrain or _clean(row.get("powertrain")),
        exterior_color_name=line.exterior_color_name or _clean(row.get("exterior_color_name")),
        exterior_color_code=line.exterior_color_code,
        interior_color_name=line.interior_color_name or _clean(row.get("interior_color_name")),
        interior_colour_code=line.interior_colour_code,
        country_code=header.country_code,
        created_by=username,
        updated_by=username,
    )
    _apply_vehicle_updates(session, vehicle, _import_row_to_update_payload(row), username)
    repo.add_vehicle(session, vehicle)
    line.quantity += 1
    line.amount_eur = _amount(line.quantity, line.fob_eur)
    return vehicle


def _apply_header_import_updates(header: PiOrderHeader, row: dict[str, Any], username: str) -> None:
    payload = {
        "officialPiNo": row.get("official_pi_no"),
        "shipName": row.get("ship_name"),
        "shippingScheduleUrl": row.get("shipping_schedule_url"),
        "feishuTrackingUrl": row.get("feishu_tracking_url"),
        "orderDate": row.get("order_date"),
        "etd": row.get("etd"),
        "eta": row.get("eta"),
        "readyForPickupDate": row.get("ready_for_pickup_date"),
    }
    _apply_header_updates(header, {k: v for k, v in payload.items() if v not in (None, "")}, username, allow_status=False)


def _clean(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _account_code(value: object) -> str:
    text = _clean(value)
    if not text:
        raise HTTPException(status_code=400, detail="orderingAccountCode is required")
    normalized = re.sub(r"[^A-Z0-9]", "", text.upper())
    if not re.fullmatch(PI_SCOPE_CODE_RE, normalized):
        raise HTTPException(status_code=400, detail="orderingAccountCode must be 2-12 letters or numbers")
    return normalized


def _market_country_codes(value: object, fallback_country: str) -> list[str]:
    raw_values: list[object]
    if isinstance(value, list):
        raw_values = value
    elif isinstance(value, str):
        raw_values = re.split(r"[,/| ]+", value)
    else:
        raw_values = []
    result: list[str] = []
    for raw in [fallback_country, *raw_values]:
        text = _clean(raw)
        if not text:
            continue
        country_code = text.upper()
        if country_code not in result:
            result.append(country_code)
    return result


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _parse_date(value: object) -> date | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, (int, float)):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid date: {value}") from exc


def _parse_import_date(value: object, label: str) -> tuple[date | None, str | None]:
    try:
        return _parse_date(value), None
    except HTTPException:
        return None, f"{label} is invalid: {value}"


def _date_str(value: date | datetime | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    return value.isoformat()


def _status(value: object, allowed: set[str], default: str) -> str:
    text = _clean(value) or default
    if text not in allowed:
        raise HTTPException(status_code=400, detail=f"Invalid status: {text}")
    return text


def _non_negative_int(value: object, field: str) -> int:
    try:
        result = int(value or 0)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=f"{field} must be a number") from exc
    if result < 0:
        raise HTTPException(status_code=400, detail=f"{field} must be non-negative")
    return result


def _decimal_or_none(value: object) -> Decimal | None:
    if value in (None, ""):
        return None
    return Decimal(str(value))


def _amount(quantity: int, fob: Decimal | float | None) -> Decimal | None:
    if fob is None:
        return None
    return Decimal(quantity) * Decimal(str(fob))


def _float(value: Decimal | float | None) -> float | None:
    return float(value) if value is not None else None


def _validate_dates(etd: date | None, eta: date | None, ready: date | None, errors: bool) -> list[str]:
    warnings: list[str] = []
    if eta and etd and eta < etd:
        if errors:
            raise HTTPException(status_code=400, detail="ETA should not be earlier than ETD")
        warnings.append("ETA is earlier than ETD")
    if ready and eta and ready < eta:
        warnings.append("Ready for Pickup Date is earlier than ETA")
    return warnings


# ── PI Deletion ──────────────────────────────────────────────────────────


def delete_pi(session: Session, pi_code: str) -> dict:
    """Hard-delete a PI and all its cascading children (lines, allocations, vehicles)."""
    header = repo.get_header_by_code(session, pi_code)
    if not header:
        raise ValueError("PI not found")
    deleted_code = header.pi_code
    repo.delete_header(session, pi_code)
    return {"pi_code": deleted_code, "deleted": True}


def delete_pi_line(session: Session, pi_line_code: str) -> dict:
    """Hard-delete a PI line and its cascading children (allocations, vehicles)."""
    line = repo.get_line_by_code(session, pi_line_code)
    if not line:
        raise ValueError("Line not found")
    deleted_code = line.pi_line_code
    repo.delete_line(session, pi_line_code)
    return {"pi_line_code": deleted_code, "deleted": True}
