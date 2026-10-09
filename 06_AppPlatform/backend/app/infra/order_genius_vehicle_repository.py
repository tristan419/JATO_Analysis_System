"""Repository helpers for Order Genius PI vehicle allocation."""

from __future__ import annotations

from datetime import date

from sqlalchemy import Integer, and_, case, cast, func, or_, select
from sqlalchemy.orm import Session

from app.db.models import PiOrderHeader, PiOrderLine, PiOrderLineAllocation, PiVehicleUnit


def next_pi_sequence(session: Session, country_code: str, order_month: str) -> int:
    stmt = select(func.max(PiOrderHeader.pi_sequence_no)).where(
        PiOrderHeader.country_code == country_code,
        PiOrderHeader.order_month == order_month,
    )
    current = session.execute(stmt).scalar_one_or_none()
    return int(current or 0) + 1


def next_pi_sequence_for_account(session: Session, ordering_account_code: str, order_month: str) -> int:
    stmt = select(func.max(PiOrderHeader.pi_sequence_no)).where(
        PiOrderHeader.ordering_account_code == ordering_account_code,
        PiOrderHeader.order_month == order_month,
    )
    current = session.execute(stmt).scalar_one_or_none()
    return int(current or 0) + 1


def next_line_sequence(session: Session, pi_id) -> int:
    stmt = select(func.max(PiOrderLine.line_sequence_no)).where(PiOrderLine.pi_id == pi_id)
    current = session.execute(stmt).scalar_one_or_none()
    return int(current or 0) + 1


def next_unit_sequence(session: Session, pi_line_code: str) -> int:
    stmt = select(func.count()).select_from(PiVehicleUnit).where(
        PiVehicleUnit.pi_line_code == pi_line_code,
    )
    return int(session.execute(stmt).scalar_one() or 0) + 1


def get_header_by_code(session: Session, pi_code: str) -> PiOrderHeader | None:
    return session.execute(
        select(PiOrderHeader).where(PiOrderHeader.pi_code == pi_code)
    ).scalars().first()


def get_line_by_code(session: Session, pi_line_code: str) -> PiOrderLine | None:
    return session.execute(
        select(PiOrderLine).where(PiOrderLine.pi_line_code == pi_line_code)
    ).scalars().first()


def get_line_for_material(
    session: Session, pi_id, material_code: str | None, bom: str | None
) -> PiOrderLine | None:
    conditions = [PiOrderLine.pi_id == pi_id]
    if material_code:
        conditions.append(PiOrderLine.material_code == material_code)
    elif bom:
        conditions.append(PiOrderLine.bom == bom)
    else:
        return None
    return session.execute(select(PiOrderLine).where(*conditions)).scalars().first()


def get_vehicle_by_car_code(session: Session, car_code: str) -> PiVehicleUnit | None:
    return session.execute(
        select(PiVehicleUnit).where(PiVehicleUnit.car_code == car_code)
    ).scalars().first()


def get_vehicle_by_vin(session: Session, vin: str) -> PiVehicleUnit | None:
    return session.execute(
        select(PiVehicleUnit).where(PiVehicleUnit.vin == vin)
    ).scalars().first()


def list_vehicles_by_vins(session: Session, vins: set[str]) -> list[PiVehicleUnit]:
    if not vins:
        return []
    return list(session.execute(select(PiVehicleUnit).where(PiVehicleUnit.vin.in_(vins))).scalars().all())


def _header_country_filter(country: str):
    return or_(
        PiOrderHeader.country_code == country,
        PiOrderHeader.market_country_codes.contains([country]),
    )


def _header_access_filter(countries: set[str] | None, brands: set[str] | None):
    # A header is visible only if a saved line has an authorized market allocation.
    lines = select(PiOrderLine.pi_id).outerjoin(
        PiOrderLineAllocation, PiOrderLineAllocation.pi_line_id == PiOrderLine.pi_line_id,
    ).where(PiOrderLine.pi_id == PiOrderHeader.pi_id)
    if brands is not None:
        lines = lines.where(func.upper(PiOrderLine.brand).in_(brands))
    if countries is not None:
        lines = lines.where(or_(PiOrderLineAllocation.market_country_code.in_(countries),
                                and_(PiOrderLineAllocation.pi_line_id.is_(None),
                                     PiOrderHeader.country_code.in_(countries))))
    units = select(PiVehicleUnit.pi_id).where(PiVehicleUnit.pi_id == PiOrderHeader.pi_id)
    if brands is not None:
        units = units.where(func.upper(PiVehicleUnit.brand).in_(brands))
    if countries is not None:
        units = units.where(PiVehicleUnit.country_code.in_(countries))
    return or_(lines.exists(), units.exists())


def pi_month_summary(session: Session, year: int, country: str | None = None,
                     allowed_countries: set[str] | None = None, allowed_brands: set[str] | None = None) -> dict:
    # Aggregate once before pagination; count market vehicles, not all vehicles in a cross-market PI.
    headers = select(PiOrderHeader.pi_id, PiOrderHeader.order_month).where(
        PiOrderHeader.order_month.like(f"{year:04d}-%"),
    )
    if country:
        headers = headers.where(_header_country_filter(country))
    if allowed_countries is not None or allowed_brands is not None:
        headers = headers.where(_header_access_filter(allowed_countries, allowed_brands))
    headers = headers.cte("browse_pis")
    vehicles = select(PiVehicleUnit.pi_id, func.count().label("units")).join(
        headers, PiVehicleUnit.pi_id == headers.c.pi_id,
    )
    if country:
        vehicles = vehicles.where(PiVehicleUnit.country_code == country)
    if allowed_countries is not None:
        vehicles = vehicles.where(PiVehicleUnit.country_code.in_(allowed_countries))
    if allowed_brands is not None:
        vehicles = vehicles.where(func.upper(PiVehicleUnit.brand).in_(allowed_brands))
    vehicles = vehicles.group_by(PiVehicleUnit.pi_id).subquery()
    stmt = (
        select(headers.c.order_month, func.count().label("pis"),
               func.coalesce(func.sum(vehicles.c.units), 0).label("units"))
        .outerjoin(vehicles, headers.c.pi_id == vehicles.c.pi_id)
    )
    rows = session.execute(stmt.group_by(headers.c.order_month).order_by(headers.c.order_month)).all()
    return {"year": year, "items": [
        {"month": row.order_month, "piCount": int(row.pis), "vehicleCount": int(row.units)}
        for row in rows
    ]}


def visible_header_markets(session: Session, pi_codes: list[str], countries: set[str] | None,
                           brands: set[str] | None) -> dict[str, set[str]]:
    market = func.coalesce(PiOrderLineAllocation.market_country_code, PiOrderHeader.country_code)
    stmt = select(PiOrderHeader.pi_code, market).join(PiOrderLine, PiOrderLine.pi_id == PiOrderHeader.pi_id).outerjoin(
        PiOrderLineAllocation, PiOrderLineAllocation.pi_line_id == PiOrderLine.pi_line_id,
    ).where(PiOrderHeader.pi_code.in_(pi_codes))
    if countries is not None:
        stmt = stmt.where(market.in_(countries))
    if brands is not None:
        stmt = stmt.where(func.upper(PiOrderLine.brand).in_(brands))
    units = select(PiVehicleUnit.pi_code, PiVehicleUnit.country_code).where(PiVehicleUnit.pi_code.in_(pi_codes))
    if countries is not None:
        units = units.where(PiVehicleUnit.country_code.in_(countries))
    if brands is not None:
        units = units.where(func.upper(PiVehicleUnit.brand).in_(brands))
    result: dict[str, set[str]] = {}
    for code, country in session.execute(stmt.union(units)):
        result.setdefault(code, set()).add(country)
    return result


def list_headers(
    session: Session,
    country: str | None = None,
    month: str | None = None,
    status: str | None = None,
    keyword: str | None = None,
    page: int = 1,
    page_size: int = 50,
    allowed_countries: set[str] | None = None,
    allowed_brands: set[str] | None = None,
) -> tuple[list[PiOrderHeader], int]:
    stmt = select(PiOrderHeader)
    if allowed_countries is not None or allowed_brands is not None:
        stmt = stmt.where(_header_access_filter(allowed_countries, allowed_brands))
    if country:
        stmt = stmt.where(_header_country_filter(country))
    if month:
        stmt = stmt.where(PiOrderHeader.order_month == month)
    if status:
        stmt = stmt.where(PiOrderHeader.status == status)
    if keyword:
        pattern = f"%{keyword}%"
        stmt = stmt.where(or_(
            PiOrderHeader.pi_code.ilike(pattern),
            PiOrderHeader.official_pi_no.ilike(pattern),
            PiOrderHeader.ship_name.ilike(pattern),
        ))
    count_stmt = select(func.count()).select_from(stmt.subquery())
    total = int(session.execute(count_stmt).scalar_one() or 0)
    rows = session.execute(
        stmt.order_by(PiOrderHeader.created_at_utc.desc())
        .offset(max(page - 1, 0) * page_size)
        .limit(page_size)
    ).scalars().all()
    return list(rows), total


def list_lines_by_pi(session: Session, pi_id) -> list[PiOrderLine]:
    return list(session.execute(
        select(PiOrderLine).where(PiOrderLine.pi_id == pi_id).order_by(PiOrderLine.line_sequence_no)
    ).scalars().all())


def list_allocations_by_pi(session: Session, pi_id) -> list[PiOrderLineAllocation]:
    return list(session.execute(
        select(PiOrderLineAllocation)
        .where(PiOrderLineAllocation.pi_id == pi_id)
        .order_by(PiOrderLineAllocation.pi_line_code, PiOrderLineAllocation.market_country_code)
    ).scalars().all())


def list_allocations_by_line(session: Session, pi_line_id) -> list[PiOrderLineAllocation]:
    return list(session.execute(
        select(PiOrderLineAllocation)
        .where(PiOrderLineAllocation.pi_line_id == pi_line_id)
        .order_by(PiOrderLineAllocation.market_country_code)
    ).scalars().all())


def list_lines_for_country_month(
    session: Session,
    country_code: str,
    order_month: str,
    include_cancelled: bool = False,
) -> list[PiOrderLine]:
    stmt = (
        select(PiOrderLine)
        .join(PiOrderHeader, PiOrderLine.pi_id == PiOrderHeader.pi_id)
        .where(
            PiOrderHeader.country_code == country_code,
            PiOrderHeader.order_month == order_month,
        )
        .order_by(PiOrderHeader.pi_sequence_no, PiOrderLine.line_sequence_no)
    )
    if not include_cancelled:
        stmt = stmt.where(PiOrderHeader.status != "cancelled")
    return list(session.execute(stmt).scalars().all())


def list_allocations_for_country_month(
    session: Session,
    country_code: str,
    order_year: int,
    order_month: int,
    include_cancelled: bool = False,
) -> list[PiOrderLineAllocation]:
    stmt = (
        select(PiOrderLineAllocation)
        .join(PiOrderHeader, PiOrderLineAllocation.pi_id == PiOrderHeader.pi_id)
        .where(
            PiOrderLineAllocation.market_country_code == country_code,
            PiOrderLineAllocation.order_year == order_year,
            PiOrderLineAllocation.order_month == order_month,
        )
        .order_by(PiOrderHeader.pi_sequence_no, PiOrderLineAllocation.pi_line_code)
    )
    if not include_cancelled:
        stmt = stmt.where(PiOrderHeader.status != "cancelled")
    return list(session.execute(stmt).scalars().all())


def list_lines_without_allocations_for_country_month(
    session: Session,
    country_code: str,
    order_month: str,
    include_cancelled: bool = False,
) -> list[PiOrderLine]:
    allocated_line_ids = select(PiOrderLineAllocation.pi_line_id)
    stmt = (
        select(PiOrderLine)
        .join(PiOrderHeader, PiOrderLine.pi_id == PiOrderHeader.pi_id)
        .where(
            PiOrderHeader.country_code == country_code,
            PiOrderHeader.order_month == order_month,
            ~PiOrderLine.pi_line_id.in_(allocated_line_ids),
        )
        .order_by(PiOrderHeader.pi_sequence_no, PiOrderLine.line_sequence_no)
    )
    if not include_cancelled:
        stmt = stmt.where(PiOrderHeader.status != "cancelled")
    return list(session.execute(stmt).scalars().all())


def count_vehicles_for_line(session: Session, pi_line_code: str) -> int:
    stmt = select(func.count()).select_from(PiVehicleUnit).where(
        PiVehicleUnit.pi_line_code == pi_line_code,
    )
    return int(session.execute(stmt).scalar_one() or 0)


def count_vehicles_for_line_country(session: Session, pi_line_code: str, country_code: str) -> int:
    stmt = select(func.count()).select_from(PiVehicleUnit).where(
        PiVehicleUnit.pi_line_code == pi_line_code,
        PiVehicleUnit.country_code == country_code,
    )
    return int(session.execute(stmt).scalar_one() or 0)


def list_vehicles_by_line(session: Session, pi_line_code: str) -> list[PiVehicleUnit]:
    return list(session.execute(
        select(PiVehicleUnit).where(PiVehicleUnit.pi_line_code == pi_line_code)
    ).scalars().all())


def list_vehicles_for_bulk_update(
    session: Session,
    *,
    pi_code: str,
    pi_line_code: str | None = None,
    lock: bool = False,
) -> list[PiVehicleUnit]:
    stmt = select(PiVehicleUnit).where(PiVehicleUnit.pi_code == pi_code)
    if pi_line_code:
        stmt = stmt.where(PiVehicleUnit.pi_line_code == pi_line_code)
    if lock:
        stmt = stmt.with_for_update().execution_options(populate_existing=True)
    return list(session.execute(
        stmt.order_by(PiVehicleUnit.pi_line_code, PiVehicleUnit.car_code)
    ).scalars().all())


def list_vehicles(
    session: Session,
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
    eta_from: date | None = None,
    eta_to: date | None = None,
    ready_from: date | None = None,
    ready_to: date | None = None,
    vin_missing_only: bool = False,
    unallocated_only: bool = False,
    page: int = 1,
    page_size: int = 100,
    allowed_countries: set[str] | None = None,
    allowed_brands: set[str] | None = None,
) -> tuple[list[PiVehicleUnit], int]:
    stmt = select(PiVehicleUnit)
    if allowed_countries is not None:
        stmt = stmt.where(PiVehicleUnit.country_code.in_(allowed_countries))
    if allowed_brands is not None:
        stmt = stmt.where(func.upper(PiVehicleUnit.brand).in_(allowed_brands))
    if order_month:
        stmt = stmt.join(PiOrderHeader, PiVehicleUnit.pi_code == PiOrderHeader.pi_code)
        stmt = stmt.where(PiOrderHeader.order_month == order_month)
    if keyword:
        pattern = f"%{keyword}%"
        stmt = stmt.where(or_(
            PiVehicleUnit.pi_code.ilike(pattern),
            PiVehicleUnit.pi_line_code.ilike(pattern),
            PiVehicleUnit.car_code.ilike(pattern),
            PiVehicleUnit.vin.ilike(pattern),
            PiVehicleUnit.material_code.ilike(pattern),
            PiVehicleUnit.bom.ilike(pattern),
            PiVehicleUnit.model_name.ilike(pattern),
            PiVehicleUnit.version.ilike(pattern),
            PiVehicleUnit.exterior_color_name.ilike(pattern),
            PiVehicleUnit.interior_color_name.ilike(pattern),
            PiVehicleUnit.brand.ilike(pattern),
            PiVehicleUnit.powertrain.ilike(pattern),
            PiVehicleUnit.country_code.ilike(pattern),
            PiVehicleUnit.ship_name.ilike(pattern),
            PiVehicleUnit.dealer_code.ilike(pattern),
            PiVehicleUnit.dealer_name.ilike(pattern),
            PiVehicleUnit.customer_ref.ilike(pattern),
            PiVehicleUnit.remark.ilike(pattern),
            PiVehicleUnit.allocation_status.ilike(pattern),
            PiVehicleUnit.logistics_status.ilike(pattern),
        ))
    filters = {
        PiVehicleUnit.pi_code: pi_code,
        PiVehicleUnit.pi_line_code: pi_line_code,
        PiVehicleUnit.car_code: car_code,
        PiVehicleUnit.vin: vin,
        PiVehicleUnit.material_code: material_code,
        PiVehicleUnit.bom: bom,
        PiVehicleUnit.country_code: country,
        PiVehicleUnit.ship_name: ship_name,
        PiVehicleUnit.allocation_status: allocation_status,
        PiVehicleUnit.logistics_status: logistics_status,
    }
    for column, value in filters.items():
        if value:
            stmt = stmt.where(column == value)
    text_filters = {
        PiVehicleUnit.brand: brand,
        PiVehicleUnit.model_name: model_name,
        PiVehicleUnit.version: version,
        PiVehicleUnit.powertrain: powertrain,
        PiVehicleUnit.exterior_color_name: exterior_color_name,
        PiVehicleUnit.interior_color_name: interior_color_name,
    }
    for column, value in text_filters.items():
        if value:
            stmt = stmt.where(column.ilike(f"%{value}%"))
    if eta_from:
        stmt = stmt.where(PiVehicleUnit.eta >= eta_from)
    if eta_to:
        stmt = stmt.where(PiVehicleUnit.eta <= eta_to)
    if ready_from:
        stmt = stmt.where(PiVehicleUnit.ready_for_pickup_date >= ready_from)
    if ready_to:
        stmt = stmt.where(PiVehicleUnit.ready_for_pickup_date <= ready_to)
    if vin_missing_only:
        stmt = stmt.where(or_(PiVehicleUnit.vin.is_(None), PiVehicleUnit.vin == ""))
    if unallocated_only:
        stmt = stmt.where(PiVehicleUnit.allocation_status == "unallocated")

    count_stmt = select(func.count()).select_from(stmt.subquery())
    total = int(session.execute(count_stmt).scalar_one() or 0)
    rows = session.execute(
        stmt.order_by(
            case((func.upper(PiVehicleUnit.brand).like("%OMODA%"), 0),
                 (func.upper(PiVehicleUnit.brand).like("%JAECOO%"), 1), else_=2),
            func.lower(PiVehicleUnit.brand),
            func.coalesce(cast(func.substring(PiVehicleUnit.model_name, r"\d+"), Integer), 2147483647),
            case((func.upper(PiVehicleUnit.powertrain) == "ICE", 0),
                 (func.upper(PiVehicleUnit.powertrain) == "HEV", 1),
                 (func.upper(PiVehicleUnit.powertrain) == "BEV", 2),
                 (func.upper(PiVehicleUnit.powertrain).in_(["PHEV", "SHS"]), 3), else_=9),
            func.lower(PiVehicleUnit.powertrain), func.lower(PiVehicleUnit.model_name),
            func.lower(PiVehicleUnit.version), PiVehicleUnit.bom,
            PiVehicleUnit.material_code, PiVehicleUnit.pi_line_code, PiVehicleUnit.car_code,
        )
        .offset(max(page - 1, 0) * page_size)
        .limit(page_size)
    ).scalars().all()
    return list(rows), total


def vehicle_summary(session: Session, pi_code: str, countries: set[str] | None = None,
                    brands: set[str] | None = None) -> dict:
    rows = session.execute(
        select(
            func.count().label("total"),
            func.sum(case((PiVehicleUnit.allocation_status == "allocated", 1), else_=0)).label("allocated"),
            func.sum(case((PiVehicleUnit.allocation_status == "reserved", 1), else_=0)).label("reserved"),
            func.sum(case((PiVehicleUnit.allocation_status == "unallocated", 1), else_=0)).label("unallocated"),
            func.sum(case((and_(PiVehicleUnit.vin.is_not(None), PiVehicleUnit.vin != ""), 1), else_=0)).label("vin_assigned"),
            func.sum(case((or_(PiVehicleUnit.vin.is_(None), PiVehicleUnit.vin == ""), 1), else_=0)).label("vin_missing"),
            func.sum(case((PiVehicleUnit.logistics_status == "on_vessel", 1), else_=0)).label("on_vessel"),
            func.sum(case((PiVehicleUnit.logistics_status == "arrived_at_port", 1), else_=0)).label("arrived"),
            func.sum(case((PiVehicleUnit.logistics_status == "ready_for_pickup", 1), else_=0)).label("ready_for_pickup"),
        ).where(PiVehicleUnit.pi_code == pi_code,
                *([func.upper(PiVehicleUnit.brand).in_(brands)] if brands is not None else []),
                *([PiVehicleUnit.country_code.in_(countries)] if countries is not None else []))
    ).first()
    if not rows:
        return {}
    return {
        "totalUnits": int(rows.total or 0),
        "allocated": int(rows.allocated or 0),
        "reserved": int(rows.reserved or 0),
        "unallocated": int(rows.unallocated or 0),
        "vinAssigned": int(rows.vin_assigned or 0),
        "vinMissing": int(rows.vin_missing or 0),
        "onVessel": int(rows.on_vessel or 0),
        "arrived": int(rows.arrived or 0),
        "readyForPickup": int(rows.ready_for_pickup or 0),
    }


def add_header(session: Session, header: PiOrderHeader) -> PiOrderHeader:
    session.add(header)
    session.flush()
    return header


def add_line(session: Session, line: PiOrderLine) -> PiOrderLine:
    session.add(line)
    session.flush()
    return line


def add_line_allocation(session: Session, allocation: PiOrderLineAllocation) -> PiOrderLineAllocation:
    session.add(allocation)
    session.flush()
    return allocation


def add_vehicle(session: Session, vehicle: PiVehicleUnit) -> PiVehicleUnit:
    session.add(vehicle)
    session.flush()
    return vehicle


def delete_header(session: Session, pi_code: str) -> bool:
    """Hard-delete a PI header. Cascades to lines, allocations, vehicles via FK."""
    header = get_header_by_code(session, pi_code)
    if not header:
        return False
    session.delete(header)
    session.flush()
    return True


def delete_line(session: Session, pi_line_code: str) -> bool:
    """Hard-delete a PI line. Cascades to allocations, vehicles via FK."""
    line = get_line_by_code(session, pi_line_code)
    if not line:
        return False
    session.delete(line)
    session.flush()
    return True
