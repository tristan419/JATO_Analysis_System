"""Repository layer for Order Genius — stateless data access functions.

All functions take ``session: Session`` as the first argument.
No commits or rollbacks — transaction control lives in the service/route layer.
"""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID, uuid4

from sqlalchemy import and_, func, select, update
from sqlalchemy.orm import Session

from app.db.models import (
    BrandColourSurchargeRule,
    CountryPaymentTermMaster,
    CountrySkuFobResolved,
    MaterialBaselineVersion,
    MaterialSkuMaster,
    MaterialSkuRemarkHistory,
    OrderQuantityCell,
    PaymentTermPriceRule,
)


# ── MaterialBaselineVersion ────────────────────────────────────────────


def create_baseline_version(
    session: Session,
    source_file_name: str,
    source_file_hash: str | None,
    baseline_name: str,
    published_by: str,
    source_upload_id: UUID | None = None,
) -> MaterialBaselineVersion:
    baseline = MaterialBaselineVersion(
        baseline_version_id=uuid4(),
        source_upload_id=source_upload_id,
        source_file_name=source_file_name,
        source_file_hash=source_file_hash,
        baseline_name=baseline_name,
        status="published",
        published_by=published_by,
        published_at_utc=datetime.now(timezone.utc),
    )
    session.add(baseline)
    return baseline


def get_latest_baseline(session: Session) -> MaterialBaselineVersion | None:
    stmt = (
        select(MaterialBaselineVersion)
        .where(MaterialBaselineVersion.status == "published")
        .order_by(MaterialBaselineVersion.created_at_utc.desc())
        .limit(1)
    )
    return session.execute(stmt).scalars().first()


def get_baseline_by_id(
    session: Session, baseline_version_id: UUID
) -> MaterialBaselineVersion | None:
    return session.get(MaterialBaselineVersion, baseline_version_id)


def list_baseline_versions(
    session: Session, limit: int = 20
) -> list[MaterialBaselineVersion]:
    stmt = (
        select(MaterialBaselineVersion)
        .order_by(MaterialBaselineVersion.created_at_utc.desc())
        .limit(limit)
    )
    return list(session.execute(stmt).scalars().all())


# ── MaterialSkuMaster ──────────────────────────────────────────────────


def bulk_create_skus(
    session: Session, skus: list[MaterialSkuMaster]
) -> None:
    session.add_all(skus)


def get_sku_by_material_code(
    session: Session, material_code: str
) -> MaterialSkuMaster | None:
    stmt = select(MaterialSkuMaster).where(
        MaterialSkuMaster.material_code == material_code,
        MaterialSkuMaster.is_active == True,
    )
    return session.execute(stmt).scalars().first()


def get_sku_by_material_code_any_status(
    session: Session, material_code: str
) -> MaterialSkuMaster | None:
    stmt = select(MaterialSkuMaster).where(
        MaterialSkuMaster.material_code == material_code
    )
    return session.execute(stmt).scalars().first()


def list_active_skus(
    session: Session,
    brand: str | None = None,
    model_name: str | None = None,
    powertrain: str | None = None,
    version: str | None = None,
    exterior_color_code: str | None = None,
    material_code_search: str | None = None,
    limit: int = 2000,
) -> list[MaterialSkuMaster]:
    stmt = select(MaterialSkuMaster).where(
        MaterialSkuMaster.is_active == True,
        MaterialSkuMaster.lifecycle_status == "active",
    )
    if brand:
        stmt = stmt.where(MaterialSkuMaster.brand == brand)
    if model_name:
        stmt = stmt.where(MaterialSkuMaster.model_name == model_name)
    if powertrain:
        stmt = stmt.where(MaterialSkuMaster.powertrain == powertrain)
    if version:
        stmt = stmt.where(MaterialSkuMaster.version == version)
    if exterior_color_code:
        stmt = stmt.where(
            MaterialSkuMaster.exterior_color_code == exterior_color_code
        )
    if material_code_search:
        stmt = stmt.where(
            MaterialSkuMaster.material_code.ilike(f"%{material_code_search}%")
        )
    stmt = stmt.order_by(MaterialSkuMaster.brand, MaterialSkuMaster.model_name)
    stmt = stmt.limit(limit)
    return list(session.execute(stmt).scalars().all())


def list_historical_skus_with_quantity(
    session: Session,
    country_code: str,
    order_year: int,
) -> list[str]:
    """Return material_codes of historical SKUs that have quantity data."""
    qty_sub = (
        select(OrderQuantityCell.material_code)
        .where(
            OrderQuantityCell.country_code == country_code,
            OrderQuantityCell.order_year == order_year,
            OrderQuantityCell.quantity > 0,
        )
        .distinct()
        .subquery()
    )
    stmt = (
        select(MaterialSkuMaster.material_code)
        .where(
            MaterialSkuMaster.lifecycle_status == "historical",
            MaterialSkuMaster.material_code.in_(select(qty_sub.c.material_code)),
        )
    )
    return [row[0] for row in session.execute(stmt).all()]


def get_active_sku_by_code(
    session: Session, material_code: str
) -> MaterialSkuMaster | None:
    stmt = select(MaterialSkuMaster).where(
        MaterialSkuMaster.material_code == material_code,
        MaterialSkuMaster.is_active == True,
        MaterialSkuMaster.lifecycle_status == "active",
    )
    return session.execute(stmt).scalars().first()


def transition_old_skus_to_historical(
    session: Session,
    new_material_codes: list[str],
    baseline_version_id: UUID,
) -> int:
    """Transition active SKUs not in the new set to historical."""
    stmt = (
        update(MaterialSkuMaster)
        .where(
            MaterialSkuMaster.is_active == True,
            MaterialSkuMaster.material_code.notin_(new_material_codes),
        )
        .values(lifecycle_status="historical", is_active=False)
    )
    result = session.execute(stmt)
    return result.rowcount


def update_sku_remark(
    session: Session,
    material_code: str,
    remark: str,
    expected_version: int,
) -> bool:
    stmt = (
        update(MaterialSkuMaster)
        .where(
            MaterialSkuMaster.material_code == material_code,
            MaterialSkuMaster.row_version == expected_version,
        )
        .values(remark=remark, row_version=expected_version + 1)
    )
    result = session.execute(stmt)
    return result.rowcount > 0


# ── Distinct filter values ─────────────────────────────────────────────


def list_distinct_brands(session: Session) -> list[str]:
    stmt = (
        select(MaterialSkuMaster.brand)
        .where(MaterialSkuMaster.is_active == True)
        .distinct()
        .order_by(MaterialSkuMaster.brand)
    )
    return [row[0] for row in session.execute(stmt).all()]


def list_distinct_models(
    session: Session, brand: str | None = None
) -> list[str]:
    stmt = (
        select(MaterialSkuMaster.model_name)
        .where(MaterialSkuMaster.is_active == True)
    )
    if brand:
        stmt = stmt.where(MaterialSkuMaster.brand == brand)
    stmt = stmt.distinct().order_by(MaterialSkuMaster.model_name)
    return [row[0] for row in session.execute(stmt).all()]


def list_distinct_powertrains(
    session: Session,
    brand: str | None = None,
    model_name: str | None = None,
) -> list[str]:
    stmt = (
        select(MaterialSkuMaster.powertrain)
        .where(
            MaterialSkuMaster.is_active == True,
            MaterialSkuMaster.powertrain.isnot(None),
        )
    )
    if brand:
        stmt = stmt.where(MaterialSkuMaster.brand == brand)
    if model_name:
        stmt = stmt.where(MaterialSkuMaster.model_name == model_name)
    stmt = stmt.distinct().order_by(MaterialSkuMaster.powertrain)
    return [row[0] for row in session.execute(stmt).all() if row[0]]


def list_distinct_versions(
    session: Session,
    brand: str | None = None,
    model_name: str | None = None,
    powertrain: str | None = None,
) -> list[str]:
    stmt = (
        select(MaterialSkuMaster.version)
        .where(MaterialSkuMaster.is_active == True)
    )
    if brand:
        stmt = stmt.where(MaterialSkuMaster.brand == brand)
    if model_name:
        stmt = stmt.where(MaterialSkuMaster.model_name == model_name)
    if powertrain:
        stmt = stmt.where(MaterialSkuMaster.powertrain == powertrain)
    stmt = stmt.distinct().order_by(MaterialSkuMaster.version)
    return [row[0] for row in session.execute(stmt).all()]


def list_distinct_colours(
    session: Session,
    brand: str | None = None,
    model_name: str | None = None,
    powertrain: str | None = None,
    version: str | None = None,
) -> list[str]:
    stmt = (
        select(MaterialSkuMaster.exterior_color_name)
        .where(MaterialSkuMaster.is_active == True)
    )
    if brand:
        stmt = stmt.where(MaterialSkuMaster.brand == brand)
    if model_name:
        stmt = stmt.where(MaterialSkuMaster.model_name == model_name)
    if powertrain:
        stmt = stmt.where(MaterialSkuMaster.powertrain == powertrain)
    if version:
        stmt = stmt.where(MaterialSkuMaster.version == version)
    stmt = stmt.distinct().order_by(MaterialSkuMaster.exterior_color_name)
    return [row[0] for row in session.execute(stmt).all()]


def list_distinct_material_codes(
    session: Session,
    brand: str | None = None,
    model_name: str | None = None,
    powertrain: str | None = None,
    version: str | None = None,
    exterior_color_name: str | None = None,
) -> list[str]:
    stmt = (
        select(MaterialSkuMaster.material_code)
        .where(MaterialSkuMaster.is_active == True)
    )
    if brand:
        stmt = stmt.where(MaterialSkuMaster.brand == brand)
    if model_name:
        stmt = stmt.where(MaterialSkuMaster.model_name == model_name)
    if powertrain:
        stmt = stmt.where(MaterialSkuMaster.powertrain == powertrain)
    if version:
        stmt = stmt.where(MaterialSkuMaster.version == version)
    if exterior_color_name:
        stmt = stmt.where(
            MaterialSkuMaster.exterior_color_name == exterior_color_name
        )
    stmt = stmt.distinct().order_by(MaterialSkuMaster.material_code)
    return [row[0] for row in session.execute(stmt).all()]


# ── Payment Terms ──────────────────────────────────────────────────────


def get_country_payment_term(
    session: Session, country_code: str
) -> CountryPaymentTermMaster | None:
    stmt = select(CountryPaymentTermMaster).where(
        CountryPaymentTermMaster.country_code == country_code,
        CountryPaymentTermMaster.is_active == True,
    )
    return session.execute(stmt).scalars().first()


def get_payment_term_rule(
    session: Session, payment_term_code: str
) -> PaymentTermPriceRule | None:
    stmt = select(PaymentTermPriceRule).where(
        PaymentTermPriceRule.payment_term_code == payment_term_code,
        PaymentTermPriceRule.is_active == True,
    )
    return session.execute(stmt).scalars().first()


def list_payment_term_rules(session: Session) -> list[PaymentTermPriceRule]:
    stmt = select(PaymentTermPriceRule).where(
        PaymentTermPriceRule.is_active == True
    )
    return list(session.execute(stmt).scalars().all())


def list_country_payment_terms(
    session: Session,
) -> list[CountryPaymentTermMaster]:
    stmt = select(CountryPaymentTermMaster).where(
        CountryPaymentTermMaster.is_active == True
    )
    return list(session.execute(stmt).scalars().all())


# ── Colour Surcharge ───────────────────────────────────────────────────


def get_brand_colour_surcharge(
    session: Session, brand: str, colour_type: str
) -> BrandColourSurchargeRule | None:
    stmt = select(BrandColourSurchargeRule).where(
        BrandColourSurchargeRule.brand == brand,
        BrandColourSurchargeRule.colour_type == colour_type,
        BrandColourSurchargeRule.is_active == True,
    )
    return session.execute(stmt).scalars().first()


def list_colour_surcharges(
    session: Session,
) -> list[BrandColourSurchargeRule]:
    stmt = select(BrandColourSurchargeRule).where(
        BrandColourSurchargeRule.is_active == True
    )
    return list(session.execute(stmt).scalars().all())


# ── CountrySkuFobResolved ──────────────────────────────────────────────


def upsert_fob_resolved(
    session: Session,
    fob: CountrySkuFobResolved,
) -> CountrySkuFobResolved:
    existing = get_fob_for_country_sku(
        session, fob.country_code, fob.material_code
    )
    if existing:
        existing.payment_term_code = fob.payment_term_code
        existing.base_fob_eur = fob.base_fob_eur
        existing.payment_term_adjustment_eur = fob.payment_term_adjustment_eur
        existing.colour_surcharge_eur = fob.colour_surcharge_eur
        existing.final_fob_eur = fob.final_fob_eur
        existing.baseline_version_id = fob.baseline_version_id
        existing.is_active = True
        return existing
    session.add(fob)
    return fob


def get_fob_for_country_sku(
    session: Session, country_code: str, material_code: str
) -> CountrySkuFobResolved | None:
    stmt = select(CountrySkuFobResolved).where(
        CountrySkuFobResolved.country_code == country_code,
        CountrySkuFobResolved.material_code == material_code,
        CountrySkuFobResolved.is_active == True,
    )
    return session.execute(stmt).scalars().first()


def list_fob_by_country(
    session: Session, country_code: str
) -> list[CountrySkuFobResolved]:
    stmt = select(CountrySkuFobResolved).where(
        CountrySkuFobResolved.country_code == country_code,
        CountrySkuFobResolved.is_active == True,
    )
    return list(session.execute(stmt).scalars().all())


def list_active_fob_material_codes(
    session: Session, country_code: str
) -> set[str]:
    stmt = select(CountrySkuFobResolved.material_code).where(
        CountrySkuFobResolved.country_code == country_code,
        CountrySkuFobResolved.is_active == True,
    )
    return {row[0] for row in session.execute(stmt).all()}


# ── OrderQuantityCell ──────────────────────────────────────────────────


def upsert_quantity_cell(
    session: Session,
    country_code: str,
    order_year: int,
    order_month: int,
    material_code: str,
    quantity: int,
    fob_eur: float,
    updated_by: str,
    expected_version: int,
) -> OrderQuantityCell | None:
    existing = get_quantity_cell(
        session, country_code, order_year, order_month, material_code
    )
    if existing:
        if existing.row_version != expected_version:
            return None  # optimistic lock failure
        existing.quantity = quantity
        existing.fob_eur = fob_eur
        existing.updated_by = updated_by
        existing.row_version = expected_version + 1
        existing.updated_at_utc = datetime.now(timezone.utc)
        return existing

    cell = OrderQuantityCell(
        order_quantity_cell_id=uuid4(),
        country_code=country_code,
        order_year=order_year,
        order_month=order_month,
        material_code=material_code,
        quantity=quantity,
        fob_eur=fob_eur,
        row_version=1,
        created_by=updated_by,
        updated_by=updated_by,
    )
    session.add(cell)
    return cell


def get_quantity_cell(
    session: Session,
    country_code: str,
    order_year: int,
    order_month: int,
    material_code: str,
) -> OrderQuantityCell | None:
    stmt = select(OrderQuantityCell).where(
        OrderQuantityCell.country_code == country_code,
        OrderQuantityCell.order_year == order_year,
        OrderQuantityCell.order_month == order_month,
        OrderQuantityCell.material_code == material_code,
    )
    return session.execute(stmt).scalars().first()


def list_quantities_for_country_year(
    session: Session, country_code: str, order_year: int
) -> list[OrderQuantityCell]:
    stmt = select(OrderQuantityCell).where(
        OrderQuantityCell.country_code == country_code,
        OrderQuantityCell.order_year == order_year,
    )
    return list(session.execute(stmt).scalars().all())


# ── Remark History ─────────────────────────────────────────────────────


def add_remark_history(
    session: Session,
    material_code: str,
    old_remark: str | None,
    new_remark: str | None,
    updated_by: str,
) -> None:
    history = MaterialSkuRemarkHistory(
        remark_history_id=uuid4(),
        material_code=material_code,
        old_remark=old_remark,
        new_remark=new_remark,
        updated_by=updated_by,
    )
    session.add(history)


def list_remark_history(
    session: Session, material_code: str, limit: int = 20
) -> list[MaterialSkuRemarkHistory]:
    stmt = (
        select(MaterialSkuRemarkHistory)
        .where(MaterialSkuRemarkHistory.material_code == material_code)
        .order_by(MaterialSkuRemarkHistory.updated_at_utc.desc())
        .limit(limit)
    )
    return list(session.execute(stmt).scalars().all())
