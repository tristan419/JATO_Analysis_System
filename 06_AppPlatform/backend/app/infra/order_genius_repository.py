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
    FobResolvedHistory,
    MaterialBaselineVersion,
    MaterialSkuMaster,
    MaterialSkuRemarkHistory,
    OrderQuantityCell,
    PaymentTermPriceRule,
    QuantityCellHistory,
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
    stmt = (
        select(MaterialSkuMaster)
        .where(MaterialSkuMaster.material_code == material_code)
        .order_by(MaterialSkuMaster.is_active.desc(), MaterialSkuMaster.row_version.desc())
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


def update_sku_lifecycle(
    session: Session,
    material_code: str,
    lifecycle_status: str,
    effective_from: str | None = None,
    effective_to: str | None = None,
    expected_version: int = 1,
) -> MaterialSkuMaster | None:
    """Update a single SKU's lifecycle status with optimistic locking.

    Uses any_status lookup so that historical SKUs can be re-activated.
    """
    sku = get_sku_by_material_code_any_status(session, material_code)
    if not sku:
        return None
    if sku.row_version != expected_version:
        return None
    sku.lifecycle_status = lifecycle_status
    if lifecycle_status == "historical":
        sku.is_active = False
    elif lifecycle_status == "active":
        sku.is_active = True
    if effective_from is not None:
        sku.effective_from_month = effective_from
    if effective_to is not None:
        sku.effective_to_month = effective_to
    sku.row_version = expected_version + 1
    sku.updated_at_utc = datetime.now(timezone.utc)
    return sku


def delete_sku(session: Session, material_code: str) -> bool:
    """Hard-delete ALL rows with this material_code (active + historical)."""
    from sqlalchemy import delete as sa_delete

    stmt = sa_delete(MaterialSkuMaster).where(
        MaterialSkuMaster.material_code == material_code
    )
    result = session.execute(stmt)
    return result.rowcount > 0


def update_sku_fob_for_country(
    session: Session,
    material_code: str,
    country_code: str,
    final_fob_eur: float | None,
    payment_term_code: str | None = None,
) -> CountrySkuFobResolved | None:
    """Update or create FOB for a specific material + country. Pass None to deactivate."""
    stmt = select(CountrySkuFobResolved).where(
        CountrySkuFobResolved.material_code == material_code,
        CountrySkuFobResolved.country_code == country_code,
        CountrySkuFobResolved.is_active == True,
    )
    if payment_term_code:
        stmt = stmt.where(CountrySkuFobResolved.payment_term_code == payment_term_code)
    existing = session.execute(stmt).scalars().first()

    if existing:
        if final_fob_eur is None:
            existing.is_active = False
            existing.updated_at_utc = datetime.now(timezone.utc)
            return existing
        existing.final_fob_eur = final_fob_eur
        existing.updated_at_utc = datetime.now(timezone.utc)
        return existing
    # Create new FOB record — use first available baseline version
    bv = session.execute(
        select(MaterialBaselineVersion.baseline_version_id).order_by(MaterialBaselineVersion.created_at_utc.desc()).limit(1)
    ).scalar()
    fob = CountrySkuFobResolved(
        country_sku_fob_id=uuid4(),
        baseline_version_id=bv or UUID("00000000-0000-0000-0000-000000000000"),
        country_code=country_code,
        material_code=material_code,
        payment_term_code=payment_term_code or "TT",
        final_fob_eur=final_fob_eur,
        fob_source_mode="manual_edit",
        is_active=True,
    )
    session.add(fob)
    return fob


def list_bom_with_fob(
    session: Session,
    brand: str | None = None,
    search: str | None = None,
    country_code: str | None = None,
    limit: int = 1000,
) -> list[dict]:
    """Return SKUs with their FOB per country, grouped for BOM admin display."""
    skus = list_all_material_skus_for_admin(session, brand=brand, search=search, country_code=country_code, limit=limit)
    if not skus:
        return []

    material_codes = [s.material_code for s in skus]
    fobs = session.execute(
        select(CountrySkuFobResolved).where(
            CountrySkuFobResolved.material_code.in_(material_codes),
            CountrySkuFobResolved.is_active == True,
        )
    ).scalars().all()

    # Build FOB map: material_code -> { country_code: { fob, paymentTerm } }
    fob_map: dict[str, dict] = {}
    for f in fobs:
        if f.material_code not in fob_map:
            fob_map[f.material_code] = {}
        fob_map[f.material_code][f.country_code] = {
            "finalFobEur": float(f.final_fob_eur),
            "uploadedFobEur": float(f.uploaded_fob_eur) if f.uploaded_fob_eur else None,
            "colourSurchargeEur": float(f.colour_surcharge_eur) if f.colour_surcharge_eur else None,
            "paymentTermCode": f.payment_term_code,
        }

    # Get all unique country codes: FOB data + configured payment terms (for new countries like NL)
    pt_countries = session.execute(
        select(CountryPaymentTermMaster.country_code).where(CountryPaymentTermMaster.is_active == True)
    ).scalars().all()
    all_countries = sorted(set(
        [f.country_code for f in fobs] + list(pt_countries)
    ))

    # Resolve source file names from baseline versions
    baseline_ids = {s.baseline_version_id for s in skus if s.baseline_version_id}
    baseline_names: dict[UUID, str] = {}
    if baseline_ids:
        baselines = session.execute(
            select(MaterialBaselineVersion).where(
                MaterialBaselineVersion.baseline_version_id.in_(baseline_ids)
            )
        ).scalars().all()
        baseline_names = {b.baseline_version_id: b.source_file_name for b in baselines}

    return [
        {
            "materialCode": s.material_code,
            "brand": s.brand,
            "modelName": s.model_name,
            "version": s.version,
            "colour": s.exterior_color_name or "",
            "colourCode": s.exterior_color_code or "",
            "colourType": s.exterior_color_type or "single",
            "colourHex": s.colour_hex,
            "colourCodeConfirmed": s.colour_code_confirmed,
            "colourTier": s.colour_tier or "single",
            "bomTemplate": s.bom_template,
            "interiorColorName": s.interior_color_name,
            "interiorColourCode": s.interior_colour_code,
            "interiorPackage": s.interior_package,
            "editionTag": s.edition_tag,
            "lifecycleStatus": s.lifecycle_status,
            "isActive": s.is_active,
            "effectiveFrom": s.effective_from_month,
            "effectiveTo": s.effective_to_month,
            "rowVersion": s.row_version,
            "fobByCountry": fob_map.get(s.material_code, {}),
            "sourceSheetName": s.source_sheet_name,
            "sourceRowNumber": s.source_row_number,
            "sourceFileName": baseline_names.get(s.baseline_version_id) if s.baseline_version_id else None,
            "sourcePayload": s.raw_payload_json,
        }
        for s in skus
    ], all_countries


def list_all_material_skus_for_admin(
    session: Session,
    country_code: str | None = None,
    brand: str | None = None,
    search: str | None = None,
    limit: int = 500,
) -> list[MaterialSkuMaster]:
    """List SKUs with optional filters for the BOM admin panel.

    Returns one row per material_code — prefers active, then highest row_version.
    """
    stmt = select(MaterialSkuMaster)
    if country_code:
        subq = select(CountrySkuFobResolved.material_code).where(
            CountrySkuFobResolved.country_code == country_code,
            CountrySkuFobResolved.is_active == True,
        ).distinct()
        stmt = stmt.where(MaterialSkuMaster.material_code.in_(subq))
    if brand:
        stmt = stmt.where(MaterialSkuMaster.brand == brand)
    if search:
        stmt = stmt.where(
            MaterialSkuMaster.material_code.ilike(f"%{search}%")
            | MaterialSkuMaster.model_name.ilike(f"%{search}%")
            | MaterialSkuMaster.brand.ilike(f"%{search}%")
        )
    stmt = stmt.order_by(
        MaterialSkuMaster.material_code,
        MaterialSkuMaster.is_active.desc(),
        MaterialSkuMaster.row_version.desc(),
    )
    stmt = stmt.limit(limit * 2)  # fetch extra to account for dedup
    all_rows = list(session.execute(stmt).scalars().all())

    # Deduplicate: one row per material_code, preferring active + highest version
    seen: set[str] = set()
    deduped: list[MaterialSkuMaster] = []
    for row in all_rows:
        if row.material_code in seen:
            continue
        seen.add(row.material_code)
        deduped.append(row)
        if len(deduped) >= limit:
            break

    # Re-sort by brand/model/version for display
    deduped.sort(key=lambda r: (r.brand or "", r.model_name or "", r.version or ""))
    return deduped


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


def update_sku_material_code(
    session: Session,
    old_material_code: str,
    new_material_code: str,
) -> bool:
    """Update a SKU's material code. Also updates related FOB records."""
    from app.db.models import CountrySkuFobResolved

    stmt = (
        update(MaterialSkuMaster)
        .where(MaterialSkuMaster.material_code == old_material_code)
        .values(material_code=new_material_code)
    )
    result = session.execute(stmt)
    # Update FOB records to match
    fob_stmt = (
        update(CountrySkuFobResolved)
        .where(CountrySkuFobResolved.material_code == old_material_code)
        .values(material_code=new_material_code)
    )
    session.execute(fob_stmt)
    return result.rowcount > 0


def update_sku_colour_tier(
    session: Session,
    material_code: str,
    colour_tier: str,
) -> bool:
    stmt = (
        update(MaterialSkuMaster)
        .where(MaterialSkuMaster.material_code == material_code)
        .values(colour_tier=colour_tier)
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


def list_all_payment_terms(
    session: Session,
) -> list[CountryPaymentTermMaster]:
    stmt = select(CountryPaymentTermMaster).order_by(
        CountryPaymentTermMaster.country_code,
        CountryPaymentTermMaster.valid_from_month.desc().nulls_last(),
    )
    return list(session.execute(stmt).scalars().all())


def create_payment_term(
    session: Session, **kw,
) -> CountryPaymentTermMaster:
    row = CountryPaymentTermMaster(
        country_payment_term_id=uuid4(), **kw,
    )
    session.add(row)
    return row


def get_country_payment_term(
    session: Session, country_code: str, order_month: str | None = None,
) -> CountryPaymentTermMaster | None:
    """Return the payment term for *country_code*.

    When *order_month* is given (YYYY-MM), the term valid at that time is
    returned.  Otherwise the currently active term is returned.
    """
    stmt = select(CountryPaymentTermMaster).where(
        CountryPaymentTermMaster.country_code == country_code,
        CountryPaymentTermMaster.is_active == True,
    )
    if order_month:
        stmt = stmt.where(
            CountryPaymentTermMaster.valid_from_month <= order_month,
            (
                CountryPaymentTermMaster.valid_to_month.is_(None)
                | (CountryPaymentTermMaster.valid_to_month >= order_month)
            ),
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
        session, fob.country_code, fob.material_code, fob.payment_term_code,
    )
    if existing:
        # Write history record before overwriting values
        old_uploaded = existing.uploaded_fob_eur
        old_final = existing.final_fob_eur
        if (
            old_uploaded != fob.uploaded_fob_eur
            or old_final != fob.final_fob_eur
        ):
            session.add(
                FobResolvedHistory(
                    country_sku_fob_id=existing.country_sku_fob_id,
                    baseline_version_id=fob.baseline_version_id,
                    country_code=fob.country_code,
                    material_code=fob.material_code,
                    payment_term_code=fob.payment_term_code,
                    old_uploaded_fob_eur=old_uploaded,
                    new_uploaded_fob_eur=fob.uploaded_fob_eur,
                    old_final_fob_eur=old_final,
                    new_final_fob_eur=fob.final_fob_eur,
                    changed_by="publish_baseline",
                )
            )
        existing.uploaded_fob_eur = fob.uploaded_fob_eur
        existing.final_fob_eur = fob.final_fob_eur
        existing.baseline_version_id = fob.baseline_version_id
        existing.fob_source_mode = fob.fob_source_mode
        existing.fob_source_country_code = fob.fob_source_country_code
        existing.is_active = True
        return existing
    session.add(fob)
    return fob


def get_fob_for_country_sku(
    session: Session,
    country_code: str,
    material_code: str,
    payment_term_code: str | None = None,  # kept for API compat, no longer filters
) -> CountrySkuFobResolved | None:
    """Get FOB for a country+material. PT is metadata only — not a filter."""
    stmt = select(CountrySkuFobResolved).where(
        CountrySkuFobResolved.country_code == country_code,
        CountrySkuFobResolved.material_code == material_code,
        CountrySkuFobResolved.is_active == True,
    )
    return session.execute(stmt).scalars().first()


def list_fob_by_country(
    session: Session,
    country_code: str,
    payment_term_code: str | None = None,
) -> list[CountrySkuFobResolved]:
    stmt = select(CountrySkuFobResolved).where(
        CountrySkuFobResolved.country_code == country_code,
        CountrySkuFobResolved.is_active == True,
    )
    if payment_term_code:
        stmt = stmt.where(
            CountrySkuFobResolved.payment_term_code == payment_term_code,
        )
    return list(session.execute(stmt).scalars().all())


def list_active_fob_material_codes(
    session: Session,
    country_code: str,
    payment_term_code: str | None = None,  # kept for API compat, no longer filters
) -> set[str]:
    return {row[0] for row in session.execute(
        select(CountrySkuFobResolved.material_code).where(
            CountrySkuFobResolved.country_code == country_code,
            CountrySkuFobResolved.is_active == True,
        )
    ).all()}


def get_country_fob_source_mapping(
    session: Session,
    target_country_code: str,
    target_payment_term_code: str,
) -> str | None:
    """Return the source country_code for a fallback mapping, or None."""
    from sqlalchemy import text as sa_text
    stmt = sa_text(
        "SELECT source_country_code FROM ordering.country_fob_source_mapping "
        "WHERE target_country_code = :target AND target_payment_term_code = :pt "
        "AND is_active = true LIMIT 1"
    )
    row = session.execute(
        stmt, {"target": target_country_code, "pt": target_payment_term_code}
    ).fetchone()
    return row[0] if row else None


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
        # Write history record before overwriting values
        if existing.quantity != quantity or existing.fob_eur != fob_eur:
            session.add(
                QuantityCellHistory(
                    country_code=country_code,
                    order_year=order_year,
                    order_month=order_month,
                    material_code=material_code,
                    old_quantity=existing.quantity,
                    new_quantity=quantity,
                    old_fob_eur=existing.fob_eur,
                    new_fob_eur=fob_eur,
                    changed_by=updated_by,
                )
            )
        existing.quantity = quantity
        existing.fob_eur = fob_eur
        existing.updated_by = updated_by
        existing.row_version = expected_version + 1
        existing.updated_at_utc = datetime.now(timezone.utc)
        return existing

    # Record initial value as history
    session.add(
        QuantityCellHistory(
            country_code=country_code,
            order_year=order_year,
            order_month=order_month,
            material_code=material_code,
            old_quantity=None,
            new_quantity=quantity,
            old_fob_eur=None,
            new_fob_eur=fob_eur,
            changed_by=updated_by,
        )
    )
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


def get_material_lifecycle(
    session: Session, country_code: str, material_code: str,
) -> list["MaterialLifecycle"]:
    from app.db.models import MaterialLifecycle

    stmt = (
        select(MaterialLifecycle)
        .where(
            MaterialLifecycle.country_code == country_code,
            MaterialLifecycle.material_code == material_code,
        )
        .order_by(MaterialLifecycle.valid_from.desc())
    )
    return list(session.execute(stmt).scalars().all())


def list_lifecycle_for_product(
    session: Session, country_code: str, product_identity: str,
) -> list["MaterialLifecycle"]:
    from app.db.models import MaterialLifecycle

    stmt = (
        select(MaterialLifecycle)
        .where(
            MaterialLifecycle.country_code == country_code,
            MaterialLifecycle.product_identity == product_identity,
        )
        .order_by(MaterialLifecycle.valid_from.desc())
    )
    return list(session.execute(stmt).scalars().all())
