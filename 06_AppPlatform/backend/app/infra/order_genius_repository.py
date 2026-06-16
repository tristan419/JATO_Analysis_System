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
from app.services.ordering_normalization import normalize_brand, normalize_brand_text


JATO_COUNTRY_NAMES_BY_CODE = {
    "AT": "Austria",
    "BE": "Belgium",
    "CH": "Switzerland",
    "CZ": "Czech Republic",
    "DE": "Germany",
    "DK": "Denmark",
    "ES": "Spain",
    "FI": "Finland",
    "FR": "France",
    "GR": "Greece",
    "HR": "Croatia",
    "HU": "Hungary",
    "IT": "Italy",
    "NL": "Netherlands",
    "NO": "Norway",
    "PL": "Poland",
    "PT": "Portugal",
    "RO": "Romania",
    "SE": "Sweden",
    "SI": "Slovenia",
    "SK": "Slovakia",
}

ORDERING_COUNTRY_NAMES_BY_CODE = {
    "BG": "Bulgaria",
    "BW": "Botswana",
    "CL": "Chile",
    "DM": "Dominican Republic",
    "GV": "Cape Verde",
    "KX": "Kuwait",
    "LV": "Latvia",
    "PU": "Portugal",
    "ZF": "South Africa",
    "ZU": "Zimbabwe",
}

COUNTRY_NAMES_BY_CODE = {
    **JATO_COUNTRY_NAMES_BY_CODE,
    **ORDERING_COUNTRY_NAMES_BY_CODE,
}


def _country_name_for_code(country_code: str) -> str:
    code = country_code.upper()
    return COUNTRY_NAMES_BY_CODE.get(code, code)


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
    if final_fob_eur is None:
        return None

    baseline = get_latest_baseline(session)
    if baseline is None:
        baseline = create_baseline_version(
            session,
            source_file_name="manual_admin",
            source_file_hash=None,
            baseline_name="Manual Admin Baseline",
            published_by="system",
        )
        session.flush()
    fob = CountrySkuFobResolved(
        country_sku_fob_id=uuid4(),
        baseline_version_id=baseline.baseline_version_id,
        country_code=country_code.upper(),
        material_code=material_code,
        payment_term_code=payment_term_code or "TT",
        final_fob_eur=final_fob_eur,
        fob_source_mode="manual_edit",
        is_active=True,
    )
    session.add(fob)
    return fob


def copy_country_fobs(
    session: Session,
    source_country_code: str,
    target_country_code: str,
    overwrite: bool = False,
    changed_by: str | None = None,
) -> dict:
    """Copy active FOB rows from one country to another for BOM Admin."""
    source_code = source_country_code.strip().upper()
    target_code = target_country_code.strip().upper()
    if not source_code or not target_code:
        raise ValueError("sourceCountryCode and targetCountryCode are required")
    if source_code == target_code:
        raise ValueError("sourceCountryCode and targetCountryCode must be different")

    source_rows = list_fob_by_country(session, source_code)
    target_term = get_country_payment_term(session, target_code)

    copied = 0
    updated = 0
    skipped = 0
    actor = changed_by or "copy_country_fobs"

    for source_row in source_rows:
        target_payment_term_code = (
            target_term.payment_term_code if target_term else source_row.payment_term_code
        )
        existing = get_fob_for_country_sku(
            session,
            target_code,
            source_row.material_code,
        )
        if existing and not overwrite:
            skipped += 1
            continue
        if existing:
            if (
                existing.payment_term_code == target_payment_term_code
                and existing.uploaded_fob_eur == source_row.uploaded_fob_eur
                and existing.final_fob_eur == source_row.final_fob_eur
                and existing.colour_surcharge_eur == source_row.colour_surcharge_eur
            ):
                skipped += 1
                continue
            session.add(
                FobResolvedHistory(
                    country_sku_fob_id=existing.country_sku_fob_id,
                    baseline_version_id=source_row.baseline_version_id,
                    country_code=target_code,
                    material_code=source_row.material_code,
                    payment_term_code=existing.payment_term_code,
                    old_uploaded_fob_eur=existing.uploaded_fob_eur,
                    new_uploaded_fob_eur=source_row.uploaded_fob_eur,
                    old_final_fob_eur=existing.final_fob_eur,
                    new_final_fob_eur=source_row.final_fob_eur,
                    changed_by=actor,
                )
            )
            existing.baseline_version_id = source_row.baseline_version_id
            existing.payment_term_code = target_payment_term_code
            existing.base_fob_eur = source_row.base_fob_eur
            existing.payment_term_adjustment_eur = source_row.payment_term_adjustment_eur
            existing.colour_surcharge_eur = source_row.colour_surcharge_eur
            existing.uploaded_fob_eur = source_row.uploaded_fob_eur
            existing.final_fob_eur = source_row.final_fob_eur
            existing.fob_source_country_code = source_code
            existing.fob_source_mode = "copied_from_country"
            existing.is_active = True
            existing.updated_at_utc = datetime.now(timezone.utc)
            updated += 1
            continue

        session.add(
            CountrySkuFobResolved(
                country_sku_fob_id=uuid4(),
                baseline_version_id=source_row.baseline_version_id,
                country_code=target_code,
                material_code=source_row.material_code,
                payment_term_code=target_payment_term_code,
                base_fob_eur=source_row.base_fob_eur,
                payment_term_adjustment_eur=source_row.payment_term_adjustment_eur,
                colour_surcharge_eur=source_row.colour_surcharge_eur,
                uploaded_fob_eur=source_row.uploaded_fob_eur,
                final_fob_eur=source_row.final_fob_eur,
                fob_source_country_code=source_code,
                fob_source_mode="copied_from_country",
                is_active=True,
            )
        )
        copied += 1

    return {
        "sourceCountryCode": source_code,
        "targetCountryCode": target_code,
        "totalSourceRows": len(source_rows),
        "copied": copied,
        "updated": updated,
        "skipped": skipped,
        "overwrite": overwrite,
    }


def list_bom_with_fob(
    session: Session,
    brand: str | None = None,
    search: str | None = None,
    country_code: str | None = None,
    limit: int = 1000,
) -> tuple[list[dict], list[str]]:
    """Return SKUs with their FOB per country, grouped for BOM admin display."""
    skus = list_all_material_skus_for_admin(session, brand=brand, search=search, country_code=country_code, limit=limit)
    all_countries = [item["countryCode"] for item in list_ordering_country_options(session)]
    if not skus:
        return [], all_countries

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
            "brand": normalize_brand(s.brand),
            "modelName": normalize_brand_text(s.model_name),
            "powertrain": s.powertrain,
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


def update_sku_metadata(
    session: Session,
    material_code: str,
    *,
    brand: str | None = None,
    model_name: str | None = None,
    version: str | None = None,
    powertrain: str | None = None,
    expected_version: int | None = None,
) -> MaterialSkuMaster | None:
    """Update product metadata for the SKU row shown in BOM Admin."""
    sku = get_sku_by_material_code_any_status(session, material_code)
    if not sku:
        return None
    if expected_version is not None and sku.row_version != expected_version:
        return None
    if brand is not None:
        sku.brand = normalize_brand(brand)
    if model_name is not None:
        sku.model_name = normalize_brand_text(model_name).strip()
    if version is not None:
        sku.version = version.strip()
    if powertrain is not None:
        sku.powertrain = powertrain.strip() or None
    sku.row_version += 1
    sku.updated_at_utc = datetime.now(timezone.utc)
    return sku


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


def list_ordering_country_options(session: Session) -> list[dict]:
    """Return countries available to ordering accounts.

    This includes JATO market countries, payment-term countries, and FOB-only
    countries such as LV, without pretending every option is a JATO market.
    """
    options: dict[str, dict] = {
        code: {
            "countryCode": code,
            "countryName": country_name,
            "paymentTermCode": None,
            "paymentMethod": None,
            "lcDays": None,
        }
        for code, country_name in JATO_COUNTRY_NAMES_BY_CODE.items()
    }
    term_rows = list_country_payment_terms(session)
    for term in term_rows:
        code = term.country_code.upper()
        options[code] = {
            "countryCode": code,
            "countryName": term.country_name or _country_name_for_code(code),
            "paymentTermCode": term.payment_term_code,
            "paymentMethod": term.payment_method,
            "lcDays": term.lc_days,
        }

    fob_country_codes = session.execute(
        select(CountrySkuFobResolved.country_code)
        .where(CountrySkuFobResolved.is_active == True)
        .distinct()
    ).scalars().all()
    for raw_code in fob_country_codes:
        code = str(raw_code or "").upper()
        if not code or code in options:
            continue
        options[code] = {
            "countryCode": code,
            "countryName": _country_name_for_code(code),
            "paymentTermCode": None,
            "paymentMethod": None,
            "lcDays": None,
        }

    return [options[code] for code in sorted(options)]


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


def upsert_colour_surcharge(
    session: Session,
    brand: str,
    colour_type: str,
    surcharge_eur: float,
) -> BrandColourSurchargeRule:
    normalized_brand = normalize_brand(brand)
    normalized_colour_type = colour_type.strip().lower()
    if normalized_colour_type not in {"dual", "special"}:
        raise ValueError("colourType must be dual or special")
    if surcharge_eur < 0:
        raise ValueError("surchargeEur must be greater than or equal to 0")

    existing = get_brand_colour_surcharge(
        session,
        normalized_brand,
        normalized_colour_type,
    )
    if existing:
        existing.surcharge_eur = surcharge_eur
        existing.updated_at_utc = datetime.now(timezone.utc)
        return existing

    rule = BrandColourSurchargeRule(
        colour_surcharge_rule_id=uuid4(),
        brand=normalized_brand,
        colour_type=normalized_colour_type,
        surcharge_eur=surcharge_eur,
        is_active=True,
    )
    session.add(rule)
    return rule


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


def list_quantities_for_country_month(
    session: Session,
    country_code: str,
    order_year: int,
    order_month: int,
    positive_only: bool = True,
) -> list[OrderQuantityCell]:
    stmt = select(OrderQuantityCell).where(
        OrderQuantityCell.country_code == country_code,
        OrderQuantityCell.order_year == order_year,
        OrderQuantityCell.order_month == order_month,
    )
    if positive_only:
        stmt = stmt.where(OrderQuantityCell.quantity > 0)
    stmt = stmt.order_by(OrderQuantityCell.material_code)
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
