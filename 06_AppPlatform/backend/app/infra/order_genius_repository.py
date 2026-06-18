"""Repository layer for Order Genius — stateless data access functions.

All functions take ``session: Session`` as the first argument.
No commits or rollbacks — transaction control lives in the service/route layer.
"""

from __future__ import annotations

import re
from collections import Counter
from copy import deepcopy
from datetime import datetime, timezone
from uuid import UUID, uuid4

from sqlalchemy import and_, func, inspect, select, update
from sqlalchemy.orm import Session

from app.db.models import (
    BrandColourSurchargeRule,
    CountryMaterialFinance,
    CountryMaterialFinanceHistory,
    CountryPaymentTermMaster,
    CountrySkuFobResolved,
    FobResolvedHistory,
    MaterialBaselineVersion,
    MaterialLifecycle,
    MaterialSkuMaster,
    MaterialSkuRemarkHistory,
    OrderQuantityCell,
    PaymentTermPriceRule,
    PiOrderLine,
    PiOrderLineAllocation,
    PiVehicleUnit,
    QuantityCellHistory,
)
from app.services.ordering_normalization import clean_text, normalize_brand, normalize_brand_text
from app.services.powertrain_normalizer import normalize_powertrain


COUNTRY_NAMES_BY_CODE: dict[str, str] = {
    "AT": "Austria",
    "BE": "Belgium",
    "BG": "Bulgaria",
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
    "LV": "Latvia",
    "NL": "Netherlands",
    "NO": "Norway",
    "PL": "Poland",
    "PT": "Portugal",
    "RO": "Romania",
    "SE": "Sweden",
    "SI": "Slovenia",
    "SK": "Slovakia",
}

SUPPORTED_ORDERING_COUNTRY_CODES = frozenset(COUNTRY_NAMES_BY_CODE)
COUNTRY_MATERIAL_FINANCE_VALUE_FIELDS = (
    "fob_eur",
    "retail_price_eur",
    "wholesale_price_eur",
    "dealer_price_eur",
    "cost_eur",
    "margin_eur",
    "margin_rate",
    "vehicle_margin_eur",
    "vehicle_margin_rate",
    "vehicle_profit_eur",
    "vehicle_profit_rate",
    "fob_delta_eur",
    "margin_delta_eur",
    "memo",
)

COUNTRY_MATERIAL_FINANCE_AUDIT_FIELDS = (
    "fob_eur",
    "retail_price_eur",
    "wholesale_price_eur",
    "dealer_price_eur",
    "cost_eur",
    "margin_eur",
    "margin_rate",
    "vehicle_margin_eur",
    "vehicle_margin_rate",
    "vehicle_profit_eur",
    "vehicle_profit_rate",
    "fob_delta_eur",
    "margin_delta_eur",
    "memo",
    "source_mode",
    "source_payload_json",
)


def _extract_canonical_powertrain(sku: MaterialSkuMaster) -> str:
    """Model name wins over stale imported powertrain values for BOM grouping/filtering."""
    raw_pt = clean_text(sku.powertrain).upper()
    model = clean_text(sku.model_name).upper()
    combined = f"{model} {raw_pt}"
    if "PHEV" in combined or "SHS" in combined or "PLUG" in combined:
        return "PHEV"
    if "MHEV" in combined or "MILD HYBRID" in combined:
        return "MHEV"
    if "REEV" in combined or "EREV" in combined or "RANGE EXTEND" in combined:
        return "REEV"
    if "FCEV" in combined or "FCV" in combined or "FUEL CELL" in combined:
        return "FCV"
    if "HEV" in combined or "HYBRID ELECTRIC" in combined:
        return "HEV"
    if "BEV" in combined or "BATTERY ELECTRIC" in combined:
        return "BEV"
    if "EV" in combined or "ELECTRIC" in combined:
        return "BEV"
    if "ICE" in combined or "PETROL" in combined or "DIESEL" in combined or "LPG" in combined:
        return "ICE"
    return normalize_powertrain(raw_pt) if raw_pt else "OTHER"


def normalize_colour_rule_name(colour_name: str | None) -> str:
    """Normalize colour names for reusable swatch rules."""
    return re.sub(r"\s+", " ", str(colour_name or "").strip()).casefold()


def normalize_colour_hex_value(colour_hex: str | None) -> str | None:
    """Normalize stored custom swatches; supports single and dual swatches."""
    text = str(colour_hex or "").strip().upper()
    if not text:
        return None
    if re.fullmatch(r"#[0-9A-F]{6}(\|#[0-9A-F]{6})?", text):
        return text
    raise ValueError("colourHex must be #RRGGBB or #RRGGBB|#RRGGBB")


def _colour_rule_group_key(
    brand: str | None,
    colour_code: str | None,
    colour_name: str | None,
) -> tuple[str, str, str] | None:
    normalized_brand = normalize_brand(str(brand or "")).strip()
    normalized_code = str(colour_code or "").strip().upper()
    normalized_name = normalize_colour_rule_name(colour_name)
    if not normalized_brand or not normalized_code or not normalized_name:
        return None
    return normalized_brand, normalized_code, normalized_name


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
    # Create new FOB record — use latest baseline, creating a manual baseline when needed.
    baseline = get_latest_baseline(session)
    if baseline is None:
        baseline = create_baseline_version(
            session=session,
            source_file_name="manual_admin",
            source_file_hash=None,
            baseline_name="manual_admin",
            published_by="system",
        )
        session.flush()
    fob = CountrySkuFobResolved(
        country_sku_fob_id=uuid4(),
        baseline_version_id=baseline.baseline_version_id,
        country_code=country_code,
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
    *,
    overwrite_existing: bool = False,
    changed_by: str | None = None,
) -> dict[str, int | str | None]:
    """Copy active FOB rows from one country to another."""
    source = str(source_country_code or "").strip().upper()
    target = str(target_country_code or "").strip().upper()
    source_rows = list_fob_by_country(session, source)
    target_term = get_country_payment_term(session, target)
    target_payment_term_code = (
        target_term.payment_term_code if target_term else None
    )

    created = 0
    updated = 0
    skipped = 0
    unchanged = 0
    for source_row in source_rows:
        existing = get_fob_for_country_sku(
            session,
            target,
            source_row.material_code,
        )
        if existing:
            if not overwrite_existing:
                skipped += 1
                continue
            changed = (
                existing.uploaded_fob_eur != source_row.uploaded_fob_eur
                or existing.final_fob_eur != source_row.final_fob_eur
                or existing.base_fob_eur != source_row.base_fob_eur
                or existing.payment_term_adjustment_eur != source_row.payment_term_adjustment_eur
                or existing.colour_surcharge_eur != source_row.colour_surcharge_eur
            )
            if changed:
                session.add(
                    FobResolvedHistory(
                        country_sku_fob_id=existing.country_sku_fob_id,
                        baseline_version_id=source_row.baseline_version_id,
                        country_code=target,
                        material_code=source_row.material_code,
                        payment_term_code=existing.payment_term_code,
                        old_uploaded_fob_eur=existing.uploaded_fob_eur,
                        new_uploaded_fob_eur=source_row.uploaded_fob_eur,
                        old_final_fob_eur=existing.final_fob_eur,
                        new_final_fob_eur=source_row.final_fob_eur,
                        changed_by=changed_by or "copy_country_fobs",
                    )
                )
                updated += 1
            else:
                unchanged += 1
            existing.baseline_version_id = source_row.baseline_version_id
            existing.base_fob_eur = source_row.base_fob_eur
            existing.payment_term_adjustment_eur = source_row.payment_term_adjustment_eur
            existing.colour_surcharge_eur = source_row.colour_surcharge_eur
            existing.uploaded_fob_eur = source_row.uploaded_fob_eur
            existing.final_fob_eur = source_row.final_fob_eur
            existing.fob_source_country_code = source
            existing.fob_source_mode = "copied_from_country"
            existing.is_active = True
            existing.updated_at_utc = datetime.now(timezone.utc)
            continue

        session.add(
            CountrySkuFobResolved(
                country_sku_fob_id=uuid4(),
                baseline_version_id=source_row.baseline_version_id,
                country_code=target,
                material_code=source_row.material_code,
                payment_term_code=target_payment_term_code or source_row.payment_term_code,
                base_fob_eur=source_row.base_fob_eur,
                payment_term_adjustment_eur=source_row.payment_term_adjustment_eur,
                colour_surcharge_eur=source_row.colour_surcharge_eur,
                uploaded_fob_eur=source_row.uploaded_fob_eur,
                final_fob_eur=source_row.final_fob_eur,
                fob_source_country_code=source,
                fob_source_mode="copied_from_country",
                is_active=True,
            )
        )
        created += 1

    return {
        "sourceCountryCode": source,
        "targetCountryCode": target,
        "sourceRows": len(source_rows),
        "copied": created,
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "unchanged": unchanged,
        "targetPaymentTermCode": target_payment_term_code,
    }


def adjust_country_fobs(
    session: Session,
    country_code: str,
    delta_eur: float,
    *,
    changed_by: str | None = None,
) -> dict[str, float | int | str]:
    """Apply a fixed EUR delta to every active FOB row for a country."""
    country = str(country_code or "").strip().upper()
    delta = round(float(delta_eur), 2)
    rows = list_fob_by_country(session, country)
    adjusted = 0
    skipped_negative = 0
    unchanged = 0

    for row in rows:
        old_value = float(row.final_fob_eur)
        new_value = round(old_value + delta, 2)
        if new_value < 0:
            skipped_negative += 1
            continue
        if new_value == old_value:
            unchanged += 1
            continue

        session.add(
            FobResolvedHistory(
                country_sku_fob_id=row.country_sku_fob_id,
                baseline_version_id=row.baseline_version_id,
                country_code=country,
                material_code=row.material_code,
                payment_term_code=row.payment_term_code,
                old_uploaded_fob_eur=row.uploaded_fob_eur,
                new_uploaded_fob_eur=row.uploaded_fob_eur,
                old_final_fob_eur=row.final_fob_eur,
                new_final_fob_eur=new_value,
                changed_by=changed_by or "adjust_country_fobs",
            )
        )
        row.final_fob_eur = new_value
        row.fob_source_mode = "manual_country_adjust"
        row.updated_at_utc = datetime.now(timezone.utc)
        adjusted += 1

    return {
        "countryCode": country,
        "deltaEur": delta,
        "rows": len(rows),
        "adjusted": adjusted,
        "skippedNegative": skipped_negative,
        "unchanged": unchanged,
    }


def list_bom_with_fob(
    session: Session,
    brand: str | None = None,
    search: str | None = None,
    country_code: str | None = None,
    limit: int = 1000,
) -> tuple[list[dict], list[str]]:
    """Return SKUs with their FOB per country, grouped for BOM admin display."""
    all_countries = list_bom_admin_country_columns(session)
    skus = list_all_material_skus_for_admin(session, brand=brand, search=search, country_code=country_code, limit=limit)
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
            "fobSourceCountryCode": f.fob_source_country_code,
            "fobSourceMode": f.fob_source_mode,
        }

    material_or_template_codes = {
        s.material_code
        for s in skus
        if s.material_code
    } | {
        s.bom_template
        for s in skus
        if s.bom_template
    }
    finance_rows = session.execute(
        select(CountryMaterialFinance.material_code, CountryMaterialFinance.country_code).where(
            CountryMaterialFinance.material_code.in_(material_or_template_codes),
            CountryMaterialFinance.is_active == True,
        )
    ).all()
    finance_country_map: dict[str, set[str]] = {}
    for material_code, finance_country_code in finance_rows:
        finance_country_map.setdefault(material_code, set()).add(finance_country_code)

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
            "version": s.version,
            "powertrain": _extract_canonical_powertrain(s),
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
            "financeCountries": sorted(
                finance_country_map.get(s.material_code, set())
                | finance_country_map.get(s.bom_template or "", set())
            ),
            "sourceSheetName": s.source_sheet_name,
            "sourceRowNumber": s.source_row_number,
            "sourceFileName": baseline_names.get(s.baseline_version_id) if s.baseline_version_id else None,
            "sourcePayload": s.raw_payload_json,
        }
        for s in skus
    ], all_countries


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    return float(value)


def _finance_audit_scalar(value: object) -> object:
    if value is None or isinstance(value, (str, bool, int, float, list, dict)):
        return deepcopy(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return str(value)


def _country_material_finance_audit_values(
    finance: CountryMaterialFinance | None,
) -> dict[str, object] | None:
    if finance is None:
        return None
    return {
        field_name: _finance_audit_scalar(getattr(finance, field_name))
        for field_name in COUNTRY_MATERIAL_FINANCE_AUDIT_FIELDS
    }


def _changed_finance_fields(
    old_values: dict[str, object] | None,
    new_values: dict[str, object],
) -> list[str]:
    if old_values is None:
        return [
            field_name
            for field_name, value in new_values.items()
            if value not in (None, "", {}, [])
        ]
    return [
        field_name
        for field_name, value in new_values.items()
        if old_values.get(field_name) != value
    ]


def _add_country_material_finance_history(
    session: Session,
    finance: CountryMaterialFinance,
    old_values: dict[str, object] | None,
    *,
    changed_by: str | None,
) -> None:
    new_values = _country_material_finance_audit_values(finance) or {}
    changed_fields = _changed_finance_fields(old_values, new_values)
    if not changed_fields:
        return
    session.add(
        CountryMaterialFinanceHistory(
            country_material_finance_id=finance.country_material_finance_id,
            country_code=finance.country_code,
            material_code=finance.material_code,
            old_values_json=old_values,
            new_values_json=new_values,
            changed_fields_json=changed_fields,
            source_mode=finance.source_mode,
            source_payload_json=deepcopy(finance.source_payload_json)
            if isinstance(finance.source_payload_json, dict)
            else None,
            changed_by=changed_by,
            changed_at_utc=datetime.now(timezone.utc),
        )
    )


def _country_material_finance_payload(
    sku: MaterialSkuMaster,
    country_code: str,
    fob: CountrySkuFobResolved | None,
    finance: CountryMaterialFinance | None,
) -> dict:
    bom_fob_eur = float(fob.final_fob_eur) if fob and fob.final_fob_eur is not None else None
    finance_fob_eur = (
        float(finance.fob_eur)
        if finance and finance.fob_eur is not None
        else bom_fob_eur
    )
    return {
        "financeId": str(finance.country_material_finance_id) if finance else None,
        "countryCode": country_code,
        "materialCode": sku.material_code,
        "brand": normalize_brand(sku.brand),
        "modelName": normalize_brand_text(sku.model_name),
        "version": sku.version,
        "powertrain": _extract_canonical_powertrain(sku),
        "colour": sku.exterior_color_name,
        "colourCode": sku.exterior_color_code,
        "bomTemplate": sku.bom_template,
        "bomFobEur": bom_fob_eur,
        "fobEur": finance_fob_eur,
        "retailPriceEur": _optional_float(finance.retail_price_eur) if finance else None,
        "wholesalePriceEur": _optional_float(finance.wholesale_price_eur) if finance else None,
        "dealerPriceEur": _optional_float(finance.dealer_price_eur) if finance else None,
        "costEur": _optional_float(finance.cost_eur) if finance else None,
        "marginEur": _optional_float(finance.margin_eur) if finance else None,
        "marginRate": _optional_float(finance.margin_rate) if finance else None,
        "vehicleMarginEur": _optional_float(finance.vehicle_margin_eur) if finance else None,
        "vehicleMarginRate": _optional_float(finance.vehicle_margin_rate) if finance else None,
        "vehicleProfitEur": _optional_float(finance.vehicle_profit_eur) if finance else None,
        "vehicleProfitRate": _optional_float(finance.vehicle_profit_rate) if finance else None,
        "fobDeltaEur": _optional_float(finance.fob_delta_eur) if finance else None,
        "marginDeltaEur": _optional_float(finance.margin_delta_eur) if finance else None,
        "memo": finance.memo if finance else None,
        "sourceMode": finance.source_mode if finance else None,
        "sourcePayload": finance.source_payload_json if finance else None,
        "updatedBy": finance.updated_by if finance else None,
        "updatedAtUtc": finance.updated_at_utc.isoformat() if finance and finance.updated_at_utc else None,
    }


def _country_template_finance_payload(
    skus: list[MaterialSkuMaster],
    country_code: str,
    fob_by_code: dict[str, CountrySkuFobResolved],
    finance: CountryMaterialFinance | None,
) -> dict:
    """Build a finance row at BOM-template grain, not exterior-colour SKU grain."""
    sorted_skus = sorted(skus, key=lambda sku: sku.material_code or "")
    sku = sorted_skus[0]
    template_code = clean_text(sku.bom_template or sku.material_code).upper()
    fob = next(
        (
            fob_by_code.get(item.material_code)
            for item in sorted_skus
            if item.material_code and fob_by_code.get(item.material_code) is not None
        ),
        None,
    )
    payload = _country_material_finance_payload(sku, country_code, fob, finance)
    payload["materialCode"] = template_code
    payload["bomTemplate"] = template_code
    payload["colour"] = ""
    payload["colourCode"] = ""
    payload["sourcePayload"] = {
        **(payload.get("sourcePayload") if isinstance(payload.get("sourcePayload"), dict) else {}),
        "skuCount": len(sorted_skus),
        "colourCodes": sorted(
            {
                clean_text(item.exterior_color_code).upper()
                for item in sorted_skus
                if clean_text(item.exterior_color_code)
            }
        ),
    }
    return payload


def list_country_material_finance(
    session: Session,
    country_code: str,
    *,
    material_codes: list[str] | None = None,
    brand: str | None = None,
    model_name: str | None = None,
    powertrain: str | None = None,
    version: str | None = None,
    limit: int = 1000,
) -> list[dict]:
    """Return country finance rows over active BOM SKUs with FOB as reference."""
    country = clean_text(country_code).upper()
    requested_codes = {
        clean_text(code).upper()
        for code in (material_codes or [])
        if clean_text(code)
    }
    normalized_brand = normalize_brand(brand) if brand else None
    normalized_model = normalize_brand_text(model_name) if model_name else None
    normalized_powertrain = normalize_powertrain(powertrain) if powertrain else None
    normalized_version = clean_text(version) if version else None

    skus = list_all_material_skus_for_admin(
        session,
        brand=normalized_brand,
        limit=limit,
    )
    filtered_skus: list[MaterialSkuMaster] = []
    for sku in skus:
        sku_code = clean_text(sku.material_code).upper()
        template_code = clean_text(sku.bom_template).upper()
        if requested_codes and sku_code not in requested_codes and template_code not in requested_codes:
            continue
        if normalized_model and normalize_brand_text(sku.model_name) != normalized_model:
            continue
        if normalized_powertrain and _extract_canonical_powertrain(sku) != normalized_powertrain:
            continue
        if normalized_version and clean_text(sku.version) != normalized_version:
            continue
        filtered_skus.append(sku)
    if not filtered_skus:
        return []

    codes = [sku.material_code for sku in filtered_skus]
    fobs = session.execute(
        select(CountrySkuFobResolved).where(
            CountrySkuFobResolved.country_code == country,
            CountrySkuFobResolved.material_code.in_(codes),
            CountrySkuFobResolved.is_active == True,
        )
    ).scalars().all()
    fob_by_code = {row.material_code: row for row in fobs}

    template_codes = {
        clean_text(sku.bom_template or sku.material_code).upper()
        for sku in filtered_skus
        if clean_text(sku.bom_template or sku.material_code)
    }
    finance_lookup_codes = template_codes | {
        clean_text(sku.material_code).upper()
        for sku in filtered_skus
        if clean_text(sku.material_code)
    }
    finances = session.execute(
        select(CountryMaterialFinance).where(
            CountryMaterialFinance.country_code == country,
            CountryMaterialFinance.material_code.in_(finance_lookup_codes),
            CountryMaterialFinance.is_active == True,
        )
    ).scalars().all()
    finance_by_code = {row.material_code: row for row in finances}

    grouped: dict[str, list[MaterialSkuMaster]] = {}
    for sku in filtered_skus:
        template_code = clean_text(sku.bom_template or sku.material_code).upper()
        grouped.setdefault(template_code, []).append(sku)

    return [
        _country_template_finance_payload(
            group,
            country,
            fob_by_code,
            finance_by_code.get(template_code)
            or next(
                (
                    finance_by_code.get(clean_text(item.material_code).upper())
                    for item in sorted(group, key=lambda sku: sku.material_code or "")
                    if finance_by_code.get(clean_text(item.material_code).upper()) is not None
                ),
                None,
            ),
        )
        for template_code, group in sorted(
            grouped.items(),
            key=lambda item: (
                item[1][0].brand or "",
                item[1][0].model_name or "",
                item[1][0].powertrain or "",
                item[1][0].version or "",
                item[0],
            ),
        )
    ]


def upsert_country_material_finance(
    session: Session,
    country_code: str,
    material_code: str,
    values: dict,
    *,
    updated_by: str | None = None,
) -> dict | None:
    """Create or update one country finance/CBU row without changing BOM FOB."""
    country = clean_text(country_code).upper()
    material = clean_text(material_code).upper()
    if "**" in material:
        template_skus = list(
            session.execute(
                select(MaterialSkuMaster)
                .where(MaterialSkuMaster.bom_template == material)
                .order_by(MaterialSkuMaster.is_active.desc(), MaterialSkuMaster.material_code)
            ).scalars().all()
        )
        sku = template_skus[0] if template_skus else None
    else:
        sku = get_sku_by_material_code_any_status(session, material)
        template_skus = [sku] if sku is not None else []
    if sku is None:
        return None

    finance = session.execute(
        select(CountryMaterialFinance).where(
            CountryMaterialFinance.country_code == country,
            CountryMaterialFinance.material_code == material,
            CountryMaterialFinance.is_active == True,
        )
    ).scalar_one_or_none()
    old_values = _country_material_finance_audit_values(finance)
    if finance is None:
        finance = CountryMaterialFinance(
            country_code=country,
            material_code=material,
            source_mode="manual",
            updated_by=updated_by,
            is_active=True,
        )
        session.add(finance)

    numeric_fields = {
        "fob_eur": "fob_eur",
        "retail_price_eur": "retail_price_eur",
        "wholesale_price_eur": "wholesale_price_eur",
        "dealer_price_eur": "dealer_price_eur",
        "cost_eur": "cost_eur",
        "margin_eur": "margin_eur",
        "margin_rate": "margin_rate",
        "vehicle_margin_eur": "vehicle_margin_eur",
        "vehicle_margin_rate": "vehicle_margin_rate",
        "vehicle_profit_eur": "vehicle_profit_eur",
        "vehicle_profit_rate": "vehicle_profit_rate",
        "fob_delta_eur": "fob_delta_eur",
        "margin_delta_eur": "margin_delta_eur",
    }
    for field_name in numeric_fields:
        if field_name in values:
            setattr(finance, field_name, values[field_name])
    if "memo" in values:
        finance.memo = values["memo"]
    if "source_payload_json" in values:
        finance.source_payload_json = values["source_payload_json"]
    finance.source_mode = clean_text(values.get("source_mode") or "manual")
    finance.updated_by = updated_by
    finance.updated_at_utc = datetime.now(timezone.utc)
    session.flush()
    _add_country_material_finance_history(
        session,
        finance,
        old_values,
        changed_by=updated_by,
    )

    if "**" in material:
        concrete_codes = [item.material_code for item in template_skus if item.material_code]
        fobs = session.execute(
            select(CountrySkuFobResolved).where(
                CountrySkuFobResolved.country_code == country,
                CountrySkuFobResolved.material_code.in_(concrete_codes),
                CountrySkuFobResolved.is_active == True,
            )
        ).scalars().all()
        return _country_template_finance_payload(
            template_skus,
            country,
            {row.material_code: row for row in fobs},
            finance,
        )
    fob = get_fob_for_country_sku(session, country, material)
    return _country_material_finance_payload(sku, country, fob, finance)


def list_country_material_finance_history(
    session: Session,
    country_code: str,
    material_code: str,
    *,
    limit: int = 50,
) -> list[dict]:
    """Return immutable audit events for one country material finance row."""
    country = clean_text(country_code).upper()
    material = clean_text(material_code).upper()
    if not country or not material:
        return []
    capped_limit = max(1, min(int(limit or 50), 200))
    rows = session.execute(
        select(CountryMaterialFinanceHistory)
        .where(
            CountryMaterialFinanceHistory.country_code == country,
            CountryMaterialFinanceHistory.material_code == material,
        )
        .order_by(CountryMaterialFinanceHistory.changed_at_utc.desc())
        .limit(capped_limit)
    ).scalars().all()
    return [
        {
            "historyId": str(row.finance_history_id),
            "financeId": str(row.country_material_finance_id)
            if row.country_material_finance_id else None,
            "countryCode": row.country_code,
            "materialCode": row.material_code,
            "oldValues": row.old_values_json,
            "newValues": row.new_values_json,
            "changedFields": row.changed_fields_json or [],
            "sourceMode": row.source_mode,
            "sourcePayload": row.source_payload_json,
            "changedBy": row.changed_by,
            "changedAtUtc": row.changed_at_utc.isoformat() if row.changed_at_utc else None,
        }
        for row in rows
    ]


def delete_orphan_template_finance(
    session: Session,
    bom_template: str | None,
) -> int:
    """Remove template-level finance when no material SKUs remain for that BOM template."""
    from sqlalchemy import delete as sa_delete

    template = clean_text(bom_template).upper()
    if not template or "**" not in template:
        return 0
    remaining = session.execute(
        select(MaterialSkuMaster.material_code)
        .where(MaterialSkuMaster.bom_template == template)
        .limit(1)
    ).scalar_one_or_none()
    if remaining is not None:
        return 0
    result = session.execute(
        sa_delete(CountryMaterialFinance).where(
            CountryMaterialFinance.material_code == template
        )
    )
    return int(result.rowcount or 0)


def copy_country_material_finance_template(
    session: Session,
    source_bom_template: str | None,
    target_bom_template: str | None,
    *,
    updated_by: str | None = None,
) -> int:
    """Copy template-level country finance rows when a BOM template is duplicated."""
    source_template = clean_text(source_bom_template).upper()
    target_template = clean_text(target_bom_template).upper()
    if not source_template or not target_template or source_template == target_template:
        return 0

    source_rows = list(
        session.execute(
            select(CountryMaterialFinance).where(
                CountryMaterialFinance.material_code == source_template,
                CountryMaterialFinance.is_active == True,
            )
        ).scalars().all()
    )
    copied = 0
    for source_row in source_rows:
        target_exists = session.execute(
            select(CountryMaterialFinance.country_material_finance_id).where(
                CountryMaterialFinance.country_code == source_row.country_code,
                CountryMaterialFinance.material_code == target_template,
                CountryMaterialFinance.is_active == True,
            )
        ).scalar_one_or_none()
        if target_exists is not None:
            continue

        payload = (
            deepcopy(source_row.source_payload_json)
            if isinstance(source_row.source_payload_json, dict)
            else {}
        )
        payload["copiedFromBomTemplate"] = source_template
        target_row = CountryMaterialFinance(
            country_code=source_row.country_code,
            material_code=target_template,
            source_mode="copied",
            source_payload_json=payload,
            updated_by=updated_by,
            is_active=True,
        )
        for field_name in COUNTRY_MATERIAL_FINANCE_VALUE_FIELDS:
            setattr(target_row, field_name, getattr(source_row, field_name))
        session.add(target_row)
        session.flush()
        _add_country_material_finance_history(
            session,
            target_row,
            None,
            changed_by=updated_by,
        )
        copied += 1
    if copied:
        session.flush()
    return copied


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


def build_colour_hex_rules_from_skus(skus: list[object]) -> list[dict]:
    """Group SKU swatches by brand + colour code + normalized colour name."""
    groups: dict[tuple[str, str, str], dict] = {}
    for sku in skus:
        colour_name = str(getattr(sku, "exterior_color_name", "") or "").strip()
        key = _colour_rule_group_key(
            getattr(sku, "brand", None),
            getattr(sku, "exterior_color_code", None),
            colour_name,
        )
        if key is None:
            continue
        brand, colour_code, normalized_colour_name = key
        group = groups.setdefault(
            key,
            {
                "brand": brand,
                "colourCode": colour_code,
                "colourName": colour_name,
                "normalizedColourName": normalized_colour_name,
                "skuCount": 0,
                "sampleMaterialCodes": [],
                "_hexCounts": Counter(),
            },
        )
        group["skuCount"] += 1
        material_code = str(getattr(sku, "material_code", "") or "").strip()
        if material_code and len(group["sampleMaterialCodes"]) < 5:
            group["sampleMaterialCodes"].append(material_code)
        try:
            colour_hex = normalize_colour_hex_value(
                getattr(sku, "colour_hex", None)
            )
        except ValueError:
            colour_hex = None
        if colour_hex:
            group["_hexCounts"][colour_hex] += 1

    rules: list[dict] = []
    for group in groups.values():
        hex_counts: Counter = group.pop("_hexCounts")
        hex_options = [
            {"colourHex": colour_hex, "skuCount": count}
            for colour_hex, count in sorted(
                hex_counts.items(),
                key=lambda item: (-item[1], item[0]),
            )
        ]
        if not hex_options:
            status = "missing"
            standard_colour_hex = None
        elif len(hex_options) == 1:
            status = "standard"
            standard_colour_hex = hex_options[0]["colourHex"]
        else:
            status = "conflict"
            standard_colour_hex = None
        rules.append(
            {
                **group,
                "status": status,
                "standardColourHex": standard_colour_hex,
                "hexOptions": hex_options,
            }
        )
    status_rank = {"conflict": 0, "missing": 1, "standard": 2}
    return sorted(
        rules,
        key=lambda item: (
            status_rank.get(item["status"], 9),
            item["brand"],
            item["colourCode"],
            item["normalizedColourName"],
        ),
    )


def _list_colour_rule_candidate_skus(
    session: Session,
    brand: str,
    colour_code: str,
) -> list[MaterialSkuMaster]:
    normalized_brand = normalize_brand(brand)
    normalized_code = colour_code.strip().upper()
    if not normalized_brand or not normalized_code:
        return []
    stmt = select(MaterialSkuMaster).where(
        MaterialSkuMaster.is_active == True,
        func.upper(MaterialSkuMaster.exterior_color_code) == normalized_code,
    )
    rows = list(session.execute(stmt).scalars().all())
    return [
        row
        for row in rows
        if normalize_brand(row.brand or "") == normalized_brand
    ]


def list_colour_hex_rules(session: Session) -> list[dict]:
    """Return derived swatch rules collected from existing material SKUs."""
    stmt = select(MaterialSkuMaster).where(MaterialSkuMaster.is_active == True)
    skus = list(session.execute(stmt).scalars().all())
    return build_colour_hex_rules_from_skus(skus)


def find_reusable_colour_hex(
    session: Session,
    brand: str,
    colour_code: str,
    colour_name: str,
) -> str | None:
    """Return a reusable colour hex only when the rule has no conflict."""
    key = _colour_rule_group_key(brand, colour_code, colour_name)
    if key is None:
        return None
    normalized_brand, normalized_code, normalized_colour_name = key
    candidates = _list_colour_rule_candidate_skus(
        session,
        normalized_brand,
        normalized_code,
    )
    matching = [
        row
        for row in candidates
        if _colour_rule_group_key(
            row.brand,
            row.exterior_color_code,
            row.exterior_color_name,
        )
        == (normalized_brand, normalized_code, normalized_colour_name)
    ]
    rules = build_colour_hex_rules_from_skus(matching)
    if len(rules) != 1 or rules[0]["status"] != "standard":
        return None
    return rules[0]["standardColourHex"]


def set_standard_colour_hex_for_rule(
    session: Session,
    brand: str,
    colour_code: str,
    colour_name: str,
    colour_hex: str,
) -> dict:
    """Resolve a swatch conflict by applying one standard to the rule key."""
    standard_colour_hex = normalize_colour_hex_value(colour_hex)
    if standard_colour_hex is None:
        raise ValueError("colourHex is required")
    key = _colour_rule_group_key(brand, colour_code, colour_name)
    if key is None:
        raise ValueError("brand, colourCode and colourName are required")
    normalized_brand, normalized_code, normalized_colour_name = key
    candidates = _list_colour_rule_candidate_skus(
        session,
        normalized_brand,
        normalized_code,
    )
    matching = [
        row
        for row in candidates
        if _colour_rule_group_key(
            row.brand,
            row.exterior_color_code,
            row.exterior_color_name,
        )
        == (normalized_brand, normalized_code, normalized_colour_name)
    ]
    if not matching:
        raise ValueError("No matching material SKUs for colour rule")
    updated_codes: list[str] = []
    now = datetime.now(timezone.utc)
    for sku in matching:
        sku.colour_hex = standard_colour_hex
        sku.updated_at_utc = now
        updated_codes.append(sku.material_code)
    return {
        "brand": normalized_brand,
        "colourCode": normalized_code,
        "colourName": str(colour_name or "").strip(),
        "normalizedColourName": normalized_colour_name,
        "colourHex": standard_colour_hex,
        "updated": len(updated_codes),
        "materialCodes": updated_codes,
    }


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
    from app.db.models import CountryMaterialFinance, CountrySkuFobResolved

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
    finance_stmt = (
        update(CountryMaterialFinance)
        .where(CountryMaterialFinance.material_code == old_material_code)
        .values(material_code=new_material_code)
    )
    session.execute(finance_stmt)
    return result.rowcount > 0


def _resolve_material_code_from_bom_template(
    bom_template: str,
    colour_code: str | None,
) -> str:
    template = clean_text(bom_template).upper()
    if not template:
        raise ValueError("bomTemplate is required")
    if "**" not in template:
        return template
    code = clean_text(colour_code).upper()
    if not code:
        raise ValueError("Colour code is required when bomTemplate contains **")
    return template.replace("**", code)


def _build_bom_template_material_code_map(
    skus: list[MaterialSkuMaster],
    bom_template: str,
) -> tuple[str, dict[str, str]]:
    normalized_template = clean_text(bom_template).upper()
    if not normalized_template:
        raise ValueError("bomTemplate is required")
    if len(skus) > 1 and "**" not in normalized_template:
        raise ValueError("BOM template for multiple colours must include **")

    mapping: dict[str, str] = {}
    seen_targets: set[str] = set()
    for sku in skus:
        target_code = _resolve_material_code_from_bom_template(
            normalized_template,
            sku.exterior_color_code,
        )
        if target_code in seen_targets:
            raise ValueError(f"Duplicate material code generated from template: {target_code}")
        seen_targets.add(target_code)
        mapping[sku.material_code] = target_code
    return normalized_template, mapping


def update_bom_template_material_codes(
    session: Session,
    material_codes: list[str],
    bom_template: str,
) -> dict[str, str]:
    codes = [clean_text(code).upper() for code in material_codes if clean_text(code)]
    codes = list(dict.fromkeys(codes))
    if not codes:
        raise ValueError("materialCodes is required")

    skus = list(
        session.execute(
            select(MaterialSkuMaster)
            .where(MaterialSkuMaster.material_code.in_(codes))
            .order_by(
                MaterialSkuMaster.material_code,
                MaterialSkuMaster.is_active.desc(),
                MaterialSkuMaster.row_version.desc(),
            )
        ).scalars().all()
    )
    deduped_skus: list[MaterialSkuMaster] = []
    seen_codes: set[str] = set()
    for sku in skus:
        if sku.material_code in seen_codes:
            continue
        seen_codes.add(sku.material_code)
        deduped_skus.append(sku)
    if len(deduped_skus) != len(codes):
        found = {sku.material_code for sku in deduped_skus}
        missing = [code for code in codes if code not in found]
        raise LookupError(f"Material code not found: {', '.join(missing)}")

    sku_by_code = {sku.material_code: sku for sku in deduped_skus}
    ordered_skus = [sku_by_code[code] for code in codes]
    normalized_template, mapping = _build_bom_template_material_code_map(
        ordered_skus,
        bom_template,
    )
    bind = session.get_bind()
    inspector = inspect(bind)
    has_material_lifecycle = inspector.has_table("material_lifecycle", schema="ordering")

    old_codes = set(mapping)
    new_codes = set(mapping.values())
    overlapping_targets = [
        new_code
        for old_code, new_code in mapping.items()
        if new_code in old_codes and new_code != old_code
    ]
    if overlapping_targets:
        raise ValueError(
            f"Target material code overlaps with an existing source code: {overlapping_targets[0]}"
        )
    conflicts = list(
        session.execute(
            select(MaterialSkuMaster.material_code).where(
                MaterialSkuMaster.material_code.in_(new_codes),
                MaterialSkuMaster.material_code.notin_(old_codes),
            )
        ).scalars().all()
    )
    if conflicts:
        raise ValueError(f"Material code already exists: {conflicts[0]}")

    old_template_codes = {
        clean_text(sku.bom_template).upper()
        for sku in ordered_skus
        if clean_text(sku.bom_template)
    }
    for old_code, new_code in mapping.items():
        sku = sku_by_code[old_code]
        sku.material_code = new_code
        sku.bom_template = normalized_template

        session.execute(
            update(CountrySkuFobResolved)
            .where(CountrySkuFobResolved.material_code == old_code)
            .values(material_code=new_code)
        )
        session.execute(
            update(CountryMaterialFinance)
            .where(CountryMaterialFinance.material_code == old_code)
            .values(material_code=new_code)
        )
        session.execute(
            update(OrderQuantityCell)
            .where(OrderQuantityCell.material_code == old_code)
            .values(material_code=new_code)
        )
        session.execute(
            update(PiOrderLine)
            .where(PiOrderLine.material_code == old_code)
            .values(material_code=new_code, bom=normalized_template)
        )
        session.execute(
            update(PiOrderLineAllocation)
            .where(PiOrderLineAllocation.material_code == old_code)
            .values(material_code=new_code)
        )
        session.execute(
            update(PiVehicleUnit)
            .where(PiVehicleUnit.material_code == old_code)
            .values(material_code=new_code, bom=normalized_template)
        )
        session.execute(
            update(MaterialSkuRemarkHistory)
            .where(MaterialSkuRemarkHistory.material_code == old_code)
            .values(material_code=new_code)
        )
        if has_material_lifecycle:
            session.execute(
                update(MaterialLifecycle)
                .where(MaterialLifecycle.material_code == old_code)
                .values(material_code=new_code)
            )
            session.execute(
                update(MaterialLifecycle)
                .where(MaterialLifecycle.replaced_by_code == old_code)
                .values(replaced_by_code=new_code)
            )

    _rekey_template_finance_rows(
        session,
        old_template_codes=old_template_codes,
        new_template_code=normalized_template,
    )
    return mapping


def _rekey_template_finance_rows(
    session: Session,
    *,
    old_template_codes: set[str],
    new_template_code: str,
) -> None:
    """Move BOM-template finance rows when a template code changes."""
    target_template = clean_text(new_template_code).upper()
    for old_template in sorted(old_template_codes):
        if not old_template or old_template == target_template:
            continue
        old_rows = list(
            session.execute(
                select(CountryMaterialFinance).where(
                    CountryMaterialFinance.material_code == old_template,
                    CountryMaterialFinance.is_active == True,
                )
            ).scalars().all()
        )
        for old_row in old_rows:
            target = session.execute(
                select(CountryMaterialFinance).where(
                    CountryMaterialFinance.country_code == old_row.country_code,
                    CountryMaterialFinance.material_code == target_template,
                    CountryMaterialFinance.is_active == True,
                )
            ).scalar_one_or_none()
            if target is None:
                old_row.material_code = target_template
                continue
            for field_name in (*COUNTRY_MATERIAL_FINANCE_VALUE_FIELDS, "source_payload_json"):
                if getattr(target, field_name) is None and getattr(old_row, field_name) is not None:
                    setattr(target, field_name, getattr(old_row, field_name))
            if not target.source_mode and old_row.source_mode:
                target.source_mode = old_row.source_mode
            if not target.updated_by and old_row.updated_by:
                target.updated_by = old_row.updated_by
            old_row.is_active = False


def update_sku_metadata(
    session: Session,
    material_codes: list[str],
    *,
    brand: str | None = None,
    model_name: str | None = None,
    version: str | None = None,
    powertrain: str | None = None,
) -> int:
    codes = [code for code in dict.fromkeys(material_codes) if code]
    if not codes:
        return 0

    vals: dict[str, str | None] = {}
    if brand is not None:
        vals["brand"] = normalize_brand(brand)
    if model_name is not None:
        vals["model_name"] = normalize_brand_text(model_name)
    if version is not None:
        vals["version"] = version
    if powertrain is not None:
        vals["powertrain"] = powertrain
    if not vals:
        return 0

    stmt = (
        update(MaterialSkuMaster)
        .where(MaterialSkuMaster.material_code.in_(codes))
        .values(**vals)
    )
    result = session.execute(stmt)
    return result.rowcount or 0


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
    """Return account/order countries from JATO, payment terms, and FOB rows."""
    options: dict[str, dict] = {
        code: {
            "countryCode": code,
            "countryName": country_name,
            "paymentTermCode": None,
            "paymentMethod": None,
            "lcDays": None,
        }
        for code, country_name in COUNTRY_NAMES_BY_CODE.items()
    }
    for row in list_country_payment_terms(session):
        code = str(row.country_code or "").strip().upper()
        if code not in SUPPORTED_ORDERING_COUNTRY_CODES:
            continue
        options[code] = {
            "countryCode": code,
            "countryName": row.country_name or COUNTRY_NAMES_BY_CODE.get(code, code),
            "paymentTermCode": row.payment_term_code,
            "paymentMethod": row.payment_method,
            "lcDays": row.lc_days,
        }

    for raw_code in list_active_fob_country_codes(session):
        code = str(raw_code or "").strip().upper()
        if code not in SUPPORTED_ORDERING_COUNTRY_CODES or code in options:
            continue
        options[code] = {
            "countryCode": code,
            "countryName": COUNTRY_NAMES_BY_CODE.get(code, code),
            "paymentTermCode": None,
            "paymentMethod": None,
            "lcDays": None,
        }

    return [options[code] for code in sorted(options)]


def list_active_fob_country_codes(session: Session) -> list[str]:
    """Return country columns that have active FOB data in BOM Admin."""
    country_codes = session.execute(
        select(CountrySkuFobResolved.country_code)
        .where(CountrySkuFobResolved.is_active == True)
        .distinct()
    ).scalars().all()
    return sorted({str(code or "").upper() for code in country_codes if str(code or "").strip()})


def list_bom_admin_country_columns(session: Session) -> list[str]:
    """Return BOM Admin country columns.

    NL is a mandatory logistics hub column even before NL FOB rows exist.
    `/fob-countries` still uses list_active_fob_country_codes so no-FOB alerts
    keep their original meaning.
    """
    countries = set(list_active_fob_country_codes(session))
    countries.add("NL")
    return ["NL", *sorted(country for country in countries if country != "NL")]


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
