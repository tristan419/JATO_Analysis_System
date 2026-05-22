"""Business logic for Order Genius — FOB resolution, matrix building, export."""

from __future__ import annotations

import io
import uuid as uuid_module
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from sqlalchemy.orm import Session

from app.infra import order_genius_repository as repo
from app.services.material_master_parser import parse_material_master_xlsx
from app.services.order_genius_export_service import generate_order_genius_excel


# ── Upload orchestration ───────────────────────────────────────────────


def preview_parsed_upload(
    session: Session,
    parsed: dict,
) -> dict:
    """Compare parsed rows against existing DB to classify new/existing SKUs."""
    rows = parsed.get("rows", [])
    existing_codes: set[str] = set()
    new_count = 0
    existing_count = 0

    for row in rows:
        mc = row.get("material_code")
        if mc and repo.get_sku_by_material_code_any_status(session, mc):
            existing_codes.add(mc)
            existing_count += 1
        else:
            new_count += 1

    return {
        "total_rows": len(rows),
        "new_skus": new_count,
        "existing_skus": existing_count,
        "sheet_names": parsed.get("sheet_names", []),
        "rows": rows,
        "warnings": parsed.get("warnings", []),
    }


def publish_baseline(
    session: Session,
    parsed: dict,
    source_file_name: str,
    source_file_hash: str | None,
    published_by: str,
    source_upload_id: UUID | None = None,
) -> dict:
    """Publish a new baseline: create version, insert SKUs, transition old ones.

    Steps:
    1. Create MaterialBaselineVersion
    2. Insert new active SKUs from parsed data
    3. Transition old SKUs (same material_code but from previous baseline) to historical
    4. Resolve FOB for all available countries
    """
    rows = parsed.get("rows", [])
    if not rows:
        raise ValueError("No rows to publish")

    # Determine baseline name
    existing_baselines = repo.list_baseline_versions(session, limit=1)
    version_num = len(existing_baselines) + 1 if existing_baselines else 1
    baseline_name = f"V{version_num}"

    # 1. Create baseline
    baseline = repo.create_baseline_version(
        session=session,
        source_file_name=source_file_name,
        source_file_hash=source_file_hash,
        baseline_name=baseline_name,
        published_by=published_by,
        source_upload_id=source_upload_id,
    )
    session.flush()  # get baseline_version_id

    # 2. Insert new SKUs
    from app.db.models import MaterialSkuMaster

    new_material_codes: list[str] = []
    new_skus: list[MaterialSkuMaster] = []
    for row in rows:
        mc = row.get("material_code", "")
        sku = MaterialSkuMaster(
            material_sku_id=uuid_module.uuid4(),
            baseline_version_id=baseline.baseline_version_id,
            brand=row.get("brand", ""),
            model_code=row.get("model_code"),
            model_name=row.get("model_name", ""),
            powertrain=row.get("powertrain"),
            version=row.get("version", ""),
            exterior_color_name=row.get("exterior_color_name", ""),
            exterior_color_code=row.get("exterior_color_code", ""),
            exterior_color_type=row.get("exterior_color_type", "single"),
            interior_color_name=row.get("interior_color_name"),
            bom_template=row.get("bom_template"),
            material_code=mc,
            lifecycle_status="active",
            is_active=True,
            is_published=True,
            source_sheet_name=row.get("sheet_name"),
            source_row_number=row.get("row_index"),
            raw_payload_json=row,
        )
        new_skus.append(sku)
        new_material_codes.append(mc)

    repo.bulk_create_skus(session, new_skus)

    # 3. Transition old SKUs to historical
    transitioned = repo.transition_old_skus_to_historical(
        session, new_material_codes, baseline.baseline_version_id
    )

    session.flush()

    # 4. Resolve FOB — only for countries that have FOB data in the Excel.
    # Default: explicit_price_by_payment_term.
    # Excel FOB = final price for that country's payment term.
    # LC is a price dimension, NOT a global adjustment formula.
    fob_source_mode = "explicit_price_by_payment_term"
    countries = repo.list_country_payment_terms(session)
    fob_count = 0
    fob_skipped: set[str] = set()
    fob_resolved: set[str] = set()
    for country_pt in countries:
        any_for_country = False
        for sku in new_skus:
            try:
                _resolve_fob_for_sku(
                    session,
                    country_pt.country_code,
                    sku,
                    baseline.baseline_version_id,
                    fob_source_mode,
                )
                fob_count += 1
                any_for_country = True
            except (ValueError, KeyError):
                pass  # SKU has no FOB column for this country
        if any_for_country:
            fob_resolved.add(country_pt.country_code)
        else:
            fob_skipped.add(country_pt.country_code)

    return {
        "baseline_version_id": str(baseline.baseline_version_id),
        "baseline_name": baseline_name,
        "sku_count": len(new_skus),
        "fob_count": fob_count,
        "fob_source_mode": fob_source_mode,
        "fob_resolved_countries": sorted(fob_resolved),
        "fob_skipped_countries": sorted(fob_skipped),
        "status": "published",
    }


# ── FOB Resolution ────────────────────────────────────────────────────


def _resolve_fob_for_sku(
    session: Session,
    country_code: str,
    sku: MaterialSkuMaster,
    baseline_version_id: UUID,
    fob_source_mode: str = "explicit_price_by_payment_term",
) -> CountrySkuFobResolved:
    """Resolve FOB for a country+SKU pair.

    Pricing model (corrected):
    - LC is NOT a global adjustment formula. It is a price dimension/key.
    - FOB = resolved by: country_code + material_code + payment_term_code
    - payment_term_price_rule is NOT used for calculation.
    - Colour surcharge only applies in uploaded_base_plus_colour mode.
    """
    from app.db.models import CountrySkuFobResolved
    from app.services.material_master_parser import _normalise_country_name

    country_pt = repo.get_country_payment_term(session, country_code)
    payment_term_code = country_pt.payment_term_code if country_pt else "TT"
    country_name = country_pt.country_name if country_pt else country_code

    # Look up country-specific FOB from parsed Excel data
    country_fobs: dict[str, float] = {}
    if sku.raw_payload_json:
        country_fobs = sku.raw_payload_json.get("country_fobs", {})

    # Match FOB by country code or normalised name
    uploaded_fob: float | None = None
    matched_col: str | None = None
    if country_code in country_fobs:
        uploaded_fob = country_fobs[country_code]
        matched_col = country_code
    else:
        for col_name, fob_val in country_fobs.items():
            if _normalise_country_name(col_name) in (
                country_name, country_code,
            ):
                uploaded_fob = fob_val
                matched_col = col_name
                break

    if uploaded_fob is None:
        # Check explicit fallback mapping (e.g. RO → HR)
        fallback_src = repo.get_country_fob_source_mapping(
            session, country_code, payment_term_code,
        )
        if fallback_src:
            # Try direct match first, then normalised name match
            uploaded_fob = country_fobs.get(fallback_src)
            if uploaded_fob is None:
                for col_name, fob_val in country_fobs.items():
                    if _normalise_country_name(col_name) in (
                        fallback_src,
                        _normalise_country_name(fallback_src),
                    ):
                        uploaded_fob = fob_val
                        break
            if uploaded_fob is not None:
                fob_source_country = fallback_src
            else:
                raise ValueError(
                    f"No FOB column for {country_code} or fallback "
                    f"{fallback_src} in SKU {sku.material_code}"
                )
        else:
            raise ValueError(
                f"No FOB column for {country_code} in SKU "
                f"{sku.material_code}"
            )

    uploaded_fob_eur = float(uploaded_fob)

    if fob_source_mode == "uploaded_base_plus_colour":
        # Excel FOB is base/single-colour — apply dual-colour surcharge only.
        # LC is NOT added here — it is already part of the uploaded FOB value.
        surcharge = repo.get_brand_colour_surcharge(
            session, sku.brand, sku.exterior_color_type,
        )
        colour_surcharge = float(surcharge.surcharge_eur) if surcharge else 0.0
        final_fob = uploaded_fob_eur + colour_surcharge
        fob_source_country = country_code
    else:
        # uploaded_final_fob or explicit_price_by_payment_term:
        # The uploaded FOB IS the final FOB for this country+payment_term.
        colour_surcharge = 0.0
        final_fob = uploaded_fob_eur
        fob_source_country = country_code

    fob = CountrySkuFobResolved(
        country_sku_fob_id=uuid_module.uuid4(),
        baseline_version_id=baseline_version_id,
        country_code=country_code,
        material_code=sku.material_code,
        payment_term_code=payment_term_code,
        uploaded_fob_eur=uploaded_fob_eur,
        final_fob_eur=final_fob,
        fob_source_country_code=fob_source_country,
        fob_source_mode=fob_source_mode,
        is_active=True,
    )
    return repo.upsert_fob_resolved(session, fob)


def _get_base_fob_from_sku(sku: MaterialSkuMaster) -> float:
    """Extract base FOB from raw_payload_json or use 0."""
    if sku.raw_payload_json:
        for key in ("base_fob_eur", "fob_eur", "final_fob_eur"):
            val = sku.raw_payload_json.get(key)
            if val is not None:
                try:
                    return float(val)
                except (TypeError, ValueError):
                    continue
    return 0.0


def resolve_fob_for_country(
    session: Session,
    country_code: str,
    material_code: str,
) -> dict | None:
    """Resolve (or re-resolve) FOB for a single country+SKU combination."""
    sku = repo.get_active_sku_by_code(session, material_code)
    if not sku:
        return None

    baseline = repo.get_latest_baseline(session)
    if not baseline:
        return None

    fob = _resolve_fob_for_sku(
        session, country_code, sku, baseline.baseline_version_id
    )
    return _fob_to_dict(fob)


def _fob_to_dict(fob: CountrySkuFobResolved) -> dict:
    return {
        "country_sku_fob_id": str(fob.country_sku_fob_id),
        "country_code": fob.country_code,
        "material_code": fob.material_code,
        "payment_term_code": fob.payment_term_code,
        "uploaded_fob_eur": (
            float(fob.uploaded_fob_eur) if fob.uploaded_fob_eur else None
        ),
        "final_fob_eur": float(fob.final_fob_eur),
        "fob_source_mode": fob.fob_source_mode,
        "fob_source_country_code": fob.fob_source_country_code,
    }


def get_fob_for_sku(
    session: Session, country_code: str, material_code: str
) -> dict | None:
    fob = repo.get_fob_for_country_sku(session, country_code, material_code)
    if not fob:
        return None
    return _fob_to_dict(fob)


# ── Matrix Building ───────────────────────────────────────────────────


def build_matrix(
    session: Session,
    country_code: str,
    year: int,
    brand: str | None = None,
    model_name: str | None = None,
    powertrain: str | None = None,
    version: str | None = None,
    colour: str | None = None,
    material_code_search: str | None = None,
) -> dict:
    """Build the Order Genius matrix for a country+year.

    Returns rows with Model, Version, Colour, Material Code, FOB, Jan-Dec, TTL.
    """
    # Get country payment term
    country_pt = repo.get_country_payment_term(session, country_code)
    payment_term_code = country_pt.payment_term_code if country_pt else None
    country_name = country_pt.country_name if country_pt else None

    # Get active SKUs matching filters
    active_skus = repo.list_active_skus(
        session,
        brand=brand,
        model_name=model_name,
        powertrain=powertrain,
        version=version,
        exterior_color_code=colour,
        material_code_search=material_code_search,
    )

    # Get FOB for these SKUs — filtered by country's default payment term
    fob_map: dict[str, CountrySkuFobResolved] = {}
    for sku in active_skus:
        fob = repo.get_fob_for_country_sku(
            session, country_code, sku.material_code, payment_term_code,
        )
        if fob:
            fob_map[sku.material_code] = fob

    # Only include SKUs that have FOB for this country
    skus_with_fob = [
        s for s in active_skus if s.material_code in fob_map
    ]

    # Get quantities for this country+year
    all_quantities = repo.list_quantities_for_country_year(
        session, country_code, year
    )
    qty_map: dict[tuple[str, int], OrderQuantityCell] = {}
    for q in all_quantities:
        qty_map[(q.material_code, q.order_month)] = q

    # Get historical SKUs that have quantity data
    historical_codes = repo.list_historical_skus_with_quantity(
        session, country_code, year
    )

    # Build rows
    rows = []
    for sku in skus_with_fob:
        fob = fob_map[sku.material_code]
        row_months: dict[str, dict] = {}
        row_ttl = 0
        for month in range(1, 13):
            cell = qty_map.get((sku.material_code, month))
            qty = cell.quantity if cell else 0
            row_ttl += qty
            row_months[str(month)] = {
                "quantity": qty,
                "is_editable": True,
                "rowVersion": cell.row_version if cell else 1,
            }

        rows.append({
            "materialCode": sku.material_code,
            "brand": sku.brand,
            "modelName": sku.model_name,
            "version": sku.version,
            "colour": sku.exterior_color_name,
            "powertrain": sku.powertrain,
            "fobEur": float(fob.final_fob_eur) if fob else None,
            "lifecycleStatus": "active",
            "editable": True,
            "displayStyle": None,
            "remark": sku.remark,
            "months": row_months,
            "ttl": row_ttl,
        })

    # Add historical rows with quantity data
    for mc in historical_codes:
        if mc in fob_map:
            continue  # already included as active
        hist_sku = repo.get_sku_by_material_code_any_status(session, mc)
        if not hist_sku:
            continue
        fob = repo.get_fob_for_country_sku(session, country_code, mc)

        row_months: dict[str, dict] = {}
        row_ttl = 0
        has_any = False
        for month in range(1, 13):
            cell = qty_map.get((mc, month))
            qty = cell.quantity if cell else 0
            if qty > 0:
                has_any = True
            row_ttl += qty
            row_months[str(month)] = {
                "quantity": qty,
                "is_editable": False,
                "rowVersion": cell.row_version if cell else 1,
            }

        if has_any or row_ttl > 0:
            rows.append({
                "materialCode": mc,
                "brand": hist_sku.brand,
                "modelName": hist_sku.model_name,
                "version": hist_sku.version,
                "colour": hist_sku.exterior_color_name,
                "powertrain": hist_sku.powertrain,
                "fobEur": float(fob.final_fob_eur) if fob else None,
                "lifecycleStatus": "historical",
                "editable": False,
                "displayStyle": "strikethrough",
                "remark": hist_sku.remark,
                "months": row_months,
                "ttl": row_ttl,
            })

    return {
        "countryCode": country_code,
        "countryName": country_name,
        "paymentTermCode": payment_term_code,
        "year": year,
        "rows": rows,
        "totalRows": len(rows),
    }


def build_options(
    session: Session,
    country_code: str,
    brand: str | None = None,
    model_name: str | None = None,
    powertrain: str | None = None,
    version: str | None = None,
    colour: str | None = None,
) -> dict:
    """Build cascading filter options for the Order Genius page.

    Only includes SKUs that have resolved FOB for the given country.
    """
    country_pt = repo.get_country_payment_term(session, country_code)
    pt_code = country_pt.payment_term_code if country_pt else None
    fob_codes = repo.list_active_fob_material_codes(
        session, country_code, pt_code,
    )

    # Get all active SKUs for this country (filtered by FOB availability)
    all_active = repo.list_active_skus(session, brand=brand, model_name=model_name,
                                       powertrain=powertrain, version=version)
    skus = [s for s in all_active if s.material_code in fob_codes]

    # Collect distinct values from the filtered skus
    brands = sorted(set(s.brand for s in skus))
    if brand:
        models = sorted(set(
            s.model_name for s in skus if s.brand == brand
        ))
    else:
        models = sorted(set(s.model_name for s in skus))

    if model_name:
        pts = sorted(set(
            s.powertrain for s in skus
            if s.model_name == model_name and s.powertrain
        ))
    else:
        pts = sorted(set(
            s.powertrain for s in skus if s.powertrain
        ))

    if version:
        vers = sorted(set(s.version for s in skus))
    else:
        vers = sorted(set(s.version for s in skus))

    colours = sorted(set(s.exterior_color_name for s in skus))
    material_codes = sorted(set(s.material_code for s in skus))

    return {
        "countryCode": country_code,
        "paymentTermCode": (
            country_pt.payment_term_code if country_pt else None
        ),
        "brands": brands,
        "models": models,
        "powertrains": pts,
        "versions": vers,
        "colours": colours,
        "materialCodes": material_codes,
    }


# ── Quantity & Remark Updates ─────────────────────────────────────────


def update_quantity_cell(
    session: Session,
    country_code: str,
    order_year: int,
    order_month: int,
    material_code: str,
    quantity: int,
    updated_by: str,
    expected_version: int,
) -> dict:
    """Save a monthly quantity cell. Rejects historical SKUs."""
    sku = repo.get_active_sku_by_code(session, material_code)
    if not sku:
        raise ValueError("Historical material code cannot be edited.")

    # Get current FOB for the cell
    fob = repo.get_fob_for_country_sku(session, country_code, material_code)
    fob_eur = float(fob.final_fob_eur) if fob else 0.0

    cell = repo.upsert_quantity_cell(
        session=session,
        country_code=country_code,
        order_year=order_year,
        order_month=order_month,
        material_code=material_code,
        quantity=quantity,
        fob_eur=fob_eur,
        updated_by=updated_by,
        expected_version=expected_version,
    )

    if cell is None:
        raise ValueError("Concurrent update conflict — refresh and try again.")

    return {
        "order_quantity_cell_id": str(cell.order_quantity_cell_id),
        "country_code": cell.country_code,
        "order_year": cell.order_year,
        "order_month": cell.order_month,
        "material_code": cell.material_code,
        "quantity": cell.quantity,
        "fob_eur": float(cell.fob_eur),
        "row_version": cell.row_version,
    }


def update_remark(
    session: Session,
    material_code: str,
    remark: str,
    changed_by: str,
    expected_version: int,
) -> dict:
    """Update SKU remark and create history entry."""
    sku = repo.get_active_sku_by_code(session, material_code)
    if not sku:
        raise ValueError("Material code not found or not active.")

    old_remark = sku.remark
    success = repo.update_sku_remark(
        session, material_code, remark, expected_version
    )
    if not success:
        raise ValueError("Concurrent update conflict — refresh and try again.")

    repo.add_remark_history(
        session, material_code, old_remark, remark, changed_by
    )

    return {
        "material_code": material_code,
        "remark": remark,
        "row_version": expected_version + 1,
    }


# ── Export ─────────────────────────────────────────────────────────────


def export_matrix(
    session: Session,
    country_code: str,
    year: int,
    include_historical_with_quantity: bool = True,
) -> io.BytesIO:
    """Generate the Order Genius Excel workbook."""
    matrix = build_matrix(session, country_code, year)
    country_name = matrix.get("countryName", country_code)
    return generate_order_genius_excel(
        matrix["rows"], country_code, country_name, year,
        include_historical_with_quantity,
    )
