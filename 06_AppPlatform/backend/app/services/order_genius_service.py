"""Business logic for Order Genius — FOB resolution, matrix building, export."""

from __future__ import annotations

import io
import uuid as uuid_module
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from sqlalchemy.orm import Session

from app.infra import order_genius_repository as repo
from app.services.material_master_parser import parse_material_master_xlsx, _infer_interior_from_tail_code


def _infer_interior_from_bom(bom_template: str | None) -> str | None:
    """Try tail-code inference, returning interior name or None."""
    if not bom_template:
        return None
    result = _infer_interior_from_tail_code(bom_template)
    return result[0] if result else None
from app.services.order_genius_export_service import (
    generate_order_genius_excel,
    generate_order_genius_pi_excel,
)
from app.services.order_quantity_parser import (
    OrderQuantityImport,
    parse_order_quantity_xlsx,
)
from app.services.ordering_normalization import (
    infer_colour_tier,
    merge_colour_tiers,
    normalize_brand,
    normalize_brand_text,
)
from app.services.powertrain_normalizer import normalize_powertrain


def _extract_canonical_pt(sku: object) -> str:
    """Extract canonical powertrain from SKU data — model name is authoritative over DB field."""
    raw_pt = str(getattr(sku, "powertrain", "") or "").upper()
    model = str(getattr(sku, "model_name", "") or "").upper()
    combined = f"{model} {raw_pt}"
    # Order: PHEV/SHS before HEV, BEV before EV
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
    return normalize_powertrain(raw_pt) if raw_pt else "Other"


def _derive_colour_tier(exterior_color_type: str) -> str:
    """Auto-classify colour_tier from exterior_color_type.
    single → single, dual → dual, matte/black edition/etc → special"""
    return infer_colour_tier(None, exterior_color_type)


def _effective_colour_tier(sku: object) -> str:
    explicit = str(getattr(sku, "colour_tier", "") or "").strip().lower()
    if explicit in {"single", "dual", "special"}:
        return explicit
    return _derive_colour_tier(getattr(sku, "exterior_color_type", None) or "single")


def _interior_by_template(skus: list[MaterialSkuMaster]) -> dict[str, str]:
    result: dict[str, str] = {}
    for sku in skus:
        template = sku.bom_template or ""
        interior = sku.interior_color_name or ""
        if template and interior and template not in result:
            result[template] = interior
    return result


def _effective_interior_name(
    sku: MaterialSkuMaster,
    interior_by_bom_template: dict[str, str],
) -> str | None:
    return (
        sku.interior_color_name
        or interior_by_bom_template.get(sku.bom_template or "")
        or _infer_interior_from_bom(sku.bom_template)
    )


# ── Upload orchestration ───────────────────────────────────────────────


def _assign_fob_based_tiers(
    session: Session,
    skus: list[MaterialSkuMaster],
) -> int:
    """After FOB resolution, group SKUs by (bom_template, country) and assign colour_tier
    based on FOB levels within each group.

    Within each group, unique FOB values are sorted:
      1 FOB level  → all 'single'
      2 FOB levels → lower FOB = 'single', higher = 'dual'
      3+ FOB levels → lowest = 'single', middle(s) = 'dual', highest = 'special'
    """
    from collections import defaultdict

    from app.db.models import CountrySkuFobResolved

    updated = 0
    if not skus:
        return updated

    # Collect all FOB data for these SKUs (keyed by material_code, not material_sku_id)
    material_codes = [s.material_code for s in skus]
    code_to_sku: dict[str, MaterialSkuMaster] = {s.material_code: s for s in skus}
    all_fobs = session.query(CountrySkuFobResolved).filter(
        CountrySkuFobResolved.material_code.in_(material_codes),
        CountrySkuFobResolved.is_active == True,
        CountrySkuFobResolved.final_fob_eur > 0,
    ).all()

    # Step 1: For each (bom_template, country), find the base FOB
    bom_base_fob: dict[tuple[str, str], float] = {}
    bom_fobs: dict[tuple[str, str], dict[str, float]] = defaultdict(dict)
    for fob in all_fobs:
        sku = code_to_sku.get(fob.material_code)
        if not sku or fob.final_fob_eur is None or float(fob.final_fob_eur) <= 0:
            continue
        bt = sku.bom_template or sku.material_code
        key = (bt, fob.country_code)
        bom_fobs[key][sku.material_code] = float(fob.final_fob_eur)
    for key, mc_fobs in bom_fobs.items():
        bom_base_fob[key] = min(mc_fobs.values())

    # Step 2: Cross-BOM base FOB per (model+version, country)
    mv_base_fob: dict[tuple[str, str], float] = {}
    mv_fobs: dict[tuple[str, str], list[float]] = defaultdict(list)
    for fob in all_fobs:
        sku = code_to_sku.get(fob.material_code)
        if not sku or fob.final_fob_eur is None or float(fob.final_fob_eur) <= 0:
            continue
        mv = f"{sku.model_name or ''}|{sku.version or ''}"
        key = (mv, fob.country_code)
        mv_fobs[key].append(float(fob.final_fob_eur))
    for key, fobs in mv_fobs.items():
        mv_base_fob[key] = min(fobs)

    # Step 3: Compute paint surcharge per SKU
    surcharge_map: dict[tuple[str, str], dict[str, float]] = defaultdict(dict)
    for fob in all_fobs:
        sku = code_to_sku.get(fob.material_code)
        if not sku or fob.final_fob_eur is None or float(fob.final_fob_eur) <= 0:
            continue
        bt = sku.bom_template or sku.material_code
        mv = f"{sku.model_name or ''}|{sku.version or ''}"
        key = (mv, fob.country_code)
        own_base = bom_base_fob.get((bt, fob.country_code))
        bom_mcs = list(bom_fobs.get((bt, fob.country_code), {}).keys())
        if own_base is not None and len(bom_mcs) >= 2:
            base = own_base
        else:
            base = mv_base_fob.get(key)
        if base is None or base == 0:
            continue
        surcharge = float(fob.final_fob_eur) - base
        surcharge_map[key][sku.material_code] = surcharge

    # Step 4: Assign tiers based on surcharge levels
    for (mv, country), mc_surcharges in surcharge_map.items():
        unique_sc = sorted(set(mc_surcharges.values()))

        # Map surcharge → tier
        sc_to_tier: dict[float, str] = {}
        if len(unique_sc) == 1:
            # Single FOB level: all single
            sc_to_tier[unique_sc[0]] = "single"
        elif len(unique_sc) == 2:
            sc_to_tier[unique_sc[0]] = "single"
            sc_to_tier[unique_sc[1]] = "dual"
        else:
            sc_to_tier[unique_sc[0]] = "single"
            for sc in unique_sc[1:-1]:
                sc_to_tier[sc] = "dual"
            sc_to_tier[unique_sc[-1]] = "special"

        for mc, sc in mc_surcharges.items():
            fob_tier = sc_to_tier.get(sc)
            if fob_tier is None:
                continue
            sku = code_to_sku.get(mc)
            tier = merge_colour_tiers(
                sku.colour_tier if sku else None,
                infer_colour_tier(
                    sku.exterior_color_name if sku else None,
                    sku.exterior_color_type if sku else None,
                    sku.edition_tag if sku else None,
                    sku.exterior_color_code if sku else None,
                    sku.colour_hex if sku else None,
                ),
                fob_tier,
            )
            if sku and sku.colour_tier != tier:
                sku.colour_tier = tier
                updated += 1

    if updated:
        session.flush()
    return updated


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

    # 2. Insert new SKUs (skip if already active with same data)
    from app.db.models import MaterialSkuMaster

    existing_codes = set(
        repo.get_sku_by_material_code_any_status(session, r.get("material_code", "")).material_code
        for r in rows if repo.get_sku_by_material_code_any_status(session, r.get("material_code", ""))
    )
    new_material_codes: list[str] = []
    new_skus: list[MaterialSkuMaster] = []
    for row in rows:
        mc = row.get("material_code", "")
        # Skip if already active — idempotent publish
        existing = repo.get_sku_by_material_code(session, mc)
        if existing:
            new_material_codes.append(mc)
            continue
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
            colour_code_confirmed=row.get("colour_code_confirmed", True),
            colour_tier=row.get("colour_tier") or _derive_colour_tier(row.get("exterior_color_type", "single")),
            edition_tag=row.get("edition_tag"),
            interior_color_name=row.get("interior_color_name") or _infer_interior_from_bom(row.get("bom_template")),
            interior_colour_code=row.get("interior_colour_code"),
            interior_package=row.get("interior_package") or row.get("interior_color_name"),
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

    # 5. Assign colour_tier based on FOB levels (FOB-based tier detection)
    tier_updates = _assign_fob_based_tiers(session, new_skus)

    return {
        "baseline_version_id": str(baseline.baseline_version_id),
        "baseline_name": baseline_name,
        "sku_count": len(new_skus),
        "fob_count": fob_count,
        "fob_source_mode": fob_source_mode,
        "fob_resolved_countries": sorted(fob_resolved),
        "fob_skipped_countries": sorted(fob_skipped),
        "tier_updates": tier_updates,
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
        "remark": fob.remark,
    }


def get_fob_for_sku(
    session: Session, country_code: str, material_code: str
) -> dict | None:
    fob = repo.get_fob_for_country_sku(session, country_code, material_code)
    if not fob:
        return None
    return _fob_to_dict(fob)


# ── Matrix Building ───────────────────────────────────────────────────


def _list_matrix_candidate_skus(
    session: Session,
    brand: str | None = None,
    model_name: str | None = None,
    powertrain: str | None = None,
    version: str | None = None,
    colour: str | None = None,
    material_code_search: str | None = None,
) -> list[MaterialSkuMaster]:
    normalized_brand = normalize_brand(brand) if brand else None
    normalized_model = normalize_brand_text(model_name) if model_name else None
    canonical_pt = normalize_powertrain(powertrain) if powertrain else None

    # Brand/model/powertrain are normalized in Python because legacy BOM rows can
    # still contain JEACOO and stale powertrain values.
    active_skus = repo.list_active_skus(
        session,
        version=version,
        exterior_color_code=colour,
        material_code_search=material_code_search,
    )
    if normalized_brand:
        active_skus = [
            sku for sku in active_skus
            if normalize_brand(sku.brand) == normalized_brand
        ]
    if normalized_model:
        active_skus = [
            sku for sku in active_skus
            if normalize_brand_text(sku.model_name) == normalized_model
        ]
    if canonical_pt:
        active_skus = [sku for sku in active_skus if _extract_canonical_pt(sku) == canonical_pt]
    return active_skus


def _historical_sku_matches_matrix_filters(
    sku: MaterialSkuMaster,
    material_code: str,
    brand: str | None = None,
    model_name: str | None = None,
    powertrain: str | None = None,
    version: str | None = None,
    colour: str | None = None,
    material_code_search: str | None = None,
) -> bool:
    if brand and normalize_brand(sku.brand) != normalize_brand(brand):
        return False
    if model_name and normalize_brand_text(sku.model_name) != normalize_brand_text(model_name):
        return False
    if version and sku.version != version:
        return False
    if colour and sku.exterior_color_code != colour:
        return False
    if powertrain and _extract_canonical_pt(sku) != normalize_powertrain(powertrain):
        return False
    if material_code_search and material_code_search.lower() not in material_code.lower():
        return False
    return True


def _build_matrix_for_country(
    session: Session,
    country_code: str,
    year: int,
    active_skus: list[MaterialSkuMaster],
    brand: str | None = None,
    model_name: str | None = None,
    powertrain: str | None = None,
    version: str | None = None,
    colour: str | None = None,
    material_code_search: str | None = None,
) -> dict:
    # Get country payment term valid for this order year
    order_month_hint = f"{year}-01"  # use January of the order year
    country_pt = repo.get_country_payment_term(session, country_code, order_month_hint)
    payment_term_code = country_pt.payment_term_code if country_pt else None
    country_name = country_pt.country_name if country_pt else None

    # Get FOB for these SKUs in one round trip. Payment term is metadata-only
    # in the current repository rule, matching get_fob_for_country_sku.
    fob_map: dict[str, CountrySkuFobResolved] = repo.list_fobs_for_country_material_codes(
        session,
        country_code,
        [sku.material_code for sku in active_skus],
        payment_term_code,
    )

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
    historical_skus = repo.get_skus_by_material_codes_any_status(
        session,
        historical_codes,
    )
    matrix_interior_by_template = _interior_by_template(
        active_skus + list(historical_skus.values())
    )
    historical_fob_map = repo.list_fobs_for_country_material_codes(
        session,
        country_code,
        historical_codes,
        payment_term_code,
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
            "bomTemplate": sku.bom_template,
            "brand": normalize_brand(sku.brand),
            "modelName": normalize_brand_text(sku.model_name),
            "version": sku.version,
            "colour": sku.exterior_color_name,
            "colourCode": sku.exterior_color_code,
            "colourType": sku.exterior_color_type,
            "colourTier": _effective_colour_tier(sku),
            "colourHex": sku.colour_hex,
            "interiorColorName": _effective_interior_name(sku, matrix_interior_by_template),
            "interiorColourCode": sku.interior_colour_code,
            "interiorPackage": sku.interior_package,
            "editionTag": sku.edition_tag,
            "powertrain": _extract_canonical_pt(sku),
            "fobEur": float(fob.final_fob_eur) if fob else None,
            "lifecycleStatus": "active",
            "editable": True,
            "displayStyle": None,
            "remark": sku.remark,
            "effectiveFrom": sku.effective_from_month,
            "effectiveTo": sku.effective_to_month,
            "months": row_months,
            "ttl": row_ttl,
        })

    # Add historical rows with quantity data (respect brand/model/version/colour filters)
    for mc in historical_codes:
        if mc in fob_map:
            continue  # already included as active
        hist_sku = historical_skus.get(mc)
        if not hist_sku:
            continue
        # Apply filters to historical rows too
        if not _historical_sku_matches_matrix_filters(
            hist_sku,
            mc,
            brand=brand,
            model_name=model_name,
            powertrain=powertrain,
            version=version,
            colour=colour,
            material_code_search=material_code_search,
        ):
            continue
        fob = historical_fob_map.get(mc)

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
                "bomTemplate": hist_sku.bom_template,
                "brand": normalize_brand(hist_sku.brand),
                "modelName": normalize_brand_text(hist_sku.model_name),
                "version": hist_sku.version,
                "colour": hist_sku.exterior_color_name,
                "colourCode": hist_sku.exterior_color_code,
                "colourType": hist_sku.exterior_color_type,
                "colourTier": _effective_colour_tier(hist_sku),
                "colourHex": hist_sku.colour_hex,
                "interiorColorName": _effective_interior_name(hist_sku, matrix_interior_by_template),
                "interiorColourCode": hist_sku.interior_colour_code,
                "interiorPackage": hist_sku.interior_package,
                "editionTag": hist_sku.edition_tag,
                "powertrain": _extract_canonical_pt(hist_sku),
                "fobEur": float(fob.final_fob_eur) if fob else None,
                "lifecycleStatus": "historical",
                "editable": False,
                "displayStyle": "strikethrough",
                "remark": hist_sku.remark,
                "effectiveFrom": hist_sku.effective_from_month,
                "effectiveTo": hist_sku.effective_to_month,
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
    active_skus = _list_matrix_candidate_skus(
        session,
        brand=brand,
        model_name=model_name,
        powertrain=powertrain,
        version=version,
        colour=colour,
        material_code_search=material_code_search,
    )
    return _build_matrix_for_country(
        session,
        country_code=country_code,
        year=year,
        active_skus=active_skus,
        brand=brand,
        model_name=model_name,
        powertrain=powertrain,
        version=version,
        colour=colour,
        material_code_search=material_code_search,
    )


def build_matrix_batch(
    session: Session,
    country_codes: list[str],
    year: int,
    brand: str | None = None,
    model_name: str | None = None,
    powertrain: str | None = None,
    version: str | None = None,
    colour: str | None = None,
    material_code_search: str | None = None,
) -> dict[str, dict]:
    """Build matrices for many countries while reusing the filtered SKU set."""
    active_skus = _list_matrix_candidate_skus(
        session,
        brand=brand,
        model_name=model_name,
        powertrain=powertrain,
        version=version,
        colour=colour,
        material_code_search=material_code_search,
    )
    return {
        country_code: _build_matrix_for_country(
            session,
            country_code=country_code,
            year=year,
            active_skus=active_skus,
            brand=brand,
            model_name=model_name,
            powertrain=powertrain,
            version=version,
            colour=colour,
            material_code_search=material_code_search,
        )
        for country_code in country_codes
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

    normalized_brand = normalize_brand(brand) if brand else None
    normalized_model = normalize_brand_text(model_name) if model_name else None

    # Get all active SKUs for this country (filtered by FOB availability).
    # Brand/model/powertrain are filtered after normalization so old JEACOO rows stay visible.
    all_active = repo.list_active_skus(session, version=version)
    skus = [s for s in all_active if s.material_code in fob_codes]
    if normalized_brand:
        skus = [s for s in skus if normalize_brand(s.brand) == normalized_brand]
    if normalized_model:
        skus = [s for s in skus if normalize_brand_text(s.model_name) == normalized_model]

    # Filter by canonical powertrain (model-name-aware)
    if powertrain:
        skus = [s for s in skus if _extract_canonical_pt(s) == normalize_powertrain(powertrain)]

    # Collect distinct values from the filtered skus
    brands = sorted(set(normalize_brand(s.brand) for s in skus if normalize_brand(s.brand)))
    if normalized_brand:
        models = sorted(set(
            normalize_brand_text(s.model_name)
            for s in skus
            if normalize_brand(s.brand) == normalized_brand and normalize_brand_text(s.model_name)
        ))
    else:
        models = sorted(set(
            normalize_brand_text(s.model_name)
            for s in skus
            if normalize_brand_text(s.model_name)
        ))

    if normalized_model:
        pts = sorted(set(
            _extract_canonical_pt(s) for s in skus
            if normalize_brand_text(s.model_name) == normalized_model
        ))
    elif normalized_brand:
        pts = sorted(set(
            _extract_canonical_pt(s) for s in skus
            if normalize_brand(s.brand) == normalized_brand
        ))
    else:
        pts = sorted(set(
            _extract_canonical_pt(s) for s in skus
        ))

    if version:
        vers = sorted(set(s.version for s in skus))
    elif brand or model_name:
        # Cascade: versions filtered by upstream selections
        filtered = skus
        if normalized_brand:
            filtered = [s for s in filtered if normalize_brand(s.brand) == normalized_brand]
        if normalized_model:
            filtered = [s for s in filtered if normalize_brand_text(s.model_name) == normalized_model]
        vers = sorted(set(s.version for s in filtered))
    else:
        vers = sorted(set(s.version for s in skus))

    # Cascade colours: filtered by all upstream selections
    colour_source = skus
    if normalized_brand:
        colour_source = [s for s in colour_source if normalize_brand(s.brand) == normalized_brand]
    if normalized_model:
        colour_source = [s for s in colour_source if normalize_brand_text(s.model_name) == normalized_model]
    if powertrain:
        canonical_pt = normalize_powertrain(powertrain)
        colour_source = [s for s in colour_source if _extract_canonical_pt(s) == canonical_pt]
    if version:
        colour_source = [s for s in colour_source if s.version == version]
    colours = sorted(set(s.exterior_color_name for s in colour_source))
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
    brand: str | None = None,
    model_name: str | None = None,
    powertrain: str | None = None,
    version: str | None = None,
    colour: str | None = None,
    material_code_search: str | None = None,
    selected_month: int | None = None,
    hide_empty_rows: bool = False,
    quantities_only: bool = False,
) -> io.BytesIO:
    """Generate the Order Genius Excel workbook."""
    matrix = build_matrix(session, country_code, year,
                          brand=brand, model_name=model_name,
                          powertrain=powertrain, version=version, colour=colour,
                          material_code_search=material_code_search)
    rows = matrix["rows"]
    export_months = [selected_month] if selected_month else list(range(1, 13))
    if quantities_only or hide_empty_rows:
        rows = [r for r in rows if any(
            (r.get("months", {}).get(str(m), {}).get("quantity", 0) or 0) > 0
            for m in export_months
        )]
    country_name = matrix.get("countryName", country_code)
    return generate_order_genius_excel(
        rows, country_code, country_name, year,
        include_historical_with_quantity,
        selected_months=export_months,
    )


def export_pi_matrix(
    session: Session,
    country_code: str,
    year: int,
    brand: str | None = None,
    model_name: str | None = None,
    powertrain: str | None = None,
    version: str | None = None,
    colour: str | None = None,
    material_code_search: str | None = None,
    selected_month: int | None = None,
    hide_empty_rows: bool = False,
    freight_eur: float | None = None,
    insurance_eur: float | None = None,
    domestic_freight_eur: float | None = None,
    domestic_insurance_eur: float | None = None,
) -> io.BytesIO:
    """Generate a PI workbook using the current Order Genius selection."""
    matrix = build_matrix(session, country_code, year,
                          brand=brand, model_name=model_name,
                          powertrain=powertrain, version=version, colour=colour,
                          material_code_search=material_code_search)
    rows = matrix["rows"]
    export_months = [selected_month] if selected_month else list(range(1, 13))
    if hide_empty_rows:
        rows = [r for r in rows if any(
            (r.get("months", {}).get(str(m), {}).get("quantity", 0) or 0) > 0
            for m in export_months
        )]

    nl_fobs = repo.list_fob_by_country(session, "NL")
    nl_fob_by_material_code = {
        fob.material_code: float(fob.final_fob_eur) if fob.final_fob_eur is not None else None
        for fob in nl_fobs
    }
    country_name = matrix.get("countryName", country_code)
    return generate_order_genius_pi_excel(
        rows=rows,
        country_code=country_code,
        country_name=country_name,
        year=year,
        quantity_month=selected_month,
        nl_fob_by_material_code=nl_fob_by_material_code,
        freight_eur=freight_eur,
        insurance_eur=insurance_eur,
        domestic_freight_eur=domestic_freight_eur,
        domestic_insurance_eur=domestic_insurance_eur,
    )


# ── Order Quantity Import (round-trip: export → edit → re-import) ─────


def preview_order_quantity_import(
    session: Session,
    parsed: OrderQuantityImport,
) -> dict:
    """Match parsed rows against the database and return a preview diff.

    Returns a dict with:
      - countryCode, year
      - matchedRows: rows with existing quantity cells found
      - newRows: material codes not found in the system
      - fobChanges: rows where Excel FOB differs from current system FOB
      - totalCells: total quantity cells in the import
      - errorCells: cells with parse errors
      - errors: file-level errors
    """
    result: dict = {
        "countryCode": parsed.country_code,
        "year": parsed.year,
        "matchedRows": [],
        "newRows": [],
        "fobChanges": [],
        "totalCells": 0,
        "errorCells": 0,
        "errors": parsed.errors.copy(),
    }

    if parsed.errors:
        return result

    # Get all active SKUs + FOB for this country
    country_pt = repo.get_country_payment_term(session, parsed.country_code)
    pt_code = country_pt.payment_term_code if country_pt else None

    for row in parsed.rows:
        # Check if material code exists
        existing_sku = repo.get_sku_by_material_code(session, row.material_code)
        if not existing_sku:
            result["newRows"].append({
                "materialCode": row.material_code,
                "modelName": row.model_name,
                "version": row.version,
                "colour": row.colour,
                "reason": "Material code not found in system",
            })
            continue

        # Check FOB divergence
        current_fob = None
        if pt_code:
            fob_record = repo.get_fob_for_country_sku(
                session, parsed.country_code, row.material_code, pt_code,
            )
            if fob_record:
                current_fob = float(fob_record.final_fob_eur)

        fob_changed = False
        if row.fob_eur is not None and current_fob is not None:
            if abs(row.fob_eur - current_fob) > 0.01:
                fob_changed = True
                result["fobChanges"].append({
                    "materialCode": row.material_code,
                    "excelFob": row.fob_eur,
                    "systemFob": current_fob,
                })

        matched_cells = []
        for cell in row.cells:
            result["totalCells"] += 1
            if cell.error:
                result["errorCells"] += 1

            # Find existing quantity cell
            existing_qty = repo.get_quantity_cell(
                session, parsed.country_code, parsed.year, cell.month, row.material_code,
            )
            old_qty = existing_qty.quantity if existing_qty else None
            old_row_version = existing_qty.row_version if existing_qty else 0

            if cell.quantity != (old_qty or 0) or cell.error:
                matched_cells.append({
                    "month": cell.month,
                    "oldQuantity": old_qty,
                    "newQuantity": cell.quantity,
                    "error": cell.error,
                    "rowVersion": old_row_version,
                })

        if matched_cells:
            result["matchedRows"].append({
                "materialCode": row.material_code,
                "modelName": row.model_name or (existing_sku.model_name if existing_sku else ""),
                "version": row.version or (existing_sku.version if existing_sku else ""),
                "colour": row.colour or (existing_sku.exterior_color_name if existing_sku else ""),
                "excelFob": row.fob_eur,
                "systemFob": current_fob,
                "fobChanged": fob_changed,
                "lifecycleStatus": existing_sku.lifecycle_status if existing_sku else "unknown",
                "cells": matched_cells,
                "rowErrors": row.errors,
            })

    return result


def apply_order_quantity_import(
    session: Session,
    parsed: OrderQuantityImport,
    username: str,
) -> dict:
    """Apply confirmed quantity changes from an imported Excel.

    Returns {"appliedCells": int, "skippedCells": int, "errors": [...]}
    """
    applied = 0
    skipped = 0
    errors: list[str] = []

    country_pt = repo.get_country_payment_term(session, parsed.country_code)
    pt_code = country_pt.payment_term_code if country_pt else None

    for row in parsed.rows:
        existing_sku = repo.get_sku_by_material_code(session, row.material_code)
        if not existing_sku:
            skipped += len(row.cells)
            errors.append(f"{row.material_code}: material code not found in active SKU master")
            continue

        # Resolve current FOB for quantity cells
        fob_eur = 0.0
        if pt_code:
            fob_record = repo.get_fob_for_country_sku(
                session, parsed.country_code, row.material_code, pt_code,
            )
            if fob_record:
                fob_eur = float(fob_record.final_fob_eur)

        for cell in row.cells:
            if cell.error:
                skipped += 1
                continue

            existing = repo.get_quantity_cell(
                session, parsed.country_code, parsed.year, cell.month, row.material_code,
            )
            expected_version = existing.row_version if existing else 0

            result = repo.upsert_quantity_cell(
                session,
                country_code=parsed.country_code,
                order_year=parsed.year,
                order_month=cell.month,
                material_code=row.material_code,
                quantity=cell.quantity,
                fob_eur=fob_eur,
                updated_by=username,
                expected_version=expected_version,
            )
            if result:
                applied += 1
            else:
                errors.append(
                    f"{row.material_code} M{cell.month}: optimistic lock failure "
                    f"(expected version {expected_version})"
                )
                skipped += 1

    return {"appliedCells": applied, "skippedCells": skipped, "errors": errors}
