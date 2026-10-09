"""Repository layer for Order Genius — stateless data access functions.

All functions take ``session: Session`` as the first argument.
No commits or rollbacks — transaction control lives in the service/route layer.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import unicodedata
from collections import Counter
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from uuid import UUID, uuid4

from sqlalchemy import and_, delete, func, inspect, or_, select, text, update
from sqlalchemy.orm import Session

from app.db.models import (
    BrandColourSurchargeRule,
    BrandColourSwatchRule,
    CountryMaterialFinance,
    CountryFobSourceMapping,
    CountryPaymentTermMaster,
    CountrySkuFobResolved,
    CountryTemplateFobPeriod,
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
    SpecialColourSurchargeRule,
)
from app.services.ordering_normalization import (
    clean_text,
    normalize_brand,
    normalize_brand_text,
    resolve_material_brand,
)
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


class CountryFobConflict(ValueError):
    """Active payment-term rows disagree on one material-country price."""


def _country_fob_groups(rows: list[CountrySkuFobResolved]) -> dict[tuple[str, str], list[CountrySkuFobResolved]]:
    groups: dict[tuple[str, str], list[CountrySkuFobResolved]] = {}
    for row in rows:
        groups.setdefault((row.material_code, row.country_code), []).append(row)
    return groups


def _consistent_country_fob(rows: list[CountrySkuFobResolved]) -> CountrySkuFobResolved | None:
    if not rows:
        return None
    # The adopted BOM Admin price is the final FOB.  Missing or stale base /
    # surcharge metadata is repairable drift, not a second business price.
    values = {round(float(row.final_fob_eur), 2) for row in rows}
    if len(values) > 1:
        raise CountryFobConflict(
            f"Conflicting FOB records for {rows[0].material_code} / {rows[0].country_code}; "
            "confirm the template country base before repricing. Payment terms do not select a price."
        )
    return min(rows, key=lambda row: str(row.payment_term_code or ""))


def _country_fob_conflict_payload(
    rows: list[CountrySkuFobResolved],
) -> dict[str, object]:
    """Describe one unresolved material-country group without choosing a row."""
    first = rows[0]
    return {
        "materialCode": first.material_code,
        "countryCode": first.country_code,
        "status": "conflict",
        "reason": "multiple_active_fob_values_require_bom_admin_confirmation",
        "records": [
            {
                "paymentTermCode": row.payment_term_code,
                "baseFobEur": float(row.base_fob_eur) if row.base_fob_eur is not None else None,
                "colourSurchargeEur": (
                    float(row.colour_surcharge_eur)
                    if row.colour_surcharge_eur is not None else None
                ),
                "finalFobEur": float(row.final_fob_eur),
            }
            for row in sorted(rows, key=lambda item: str(item.payment_term_code or ""))
        ],
    }


COLOUR_TIERS = frozenset({"single", "dual", "special"})
COLOUR_SURCHARGE_MANUAL_SOURCE_MODES = frozenset(
    {"manual_edit", "manual_country_adjust"}
)
COLOUR_SURCHARGE_EXPLICIT_FINAL_SOURCE_MODES = frozenset(
    {"uploaded_final_fob", "explicit_price_by_payment_term"}
)
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


def _extract_canonical_powertrain(sku: MaterialSkuMaster) -> str:
    """Normalize only the saved field; a name must never supply a powertrain."""
    raw_pt = clean_text(sku.powertrain).upper()
    if raw_pt in {"ICE", "BEV", "HEV", "PHEV", "MHEV", "REEV", "FCV", "OTHER"}:
        return raw_pt
    return normalize_powertrain(raw_pt) if raw_pt else ""


def resolve_effective_colour_tier(sku: object) -> str | None:
    """Return only an explicitly saved canonical pricing tier."""
    normalized = clean_text(getattr(sku, "colour_tier", None)).lower()
    return normalized if normalized in COLOUR_TIERS else None


def normalize_colour_rule_name(colour_name: str | None) -> str:
    """Normalize colour names for reusable swatch rules."""
    return re.sub(r"\s+", " ", str(colour_name or "").strip()).casefold()


def normalize_colour_rule_alias(colour_name: str | None) -> str:
    """Normalize a display name for safe, non-mutating alias lookup.

    This only collapses presentation differences (Unicode width, punctuation,
    whitespace and a trailing colour-code annotation). It never invents a
    colour value or treats a fuzzy match as an automatic write.
    """
    text = unicodedata.normalize("NFKC", str(colour_name or "")).strip()
    text = re.sub(r"\s*[()]\s*[A-Za-z0-9]{1,4}\s*[)]\s*$", "", text)
    text = re.sub(r"[^0-9A-Za-z\u0080-\uffff]+", " ", text)
    return re.sub(r"\s+", " ", text).strip().casefold()


def normalize_colour_hex_value(colour_hex: str | None) -> str | None:
    """Normalize stored custom swatches; supports single and dual swatches."""
    text = str(colour_hex or "").strip().upper()
    if not text:
        return None
    if re.fullmatch(r"#[0-9A-F]{6}(\|#[0-9A-F]{6})?", text):
        return text
    raise ValueError("colourHex must be #RRGGBB or #RRGGBB|#RRGGBB")


def _colour_rule_key(
    brand: str | None,
    colour_code: str | None,
) -> tuple[str, str] | None:
    normalized_brand = normalize_brand(str(brand or "")).strip()
    normalized_code = str(colour_code or "").strip().upper()
    if not normalized_brand or not normalized_code:
        return None
    return normalized_brand, normalized_code


def is_placeholder_colour_name(
    colour_name: str | None,
    colour_code: str | None,
) -> bool:
    """Return whether a colour name is blank or merely repeats its code."""
    normalized_name = normalize_colour_rule_name(colour_name)
    normalized_code = normalize_colour_rule_name(colour_code)
    return not normalized_name or normalized_name == normalized_code


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


def get_current_baseline_sku_by_code(
    session: Session,
    material_code: str,
) -> MaterialSkuMaster | None:
    """Return one SKU exactly as saved in the latest published material baseline."""
    baseline = get_latest_baseline(session)
    if baseline is None:
        return None
    stmt = select(MaterialSkuMaster).where(
        MaterialSkuMaster.baseline_version_id == baseline.baseline_version_id,
        MaterialSkuMaster.material_code == material_code,
    )
    return session.execute(stmt).scalars().first()


def get_skus_by_material_codes_any_status(
    session: Session, material_codes: list[str],
) -> dict[str, MaterialSkuMaster]:
    """Return the best SKU row for each material code, including historical rows."""
    codes = sorted({str(code or "").strip() for code in material_codes if str(code or "").strip()})
    if not codes:
        return {}
    stmt = (
        select(MaterialSkuMaster)
        .where(MaterialSkuMaster.material_code.in_(codes))
        .order_by(
            MaterialSkuMaster.material_code,
            MaterialSkuMaster.is_active.desc(),
            MaterialSkuMaster.row_version.desc(),
        )
    )
    result: dict[str, MaterialSkuMaster] = {}
    for sku in session.execute(stmt).scalars().all():
        result.setdefault(sku.material_code, sku)
    return result


def list_active_skus(
    session: Session,
    brand: str | None = None,
    model_name: str | None = None,
    powertrain: str | None = None,
    version: str | None = None,
    exterior_color_code: str | None = None,
    material_code_search: str | None = None,
    target_date: date | None = None,
    limit: int = 2000,
    allowed_brands: set[str] | None = None,
) -> list[MaterialSkuMaster]:
    today = date.today()
    stmt = select(MaterialSkuMaster).where(
        or_(
            and_(
                MaterialSkuMaster.effective_from_date.is_(None),
                MaterialSkuMaster.effective_to_date.is_(None),
                MaterialSkuMaster.is_active == True,
                MaterialSkuMaster.lifecycle_status.in_(("active", "phase_out")),
            ),
            and_(
                or_(
                    MaterialSkuMaster.effective_from_date.is_not(None),
                    MaterialSkuMaster.effective_to_date.is_not(None),
                ),
                or_(
                    MaterialSkuMaster.effective_to_date.is_(None),
                    MaterialSkuMaster.effective_to_date >= today,
                ),
            ),
        )
    )
    if allowed_brands is not None:
        stmt = stmt.where(func.upper(MaterialSkuMaster.brand).in_(allowed_brands))
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
    rows = list(session.execute(stmt).scalars().all())
    current = target_date or today
    return [
        row for row in rows
        if resolve_effective_lifecycle_status(row, current) in {"active", "phase_out"}
    ][:limit]


def list_skus_including_historical(
    session: Session,
    version: str | None = None,
    exterior_color_code: str | None = None,
    material_code_search: str | None = None,
    limit: int = 4000,
    allowed_brands: set[str] | None = None,
) -> list[MaterialSkuMaster]:
    """Return the current material master rows without hiding Historical SKUs."""
    baseline = get_latest_baseline(session)
    if baseline is None:
        return []
    stmt = select(MaterialSkuMaster).where(
        MaterialSkuMaster.baseline_version_id == baseline.baseline_version_id,
    )
    if allowed_brands is not None:
        stmt = stmt.where(func.upper(MaterialSkuMaster.brand).in_(allowed_brands))
    if version:
        stmt = stmt.where(MaterialSkuMaster.version == version)
    if exterior_color_code:
        stmt = stmt.where(MaterialSkuMaster.exterior_color_code == exterior_color_code)
    if material_code_search:
        stmt = stmt.where(MaterialSkuMaster.material_code.ilike(f"%{material_code_search}%"))
    stmt = stmt.order_by(MaterialSkuMaster.brand, MaterialSkuMaster.model_name).limit(limit)
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
    today = date.today()
    stmt = (
        select(MaterialSkuMaster).where(
            MaterialSkuMaster.material_code == material_code,
            or_(
                and_(
                    MaterialSkuMaster.effective_from_date.is_(None),
                    MaterialSkuMaster.effective_to_date.is_(None),
                    MaterialSkuMaster.is_active == True,
                    MaterialSkuMaster.lifecycle_status.in_(("active", "phase_out")),
                ),
                and_(
                    or_(
                        MaterialSkuMaster.effective_from_date.is_not(None),
                        MaterialSkuMaster.effective_to_date.is_not(None),
                    ),
                    or_(
                        MaterialSkuMaster.effective_to_date.is_(None),
                        MaterialSkuMaster.effective_to_date >= today,
                    ),
                ),
            ),
        )
        .order_by(MaterialSkuMaster.row_version.desc())
    )
    for sku in session.execute(stmt).scalars().all():
        if resolve_effective_lifecycle_status(sku, today) in {"active", "phase_out"}:
            return sku
    return None


def resolve_effective_lifecycle_status(
    sku: MaterialSkuMaster,
    target_date: date,
) -> str:
    """Resolve lifecycle from exact dates, falling back to the legacy label."""
    effective_from, effective_to = get_lifecycle_dates(sku)
    if effective_from is not None and target_date < effective_from:
        return "not_yet_active"
    if effective_to is not None:
        if target_date > effective_to:
            return "historical"
        return "phase_out"
    status = clean_text(getattr(sku, "lifecycle_status", "active")).lower()
    return status if status in {"active", "phase_out", "historical"} else "active"


def get_lifecycle_dates(sku: object) -> tuple[date | None, date | None]:
    return (
        getattr(sku, "effective_from_date", None),
        getattr(sku, "effective_to_date", None),
    )


def list_bom_template_skus(
    session: Session,
    bom_template: str,
    baseline_version_id: UUID,
) -> list[MaterialSkuMaster]:
    template = clean_text(bom_template).upper()
    rows = list(
        session.execute(
            select(MaterialSkuMaster)
            .where(
                MaterialSkuMaster.baseline_version_id == baseline_version_id,
                func.upper(MaterialSkuMaster.bom_template) == template,
            )
            .order_by(MaterialSkuMaster.material_code)
        ).scalars().all()
    )
    return [
        row for row in rows
        if clean_text(getattr(row, "bom_template", "")).upper() == template
    ]


def get_bom_template_lifecycle(
    session: Session,
    bom_template: str,
    baseline_version_id: UUID,
) -> dict:
    rows = list_bom_template_skus(session, bom_template, baseline_version_id)
    if not rows:
        raise LookupError("BOM template not found")
    boundaries = {get_lifecycle_dates(row) for row in rows}
    inconsistent = len(boundaries) > 1
    effective_from, effective_to = next(iter(boundaries)) if not inconsistent else (None, None)
    return {
        "bomTemplate": clean_text(bom_template).upper(),
        "effectiveFrom": effective_from,
        "effectiveTo": effective_to,
        "inconsistent": inconsistent,
        "materials": [row.material_code for row in rows],
        "rows": rows,
    }


def preview_bom_template_lifecycle_update(
    session: Session,
    material_code: str,
    effective_from: date | None,
    effective_to: date | None,
) -> dict:
    if effective_from is not None and effective_to is not None and effective_to < effective_from:
        raise ValueError("Final order date must be on or after first order date / 截止日不得早于开始日")
    anchor = get_sku_by_material_code_any_status(session, material_code)
    if anchor is None:
        raise LookupError("Material code not found")
    template = clean_text(anchor.bom_template or anchor.material_code).upper()
    rows = list_bom_template_skus(session, template, anchor.baseline_version_id)
    if not rows:
        rows = [anchor]
    periods = list(
        session.execute(
            select(CountryTemplateFobPeriod)
            .where(CountryTemplateFobPeriod.bom_template == template, CountryTemplateFobPeriod.status == "active")
            .order_by(
                CountryTemplateFobPeriod.country_code,
                CountryTemplateFobPeriod.valid_from,
            )
        ).scalars().all()
    )
    impacts: list[dict] = []
    for period in periods:
        before_start = effective_from is not None and period.valid_from < effective_from
        after_end = effective_to is not None and (
            period.valid_to is None or period.valid_to > effective_to
        )
        if before_start or after_end:
            impacts.append({
                "periodId": str(period.country_template_fob_period_id),
                "countryCode": period.country_code,
                "validFrom": period.valid_from.isoformat(),
                "validTo": period.valid_to.isoformat() if period.valid_to else None,
                "beforeTemplateStart": before_start,
                "afterTemplateEnd": after_end,
                "suggestedActions": [
                    "Edit price period",
                    "Extend template lifecycle",
                    "Cancel",
                ],
            })
    return {
        "bomTemplate": template,
        "materialCodes": [row.material_code for row in rows],
        "effectiveFrom": effective_from.isoformat() if effective_from else None,
        "effectiveTo": effective_to.isoformat() if effective_to else None,
        "affectedPeriods": impacts,
        "canApply": not impacts,
    }


def review_bom_template_lifecycles(
    session: Session,
    *,
    today: date | None = None,
    warning_days: int = 60,
    allowed_brands: set[str] | None = None,
    allowed_countries: set[str] | None = None,
) -> dict:
    """Summarise lifecycle drift using the existing template and FOB-period facts."""
    review_date = today or date.today()
    baseline = get_latest_baseline(session)
    if baseline is None:
        return {"asOf": review_date.isoformat(), "warningDays": warning_days, "items": []}

    sku_rows = list(
        session.execute(
            select(MaterialSkuMaster)
            .where(MaterialSkuMaster.baseline_version_id == baseline.baseline_version_id)
            .order_by(MaterialSkuMaster.bom_template, MaterialSkuMaster.material_code)
        ).scalars().all()
    )
    if allowed_brands is not None:
        sku_rows = [row for row in sku_rows if str(row.brand or "").upper() in allowed_brands]
    rows_by_template: dict[str, list[MaterialSkuMaster]] = {}
    for row in sku_rows:
        template = clean_text(row.bom_template or row.material_code).upper()
        if template:
            rows_by_template.setdefault(template, []).append(row)

    period_rows = list(
        session.execute(
            select(CountryTemplateFobPeriod).where(CountryTemplateFobPeriod.status == "active").order_by(
                CountryTemplateFobPeriod.bom_template,
                CountryTemplateFobPeriod.country_code,
                CountryTemplateFobPeriod.valid_from,
            )
        ).scalars().all()
    )
    if allowed_countries is not None:
        period_rows = [row for row in period_rows if row.country_code in allowed_countries]
    periods_by_template: dict[str, list[CountryTemplateFobPeriod]] = {}
    for period in period_rows:
        periods_by_template.setdefault(period.bom_template, []).append(period)

    items: list[dict] = []
    warning_deadline = review_date + timedelta(days=max(0, warning_days))
    for template, rows in rows_by_template.items():
        anchor = rows[0]
        boundaries = {get_lifecycle_dates(row) for row in rows}
        if len(boundaries) > 1:
            items.append({
                "kind": "inconsistent_template",
                "severity": "error",
                "bomTemplate": template,
                "materialCode": anchor.material_code,
                "countryCode": None,
                "message": "Colour SKUs in this template have different lifecycle dates.",
                "messageZh": "同一模板内的颜色 SKU 生命周期日期不一致。",
                "suggestedActions": ["Open template", "Align template lifecycle"],
            })
            effective_from = effective_to = None
        else:
            effective_from, effective_to = next(iter(boundaries))
            if effective_to is not None and effective_to < review_date:
                pending_archive = any(
                    bool(row.is_active) or clean_text(row.lifecycle_status).lower() != "historical"
                    for row in rows
                )
                if pending_archive:
                    items.append({
                        "kind": "expired_pending_archive",
                        "severity": "warning",
                        "bomTemplate": template,
                        "materialCode": anchor.material_code,
                        "countryCode": None,
                        "effectiveFrom": effective_from.isoformat() if effective_from else None,
                        "effectiveTo": effective_to.isoformat(),
                        "message": f"Final order date {effective_to.isoformat()} has passed; review archive state.",
                        "messageZh": f"最终下单日 {effective_to.isoformat()} 已过，请确认归档状态。",
                        "suggestedActions": ["Open template", "Archive as Historical", "Extend final order date"],
                    })
            elif effective_to is not None and effective_to <= warning_deadline:
                items.append({
                    "kind": "expiring_soon",
                    "severity": "info",
                    "bomTemplate": template,
                    "materialCode": anchor.material_code,
                    "countryCode": None,
                    "effectiveFrom": effective_from.isoformat() if effective_from else None,
                    "effectiveTo": effective_to.isoformat(),
                    "message": f"Final order date is {effective_to.isoformat()}.",
                    "messageZh": f"最终下单日为 {effective_to.isoformat()}。",
                    "suggestedActions": ["Open template", "Extend final order date"],
                })

        periods_by_country: dict[str, list[CountryTemplateFobPeriod]] = {}
        for period in periods_by_template.get(template, []):
            periods_by_country.setdefault(period.country_code, []).append(period)
            before_start = effective_from is not None and period.valid_from < effective_from
            after_end = effective_to is not None and (
                period.valid_to is None or period.valid_to > effective_to
            )
            if before_start or after_end:
                items.append({
                    "kind": "period_outside_lifecycle",
                    "severity": "error",
                    "bomTemplate": template,
                    "materialCode": anchor.material_code,
                    "countryCode": period.country_code,
                    "periodId": str(period.country_template_fob_period_id),
                    "validFrom": period.valid_from.isoformat(),
                    "validTo": period.valid_to.isoformat() if period.valid_to else None,
                    "message": "Country FOB period exceeds the template lifecycle.",
                    "messageZh": "国家 FOB 区间超出模板生命周期。",
                    "suggestedActions": ["Open template", "Edit price period", "Extend template lifecycle"],
                })
        for country_code, country_periods in periods_by_country.items():
            ordered = sorted(country_periods, key=lambda row: row.valid_from)
            for previous, current in zip(ordered, ordered[1:]):
                overlaps = previous.valid_to is None or previous.valid_to >= current.valid_from
                if overlaps:
                    items.append({
                        "kind": "overlapping_periods",
                        "severity": "error",
                        "bomTemplate": template,
                        "materialCode": anchor.material_code,
                        "countryCode": country_code,
                        "periodId": str(current.country_template_fob_period_id),
                        "validFrom": current.valid_from.isoformat(),
                        "validTo": current.valid_to.isoformat() if current.valid_to else None,
                        "message": "Country FOB periods overlap.",
                        "messageZh": "国家 FOB 日期区间互相重叠。",
                        "suggestedActions": ["Open template", "Edit price periods"],
                    })

    return {
        "asOf": review_date.isoformat(),
        "warningDays": warning_days,
        "items": items,
    }


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


def update_bom_template_lifecycle(
    session: Session,
    material_code: str,
    lifecycle_status: str,
    effective_from: date | None = None,
    effective_to: date | None = None,
    expected_version: int = 1,
) -> dict | None:
    """Update exact lifecycle dates for every colour in one BOM template."""
    anchor = get_sku_by_material_code_any_status(session, material_code)
    if anchor is None or anchor.row_version != expected_version:
        return None
    if effective_from is not None and effective_to is not None and effective_to < effective_from:
        raise ValueError("effectiveTo must be on or after effectiveFrom")
    status = clean_text(lifecycle_status).lower()
    if status not in {"active", "phase_out", "historical"}:
        raise ValueError("lifecycleStatus must be active, phase_out or historical")
    preview = preview_bom_template_lifecycle_update(
        session,
        material_code,
        effective_from,
        effective_to,
    )
    if preview["affectedPeriods"]:
        raise ValueError("Country FOB periods exceed the proposed template lifecycle")
    template = preview["bomTemplate"]
    rows = list_bom_template_skus(session, template, anchor.baseline_version_id) or [anchor]
    now = datetime.now(timezone.utc)
    current_date = date.today()
    for sku in rows:
        sku.effective_from_date = effective_from
        sku.effective_to_date = effective_to
        sku.lifecycle_status = status
        effective_status = resolve_effective_lifecycle_status(sku, current_date)
        sku.lifecycle_status = effective_status if effective_status != "not_yet_active" else "active"
        sku.is_active = effective_status in {"active", "phase_out"}
        sku.row_version += 1
        sku.updated_at_utc = now
    return {
        **preview,
        "lifecycleStatus": resolve_effective_lifecycle_status(anchor, current_date),
        "rowVersions": {sku.material_code: sku.row_version for sku in rows},
    }


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
    remark: str | None = None,
    update_remark: bool = False,
) -> CountrySkuFobResolved | None:
    """Update a material-country base and derive its final FOB.

    The legacy argument name is retained for API compatibility. BOM Admin
    callers pass the Single/base value; Dual/Special surcharge is resolved
    here so no write path persists a derived colour as an independent base.
    """
    sku = get_sku_by_material_code_any_status(session, material_code)
    if sku is None:
        return None
    stmt = select(CountrySkuFobResolved).where(
        CountrySkuFobResolved.material_code == material_code,
        CountrySkuFobResolved.country_code == country_code,
        CountrySkuFobResolved.is_active == True,
    )
    existing_rows = list(session.execute(stmt).scalars().all())

    if final_fob_eur is None:
        now = datetime.now(timezone.utc)
        for row in existing_rows:
            row.is_active = False
            row.updated_at_utc = now
        return existing_rows[0] if existing_rows else None

    tier = resolve_effective_colour_tier(sku)
    decision = resolve_colour_surcharge_for_sku(session, sku, tier)
    if decision["status"] == "missing_tier":
        raise ValueError(f"Colour tier is required for {material_code}")
    if decision["status"] == "missing_rule":
        raise ValueError(
            f"No {tier} colour surcharge rule is configured for {material_code}"
        )
    base_fob = round(float(final_fob_eur), 2)
    surcharge = float(decision["amount"] or 0.0)
    new_final = round(base_fob + surcharge, 2)
    stored_surcharge = surcharge if surcharge > 0 else None
    now = datetime.now(timezone.utc)

    for existing in existing_rows:
        old_final = float(existing.final_fob_eur or 0.0)
        if round(old_final, 2) != new_final:
            session.add(
                FobResolvedHistory(
                    country_sku_fob_id=existing.country_sku_fob_id,
                    baseline_version_id=existing.baseline_version_id,
                    country_code=existing.country_code,
                    material_code=existing.material_code,
                    payment_term_code=existing.payment_term_code,
                    old_uploaded_fob_eur=existing.uploaded_fob_eur,
                    new_uploaded_fob_eur=base_fob,
                    old_final_fob_eur=existing.final_fob_eur,
                    new_final_fob_eur=new_final,
                    changed_by="manual_edit",
                )
            )
        existing.base_fob_eur = base_fob
        existing.uploaded_fob_eur = base_fob
        existing.colour_surcharge_eur = stored_surcharge
        existing.final_fob_eur = new_final
        existing.fob_source_country_code = None
        existing.fob_source_mode = "manual_edit"
        if update_remark:
            existing.remark = remark or None
        existing.updated_at_utc = now
    if existing_rows:
        return existing_rows[0]

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
        base_fob_eur=base_fob,
        uploaded_fob_eur=base_fob,
        colour_surcharge_eur=stored_surcharge,
        final_fob_eur=new_final,
        fob_source_mode="manual_edit",
        remark=(remark or None) if update_remark else None,
        is_active=True,
    )
    session.add(fob)
    return fob


def update_bom_template_base_fob(
    session: Session,
    bom_template: str,
    material_codes: list[str],
    country_code: str,
    base_fob_eur: float | None,
    *,
    remark: str | None = None,
    update_remark: bool = False,
    changed_by: str | None = None,
) -> dict[str, object]:
    """Persist one country base and derive every colour in a BOM template.

    A template edit is intentionally different from a per-SKU final-price edit:
    it updates the shared base on every active colour row and recalculates the
    tier surcharge.  The base remains present even when the last Single colour
    is moved away because derived rows retain ``base_fob_eur``.
    """
    normalized_template = clean_text(bom_template).upper()
    normalized_country = clean_text(country_code).upper()
    normalized_codes = [clean_text(code).upper() for code in material_codes if clean_text(code)]
    if not normalized_template:
        raise ValueError("bomTemplate is required")
    if not normalized_country:
        raise ValueError("countryCode is required")
    if not normalized_codes:
        raise ValueError("materialCodes is required")
    if base_fob_eur is not None and base_fob_eur < 0:
        raise ValueError("baseFobEur must be greater than or equal to 0")

    skus = list(
        session.execute(
            select(MaterialSkuMaster).where(
                MaterialSkuMaster.material_code.in_(normalized_codes),
                MaterialSkuMaster.bom_template == normalized_template,
                MaterialSkuMaster.is_active == True,
            )
        ).scalars().all()
    )
    if not skus:
        raise LookupError("No active SKUs found for BOM template")

    rows = list(
        session.execute(
            select(CountrySkuFobResolved).where(
                CountrySkuFobResolved.material_code.in_([sku.material_code for sku in skus]),
                CountrySkuFobResolved.country_code == normalized_country,
                CountrySkuFobResolved.is_active == True,
            )
        ).scalars().all()
    )
    rows_by_material: dict[str, list[CountrySkuFobResolved]] = {}
    for row in rows:
        rows_by_material.setdefault(row.material_code, []).append(row)

    payment_term = get_country_payment_term(session, normalized_country)
    payment_term_code = payment_term.payment_term_code if payment_term else "TT"
    baseline = get_latest_baseline(session)
    if baseline is None and base_fob_eur is not None:
        baseline = create_baseline_version(
            session=session,
            source_file_name="manual_admin",
            source_file_hash=None,
            baseline_name="manual_admin",
            published_by=changed_by or "system",
        )
        session.flush()

    details: list[dict[str, object]] = []
    updated = 0
    created = 0
    cleared = 0
    now = datetime.now(timezone.utc)
    for sku in skus:
        sku_rows = rows_by_material.get(sku.material_code, [])
        if base_fob_eur is None:
            for row in sku_rows:
                row.is_active = False
                row.updated_at_utc = now
                cleared += 1
            details.append({"materialCode": sku.material_code, "status": "cleared", "rows": len(sku_rows)})
            continue

        tier = resolve_effective_colour_tier(sku)
        if tier is None:
            raise ValueError(f"Colour tier is required for {sku.material_code}")
        decision = resolve_colour_surcharge_for_sku(session, sku, tier)
        if decision["status"] == "missing_rule":
            raise ValueError(
                f"No {tier} colour surcharge rule is configured for {sku.material_code}"
            )
        surcharge = float(decision["amount"] or 0.0)
        final_fob = round(float(base_fob_eur) + surcharge, 2)
        target_rows = sku_rows
        if not target_rows:
            if baseline is None:
                raise ValueError("No baseline available for BOM template FOB")
            target_rows = [
                CountrySkuFobResolved(
                    country_sku_fob_id=uuid4(),
                    baseline_version_id=baseline.baseline_version_id,
                    country_code=normalized_country,
                    material_code=sku.material_code,
                    payment_term_code=payment_term_code,
                    is_active=True,
                )
            ]
            session.add(target_rows[0])
            created += 1
        for row in target_rows:
            old_final = float(row.final_fob_eur or 0)
            if round(old_final, 2) != final_fob:
                session.add(
                    FobResolvedHistory(
                        country_sku_fob_id=row.country_sku_fob_id,
                        baseline_version_id=row.baseline_version_id,
                        country_code=row.country_code,
                        material_code=row.material_code,
                        payment_term_code=row.payment_term_code,
                        old_uploaded_fob_eur=row.uploaded_fob_eur,
                        new_uploaded_fob_eur=base_fob_eur,
                        old_final_fob_eur=row.final_fob_eur,
                        new_final_fob_eur=final_fob,
                        changed_by=changed_by or "bom_template_base_edit",
                    )
                )
                updated += 1
            row.base_fob_eur = base_fob_eur
            row.uploaded_fob_eur = base_fob_eur
            row.colour_surcharge_eur = surcharge or None
            row.final_fob_eur = final_fob
            row.fob_source_country_code = None
            row.fob_source_mode = "template_base"
            if update_remark:
                row.remark = remark or None
            row.updated_at_utc = now
        details.append({
            "materialCode": sku.material_code,
            "tier": tier,
            "baseFobEur": float(base_fob_eur),
            "colourSurchargeEur": surcharge or None,
            "finalFobEur": final_fob,
            "rows": len(target_rows),
        })
    return {
        "bomTemplate": normalized_template,
        "countryCode": normalized_country,
        "baseFobEur": base_fob_eur,
        "updated": updated,
        "created": created,
        "cleared": cleared,
        "details": details,
    }


def list_country_template_fob_periods(
    session: Session,
    country_code: str,
    bom_template: str,
) -> list[CountryTemplateFobPeriod]:
    return list(
        session.execute(
            select(CountryTemplateFobPeriod)
            .where(
                CountryTemplateFobPeriod.country_code == clean_text(country_code).upper(),
                CountryTemplateFobPeriod.bom_template == clean_text(bom_template).upper(),
                CountryTemplateFobPeriod.status == "active",
            )
            .order_by(CountryTemplateFobPeriod.valid_from)
        ).scalars().all()
    )


def has_country_template_fob_periods(
    session: Session,
    country_code: str,
    bom_template: str,
) -> bool:
    return session.execute(
        select(CountryTemplateFobPeriod.country_template_fob_period_id)
        .where(
            CountryTemplateFobPeriod.country_code == clean_text(country_code).upper(),
            CountryTemplateFobPeriod.bom_template == clean_text(bom_template).upper(),
            CountryTemplateFobPeriod.status == "active",
        )
        .limit(1)
    ).scalar_one_or_none() is not None


def resolve_country_template_fob_period(
    session: Session,
    country_code: str,
    bom_template: str,
    target_date: date,
) -> CountryTemplateFobPeriod | None:
    rows = list(
        session.execute(
            select(CountryTemplateFobPeriod).where(
                CountryTemplateFobPeriod.country_code == clean_text(country_code).upper(),
                CountryTemplateFobPeriod.bom_template == clean_text(bom_template).upper(),
                CountryTemplateFobPeriod.status == "active",
                CountryTemplateFobPeriod.valid_from <= target_date,
                or_(
                    CountryTemplateFobPeriod.valid_to.is_(None),
                    CountryTemplateFobPeriod.valid_to >= target_date,
                ),
            )
        ).scalars().all()
    )
    if len(rows) > 1:
        raise ValueError(
            f"Overlapping FOB periods for {clean_text(bom_template).upper()} / "
            f"{clean_text(country_code).upper()} on {target_date.isoformat()}"
        )
    return rows[0] if rows else None


def save_country_template_fob_period(
    session: Session,
    *,
    country_code: str,
    bom_template: str,
    valid_from: date,
    valid_to: date | None,
    base_fob_eur: float,
    remark: str | None,
    changed_by: str,
    period_id: UUID | None = None,
    row_version: int | None = None,
) -> CountryTemplateFobPeriod:
    country = clean_text(country_code).upper()
    template = clean_text(bom_template).upper()
    if not country or not template:
        raise ValueError("countryCode and bomTemplate are required")
    if valid_to is not None and valid_to < valid_from:
        raise ValueError("validTo must be on or after validFrom")
    if not math.isfinite(base_fob_eur) or base_fob_eur < 0:
        raise ValueError("baseFobEur must be greater than or equal to 0")

    baseline = get_latest_baseline(session)
    if baseline is not None:
        try:
            lifecycle = get_bom_template_lifecycle(
                session,
                template,
                baseline.baseline_version_id,
            )
        except LookupError:
            lifecycle = None
        if lifecycle is not None:
            if lifecycle["inconsistent"]:
                raise ValueError(
                    "BOM template lifecycle is inconsistent across colour SKUs; align it before saving FOB periods"
                )
            template_from = lifecycle["effectiveFrom"]
            template_to = lifecycle["effectiveTo"]
            if template_from is not None and valid_from < template_from:
                raise ValueError(
                    f"FOB period starts before the template lifecycle ({template_from.isoformat()})"
                )
            if template_to is not None and (valid_to is None or valid_to > template_to):
                raise ValueError(
                    f"FOB period exceeds the template final order date ({template_to.isoformat()})"
                )

    row = (
        session.get(CountryTemplateFobPeriod, period_id)
        if period_id is not None
        else None
    )
    if period_id is not None and (row is None or row.status != "active"):
        raise LookupError("FOB period not found")
    if row is not None and row.row_version != row_version:
        raise RuntimeError("FOB period changed; refresh and retry")
    if row is not None and (
        row.country_code != country or row.bom_template != template
    ):
        raise ValueError("FOB period country and BOM template cannot be changed")

    overlap = select(CountryTemplateFobPeriod.country_template_fob_period_id).where(
        CountryTemplateFobPeriod.country_code == country,
        CountryTemplateFobPeriod.bom_template == template,
        CountryTemplateFobPeriod.status == "active",
        or_(
            CountryTemplateFobPeriod.valid_to.is_(None),
            CountryTemplateFobPeriod.valid_to >= valid_from,
        ),
    )
    if valid_to is not None:
        overlap = overlap.where(CountryTemplateFobPeriod.valid_from <= valid_to)
    if period_id is not None:
        overlap = overlap.where(
            CountryTemplateFobPeriod.country_template_fob_period_id != period_id
        )
    if session.execute(overlap.limit(1)).scalar_one_or_none() is not None:
        raise ValueError("FOB periods cannot overlap for the same template and country")

    if base_fob_eur > 0:
        for other in list_country_template_fob_periods(session, country, template):
            if other.country_template_fob_period_id == period_id or float(other.base_fob_eur) <= 0:
                continue
            # Even disjoint days within one month must share one positive base.
            if ((other.valid_to is None or (other.valid_to.year, other.valid_to.month) >= (valid_from.year, valid_from.month))
                    and (valid_to is None or (other.valid_from.year, other.valid_from.month) <= (valid_to.year, valid_to.month))
                    and round(float(other.base_fob_eur), 2) != round(base_fob_eur, 2)):
                raise ValueError("One month must use one Single base; align the prices or start the new price next month / 同月只能有一个基准价，请统一价格或从下月开始")

    if row is None:
        row = CountryTemplateFobPeriod(
            country_template_fob_period_id=uuid4(),
            country_code=country,
            bom_template=template,
            valid_from=valid_from,
            base_fob_eur=round(base_fob_eur, 2),
            created_by=changed_by,
        )
        session.add(row)
    else:
        row.row_version += 1
    row.country_code = country
    row.bom_template = template
    row.valid_from = valid_from
    row.valid_to = valid_to
    row.base_fob_eur = round(base_fob_eur, 2)
    row.remark = clean_text(remark) or None
    row.updated_by = changed_by
    return row


def delete_country_template_fob_period(
    session: Session,
    period_id: UUID,
    row_version: int,
) -> None:
    row = session.get(CountryTemplateFobPeriod, period_id)
    if row is None or row.status != "active":
        raise LookupError("FOB period not found")
    if row.row_version != row_version:
        raise RuntimeError("FOB period changed; refresh and retry")
    row.status = "deleted"
    row.row_version += 1


def preview_country_template_fob_period_deletion(
    session: Session, period_id: UUID, row_version: int,
) -> dict:
    """Resolve the last-period impact using the same trusted Single base as repricing."""
    period = session.get(CountryTemplateFobPeriod, period_id)
    if period is None or period.status != "active":
        raise LookupError("FOB period not found")
    if period.row_version != row_version:
        raise RuntimeError("FOB period changed; refresh and retry")
    country_code, bom_template = period.country_code, period.bom_template
    periods = list_country_template_fob_periods(session, country_code, bom_template)
    baseline = get_latest_baseline(session)
    skus = list_bom_template_skus(session, bom_template, baseline.baseline_version_id) if baseline else []
    if not skus:
        raise LookupError("BOM template not found")
    base = None
    evidence = []
    candidates: set[float] = set()
    for sku in skus:
        fob = get_fob_for_country_sku(session, country_code, sku.material_code)
        if fob is not None:
            evidence.append([sku.material_code, sku.row_version, sku.colour_tier, float(fob.final_fob_eur),
                             float(fob.base_fob_eur) if fob.base_fob_eur is not None else None])
            if float(fob.final_fob_eur) == 0 and fob.base_fob_eur in (None, 0):
                base = 0.0
                candidates.add(base)
                continue
            resolution = _resolve_colour_surcharge_reprice_base(session, sku, fob)
            if len(periods) == 1 and resolution.get("baseFobEur") is None:
                if resolution["status"] == "ambiguous":
                    raise ValueError(f"Conflicting default Single bases: {resolution.get('candidates')}; confirm the template/country base in BOM Admin")
                raise ValueError("Missing trusted default Single base; confirm the template/country base in BOM Admin")
            if resolution.get("baseFobEur") is not None:
                base = float(resolution["baseFobEur"])
                candidates.add(base)
    if len(periods) == 1 and len(candidates) > 1:
        raise ValueError("Conflicting default Single bases; confirm the template/country base in BOM Admin / 长期基准冲突，请确认模板国家基准")
    fingerprint = hashlib.sha256(json.dumps(
        [str(period_id), [(str(p.country_template_fob_period_id), p.row_version) for p in periods], evidence, base],
        sort_keys=True,
    ).encode()).hexdigest()
    return {"fingerprint": fingerprint, "lastPeriod": len(periods) == 1, "defaultBaseFobEur": base}


def country_template_fob_period_payload(row: CountryTemplateFobPeriod) -> dict:
    return {
        "periodId": str(row.country_template_fob_period_id), "countryCode": row.country_code,
        "bomTemplate": row.bom_template, "validFrom": row.valid_from.isoformat(),
        "validTo": row.valid_to.isoformat() if row.valid_to else None,
        "baseFobEur": float(row.base_fob_eur), "remark": row.remark, "rowVersion": row.row_version,
    }


def initialize_sku_fobs_from_source(
    session: Session,
    target_material_code: str,
    source_material_code: str,
    *,
    changed_by: str | None = None,
) -> dict[str, object]:
    """Create automatic FOB rows for a new colour from a trusted source SKU.

    Dual and Special rows always resolve their base from a Single SKU in the
    target BOM template and apply the shared surcharge resolver.  The source
    SKU only supplies the available country/payment-term rows; its final price
    is never copied as the target price.
    """
    target = get_sku_by_material_code_any_status(session, target_material_code)
    source = get_sku_by_material_code_any_status(session, source_material_code)
    result: dict[str, object] = {
        "sourceMaterialCode": source_material_code,
        "materialCode": target_material_code,
        "rows": 0,
        "created": 0,
        "skippedNoBase": 0,
        "skippedAmbiguous": 0,
        "skippedMissingTier": 0,
        "skippedMissingRule": 0,
        "details": [],
    }
    if target is None or source is None:
        return result

    source_rows = list(
        session.execute(
            select(CountrySkuFobResolved).where(
                CountrySkuFobResolved.material_code == source.material_code,
                CountrySkuFobResolved.is_active == True,
                CountrySkuFobResolved.final_fob_eur > 0,
            )
        ).scalars().all()
    )
    target_tier = resolve_effective_colour_tier(target)
    if target_tier is None:
        result["skippedMissingTier"] = len(source_rows)
        result["details"] = [
            {"countryCode": row.country_code, "status": "skipped", "reason": "missing_colour_tier"}
            for row in source_rows
        ]
        return result
    decision = resolve_colour_surcharge_for_sku(session, target, target_tier)
    if decision["status"] == "missing_rule":
        result["skippedMissingRule"] = len(source_rows)
        result["details"] = [
            {"countryCode": row.country_code, "status": "skipped", "reason": "missing_colour_surcharge_rule"}
            for row in source_rows
        ]
        return result
    surcharge = float(decision["amount"] or 0.0)
    details = result["details"]
    assert isinstance(details, list)

    for source_row in source_rows:
        result["rows"] = int(result["rows"]) + 1
        resolution = _resolve_colour_surcharge_reprice_base(session, target, source_row)
        base_fob = resolution["baseFobEur"]
        colour_surcharge = surcharge if surcharge > 0 else None
        if base_fob is None:
            ambiguous = resolution["status"] == "ambiguous"
            counter = "skippedAmbiguous" if ambiguous else "skippedNoBase"
            result[counter] = int(result[counter]) + 1
            details.append({
                "countryCode": source_row.country_code,
                "status": "skipped",
                "reason": "ambiguous_single_base" if ambiguous else "missing_single_base",
            })
            continue

        baseline_id = source_row.baseline_version_id
        final_fob = round(base_fob + (colour_surcharge or 0.0), 2)
        session.add(
            CountrySkuFobResolved(
                country_sku_fob_id=uuid4(),
                baseline_version_id=baseline_id,
                country_code=source_row.country_code,
                material_code=target.material_code,
                payment_term_code=source_row.payment_term_code,
                base_fob_eur=base_fob,
                colour_surcharge_eur=colour_surcharge,
                uploaded_fob_eur=base_fob,
                final_fob_eur=final_fob,
                fob_source_country_code=source_row.country_code,
                fob_source_mode="uploaded_base_plus_colour" if target_tier != "single" else "copied_from_template_colour",
                remark=source_row.remark,
                is_active=True,
            )
        )
        result["created"] = int(result["created"]) + 1
        cast_details = result["details"]
        assert isinstance(cast_details, list)
        cast_details.append({
            "countryCode": source_row.country_code,
            "baseFobEur": base_fob,
            "colourSurchargeEur": colour_surcharge,
            "finalFobEur": final_fob,
            "status": "created",
        })
    return result


def clear_country_fobs(session: Session, country_code: str, allowed_brands: set[str] | None = None) -> int:
    """Deactivate all active BOM FOB rows for one country column."""
    result = session.execute(
        update(CountrySkuFobResolved)
        .where(
            *([CountrySkuFobResolved.material_code.in_(select(MaterialSkuMaster.material_code).where(
                func.upper(MaterialSkuMaster.brand).in_(allowed_brands)))] if allowed_brands is not None else []),
            CountrySkuFobResolved.country_code == country_code,
            CountrySkuFobResolved.is_active == True,
        )
        .values(
            is_active=False,
            fob_source_mode="country_column_trash",
            updated_at_utc=datetime.now(timezone.utc),
        )
    )
    return int(result.rowcount or 0)


def list_country_fob_trash(session: Session, allowed_brands: set[str] | None = None,
                          allowed_countries: set[str] | None = None) -> list[dict[str, object]]:
    """List country columns currently parked in BOM FOB trash."""
    active_countries = {
        str(code or "").strip().upper()
        for code in session.execute(
            select(CountrySkuFobResolved.country_code)
            .where(CountrySkuFobResolved.is_active == True)
            .distinct()
        ).scalars().all()
        if len(str(code or "").strip()) == 2
    }
    rows = session.execute(
        select(
            CountrySkuFobResolved.country_code,
            CountrySkuFobResolved.fob_source_mode,
            CountrySkuFobResolved.updated_at_utc,
        )
        .where(
            CountrySkuFobResolved.is_active == False,
            *([CountrySkuFobResolved.country_code.in_(allowed_countries)] if allowed_countries is not None else []),
            *([CountrySkuFobResolved.material_code.in_(select(MaterialSkuMaster.material_code).where(
                func.upper(MaterialSkuMaster.brand).in_(allowed_brands)))] if allowed_brands is not None else []),
        )
    ).all()
    by_country: dict[str, dict[str, object]] = {}
    for country_code, source_mode, deleted_at in rows:
        country = str(country_code or "").strip().upper()
        if len(country) != 2:
            continue
        if source_mode != "country_column_trash" and country in active_countries:
            continue
        item = by_country.setdefault(country, {"countryCode": country, "rows": 0, "deletedAtUtc": None})
        item["rows"] = int(item["rows"]) + 1
        current_deleted_at = item["deletedAtUtc"]
        if current_deleted_at is None or (deleted_at is not None and deleted_at > current_deleted_at):
            item["deletedAtUtc"] = deleted_at
    return [by_country[country] for country in sorted(by_country)]


def _select_country_trash_rows(session: Session, country: str, allowed_brands: set[str] | None = None) -> list[CountrySkuFobResolved]:
    active_country_exists = session.execute(
        select(CountrySkuFobResolved.country_sku_fob_id)
        .where(
            CountrySkuFobResolved.country_code == country,
            CountrySkuFobResolved.is_active == True,
        )
        .limit(1)
    ).first()
    stmt = select(CountrySkuFobResolved).where(
        CountrySkuFobResolved.country_code == country,
        CountrySkuFobResolved.is_active == False,
        *([CountrySkuFobResolved.material_code.in_(select(MaterialSkuMaster.material_code).where(
            func.upper(MaterialSkuMaster.brand).in_(allowed_brands)))] if allowed_brands is not None else []),
    )
    if active_country_exists:
        stmt = stmt.where(CountrySkuFobResolved.fob_source_mode == "country_column_trash")
    return list(session.execute(stmt).scalars().all())


def restore_country_fobs_from_trash(session: Session, country_code: str, allowed_brands: set[str] | None = None) -> dict[str, int | str]:
    """Restore a trashed country column, skipping rows that now have active replacements."""
    country = str(country_code or "").strip().upper()
    trashed_rows = _select_country_trash_rows(session, country, allowed_brands)
    restored = 0
    skipped_active_conflict = 0
    now = datetime.now(timezone.utc)
    for row in trashed_rows:
        active_exists = session.execute(
            select(CountrySkuFobResolved.country_sku_fob_id)
            .where(
                CountrySkuFobResolved.country_code == row.country_code,
                CountrySkuFobResolved.material_code == row.material_code,
                CountrySkuFobResolved.payment_term_code == row.payment_term_code,
                CountrySkuFobResolved.is_active == True,
            )
            .limit(1)
        ).first()
        if active_exists:
            skipped_active_conflict += 1
            continue
        row.is_active = True
        row.fob_source_mode = "country_column_restore"
        row.updated_at_utc = now
        restored += 1
    return {
        "countryCode": country,
        "rows": len(trashed_rows),
        "restored": restored,
        "skippedActiveConflict": skipped_active_conflict,
    }


def purge_country_fob_trash(session: Session, country_code: str, allowed_brands: set[str] | None = None) -> int:
    """Permanently delete inactive trash rows for one country."""
    country = str(country_code or "").strip().upper()
    trashed_ids = [row.country_sku_fob_id for row in _select_country_trash_rows(session, country, allowed_brands)]
    if not trashed_ids:
        return 0
    result = session.execute(
        delete(CountrySkuFobResolved).where(
            CountrySkuFobResolved.country_sku_fob_id.in_(trashed_ids),
        )
    )
    return int(result.rowcount or 0)


def copy_country_fobs(
    session: Session,
    source_country_code: str,
    target_country_code: str,
    *,
    overwrite_existing: bool = False,
    changed_by: str | None = None,
    allowed_brands: set[str] | None = None,
) -> dict[str, int | str | None]:
    """Copy trusted country bases and derive each target colour price."""
    source = clean_text(source_country_code).upper()
    target = clean_text(target_country_code).upper()
    if source == target:
        raise ValueError("Source and target countries must differ")
    source_rows = list_fob_by_country(session, source)
    if allowed_brands is not None:
        skus = get_skus_by_material_codes_any_status(session, [row.material_code for row in source_rows])
        source_rows = [row for row in source_rows if row.material_code in skus
                       and str(skus[row.material_code].brand or "").upper() in allowed_brands]
    target_term = get_country_payment_term(session, target)
    target_payment_term_code = target_term.payment_term_code if target_term else None
    created = updated = skipped = unchanged = repriced = skipped_ambiguous = 0
    plans = []
    for group in _country_fob_groups(source_rows).values():
        # Source finals are historical output, not the pricing authority. Pick
        # one metadata carrier and let the shared template-country base resolver
        # decide whether the group is actually safe to derive.
        source_row = min(group, key=lambda row: str(row.payment_term_code or ""))
        sku = get_sku_by_material_code(session, source_row.material_code)
        if sku is None:
            skipped += 1
            continue
        tier = resolve_effective_colour_tier(sku)
        decision = resolve_colour_surcharge_for_sku(session, sku, tier)
        resolution = _resolve_colour_surcharge_reprice_base(session, sku, source_row)
        if decision["amount"] is None or resolution["baseFobEur"] is None:
            skipped += 1
            if resolution["status"] == "ambiguous":
                skipped_ambiguous += 1
            continue
        existing = list(session.execute(select(CountrySkuFobResolved).where(
            CountrySkuFobResolved.material_code == source_row.material_code,
            CountrySkuFobResolved.country_code == target,
            CountrySkuFobResolved.is_active == True,
        )).scalars().all())
        if existing and not overwrite_existing:
            skipped += 1
            continue
        base = float(resolution["baseFobEur"])
        if not overwrite_existing:
            target_scope = CountrySkuFobResolved(country_code=target)
            target_base = _resolve_colour_surcharge_reprice_base(session, sku, target_scope)
            if target_base["status"] == "ambiguous":
                skipped += 1
                continue
            if target_base["baseFobEur"] is not None:
                base = float(target_base["baseFobEur"])
        plans.append((source_row, existing, base, float(decision["amount"]), tier))

    # Plans are frozen before writes so colour order cannot affect a base.
    for source_row, existing, base, surcharge, tier in plans:
        final = round(base + surcharge, 2)
        if not existing:
            row = CountrySkuFobResolved(
                baseline_version_id=source_row.baseline_version_id,
                country_code=target, material_code=source_row.material_code,
                payment_term_code=target_payment_term_code or source_row.payment_term_code,
                is_active=True,
            )
            session.add(row)
            existing = [row]
            created += 1
        for row in existing:
            if row.final_fob_eur is not None:
                if float(row.final_fob_eur) == final and row.base_fob_eur == base and row.colour_surcharge_eur == (surcharge or None):
                    unchanged += 1
                else:
                    session.add(FobResolvedHistory(
                        country_sku_fob_id=row.country_sku_fob_id,
                        baseline_version_id=row.baseline_version_id,
                        country_code=target, material_code=row.material_code,
                        payment_term_code=row.payment_term_code,
                        old_uploaded_fob_eur=row.uploaded_fob_eur, new_uploaded_fob_eur=base,
                        old_final_fob_eur=row.final_fob_eur, new_final_fob_eur=final,
                        changed_by=changed_by or "copy_country_fobs",
                    ))
                    updated += 1
            row.base_fob_eur = row.uploaded_fob_eur = base
            row.colour_surcharge_eur = surcharge or None
            row.final_fob_eur = final
            row.payment_term_adjustment_eur = None
            row.fob_source_mode = "template_base"
            row.fob_source_country_code = source
            row.remark = source_row.remark
            row.updated_at_utc = datetime.now(timezone.utc)
            if tier in {"dual", "special"}:
                repriced += 1
    return {
        "sourceCountryCode": source, "targetCountryCode": target,
        "sourceRows": len(source_rows), "copied": created, "created": created,
        "updated": updated, "skipped": skipped, "skippedAmbiguous": skipped_ambiguous,
        "unchanged": unchanged,
        "repriced": repriced, "targetPaymentTermCode": target_payment_term_code,
    }


def adjust_country_fobs(
    session: Session,
    country_code: str,
    delta_eur: float,
    *,
    changed_by: str | None = None,
    allowed_brands: set[str] | None = None,
) -> dict[str, float | int | str]:
    """Adjust every country base, then derive the stored colour price."""
    country = str(country_code or "").strip().upper()
    delta = round(float(delta_eur), 2)
    rows = list_fob_by_country(session, country)
    if allowed_brands is not None:
        skus = get_skus_by_material_codes_any_status(session, [row.material_code for row in rows])
        rows = [row for row in rows if row.material_code in skus
                and str(skus[row.material_code].brand or "").upper() in allowed_brands]
    adjusted = 0
    skipped_negative = 0
    unchanged = 0
    skipped_no_base = 0
    skipped_ambiguous = 0
    skipped_missing_tier = 0
    skipped_missing_rule = 0

    # Resolve every base and surcharge before mutating any row.  A derived row
    # without a stored base must see the pre-adjustment Single value; otherwise
    # the result depends on whether the Single row happened to be processed
    # first (and the delta can be applied twice).
    plans: list[tuple[CountrySkuFobResolved, float, float, float]] = []
    for row in rows:
        sku = get_sku_by_material_code_any_status(session, row.material_code)
        if sku is None:
            skipped_no_base += 1
            continue
        tier = resolve_effective_colour_tier(sku)
        if tier is None:
            skipped_missing_tier += 1
            continue
        decision = resolve_colour_surcharge_for_sku(session, sku, tier)
        if decision["status"] == "missing_rule":
            skipped_missing_rule += 1
            continue
        surcharge = float(decision["amount"] or 0.0)
        old_value = float(row.final_fob_eur)
        resolution = _resolve_colour_surcharge_reprice_base(session, sku, row)
        if resolution["status"] == "ambiguous":
            skipped_ambiguous += 1
            continue
        old_base = resolution["baseFobEur"]
        if old_base is None:
            skipped_no_base += 1
            continue
        new_base = round(old_base + delta, 2)
        new_value = round(new_base + surcharge, 2)
        if new_value < 0:
            skipped_negative += 1
            continue
        if new_value == old_value:
            unchanged += 1
            continue

        plans.append((row, old_base, surcharge, new_value))

    for row, old_base, surcharge, new_value in plans:
        new_base = round(old_base + delta, 2)

        session.add(
            FobResolvedHistory(
                country_sku_fob_id=row.country_sku_fob_id,
                baseline_version_id=row.baseline_version_id,
                country_code=country,
                material_code=row.material_code,
                payment_term_code=row.payment_term_code,
                old_uploaded_fob_eur=row.uploaded_fob_eur,
                new_uploaded_fob_eur=new_base,
                old_final_fob_eur=row.final_fob_eur,
                new_final_fob_eur=new_value,
                changed_by=changed_by or "adjust_country_fobs",
            )
        )
        row.base_fob_eur = new_base
        row.uploaded_fob_eur = new_base
        row.colour_surcharge_eur = surcharge if surcharge > 0 else None
        row.final_fob_eur = new_value
        row.fob_source_mode = "template_base_country_adjust"
        row.fob_source_country_code = None
        row.updated_at_utc = datetime.now(timezone.utc)
        adjusted += 1

    return {
        "countryCode": country,
        "deltaEur": delta,
        "rows": len(rows),
        "adjusted": adjusted,
        "skippedNegative": skipped_negative,
        "unchanged": unchanged,
        "skippedNoBase": skipped_no_base,
        "skippedAmbiguous": skipped_ambiguous,
        "skippedMissingTier": skipped_missing_tier,
        "skippedMissingRule": skipped_missing_rule,
    }


def list_bom_with_fob(
    session: Session,
    brand: str | None = None,
    search: str | None = None,
    country_code: str | None = None,
    limit: int = 1000,
    *,
    include_conflicts: bool = False,
    allowed_brands: set[str] | None = None,
    allowed_countries: set[str] | None = None,
) -> tuple[list[dict], list[str]] | tuple[list[dict], list[str], list[dict[str, object]]]:
    """Return SKUs with their FOB per country, grouped for BOM admin display."""
    all_countries = list_active_fob_country_codes(session)
    if allowed_countries is not None:
        all_countries = [country for country in all_countries if country in allowed_countries]
    skus = list_all_material_skus_for_admin(session, brand=brand, search=search, country_code=country_code,
                                           limit=limit, allowed_brands=allowed_brands)
    if not skus:
        empty_result = ([], all_countries, [])
        return empty_result if include_conflicts else empty_result[:2]

    colour_standards = list_persistent_colour_standard_map(session)

    material_codes = [s.material_code for s in skus]
    fobs = session.execute(
        select(CountrySkuFobResolved).where(
            CountrySkuFobResolved.material_code.in_(material_codes),
            CountrySkuFobResolved.is_active == True,
            *([CountrySkuFobResolved.country_code.in_(allowed_countries)] if allowed_countries is not None else []),
        )
    ).scalars().all()

    periods_by_template: dict[str, dict[str, list[dict]]] = {}
    periods = session.execute(select(CountryTemplateFobPeriod).where(
        CountryTemplateFobPeriod.bom_template.in_({s.bom_template for s in skus if s.bom_template}),
        CountryTemplateFobPeriod.status == "active",
        *([CountryTemplateFobPeriod.country_code.in_(allowed_countries)] if allowed_countries is not None else []),
    ).order_by(CountryTemplateFobPeriod.valid_from)).scalars().all()
    for period in periods:
        periods_by_template.setdefault(period.bom_template, {}).setdefault(period.country_code, []).append(
            country_template_fob_period_payload(period)
        )

    # Build FOB map: material_code -> { country_code: { fob, paymentTerm } }
    fob_map: dict[str, dict] = {}
    fob_conflict_map: dict[str, dict[str, dict[str, object]]] = {}
    fob_conflicts: list[dict[str, object]] = []
    for group in _country_fob_groups(fobs).values():
        try:
            f = _consistent_country_fob(group)
        except CountryFobConflict:
            conflict = _country_fob_conflict_payload(group)
            fob_conflicts.append(conflict)
            fob_conflict_map.setdefault(group[0].material_code, {})[group[0].country_code] = conflict
            continue
        assert f is not None
        if f.material_code not in fob_map:
            fob_map[f.material_code] = {}
        fob_entry = {
            "finalFobEur": float(f.final_fob_eur),
            "paymentTermCode": f.payment_term_code,
            "fobSourceMode": f.fob_source_mode,
        }
        if f.base_fob_eur is not None:
            fob_entry["baseFobEur"] = float(f.base_fob_eur)
        if f.uploaded_fob_eur:
            fob_entry["uploadedFobEur"] = float(f.uploaded_fob_eur)
        if f.colour_surcharge_eur:
            fob_entry["colourSurchargeEur"] = float(f.colour_surcharge_eur)
        if f.fob_source_country_code:
            fob_entry["fobSourceCountryCode"] = f.fob_source_country_code
        if f.remark:
            fob_entry["remark"] = f.remark
        fob_map[f.material_code][f.country_code] = fob_entry

    for material_code, country_conflicts in fob_conflict_map.items():
        fob_map.setdefault(material_code, {}).update({
            country: {
                "status": "conflict",
                "reason": conflict["reason"],
                "records": conflict["records"],
            }
            for country, conflict in country_conflicts.items()
        })

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

    def slim_source_payload(payload: object) -> dict:
        if not isinstance(payload, dict):
            return {}
        slim: dict[str, object] = {}
        for key in ("sheet_name", "row_index", "warnings"):
            value = payload.get(key)
            if value not in (None, "", []):
                slim[key] = value
        return slim

    interior_by_template = {
        s.bom_template: (s.interior_color_name, s.interior_colour_code, s.interior_package)
        for s in skus if s.bom_template and s.interior_color_name
    }
    payloads: list[dict] = []
    for s in skus:
        display_colour_name, display_colour_hex = resolve_colour_display_values(
            s,
            colour_standards,
        )
        payloads.append({
            "materialCode": s.material_code,
            "brand": resolve_material_brand(s.brand, s.model_name, s.bom_template),
            "modelCode": getattr(s, "model_code", None) or "",
            "modelName": normalize_brand_text(s.model_name),
            "powertrain": _extract_canonical_powertrain(s),
            "version": s.version,
            "colour": display_colour_name or "",
            "colourCode": s.exterior_color_code or "",
            "colourType": s.exterior_color_type or "single",
            "colourHex": display_colour_hex,
            "storedColourHex": s.colour_hex,
            "colourCodeConfirmed": s.colour_code_confirmed,
            "colourTier": resolve_effective_colour_tier(s),
            "colourPricing": resolve_colour_surcharge_for_sku(session, s, resolve_effective_colour_tier(s)),
            "bomTemplate": s.bom_template,
            "interiorColorName": s.interior_color_name or interior_by_template.get(s.bom_template, (None, None, None))[0],
            "interiorColourCode": s.interior_colour_code or interior_by_template.get(s.bom_template, (None, None, None))[1],
            "interiorPackage": s.interior_package or interior_by_template.get(s.bom_template, (None, None, None))[2],
            "editionTag": s.edition_tag,
            "remark": s.remark,
            "lifecycleStatus": resolve_effective_lifecycle_status(s, date.today()),
            "isActive": s.is_active,
            "effectiveFrom": get_lifecycle_dates(s)[0].isoformat() if get_lifecycle_dates(s)[0] else None,
            "effectiveTo": get_lifecycle_dates(s)[1].isoformat() if get_lifecycle_dates(s)[1] else None,
            "rowVersion": s.row_version,
            "fobByCountry": fob_map.get(s.material_code, {}),
            "fobPeriodsByCountry": periods_by_template.get(s.bom_template, {}),
            "financeCountries": sorted(
                finance_country_map.get(s.material_code, set())
                | finance_country_map.get(s.bom_template or "", set())
            ),
            "sourceSheetName": s.source_sheet_name,
            "sourceRowNumber": s.source_row_number,
            "sourceFileName": baseline_names.get(s.baseline_version_id) if s.baseline_version_id else None,
            "sourcePayload": slim_source_payload(s.raw_payload_json),
        })
    if include_conflicts:
        return payloads, all_countries, fob_conflicts
    return payloads, all_countries


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    return float(value)


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
        "brand": resolve_material_brand(sku.brand, sku.model_name, sku.bom_template),
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

    if "**" in material:
        concrete_codes = [item.material_code for item in template_skus if item.material_code]
        if "fob_eur" in values:
            for concrete_code in concrete_codes:
                update_sku_fob_for_country(
                    session,
                    concrete_code,
                    country,
                    values["fob_eur"],
                )
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
    if "fob_eur" in values:
        update_sku_fob_for_country(session, material, country, values["fob_eur"])
    fob = get_fob_for_country_sku(session, country, material)
    return _country_material_finance_payload(sku, country, fob, finance)


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
    allowed_brands: set[str] | None = None,
) -> list[MaterialSkuMaster]:
    """List SKUs with optional filters for the BOM admin panel.

    Returns one row per material_code — prefers active, then highest row_version.
    """
    stmt = select(MaterialSkuMaster)
    if allowed_brands is not None:
        stmt = stmt.where(func.upper(MaterialSkuMaster.brand).in_(allowed_brands))
    if country_code:
        stmt = stmt.where(MaterialSkuMaster.material_code.in_(list_active_fob_material_codes(session, country_code)))
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


def build_colour_hex_rules_from_skus(
    skus: list[object],
    standards: dict[tuple[str, str], BrandColourSwatchRule] | None = None,
) -> list[dict]:
    """Derive reusable colour rules from active SKUs by normalized brand + code."""
    groups: dict[tuple[str, str], dict] = {}
    for sku in skus:
        if not getattr(sku, "is_active", True) or resolve_effective_lifecycle_status(sku, date.today()) == "historical":
            continue
        colour_name = str(getattr(sku, "exterior_color_name", "") or "").strip()
        key = _colour_rule_key(
            resolve_material_brand(
                getattr(sku, "brand", None),
                getattr(sku, "model_name", None),
                getattr(sku, "bom_template", None),
            ),
            getattr(sku, "exterior_color_code", None),
        )
        if key is None:
            continue
        brand, colour_code = key
        group = groups.setdefault(
            key,
            {
                "brand": brand,
                "colourCode": colour_code,
                "skuCount": 0,
                "sampleMaterialCodes": [],
                "placeholderNameSkuCount": 0,
                "missingSwatchSkuCount": 0,
                "_skus": [],
                "_nameCounts": {},
                "_hexCounts": Counter(),
            },
        )
        group["skuCount"] += 1
        group["_skus"].append(sku)
        material_code = str(getattr(sku, "material_code", "") or "").strip()
        if material_code and len(group["sampleMaterialCodes"]) < 5:
            group["sampleMaterialCodes"].append(material_code)
        if is_placeholder_colour_name(colour_name, colour_code):
            group["placeholderNameSkuCount"] += 1
        else:
            normalized_name = normalize_colour_rule_name(colour_name)
            option = group["_nameCounts"].setdefault(
                normalized_name,
                {"skuCount": 0, "displayCounts": Counter()},
            )
            option["skuCount"] += 1
            option["displayCounts"][colour_name] += 1
        try:
            colour_hex = normalize_colour_hex_value(
                getattr(sku, "colour_hex", None)
            )
        except ValueError:
            colour_hex = None
        if colour_hex:
            group["_hexCounts"][colour_hex] += 1
        else:
            group["missingSwatchSkuCount"] += 1

    rules: list[dict] = []
    for group in groups.values():
        source_skus = group.pop("_skus")
        name_counts = group.pop("_nameCounts")
        hex_counts: Counter = group.pop("_hexCounts")
        name_options = []
        for normalized_name, option in name_counts.items():
            display_name = sorted(
                option["displayCounts"].items(),
                key=lambda item: (-item[1], item[0].casefold(), item[0]),
            )[0][0]
            name_options.append({
                "colourName": display_name,
                "normalizedColourName": normalized_name,
                "skuCount": option["skuCount"],
            })
        name_options.sort(
            key=lambda item: (-item["skuCount"], item["normalizedColourName"])
        )
        hex_options = [
            {"colourHex": colour_hex, "skuCount": count}
            for colour_hex, count in sorted(
                hex_counts.items(),
                key=lambda item: (-item[1], item[0]),
            )
        ]
        has_name_conflict = len(name_options) > 1
        has_swatch_conflict = len(hex_options) > 1
        standard_colour_name = name_options[0]["colourName"] if len(name_options) == 1 else None
        normalized_colour_name = (
            name_options[0]["normalizedColourName"] if len(name_options) == 1 else None
        )
        standard_colour_hex = hex_options[0]["colourHex"] if len(hex_options) == 1 else None
        standard = (standards or {}).get((group["brand"], group["colourCode"]))
        if standard is not None:
            standard_colour_name = standard.colour_name
            normalized_colour_name = normalize_colour_rule_name(standard.colour_name)
            has_name_conflict = has_name_conflict or any(
                option["normalizedColourName"] != normalized_colour_name for option in name_options
            )
            if not any(option["normalizedColourName"] == normalized_colour_name for option in name_options):
                name_options.append({"colourName": standard.colour_name,
                                     "normalizedColourName": normalized_colour_name, "skuCount": 0})
            if standard.colour_hex:
                standard_colour_hex = normalize_colour_hex_value(standard.colour_hex)
                has_swatch_conflict = has_swatch_conflict or any(
                    option["colourHex"] != standard_colour_hex for option in hex_options
                )
                if not any(option["colourHex"] == standard_colour_hex for option in hex_options):
                    hex_options.append({"colourHex": standard_colour_hex, "skuCount": 0})

        changes: list[dict] = []
        if standard_colour_name and not has_name_conflict and not has_swatch_conflict:
            for sku in source_skus:
                old_name = str(
                    getattr(sku, "exterior_color_name", "") or ""
                ).strip()
                if is_placeholder_colour_name(old_name, group["colourCode"]):
                    new_name = standard_colour_name
                else:
                    new_name = old_name
                try:
                    old_hex = normalize_colour_hex_value(
                        getattr(sku, "colour_hex", None)
                    )
                except ValueError:
                    old_hex = None
                new_hex = old_hex or standard_colour_hex
                if new_name != old_name or old_hex != new_hex:
                    changes.append({
                        "materialCode": str(
                            getattr(sku, "material_code", "") or ""
                        ).strip(),
                        "brand": group["brand"],
                        "colourCode": group["colourCode"],
                        "oldColourName": old_name or None,
                        "newColourName": new_name,
                        "oldColourHex": old_hex,
                        "newColourHex": new_hex,
                    })
        if has_name_conflict:
            status = "name_conflict"
        elif has_swatch_conflict:
            status = "swatch_conflict"
        elif changes:
            status = "fillable"
        elif not standard_colour_name or not standard_colour_hex:
            status = "missing"
        else:
            status = "complete"
        rules.append(
            {
                **group,
                "colourName": standard_colour_name,
                "normalizedColourName": normalized_colour_name,
                "status": status,
                "standardColourName": standard_colour_name,
                "standardColourHex": standard_colour_hex,
                "nameOptions": name_options,
                "hexOptions": hex_options,
                "hasNameConflict": has_name_conflict,
                "hasSwatchConflict": has_swatch_conflict,
                "fillableSkuCount": len(changes),
                "previewChanges": changes,
            }
        )
    status_rank = dict(name_conflict=0, swatch_conflict=1, missing=2, fillable=3, complete=4)
    return sorted(
        rules,
        key=lambda item: (
            status_rank.get(item["status"], 9),
            item["brand"],
            item["colourCode"],
        ),
    )


def list_persistent_colour_standard_map(
    session: Session,
) -> dict[tuple[str, str], BrandColourSwatchRule]:
    """Load the durable brand+code standard records used by every display path."""
    rows = session.execute(
        select(BrandColourSwatchRule).where(BrandColourSwatchRule.is_active == True)
    ).scalars().all()
    result: dict[tuple[str, str], BrandColourSwatchRule] = {}
    for row in rows:
        if not isinstance(row, BrandColourSwatchRule):
            continue
        key = _colour_rule_key(row.brand, row.colour_code)
        if key is not None:
            result[key] = row
    return result


def resolve_colour_display_values(
    sku: object,
    standards: dict[tuple[str, str], BrandColourSwatchRule] | None = None,
) -> tuple[str | None, str | None]:
    """Return one shared colour name/hex pair for BOM and Matrix rendering."""
    key = _colour_rule_key(
        resolve_material_brand(
            getattr(sku, "brand", None),
            getattr(sku, "model_name", None),
            getattr(sku, "bom_template", None),
        ),
        getattr(sku, "exterior_color_code", None),
    )
    standard = standards.get(key) if standards and key else None
    if standard is not None and getattr(sku, "is_active", True) and resolve_effective_lifecycle_status(sku, date.today()) != "historical":
        return standard.colour_name, standard.colour_hex or getattr(sku, "colour_hex", None)
    return (
        str(getattr(sku, "exterior_color_name", "") or "") or None,
        getattr(sku, "colour_hex", None),
    )


def _upsert_persistent_colour_standard(
    session: Session,
    brand: str,
    colour_code: str,
    colour_name: str,
    colour_hex: str | None,
) -> BrandColourSwatchRule:
    standard_hex = normalize_colour_hex_value(colour_hex)
    if colour_hex is not None and standard_hex is None:
        raise ValueError("colourHex must be valid when supplied; omit it to keep existing swatches")
    key = _colour_rule_key(brand, colour_code)
    if key is None:
        raise ValueError("brand and colourCode are required")
    standard_name = str(colour_name or "").strip()
    if is_placeholder_colour_name(standard_name, key[1]):
        raise ValueError("A non-placeholder colourName is required")
    existing = session.execute(
        select(BrandColourSwatchRule).where(
            BrandColourSwatchRule.brand == key[0],
            BrandColourSwatchRule.colour_code == key[1],
            BrandColourSwatchRule.is_active == True,
        )
    ).scalars().first()
    if isinstance(existing, BrandColourSwatchRule):
        if existing.colour_name != standard_name or (standard_hex is not None and existing.colour_hex != standard_hex):
            existing.colour_name = standard_name
            if standard_hex is not None:
                existing.colour_hex = standard_hex
            existing.updated_at_utc = datetime.now(timezone.utc)
        return existing
    standard = BrandColourSwatchRule(
        brand_colour_swatch_rule_id=uuid4(),
        brand=key[0],
        colour_code=key[1],
        colour_name=standard_name,
        colour_hex=standard_hex,
        is_active=True,
    )
    session.add(standard)
    return standard


def upsert_colour_standard_from_sku(
    session: Session,
    brand: str,
    colour_code: str,
    colour_name: str,
    colour_hex: str,
) -> BrandColourSwatchRule:
    """Persist an explicit SKU swatch and synchronise matching active SKUs."""
    standard = _upsert_persistent_colour_standard(
        session,
        brand,
        colour_code,
        colour_name,
        colour_hex,
    )
    candidates = _list_colour_rule_candidate_skus(
        session,
        standard.brand,
        standard.colour_code,
    )
    now = datetime.now(timezone.utc)
    for sku in candidates:
        sku.exterior_color_name = standard.colour_name
        sku.colour_hex = standard.colour_hex
        sku.updated_at_utc = now
    return standard


def _list_colour_rule_candidate_skus(
    session: Session,
    brand: str,
    colour_code: str,
    allowed_brands: set[str] | None = None,
) -> list[MaterialSkuMaster]:
    normalized_brand = normalize_brand(brand)
    normalized_code = colour_code.strip().upper()
    if not normalized_brand or not normalized_code:
        return []
    stmt = select(MaterialSkuMaster).where(
        MaterialSkuMaster.is_active == True,
        func.upper(MaterialSkuMaster.exterior_color_code) == normalized_code,
        *([func.upper(MaterialSkuMaster.brand).in_(allowed_brands)] if allowed_brands is not None else []),
    )
    rows = list(session.execute(stmt).scalars().all())
    return [
        row
        for row in rows
        if resolve_material_brand(
            getattr(row, "brand", None),
            getattr(row, "model_name", None),
            getattr(row, "bom_template", None),
        ) == normalized_brand
        and getattr(row, "is_active", True)
        and resolve_effective_lifecycle_status(row, date.today()) != "historical"
    ]


def _list_colour_rule_name_candidates(
    session: Session,
    brand: str,
    colour_name: str,
    allowed_brands: set[str] | None = None,
) -> list[dict]:
    """Return existing brand+code rules matching one normalized display alias."""
    normalized_brand = normalize_brand(brand)
    alias = normalize_colour_rule_alias(colour_name)
    if not normalized_brand or not alias:
        return []
    # Inspect the entire code group, not just matching-name rows: otherwise
    # a different name or HEX in that group could be hidden from conflict checks.
    rules = list_colour_hex_rules(session, allowed_brands=allowed_brands)
    candidates: list[dict] = []
    for rule in rules:
        if rule["brand"] != normalized_brand or not any(
            normalize_colour_rule_alias(option["colourName"]) == alias for option in rule["nameOptions"]
        ):
            continue
        candidates.append({
            "brand": rule["brand"],
            "colourCode": rule["colourCode"],
            "colourName": rule["standardColourName"],
            "colourHex": rule["standardColourHex"],
            "status": rule["status"],
            "hasNameConflict": rule["hasNameConflict"],
            "hasSwatchConflict": rule["hasSwatchConflict"],
        })
    return sorted(candidates, key=lambda item: item["colourCode"])


def list_colour_hex_rules(session: Session, allowed_brands: set[str] | None = None) -> list[dict]:
    """Return SKU-derived rules with durable standards taking precedence."""
    stmt = select(MaterialSkuMaster).where(MaterialSkuMaster.is_active == True)
    if allowed_brands is not None:
        stmt = stmt.where(func.upper(MaterialSkuMaster.brand).in_(allowed_brands))
    skus = list(session.execute(stmt).scalars().all())
    standards = list_persistent_colour_standard_map(session)
    if allowed_brands is not None:
        standards = {key: value for key, value in standards.items() if key[0] in allowed_brands}
    rules = build_colour_hex_rules_from_skus(skus, standards)
    by_key = {(rule["brand"], rule["colourCode"]): rule for rule in rules}
    for key, standard in standards.items():
        rule = by_key.get(key)
        if rule is None:
            rule = {
                "brand": key[0],
                "colourCode": key[1],
                "skuCount": 0,
                "sampleMaterialCodes": [],
                "placeholderNameSkuCount": 0,
                "missingSwatchSkuCount": 0,
                "colourName": standard.colour_name,
                "normalizedColourName": normalize_colour_rule_name(standard.colour_name),
                "status": "complete" if standard.colour_hex else "missing",
                "standardColourName": standard.colour_name,
                "standardColourHex": standard.colour_hex,
                "nameOptions": [{
                    "colourName": standard.colour_name,
                    "normalizedColourName": normalize_colour_rule_name(standard.colour_name),
                    "skuCount": 0,
                }],
                "hexOptions": [{"colourHex": standard.colour_hex, "skuCount": 0}] if standard.colour_hex else [],
                "hasNameConflict": False,
                "hasSwatchConflict": False,
                "fillableSkuCount": 0,
                "previewChanges": [],
            }
            rules.append(rule)
            by_key[key] = rule
    status_rank = dict(name_conflict=0, swatch_conflict=1, missing=2, fillable=3, complete=4)
    return sorted(
        rules,
        key=lambda item: (
            status_rank.get(item["status"], 9),
            item["brand"],
            item["colourCode"],
        ),
    )


def summarize_invalid_colour_rule_identities(
    session: Session,
    *,
    sample_limit: int = 5,
) -> dict[str, int | list[str]]:
    """Report active SKUs excluded from shared rules because identity is incomplete."""
    stmt = select(MaterialSkuMaster).where(MaterialSkuMaster.is_active == True)
    invalid_codes = []
    for sku in session.execute(stmt).scalars().all():
        brand = resolve_material_brand(
            getattr(sku, "brand", None),
            getattr(sku, "model_name", None),
            getattr(sku, "bom_template", None),
        )
        if _colour_rule_key(brand, getattr(sku, "exterior_color_code", None)) is None:
            material_code = clean_text(sku.material_code)
            if material_code:
                invalid_codes.append(material_code)
    invalid_codes.sort()
    return {
        "invalidIdentitySkuCount": len(invalid_codes),
        "invalidIdentitySampleMaterialCodes": invalid_codes[:max(0, sample_limit)],
    }


def summarize_colour_hex_rules(rules: list[dict]) -> dict[str, int]:
    counts = Counter(rule["status"] for rule in rules)
    return {
        "totalRules": len(rules),
        "fillable": counts["fillable"],
        "missing": counts["missing"],
        "nameConflict": sum(1 for rule in rules if rule["hasNameConflict"]),
        "swatchConflict": sum(1 for rule in rules if rule["hasSwatchConflict"]),
        "complete": counts["complete"],
        "fillableSkus": sum(
            int(rule["fillableSkuCount"])
            for rule in rules
            if rule["status"] == "fillable"
        ),
    }


def _build_colour_standard_preview(
    skus: list[MaterialSkuMaster],
    standards: dict[tuple[str, str], BrandColourSwatchRule],
) -> dict:
    """Plan only missing fields from unique same-code values; never guess or overwrite."""
    rules = build_colour_hex_rules_from_skus(skus, standards)

    planned_rules: list[dict] = []
    items: list[dict] = []
    unresolved_rule_count = 0
    unresolved_conflict_count = 0
    for rule in rules:
        key = (rule["brand"], rule["colourCode"])
        if rule["hasNameConflict"] or rule["hasSwatchConflict"]:
            unresolved_conflict_count += 1
            continue
        if not rule["standardColourName"]:
            unresolved_rule_count += 1
            continue
        if key in standards and not rule["previewChanges"]:
            continue
        rule_plan = {
            "brand": key[0],
            "colourCode": key[1],
            "colourName": rule["standardColourName"],
            "colourHex": rule["standardColourHex"],
            "source": "persistent_rule" if key in standards else "existing_sku",
            "skuCount": int(rule["skuCount"]),
            "hasNameConflict": bool(rule["hasNameConflict"]),
            "hasSwatchConflict": bool(rule["hasSwatchConflict"]),
            "nameOptions": rule["nameOptions"],
        }
        planned_rules.append(rule_plan)
        items.extend(rule["previewChanges"])
    planned_rules.sort(key=lambda item: (item["brand"], item["colourCode"]))
    items.sort(key=lambda item: item["materialCode"])
    return {
        "rules": planned_rules,
        "items": items,
        "unresolvedRuleCount": unresolved_rule_count,
        "unresolvedConflictCount": unresolved_conflict_count,
    }


def _colour_fill_fingerprint(rules: list[dict], items: list[dict]) -> str:
    payload = json.dumps(
        {"rules": rules, "items": items},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def preview_colour_rule_fills(session: Session, allowed_brands: set[str] | None = None) -> dict:
    """Preview brand+code standards and SKU synchronization without writing."""
    skus = list(
        session.execute(
            select(MaterialSkuMaster).where(MaterialSkuMaster.is_active == True,
                   *([func.upper(MaterialSkuMaster.brand).in_(allowed_brands)] if allowed_brands is not None else []))
        ).scalars().all()
    )
    plan = _build_colour_standard_preview(
        skus,
        list_persistent_colour_standard_map(session),
    )
    return {
        **plan,
        "total": len(plan["items"]),
        "ruleCount": len(plan["rules"]),
        "generatedRuleCount": 0,
        "fingerprint": _colour_fill_fingerprint(plan["rules"], plan["items"]),
    }


def lookup_colour_rule(
    session: Session,
    brand: str,
    colour_code: str,
    *,
    colour_name: str | None = None,
    allowed_brands: set[str] | None = None,
) -> dict:
    """Resolve a shared colour rule without mutating any SKU.

    Brand+code is authoritative. Same-name other-code saved swatches are only
    suggestions: the caller must explicitly adopt their HEX before saving.
    Never generate or approximate a swatch from a colour name.
    """
    normalized_brand = normalize_brand(brand)
    normalized_code = str(colour_code or "").strip().upper()
    if not normalized_brand and not str(colour_name or "").strip():
        raise ValueError("brand and colourCode or colourName are required")
    if normalized_code:
        persistent = list_persistent_colour_standard_map(session).get(
            (normalized_brand, normalized_code)
        )
        candidates = _list_colour_rule_candidate_skus(session, normalized_brand, normalized_code, allowed_brands)
        rules = build_colour_hex_rules_from_skus(
            candidates, {(normalized_brand, normalized_code): persistent} if persistent is not None else None,
        )
        if rules and (rules[0]["hasNameConflict"] or rules[0]["hasSwatchConflict"]):
            rule = rules[0]
            return {
                "brand": normalized_brand, "colourCode": normalized_code,
                "status": rule["status"], "colourName": rule["standardColourName"],
                "colourHex": rule["standardColourHex"], "source": "brand_code_rule",
                "hasNameConflict": rule["hasNameConflict"],
                "hasSwatchConflict": rule["hasSwatchConflict"], "nameCandidates": [],
            }
        if persistent is not None:
            resolved_hex = rules[0]["standardColourHex"] if rules else persistent.colour_hex
            return {
                "brand": normalized_brand,
                "colourCode": normalized_code,
                "status": "complete" if resolved_hex else "missing",
                "colourName": persistent.colour_name,
                "colourHex": resolved_hex,
                "source": "persistent_rule",
                "hasNameConflict": False,
                "hasSwatchConflict": False,
                "nameCandidates": [],
            }
        if rules:
            rule = rules[0]
            reusable = bool(rule["standardColourName"])
            if reusable:
                return {
                    "brand": normalized_brand,
                    "colourCode": normalized_code,
                    "status": rule["status"],
                    "colourName": rule["standardColourName"],
                    "colourHex": rule["standardColourHex"],
                    "source": "brand_code_rule",
                    "hasNameConflict": rule["hasNameConflict"],
                    "hasSwatchConflict": rule["hasSwatchConflict"],
                    "nameCandidates": [],
                }

    name_candidates = _list_colour_rule_name_candidates(
        session,
        normalized_brand,
        str(colour_name or ""),
        allowed_brands,
    )
    reusable_candidates = [
        candidate
        for candidate in name_candidates
        if candidate["status"] in {"fillable", "complete"}
        and not candidate["hasNameConflict"]
        and not candidate["hasSwatchConflict"]
    ]
    return {
        "brand": normalized_brand,
        "colourCode": normalized_code,
        "status": "missing",
        "colourName": None,
        "colourHex": None,
        "source": "name_candidates" if reusable_candidates else "none",
        "hasNameConflict": False,
        "hasSwatchConflict": False,
        "nameCandidates": [candidate for candidate in reusable_candidates if candidate["colourHex"]],
    }


def resolve_colour_attributes(
    session: Session,
    brand: str,
    colour_code: str,
    *,
    colour_name: str | None = None,
    colour_hex: str | None = None,
    colour_hex_supplied: bool = False,
) -> dict:
    """Fill missing/placeholder attributes from one unambiguous brand+code rule."""
    explicit_name = str(colour_name or "").strip()
    explicit_hex = normalize_colour_hex_value(colour_hex)
    rule = lookup_colour_rule(
        session,
        brand,
        colour_code,
        colour_name=explicit_name,
    )
    reusable = rule["source"] in {
        "brand_code_rule",
        "persistent_rule",
    } and not rule["hasNameConflict"] and not rule["hasSwatchConflict"]
    resolved_name = explicit_name
    if is_placeholder_colour_name(explicit_name, colour_code) and reusable:
        resolved_name = rule["colourName"]
    resolved_hex = (
        explicit_hex
        if colour_hex_supplied
        else (explicit_hex or (rule["colourHex"] if reusable else None))
    )
    return {
        **rule,
        "colourName": resolved_name or None,
        "colourHex": resolved_hex,
    }


def apply_colour_rule_fills(
    session: Session,
    material_codes: list[str],
    preview_fingerprint: str,
    allowed_brands: set[str] | None = None,
) -> dict:
    """Apply only currently deterministic preview changes in the caller transaction."""
    skus = list(
        session.execute(
            select(MaterialSkuMaster)
            .where(MaterialSkuMaster.is_active == True,
                   *([func.upper(MaterialSkuMaster.brand).in_(allowed_brands)] if allowed_brands is not None else []))
            .with_for_update()
        ).scalars().all()
    )
    standards = list_persistent_colour_standard_map(session)
    plan = _build_colour_standard_preview(skus, standards)
    preview_items = plan["items"]
    requested = {
        str(code or "").strip().upper()
        for code in material_codes
        if str(code or "").strip()
    }
    current = {item["materialCode"].upper() for item in preview_items}
    current_fingerprint = _colour_fill_fingerprint(plan["rules"], preview_items)
    if requested != current or preview_fingerprint != current_fingerprint:
        raise ValueError(
            "Colour rule preview is stale; refresh preview before applying"
        )
    candidates = {sku.material_code: sku for sku in skus}
    now = datetime.now(timezone.utc)
    for rule in plan["rules"]:
        _upsert_persistent_colour_standard(
            session,
            rule["brand"],
            rule["colourCode"],
            rule["colourName"],
            rule["colourHex"],
        )
    applied: list[dict] = []
    for item in preview_items:
        sku = candidates.get(item["materialCode"])
        if sku is None:
            continue
        sku.exterior_color_name = item["newColourName"]
        sku.colour_hex = item["newColourHex"]
        sku.row_version = int(getattr(sku, "row_version", 1) or 1) + 1
        sku.updated_at_utc = now
        applied.append(item)
    applied.sort(key=lambda item: item["materialCode"])
    planned_sku_count = sum(int(rule["skuCount"]) for rule in plan["rules"])
    return {
        "updated": len(applied),
        "unchanged": max(0, planned_sku_count - len(applied)),
        "rulesCreated": sum((rule["brand"], rule["colourCode"]) not in standards for rule in plan["rules"]),
        "generatedRules": 0,
        "conflicts": int(plan["unresolvedConflictCount"]),
        "missingRules": int(plan["unresolvedRuleCount"]),
        "materialCodes": [item["materialCode"] for item in applied],
        "items": applied,
        "fingerprint": current_fingerprint,
    }


def set_standard_colour_hex_for_rule(
    session: Session,
    brand: str,
    colour_code: str,
    colour_name: str,
    colour_hex: str | None = None,
) -> dict:
    """Confirm a shared name; an omitted swatch preserves every existing HEX."""
    key = _colour_rule_key(brand, colour_code)
    if key is None:
        raise ValueError("brand and colourCode are required")
    standard_colour_name = str(colour_name or "").strip()
    if is_placeholder_colour_name(standard_colour_name, colour_code):
        raise ValueError("A non-placeholder colourName is required")
    normalized_brand, normalized_code = key
    candidates = _list_colour_rule_candidate_skus(
        session,
        normalized_brand,
        normalized_code,
    )
    standard = _upsert_persistent_colour_standard(
        session,
        normalized_brand,
        normalized_code,
        standard_colour_name,
        colour_hex,
    )
    updated_codes: list[str] = []
    now = datetime.now(timezone.utc)
    for sku in candidates:
        new_hex = standard.colour_hex if colour_hex is not None else sku.colour_hex
        if sku.exterior_color_name == standard_colour_name and sku.colour_hex == new_hex:
            continue
        sku.exterior_color_name = standard_colour_name
        sku.colour_hex = new_hex
        sku.updated_at_utc = now
        sku.row_version = int(getattr(sku, "row_version", 0) or 0) + 1
        updated_codes.append(sku.material_code)
    return {
        "brand": normalized_brand,
        "colourCode": normalized_code,
        "colourName": standard_colour_name,
        "normalizedColourName": normalize_colour_rule_name(standard_colour_name),
        "colourHex": standard.colour_hex,
        "updated": len(updated_codes),
        "materialCodes": updated_codes,
        "source": "persistent_rule",
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


def require_unreferenced_material_codes(session: Session, material_codes: list[str]) -> None:
    """Only unreferenced entry mistakes may be renumbered in place."""
    if not material_codes:
        return
    referenced = session.execute(select(or_(
        select(PiOrderLine.pi_line_id).where(PiOrderLine.material_code.in_(material_codes)).exists(),
        select(PiOrderLineAllocation.pi_line_allocation_id).where(PiOrderLineAllocation.material_code.in_(material_codes)).exists(),
        select(PiVehicleUnit.vehicle_unit_id).where(PiVehicleUnit.material_code.in_(material_codes)).exists(),
    ))).scalar_one_or_none()
    if referenced:
        raise ValueError("Referenced by a PI: add/copy a new material version and archive the original. / 已有 PI 引用，请新增或复制新版物料并归档旧版，保留原订单及数量记录。")


def update_sku_material_code(
    session: Session,
    old_material_code: str,
    new_material_code: str,
) -> bool:
    """Correct an unreferenced SKU and its selection quantity/finance keys."""
    old_code = clean_text(old_material_code).upper()
    new_code = clean_text(new_material_code).upper()
    if not old_code or not new_code:
        raise ValueError("material code is required")
    if old_code == new_code:
        return get_sku_by_material_code_any_status(session, old_code) is not None
    require_unreferenced_material_codes(session, [old_code])

    conflict = session.execute(
        select(MaterialSkuMaster.material_code).where(
            MaterialSkuMaster.material_code == new_code,
        )
    ).scalar_one_or_none()
    if conflict:
        raise ValueError(f"Material code already exists: {new_code}")

    bind = session.get_bind()
    inspector = inspect(bind)
    has_material_lifecycle = inspector.has_table("material_lifecycle", schema="ordering")

    stmt = (
        update(MaterialSkuMaster)
        .where(MaterialSkuMaster.material_code == old_code)
        .values(material_code=new_code)
        .execution_options(synchronize_session="fetch")
    )
    result = session.execute(stmt)
    if not result.rowcount:
        return False

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
    return True


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
    require_unreferenced_material_codes(session, [
        sku.material_code for sku in ordered_skus
        if mapping[sku.material_code] != sku.material_code
        or clean_text(sku.bom_template).upper() != normalized_template
    ])
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
        session.execute(
            update(MaterialSkuMaster)
            .where(MaterialSkuMaster.material_code == old_code)
            .values(material_code=new_code, bom_template=normalized_template)
            .execution_options(synchronize_session="fetch")
        )

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
        .where(
            MaterialSkuMaster.material_code == material_code,
            MaterialSkuMaster.is_active == True,
        )
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
    country_codes += session.execute(select(CountryTemplateFobPeriod.country_code).where(
        CountryTemplateFobPeriod.status == "active",
    ).distinct()).scalars().all()
    return sorted({str(code or "").upper() for code in country_codes if str(code or "").strip()})


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


def _normalize_special_colour_code(colour_code: str | None) -> str:
    return clean_text(colour_code).upper()


def list_special_colour_surcharges(
    session: Session,
) -> list[SpecialColourSurchargeRule]:
    stmt = (
        select(SpecialColourSurchargeRule)
        .where(SpecialColourSurchargeRule.is_active == True)
        .order_by(
            SpecialColourSurchargeRule.brand,
            SpecialColourSurchargeRule.model_name,
            SpecialColourSurchargeRule.colour_code,
        )
    )
    return list(session.execute(stmt).scalars().all())


def get_special_colour_surcharge_for_sku(
    session: Session,
    sku: MaterialSkuMaster,
    colour_tier: str = "special",
) -> SpecialColourSurchargeRule | None:
    """Return the most specific override for one SKU and saved BOM tier."""
    normalized_brand = resolve_material_brand(
        getattr(sku, "brand", None),
        getattr(sku, "model_name", None),
        getattr(sku, "bom_template", None),
    )
    normalized_model = normalize_brand_text(getattr(sku, "model_name", None))
    normalized_code = _normalize_special_colour_code(
        getattr(sku, "exterior_color_code", None)
    )
    normalized_tier = clean_text(colour_tier).lower()
    if normalized_tier not in {"dual", "special"}:
        return None
    if not normalized_brand or not normalized_code:
        return None

    exact = session.execute(
        select(SpecialColourSurchargeRule).where(
            SpecialColourSurchargeRule.brand == normalized_brand,
            SpecialColourSurchargeRule.model_name == normalized_model,
            SpecialColourSurchargeRule.colour_code == normalized_code,
            SpecialColourSurchargeRule.colour_tier == normalized_tier,
            SpecialColourSurchargeRule.is_active == True,
        )
    ).scalars().first()
    if exact is not None:
        return exact

    return session.execute(
        select(SpecialColourSurchargeRule).where(
            SpecialColourSurchargeRule.brand == normalized_brand,
            SpecialColourSurchargeRule.model_name.is_(None),
            SpecialColourSurchargeRule.colour_code == normalized_code,
            SpecialColourSurchargeRule.colour_tier == normalized_tier,
            SpecialColourSurchargeRule.is_active == True,
        )
    ).scalars().first()


def upsert_special_colour_surcharge(
    session: Session,
    brand: str,
    colour_code: str,
    surcharge_eur: float,
    *,
    model_name: str | None = None,
    colour_name: str | None = None,
    colour_tier: str = "special",
) -> SpecialColourSurchargeRule:
    normalized_brand = normalize_brand(brand)
    normalized_model = normalize_brand_text(model_name) if model_name else None
    normalized_code = _normalize_special_colour_code(colour_code)
    normalized_name = clean_text(colour_name) or None
    normalized_tier = clean_text(colour_tier).lower()
    if not normalized_brand:
        raise ValueError("brand is required")
    if not normalized_code:
        raise ValueError("colourCode is required")
    if normalized_tier not in {"dual", "special"}:
        raise ValueError("colourTier must be dual or special")
    if surcharge_eur < 0:
        raise ValueError("surchargeEur must be greater than or equal to 0")

    existing = session.execute(
        select(SpecialColourSurchargeRule).where(
            SpecialColourSurchargeRule.brand == normalized_brand,
            SpecialColourSurchargeRule.model_name == normalized_model,
            SpecialColourSurchargeRule.colour_code == normalized_code,
            SpecialColourSurchargeRule.colour_tier == normalized_tier,
            SpecialColourSurchargeRule.is_active == True,
        )
    ).scalars().first()
    if existing:
        existing.colour_name = normalized_name or existing.colour_name
        existing.surcharge_eur = surcharge_eur
        existing.updated_at_utc = datetime.now(timezone.utc)
        return existing

    rule = SpecialColourSurchargeRule(
        special_colour_surcharge_rule_id=uuid4(),
        brand=normalized_brand,
        model_name=normalized_model,
        colour_code=normalized_code,
        colour_tier=normalized_tier,
        colour_name=normalized_name,
        surcharge_eur=surcharge_eur,
        is_active=True,
    )
    session.add(rule)
    return rule


def resolve_colour_surcharge_for_sku(
    session: Session,
    sku: MaterialSkuMaster,
    colour_tier: str | None,
) -> dict[str, object]:
    """Return the surcharge amount and whether it is explicitly configured."""
    normalized_tier = clean_text(colour_tier).lower()
    if normalized_tier == "single":
        return {"status": "single", "amount": 0.0, "source": "single"}
    if normalized_tier not in {"dual", "special"}:
        return {"status": "missing_tier", "amount": None, "source": None}

    special_rule = get_special_colour_surcharge_for_sku(
        session, sku, normalized_tier
    )
    special_amount = getattr(special_rule, "surcharge_eur", None)
    if special_amount is not None:
        amount = float(special_amount)
        return {
            "status": "explicit_zero" if amount == 0 else "matched_amount",
            "amount": amount,
            "source": "model_colour" if getattr(special_rule, "model_name", None) else "brand_colour",
        }

    brand = resolve_material_brand(
        getattr(sku, "brand", None),
        getattr(sku, "model_name", None),
        getattr(sku, "bom_template", None),
    )
    rule = get_brand_colour_surcharge(
        session,
        brand,
        normalized_tier,
    )
    brand_amount = getattr(rule, "surcharge_eur", None)
    if brand_amount is None:
        return {"status": "missing_rule", "amount": None, "source": None}
    amount = float(brand_amount)
    return {
        "status": "explicit_zero" if amount == 0 else "matched_amount",
        "amount": amount,
        "source": "brand_tier",
    }


def _positive_float(value: object) -> float | None:
    if value is None:
        return None
    parsed = float(value)
    return parsed if parsed > 0 else None


def _find_colour_surcharge_base_resolution(
    session: Session,
    sku: MaterialSkuMaster,
    country_code: str,
    payment_term_code: str | None = None,
) -> dict[str, object]:
    """Resolve one exact Single base without treating a derived colour as source.

    A colour price is only auto-repairable when the same BOM template and
    country have one unambiguous active Single FOB. Payment term is reference
    metadata only. Rows without a template stay scoped to their material code.
    """
    template = clean_text(getattr(sku, "bom_template", None)).upper()
    single_tier = func.lower(func.trim(MaterialSkuMaster.colour_tier)) == "single"
    stmt = (
        select(CountrySkuFobResolved.final_fob_eur)
        .select_from(CountrySkuFobResolved)
        .join(
            MaterialSkuMaster,
            MaterialSkuMaster.material_code == CountrySkuFobResolved.material_code,
        )
        .where(
            CountrySkuFobResolved.country_code == country_code,
            CountrySkuFobResolved.is_active == True,
            CountrySkuFobResolved.final_fob_eur > 0,
            MaterialSkuMaster.is_active == True,
            single_tier,
        )
    )
    if template:
        stmt = stmt.where(MaterialSkuMaster.bom_template == template)
    else:
        stmt = stmt.where(MaterialSkuMaster.material_code == sku.material_code)
    values = sorted(
        {
            round(float(value), 2)
            for value in session.execute(stmt).scalars().all()
            if _positive_float(value) is not None
        }
    )
    if not values:
        return {"status": "missing", "baseFobEur": None, "candidates": []}
    if len(values) > 1:
        return {"status": "ambiguous", "baseFobEur": None, "candidates": values}
    return {"status": "resolved", "baseFobEur": values[0], "candidates": values}


def _infer_existing_colour_surcharge_base_fob(
    fob: CountrySkuFobResolved,
) -> float | None:
    base = _positive_float(fob.base_fob_eur)
    if base is not None:
        return base
    surcharge = _positive_float(fob.colour_surcharge_eur) or 0.0
    if surcharge > 0:
        inferred = round(float(fob.final_fob_eur) - surcharge, 2)
        return inferred if inferred > 0 else None
    return None


def _resolve_colour_surcharge_reprice_base(
    session: Session,
    sku: MaterialSkuMaster,
    row: CountrySkuFobResolved,
) -> dict[str, object]:
    """Resolve the base shared by audit and write paths.

    Payment terms are reference metadata only.  A conflicting Single candidate
    remains ambiguous; an older row-local value cannot choose one. If no
    Single exists, all stored template bases must agree and an explicit
    template edit must establish that base.
    """
    resolution = _find_colour_surcharge_base_resolution(
        session, sku, row.country_code, None
    )
    if resolution["status"] in {"resolved", "ambiguous"}:
        return resolution
    template = clean_text(getattr(sku, "bom_template", None)).upper()
    if not template:
        return resolution
    # A row-local legacy manual value cannot prove a template base. Inspect
    # every stored base, and require evidence of an explicit template edit.
    stored = session.execute(
        select(CountrySkuFobResolved.base_fob_eur, CountrySkuFobResolved.fob_source_mode)
        .join(MaterialSkuMaster, MaterialSkuMaster.material_code == CountrySkuFobResolved.material_code)
        .where(
            MaterialSkuMaster.bom_template == template,
            MaterialSkuMaster.is_active == True,
            CountrySkuFobResolved.country_code == row.country_code,
            CountrySkuFobResolved.is_active == True,
            CountrySkuFobResolved.base_fob_eur > 0,
        )
    ).all()
    values = sorted({round(float(base), 2) for base, _source in stored})
    if len(values) > 1:
        return {"status": "ambiguous", "baseFobEur": None, "candidates": values}
    if values and any(source in {"template_base", "template_base_country_adjust"} for _base, source in stored):
        return {"status": "stored", "baseFobEur": values[0], "candidates": values}
    return resolution


def reprice_sku_colour_surcharge_fobs(
    session: Session,
    material_code: str,
    *,
    country_code: str | None = None,
    changed_by: str | None = None,
    allowed_countries: set[str] | None = None,
) -> dict[str, object]:
    """Recalculate derived FOB rows after a SKU colour tier changes.

    BOM Admin manual edits are country-base edits, not final-colour locks.  A
    manual row with a trusted base (stored on the row or found on the matching
    Single SKU) therefore follows the same surcharge calculation as an
    automatic row.  A row without any trusted base remains protected.  Source
    mode is provenance, not a permanent colour-price lock: an imported final
    row is recalculated when the matching Single base is unambiguous.
    """
    sku = get_sku_by_material_code(session, material_code)
    if sku is None:
        return {
            "materialCode": material_code,
            "rows": 0,
            "updated": 0,
            "unchanged": 0,
            "skippedManual": 0,
            "skippedNoBase": 0,
            "skippedAmbiguous": 0,
            "skippedMissingTier": 0,
            "skippedMissingRule": 0,
            "details": [],
        }

    colour_tier = resolve_effective_colour_tier(sku)
    surcharge_decision = resolve_colour_surcharge_for_sku(session, sku, colour_tier)

    rows = list(
        session.execute(
            select(CountrySkuFobResolved).where(
                CountrySkuFobResolved.material_code == sku.material_code,
                CountrySkuFobResolved.is_active == True,
                CountrySkuFobResolved.final_fob_eur > 0,
                *([CountrySkuFobResolved.country_code.in_(allowed_countries)] if allowed_countries is not None else []),
                *(
                    [CountrySkuFobResolved.country_code == country_code]
                    if country_code
                    else []
                ),
            )
        ).scalars().all()
    )
    updated = 0
    unchanged = 0
    skipped_manual = 0
    skipped_no_base = 0
    skipped_ambiguous = 0
    skipped_missing_tier = 0
    skipped_missing_rule = 0
    details: list[dict] = []
    now = datetime.now(timezone.utc)

    if surcharge_decision["status"] in {"missing_tier", "missing_rule"}:
        reason = (
            "missing_colour_tier"
            if surcharge_decision["status"] == "missing_tier"
            else "missing_colour_surcharge_rule"
        )
        if surcharge_decision["status"] == "missing_tier":
            skipped_missing_tier = len(rows)
        else:
            skipped_missing_rule = len(rows)
        details = [
            {
                "countryCode": row.country_code,
                "oldFinalFobEur": float(row.final_fob_eur),
                "newFinalFobEur": float(row.final_fob_eur),
                "colourSurchargeEur": row.colour_surcharge_eur,
                "status": "skipped",
                "reason": reason,
            }
            for row in rows
        ]
        return {
            "materialCode": sku.material_code,
            "brand": resolve_material_brand(
                getattr(sku, "brand", None),
                getattr(sku, "model_name", None),
                getattr(sku, "bom_template", None),
            ),
            "colourCode": clean_text(sku.exterior_color_code).upper(),
            "colourTier": colour_tier,
            "surchargeEur": None,
            "rows": len(rows),
            "updated": 0,
            "unchanged": 0,
            "skippedManual": 0,
            "skippedNoBase": 0,
            "skippedAmbiguous": 0,
            "skippedMissingTier": skipped_missing_tier,
            "skippedMissingRule": skipped_missing_rule,
            "details": details,
        }
    surcharge = float(surcharge_decision["amount"] or 0.0)

    for row in rows:
        old_final = float(row.final_fob_eur)
        is_manual_base_edit = row.fob_source_mode in COLOUR_SURCHARGE_MANUAL_SOURCE_MODES

        if colour_tier == "single":
            base_fob = _infer_existing_colour_surcharge_base_fob(row) or float(row.final_fob_eur)
            new_surcharge = None
        else:
            resolution = _resolve_colour_surcharge_reprice_base(session, sku, row)
            base_fob = resolution["baseFobEur"]
            if base_fob is None:
                if resolution["status"] == "ambiguous":
                    skipped_ambiguous += 1
                    reason = "ambiguous_single_base"
                elif is_manual_base_edit:
                    skipped_manual += 1
                    reason = "manual_fob"
                else:
                    skipped_no_base += 1
                    reason = "missing_single_base"
                details.append({
                    "countryCode": row.country_code,
                    "oldFinalFobEur": old_final,
                    "newFinalFobEur": old_final,
                    "colourSurchargeEur": (
                        float(row.colour_surcharge_eur)
                        if row.colour_surcharge_eur is not None
                        else None
                    ),
                    "status": "skipped",
                    "reason": reason,
                })
                continue
            new_surcharge = surcharge if surcharge > 0 else None

        new_final = round(base_fob + (new_surcharge or 0.0), 2)
        final_changed = round(old_final, 2) != new_final
        derived_source_mode = row.fob_source_mode
        if is_manual_base_edit:
            derived_source_mode = (
                "template_base_country_adjust"
                if row.fob_source_mode == "manual_country_adjust"
                else "template_base"
            )
        meta_changed = (
            row.base_fob_eur != base_fob
            or row.colour_surcharge_eur != new_surcharge
            or row.uploaded_fob_eur != base_fob
            or (is_manual_base_edit and row.fob_source_mode != derived_source_mode)
            or (is_manual_base_edit and row.fob_source_country_code is not None)
        )
        if not final_changed and not meta_changed:
            unchanged += 1
            details.append({
                "countryCode": row.country_code,
                "oldFinalFobEur": old_final,
                "newFinalFobEur": new_final,
                "colourSurchargeEur": new_surcharge,
                "status": "unchanged",
                "reason": None,
            })
            continue

        if final_changed:
            session.add(
                FobResolvedHistory(
                    country_sku_fob_id=row.country_sku_fob_id,
                    baseline_version_id=row.baseline_version_id,
                    country_code=row.country_code,
                    material_code=row.material_code,
                    payment_term_code=row.payment_term_code,
                    old_uploaded_fob_eur=row.uploaded_fob_eur,
                    new_uploaded_fob_eur=base_fob,
                    old_final_fob_eur=row.final_fob_eur,
                    new_final_fob_eur=new_final,
                    changed_by=changed_by or "colour_surcharge_reprice",
                )
            )
        row.base_fob_eur = base_fob
        row.colour_surcharge_eur = new_surcharge
        row.uploaded_fob_eur = base_fob
        if is_manual_base_edit:
            row.fob_source_country_code = None
            row.fob_source_mode = derived_source_mode
        row.final_fob_eur = new_final
        row.updated_at_utc = now
        updated += 1
        details.append({
            "countryCode": row.country_code,
            "oldFinalFobEur": old_final,
            "newFinalFobEur": new_final,
            "colourSurchargeEur": new_surcharge,
            "status": "updated",
            "reason": None,
        })

    return {
        "materialCode": sku.material_code,
        "brand": resolve_material_brand(
            getattr(sku, "brand", None),
            getattr(sku, "model_name", None),
            getattr(sku, "bom_template", None),
        ),
        "colourCode": clean_text(sku.exterior_color_code).upper(),
        "colourTier": colour_tier,
        "surchargeEur": surcharge,
        "rows": len(rows),
        "updated": updated,
        "unchanged": unchanged,
        "skippedManual": skipped_manual,
        "skippedNoBase": skipped_no_base,
        "skippedAmbiguous": skipped_ambiguous,
        "skippedMissingTier": skipped_missing_tier,
        "skippedMissingRule": skipped_missing_rule,
        "details": details,
    }


def reprice_brand_colour_surcharge_fobs(
    session: Session,
    brand: str,
    colour_tier: str,
    *,
    changed_by: str | None = None,
    allowed_countries: set[str] | None = None,
    allowed_brands: set[str] | None = None,
) -> dict[str, int | str]:
    """Recalculate all active SKUs affected by one brand/tier surcharge rule."""
    normalized_brand = normalize_brand(brand)
    normalized_tier = clean_text(colour_tier).lower()
    candidates = list(
        session.execute(
            select(MaterialSkuMaster)
            .where(
                MaterialSkuMaster.is_active == True,
                func.lower(func.trim(MaterialSkuMaster.colour_tier)) == normalized_tier,
                *([func.upper(MaterialSkuMaster.brand).in_(allowed_brands)] if allowed_brands is not None else []),
            )
            .order_by(MaterialSkuMaster.material_code)
        ).scalars().all()
    )
    material_codes = [
        sku.material_code
        for sku in candidates
        if resolve_material_brand(
            getattr(sku, "brand", None),
            getattr(sku, "model_name", None),
            getattr(sku, "bom_template", None),
        ) == normalized_brand
    ]
    totals = {
        "brand": normalized_brand,
        "colourTier": normalized_tier,
        "skus": len(material_codes),
        "rows": 0,
        "updated": 0,
        "unchanged": 0,
        "skippedManual": 0,
        "skippedNoBase": 0,
        "skippedAmbiguous": 0,
        "skippedMissingTier": 0,
        "skippedMissingRule": 0,
    }
    for code in material_codes:
        result = reprice_sku_colour_surcharge_fobs(
            session,
            code,
            allowed_countries=allowed_countries,
            changed_by=changed_by or "colour_surcharge_rule_update",
        )
        for key in (
            "rows",
            "updated",
            "unchanged",
            "skippedManual",
            "skippedNoBase",
            "skippedAmbiguous",
            "skippedMissingTier",
            "skippedMissingRule",
        ):
            totals[key] = int(totals[key]) + int(result[key])
    return totals


def reprice_special_colour_surcharge_fobs(
    session: Session,
    brand: str,
    colour_code: str,
    *,
    model_name: str | None = None,
    colour_tier: str = "special",
    changed_by: str | None = None,
    allowed_countries: set[str] | None = None,
    allowed_brands: set[str] | None = None,
) -> dict[str, int | str]:
    """Recalculate active SKUs affected by one tier-qualified override."""
    normalized_brand = normalize_brand(brand)
    normalized_model = normalize_brand_text(model_name) if model_name else None
    normalized_code = _normalize_special_colour_code(colour_code)
    normalized_tier = clean_text(colour_tier).lower()
    if normalized_tier not in {"dual", "special"}:
        raise ValueError("colourTier must be dual or special")
    stmt = select(MaterialSkuMaster).where(
        MaterialSkuMaster.is_active == True,
        func.lower(func.trim(MaterialSkuMaster.colour_tier)) == normalized_tier,
        func.upper(MaterialSkuMaster.exterior_color_code) == normalized_code,
        *([func.upper(MaterialSkuMaster.brand).in_(allowed_brands)] if allowed_brands is not None else []),
    )
    if normalized_model:
        stmt = stmt.where(MaterialSkuMaster.model_name == normalized_model)
    candidates = list(
        session.execute(stmt.order_by(MaterialSkuMaster.material_code)).scalars().all()
    )
    material_codes = [
        sku.material_code
        for sku in candidates
        if resolve_material_brand(
            getattr(sku, "brand", None),
            getattr(sku, "model_name", None),
            getattr(sku, "bom_template", None),
        ) == normalized_brand
        and (not normalized_model or normalize_brand_text(getattr(sku, "model_name", None)) == normalized_model)
    ]
    totals = {
        "brand": normalized_brand,
        "modelName": normalized_model or "",
        "colourCode": normalized_code,
        "colourTier": normalized_tier,
        "skus": len(material_codes),
        "rows": 0,
        "updated": 0,
        "unchanged": 0,
        "skippedManual": 0,
        "skippedNoBase": 0,
        "skippedAmbiguous": 0,
        "skippedMissingTier": 0,
        "skippedMissingRule": 0,
    }
    for code in material_codes:
        result = reprice_sku_colour_surcharge_fobs(
            session,
            code,
            allowed_countries=allowed_countries,
            changed_by=changed_by or "special_colour_surcharge_update",
        )
        for key in (
            "rows",
            "updated",
            "unchanged",
            "skippedManual",
            "skippedNoBase",
            "skippedAmbiguous",
            "skippedMissingTier",
            "skippedMissingRule",
        ):
            totals[key] = int(totals[key]) + int(result[key])
    return totals


def _colour_surcharge_reprice_item(
    session: Session,
    sku: MaterialSkuMaster,
    row: CountrySkuFobResolved,
) -> dict[str, object]:
    """Build one auditable derived-colour price decision without writing."""
    tier = resolve_effective_colour_tier(sku)
    brand = resolve_material_brand(
        getattr(sku, "brand", None),
        getattr(sku, "model_name", None),
        getattr(sku, "bom_template", None),
    )
    item: dict[str, object] = {
        "materialCode": sku.material_code,
        "brand": brand,
        "modelName": getattr(sku, "model_name", None),
        "version": getattr(sku, "version", None),
        "bomTemplate": getattr(sku, "bom_template", None),
        "colourCode": clean_text(getattr(sku, "exterior_color_code", None)).upper(),
        "colourName": getattr(sku, "exterior_color_name", None),
        "colourTier": tier,
        "countryCode": row.country_code,
        "paymentTermCode": row.payment_term_code,
        "currentFinalFobEur": round(float(row.final_fob_eur), 2),
        "currentBaseFobEur": (
            round(float(row.base_fob_eur), 2)
            if row.base_fob_eur is not None
            else None
        ),
        "currentColourSurchargeEur": (
            round(float(row.colour_surcharge_eur), 2)
            if row.colour_surcharge_eur is not None
            else None
        ),
        "currentSourceMode": row.fob_source_mode,
        "currentUploadedFobEur": float(row.uploaded_fob_eur) if getattr(row, "uploaded_fob_eur", None) is not None else None,
        "category": "already_correct",
        "reason": None,
        "trustedSingleBaseFobEur": None,
        "surchargeEur": None,
        "expectedFinalFobEur": None,
    }
    if tier is None:
        item["category"] = "missing_tier"
        item["reason"] = "colour_tier_not_configured"
        return item
    if tier == "single":
        item["category"] = "not_applicable"
        item["reason"] = "single_colour"
        return item
    surcharge_decision = resolve_colour_surcharge_for_sku(session, sku, tier)
    item["surchargeRuleStatus"] = surcharge_decision["status"]
    item["surchargeRuleSource"] = surcharge_decision["source"]
    if surcharge_decision["status"] == "missing_rule":
        item["category"] = "missing_rule"
        item["reason"] = "no_colour_surcharge_rule_for_brand_and_tier"
        return item
    resolution = _resolve_colour_surcharge_reprice_base(session, sku, row)
    item["singleBaseCandidates"] = resolution["candidates"]
    if resolution["status"] == "ambiguous":
        item["category"] = "ambiguous_base"
        item["reason"] = "multiple_single_bases_for_template_country"
        return item
    if resolution["status"] not in {"resolved", "stored"}:
        if row.fob_source_mode in COLOUR_SURCHARGE_EXPLICIT_FINAL_SOURCE_MODES:
            item["category"] = "explicit_final"
            item["reason"] = "explicit_final_without_single_base"
        else:
            item["category"] = "missing_base"
            item["reason"] = "no_single_base_for_template_country"
        return item

    base_fob = float(resolution["baseFobEur"])
    surcharge = float(surcharge_decision["amount"] or 0.0)
    expected = round(base_fob + (surcharge or 0.0), 2)
    expected_surcharge = round(surcharge, 2) if surcharge > 0 else None
    item["trustedSingleBaseFobEur"] = round(base_fob, 2)
    item["surchargeEur"] = expected_surcharge
    item["expectedFinalFobEur"] = expected
    metadata_matches = (
        row.base_fob_eur is not None
        and round(float(row.base_fob_eur), 2) == round(base_fob, 2)
        and getattr(row, "uploaded_fob_eur", None) is not None
        and round(float(row.uploaded_fob_eur), 2) == round(base_fob, 2)
        and (
            (row.colour_surcharge_eur is None and expected_surcharge is None)
            or (
                row.colour_surcharge_eur is not None
                and expected_surcharge is not None
                and round(float(row.colour_surcharge_eur), 2) == expected_surcharge
            )
        )
    )
    if round(float(row.final_fob_eur), 2) != expected or not metadata_matches:
        item["category"] = "auto_reprice"
        item["reason"] = "derived_price_or_metadata_drift"
    return item


def _colour_surcharge_reprice_fingerprint(items: list[dict[str, object]]) -> str:
    payload = [
        {
            key: item.get(key)
            for key in (
                "materialCode",
                "countryCode",
                "paymentTermCode",
                "category",
                "currentFinalFobEur",
                "currentBaseFobEur",
                "currentColourSurchargeEur",
                "currentUploadedFobEur",
                "colourTier",
                "singleBaseCandidates",
                "surchargeRuleSource",
                "trustedSingleBaseFobEur",
                "surchargeEur",
                "expectedFinalFobEur",
                "currentSourceMode",
            )
        }
        for item in items
    ]
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def audit_colour_surcharge_reprice(
    session: Session,
    *,
    material_codes: list[str] | None = None,
    country_code: str | None = None,
    allowed_brands: set[str] | None = None,
    allowed_countries: set[str] | None = None,
) -> dict[str, object]:
    """Audit all active Dual/Special FOB rows without changing any data."""
    normalized_codes = sorted(
        {clean_text(code) for code in (material_codes or []) if clean_text(code)}
    )
    normalized_country = clean_text(country_code).upper() if country_code else None
    stmt = select(MaterialSkuMaster).where(MaterialSkuMaster.is_active == True)
    if allowed_brands is not None:
        stmt = stmt.where(func.upper(MaterialSkuMaster.brand).in_(allowed_brands))
    if normalized_codes:
        stmt = stmt.where(MaterialSkuMaster.material_code.in_(normalized_codes))
    skus = list(session.execute(stmt.order_by(MaterialSkuMaster.material_code)).scalars().all())
    if not skus:
        return {
            "filters": {"materialCodes": normalized_codes, "countryCode": normalized_country},
            "fingerprint": _colour_surcharge_reprice_fingerprint([]),
            "summary": {"rows": 0, "autoReprice": 0, "alreadyCorrect": 0, "missingBase": 0, "ambiguousBase": 0, "explicitFinal": 0, "missingTier": 0, "missingRule": 0, "notApplicable": 0},
            "items": [],
        }
    codes = [sku.material_code for sku in skus]
    row_stmt = select(CountrySkuFobResolved).where(
        CountrySkuFobResolved.material_code.in_(codes),
        CountrySkuFobResolved.is_active == True,
        CountrySkuFobResolved.final_fob_eur > 0,
    )
    if normalized_country:
        row_stmt = row_stmt.where(CountrySkuFobResolved.country_code == normalized_country)
    if allowed_countries is not None:
        row_stmt = row_stmt.where(CountrySkuFobResolved.country_code.in_(allowed_countries))
    rows = list(
        session.execute(
            row_stmt.order_by(
                CountrySkuFobResolved.material_code,
                CountrySkuFobResolved.country_code,
                CountrySkuFobResolved.payment_term_code,
            )
        ).scalars().all()
    )
    sku_by_code = {sku.material_code: sku for sku in skus}
    duplicate_records = []
    for (material, country), group in _country_fob_groups(rows).items():
        if len(group) < 2:
            continue
        try:
            _consistent_country_fob(group)
            status = "equal"
        except CountryFobConflict:
            status = "conflict"
        duplicate_records.append({
            "materialCode": material, "countryCode": country, "status": status,
            "records": [{"paymentTermCode": row.payment_term_code,
                         "baseFobEur": row.base_fob_eur,
                         "finalFobEur": float(row.final_fob_eur)} for row in group],
        })
    items = [
        _colour_surcharge_reprice_item(session, sku_by_code[row.material_code], row)
        for row in rows
        if row.material_code in sku_by_code
    ]
    counts = Counter(item["category"] for item in items)
    summary = {
        "rows": len(items),
        "autoReprice": int(counts.get("auto_reprice", 0)),
        "alreadyCorrect": int(counts.get("already_correct", 0)),
        "missingBase": int(counts.get("missing_base", 0)),
        "ambiguousBase": int(counts.get("ambiguous_base", 0)),
        "explicitFinal": int(counts.get("explicit_final", 0)),
        "missingTier": int(counts.get("missing_tier", 0)),
        "missingRule": int(counts.get("missing_rule", 0)),
        "notApplicable": int(counts.get("not_applicable", 0)),
    }
    return {
        "filters": {"materialCodes": normalized_codes, "countryCode": normalized_country},
        "fingerprint": _colour_surcharge_reprice_fingerprint(items),
        "summary": summary,
        "items": items,
        "duplicateCountryPrices": duplicate_records,
    }


def apply_colour_surcharge_reprice_audit(
    session: Session,
    preview_fingerprint: str,
    *,
    material_codes: list[str] | None = None,
    country_code: str | None = None,
    changed_by: str | None = None,
    allowed_brands: set[str] | None = None,
    allowed_countries: set[str] | None = None,
) -> dict[str, object]:
    """Apply only a previously reviewed, unchanged audit plan."""
    # Hold the existing PostgreSQL transaction stable while validating and
    # writing the plan, including inserts into the rule/base read set.
    if isinstance(session, Session) and session.get_bind().dialect.name == "postgresql":
        session.execute(text(
            "LOCK TABLE ordering.material_sku_master, ordering.country_sku_fob_resolved, "
            "ordering.brand_colour_surcharge_rule, ordering.special_colour_surcharge_rule "
            "IN SHARE ROW EXCLUSIVE MODE"
        ))
    audit = audit_colour_surcharge_reprice(
        session,
        allowed_brands=allowed_brands,
        allowed_countries=allowed_countries,
        material_codes=material_codes,
        country_code=country_code,
    )
    if preview_fingerprint != audit["fingerprint"]:
        raise ValueError("Colour surcharge audit is stale; refresh the audit before applying")
    items = [item for item in audit["items"] if item["category"] == "auto_reprice"]
    # One call reprices all active payment-term rows for a material/country.
    # Payment terms remain reference metadata and must not cause duplicate work.
    unique_items: list[dict[str, object]] = []
    seen_targets: set[tuple[str, str]] = set()
    for item in items:
        target = (str(item["materialCode"]), str(item["countryCode"]))
        if target in seen_targets:
            continue
        seen_targets.add(target)
        unique_items.append(item)
    items = unique_items
    totals = {"requested": len(items), "updated": 0, "unchanged": 0, "skipped": 0}
    details: list[dict[str, object]] = []
    for item in items:
        result = reprice_sku_colour_surcharge_fobs(
            session,
            str(item["materialCode"]),
            country_code=str(item["countryCode"]),
            allowed_countries=allowed_countries,
            changed_by=changed_by or "colour_surcharge_audit_apply",
        )
        updated = int(result["updated"])
        totals["updated"] = int(totals["updated"]) + updated
        totals["unchanged"] = int(totals["unchanged"]) + int(result["unchanged"])
        totals["skipped"] = int(totals["skipped"]) + int(
            result["skippedManual"]
        ) + int(result["skippedNoBase"]) + int(result["skippedAmbiguous"])
        totals["skipped"] += int(result.get("skippedMissingTier", 0)) + int(result.get("skippedMissingRule", 0))
        details.append(
            {
                "materialCode": item["materialCode"],
                "countryCode": item["countryCode"],
                "updated": updated,
                "skippedManual": result["skippedManual"],
                "skippedNoBase": result["skippedNoBase"],
                "skippedAmbiguous": result["skippedAmbiguous"],
                "skippedMissingTier": result.get("skippedMissingTier", 0),
                "skippedMissingRule": result.get("skippedMissingRule", 0),
            }
        )
    return {"previewFingerprint": preview_fingerprint, "totals": totals, "details": details}


# ── CountrySkuFobResolved ──────────────────────────────────────────────


def upsert_fob_resolved(
    session: Session,
    fob: CountrySkuFobResolved,
) -> CountrySkuFobResolved:
    existing_rows = list(session.execute(
        select(CountrySkuFobResolved).where(
            CountrySkuFobResolved.country_code == fob.country_code,
            CountrySkuFobResolved.material_code == fob.material_code,
            CountrySkuFobResolved.is_active == True,
        ).order_by(CountrySkuFobResolved.payment_term_code)
    ).scalars().all())
    if existing_rows:
        # Payment terms are retained as history/metadata, but every active row
        # for this material-country must reflect the one BOM Admin price.
        for existing in existing_rows:
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
                        payment_term_code=existing.payment_term_code,
                        old_uploaded_fob_eur=old_uploaded,
                        new_uploaded_fob_eur=fob.uploaded_fob_eur,
                        old_final_fob_eur=old_final,
                        new_final_fob_eur=fob.final_fob_eur,
                        changed_by="publish_baseline",
                    )
                )
            existing.base_fob_eur = fob.base_fob_eur
            existing.payment_term_adjustment_eur = fob.payment_term_adjustment_eur
            existing.colour_surcharge_eur = fob.colour_surcharge_eur
            existing.uploaded_fob_eur = fob.uploaded_fob_eur
            existing.final_fob_eur = fob.final_fob_eur
            existing.baseline_version_id = fob.baseline_version_id
            existing.fob_source_mode = fob.fob_source_mode
            existing.fob_source_country_code = fob.fob_source_country_code
            existing.remark = fob.remark
            existing.is_active = True
        return existing_rows[0]
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
    return _consistent_country_fob(list(session.execute(stmt).scalars().all()))


def list_fobs_for_country_material_codes(
    session: Session,
    country_code: str,
    material_codes: list[str],
    payment_term_code: str | None = None,  # kept for API compat, no longer filters
    *,
    include_conflicts: bool = False,
) -> dict[str, CountrySkuFobResolved] | tuple[dict[str, CountrySkuFobResolved], list[dict[str, object]]]:
    """Return active FOB rows keyed by material code for one country."""
    codes = sorted({str(code or "").strip() for code in material_codes if str(code or "").strip()})
    if not codes:
        return ({}, []) if include_conflicts else {}
    stmt = (
        select(CountrySkuFobResolved)
        .where(
            CountrySkuFobResolved.country_code == country_code,
            CountrySkuFobResolved.material_code.in_(codes),
            CountrySkuFobResolved.is_active == True,
            CountrySkuFobResolved.final_fob_eur > 0,
        )
        .order_by(CountrySkuFobResolved.material_code)
    )
    result: dict[str, CountrySkuFobResolved] = {}
    conflicts: list[dict[str, object]] = []
    for group in _country_fob_groups(list(session.execute(stmt).scalars().all())).values():
        try:
            row = _consistent_country_fob(group)
        except CountryFobConflict:
            conflicts.append(_country_fob_conflict_payload(group))
            continue
        assert row is not None
        result[row.material_code] = row
    return (result, conflicts) if include_conflicts else result


def list_fob_by_country(
    session: Session,
    country_code: str,
    payment_term_code: str | None = None,
) -> list[CountrySkuFobResolved]:
    stmt = select(CountrySkuFobResolved).where(
        CountrySkuFobResolved.country_code == country_code,
        CountrySkuFobResolved.is_active == True,
    )
    return list(session.execute(stmt).scalars().all())


def list_active_fob_material_codes(
    session: Session,
    country_code: str,
    payment_term_code: str | None = None,  # kept for API compat, no longer filters
) -> set[str]:
    defaults = {row[0] for row in session.execute(
        select(CountrySkuFobResolved.material_code).where(
            CountrySkuFobResolved.country_code == country_code,
            CountrySkuFobResolved.is_active == True,
        )
    ).all()}
    period_materials = session.execute(select(MaterialSkuMaster.material_code).where(
        MaterialSkuMaster.bom_template.in_(select(CountryTemplateFobPeriod.bom_template).where(
            CountryTemplateFobPeriod.country_code == country_code,
            CountryTemplateFobPeriod.status == "active",
        )),
    )).scalars().all()
    return defaults | set(period_materials)


def get_country_fob_source_mapping(
    session: Session,
    target_country_code: str,
    target_payment_term_code: str | None = None,
) -> str | None:
    """Return the one source country for a target country.

    ``target_payment_term_code`` remains an ignored compatibility argument for
    old import callers.  Payment terms describe settlement, not FOB ownership.
    """
    target = clean_text(target_country_code).upper()
    sources = sorted({
        source
        for source in session.execute(
            select(CountryFobSourceMapping.source_country_code).where(
                CountryFobSourceMapping.target_country_code == target,
                CountryFobSourceMapping.is_active == True,
            )
        ).scalars().all()
        if source
    })
    if len(sources) > 1:
        raise ValueError(
            f"Conflicting FOB source mappings for country {target}: "
            f"{', '.join(sources)}"
        )
    return sources[0] if sources else None


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
